package nsq

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	gonsq "github.com/nsqio/go-nsq"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/drivers/nsq/nsqfake"
)

// TestADeliveryWhoseLeasePassedDoesNotHoldADroppedConnectionOpen is the
// connection half of the lost-lease rule, and it runs against a REAL go-nsq
// consumer because the defect lives in go-nsq's connection and not in this
// driver's model of a channel.
//
// go-nsq counts every message a connection delivers and gives the count back
// only when the message is answered with FIN or REQ. A connection whose read
// side fails with that count above zero does not finish closing: its cleanup
// waits for the count to reach zero, and the consumer reconnects to that nsqd
// only once the cleanup has finished. A delivery the lease check took out of
// circulation and left UNANSWERED, so that a frame for it could not land on a
// redelivery, therefore kept a dropped connection half-open for ever, and the
// subscription never received from that nsqd again. The redelivery that was
// supposed to supersede it and pay its debt could never arrive, because it
// needed the new connection the old one was blocking.
//
// Both places the lease check takes a delivery are covered: a settlement
// refused because the lease had passed, and a delivery Fetch dropped from the
// ring for the same reason. In each, the lease passes, the link drops, and the
// consumer has to reconnect and receive the next message.
func TestADeliveryWhoseLeasePassedDoesNotHoldADroppedConnectionOpen(t *testing.T) {
	for name, lose := range map[string]func(t *testing.T, src *source, w *wireNSQD, clock *nsqfake.Server){
		// The control: a delivery answered before the link drops leaves
		// nothing in flight, so a failure of the other two is the in-flight
		// count and not this file's server.
		"a delivery acknowledged before the link dropped": func(t *testing.T, src *source, w *wireNSQD, _ *nsqfake.Server) {
			d := wireFetch(t, src)
			if err := src.Settler().Ack(context.Background(), &d); err != nil {
				t.Fatalf("Ack: %v", err)
			}
			w.drop()
		},
		"a settlement refused after the lease passed": func(t *testing.T, src *source, w *wireNSQD, clock *nsqfake.Server) {
			d := wireFetch(t, src)
			clock.Advance(45 * time.Second)
			w.drop()
			if err := src.Settler().Ack(context.Background(), &d); !errors.Is(err, mq.ErrLeaseLost) {
				t.Fatalf("an Ack after the lease passed answered %v, want ErrLeaseLost", err)
			}
		},
		"a delivery dropped from the ring after the lease passed": func(t *testing.T, src *source, w *wireNSQD, clock *nsqfake.Server) {
			// Recorded, so that its lease is measured from the clock BEFORE
			// the advance: the handler stores the deadline before it records
			// the delivery.
			waitUntil(t, "the first delivery to be recorded", func() bool {
				src.mu.Lock()
				defer src.mu.Unlock()
				return len(src.inflight) == 1
			})
			clock.Advance(45 * time.Second)
			w.drop()
		},
	} {
		t.Run(name, func(t *testing.T) {
			w := newWireNSQD(t)
			// The model is only the driver's clock here, so that a lease
			// can pass without a wait; the broker is wireNSQD.
			clock := nsqfake.New()
			b, err := OpenBackend(context.Background(), Config{
				Addrs:       []string{w.ln.Addr().String()},
				AckDeadline: 30 * time.Second,
				LeaseGuard:  time.Second,
				// How long go-nsq waits before redialling an nsqd whose
				// connection has closed, when there is no lookupd.
				LookupdPollInterval: 20 * time.Millisecond,
				Clock:               clock,
			})
			if err != nil {
				t.Fatalf("OpenBackend: %v", err)
			}
			t.Cleanup(func() { _ = b.Close() })
			s, err := b.OpenSource(context.Background(), mq.SourceConfig{
				Topic: "orders", Subscription: "workers", Prefetch: 1,
				Metadata: mq.MetadataNativeStrict, AckDeadline: 30 * time.Second,
			})
			if err != nil {
				t.Fatalf("OpenSource: %v", err)
			}
			t.Cleanup(func() { _ = s.Close() })
			src := s.(*source)

			lose(t, src, w, clock)

			next := wireFetch(t, src)
			if got := string(next.Message.Body); got != "message-2" {
				t.Fatalf("the delivery after the reconnect is %q, want message-2 from the second connection", got)
			}
			if err := src.Settler().Ack(context.Background(), &next); err != nil {
				t.Fatalf("Ack of the delivery from the new connection: %v", err)
			}
		})
	}
}

// wireFetch fetches one delivery, failing the test if none arrives.
//
// The bound is the whole assertion for the case this file is about: a
// subscription whose dropped connection never finished closing never receives
// again, so it is a Fetch that never returns.
func wireFetch(t *testing.T, src *source) mq.DriverDelivery {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	buf := make([]mq.DriverDelivery, 1)
	n, err := src.Fetch(ctx, buf)
	if err != nil || n != 1 {
		t.Fatalf("no delivery arrived (n=%d, err=%v): the consumer never got back to the nsqd whose "+
			"connection dropped, because go-nsq was still waiting for an answer to a delivery the "+
			"driver had stopped vouching for", n, err)
	}
	return buf[0]
}

// waitUntil polls cond until it holds, failing the test after a bound.
func waitUntil(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

// wireNSQD is as much of nsqd's TCP protocol as a real go-nsq consumer needs to
// subscribe, take one message on each connection and then lose the connection.
//
// nsqfake cannot stand in here: it replaces go-nsq's consumer altogether, so
// nothing that runs against it ever meets go-nsq's in-flight count.
type wireNSQD struct {
	ln net.Listener
	wg sync.WaitGroup

	mu     sync.Mutex
	conns  []net.Conn
	sent   int
	closed bool
}

func newWireNSQD(t *testing.T) *wireNSQD {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	w := &wireNSQD{ln: ln}
	w.wg.Add(1)
	go func() {
		defer w.wg.Done()
		w.accept()
	}()
	t.Cleanup(w.close)
	return w
}

func (w *wireNSQD) accept() {
	for {
		c, err := w.ln.Accept()
		if err != nil {
			return
		}
		w.mu.Lock()
		if w.closed {
			w.mu.Unlock()
			_ = c.Close()
			return
		}
		w.conns = append(w.conns, c)
		w.mu.Unlock()
		w.wg.Add(1)
		go func() {
			defer w.wg.Done()
			w.serve(c)
		}()
	}
}

// serve answers one connection: OK to IDENTIFY and SUB, CLOSE_WAIT to CLS, and
// one message the first time the client says it is ready for one.
func (w *wireNSQD) serve(c net.Conn) {
	defer func() { _ = c.Close() }()
	r := bufio.NewReader(c)
	if _, err := io.ReadFull(r, make([]byte, len(gonsq.MagicV2))); err != nil {
		return
	}
	sent := false
	for {
		line, err := r.ReadString('\n')
		if err != nil {
			return
		}
		verb, arg, _ := strings.Cut(strings.TrimSpace(line), " ")
		switch verb {
		case "IDENTIFY", "AUTH":
			var size [4]byte
			if _, err := io.ReadFull(r, size[:]); err != nil {
				return
			}
			if _, err := io.CopyN(io.Discard, r, int64(binary.BigEndian.Uint32(size[:]))); err != nil {
				return
			}
			err = wireFrame(c, gonsq.FrameTypeResponse, []byte("OK"))
		case "SUB":
			err = wireFrame(c, gonsq.FrameTypeResponse, []byte("OK"))
		case "CLS":
			err = wireFrame(c, gonsq.FrameTypeResponse, []byte("CLOSE_WAIT"))
		case "RDY":
			if n, _ := strconv.Atoi(arg); n > 0 && !sent {
				sent = true
				err = wireFrame(c, gonsq.FrameTypeMessage, w.nextMessage())
			}
		}
		if err != nil {
			return
		}
	}
}

// nextMessage encodes the next message, numbered across connections so that a
// test can tell which connection a delivery came on.
func (w *wireNSQD) nextMessage() []byte {
	w.mu.Lock()
	w.sent++
	n := w.sent
	w.mu.Unlock()
	body := "message-" + strconv.Itoa(n)
	b := make([]byte, 10+gonsq.MsgIDLength+len(body))
	binary.BigEndian.PutUint64(b[0:], uint64(time.Now().UnixNano()))
	binary.BigEndian.PutUint16(b[8:], 1)
	copy(b[10:], fmt.Sprintf("%0*d", gonsq.MsgIDLength, n))
	copy(b[10+gonsq.MsgIDLength:], body)
	return b
}

// wireFrame writes one nsqd frame in a single write, so that a response and a
// message written from two goroutines cannot interleave.
func wireFrame(c net.Conn, frameType int32, data []byte) error {
	b := make([]byte, 8+len(data))
	binary.BigEndian.PutUint32(b[0:], uint32(4+len(data)))
	binary.BigEndian.PutUint32(b[4:], uint32(frameType))
	copy(b[8:], data)
	_, err := c.Write(b)
	return err
}

// drop closes every connection the server holds, which the client sees as the
// link going away.
func (w *wireNSQD) drop() {
	w.mu.Lock()
	defer w.mu.Unlock()
	for _, c := range w.conns {
		_ = c.Close()
	}
	w.conns = nil
}

func (w *wireNSQD) close() {
	_ = w.ln.Close()
	w.mu.Lock()
	w.closed = true
	w.mu.Unlock()
	w.drop()
	w.wg.Wait()
}
