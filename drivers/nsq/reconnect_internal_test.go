package nsq

import (
	"bufio"
	"context"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	gonsq "github.com/iqhive/go-nsq"

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

// TestAnExtendOnADroppedConnectionIsLeaseLost holds Extend to what a TOUCH can
// still do: once the connection that carried a delivery has gone, nothing
// reaches nsqd, so Extend reports ErrLeaseLost, and the subscription comes back
// on a new connection without waiting for the handler.
//
// go-nsq v1.1.0 discarded a TOUCH on a closing connection without a word, so
// Extend reported success while nsqd kept the original timeout and gave the
// message to another consumer when it ran out: two handlers on one message,
// which is what extending a lease is meant to prevent. The delivery, still
// unanswered, also kept go-nsq from finishing the close and reconnecting.
func TestAnExtendOnADroppedConnectionIsLeaseLost(t *testing.T) {
	w := newWireNSQD(t)
	b := openWireBackend(t, w, Config{
		AckDeadline: 30 * time.Second, LeaseGuard: time.Second, LookupdPollInterval: 20 * time.Millisecond,
	})
	s, err := b.OpenSource(context.Background(), mq.SourceConfig{
		Topic: "orders", Subscription: "workers", Prefetch: 1,
		Metadata: mq.MetadataNativeStrict, AckDeadline: 30 * time.Second,
	})
	if err != nil {
		t.Fatalf("OpenSource: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	src := s.(*source)
	ext, ok := src.Settler().(mq.DeadlineExtender)
	if !ok {
		t.Fatal("the settler does not extend")
	}
	d := wireFetch(t, src)
	if err := ext.Extend(context.Background(), &d, 20*time.Second); err != nil {
		t.Fatalf("Extend on a live connection: %v", err)
	}
	waitUntil(t, "the TOUCH to reach nsqd", func() bool { return w.received("TOUCH") })

	w.drop()
	waitUntil(t, "Extend to report the lease lost on the dropped connection", func() bool {
		err = ext.Extend(context.Background(), &d, 20*time.Second)
		return err != nil
	})
	if !errors.Is(err, mq.ErrLeaseLost) {
		t.Fatalf("Extend on a dropped connection = %v, want ErrLeaseLost", err)
	}
	if err := src.Settler().Ack(context.Background(), &d); !errors.Is(err, mq.ErrLeaseLost) {
		t.Fatalf("Ack after Extend reported the lease lost = %v, want ErrLeaseLost", err)
	}
	// The sweeper answers the delivery, which lets go-nsq finish closing the
	// connection and redial, and the next message comes on the new one.
	next := wireFetch(t, src)
	if got := string(next.Message.Body); got != "message-2" {
		t.Fatalf("the delivery after the reconnect is %q, want message-2 from the second connection", got)
	}
	if err := src.Settler().Ack(context.Background(), &next); err != nil {
		t.Fatalf("Ack of the delivery from the new connection: %v", err)
	}
}

// TestAPublishAfterNSQDDroppedTheConnectionIsRaceFree holds nj-28, under -race:
// a publication after nsqd closed the producer's connection reconnects.
// go-nsq v1.1.0's producer read the old connection for a log line after it had
// signalled that it was done with it, and the reconnect replaced it then, so
// the race detector failed any test that published across a dropped
// connection.
func TestAPublishAfterNSQDDroppedTheConnectionIsRaceFree(t *testing.T) {
	w := newWireNSQD(t)
	b := openWireBackend(t, w, Config{})
	snk, err := b.OpenSink(context.Background(), mq.SinkConfig{Topic: "orders"})
	if err != nil {
		t.Fatalf("OpenSink: %v", err)
	}
	publish := func() error { return snk.Publish(context.Background(), &mq.Message{Body: []byte("x")}, nil) }
	if err := publish(); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	w.drop()
	// go-nsq refuses with ErrNotConnected until it has finished with the
	// dropped connection, and connects again on the next publication after.
	waitUntil(t, "a publication on a new connection", func() bool {
		return publish() == nil && slices.Contains(w.commandsOf(1), "PUB orders")
	})
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
// With credit set it sends instead as nsqd does, a new message whenever fewer
// are in flight than the client's RDY, and takes a FIN or a REQ as the end of
// one.
//
// nsqfake cannot stand in here: it replaces go-nsq's consumer altogether, so
// nothing that runs against it ever meets go-nsq's in-flight count.
type wireNSQD struct {
	ln net.Listener
	wg sync.WaitGroup

	// identified receives a connection's index once the server has read its
	// IDENTIFY. Connections are numbered from zero in the order the server
	// accepted them.
	identified chan int
	// done is closed by close, which releases a held handshake.
	done chan struct{}

	mu sync.Mutex
	// hold delays the answer to connection n's IDENTIFY until the channel is
	// closed, which is a handshake still in progress. hangUp closes
	// connection n instead of answering its IDENTIFY. quiet stops connection
	// n sending its message. A test sets them, and credit, before the
	// connection they name is made.
	hold   map[int]chan struct{}
	hangUp map[int]bool
	quiet  map[int]bool
	credit bool
	// reset answers connection n's IDENTIFY and then resets the connection,
	// which is an nsqd going away under a connection a moment after it came
	// up.
	reset map[int]bool
	// identify, when set, is the answer to IDENTIFY: the JSON an nsqd that
	// negotiates features sends. auth is the answer to AUTH, publish the
	// answer to PUB, MPUB and DPUB, and subscribe the answer to SUB. An error
	// frame among them closes the connection once it is sent, as nsqd closes
	// one after a fatal error.
	identify  string
	auth      wireAnswer
	publish   wireAnswer
	subscribe wireAnswer
	// holdCLS delays the answer to connection n's CLS until the channel is
	// closed, which is a consumer stop still in progress.
	holdCLS map[int]chan struct{}
	// upgrade, when set, is the TLS configuration the server upgrades each
	// connection with once it has answered IDENTIFY, which is what an nsqd
	// whose answer agrees to TLS does next. It pairs with an identify answer
	// saying "tls_v1":true.
	upgrade  *tls.Config
	upgraded map[int]bool
	commands map[int][]string
	// ended records the connections whose serving has stopped, because the
	// client closed them or the server did.
	ended    map[int]bool
	conns    []net.Conn
	accepted int
	sent     int
	closed   bool
}

func newWireNSQD(t *testing.T) *wireNSQD {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	w := &wireNSQD{
		ln:         ln,
		identified: make(chan int, 1),
		done:       make(chan struct{}),
		hold:       map[int]chan struct{}{},
		hangUp:     map[int]bool{},
		quiet:      map[int]bool{},
		reset:      map[int]bool{},
		holdCLS:    map[int]chan struct{}{},
		upgraded:   map[int]bool{},
		commands:   map[int][]string{},
		ended:      map[int]bool{},
	}
	w.wg.Add(1)
	go func() {
		defer w.wg.Done()
		w.accept()
	}()
	t.Cleanup(w.close)
	return w
}

// openWireBackend opens a backend whose one nsqd is w, closed when the test
// ends.
func openWireBackend(t *testing.T, w *wireNSQD, cfg Config) *Backend {
	t.Helper()
	cfg.Addrs = []string{w.ln.Addr().String()}
	cfg.APITimeout = 2 * time.Second
	b, err := OpenBackend(context.Background(), cfg)
	if err != nil {
		t.Fatalf("OpenBackend: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })
	return b
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
		n := w.accepted
		w.accepted++
		w.conns = append(w.conns, c)
		w.mu.Unlock()
		w.wg.Add(1)
		go func() {
			defer w.wg.Done()
			w.serve(n, c)
		}()
	}
}

// serve answers connection n: IDENTIFY, AUTH, PUB, MPUB, DPUB and SUB with the
// answers the server was given (OK when none), CLOSE_WAIT to CLS, and one
// message the first time the client says it is ready for one. Every command
// after IDENTIFY is recorded.
func (w *wireNSQD) serve(n int, c net.Conn) {
	defer func() {
		_ = c.Close()
		w.mu.Lock()
		w.ended[n] = true
		w.mu.Unlock()
	}()
	r := bufio.NewReader(c)
	if _, err := io.ReadFull(r, make([]byte, len(gonsq.MagicV2))); err != nil {
		return
	}
	sent := false
	rdy, inflight := 0, 0
	more := func() error {
		for inflight < rdy {
			inflight++
			if err := wireFrame(c, gonsq.FrameTypeMessage, w.nextMessage()); err != nil {
				return err
			}
		}
		return nil
	}
	for {
		line, err := r.ReadString('\n')
		if err != nil {
			return
		}
		verb, arg, _ := strings.Cut(strings.TrimSpace(line), " ")
		w.mu.Lock()
		hold, hangUp, quiet, credit, reset := w.hold[n], w.hangUp[n], w.quiet[n], w.credit, w.reset[n]
		identify, auth, publish, subscribe, upgrade := w.identify, w.auth, w.publish, w.subscribe, w.upgrade
		holdCLS := w.holdCLS[n]
		if verb != "IDENTIFY" {
			w.commands[n] = append(w.commands[n], strings.TrimSpace(line))
		}
		w.mu.Unlock()
		switch verb {
		case "IDENTIFY", "AUTH", "PUB", "MPUB", "DPUB":
			var size [4]byte
			if _, err := io.ReadFull(r, size[:]); err != nil {
				return
			}
			if _, err := io.CopyN(io.Discard, r, int64(binary.BigEndian.Uint32(size[:]))); err != nil {
				return
			}
			answer := publish
			switch verb {
			case "IDENTIFY":
				select {
				case w.identified <- n:
				default:
				}
				if hangUp {
					return
				}
				if hold != nil {
					select {
					case <-hold:
					case <-w.done:
						return
					}
				}
				answer = wireAnswer{data: identify}
			case "AUTH":
				answer = auth
			}
			if err := answer.write(c); err != nil || answer.frameType == gonsq.FrameTypeError {
				return
			}
			if verb == "IDENTIFY" && reset {
				// An RST rather than a FIN, so that the client's next write
				// fails instead of going into a half-closed connection.
				_ = c.(*net.TCPConn).SetLinger(0)
				return
			}
			if verb == "IDENTIFY" && upgrade != nil {
				tc := tls.Server(c, upgrade)
				if err := tc.Handshake(); err != nil {
					return
				}
				w.mu.Lock()
				w.upgraded[n] = true
				w.mu.Unlock()
				c, r = tc, bufio.NewReader(tc)
				if err := wireFrame(c, gonsq.FrameTypeResponse, []byte("OK")); err != nil {
					return
				}
			}
		case "SUB":
			if err := subscribe.write(c); err != nil || subscribe.frameType == gonsq.FrameTypeError {
				return
			}
		case "CLS":
			if holdCLS != nil {
				select {
				case <-holdCLS:
				case <-w.done:
					return
				}
			}
			err = wireFrame(c, gonsq.FrameTypeResponse, []byte("CLOSE_WAIT"))
		case "RDY":
			count, _ := strconv.Atoi(arg)
			switch {
			case credit:
				rdy = count
				err = more()
			case count > 0 && !sent && !quiet:
				sent = true
				err = wireFrame(c, gonsq.FrameTypeMessage, w.nextMessage())
			}
		case "FIN", "REQ":
			if credit {
				inflight--
				err = more()
			}
		}
		if err != nil {
			return
		}
	}
}

// commandsOf is every command connection n has sent after its IDENTIFY.
func (w *wireNSQD) commandsOf(n int) []string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return slices.Clone(w.commands[n])
}

// received reports whether any connection sent the server a command with this
// verb.
func (w *wireNSQD) received(verb string) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	for _, cmds := range w.commands {
		for _, c := range cmds {
			if c == verb || strings.HasPrefix(c, verb+" ") {
				return true
			}
		}
	}
	return false
}

// wasUpgraded reports whether connection n completed the TLS upgrade.
func (w *wireNSQD) wasUpgraded(n int) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.upgraded[n]
}

// hasEnded reports whether connection n is no longer being served.
func (w *wireNSQD) hasEnded(n int) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.ended[n]
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

// wireAnswer is a frame the server sends back. The zero value is nsqd's OK.
type wireAnswer struct {
	frameType int32
	data      string
}

func (a wireAnswer) write(c net.Conn) error {
	data := a.data
	if data == "" {
		data = "OK"
	}
	return wireFrame(c, a.frameType, []byte(data))
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
	if !w.closed {
		close(w.done)
	}
	w.closed = true
	w.mu.Unlock()
	w.drop()
	w.wg.Wait()
}
