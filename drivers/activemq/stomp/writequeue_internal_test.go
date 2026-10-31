package stomp

import (
	"bytes"
	"context"
	"errors"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	gostomp "github.com/go-stomp/stomp/v3"
	"github.com/go-stomp/stomp/v3/frame"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/drivers/activemq/stomp/internal/stompsrv"
	"github.com/iqhive/iqmq/internal/activemq"
	"github.com/iqhive/iqmq/internal/errcode"
)

// A broker that stops reading a socket it still holds open fills go-stomp's
// write queue. `Conn.Send`, `Conn.Ack` and `Conn.Nack` take no context, and on
// a full queue each one waited up to MsgSendTimeout (Config.ReceiptTimeout)
// for a slot, holding the library's closeMutex and, for a settlement,
// settleMu. The tests here stop the server reading ([stompsrv.Server.StopReading])
// and hold a frame that waits for no answer to the caller's context, and they
// hold a full queue whose writer is still moving to the opposite answer: the
// frame waits for room and goes out.

// deafTimeout is Config.ReceiptTimeout here, and so go-stomp's MsgSendTimeout:
// how long a frame used to wait for a slot in a full write queue, whatever its
// context. It is also [stallAfter].
const deafTimeout = 5 * time.Second

// stalledBody is a body large enough that a few frames fill the socket's
// buffers, and within the server's MaxDeclaredBody.
func stalledBody() []byte { return bytes.Repeat([]byte("x"), 512<<10) }

// deafBackend is a server and a backend dialled to it with receipt as its
// Config.ReceiptTimeout.
func deafBackend(t *testing.T, receipt time.Duration) (*stompsrv.Server, *Backend) {
	t.Helper()
	return backendOn(t, stompsrv.Options{}, receipt)
}

// backendOn is [deafBackend] with a server started with o, as Artemis.
func backendOn(t *testing.T, o stompsrv.Options, receipt time.Duration) (*stompsrv.Server, *Backend) {
	t.Helper()
	o.ServerHeader = activemq.ServerArtemis + "/2.44.0"
	srv, err := stompsrv.New(o)
	if err != nil {
		t.Fatalf("starting the in-process STOMP server: %v", err)
	}
	t.Cleanup(func() {
		_ = srv.Close()
		for _, v := range srv.Violations() {
			t.Errorf("the driver committed a STOMP protocol violation: %v", v)
		}
	})
	ctx, cancel := context.WithTimeout(context.Background(), requeueBudget)
	defer cancel()
	b, err := OpenBackend(ctx, Config{
		Broker:         activemq.Artemis,
		Addrs:          []string{srv.Addr()},
		ReceiptTimeout: receipt,
		DialTimeout:    requeueBudget,
		NoReconnect:    true,
	})
	if err != nil {
		t.Fatalf("OpenBackend: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })
	return srv, b
}

// fillQueue writes SEND frames on conn, past the driver, until go-stomp's
// write queue is full and its writer has stopped, and returns how many it
// wrote. The server must have stopped reading conn.
//
// A full queue alone says the writer is behind, not that it has stopped: the
// socket's buffers may still be taking bytes, and the frames went in faster
// than one is written. Measured, a settlement made at that moment found a slot
// and went out. So the queue has to stay full for stallWindow, which a writer
// that is still writing cannot manage: it takes a frame of stalledBody's size
// off the queue in about a millisecond. The kernel's buffers give no signal a
// test could wait on instead.
func fillQueue(t *testing.T, conn *gostomp.Conn) int {
	t.Helper()
	body := stalledBody()
	n := 0
	for {
		for conn.Stats().CurrentWriteChanSize < writeQueue {
			if n == 1024 {
				t.Fatalf("%d frames of %d bytes went out and go-stomp's writer never stopped", n, len(body))
			}
			// Room was checked first and nothing else is writing, so this
			// queues the frame and returns.
			if err := conn.Send("/queue/filler", "", body); err != nil {
				t.Fatalf("filler frame %d: %v", n, err)
			}
			n++
		}
		if staysFull(conn) {
			return n
		}
	}
}

// stallWindow is how long go-stomp's write queue has to stay full before
// [fillQueue] calls its writer stopped.
const stallWindow = 300 * time.Millisecond

// staysFull reports whether conn's write queue stays full for stallWindow.
func staysFull(conn *gostomp.Conn) bool {
	until := time.Now().Add(stallWindow)
	for time.Now().Before(until) {
		if conn.Stats().CurrentWriteChanSize < writeQueue {
			return false
		}
		time.Sleep(time.Millisecond)
	}
	return true
}

// backendConn is the backend's live connection.
func backendConn(b *Backend) *gostomp.Conn {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.conn
}

// sourceConn is the source's live connection.
func sourceConn(s *Source) *gostomp.Conn {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.conn
}

// awaitWaiter waits until a frame is waiting for room on conn's socket, which
// is when it has taken the socket's progress channel ([watchedConn.progressed]),
// or until done has a result, whichever is first. It reports whether a frame
// is waiting.
func awaitWaiter(t *testing.T, b *Backend, conn *gostomp.Conn, done <-chan timed) bool {
	t.Helper()
	sock := b.writerOf(conn)
	if sock == nil {
		t.Fatal("the connection has no watched socket")
	}
	// A frame that gave up waiting leaves its channel behind, and the next
	// write would clear it. On a stalled socket none comes, so it is cleared
	// here: a frame already waiting wakes, finds no room and takes a new one.
	// Without this, a test that waited once already read the first wait's
	// channel as the second wait and asserted before the second had begun.
	sock.wake()
	deadline := time.Now().Add(requeueBudget)
	for time.Now().Before(deadline) {
		sock.pmu.Lock()
		waiting := sock.progress != nil
		sock.pmu.Unlock()
		if waiting {
			return true
		}
		if len(done) > 0 {
			return false
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("nothing waited for room on the connection within %v", requeueBudget)
	return false
}

// isStall reports whether err is the stall's refusal, [errWriteStalled]. An
// errcode sentinel is Is every error of its code, so the outer bound's
// refusal would pass errors.Is; the sentinel itself is compared instead.
func isStall(err error) bool {
	var e *errcode.Error
	return errors.As(err, &e) && e == errWriteStalled
}

// timed is an outcome and how long it took.
type timed struct {
	err  error
	took time.Duration
}

// TestAReceiptlessPublicationEndsWithItsContextWhenTheBrokerStopsReading is a
// ConfirmNone publication on a connection the broker has stopped reading.
//
// It used to wait for a slot in go-stomp's write queue for the whole of
// Config.ReceiptTimeout, whatever its context said, and then report the
// library's send timeout. Now it waits for room until its context ends, and
// once the socket has taken nothing for [stallAfter] it is refused at once.
// Both answers are definite, nothing is queued, and the publication after the
// broker reads again goes out.
func TestAReceiptlessPublicationEndsWithItsContextWhenTheBrokerStopsReading(t *testing.T) {
	t.Parallel()
	srv, b := deafBackend(t, deafTimeout)
	ctx, cancel := context.WithTimeout(context.Background(), requeueBudget)
	defer cancel()
	snk, err := b.OpenSink(ctx, mq.SinkConfig{Topic: "orders", Confirmation: mq.ConfirmNone})
	if err != nil {
		t.Fatalf("OpenSink: %v", err)
	}
	if err := srv.StopReading(); err != nil {
		t.Fatal(err)
	}
	const deadline = 100 * time.Millisecond
	body := stalledBody()
	queued, ended := 0, 0
	var refused error
	began := time.Now()
	for {
		if queued+ended == 1024 || time.Since(began) > 2*deafTimeout {
			t.Fatalf("%d publications of %d bytes were queued and %d ended with their context on a connection "+
				"nothing reads in %v, and none was refused: the stall (%v) should have been", queued, len(body),
				ended, time.Since(began), stallAfter(deafTimeout))
		}
		pctx, pcancel := context.WithTimeout(context.Background(), deadline)
		start := time.Now()
		err := snk.Publish(pctx, &mq.Message{Body: body}, nil)
		took := time.Since(start)
		pcancel()
		if took > deafTimeout/2 {
			t.Fatalf("publication %d with a %v context took %v (ReceiptTimeout %v) and returned %v: it waited "+
				"for a slot in go-stomp's write queue", queued+ended, deadline, took, deafTimeout, err)
		}
		if err == nil {
			queued++
			continue
		}
		if mq.IsAmbiguous(err) {
			t.Fatalf("publication %d returned %v, ambiguous: nothing was queued", queued+ended, err)
		}
		if mq.CodeOf(err) == mq.CodeDeadlineExceeded {
			ended++
			continue
		}
		refused = err
		if took >= deadline {
			t.Errorf("the refusal took %v, as long as the publication's context: a socket that has taken nothing "+
				"for stallAfter (%v) is refused at once", took, stallAfter(deafTimeout))
		}
		break
	}
	if mq.CodeOf(refused) != mq.CodeBackpressure || !isStall(refused) {
		t.Errorf("a stalled write queue was refused with %v (code %v), want the stall's CodeBackpressure", refused,
			mq.CodeOf(refused))
	}
	if !mq.IsRetryable(refused) {
		t.Errorf("the refusal %v is not retryable", refused)
	}
	if ended == 0 {
		t.Error("no publication waited for room until its context ended before the stall was called")
	}
	conn := backendConn(b)
	if n := conn.Stats().CurrentWriteChanSize; n > writeQueue-writeReserve {
		t.Errorf("the write queue holds %d frames, more than %d: receipt-less publications took the last %d slots, "+
			"which are left to the frames that wait for a receipt", n, writeQueue-writeReserve, writeReserve)
	}
	if got := b.unansweredOn(conn).Load(); got != int64(queued) {
		t.Errorf("the connection counts %d unanswered frames after %d queued publications: one that was refused "+
			"or ended with its context was counted", got, queued)
	}
	if got := b.Ambiguous(); got != 0 {
		t.Errorf("Ambiguous is %d after refusals that queued nothing", got)
	}

	srv.ResumeReading()
	if err := srv.WaitFrames(frame.SEND, queued, requeueBudget); err != nil {
		t.Fatalf("the server never read the queued publications: %v", err)
	}
	if err := snk.Publish(ctx, &mq.Message{Body: []byte("again")}, nil); err != nil {
		t.Fatalf("the retry once the broker reads again: %v", err)
	}
	if err := srv.WaitFrames(frame.SEND, queued+1, requeueBudget); err != nil {
		t.Fatalf("the retried publication never arrived: %v", err)
	}
}

// TestBackendCloseDoesNotWaitBehindAPublication is Backend.Close while a
// receipt-less publication meets a full write queue.
//
// The publication used to hold go-stomp's closeMutex for the whole of
// Config.ReceiptTimeout, so Close's DISCONNECT waited for it and then for a
// slot of its own: two receipt timeouts. Now the publication waits for room
// holding nothing, and Close waits for nothing but its own DISCONNECT, which
// on a broker that reads nothing is one receipt timeout before the connection
// is dropped.
func TestBackendCloseDoesNotWaitBehindAPublication(t *testing.T) {
	t.Parallel()
	const receipt = 3 * time.Second
	srv, b := deafBackend(t, receipt)
	ctx, cancel := context.WithTimeout(context.Background(), requeueBudget)
	defer cancel()
	snk, err := b.OpenSink(ctx, mq.SinkConfig{Topic: "orders", Confirmation: mq.ConfirmNone})
	if err != nil {
		t.Fatalf("OpenSink: %v", err)
	}
	if err := srv.StopReading(); err != nil {
		t.Fatal(err)
	}
	fillQueue(t, backendConn(b))
	published := make(chan error, 1)
	go func() {
		pctx, pcancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer pcancel()
		published <- snk.Publish(pctx, &mq.Message{Body: []byte("late")}, nil)
	}()
	// A publication that ends with its context is done here. One that waits
	// on go-stomp's queue is still inside the library after this long, which
	// is where Close has to meet it for the test to say anything.
	done := false
	select {
	case err = <-published:
		done = true
	case <-time.After(200 * time.Millisecond):
	}
	start := time.Now()
	_ = b.Close()
	took := time.Since(start)
	if limit := receipt + receipt/2; took > limit {
		t.Errorf("Backend.Close took %v with ReceiptTimeout %v: it waited behind a publication holding go-stomp's "+
			"closeMutex. Its own DISCONNECT accounts for one ReceiptTimeout at most", took, receipt)
	}
	if !done {
		err = <-published
	}
	if code := mq.CodeOf(err); mq.IsAmbiguous(err) || (code != mq.CodeDeadlineExceeded && code != mq.CodeBackpressure) {
		t.Errorf("the publication met a full write queue and returned %v (code %v, ambiguous %v), want its "+
			"context's end or CodeBackpressure, definite", err, code, mq.IsAmbiguous(err))
	}
}

// sendsInLibrary is how many goroutines are inside go-stomp's Conn.Send,
// waiting for a slot, for closeMutex or for a RECEIPT.
func sendsInLibrary() int {
	buf := make([]byte, 1<<22)
	for {
		n := runtime.Stack(buf, true)
		if n < len(buf) {
			return strings.Count(string(buf[:n]), "go-stomp/stomp/v3.(*Conn).Send(")
		}
		buf = make([]byte, 2*len(buf))
	}
}

// TestReceiptedPublishersDoNotPileUpOnAStoppedBroker is ConfirmAccepted
// publications with deadlines on a connection the broker has stopped reading,
// and then Backend.Close.
//
// Each used to start a goroutine at once, which parked in go-stomp waiting
// for a slot while holding the library's closeMutex for up to ReceiptTimeout,
// one after another, and Close's DISCONNECT queued behind all of them:
// measured, 1270 such goroutines after 4 s of 32 publishers, and here 160
// after 160 publications, with Close not back after 6 s. A receipted
// publication now waits for room before it starts its goroutine, so the
// goroutines are the frames that found room (25 to 28 here), and Close waits
// for its own DISCONNECT and nothing else (2.0 s). It is not parallel, because it counts the
// goroutines in go-stomp's Send across the whole process.
func TestReceiptedPublishersDoNotPileUpOnAStoppedBroker(t *testing.T) {
	const receipt = 2 * time.Second
	srv, b := deafBackend(t, receipt)
	ctx, cancel := context.WithTimeout(context.Background(), requeueBudget)
	defer cancel()
	snk, err := b.OpenSink(ctx, mq.SinkConfig{Topic: "orders", Confirmation: mq.ConfirmAccepted})
	if err != nil {
		t.Fatalf("OpenSink: %v", err)
	}
	if err := srv.StopReading(); err != nil {
		t.Fatal(err)
	}
	before := sendsInLibrary()
	const publishers, phase = 16, time.Second
	body := stalledBody()
	var wg sync.WaitGroup
	attempts := make([]int, publishers)
	for p := range publishers {
		wg.Go(func() {
			end := time.Now().Add(phase)
			for time.Now().Before(end) {
				pctx, pcancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
				_ = snk.Publish(pctx, &mq.Message{Body: body}, nil)
				pcancel()
				attempts[p]++
			}
		})
	}
	wg.Wait()
	total := 0
	for _, n := range attempts {
		total += n
	}
	parked := sendsInLibrary() - before
	t.Logf("%d receipted publications, %d goroutines in go-stomp's Send", total, parked)
	if limit := 2 * writeQueue; parked > limit {
		t.Errorf("%d goroutines are in go-stomp's Send after %d receipted publications on a broker that reads "+
			"nothing, more than %d: publications started sends that had no room to go into", parked, total, limit)
	}
	done := make(chan time.Duration, 1)
	go func() {
		start := time.Now()
		_ = b.Close()
		done <- time.Since(start)
	}()
	select {
	case took := <-done:
		t.Logf("Backend.Close took %v", took)
		if limit := receipt + receipt/2; took > limit {
			t.Errorf("Backend.Close took %v with ReceiptTimeout %v: its DISCONNECT waited behind receipted sends",
				took, receipt)
		}
	case <-time.After(3 * receipt):
		t.Fatalf("Backend.Close had not returned after %v with ReceiptTimeout %v and %d goroutines in go-stomp's "+
			"Send: its DISCONNECT is queued behind them", 3*receipt, receipt, parked)
	}
}

// TestHandOverRunsUnderGoStompsLock pins the go-stomp behaviour
// [Backend.handOver] rests on: the library runs a SEND's options while it
// holds its closeMutex, right before it queues the frame, with a receipt and
// without one.
//
// If a go-stomp upgrade ran them anywhere else, the gate would be let go with
// nothing holding the next frame back, and more frames than there is room for
// could reach the library and park on its lock. A simulation of that failed
// TestReceiptedPublishersDoNotPileUpOnAStoppedBroker one run in three. Here a
// second Send starts on the same connection from inside the option: under the
// lock it cannot return before the first frame is queued, so it must still be
// blocked when the option ends, and the server must read the first frame
// first.
func TestHandOverRunsUnderGoStompsLock(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		opts []func(*frame.Frame) error
	}{
		{"without a receipt", nil},
		{"with a receipt", []func(*frame.Frame) error{gostomp.SendOpt.Receipt}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			srv, err := stompsrv.New(stompsrv.Options{})
			if err != nil {
				t.Fatalf("starting the in-process STOMP server: %v", err)
			}
			t.Cleanup(func() { _ = srv.Close() })
			conn, err := gostomp.Dial("tcp", srv.Addr())
			if err != nil {
				t.Fatalf("dialling the server: %v", err)
			}
			t.Cleanup(func() { _ = conn.Disconnect() })
			// lockWindow is how long the option holds the first Send inside
			// go-stomp. A second Send that is not held back by the lock
			// queues its frame and returns in microseconds.
			const lockWindow = 100 * time.Millisecond
			second := make(chan error, 1)
			var early error
			returned := false
			probe := func(*frame.Frame) error {
				go func() { second <- conn.Send("/queue/second", "", []byte("2")) }()
				select {
				case early = <-second:
					returned = true
				case <-time.After(lockWindow):
				}
				return nil
			}
			opts := append(append([]func(*frame.Frame) error(nil), tc.opts...), probe)
			if err := conn.Send("/queue/first", "", []byte("1"), opts...); err != nil {
				t.Fatalf("the first Send: %v", err)
			}
			if returned {
				t.Fatalf("a Send started inside the first one's option returned (%v) while the option ran: go-stomp "+
					"runs a SEND's options without holding its closeMutex, so Backend.handOver lets the gate go with "+
					"nothing holding the next frame back", early)
			}
			if err := <-second; err != nil {
				t.Fatalf("the second Send: %v", err)
			}
			if err := srv.WaitFrames(frame.SEND, 2, requeueBudget); err != nil {
				t.Fatalf("the server never read both SENDs: %v", err)
			}
			sends := srv.FramesOf(frame.SEND)
			if got := sends[0].Get(frame.Destination); got != "/queue/first" {
				t.Errorf("the server read %q first, want /queue/first: go-stomp queued the second frame ahead of "+
					"the one whose options were running", got)
			}
		})
	}
}

// settlements are the two settlement frames that wait for no answer.
var settlements = []struct {
	name    string
	command string
	settle  func(*Source, context.Context, *mq.DriverDelivery) error
}{
	{"ack", frame.ACK, func(s *Source, ctx context.Context, d *mq.DriverDelivery) error {
		return s.Settler().Ack(ctx, d)
	}},
	{"nack drop", frame.NACK, func(s *Source, ctx context.Context, d *mq.DriverDelivery) error {
		var o mq.NackOptions
		if err := o.Apply(mq.NackDrop()); err != nil {
			return err
		}
		return s.Settler().Nack(ctx, d, o)
	}},
}

// TestASettlementEndsWithItsContextWhenTheBrokerStopsReading is an ACK and a
// NACK frame on a source connection the broker has stopped reading.
//
// Each used to wait for a slot for the whole of Config.ReceiptTimeout while
// holding settleMu for reading, so a requeue's reset, which takes it for
// writing, waited as long. Now each waits for room holding neither lock, ends
// with its context, and is refused once the socket has taken nothing for
// [stallAfter]. Neither answer burns the delivery's slot or counts it stale,
// and the same settlement goes out once the broker reads again.
func TestASettlementEndsWithItsContextWhenTheBrokerStopsReading(t *testing.T) {
	t.Parallel()
	for _, tc := range settlements {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			srv, b, s := requeueFixture(t, func(c *Config) { c.ReceiptTimeout = deafTimeout })
			ids, err := srv.Deliver(s.Destination(), stompsrv.Message{Body: []byte("one")})
			if err != nil {
				t.Fatalf("Deliver: %v", err)
			}
			d := fetchAll(t, s, 1)
			// The source's connection is the newest: it is dialled after the
			// backend's.
			if err := srv.StopReading(); err != nil {
				t.Fatal(err)
			}
			conn := sourceConn(s)
			filled := fillQueue(t, conn)

			// A short context ends the wait.
			sctx, scancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			start := time.Now()
			err = tc.settle(s, sctx, &d[0])
			took := time.Since(start)
			scancel()
			if took > deafTimeout/2 {
				t.Fatalf("%s with a 100ms context took %v (ReceiptTimeout %v) and returned %v: it waited for a slot "+
					"in go-stomp's write queue holding settleMu, which a requeue's reset waited behind",
					tc.name, took, deafTimeout, err)
			}
			if code := mq.CodeOf(err); mq.IsAmbiguous(err) || (code != mq.CodeDeadlineExceeded && code != mq.CodeBackpressure) {
				t.Errorf("%s on a full write queue returned %v (code %v, ambiguous %v), want its context's end or "+
					"CodeBackpressure, definite: no frame was queued", tc.name, err, code, mq.IsAmbiguous(err))
			}

			// A long one is cut short by the stall, and holds no lock while
			// it waits.
			result := make(chan timed, 1)
			go func() {
				lctx, lcancel := context.WithTimeout(context.Background(), requeueBudget)
				defer lcancel()
				start := time.Now()
				err := tc.settle(s, lctx, &d[0])
				result <- timed{err, time.Since(start)}
			}()
			if awaitWaiter(t, b, conn, result) {
				locked := s.settleMu.TryLock()
				if locked {
					s.settleMu.Unlock()
				}
				if !locked {
					t.Error("settleMu is held while the settlement waits for room, so a requeue's reset waits behind it")
				}
			}
			got := <-result
			if !isStall(got.err) {
				t.Errorf("%s with a %v context ended after %v with %v: the socket had taken nothing since before it "+
					"began, so the stall (%v) should have ended it first", tc.name, requeueBudget, got.took, got.err,
					stallAfter(deafTimeout))
			}
			if mq.CodeOf(got.err) != mq.CodeBackpressure || !errors.Is(got.err, mq.ErrBackpressure) {
				t.Errorf("%s on a stalled write queue returned %v (code %v), want CodeBackpressure", tc.name, got.err,
					mq.CodeOf(got.err))
			}
			if mq.IsAmbiguous(got.err) || !mq.IsRetryable(got.err) {
				t.Errorf("the refusal is ambiguous=%v retryable=%v, want a definite, retryable one: no frame was "+
					"queued", mq.IsAmbiguous(got.err), mq.IsRetryable(got.err))
			}
			if got := s.StaleSettlements(); got != 0 {
				t.Errorf("StaleSettlements is %d: a settlement that queued nothing was counted as stale", got)
			}

			srv.ResumeReading()
			if err := srv.WaitFrames(frame.SEND, filled, requeueBudget); err != nil {
				t.Fatalf("the server never read the filler: %v", err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), requeueBudget)
			defer cancel()
			if err := tc.settle(s, ctx, &d[0]); err != nil {
				t.Fatalf("settling again once the broker reads: %v. A settlement that queued nothing has to leave "+
					"the delivery settleable", err)
			}
			if err := srv.WaitFrames(tc.command, 1, requeueBudget); err != nil {
				t.Fatalf("the settlement never arrived: %v", err)
			}
			if got := srv.FramesOf(tc.command)[0].Get(frame.Id); got != ids[0] {
				t.Errorf("the %s frame names %q, want the delivery's %q", tc.command, got, ids[0])
			}
		})
	}
}

// moving is a ReceiptTimeout long enough that [stallAfter] cannot end a wait
// in the time a test takes to start the writer again, and promptly is how soon
// after the writer moves a waiting frame has to go out: the server reads a
// full queue's frames in milliseconds, and the stall timer would not wake the
// wait for 40 s.
const (
	moving   = 40 * time.Second
	promptly = 2 * time.Second
)

// TestAFullQueueWaitsForAWriterThatMoves is a publication and a settlement
// that meet a full write queue whose writer then moves again.
//
// A full queue is a broker that is behind, which a burst makes of a broker
// that is working, so it is not a refusal: the frame waits for room and goes
// out. Refusing at the queue's depth failed four scenarios of the conformance
// suite on a working broker.
func TestAFullQueueWaitsForAWriterThatMoves(t *testing.T) {
	t.Parallel()
	t.Run("publication", func(t *testing.T) {
		t.Parallel()
		srv, b := deafBackend(t, moving)
		ctx, cancel := context.WithTimeout(context.Background(), requeueBudget)
		defer cancel()
		snk, err := b.OpenSink(ctx, mq.SinkConfig{Topic: "orders", Confirmation: mq.ConfirmNone})
		if err != nil {
			t.Fatalf("OpenSink: %v", err)
		}
		if err := srv.StopReading(); err != nil {
			t.Fatal(err)
		}
		conn := backendConn(b)
		filled := fillQueue(t, conn)
		result := make(chan timed, 1)
		go func() { result <- timed{err: snk.Publish(ctx, &mq.Message{Body: []byte("waits")}, nil)} }()
		if !awaitWaiter(t, b, conn, result) {
			t.Fatalf("the publication returned %v without waiting for room", (<-result).err)
		}
		srv.ResumeReading()
		resumed := time.Now()
		if err := (<-result).err; err != nil {
			t.Fatalf("a publication that met a full queue whose writer then moved returned %v, want nil", err)
		}
		if took := time.Since(resumed); took > promptly {
			t.Errorf("the publication went out %v after the writer moved: the writer's progress has to wake it", took)
		}
		if err := srv.WaitFrames(frame.SEND, filled+1, requeueBudget); err != nil {
			t.Fatalf("the publication never arrived: %v", err)
		}
	})
	for _, tc := range settlements {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			srv, b, s := requeueFixture(t, func(c *Config) { c.ReceiptTimeout = moving })
			ids, err := srv.Deliver(s.Destination(), stompsrv.Message{Body: []byte("one")})
			if err != nil {
				t.Fatalf("Deliver: %v", err)
			}
			d := fetchAll(t, s, 1)
			if err := srv.StopReading(); err != nil {
				t.Fatal(err)
			}
			conn := sourceConn(s)
			fillQueue(t, conn)
			ctx, cancel := context.WithTimeout(context.Background(), requeueBudget)
			defer cancel()
			result := make(chan timed, 1)
			go func() { result <- timed{err: tc.settle(s, ctx, &d[0])} }()
			if !awaitWaiter(t, b, conn, result) {
				t.Fatalf("%s returned %v without waiting for room", tc.name, (<-result).err)
			}
			srv.ResumeReading()
			resumed := time.Now()
			if err := (<-result).err; err != nil {
				t.Fatalf("%s that met a full queue whose writer then moved returned %v, want nil", tc.name, err)
			}
			if took := time.Since(resumed); took > promptly {
				t.Errorf("%s went out %v after the writer moved: the writer's progress has to wake it", tc.name, took)
			}
			if err := srv.WaitFrames(tc.command, 1, requeueBudget); err != nil {
				t.Fatalf("the settlement never arrived: %v", err)
			}
			if got := srv.FramesOf(tc.command)[0].Get(frame.Id); got != ids[0] {
				t.Errorf("the %s frame names %q, want the delivery's %q", tc.command, got, ids[0])
			}
		})
	}
}

// TestASlowLinkIsNotAStall is a broker that reads every byte, at the far end
// of a link slow enough that the socket takes nothing for seconds at a time.
//
// A socket Write returns only once the whole frame is in the kernel's buffer,
// and the stall detector timed whole Writes against a quarter of
// ReceiptTimeout, so it refused a publisher whose broker was reading all
// along: measured behind a link of about 100 KB/s, 344 of 373 one-megabyte
// ConfirmNone publications at the default ReceiptTimeout, where go-stomp had
// refused none. The socket is now handed a frame in pieces, and the stall is
// the whole of ReceiptTimeout with no piece taken, because the kernel reports
// a slow reader's progress only once a third of a full send buffer has
// drained ([stallAfter]): here about every 3 s.
func TestASlowLinkIsNotAStall(t *testing.T) {
	t.Parallel()
	const (
		receipt = 8 * time.Second // stallAfter
		rate    = 512 << 10       // a third of a 4 MiB send buffer drains in under 3 s
		size    = 256 << 10
	)
	srv, b := backendOn(t, stompsrv.Options{ReadRate: rate}, receipt)
	ctx, cancel := context.WithTimeout(context.Background(), requeueBudget)
	defer cancel()
	snk, err := b.OpenSink(ctx, mq.SinkConfig{Topic: "orders", Confirmation: mq.ConfirmNone})
	if err != nil {
		t.Fatalf("OpenSink: %v", err)
	}
	body := bytes.Repeat([]byte("x"), size)
	// The socket's buffers and the gate's share of the queue fill at once,
	// and then a publication waits for the kernel to take more of the slow
	// link's bytes: seconds, longer than the quarter of ReceiptTimeout that
	// refused it before. That wait is the test, so the loop ends after it.
	long := receipt / 4
	const most = 128
	waited := false
	for i := 0; i < most && !waited; i++ {
		pctx, pcancel := context.WithTimeout(context.Background(), 2*receipt)
		start := time.Now()
		err := snk.Publish(pctx, &mq.Message{Body: body}, nil)
		took := time.Since(start)
		pcancel()
		if err != nil {
			t.Fatalf("publication %d took %v and returned %v, with the broker reading %d bytes a second all along",
				i, took, err, rate)
		}
		waited = took >= long
	}
	if !waited {
		t.Fatalf("none of %d publications waited %v: the link was not slow enough for the test to say anything",
			most, long)
	}
	// The cleanup's DISCONNECT would queue behind minutes of frames.
	_ = srv.DropConnection()
}

// TestABurstOnAReadingConnectionIsNotRefused is the conformance suite's
// failure in miniature: publishers and a consumer that outrun go-stomp's
// writer on a broker that reads everything. Measured on the pinned images,
// the consumer's ACKs ran up to 70 frames ahead of the writer. Not one frame
// may be refused.
func TestABurstOnAReadingConnectionIsNotRefused(t *testing.T) {
	t.Parallel()
	t.Run("publications", func(t *testing.T) {
		t.Parallel()
		srv, b := deafBackend(t, requeueBudget)
		ctx, cancel := context.WithTimeout(context.Background(), requeueBudget)
		defer cancel()
		snk, err := b.OpenSink(ctx, mq.SinkConfig{Topic: "orders", Confirmation: mq.ConfirmNone})
		if err != nil {
			t.Fatalf("OpenSink: %v", err)
		}
		const publishers, each = 8, 100
		body := bytes.Repeat([]byte("x"), 16<<10)
		errs := make(chan error, publishers*each)
		var wg sync.WaitGroup
		for range publishers {
			wg.Go(func() {
				for range each {
					if err := snk.Publish(ctx, &mq.Message{Body: body}, nil); err != nil {
						errs <- err
					}
				}
			})
		}
		wg.Wait()
		close(errs)
		for err := range errs {
			t.Errorf("a publication on a connection the broker reads was refused: %v", err)
		}
		if err := srv.WaitFrames(frame.SEND, publishers*each, requeueBudget); err != nil {
			t.Fatalf("not every publication arrived: %v", err)
		}
	})
	t.Run("acknowledgements", func(t *testing.T) {
		t.Parallel()
		srv, b := deafBackend(t, requeueBudget)
		ctx, cancel := context.WithTimeout(context.Background(), requeueBudget)
		defer cancel()
		const n = 500
		opened, err := b.OpenSource(ctx, mq.SourceConfig{Topic: "orders", Subscription: "workers", Prefetch: n})
		if err != nil {
			t.Fatalf("OpenSource: %v", err)
		}
		s, ok := opened.(*Source)
		if !ok {
			t.Fatalf("OpenSource returned %T", opened)
		}
		t.Cleanup(func() { _ = s.Close() })
		if err := srv.WaitSubscribed(s.Destination(), 1, requeueBudget); err != nil {
			t.Fatalf("the driver never subscribed: %v", err)
		}
		msgs := make([]stompsrv.Message, n)
		for i := range msgs {
			msgs[i] = stompsrv.Message{Body: []byte("m")}
		}
		if _, err := srv.Deliver(s.Destination(), msgs...); err != nil {
			t.Fatalf("Deliver: %v", err)
		}
		dst := make([]mq.DriverDelivery, 64)
		var wg sync.WaitGroup
		errs := make(chan error, n)
		for got := 0; got < n; {
			k, err := s.Fetch(ctx, dst)
			if err != nil {
				t.Fatalf("Fetch after %d of %d deliveries: %v", got, n, err)
			}
			for i := range dst[:k] {
				d := dst[i]
				wg.Go(func() {
					if err := s.Settler().Ack(ctx, &d); err != nil {
						errs <- err
					}
				})
			}
			got += k
		}
		wg.Wait()
		close(errs)
		for err := range errs {
			t.Errorf("an ACK on a connection the broker reads was refused: %v", err)
		}
		if err := srv.WaitFrames(frame.ACK, n, requeueBudget); err != nil {
			t.Fatalf("not every ACK arrived: %v", err)
		}
	})
}
