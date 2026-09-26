package iqmq_test

import (
	"context"
	"errors"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/internal/fuzzutil"
)

// shutdownWorld is what FuzzShutdown drives: one client, the handles it
// opened and the broker behind it.
type shutdownWorld struct {
	b    *fakeBroker
	cl   *mq.Client
	pub  *mq.PublisherHandle
	cons *mq.ConsumerHandle
	// held are deliveries a Receive handed over and nothing has settled. They
	// are what makes "a settlement that arrives after the close" reachable.
	held []mq.Delivery
	// closed records that the client has been closed or shut down, which is
	// what every later operation has to answer for.
	closed atomic.Bool
	seq    int
}

// shutdownScript is the op script. Every step is an operation the specification gives a
// rule for, and the rule is checked where the step applies it.
var shutdownScript = &fuzzutil.Script[*shutdownWorld]{
	MaxSteps: 48,
	Steps: []fuzzutil.Step[*shutdownWorld]{
		{Name: "publish", Weight: 3, Run: func(t testing.TB, r *fuzzutil.Reader, w *shutdownWorld) {
			w.seq++
			m := &mq.Message{ID: strconv.Itoa(w.seq), Key: []byte("k" + strconv.Itoa(r.Int(0, 3))), Body: r.Bytes(8)}
			err := w.pub.Publish(context.Background(), m)
			w.checkClosed(t, "Publish", err)
		}},
		{Name: "publish-via-client", Run: func(t testing.TB, r *fuzzutil.Reader, w *shutdownWorld) {
			w.seq++
			m := &mq.Message{ID: strconv.Itoa(w.seq), Body: r.Bytes(4)}
			err := w.cl.Publish(context.Background(), "orders", m)
			w.checkClosed(t, "Client.Publish", err)
		}},
		{Name: "receive", Weight: 3, Run: func(t testing.TB, r *fuzzutil.Reader, w *shutdownWorld) {
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
			defer cancel()
			dst := make([]mq.Delivery, r.Int(1, 4))
			n, err := w.cons.Receive(ctx, dst)
			if n < 0 || n > len(dst) {
				t.Fatalf("Receive returned %d for a slice of %d", n, len(dst))
			}
			w.held = append(w.held, dst[:n]...)
			if w.closed.Load() && n > 0 {
				t.Fatalf("Receive handed over %d deliveries after the client closed", n)
			}
			_ = err
		}},
		{Name: "ack", Weight: 2, Run: func(t testing.TB, r *fuzzutil.Reader, w *shutdownWorld) {
			d := w.take(r)
			if d == nil {
				return
			}
			w.checkSettlement(t, "Ack", d.Ack())
		}},
		{Name: "nack", Weight: 2, Run: func(t testing.TB, r *fuzzutil.Reader, w *shutdownWorld) {
			d := w.take(r)
			if d == nil {
				return
			}
			w.checkSettlement(t, "Nack", d.Nack())
		}},
		{Name: "extend", Run: func(t testing.TB, r *fuzzutil.Reader, w *shutdownWorld) {
			d := w.take(r)
			if d == nil {
				return
			}
			// An extension is not a settlement, so the delivery stays
			// settleable and goes back in the held set.
			_ = d.Extend(time.Duration(r.Int(1, 1000)) * time.Millisecond)
			w.held = append(w.held, *d)
		}},
		{Name: "run", Run: func(t testing.TB, r *fuzzutil.Reader, w *shutdownWorld) {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
			defer cancel()
			fail := r.Bool()
			err := w.cons.Run(ctx, mq.HandlerFunc(func(context.Context, *mq.Delivery) error {
				if fail {
					return errors.New("scripted failure")
				}
				return nil
			}), mq.WithRetryBackoff(mq.ConstantBackoff(0)), mq.WithDrainTimeout(time.Second))
			switch {
			case err == nil:
				t.Fatal("Run returned nil: it ends with the halt, the context or ErrClosed")
			case w.closed.Load() && !errors.Is(err, mq.ErrClosed) && !errors.Is(err, context.DeadlineExceeded):
				t.Fatalf("Run on a closed consumer = %v, want ErrClosed", err)
			}
		}},
		{Name: "stats", Run: func(t testing.TB, _ *fuzzutil.Reader, w *shutdownWorld) {
			s := w.cons.Stats()
			if s.InFlight < 0 || s.Acked < 0 || s.Nacked < 0 || s.LeaseExtensions < 0 {
				t.Fatalf("a counter went negative: %+v", s)
			}
			if w.closed.Load() && s.InFlight != 0 {
				t.Fatalf("InFlight = %d on a closed consumer, want 0 (terminal release)", s.InFlight)
			}
		}},
		{Name: "health", Run: func(t testing.TB, _ *fuzzutil.Reader, w *shutdownWorld) {
			h := w.cl.Health()
			if !h.State.Valid() {
				t.Fatalf("Health = %+v, which names no state", h)
			}
			if w.closed.Load() && h.State != mq.HealthClosed {
				t.Fatalf("Health = %v on a closed client, want HealthClosed", h.State)
			}
		}},
		{Name: "ping", Run: func(t testing.TB, _ *fuzzutil.Reader, w *shutdownWorld) {
			err := w.cl.Ping(context.Background())
			w.checkClosed(t, "Ping", err)
		}},
		{Name: "flush", Run: func(t testing.TB, _ *fuzzutil.Reader, w *shutdownWorld) {
			_ = w.pub.Flush(context.Background())
		}},
		{Name: "shutdown-publisher", Run: func(t testing.TB, r *fuzzutil.Reader, w *shutdownWorld) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Duration(r.Int(0, 20))*time.Millisecond)
			defer cancel()
			_ = w.pub.Shutdown(ctx)
		}},
		{Name: "close-publisher", Run: func(t testing.TB, _ *fuzzutil.Reader, w *shutdownWorld) {
			if err := w.pub.Close(); err != nil {
				t.Fatalf("Publisher.Close = %v, want nil (it is idempotent)", err)
			}
		}},
		{Name: "shutdown-consumer", Run: func(t testing.TB, r *fuzzutil.Reader, w *shutdownWorld) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Duration(r.Int(0, 20))*time.Millisecond)
			defer cancel()
			_ = w.cons.Shutdown(ctx)
		}},
		{Name: "close-consumer", Run: func(t testing.TB, _ *fuzzutil.Reader, w *shutdownWorld) {
			if err := w.cons.Close(); err != nil {
				t.Fatalf("Consumer.Close = %v, want nil (it is idempotent)", err)
			}
		}},
		{Name: "shutdown-client", Run: func(t testing.TB, r *fuzzutil.Reader, w *shutdownWorld) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Duration(r.Int(0, 20))*time.Millisecond)
			defer cancel()
			w.closed.Store(true)
			_ = w.cl.Shutdown(ctx)
		}},
		{Name: "close-client", Run: func(t testing.TB, _ *fuzzutil.Reader, w *shutdownWorld) {
			w.closed.Store(true)
			if err := w.cl.Close(); err != nil {
				t.Fatalf("Client.Close = %v, want nil (it is idempotent)", err)
			}
		}},
		{Name: "yield", Run: func(_ testing.TB, r *fuzzutil.Reader, _ *shutdownWorld) {
			fuzzutil.MaybeYield(r)
		}},
	},
}

// take removes one held delivery and returns it, or nil when nothing is held.
func (w *shutdownWorld) take(r *fuzzutil.Reader) *mq.Delivery {
	if len(w.held) == 0 {
		return nil
	}
	i := r.Int(0, len(w.held)-1)
	d := w.held[i]
	w.held = append(w.held[:i], w.held[i+1:]...)
	return &d
}

// checkClosed holds an operation to the rule: once the client is closed,
// every operation reports it rather than doing the work.
func (w *shutdownWorld) checkClosed(t testing.TB, op string, err error) {
	t.Helper()
	if w.closed.Load() && err == nil {
		t.Fatalf("%s succeeded after the client closed", op)
	}
}

// checkSettlement holds a settlement to the answers: it succeeds, or it
// names one of the reasons it could not.
func (w *shutdownWorld) checkSettlement(t testing.TB, op string, err error) {
	t.Helper()
	if err == nil {
		return
	}
	switch {
	case errors.Is(err, mq.ErrAlreadySettled),
		errors.Is(err, mq.ErrClosed),
		errors.Is(err, mq.ErrLeaseLost),
		errors.Is(err, mq.ErrNotSettleable),
		errors.Is(err, mq.ErrSettlementUnknown),
		errors.Is(err, mq.ErrUnsupported):
	default:
		t.Fatalf("%s = %v, which is not one of the answers", op, err)
	}
}

// FuzzShutdown is the target for the lifecycle: arbitrary
// interleavings of publishing, receiving, settling, running and closing,
// against the rules the specification states for each.
//
// The shape it exists to find is the one this project keeps finding: a
// shutdown that closes something before the operations already inside it have
// left, and the accounting that is then wrong for ever. Every step checks its
// own rule, and the invariant after the whole script is the specification's — every
// published record in exactly one place, and nothing left in flight on a
// closed consumer.
func FuzzShutdown(f *testing.F) {
	w := fuzzutil.NewWriter()
	for _, step := range []string{
		"publish", "publish", "receive", "ack", "shutdown-consumer", "receive",
		"stats", "shutdown-client", "publish", "health", "close-client", "ping",
	} {
		shutdownScript.WriteStep(w, step)
		switch step {
		case "publish":
			w.Int(0, 3, 1)
			w.Bytes(8, []byte("body"))
		case "publish-via-client":
			w.Bytes(4, []byte("b"))
		case "receive":
			w.Int(1, 4, 2)
		case "ack", "nack":
			w.Int(0, 0, 0)
		case "shutdown-consumer", "shutdown-client":
			w.Int(0, 20, 5)
		}
	}
	f.Add(w.Data())

	second := fuzzutil.NewWriter()
	for _, step := range []string{"publish", "receive", "extend", "nack", "close-consumer", "close-client"} {
		shutdownScript.WriteStep(second, step)
		switch step {
		case "publish":
			second.Int(0, 3, 0)
			second.Bytes(8, []byte("x"))
		case "receive":
			second.Int(1, 4, 1)
		case "extend", "nack":
			second.Int(0, 0, 0)
			if step == "extend" {
				second.Int(1, 1000, 250)
			}
		}
	}
	f.Add(second.Data())

	third := fuzzutil.NewWriter()
	for _, step := range []string{"publish", "publish", "run", "stats", "shutdown-client", "run"} {
		shutdownScript.WriteStep(third, step)
		switch step {
		case "publish":
			third.Int(0, 3, 2)
			third.Bytes(8, []byte("z"))
		case "run":
			third.Bool(false)
		case "shutdown-client":
			third.Int(0, 20, 1)
		}
	}
	f.Add(third.Data())

	f.Fuzz(func(t *testing.T, data []byte) {
		b := newFakeBroker(fakeConfig{extend: true, deadLetter: true}, "orders", "dead")
		cl := mq.NewClient(b)
		pub, err := cl.Publisher(context.Background(), mq.PublisherConfig{Topic: "orders"})
		if err != nil {
			t.Fatalf("opening the publisher: %v", err)
		}
		cons, err := cl.Consumer(context.Background(), mq.ConsumerConfig{Topic: "orders", Subscription: "billing"})
		if err != nil {
			t.Fatalf("opening the consumer: %v", err)
		}
		w := &shutdownWorld{b: b, cl: cl, pub: pub, cons: cons}
		shutdownScript.Run(t, data, w)
		w.closed.Store(true)
		if err := cl.Close(); err != nil {
			t.Fatalf("closing the client: %v", err)
		}
		if got := cons.Stats().InFlight; got != 0 {
			t.Fatalf("InFlight = %d after the client closed, want 0 (terminal release)", got)
		}
		if counts := b.counts("orders", "billing"); !counts.conserved() {
			t.Fatalf("The specification conservation broken: %+v", counts)
		}
	})
}
