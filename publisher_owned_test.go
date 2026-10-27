package iqmq_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
)

// releaseLedger counts the releases of every message handed to an owned
// publication, and fails on the second release of one.
//
// Both directions are silent in production and both are what the specification's "Release
// called exactly once" is about: a release that never happens is a buffer the
// caller's pool never gets back, and a second release is a use-after-free of
// a buffer somebody else now owns.
type releaseLedger struct {
	tb testing.TB

	mu    sync.Mutex
	count map[*mq.Message]int
}

func newReleaseLedger(tb testing.TB) *releaseLedger {
	return &releaseLedger{tb: tb, count: map[*mq.Message]int{}}
}

// own wraps m in an OwnedMessage whose Release this ledger counts.
func (l *releaseLedger) own(m *mq.Message) mq.OwnedMessage {
	return mq.OwnedMessage{Message: m, Release: l.release}
}

func (l *releaseLedger) release(m *mq.Message) {
	l.mu.Lock()
	l.count[m]++
	n := l.count[m]
	l.mu.Unlock()
	if n > 1 {
		l.tb.Errorf("a message was released %d times: a pooled buffer released twice is a use-after-free ", n)
	}
}

// releases is how many times m was released.
func (l *releaseLedger) releases(m *mq.Message) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.count[m]
}

// total is how many releases the ledger saw.
func (l *releaseLedger) total() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	n := 0
	for _, c := range l.count {
		n += c
	}
	return n
}

// TestPublishOwnedReleasesExactlyOnce is the rule on every path out of
// PublishOwned, for a driver that takes ownership and for one the core adapts.
//
// The error paths are the ones that leak: a release skipped when a
// publication is refused before the driver is reached is invisible to the
// caller, which is waiting for a buffer that never comes back.
func TestPublishOwnedReleasesExactlyOnce(t *testing.T) {
	t.Parallel()
	driverErr := errors.New("the broker refused it")
	shapes := []struct {
		name string
		open func(*advBroker, mq.SinkConfig) mq.Sink
	}{
		{"ownedsink", openOwned},
		{"adapted", openPlain},
	}
	for _, shape := range shapes {
		t.Run(shape.name, func(t *testing.T) {
			t.Parallel()
			cases := []struct {
				name    string
				wantErr error
				setup   func(t *testing.T, l *releaseLedger) (*mq.PublisherHandle, *mq.Message, []mq.PublishOption)
			}{
				{"accepted", nil, func(t *testing.T, _ *releaseLedger) (*mq.PublisherHandle, *mq.Message, []mq.PublishOption) {
					_, p := advClient(t, shape.open, mq.PublisherConfig{})
					return p, &mq.Message{Body: []byte("x")}, nil
				}},
				{"driver-refused", driverErr, func(t *testing.T, _ *releaseLedger) (*mq.PublisherHandle, *mq.Message, []mq.PublishOption) {
					b, p := advClient(t, shape.open, mq.PublisherConfig{})
					if shape.name == "ownedsink" {
						advSinkOf[*advOwnedSink](t, b).setRefuse(driverErr)
					} else {
						b.setFail(func(int, *mq.Message) error { return driverErr })
					}
					return p, &mq.Message{Body: []byte("x")}, nil
				}},
				{"unsupported-option", mq.ErrUnsupported, func(t *testing.T, _ *releaseLedger) (*mq.PublisherHandle, *mq.Message, []mq.PublishOption) {
					b := newAdvBroker(shape.open, mq.FeatureScheduledPublish)
					p := advPublisher(t, b, mq.PublisherConfig{})
					return p, &mq.Message{Body: []byte("x")}, []mq.PublishOption{mq.Delay(time.Minute)}
				}},
				{"bad-option", mq.ErrUnsupported, func(t *testing.T, _ *releaseLedger) (*mq.PublisherHandle, *mq.Message, []mq.PublishOption) {
					_, p := advClient(t, shape.open, mq.PublisherConfig{})
					return p, &mq.Message{Body: []byte("x")}, []mq.PublishOption{{}}
				}},
				{"too-large", mq.ErrMessageTooLarge, func(t *testing.T, _ *releaseLedger) (*mq.PublisherHandle, *mq.Message, []mq.PublishOption) {
					_, p := advClient(t, shape.open, mq.PublisherConfig{})
					return p, &mq.Message{Body: make([]byte, (1<<20)+1)}, nil
				}},
				{"closed", mq.ErrClosed, func(t *testing.T, _ *releaseLedger) (*mq.PublisherHandle, *mq.Message, []mq.PublishOption) {
					_, p := advClient(t, shape.open, mq.PublisherConfig{})
					if err := p.Close(); err != nil {
						t.Fatalf("Close: %v", err)
					}
					return p, &mq.Message{Body: []byte("x")}, nil
				}},
			}
			for _, c := range cases {
				t.Run(c.name, func(t *testing.T) {
					t.Parallel()
					l := newReleaseLedger(t)
					p, m, opts := c.setup(t, l)
					err := p.PublishOwned(context.Background(), l.own(m), opts...)
					switch {
					case c.name == "bad-option" && err == nil:
						t.Fatal("a zero PublishOption was accepted")
					case c.name == "bad-option":
						// Its code is what matters; the specification names no sentinel.
						if mq.CodeOf(err) != mq.CodeInvalidArgument {
							t.Fatalf("PublishOwned = %v (code %v), want CodeInvalidArgument", err, mq.CodeOf(err))
						}
					case c.wantErr == nil && err != nil:
						t.Fatalf("PublishOwned = %v, want nil", err)
					case c.wantErr != nil && !errors.Is(err, c.wantErr):
						t.Fatalf("PublishOwned = %v, want %v", err, c.wantErr)
					}
					if got := l.releases(m); got != 1 {
						t.Fatalf("the message was released %d times, want exactly 1 ", got)
					}
				})
			}
		})
	}
}

// TestPublishOwnedNilMessageIsStillReleased pins the degenerate transfer:
// ownership passed at invocation even though there is no message to publish,
// so the release still owes the caller one call.
func TestPublishOwnedNilMessageIsStillReleased(t *testing.T) {
	t.Parallel()
	_, p := advClient(t, openOwned, mq.PublisherConfig{})
	var got int
	err := p.PublishOwned(context.Background(), mq.OwnedMessage{Release: func(*mq.Message) { got++ }})
	if mq.CodeOf(err) != mq.CodeInvalidArgument {
		t.Fatalf("PublishOwned(nil message) = %v, want CodeInvalidArgument", err)
	}
	if got != 1 {
		t.Fatalf("Release ran %d times, want exactly 1", got)
	}
}

// TestPublishOwnedReleaseRunsOutsideEveryLock is the specification's "never under
// internal locks".
//
// The test is re-entrancy, because that is what the rule exists for: a
// caller's Release returns a buffer to a pool and may publish the next
// message straight away. A release that ran under a publisher lock would
// deadlock here rather than merely being slow, and a release that ran inside
// the publication gate would park the whole shutdown behind user code.
func TestPublishOwnedReleaseRunsOutsideEveryLock(t *testing.T) {
	t.Parallel()
	for _, shape := range []struct {
		name string
		open func(*advBroker, mq.SinkConfig) mq.Sink
	}{{"ownedsink", openOwned}, {"adapted", openPlain}} {
		t.Run(shape.name, func(t *testing.T) {
			t.Parallel()
			// The client is deliberately not closed by a cleanup: a release
			// that really did run under a lock leaves a publication parked
			// inside the publication gate, and a Close would then wait for
			// the gate to drain rather than letting this test report.
			b := newAdvBroker(shape.open)
			cl := mq.NewClient(b)
			p, err := cl.Publisher(context.Background(), mq.PublisherConfig{Topic: "orders"})
			if err != nil {
				t.Fatalf("opening the publisher: %v", err)
			}
			done := make(chan struct{})
			depth := 0
			var release func(*mq.Message)
			release = func(*mq.Message) {
				depth++
				if depth > 3 {
					return
				}
				// Publishing from inside the release is the whole point: it
				// takes the publisher's paths again, from the goroutine that
				// is holding whatever the release path was holding.
				_ = p.PublishOwned(context.Background(), mq.OwnedMessage{
					Message: &mq.Message{Body: []byte("nested")},
					Release: release,
				})
			}
			go func() {
				defer close(done)
				_ = p.PublishOwned(context.Background(), mq.OwnedMessage{
					Message: &mq.Message{Body: []byte("outer")},
					Release: release,
				})
			}()
			select {
			case <-done:
			case <-time.After(10 * time.Second):
				t.Fatal("a release that publishes again deadlocked: the release ran under an internal lock ")
			}
			if got := b.nativeCalls(); got != 4 {
				t.Fatalf("the driver saw %d publications, want 4", got)
			}
			if err := cl.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}
		})
	}
}

// TestPublishBatchOwnedReleasesEveryMessageExactlyOnce is for a batch:
// ownership transfers for every item at invocation, so a validation failure
// that publishes nothing still releases everything.
func TestPublishBatchOwnedReleasesEveryMessageExactlyOnce(t *testing.T) {
	t.Parallel()
	for _, shape := range []struct {
		name string
		open func(*advBroker, mq.SinkConfig) mq.Sink
	}{{"ownedsink", openOwned}, {"adapted", openPlain}} {
		t.Run(shape.name, func(t *testing.T) {
			t.Parallel()
			t.Run("accepted", func(t *testing.T) {
				t.Parallel()
				b, p := advClient(t, shape.open, mq.PublisherConfig{})
				l := newReleaseLedger(t)
				oms := ownedBatch(l, "a", "b", "c")
				if err := p.PublishBatchOwned(context.Background(), oms); err != nil {
					t.Fatalf("PublishBatchOwned: %v", err)
				}
				assertReleasedOnce(t, l, oms)
				if got := len(b.bodies()); got != 3 {
					t.Fatalf("the driver saw %d publications, want 3", got)
				}
			})
			t.Run("validation-failure", func(t *testing.T) {
				t.Parallel()
				b, p := advClient(t, shape.open, mq.PublisherConfig{})
				l := newReleaseLedger(t)
				oms := ownedBatch(l, "a", "b", "c")
				oms[1].Message.Body = make([]byte, (1<<20)+1)
				err := p.PublishBatchOwned(context.Background(), oms)
				if !errors.Is(err, mq.ErrMessageTooLarge) {
					t.Fatalf("PublishBatchOwned = %v, want ErrMessageTooLarge", err)
				}
				// The specification: nothing is published at all when validation fails —
				// and every message still belongs to the publisher, so every
				// one is released.
				if got := b.nativeCalls(); got != 0 {
					t.Fatalf("the driver saw %d publications after a validation failure, want 0", got)
				}
				assertReleasedOnce(t, l, oms)
			})
			t.Run("partial-failure", func(t *testing.T) {
				t.Parallel()
				b, p := advClient(t, shape.open, mq.PublisherConfig{})
				l := newReleaseLedger(t)
				oms := ownedBatch(l, "a", "b", "c")
				b.setFail(func(n int, _ *mq.Message) error {
					if n == 2 {
						return errors.New("refused")
					}
					return nil
				})
				err := p.PublishBatchOwned(context.Background(), oms)
				if !errors.Is(err, mq.ErrPartial) {
					t.Fatalf("PublishBatchOwned = %v, want a partial batch error", err)
				}
				var be *mq.BatchError
				if !errors.As(err, &be) || len(be.Errs) != 3 {
					t.Fatalf("PublishBatchOwned = %v, want a *BatchError with 3 results", err)
				}
				if be.Errs[0] != nil || be.Errs[1] == nil || be.Errs[2] != nil {
					t.Fatalf("the batch results are %v, want only the second to have failed", be.Errs)
				}
				assertReleasedOnce(t, l, oms)
			})
			t.Run("closed", func(t *testing.T) {
				t.Parallel()
				_, p := advClient(t, shape.open, mq.PublisherConfig{})
				l := newReleaseLedger(t)
				oms := ownedBatch(l, "a", "b")
				if err := p.Close(); err != nil {
					t.Fatalf("Close: %v", err)
				}
				if err := p.PublishBatchOwned(context.Background(), oms); !errors.Is(err, mq.ErrClosed) {
					t.Fatalf("PublishBatchOwned = %v, want ErrClosed", err)
				}
				assertReleasedOnce(t, l, oms)
			})
		})
	}
}

// ownedBatch builds a batch of owned messages with the given bodies.
func ownedBatch(l *releaseLedger, bodies ...string) []mq.OwnedMessage {
	out := make([]mq.OwnedMessage, 0, len(bodies))
	for _, body := range bodies {
		out = append(out, l.own(&mq.Message{Body: []byte(body)}))
	}
	return out
}

// assertReleasedOnce checks the count for every message of a batch.
func assertReleasedOnce(tb testing.TB, l *releaseLedger, oms []mq.OwnedMessage) {
	tb.Helper()
	for i, om := range oms {
		if got := l.releases(om.Message); got != 1 {
			tb.Fatalf("message %d was released %d times, want exactly 1 ", i, got)
		}
	}
	if got := l.total(); got != len(oms) {
		tb.Fatalf("the ledger saw %d releases in total, want %d", got, len(oms))
	}
}

// TestPublishBatchOwnedCountsWhatAFatalErrorStopped is PublisherStats.Failed
// after an owned batch a closed sink stopped at its first message. The batch
// answers for all three messages, the first with the sink's error and the
// rest as not attempted, and PublishBatch counts all three as failed; the
// owned batch counted only the one it sent, so the two paths disagreed about
// the same outcome.
func TestPublishBatchOwnedCountsWhatAFatalErrorStopped(t *testing.T) {
	t.Parallel()
	for _, shape := range []struct {
		name string
		open func(*advBroker, mq.SinkConfig) mq.Sink
	}{{"ownedsink", openOwned}, {"adapted", openPlain}} {
		t.Run(shape.name, func(t *testing.T) {
			t.Parallel()
			b, p := advClient(t, shape.open, mq.PublisherConfig{})
			b.setFail(func(n int, _ *mq.Message) error {
				if n == 1 {
					return mq.ErrClosed
				}
				return nil
			})
			l := newReleaseLedger(t)
			oms := ownedBatch(l, "a", "b", "c")
			if err := p.PublishBatchOwned(context.Background(), oms); !errors.Is(err, mq.ErrClosed) {
				t.Fatalf("PublishBatchOwned = %v, want ErrClosed", err)
			}
			if got := b.nativeCalls(); got != 1 {
				t.Fatalf("the driver saw %d publications, want 1: a closed sink stops the batch", got)
			}
			if st := p.Stats(); st.Failed != 3 || st.Published != 0 || st.Pending != 0 {
				t.Fatalf("Stats = %+v, want 3 failed, none published and none pending", st)
			}
			assertReleasedOnce(t, l, oms)
		})
	}
}
