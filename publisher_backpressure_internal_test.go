package iqmq

import (
	"context"
	"errors"
	"sync"
	"testing"
)

// acceptingBackend is the smallest backend a publisher opens over: a
// synchronous sink that accepts everything, so nothing is ever outstanding at
// the driver. It is here because an internal test cannot import memory.
type acceptingBackend struct{ caps Capabilities }

func newAcceptingBackend() *acceptingBackend {
	var caps Capabilities
	for f := Feature(0); int(f) < FeatureCount; f++ {
		_ = caps.Set(f, Support{Level: Native, Scope: ScopeTopic})
	}
	return &acceptingBackend{caps: caps}
}

func (b *acceptingBackend) Info() DriverInfo           { return DriverInfo{Name: "accepting"} }
func (b *acceptingBackend) Capabilities() Capabilities { return b.caps }

func (b *acceptingBackend) OpenSink(context.Context, SinkConfig) (Sink, error) {
	return acceptingSink{}, nil
}

func (b *acceptingBackend) OpenSource(context.Context, SourceConfig) (Source, error) {
	return nil, ErrUnsupported
}

func (b *acceptingBackend) Ping(context.Context) error { return nil }
func (b *acceptingBackend) Native() any                { return nil }
func (b *acceptingBackend) Close() error               { return nil }

type acceptingSink struct{}

func (acceptingSink) Publish(context.Context, *Message, *PublishOptions) error { return nil }
func (acceptingSink) Close() error                                             { return nil }

// TestADropCaughtHalfDoneByACloseReportsTheDrop replays, on one goroutine,
// the interleaving in which a Close runs between the two halves of a
// BackpressureDropOldest drop. The drop takes its victim out of the queue and
// marks it under the bound's lock, and completes it once the lock is
// released; admission runs outside the publication gate, so the Close does
// not wait for it. The Close skipped the marked victim when it settled the
// batch, and its completion table then abandoned the victim's slot: the
// callback of a publication the driver was never handed reported an unknown
// outcome (CodeSettlementUnknown, "abandoned by close"), while Stats counted
// it as a drop.
//
// The two halves are the DropOldest arm of pendingBound.admit, done by hand,
// because nothing blocks between them for a test to park on.
// TestBackpressureDropOldestCaughtByACloseStillReportsTheDrop is the same
// race through the real admission path.
func TestADropCaughtHalfDoneByACloseReportsTheDrop(t *testing.T) {
	for _, mode := range []CallbackMode{CallbackInline, CallbackDeferred} {
		t.Run(mode.String(), func(t *testing.T) {
			cl := NewClient(newAcceptingBackend())
			t.Cleanup(func() { _ = cl.Close() })
			p, err := cl.Publisher(context.Background(), PublisherConfig{
				Topic:              "orders",
				Backpressure:       BackpressureDropOldest,
				MaxPendingMessages: 2,
				// A batch that never fills keeps both publications queued,
				// where the drop can reach them.
				Batching:  BatchingConfig{MaxMessages: 1 << 20},
				Callbacks: mode,
			})
			if err != nil {
				t.Fatal(err)
			}
			var (
				mu       sync.Mutex
				outcomes = map[string]error{}
			)
			for _, body := range []string{"oldest", "newer"} {
				if err := p.PublishAsync(context.Background(), &Message{Body: []byte(body)}, func(r PublishResult) {
					mu.Lock()
					outcomes[body] = r.Err
					mu.Unlock()
				}); err != nil {
					t.Fatalf("PublishAsync(%s): %v", body, err)
				}
			}
			// The drop's first half.
			p.bound.mu.Lock()
			victim := p.bound.unlinkOldestLocked()
			p.bound.mu.Unlock()
			if victim == nil {
				t.Fatal("nothing was queued to drop")
			}
			if err := p.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}
			// The drop's second half.
			victim.drop()
			mu.Lock()
			defer mu.Unlock()
			if got := outcomes["oldest"]; !errors.Is(got, ErrBackpressure) || IsAmbiguous(got) {
				t.Errorf("the dropped publication's callback = %v (code %v), want ErrBackpressure", got, CodeOf(got))
			}
			if got := outcomes["newer"]; !errors.Is(got, ErrClosed) {
				t.Errorf("the queued publication's callback = %v, want ErrClosed", got)
			}
			if got := p.Stats().Drops; got != 1 {
				t.Errorf("Stats().Drops = %d, want 1", got)
			}
		})
	}
}
