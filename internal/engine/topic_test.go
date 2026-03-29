package engine

import (
	"context"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"bitbucket.org/iqhive/iqmq/internal/clock"
)

// hookClock is the real clock with a trap door: while it is armed, the next
// read from the shared-log publish path is parked until the test releases it.
// That path reads the clock between reserving its slot and appending it,
// which is the window a Subscribe must not be able to land inside.
//
// The trap names the caller it is for. The engine's clock is read by more
// than the publisher — the scheduler goroutine that New starts reads it as
// its first act, with no ordering against the test — so a trap that parked
// whichever goroutine got there first parked the scheduler instead of the
// publication on a loaded host, left the publication unparked, and turned a
// correct outcome into a failure.
type hookClock struct {
	clock.Real
	armed   atomic.Bool
	entered chan struct{}
	release chan struct{}
}

func (h *hookClock) Now() time.Time {
	if h.armed.Load() && publishing() && h.armed.CompareAndSwap(true, false) {
		close(h.entered)
		<-h.release
	}
	return time.Now()
}

// publishing reports whether the calling goroutine is inside the shared-log
// publish path.
func publishing() bool {
	var pcs [32]uintptr
	frames := runtime.CallersFrames(pcs[:runtime.Callers(3, pcs[:])])
	for {
		f, more := frames.Next()
		if strings.Contains(f.Function, ").publishShared") {
			return true
		}
		if !more {
			return false
		}
	}
}

// TestSubscribeDuringSharedPublish: under the shared partition log a
// publication reserves a slot, accounts for the record in every subscription
// it is for, and only then appends the slot. A Subscribe that lands in that
// window must not be given a cursor pointing at the slot: the record was
// never accounted for it, and nothing else — its reference count, the lane
// gauges of the subscriptions that were admitted, the engine totals — was
// either.
//
// Taking the cursor from the log's head alone did exactly that. The
// subscription was delivered a record it was never admitted (Ready without
// Published, so Conserved was false); acknowledging it dropped the last
// reference, trimmed the slot and advanced the legitimate subscriber's cursor
// past a record it had been admitted; and the settlement it should never have
// made unreserved a lane that never accounted, driving the byte gauges
// negative.
func TestSubscribeDuringSharedPublish(t *testing.T) {
	hook := &hookClock{entered: make(chan struct{}), release: make(chan struct{})}
	e, err := New(Config[int64]{
		Kinds:           KindSet{Arch: LogCursor},
		Clock:           hook,
		Clone:           func(v int64) int64 { return v },
		DefaultTopology: TopologyConfig{Partitions: 1, AckDeadline: -1},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer func() { _ = e.Close() }()
	top, err := e.CreateTopic("t", TopologyConfig{})
	if err != nil {
		t.Fatal(err)
	}
	s0, err := top.Subscribe("s0", SubscriptionConfig{})
	if err != nil {
		t.Fatal(err)
	}
	c0, err := s0.Attach(ConsumerConfig{})
	if err != nil {
		t.Fatal(err)
	}

	// Park a publication between its reservation and its append.
	hook.armed.Store(true)
	done := make(chan error, 1)
	go func() {
		done <- top.Publish(context.Background(), Publish[int64]{Payload: 1, Size: 8})
	}()
	<-hook.entered

	s1, err := top.Subscribe("s1", SubscriptionConfig{})
	if err != nil {
		t.Fatalf("Subscribe during a publication: %v", err)
	}
	close(hook.release)
	if err := <-done; err != nil {
		t.Fatalf("publish: %v", err)
	}

	if snap := s1.Inspect(); snap.Published != 0 || snap.Ready != 0 || snap.Bytes != 0 || !snap.Conserved() {
		t.Fatalf("the subscription that joined mid-publication holds a record "+
			"it was never admitted: %+v", snap)
	}
	if snap := s0.Inspect(); snap.Published != 1 || snap.Ready != 1 || !snap.Conserved() {
		t.Fatalf("the subscription the publication was for: %+v", snap)
	}
	c1, err := s1.Attach(ConsumerConfig{})
	if err != nil {
		t.Fatal(err)
	}
	dst := make([]Claim[int64], 2)
	if n, _ := c1.Fetch(poll(t), dst); n != 0 {
		t.Fatalf("the new subscription was delivered %d records it was never admitted", n)
	}
	// The record is still the older subscription's, and settling it leaves
	// the accounting where it started.
	n, err := c0.Fetch(poll(t), dst)
	if n != 1 || err != nil {
		t.Fatalf("the admitted subscription fetched %d records: %v", n, err)
	}
	if err := c0.Ack(dst[0].Env, dst[0].Gen); err != nil {
		t.Fatal(err)
	}
	if msgs, bytes, _ := e.Totals(); msgs != 0 || bytes != 0 {
		t.Fatalf("engine totals after the record left: %d messages, %d bytes", msgs, bytes)
	}
	// The next publication is for both of them.
	if err := top.Publish(context.Background(), Publish[int64]{Payload: 2, Size: 8}); err != nil {
		t.Fatal(err)
	}
	for _, s := range []*Subscription[int64]{s0, s1} {
		if snap := s.Inspect(); snap.Ready != 1 || !snap.Conserved() {
			t.Fatalf("subscription %s missed the publication that followed it: %+v", s.Name(), snap)
		}
	}
	for _, c := range []*Consumer[int64]{c0, c1} {
		n, err := c.Fetch(poll(t), dst)
		if n != 1 || err != nil || dst[0].Payload != 2 {
			t.Fatalf("fetch: n=%d payload=%d err=%v", n, dst[0].Payload, err)
		}
		if err := c.Ack(dst[0].Env, dst[0].Gen); err != nil {
			t.Fatal(err)
		}
	}
	if msgs, bytes, _ := e.Totals(); msgs != 0 || bytes != 0 {
		t.Fatalf("engine totals at the end: %d messages, %d bytes", msgs, bytes)
	}
}

// TestSubscribeRacesPublish is the same property without the trap door: many
// subscriptions created while publishers run. Every subscription must end
// holding exactly what it was admitted, whatever the architecture — the
// shared log is the one that shares state between them, but a per-subscription
// architecture must not drift either.
func TestSubscribeRacesPublish(t *testing.T) {
	for _, a := range Arches() {
		t.Run(a.String(), func(t *testing.T) {
			const (
				subs       = 64
				publishers = 4
			)
			e, err := New(Config[int64]{
				Kinds: KindSet{Arch: a},
				Clone: func(v int64) int64 { return v },
				DefaultTopology: TopologyConfig{
					Partitions: 1, CapacityMessages: 4096, AckDeadline: -1,
				},
			})
			if err != nil {
				t.Fatalf("New: %v", err)
			}
			defer func() { _ = e.Close() }()
			top, err := e.CreateTopic("t", TopologyConfig{})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := top.Subscribe("s", SubscriptionConfig{}); err != nil {
				t.Fatal(err)
			}
			var stop atomic.Bool
			var wg sync.WaitGroup
			for range publishers {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for i := 0; !stop.Load(); i++ {
						// A full log is a fine outcome here; what matters is
						// that nothing is admitted to a subscription that was
						// not part of the publication.
						_ = top.Publish(context.Background(), Publish[int64]{
							Payload: int64(i), Size: 1,
						})
					}
				}()
			}
			made := make([]*Subscription[int64], 0, subs)
			for i := range subs {
				s, err := top.Subscribe("s"+string(rune('a'+i%26))+string(rune('a'+i/26)),
					SubscriptionConfig{})
				if err != nil {
					t.Fatalf("subscribe %d: %v", i, err)
				}
				made = append(made, s)
			}
			stop.Store(true)
			wg.Wait()
			for _, s := range made {
				snap := s.Inspect()
				if !snap.Conserved() {
					t.Fatalf("subscription %s is not conserved: %+v", s.Name(), snap)
				}
				if int64(snap.Ready) != snap.Published {
					t.Fatalf("subscription %s holds %d records and was admitted %d: %+v",
						s.Name(), snap.Ready, snap.Published, snap)
				}
			}
		})
	}
}
