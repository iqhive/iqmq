package engine

import (
	"context"
	"errors"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"bitbucket.org/iqhive/iqmq/internal/errcode"
	"bitbucket.org/iqhive/iqmq/internal/lifecycle"
)

// releaseLedger records one release per payload.
type releaseLedger struct {
	mu  sync.Mutex
	rel map[int64]int
}

func newLedger() *releaseLedger { return &releaseLedger{rel: map[int64]int{}} }

func (l *releaseLedger) release(v int64) {
	l.mu.Lock()
	l.rel[v]++
	l.mu.Unlock()
}

// check reports the total number of releases and the first payload released
// more than once.
func (l *releaseLedger) check() (int, int64, int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	total := 0
	for v, n := range l.rel {
		total += n
		if n != 1 {
			return total, v, n
		}
	}
	return total, 0, 0
}

func TestEngineCloseIsIdempotent(t *testing.T) {
	eng, err := New(Config[int64]{Clone: func(v int64) int64 { return v }})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if eng.Closed() {
		t.Fatal("a fresh engine reports itself closed")
	}
	if got := eng.Lifecycle().Phase(); got != lifecycle.Running {
		t.Fatalf("a fresh engine is in phase %s", got)
	}
	const callers = 16
	var wg sync.WaitGroup
	errs := make([]error, callers)
	for i := range callers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if i%2 == 0 {
				errs[i] = eng.Close()
				return
			}
			errs[i] = eng.Shutdown(context.Background())
		}()
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("caller %d: %v", i, err)
		}
	}
	if !eng.Closed() {
		t.Fatal("the engine does not report itself closed")
	}
	if got := eng.Lifecycle().Phase(); got != lifecycle.Closed {
		t.Fatalf("phase %s after close", got)
	}
	if _, err := eng.CreateTopic("late", TopologyConfig{}); !errors.Is(err, errcode.ErrClosed) {
		t.Fatalf("CreateTopic after close: %v", err)
	}
}

// TestEngineShutdownWaitsForHandlers is the graceful half of
// the message contract: Shutdown does not drop what a consumer still holds
// until the consumer settles it or the drain deadline passes.
func TestEngineShutdownWaitsForHandlers(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		led := newLedger()
		eng, err := New(Config[int64]{
			Clone:        func(v int64) int64 { return v },
			DrainTimeout: time.Minute,
			DefaultTopology: TopologyConfig{
				Partitions: 1, CapacityMessages: 16, AckDeadline: time.Hour,
			},
		})
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		top, err := eng.CreateTopic("t", TopologyConfig{})
		if err != nil {
			t.Fatalf("CreateTopic: %v", err)
		}
		sub, err := top.Subscribe("s", SubscriptionConfig{})
		if err != nil {
			t.Fatalf("Subscribe: %v", err)
		}
		con, err := sub.Attach(ConsumerConfig{})
		if err != nil {
			t.Fatalf("Attach: %v", err)
		}
		if err := top.Publish(context.Background(), Publish[int64]{
			Payload: 1, Size: 1, Mode: CopyOwned, Release: led.release,
		}); err != nil {
			t.Fatalf("Publish: %v", err)
		}
		dst := make([]Claim[int64], 1)
		n, err := con.Fetch(context.Background(), dst)
		if err != nil || n != 1 {
			t.Fatalf("Fetch: %d, %v", n, err)
		}

		done := make(chan error, 1)
		go func() { done <- eng.Shutdown(context.Background()) }()
		synctest.Wait()
		select {
		case err := <-done:
			t.Fatalf("Shutdown returned %v with a delivery in flight", err)
		default:
		}
		if _, _, n := led.check(); n != 0 {
			t.Fatal("a payload was released while its handler was running")
		}
		if err := con.Ack(dst[0].Env, dst[0].Gen); err != nil {
			t.Fatalf("Ack under shutdown: %v", err)
		}
		if err := <-done; err != nil {
			t.Fatalf("Shutdown: %v", err)
		}
		total, v, count := led.check()
		if count != 0 {
			t.Fatalf("payload %d released %d times", v, count)
		}
		if total != 1 {
			t.Fatalf("%d payloads released, want 1", total)
		}
	})
}

// TestEngineShutdownDrainDeadline is the other half: a handler that never
// settles costs the shutdown the drain deadline and no more.
func TestEngineShutdownDrainDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		led := newLedger()
		eng, err := New(Config[int64]{
			Clone:        func(v int64) int64 { return v },
			DrainTimeout: 5 * time.Second,
			DefaultTopology: TopologyConfig{
				Partitions: 1, CapacityMessages: 16, AckDeadline: time.Hour,
			},
		})
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		top, _ := eng.CreateTopic("t", TopologyConfig{})
		sub, _ := top.Subscribe("s", SubscriptionConfig{})
		con, _ := sub.Attach(ConsumerConfig{})
		if err := top.Publish(context.Background(), Publish[int64]{
			Payload: 1, Size: 1, Mode: CopyOwned, Release: led.release,
		}); err != nil {
			t.Fatalf("Publish: %v", err)
		}
		dst := make([]Claim[int64], 1)
		if n, err := con.Fetch(context.Background(), dst); err != nil || n != 1 {
			t.Fatalf("Fetch: %d, %v", n, err)
		}
		start := eng.Clock().Now()
		err = eng.Shutdown(context.Background())
		if !errors.Is(err, lifecycle.ErrDrainTimeout) {
			t.Fatalf("Shutdown returned %v, want the drain timeout", err)
		}
		if waited := eng.Clock().Now().Sub(start); waited != 5*time.Second {
			t.Fatalf("the drain waited %v, want the 5s deadline", waited)
		}
		if got := eng.Lifecycle().Phase(); got != lifecycle.Closed {
			t.Fatalf("phase %s: the deadline stopped the shutdown", got)
		}
		total, v, count := led.check()
		if count != 0 {
			t.Fatalf("payload %d released %d times", v, count)
		}
		if total != 1 {
			t.Fatalf("%d payloads released after the deadline, want 1", total)
		}
		if msgs, bytes, _ := eng.Totals(); msgs != 0 || bytes != 0 {
			t.Fatalf("engine totals after shutdown: msgs=%d bytes=%d", msgs, bytes)
		}
	})
}

// TestEngineShutdownWaitsForDeadLetterPublication is the DLQ-before-ack
// ordering hook seen from the shutdown: the settling phase does not drop what
// the lanes hold while a dead-letter publication is still in flight.
func TestEngineShutdownWaitsForDeadLetterPublication(t *testing.T) {
	eng, err := New(Config[int64]{Clone: func(v int64) int64 { return v }})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	p := eng.Lifecycle().BeginDeadLetter()
	if p == nil {
		t.Fatal("BeginDeadLetter refused on a running engine")
	}
	done := make(chan error, 1)
	go func() { done <- eng.Shutdown(context.Background()) }()
	select {
	case err := <-done:
		t.Fatalf("Shutdown returned %v with a dead-letter publication in flight", err)
	case <-time.After(20 * time.Millisecond):
	}
	p.Commit()
	if err := <-done; err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	if eng.Lifecycle().BeginDeadLetter() != nil {
		t.Fatal("a dead-letter publication was opened on a closed engine")
	}
}

// closeRaceRounds is how many interleavings each of the engine-close races
// below drives. The class they cover — a record, a subscription or a topic
// that becomes visible after the shutdown has walked past it — appears at a
// handful of failures per thousand rounds, so a hundred proves nothing
// (hack/PLAYBOOK.md).
const closeRaceRounds = 3000

// TestPublishRacesEngineCloseUnderTheGate races publishers against
// Engine.Close. Every payload the engine accepted must be released exactly
// once and the engine's reservations must return to zero: a record placed
// into a lane the settling phase has already drained is a payload nothing
// releases and a reservation nothing returns.
func TestPublishRacesEngineCloseUnderTheGate(t *testing.T) {
	const publishers = 4
	for round := range closeRaceRounds {
		led := newLedger()
		var accepted atomic.Int64
		eng, err := New(Config[int64]{
			Clone: func(v int64) int64 { return v },
			DefaultTopology: TopologyConfig{
				Partitions: 2, CapacityMessages: 64, AckDeadline: time.Second,
			},
		})
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		top, err := eng.CreateTopic("t", TopologyConfig{})
		if err != nil {
			t.Fatalf("CreateTopic: %v", err)
		}
		for _, name := range []string{"a", "b"} {
			if _, err := top.Subscribe(name, SubscriptionConfig{}); err != nil {
				t.Fatalf("Subscribe: %v", err)
			}
		}
		var wg sync.WaitGroup
		wg.Add(publishers + 1)
		for g := range publishers {
			go func() {
				defer wg.Done()
				for i := range 8 {
					v := int64(g*8 + i + 1)
					if err := top.Publish(context.Background(), Publish[int64]{
						Payload: v, Size: 1, Mode: CopyOwned, Release: led.release,
					}); err == nil {
						accepted.Add(1)
					}
				}
			}()
		}
		go func() {
			defer wg.Done()
			for range round % 11 {
				runtime.Gosched()
			}
			if err := eng.Close(); err != nil {
				t.Errorf("close engine: %v", err)
			}
		}()
		wg.Wait()
		if t.Failed() {
			return
		}
		total, v, count := led.check()
		if count != 0 {
			t.Fatalf("round %d: payload %d released %d times", round, v, count)
		}
		if got := int(accepted.Load()); total != got {
			t.Fatalf("round %d: %d payloads accepted, %d released (leaked %d)",
				round, got, total, got-total)
		}
		if msgs, bytes, _ := eng.Totals(); msgs != 0 || bytes != 0 {
			t.Fatalf("round %d: engine totals after close: msgs=%d bytes=%d", round, msgs, bytes)
		}
	}
}

// TestSubscribeRacesEngineClose creates subscriptions while the engine
// closes. A subscription that becomes visible after the settling phase has
// walked the topic would never be closed or drained, and the records a
// concurrent publication put in its lanes would never be released.
func TestSubscribeRacesEngineClose(t *testing.T) {
	for round := range closeRaceRounds {
		led := newLedger()
		var accepted atomic.Int64
		eng, err := New(Config[int64]{
			Clone: func(v int64) int64 { return v },
			DefaultTopology: TopologyConfig{
				Partitions: 1, CapacityMessages: 64, AckDeadline: time.Second,
			},
		})
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		top, err := eng.CreateTopic("t", TopologyConfig{})
		if err != nil {
			t.Fatalf("CreateTopic: %v", err)
		}
		var wg sync.WaitGroup
		wg.Add(3)
		go func() {
			defer wg.Done()
			for i := range 8 {
				name := string(rune('a' + i))
				if _, err := top.Subscribe(name, SubscriptionConfig{}); err != nil &&
					!errors.Is(err, errcode.ErrClosed) {
					t.Errorf("Subscribe: %v", err)
					return
				}
			}
		}()
		go func() {
			defer wg.Done()
			for i := range 16 {
				v := int64(i + 1)
				if err := top.Publish(context.Background(), Publish[int64]{
					Payload: v, Size: 1, Mode: CopyOwned, Release: led.release,
				}); err == nil {
					accepted.Add(1)
				}
			}
		}()
		go func() {
			defer wg.Done()
			for range round % 7 {
				runtime.Gosched()
			}
			if err := eng.Close(); err != nil {
				t.Errorf("close engine: %v", err)
			}
		}()
		wg.Wait()
		if t.Failed() {
			return
		}
		total, v, count := led.check()
		if count != 0 {
			t.Fatalf("round %d: payload %d released %d times", round, v, count)
		}
		if got := int(accepted.Load()); total != got {
			t.Fatalf("round %d: %d payloads accepted, %d released (leaked %d)",
				round, got, total, got-total)
		}
		if msgs, bytes, _ := eng.Totals(); msgs != 0 || bytes != 0 {
			t.Fatalf("round %d: engine totals after close: msgs=%d bytes=%d", round, msgs, bytes)
		}
	}
}

// TestCreateTopicRacesEngineClose creates topics while the engine closes.
// A topic registered after the settling phase has walked the registry is one
// the shutdown never visits; the gate refuses it instead.
func TestCreateTopicRacesEngineClose(t *testing.T) {
	for round := range closeRaceRounds {
		eng, err := New(Config[int64]{
			Clone: func(v int64) int64 { return v },
			DefaultTopology: TopologyConfig{
				Partitions: 1, CapacityMessages: 8, AckDeadline: time.Second,
			},
		})
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		var wg sync.WaitGroup
		wg.Add(2)
		created := make([]string, 0, 8)
		var cmu sync.Mutex
		go func() {
			defer wg.Done()
			for i := range 8 {
				name := "t" + string(rune('a'+i))
				switch _, err := eng.CreateTopic(name, TopologyConfig{}); {
				case err == nil:
					cmu.Lock()
					created = append(created, name)
					cmu.Unlock()
				case errors.Is(err, errcode.ErrClosed):
				default:
					t.Errorf("CreateTopic: %v", err)
					return
				}
			}
		}()
		go func() {
			defer wg.Done()
			for range round % 5 {
				runtime.Gosched()
			}
			if err := eng.Close(); err != nil {
				t.Errorf("close engine: %v", err)
			}
		}()
		wg.Wait()
		if t.Failed() {
			return
		}
		// Every topic the engine said it had created is one Engine.Close
		// can still see: a topic that "exists" but no walk reaches is the
		// registry half of the defect class.
		cmu.Lock()
		names := append([]string(nil), created...)
		cmu.Unlock()
		for _, name := range names {
			if _, err := eng.Topic(name); err != nil {
				t.Fatalf("round %d: topic %s was created but the engine does not have it: %v",
					round, name, err)
			}
		}
		seen := 0
		eng.Topics(func(*Topic[int64]) bool { seen++; return true })
		if seen != len(names) {
			t.Fatalf("round %d: %d topics created, %d in the registry", round, len(names), seen)
		}
	}
}

// TestFetchRacesEngineClose races consumers against Engine.Close. A fetch
// that claimed a record and had not yet handed it over when the settling
// phase drained the lanes would hold the only reference to it.
func TestFetchRacesEngineClose(t *testing.T) {
	const consumers = 4
	for round := range closeRaceRounds {
		led := newLedger()
		var accepted atomic.Int64
		eng, err := New(Config[int64]{
			Clone: func(v int64) int64 { return v },
			DefaultTopology: TopologyConfig{
				Partitions: 2, CapacityMessages: 64, AckDeadline: time.Hour,
			},
		})
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		top, err := eng.CreateTopic("t", TopologyConfig{})
		if err != nil {
			t.Fatalf("CreateTopic: %v", err)
		}
		sub, err := top.Subscribe("s", SubscriptionConfig{})
		if err != nil {
			t.Fatalf("Subscribe: %v", err)
		}
		for i := range 16 {
			v := int64(i + 1)
			if err := top.Publish(context.Background(), Publish[int64]{
				Payload: v, Size: 1, Mode: CopyOwned, Release: led.release,
			}); err == nil {
				accepted.Add(1)
			}
		}
		var wg sync.WaitGroup
		wg.Add(consumers + 1)
		for range consumers {
			con, err := sub.Attach(ConsumerConfig{})
			if err != nil {
				t.Fatalf("Attach: %v", err)
			}
			go func() {
				defer wg.Done()
				dst := make([]Claim[int64], 4)
				for {
					n, err := con.Fetch(context.Background(), dst)
					if err != nil {
						return
					}
					for i := range n {
						if err := con.Ack(dst[i].Env, dst[i].Gen); err != nil {
							return
						}
					}
				}
			}()
		}
		go func() {
			defer wg.Done()
			for range round % 13 {
				runtime.Gosched()
			}
			if err := eng.Close(); err != nil {
				t.Errorf("close engine: %v", err)
			}
		}()
		wg.Wait()
		if t.Failed() {
			return
		}
		total, v, count := led.check()
		if count != 0 {
			t.Fatalf("round %d: payload %d released %d times", round, v, count)
		}
		if got := int(accepted.Load()); total != got {
			t.Fatalf("round %d: %d payloads accepted, %d released (leaked %d)",
				round, got, total, got-total)
		}
		if msgs, bytes, _ := eng.Totals(); msgs != 0 || bytes != 0 {
			t.Fatalf("round %d: engine totals after close: msgs=%d bytes=%d", round, msgs, bytes)
		}
	}
}

// TestAcceptedPublicationIsPlacedBeforeCloseReturns is the property the
// publish gate buys over the place-then-re-check protocol it wraps: a
// publication the engine accepted is in a lane, or in the retained log,
// before the settling phase walks it, so the shutdown itself releases the
// payload. Nothing is placed behind the drain and left to a goroutine Close
// is no longer waiting for, and nothing is leaked.
//
// It runs over the routes a publication can take to a placement, because a
// gate that covers all of them but one is not a perimeter. The retained-log
// routes matter as much as the lane ones: on a stream, a publication no
// subscription matched is still accepted and still appended, and there its
// payload comes back only when the entry is dropped. The trim and fetch
// routes are here because they are where the engine hands a record back
// *outside* its ticket, which is the half of the contract the routes above
// cannot see.
//
// What it asserts about ordering is what the engine can promise. A release is
// the caller's own code, so Close never waits for one (
// The specification): the drops a publication or a fetch defers past its ticket
// therefore run on the caller's own goroutine, inside the Publish or the
// Fetch that displaced the record, and one of those calls racing Close can
// finish after Close has returned. What must never happen is a payload coming
// home when no call of the caller's is in flight at all — that is a goroutine
// of the engine's releasing behind the drain, which is the defect the gate
// exists to prevent — or a payload not coming home at all. Both are checked
// here, together with the engine's own ledger.
//
// The check has teeth: letting the placement proceed when the ticket is
// refused — committing and leaving reap to drain it, which is what the engine
// did before this increment — makes it fail within a few hundred rounds under
// -race, and so does an ungated append to the retained log, which leaks the
// record outright.
//
// Which of its assertions carry that weight is worth being plain about. The
// two doing the work are the ledger ones: every accepted publication is
// released exactly once, and the engine's totals are zero afterwards. The
// orphaned-release check is latent on this tree — instrumented over all five
// routes at 3,000 rounds each, no payload is released after Close returned at
// all, so the predicate guarding it is never reached. It is kept because it is
// the assertion that fails first if Close ever stops waiting for a placement
// it accepted, and it is written per-goroutine (inCallerCall) so that it will
// mean something when it is: the counter it replaces was one atomic shared by
// four publishers and every consumer, which reads non-zero for nearly the
// whole active phase, so it could not have told an engine-goroutine release
// from a caller's own even had one occurred.
func TestAcceptedPublicationIsPlacedBeforeCloseReturns(t *testing.T) {
	stream := TopologyConfig{
		Kind: TopologyStream, Partitions: 1, CapacityMessages: 128,
		AckDeadline: time.Second, Retention: Retention{MaxMessages: 4096},
	}
	// A retention every publication trims: the entries it displaces are the
	// ones Topic.after drops once the publish ticket is home.
	trimming := stream
	trimming.Retention = Retention{MaxMessages: 4}
	for _, tc := range []closeRaceCase{
		// The lane route: one subscription takes every publication.
		{name: "queue", sub: "s"},
		// The retained-only route with no subscription at all.
		{name: "stream/nosubs", top: stream},
		// The retained-only route under ordinary subject-filtered traffic:
		// a live subscription whose filter does not match the publisher.
		{
			name: "stream/unmatched", top: withWildcards(stream),
			sub: "s", filter: "other.>", subject: "mine.one",
		},
		// The trim route: the append displaces older entries, whose last
		// reference is the publisher's payload.
		{name: "stream/trim", top: trimming},
		// The fetch route: consumers claim and acknowledge while the close
		// races them, so the record's last reference goes on their
		// goroutines rather than on a publisher's.
		{name: "queue/fetch", sub: "s", consumers: 4},
	} {
		t.Run(tc.name, func(t *testing.T) {
			acceptedPublicationIsPlacedBeforeCloseReturns(t, tc)
		})
	}
}

// closeRaceCase is one topology and workload raced against Engine.Close.
type closeRaceCase struct {
	name string
	top  TopologyConfig
	// sub is the subscription to create, if any, and filter is its subject
	// filter; subject is what the publishers publish to.
	sub, filter, subject string
	// consumers is how many consumers fetch and acknowledge alongside the
	// publishers; zero leaves the records where the publication put them.
	consumers int
}

// withWildcards returns c with subject routing enabled.
func withWildcards(c TopologyConfig) TopologyConfig { c.Wildcards = true; return c }

// inCallerCall reports whether the calling goroutine is inside one of the
// calls this test makes of its own — a Publish, a Fetch or an Ack — rather
// than on one of the engine's goroutines. A release inside one of those calls
// is that call's own work finishing, which the specification allows after Close
// has returned; a release on any other goroutine is the engine acting on its
// own after it said it was done, which is what this test is looking for.
//
// It is read from the callback's own stack. A counter incremented around each
// call cannot answer this: with four publishers and four consumers running,
// some call of somebody's is in flight for nearly the whole active phase, so a
// shared counter reads non-zero for an engine-goroutine release just as it
// does for a caller's own, and the check it guards never fires.
func inCallerCall() bool {
	buf := make([]byte, 16384)
	s := string(buf[:runtime.Stack(buf, false)])
	for _, caller := range []string{").Publish(", ").Fetch(", ").Ack("} {
		if strings.Contains(s, caller) {
			return true
		}
	}
	return false
}

func acceptedPublicationIsPlacedBeforeCloseReturns(t *testing.T, tc closeRaceCase) {
	t.Helper()
	const publishers = 4
	for round := range closeRaceRounds {
		var (
			mu sync.Mutex
			// late are the payloads released after Close returned, and
			// orphaned the ones released with no call of the caller's in
			// flight to account for them.
			late, orphaned []int64
			closeRet       atomic.Bool
			accepted       atomic.Int64
			released       atomic.Int64
		)
		eng, err := New(Config[int64]{
			// The architecture follows the case's topology: the stream and
			// wildcard cases cannot run on a shared partition log
			// , the queue case runs on whatever the
			// bake-off bound.
			Kinds: KindSet{Arch: archFor(tc.top)},
			Clone: func(v int64) int64 { return v },
			DefaultTopology: TopologyConfig{
				Partitions: 1, CapacityMessages: 128, AckDeadline: time.Hour,
			},
		})
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		tp, err := eng.CreateTopic("t", tc.top)
		if err != nil {
			t.Fatalf("CreateTopic: %v", err)
		}
		var sub *Subscription[int64]
		if tc.sub != "" {
			if sub, err = tp.Subscribe(tc.sub, SubscriptionConfig{Filter: tc.filter}); err != nil {
				t.Fatalf("Subscribe: %v", err)
			}
		}
		release := func(v int64) {
			released.Add(1)
			if !closeRet.Load() {
				return
			}
			mu.Lock()
			late = append(late, v)
			if !inCallerCall() {
				orphaned = append(orphaned, v)
			}
			mu.Unlock()
		}
		var wg sync.WaitGroup
		wg.Add(publishers + tc.consumers + 1)
		for g := range publishers {
			go func() {
				defer wg.Done()
				for i := range 8 {
					v := int64(g*8 + i + 1)
					err := tp.Publish(context.Background(), Publish[int64]{
						Subject: tc.subject, Payload: v, Size: 1,
						Mode: CopyOwned, Release: release,
					})
					if err == nil {
						accepted.Add(1)
					}
				}
			}()
		}
		for range tc.consumers {
			con, err := sub.Attach(ConsumerConfig{})
			if err != nil {
				t.Fatalf("Attach: %v", err)
			}
			go func() {
				defer wg.Done()
				dst := make([]Claim[int64], 4)
				for {
					n, err := con.Fetch(context.Background(), dst)
					for i := range n {
						// The acknowledgement is what drops the record's
						// last reference on this route.
						_ = con.Ack(dst[i].Env, dst[i].Gen)
					}
					if err != nil {
						return
					}
				}
			}()
		}
		go func() {
			defer wg.Done()
			for range round % 11 {
				runtime.Gosched()
			}
			if err := eng.Close(); err != nil {
				t.Errorf("close engine: %v", err)
			}
			closeRet.Store(true)
		}()
		wg.Wait()
		if t.Failed() {
			return
		}
		mu.Lock()
		bad := append([]int64(nil), orphaned...)
		mu.Unlock()
		if len(bad) > 0 {
			t.Fatalf("round %d: %d payload(s) released after Close returned with no call "+
				"of the caller's in flight: %v", round, len(bad), bad)
		}
		// Every accepted publication is accounted for, and no refused one
		// had its payload taken: a refusal leaves the payload with the
		// caller (adr/ADR-017-assumptions.md).
		if got, want := released.Load(), accepted.Load(); got != want {
			t.Fatalf("round %d: %d accepted, %d released", round, want, got)
		}
		if msgs, bytes, _ := eng.Totals(); msgs != 0 || bytes != 0 {
			t.Fatalf("round %d: engine totals after close: msgs=%d bytes=%d", round, msgs, bytes)
		}
	}
}
