package engine

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"bitbucket.org/iqhive/iqmq/internal/clock"
	"bitbucket.org/iqhive/iqmq/internal/errcode"
)

// The ownership contract of an owned publication is the one thing every
// fan-out architecture has to keep and nothing in the bake-off's gate used to
// check: the message contract gives `Publish.Release` "called exactly once
// (incl. Rejection, close, permanent failure)", the specification makes the terminal
// release an invariant, and the specification makes losing an accepted message and
// double-releasing owned memory disqualifiers outright.
//
// The gate runs before it writes a binding is the family's
// package suite, and it attributes a failure to a kind only when the failing
// test names the kind — `TestModel/<slug>`. Everything else in the package
// runs against whichever kind `auto_arch.go` currently binds, so a kind could
// win its family, be bound, and only then break a contract nothing had ever
// run against it. That is what these subtests close: they are arms of
// `TestModel/<arch>`, so an architecture that leaks a payload on any terminal
// route is disqualified by the gate before the binding is written
// (adr/ADR-017-assumptions.md).

// archOwnedHarness is one engine of a named architecture, with a ledger of
// which owned payloads the engine took and which have come home.
type archOwnedHarness struct {
	arch Arch
	eng  *Engine[int64]
	top  *Topic[int64]
	fake *clock.Fake
	next int64

	mu       sync.Mutex
	released map[int64]int
	owned    map[int64]bool
	refused  map[int64]bool
}

// newArchOwnedHarness builds the engine for one route on architecture a.
func newArchOwnedHarness(t *testing.T, a Arch, cfg TopologyConfig) *archOwnedHarness {
	t.Helper()
	h := &archOwnedHarness{
		arch:     a.Resolve(),
		fake:     clock.NewFake(time.Unix(0, 0)),
		released: map[int64]int{},
		owned:    map[int64]bool{},
		refused:  map[int64]bool{},
	}
	eng, err := New(Config[int64]{
		Kinds: KindSet{Arch: a},
		Clock: h.fake,
		// A copying architecture needs a clone and refuses the publication
		// without one; the sharing ones never call it.
		Clone:           func(v int64) int64 { return v },
		DrainTimeout:    5 * time.Second,
		DefaultTopology: cfg,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	h.eng = eng
	top, err := eng.CreateTopic("t", TopologyConfig{})
	if err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	h.top = top
	return h
}

// publish makes one owned publication of the next payload value and records
// who owns the payload afterwards, by the rule nil and
// errcode.ErrPartial mean the engine took the record, every other error means
// the caller still holds it.
func (h *archOwnedHarness) publish(p Publish[int64]) error {
	h.mu.Lock()
	h.next++
	v := h.next
	h.mu.Unlock()
	p.Payload = v
	p.Size = 1
	p.Mode = CopyOwned
	p.Release = func(x int64) {
		h.mu.Lock()
		h.released[x]++
		h.mu.Unlock()
	}
	err := h.top.Publish(context.Background(), p)
	h.mu.Lock()
	if err == nil || errors.Is(err, errcode.ErrPartial) {
		h.owned[v] = true
	} else {
		h.refused[v] = true
	}
	h.mu.Unlock()
	return err
}

// subscribe adds a subscription with a consumer attached.
func (h *archOwnedHarness) subscribe(t *testing.T, name string) (*Subscription[int64], *Consumer[int64]) {
	t.Helper()
	sub, err := h.top.Subscribe(name, SubscriptionConfig{})
	if err != nil {
		t.Fatalf("Subscribe(%s): %v", name, err)
	}
	c, err := sub.Attach(ConsumerConfig{})
	if err != nil {
		t.Fatalf("Attach(%s): %v", name, err)
	}
	return sub, c
}

// fetch takes up to n claims, without settling them.
func (h *archOwnedHarness) fetch(t *testing.T, c *Consumer[int64], n int) []Claim[int64] {
	t.Helper()
	dst := make([]Claim[int64], n)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	got, err := c.Fetch(ctx, dst)
	if err != nil && !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
		t.Fatalf("Fetch: %v", err)
	}
	return dst[:got]
}

// drain fetches and acknowledges until the subscription hands out nothing.
func (h *archOwnedHarness) drain(t *testing.T, c *Consumer[int64]) {
	t.Helper()
	for {
		cls := h.fetch(t, c, 8)
		if len(cls) == 0 {
			return
		}
		for _, cl := range cls {
			if err := c.Ack(cl.Env, cl.Gen); err != nil {
				t.Fatalf("Ack: %v", err)
			}
		}
	}
}

// home returns how many of the payloads the engine took have come home, and
// how many came home more than once.
func (h *archOwnedHarness) home() (done, twice, taken int) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for v := range h.owned {
		switch n := h.released[v]; {
		case n == 1:
			done++
		case n > 1:
			twice++
		}
	}
	return done, twice, len(h.owned)
}

// check holds the engine to the rule: every payload it accepted comes home
// exactly once, and one it refused never comes home. A release may run on a
// goroutine of the engine's own — the scheduler's time-to-live sweep, a
// dead-letter publication — so the wait is a poll rather than an instant.
func (h *archOwnedHarness) check(t *testing.T, phase string) {
	t.Helper()
	deadline := time.Now().Add(500 * time.Millisecond)
	for {
		done, twice, taken := h.home()
		if twice == 0 && done == taken {
			break
		}
		if twice > 0 || time.Now().After(deadline) {
			h.mu.Lock()
			for v := range h.owned {
				switch n := h.released[v]; {
				case n == 0:
					t.Errorf("%s: the engine accepted payload %d and never handed it back: a payload no subscription will release is a leak of caller-owned memory (the message contract)", phase, v)
				case n > 1:
					t.Errorf("%s: payload %d came home %d times, want once: a double release ", phase, v, n)
				}
			}
			h.mu.Unlock()
			break
		}
		time.Sleep(time.Millisecond)
	}
	h.checkRefused(t, phase)
}

// checkRefused holds the other half of the rule: a publication the engine
// refused leaves its payload with the caller, so releasing it is a double free
// waiting to happen.
func (h *archOwnedHarness) checkRefused(t *testing.T, phase string) {
	t.Helper()
	h.mu.Lock()
	defer h.mu.Unlock()
	for v := range h.refused {
		if n := h.released[v]; n != 0 {
			t.Errorf("%s: the publication of payload %d was refused and the payload came home %d times; ownership stays with the caller ", phase, v, n)
		}
	}
}

// close shuts the engine down, which drops everything it still holds.
func (h *archOwnedHarness) close(t *testing.T) {
	t.Helper()
	if err := h.eng.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

// archOwnedRoute is one terminal route an owned publication can take. Every
// route is written so that every architecture can run it: nothing here asks
// for a stream, priorities, wildcards or a per-subscription capacity, which a
// shared partition log refuses.
type archOwnedRoute struct {
	name string
	top  TopologyConfig
	run  func(t *testing.T, h *archOwnedHarness)
	// settles reports that the route reaches a terminal state on its own, so
	// the ledger is checked before Close as well as after. Without that check
	// a payload that only comes home because Close dropped what the engine
	// was still holding reads as correct.
	settles bool
}

// archOwnedRoutes are the terminal routes of the memory engine.
func archOwnedRoutes() []archOwnedRoute {
	base := TopologyConfig{Partitions: 1, CapacityMessages: 8, AckDeadline: -1}
	full := base
	full.CapacityMessages = 2
	dropNewest := full
	dropNewest.Overflow = OverflowDropNewest
	dropOldest := full
	dropOldest.Overflow = OverflowDropOldest
	attempts := base
	attempts.MaxAttempts = 1
	return []archOwnedRoute{{
		// The ordinary route: delivered, acknowledged, released.
		name: "ack", top: base, settles: true,
		run: func(t *testing.T, h *archOwnedHarness) {
			_, c := h.subscribe(t, "s")
			for range 4 {
				if err := h.publish(Publish[int64]{Partition: 0}); err != nil {
					t.Fatalf("Publish: %v", err)
				}
			}
			h.drain(t, c)
		},
	}, {
		// Two subscriptions: who gives the payload back, and when.
		name: "fanout-ack", top: base, settles: true,
		run: func(t *testing.T, h *archOwnedHarness) {
			_, a := h.subscribe(t, "a")
			_, b := h.subscribe(t, "b")
			for range 4 {
				if err := h.publish(Publish[int64]{Partition: 0}); err != nil {
					t.Fatalf("Publish: %v", err)
				}
			}
			if h.arch.Copies() {
				// A copying architecture is defined by the copy it makes:
				// every subscription observes a clone and the caller's
				// payload goes back at publication, so it is already home
				// before anything is delivered (the memory engine).
				h.check(t, "at publication, under a copying architecture")
				h.drain(t, a)
				h.drain(t, b)
				return
			}
			h.drain(t, a)
			// A sharing architecture holds one reference-counted record for
			// the whole fan-out, so half the fan-out having settled releases
			// nothing: the payload is the last subscription's to give back.
			// This is the reference count of the record and, under a shared
			// log, of the slot as well.
			if done, _, taken := h.home(); done != 0 {
				t.Errorf("%d of %d payloads came home while a subscription still held the record", done, taken)
			}
			h.drain(t, b)
		},
	}, {
		// A topic with no subscription discards the publication and hands
		// the payload straight back.
		name: "no-subscription", top: base, settles: true,
		run: func(t *testing.T, h *archOwnedHarness) {
			for range 4 {
				if err := h.publish(Publish[int64]{Partition: 0}); err != nil {
					t.Fatalf("Publish: %v", err)
				}
			}
		},
	}, {
		// Close drops what a lane still holds, ready or in flight.
		name: "close-while-ready", top: base,
		run: func(t *testing.T, h *archOwnedHarness) {
			h.subscribe(t, "s")
			for range 4 {
				if err := h.publish(Publish[int64]{Partition: 0}); err != nil {
					t.Fatalf("Publish: %v", err)
				}
			}
		},
	}, {
		name: "close-while-in-flight", top: base,
		run: func(t *testing.T, h *archOwnedHarness) {
			_, c := h.subscribe(t, "s")
			for range 4 {
				if err := h.publish(Publish[int64]{Partition: 0}); err != nil {
					t.Fatalf("Publish: %v", err)
				}
			}
			if n := len(h.fetch(t, c, 4)); n == 0 {
				t.Fatal("nothing was delivered")
			}
		},
	}, {
		// A subscription that leaves releases everything it held, and under
		// a shared log gives up every slot reference its cursor stood on.
		name: "subscription-close", top: base, settles: true,
		run: func(t *testing.T, h *archOwnedHarness) {
			sub, c := h.subscribe(t, "s")
			for range 4 {
				if err := h.publish(Publish[int64]{Partition: 0}); err != nil {
					t.Fatalf("Publish: %v", err)
				}
			}
			h.fetch(t, c, 2)
			if err := sub.Close(); err != nil {
				t.Fatalf("Subscription.Close: %v", err)
			}
		},
	}, {
		// A record that has run out of attempts is dropped rather than
		// requeued, and the drop is terminal.
		name: "nack-past-max-attempts", top: attempts, settles: true,
		run: func(t *testing.T, h *archOwnedHarness) {
			_, c := h.subscribe(t, "s")
			for range 2 {
				if err := h.publish(Publish[int64]{Partition: 0}); err != nil {
					t.Fatalf("Publish: %v", err)
				}
			}
			for _, cl := range h.fetch(t, c, 2) {
				if err := c.Nack(cl.Env, cl.Gen, 0); err != nil {
					t.Fatalf("Nack: %v", err)
				}
			}
		},
	}, {
		// A bounded configuration goes on accepting once its records have
		// been settled. A slot, an envelope or a reservation that is never
		// reclaimed is unbounded memory under a bounded configuration
		// even where the payload itself comes home:
		// under a shared log the payload's reference count and the slot's
		// are two separate counters, and only one of them is what the
		// publishers wait for.
		name: "capacity-returns", top: full, settles: true,
		run: func(t *testing.T, h *archOwnedHarness) {
			_, c := h.subscribe(t, "s")
			for i := range 16 {
				if err := h.publish(Publish[int64]{Partition: 0}); err != nil {
					t.Fatalf("publication %d into a lane whose records have all been settled was refused: %v: the capacity of a settled record was never reclaimed", i, err)
				}
				h.drain(t, c)
			}
		},
	}, {
		// A refused publication leaves its payload with the caller.
		name: "refused", top: full,
		run: func(t *testing.T, h *archOwnedHarness) {
			h.subscribe(t, "s")
			refused := 0
			for range 6 {
				if err := h.publish(Publish[int64]{Partition: 0}); err != nil {
					if !errors.Is(err, errcode.ErrQueueFull) {
						t.Fatalf("Publish: %v", err)
					}
					refused++
				}
			}
			if refused == 0 {
				t.Fatal("no publication was refused, so the route observed nothing")
			}
			h.checkRefused(t, "while the engine holds the rest")
		},
	}, {
		// An overflow policy that discards reports success, so the engine
		// owns the payload and releases it exactly as it does a delivered
		// one.
		name: "overflow-drop-newest", top: dropNewest, settles: true,
		run: func(t *testing.T, h *archOwnedHarness) {
			_, c := h.subscribe(t, "s")
			for range 6 {
				if err := h.publish(Publish[int64]{Partition: 0}); err != nil {
					t.Fatalf("Publish: %v", err)
				}
			}
			h.drain(t, c)
		},
	}, {
		name: "overflow-drop-oldest", top: dropOldest, settles: true,
		run: func(t *testing.T, h *archOwnedHarness) {
			_, c := h.subscribe(t, "s")
			for range 6 {
				if err := h.publish(Publish[int64]{Partition: 0}); err != nil {
					t.Fatalf("Publish: %v", err)
				}
			}
			h.drain(t, c)
		},
	}, {
		// A record that outlives its time to live is dropped by the sweep or
		// by the claim that would have delivered it, and the drop releases
		// the payload.
		name: "ttl-expiry", top: base, settles: true,
		run: func(t *testing.T, h *archOwnedHarness) {
			_, c := h.subscribe(t, "s")
			for range 4 {
				if err := h.publish(Publish[int64]{
					Partition: 0, TTL: 10 * time.Millisecond,
				}); err != nil {
					t.Fatalf("Publish: %v", err)
				}
			}
			h.fake.Advance(time.Second)
			if n := len(h.fetch(t, c, 4)); n != 0 {
				t.Errorf("%d expired records were delivered", n)
			}
		},
	}, {
		// A delayed publication waits in the subscription's own structure
		// rather than in the lane's ready set, which is a holder of its own
		// under every architecture.
		name: "close-while-delayed", top: base,
		run: func(t *testing.T, h *archOwnedHarness) {
			h.subscribe(t, "s")
			for range 4 {
				if err := h.publish(Publish[int64]{
					Partition: 0, Delay: time.Hour,
				}); err != nil {
					t.Fatalf("Publish: %v", err)
				}
			}
		},
	}}
}

// archOwnership runs every terminal route on architecture a, and then the
// concurrent one.
func archOwnership(t *testing.T, a Arch) {
	t.Helper()
	// Indexed rather than ranged by value: the route carries three
	// TopologyConfigs' worth of fields and gocritic's rangeValCopy refuses
	// the copy.
	routes := archOwnedRoutes()
	for i := range routes {
		rt := &routes[i]
		t.Run(rt.name, func(t *testing.T) {
			h := newArchOwnedHarness(t, a, rt.top)
			rt.run(t, h)
			if rt.settles {
				h.check(t, "once every publication had reached a terminal state")
			}
			h.close(t)
			h.check(t, "after Close")
		})
	}
	// hack/PLAYBOOK.md's recurring defect class, per architecture: a record
	// put into a store while its subscription or the engine is closing, whose
	// placement lands after the drain, so nothing will ever settle it -- and
	// under a shared log that leaks the slot's reference as well as the
	// record's. TestPublishRacesSubscriptionClose already races the publish
	// half over every architecture; the requeue half raced only the retry
	// placements, on whichever architecture was bound. Two thousand rounds
	// per arm, because the known instances of this class appeared at five to
	// twenty failures per two or three thousand and a hundred rounds proves
	// nothing (adr/ADR-017-assumptions.md).
	for _, closer := range []string{"requeue-races-subscription-close", "requeue-races-engine-close"} {
		t.Run(closer, func(t *testing.T) {
			raceNackClose(t, KindSet{Arch: a}, closer == "requeue-races-subscription-close")
		})
	}
}
