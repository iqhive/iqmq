package flush

import (
	"testing"
	"time"
)

// TestModel runs every kind through the reference simulation.
//
// The four kinds do not agree on an answer, so the model cannot compare them
// against one oracle. What it does instead is hold each of them to what its
// own documentation claims — the cost it promises, the latency bound it
// promises, and how much it can be holding when the process stops — which is
// the property a caller chooses a kind by. The shared invariants (nothing
// lost, nothing sent twice, never fewer round trips than the broker's batch
// limit allows) are asserted for every kind.
func TestModel(t *testing.T) {
	for _, k := range Kinds() {
		t.Run(k.String(), func(t *testing.T) {
			t.Run("rate=1", func(t *testing.T) { modelRate(t, k, 1) })
			t.Run("rate=10", func(t *testing.T) { modelRate(t, k, 10) })
			t.Run("rate=100", func(t *testing.T) { modelRate(t, k, 100) })
			t.Run("burst", func(t *testing.T) { modelBurst(t, k) })
			t.Run("shutdown", func(t *testing.T) { modelShutdown(t, k) })
			t.Run("idle", func(t *testing.T) { modelIdle(t, k) })
			t.Run("kit", func(t *testing.T) { modelKit(t, k) })
		})
	}
}

// modelRate drives one kind at one arrival rate — `perInterval` settlements
// per ticker period — and checks the kind's own claims.
func modelRate(t *testing.T, k Kind, perInterval int) {
	const settlements = 500
	const maxItems = DefaultMaxItems
	interval := DefaultInterval
	cfg := simConfig{
		Settlements: settlements,
		Every:       int64(interval/time.Microsecond) / int64(perInterval),
		MaxItems:    maxItems,
		Interval:    interval,
	}
	s := newSim(t, k, cfg)
	s.run()

	// The shared invariants. Every kind has to satisfy these, and a policy
	// that loses or duplicates a settlement is caught here whatever its
	// cost.
	if s.sent != settlements {
		t.Fatalf("%d of %d settlements were sent: a policy may delay a settlement and may never lose one", s.sent, settlements)
	}
	if len(s.queued) != 0 {
		t.Fatalf("%d settlement(s) were still queued after the close", len(s.queued))
	}
	if floor := minTrips(settlements, maxItems); s.roundTrips < floor {
		t.Fatalf("%d round trips for %d settlements is below the floor of %d: a batch carries at most %d entries", s.roundTrips, settlements, floor, maxItems)
	}
	if s.roundTrips > settlements {
		t.Fatalf("%d round trips for %d settlements: no policy may cost more than one call each", s.roundTrips, settlements)
	}
	for i, n := range s.batches {
		if n > maxItems {
			t.Fatalf("batch %d carried %d entries, over the broker's limit of %d", i, n, maxItems)
		}
	}

	// The per-kind claims.
	intervalUS := float64(interval / time.Microsecond)
	switch k.Resolve() {
	case Immediate:
		if s.roundTrips != settlements {
			t.Fatalf("immediate made %d round trips for %d settlements; it sends every settlement on its own", s.roundTrips, settlements)
		}
		if s.maxLatency != 0 {
			t.Fatalf("immediate held a settlement for %dµs; it adds no latency at all", s.maxLatency)
		}
	case Count:
		if want := minTrips(settlements, maxItems); s.roundTrips != want {
			t.Fatalf("count made %d round trips for %d settlements; it sends only full batches, which is %d", s.roundTrips, settlements, want)
		}
	case Time:
		if s.maxLatency > int64(intervalUS) {
			t.Fatalf("time held a settlement for %dµs, over its own %gµs interval; the interval is the bound it promises whatever the rate", s.maxLatency, intervalUS)
		}
		if perInterval <= maxItems && s.roundTrips > minTrips(settlements, perInterval)+2 {
			t.Fatalf("time made %d round trips at %d settlements per interval; it coalesces a period's arrivals into one call", s.roundTrips, perInterval)
		}
	case Hybrid:
		if s.maxLatency > int64(intervalUS) {
			t.Fatalf("hybrid held a settlement for %dµs, over its own %gµs interval; the interval is what bounds the latency count alone cannot bound", s.maxLatency, intervalUS)
		}
		if want := minTrips(settlements, maxItems); perInterval > maxItems && s.roundTrips > want+minTrips(settlements, perInterval)+2 {
			t.Fatalf("hybrid made %d round trips at %d settlements per interval; above the batch size the count is what bounds the cost, so it should approach %d", s.roundTrips, perInterval, want)
		}
	case Auto, kindCount:
		t.Fatalf("Kinds() produced %v, which is not a concrete kind", k)
	}
}

// modelBurst is the case the cost claims are clearest in: MaxItems*4
// settlements arrive back to back with no time passing at all.
//
// Immediate pays four times MaxItems round trips for it. The other three pay
// four, because a burst fills a batch — and Time pays four rather than one
// because the queue is split on the broker's own limit even when one tick
// drains the whole thing.
func modelBurst(t *testing.T, k Kind) {
	const maxItems = 4
	n := maxItems * 4
	s := newSim(t, k, simConfig{Settlements: n, Every: 0, MaxItems: maxItems, Interval: time.Second})
	s.run()
	if s.sent != n {
		t.Fatalf("%d of %d settlements were sent", s.sent, n)
	}
	want := n / maxItems
	if k.Resolve() == Immediate {
		want = n
	}
	if s.roundTrips != want {
		t.Fatalf("%s made %d round trips for a burst of %d at a batch limit of %d, want %d", k, s.roundTrips, n, maxItems, want)
	}
}

// modelShutdown is the third criterion term.
//
// MaxItems-1 settlements arrive and then the source closes. Nothing may be
// lost, and what Close had to drain may not exceed what the policy says it can
// be holding — which is zero for Immediate and MaxItems-1 for the rest.
func modelShutdown(t *testing.T, k Kind) {
	const maxItems = 8
	n := maxItems - 1
	// The interval is longer than the whole run, so no tick can fire and
	// what Close finds is the policy's own worst case.
	s := newSim(t, k, simConfig{Settlements: n, Every: 1, MaxItems: maxItems, Interval: time.Hour})
	s.run()
	if s.sent != n {
		t.Fatalf("%d of %d settlements survived the close: a settlement the driver holds is one a caller is still waiting for, and Close sends it", s.sent, n)
	}
	if bound := s.p.MaxHeld(); bound >= 0 && s.held > bound {
		t.Fatalf("%s held %d settlements at the close but claims to hold at most %d", k, s.held, bound)
	}
	if k.Resolve() == Immediate && s.held != 0 {
		t.Fatalf("immediate held %d settlements at the close; it holds none, ever", s.held)
	}
	if k.Resolve() != Immediate && s.held != n {
		t.Fatalf("%s held %d of %d settlements at the close; none of them completes a batch and no tick fired, so all of them are still queued", k, s.held, n)
	}
}

// modelIdle is the property an idle consumer needs: a ticker that fires over
// an empty queue must send nothing, or a consumer with nothing to do becomes
// a request every interval for ever.
func modelIdle(t *testing.T, k Kind) {
	p, err := New(k, Config{})
	if err != nil {
		t.Fatalf("New(%v): %v", k, err)
	}
	if p.Ticked(0) {
		t.Fatalf("%s sends on a tick that found an empty queue; an idle consumer would then cost one API call per interval for ever", k)
	}
	// Every kind has to bound how long a settlement can be held, and there
	// are only two ways to do it: send when the batch is full, or run a
	// ticker. A kind that did neither would hold a caller inside Ack until
	// something unrelated flushed the queue, which is not a policy but a
	// hang.
	_, ticks := p.Ticker()
	if !p.Submitted(p.MaxItems()) && !ticks {
		t.Fatalf("%s neither sends at its batch limit of %d nor asks for a ticker, so nothing bounds how long it holds a settlement", k, p.MaxItems())
	}
	// A policy that bounds the queue must not need a ticker to stay bounded,
	// and one that does not bound the queue must have a ticker to drain it.
	// Those are the only two shapes that are not a hang.
	if (p.MaxHeld() < 0) != (!p.Submitted(p.MaxItems())) {
		t.Fatalf("%s says MaxHeld=%d and Submitted(%d)=%v; a policy bounds its queue exactly when it sends at the batch limit",
			k, p.MaxHeld(), p.MaxItems(), p.Submitted(p.MaxItems()))
	}
	if p.MaxHeld() < 0 && !ticks {
		t.Fatalf("%s bounds neither the queue nor the wait", k)
	}
}

// modelKit is the differential half: the policy is a statement about what
// [driverkit.BatchSettler] does, so the two have to agree.
//
// It is in the model rather than only in the fuzz target because this is the
// assertion that keeps the family honest. If the policy said "send at ten" and
// the kit sent at eight, the bake-off would be measuring a decision nothing
// implements.
func modelKit(t *testing.T, k Kind) {
	kitAgrees(t, k)
}
