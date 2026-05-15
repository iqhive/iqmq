package flush

import (
	"testing"
	"time"
)

// The reference simulation, and its cost model.
//
// the decision column for this row is "cost, ack latency, shutdown
// correctness", and none of the three is a property of a policy's own
// arithmetic: `Submitted` is two integer comparisons whatever the kind, so a
// microbenchmark of it says nothing about the family. What the kinds differ
// about is what a STREAM of settlements costs, and that is what this
// simulation drives.
//
// The clock is virtual and its unit is one microsecond, so nothing here sleeps
// and two runs of the same script produce the same numbers. One settlement
// arrives every `Every` microseconds; the shared ticker fires every
// `Interval`; a send is one round trip per `MaxItems` of the queue it drained,
// which is the ceiling [driverkit.BatchSettler] splits on. The three numbers
// are then:
//
//	roundtrips/settlement  the cost: API calls divided by settlements, so
//	                       1.0 is Immediate's and 1/MaxItems is the floor
//	latency/settlement     the ack latency: virtual microseconds between a
//	                       settlement being queued and its batch being sent
//	held                   what Close had to drain, which is the shutdown
//	                       term: nothing may be lost, and the number is how
//	                       much was still in the driver when the process
//	                       stopped
//
// What the simulation deliberately does NOT model is the round trip's own
// duration. A batch settlement's wall-clock cost is dominated by the network,
// and a model that guessed at it would decide the family on the guess; the
// criterion's word is "cost", and the count of round trips is the cost the
// service actually charges for.

// simConfig is one simulation run.
type simConfig struct {
	// Settlements is how many settlements arrive.
	Settlements int
	// Every is the virtual microseconds between arrivals.
	Every int64
	// MaxItems is the policy's batch ceiling.
	MaxItems int
	// Interval is the policy's ticker period.
	Interval time.Duration
}

// sim drives one policy over the virtual clock.
type sim struct {
	p   Policy
	cfg simConfig

	now      int64   // virtual microseconds
	nextTick int64   // when the shared ticker fires next, or -1 for no ticker
	queued   []int64 // the arrival time of every queued settlement

	// The recorded outcome.
	roundTrips int
	sent       int
	latency    int64
	maxLatency int64
	held       int
	batches    []int
}

// newSim builds a simulation for one kind.
func newSim(t testing.TB, k Kind, cfg simConfig) *sim {
	t.Helper()
	if cfg.Settlements <= 0 {
		cfg.Settlements = 100
	}
	if cfg.Every <= 0 {
		cfg.Every = 1
	}
	if cfg.MaxItems <= 0 {
		cfg.MaxItems = DefaultMaxItems
	}
	if cfg.Interval <= 0 {
		cfg.Interval = DefaultInterval
	}
	p, err := New(k, Config{MaxItems: cfg.MaxItems, Interval: cfg.Interval})
	if err != nil {
		t.Fatalf("New(%v): %v", k, err)
	}
	s := &sim{p: p, cfg: cfg, nextTick: -1}
	if d, ok := p.Ticker(); ok {
		s.nextTick = int64(d / time.Microsecond)
	}
	return s
}

// run drives every arrival and then closes, which is what a source does.
func (s *sim) run() {
	for range s.cfg.Settlements {
		s.advance(s.now + s.cfg.Every)
		s.queued = append(s.queued, s.now)
		if s.p.Submitted(len(s.queued)) {
			s.flush()
		}
	}
	// Close: the kit sends whatever is queued whether the policy asked for
	// it or not, which is the shutdown-correctness half of the criterion.
	// No synthetic final tick is fired first, deliberately: what a policy is
	// holding when the process stops is the number the criterion asks for,
	// and a model that gave every run one last tick would report zero for
	// every kind and prove nothing.
	s.held = len(s.queued)
	s.flush()
}

// advance moves the virtual clock to t, firing every tick that falls due.
func (s *sim) advance(t int64) {
	for s.nextTick >= 0 && s.nextTick <= t {
		s.now = s.nextTick
		s.nextTick += int64(s.cfg.Interval / time.Microsecond)
		if s.p.Ticked(len(s.queued)) {
			s.flush()
		}
	}
	if t > s.now {
		s.now = t
	}
}

// flush sends everything queued, in batches of at most MaxItems.
func (s *sim) flush() {
	held := s.queued
	s.queued = nil
	for len(held) > 0 {
		n := min(len(held), s.p.MaxItems())
		s.roundTrips++
		s.batches = append(s.batches, n)
		for _, at := range held[:n] {
			lat := s.now - at
			s.latency += lat
			if lat > s.maxLatency {
				s.maxLatency = lat
			}
			s.sent++
		}
		held = held[n:]
	}
}

// minTrips is the floor: no policy can settle n items in fewer than this many
// round trips, because MaxItems is the broker's own batch limit.
func minTrips(n, maxItems int) int { return (n + maxItems - 1) / maxItems }
