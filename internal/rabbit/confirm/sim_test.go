package confirm

// The reference simulation.
//
// Every arm of the benchmark and every subtest of the model drives this, and
// nothing in it reads a real clock or performs any I/O: a publication is a
// virtual microsecond of local work and a confirm becomes due one virtual
// round trip after the publication was submitted. That is what makes the family's numbers reproducible, and it is why `ns/op` is reported BESIDE the
// criterion rather than as it — the interesting cost of a confirm strategy is
// what it does to a publisher's elapsed time, not what `OnSubmit` costs to
// call, which is one integer comparison in all three kinds.
//
// # The cost model
//
//	publishCost   virtual microseconds of local work per `basic.publish`.
//	rtt           virtual microseconds between a publication being submitted
//	              and its confirm becoming available at the client.
//	ackEvery      how many consecutive due confirms the broker coalesces into
//	              one `basic.ack{multiple=true}` frame. One means every
//	              publication gets its own frame; a larger number is the
//	              cumulative form, which is what a real broker does under
//	              load and which the tracker has to apply correctly.
//
// The publisher's elapsed virtual time is what "throughput" means here, and it
// is the quantity the criterion reads: a per-message strategy pays
// publishCost+rtt per message, and a pipelined one pays publishCost per
// message plus one rtt to fill the pipeline, so the two separate by a factor
// of rtt/publishCost however fast the code is.

import "testing"

// simConfig is one run of the simulation.
type simConfig struct {
	// Publications is how many messages the run publishes.
	Publications int
	// Batch is how many publications one logical batch holds. A blocking
	// strategy waits for its outstanding confirms at each batch boundary,
	// which is what `mq.BatchSink.PublishBatch` owes its caller. Zero means
	// one batch for the whole run.
	Batch int
	// PublishCost is the local work per publication, in virtual
	// microseconds.
	PublishCost int64
	// RTT is the virtual round trip to the broker and back.
	RTT int64
	// AckEvery is how many due confirms the broker coalesces into one
	// cumulative frame. Zero means one.
	AckEvery int
	// Window is the strategy's window; zero means the kind's default.
	Window int
	// DetachAtEnd closes the channel instead of flushing, which is what
	// measures the specification's "ambiguity accuracy": every publication still
	// outstanding becomes one Ambiguous outcome.
	DetachAtEnd bool
	// Tokens gives every publication a correlation token, as a driver does
	// for a `mandatory` publication under a strategy that needs one.
	Tokens bool
}

// sim is one run's state.
type sim struct {
	tb  testing.TB
	st  Strategy
	tr  *Tracker
	cfg simConfig

	now int64
	// due is the virtual time each outstanding publication's confirm becomes
	// available, and submitted when it was published.
	due       map[uint64]int64
	submitted map[uint64]int64
	// order is the outstanding tags in increasing order, which is what keeps
	// one step proportional to the WINDOW rather than to everything the run
	// has ever published. Tags are issued in order and resolved in nearly
	// that order, so the slice is compacted in place rather than searched.
	order []uint64
	// pending is the tags whose confirm is due but which the broker has not
	// yet framed, in tag order.
	pending []uint64

	stalls int
	frames int
	// maxOut is the largest number of publications that were outstanding at
	// once, sampled where it really peaks: immediately after a submission
	// and BEFORE the strategy's own wait. A sample taken after the wait
	// reads the window minus one and would pass for a strategy whose window
	// was one narrower than it claims.
	maxOut    int
	latency   int64
	resolved  int
	confirmed int
	rejected  int
	ambiguous int
	// seen counts terminal outcomes per tag, which is how "nothing is
	// resolved twice" becomes an assertion rather than a hope.
	seen map[uint64]int
}

// newSim builds a run. It fails the test on a configuration the strategy
// refuses, because that is a defect in the test and not an outcome.
func newSim(tb testing.TB, k Kind, cfg simConfig) *sim {
	tb.Helper()
	st, err := New(k, Config{Window: cfg.Window})
	if err != nil {
		tb.Fatalf("building the %v strategy: %v", k, err)
	}
	if cfg.AckEvery <= 0 {
		cfg.AckEvery = 1
	}
	if cfg.Batch <= 0 {
		cfg.Batch = cfg.Publications
	}
	return &sim{
		tb:        tb,
		st:        st,
		tr:        NewTracker(st.Window()),
		cfg:       cfg,
		due:       make(map[uint64]int64, st.Window()+1),
		submitted: make(map[uint64]int64, st.Window()+1),
		seen:      make(map[uint64]int, cfg.Publications),
	}
}

// run publishes the whole workload.
func (s *sim) run() {
	for i := range s.cfg.Publications {
		s.publish(i)
		if (i+1)%s.cfg.Batch == 0 && s.st.Blocks() && s.st.Window() > 1 {
			// The batch boundary. A per-message strategy has nothing
			// outstanding here, and an asynchronous one owes the caller no
			// wait at all.
			s.flush()
		}
	}
	if s.cfg.DetachAtEnd {
		s.detach()
		return
	}
	s.drain()
}

// publish submits one publication and applies the strategy's decision.
func (s *sim) publish(i int) {
	s.tb.Helper()
	s.now += s.cfg.PublishCost
	token := ""
	if s.cfg.Tokens {
		token = tokenOf(i)
	}
	tag, err := s.tr.Submit(token)
	if err != nil {
		s.tb.Fatalf("publication %d refused: %v", i, err)
	}
	s.submitted[tag] = s.now
	s.due[tag] = s.now + s.cfg.RTT
	s.order = append(s.order, tag)
	outstanding := s.tr.Outstanding()
	s.maxOut = max(s.maxOut, outstanding)
	switch a := s.st.OnSubmit(outstanding); a {
	case WaitTag:
		s.waitFor(tag)
	case WaitWindow:
		s.waitBelow(s.st.Window())
	case Continue, Defer:
	default:
		s.tb.Fatalf("publication %d: the strategy asked for %v", i, a)
	}
}

// waitFor advances the virtual clock until one publication's confirm has been
// applied.
func (s *sim) waitFor(tag uint64) {
	s.stalls++
	for s.tr.Outstanding() > 0 {
		if _, live := s.due[tag]; !live {
			return
		}
		s.step()
	}
}

// waitBelow advances the virtual clock until fewer than w publications are
// outstanding. It is backpressure and not a confirmation boundary.
func (s *sim) waitBelow(w int) {
	s.stalls++
	for s.tr.Outstanding() >= w {
		s.step()
	}
}

// flush advances the virtual clock until nothing is outstanding, which is what
// a blocking strategy owes its caller at the end of a batch.
func (s *sim) flush() {
	if s.tr.Outstanding() == 0 {
		return
	}
	s.stalls++
	for s.tr.Outstanding() > 0 {
		s.step()
	}
}

// drain advances the clock until nothing is outstanding WITHOUT counting a
// stall.
//
// It is how a run ends, and it is deliberately not [sim.flush]. A blocking
// strategy's last batch boundary has already flushed, so this is a no-op for
// it; an asynchronous strategy owes its caller no wait at all, and counting the
// run's own tidying as a publisher stall would put the two kinds' stall counts
// one apart for a reason that is the simulation's rather than the strategy's.
func (s *sim) drain() {
	for s.tr.Outstanding() > 0 {
		s.step()
	}
}

// step advances the clock to the point where the broker frames its next batch
// of confirms, and applies them.
//
// The broker coalesces: it waits until AckEvery confirms are due (or until
// everything outstanding is due, whichever comes first) and then sends one
// `basic.ack{multiple=true}` per group. That is what a real broker does under
// load, and it is the behaviour the tracker's cumulative path exists for — so
// a simulation that always framed one confirm at a time would never exercise
// it, and the family's hardest-to-notice defect would be invisible.
func (s *sim) step() {
	// The due times of the outstanding publications, which are in tag order
	// and therefore already sorted: a tag issued later was submitted later
	// and becomes due later.
	target := int64(-1)
	seen := 0
	for _, tag := range s.order {
		d, live := s.due[tag]
		if !live {
			continue
		}
		target = d
		seen++
		if seen >= s.cfg.AckEvery {
			break
		}
	}
	if target < 0 {
		s.tb.Fatal("the simulation waited with nothing outstanding: the strategy asked for a wait it cannot be released from")
	}
	if target > s.now {
		s.now = target
	}
	// Everything whose confirm is due at or before now, in tag order, which
	// is the order the broker would frame them in.
	s.pending = s.pending[:0]
	for _, tag := range s.order {
		if d, live := s.due[tag]; live && d <= s.now {
			s.pending = append(s.pending, tag)
		}
	}
	if len(s.pending) == 0 {
		s.tb.Fatalf("no confirm is due at virtual time %d although one was expected", s.now)
	}
	for i := 0; i < len(s.pending); i += s.cfg.AckEvery {
		end := min(i+s.cfg.AckEvery, len(s.pending))
		group := s.pending[i:end]
		high := group[len(group)-1]
		s.frames++
		multiple := len(group) > 1
		s.apply(s.tr.Ack(high, multiple))
	}
}

// detach closes the channel, resolving everything outstanding as Ambiguous.
func (s *sim) detach() { s.apply(s.tr.Detach()) }

// apply records a batch of outcomes and checks the invariants every kind owes.
func (s *sim) apply(out []Outcome) {
	for _, o := range out {
		s.seen[o.Tag]++
		if s.seen[o.Tag] > 1 {
			s.tb.Fatalf("tag %d was resolved %d times: a publication has exactly one outcome", o.Tag, s.seen[o.Tag])
		}
		if !o.Result.Terminal() {
			s.tb.Fatalf("tag %d resolved to %v, which is not terminal", o.Tag, o.Result)
		}
		delete(s.due, o.Tag)
		s.compact()
		s.resolved++
		switch o.Result {
		case Pending, resultCount:
			// Unreachable: the guard above rejects a non-terminal result, and
			// the sentinel is not a result. Naming them is what makes the
			// switch total, so a fourth outcome does not land in a silent
			// default.
			s.tb.Fatalf("tag %d resolved to %v", o.Tag, o.Result)
		case Confirmed:
			s.confirmed++
			s.latency += s.now - s.submitted[o.Tag]
		case Rejected:
			s.rejected++
			s.latency += s.now - s.submitted[o.Tag]
		case Ambiguous:
			s.ambiguous++
		}
	}
}

// compact drops the tags that are no longer outstanding from the order slice,
// from the front where they nearly always are.
func (s *sim) compact() {
	i := 0
	for i < len(s.order) {
		if _, live := s.due[s.order[i]]; live {
			break
		}
		i++
	}
	if i > 0 {
		s.order = append(s.order[:0], s.order[i:]...)
	}
}

// elapsed is the run's virtual duration, which is what "throughput" means in
// this family.
func (s *sim) elapsed() int64 { return s.now }

// tokenOf is the correlation token a driver would put on publication i. It is
// a counter and not a random value, which is what makes
// Tracker.ErrDuplicateToken unreachable in practice.
func tokenOf(i int) string {
	const digits = "0123456789abcdefghijklmnopqrstuvwxyz"
	if i == 0 {
		return "p0"
	}
	var buf [16]byte
	n := len(buf)
	for i > 0 {
		n--
		buf[n] = digits[i%36]
		i /= 36
	}
	return "p" + string(buf[n:])
}
