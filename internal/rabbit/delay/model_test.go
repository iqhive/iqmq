package delay

import (
	"testing"
	"time"
)

// TestModel runs every kind through the properties a delayed
// redelivery has to have.
//
// The two kinds do not agree on an answer — one grants the delay exactly and
// the other rounds it up to a bucket — so there is no single oracle. What the
// model does is hold each of them to the rules put on ANY delay,
// which is the half that must not differ:
//
//   - the granted delay is never SHORTER than the requested one. The specification makes a
//     delay a minimum, and a message delivered early breaks the contract the
//     caller wrote. This is the invariant that matters most, and it is the one
//     a bucket ladder gets wrong if it rounds the convenient way.
//   - the route back reaches exactly the target that asked for it, so a
//     delayed redelivery on one subscription does not become a duplicate on
//     every other subscription of the topic.
//   - a delay longer than the strategy can express is refused rather than
//     truncated.
//   - the plan's own arithmetic is consistent: the queue it declares carries
//     the TTL the plan granted, and the publication is routed to that queue.
func TestModel(t *testing.T) {
	for _, k := range Kinds() {
		t.Run(k.String(), func(t *testing.T) {
			t.Run("never-early", func(t *testing.T) { modelNeverEarly(t, k) })
			t.Run("route-reaches-the-target", func(t *testing.T) { modelRoute(t, k) })
			t.Run("too-long-is-refused", func(t *testing.T) { modelTooLong(t, k) })
			t.Run("zero-delay-costs-nothing", func(t *testing.T) { modelZero(t, k) })
			t.Run("queue-count", func(t *testing.T) { modelQueues(t, k) })
			t.Run("plan-is-self-consistent", func(t *testing.T) { modelConsistent(t, k) })
		})
	}
}

// modelDelays is the set of requested delays the model sweeps: powers of ten,
// ladder boundaries and the awkward values just above and below them.
func modelDelays(st Strategy) []time.Duration {
	out := make([]time.Duration, 0, 14)
	// The strategy's own ceiling and the value just below it are in the same
	// append as the fixed values, because they are the same kind of input: the
	// ladder walk's off-by-one shows at the ceiling and nowhere else.
	out = append(out,
		time.Millisecond, 10*time.Millisecond, 99*time.Millisecond,
		100*time.Millisecond, 101*time.Millisecond,
		time.Second, 1500*time.Millisecond,
		30*time.Second, 59*time.Second, time.Minute,
		5*time.Minute, time.Hour,
		st.MaxDelay(), st.MaxDelay()-time.Nanosecond,
	)
	return out
}

// newStrategy builds a strategy or fails the test, which is what every subtest
// starts with.
func newStrategy(t *testing.T, k Kind) Strategy {
	t.Helper()
	st, err := New(k, Config{})
	if err != nil {
		t.Fatalf("building the %v strategy: %v", k, err)
	}
	return st
}

// target is the redelivery target every subtest uses.
func target() Target { return RedeliveryTarget("iqmq.orders.workers") }

// request is one delayed redelivery of that target.
func request(d time.Duration) Request {
	return Request{Target: target(), Delay: d, DelayExchange: "iqmq.orders.delayed"}
}

// modelNeverEarly is the invariant the specification makes: a delay is a minimum.
func modelNeverEarly(t *testing.T, k Kind) {
	st := newStrategy(t, k)
	for _, d := range modelDelays(st) {
		if d > st.MaxDelay() || d < 0 {
			continue
		}
		p, err := st.Plan(request(d))
		if err != nil {
			t.Fatalf("delay %v refused: %v", d, err)
		}
		if p.Granted < d {
			t.Fatalf("a request for %v was granted %v: the message contract makes a delay a MINIMUM, so a message may be late and never early", d, p.Granted)
		}
		// And the rounding is bounded, which is the half that makes the
		// invariant above mean something: without it a strategy that
		// granted a day for every request would pass. The bound is the
		// strategy's own arithmetic rather than a tolerance -- a delay at
		// or below the resolution is granted exactly the resolution, and
		// anything above it is granted less than double, because the
		// reference kind's ladder doubles and the other kind is exact.
		switch {
		case d <= st.Resolution():
			if p.Granted != st.Resolution() {
				t.Fatalf("a request for %v was granted %v, and the strategy's resolution is %v: a delay below the resolution is granted exactly the resolution",
					d, p.Granted, st.Resolution())
			}
		case p.Granted >= 2*d:
			t.Fatalf("a request for %v was granted %v, which is at least double: a delay rounded up without a bound is not a delay",
				d, p.Granted)
		}
	}
}

// modelRoute is the fan-out bug this package's design exists to avoid: a
// delayed redelivery must reach the ONE subscription that asked for it.
func modelRoute(t *testing.T, k Kind) {
	st := newStrategy(t, k)
	p, err := st.Plan(request(30 * time.Second))
	if err != nil {
		t.Fatal(err)
	}
	switch {
	case p.Declare != nil:
		// The park-and-dead-letter route. The queue's dead-letter route has
		// to be the target's, or the message comes back somewhere else.
		if p.Declare.DeadLetterExchange != target().Exchange {
			t.Errorf("the delay queue dead-letters to the exchange %q, want %q: the topic's exchange would fan the redelivery out to every subscription",
				p.Declare.DeadLetterExchange, target().Exchange)
		}
		if p.Declare.DeadLetterRoutingKey != target().RoutingKey {
			t.Errorf("the delay queue dead-letters with the routing key %q, want %q", p.Declare.DeadLetterRoutingKey, target().RoutingKey)
		}
		// And the publication has to reach the delay queue, which on the
		// default exchange means the routing key IS the queue's name.
		if p.Exchange != "" || p.RoutingKey != p.Declare.Name {
			t.Errorf("the plan publishes to exchange %q with key %q, but the delay queue is %q: the message would not reach the queue that holds it",
				p.Exchange, p.RoutingKey, p.Declare.Name)
		}
	default:
		// The broker-holds-it route. The routing key is the target's,
		// because the delayed exchange routes the message when the delay
		// elapses exactly as it would have routed it at once.
		if p.RoutingKey != target().RoutingKey {
			t.Errorf("the plan publishes with the routing key %q, want the target's %q", p.RoutingKey, target().RoutingKey)
		}
	}
}

// modelTooLong is the refusal rather than the truncation.
func modelTooLong(t *testing.T, k Kind) {
	st := newStrategy(t, k)
	for _, d := range []time.Duration{st.MaxDelay() + time.Nanosecond, st.MaxDelay() * 2, 1 << 62} {
		if _, err := st.Plan(request(d)); err == nil {
			t.Fatalf("a delay of %v was accepted although the strategy's ceiling is %v: truncating it would deliver the message days early", d, st.MaxDelay())
		}
	}
	if _, err := st.Plan(request(-time.Second)); err == nil {
		t.Fatal("a negative delay was accepted")
	}
}

// modelZero is the case both kinds must NOT pay for: an immediate redelivery
// is not a delayed one, and routing it through this driver's own delay
// topology would put two broker hops in front of every nack.
func modelZero(t *testing.T, k Kind) {
	st := newStrategy(t, k)
	p, err := st.Plan(request(0))
	if err != nil {
		t.Fatal(err)
	}
	if p.Declare != nil {
		t.Errorf("a delay of zero declared the queue %q", p.Declare.Name)
	}
	if p.Header != "" {
		t.Errorf("a delay of zero set the header %q", p.Header)
	}
	if p.Hops != 1 {
		t.Errorf("a delay of zero costs %d broker hops, want 1", p.Hops)
	}
	if p.Exchange != target().Exchange || p.RoutingKey != target().RoutingKey {
		t.Errorf("a delay of zero publishes to (%q, %q), want the target (%q, %q)", p.Exchange, p.RoutingKey, target().Exchange, target().RoutingKey)
	}
	if p.Granted != 0 {
		t.Errorf("a delay of zero granted %v", p.Granted)
	}
}

// modelQueues is the specification's "queue count growth" as a check rather than a
// measurement: the count a strategy reports has to be the number of distinct
// queues its own plans declare, or the benchmark is reporting a number nothing
// constrains.
func modelQueues(t *testing.T, k Kind) {
	st := newStrategy(t, k)
	reqs := make([]Request, 0, 64)
	for i := range 64 {
		// Delays spread over three decades, so that a bucketing strategy
		// really does collapse them and an exact one really does not need
		// to.
		reqs = append(reqs, request(time.Duration(i+1)*137*time.Millisecond))
	}
	claimed := st.Queues(reqs)
	declared := map[string]struct{}{}
	for _, req := range reqs {
		p, err := st.Plan(req)
		if err != nil {
			t.Fatalf("delay %v refused: %v", req.Delay, err)
		}
		if p.Declare != nil {
			declared[p.Declare.Name] = struct{}{}
		}
	}
	if claimed != len(declared) {
		t.Fatalf("Queues reported %d and the plans declared %d distinct queues: the metric the criterion is read on does not follow from the plans",
			claimed, len(declared))
	}
	// The two kinds have to differ here, or the family has two names for one
	// behaviour.
	switch k.Resolve() {
	case Plugin:
		if claimed != 0 {
			t.Errorf("the plugin strategy needs %d delay queues, want none: the exchange holds the messages", claimed)
		}
	case TTLDLX:
		if claimed < 2 {
			t.Errorf("the TTL strategy needs %d delay queues for 64 delays spread over three decades: it is not bucketing per delay at all", claimed)
		}
		if claimed > len(DefaultLadder()) {
			t.Errorf("the TTL strategy needs %d delay queues, more than the %d buckets its ladder has", claimed, len(DefaultLadder()))
		}
	case Auto, kindCount:
		// Unreachable: Resolve never answers Auto and the sentinel is not a
		// kind. Naming both is what makes the switch total, so a new kind
		// does not land in a silent default.
		t.Fatalf("Resolve answered %v", k.Resolve())
	}
}

// modelConsistent checks the plan's internal arithmetic: the queue's TTL is
// the delay the plan says it granted, and the header's value is too.
//
// It is separate from modelNeverEarly because the two can disagree: a plan
// that granted the right delay and declared a queue with a different TTL would
// satisfy every other subtest and hold the message for the wrong length of
// time.
func modelConsistent(t *testing.T, k Kind) {
	st := newStrategy(t, k)
	for _, d := range modelDelays(st) {
		if d <= 0 || d > st.MaxDelay() {
			continue
		}
		p, err := st.Plan(request(d))
		if err != nil {
			t.Fatalf("delay %v refused: %v", d, err)
		}
		want := millis(p.Granted)
		switch {
		case p.Declare != nil:
			if p.Declare.TTLMillis != want {
				t.Fatalf("delay %v: the plan granted %v but declared a queue with an x-message-ttl of %d ms", d, p.Granted, p.Declare.TTLMillis)
			}
			if p.Hops != 2 {
				t.Fatalf("delay %v: a park-and-dead-letter plan reports %d broker hops, want 2", d, p.Hops)
			}
		case p.Header != "":
			if p.HeaderMillis != want {
				t.Fatalf("delay %v: the plan granted %v but set %s to %d ms", d, p.Granted, p.Header, p.HeaderMillis)
			}
			if p.Header != DelayHeader {
				t.Fatalf("delay %v: the plan sets the header %q, want %q", d, p.Header, DelayHeader)
			}
			if p.Hops != 1 {
				t.Fatalf("delay %v: a broker-holds-it plan reports %d broker hops, want 1", d, p.Hops)
			}
		default:
			t.Fatalf("delay %v produced a plan that neither declares a queue nor sets a header, so nothing delays the message: %+v", d, p)
		}
	}
}
