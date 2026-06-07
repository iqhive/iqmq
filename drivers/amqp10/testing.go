package amqp10

import (
	"context"
	"strconv"
	"strings"
	"time"

	mq "bitbucket.org/iqhive/iqmq"
	"bitbucket.org/iqhive/iqmq/driverkit"
	"bitbucket.org/iqhive/iqmq/testkit"
)

// ConformanceHarness is what a profile built on this core hands the
// conformance kit.
//
// It is exported, and in a non-test file, for the reason drivers/sqs's is: two
// different modules run it. A profile's own env-gated test runs it against an
// endpoint named by an environment variable, and `conformance/suites` runs it
// against the pinned container, which is the evidence the generated capability
// matrix is built from. A harness duplicated in two places would drift, and the
// half that drifted would be the one nobody ran.
//
// What it encodes is the part of a conformance run that belongs to the CORE:
// which of a scenario's topology Needs AMQP 1.0 can express at all, how a
// scenario's purpose becomes a topic, and the scenarios this protocol documents
// as inapplicable. Everything broker-specific comes in through the profile,
// which is why this takes one.
func ConformanceHarness(cfg Config, profile Profile, ev testkit.Evidence, broker testkit.Broker) testkit.Harness {
	nonce := strconv.FormatInt(time.Now().UnixNano(), 36)
	seq := 0
	return testkit.Harness{
		Driver:   profile.Driver,
		Profile:  profile.Name,
		Evidence: ev,
		Broker:   broker,
		Client:   testkit.ClientInfo{Module: ClientModule, Version: ClientVersion},
		Timeout:  40 * time.Second,
		// The specification asks for 100,000 interleaved messages. Every publication here
		// is a transfer and a disposition — a round trip the broker commits
		// before it answers — so the figure is lowered to keep one scenario
		// inside the suite's per-scenario timeout. A profile's driver page says
		// so, which is what the specification asks of a driver that lowers it.
		Fidelity: 5_000,
		TopicName: func(purpose string) string {
			seq++
			return "iqmq-" + nonce + "-" + strconv.Itoa(seq) + "-" + sanitise(purpose)
		},
		Open: func(ctx context.Context, n testkit.Need) (*mq.Client, error) {
			if err := checkNeed(n); err != nil {
				return nil, err
			}
			return Open(ctx, profile, cfg)
		},
		// Quiesce is a connection liveness check, which is the strongest
		// "everything before this has been processed" this protocol offers:
		// a confirmed publication has already been acted on by the time its
		// disposition arrives, so there is nothing else to wait for.
		Quiesce: func(ctx context.Context, c *mq.Client) error { return c.Ping(ctx) },
		Skip:    conformanceSkips(),
	}
}

// sanitise turns a scenario's purpose into characters every broker's address
// grammar accepts: letters, digits and hyphens.
func sanitise(purpose string) string {
	return strings.Map(func(r rune) rune {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '-':
			return r
		default:
			return '-'
		}
	}, purpose)
}

// checkNeed answers whether this protocol can build the topology a scenario
// asks for.
//
// It refuses almost nothing, and that is deliberate: the specification recorded that the
// obvious reading of Need — "refuse what this broker cannot build" — skipped
// eleven scenarios, ten of them the NEGATIVE direction of a claim the driver
// makes, and a skipped negative leaves the claim unverified. The rule that works
// is narrower: refuse a Need only when accepting it would make a scenario prove
// something FALSE.
//
// A priority topology, for instance, is accepted: the AMQP message header has a
// priority field and what the broker does with it is the broker's business, so
// the scenario that runs is what sees the answer.
func checkNeed(n testkit.Need) error {
	if n.Partitions > 1 {
		// The one refusal. A scenario that asks for several partitions is
		// asking for a topology whose absence would make it pass for the wrong
		// reason: everything would arrive in order because there is only ever
		// one lane.
		return unsupportedNeed("partitions; an AMQP 1.0 address is the only lane there is")
	}
	if n.Retain {
		// A retained topic is what replay and the seek positions need, and a
		// receiving link reads what is at an address rather than a history.
		// Accepting the Need would make a replay scenario read an empty
		// address and call it a pass.
		return unsupportedNeed("a retained topic; a receiving link attaches to an address and reads what is there, so there is no history to replay")
	}
	return nil
}

// unsupportedNeed is the refusal the kit turns into a skip with a reason.
func unsupportedNeed(what string) error { return &needError{what: what} }

// needError names the topology this protocol cannot build. It wraps
// ErrUnsupported so that the kit's errors.Is finds the answer, and carries
// the reason so that the skip in the report says what was missing.
type needError struct{ what string }

// Error renders the refusal.
func (e *needError) Error() string { return "iqmq/amqp10: AMQP 1.0 has no " + e.what }

// Unwrap reaches the sentinel.
func (e *needError) Unwrap() error { return mq.ErrUnsupported }

// conformanceSkips names the scenarios this protocol documents as inapplicable,
// with the reason, which the report publishes.
//
// A skip is never a pass: the claim a skipped scenario would have proved stays
// unverified in the generated matrix, which is the only honest answer for
// something nobody checked. Each entry names something the PROTOCOL does not
// have, which is why they belong to the core rather than to a profile — a
// profile may add its own.
func conformanceSkips() map[string]string {
	return map[string]string{
		"start/positions": "a receiving link attaches to an address and reads what is there. AMQP 1.0 has no committed position and no retained history, so StartLatest and StartEarliest are the same place and a consumer opened after a publication still receives it; the driver refuses a time or a sequence rather than pretending to one",
	}
}

// The exported test accessors.
//
// Every one of them reports something the portable API cannot show, and each is
// exported because this module's own critical tests live in another package —
// which is deliberate: a test that reached into unexported state would be
// testing the implementation, and a test that could not reach these at all
// could not tell a driver that never entered the case from one that handled it
// (the reason drivers/sqs exports the same shape).

// MapForTest maps a native error through a profile rules.
//
// It is exported because the table is a set of predicates over go-amqp's own
// error types and the protocol's condition symbols, and the only way to hold
// every row to its code, its retryability and its ambiguity is to feed the
// mapper the value. Driving each row through a real broker would need a fault
// per row and would still not reach the rows whose operation nothing in the
// suite performs.
func MapForTest(b *Backend, op string, err error) error {
	return b.errs.Map(op, "t", "s", err)
}

// MapNetForTest is [MapForTest] for a failure whose request may already have
// left the socket, which is the distinction the specification asks a driver to make and the
// one an ambiguity test is about.
func MapNetForTest(b *Backend, op string, err error, sentAlready bool) error {
	return b.errs.MapNet(op, "t", "s", err, sentAlready)
}

// MapperForTest is the mapping a sink or a source would use, built without
// opening either.
//
// The mapping is pure, so it can be tested on its own — which is what makes
// FuzzAMQPMapping possible with no connection at all.
func MapperForTest(p Profile, mode mq.MetadataMode, caps mq.Capabilities, durable bool) (*Mapper, error) {
	return NewMapper(p, mode, caps, durable)
}

// CapabilitiesForTest builds a profile's capability table without opening a
// backend, so that a golden test and an over-claim test need no peer.
func CapabilitiesForTest(p Profile, f Facts) (mq.Capabilities, error) { return capabilities(p, f) }

// MissingFeaturesForTest is the features a profile's table never mentions, in
// inventory order.
//
// It is [bitbucket.org/iqhive/iqmq/driverkit.CapabilityBuilder.Missing] through the
// one door a test can reach, and it exists so that a driver's own test can
// assert the whole inventory was considered — which is the difference
// between a feature this driver decided it does not have and one nobody looked
// at.
func MissingFeaturesForTest(p Profile, f Facts) ([]mq.Feature, error) {
	b, err := capabilityBuilder(p, f)
	if err != nil {
		return nil, err
	}
	return b.Missing(), nil
}

// SinksForTest is every sink a backend currently has open.
func SinksForTest(b *Backend) []*Sink {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make([]*Sink, 0, len(b.sinks))
	for s := range b.sinks {
		out = append(out, s)
	}
	return out
}

// SourcesForTest is every source a backend currently has open.
func SourcesForTest(b *Backend) []*Source {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make([]*Source, 0, len(b.srcs))
	for s := range b.srcs {
		out = append(out, s)
	}
	return out
}

// WarmForTest attaches every open source's receiving link and issues its
// credit, without fetching anything.
//
// It is what a conformance harness's `Quiesce` needs and nothing else can give
// it. A receiving link is attached by the first Fetch (links.go says why), so
// until then the peer has been told nothing and has sent nothing — and four of the probes are about what the BROKER does with the messages it still
// HOLDS: the priority order, a TTL expiry, a de-duplication window, and whether
// one Receive can return more than one delivery. Every one of them publishes,
// then quiesces, then receives once, and every one of them reads the first
// Fetch's single transfer instead of the queue's contents unless the link was
// attached first.
//
// It is in this file rather than on [Source] because it is a test seam and not
// an API: a caller who wants the broker streaming before the first Fetch asks
// for a prefetch and fetches.
func WarmForTest(ctx context.Context, b *Backend) error {
	for _, s := range SourcesForTest(b) {
		if err := s.warm(ctx); err != nil {
			return err
		}
	}
	return nil
}

// AwaitGenerationForTest waits until the backend's connection generation has
// passed want, or the deadline passes.
//
// It is exported because a reconnection is asynchronous — the supervisor
// notices, bumps and re-dials on its own goroutine — and a test that slept
// instead would be a test that is slow on a quiet machine and flaky on a busy
// one.
func AwaitGenerationForTest(b *Backend, want uint32, budget time.Duration) bool {
	deadline := time.Now().Add(budget)
	for time.Now().Before(deadline) {
		if b.Generation() > want {
			return true
		}
		time.Sleep(time.Millisecond)
	}
	return b.Generation() > want
}

// AwaitReconnectForTest waits until the dial loop has re-established the
// connection at least want times, or the deadline passes.
//
// It is not the same question as [AwaitGenerationForTest], and the difference
// is the reconnect window. The generation is bumped the instant the connection
// dies, which is what makes a stale settlement detectable AT ONCE; the
// replacement connection arrives one backoff later. A test that waited for the
// generation and then expected a link to attach would be racing the dial.
func AwaitReconnectForTest(b *Backend, want uint64, budget time.Duration) bool {
	deadline := time.Now().Add(budget)
	for time.Now().Before(deadline) {
		if b.Stats().Reconnects >= want {
			return true
		}
		time.Sleep(time.Millisecond)
	}
	return b.Stats().Reconnects >= want
}

// compile-time proof that the accessors above are enough to build a backend
// from a test: the ownership value a test hands BackendWithClient is the kit's.
var _ = driverkit.ClientBorrowed
