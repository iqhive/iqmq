package nats

import (
	"context"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/testkit"
)

// ConformanceHarness is what this driver hands the conformance kit.
//
// It is exported, and in a non-test file, because two different modules run it:
// this module's own env-gated test against an embedded server, and
// The conformance suite against the pinned container, which is the
// evidence the generated capability matrix is built from. A harness duplicated
// in two places would drift, and the half that drifted would be the one nobody
// ran.
//
// The harness encodes one thing only the driver knows: which of a scenario's
// topology Needs this broker can build. NATS Core can build almost none of them
// — there is no dead-letter destination, no lease, no retention, no priority
// and no de-duplication window — and a Need it cannot build is answered with
// ErrUnsupported, which the kit records as a skip with the reason rather than a
// failure. That is the designed route for "this scenario does not apply to this
// broker", and using it is what keeps the report honest: a skipped scenario
// proves nothing and its claim stays unverified.
func ConformanceHarness(cfg Config, ev testkit.Evidence, broker testkit.Broker) testkit.Harness {
	return ConformanceHarnessWith(cfg, ev, broker, Open)
}

// ConformanceHarnessWith is [ConformanceHarness] with each scenario's client
// opened by open rather than by this driver's own [Open].
//
// It exists for github.com/iqhive/iqmq/drivers/natsunified, the facade over
// this driver and JetStream. The evidence has to be produced through the
// entry point a caller really uses, and a facade whose conformance run went
// straight to this driver's Open would prove nothing about the facade — so the
// facade needs to substitute its own Open while keeping everything else this
// harness knows: the topology a Need can be built as, the subject naming, the
// wildcard syntax, the lowered fidelity and the documented skips.
//
// The seam is here rather than in [github.com/iqhive/iqmq/testkit.Harness]
// because what a facade has to replace is the CONFIGURATION step. By the time
// Harness.Open is called the scenario's Need has already been turned into this
// driver's own Config, and that translation is the part a facade has to route
// through itself. Recorded in the maintenance notes as a gap in the kit's Harness
// contract.
func ConformanceHarnessWith(cfg Config, ev testkit.Evidence, broker testkit.Broker, open func(context.Context, Config) (*mq.Client, error)) testkit.Harness {
	var seq atomic.Uint64
	nonce := strconv.FormatInt(time.Now().UnixNano(), 36)
	return testkit.Harness{
		Driver:   DriverName,
		Profile:  "core",
		Evidence: ev,
		Broker:   broker,
		Client:   testkit.ClientInfo{Module: clientModule, Version: natsVersion},
		Timeout:  30 * time.Second,
		// The specification asks for 100,000 interleaved messages. NATS Core is
		// at-most-once, so the scenario's own "no message was lost" is a
		// real check here rather than a formality — and every message is a
		// round trip through a socket, so the figure is lowered to keep one
		// scenario inside the suite's per-scenario timeout. The driver page
		// says so, which is what the specification asks of a driver that lowers it.
		Fidelity: 20_000,
		TopicName: func(purpose string) string {
			return "iqmqconf." + nonce + "." + strconv.FormatUint(seq.Add(1), 36) + "." + sanitise(purpose)
		},
		Open: func(ctx context.Context, n testkit.Need) (*mq.Client, error) {
			if err := checkNeed(n); err != nil {
				return nil, err
			}
			return open(ctx, cfg)
		},
		// A pattern that matches the scenario's topic and everything else
		// under the same root, which is what FeatureWildcardSubscribe means
		// here and what lets the kit prove the claim in both directions
		// rather than take it on trust.
		Wildcard: func(topic string) string {
			if i := strings.LastIndexByte(topic, '.'); i > 0 {
				return topic[:i] + ".*"
			}
			return ""
		},
		Skip: conformanceSkips(),
	}
}

// natsVersion is the client library's version, held here so that the harness
// does not import the client for one constant.
const natsVersion = "v1.52.0"

// clientModule is that library's module path, held beside the version so the
// pair has ONE source in this package.
const clientModule = "github.com/nats-io/nats.go"

// checkNeed answers whether this broker can build the topology a scenario asks
// for.
//
// It refuses almost nothing, and that is the opposite of what it first did. The
// obvious reading of Need is "refuse what this broker cannot build", and a NATS
// Core harness written that way refuses a dead-letter destination, a lease, a
// retained topic, a priority and a de-duplication window — which skipped
// eleven scenarios, ten of which were the NEGATIVE direction of a claim this
// driver makes. A skipped negative leaves the claim unverified, so refusing the
// Need was throwing away exactly the evidence the specification asks for: the API refuses
// each of those five with ErrUnsupported, and a scenario that runs is what sees
// it.
//
// So the rule here is narrower: refuse a Need only when accepting it would make
// a scenario prove something FALSE. A Need whose topology is simply moot on
// this broker is accepted and the refusal happens where it belongs — at the
// publication control, at the settlement, at the start position — which is
// where put it anyway.
func checkNeed(n testkit.Need) error {
	if n.Partitions > 1 {
		// The one refusal left. A scenario that asks for several partitions
		// is asking for a topology whose absence would make it pass for the
		// wrong reason: everything would arrive in order because there is
		// only ever one lane.
		return unsupportedNeed("more than one partition: a subject is the only partition there is")
	}
	return nil
}

// unsupportedNeed is the refusal the kit turns into a skip with a reason.
func unsupportedNeed(what string) error {
	return &needError{what: what}
}

// needError names the topology this broker cannot build. It wraps
// ErrUnsupported so that the kit's errors.Is finds the answer, and carries
// the reason so that the skip in the report says what was missing.
type needError struct{ what string }

func (e *needError) Error() string {
	return "iqmq/nats: NATS Core has no " + e.what
}

func (e *needError) Unwrap() error { return mq.ErrUnsupported }

// sanitise turns a scenario name into a subject token.
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

// conformanceSkips names the scenarios this driver documents as inapplicable,
// with the reason, which the report publishes.
//
// Every entry is a scenario whose SUBJECT is settlement. AckNone model
// says this driver has none: [Source.Settler] is nil and the core answers every
// Ack, Nack and Reject with ErrNotSettleable. There is no way to make these
// scenarios mean something here, and there is no Need shape that lets the kit
// discover that for itself — the kit has no notion of a driver that does not
// settle, which is the first of this increment's findings and is recorded in
// the maintenance notes.
//
// It is deliberately short. Everything else the kit asks of a driver is run,
// including every scenario that merely happens to acknowledge on its way past.
func conformanceSkips() map[string]string {
	const noSettlement = "NATS Core has no settlement of any kind (AckNone): Source.Settler is nil, so Ack, Nack and Reject all report ErrNotSettleable and this scenario has nothing to observe"
	return map[string]string{
		"settle/ack-prevents-redelivery":          noSettlement,
		"settle/ack-idempotency":                  noSettlement,
		"settle/concurrent-serialisation":         noSettlement,
		"settle/stale-rejection":                  noSettlement,
		"lease/loss":                              noSettlement,
		"lease/expiry-redelivery":                 noSettlement,
		"nack/immediate-redelivery":               noSettlement,
		"dlq/failed-publish-never-acks":           noSettlement,
		"capability/DurableSubscriptions/refused": "a subscription that keeps nothing shows as a message that never arrives, not as an error: the kit's negative direction needs a refusal it can tell from a slow broker (see the maintenance notes)",
	}
}
