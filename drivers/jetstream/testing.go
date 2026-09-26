package jetstream

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
// The conformance suite against the pinned container, which is
// the evidence the generated capability matrix is built from. A harness
// duplicated in two places would drift, and the half that drifted would be the
// one nobody ran.
//
// The harness encodes the two things only the driver knows: which of a
// scenario's topology Needs this broker can build, and which subjects the run's
// stream captures. The second is load-bearing — the specification forbids creating topology
// behind a caller's back, so a subject no stream holds is a refusal and not a
// silent success, and the scenario that checks exactly that
// (`topology/no-hidden-creation`) passes because the harness names its
// "never-provisioned" topic OUTSIDE the run's tree.
func ConformanceHarness(cfg Config, ev testkit.Evidence, broker testkit.Broker) testkit.Harness {
	return ConformanceHarnessWith(cfg, ev, broker, Open)
}

// ConformanceHarnessWith is [ConformanceHarness] with each scenario's client
// opened by open rather than by this driver's own [Open].
//
// It exists for github.com/iqhive/iqmq/drivers/natsunified, the facade over
// NATS Core and this driver. The evidence has to be produced through the
// entry point a caller really uses, and a facade whose conformance run went
// straight to this driver's Open would prove nothing about the facade — so the
// facade needs to substitute its own Open while keeping everything else this
// harness knows, which on this broker is a great deal: the run's stream, the
// subject subtree per scenario, the deliberately unprovisioned name, the
// Need-to-stream-and-consumer translation, the lowered fidelity and the
// documented skips. A facade that copied [conformanceConfig] instead would hold
// a second, unexercised copy of the only code in this driver that decides
// whether half the suite can run at all.
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
	root := "iqmqjs." + nonce
	stream := "IQMQJS_" + strings.ToUpper(nonce)
	return testkit.Harness{
		Driver: DriverName,
		// No profile. Harness.Profile is the broker VARIANT, and ADR-010 row
		// The specification gave each NATS profile a module of its own, so the driver name
		// already is the variant: "jetstream/jetstream" would name one thing
		// twice, in the report key, the capability page and the matrix row.
		Evidence: ev,
		Broker:   broker,
		Client:   testkit.ClientInfo{Module: clientModule, Version: natsVersion},
		Timeout:  40 * time.Second,
		// The specification asks for 100,000 interleaved messages. Every publication here
		// is a synchronous PubAck — a round trip that the stream's storage
		// commits before it answers — so the figure is lowered to keep one
		// scenario inside the suite's per-scenario timeout. The driver page
		// says so, which is what the specification asks of a driver that lowers it.
		Fidelity: 10_000,
		TopicName: func(purpose string) string {
			if purpose == unprovisionedPurpose {
				// The one name the run's stream deliberately does not
				// capture. The rule is about topology a driver creates
				// behind the caller's back, and the honest way to check it
				// is a subject nobody provisioned — which on this broker
				// means a subject outside the stream's own list. Naming it
				// here is the harness saying which names are provisioned,
				// which is exactly what a harness is for.
				return "iqmqjs-unprovisioned." + nonce + "." + strconv.FormatUint(seq.Add(1), 36)
			}
			// Every scenario's topic gets a SUBTREE of its own, not a
			// leaf beside its neighbours. Harness.Wildcard then has a
			// pattern that matches this scenario's topic and nothing
			// else — which is what the filtering half of the wildcard
			// claim needs, and what keeps two scenarios' pattern
			// subscriptions from resolving to one durable consumer.
			return root + "." + strconv.FormatUint(seq.Add(1), 36) + "." + sanitise(purpose)
		},
		Open: func(ctx context.Context, n testkit.Need) (*mq.Client, error) {
			if err := checkNeed(n); err != nil {
				return nil, err
			}
			return open(ctx, conformanceConfig(cfg, n, stream, root))
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
		// Quiesce is a round trip to the server, which is the strongest
		// "everything before this has been processed" this broker offers: the
		// stream commits a publication before it answers it, so a flush
		// behind the publications means they are all stored.
		Quiesce: func(ctx context.Context, c *mq.Client) error { return c.Ping(ctx) },
		Skip:    conformanceSkips(),
	}
}

// unprovisionedPurpose is the purpose the kit asks a topic name for when it
// wants one nobody has created (`topology/no-hidden-creation`).
//
// It is a coupling to a string in the kit and it is stated here rather than
// hidden: if the kit renames it, this driver's `topology/no-hidden-creation`
// goes from a pass to a skip, which is visible in the report rather than silent.
const unprovisionedPurpose = "never-provisioned"

// natsVersion is the client library's version, held here so that the harness
// does not import the client for one constant.
const natsVersion = "v1.52.0"

// clientModule is that library's module path, held beside the version so the
// pair has ONE source in this package.
const clientModule = "github.com/nats-io/nats.go"

// conformanceConfig is the configuration one scenario's client is opened with.
//
// The topology a scenario asks for becomes stream and consumer settings, which
// is the whole of what [testkit.Need] is for on this broker: the dead-letter
// subject, the lease, the attempt ceiling, the de-duplication window and the
// message-size ceiling are all things JetStream really has, and each of them is
// set from the Need rather than assumed.
func conformanceConfig(base Config, n testkit.Need, stream, root string) Config {
	cfg := base
	cfg.Stream = stream
	cfg.SubjectPrefix = ""
	cfg.ManageStream = true
	// One stream for the whole run, covering every topic the kit names. It is
	// how a real deployment uses JetStream — one stream, many subjects — and
	// it is what makes the wildcard scenarios reachable: a pattern
	// subscription needs a sibling subject to NOT receive, and a stream
	// scoped to one scenario's topic would refuse the publication that proves
	// the filtering.
	cfg.StreamSubjects = []string{root + ".>", cfg.scheduleRoot() + ".>"}
	cfg.DeadLetterTopic = n.DeadLetterTopic
	if n.AckDeadline > 0 {
		cfg.AckWait = n.AckDeadline
	} else if n.AckDeadline < 0 {
		// "A lease that never expires" has no JetStream spelling; the longest
		// one a scenario could outlive is the honest approximation, and it is
		// far longer than any scenario's timeout.
		cfg.AckWait = time.Hour
	}
	if n.MaxAttempts > 0 {
		cfg.MaxDeliver = int(n.MaxAttempts)
	}
	if n.DedupWindow > 0 {
		cfg.DuplicateWindow = n.DedupWindow
	}
	// Always set, never merely raised: a ceiling left over from the previous
	// scenario would make the next one measure the wrong number.
	cfg.MaxMsgSize = 0
	if n.MaxMessageSize > 0 {
		cfg.MaxMsgSize = int32(min(n.MaxMessageSize, 1<<30))
	}
	return cfg
}

// checkNeed answers whether this broker can build the topology a scenario asks
// for.
//
// It refuses almost nothing, and that is deliberate: the specification recorded that the
// obvious reading of Need — "refuse what this broker cannot build" — skipped
// eleven scenarios, ten of them the NEGATIVE direction of a claim the driver
// makes, and a skipped negative leaves the claim unverified. The rule that works
// is narrower: refuse a Need only when accepting it would make a scenario prove
// something FALSE.
//
// A priority topology, for instance, is accepted: JetStream has no priority, the
// publication control is refused with ErrUnsupported, and the scenario that runs
// is what sees it.
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

// needError names the topology this broker cannot build. It wraps ErrUnsupported
// so that the kit's errors.Is finds the answer, and carries the reason so
// that the skip in the report says what was missing.
type needError struct{ what string }

func (e *needError) Error() string {
	return "iqmq/jetstream: JetStream has no " + e.what
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
// It is deliberately short, and each entry is a defect in the KIT rather than a
// behaviour this driver lacks — which is why each is also recorded in
// the maintenance notes with an owner. A skip is never a pass: the claim a skipped
// scenario would have proved stays unverified in the generated matrix, which is
// the only honest answer for something nobody checked.
func conformanceSkips() map[string]string {
	return map[string]string{
		// capability/ExactlyOnceDelivery/refused was skipped here for as
		// long as a probe that PASSED was read as refuting the denial. The
		// kit no longer does that: ExactlyOnceDelivery is a guarantee, and
		// `refusedGuarantee` reports a probe that saw no counter-example as
		// a skip whose note says an absence is not observable in one run.
		// The entry is therefore gone — a skip
		// whose stated reason has been fixed keeps telling the reader the
		// scenario cannot run here. The FOLLOWUP it
		// named is still open: the kit still has no probe that separates
		// publication de-duplication from exactly-once DELIVERY.

		// The TTL probe opens its consumer BEFORE publishing, so a driver
		// that prefetches has the message in hand long before the TTL runs
		// out: what the probe then observes is a message in a client buffer
		// and not a message in a stream. The stream expiry is real and is
		// held by the driver's own TestMessageTTLExpiresAMessage, whose
		// consumer starts after the expiry.
		"capability/MessageTTL/works": "the probe opens its consumer before publishing, so a prefetching driver holds the message before its TTL expires and the probe observes a client buffer rather than the stream; drivers/jetstream's TestMessageTTLExpiresAMessage holds the real expiry with a consumer that starts afterwards (see the maintenance notes)",
	}
}
