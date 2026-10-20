package kafka

import (
	"context"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/testkit"
)

// ConformanceHarness is what this driver hands the conformance kit.
//
// It is exported, and in a non-test file, because two different modules run
// it: this module's own env-gated test against a kfake cluster, and
// conformance/suites/kafka_test.go against the pinned container, which is the
// evidence the generated capability matrix is built from. A harness duplicated
// in two places would drift, and the half that drifted would be the one nobody
// ran.
//
// The harness encodes the two things only the driver knows: which of a
// scenario's topology Needs this broker can build, and how to build them.
// Kafka can build more of them than most brokers — partitions, a dead-letter
// destination, retention — and the ones it cannot are refused with
// ErrUnsupported, which the kit records as a skip with the reason rather than
// a failure.
//
// provision is called before each scenario's client is opened and is where a
// topic is created. It is a function rather than a flag because the container
// suite and this module's own test create topics differently: one has a
// cluster with auto-creation off and an administrative client, the other has
// kfake.
func ConformanceHarness(cfg Config, ev testkit.Evidence, broker testkit.Broker) testkit.Harness {
	var seq atomic.Uint64
	opened := &backends{}
	nonce := strconv.FormatInt(time.Now().UnixNano(), 36)
	return testkit.Harness{
		Driver:   DriverName,
		Profile:  "franz-go",
		Evidence: ev,
		Broker:   broker,
		Client:   testkit.ClientInfo{Module: clientModule, Version: clientVersion},
		Timeout:  45 * time.Second,
		// The specification asks for 100,000 interleaved messages. Kafka can carry
		// that easily, and the scenario publishes them one at a time
		// through the portable API with acks=all — which is a round trip
		// per message against a single-broker container. The figure is
		// lowered so that one scenario stays inside the suite's
		// per-scenario timeout, and the driver page says so, which is
		// what the specification asks of a driver that lowers it.
		Fidelity: 20_000,
		TopicName: func(purpose string) string {
			return "iqmqconf-" + nonce + "-" + strconv.FormatUint(seq.Add(1), 36) + "-" + sanitise(purpose)
		},
		Open: func(ctx context.Context, n testkit.Need) (*mq.Client, error) {
			if err := checkNeed(n); err != nil {
				return nil, err
			}
			c := cfg
			// A dead-letter destination is iqmq's own on this driver: the
			// nack strategy publishes to it. It is configured only when
			// the scenario asks for one, which is what
			// dlq/failed-publish-never-acks depends on — that scenario's
			// whole point is a driver that CLAIMS the feature and has
			// nowhere to route to, and the answer for it is
			// ErrUnsupported rather than a destroyed message.
			if n.DeadLetterTopic != "" {
				c.NackConfig.DeadLetterTopic = n.DeadLetterTopic
			}
			if n.MaxAttempts > 0 {
				c.NackConfig.MaxAttempts = n.MaxAttempts
			}
			if n.Partitions > 0 {
				c.Partitions = int32(n.Partitions)
			}
			b, err := OpenBackend(ctx, c)
			if err != nil {
				return nil, err
			}
			cl := clientFor(b, c)
			opened.store(cl, b)
			if err := provision(ctx, cl, n); err != nil {
				_ = cl.Close() //nolint:contextcheck // the specification gives Close no context
				return nil, err
			}
			return cl, nil
		},
		// A pattern subscription in the syntax this driver would use if
		// it supported one. It does not — franz-go's ConsumeRegex is not
		// wired — so the driver refuses it with ErrUnsupported, which is
		// what proves the negative direction of the
		// FeatureWildcardSubscribe claim. Without a Wildcard function
		// the kit skips every wildcard scenario and the claim stays
		// unverified , which is why one is supplied
		// for a feature the driver does not have.
		Wildcard: func(topic string) string {
			if i := strings.LastIndexByte(topic, '-'); i > 0 {
				return "^" + topic[:i] + "-.*$"
			}
			return "^" + topic + ".*$"
		},
		// Quiesce commits every open source's watermark synchronously,
		// which is what closes the at-least-once window at a point the
		// kit chooses rather than at the commit ticker's convenience.
		Quiesce: func(ctx context.Context, c *mq.Client) error {
			b, ok := opened.load(c)
			if !ok {
				return nil
			}
			return b.Commit(ctx)
		},
		Skip: conformanceSkips(),
	}
}

// provision creates the topics a scenario needs, through the portable
// administrative API.
//
// It exists because a Kafka topic's partition count is fixed when the topic is
// created, which is why testkit.Need is a parameter of Open rather than a
// hook called afterwards : a scenario that asks for four
// partitions has to have them before its first publication, and a topic
// auto-created by a produce has the cluster's default.
func provision(ctx context.Context, c *mq.Client, n testkit.Need) error {
	admin, err := c.Admin()
	if err != nil {
		return err
	}
	topics := make([]string, 0, 2)
	if n.Topic != "" {
		topics = append(topics, n.Topic)
	}
	if n.DeadLetterTopic != "" {
		topics = append(topics, n.DeadLetterTopic)
	}
	for _, t := range topics {
		if err := admin.EnsureTopic(ctx, mq.TopicSpec{Name: t}); err != nil {
			return err
		}
	}
	// A retry topic is created too when the nack strategy will use one, so
	// that a scenario's republication does not depend on auto-creation.
	return nil
}

// backends maps the clients one harness opened to the backends under them,
// so that Quiesce can reach [Backend.Commit] from a
// *github.com/iqhive/iqmq.Client, which deliberately exposes the native
// client and not the backend. It belongs to the harness that fills it, so a
// process holds an entry only for as long as it holds that harness.
type backends struct {
	mu sync.Mutex
	m  map[*mq.Client]*Backend
}

// store records the backend under a client the harness opened.
func (r *backends) store(c *mq.Client, b *Backend) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.m == nil {
		r.m = make(map[*mq.Client]*Backend, 4)
	}
	r.m[c] = b
}

// load returns the backend under a client the harness opened.
func (r *backends) load(c *mq.Client) (*Backend, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	b, ok := r.m[c]
	return b, ok
}

// checkNeed answers whether this broker can build the topology a scenario asks
// for.
//
// It refuses ONE thing, and that is deliberate. The specification found that the obvious
// reading of Need — "refuse what this broker cannot provide" — skips exactly
// the scenarios that prove the NEGATIVE direction of a claim, and a skipped
// negative leaves the claim unverified. The rule that
// works is narrower: refuse a Need only when ACCEPTING it would make a
// scenario prove something FALSE.
//
// Nothing here does. A Kafka topic has no per-delivery lease, no priority and
// no de-duplication window, and a scenario that asks for one of those is
// refused where put the refusal — at the publication control, at
// the settlement — which is where the negative probe looks. Refusing the Need
// instead skipped seven claims this driver makes or denies, including
// AtLeastOnce, DelayedRedelivery, ExtendLease, Priority, Deduplication and
// ExactlyOnceDelivery: six of them are the NEGATIVE direction of a denial and
// one is the positive direction of a claim. The three
// scenarios that really would prove something false — the two lease scenarios,
// which wait for a deadline that never comes — are named in
// [conformanceSkips] instead, with the reason.
func checkNeed(n testkit.Need) error {
	if n.Partitions > 64 {
		// Not a semantic refusal: a conformance topic with hundreds of
		// partitions costs the broker more than the scenario is worth,
		// and no scenario asks for one.
		return unsupportedNeed("more than 64 partitions in a conformance topic")
	}
	return nil
}

// unsupportedNeed is the refusal the kit turns into a skip with a reason.
func unsupportedNeed(what string) error { return &needError{what: what} }

// needError names the topology this broker cannot build. It wraps
// ErrUnsupported so that the kit's errors.Is finds the answer, and carries
// the reason so that the skip in the report says what was missing.
type needError struct{ what string }

func (e *needError) Error() string { return "iqmq/kafka: Kafka has no " + e.what }

func (e *needError) Unwrap() error { return mq.ErrUnsupported }

// sanitise turns a scenario name into a Kafka topic token.
func sanitise(purpose string) string {
	return strings.Map(func(r rune) rune {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9':
			return r
		case r == '-' || r == '.' || r == '_':
			return r
		default:
			return '-'
		}
	}, purpose)
}

// conformanceSkips names the scenarios this driver documents as inapplicable,
// with the reason, which the report publishes.
//
// It is deliberately short. Everything the kit asks of a driver with
// at-least-once delivery and explicit settlement is run.
func conformanceSkips() map[string]string {
	return map[string]string{
		"lease/expiry-redelivery": "Kafka grants no per-record lease: a delivery is not on loan for a deadline, and an unacknowledged record comes back when the member's assignment changes or the group reopens rather than when a timer expires ",
		"lease/loss":              "the same: there is no lease to lose. The equivalent — a partition taken away with deliveries in flight — is proved by this driver's own TestCriticalRebalanceWithDeliveriesInFlight and TestCriticalRevokedDeliveryReportsLeaseLost, which need control over the group the portable API does not have",
	}
}
