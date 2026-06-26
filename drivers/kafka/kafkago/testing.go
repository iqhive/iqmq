package kafkago

import (
	"context"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	kgo "github.com/segmentio/kafka-go"

	mq "bitbucket.org/iqhive/iqmq"
	"bitbucket.org/iqhive/iqmq/testkit"
)

// ConformanceHarness is what this driver hands the conformance kit.
//
// It is exported, and in a non-test file, because two different modules run
// it: this module's own env-gated test against an in-process cluster, and
// The conformance suite against the pinned container, which is
// the evidence the generated capability matrix is built from. A harness
// duplicated in two places would drift, and the half that drifted would be the
// one nobody ran.
//
// Driver and Profile together decide the report's identity, and they are
// chosen so that the evidence for the two Kafka drivers can be COMPARED:
// both say `kafka`, the profiles are `franz-go` and `kafkago`, so the reports
// are kafka-franz-go.json and kafka-kafkago.json and the generated matrix puts
// them in adjacent columns. A scenario that passes for one and not the other
// is then visible at a glance, which is the whole point.
func ConformanceHarness(cfg Config, ev testkit.Evidence, broker testkit.Broker) testkit.Harness {
	var seq atomic.Uint64
	nonce := strconv.FormatInt(time.Now().UnixNano(), 36)
	return testkit.Harness{
		Driver:   DriverName,
		Profile:  ProfileName,
		Evidence: ev,
		Broker:   broker,
		Client:   testkit.ClientInfo{Module: clientModule, Version: clientVersion},
		Timeout:  45 * time.Second,
		// The specification asks for 100,000 interleaved messages. Kafka can carry that
		// easily, and the scenario publishes them one at a time through the
		// portable API with acks=all — which is a round trip per message
		// against a single-broker container. The figure is lowered so that
		// one scenario stays inside the suite's per-scenario timeout, and the
		// driver page says so, which is what the specification asks of a driver that
		// lowers it. It is the SAME figure the franz-go driver uses, because
		// a fidelity scenario measured at two sizes would not be comparable
		// between the two profiles.
		Fidelity: 20_000,
		TopicName: func(purpose string) string {
			return "iqmqkgo-" + nonce + "-" + strconv.FormatUint(seq.Add(1), 36) + "-" + sanitise(purpose)
		},
		Open: func(ctx context.Context, n testkit.Need) (*mq.Client, error) {
			if err := checkNeed(n); err != nil {
				return nil, err
			}
			c := cfg
			// A dead-letter destination is iqmq's own on this driver: the nack
			// strategy publishes to it. It is configured only when the
			// scenario asks for one, which is what
			// dlq/failed-publish-never-acks depends on — that scenario's whole
			// point is a driver that CLAIMS the feature and has nowhere to
			// route to, and the answer for it is ErrUnsupported rather than
			// a destroyed message.
			if n.DeadLetterTopic != "" {
				c.NackConfig.DeadLetterTopic = n.DeadLetterTopic
			}
			if n.MaxAttempts > 0 {
				c.NackConfig.MaxAttempts = n.MaxAttempts
			}
			if n.Partitions > 0 {
				// checkNeed bounds it at 64.
				c.Partitions = int32(n.Partitions)
			}
			cl, err := Open(ctx, c)
			if err != nil {
				return nil, err
			}
			if err := provision(ctx, cl, n); err != nil {
				_ = cl.Close() //nolint:contextcheck // the specification gives Close no context
				return nil, err
			}
			return cl, nil
		},
		// A pattern subscription in the syntax this driver would use if it
		// supported one. It does not — kafka-go's consumer group takes a topic
		// LIST and has no pattern surface at all — so the driver refuses it
		// with ErrUnsupported, which is what proves the negative direction of
		// the FeatureWildcardSubscribe claim. Without a Wildcard function the
		// kit skips every wildcard scenario and the claim stays unverified
		// , which is why one is supplied for a feature the
		// driver does not have.
		Wildcard: func(topic string) string {
			if i := strings.LastIndexByte(topic, '-'); i > 0 {
				return "^" + topic[:i] + "-.*$"
			}
			return "^" + topic + ".*$"
		},
		// Quiesce commits every open source's watermark synchronously, which
		// is what closes the at-least-once window at a point the kit chooses
		// rather than at the commit ticker's convenience.
		Quiesce: func(ctx context.Context, c *mq.Client) error {
			b, ok := backendOf(c)
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
// created, which is why testkit.Need is a parameter of Open rather than a hook
// called afterwards : a scenario that asks for four
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
	return nil
}

// backendOf recovers the driver's backend from a client, for the two harness
// functions that need the driver's own surface.
//
// It goes through Native rather than through a field, because
// [bitbucket.org/iqhive/iqmq.Client] deliberately exposes the NATIVE client and
// not the backend. The backend keeps a registry of the writers it built, which
// is the only route back.
func backendOf(c *mq.Client) (*Backend, bool) {
	w, ok := c.Native().(*kgo.Writer)
	if !ok {
		return nil, false
	}
	return registry.Load(w)
}

// registry maps a native writer back to the backend that owns it, so that
// [ConformanceHarness]'s Quiesce can reach [Backend.Commit] from a
// *bitbucket.org/iqhive/iqmq.Client.
//
// It is the smallest thing that works and it is NOT a global registry of
// clients: entries are added when this package opens a backend and removed
// when it closes, so a process that opened and closed a client holds nothing.
// The alternative — a route from mq.Client back to the SPI backend — is the
// core's to add, and is recorded in the maintenance notes.
var registry backendRegistry

// backendRegistry is a small concurrent map from native writer to backend.
type backendRegistry struct {
	m atomic.Pointer[map[*kgo.Writer]*Backend]
}

// Store records the pair.
func (r *backendRegistry) Store(native *kgo.Writer, b *Backend) {
	for {
		old := r.m.Load()
		next := make(map[*kgo.Writer]*Backend, 4)
		if old != nil {
			for k, v := range *old {
				next[k] = v
			}
		}
		next[native] = b
		if r.m.CompareAndSwap(old, &next) {
			return
		}
	}
}

// Delete forgets the pair.
func (r *backendRegistry) Delete(native *kgo.Writer) {
	for {
		old := r.m.Load()
		if old == nil {
			return
		}
		next := make(map[*kgo.Writer]*Backend, len(*old))
		for k, v := range *old {
			if k != native {
				next[k] = v
			}
		}
		if r.m.CompareAndSwap(old, &next) {
			return
		}
	}
}

// Load returns the backend that owns a native writer.
func (r *backendRegistry) Load(native *kgo.Writer) (*Backend, bool) {
	old := r.m.Load()
	if old == nil {
		return nil, false
	}
	b, ok := (*old)[native]
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
// the settlement — which is where the negative probe looks. The franz-go
// driver found that refusing the Need instead skipped seven claims it makes or
// denies , and the same reasoning holds here.
func checkNeed(n testkit.Need) error {
	if n.Partitions > 64 {
		// Not a semantic refusal: a conformance topic with hundreds of
		// partitions costs the broker more than the scenario is worth — and on
		// this driver it also costs one goroutine and one Reader per partition
		// per member — and no scenario asks for one.
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

func (e *needError) Error() string { return "iqmq/kafkago: Kafka has no " + e.what }

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
// It is deliberately the franz-go driver's list, unchanged, and that is a
// claim rather than a copy: the two drivers skip the same two scenarios for
// the same reason, so every OTHER difference between the two reports is a
// difference in what the client can do rather than in what the harness chose
// not to run. A skip added here that franz-go does not have would make the
// two reports incomparable, which is the one thing the specification needs them to
// be.
func conformanceSkips() map[string]string {
	return map[string]string{
		"lease/expiry-redelivery": "Kafka grants no per-record lease: a delivery is not on loan for a deadline, and an unacknowledged record comes back when the member's assignment changes or the group reopens rather than when a timer expires ",
		"lease/loss":              "the same: there is no lease to lose. The equivalent — a generation ending with deliveries in flight — is proved by this driver's own TestCriticalRebalanceWithDeliveriesInFlight and TestCriticalRevokedDeliveryReportsLeaseLost, which need control over the group the portable API does not have",
	}
}
