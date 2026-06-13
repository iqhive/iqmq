package pulsar

import (
	"context"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	mq "bitbucket.org/iqhive/iqmq"
	"bitbucket.org/iqhive/iqmq/testkit"
)

// ConformanceRetentionMinutes is how long the conformance harness asks the
// namespace to retain an acknowledged entry.
//
// Pulsar's default retention is ZERO, under which an entry every subscription
// has acknowledged may be deleted at once — and FeatureReplay is the claim that
// rests on it. A conformance run that means to VERIFY the replay claim has to
// set a policy, which is the namespace operator's job and not the driver's; the
// harness is the operator here. It is why the claim is Conditional and why the
// condition names the namespace (see [capabilities]).
const ConformanceRetentionMinutes = 60

// ConformanceProfiles are the subscription types this driver publishes
// conformance evidence for, in the order the reports are written.
//
// Two of the four, and the choice is about what a report can PROVE rather than
// about cost. Shared is the driver's default and the table a reader of the
// matrix will meet, and it claims no ordering at all. Key_Shared is the one
// type on which the per-key claim can be proved in both directions — a
// claimed ordering needs a run that keeps it, and an unclaimed guarantee can
// only ever be left unrefuted. Exclusive and Failover admit one active consumer,
// so a suite run against them would lose the competing-consumer and fan-out
// scenarios to a refusal; their claims are held by
// TestCriticalSubscriptionModes against the in-process model instead, and the
// driver page says which ordering claim each of the four supports.
func ConformanceProfiles() []SubscriptionKind {
	return []SubscriptionKind{SubscriptionShared, SubscriptionKeyShared}
}

// ConformanceHarness is what this driver hands the conformance kit.
//
// It is exported, and in a non-test file, because two different modules run it:
// this module's own env-gated test against an endpoint named by
// IQMQ_PULSAR_ADDR, and the conformance suite against the pinned
// apachepulsar/pulsar image, which is the evidence the generated capability
// matrix is built from. A harness duplicated in two places would drift, and the
// half that drifted would be the one nobody ran.
//
// The PROFILE is the subscription type, and that is the increment's whole
// point: the ordering claims are a property of it and of nothing else, so
// two subscription types are two capability tables and two reports.
func ConformanceHarness(cfg Config, ev testkit.Evidence, broker testkit.Broker) testkit.Harness {
	var seq atomic.Uint64
	var retention sync.Once
	nonce := strconv.FormatInt(time.Now().UnixNano(), 36)
	return testkit.Harness{
		Driver:   DriverName,
		Profile:  cfg.SubscriptionType.String(),
		Evidence: ev,
		Broker:   broker,
		Client:   testkit.ClientInfo{Module: ClientModule, Version: ClientVersion},
		Timeout:  60 * time.Second,
		// The specification asks for 100,000 interleaved messages. Every publication
		// here is a Producer.Send that waits for the broker's send
		// receipt — which for a persistent topic is after the entry is
		// written to the write quorum — and the scenario publishes them
		// one at a time, so the figure is lowered to keep the scenario
		// inside the suite's timeout. docs/drivers/pulsar.md says so,
		// which is what the specification asks of a driver that lowers it.
		Fidelity: 2000,
		TopicName: func(purpose string) string {
			// A Pulsar name segment holds letters, digits and
			// `-`, `_`, `=`, `:`, `.`, so a scenario's purpose — which
			// carries slashes — is sanitised. The nonce and the
			// counter come FIRST so that two scenarios never share a
			// topic when a long purpose is truncated.
			head := "iqmqconf-" + nonce + "-" + strconv.FormatUint(seq.Add(1), 36) + "-"
			return head + truncate(sanitise(purpose), MaxNameLen-len(head))
		},
		Secrets: cfg.Secrets(),
		Open: func(ctx context.Context, n testkit.Need) (*mq.Client, error) {
			c, err := Open(ctx, shape(cfg, n))
			if err != nil {
				return nil, err
			}
			if err := provision(ctx, c, n, &retention); err != nil {
				// mq.Client.Close takes no context — the specification makes it
				// prompt and bounded rather than cancellable — so
				// there is no context to pass on here.
				_ = c.Close() //nolint:contextcheck // Client.Close has no context parameter to pass
				return nil, err
			}
			return c, nil
		},
		// Pulsar matches a regex subscription against the WHOLE topic
		// name, and PatternOf is what qualifies it; what the kit needs
		// here is only the pattern's own syntax. `.*` after the topic
		// matches the topic itself and anything under it, and matches no
		// OTHER scenario's topic because every name carries its own
		// counter.
		Wildcard: func(topic string) string { return topic + ".*" },
		Quiesce:  quiesce,
		Skip:     conformanceSkips(cfg),
		// Advance is deliberately nil. There is no way to move a real
		// broker's clock, and the driver's own lease is on the
		// configuration's Clock — which the harness leaves unset, so a
		// scenario that needs a lease to expire waits in real seconds.
		// That is why shape() gives every scenario a short lease rather
		// than the driver's default minute.
	}
}

// shape applies the parts of a scenario's Need that are CONFIGURATION on this
// driver rather than topology.
//
// Three of the settings deserve stating.
//
// Metadata is MetadataAdaptive. Pulsar's key is a protobuf string, so a binary
// key and an EMPTY key have nowhere native to go, and `publish/key-roundtrip`
// sends both; the specification makes the envelope an emulation a deployment asks for once,
// and the kit configures every handle at the zero value, so the ask has to be
// here. It is why `metadata/native-strict-rejection` is documented as
// inapplicable.
//
// The dead-letter destination comes from the Need, which is what makes
// FeatureDeadLetter a per-scenario claim: a scenario whose topology names no
// destination gets a client that does not advertise the feature, and Reject is
// then refused rather than destroying the message.
//
// BatchingMaxPublishDelay is one millisecond rather than the library's ten. A
// synchronous Send waits for its batch to close, so the default makes every
// publication in a one-at-a-time loop cost ten milliseconds — which is the
// fidelity scenario's whole budget spent on a batching delay that buys nothing
// when there is one publisher.
func shape(cfg Config, n testkit.Need) Config {
	out := cfg
	out.EnsureTopology = true
	if out.Metadata == mq.MetadataNativeStrict {
		out.Metadata = mq.MetadataAdaptive
	}
	if out.BatchingMaxPublishDelay == 0 {
		out.BatchingMaxPublishDelay = time.Millisecond
	}
	if out.AutoDiscoveryPeriod == 0 {
		out.AutoDiscoveryPeriod = time.Second
	}
	if out.NackRedeliveryDelay == 0 {
		out.NackRedeliveryDelay = 500 * time.Millisecond
	}
	switch {
	case n.AckDeadline > 0:
		out.AckTimeout = n.AckDeadline
	case n.AckDeadline < 0:
		// The kit's "leases that never expire". The honest answer for an
		// emulated lease is NO lease rather than a very long one: the
		// driver simply does not track a deadline, which is also Pulsar's
		// own behaviour.
		out.DisableAckTimeout = true
		out.AckTimeout, out.LeaseGuard = 0, 0
	default:
		// Shorter than the driver's default minute, because a scenario
		// that waits for a lease to expire waits in real seconds.
		out.AckTimeout = 10 * time.Second
	}
	if out.AckTimeout > 0 {
		out.LeaseGuard = min(DefaultLeaseGuard, out.AckTimeout/5)
	}
	if n.DeadLetterTopic != "" {
		out.DeadLetterTopic = n.DeadLetterTopic
		out.MaxAttempts = max(n.MaxAttempts, 1)
	}
	if n.MaxMessageSize > 0 {
		// The limit really is enforced and advertised, so the
		// oversized-payload scenario refuses a message just over it
		// rather than a five-mebibyte one.
		out.MaxMessageBytes = int64(n.MaxMessageSize)
	}
	if n.Partitions > 1 {
		out.Partitions = n.Partitions
	}
	return out
}

// provision creates the topic and every subscription a scenario named, before
// the scenario publishes anything — and sets the namespace's retention policy
// once.
//
// Both matter on this broker. A subscription created AFTER a publication starts
// at the end of the topic unless it asks for the beginning, so a fan-out
// scenario that published first would see its second subscription receive
// nothing through no fault of the driver. And the retention policy is what
// makes the replay claim verifiable at all.
func provision(ctx context.Context, c *mq.Client, n testkit.Need, retention *sync.Once) error {
	admin, err := c.Admin()
	if err != nil {
		// A run with no administrative endpoint provisions nothing and
		// relies on the namespace's automatic topic creation, which is
		// what a deployment without one has.
		if mq.CodeOf(err) == mq.CodeUnsupported {
			return nil
		}
		return err
	}
	var rerr error
	retention.Do(func() {
		if setter, ok := admin.(interface {
			SetNamespaceRetention(context.Context, int, int) error
		}); ok {
			rerr = setter.SetNamespaceRetention(ctx, ConformanceRetentionMinutes, 100)
		}
	})
	if rerr != nil {
		return rerr
	}
	topics := []string{n.Topic}
	if n.DeadLetterTopic != "" {
		topics = append(topics, n.DeadLetterTopic)
	}
	for _, topic := range topics {
		if err := admin.EnsureTopic(ctx, mq.TopicSpec{Name: topic}); err != nil {
			return err
		}
		subs := n.Subscriptions
		if len(subs) == 0 {
			subs = []string{n.Sub(0)}
		}
		for _, sub := range subs {
			if err := admin.EnsureSubscription(ctx, mq.SubscriptionSpec{Topic: topic, Name: sub}); err != nil {
				return err
			}
		}
	}
	return nil
}

// quiesceDelay is how long Quiesce waits.
//
// Pulsar's delayed-delivery tracker ticks at `delayedDeliveryTickTimeMillis`,
// a second by default, and a scenario that expects NOTHING to arrive has to
// outlast one dispatch cycle before concluding it. There is nothing to poll:
// the administrative API reports a subscription's backlog, and a backlog of
// zero does not mean the message has reached this consumer's socket.
const quiesceDelay = 300 * time.Millisecond

// quiesce waits until the broker has finished what it was doing.
func quiesce(ctx context.Context, _ *mq.Client) error {
	timer := time.NewTimer(quiesceDelay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// conformanceSkips names the scenarios this driver documents as inapplicable,
// with the reason, which the report publishes.
//
// Each entry names something Pulsar does not have or something the kit cannot
// observe on it, and each was added after watching the scenario fail against
// the pinned container rather than in anticipation.
func conformanceSkips(cfg Config) map[string]string {
	skips := map[string]string{
		"metadata/native-strict-rejection": "this harness configures Config.Metadata, which is the deployment's one-time \"yes, envelope on this broker\" (the specification makes it an emulation available under EmulationAll, and the SPI carries no emulation policy). Pulsar's message key is a protobuf string, so a binary key and an EMPTY key have nowhere native to go and `publish/key-roundtrip` sends both. A handle that names MetadataNativeStrict explicitly is indistinguishable from one that named nothing, so no handle in this run is in strict mode; the refusal itself is covered by the driver's own TestStrictModeRefusesBeforeAnyIO and TestRepresentableNamesTheFiveLossesPulsarImposes",
	}
	if len(cfg.Secrets()) == 0 {
		skips["secrets/redaction"] = "this harness declares no credential, because the pinned Pulsar standalone runs without authentication; the scenario needs a secret to look for and skips rather than passing vacuously"
	}
	return skips
}

// sanitise turns a scenario name into Pulsar name characters.
func sanitise(purpose string) string {
	return strings.Map(func(r rune) rune {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9':
			return r
		case r == '.', r == '_', r == '-', r == '=', r == ':':
			return r
		default:
			return '-'
		}
	}, purpose)
}

// truncate cuts a name to n characters, and to nothing when n is not positive.
func truncate(s string, n int) string {
	if n <= 0 {
		return ""
	}
	if len(s) <= n {
		return s
	}
	return s[:n]
}
