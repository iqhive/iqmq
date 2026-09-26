package kafkago_test

import (
	"strings"
	"testing"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit/nack"
	franz "github.com/iqhive/iqmq/drivers/kafka"
	kq "github.com/iqhive/iqmq/drivers/kafka/kafkago"
)

// TestCapabilitiesConsiderEveryFeature is the difference between a feature the
// driver decided it does not have and one nobody looked at.
//
// An unmentioned feature renders as unsupported, which is the safe answer and
// is also indistinguishable from an oversight. The driver's table states all
// of the inventory explicitly and its construction refuses a table that
// does not, so this test is what fails when the inventory grows and nobody
// revisited the driver.
func TestCapabilitiesConsiderEveryFeature(t *testing.T) {
	c, _ := openClient(t)
	caps := c.Capabilities()
	for f := range mq.FeatureCount {
		feature := mq.Feature(f)
		s := caps.Support(feature)
		if !s.Level.Valid() {
			t.Errorf("%v is advertised at a level outside the five", feature)
		}
		switch s.Level {
		case mq.Conditional, mq.EmulatedVolatile, mq.EmulatedDurable:
			if s.Note == "" {
				t.Errorf("%v is advertised as %v with no note: the specification requires one", feature, s.Level)
			}
		case mq.Native, mq.Unsupported:
		}
	}
}

// TestCapabilitiesMatchTheBroker holds the table to what Kafka is: a durable
// partitioned log with cumulative acknowledgement, and nothing else.
func TestCapabilitiesMatchTheBroker(t *testing.T) {
	c, _ := openClient(t)
	caps := c.Capabilities()
	for _, f := range []mq.Feature{
		mq.FeatureDurableStorage,
		mq.FeatureAtLeastOnce,
		mq.FeatureExplicitAck,
		mq.FeatureConsumerGroups,
		mq.FeatureDurableSubscriptions,
		mq.FeatureFanOut,
		mq.FeatureReplay,
		mq.FeatureSeekByTime,
		mq.FeatureSeekBySequence,
		mq.FeatureNativeHeaders,
		mq.FeatureBinaryHeaders,
		mq.FeatureDuplicateHeaders,
		mq.FeatureBatchPublish,
		mq.FeatureBatchReceive,
		mq.FeatureBatchSettle,
		mq.FeatureDeadLetter,
		mq.FeatureNack,
		mq.FeatureOrderedByKey,
		mq.FeatureOrderedByPartition,
		mq.FeaturePublisherConfirm,
		mq.FeatureDurableConfirm,
		mq.FeatureAdmin,
		mq.FeatureReceipts,
	} {
		if !caps.Has(f) {
			t.Errorf("%v is not advertised and Kafka has it", f)
		}
	}
	for _, f := range []mq.Feature{
		mq.FeatureAtMostOnce,
		mq.FeatureExactlyOnceDelivery,
		mq.FeatureDeduplication,
		mq.FeatureScheduledPublish,
		mq.FeatureMessageTTL,
		mq.FeaturePriority,
		mq.FeatureAtomicBatchPublish,
		mq.FeatureExtendLease,
		mq.FeatureWildcardSubscribe,
		mq.FeatureServerFilter,
		mq.FeatureRequestReply,
		mq.FeatureNativeTracing,
	} {
		if caps.Has(f) {
			t.Errorf("%v is advertised and Kafka does not have it", f)
		}
	}
	// Global order is the conditional one: it holds for a single-partition
	// topic and for no other, which the driver cannot know.
	if s := caps.Support(mq.FeatureOrderedGlobally); s.Level != mq.Conditional {
		t.Errorf("OrderedGlobally is %v, want conditional on a single-partition topic", s.Level)
	}
}

// TestCapabilitiesWithdrawnFromFranzGo is the heart of the capability
// half: the claims the franz-go driver makes and this one cannot.
//
// It compares the two tables directly rather than asserting three constants,
// because what matters is the DIFFERENCE — a caller porting between the two
// drivers needs to know exactly which guarantees it loses, and a test that
// only said "kafkago does not claim idempotent publication" would pass just as
// well on a day when the franz-go driver stopped claiming it either.
func TestCapabilitiesWithdrawnFromFranzGo(t *testing.T) {
	topic := newTopic("caps")
	cl := cluster(t, 1, []string{topic})
	goCaps := openOn(t, cl).Capabilities()
	franzClient, err := franz.Open(t.Context(), franz.Config{Brokers: cl.ListenAddrs(), Linger: -1})
	if err != nil {
		t.Fatalf("opening the franz-go driver: %v", err)
	}
	t.Cleanup(func() { _ = franzClient.Close() })
	franzCaps := franzClient.Capabilities()

	// The three withdrawals, each with the note that has to say why.
	for _, f := range []mq.Feature{mq.FeatureIdempotentPublish, mq.FeatureTransactions} {
		if goCaps.Has(f) {
			t.Errorf("kafkago advertises %v and kafka-go's writer sends no producer id and has no transactional id", f)
		}
		if s := goCaps.Support(f); s.Note == "" {
			t.Errorf("kafkago withdraws %v with no note: a caller porting from the franz-go driver has to be told why", f)
		}
	}
	// IdempotentPublish is the one the franz-go driver claims NATIVELY, and
	// the asymmetry is the finding.
	if s := franzCaps.Support(mq.FeatureIdempotentPublish); s.Level != mq.Native {
		t.Fatalf("the franz-go driver advertises IdempotentPublish as %v; this test compares the two and there is nothing to compare", s.Level)
	}
	// Everything else must be the SAME LEVEL under both drivers. A divergence
	// that is not one of the three withdrawals is either a defect in this
	// driver or a claim the driver page has to explain, and either way it must
	// not land unnoticed.
	withdrawn := map[mq.Feature]bool{
		mq.FeatureIdempotentPublish: true,
		mq.FeatureTransactions:      true,
	}
	for f := range mq.FeatureCount {
		feature := mq.Feature(f)
		if withdrawn[feature] {
			continue
		}
		g, fr := goCaps.Support(feature), franzCaps.Support(feature)
		if g.Level != fr.Level {
			t.Errorf("%v: kafkago advertises %v and the franz-go driver %v; the only intended differences are %v",
				feature, g.Level, fr.Level, sortedFeatures(withdrawn))
		}
	}
}

// sortedFeatures renders a feature set for a failure message.
func sortedFeatures(set map[mq.Feature]bool) string {
	names := make([]string, 0, len(set))
	for f := range set {
		names = append(names, f.String())
	}
	return strings.Join(names, ", ")
}

// TestCapabilitiesFollowTheNackStrategy is the rule that a claim which
// depends on the configuration is stated per client and not per call.
//
// Delayed redelivery is the case: only block-partition and a custom strategy
// hold a record and redeliver it after a delay, so a client configured with
// retry topics or stop-the-consumer does not have the feature and must not
// claim it.
func TestCapabilitiesFollowTheNackStrategy(t *testing.T) {
	topic := newTopic("caps-nack")
	cl := cluster(t, 1, []string{topic})
	for _, tc := range []struct {
		kind nack.Kind
		want bool
	}{
		{nack.Auto, true},
		{nack.BlockPartition, true},
		{nack.RetryTopic, false},
		{nack.Stop, false},
	} {
		t.Run(tc.kind.String(), func(t *testing.T) {
			c := openOn(t, cl, func(cfg *kq.Config) {
				cfg.Nack = tc.kind
				if tc.kind == nack.RetryTopic {
					cfg.NackConfig.Retries = 1
				}
			})
			if got := c.Capabilities().Has(mq.FeatureDelayedRedelivery); got != tc.want {
				t.Errorf("with nack=%v, DelayedRedelivery is advertised %v, want %v", tc.kind, got, tc.want)
			}
			// Nack itself is offered whatever the strategy: a negative
			// settlement always does something.
			if !c.Capabilities().Has(mq.FeatureNack) {
				t.Errorf("with nack=%v, Nack is not advertised at all", tc.kind)
			}
		})
	}
}

// TestCapabilityLimitsAreOnlyWhatTheClusterSaid is the rule that an
// unadvertised limit is unknown and never an overloaded zero.
func TestCapabilityLimitsAreOnlyWhatTheClusterSaid(t *testing.T) {
	c, _ := openClient(t)
	lim := c.Capabilities().Limits()
	if !lim.MaxMessageBytes.Known {
		t.Error("MaxMessageBytes is unknown; the driver reads message.max.bytes at open through DescribeConfigs")
	}
	if lim.MaxMessageBytes.Known && lim.MaxMessageBytes.Value <= 0 {
		t.Errorf("MaxMessageBytes is known and %d", lim.MaxMessageBytes.Value)
	}
	// Every limit Kafka does not bound on its own, by the key a caller asks
	// for it under: Limit is a value AND a "did the driver say", and
	// reading an unadvertised limit as zero is what the second field exists to
	// stop.
	for _, key := range []mq.LimitKey{
		mq.LimitMaxHeaderCount,
		mq.LimitMaxHeaderBytes,
		mq.LimitMaxBatchMessages,
		mq.LimitMaxDelay,
		mq.LimitMaxLease,
	} {
		if l := c.Capabilities().Limit(key); l.Known {
			t.Errorf("%v is advertised as %d and Kafka bounds none of them on its own", key, l.Value)
		}
	}
	// The two it does bound are the message size and, by the same figure, the
	// batch: one produce request carries many records and the broker bounds
	// both by message.max.bytes.
	if !lim.MaxBatchBytes.Known || lim.MaxBatchBytes.Value != lim.MaxMessageBytes.Value {
		t.Errorf("MaxBatchBytes is %+v and MaxMessageBytes is %+v; the broker bounds both by message.max.bytes", lim.MaxBatchBytes, lim.MaxMessageBytes)
	}
}

// TestDriverInfoNamesTheClient is the driver says which library it is,
// and it says the same broker name as the other Kafka driver so that the two
// reports can be compared.
func TestDriverInfoNamesTheClient(t *testing.T) {
	c, _ := openClient(t)
	info := c.Info()
	if info.Name != "kafka" {
		t.Errorf("DriverInfo.Name = %q, want %q: both Kafka drivers name the broker, and the profile is what tells them apart", info.Name, "kafka")
	}
	if info.Profile != "kafkago" {
		t.Errorf("DriverInfo.Profile = %q, want %q", info.Profile, "kafkago")
	}
	if info.ClientName != "github.com/segmentio/kafka-go" {
		t.Errorf("DriverInfo.ClientName = %q", info.ClientName)
	}
	if !strings.HasPrefix(info.ClientVersion, "v0.4.") {
		t.Errorf("DriverInfo.ClientVersion = %q, want the pinned kafka-go version", info.ClientVersion)
	}
	// BrokerVersion is deliberately empty: Kafka reports supported API
	// versions rather than a version string, so the specification makes the honest answer
	// "the driver did not say".
	if info.BrokerVersion != "" {
		t.Errorf("DriverInfo.BrokerVersion = %q; Kafka does not report one", info.BrokerVersion)
	}
}
