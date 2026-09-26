package kafka_test

import (
	"strings"
	"testing"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit/nack"
	kq "github.com/iqhive/iqmq/drivers/kafka"
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
		mq.FeatureAdmin,
		mq.FeatureReceipts,
		mq.FeaturePublisherConfirm,
		mq.FeatureDurableConfirm,
		mq.FeatureOrderedByKey,
		mq.FeatureOrderedByPartition,
		mq.FeatureIdempotentPublish,
		mq.FeatureNack,
	} {
		if !caps.Has(f) {
			t.Errorf("%v is not advertised and Kafka has it", f)
		}
	}
	for _, f := range []mq.Feature{
		// Kafka is at-least-once and has no exactly-once delivery.
		mq.FeatureAtMostOnce,
		mq.FeatureExactlyOnceDelivery,
		mq.FeatureDeduplication,
		// No per-message TTL, no scheduled publication, no priority.
		mq.FeatureScheduledPublish,
		mq.FeatureMessageTTL,
		mq.FeaturePriority,
		// A produce is atomic per partition and not across partitions.
		mq.FeatureAtomicBatchPublish,
		// No per-record lease, so nothing to extend.
		mq.FeatureExtendLease,
		// Not wired, and not offered (see docs/drivers/kafka.md).
		mq.FeatureWildcardSubscribe,
		mq.FeatureServerFilter,
		mq.FeatureRequestReply,
		mq.FeatureNativeTracing,
		// Configuration-dependent and off by default: a table that
		// claimed it unconditionally would be claiming behaviour the
		// caller never switched on.
		mq.FeatureTransactions,
	} {
		if caps.Has(f) {
			t.Errorf("%v is advertised at %v and Kafka does not have it here", f, caps.Support(f).Level)
		}
	}
	// Global ordering is conditional on a single-partition topic, which is
	// a topology the deployment chooses and the driver cannot know.
	if lvl := caps.Support(mq.FeatureOrderedGlobally).Level; lvl != mq.Conditional {
		t.Errorf("OrderedGlobally is %v, want Conditional: Kafka orders within a partition and never across partitions", lvl)
	}
}

// TestCapabilitiesFollowTheConfiguration: three claims are things iqmq does
// rather than things the cluster offers, so they depend on what the caller
// configured. A table that claimed them unconditionally would be a lie the
// caller could act on.
func TestCapabilitiesFollowTheConfiguration(t *testing.T) {
	noIdem, _ := openClient(t, func(c *kq.Config) { c.DisableIdempotency = true })
	if noIdem.Capabilities().Has(mq.FeatureIdempotentPublish) {
		t.Error("IdempotentPublish is advertised with the idempotent producer turned off")
	}

	stopNack, _ := openClient(t, func(c *kq.Config) { c.Nack = nack.Stop })
	if stopNack.Capabilities().Has(mq.FeatureDelayedRedelivery) {
		t.Error("DelayedRedelivery is advertised under the stop-the-consumer strategy, which redelivers nothing")
	}
	blockNack, _ := openClient(t)
	if lvl := blockNack.Capabilities().Support(mq.FeatureDelayedRedelivery).Level; lvl != mq.EmulatedVolatile {
		t.Errorf("under block-partition, DelayedRedelivery is %v, want EmulatedVolatile: the hold is in this process and does not survive it", lvl)
	}

	withTx, _ := openClient(t, func(c *kq.Config) { c.TransactionalID = "tx-1" })
	if lvl := withTx.Capabilities().Support(mq.FeatureTransactions).Level; lvl != mq.Conditional {
		t.Errorf("with a transactional id configured, Transactions is %v, want Conditional", lvl)
	}
}

// TestDeadLetterIsClaimedUnconditionallyAndRefusedWithoutADestination is the
// other half of that claim : the table says the feature is
// there, and a client with no destination configured refuses a Reject rather
// than destroying the message. Both are the same behaviour described from two
// sides, and the specification requires a constant table.
func TestDeadLetterIsClaimedUnconditionallyAndRefusedWithoutADestination(t *testing.T) {
	bare, _ := openClient(t)
	s := bare.Capabilities().Support(mq.FeatureDeadLetter)
	if s.Level != mq.EmulatedDurable {
		t.Fatalf("DeadLetter is %v with no destination configured, want EmulatedDurable with the condition in its note", s.Level)
	}
	if !strings.Contains(s.Note, "ErrUnsupported") {
		t.Fatalf("the note does not say what happens with no destination configured: %q", s.Note)
	}
	withDLQ, _ := openClient(t, func(c *kq.Config) { c.NackConfig.DeadLetterTopic = "orders.dlq" })
	if got := withDLQ.Capabilities().Support(mq.FeatureDeadLetter).Level; got != s.Level {
		t.Fatalf("the claim changed to %v when a destination was configured; the specification makes a table constant", got)
	}
}

// TestLimitsAreOnlyWhatTheClusterSaid: the specification forbids reading an unadvertised
// limit as zero, so a limit Kafka does not bound has to come back unknown.
func TestLimitsAreOnlyWhatTheClusterSaid(t *testing.T) {
	c, _ := openClient(t)
	lim := c.Capabilities().Limits()
	if !lim.MaxMessageBytes.Known || lim.MaxMessageBytes.Value <= 0 {
		t.Errorf("MaxMessageBytes = %+v; the cluster's message.max.bytes is the one limit this driver really knows", lim.MaxMessageBytes)
	}
	for name, l := range map[string]mq.Limit[int]{
		"MaxHeaderCount":   lim.MaxHeaderCount,
		"MaxBatchMessages": lim.MaxBatchMessages,
	} {
		if l.Known {
			t.Errorf("%s is advertised as %d and Kafka bounds it only by the message size", name, l.Value)
		}
	}
	if lim.MaxDelay.Known || lim.MaxLease.Known {
		t.Errorf("a delay or lease limit is advertised: Kafka has neither (delay %+v, lease %+v)", lim.MaxDelay, lim.MaxLease)
	}
}

// TestInfoNamesTheClient is every field the driver does not know is
// empty, which is "the driver did not say" rather than a claim.
func TestInfoNamesTheClient(t *testing.T) {
	c, _ := openClient(t)
	info := c.Info()
	if info.Name != "kafka" || info.Profile != "franz-go" {
		t.Fatalf("info = %+v", info)
	}
	if info.ClientName != "github.com/twmb/franz-go" || info.ClientVersion == "" {
		t.Fatalf("the client library is %q %q", info.ClientName, info.ClientVersion)
	}
	if info.BrokerVersion != "" {
		t.Fatalf("BrokerVersion is %q; Kafka reports supported API versions rather than a version string, so the honest answer is empty", info.BrokerVersion)
	}
}
