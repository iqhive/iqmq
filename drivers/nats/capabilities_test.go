package nats_test

import (
	"testing"

	mq "bitbucket.org/iqhive/iqmq"
)

// TestCapabilitiesConsiderEveryFeature is the difference between a feature the
// driver decided it does not have and one nobody looked at.
//
// An unmentioned feature renders as unsupported, which is the safe answer and
// is also indistinguishable from an oversight. The driver's table states all 39
// explicitly and its construction refuses a table that does not, so this test
// is what fails when the inventory grows and nobody revisited the driver.
func TestCapabilitiesConsiderEveryFeature(t *testing.T) {
	caps := openClient(t).Capabilities()
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

// TestCapabilitiesAreHonestAboutDurability is the rule for a broker that
// stores nothing, applied to every claim that would imply it does.
//
// NATS Core keeps no message anywhere. A claim of durable storage, durable
// confirmation, at-least-once delivery or replay would each be a caller's
// reason to stop keeping its own copy, so each of them being absent is load
// bearing rather than incidental.
func TestCapabilitiesAreHonestAboutDurability(t *testing.T) {
	caps := openClient(t).Capabilities()
	for _, f := range []mq.Feature{
		mq.FeatureDurableStorage,
		mq.FeatureDurableConfirm,
		mq.FeatureDurableSubscriptions,
		mq.FeatureAtLeastOnce,
		mq.FeatureExactlyOnceDelivery,
		mq.FeatureExplicitAck,
		mq.FeatureNack,
		mq.FeatureExtendLease,
		mq.FeatureDeadLetter,
		mq.FeatureReplay,
		mq.FeatureSeekByTime,
		mq.FeatureSeekBySequence,
		mq.FeatureMessageTTL,
		mq.FeatureScheduledPublish,
		mq.FeatureDelayedRedelivery,
		mq.FeaturePriority,
		mq.FeatureDeduplication,
		mq.FeatureAdmin,
		mq.FeatureBinaryHeaders,
		mq.FeatureReceipts,
	} {
		if caps.Has(f) {
			t.Errorf("NATS Core advertises %v at %v: it stores nothing and settles nothing", f, caps.Support(f).Level)
		}
	}
	// And the positives, each of which a scenario in this package proves.
	for _, f := range []mq.Feature{
		mq.FeatureAtMostOnce,
		mq.FeatureConsumerGroups,
		mq.FeatureFanOut,
		mq.FeatureNativeHeaders,
		mq.FeatureDuplicateHeaders,
		mq.FeatureBatchReceive,
		mq.FeatureBatchPublish,
		mq.FeatureWildcardSubscribe,
		mq.FeatureRequestReply,
		mq.FeaturePublisherConfirm,
		mq.FeatureOrderedGlobally,
		mq.FeatureOrderedByKey,
		mq.FeatureOrderedByPartition,
	} {
		if !caps.Has(f) {
			t.Errorf("NATS Core does not advertise %v, which this driver has", f)
		}
	}
}

// TestVolatileEmulationNeverSatisfiesDurability is the one rule a plain
// ordinal comparison gets wrong, checked on the claim this driver makes at that
// level.
func TestVolatileEmulationNeverSatisfiesDurability(t *testing.T) {
	caps := openClient(t).Capabilities()
	if got := caps.Support(mq.FeatureBatchPublish).Level; got != mq.EmulatedVolatile {
		t.Fatalf("BatchPublish is advertised at %v, want emulatedvolatile", got)
	}
	if err := caps.Require(mq.RequireAtLeast(mq.FeatureBatchPublish, mq.EmulatedVolatile)); err != nil {
		t.Fatalf("a requirement for exactly the advertised level was refused: %v", err)
	}
	if err := caps.Require(mq.RequireAtLeast(mq.FeatureBatchPublish, mq.EmulatedDurable)); err == nil {
		t.Fatal("a volatile emulation satisfied a durable requirement")
	}
}

// TestAdvertisedLimitIsTheServerLimit is the rule that an unadvertised limit
// is never an overloaded zero: the one limit this driver knows is the server's
// own max_payload, and every other limit stays unknown.
func TestAdvertisedLimitIsTheServerLimit(t *testing.T) {
	caps := openClient(t).Capabilities()
	lim := caps.Limit(mq.LimitMaxMessageBytes)
	if !lim.Known || lim.Value <= 0 {
		t.Fatalf("MaxMessageBytes = %+v, want the server's max_payload", lim)
	}
	for _, key := range []mq.LimitKey{
		mq.LimitMaxBatchMessages,
		mq.LimitMaxBatchBytes,
		mq.LimitMaxHeaderCount,
		mq.LimitMaxHeaderBytes,
		mq.LimitMaxDelay,
		mq.LimitMaxLease,
	} {
		if got := caps.Limit(key); got.Known {
			t.Errorf("%v is advertised as %d, but this driver does not know it", key, got.Value)
		}
	}
}

// TestRequirementsFailAtConstruction is a handle that needs a capability
// the driver does not have fails when it is built, not at the first message.
func TestRequirementsFailAtConstruction(t *testing.T) {
	c := openClient(t)
	_, err := c.Publisher(t.Context(), mq.PublisherConfig{
		Topic:        newTopic("requires"),
		Requirements: []mq.Requirement{mq.RequireAtLeast(mq.FeatureDurableStorage, mq.Native)},
	})
	if err == nil {
		t.Fatal("a publisher requiring durable storage was accepted")
	}
	if code := mq.CodeOf(err); code != mq.CodeUnsupported {
		t.Fatalf("the refusal carries code %v, want Unsupported", code)
	}
}
