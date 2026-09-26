package gcppubsub_test

import (
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
	gps "github.com/iqhive/iqmq/drivers/gcppubsub"
)

// TestCapabilitiesConsidersEveryFeature is what keeps the table honest as the inventory grows: an unadvertised feature renders as unsupported, which
// is the safe answer and is indistinguishable from one the driver forgot.
//
// The construction itself is half the assertion — capabilities() refuses a
// table that does not mention every feature, so a backend that opens has one
// — and the loop is the other half: every conditional and every emulated
// claim has to state its condition or its emulation, which is what the specification
// requires and what the conformance kit's `capability/*/claim` scenario
// checks.
func TestCapabilitiesConsidersEveryFeature(t *testing.T) {
	c, _ := scripted(t)
	caps := c.Capabilities()
	for f := range mq.FeatureCount {
		feature := mq.Feature(f)
		s := caps.Support(feature)
		if !s.Level.Valid() {
			t.Fatalf("feature %v has level %v", feature, s.Level)
		}
		switch s.Level {
		case mq.Conditional, mq.EmulatedVolatile, mq.EmulatedDurable:
			if s.Note == "" {
				t.Errorf("%v is %v with no note; the specification requires the condition or the emulation to be stated", feature, s.Level)
			}
		case mq.Native, mq.Unsupported:
		}
	}
}

// TestCapabilityTableDoesNotDependOnTheConfiguration is the specification's "capabilities
// are constant per client", and it is the property the rule
// turns on.
//
// The conformance kit runs two scenarios per claim and shapes the topology
// differently for each, so a table computed from Config answers "unsupported"
// to the scenario whose Need does not configure the feature — the positive
// probe is gated off and the negative probe is asked to refute a denial the
// driver does not make, and the claim is proved in neither direction. A report
// full of that looks exactly like a passing report.
func TestCapabilityTableDoesNotDependOnTheConfiguration(t *testing.T) {
	plain, _ := scripted(t)
	loaded, _ := scripted(t, func(cfg *gps.Config) {
		cfg.Ordering = true
		cfg.RetainAcked = true
		cfg.Retention = gps.MinRetention
		cfg.ExactlyOnce = true
		cfg.Filter = `attributes.k = "v"`
		cfg.DeadLetterTopic = "dlq"
		cfg.MaxDeliveryAttempts = 5
	})
	a, b := plain.Capabilities(), loaded.Capabilities()
	for f := range mq.FeatureCount {
		feature := mq.Feature(f)
		if a.Support(feature).Level != b.Support(feature).Level {
			t.Errorf("%v is %v with a plain configuration and %v with a loaded one; the specification makes capabilities constant per client",
				feature, a.Support(feature).Level, b.Support(feature).Level)
		}
	}
}

// TestClaimsThisDriverDeliberatelyDoesNotMake pins the two claims the
// service's own vocabulary would suggest.
//
// Pub/Sub calls a subscription setting "exactly-once delivery", and it is a
// guarantee about a SETTLEMENT — an acknowledged message is never redelivered
// — rather than about delivery multiplicity: an unacknowledged message whose
// deadline expires is redelivered on such a subscription exactly as on any
// other. The feature is the second thing, so this driver does not claim it
// however the subscription is configured. And there is no publication timer at
// all, so a scheduled publication is not a claim either.
func TestClaimsThisDriverDeliberatelyDoesNotMake(t *testing.T) {
	c, _ := scripted(t, func(cfg *gps.Config) { cfg.ExactlyOnce = true })
	caps := c.Capabilities()
	for _, f := range []mq.Feature{
		mq.FeatureExactlyOnceDelivery,
		mq.FeatureScheduledPublish,
		mq.FeatureMessageTTL,
		mq.FeatureDeduplication,
		mq.FeatureIdempotentPublish,
		mq.FeatureBinaryHeaders,
		mq.FeatureDuplicateHeaders,
		mq.FeatureOrderedByPartition,
		mq.FeatureOrderedGlobally,
		mq.FeatureWildcardSubscribe,
		mq.FeatureAtMostOnce,
		mq.FeaturePriority,
		mq.FeatureTransactions,
		mq.FeatureRequestReply,
		mq.FeatureSeekBySequence,
		mq.FeatureAtomicBatchPublish,
		mq.FeatureNativeTracing,
	} {
		if caps.Has(f) {
			t.Errorf("the table claims %v", f)
		}
	}
	// And the ones it does make, so that a claim silently disappearing is a
	// test failure rather than a capability matrix that quietly shrinks.
	for _, f := range []mq.Feature{
		mq.FeatureDurableStorage, mq.FeatureAtLeastOnce, mq.FeatureExplicitAck,
		mq.FeatureNack, mq.FeatureDelayedRedelivery, mq.FeatureExtendLease,
		mq.FeatureDeadLetter, mq.FeatureConsumerGroups, mq.FeatureDurableSubscriptions,
		mq.FeatureFanOut, mq.FeatureReplay, mq.FeatureSeekByTime,
		mq.FeatureOrderedByKey, mq.FeatureNativeHeaders, mq.FeatureBatchPublish,
		mq.FeatureBatchReceive, mq.FeatureBatchSettle, mq.FeatureServerFilter,
		mq.FeatureAdmin, mq.FeaturePublisherConfirm, mq.FeatureDurableConfirm,
		mq.FeatureReceipts,
	} {
		if !caps.Has(f) {
			t.Errorf("the table no longer claims %v", f)
		}
	}
}

// TestEmulatedClaimsNeverSatisfyADurabilityRequirement is the rule about
// the levels themselves: an EmulatedVolatile support must fail
// RequireAtLeast(EmulatedDurable), because the whole point of the level is
// that the emulation does not survive a process death.
func TestEmulatedClaimsNeverSatisfyADurabilityRequirement(t *testing.T) {
	c, _ := scripted(t)
	caps := c.Capabilities()
	for _, f := range []mq.Feature{mq.FeatureDelayedRedelivery, mq.FeatureExtendLease} {
		if lvl := caps.Support(f).Level; lvl != mq.EmulatedVolatile {
			t.Fatalf("%v is %v, want EmulatedVolatile", f, lvl)
		}
		if err := caps.Require(mq.RequireAtLeast(f, mq.EmulatedDurable)); err == nil {
			t.Errorf("%v satisfies a durable requirement; the specification says a volatile emulation never does", f)
		}
		if err := caps.Require(mq.RequireAtLeast(f, mq.EmulatedVolatile)); err != nil {
			t.Errorf("%v does not satisfy its own level: %v", f, err)
		}
	}
}

// TestLimitsAdvertiseOnlyWhatTheDriverKnows is the numeric half, and the
// reason `limits/advertised` exists: a driver that advertises nothing leaves
// every scenario that skips on an unknown limit blind to the lookup having
// failed.
func TestLimitsAdvertiseOnlyWhatTheDriverKnows(t *testing.T) {
	c, _ := scripted(t)
	caps := c.Capabilities()
	for _, tc := range []struct {
		key  mq.LimitKey
		want int64
	}{
		{mq.LimitMaxMessageBytes, gps.MaxMessageBytes},
		{mq.LimitMaxBatchMessages, gps.MaxBatchMessages},
		{mq.LimitMaxBatchBytes, gps.MaxMessageBytes},
		{mq.LimitMaxHeaderBytes, gps.MaxAttributeValueBytes},
		{mq.LimitMaxLease, int64(gps.MaxAckDeadline)},
	} {
		lim := caps.Limit(tc.key)
		if !lim.Known {
			t.Errorf("%v is not advertised", tc.key)
			continue
		}
		if lim.Value != tc.want {
			t.Errorf("%v is %d, want %d", tc.key, lim.Value, tc.want)
		}
	}
	// The header COUNT is the service's hundred less the attributes this
	// driver reserves: advertising a hundred would tell a caller it may send
	// a hundred headers, and the ninety-first would then push the message
	// into the envelope or into a refusal.
	count := caps.Limit(mq.LimitMaxHeaderCount)
	if !count.Known || count.Value >= gps.MaxAttributes || count.Value <= 0 {
		t.Errorf("MaxHeaderCount is %v, want a positive number below the service's %d", count, gps.MaxAttributes)
	}
	// And the delay the in-process hold can honour is what is left of the
	// extension budget, which is a real number rather than an unknown.
	delay := caps.Limit(mq.LimitMaxDelay)
	if !delay.Known || delay.Value != int64(gps.DefaultMaxExtension) {
		t.Errorf("MaxDelay is %v, want the extension budget %v", delay, gps.DefaultMaxExtension)
	}
}

// TestInfoNamesTheClientGeneration is why DriverInfo.Profile is the client
// library here rather than a broker profile: the specification is a bake-off between
// two client generations behind one capability table, and a caller who read
// "gcppubsub" without knowing which library answered would not know which set
// of upstream bugs it has.
func TestInfoNamesTheClientGeneration(t *testing.T) {
	l := newLive(t)
	c := l.open(t)
	info := c.Info()
	if info.Name != gps.DriverName {
		t.Errorf("Info().Name is %q", info.Name)
	}
	if info.Profile != "v2" {
		t.Errorf("Info().Profile is %q, want the client generation", info.Profile)
	}
	if info.ClientName != "cloud.google.com/go/pubsub/v2" || info.ClientVersion == "" {
		t.Errorf("Info() names the client %q %q", info.ClientName, info.ClientVersion)
	}
	// The scripted fake names itself, so a report or a log line made against
	// it can never be mistaken for one made against a real client.
	f, _ := scripted(t)
	if p := f.Info().Profile; p != "fake" {
		t.Errorf("the fake reports profile %q, want \"fake\"", p)
	}
	_ = time.Second
}
