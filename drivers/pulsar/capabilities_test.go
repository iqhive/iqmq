package pulsar_test

import (
	"testing"

	mq "github.com/iqhive/iqmq"
	pd "github.com/iqhive/iqmq/drivers/pulsar"
)

// TestCapabilitiesConsidersEveryFeature is what keeps a feature from going
// unmentioned as the inventory grows.
//
// A feature nobody mentioned renders as unsupported, which is the safe answer —
// and it is indistinguishable from a feature the driver forgot. The table's own
// construction refuses an incomplete one; this is the test that says so.
func TestCapabilitiesConsidersEveryFeature(t *testing.T) {
	b, _ := newRig(t, nil)
	caps := b.Capabilities()
	for i := range mq.FeatureCount {
		f := mq.Feature(i)
		s := caps.Support(f)
		if !s.Level.Valid() {
			t.Errorf("%v is advertised at a level outside the five", f)
		}
		// the specification's own rules about claims, checked here as well as by the
		// conformance kit, because the kit needs a broker and this does
		// not.
		switch s.Level {
		case mq.Conditional:
			if s.Note == "" {
				t.Errorf("%v is conditional with no condition stated", f)
			}
		case mq.EmulatedVolatile, mq.EmulatedDurable:
			if s.Note == "" {
				t.Errorf("%v is %v with no note saying what the emulation is", f, s.Level)
			}
		case mq.Native, mq.Unsupported:
		}
		if s.Level != mq.Unsupported && s.Scope == mq.ScopeNone {
			t.Errorf("%v is advertised at %v with no scope", f, s.Level)
		}
	}
}

// TestCapabilitiesFollowTheConfiguration holds the five places the table
// depends on the configuration, each of which is a real difference in what the
// broker will do.
func TestCapabilitiesFollowTheConfiguration(t *testing.T) {
	for name, tc := range map[string]struct {
		mutate func(*pd.Config)
		checks map[mq.Feature]mq.Level
	}{
		"a non-persistent topic is not durable": {
			mutate: func(c *pd.Config) { c.Domain = pd.DomainNonPersistent },
			checks: map[mq.Feature]mq.Level{
				mq.FeatureDurableStorage: mq.Unsupported,
				mq.FeatureDurableConfirm: mq.Unsupported,
			},
		},
		"a persistent topic is": {
			mutate: func(c *pd.Config) {},
			checks: map[mq.Feature]mq.Level{
				mq.FeatureDurableStorage: mq.Native,
				mq.FeatureDurableConfirm: mq.Native,
			},
		},
		"no dead-letter destination, a conditional claim": {
			mutate: func(c *pd.Config) {},
			checks: map[mq.Feature]mq.Level{mq.FeatureDeadLetter: mq.Conditional},
		},
		"a dead-letter destination": {
			mutate: func(c *pd.Config) { c.DeadLetterTopic = "orders-dlq" },
			checks: map[mq.Feature]mq.Level{mq.FeatureDeadLetter: mq.Native},
		},
		"the envelope carries the two header features it has to": {
			mutate: func(c *pd.Config) { c.Metadata = mq.MetadataAdaptive },
			checks: map[mq.Feature]mq.Level{
				mq.FeatureNativeHeaders:    mq.Native,
				mq.FeatureBinaryHeaders:    mq.EmulatedDurable,
				mq.FeatureDuplicateHeaders: mq.EmulatedDurable,
			},
		},
		"without it they are unsupported": {
			mutate: func(c *pd.Config) {},
			checks: map[mq.Feature]mq.Level{
				mq.FeatureNativeHeaders:    mq.Native,
				mq.FeatureBinaryHeaders:    mq.Unsupported,
				mq.FeatureDuplicateHeaders: mq.Unsupported,
			},
		},
		"the retry topic makes the delay native and the seek impossible": {
			mutate: func(c *pd.Config) { c.RetryEnable, c.DeadLetterTopic = true, "orders-dlq" },
			checks: map[mq.Feature]mq.Level{
				mq.FeatureDelayedRedelivery: mq.Native,
				mq.FeatureSeekByTime:        mq.Unsupported,
			},
		},
		"without it the delay is emulated and the seek is native": {
			mutate: func(c *pd.Config) {},
			checks: map[mq.Feature]mq.Level{
				mq.FeatureDelayedRedelivery: mq.EmulatedVolatile,
				mq.FeatureSeekByTime:        mq.Native,
			},
		},
		"an administrative endpoint is what Admin needs": {
			mutate: func(c *pd.Config) {},
			checks: map[mq.Feature]mq.Level{mq.FeatureAdmin: mq.Unsupported},
		},
		"transactions are opt-in and reached through x/pulsartx": {
			mutate: func(c *pd.Config) { c.EnableTransactions = true },
			checks: map[mq.Feature]mq.Level{mq.FeatureTransactions: mq.Conditional},
		},
	} {
		t.Run(name, func(t *testing.T) {
			b, _ := newRig(t, tc.mutate)
			caps := b.Capabilities()
			for f, want := range tc.checks {
				if got := caps.Support(f).Level; got != want {
					t.Errorf("%v is advertised at %v, want %v", f, got, want)
				}
			}
		})
	}
}

// TestCapabilitiesNeverClaimWhatPulsarDoesNotHave is the list of denials, each
// of which the driver also refuses at the call: the specification requires an unadvertised
// feature to be refused rather than quietly done differently.
func TestCapabilitiesNeverClaimWhatPulsarDoesNotHave(t *testing.T) {
	b, _ := newRig(t, nil)
	caps := b.Capabilities()
	for _, f := range []mq.Feature{
		mq.FeatureAtMostOnce,
		mq.FeatureExactlyOnceDelivery,
		mq.FeatureMessageTTL,
		mq.FeaturePriority,
		mq.FeatureAtomicBatchPublish,
		mq.FeatureIdempotentPublish,
		mq.FeatureDeduplication,
		mq.FeatureServerFilter,
		mq.FeatureRequestReply,
		mq.FeatureNativeTracing,
		mq.FeatureSeekBySequence,
	} {
		if caps.Has(f) {
			t.Errorf("the driver advertises %v, which Pulsar does not have", f)
		}
	}
	// And the ones it does.
	for _, f := range []mq.Feature{
		mq.FeatureAtLeastOnce, mq.FeatureExplicitAck, mq.FeatureNack,
		mq.FeatureScheduledPublish, mq.FeatureDurableSubscriptions, mq.FeatureFanOut,
		mq.FeatureReplay, mq.FeatureBatchPublish, mq.FeatureBatchReceive,
		mq.FeatureBatchSettle, mq.FeatureWildcardSubscribe, mq.FeaturePublisherConfirm,
		mq.FeatureReceipts, mq.FeatureConsumerGroups,
	} {
		if !caps.Has(f) {
			t.Errorf("the driver advertises no %v, which Pulsar has", f)
		}
	}
}

// TestLimitsAreClaimsNotGuesses holds the rule about limits: an
// unadvertised limit is a different answer from zero, and MaxLease is
// deliberately absent because the emulated lease has no maximum.
func TestLimitsAreClaimsNotGuesses(t *testing.T) {
	b, _ := newRig(t, nil)
	caps := b.Capabilities()
	if lim := caps.Limit(mq.LimitMaxMessageBytes); !lim.Known || lim.Value != pd.DefaultMaxMessageBytes {
		t.Fatalf("MaxMessageBytes is %v; a configuration that asked nothing takes the broker's own "+
			"documented default", lim)
	}
	if lim := caps.Limit(mq.LimitMaxDelay); !lim.Known {
		t.Fatal("MaxDelay is unknown, and this driver bounds a delay itself")
	}
	if lim := caps.Limit(mq.LimitMaxLease); lim.Known {
		t.Fatalf("MaxLease is advertised as %v; the emulated lease has no maximum — an extension is a "+
			"store to a deadline in this process — and the specification forbids reading an advertised limit as "+
			"anything but a bound", lim)
	}
}
