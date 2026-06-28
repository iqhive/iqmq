package snssqs_test

import (
	"strings"
	"testing"

	mq "bitbucket.org/iqhive/iqmq"
	sns "bitbucket.org/iqhive/iqmq/drivers/snssqs"
	sq "bitbucket.org/iqhive/iqmq/drivers/sqs"
)

// TestCapabilitiesConsidersEveryFeature is what keeps the table honest as the inventory grows: an unadvertised feature renders as unsupported, which
// is the safe answer and is indistinguishable from one the driver forgot.
//
// The construction itself is the assertion: capabilities() refuses a table
// that does not mention every feature, so a backend that opens has one. What
// is checked here is the other half of the rule about claims — a level that
// exists, and a stated condition wherever one is required.
func TestCapabilitiesConsidersEveryFeature(t *testing.T) {
	caps := open(t).client.Capabilities()
	for f := range mq.FeatureCount {
		s := caps.Support(mq.Feature(f))
		if !s.Level.Valid() {
			t.Fatalf("feature %v has level %v", mq.Feature(f), s.Level)
		}
		switch s.Level {
		case mq.Conditional, mq.EmulatedVolatile, mq.EmulatedDurable:
			if s.Note == "" {
				t.Errorf("%v is %v with no note; the specification requires the condition or the emulation to be stated", mq.Feature(f), s.Level)
			}
		case mq.Native, mq.Unsupported:
		}
	}
}

// TestTheTableDiffersFromTheQueueDriverWhereTheFanOutSaysItShould is the
// reason this driver is a driver rather than a configuration of the other one.
//
// Four differences, and all four are the fan-out. A table that answered the
// same as `drivers/sqs`'s would let a caller plan a per-message delay SNS
// cannot honour, or believe a publication reaches one queue.
func TestTheTableDiffersFromTheQueueDriverWhereTheFanOutSaysItShould(t *testing.T) {
	fan := open(t).client.Capabilities()

	// 1. The fan-out itself.
	if !fan.Has(mq.FeatureFanOut) {
		t.Fatalf("the fan-out driver does not claim FeatureFanOut")
	}
	if got := fan.Support(mq.FeatureFanOut).Level; got != mq.Native {
		t.Errorf("FeatureFanOut is %v, want native", got)
	}
	if note := fan.Support(mq.FeatureFanOut).Note; !strings.Contains(note, "ITS OWN copy") {
		t.Errorf("the fan-out note does not say that each subscription settles its own copy: %q", note)
	}

	// 2. No per-message timer. This is the claim a caller porting a
	// publisher from the queue driver loses, and the specification makes dropping it
	// silently a disqualifier — so it has to be ABSENT from the table as
	// well as refused at the sink.
	if fan.Has(mq.FeatureScheduledPublish) {
		t.Errorf("the fan-out driver claims a scheduled publication; SNS has no per-message timer")
	}

	// 3. The filter policy is conditional, with its condition stated.
	filter := fan.Support(mq.FeatureServerFilter)
	if filter.Level != mq.Conditional {
		t.Errorf("FeatureServerFilter is %v, want conditional: without a policy configured for the subscription there is no filter at all", filter.Level)
	}
	for _, want := range []string{"FilterPolicies", "FilterPolicyScope", "envelope"} {
		if !strings.Contains(filter.Note, want) {
			t.Errorf("the filter condition does not mention %q: %q", want, filter.Note)
		}
	}

	// 4. A quarter of the message size.
	if got := fan.Limits().MaxMessageBytes; !got.Known || got.Value != sns.MaxMessageBytes {
		t.Errorf("MaxMessageBytes = %d (known %v), want SNS's %d", got.Value, got.Known, sns.MaxMessageBytes)
	}
	if sns.MaxMessageBytes >= sq.MaxMessageBytes {
		t.Errorf("SNS's message limit (%d) is not smaller than a queue's (%d); the whole point of the constant is that it is", sns.MaxMessageBytes, sq.MaxMessageBytes)
	}
}

// TestTheTableAgreesWithTheQueueDriverEverywhereElse is the other half of the
// composition's claim: the consume side IS `drivers/sqs`, so the features that
// describe consumption have to say the same thing.
//
// It is a real check rather than a tautology — the two tables are written
// independently, in two files — and it is what would catch a fan-out driver
// that quietly stopped claiming, say, FeatureExtendLease while still
// forwarding Extend to a settler that implements it.
func TestTheTableAgreesWithTheQueueDriverEverywhereElse(t *testing.T) {
	fan := open(t).client.Capabilities()
	// The queue driver's standard-profile table, built through that
	// driver's own public constructor rather than copied.
	qcaps := queueCapabilities(t)
	for _, f := range []mq.Feature{
		mq.FeatureDurableStorage, mq.FeatureAtLeastOnce, mq.FeatureExplicitAck,
		mq.FeatureDurableSubscriptions, mq.FeatureNativeHeaders, mq.FeatureBinaryHeaders,
		mq.FeatureBatchPublish, mq.FeatureBatchReceive, mq.FeatureBatchSettle,
		mq.FeatureNack, mq.FeatureDelayedRedelivery, mq.FeatureExtendLease,
		mq.FeatureAdmin, mq.FeatureReceipts, mq.FeatureConsumerGroups,
		mq.FeaturePublisherConfirm, mq.FeatureDurableConfirm, mq.FeatureDeadLetter,
		mq.FeatureAtMostOnce, mq.FeatureExactlyOnceDelivery, mq.FeatureMessageTTL,
		mq.FeaturePriority, mq.FeatureSeekByTime, mq.FeatureSeekBySequence,
		mq.FeatureOrderedByKey, mq.FeatureOrderedByPartition, mq.FeatureOrderedGlobally,
		mq.FeatureDuplicateHeaders, mq.FeatureAtomicBatchPublish, mq.FeatureTransactions,
		mq.FeatureWildcardSubscribe, mq.FeatureRequestReply, mq.FeatureNativeTracing,
		mq.FeatureIdempotentPublish, mq.FeatureDeduplication,
	} {
		if got, want := fan.Support(f).Level, qcaps.Support(f).Level; got != want {
			t.Errorf("%v is %v on the fan-out driver and %v on the queue driver: the consume side is the same code, so a difference here is one of the two tables being wrong",
				f, got, want)
		}
	}
	// And the three that must DIFFER, asserted as differences so that a
	// future edit cannot quietly make the two tables identical. FeatureReplay
	// is not among them: it is unsupported on both, and the difference the
	// fan-out makes to it is that the kit's negative probe can no longer
	// refute the claim , which is a fact about the
	// conformance report rather than about the table.
	for _, f := range []mq.Feature{mq.FeatureFanOut, mq.FeatureScheduledPublish, mq.FeatureServerFilter} {
		if fan.Support(f).Level == qcaps.Support(f).Level {
			t.Errorf("%v is %v on both drivers, and the fan-out is supposed to change it", f, fan.Support(f).Level)
		}
	}
}

// TestLimitsAreTheServiceLimits holds the specification's "service limits
// discovered/documented from the tested API version": each number is the
// pinned SDKs', and each unknown is honestly unknown.
//
// MaxDelay is the interesting row. It is UNKNOWN rather than zero, and the
// difference matters: a zero Limit with Known true would say "the longest
// delay is nothing", which is a different claim from "this broker has no
// per-message delay". The capability table makes the second claim through
// FeatureScheduledPublish, and the specification forbids an unadvertised limit from being
// read as a zero.
func TestLimitsAreTheServiceLimits(t *testing.T) {
	lim := open(t).client.Capabilities().Limits()
	for _, tc := range []struct {
		name  string
		known bool
		got   int64
		want  int64
	}{
		{"MaxMessageBytes", lim.MaxMessageBytes.Known, lim.MaxMessageBytes.Value, sns.MaxMessageBytes},
		{"MaxBatchMessages", lim.MaxBatchMessages.Known, int64(lim.MaxBatchMessages.Value), sns.MaxBatchEntries},
		{"MaxBatchBytes", lim.MaxBatchBytes.Known, lim.MaxBatchBytes.Value, sns.MaxMessageBytes},
		{"MaxHeaderCount", lim.MaxHeaderCount.Known, int64(lim.MaxHeaderCount.Value), sns.MaxAttributes - 1},
		{"MaxLease", lim.MaxLease.Known, int64(lim.MaxLease.Value), int64(sns.MaxVisibility)},
	} {
		if !tc.known {
			t.Errorf("%s is unknown; the pinned SDKs state it", tc.name)
			continue
		}
		if tc.got != tc.want {
			t.Errorf("%s = %d, want %d", tc.name, tc.got, tc.want)
		}
	}
	if lim.MaxDelay.Known {
		t.Errorf("MaxDelay is advertised as %v; SNS has no per-message timer, and a zero with Known true would read as a limit of nothing", lim.MaxDelay.Value)
	}
	if lim.MaxHeaderBytes.Known {
		t.Errorf("MaxHeaderBytes is advertised as %d; neither service states a header-byte ceiling separately from the message limit", lim.MaxHeaderBytes.Value)
	}
}

// TestInfoNamesTheDriverAndBothClients is.
func TestInfoNamesTheDriverAndBothClients(t *testing.T) {
	info := open(t).client.Info()
	if info.Name != "snssqs" {
		t.Errorf("name = %q", info.Name)
	}
	if info.Profile != "standard" {
		t.Errorf("profile = %q, want \"standard\": SNS has a FIFO topic profile this driver refuses rather than implements, so the field says which one a caller has", info.Profile)
	}
	if info.ClientName != sns.ClientModule {
		t.Errorf("client = %q, want the SNS SDK", info.ClientName)
	}
	if info.ClientVersion != sns.ClientVersion {
		t.Errorf("client version = %q", info.ClientVersion)
	}
}
