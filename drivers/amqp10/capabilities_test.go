package amqp10_test

import (
	"context"
	"flag"
	"os"
	"path/filepath"
	"testing"

	mq "bitbucket.org/iqhive/iqmq"
	aq "bitbucket.org/iqhive/iqmq/drivers/amqp10"
)

// update rewrites the golden file rather than comparing against it.
//
// A golden file nobody has watched change is a golden file that records whatever
// the code did on the day it was written, so the flag prints the path it wrote
// and the diff is what a reviewer reads.
var update = flag.Bool("update", false, "rewrite the golden files in testdata")

// TestCapabilitiesConsidersEveryFeature is what turns "the driver decided it
// does not have this" into something a test can tell apart from "nobody looked".
//
// The specification makes the absent half of a capability table as load-bearing as the
// present half: a caller reads it to decide whether it may rely on a behaviour,
// and a feature nobody mentioned renders as unsupported — which is the safe
// answer and is indistinguishable from one the driver forgot. This is the check
// that fires when the inventory grows.
func TestCapabilitiesConsidersEveryFeature(t *testing.T) {
	for name, p := range map[string]aq.Profile{
		"the test profile": testProfile(),
		"a bare profile":   bareProfile(),
	} {
		missing, err := aq.MissingFeaturesForTest(p, aq.Facts{})
		if err != nil {
			t.Fatalf("%s: building the table: %v", name, err)
		}
		if len(missing) > 0 {
			t.Errorf("%s never mentions %v of the inventory; a feature nobody considered renders as unsupported "+
				"and is indistinguishable from one the driver forgot", name, missing)
		}
	}
}

// TestCapabilitiesGolden compares the rendered table against a committed file.
//
// A capability claim that cannot be read is a claim on trust. The generated
// matrix is built from a conformance run, which needs a broker; this is the
// half that needs nothing, so a change to any claim or any note is a diff a
// reviewer sees rather than a cell nobody regenerated.
//
// Run with -update to rewrite the golden file, and read the diff before you do.
func TestCapabilitiesGolden(t *testing.T) {
	caps, err := aq.CapabilitiesForTest(testProfile(), aq.Facts{MaxMessageBytes: 1 << 20})
	if err != nil {
		t.Fatalf("building the table: %v", err)
	}
	got := aq.DescribeCapabilities(caps)
	path := filepath.Join("testdata", "capabilities.golden")
	if *update {
		if err := os.WriteFile(path, []byte(got), 0o644); err != nil {
			t.Fatalf("writing the golden file: %v", err)
		}
		t.Logf("wrote %s", path)
		return
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("reading the golden file: %v (run with -update to create it)", err)
	}
	if got != string(want) {
		t.Fatalf("the capability table has changed.\n--- want ---\n%s\n--- got ---\n%s", want, got)
	}
}

// TestCapabilitiesRefuseAnOverclaim is the enforcement half of the
// [aq.Profile.Capabilities] seam, and the reason that seam is safe to hand to
// somebody else.
//
// A profile is written by whoever knows the broker, and the obvious mistake is
// to claim a feature the BROKER has and this CORE has no code for. The specification makes
// the table the thing a caller acts on, so a table that can lie is worse than a
// table that says nothing — and the refusal happens at Open rather than being
// published in the matrix.
func TestCapabilitiesRefuseAnOverclaim(t *testing.T) {
	for _, tc := range []struct {
		name  string
		claim aq.Claim
		code  mq.ErrorCode
	}{
		{
			// Artemis really does extend a message's delivery timeout, and
			// AMQP 1.0 has no way to ask.
			name:  "lease extension",
			claim: aq.Native(mq.FeatureExtendLease, mq.ScopeTopic),
		},
		{
			// A disposition frame CAN cover a range of delivery ids, and
			// go-amqp exposes only the per-message wrappers.
			name:  "batched settlement",
			claim: aq.Native(mq.FeatureBatchSettle, mq.ScopeTopic),
		},
		{
			// application-properties is a map. No profile can make it not a
			// map.
			name:  "duplicate headers",
			claim: aq.Native(mq.FeatureDuplicateHeaders, mq.ScopeTopic),
		},
		{
			name:  "a batched publication",
			claim: aq.Native(mq.FeatureBatchPublish, mq.ScopeTopic),
		},
		{
			name:  "transactions",
			claim: aq.Native(mq.FeatureTransactions, mq.ScopeTopic),
		},
		{
			name:  "seeking by time",
			claim: aq.Native(mq.FeatureSeekByTime, mq.ScopeTopic),
		},
		{
			name:  "replay",
			claim: aq.Native(mq.FeatureReplay, mq.ScopeTopic),
		},
		{
			// A name rather than a ceiling: a feature outside the specification's
			// inventory is a caller's mistake and not a profile that claimed
			// too much, so it carries a different code.
			name:  "a feature outside the inventory",
			claim: aq.Claim{Feature: mq.Feature(mq.FeatureCount + 7), Support: mq.Support{Level: mq.Native}},
			code:  mq.CodeInvalidArgument,
		},
		{
			name:  "a support level outside the enumeration",
			claim: aq.Claim{Feature: mq.FeatureDeadLetter, Support: mq.Support{Level: mq.Level(99)}},
			code:  mq.CodeInvalidConfig,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			want := tc.code
			if want == 0 {
				want = mq.CodeInvalidConfig
			}
			p := testProfile()
			p.Capabilities = func(aq.Facts) []aq.Claim { return []aq.Claim{tc.claim} }
			_, err := aq.CapabilitiesForTest(p, aq.Facts{})
			if err == nil {
				t.Fatalf("a profile claiming %s was accepted; the core has no code for it, so the claim would be "+
					"published in the capability matrix and answered with ErrUnsupported ", tc.name)
			}
			assertCode(t, err, want, "a profile claiming "+tc.name)
		})
	}
}

// TestCapabilitiesRefuseAClaimWithNoValueBehindIt is the three conditional
// ceilings: the core HAS the code and the profile has to supply the one value it
// needs, so "may this be claimed" is a question about this profile rather than
// about the feature.
func TestCapabilitiesRefuseAClaimWithNoValueBehindIt(t *testing.T) {
	for _, tc := range []struct {
		name  string
		claim aq.Claim
		strip func(*aq.Profile)
	}{
		{
			name:  "FeatureAdmin with no Profile.Admin",
			claim: aq.Conditional(mq.FeatureAdmin, mq.ScopeTopic, "a management endpoint"),
			strip: func(p *aq.Profile) { p.Admin = nil },
		},
		{
			name:  "FeatureDurableConfirm with no Profile.Durable",
			claim: aq.Native(mq.FeatureDurableConfirm, mq.ScopeTopic),
			strip: func(p *aq.Profile) { p.Durable = aq.DurableRule{} },
		},
		{
			name:  "FeatureScheduledPublish with no Profile.Delay",
			claim: aq.Conditional(mq.FeatureScheduledPublish, mq.ScopeTopic, "an annotation"),
			strip: func(p *aq.Profile) { p.Delay = aq.DelayRule{} },
		},
		{
			name:  "FeatureDelayedRedelivery with no Profile.NackDelay",
			claim: aq.Conditional(mq.FeatureDelayedRedelivery, mq.ScopeTopic, "an annotation"),
			strip: func(p *aq.Profile) { p.NackDelay = aq.NackDelayRule{} },
		},
		{
			name:  "FeatureDeduplication with no Profile.Dedup",
			claim: aq.Conditional(mq.FeatureDeduplication, mq.ScopeTopic, "a property"),
			strip: func(p *aq.Profile) { p.Dedup = aq.SlotRule{} },
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := testProfile()
			tc.strip(&p)
			p.Capabilities = func(aq.Facts) []aq.Claim { return []aq.Claim{tc.claim} }
			if _, err := aq.CapabilitiesForTest(p, aq.Facts{}); err == nil {
				t.Fatalf("a profile claiming %s was accepted; the table would advertise something Client.Admin or the "+
					"publish path answers ErrUnsupported to", tc.name)
			}
			// And with the value in place, the same claim is accepted: the
			// check is about the value and not about the feature.
			ok := testProfile()
			ok.Admin = func(context.Context, *aq.Backend) (mq.Admin, error) { return nil, mq.ErrUnsupported }
			ok.Capabilities = func(aq.Facts) []aq.Claim { return []aq.Claim{tc.claim} }
			if _, err := aq.CapabilitiesForTest(ok, aq.Facts{}); err != nil {
				t.Fatalf("the same claim with its value in place was refused: %v", err)
			}
		})
	}
}

// TestCapabilitiesProfileMayNarrow is the other direction of the seam: a
// profile that knows its broker lacks something the core CAN do withdraws the
// claim, and withdrawing is always allowed.
func TestCapabilitiesProfileMayNarrow(t *testing.T) {
	p := testProfile()
	p.Capabilities = func(aq.Facts) []aq.Claim {
		return []aq.Claim{
			// A broker that accepts the AMQP header's priority field and
			// ignores it. The core cannot tell; the profile can.
			aq.Withdraw(mq.FeaturePriority),
			aq.Withdraw(mq.FeatureMessageTTL),
		}
	}
	caps, err := aq.CapabilitiesForTest(p, aq.Facts{})
	if err != nil {
		t.Fatalf("a narrowing profile was refused: %v", err)
	}
	if caps.Has(mq.FeaturePriority) || caps.Has(mq.FeatureMessageTTL) {
		t.Fatal("the profile's withdrawal did not reach the table")
	}
	// And the base table really did claim them, so the withdrawal is a
	// narrowing rather than a no-op.
	base, err := aq.CapabilitiesForTest(bareProfile(), aq.Facts{})
	if err != nil {
		t.Fatalf("the base table: %v", err)
	}
	if !base.Has(mq.FeaturePriority) || !base.Has(mq.FeatureMessageTTL) {
		t.Fatal("the base table does not claim the priority and time-to-live fields of the AMQP message header, " +
			"so the narrowing test above proves nothing")
	}
}

// TestCapabilitiesConditionalsStateTheirCondition is the rule and the
// builder's refusal, checked over the whole table rather than case by case: a
// conditional support with no note tells a program that something might work,
// under conditions nobody wrote down.
func TestCapabilitiesConditionalsStateTheirCondition(t *testing.T) {
	caps, err := aq.CapabilitiesForTest(testProfile(), aq.Facts{})
	if err != nil {
		t.Fatalf("building the table: %v", err)
	}
	for i := range mq.FeatureCount {
		f := mq.Feature(i)
		s := caps.Support(f)
		switch s.Level {
		case mq.Conditional, mq.EmulatedDurable, mq.EmulatedVolatile:
			if s.Note == "" {
				t.Errorf("%v is %v and states no condition ", f, s.Level)
			}
		case mq.Unsupported, mq.Native:
		}
	}
}

// TestCoreCeilingIsHonestAboutWhatTheCoreDoes holds the ceiling table itself:
// every feature the core really implements has a ceiling above Unsupported, and
// every feature it cannot do has Unsupported — which is what makes the overclaim
// check above mean anything.
func TestCoreCeilingIsHonestAboutWhatTheCoreDoes(t *testing.T) {
	cannot := []mq.Feature{
		mq.FeatureExactlyOnceDelivery,
		mq.FeatureReplay,
		mq.FeatureSeekByTime,
		mq.FeatureSeekBySequence,
		mq.FeatureDuplicateHeaders,
		mq.FeatureBatchPublish,
		mq.FeatureAtomicBatchPublish,
		mq.FeatureBatchSettle,
		mq.FeatureExtendLease,
		mq.FeatureTransactions,
		mq.FeatureRequestReply,
		mq.FeatureNativeTracing,
	}
	for _, f := range cannot {
		if got := aq.CoreCeiling(f); got != mq.Unsupported {
			t.Errorf("CoreCeiling(%v) = %v, want unsupported: this core has no code for it", f, got)
		}
	}
	can := []mq.Feature{
		mq.FeatureAtLeastOnce,
		mq.FeatureExplicitAck,
		mq.FeatureNack,
		mq.FeatureNativeHeaders,
		mq.FeatureBinaryHeaders,
		mq.FeatureBatchReceive,
		mq.FeaturePublisherConfirm,
		mq.FeatureMessageTTL,
		mq.FeaturePriority,
		mq.FeatureDeadLetter,
		mq.FeatureScheduledPublish,
		mq.FeatureDelayedRedelivery,
		mq.FeatureAdmin,
		mq.FeatureReceipts,
	}
	for _, f := range can {
		if got := aq.CoreCeiling(f); got == mq.Unsupported {
			t.Errorf("CoreCeiling(%v) = unsupported, want a level a profile can claim", f)
		}
	}
	if got := aq.CoreCeiling(mq.Feature(mq.FeatureCount + 3)); got != mq.Unsupported {
		t.Errorf("CoreCeiling of a feature outside the inventory = %v, want unsupported", got)
	}
}

// TestCapabilitiesLimitsAreReadOffThePeer holds the rule that an
// unadvertised limit is never an overloaded zero.
func TestCapabilitiesLimitsAreReadOffThePeer(t *testing.T) {
	silent, err := aq.CapabilitiesForTest(testProfile(), aq.Facts{})
	if err != nil {
		t.Fatalf("building the table: %v", err)
	}
	if lim := silent.Limit(mq.LimitMaxMessageBytes); lim.Known {
		t.Fatalf("a peer that declared no message ceiling produced a KNOWN limit of %d; the specification forbids reading an "+
			"unadvertised limit as zero, and a limit of zero would refuse every message", lim.Value)
	}
	// And a lease limit is never advertised at all: AMQP 1.0 grants no lease,
	// so an advertised zero would read as "every lease expires immediately".
	if lim := silent.Limit(mq.LimitMaxLease); lim.Known {
		t.Fatalf("a lease limit of %d is advertised; AMQP 1.0 grants no lease", lim.Value)
	}
	spoken, err := aq.CapabilitiesForTest(testProfile(), aq.Facts{MaxMessageBytes: 1 << 20})
	if err != nil {
		t.Fatalf("building the table: %v", err)
	}
	lim := spoken.Limit(mq.LimitMaxMessageBytes)
	if !lim.Known || lim.Value != 1<<20 {
		t.Fatalf("the peer's declared ceiling reached the table as %+v, want a known 1048576", lim)
	}
	// The profile's delay ceiling becomes MaxDelay, which is what a caller
	// reads before it schedules something.
	if d := spoken.Limit(mq.LimitMaxDelay); !d.Known || d.Value != int64(3600e9) {
		t.Fatalf("the profile's delay ceiling reached the table as %+v, want a known hour", d)
	}
}
