package natsunified_test

import (
	"sort"
	"testing"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/drivers/jetstream"
	"github.com/iqhive/iqmq/drivers/nats"
	"github.com/iqhive/iqmq/drivers/natsunified"
)

// TestCapabilitiesAreTheProfilesOwn is the check: the facade has
// no capability table, so a client opened through it advertises exactly what the
// same client opened through the profile driver advertises.
//
// The comparison is per feature and includes the LEVEL, the SCOPE and the NOTE,
// and then the limits. The note matters as much as the level here: the specification makes a
// conditional claim's condition part of the claim, and a facade that merged two
// tables would most likely get the levels right and lose the notes — which is
// the same defect one step further down, because a note is how a caller learns
// that "publisher confirm" means the socket on one profile and the disk on the
// other.
func TestCapabilitiesAreTheProfilesOwn(t *testing.T) {
	t.Run("core", func(t *testing.T) {
		cfg := coreConfig(t)
		direct, err := nats.Open(t.Context(), cfg.Core)
		if err != nil {
			t.Fatalf("opening drivers/nats directly: %v", err)
		}
		defer func() { _ = direct.Close() }()
		compareCapabilities(t, "core", openFacade(t, cfg).Capabilities(), direct.Capabilities())
	})
	t.Run("jetstream", func(t *testing.T) {
		cfg := jsConfig(t)
		// The same stream, so the two clients read the same facts: this is a
		// comparison of two routes to one deployment and not of two
		// deployments.
		direct, err := jetstream.Open(t.Context(), cfg.JetStream)
		if err != nil {
			t.Fatalf("opening drivers/jetstream directly: %v", err)
		}
		defer func() { _ = direct.Close() }()
		compareCapabilities(t, "jetstream", openFacade(t, cfg).Capabilities(), direct.Capabilities())
	})
}

// compareCapabilities fails for every feature on which the two tables differ.
func compareCapabilities(tb testing.TB, profile string, facade, direct mq.Capabilities) {
	tb.Helper()
	for i := range mq.FeatureCount {
		f := mq.Feature(i)
		a, b := facade.Support(f), direct.Support(f)
		if a.Level != b.Level || a.Scope != b.Scope || a.Note != b.Note {
			tb.Errorf("%s: %v differs: through the facade %v/%v %q, through the driver %v/%v %q",
				profile, f, a.Level, a.Scope, a.Note, b.Level, b.Scope, b.Note)
		}
		if facade.Has(f) != direct.Has(f) {
			tb.Errorf("%s: %v: facade Has()=%v, driver Has()=%v", profile, f, facade.Has(f), direct.Has(f))
		}
	}
	if facade.Limits() != direct.Limits() {
		tb.Errorf("%s: limits differ: facade %+v, driver %+v", profile, facade.Limits(), direct.Limits())
	}
}

// TestCapabilitiesAreNeitherTheUnionNorTheIntersection is the other half, and it is the check that would catch a future facade that
// grew a table of its own.
//
// The two features it names are the ones that make the union and the
// intersection both false, so a table that is either is caught here:
//
//   - FeatureExplicitAck is native on JetStream and unsupported on Core. The
//     UNION would claim it on Core, where Source.Settler() is nil and every Ack
//     answers ErrNotSettleable.
//   - The INTERSECTION would withdraw it on JetStream, where the broker really
//     does settle.
//
// It is stated as two assertions about the live tables rather than by
// constructing a union and comparing, because what matters is not that the
// facade computed something different — it is that each profile's answer is the
// one that deployment can honour.
func TestCapabilitiesAreNeitherTheUnionNorTheIntersection(t *testing.T) {
	core := openFacade(t, coreConfig(t)).Capabilities()
	js := openFacade(t, jsConfig(t)).Capabilities()

	for _, f := range []mq.Feature{
		mq.FeatureExplicitAck, mq.FeatureNack, mq.FeatureAtLeastOnce,
		mq.FeatureExtendLease, mq.FeatureReplay, mq.FeatureSeekBySequence,
		mq.FeatureDurableSubscriptions, mq.FeatureReceipts,
	} {
		if core.Has(f) {
			t.Errorf("the Core profile advertises %v; a facade that answered the UNION of the two tables would, and NATS Core cannot (Settler() is nil)", f)
		}
		if !js.Has(f) {
			t.Errorf("the JetStream profile does not advertise %v; a facade that answered the INTERSECTION would withdraw it, and JetStream honours it natively", f)
		}
	}
	// The mirror case: a Core claim JetStream does not make. The union would
	// give it to JetStream, where no AckNone consumer is ever opened.
	for _, f := range []mq.Feature{mq.FeatureAtMostOnce, mq.FeatureRequestReply} {
		if !core.Has(f) {
			t.Errorf("the Core profile does not advertise %v, which it has", f)
		}
		if js.Has(f) {
			t.Errorf("the JetStream profile advertises %v; only the UNION would, and this driver opens no AckNone consumer", f)
		}
	}
}

// TestCommonIsTheLiveIntersection holds [natsunified.Common] against the two
// tables a real deployment produces.
//
// Common is what a caller who selects a profile from a DSN at run time may rely
// on, so a hand-written list that drifted from the drivers would be the facade
// promising something one of them no longer has — or quietly withholding
// something both of them gained. The intersection is computed here rather than
// asserted as a literal, so a claim added to or removed from either driver fails
// this test rather than passing silently.
func TestCommonIsTheLiveIntersection(t *testing.T) {
	core := openFacade(t, coreConfig(t)).Capabilities()
	js := openFacade(t, jsConfig(t)).Capabilities()

	var live []mq.Feature
	for i := range mq.FeatureCount {
		if f := mq.Feature(i); core.Has(f) && js.Has(f) {
			live = append(live, f)
		}
	}
	got := natsunified.Common()
	if len(got) == 0 {
		t.Fatal("Common() is empty; a facade that guarantees nothing is not a facade")
	}
	if featureNames(live) != featureNames(got) {
		t.Errorf("Common() = %s\nlive intersection = %s\nCommon must be exactly the features both profiles advertise", featureNames(got), featureNames(live))
	}
	for _, f := range got {
		if !natsunified.CommonHas(f) {
			t.Errorf("CommonHas(%v) is false for a feature Common() lists", f)
		}
	}
	for _, f := range []mq.Feature{mq.FeatureExplicitAck, mq.FeatureAtMostOnce, mq.FeatureRequestReply, mq.FeatureDurableStorage} {
		if natsunified.CommonHas(f) {
			t.Errorf("CommonHas(%v) is true for a feature only one profile has", f)
		}
	}
}

// TestCommonSharesFeaturesAtDifferentLevels is the warning in [Common]'s
// documentation, made a check rather than a sentence.
//
// Two features are in Common and mean different things, and both are the kind of
// difference a caller would be caught by:
//
//   - BatchPublish is EmulatedVolatile on Core (one flush, nothing stored) and
//     EmulatedDurable on JetStream (every PubAck awaited). The specification makes volatile
//     emulation never satisfy a durability requirement, so the same
//     RequireAtLeast passes on one profile and fails on the other from a feature
//     both "have".
//   - PublisherConfirm is native on both, and the boundary is the socket on one
//     and the stream's storage on the other. The levels agree, so only the note
//     tells them apart — which is why compareCapabilities checks notes.
func TestCommonSharesFeaturesAtDifferentLevels(t *testing.T) {
	core := openFacade(t, coreConfig(t)).Capabilities()
	js := openFacade(t, jsConfig(t)).Capabilities()

	cb, jb := core.Support(mq.FeatureBatchPublish), js.Support(mq.FeatureBatchPublish)
	if cb.Level != mq.EmulatedVolatile {
		t.Errorf("Core BatchPublish is %v, want emulatedvolatile", cb.Level)
	}
	if jb.Level != mq.EmulatedDurable {
		t.Errorf("JetStream BatchPublish is %v, want emulateddurable", jb.Level)
	}
	if !natsunified.CommonHas(mq.FeatureBatchPublish) {
		t.Error("BatchPublish is not in Common(), so this test no longer says anything about a shared feature")
	}
	durable := mq.RequireAtLeast(mq.FeatureBatchPublish, mq.EmulatedDurable)
	if err := core.Require(durable); err == nil {
		t.Error("Core met RequireAtLeast(BatchPublish, EmulatedDurable); the specification forbids volatile emulation satisfying a durable requirement")
	}
	if err := js.Require(durable); err != nil {
		t.Errorf("JetStream did not meet RequireAtLeast(BatchPublish, EmulatedDurable): %v", err)
	}

	cc, jc := core.Support(mq.FeaturePublisherConfirm), js.Support(mq.FeaturePublisherConfirm)
	if cc.Level != mq.Native || jc.Level != mq.Native {
		t.Errorf("PublisherConfirm levels are %v and %v, want native on both", cc.Level, jc.Level)
	}
	if cc.Note == jc.Note {
		t.Errorf("both profiles state the same confirmation boundary %q; they are the socket and the disk and a caller reads the note to tell them apart", cc.Note)
	}
}

// featureNames renders a feature list for a failure message, sorted so that two lists
// compare by content and not by order.
func featureNames(fs []mq.Feature) string {
	out := make([]string, 0, len(fs))
	for _, f := range fs {
		out = append(out, f.String())
	}
	sort.Strings(out)
	s := ""
	for i, n := range out {
		if i > 0 {
			s += " "
		}
		s += n
	}
	return s
}
