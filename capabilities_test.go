package iqmq_test

import (
	"errors"
	"strings"
	"testing"
	"time"

	mq "bitbucket.org/iqhive/iqmq"
	"bitbucket.org/iqhive/iqmq/internal/fuzzutil"
)

// TestLimitUnknownIsNotZero is the rule the specification states and never
// measured: "unknown is never an overloaded zero".
//
// The specification prototyped Support only, so nothing in the bake-off exercised
// Limit[T] at all (docs/api-freeze.md says so and warns not to read the row as covering it). The rule therefore gets its own test rather than
// riding along with a benchmark: a Limit whose Known is false and one whose
// Known is true with Value zero are different answers, and no caller may
// confuse them.
func TestLimitUnknownIsNotZero(t *testing.T) {
	unknown := mq.Limit[int64]{Value: 0, Known: false}
	zero := mq.Limit[int64]{Value: 0, Known: true}

	if unknown == zero {
		t.Fatal("an unadvertised limit and an advertised limit of zero are the same value: " +
			"Limit's Known field is decoration")
	}
	if unknown.Value != zero.Value {
		t.Fatal("the fixture is wrong: both limits must carry value zero, or the test proves nothing")
	}
	if unknown.Known {
		t.Fatal("an unadvertised limit reported Known")
	}
	if !zero.Known {
		t.Fatal("an advertised limit of zero did not report Known")
	}

	t.Run("CapabilitiesKeepTheDistinction", func(t *testing.T) {
		// A driver that advertises a zero limit and one that advertises
		// nothing must be distinguishable through Capabilities, not only in
		// the struct the driver filled in.
		var silent mq.Capabilities
		var strict mq.Capabilities
		strict.SetLimits(mq.Limits{
			MaxMessageBytes:  mq.Limit[int64]{Value: 0, Known: true},
			MaxBatchMessages: mq.Limit[int]{Value: 0, Known: true},
			MaxDelay:         mq.Limit[time.Duration]{Value: 0, Known: true},
		})
		for _, key := range []mq.LimitKey{
			mq.LimitMaxMessageBytes, mq.LimitMaxBatchMessages, mq.LimitMaxDelay,
		} {
			s, x := silent.Limit(key), strict.Limit(key)
			if s.Known {
				t.Errorf("%v: a driver that advertised nothing reported Known", key)
			}
			if !x.Known {
				t.Errorf("%v: a driver that advertised zero did not report Known", key)
			}
			if s == x {
				t.Errorf("%v: the two answers are indistinguishable (%+v)", key, s)
			}
		}
	})

	t.Run("ZeroCapabilitiesAdvertiseNothing", func(t *testing.T) {
		var zero mq.Capabilities
		for k := mq.LimitKey(0); int(k) < mq.LimitKeyCount; k++ {
			if got := zero.Limit(k); got.Known {
				t.Errorf("%v on the zero Capabilities = %+v, want unknown", k, got)
			}
		}
		if l := zero.Limits(); l != (mq.Limits{}) {
			t.Errorf("Limits on the zero Capabilities = %+v, want every limit unknown", l)
		}
	})

	t.Run("EveryLimitKeyIsReachable", func(t *testing.T) {
		// A key the accessor forgot would silently answer "unknown" for a
		// limit the driver did advertise, which is exactly the confusion the
		// rule forbids. Each key is set to a distinct non-zero value and
		// read back, so a missing case in the switch fails here.
		var caps mq.Capabilities
		caps.SetLimits(mq.Limits{
			MaxMessageBytes:  mq.Limit[int64]{Value: 11, Known: true},
			MaxBatchMessages: mq.Limit[int]{Value: 22, Known: true},
			MaxBatchBytes:    mq.Limit[int64]{Value: 33, Known: true},
			MaxHeaderCount:   mq.Limit[int]{Value: 44, Known: true},
			MaxHeaderBytes:   mq.Limit[int64]{Value: 55, Known: true},
			MaxDelay:         mq.Limit[time.Duration]{Value: 66, Known: true},
			MaxLease:         mq.Limit[time.Duration]{Value: 77, Known: true},
		})
		want := map[mq.LimitKey]int64{
			mq.LimitMaxMessageBytes:  11,
			mq.LimitMaxBatchMessages: 22,
			mq.LimitMaxBatchBytes:    33,
			mq.LimitMaxHeaderCount:   44,
			mq.LimitMaxHeaderBytes:   55,
			mq.LimitMaxDelay:         66,
			mq.LimitMaxLease:         77,
		}
		if len(want) != mq.LimitKeyCount {
			t.Fatalf("the table names %d keys, LimitKeyCount is %d", len(want), mq.LimitKeyCount)
		}
		for k, v := range want {
			got := caps.Limit(k)
			if !got.Known || got.Value != v {
				t.Errorf("Limit(%v) = %+v, want {%d true}", k, got, v)
			}
		}
		if got := caps.Limit(mq.LimitKey(99)); got.Known {
			t.Errorf("an out-of-range key = %+v, want unknown", got)
		}
	})
}

// TestCapabilities holds the rules on Has, Support and Set.
func TestCapabilities(t *testing.T) {
	t.Run("ZeroValueAdvertisesNothing", func(t *testing.T) {
		var caps mq.Capabilities
		for f := mq.Feature(0); int(f) < mq.FeatureCount; f++ {
			if caps.Has(f) {
				t.Fatalf("the zero Capabilities has %v", f)
			}
			if got := caps.Support(f); got != (mq.Support{}) {
				t.Fatalf("the zero Capabilities reports %+v for %v", got, f)
			}
		}
	})

	t.Run("HasNeverPanics", func(t *testing.T) {
		// The specification target 9: "Has never panics". An unknown feature is
		// unsupported.
		var caps mq.Capabilities
		if err := caps.Set(mq.FeatureExplicitAck, mq.Support{Level: mq.Native}); err != nil {
			t.Fatal(err)
		}
		for _, f := range []mq.Feature{mq.Feature(mq.FeatureCount), mq.Feature(9999), ^mq.Feature(0)} {
			if caps.Has(f) {
				t.Fatalf("an out-of-inventory feature %d is supported", f)
			}
			if got := caps.Support(f); got != (mq.Support{}) {
				t.Fatalf("an out-of-inventory feature %d reports %+v", f, got)
			}
		}
	})

	t.Run("SetRejectsWhatItCannotAnswerLater", func(t *testing.T) {
		var caps mq.Capabilities
		bad := []struct {
			name string
			f    mq.Feature
			s    mq.Support
		}{
			{"feature", mq.Feature(mq.FeatureCount), mq.Support{Level: mq.Native}},
			{"level", mq.FeatureNack, mq.Support{Level: mq.Level(99)}},
			{"scope", mq.FeatureNack, mq.Support{Level: mq.Native, Scope: mq.Scope(99)}},
		}
		for _, c := range bad {
			err := caps.Set(c.f, c.s)
			if !isCode(err, mq.CodeInvalidArgument) {
				t.Errorf("Set with a bad %s = %v, want CodeInvalidArgument: a capability set "+
					"that stored an unknown %s would answer requirements about it later", c.name, err, c.name)
			}
		}
	})

	t.Run("SupportCarriesTheCondition", func(t *testing.T) {
		// The specification: "conditional support names and tests its condition".
		var caps mq.Capabilities
		note := "only on a FIFO queue"
		if err := caps.Set(mq.FeatureOrderedByKey, mq.Support{
			Level: mq.Conditional, Scope: mq.ScopeKey, Note: note,
		}); err != nil {
			t.Fatal(err)
		}
		got := caps.Support(mq.FeatureOrderedByKey)
		if got.Note != note || got.Scope != mq.ScopeKey || got.Level != mq.Conditional {
			t.Fatalf("Support = %+v, want the level, scope and note the driver stated", got)
		}
		if !caps.Has(mq.FeatureOrderedByKey) {
			t.Fatal("conditional support is not Has")
		}
	})

	t.Run("LevelRankIsNotDeclarationOrder", func(t *testing.T) {
		// The specification: "Levels are categories. Rank() gives the documented ordinal
		// used by RequireAtLeast." Volatile emulation ranks BELOW conditional
		// support, which the constants' own order does not say.
		if mq.EmulatedVolatile <= mq.Conditional {
			t.Fatal("the fixture is wrong: EmulatedVolatile must declare after Conditional " +
				"for this test to say anything")
		}
		if mq.EmulatedVolatile.Rank() >= mq.Conditional.Rank() {
			t.Fatalf("Rank(EmulatedVolatile)=%d, Rank(Conditional)=%d: the specification ranks volatile emulation lower",
				mq.EmulatedVolatile.Rank(), mq.Conditional.Rank())
		}
		want := []mq.Level{mq.Unsupported, mq.EmulatedVolatile, mq.Conditional, mq.EmulatedDurable, mq.Native}
		for i := 1; i < len(want); i++ {
			if want[i-1].Rank() >= want[i].Rank() {
				t.Fatalf("Rank(%v)=%d is not below Rank(%v)=%d",
					want[i-1], want[i-1].Rank(), want[i], want[i].Rank())
			}
		}
		if mq.EmulatedVolatile.Durable() {
			t.Fatal("EmulatedVolatile reported Durable: the specification says it NEVER satisfies a durability requirement")
		}
		for _, l := range []mq.Level{mq.Native, mq.EmulatedDurable, mq.Conditional} {
			if !l.Durable() {
				t.Fatalf("%v did not report Durable", l)
			}
		}
		if mq.Level(99).Valid() || mq.Level(99).Rank() != 0 || mq.Level(99).String() != "level(?)" {
			t.Error("an out-of-range level is not handled")
		}
	})

	t.Run("FeatureNames", func(t *testing.T) {
		seen := map[string]mq.Feature{}
		for f := mq.Feature(0); int(f) < mq.FeatureCount; f++ {
			name := f.String()
			if name == "" || name == "Feature(?)" {
				t.Fatalf("feature %d has no name", f)
			}
			if prev, ok := seen[name]; ok {
				t.Fatalf("features %d and %d share the name %q", prev, f, name)
			}
			seen[name] = f
			back, ok := mq.ParseFeature(name)
			if !ok || back != f {
				t.Fatalf("ParseFeature(%q) = (%d, %v), want (%d, true)", name, back, ok, f)
			}
		}
		if mq.FeatureCount != 39 {
			t.Errorf("the inventory names 39 features; FeatureCount is %d", mq.FeatureCount)
		}
		if _, ok := mq.ParseFeature("NoSuchFeature"); ok {
			t.Error("ParseFeature accepted a name that is not in the inventory")
		}
	})
}

// TestRequirements holds the two requirement forms, including the rule
// that no ordinal can talk a volatile emulation into satisfying a durable
// requirement.
func TestRequirements(t *testing.T) {
	caps := func(l mq.Level, note string) mq.Capabilities {
		var c mq.Capabilities
		if err := c.Set(mq.FeatureScheduledPublish, mq.Support{Level: l, Note: note}); err != nil {
			t.Fatal(err)
		}
		return c
	}

	t.Run("AtLeastComparesRank", func(t *testing.T) {
		cases := []struct {
			have mq.Level
			min  mq.Level
			ok   bool
		}{
			{mq.Native, mq.Native, true},
			{mq.Native, mq.EmulatedDurable, true},
			{mq.EmulatedDurable, mq.Native, false},
			{mq.Conditional, mq.EmulatedVolatile, true},
			{mq.EmulatedVolatile, mq.Conditional, false},
			{mq.Unsupported, mq.EmulatedVolatile, false},
			{mq.Unsupported, mq.Unsupported, true},
		}
		for _, c := range cases {
			err := caps(c.have, "").Require(mq.RequireAtLeast(mq.FeatureScheduledPublish, c.min))
			if (err == nil) != c.ok {
				t.Errorf("have %v, RequireAtLeast %v: err = %v, want ok=%v", c.have, c.min, err, c.ok)
			}
		}
	})

	t.Run("VolatileNeverSatisfiesDurable", func(t *testing.T) {
		// Rank(EmulatedVolatile)=1 is below Rank(Conditional)=2, so the rank
		// comparison alone already refuses RequireAtLeast(EmulatedDurable).
		// The rule still has to be stated, because a future re-ranking that
		// put volatile emulation above conditional support would otherwise
		// let it satisfy a durable requirement silently.
		for _, min := range []mq.Level{mq.Conditional, mq.EmulatedDurable, mq.Native} {
			if !min.Durable() {
				continue
			}
			err := caps(mq.EmulatedVolatile, "in-process timer").
				Require(mq.RequireAtLeast(mq.FeatureScheduledPublish, min))
			if err == nil {
				t.Fatalf("volatile emulation satisfied RequireAtLeast(%v): the specification says it never does", min)
			}
			if !errors.Is(err, mq.ErrUnsupported) {
				t.Fatalf("the failure does not match ErrUnsupported: %v", err)
			}
			if !strings.Contains(err.Error(), "in-process timer") {
				t.Fatalf("the failure dropped the condition the driver stated: %v", err)
			}
		}
	})

	t.Run("AnyIsCategorical", func(t *testing.T) {
		req := mq.RequireAny(mq.FeatureScheduledPublish, mq.Native, mq.EmulatedDurable)
		for _, l := range []mq.Level{mq.Native, mq.EmulatedDurable} {
			if err := caps(l, "").Require(req); err != nil {
				t.Errorf("RequireAny accepted set did not accept %v: %v", l, err)
			}
		}
		for _, l := range []mq.Level{mq.Unsupported, mq.Conditional, mq.EmulatedVolatile} {
			if err := caps(l, "").Require(req); err == nil {
				t.Errorf("RequireAny accepted %v, which is not in its set", l)
			}
		}
		// RequireAny is not an ordinal: a set that names only the middle
		// level rejects the one above it.
		mid := mq.RequireAny(mq.FeatureScheduledPublish, mq.EmulatedDurable)
		if err := caps(mq.Native, "").Require(mid); err == nil {
			t.Error("RequireAny(EmulatedDurable) accepted Native: the form is categorical, not a threshold")
		}
	})

	t.Run("InvalidRequirementsAreReported", func(t *testing.T) {
		c := caps(mq.Native, "")
		bad := []struct {
			name string
			req  mq.Requirement
		}{
			{"zero value", mq.Requirement{}},
			{"empty acceptance set", mq.RequireAny(mq.FeatureScheduledPublish)},
			{"unknown feature", mq.RequireAtLeast(mq.Feature(9999), mq.Native)},
			{"unknown level", mq.RequireAtLeast(mq.FeatureScheduledPublish, mq.Level(99))},
			{"only invalid levels", mq.RequireAny(mq.FeatureScheduledPublish, mq.Level(99))},
		}
		for _, b := range bad {
			err := c.Require(b.req)
			if !isCode(err, mq.CodeInvalidArgument) {
				t.Errorf("Require(%s) = %v, want CodeInvalidArgument: an empty acceptance set "+
					"accepts nothing, which is never what a caller meant", b.name, err)
			}
		}
	})

	t.Run("FirstFailureWins", func(t *testing.T) {
		var c mq.Capabilities
		if err := c.Set(mq.FeatureNack, mq.Support{Level: mq.Native}); err != nil {
			t.Fatal(err)
		}
		err := c.Require(
			mq.RequireAtLeast(mq.FeatureNack, mq.Native),
			mq.RequireAtLeast(mq.FeatureReplay, mq.Native),
			mq.RequireAtLeast(mq.FeaturePriority, mq.Native),
		)
		if err == nil || !strings.Contains(err.Error(), "Replay") {
			t.Fatalf("Require reported %v, want the first failing requirement (Replay)", err)
		}
		if err := c.Require(); err != nil {
			t.Fatalf("Require with no requirements = %v, want nil", err)
		}
	})

	t.Run("StringNamesTheForm", func(t *testing.T) {
		if got := mq.RequireAtLeast(mq.FeatureNack, mq.Native).String(); got != "RequireAtLeast(Nack, native)" {
			t.Errorf("String = %q", got)
		}
		got := mq.RequireAny(mq.FeatureNack, mq.Native, mq.Conditional).String()
		if got != "RequireAny(Nack, conditional, native)" {
			t.Errorf("String = %q", got)
		}
		if got := mq.RequireAtLeast(mq.FeatureNack, mq.Native).Feature(); got != mq.FeatureNack {
			t.Errorf("Feature = %v", got)
		}
	})
}

// FuzzCapabilityRequirements is target 9, re-homed to the root from
// x/apibench/b7: acceptance and rejection are correct and Has never panics,
// over arbitrary capability sets and arbitrary requirements.
//
// The verdict is checked against an independent restatement of the two
// acceptance rules, so a Require that agreed with itself still fails.
func FuzzCapabilityRequirements(f *testing.F) {
	levels := []mq.Level{
		mq.Unsupported, mq.Conditional, mq.EmulatedVolatile, mq.EmulatedDurable, mq.Native,
		mq.Level(99), // out of range, to prove Set refuses it and Has survives it
	}
	scopes := []mq.Scope{mq.ScopeNone, mq.ScopeKey, mq.ScopePartition, mq.ScopeGroup, mq.ScopeTopic}
	seed := func(nSet, nReq int) []byte {
		w := fuzzutil.NewWriter()
		w.Int(0, 8, nSet)
		for i := range nSet {
			w.Int(0, mq.FeatureCount, i)
			w.Choice(len(levels), i%len(levels))
			w.Choice(len(scopes), i%len(scopes))
			w.Bool(i%2 == 0)
		}
		w.Int(0, 8, nReq)
		for i := range nReq {
			w.Int(0, mq.FeatureCount, i)
			w.Bool(i%2 == 0)
			w.Choice(len(levels), i%len(levels))
			w.Int(0, 31, 1<<(i%5))
		}
		return w.Data()
	}
	f.Add(seed(0, 1))
	f.Add(seed(3, 3))
	f.Add(seed(8, 8))
	f.Fuzz(func(t *testing.T, data []byte) {
		r := fuzzutil.NewReader(data)
		var caps mq.Capabilities
		// model[f] is the support the capability set should hold for f, by the rule that Set stores only what it can answer later.
		model := map[mq.Feature]mq.Support{}
		nSet := r.Int(0, 8)
		for range nSet {
			// The feature index reaches one past the inventory, so an
			// out-of-range feature is reachable.
			feat := mq.Feature(r.Int(0, mq.FeatureCount))
			sup := mq.Support{
				Level: levels[r.Choice(len(levels))],
				Scope: scopes[r.Choice(len(scopes))],
			}
			if r.Bool() {
				sup.Note = "a stated condition"
			}
			err := caps.Set(feat, sup)
			valid := int(feat) < mq.FeatureCount && sup.Level.Valid() && sup.Scope.Valid()
			if (err == nil) != valid {
				t.Fatalf("Set(%v, %+v) = %v, want an error only for an invalid argument (valid=%v)",
					feat, sup, err, valid)
			}
			if valid {
				model[feat] = sup
			}
		}
		// Has and Support never panic, for every feature and a few beyond.
		for i := 0; i <= mq.FeatureCount+2; i++ {
			feat := mq.Feature(i)
			want := model[feat]
			if got := caps.Support(feat); got != want {
				t.Fatalf("Support(%v) = %+v, want %+v", feat, got, want)
			}
			if got, wantHas := caps.Has(feat), want.Level != mq.Unsupported; got != wantHas {
				t.Fatalf("Has(%v) = %v, want %v", feat, got, wantHas)
			}
		}
		nReq := r.Int(0, 8)
		for range nReq {
			feat := mq.Feature(r.Int(0, mq.FeatureCount))
			ordinal := r.Bool()
			min := levels[r.Choice(len(levels))]
			mask := r.Int(0, 31)
			var req mq.Requirement
			if ordinal {
				req = mq.RequireAtLeast(feat, min)
			} else {
				var accept []mq.Level
				for i, l := range levels {
					if i < 5 && mask&(1<<i) != 0 {
						accept = append(accept, l)
					}
				}
				req = mq.RequireAny(feat, accept...)
			}
			got := caps.Require(req)
			want := wantRequirement(model[feat], feat, ordinal, min, mask)
			if (got == nil) != (want == nil) {
				t.Fatalf("Require(%v) against %+v = %v, the rules say %v",
					req, model[feat], got, want)
			}
			if want != nil && mq.CodeOf(got) != mq.CodeOf(want) {
				t.Fatalf("Require(%v) reported %v (code %v), the rules say code %v",
					req, got, mq.CodeOf(got), mq.CodeOf(want))
			}
		}
	})
}

// wantRequirement restates the acceptance rules independently of
// requirements.go, and returns a stand-in error carrying the code the failure
// must have.
func wantRequirement(have mq.Support, f mq.Feature, ordinal bool, min mq.Level, mask int) error {
	if int(f) >= mq.FeatureCount {
		return mq.ErrInvalidTopic // any CodeInvalidArgument stand-in
	}
	if ordinal {
		if !min.Valid() {
			return mq.ErrInvalidTopic
		}
		// The rule, restated: a durable requirement is never met by a level
		// that is not durable, and otherwise the ranks decide.
		if min.Durable() && !have.Level.Durable() {
			return mq.ErrUnsupported
		}
		if have.Level.Rank() < min.Rank() {
			return mq.ErrUnsupported
		}
		return nil
	}
	accept := mask & 31
	if accept == 0 {
		return mq.ErrInvalidTopic
	}
	if !have.Level.Valid() || accept&(1<<have.Level) == 0 {
		return mq.ErrUnsupported
	}
	return nil
}
