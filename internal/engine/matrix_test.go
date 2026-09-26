package engine

import (
	"os"
	"slices"
	"strings"
	"testing"

	"github.com/iqhive/iqmq/internal/hash"
	"github.com/iqhive/iqmq/internal/pool"
	"github.com/iqhive/iqmq/internal/registry"
	"github.com/iqhive/iqmq/internal/stats"
	"github.com/iqhive/iqmq/internal/timer"
	"github.com/iqhive/iqmq/internal/wait"
)

// matrixMode selects how wide the kind matrix is: one family varied at a time
// (the default, about thirty sets), every pair of families, or the full
// cartesian product. It is read from ENGINE_MATRIX.
func matrixMode() string {
	switch os.Getenv("ENGINE_MATRIX") {
	case "pairwise":
		return "pairwise"
	case "full":
		return "full"
	default:
		return "one"
	}
}

// referenceKinds is the set every matrix varies from: the Safe kind of every
// family, which is the mutex-backed reference implementation.
func referenceKinds() KindSet {
	ks := SafeKinds
	ks.Safe = false
	return ks
}

// family describes one variant family of the engine as a setter over KindSet,
// plus a reader for the kind a set ends up running.
//
// get reads the RESOLVED set, never the fields a caller wrote: the API contract
// the covering criterion is stated over the kinds the engine actually
// runs, and an Auto field or a Safe override names a different kind than the
// struct literal does.
type family struct {
	name string
	n    int
	set  func(*KindSet, int)
	get  func(KindSet) string
}

// families lists the engine's variant families in KindSet order. It is the
// single enumeration the criterion is computed from, so a tenth family or
// one more kind of an existing family widens the criterion with no edit here
// beyond the row itself.
func families() []family {
	return []family{
		{"arch", len(Arches()), func(k *KindSet, i int) { k.Arch = Arches()[i] }, func(k KindSet) string { return k.Resolve().Arch.String() }},
		{"queue", len(QueueKinds()), func(k *KindSet, i int) { k.Queue = QueueKinds()[i] }, func(k KindSet) string { return k.Resolve().Queue.String() }},
		{"wait", len(wait.Kinds()), func(k *KindSet, i int) { k.Wait = wait.Kinds()[i] }, func(k KindSet) string { return k.Resolve().Wait.String() }},
		{"timer", len(timer.Kinds()), func(k *KindSet, i int) { k.Timer = timer.Kinds()[i] }, func(k KindSet) string { return k.Resolve().Timer.String() }},
		{"hash", len(hash.Kinds()), func(k *KindSet, i int) { k.Hash = hash.Kinds()[i] }, func(k KindSet) string { return k.Resolve().Hash.String() }},
		{"pool", len(pool.Kinds()), func(k *KindSet, i int) { k.Pool = pool.Kinds()[i] }, func(k KindSet) string { return k.Resolve().Pool.String() }},
		{"registry", len(registry.Kinds()), func(k *KindSet, i int) { k.Registry = registry.Kinds()[i] }, func(k KindSet) string { return k.Resolve().Registry.String() }},
		{"stats", len(stats.Kinds()), func(k *KindSet, i int) { k.Stats = stats.Kinds()[i] }, func(k KindSet) string { return k.Resolve().Stats.String() }},
		{"retry", len(RetryLanes()), func(k *KindSet, i int) { k.RetryLane = RetryLanes()[i] }, func(k KindSet) string { return k.Resolve().RetryLane.String() }},
	}
}

// kindMatrix returns the kind sets the matrix mode asks for. The reference
// set and its Safe variant are always first.
func kindMatrix(mode string) []KindSet {
	fams := families()
	base := referenceKinds()
	safe := base
	safe.Safe = true
	out := []KindSet{base, safe}
	seen := map[string]bool{base.String(): true, safe.String(): true}
	add := func(ks KindSet) {
		if s := ks.String(); !seen[s] {
			seen[s] = true
			out = append(out, ks)
		}
	}
	for fi, f := range fams {
		for i := range f.n {
			ks := base
			f.set(&ks, i)
			add(ks)
			if mode == "one" {
				continue
			}
			for gj := fi + 1; gj < len(fams); gj++ {
				g := fams[gj]
				for j := range g.n {
					ks2 := ks
					g.set(&ks2, j)
					add(ks2)
				}
			}
		}
	}
	if mode != "full" {
		return out
	}
	return fullMatrix(fams)
}

// fullMatrix enumerates the cartesian product of every family.
func fullMatrix(fams []family) []KindSet {
	sets := []KindSet{referenceKinds()}
	for _, f := range fams {
		next := make([]KindSet, 0, len(sets)*f.n)
		for _, ks := range sets {
			for i := range f.n {
				k := ks
				f.set(&k, i)
				next = append(next, k)
			}
		}
		sets = next
	}
	return sets
}

// topology is one slice-4 topology profile the canonical script runs on. The
// script is the same for all of them: an operation a topology does not offer
// is a no-op, exactly as the engine ignores a field it does not read, so the
// matrix multiplies the kind sets by the topologies rather than by a second
// set of scripts.
type topology struct {
	name  string
	apply func(*harnessConfig)
}

// topologies lists the topology profiles the matrix sweeps.
func topologies() []topology {
	return []topology{
		{"base", func(*harnessConfig) {}},
		{"stream", func(hc *harnessConfig) {
			hc.stream = true
			hc.retention = Retention{MaxMessages: 4}
		}},
		{"priority", func(hc *harnessConfig) { hc.priority = true }},
		{"wildcards", func(hc *harnessConfig) { hc.wildcards = true }},
		{"dedup", func(hc *harnessConfig) { hc.dedup = 8 * tick }},
		{"sticky", func(hc *harnessConfig) { hc.sticky = true }},
		{"stream+wildcards", func(hc *harnessConfig) {
			hc.stream = true
			hc.retention = Retention{MaxMessages: 4}
			hc.wildcards = true
		}},
		{"stream+priority", func(hc *harnessConfig) {
			hc.stream = true
			hc.retention = Retention{MaxMessages: 4, MaxAge: 32 * tick}
			hc.priority = true
		}},
	}
}

// runsTopology says whether the matrix mode runs the kind set at index i on
// the topology named top.
//
// In the narrow mode only the first two sets — the reference set and its Safe
// variant — carry the whole topology sweep; the rest run the base topology,
// which is what "one family at a time" means for the kinds. Every other mode
// crosses every kind set with every topology, which is the half of
// the test suite's criterion that is about topologies.
//
// It is a function rather than a condition inside the loop so that the
// criterion check can ask it the same question without running the matrix.
func runsTopology(mode string, i int, top string) bool {
	return mode != "one" || i <= 1 || top == "base"
}

// TestEngineMatrix runs the differential script against the model for every
// kind set the matrix mode asks for, on every slice-4 topology. The default
// mode varies one family at a time and sweeps the topologies on the reference
// set only; ENGINE_MATRIX=pairwise and ENGINE_MATRIX=full sweep every
// topology for every kind set.
func TestEngineMatrix(t *testing.T) {
	mode := matrixMode()
	sets := kindMatrix(mode)
	tops := topologies()
	t.Logf("engine matrix %s: %d kind sets x %d topologies", mode, len(sets), len(tops))
	for i, ks := range sets {
		for _, top := range tops {
			if !runsTopology(mode, i, top.name) {
				continue
			}
			t.Run(ks.String()+"/topology="+top.name, func(t *testing.T) {
				t.Parallel()
				hc := scriptConfig(ks)
				top.apply(&hc)
				runScriptOn(t, hc)
			})
		}
	}
}

// TestKindMatrixShape checks the matrix generator itself: the reference set is
// first, no set repeats, and every kind of every family appears somewhere.
func TestKindMatrixShape(t *testing.T) {
	sets := kindMatrix("one")
	if len(sets) < 30 {
		t.Fatalf("one-at-a-time matrix has only %d sets", len(sets))
	}
	if !sets[1].Safe {
		t.Fatalf("the safe reference set is not in the matrix: %v", sets[1])
	}
	seen := map[string]bool{}
	for _, ks := range sets {
		if seen[ks.String()] {
			t.Fatalf("duplicate kind set %v", ks)
		}
		seen[ks.String()] = true
	}
	for _, k := range QueueKinds() {
		found := false
		for _, ks := range sets {
			if ks.Resolve().Queue == k {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("queue kind %v is in no matrix entry", k)
		}
	}
	for _, a := range Arches() {
		found := false
		for _, ks := range sets {
			if ks.Resolve().Arch == a {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("architecture %v is in no matrix entry", a)
		}
	}
	for _, k := range RetryLanes() {
		found := false
		for _, ks := range sets {
			if ks.Resolve().RetryLane == k {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("retry placement %v is in no matrix entry", k)
		}
	}
	if n := len(kindMatrix("pairwise")); n <= len(sets) {
		t.Fatalf("pairwise matrix (%d) is no wider than one-at-a-time (%d)", n, len(sets))
	}
}

// pairObligation is one 2-wise obligation of the test suite's engine
// covering criterion: two distinct variant families, and one kind of each.
type pairObligation struct {
	f, fk string
	g, gk string
}

// criterionPairs returns every 2-wise obligation the covering criterion
// demands. It is COMPUTED from families(), not written down: for each
// unordered pair of distinct families and each pair of their kinds, the kind
// set that selects both, read back through the family's own resolver. A new
// family or a new kind widens the criterion with no edit here.
func criterionPairs() map[pairObligation]bool {
	fams := families()
	base := referenceKinds()
	want := make(map[pairObligation]bool)
	for fi := range fams {
		for gj := fi + 1; gj < len(fams); gj++ {
			for i := range fams[fi].n {
				for j := range fams[gj].n {
					ks := base
					fams[fi].set(&ks, i)
					fams[gj].set(&ks, j)
					want[pairObligation{
						f: fams[fi].name, fk: fams[fi].get(ks),
						g: fams[gj].name, gk: fams[gj].get(ks),
					}] = true
				}
			}
		}
	}
	return want
}

// coveredPairs returns the obligations a list of kind sets discharges.
//
// A Safe set is not counted. Safe resolves every family to its reference kind
// whatever the other fields say, so counting it would let the one
// configuration that contains no lock-free data structure at all stand in for
// the ones that do. It is the differential oracle, not a covered point.
func coveredPairs(sets []KindSet) map[pairObligation]bool {
	fams := families()
	got := make(map[pairObligation]bool)
	for _, ks := range sets {
		if ks.Safe {
			continue
		}
		for fi := range fams {
			for gj := fi + 1; gj < len(fams); gj++ {
				got[pairObligation{
					f: fams[fi].name, fk: fams[fi].get(ks),
					g: fams[gj].name, gk: fams[gj].get(ks),
				}] = true
			}
		}
	}
	return got
}

// configurations counts the (kind set, topology) pairs a matrix mode runs,
// which is what the specification counts a configuration as.
func configurations(mode string) int {
	n := 0
	for i := range kindMatrix(mode) {
		for _, top := range topologies() {
			if runsTopology(mode, i, top.name) {
				n++
			}
		}
	}
	return n
}

// TestMatrixMeetsCoveringCriterion holds the matrix to the test suite
// rather than to a sentence about it.
//
// the seventh clause used to say the memory engine passes its checks "for
// every engine combination". It cannot: nine families of 4, 7, 7, 6, 5, 5, 4,
// 3 and 3 kinds are 1,058,400 kind sets and, across eight topology profiles,
// 8,467,200 configurations, where the 2-wise arm is 4,576 and about five
// seconds under -race. The specification replaced the universal claim with a criterion,
// and a criterion that is only written down is a claim like any other. This
// test computes the required set and fails when the matrix does not cover it.
//
// It checks both halves of the criterion — the 2-wise obligations over the
// kind families, and the exhaustive cross with the topology profiles — and
// then checks that the criterion can fail at all, because a covering check
// that nothing can miss is this repository's worst failure mode.
func TestMatrixMeetsCoveringCriterion(t *testing.T) {
	fams := families()

	// The criterion is computed from families() and so is the matrix, so
	// dropping a family would narrow BOTH and the covering check would still
	// pass — an oracle that agrees with its subject by construction, which is
	// the check-that-cannot-fail this repository has shipped before.
	// The nine families are therefore written down here,
	// in KindSet order, and the specification names the same nine. Adding a KIND is meant
	// to widen the criterion with no edit; adding or removing a FAMILY is a
	// change to what the seventh clause promises and must be made twice.
	wantFamilies := []string{"arch", "queue", "wait", "timer", "hash", "pool", "registry", "stats", "retry"}
	names := make([]string, 0, len(fams))
	for _, f := range fams {
		names = append(names, f.name)
		if f.n < 2 {
			t.Errorf("family %s has %d kinds: a family with one kind discharges no pair", f.name, f.n)
		}
	}
	if !slices.Equal(names, wantFamilies) {
		t.Fatalf("variant families are %v, the test suite names %v", names, wantFamilies)
	}

	want := criterionPairs()

	// The criterion demands something, and the enumeration counted it right.
	// sum(n_i*n_j) over i<j is ((sum n)^2 - sum n^2)/2, which is a different
	// computation over the same family sizes: it catches a pair loop that
	// skips a family or counts one twice, and it catches two kinds of one
	// family that render as the same name, which would silently collapse two
	// obligations into one.
	sum, sq := 0, 0
	for _, f := range fams {
		sum += f.n
		sq += f.n * f.n
	}
	if n := (sum*sum - sq) / 2; len(want) != n {
		t.Fatalf("criterion enumerates %d pair obligations, the family sizes say %d", len(want), n)
	}
	if len(want) == 0 {
		t.Fatal("the covering criterion demands nothing")
	}

	// The kinds half: the pairwise matrix discharges every obligation.
	got := coveredPairs(kindMatrix("pairwise"))
	var missing []string
	for p := range want {
		if !got[p] {
			missing = append(missing, p.f+"="+p.fk+" with "+p.g+"="+p.gk)
		}
	}
	if len(missing) > 0 {
		slices.Sort(missing)
		shown := missing
		if len(shown) > 8 {
			shown = shown[:8]
		}
		t.Fatalf("the pairwise matrix leaves %d of %d criterion pairs uncovered: %s",
			len(missing), len(want), strings.Join(shown, "; "))
	}

	// The topologies half: the criterion crosses every kind set with every
	// topology profile, exhaustively rather than pairwise, so the pairwise
	// mode must run all of them.
	sets := kindMatrix("pairwise")
	tops := topologies()
	for i := range sets {
		for _, top := range tops {
			if !runsTopology("pairwise", i, top.name) {
				t.Fatalf("pairwise mode skips kind set %d (%v) on topology %s", i, sets[i], top.name)
			}
		}
	}
	if n, full := configurations("pairwise"), len(sets)*len(tops); n != full {
		t.Fatalf("pairwise mode runs %d configurations, the full cross is %d", n, full)
	}
	t.Logf("criterion: %d pair obligations; pairwise runs %d kind sets x %d topologies = %d configurations",
		len(want), len(sets), len(tops), configurations("pairwise"))

	// The criterion must be able to go unmet, or covering it says nothing.
	// The default narrow mode is the standing counter-example: varying one
	// family at a time discharges only the pairs in which at least one family
	// holds its reference kind, and it sweeps the topologies on the two
	// reference sets alone.
	narrow := coveredPairs(kindMatrix("one"))
	short := 0
	for p := range want {
		if !narrow[p] {
			short++
		}
	}
	if short == 0 {
		t.Fatal("the one-family-at-a-time matrix already satisfies the 2-wise criterion, so covering it distinguishes nothing")
	}
	t.Logf("default narrow mode leaves %d of %d pairs uncovered and runs %d configurations",
		short, len(want), configurations("one"))
}
