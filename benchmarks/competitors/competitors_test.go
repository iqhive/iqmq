package competitors_test

import (
	"os"
	"strings"
	"testing"

	"github.com/iqhive/iqmq/benchmarks/competitors"
	"github.com/iqhive/iqmq/benchmarks/opbench"
)

// TestEveryFamilyStatesItsSemanticsAndItsProof is the check that makes the
// published page's central claim checkable: a comparison whose semantics are
// not stated is not a comparison, and "we configured it" is not the same
// statement as "we checked it".
func TestEveryFamilyStatesItsSemanticsAndItsProof(t *testing.T) {
	for _, f := range competitors.Families() {
		t.Run(f.Name, func(t *testing.T) {
			if len(f.Semantics) < 40 {
				t.Fatalf("Semantics is %q, which does not state a delivery guarantee or a confirmation boundary", f.Semantics)
			}
			if len(f.Proof) < 40 {
				t.Fatalf("Proof is %q; configuring an arm and checking it are different statements", f.Proof)
			}
			if f.Semantics == f.Proof {
				t.Fatal("Semantics and Proof are the same string")
			}
			if len(competitors.ArmsFor(f.Name)) < 2 {
				t.Fatalf("the %s family has %d arm(s); a family with fewer than two is not a comparison",
					f.Name, len(competitors.ArmsFor(f.Name)))
			}
		})
	}
}

// TestEveryArmStatesWhatItConfigured pins the other half: an arm that reached
// the family's semantics by accident cannot be checked by a reader.
func TestEveryArmStatesWhatItConfigured(t *testing.T) {
	seen := map[string]bool{}
	for _, a := range competitors.Arms() {
		if seen[a.Name()] {
			t.Fatalf("two arms are called %s", a.Name())
		}
		seen[a.Name()] = true
		if len(a.Config) < 40 {
			t.Fatalf("%s: Config is %q, which does not say what was configured", a.Name(), a.Config)
		}
		if a.Version == "" {
			t.Fatalf("%s: no pinned version", a.Name())
		}
		if a.Open == nil {
			t.Fatalf("%s: no Open", a.Name())
		}
		if !contains(competitors.Libraries(), a.Library) {
			t.Fatalf("%s: %q is not one of the four library names %v", a.Name(), a.Library, competitors.Libraries())
		}
	}
}

// TestPinnedVersionsMatchGoMod is why competitors.Pinned() exists as literals:
// the published page reports a version, and a page that reports a version the
// run did not use is worse than a page with no version on it.
//
// It reads go.mod rather than the build info, because the build info of a test
// binary in a workspace does not name the module versions the campaign would
// resolve.
func TestPinnedVersionsMatchGoMod(t *testing.T) {
	data, err := os.ReadFile("go.mod")
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(string(data), "\n")
	got := map[string]string{}
	for _, line := range lines {
		fields := strings.Fields(strings.TrimSpace(line))
		if len(fields) < 2 || !strings.HasPrefix(fields[1], "v") {
			continue
		}
		got[fields[0]] = fields[1]
	}
	for mod, want := range competitors.Pinned() {
		have, ok := got[mod]
		if !ok {
			t.Fatalf("competitors.Pinned() names %s, which go.mod does not require", mod)
		}
		if have != want {
			t.Fatalf("competitors.Pinned() says %s %s and go.mod says %s; the published page would claim a "+
				"version the run did not use", mod, want, have)
		}
	}
	if len(competitors.PinnedList()) != len(competitors.Pinned()) {
		t.Fatal("PinnedList dropped an entry")
	}
}

// TestEveryExclusionNamesRealRows is the check that keeps Exclusions() honest.
//
// An exclusion for a row nobody measures is a reassurance rather than a
// disclosure, and an exclusion that matches nothing is how a comparison comes
// to look more careful than it is. Every exclusion must match at least one
// benchmark name this package actually produces -- except the one that says an
// arm does not exist, which must match NOTHING and says so.
func TestEveryExclusionNamesRealRows(t *testing.T) {
	names := competitors.Names()
	if len(names) == 0 {
		t.Fatal("competitors.Names() is empty")
	}
	for _, e := range competitors.Exclusions() {
		t.Run(e.Library+"/"+e.Family+"/"+e.Op, func(t *testing.T) {
			if len(e.Reason) < 60 {
				t.Fatalf("Reason is %q, which does not say why the comparison cannot be made equivalent", e.Reason)
			}
			matched := 0
			for _, n := range names {
				// BenchmarkCompare/<library>/<family>/<op>/<size>
				parts := strings.Split(n, "/")
				if len(parts) != 5 {
					t.Fatalf("%q is not BenchmarkCompare/<library>/<family>/<op>/<size>", n)
				}
				if reason, ok := competitors.Excluded(parts[1], parts[2], parts[3]); ok && reason == e.Reason {
					matched++
				}
			}
			if strings.Contains(e.Reason, "No such arm exists") {
				if matched != 0 {
					t.Fatalf("the exclusion says no such arm exists and it matched %d measured row(s)", matched)
				}
				// And there really is no such arm.
				for _, a := range competitors.Arms() {
					if a.Library == e.Library && (e.Family == "*" || a.Family == e.Family) {
						t.Fatalf("the exclusion says %s/%s does not exist and Arms() declares it", e.Library, e.Family)
					}
				}
				return
			}
			if matched == 0 {
				t.Fatalf("the exclusion matches none of the %d measured rows, so it discloses nothing", len(names))
			}
		})
	}
}

// TestWhatIsLeftComparedIsPublishOnly states the consequence of the exclusions
// in one place, so that the size of the honest table is a declared fact rather
// than something a reader has to derive.
//
// It is also a guard: adding an operation to opbench without deciding whether
// it is comparable would change this count, and the four withdrawn EndToEnd
// rows are what happens when that decision is made after publication rather
// than before.
func TestWhatIsLeftComparedIsPublishOnly(t *testing.T) {
	compared := map[string]int{}
	total := 0
	for _, n := range competitors.Names() {
		parts := strings.Split(n, "/")
		total++
		if _, ok := competitors.Excluded(parts[1], parts[2], parts[3]); ok {
			continue
		}
		compared[parts[3]]++
	}
	if total != 7*len(opbench.Ops())*len(opbench.Sizes()) {
		t.Fatalf("Names() produced %d rows for 7 arms, %d ops and %d sizes",
			total, len(opbench.Ops()), len(opbench.Sizes()))
	}
	// Publish for the nats family's four arms; PublishBatch for the three of
	// those with a client-level batch API (every arm but gocloud). The whole
	// in-process family is measured and not compared, because its arms
	// disagree on payload ownership , so nothing from it
	// counts here.
	want := map[string]int{
		"Publish":      4 * len(opbench.Sizes()),
		"PublishBatch": 3 * len(opbench.Sizes()),
	}
	for op, n := range want {
		if compared[op] != n {
			t.Fatalf("%s: %d compared row(s), want %d", op, compared[op], n)
		}
	}
	for op, n := range compared {
		if want[op] == 0 {
			t.Fatalf("%s has %d compared row(s) and is not one of the operations the specification budgets; "+
				"if an operation became comparable, say so in Exclusions() and here", op, n)
		}
	}
	if compared["Receive"] != 0 || compared["EndToEnd"] != 0 {
		t.Fatal("Receive or EndToEnd is being compared; both contain a round trip whose prefetch differs per arm")
	}
	// And nothing from the in-process family, at any operation.
	for _, n := range competitors.Names() {
		parts := strings.Split(n, "/")
		if parts[2] != "memory" {
			continue
		}
		if _, ok := competitors.Excluded(parts[1], parts[2], parts[3]); !ok {
			t.Fatalf("%s is compared; the in-process arms disagree on payload ownership, so none of them is", n)
		}
	}
}

// TestNamesAreCanonical checks the shape of every name this package
// declares, because the renderer and the run script both key on it.
func TestNamesAreCanonical(t *testing.T) {
	for _, a := range competitors.Arms() {
		want := opbench.Names("BenchmarkCompare", a.Name())
		for _, n := range want {
			if !strings.HasPrefix(n, "BenchmarkCompare/"+a.Library+"/"+a.Family+"/") {
				t.Fatalf("%q does not start with BenchmarkCompare/%s/%s/", n, a.Library, a.Family)
			}
		}
		if len(want) != len(opbench.Ops())*len(opbench.Sizes()) {
			t.Fatalf("%s produced %d names", a.Name(), len(want))
		}
	}
}

func contains(xs []string, s string) bool {
	for _, x := range xs {
		if x == s {
			return true
		}
	}
	return false
}
