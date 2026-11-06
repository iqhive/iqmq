package main

import (
	"path/filepath"
	"strings"
	"testing"
)

// listedNames is a -benchtime=1x listing of the in-process benchmarks the
// self-test's budgets document names: its specification budget, both `pr`
// patterns and its three waivers, each with the gate sets whose packages
// report it. It holds no ratio arm, because a listing starts no broker.
const listedNames = "BenchmarkHeaders/registry/Get/n=8-16 pr\n" +
	"BenchmarkHeaders/waived/Get/n=8-16 pr\n" +
	"BenchmarkMemory/PubSubAck/owned-16 pr\n" +
	"BenchmarkMemory/PubSubAck/waivedall/1P1C-16 pr\n" +
	"BenchmarkMemory/PubSubAck/waivedtime/1P1C-16 pr\n"

// lintAgainstListing runs `-lint -names` the way `make budgets-lint` does on a
// fresh clone: no stored campaign, so the listing is the only inventory.
func lintAgainstListing(t *testing.T, budgetsDoc, names string) (int, string) {
	t.Helper()
	dir := t.TempDir()
	cfg := writeFile(t, filepath.Join(dir, "budgets.yaml"), budgetsDoc)
	list := writeFile(t, filepath.Join(dir, "names.txt"), names)
	code, _, errOut := invoke(t, "-lint", "-budgets", cfg, "-names", list,
		"-results", filepath.Join(dir, "no-stored-campaign"), "-scorecards", emptyDir(t, dir, "scorecards"))
	return code, errOut
}

// TestLintChecksAMistypedMatchAgainstTheListing is tt-19. budgets-lint checked
// patterns only against a stored campaign, and the public tree stores none, so
// a mistyped `match:` passed the lint and then matched nothing at the gate,
// which reads the same as a budget that is met. A listing of the names the
// gate can measure now stands in for the campaign.
func TestLintChecksAMistypedMatchAgainstTheListing(t *testing.T) {
	t.Run("the self-test document is clean against its listing", func(t *testing.T) {
		code, errOut := lintAgainstListing(t, selfTestBudgets, listedNames)
		if code != 0 {
			t.Fatalf("exit %d, want 0; stderr:\n%s", code, errOut)
		}
		if !strings.Contains(errOut, "the -benchtime=1x listing") {
			t.Errorf("the verdict does not say what the patterns were checked against:\n%s", errOut)
		}
		// The two ratio budgets and the `ratio` gate set's two patterns need
		// a broker, so a listing cannot answer for them; saying so is what
		// keeps their silence from reading as a pass.
		if !strings.Contains(errOut, "6 ratio pattern(s) were not checked") {
			t.Errorf("the verdict does not count the ratio patterns it could not check:\n%s", errOut)
		}
	})
	for _, c := range []struct {
		name, from, to, want string
	}{
		{
			"a mistyped budget match", `match: "BenchmarkHeaders/*/Get/n=8"`, `match: "BenchmarkHeaders/*/Gte/n=8"`,
			"budget BenchmarkHeaders/*/Gte/n=8 matches none of the 5 benchmark names in the -benchtime=1x listing",
		},
		{
			"a mistyped gate set pattern", `"BenchmarkHeaders/*"]`, `"BenchmarkHeader/*"]`,
			"gate set pr pattern BenchmarkHeader/* matches none",
		},
		{
			"a mistyped waiver", `match: "BenchmarkHeaders/waived/Get/n=8"`, `match: "BenchmarkHeaders/waved/Get/n=8"`,
			"waiver BenchmarkHeaders/waved/Get/n=8 matches none",
		},
	} {
		t.Run(c.name, func(t *testing.T) {
			doc := strings.Replace(selfTestBudgets, c.from, c.to, 1)
			if doc == selfTestBudgets {
				t.Fatalf("the fixture has no %q to mistype", c.from)
			}
			code, errOut := lintAgainstListing(t, doc, listedNames)
			if code != 1 {
				t.Fatalf("exit %d, want 1; stderr:\n%s", code, errOut)
			}
			if !strings.Contains(errOut, c.want) {
				t.Errorf("the refusal does not say %q:\n%s", c.want, errOut)
			}
		})
	}
	t.Run("an empty listing is not a pass", func(t *testing.T) {
		code, errOut := lintAgainstListing(t, selfTestBudgets, "\n")
		if code != 2 {
			t.Fatalf("exit %d, want 2; stderr:\n%s", code, errOut)
		}
		if !strings.Contains(errOut, "holds no benchmark names") {
			t.Errorf("exit 2 for the wrong reason:\n%s", errOut)
		}
	})
}

// TestLintChecksAGateSetAgainstItsOwnPackages is tt6-4. The listing used to
// be one list of every in-process gate set's packages, and each set's
// patterns were checked against all of it, so a `pr` pattern naming a
// benchmark that only a `nightly` package holds passed, and a `kind:
// specification` budget over such a row was counted as evaluated by `pr`.
// hack/benchgate.sh builds and runs only the set's own packages, so the gate
// never measured either.
func TestLintChecksAGateSetAgainstItsOwnPackages(t *testing.T) {
	// The nightly set's packages hold every benchmark; the pr set's hold the
	// Headers rows but one.
	doc := strings.Replace(selfTestBudgets, "  ratio:", "  nightly: [\"*\"]\n  ratio:", 1)
	if doc == selfTestBudgets {
		t.Fatal("the fixture has no `ratio:` gate set to put `nightly` before")
	}
	const prHeaders = "BenchmarkHeaders/registry/Get/n=8-16 nightly,pr\n" +
		"BenchmarkHeaders/waived/Get/n=8-16 nightly\n"
	const nightlyMemory = "BenchmarkMemory/PubSubAck/owned-16 nightly\n" +
		"BenchmarkMemory/PubSubAck/waivedall/1P1C-16 nightly\n" +
		"BenchmarkMemory/PubSubAck/waivedtime/1P1C-16 nightly\n"
	const prMemory = "BenchmarkMemory/PubSubAck/owned-16 nightly,pr\n" +
		"BenchmarkMemory/PubSubAck/waivedall/1P1C-16 nightly,pr\n" +
		"BenchmarkMemory/PubSubAck/waivedtime/1P1C-16 nightly,pr\n"
	t.Run("a pr pattern only a nightly package can measure", func(t *testing.T) {
		code, errOut := lintAgainstListing(t, doc, "BenchmarkHeaders/registry/Get/n=8-16 nightly,pr\n"+
			"BenchmarkHeaders/waived/Get/n=8-16 nightly,pr\n"+nightlyMemory)
		if code != 1 {
			t.Fatalf("exit %d, want 1; stderr:\n%s", code, errOut)
		}
		const want = "gate set pr pattern BenchmarkMemory/PubSubAck/* matches none of the 2 benchmark names in the -benchtime=1x listing from the pr gate set's packages"
		if !strings.Contains(errOut, want) {
			t.Errorf("the refusal does not say %q:\n%s", want, errOut)
		}
	})
	t.Run("a specification budget over a row only a nightly package holds", func(t *testing.T) {
		code, errOut := lintAgainstListing(t, doc, prHeaders+prMemory)
		if code != 1 {
			t.Fatalf("exit %d, want 1; stderr:\n%s", code, errOut)
		}
		const want = "the `pr` gate set selects only 1 of them; declare `unevaluated: 1`"
		if !strings.Contains(errOut, want) {
			t.Errorf("the refusal does not say %q:\n%s", want, errOut)
		}
	})
	t.Run("the same rows in the pr set's packages", func(t *testing.T) {
		code, errOut := lintAgainstListing(t, doc, "BenchmarkHeaders/registry/Get/n=8-16 nightly,pr\n"+
			"BenchmarkHeaders/waived/Get/n=8-16 nightly,pr\n"+prMemory)
		if code != 0 {
			t.Fatalf("exit %d, want 0; stderr:\n%s", code, errOut)
		}
	})
	t.Run("a listing that names no gate set", func(t *testing.T) {
		code, errOut := lintAgainstListing(t, doc, "BenchmarkHeaders/registry/Get/n=8-16\n")
		if code != 2 {
			t.Fatalf("exit %d, want 2; stderr:\n%s", code, errOut)
		}
		if !strings.Contains(errOut, "names no gate set") {
			t.Errorf("exit 2 for the wrong reason:\n%s", errOut)
		}
	})
}

// TestLintMatchesOnlyTargetsAgainstCampaignRows: a `campaign` row of the
// listing comes from a family's own grid, which the gate never sets, so a
// §8.10 target (a campaign figure) may match it and a gate-set pattern or a
// budget may not: the gate would select nothing for either.
func TestLintMatchesOnlyTargetsAgainstCampaignRows(t *testing.T) {
	const (
		campaignRow = "campaign BenchmarkMemory/PubSubAck/full/1P1C\n"
		target      = "targets:\n  - {match: \"BenchmarkMemory/PubSubAck/full/*\", msgs_s_target: 1}\n"
	)
	t.Run("a target", func(t *testing.T) {
		code, errOut := lintAgainstListing(t, selfTestBudgets+target, listedNames+campaignRow)
		if code != 0 {
			t.Fatalf("exit %d, want 0; stderr:\n%s", code, errOut)
		}
		if !strings.Contains(errOut, "1 campaign row(s) from a family's own grid, for the targets only") {
			t.Errorf("the verdict does not count the campaign rows:\n%s", errOut)
		}
	})
	for _, c := range []struct{ name, from, to, want string }{
		{
			"a gate set pattern", `"BenchmarkHeaders/*"]`, `"BenchmarkHeaders/*", "BenchmarkMemory/PubSubAck/full/*"]`,
			"gate set pr pattern BenchmarkMemory/PubSubAck/full/* matches none",
		},
		{
			"a budget", `match: "BenchmarkHeaders/*/Get/n=8"`, `match: "BenchmarkMemory/PubSubAck/full/*"`,
			"budget BenchmarkMemory/PubSubAck/full/* matches none",
		},
	} {
		t.Run(c.name, func(t *testing.T) {
			doc := strings.Replace(selfTestBudgets, c.from, c.to, 1)
			if doc == selfTestBudgets {
				t.Fatalf("the fixture has no %q to change", c.from)
			}
			code, errOut := lintAgainstListing(t, doc, listedNames+campaignRow)
			if code != 1 {
				t.Fatalf("exit %d, want 1; stderr:\n%s", code, errOut)
			}
			if !strings.Contains(errOut, c.want) {
				t.Errorf("the refusal does not say %q:\n%s", c.want, errOut)
			}
		})
	}
}

// TestNamesIsRefusedOutsideLint: -names is read by -lint alone, so anywhere
// else it is a path argument that goes nowhere.
func TestNamesIsRefusedOutsideLint(t *testing.T) {
	dir := t.TempDir()
	list := writeFile(t, filepath.Join(dir, "names.txt"), listedNames)
	raw := writeFile(t, filepath.Join(dir, "raw.txt"), bothArms())
	code, _, errOut := invoke(t, "-ratios", "-budgets", budgetsFile(t, dir), "-run", raw, "-gate", "ratio", "-names", list)
	if code != 2 {
		t.Fatalf("exit %d, want 2; stderr:\n%s", code, errOut)
	}
	if !strings.Contains(errOut, "-names") {
		t.Errorf("the refusal does not name the flag:\n%s", errOut)
	}
}
