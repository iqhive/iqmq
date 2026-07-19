package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestIssueRef pins what counts as a reference a reader can follow. The rule
// exists because the test suite makes the ratchet's exemption conditional
// on an "allow-listed justification", and a justification nobody can look up is
// not one.
func TestIssueRef(t *testing.T) {
	for _, s := range []string{
		"#412",
		"iqhive/iqmq#412",
		"#412 the topic grammar moves to the root package in A26",
		"https://bitbucket.org/iqhive/iqmq/issues/412",
		"ADR-017 assumption A23.14",
		"assumption A23.14",
	} {
		if !issueRef(s) {
			t.Errorf("issueRef(%q) = false, want true", s)
		}
	}
	for _, s := range []string{
		"",
		"   ",
		"TODO",
		"later",
		"see the tracker",
		"https://",
		"#",
		"#abc",
		"ADR-17",         // three digits, as every file in adr/ is named
		"assumption A23", // an assumption identifier carries an ordinal
	} {
		if issueRef(s) {
			t.Errorf("issueRef(%q) = true, want false", s)
		}
	}
}

// needyTree writes a tree with one package that the ratchet asks for a fuzz
// target from (an exported Parse over a string, and no Fuzz function) and one
// that it does not.
func needyTree(t *testing.T, allow string) string {
	t.Helper()
	root := t.TempDir()
	write := func(path, content string) {
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	write(filepath.Join(root, "parser", "p.go"),
		"package parser\n\nfunc ParseThing(s string) int { return len(s) }\n")
	write(filepath.Join(root, "plain", "p.go"), "package plain\n\nfunc Add(a, b int) int { return a + b }\n")
	write(filepath.Join(root, "hack", "fuzz-ratchet.yaml"), "baseline_count: 0\nrequired: []\n"+allow)
	return root
}

func TestAllowEntryNeedsAnIssueReference(t *testing.T) {
	cases := []struct {
		name  string
		allow string
		want  string
	}{
		{
			name:  "reason and issue",
			allow: "allow:\n  - package: parser\n    reason: the grammar moves to the root package in A26\n    issue: \"#412\"\n",
		},
		{
			name:  "no issue",
			allow: "allow:\n  - package: parser\n    reason: the grammar moves to the root package in A26\n",
			want:  `allow entry parser needs an issue reference, not ""`,
		},
		{
			name:  "issue that is not a reference",
			allow: "allow:\n  - package: parser\n    reason: the grammar moves to the root package in A26\n    issue: TODO\n",
			want:  `allow entry parser needs an issue reference, not "TODO"`,
		},
		{
			name:  "neither",
			allow: "allow:\n  - package: parser\n",
			want:  "allow entry parser needs a reason and an issue reference",
		},
		{
			name:  "no package",
			allow: "allow:\n  - reason: because\n    issue: \"#1\"\n",
			want:  "allow entry without a package",
		},
		{
			// The entry the check used to accept in silence: a well-formed
			// exemption is still pointless on a package that needs none.
			name:  "stale",
			allow: "allow:\n  - package: plain\n    reason: not a parser\n    issue: \"#412\"\n",
			want:  "allow entry plain is stale: the package needs no exemption",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			root := needyTree(t, c.allow)
			err := run(root, filepath.Join("hack", "fuzz-ratchet.yaml"), false)
			if c.want == "" {
				if err != nil {
					t.Fatalf("a well-formed exemption was refused: %v", err)
				}
				return
			}
			if err == nil {
				t.Fatal("the entry was accepted")
			}
			if !strings.Contains(collect(t, root), c.want) {
				t.Errorf("the refusal does not say %q", c.want)
			}
		})
	}
}

// collect re-runs the check with stderr captured, so a test can assert on the
// per-problem lines and not only on the summary error.
func collect(t *testing.T, root string) string {
	t.Helper()
	old := os.Stderr
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	os.Stderr = w
	_ = run(root, filepath.Join("hack", "fuzz-ratchet.yaml"), false)
	_ = w.Close()
	os.Stderr = old
	var b strings.Builder
	buf := make([]byte, 4096)
	for {
		n, err := r.Read(buf)
		b.Write(buf[:n])
		if err != nil {
			break
		}
	}
	return b.String()
}

// TestMissingTargetIsStillRefused holds the rule the allow list is an exemption
// from, so the exemption tests above cannot pass by the rule being vacuous.
func TestMissingTargetIsStillRefused(t *testing.T) {
	root := needyTree(t, "allow: []\n")
	err := run(root, filepath.Join("hack", "fuzz-ratchet.yaml"), false)
	if err == nil {
		t.Fatal("a parsing package with no Fuzz target passed")
	}
	if got := collect(t, root); !strings.Contains(got, "parser: no Fuzz target (exported ParseThing)") {
		t.Errorf("the refusal does not name the package and the reason: %s", got)
	}
}

// TestABaselineReductionIsDeclaredAndChecked is assumption C02.7.
//
// The case that matters most is the last one: a reduction justified by the
// removal of a path that is STILL THERE. Such a reduction is either wrong or
// has been reverted, and either way it is now standing licence for a coverage
// regression somewhere else in the tree.
func TestABaselineReductionIsDeclaredAndChecked(t *testing.T) {
	cases := []struct {
		name      string
		reduction string
		want      string
	}{
		{
			name: "well formed",
			reduction: "reductions:\n  - from: 20\n    to: 0\n    removed: [gone]\n" +
				"    reason: the prototypes of a frozen API shape left the tree\n    issue: assumption C02.7\n",
		},
		{
			name: "no issue",
			reduction: "reductions:\n  - from: 20\n    to: 0\n    removed: [gone]\n" +
				"    reason: because\n",
			want: `reduction 1 (20 -> 0) needs an issue reference, not ""`,
		},
		{
			name: "no reason",
			reduction: "reductions:\n  - from: 20\n    to: 0\n    removed: [gone]\n" +
				"    issue: assumption C02.7\n",
			want: "reduction 1 (20 -> 0) needs a reason",
		},
		{
			name: "no removed path",
			reduction: "reductions:\n  - from: 20\n    to: 0\n" +
				"    reason: because\n    issue: assumption C02.7\n",
			want: "reduction 1 (20 -> 0) names no removed path",
		},
		{
			name: "not a reduction",
			reduction: "reductions:\n  - from: 5\n    to: 20\n    removed: [gone]\n" +
				"    reason: because\n    issue: assumption C02.7\n",
			want: "which is not a reduction",
		},
		{
			// The dangerous one: the subject of the justification is still
			// in the tree, so the reduction is excusing something else.
			name: "the removed path is still there",
			reduction: "reductions:\n  - from: 20\n    to: 0\n    removed: [plain]\n" +
				"    reason: because\n    issue: assumption C02.7\n",
			want: "is justified by the removal of plain, which is still in the tree",
		},
		{
			// A record that claims more targets survived than the tree has
			// is a record of a different deletion.
			name: "the declared landing count overstates the tree",
			reduction: "reductions:\n  - from: 20\n    to: 5\n    removed: [gone]\n" +
				"    reason: because\n    issue: assumption C02.7\n",
			want: "the highest declared reduction lands at 5 target(s) and the tree has 0",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			root := needyTree(t, "allow:\n  - package: parser\n    reason: a fixture\n    issue: \"#1\"\n"+c.reduction)
			err := run(root, filepath.Join("hack", "fuzz-ratchet.yaml"), false)
			if c.want == "" {
				if err != nil {
					t.Fatalf("a well-formed reduction was refused: %v", err)
				}
				return
			}
			if err == nil {
				t.Fatal("the reduction was accepted")
			}
			if got := collect(t, root); !strings.Contains(got, c.want) {
				t.Errorf("the refusal does not say %q; got %s", c.want, got)
			}
		})
	}
}
