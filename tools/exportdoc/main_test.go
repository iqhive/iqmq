package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestRestatesNameCalibration is what holds the restatement heuristic honest.
// Every "yes" case is a comment this repository or a close variant of it
// actually carries, and every "no" case is one that says something the name
// does not. A word added to `filler` that flips a "no" case fails here.
func TestRestatesNameCalibration(t *testing.T) {
	yes := []struct{ name, doc string }{
		{"Topic", "Topic is the topic."},
		{"Headers", "Headers is the headers."},
		{"MaxMessages", "MaxMessages is the maximum number of messages."},
		{"String", "String returns the string."},
		{"Kind", "Kind is the kind of this value."},
		{"DeadLetterFailures", "DeadLetterFailures is the count of dead letter failures."},
		{"Client.Close", "Close closes the client."},
		{"Stats", "Stats are the stats."},
	}
	for _, c := range yes {
		first, got := restatesName(c.name, c.doc)
		if !got {
			t.Errorf("restatesName(%q, %q) = false, want true (first sentence %q)", c.name, c.doc, first)
		}
	}
	no := []struct{ name, doc string }{
		{"Topic", "Topic is the destination the driver resolves the publication against."},
		{"Headers", "Headers is an ordered slice of byte-slice pairs: duplicates and insertion order survive."},
		{"String", "String returns the slug the bake-off files use."},
		{"Client.Close", "Close releases every handle at once and does not wait for an in-flight delivery; Shutdown is the bounded form."},
		{"Kind", "Kind selects which of the family's implementations New returns."},
		{"MaxMessages", "MaxMessages flushes a batch once this many publications are buffered; zero leaves the default."},
		{"Extend", "Deprecated: Extend now takes the lease scheduler's lead time."},
		{"Len", "Len is exact only for a kind whose ExactLen says so. Hot path: no allocation."},
	}
	for _, c := range no {
		first, got := restatesName(c.name, c.doc)
		if got {
			t.Errorf("restatesName(%q, %q) = true, want false (first sentence %q)", c.name, c.doc, first)
		}
	}
}

func TestSplitIdent(t *testing.T) {
	for _, c := range []struct {
		in   string
		want []string
	}{
		{"MaxBatchBytes", []string{"max", "batch", "bytes"}},
		{"DSNFor", []string{"dsn", "for"}},
		{"publishV2", []string{"publish", "v2"}},
		{"ID", []string{"id"}},
		{"", nil},
	} {
		got := splitIdent(c.in)
		if strings.Join(got, ",") != strings.Join(c.want, ",") {
			t.Errorf("splitIdent(%q) = %v, want %v", c.in, got, c.want)
		}
	}
}

// TestNamesIdentGroupedFields pins the rule that took this check's own false
// positives from 129 to 48: a field with no comment of its own is documented
// when the group's comment names it.
func TestNamesIdentGroupedFields(t *testing.T) {
	const grouped = "Start and End bracket the operation on the logical clock: Start is taken before the call and End after it returns."
	if !namesIdent(grouped, "End") {
		t.Error("the grouped comment does not name End")
	}
	if !namesIdent("RecordHeader is the Kafka-shaped native header: a string Key and a []byte Value.", "Value") {
		t.Error("the type comment does not name Value")
	}
	if namesIdent("Depth is how many messages are ready.", "Deferred") {
		t.Error("Depth's comment must not count as documenting Deferred")
	}
	if namesIdent("MaxBatchBytes bounds one batch.", "MaxBatch") {
		t.Error("a prefix of a longer identifier must not match")
	}
}

// writeTree materialises a throwaway repository: a go.mod, a API requirements
// with §13's sentence, and whatever files the caller names.
func writeTree(t *testing.T, files map[string]string) string {
	t.Helper()
	root := t.TempDir()
	base := map[string]string{
		"go.mod":                 "module example.test\n\ngo 1.26\n",
		"hack/api/workflows.txt": "settle\nheaders\n",
		"api/api.go":             "// Package api is the surface.\npackage api\n\n// Kept states the contract of Kept rather than its name.\nfunc Kept() {}\n",
		"api/api_test.go":        "package api\n\nfunc ExampleKept() {}\n",
	}
	for k, v := range files {
		base[k] = v
	}
	for name, body := range base {
		if body == "" {
			continue
		}
		p := filepath.Join(root, name)
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return root
}

func policy(t *testing.T, root, body string) ([]policyEntry, []exampleEntry) {
	t.Helper()
	p := filepath.Join(root, "policy")
	if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	mods, exs, err := readPolicy(p)
	if err != nil {
		t.Fatalf("readPolicy: %v", err)
	}
	return mods, exs
}

const okPolicy = ".\trequire\tthe surface\nexample\tsettle\tapi:ExampleKept\tcovered\nexample\theaders\tapi:ExampleKept\tcovered\n"

func runTree(t *testing.T, root, pol string) (string, string, error) {
	t.Helper()
	mods, exs := policy(t, root, pol)
	var out, errb bytes.Buffer
	err := run(&out, &errb, root, mods, exs, false, false, 40)
	return out.String(), errb.String(), err
}

// TestCleanTreePasses is the control: without it every test below could be
// passing for the wrong reason.
func TestCleanTreePasses(t *testing.T) {
	root := writeTree(t, nil)
	out, errb, err := runTree(t, root, okPolicy)
	if err != nil {
		t.Fatalf("clean tree failed: %v\nstdout:\n%s\nstderr:\n%s", err, out, errb)
	}
	if !strings.Contains(out, "2 of 2 §13 workflows have a runnable example") {
		t.Errorf("stdout does not report full example coverage:\n%s", out)
	}
}

func TestUndocumentedDeclarationFails(t *testing.T) {
	root := writeTree(t, map[string]string{
		"api/more.go": "package api\n\nfunc Bare() {}\n",
	})
	out, errb, err := runTree(t, root, okPolicy)
	if err == nil {
		t.Fatalf("an undocumented exported func passed:\nstdout:\n%s", out)
	}
	if !strings.Contains(errb, "func Bare") || !strings.Contains(errb, "have no doc comment") {
		t.Errorf("stderr does not name the undocumented declaration:\n%s", errb)
	}
}

func TestReportModeDoesNotFail(t *testing.T) {
	root := writeTree(t, map[string]string{
		"api/more.go": "package api\n\nfunc Bare() {}\n",
	})
	if _, _, err := runTree(t, root, strings.Replace(okPolicy, "\trequire\t", "\treport\t", 1)); err != nil {
		t.Fatalf("report mode failed: %v", err)
	}
}

func TestRestateCeilingFails(t *testing.T) {
	root := writeTree(t, map[string]string{
		"api/more.go": "package api\n\n// Noise is the noise.\nfunc Noise() {}\n",
	})
	_, errb, err := runTree(t, root, strings.Replace(okPolicy, "\trequire\t", "\trequire,restate-max=10\t", 1))
	if err == nil {
		t.Fatal("a 50% restatement rate passed a 10% ceiling")
	}
	if !strings.Contains(errb, "only restate the identifier's name") {
		t.Errorf("stderr does not name the ceiling:\n%s", errb)
	}
}

func TestFieldsMinFails(t *testing.T) {
	root := writeTree(t, map[string]string{
		"api/more.go": "package api\n\n// Cfg carries the knobs.\ntype Cfg struct {\n\tA int\n\tB int\n}\n",
	})
	_, errb, err := runTree(t, root, strings.Replace(okPolicy, "\trequire\t", "\trequire,fields-min=100\t", 1))
	if err == nil {
		t.Fatal("two undocumented exported fields passed a 100% floor")
	}
	if !strings.Contains(errb, "under the 100% the policy requires") {
		t.Errorf("stderr does not name the floor:\n%s", errb)
	}
}

// TestFieldsMinNamesTheField is the difference between a gate that fails and
// a gate somebody can act on: the percentage says a module slipped, the field
// says what to write. Before this, the only way to learn the name was to
// re-run `make exported-doc` with STRICT=1.
func TestFieldsMinNamesTheField(t *testing.T) {
	root := writeTree(t, map[string]string{
		"api/more.go": "package api\n\n// Cfg carries the knobs.\ntype Cfg struct {\n\t// A is the one with a comment.\n\tA int\n\tB int\n}\n",
	})
	_, errb, err := runTree(t, root, strings.Replace(okPolicy, "\trequire\t", "\trequire,fields-min=100\t", 1))
	if err == nil {
		t.Fatal("one undocumented exported field passed a 100% floor")
	}
	if !strings.Contains(errb, "field Cfg.B") {
		t.Errorf("stderr does not name the undocumented field:\n%s", errb)
	}
	if strings.Contains(errb, "field Cfg.A") {
		t.Errorf("stderr names the field that IS documented:\n%s", errb)
	}
}

// TestFieldsMinBitesInReportMode is the property `the exported-doc policy`'s
// `conformance` line rests on, and the one a reader of
// TestReportModeDoesNotFail would most easily assume the other way. `report`
// says a module's DECLARATION coverage is not a failure; it says nothing about
// a fields-min the same line went on to name. Were the floor silent in report
// mode, holding `conformance` at 100 % would be a policy edit that checks
// nothing.
func TestFieldsMinBitesInReportMode(t *testing.T) {
	root := writeTree(t, map[string]string{
		"api/more.go": "package api\n\nfunc Bare() {}\n\n// Cfg carries the knobs.\ntype Cfg struct {\n\tA int\n}\n",
	})
	pol := strings.Replace(okPolicy, "\trequire\t", "\treport,fields-min=100\t", 1)
	_, errb, err := runTree(t, root, pol)
	if err == nil {
		t.Fatal("an undocumented exported field passed a 100% floor on a report-mode module")
	}
	if !strings.Contains(errb, "under the 100% the policy requires") || !strings.Contains(errb, "field Cfg.A") {
		t.Errorf("stderr does not report the floor and the field:\n%s", errb)
	}
	// ... and the mode still swallows the undocumented DECLARATION beside
	// it, so this test is about the floor and not about report mode having
	// quietly become require.
	if strings.Contains(errb, "func Bare") {
		t.Errorf("report mode reported an undocumented declaration:\n%s", errb)
	}
}

func TestWorkflowMissingFromPolicyFails(t *testing.T) {
	root := writeTree(t, nil)
	_, errb, err := runTree(t, root, ".\trequire\tthe surface\nexample\tsettle\tapi:ExampleKept\tcovered\n")
	if err == nil {
		t.Fatal("a policy that names one of §13's two workflows passed")
	}
	if !strings.Contains(errb, `names the workflow "headers" and the policy does not`) {
		t.Errorf("stderr does not name the dropped workflow:\n%s", errb)
	}
}

func TestExampleThatDoesNotExistFails(t *testing.T) {
	root := writeTree(t, nil)
	_, errb, err := runTree(t, root, ".\trequire\tthe surface\nexample\tsettle\tapi:ExampleGone\tcovered\nexample\theaders\tapi:ExampleKept\tcovered\n")
	if err == nil {
		t.Fatal("a policy naming an example that does not exist passed")
	}
	if !strings.Contains(errb, "example(s) that do not exist: api:ExampleGone") {
		t.Errorf("stderr does not name the missing example:\n%s", errb)
	}
}

func TestWorkflowNotInSpecFails(t *testing.T) {
	root := writeTree(t, nil)
	_, errb, err := runTree(t, root, okPolicy+"example\tinvented\tapi:ExampleKept\tnot in §13\n")
	if err == nil {
		t.Fatal("a policy workflow §13 does not name passed")
	}
	if !strings.Contains(errb, "which the workflow list does not contain") {
		t.Errorf("stderr does not reject the invented workflow:\n%s", errb)
	}
}

func TestDeclaredGapIsReportedAndNotSilent(t *testing.T) {
	root := writeTree(t, nil)
	out, _, err := runTree(t, root, ".\trequire\tthe surface\nexample\tsettle\tapi:ExampleKept\tcovered\nexample\theaders\t-\tno example: the type is unexported here\n")
	if err != nil {
		t.Fatalf("a declared gap with a reason failed: %v", err)
	}
	if !strings.Contains(out, "NO EXAMPLE: no example: the type is unexported here") {
		t.Errorf("a declared gap is not printed with its reason:\n%s", out)
	}
	if !strings.Contains(out, "1 declared with no example and a reason") {
		t.Errorf("the summary hides the declared gap:\n%s", out)
	}
}

func TestMissingSpecSentenceFails(t *testing.T) {
	root := writeTree(t, map[string]string{
		"hack/api/workflows.txt": "# No workflows configured\n",
	})
	_, errb, err := runTree(t, root, okPolicy)
	if err == nil {
		t.Fatal("a API requirements with no §13 example sentence passed")
	}
	if !strings.Contains(errb, "workflow requirements list must name at least two workflows") {
		t.Errorf("stderr does not say the workflow list is unreadable:\n%s", errb)
	}
}

func TestUnlistedModuleFails(t *testing.T) {
	root := writeTree(t, map[string]string{
		"nested/go.mod": "module example.test/nested\n\ngo 1.26\n",
		"nested/n.go":   "// Package nested is nested.\npackage nested\n",
	})
	_, errb, err := runTree(t, root, okPolicy)
	if err == nil {
		t.Fatal("a module absent from the policy passed")
	}
	if !strings.Contains(errb, "module nested is not in the policy") {
		t.Errorf("stderr does not name the unlisted module:\n%s", errb)
	}
}

func TestPolicyGrammar(t *testing.T) {
	root := t.TempDir()
	for _, bad := range []string{
		".\trequire\n",
		".\tmaybe\treason\n",
		".\trequire,restate-max=200\treason\n",
		".\trequire,unknown=1\treason\n",
		".\trequire\treason\n.\treport\ttwice\n",
		"example\tsettle\tapi:ExampleKept\n",
		"# only a comment\n",
	} {
		p := filepath.Join(root, "bad")
		if err := os.WriteFile(p, []byte(bad), 0o644); err != nil {
			t.Fatal(err)
		}
		if _, _, err := readPolicy(p); err == nil {
			t.Errorf("readPolicy accepted %q", bad)
		}
	}
}

// TestMainPackagesAreNotInTheSurface is finding 3 of the C03 brief in test
// form: a command's identifiers are not importable, so they are not part of
// any module's API and a change to one must not read as an API change.
func TestMainPackagesAreNotInTheSurface(t *testing.T) {
	root := writeTree(t, map[string]string{
		"cmd/tool/main.go": "package main\n\ntype Undocumented struct{ Field int }\n\nfunc main() {}\n",
	})
	if _, _, err := runTree(t, root, okPolicy); err != nil {
		t.Fatalf("an undocumented declaration in package main failed the check: %v", err)
	}
}
