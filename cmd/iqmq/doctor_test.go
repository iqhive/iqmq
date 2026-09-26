package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	mq "github.com/iqhive/iqmq"
)

// TestDoctorAgainstMemory is the whole report against a live target, which is
// the only driver a unit test can reach.
func TestDoctorAgainstMemory(t *testing.T) {
	h := newHarness(t)
	if got := h.run("doctor", memDSN); got != exitOK {
		t.Fatalf("doctor = %d\nstdout:\n%s\nstderr:\n%s", got, h.stdout(), h.stderr())
	}
	out := h.stdout()
	for _, want := range []string{
		"iqmq doctor",
		"build",
		"target",
		"memory://",
		"driver", "memory",
		"ping", "ok in",
		"health", "ready",
		"capabilities AS ADVERTISED BY THIS DRIVER",
		"variant kinds in force for this target",
		"variant bindings in this build",
		"schemes this binary opens",
		"environment",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("doctor's report does not contain %q:\n%s", want, out)
		}
	}
}

// TestDoctorSaysAdvertisedAndNotVerified is the labelling the matrix makes
// necessary: an advertisement presented as evidence is the defect that matrix
// spent an increment removing from two other artefacts.
func TestDoctorSaysAdvertisedAndNotVerified(t *testing.T) {
	h := newHarness(t)
	if got := h.run("doctor", memDSN); got != exitOK {
		t.Fatalf("doctor = %d: %s", got, h.stderr())
	}
	out := h.stdout()
	for _, want := range []string{
		"AS ADVERTISED BY THIS DRIVER",
		"what the driver claims about itself",
		"NOT probe evidence",
		"docs/drivers/matrix.md",
		"[never probed]",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("doctor's capability section does not say %q, so a reader could take an "+
				"advertisement for evidence:\n%s", want, out)
		}
	}
	// The nine are named in the output, not alluded to.
	for _, f := range neverProbedFeatures {
		if !strings.Contains(out, f) {
			t.Errorf("doctor does not name the never-probed feature %q in its own output", f)
		}
	}

	// And the JSON says so in the field NAME, because a consumer reading
	// `level: native` has no way to know it is reading a claim.
	h2 := newHarness(t)
	if got := h2.run("doctor", "-o", "json", memDSN); got != exitOK {
		t.Fatalf("doctor -o json = %d: %s", got, h2.stderr())
	}
	var rep doctorReport
	if err := json.Unmarshal([]byte(h2.stdout()), &rep); err != nil {
		t.Fatalf("doctor's JSON does not parse: %v\n%s", err, h2.stdout())
	}
	if !strings.Contains(h2.stdout(), `"advertised_level"`) {
		t.Errorf(`doctor's JSON has no "advertised_level" field; a bare "level" reads as evidence`)
	}
	if !strings.Contains(h2.stdout(), `"advertised_features"`) {
		t.Errorf(`doctor's JSON has no "advertised_features" field`)
	}
	if rep.Target == nil {
		t.Fatal("doctor's JSON carries no target")
	}
	if len(rep.Target.Features) != mq.FeatureCount {
		t.Errorf("doctor reported %d features, want all %d of the inventory: "+
			"a table listing only what works makes the absence invisible",
			len(rep.Target.Features), mq.FeatureCount)
	}
	probed := 0
	for _, f := range rep.Target.Features {
		if f.NeverProbed {
			probed++
		}
	}
	if probed != len(neverProbedFeatures) {
		t.Errorf("%d rows are marked never-probed, want %d", probed, len(neverProbedFeatures))
	}
}

// TestNeverProbedMatchesTheGeneratedMatrix ties the CLI's list to the
// artefact generates from the evidence itself.
//
// The list is compiled into the binary because a CLI cannot read a docs page
// at run time. This is what stops it going stale as the conformance kit gains
// probes: the day a probe for Transactions lands, the generated matrix drops
// it from that section and this test fails.
func TestNeverProbedMatchesTheGeneratedMatrix(t *testing.T) {
	path := filepath.Join(repoRoot(t), "docs", "drivers", "matrix.md")
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		t.Skip("no generated capability matrix; generate fresh conformance results before comparing claims")
	}
	if err != nil {
		t.Fatalf("reading the generated matrix: %v", err)
	}
	doc := string(data)
	const heading = "### No behavioural probe in any run"
	i := strings.Index(doc, heading)
	if i < 0 {
		t.Fatalf("%s has no %q section; the generated matrix's shape has changed and this "+
			"check can no longer read it", path, heading)
	}
	rest := doc[i+len(heading):]
	if j := strings.Index(rest, "\n### "); j >= 0 {
		rest = rest[:j]
	}
	item := regexp.MustCompile("(?m)^- `([A-Za-z]+)`$")
	found := item.FindAllStringSubmatch(rest, -1)
	want := make([]string, 0, len(found))
	for _, m := range found {
		want = append(want, m[1])
	}
	if len(want) == 0 {
		t.Fatalf("no feature was read out of %s's %q section, so this check cannot fail "+
			"and is worthless; the section read:\n%s", path, heading, rest)
	}
	if strings.Join(want, " ") != strings.Join(neverProbedFeatures, " ") {
		t.Errorf("cmd/iqmq's never-probed list disagrees with the generated matrix.\n"+
			"  docs/drivers/matrix.md: %v\n  cmd/iqmq/doctor.go:      %v\n"+
			"Update neverProbedFeatures; doctor is telling users a claim can be held to "+
			"when it cannot, or the other way round.", want, neverProbedFeatures)
	}
}

// TestDoctorReportsBothKindsOfVariantFact: the build's Auto bindings and the
// target's resolved kinds are different facts and the report keeps them apart.
func TestDoctorReportsBothKindsOfVariantFact(t *testing.T) {
	h := newHarness(t)
	if got := h.run("doctor", "-o", "json", memDSN); got != exitOK {
		t.Fatalf("doctor = %d: %s", got, h.stderr())
	}
	var rep doctorReport
	if err := json.Unmarshal([]byte(h.stdout()), &rep); err != nil {
		t.Fatalf("parsing doctor's JSON: %v", err)
	}
	if len(rep.Variants) != len(variantFamilies()) {
		t.Errorf("doctor reported %d build bindings, want %d", len(rep.Variants), len(variantFamilies()))
	}
	if rep.Build.Provisional == 0 {
		t.Error("doctor reports no family as provisionally bound; every binding in this " +
			"repository is provisional until the v1 defaults freeze, so this is wrong")
	}
	if rep.Target == nil || len(rep.Target.Kinds) == 0 {
		t.Fatal("doctor reported no resolved kind for a memory target, which pins nine families")
	}
	for _, k := range rep.Target.Kinds {
		if k.Source != "auto" && k.Source != "dsn" {
			t.Errorf("kind %s/%s has source %q, want auto or dsn", k.Family, k.Key, k.Source)
		}
		if k.Kind == "" || k.Kind == "auto" {
			t.Errorf("kind %s/%s resolved to %q; an unresolved Auto is not an answer",
				k.Family, k.Key, k.Kind)
		}
	}
}

// TestDoctorDistinguishesADSNSelectedKindFromAnAutoBoundOne is the half that
// makes the source field worth having: "the DSN asked for it" and "the
// bake-off's provisional winner happens to be it" are different answers.
func TestDoctorDistinguishesADSNSelectedKindFromAnAutoBoundOne(t *testing.T) {
	auto := kindsFor(t, memDSN)
	pinned := kindsFor(t, "memory://?autocreate=true&engine=safe")

	// The specification is the exception and it is not an accident: the fan-out admission
	// policy is a per-topic setting the memory DSN spells `fanout=`, not part
	// of the engine kind set `engine=safe` pins, so it stays Auto in both.
	// A test that asserted "everything is from the DSN" would have to be
	// wrong about one of the two, and the report has to be right about both.
	const perTopic = "A7/fanout"
	for key, k := range auto {
		if k.Source != "auto" {
			t.Errorf("%s came from %q for a DSN that names no kind, want auto", key, k.Source)
		}
	}
	for key, k := range pinned {
		if key == perTopic {
			if k.Source != "auto" {
				t.Errorf("%s came from %q for engine=safe; engine= does not select the "+
					"per-topic fan-out policy and the report must not claim it did", key, k.Source)
			}
			continue
		}
		if k.Source != "dsn" {
			t.Errorf("%s came from %q for engine=safe, want dsn", key, k.Source)
		}
	}
	if _, ok := pinned[perTopic]; !ok {
		t.Errorf("the memory report has no %s row; the fan-out policy is a family the DSN "+
			"selects and doctor should report it", perTopic)
	}
	// And the kinds really differ, or the two reports would be
	// indistinguishable and the source field would be decoration.
	differs := false
	for key, k := range auto {
		if key == perTopic {
			continue
		}
		if p, ok := pinned[key]; ok && p.Kind != k.Kind {
			differs = true
			break
		}
	}
	if !differs {
		t.Errorf("engine=safe resolved to the same kinds as the Auto bindings, so this test "+
			"cannot distinguish them.\nauto:   %v\npinned: %v", auto, pinned)
	}
}

// kindsFor reports one target's resolved kinds, by family key.
func kindsFor(t *testing.T, target string) map[string]kindReport {
	t.Helper()
	entry, ok := entryForScheme("memory")
	if !ok || entry.resolved == nil {
		t.Fatal("the memory driver reports no resolved kinds")
	}
	rows, err := entry.resolved(target)
	if err != nil {
		t.Fatalf("resolving %s: %v", target, err)
	}
	out := map[string]kindReport{}
	for _, r := range rows {
		out[r.Family+"/"+r.Key] = r
	}
	return out
}

// TestDoctorNoConnectPerformsNoIO: --no-connect has to be usable against a
// broker that is not there, which is most of the cases somebody runs doctor
// for.
func TestDoctorNoConnectPerformsNoIO(t *testing.T) {
	h := newFullHarness(t)
	// A DSN naming an address nothing is listening on. With --no-connect it
	// must still report the driver and the configuration.
	if got := h.run("doctor", "--no-connect", "kafka://127.0.0.1:1/prefix?clientid=x"); got != exitOK {
		t.Fatalf("doctor --no-connect = %d\nstderr:\n%s", got, h.stderr())
	}
	out := h.stdout()
	for _, want := range []string{"kafka", "127.0.0.1:1", "config"} {
		if !strings.Contains(out, want) {
			t.Errorf("doctor --no-connect does not report %q:\n%s", want, out)
		}
	}
	if strings.Contains(out, "ping") {
		t.Errorf("doctor --no-connect pinged the broker:\n%s", out)
	}
}

// TestDoctorReportsTheUnregisterableModules: "the CLI cannot open it" and
// "iqmq has no such driver" are different facts.
func TestDoctorReportsTheUnregisterableModules(t *testing.T) {
	h := newFullHarness(t)
	if got := h.run("doctor", "--drivers"); got != exitOK {
		t.Fatalf("doctor --drivers = %d: %s", got, h.stderr())
	}
	out := h.stdout()
	if !strings.Contains(out, "does NOT register") {
		t.Errorf("doctor --drivers does not report the unregistered modules:\n%s", out)
	}
	for _, u := range unreachableDrivers() {
		if !strings.Contains(out, u.module) {
			t.Errorf("doctor --drivers does not name %s", u.module)
		}
	}
	for _, want := range []string{"natsunified", "amqp10", "v1compat", "goredis"} {
		if !strings.Contains(out, want) {
			t.Errorf("doctor --drivers does not mention %q:\n%s", want, out)
		}
	}
}

// TestDoctorDSNFromTheEnvironment covers the documented fallback.
func TestDoctorDSNFromTheEnvironment(t *testing.T) {
	h := newHarness(t).withEnv(map[string]string{"IQMQ_DSN": memDSN})
	if got := h.run("doctor"); got != exitOK {
		t.Fatalf("doctor with $IQMQ_DSN = %d\nstderr:\n%s", got, h.stderr())
	}
	if !strings.Contains(h.stdout(), "target") {
		t.Errorf("doctor did not resolve the target from $IQMQ_DSN:\n%s", h.stdout())
	}
}

// TestDoctorLimitsAreNeverAnOverloadedZero is the rule, read off the
// report: an unadvertised limit must be ABSENT and not zero.
func TestDoctorLimitsAreNeverAnOverloadedZero(t *testing.T) {
	h := newHarness(t)
	if got := h.run("doctor", "-o", "json", memDSN); got != exitOK {
		t.Fatalf("doctor = %d: %s", got, h.stderr())
	}
	if strings.Contains(h.stdout(), `"limits": null`) {
		// Nothing advertised; the text form has to say so rather than
		// printing an empty section.
		h2 := newHarness(t)
		h2.run("doctor", memDSN)
		if !strings.Contains(h2.stdout(), "advertises no limit at all") {
			t.Errorf("the driver advertises no limit and doctor's text report does not say so:\n%s",
				h2.stdout())
		}
	}
	var rep doctorReport
	if err := json.Unmarshal([]byte(h.stdout()), &rep); err != nil {
		t.Fatalf("parsing: %v", err)
	}
	if rep.Target == nil {
		t.Fatal("no target")
	}
	for _, l := range rep.Target.Limits {
		if l.Limit == "" {
			t.Errorf("a limit row has no name: %+v", l)
		}
	}
	if !strings.Contains(strings.ToLower(h.stdout()), "advertised") {
		t.Error("doctor's JSON never uses the word advertised")
	}
}

// TestDoctorTooManyArguments: a second positional is a mistake, not a second
// target, and doctor says so rather than silently ignoring it.
func TestDoctorTooManyArguments(t *testing.T) {
	h := newFullHarness(t)
	if got := h.run("doctor", memDSN, "extra"); got != exitUsage {
		t.Errorf("doctor with two positionals = %d, want %d", got, exitUsage)
	}
	if !strings.Contains(h.stderr(), "at most one DSN") {
		t.Errorf("the refusal does not say why:\n%s", h.stderr())
	}
}
