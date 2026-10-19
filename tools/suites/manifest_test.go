package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/iqhive/iqmq/testkit"
)

// root is the repository root from this package's directory.
const root = "../.."

// TestTheManifestIsTrueOfThisRepository is the check `make suites-check` runs.
func TestTheManifestIsTrueOfThisRepository(t *testing.T) {
	m, err := Load(root)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	for _, err := range m.Validate(root) {
		t.Errorf("%v", err)
	}
	if len(m.Suites) == 0 {
		t.Fatal("the manifest lists no suites")
	}
	if got := len(m.Select("integration", 0)); got == 0 {
		t.Error("no suite declares an integration run, so CI drives no conformance suite")
	}
}

// TestValidateRefusesEveryShapeOfBrokenEntry is the half that makes the check
// worth running. Each row breaks one thing about a manifest that validates,
// and the validator has to say so: a check nobody has watched fail is assumed
// not to work.
func TestValidateRefusesEveryShapeOfBrokenEntry(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*Manifest)
		want   string
	}{
		{
			name:   "a suite whose integration test does not exist",
			mutate: func(m *Manifest) { m.Suites = append(m.Suites, unlanded()) },
			want:   "has no TestConformance_not_a_driver",
		},
		{
			name: "a suite declaring a per-suite run mode whose test does not exist",
			mutate: func(m *Manifest) {
				s := &m.Suites[1]
				s.Runs = append(s.Runs, "chaos")
			},
			want: "has no TestChaos_nats",
		},
		{
			name: "a suite declaring a run mode whose tests are not in its package",
			mutate: func(m *Manifest) {
				s := &m.Suites[1]
				s.Runs = append(s.Runs, "critical")
			},
			want: "holds no test called TestCritical*",
		},
		{
			name:   "a test name that is not the one the run mode resolves",
			mutate: func(m *Manifest) { m.Suites[1].Test = "TestConformance_nats_core" },
			want:   "the entry and the run would drive different tests",
		},
		{
			name:   "a package that is not a directory",
			mutate: func(m *Manifest) { m.Suites[1].Package = "drivers/nowhere" },
			want:   "which is not a directory in this repository",
		},
		{
			name:   "a report nobody committed",
			mutate: func(m *Manifest) { m.Suites[1].Reports = []string{"nats-core-v2.json"} },
			want:   "no such file or directory",
		},
		{
			name:   "a report belonging to another driver",
			mutate: func(m *Manifest) { m.Suites[1].Reports = []string{"memory.json"} },
			want:   "whose driver is \"memory\"",
		},
		{
			name:   "an evidence kind the report contradicts",
			mutate: func(m *Manifest) { m.Suites[1].Evidence = "managed" },
			want:   "was made with \"container\"",
		},
		{
			name:   "an evidence kind that is not one of the four",
			mutate: func(m *Manifest) { m.Suites[1].Evidence = "probably" },
			want:   "want one of container, emulator, managed, inprocess",
		},
		{
			name:   "a maintenance tier outside §2.7's three",
			mutate: func(m *Manifest) { m.Suites[1].Tier = 4 },
			want:   "§2.7's tiers are 1, 2 and 3",
		},
		{
			name:   "a suite with no run mode at all",
			mutate: func(m *Manifest) { m.Suites[1].Runs = nil },
			want:   "declares no run mode, so nothing would ever run it",
		},
		{
			name:   "a suite naming no report and not marked as a cross-check",
			mutate: func(m *Manifest) { m.Suites[1].Reports = nil },
			want:   "names no report",
		},
		{
			name:   "an entry deleted, leaving its suite unrun and its report unclaimed",
			mutate: func(m *Manifest) { m.Suites = m.Suites[:len(m.Suites)-2] },
			want:   "and no entry of conformance/suites.json names it",
		},
		{
			name:   "two entries of one name",
			mutate: func(m *Manifest) { m.Suites = append(m.Suites, m.Suites[1]) },
			want:   "two entries are called",
		},
		{
			name:   "a schema the reader does not know",
			mutate: func(m *Manifest) { m.Schema = 2 },
			want:   "schema is 2, want 1",
		},
		{
			name:   "the state this file was written to end",
			mutate: func(m *Manifest) { m.Suites = nil },
			want:   "indistinguishable from a green pipeline",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fixtureRoot := manifestReportFixture(t)
			m, err := Load(root)
			if err != nil {
				t.Fatalf("Load: %v", err)
			}
			tc.mutate(m)
			errs := m.Validate(fixtureRoot)
			if len(errs) == 0 {
				t.Fatalf("the validator accepted a manifest with %s", tc.name)
			}
			joined := make([]string, 0, len(errs))
			for _, e := range errs {
				joined = append(joined, e.Error())
			}
			if !strings.Contains(strings.Join(joined, "\n"), tc.want) {
				t.Errorf("the validator said\n%s\nwant something mentioning %q", strings.Join(joined, "\n"), tc.want)
			}
		})
	}
}

// TestValidateRefusesAnUnimplementedMode covers the branch no real mode can
// reach any more.
func TestValidateRefusesAnUnimplementedMode(t *testing.T) {
	m, err := Load(root)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	reg := append(registry(nil), Modes()...)
	reg = append(reg, Mode{
		Name: "notyet", Dir: "conformance/notyet", Prefix: "TestNotYet",
		Timeout: "1m", Note: "a mode whose package does not exist",
	})
	m.Suites[1].Runs = append(m.Suites[1].Runs, "notyet")
	errs := m.validateWith(root, reg)
	joined := make([]string, 0, len(errs))
	for _, e := range errs {
		joined = append(joined, e.Error())
	}
	if !strings.Contains(strings.Join(joined, "\n"), "which this repository does not implement") {
		t.Fatalf("the validator said\n%s\nwant something mentioning the unimplemented mode", strings.Join(joined, "\n"))
	}

	// And the other direction: a mode marked implemented whose directory is
	// not there must also be refused, or the registry's claim is decoration.
	reg[len(reg)-1].Implemented = true
	errs = m.validateWith(root, reg)
	joined = joined[:0]
	for _, e := range errs {
		joined = append(joined, e.Error())
	}
	if !strings.Contains(strings.Join(joined, "\n"), "claims to be implemented and conformance/notyet does not exist") {
		t.Fatalf("the validator said\n%s\nwant something about the missing directory", strings.Join(joined, "\n"))
	}
}

// unlanded is an entry for a driver that is not in this repository, which is
// the shape of the mistake the first person to add a suite entry makes: the
// entry is written before the test it names exists, and the run then selects
// nothing and exits 0.
//
// The name is deliberately NOT a driver this repository will ever have. It was
// "rabbitmq" until that driver landed, and the fixture then collided with the
// real entry: the validator reported the duplicate NAME instead of the missing
// TEST, so the subtest failed on a message about something else entirely and
// the case it exists for stopped being covered. A name no driver can take is
// the only version of this fixture that cannot rot.
func unlanded() Suite {
	return Suite{
		Name: "not-a-driver", Driver: "not-a-driver", Package: "drivers/nats",
		Test: "TestConformance_not_a_driver", Reports: []string{"nats-core.json"},
		Evidence: "container", Tier: 2, Runs: []string{"integration"},
	}
}

// TestResolveSelectsExactlyOneTestPerMode is the check that has to cover
// EVERY per-suite mode rather than the one the author was thinking about.
//
// Two defects live here. The first is B05's hand-off: `-run
// '^TestConformance_nats'` also selects TestConformance_natsunified, so a
// DRIVER=nats job ran two suites and filed the second one's report under the
// first one's name — which is why the pattern is anchored at both ends. The
// second was found by this test on the branch that wrote it: Resolve used the
// entry's own Test for every per-suite mode, and Test names the INTEGRATION
// run alone, so `make matrix DRIVER=jetstream` ran
// `-run '^TestConformance_jetstream$'` inside conformance/matrix and selected
// nothing. `go test` exits 0 and prints PASS when its pattern selects
// nothing, so the nightly broker-version matrix would have been green and
// measured nothing — family A29's failure, in the file that exists to prevent
// it.
func TestResolveSelectsExactlyOneTestPerMode(t *testing.T) {
	m, err := Load(root)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	checked := 0
	for _, md := range Modes() {
		if !md.Implemented || !md.PerSuite {
			continue
		}
		names, err := testNames(root + "/" + md.Dir)
		if err != nil {
			t.Fatalf("testNames(%s): %v", md.Dir, err)
		}
		for _, s := range m.Select(md.Name, 0) {
			res, err := m.Resolve(s.Name, md.Name)
			if err != nil {
				t.Fatalf("Resolve(%s, %s): %v", s.Name, md.Name, err)
			}
			re, err := regexp.Compile(res.Pattern)
			if err != nil {
				t.Fatalf("%s/%s: the pattern %q does not compile: %v", md.Name, s.Name, res.Pattern, err)
			}
			var matched []string
			for name := range names {
				if re.MatchString(name) {
					matched = append(matched, name)
				}
			}
			if len(matched) != 1 {
				t.Errorf("%s/%s resolves to %q in %s, which selects %v; want exactly one test, and `go test` reports a pattern that selects nothing as a pass",
					md.Name, s.Name, res.Pattern, md.Dir, matched)
				continue
			}
			if md.Name == "integration" && matched[0] != s.Test {
				t.Errorf("integration/%s resolves to %q, which selects %s; the entry names %s",
					s.Name, res.Pattern, matched[0], s.Test)
			}
			checked++
		}
	}
	// Two modes and eleven runs today. A mode that stopped being per-suite
	// would silently empty this loop.
	if checked < 11 {
		t.Errorf("only %d per-suite run(s) were checked; the loop is not reaching the modes", checked)
	}
}

// TestANonPerSuiteModeSelectsSomething is the other half: the critical and
// in-process modes are not named after the suite, so what has to be true is
// that the prefix selects at least one test in the suite's own package.
func TestANonPerSuiteModeSelectsSomething(t *testing.T) {
	m, err := Load(root)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	for _, md := range Modes() {
		if !md.Implemented || md.PerSuite {
			continue
		}
		for _, s := range m.Select(md.Name, 0) {
			res, err := m.Resolve(s.Name, md.Name)
			if err != nil {
				t.Fatalf("Resolve(%s, %s): %v", s.Name, md.Name, err)
			}
			names, err := testNames(root + "/" + res.Dir)
			if err != nil {
				t.Fatalf("testNames(%s): %v", res.Dir, err)
			}
			re, err := regexp.Compile(res.Pattern)
			if err != nil {
				t.Fatalf("%s/%s: %v", md.Name, s.Name, err)
			}
			n := 0
			for name := range names {
				if re.MatchString(name) {
					n++
				}
			}
			if n == 0 {
				t.Errorf("%s/%s resolves to %q in %s, which selects no test", md.Name, s.Name, res.Pattern, res.Dir)
			}
		}
	}
}

// TestModeRegistryAgreesWithTheRepository holds the registry's Implemented
// flag in both directions. It is the check the `critical` step of the
// integration workflow needed: that step ran `make critical`, which resolved
// ./critical/... in the conformance module, and conformance/critical has never
// existed — so the first suite entry anyone added would have turned a skipped
// job into a red one at a step that cannot pass.
func TestModeRegistryAgreesWithTheRepository(t *testing.T) {
	for _, md := range Modes() {
		if md.Dir == "" {
			continue // resolved against the suite's own package
		}
		found, err := testNames(root + "/" + md.Dir)
		if err != nil {
			if md.Implemented {
				t.Errorf("run mode %q claims to be implemented and %s cannot be read: %v", md.Name, md.Dir, err)
			}
			t.Logf("run mode %q: %s is absent, which is why it is not implemented", md.Name, md.Dir)
			continue
		}
		got := countPrefix(found, md.Prefix)
		if md.Implemented && got == 0 {
			t.Errorf("run mode %q claims to be implemented and %s holds no %s*", md.Name, md.Dir, md.Prefix)
		}
		if !md.Implemented && got != 0 {
			t.Errorf("run mode %q is marked unimplemented and %s holds %d %s*", md.Name, md.Dir, got, md.Prefix)
		}
	}
	for _, md := range Modes() {
		if md.Note == "" {
			t.Errorf("run mode %q has no note; the note is what a reader of `make chaos` gets instead of a build error", md.Name)
		}
		if md.Timeout == "" {
			t.Errorf("run mode %q has no timeout", md.Name)
		}
	}
}

// TestIdentMatchesWhatTheScriptDoes pins the one substitution hack/
// conformance.sh and this package share.
func TestIdentMatchesWhatTheScriptDoes(t *testing.T) {
	for in, want := range map[string]string{
		"nats":          "nats",
		"kafka-kafkago": "kafka_kafkago",
		"sqs-fifo":      "sqs_fifo",
		"a/b":           "a_b",
	} {
		if got := Ident(in); got != want {
			t.Errorf("Ident(%q) = %q, want %q", in, got, want)
		}
	}
}

// manifestReportFixture creates synthetic reports for validator tests. No measured data is required.
func manifestReportFixture(t *testing.T) string {
	t.Helper()
	source, err := filepath.Abs(root)
	if err != nil {
		t.Fatal(err)
	}
	dest := t.TempDir()
	entries, err := os.ReadDir(source)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if e.Name() == ".git" {
			continue
		}
		if e.Name() == "conformance" {
			if err := os.Mkdir(filepath.Join(dest, "conformance"), 0o755); err != nil {
				t.Fatal(err)
			}
			children, err := os.ReadDir(filepath.Join(source, "conformance"))
			if err != nil {
				t.Fatal(err)
			}
			for _, child := range children {
				if child.Name() == "reports" {
					continue
				}
				if err := os.Symlink(filepath.Join(source, "conformance", child.Name()), filepath.Join(dest, "conformance", child.Name())); err != nil {
					t.Fatal(err)
				}
			}
		} else if err := os.Symlink(filepath.Join(source, e.Name()), filepath.Join(dest, e.Name())); err != nil {
			t.Fatal(err)
		}
	}
	m, err := Load(source)
	if err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(dest, ReportDir)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	for i := range m.Suites {
		suite := &m.Suites[i]
		for _, name := range suite.Reports {
			profile := strings.TrimPrefix(strings.TrimSuffix(name, ".json"), strings.ReplaceAll(suite.Driver, "/", "-"))
			profile = strings.TrimPrefix(profile, "-")
			r := testkit.Report{Schema: testkit.Schema, Driver: suite.Driver, Profile: profile, Evidence: testkit.Evidence(suite.Evidence), Date: "2020-01-01T00:00:00Z"}
			data, err := json.Marshal(r)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(dir, name), data, 0o644); err != nil {
				t.Fatal(err)
			}
		}
	}
	return dest
}
