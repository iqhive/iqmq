package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/iqhive/iqmq/conformance/report"
)

// digest is a plausible image digest for the fixtures.
const digest = "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

// now is the instant every test reads as the present, so that nothing in this
// file depends on the wall clock.
var now = time.Date(2026, 9, 15, 12, 0, 0, 0, time.UTC)

// fixture is a repository root with the three things confgen reads besides the
// reports: the suite list that says which run publishes each report, the image
// lock a report's broker has to be pinned by, and the README whose generated
// block carries the matrix.
type fixture struct {
	t        *testing.T
	root     string
	reports  string
	incoming string
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	f := &fixture{t: t, root: t.TempDir()}
	f.reports = filepath.Join(f.root, "reports")
	f.incoming = filepath.Join(f.root, "incoming")
	f.writeFile(filepath.Join("conformance", "suites.json"), `{
	  "schema": 1,
	  "suites": [
	    {"name": "alpha", "driver": "alpha", "reports": ["alpha-one.json"], "image": "alphabroker", "evidence": "container", "runs": ["integration"]},
	    {"name": "beta",  "driver": "beta",  "reports": ["beta.json"],      "image": "betabroker",  "evidence": "emulator",  "runs": ["integration"]},
	    {"name": "mem",   "driver": "mem",   "reports": ["mem.json"],       "evidence": "inprocess", "runs": ["inprocess"]},
	    {"name": "alpha-cross", "driver": "alpha", "reports": ["alpha-one.json"], "cross_check": true, "image": "alphabroker", "evidence": "container", "runs": ["integration"]}
	  ]
	}`)
	f.writeFile(filepath.Join("conformance", "harness", "images.lock"), "# name image@digest\nalphabroker alpha/broker@"+digest+"\nbetabroker beta/broker@"+digest+"\n")
	f.writeFile("README.md", "intro\n<!-- matrix:start -->\nold\n<!-- matrix:end -->\ntail\n")
	return f
}

func (f *fixture) writeFile(rel, content string) {
	f.t.Helper()
	path := filepath.Join(f.root, rel)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		f.t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		f.t.Fatal(err)
	}
}

func (f *fixture) opts() options {
	return options{
		reports: f.reports, root: f.root, mode: IncomingMode,
		maxStale: DefaultMaxStale, maxAge: DefaultMaxIncomingAge, now: now,
	}
}

// sample is a report that passes every rule, so that a test can break exactly
// one thing and know which rule refused it.
func sample(driver, profile, image string, evidence report.Evidence) *report.Report {
	return &report.Report{
		Schema: report.Schema, Driver: driver, Profile: profile, Evidence: evidence,
		Go: "go1.27.1", Date: now.Add(-2 * time.Hour).Format(time.RFC3339),
		Commit: "0123456789abcdef0123456789abcdef01234567",
		Broker: report.Broker{Image: image}, Client: report.Client{Module: "m", Version: "v1"},
		Results: []report.Result{
			{Scenario: "capability/Nack/works", Feature: "Nack", Claimed: report.LevelNative, Positive: true, Outcome: report.Pass},
			{Scenario: "capability/Nack/claim", Feature: "Nack", Claimed: report.LevelNative, Outcome: report.Pass},
			{Scenario: "capability/Priority/refused", Feature: "Priority", Claimed: report.LevelUnsupported, Outcome: report.Pass},
			// Advertised, and nothing but the claim check ran:
			// the state the matrix used to render as unsupported.
			{Scenario: "capability/Receipts/works", Feature: "Receipts", Claimed: report.LevelNative, Positive: true, Outcome: report.Skip, Note: "no portable probe for Receipts yet"},
			{Scenario: "capability/Receipts/claim", Feature: "Receipts", Claimed: report.LevelNative, Outcome: report.Pass},
			// A denied guarantee no run contradicted.
			{
				Scenario: "capability/OrderedGlobally/refused", Feature: "OrderedGlobally", Claimed: report.LevelUnsupported,
				Kind: report.KindGuarantee, Outcome: report.Skip, Note: "a guarantee's ABSENCE is not observable in one run",
			},
		},
	}
}

// corpus writes the three reports every suite of the fixture publishes.
func (f *fixture) corpus(dir string) {
	f.t.Helper()
	for _, r := range []*report.Report{
		sample("alpha", "one", "alpha/broker:1.2.3", report.EvidenceContainer),
		sample("beta", "", "beta/broker:4", report.EvidenceEmulator),
		sample("mem", "", "in-process", report.EvidenceInProcess),
	} {
		if _, err := r.Save(dir); err != nil {
			f.t.Fatal(err)
		}
	}
}

// wantRefusal runs confgen and requires it to fail with a message naming want.
func wantRefusal(t *testing.T, opt options, want string) {
	t.Helper()
	err := run(opt)
	if err == nil {
		t.Fatalf("confgen accepted the corpus; it must refuse it because %s", want)
	}
	if !strings.Contains(err.Error(), want) {
		t.Errorf("refused for the wrong reason; want a message naming %q, got:\n%v", want, err)
	}
}

func TestGenerateAndCheck(t *testing.T) {
	f := newFixture(t)
	f.corpus(f.reports)
	opt := f.opts()
	if err := run(opt); err != nil {
		t.Fatal(err)
	}
	matrix := f.read(filepath.Join("docs", "drivers", "matrix.md"))
	// The four states the old matrix rendered as one em dash.
	for _, want := range []string{
		"| Nack | N | N◐ | N |",           // verified, and the emulator's evidence marked
		"| Priority | — | — | — |",        // the refusal was observed
		"| Receipts | (N) | (N) | (N) |",  // advertised and unchecked
		"| OrderedGlobally | · | · | · |", // a denied guarantee nothing contradicted
		"| Transactions | ? | ? | ? |",    // nothing said anything either way
		"No behavioural probe in any run", // the coverage section
		"## Provenance",
		"`alpha/broker:1.2.3`",
	} {
		if !strings.Contains(matrix, want) {
			t.Errorf("matrix does not contain %q:\n%s", want, matrix)
		}
	}
	readme := f.read("README.md")
	if !strings.Contains(readme, "alpha (one)") || strings.Contains(readme, "old") || !strings.HasSuffix(readme, "tail\n") {
		t.Errorf("README:\n%s", readme)
	}
	caps := f.read("capabilities.json")
	for _, want := range []string{
		`"evidence": "emulator"`,
		`"state": "advertised"`, // the machine-readable answer for a claim nobody probed
		`"state": "unrefuted"`,
		`"probed": false`,
		`"kind": "guarantee"`,
		`"unrefuted": true`,
		`"go": "go1.27.1"`, // provenance in the machine-readable artefact too
	} {
		if !strings.Contains(caps, want) {
			t.Errorf("capabilities.json does not contain %q", want)
		}
	}
	page := f.read(filepath.Join("docs", "drivers", "alpha-one.capabilities.md"))
	for _, want := range []string{
		"| Receipts | native | **advertised only** | no portable probe for Receipts yet |",
		"| OrderedGlobally | unsupported | unrefuted | a guarantee's absence is not observable in one run |",
		"| Priority | unsupported | refusal observed |  |",
		"| Nack | native | verified by a probe |  |",
	} {
		if !strings.Contains(page, want) {
			t.Errorf("the driver page does not contain %q:\n%s", want, page)
		}
	}
	opt.check = true
	if err := run(opt); err != nil {
		t.Errorf("check after generate: %v", err)
	}
	f.writeFile("capabilities.json", "stale")
	if err := run(opt); err == nil {
		t.Error("check did not detect drift")
	}
	if m := renderMatrix(nil); !strings.Contains(m, "No conformance reports") {
		t.Error("empty matrix")
	}
}

func (f *fixture) read(rel string) string {
	f.t.Helper()
	data, err := os.ReadFile(filepath.Join(f.root, rel))
	if err != nil {
		f.t.Fatal(err)
	}
	return string(data)
}

func TestRefusesAReportWithoutProvenance(t *testing.T) {
	for _, tc := range []struct {
		name   string
		break_ func(*report.Report)
		want   string
	}{
		{"an image nothing pins", func(r *report.Report) { r.Broker.Image = "totally-made-up/broker:latest" }, "is not one conformance/harness/images.lock pins"},
		{"a digest the lock pins differently", func(r *report.Report) {
			r.Broker.Image = "alpha/broker@sha256:ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
		}, "names a digest conformance/harness/images.lock pins differently"},
		{"no commit", func(r *report.Report) { r.Commit = "" }, "is not a commit"},
		{"not a commit", func(r *report.Report) { r.Commit = "the-tip-of-my-branch" }, "is not a commit"},
		{"no Go toolchain", func(r *report.Report) { r.Go = "" }, "is not a Go toolchain version"},
		{"a date in the future", func(r *report.Report) { r.Date = now.Add(48 * time.Hour).Format(time.RFC3339) }, "which is in the future"},
		{"an evidence kind the suite does not declare", func(r *report.Report) { r.Evidence = report.EvidenceManaged }, "evidence and the report was made with"},
		// Pinned by this repository, and the wrong broker for this
		// suite: a Redis column whose version is a Kafka image.
		{"another suite's broker", func(r *report.Report) { r.Broker.Image = "beta/broker:4" }, `runs against "alphabroker"`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t)
			f.corpus(f.reports)
			r := sample("alpha", "one", "alpha/broker:1.2.3", report.EvidenceContainer)
			tc.break_(r)
			if _, err := r.Save(f.reports); err != nil {
				t.Fatal(err)
			}
			wantRefusal(t, f.opts(), tc.want)
		})
	}
}

// TestRefusesACorpusThatIsNotInBijectionWithTheSuites is trap 3: every suite
// should produce a report and every report should belong to a suite. A report
// from a retired suite became a matrix column whose provenance nothing stated,
// and a suite whose report never arrived left the matrix silently short of one.
func TestRefusesACorpusThatIsNotInBijectionWithTheSuites(t *testing.T) {
	t.Run("a report no suite publishes", func(t *testing.T) {
		f := newFixture(t)
		f.corpus(f.reports)
		r := sample("retired", "", "alpha/broker:1.2.3", report.EvidenceContainer)
		if _, err := r.Save(f.reports); err != nil {
			t.Fatal(err)
		}
		wantRefusal(t, f.opts(), "no suite in conformance/suites.json publishes it")
	})
	t.Run("a suite whose report is missing", func(t *testing.T) {
		f := newFixture(t)
		f.corpus(f.reports)
		if err := os.Remove(filepath.Join(f.reports, "beta.json")); err != nil {
			t.Fatal(err)
		}
		wantRefusal(t, f.opts(), "does not hold it; a suite whose report never arrives")
	})
	t.Run("a cross-check is not a publisher", func(t *testing.T) {
		// alpha-cross names alpha-one.json and is a cross_check, so it
		// owes no report of its own -- and it may not be what makes
		// alpha-one.json's provenance, either.
		f := newFixture(t)
		f.corpus(f.reports)
		if err := run(f.opts()); err != nil {
			t.Fatalf("a cross-check naming another suite's report is not a defect: %v", err)
		}
	})
}

// TestRefusesAStaleReportInTheCorpus: a matrix built from reports months apart
// is not one claim about one tree.
func TestRefusesAStaleReportInTheCorpus(t *testing.T) {
	f := newFixture(t)
	f.corpus(f.reports)
	r := sample("beta", "", "beta/broker:4", report.EvidenceEmulator)
	r.Date = now.Add(-100 * 24 * time.Hour).Format(time.RFC3339)
	if _, err := r.Save(f.reports); err != nil {
		t.Fatal(err)
	}
	wantRefusal(t, f.opts(), "before the newest report in this corpus")
}

// TestRefusesAReportRecordingAFailure: a driver that does not pass the suite
// does not publish a capability.
func TestRefusesAReportRecordingAFailure(t *testing.T) {
	f := newFixture(t)
	f.corpus(f.reports)
	r := sample("alpha", "one", "alpha/broker:1.2.3", report.EvidenceContainer)
	r.Results = append(r.Results, report.Result{
		Scenario: "capability/Replay/works", Feature: "Replay", Claimed: report.LevelNative,
		Positive: true, Outcome: report.Fail, Note: "it did not work",
	})
	if _, err := r.Save(f.reports); err != nil {
		t.Fatal(err)
	}
	wantRefusal(t, f.opts(), "FAILED")
}

// TestRefusesACorpusWithoutTheKitsCapabilityScenarios pins the contract the
// rendering depends on: the separation of "advertised" from "verified" reads
// the `capability/<feature>/claim` scenario, so a report written by something
// else is refused rather than rendered as a driver with no claims.
func TestRefusesACorpusWithoutTheKitsCapabilityScenarios(t *testing.T) {
	f := newFixture(t)
	f.corpus(f.reports)
	r := sample("beta", "", "beta/broker:4", report.EvidenceEmulator)
	r.Results = []report.Result{{Scenario: "something/else", Outcome: report.Pass}}
	if _, err := r.Save(f.reports); err != nil {
		t.Fatal(err)
	}
	wantRefusal(t, f.opts(), "carries no capability/")
}

// TestAGeneratedPageNoReportProducesIsDrift is trap 5's one real half. Every
// artefact confgen WRITES is compared, so an artefact it once wrote and no
// longer writes was compared with nothing: remove a driver's report and its
// page stayed on disk stating capabilities from evidence that is gone, while
// `confgen -check` exited 0.
func TestAGeneratedPageNoReportProducesIsDrift(t *testing.T) {
	f := newFixture(t)
	f.corpus(f.reports)
	if err := run(f.opts()); err != nil {
		t.Fatal(err)
	}
	// Retire the suite AND its report, so the corpus is in bijection
	// again and the only thing left over is the page.
	f.writeFile(filepath.Join("conformance", "suites.json"), `{
	  "schema": 1,
	  "suites": [
	    {"name": "alpha", "driver": "alpha", "reports": ["alpha-one.json"], "image": "alphabroker", "evidence": "container", "runs": ["integration"]},
	    {"name": "mem",   "driver": "mem",   "reports": ["mem.json"],       "evidence": "inprocess", "runs": ["inprocess"]}
	  ]
	}`)
	if err := os.Remove(filepath.Join(f.reports, "beta.json")); err != nil {
		t.Fatal(err)
	}
	orphan := filepath.Join(f.root, "docs", "drivers", "beta.capabilities.md")
	if _, err := os.Stat(orphan); err != nil {
		t.Fatal(err)
	}
	opt := f.opts()
	opt.check = true
	err := run(opt)
	if err == nil || !strings.Contains(err.Error(), "no report produces it") {
		t.Errorf("-check accepted a page no report produces: %v", err)
	}
	opt.check = false
	if err := run(opt); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(orphan); !os.IsNotExist(err) {
		t.Errorf("generate left %s behind", orphan)
	}
	// A page this command did not write is not this command's to delete.
	hand := filepath.Join(f.root, "docs", "drivers", "handwritten.capabilities.md")
	if err := os.WriteFile(hand, []byte("# by a person\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := run(opt); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(hand); err != nil {
		t.Errorf("generate deleted a page it did not write: %v", err)
	}
}

// TestDeliverRefusesEveryWayANightlyCanRefreshNothing is trap 1, the one the
// increment exists for. The integration workflow ran
//
//	cp -f conformance/reports/incoming/*.json conformance/reports/latest/ || true
//	make generate
//
// and every failure below was invisible: the `||` swallowed the copy, the
// generation regenerated the COMMITTED reports, the diff was empty and the job
// reported success having refreshed nothing.
func TestDeliverRefusesEveryWayANightlyCanRefreshNothing(t *testing.T) {
	deliveries := func(f *fixture) []*report.Report {
		return []*report.Report{
			fresh(sample("alpha", "one", "alpha/broker:1.2.3", report.EvidenceContainer)),
			fresh(sample("beta", "", "beta/broker:4", report.EvidenceEmulator)),
		}
	}
	t.Run("no download directory at all", func(t *testing.T) {
		f := newFixture(t)
		f.corpus(f.reports)
		opt := f.opts()
		opt.incoming = f.incoming
		wantRefusal(t, opt, "produced nothing")
	})
	t.Run("an empty download", func(t *testing.T) {
		f := newFixture(t)
		f.corpus(f.reports)
		if err := os.MkdirAll(f.incoming, 0o755); err != nil {
			t.Fatal(err)
		}
		opt := f.opts()
		opt.incoming = f.incoming
		wantRefusal(t, opt, "the download produced nothing, and regenerating from the committed reports would republish them")
	})
	t.Run("a suite that delivered nothing", func(t *testing.T) {
		f := newFixture(t)
		f.corpus(f.reports)
		if _, err := deliveries(f)[0].Save(f.incoming); err != nil {
			t.Fatal(err)
		}
		opt := f.opts()
		opt.incoming = f.incoming
		wantRefusal(t, opt, "delivered no beta.json")
	})
	t.Run("a report that is not newer than the one it replaces", func(t *testing.T) {
		f := newFixture(t)
		f.corpus(f.reports)
		for _, r := range deliveries(f) {
			if _, err := r.Save(f.incoming); err != nil {
				t.Fatal(err)
			}
		}
		// The alpha delivery is re-dated to the committed report's own
		// instant: the container ran, wrote nothing new, and the job
		// would have published it as this night's evidence.
		stale := sample("alpha", "one", "alpha/broker:1.2.3", report.EvidenceContainer)
		if _, err := stale.Save(f.incoming); err != nil {
			t.Fatal(err)
		}
		opt := f.opts()
		opt.incoming = f.incoming
		wantRefusal(t, opt, "which is not after the committed report's")
	})
	t.Run("a report from a suite nothing publishes", func(t *testing.T) {
		f := newFixture(t)
		f.corpus(f.reports)
		for _, r := range deliveries(f) {
			if _, err := r.Save(f.incoming); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := fresh(sample("retired", "", "alpha/broker:1.2.3", report.EvidenceContainer)).Save(f.incoming); err != nil {
			t.Fatal(err)
		}
		opt := f.opts()
		opt.incoming = f.incoming
		wantRefusal(t, opt, "a report from a retired suite is not evidence about this repository")
	})
	t.Run("a report older than a nightly's own period", func(t *testing.T) {
		f := newFixture(t)
		f.corpus(f.reports)
		for _, r := range deliveries(f) {
			if _, err := r.Save(f.incoming); err != nil {
				t.Fatal(err)
			}
		}
		old := sample("beta", "", "beta/broker:4", report.EvidenceEmulator)
		old.Date = now.Add(-30 * 24 * time.Hour).Format(time.RFC3339)
		if _, err := old.Save(f.incoming); err != nil {
			t.Fatal(err)
		}
		opt := f.opts()
		opt.incoming = f.incoming
		wantRefusal(t, opt, "the download carried an old artefact rather than the run that was just made")
	})
	t.Run("a file that is not the report it is named after", func(t *testing.T) {
		f := newFixture(t)
		f.corpus(f.reports)
		for _, r := range deliveries(f) {
			if _, err := r.Save(f.incoming); err != nil {
				t.Fatal(err)
			}
		}
		data, err := os.ReadFile(filepath.Join(f.incoming, "beta.json"))
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(f.incoming, "alpha-one.json"), data, 0o644); err != nil {
			t.Fatal(err)
		}
		opt := f.opts()
		opt.incoming = f.incoming
		wantRefusal(t, opt, "is not the report it is named after")
	})
}

// fresh re-dates a report to an hour ago, which is what a run that has just
// finished looks like.
func fresh(r *report.Report) *report.Report {
	r.Date = now.Add(-time.Hour).Format(time.RFC3339)
	return r
}

// TestTheWholeNightlyPathEndToEnd is the exercise trap 1 asks for: synthesise
// the delivery the nightly would make, run the aggregate step, and show the
// artefacts changing. Then show the same step refusing when the delivery is
// empty.
func TestTheWholeNightlyPathEndToEnd(t *testing.T) {
	f := newFixture(t)
	f.corpus(f.reports)
	if err := run(f.opts()); err != nil {
		t.Fatal(err)
	}
	before := f.read(filepath.Join("docs", "drivers", "matrix.md"))
	beforeCaps := f.read("capabilities.json")
	// A night in which beta's Receipts probe finally arrives and alpha's
	// broker is upgraded.
	alpha := fresh(sample("alpha", "one", "alpha/broker:1.2.4", report.EvidenceContainer))
	beta := fresh(sample("beta", "", "beta/broker:4", report.EvidenceEmulator))
	for i := range beta.Results {
		if beta.Results[i].Scenario == "capability/Receipts/works" {
			beta.Results[i].Outcome, beta.Results[i].Note = report.Pass, ""
		}
	}
	for _, r := range []*report.Report{alpha, beta} {
		if _, err := r.Save(f.incoming); err != nil {
			t.Fatal(err)
		}
	}
	opt := f.opts()
	opt.incoming = f.incoming
	if err := run(opt); err != nil {
		t.Fatal(err)
	}
	after := f.read(filepath.Join("docs", "drivers", "matrix.md"))
	if after == before {
		t.Fatal("the delivery changed nothing, which is the defect this path is here to make impossible")
	}
	if !strings.Contains(after, "| Receipts | (N) | N◐ | (N) |") {
		t.Errorf("beta's newly probed claim did not become verified evidence:\n%s", section(after, "| Receipts |"))
	}
	if !strings.Contains(after, "`alpha/broker:1.2.4`") || strings.Contains(after, "`alpha/broker:1.2.3`") {
		t.Error("the provenance table still names the broker image the previous run used")
	}
	if f.read("capabilities.json") == beforeCaps {
		t.Error("capabilities.json did not change")
	}
	// The committed report directory now holds what was delivered.
	if got := f.read(filepath.Join("reports", "alpha-one.json")); !strings.Contains(got, "alpha/broker:1.2.4") {
		t.Error("the delivery was not merged into the report directory")
	}
	// And the same step, with nothing delivered, refuses instead of
	// republishing what is already there.
	for _, name := range []string{"alpha-one.json", "beta.json"} {
		if err := os.Remove(filepath.Join(f.incoming, name)); err != nil {
			t.Fatal(err)
		}
	}
	wantRefusal(t, opt, "the download produced nothing")
}

// section is the line containing want, for a readable failure.
func section(doc, want string) string {
	for _, line := range strings.Split(doc, "\n") {
		if strings.HasPrefix(line, want) {
			return line
		}
	}
	return "(no line begins " + want + ")"
}

// TestTheSuiteListAndTheImageLockAreRequired: a check that silently skips
// itself when its input is missing is the defect this repository has most of.
func TestTheSuiteListAndTheImageLockAreRequired(t *testing.T) {
	for _, path := range []string{
		filepath.Join("conformance", "suites.json"),
		filepath.Join("conformance", "harness", "images.lock"),
		"README.md",
	} {
		t.Run(path, func(t *testing.T) {
			f := newFixture(t)
			f.corpus(f.reports)
			if err := os.Remove(filepath.Join(f.root, path)); err != nil {
				t.Fatal(err)
			}
			if err := run(f.opts()); err == nil {
				t.Errorf("confgen generated the artefacts with no %s", path)
			}
		})
	}
	t.Run("a README with no generated block", func(t *testing.T) {
		f := newFixture(t)
		f.corpus(f.reports)
		f.writeFile("README.md", "no block here\n")
		wantRefusal(t, f.opts(), "is not generated by anything")
	})
}

// TestANightlyDeliveryWithUnreadLevelsIsAggregated is nightly run 35553662643
// in miniature. All of that night's suites passed, and 18 of
// its 21 reports carried a result whose feature was named and whose level was
// EMPTY -- a documented skip or a refused topology, both of which return before
// the kit has a client -- and `make aggregate` refused every one of them, then
// reported each as a suite that "delivered no" report on top.
//
// The kit now writes "unread" on those paths, so the same delivery is
// aggregated, and the claim no scenario could read renders as exactly that: not
// as "advertised", which is where an empty level fell through to, and not as
// "unsupported", which the driver never said.
func TestANightlyDeliveryWithUnreadLevelsIsAggregated(t *testing.T) {
	topology := "the driver cannot build the topology this scenario needs: no retained topic"
	withUnread := func(r *report.Report, level report.Level) *report.Report {
		r.Results = append(r.Results,
			report.Result{Scenario: "capability/Replay/works", Feature: "Replay", Claimed: level, Positive: true, Outcome: report.Skip, Note: topology},
			report.Result{Scenario: "capability/Replay/claim", Feature: "Replay", Claimed: level, Outcome: report.Skip, Note: topology},
			report.Result{Scenario: "capability/Replay/refused", Feature: "Replay", Claimed: level, Outcome: report.Skip, Note: topology},
		)
		return r
	}
	f := newFixture(t)
	f.corpus(f.reports)
	if err := run(f.opts()); err != nil {
		t.Fatal(err)
	}
	for _, r := range []*report.Report{
		withUnread(fresh(sample("alpha", "one", "alpha/broker:1.2.3", report.EvidenceContainer)), report.LevelUnread),
		fresh(sample("beta", "", "beta/broker:4", report.EvidenceEmulator)),
	} {
		if _, err := r.Save(f.incoming); err != nil {
			t.Fatal(err)
		}
	}
	opt := f.opts()
	opt.incoming = f.incoming
	if err := run(opt); err != nil {
		t.Fatalf("a delivery whose unread levels say so was refused: %v", err)
	}
	matrix := f.read(filepath.Join("docs", "drivers", "matrix.md"))
	if !strings.Contains(matrix, "| Replay | ∅ | ? | ? |") {
		t.Errorf("the unread claim is not rendered as one:\n%s", section(matrix, "| Replay |"))
	}
	if !strings.Contains(matrix, "unread: no scenario reached a client") {
		t.Error("the coverage table does not count unread claims")
	}
	page := f.read(filepath.Join("docs", "drivers", "alpha-one.capabilities.md"))
	if want := "| Replay | unread | level not read | " + topology + " |"; !strings.Contains(page, want) {
		t.Errorf("the driver page does not contain %q (the reason once, not three times):\n%s", want, page)
	}
	if caps := f.read("capabilities.json"); !strings.Contains(caps, `"state": "unread"`) || strings.Contains(caps, `"level": ""`) {
		t.Error("capabilities.json does not carry the unread state, or carries an empty level")
	}

	// The form the nightly actually delivered -- the same results with NO
	// level -- is still refused, by name, and nothing is merged.
	f2 := newFixture(t)
	f2.corpus(f2.reports)
	for _, r := range []*report.Report{
		withUnread(fresh(sample("alpha", "one", "alpha/broker:1.2.3", report.EvidenceContainer)), ""),
		fresh(sample("beta", "", "beta/broker:4", report.EvidenceEmulator)),
	} {
		// Save validates, so the legacy form is written as the nightly's
		// writer wrote it: straight to disk.
		data, err := json.MarshalIndent(r, "", "  ")
		if err != nil {
			t.Fatal(err)
		}
		f2.writeFile(filepath.Join("incoming", r.FileName()), string(data))
	}
	opt2 := f2.opts()
	opt2.incoming = f2.incoming
	wantRefusal(t, opt2, `result "capability/Replay/works" has invalid claimed level ""`)
}

// TestADriverPageGivesTheProbesReasonWhenOnlyANeighbourPassed is the
// published half of the T1 finding of the second 2026-10-29 bug hunt. A claim
// whose own probe skipped, beside a neighbouring scenario that passed, was
// rendered "verified by a probe", and because a verified claim carries no Why
// the probe's skip (the one record that nothing had seen the feature work) was
// dropped from the page as well.
func TestADriverPageGivesTheProbesReasonWhenOnlyANeighbourPassed(t *testing.T) {
	const reason = "the dead-letter destination cannot be consumed through the portable API"
	r := sample("alpha", "one", "alpha/broker:1.2.3", report.EvidenceContainer)
	r.Results = append(r.Results,
		report.Result{Scenario: "capability/DeadLetter/works", Feature: "DeadLetter", Claimed: report.LevelConditional, Positive: true, Outcome: report.Skip, Note: reason},
		report.Result{Scenario: "capability/DeadLetter/claim", Feature: "DeadLetter", Claimed: report.LevelConditional, Outcome: report.Pass},
		report.Result{Scenario: "dlq/failed-publish-never-acks", Feature: "DeadLetter", Claimed: report.LevelConditional, Positive: true, Outcome: report.Pass},
	)
	e := newEntry(r)
	c := e.Features["DeadLetter"]
	if c.Verified || c.State != string(report.StateAdvertised) || c.Probed {
		t.Errorf("DeadLetter renders as state %q, verified %v, probed %v; want advertised, unverified and unprobed, because its own probe never ran",
			c.State, c.Verified, c.Probed)
	}
	if !strings.Contains(c.Why, reason) {
		t.Errorf("DeadLetter's Why is %q, want the probe's own reason %q", c.Why, reason)
	}
	if page := renderDriver(&e); !strings.Contains(page, "| DeadLetter | conditional | **advertised only** | "+reason) {
		t.Errorf("the driver page does not give the claim as advertised with the probe's reason:\n%s", section(page, "| DeadLetter |"))
	}
}
