package testkit_test

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/testkit"
)

// sample is a small valid report.
func sample() *testkit.Report {
	return &testkit.Report{
		Schema:   testkit.Schema,
		Driver:   "memory",
		Broker:   testkit.Broker{Image: "in-process"},
		Client:   testkit.ClientInfo{Module: "engine", Version: "v0"},
		Go:       "go1.26.0",
		Date:     time.Date(2026, 9, 13, 12, 0, 0, 0, time.UTC).Format(time.RFC3339),
		Evidence: testkit.EvidenceInProcess,
		Results: []testkit.Result{
			{Scenario: "a", Outcome: testkit.OutcomePass},
			{Scenario: "b", Feature: "Nack", Claimed: testkit.LevelNative, Positive: true, Outcome: testkit.OutcomePass},
			{Scenario: "c", Feature: "Nack", Claimed: testkit.LevelNative, Outcome: testkit.OutcomePass},
			{Scenario: "d", Outcome: testkit.OutcomeSkip, Note: "no broker"},
			{Scenario: "e", Outcome: testkit.OutcomeFail, Note: "broke"},
		},
	}
}

// TestReportShape pins what confgen reads: the JSON keys, the counts, the
// per-feature verification and the conventional file name.
func TestReportShape(t *testing.T) {
	r := sample()
	if err := r.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	pass, fail, skip := r.Counts()
	if pass != 3 || fail != 1 || skip != 1 {
		t.Fatalf("counts are %d/%d/%d", pass, fail, skip)
	}
	if got := r.Failures(); len(got) != 1 || got[0].Scenario != "e" {
		t.Fatalf("Failures is %+v", got)
	}
	if _, ok := r.Find("b"); !ok {
		t.Error("Find could not see a scenario the report holds")
	}
	if r.FileName() != "memory.json" {
		t.Errorf("FileName is %q", r.FileName())
	}
	r.Profile = "fifo"
	if r.Key() != "memory/fifo" || r.FileName() != "memory-fifo.json" {
		t.Errorf("Key is %q, FileName is %q", r.Key(), r.FileName())
	}
	r.Profile = ""

	var buf bytes.Buffer
	if err := r.Write(&buf); err != nil {
		t.Fatalf("Write: %v", err)
	}
	// The keys are the ones the conformance module's reader accepts, and
	// that reader rejects unknown fields: a key renamed here is a report
	// confgen cannot read at all.
	var generic map[string]any
	if err := json.Unmarshal(buf.Bytes(), &generic); err != nil {
		t.Fatalf("the report is not JSON: %v", err)
	}
	for _, key := range []string{"schema", "driver", "broker", "client", "go", "date", "evidence", "results"} {
		if _, ok := generic[key]; !ok {
			t.Errorf("the report has no %q key; conformance/report.Read would refuse it", key)
		}
	}
	results, _ := generic["results"].([]any)
	if len(results) != 5 {
		t.Fatalf("the report carries %d results", len(results))
	}
	first, _ := results[1].(map[string]any)
	for _, key := range []string{"scenario", "feature", "claimed", "positive", "outcome"} {
		if _, ok := first[key]; !ok {
			t.Errorf("a result has no %q key", key)
		}
	}
}

// TestReportValidationRefusesAnUnreadableReport pins the structural rules: a
// report that cannot be read honestly is refused where it is written rather than
// where it is consumed.
func TestReportValidationRefusesAnUnreadableReport(t *testing.T) {
	for name, mutate := range map[string]func(*testkit.Report){
		"no driver":       func(r *testkit.Report) { r.Driver = "" },
		"wrong schema":    func(r *testkit.Report) { r.Schema = 99 },
		"no evidence":     func(r *testkit.Report) { r.Evidence = "" },
		"bad evidence":    func(r *testkit.Report) { r.Evidence = "guesswork" },
		"bad date":        func(r *testkit.Report) { r.Date = "yesterday" },
		"unnamed result":  func(r *testkit.Report) { r.Results[0].Scenario = "" },
		"bad outcome":     func(r *testkit.Report) { r.Results[0].Outcome = "maybe" },
		"bad claim level": func(r *testkit.Report) { r.Results[1].Claimed = "quite good" },
	} {
		r := sample()
		mutate(r)
		if err := r.Validate(); err == nil {
			t.Errorf("%s was accepted", name)
		}
		if err := r.Write(&bytes.Buffer{}); err == nil {
			t.Errorf("%s was written", name)
		}
	}
}

// TestReportSave writes a report where confgen looks for one.
func TestReportSave(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "reports", "latest")
	path, err := sample().Save(dir)
	if err != nil {
		t.Fatalf("Save: %v", err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("reading it back: %v", err)
	}
	if !strings.HasSuffix(path, "memory.json") {
		t.Errorf("Save wrote %q", path)
	}
	if !bytes.Contains(data, []byte(`"driver": "memory"`)) {
		t.Errorf("the file is %s", data)
	}
}

// TestScenariosAreWellFormed pins the suite itself: names are unique and stable,
// every capability scenario names a real feature, and the capability gates are
// complementary — exactly one of "claim" and "refused" can run for any driver,
// which is what makes a claim provable in both directions rather than twice in
// one.
func TestScenariosAreWellFormed(t *testing.T) {
	seen := map[string]bool{}
	var positives, negatives int
	for _, s := range testkit.Scenarios() {
		if s.Name == "" {
			t.Fatal("a scenario has no name")
		}
		if seen[s.Name] {
			t.Errorf("two scenarios are called %q; a driver's matrix entry is keyed by the name", s.Name)
		}
		seen[s.Name] = true
		if s.Run == nil {
			t.Errorf("%s has nothing to run", s.Name)
		}
		if s.Has && !s.Feature.Valid() {
			t.Errorf("%s names a feature outside the inventory", s.Name)
		}
		if s.Has {
			if s.Positive {
				positives++
			} else {
				negatives++
			}
		}
	}
	if positives == 0 || negatives == 0 {
		t.Fatalf("the suite has %d positive and %d negative capability scenarios; the specification requires both", positives, negatives)
	}
	// Every feature of the inventory has all three scenarios.
	for i := range mq.FeatureCount {
		f := mq.Feature(i)
		for _, suffix := range []string{"/works", "/claim", "/refused"} {
			name := "capability/" + f.String() + suffix
			if !seen[name] {
				t.Errorf("the suite has no %s", name)
			}
		}
	}
	// The two negative scenarios are gated complementarily: for any
	// capability table exactly one of them runs.
	var withNack, withoutNack mq.Capabilities
	if err := withNack.Set(mq.FeatureNack, mq.Support{Level: mq.Native}); err != nil {
		t.Fatalf("Set: %v", err)
	}
	for _, s := range testkit.Scenarios() {
		if s.Name != "capability/Nack/claim" && s.Name != "capability/Nack/refused" {
			continue
		}
		if s.Gate == nil {
			t.Fatalf("%s has no gate", s.Name)
		}
		if s.Gate(withNack) == s.Gate(withoutNack) {
			t.Errorf("%s's gate does not distinguish a driver that claims the feature from one that does not", s.Name)
		}
	}
}

// TestRunOne pins the entry point the self-tests use.
func TestRunOne(t *testing.T) {
	h := brokenHarness(testkit.Faults{})
	if _, err := testkit.RunOne(t.Context(), h, "no/such/scenario"); err == nil {
		t.Error("RunOne accepted a scenario name the suite does not have")
	}
	res, err := testkit.RunOne(t.Context(), h, "client/ping")
	if err != nil {
		t.Fatalf("RunOne: %v", err)
	}
	if res.Outcome != testkit.OutcomePass {
		t.Fatalf("client/ping reported %q: %s", res.Outcome, res.Note)
	}
}

// TestHarnessValidation pins what a harness must say before anything is opened.
func TestHarnessValidation(t *testing.T) {
	for name, h := range map[string]testkit.Harness{
		"no driver":    {Evidence: testkit.EvidenceInProcess, Open: brokenHarness(testkit.Faults{}).Open},
		"no Open":      {Driver: "x", Evidence: testkit.EvidenceInProcess},
		"no evidence":  {Driver: "x", Open: brokenHarness(testkit.Faults{}).Open},
		"bad evidence": {Driver: "x", Open: brokenHarness(testkit.Faults{}).Open, Evidence: "hearsay"},
		"bad timeout":  {Driver: "x", Open: brokenHarness(testkit.Faults{}).Open, Evidence: testkit.EvidenceInProcess, Timeout: -1},
		"bad fidelity": {Driver: "x", Open: brokenHarness(testkit.Faults{}).Open, Evidence: testkit.EvidenceInProcess, Fidelity: -1},
	} {
		if _, err := testkit.Run(t.Context(), h); err == nil {
			t.Errorf("a harness with %s was accepted", name)
		}
	}
}

// TestSkipHelpers pins the skip protocol: a skip carries its reason, and a
// driver's ErrUnsupported becomes one rather than a failure.
func TestSkipHelpers(t *testing.T) {
	if reason, ok := testkit.IsSkip(testkit.Skip("because")); !ok || reason != "because" {
		t.Errorf("IsSkip is %q, %v", reason, ok)
	}
	if _, ok := testkit.IsSkip(mq.ErrClosed); ok {
		t.Error("a plain error was read as a skip")
	}
	if _, ok := testkit.IsSkip(testkit.SkipUnsupported(mq.ErrUnsupported, "a delayed nack")); !ok {
		t.Error("SkipUnsupported did not turn ErrUnsupported into a skip")
	}
	if err := testkit.SkipUnsupported(mq.ErrClosed, "x"); err == nil {
		t.Error("SkipUnsupported swallowed an error that is not ErrUnsupported")
	}
}

// TestVerifyKeepsTheLevelAResultWithoutOneCannotCarry pins the same repair
// conformance/report needed: a result the kit writes before it has a client
// carries no claimed level, and Verify took the level from whichever result
// came first. Without this, filing those results against
// their feature -- which is what makes their reason recoverable -- would turn
// every such driver's proven claim into an empty level.
func TestVerifyKeepsTheLevelAResultWithoutOneCannotCarry(t *testing.T) {
	r := &testkit.Report{
		Schema: testkit.Schema, Driver: "d", Evidence: testkit.EvidenceInProcess, Date: "2026-09-15T00:00:00Z",
		Results: []testkit.Result{
			{Scenario: "capability/Replay/works", Feature: "Replay", Claimed: testkit.LevelNative, Positive: true, Outcome: testkit.OutcomePass},
			{Scenario: "capability/Replay/claim", Feature: "Replay", Claimed: testkit.LevelNative, Outcome: testkit.OutcomePass},
			{Scenario: "capability/Replay/refused", Feature: "Replay", Claimed: testkit.LevelUnread, Outcome: testkit.OutcomeSkip, Note: "the driver cannot build the topology this scenario needs"},
		},
	}
	if err := r.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	vs := r.Verify()
	if len(vs) != 1 {
		t.Fatalf("Verify: %d features, want 1", len(vs))
	}
	if vs[0].Claimed != testkit.LevelNative {
		t.Errorf("Claimed is %q, want %q", vs[0].Claimed, testkit.LevelNative)
	}
	if !vs[0].Proven() {
		t.Error("a proven native claim became unproven because a result carried no level")
	}
}

// TestAScenarioThatReachedAClientRecordsTheLevelItRead is the other half LevelUnread is written on the three paths that never get a
// client and on no other. A scenario that opened one read the driver's
// capability table, and its result carries one of the five levels whatever
// its outcome -- including the capability gate's skip, which comes after Open.
func TestAScenarioThatReachedAClientRecordsTheLevelItRead(t *testing.T) {
	h := brokenHarness(testkit.Faults{})
	for _, name := range []string{"capability/Nack/works", "capability/Nack/claim", "capability/Nack/refused"} {
		res, err := testkit.RunOne(t.Context(), h, name)
		if err != nil {
			t.Fatalf("RunOne(%s): %v", name, err)
		}
		switch res.Claimed {
		case testkit.LevelUnsupported, testkit.LevelConditional, testkit.LevelEmulatedVolatile,
			testkit.LevelEmulatedDurable, testkit.LevelNative:
		default:
			t.Errorf("%s (%s) reached a client and recorded the level %q", name, res.Outcome, res.Claimed)
		}
	}
}
