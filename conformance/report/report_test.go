package report

import (
	"bytes"
	"strings"
	"testing"
)

func sample() *Report {
	return &Report{
		Schema: Schema, Driver: "memory", Evidence: EvidenceInProcess, Go: "go1.27.0",
		Date: "2026-09-05T10:00:00Z", Broker: Broker{Image: "inprocess"}, Client: Client{Module: "-", Version: "-"},
		Results: []Result{
			{Scenario: "publish-roundtrip", Outcome: Pass},
			{Scenario: "delay-native", Feature: "DelayedRedelivery", Claimed: LevelNative, Positive: true, Outcome: Pass},
			{Scenario: "delay-unsupported-path", Feature: "DelayedRedelivery", Claimed: LevelNative, Positive: false, Outcome: Pass},
			{Scenario: "priority-positive", Feature: "Priority", Claimed: LevelNative, Positive: true, Outcome: Pass},
			{Scenario: "ttl-rejects", Feature: "MessageTTL", Claimed: LevelUnsupported, Positive: false, Outcome: Pass},
			{Scenario: "seek", Feature: "SeekByTime", Claimed: LevelNative, Positive: true, Outcome: Fail, Note: "boom"},
		},
	}
}

func TestRoundTripAndVerify(t *testing.T) {
	r := sample()
	var buf bytes.Buffer
	if err := Write(&buf, r); err != nil {
		t.Fatal(err)
	}
	back, err := Read(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if back.Key() != "memory" || len(back.Results) != 6 || back.FileName() != "memory.json" {
		t.Errorf("round trip = %+v", back)
	}
	back.Profile = "fifo"
	if back.Key() != "memory/fifo" || back.FileName() != "memory-fifo.json" {
		t.Error("profile key")
	}
	want := map[string]bool{"DelayedRedelivery": true, "Priority": false, "MessageTTL": true, "SeekByTime": false}
	for _, v := range r.Verify() {
		if got := v.Proven(); got != want[v.Feature] {
			t.Errorf("%s proven = %v, want %v (%+v)", v.Feature, got, want[v.Feature], v)
		}
	}
}

func TestValidateAndLoad(t *testing.T) {
	bad := []func(*Report){
		func(r *Report) { r.Schema = 2 },
		func(r *Report) { r.Driver = "" },
		func(r *Report) { r.Evidence = "guess" },
		func(r *Report) { r.Date = "yesterday" },
		func(r *Report) { r.Results[0].Scenario = "" },
		func(r *Report) { r.Results[0].Outcome = "maybe" },
		func(r *Report) { r.Results[1].Claimed = "yes" },
		// A result about a feature with NO level, on every outcome:
		// it is what a kit that lost the level writes.
		func(r *Report) { r.Results[1].Claimed = "" },
		func(r *Report) { r.Results[5].Claimed = "" },
		func(r *Report) { r.Results[1].Claimed, r.Results[1].Outcome, r.Results[1].Note = "", Skip, "n" },
		// "unread" on a PASS: a scenario cannot pass without the client
		// its level is read from.
		func(r *Report) { r.Results[1].Claimed = LevelUnread },
	}
	for i, mutate := range bad {
		r := sample()
		mutate(r)
		if err := r.Validate(); err == nil {
			t.Errorf("case %d validated", i)
		}
	}
	dir := t.TempDir()
	old := sample()
	old.Date = "2026-01-01T00:00:00Z"
	if _, err := old.Save(dir); err != nil {
		t.Fatal(err)
	}
	newer := sample()
	newer.Results = newer.Results[:1]
	// Same key: a distinct file name is needed to keep both on disk.
	if _, err := newer.Save(dir + "/sub"); err != nil {
		t.Fatal(err)
	}
	other := sample()
	other.Driver = "kafka"
	other.Evidence = EvidenceContainer
	if _, err := other.Save(dir); err != nil {
		t.Fatal(err)
	}
	rs, err := LoadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(rs) != 2 || rs[0].Driver != "kafka" || rs[1].Driver != "memory" {
		t.Errorf("LoadDir = %d reports", len(rs))
	}
	if _, err := Parse([]byte(`{"schema":1,"driver":"x","unknown":1}`)); err == nil {
		t.Error("unknown field accepted")
	}
}

func FuzzParse(f *testing.F) {
	var buf bytes.Buffer
	_ = Write(&buf, sample())
	f.Add(buf.Bytes())
	f.Add([]byte(`{"schema":1}`))
	f.Fuzz(func(t *testing.T, data []byte) {
		r, err := Parse(data)
		if err != nil {
			return
		}
		for _, v := range r.Verify() {
			_ = v.Proven()
		}
		var out bytes.Buffer
		if err := Write(&out, r); err != nil {
			t.Fatalf("valid report failed to write: %v", err)
		}
	})
}

// TestVerifiedStateSeparatesTheFourWaysAClaimCanBeUnproven pins the
// distinction the capability matrix was flattening: "—" meant a proven
// refusal, an advertised claim nobody probed, a denial nobody could probe and
// a scenario that failed, all four with the same glyph.
func TestVerifiedStateSeparatesTheFourWaysAClaimCanBeUnproven(t *testing.T) {
	r := &Report{
		Schema: Schema, Driver: "d", Evidence: EvidenceInProcess, Date: "2026-09-15T00:00:00Z",
		Results: []Result{
			// Advertised and proven both ways.
			{Scenario: "capability/Nack/works", Feature: "Nack", Claimed: LevelNative, Positive: true, Outcome: Pass},
			{Scenario: "capability/Nack/claim", Feature: "Nack", Claimed: LevelNative, Outcome: Pass},
			// Denied and the refusal observed.
			{Scenario: "capability/Priority/refused", Feature: "Priority", Claimed: LevelUnsupported, Outcome: Pass},
			// Advertised, and the only thing that ran was the
			// check of the CLAIM: no behavioural probe at all.
			{Scenario: "capability/Receipts/works", Feature: "Receipts", Claimed: LevelNative, Positive: true, Outcome: Skip, Note: "no portable probe for Receipts yet"},
			{Scenario: "capability/Receipts/claim", Feature: "Receipts", Claimed: LevelNative, Outcome: Pass},
			// Denied, and nothing established the denial.
			{Scenario: "capability/Transactions/refused", Feature: "Transactions", Claimed: LevelUnsupported, Outcome: Skip, Note: "no portable probe for Transactions yet"},
			// A denied guarantee no run contradicted.
			{Scenario: "capability/OrderedGlobally/refused", Feature: "OrderedGlobally", Claimed: LevelUnsupported, Kind: KindGuarantee, Outcome: Skip, Note: "a guarantee's ABSENCE is not observable in one run"},
			// A scenario that failed.
			{Scenario: "capability/Replay/works", Feature: "Replay", Claimed: LevelNative, Positive: true, Outcome: Fail, Note: "it did not work"},
			// An advertised claim whose probe never got a client,
			// so the kit filed the result against NO feature: the
			// reason has to be recovered from the scenario name or
			// the driver page can only say "false".
			{Scenario: "capability/SeekByTime/claim", Feature: "SeekByTime", Claimed: LevelNative, Outcome: Pass},
			{Scenario: "capability/SeekByTime/works", Outcome: Skip, Note: "the driver cannot build the topology this scenario needs"},
			// A featureless result is never EVIDENCE, whatever its
			// outcome: a scenario that never ran cannot refute a
			// denial.
			{Scenario: "capability/Transactions/refused", Outcome: Pass},
		},
	}
	if err := r.Validate(); err != nil {
		t.Fatal(err)
	}
	want := map[string]State{
		"Nack": StateVerified, "Priority": StateRefuted, "Receipts": StateAdvertised,
		"Transactions": StateUnverified, "OrderedGlobally": StateUnrefuted, "Replay": StateFailed,
		"SeekByTime": StateAdvertised,
	}
	wantProbed := map[string]bool{
		"Nack": true, "Priority": true, "Receipts": false, "Transactions": false,
		"OrderedGlobally": false, "Replay": true, "SeekByTime": false,
	}
	for _, v := range r.Verify() {
		if got := v.State(); got != want[v.Feature] {
			t.Errorf("%s: state %q, want %q", v.Feature, got, want[v.Feature])
		}
		if got := v.Probed; got != wantProbed[v.Feature] {
			t.Errorf("%s: probed %v, want %v", v.Feature, got, wantProbed[v.Feature])
		}
	}
	// The reason an unverified claim is unverified has to survive into the
	// generated artefacts, or the driver page can only say "false".
	for _, v := range r.Verify() {
		if v.Feature == "Receipts" && !strings.Contains(v.Note, "no portable probe") {
			t.Errorf("Receipts: the skip's reason was dropped, so nothing can say WHY the claim is unverified: %q", v.Note)
		}
		if v.Feature == "SeekByTime" && !strings.Contains(v.Note, "topology") {
			t.Errorf("SeekByTime: a result the kit filed against no feature lost its reason: %q", v.Note)
		}
	}
}

// TestVerifyKeepsTheAdvertisedLevelWhenAScenarioLostIt pins the repair for
// results the kit writes before it has a client: they carry no claimed level
// (LevelUnread since), and Verify used to take the level from
// whichever result came first.
func TestVerifyKeepsTheAdvertisedLevelWhenAScenarioLostIt(t *testing.T) {
	r := &Report{
		Schema: Schema, Driver: "d", Evidence: EvidenceInProcess, Date: "2026-09-15T00:00:00Z",
		Results: []Result{
			{Scenario: "capability/Replay/works", Feature: "Replay", Claimed: LevelNative, Positive: true, Outcome: Pass},
			{Scenario: "capability/Replay/claim", Feature: "Replay", Claimed: LevelNative, Outcome: Pass},
			// The topology refusal is LAST, which is the order the
			// kit writes and the order the committed corpus has.
			{Scenario: "capability/Replay/refused", Feature: "Replay", Claimed: LevelUnread, Outcome: Skip, Note: "the driver cannot build the topology this scenario needs"},
		},
	}
	if err := r.Validate(); err != nil {
		t.Fatal(err)
	}
	vs := r.Verify()
	if len(vs) != 1 {
		t.Fatalf("Verify: %d features, want 1", len(vs))
	}
	if vs[0].Claimed != LevelNative {
		t.Errorf("Claimed is %q; a result written before the client opened carries no level and must not erase the one that did", vs[0].Claimed)
	}
	if vs[0].State() != StateVerified {
		t.Errorf("state %q, want %q: losing the level turns a proven native claim into an unproven one", vs[0].State(), StateVerified)
	}
}

// TestAClaimWhoseLevelNoScenarioReadIsUnread is the rendering half. Every scenario about a feature can return before it has a
// client -- activemq refuses the retained topic all three Replay scenarios
// need, since `claim` gets the probe's shape -- and then nothing in the report
// says what the driver advertises. An empty level fell through State to
// "advertised", which would have rendered "(?)" and "advertised only" for a
// claim no driver made.
func TestAClaimWhoseLevelNoScenarioReadIsUnread(t *testing.T) {
	topology := "the driver cannot build the topology this scenario needs: no retained topic"
	r := &Report{
		Schema: Schema, Driver: "d", Evidence: EvidenceContainer, Date: "2026-09-22T00:00:00Z",
		Results: []Result{
			{Scenario: "capability/Replay/works", Feature: "Replay", Claimed: LevelUnread, Positive: true, Outcome: Skip, Note: topology},
			{Scenario: "capability/Replay/claim", Feature: "Replay", Claimed: LevelUnread, Outcome: Skip, Note: topology},
			{Scenario: "capability/Replay/refused", Feature: "Replay", Claimed: LevelUnread, Outcome: Skip, Note: topology},
			// A documented skip on a general scenario, and nothing else
			// about the feature.
			{Scenario: "dlq/failed-publish-never-acks", Feature: "DeadLetter", Claimed: LevelUnread, Outcome: Skip, Note: "the driver documents this scenario as inapplicable: no dead letter"},
			// And one whose level another scenario DID read, which
			// "unread" must not overwrite.
			{Scenario: "lease/expiry-redelivery", Feature: "ExtendLease", Claimed: LevelUnread, Outcome: Skip, Note: "documented"},
			{Scenario: "capability/ExtendLease/refused", Feature: "ExtendLease", Claimed: LevelUnsupported, Outcome: Pass},
		},
	}
	if err := r.Validate(); err != nil {
		t.Fatal(err)
	}
	want := map[string]State{"Replay": StateUnread, "DeadLetter": StateUnread, "ExtendLease": StateRefuted}
	for _, v := range r.Verify() {
		if got := v.State(); got != want[v.Feature] {
			t.Errorf("%s: state %q, want %q", v.Feature, got, want[v.Feature])
		}
		if v.Feature == "ExtendLease" && v.Claimed != LevelUnsupported {
			t.Errorf("ExtendLease: level %q; an unread result overwrote the level another scenario read", v.Claimed)
		}
		if v.Feature != "Replay" {
			continue
		}
		if v.Claimed != LevelUnread || v.Proven() || v.Probed {
			t.Errorf("Replay: %+v; want an unread, unproven, unprobed claim", v)
		}
		// The reason is the only thing the run knows, and it is said
		// once however many of the three scenarios gave it.
		if v.Note != topology {
			t.Errorf("Replay: note %q, want the topology reason exactly once", v.Note)
		}
	}
}
