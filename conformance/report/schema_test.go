package report_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/conformance/report"
	"github.com/iqhive/iqmq/testkit"
)

// The report is declared twice on purpose: testkit is in the root module,
// which depends on the standard library alone, and this package is in a module
// that depends on testcontainers, so neither can import the other's
// declaration. What holds them together is this — and until it was written,
// nothing did: a field added on one side would have been silently dropped on
// the other, and report.Read refuses an unknown field, so the first report
// written after such a change would have failed to parse in `make
// generate-check` with no clue where the difference was.
func TestReportMatchesConformanceSchema(t *testing.T) {
	for _, pair := range []struct {
		what      string
		kit, conf any
	}{
		{"Report", testkit.Report{}, report.Report{}},
		{"Result", testkit.Result{}, report.Result{}},
		{"Broker", testkit.Broker{}, report.Broker{}},
		{"Client", testkit.ClientInfo{}, report.Client{}},
	} {
		kit := jsonFields(reflect.TypeOf(pair.kit))
		conf := jsonFields(reflect.TypeOf(pair.conf))
		if !reflect.DeepEqual(kit, conf) {
			t.Errorf("%s: testkit writes %v and conformance/report reads %v; a field on one side only is a report the other cannot read",
				pair.what, kit, conf)
		}
	}
	// The enumerations too: a value testkit writes that report.Validate
	// refuses is a report confgen rejects.
	for _, v := range []struct {
		what      string
		kit, conf []string
	}{
		{
			"evidence",
			[]string{
				string(testkit.EvidenceContainer), string(testkit.EvidenceEmulator),
				string(testkit.EvidenceManaged), string(testkit.EvidenceInProcess),
			},
			[]string{
				string(report.EvidenceContainer), string(report.EvidenceEmulator),
				string(report.EvidenceManaged), string(report.EvidenceInProcess),
			},
		},
		{
			"outcome",
			[]string{string(testkit.OutcomePass), string(testkit.OutcomeFail), string(testkit.OutcomeSkip)},
			[]string{string(report.Pass), string(report.Fail), string(report.Skip)},
		},
		{
			"kind",
			[]string{string(testkit.KindBehaviour), string(testkit.KindGuarantee)},
			[]string{string(report.KindBehaviour), string(report.KindGuarantee)},
		},
		{
			"level",
			[]string{
				string(testkit.LevelUnsupported), string(testkit.LevelConditional),
				string(testkit.LevelEmulatedVolatile), string(testkit.LevelEmulatedDurable),
				string(testkit.LevelNative), string(testkit.LevelUnread),
			},
			[]string{
				string(report.LevelUnsupported), string(report.LevelConditional),
				string(report.LevelEmulatedVolatile), string(report.LevelEmulatedDurable),
				string(report.LevelNative), string(report.LevelUnread),
			},
		},
	} {
		if !reflect.DeepEqual(v.kit, v.conf) {
			t.Errorf("%s: testkit spells them %v and conformance/report %v", v.what, v.kit, v.conf)
		}
	}
	if testkit.Schema != report.Schema {
		t.Errorf("testkit.Schema is %d and report.Schema is %d", testkit.Schema, report.Schema)
	}
}

// TestAReportTheKitWritesIsOneConfgenReads is the same claim end to end: a
// report from the kit, through the reader confgen uses, with every field the
// kit can set populated.
//
// report.Read refuses unknown fields, so this is what catches a key the kit
// added and this module did not — including the two this increment added,
// `kind` on a result and `limits` on the report.
func TestAReportTheKitWritesIsOneConfgenReads(t *testing.T) {
	kit := &testkit.Report{
		Schema:   testkit.Schema,
		Driver:   "memory",
		Profile:  "fifo",
		Broker:   testkit.Broker{Image: "in-process", Version: "v0"},
		Client:   testkit.ClientInfo{Module: "m", Version: "v"},
		Go:       "go1.27.0",
		Date:     time.Now().UTC().Format(time.RFC3339),
		Commit:   "abc123",
		Evidence: testkit.EvidenceInProcess,
		Limits:   map[string]int64{"MaxMessageBytes": 1 << 20, "MaxLease": int64(30 * time.Second)},
		Results: []testkit.Result{
			{Scenario: "publish/roundtrip-bodies", Outcome: testkit.OutcomePass, Duration: time.Millisecond},
			{
				Scenario: "capability/OrderedByKey/refused", Feature: "OrderedByKey",
				Claimed: testkit.LevelUnsupported, Kind: testkit.KindGuarantee,
				Outcome: testkit.OutcomeSkip, Note: "one run cannot show a guarantee's absence",
			},
			{
				Scenario: "capability/Nack/works", Feature: "Nack", Claimed: testkit.LevelNative,
				Positive: true, Outcome: testkit.OutcomePass,
			},
			{
				Scenario: "capability/Nack/claim", Feature: "Nack", Claimed: testkit.LevelNative,
				Outcome: testkit.OutcomePass,
			},
		},
	}
	var buf bytes.Buffer
	if err := kit.Write(&buf); err != nil {
		t.Fatalf("the kit refused to write its own report: %v", err)
	}
	back, err := report.Parse(buf.Bytes())
	if err != nil {
		t.Fatalf("conformance/report could not read a report the kit wrote: %v\n%s", err, buf.String())
	}
	if back.Key() != "memory/fifo" || back.FileName() != "memory-fifo.json" {
		t.Errorf("key %q, file %q", back.Key(), back.FileName())
	}
	if got := back.Limits["MaxMessageBytes"]; got != 1<<20 {
		t.Errorf("the advertised limits did not survive the round trip: %v", back.Limits)
	}
	var guarantee, unrefuted int
	for _, v := range back.Verify() {
		if v.Guarantee {
			guarantee++
		}
		if v.Unrefuted() {
			unrefuted++
		}
	}
	if guarantee != 1 || unrefuted != 1 {
		t.Errorf("the guarantee kind did not survive the round trip: %d guarantee(s), %d unrefuted", guarantee, unrefuted)
	}
	if !strings.Contains(buf.String(), `"kind": "guarantee"`) {
		t.Error("the report does not carry the kind a reader needs to tell one-sided evidence from two-sided")
	}
}

// TestEveryResultTheKitWritesBeforeAClientIsOneConfgenReads checks that
// the kit writer and report reader agree on early-return results.
//
// It runs documented skips, refused topologies and failed opens through
// RunOne, writes the results with the kit writer, and reads them with
// the report reader. LevelUnread must survive that round trip.
func TestEveryResultTheKitWritesBeforeAClientIsOneConfgenReads(t *testing.T) {
	skip := "dlq/failed-publish-never-acks"
	h := testkit.Harness{
		Driver: "fixture", Evidence: testkit.EvidenceContainer, Timeout: time.Second,
		Skip: map[string]string{skip: "this configuration publishes no dead letter"},
		Open: func(_ context.Context, n testkit.Need) (*mq.Client, error) {
			if n.Retain {
				return nil, fmt.Errorf("fixture: no retained topic: %w", mq.ErrUnsupported)
			}
			return nil, errors.New("fixture: dial tcp: connection refused")
		},
	}
	kit := &testkit.Report{
		Schema: testkit.Schema, Driver: "fixture", Broker: testkit.Broker{Image: "fixture"},
		Client: testkit.ClientInfo{Module: "m", Version: "v"}, Go: "go1.27.0",
		Date: time.Now().UTC().Format(time.RFC3339), Commit: "abc1234", Evidence: testkit.EvidenceContainer,
	}
	for _, name := range []string{
		skip,                        // the documented skip
		"capability/Replay/works",   // the refused topology (Replay needs a retained topic)
		"capability/Replay/claim",   // the same, since `claim` gets the probe's shape
		"capability/Replay/refused", // the same
		"capability/Nack/works",     // an Open that failed outright
	} {
		res, err := testkit.RunOne(t.Context(), h, name)
		if err != nil {
			t.Fatalf("RunOne(%s): %v", name, err)
		}
		if res.Feature == "" {
			t.Fatalf("%s carries no feature, so this test is not exercising the case it is for", name)
		}
		kit.Results = append(kit.Results, res)
	}
	var buf bytes.Buffer
	if err := kit.Write(&buf); err != nil {
		t.Fatalf("the kit refused to write its own early returns: %v", err)
	}
	back, err := report.Parse(buf.Bytes())
	if err != nil {
		t.Fatalf("confgen's reader refused a report the kit wrote: %v\n%s", err, buf.String())
	}
	states := map[string]report.State{}
	for _, v := range back.Verify() {
		states[v.Feature] = v.State()
	}
	// Replay and DeadLetter never reached a client; Nack's Open FAILED,
	// which is a failure whatever the level.
	want := map[string]report.State{"Replay": report.StateUnread, "DeadLetter": report.StateUnread, "Nack": report.StateFailed}
	if !reflect.DeepEqual(states, want) {
		t.Errorf("states %v, want %v", states, want)
	}
}

// jsonFields is a struct's JSON keys, with the Go type of each, in sorted
// order. The type is in there because a field that is an int on one side and a
// string on the other round-trips into a parse error rather than a mismatch.
func jsonFields(t reflect.Type) []string {
	var out []string
	for i := range t.NumField() {
		f := t.Field(i)
		if !f.IsExported() {
			continue
		}
		tag, _, _ := strings.Cut(f.Tag.Get("json"), ",")
		if tag == "-" {
			continue
		}
		if tag == "" {
			tag = f.Name
		}
		kind := f.Type.Kind().String()
		if f.Type.Kind() == reflect.Slice || f.Type.Kind() == reflect.Map {
			kind += " of " + f.Type.Elem().Kind().String()
		}
		out = append(out, tag+" "+kind)
	}
	sort.Strings(out)
	return out
}
