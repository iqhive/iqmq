package testkit

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	mq "bitbucket.org/iqhive/iqmq"
)

// TestAFeatureScenarioThatNeverReachedAClientSaysItsLevelIsUnread checks
// that a documented skip, a refused topology and a failed Open retain
// their feature name and report LevelUnread when no client was reached.
//
// Each case exercises runScenario directly. An empty level must fail
// validation rather than stand in for an unread capability table.
func TestAFeatureScenarioThatNeverReachedAClientSaysItsLevelIsUnread(t *testing.T) {
	scenario := func(name string) Scenario {
		for _, s := range Scenarios() {
			if s.Name == name {
				return s
			}
		}
		t.Fatalf("the suite has no scenario %q", name)
		return Scenario{}
	}
	harness := func(open func(context.Context, Need) (*mq.Client, error), skip map[string]string) Harness {
		return Harness{
			Driver: "fixture", Evidence: EvidenceInProcess, Timeout: time.Second,
			Open: open, Skip: skip,
		}
	}
	mustNotOpen := func(context.Context, Need) (*mq.Client, error) {
		t.Error("a documented skip opened a client")
		return nil, errors.New("unreachable")
	}
	refuse := func(context.Context, Need) (*mq.Client, error) {
		return nil, fmt.Errorf("fixture: no retained topic: %w", mq.ErrUnsupported)
	}
	broken := func(context.Context, Need) (*mq.Client, error) {
		return nil, errors.New("dial tcp: connection refused")
	}

	for _, tc := range []struct {
		path     string
		scenario string
		h        Harness
		outcome  Outcome
	}{
		// A general scenario that carries a feature, which is where the
		// nightly's first refusal was: `dlq/failed-publish-never-acks`.
		{"a documented skip", "dlq/failed-publish-never-acks", harness(mustNotOpen, map[string]string{"dlq/failed-publish-never-acks": "no dead letter"}), OutcomeSkip},
		{"a topology the driver refuses", "capability/Replay/claim", harness(refuse, nil), OutcomeSkip},
		{"the open itself failed", "capability/Replay/works", harness(broken, nil), OutcomeFail},
	} {
		t.Run(tc.path, func(t *testing.T) {
			res, _, opened := runScenario(t.Context(), tc.h, scenario(tc.scenario))
			if opened {
				t.Fatal("the fixture reached a client, so it is not testing an early return")
			}
			if res.Outcome != tc.outcome || res.Feature == "" {
				t.Fatalf("outcome %q, feature %q; want %q and a feature", res.Outcome, res.Feature, tc.outcome)
			}
			if res.Claimed != LevelUnread {
				t.Errorf("claimed %q, want %q: the level was never read and the result has to say so rather than leave it empty", res.Claimed, LevelUnread)
			}
			rep := &Report{
				Schema: Schema, Driver: "fixture", Date: time.Now().UTC().Format(time.RFC3339),
				Evidence: EvidenceInProcess, Results: []Result{res},
			}
			if err := rep.Validate(); err != nil {
				t.Errorf("the kit refused the result it had just written: %v", err)
			}
			if vs := rep.Verify(); len(vs) != 1 || vs[0].Claimed != LevelUnread || vs[0].Proven() {
				t.Errorf("Verify: %+v; want one feature whose level is unread and which is not proven", vs)
			}
		})
	}

	base := func(res Result) *Report {
		return &Report{
			Schema: Schema, Driver: "fixture", Date: time.Now().UTC().Format(time.RFC3339),
			Evidence: EvidenceInProcess, Results: []Result{res},
		}
	}
	// An EMPTY level is refused wherever a feature is named: it is what a
	// kit that lost a level writes, and conformance/report refuses it too.
	for _, o := range []Outcome{OutcomePass, OutcomeSkip, OutcomeFail} {
		empty := Result{Scenario: "capability/Replay/works", Feature: "Replay", Outcome: o, Note: "n"}
		if err := base(empty).Validate(); !errors.Is(err, errReportLevel) {
			t.Errorf("an empty level on a %s was accepted (%v)", o, err)
		}
	}
	// A PASS always had a client, so "unread" there is false.
	passed := Result{Scenario: "capability/Replay/works", Feature: "Replay", Claimed: LevelUnread, Outcome: OutcomePass}
	if err := base(passed).Validate(); !errors.Is(err, errReportLevel) {
		t.Errorf("a PASS whose level was never read was accepted (%v): a scenario cannot pass without a client", err)
	}
	// A level that is none of the five and not "unread" is refused
	// wherever it appears.
	bogus := Result{Scenario: "capability/Replay/works", Feature: "Replay", Outcome: OutcomeSkip, Claimed: Level("partial")}
	if err := base(bogus).Validate(); !errors.Is(err, errReportLevel) {
		t.Errorf("a claimed level of %q was accepted on a skip: %v", bogus.Claimed, err)
	}
}
