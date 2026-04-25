package testkit

import (
	"context"
	"errors"
	"strings"
	"testing"

	mq "bitbucket.org/iqhive/iqmq"
)

// TestADeadSkipKeyIsRefused is the allowlist rule applied to conformance
// skips: an exemption whose subject no longer exists keeps excusing a driver
// for something nobody runs, and the driver's published matrix entry still
// carries the reason.
//
// It is the shape this repository has had twice in other allowlists, and a
// conformance skip is the version a reader is most likely to believe: the
// matrix says "inapplicable" with a paragraph of justification, and no
// scenario by that name has existed for months.
//
// No driver carries one today — all 17 keys across the tree name a live
// scenario — so this check is preventive, and it is the reason it needs a
// test of its own rather than a green run.
func TestADeadSkipKeyIsRefused(t *testing.T) {
	live := Scenarios()[0].Name

	ok := Harness{
		Driver:   "d",
		Evidence: EvidenceInProcess,
		// validate never opens anything; the opener only has to be non-nil.
		Open: func(context.Context, Need) (*mq.Client, error) {
			return nil, errors.New("this harness is never opened")
		},
		Skip: map[string]string{live: "a live scenario, documented as inapplicable"},
	}
	if err := ok.validate(); err != nil {
		t.Fatalf("a harness skipping a scenario that exists was refused: %v", err)
	}

	dead := ok
	dead.Skip = map[string]string{
		live:                          "a live one",
		"settle/a-scenario-long-gone": "the exemption that outlived its subject",
	}
	err := dead.validate()
	if err == nil {
		t.Fatal("a Skip key naming no scenario was accepted: the driver's matrix would keep publishing " +
			"an exemption for something the kit does not run")
	}
	if !errors.Is(err, errDeadSkip) {
		t.Errorf("validate reported %v, want errDeadSkip", err)
	}
	if !strings.Contains(err.Error(), "settle/a-scenario-long-gone") {
		t.Errorf("the refusal does not name the dead key: %v", err)
	}
	if strings.Contains(err.Error(), live) {
		t.Errorf("the refusal names the LIVE key as well: %v", err)
	}
}
