package testkit

import (
	"errors"
	"strings"
	"testing"

	mq "github.com/iqhive/iqmq"
)

// TestHealthNote covers what a failing scenario will carry about its driver.
//
// What this does NOT cover, stated rather than implied: that `run` calls it on
// the failure path. That is covered end to end only for the Ready case, by
// TestAFailingScenarioOnAHealthyDriverAddsNoHealthNote below -- degrading a
// driver's health from a test would need a new `Faults` field and the driver
// plumbing to honour it, which is more surface than this note is worth. The
// branch that matters most for the kafka case, Degraded with an error, is
// covered here at the unit level.
func TestHealthNote(t *testing.T) {
	for _, tc := range []struct {
		label     string
		in        mq.HealthSnapshot
		want      string
		wantParts []string
	}{
		{
			// The whole point of the empty case: a scenario that fails on a
			// wrong body must not gain a line about a healthy driver.
			label: "Ready says nothing",
			in:    mq.HealthSnapshot{State: mq.HealthReady},
			want:  "",
		},
		{
			// The kafka case. `fetchErr` sends kgo.ErrDataLoss to
			// health.Degraded and returns nil, so this is where the cause of
			// "no delivery arrived" actually lives.
			label:     "Degraded carries its error",
			in:        mq.HealthSnapshot{State: mq.HealthDegraded, Err: errors.New("data loss: reset to offset 42")},
			wantParts: []string{"degraded", "data loss", "offset 42"},
		},
		{
			// A state without an error must still be reported: "not Ready and
			// nobody said why" is itself worth seeing in a failing log.
			label:     "a state with no error still reports the state",
			in:        mq.HealthSnapshot{State: mq.HealthDegraded},
			wantParts: []string{"degraded"},
		},
	} {
		t.Run(tc.label, func(t *testing.T) {
			got := healthNote(tc.in)
			if tc.want != "" || len(tc.wantParts) == 0 {
				if got != tc.want {
					t.Fatalf("healthNote = %q, want %q", got, tc.want)
				}
				return
			}
			for _, part := range tc.wantParts {
				if !strings.Contains(got, part) {
					t.Errorf("the note does not mention %q, so a reader of a failing log cannot act on it: %q", part, got)
				}
			}
		})
	}
}
