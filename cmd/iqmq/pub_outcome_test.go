package main

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	mq "github.com/iqhive/iqmq"
)

// TestPubNamesAnAmbiguousPublicationAsNotConfirmed: `iqmq pub` told an operator
// a publication was "refused" unless its code was CodeAmbiguous, so a
// connection that died after the write -- CodeUnavailable with the Ambiguous
// flag, the answer most drivers give -- read as a refusal, and an operator who
// published it again duplicated it. It now asks mq.IsAmbiguous, the same
// question the rest of the library asks.
func TestPubNamesAnAmbiguousPublicationAsNotConfirmed(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		want string
	}{
		{"a refusal", &mq.OpError{Code: mq.CodeInvalidArgument}, "refused"},
		{"ambiguous by its flag", &mq.OpError{Code: mq.CodeUnavailable, Retryable: true, Ambiguous: true}, notConfirmed},
		{"ambiguous by its code", &mq.OpError{Code: mq.CodeAmbiguous}, notConfirmed},
		{"ambiguous, wrapped", fmt.Errorf("x: %w", &mq.OpError{Code: mq.CodeUnavailable, Ambiguous: true}), notConfirmed},
		{"a plain error", errors.New("boom"), "refused"},
	} {
		if got := outcomeWord(tc.err); got != tc.want {
			t.Errorf("%s: outcomeWord(%v) = %q, want %q", tc.name, tc.err, got, tc.want)
		}
	}
}

// TestPubConfirmsNothingAfterAFailedFlush: Publish returning nil means a
// buffering driver admitted the message, and the Flush after it is what
// confirms it. pub counted the admissions as confirmations, so a run whose
// Flush failed printed "confirmed 3 of 3" beside "1 of 3 publications were not
// confirmed". A failed Flush leaves every admitted publication's acceptance
// unknown, which is what the counts now say.
func TestPubConfirmsNothingAfterAFailedFlush(t *testing.T) {
	sink := &fakeSink{log: &seamLog{}, flushErr: errors.New("the buffer could not be sent")}
	h := sinkHarness(t, sink).withStdin("a\nb\nc\n")
	if code := h.run("pub", "seamdst://", "mirror", "--file", "-"); code != exitFailure {
		t.Fatalf("pub = %d with a failing Flush, want %d\nstderr:\n%s", code, exitFailure, h.stderr())
	}
	if !strings.Contains(h.stdout(), "confirmed 0 of 3") {
		t.Errorf("pub counted admissions as confirmations after a failed Flush:\n%s", h.stdout())
	}
	for _, want := range []string{notConfirmed, "3 of 3 publications were not confirmed"} {
		if !strings.Contains(h.stderr(), want) {
			t.Errorf("stderr does not say %q:\n%s", want, h.stderr())
		}
	}
}
