package main

import (
	"errors"
	"fmt"
	"testing"

	mq "bitbucket.org/iqhive/iqmq"
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
