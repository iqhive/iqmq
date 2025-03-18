package errcode

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
)

func TestCodeNames(t *testing.T) {
	for c := Code(0); c < codeCount; c++ {
		s := c.String()
		if s == "" || strings.HasPrefix(s, "Code(") {
			t.Errorf("code %d has no name", c)
		}
		if !c.Valid() {
			t.Errorf("code %d reported invalid", c)
		}
	}
	if got := Code(999).String(); got != "Code(999)" {
		t.Errorf("unknown code name = %q", got)
	}
	if Code(999).Valid() {
		t.Error("Code(999) reported valid")
	}
	if Count != int(codeCount) {
		t.Errorf("Count = %d, want %d", Count, codeCount)
	}
}

func TestSentinelCodes(t *testing.T) {
	cases := []struct {
		err  *Error
		code Code
	}{
		{ErrClosed, Closed},
		{ErrUnsupported, Unsupported},
		{ErrNotSettleable, NotSettleable},
		{ErrAlreadySettled, AlreadySettled},
		{ErrSettlementInProgress, SettlementInProgress},
		{ErrSettlementUnknown, SettlementUnknown},
		{ErrLeaseLost, LeaseLost},
		{ErrMessageTooLarge, MessageTooLarge},
		{ErrBackpressure, Backpressure},
		{ErrQueueFull, ResourceExhausted},
		{ErrPoisonMessage, PoisonMessage},
		{ErrTopicNotFound, NotFound},
		{ErrInvalidTopic, InvalidArgument},
		{ErrDriverNotRegistered, DriverNotRegistered},
		{ErrCanceled, Canceled},
		{ErrDeadlineExceeded, DeadlineExceeded},
	}
	for _, c := range cases {
		if c.err.Code() != c.code {
			t.Errorf("%v: code %v, want %v", c.err, c.err.Code(), c.code)
		}
		if c.err.Error() == "" {
			t.Errorf("%v: empty message", c.code)
		}
	}
}

type opErr struct {
	code Code
	err  error
}

func (o *opErr) Error() string { return "op: " + o.err.Error() }
func (o *opErr) Code() Code    { return o.code }
func (o *opErr) Unwrap() error { return o.err }

func TestIsMatchesByCode(t *testing.T) {
	if !errors.Is(ErrClosed, ErrClosed) {
		t.Error("sentinel does not match itself")
	}
	if !errors.Is(New(Closed, "other closed"), ErrClosed) {
		t.Error("distinct sentinel with same code does not match")
	}
	if errors.Is(ErrClosed, ErrLeaseLost) {
		t.Error("different codes matched")
	}
	wrapped := fmt.Errorf("outer: %w", ErrLeaseLost)
	if !errors.Is(wrapped, ErrLeaseLost) {
		t.Error("wrapped sentinel did not match")
	}
	// A foreign Coder (like the root package's *OpError) matches the sentinel
	// with the same code even though it is a different type.
	op := &opErr{code: Closed, err: errors.New("native")}
	if !errors.Is(ErrClosed, op) {
		t.Error("sentinel.Is did not accept a foreign Coder target")
	}
}

func TestCodeOf(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want Code
	}{
		{"nil", nil, Unknown},
		{"plain", errors.New("x"), Unknown},
		{"sentinel", ErrLeaseLost, LeaseLost},
		{"wrapped", fmt.Errorf("a: %w", fmt.Errorf("b: %w", ErrBackpressure)), Backpressure},
		{"coder", &opErr{code: Unavailable, err: errors.New("dial")}, Unavailable},
		{"canceled", context.Canceled, Canceled},
		{"wrapped canceled", fmt.Errorf("x: %w", context.Canceled), Canceled},
		{"deadline", context.DeadlineExceeded, DeadlineExceeded},
		{"joined", errors.Join(errors.New("plain"), ErrQueueFull), ResourceExhausted},
		{"joined none", errors.Join(errors.New("a"), errors.New("b")), Unknown},
	}
	for _, c := range cases {
		if got := CodeOf(c.err); got != c.want {
			t.Errorf("%s: CodeOf = %v, want %v", c.name, got, c.want)
		}
	}
}

func TestAllocs(t *testing.T) {
	wrapped := fmt.Errorf("x: %w", ErrClosed)
	if n := testing.AllocsPerRun(1000, func() {
		if !errors.Is(wrapped, ErrClosed) {
			t.Fatal("mismatch")
		}
	}); n != 0 {
		t.Errorf("errors.Is allocated %v times", n)
	}
	if n := testing.AllocsPerRun(1000, func() {
		if CodeOf(wrapped) != Closed {
			t.Fatal("mismatch")
		}
	}); n != 0 {
		t.Errorf("CodeOf allocated %v times", n)
	}
}

// TestNotAttemptedNeverCarriesAnUnknownOutcome pins the one rule every
// not-attempted answer shares, the core's and the drivers': a message that never left the process has a definite outcome, so
// its code is the stopping failure's own -- which keeps errors.Is against
// ErrClosed or context.DeadlineExceeded working -- EXCEPT when that code means
// an unknown outcome. stomp's receipt timeout is CodeSettlementUnknown, and a
// not-sent answer that copied it read "may be in the broker".
func TestNotAttemptedNeverCarriesAnUnknownOutcome(t *testing.T) {
	for _, tc := range []struct{ in, want Code }{
		{Ambiguous, Unavailable},
		{SettlementUnknown, Unavailable},
		{Closed, Closed},
		{Canceled, Canceled},
		{DeadlineExceeded, DeadlineExceeded},
		{Unavailable, Unavailable},
		{MessageTooLarge, MessageTooLarge},
		{Unknown, Unknown},
	} {
		if got := NotAttempted(tc.in); got != tc.want {
			t.Errorf("NotAttempted(%v) = %v, want %v", tc.in, got, tc.want)
		}
	}
	if CodeOf(ErrNotAttempted) != Unknown {
		t.Errorf("ErrNotAttempted carries %v: the cause must add no code of its own", CodeOf(ErrNotAttempted))
	}
}
