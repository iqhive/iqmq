package middleware_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	mq "bitbucket.org/iqhive/iqmq"
	"bitbucket.org/iqhive/iqmq/driverkit"
	"bitbucket.org/iqhive/iqmq/internal/errcode"
	"bitbucket.org/iqhive/iqmq/middleware"
)

// The failures below are the ones a driver really produces, built by the
// real driverkit.ErrorMapper.MapNet rather than by hand: a round trip whose
// request may already have left the socket, failing with EOF, a closed
// connection or a reset. MapNet classifies every one of them as
// CodeUnavailable, Retryable AND Ambiguous, and about eleven drivers call it on
// their publish path. TestRetryNeverRepeatsAnAmbiguousOutcome uses
// ErrSettlementUnknown, whose code is not on Transient's list at all, so it
// could not see that Transient read the code and never the ambiguity.

// errLinkDown is a transport failure that the one link rule every driver
// writes matches -- {Match: a net.Error, Code: Unavailable, Retryable: true}
// -- which is the path through MapNet where a matching rule decides the code
// and retryability and ClassifyNet's ambiguity is ORed in.
var errLinkDown = &net.OpError{Op: "write", Net: "tcp", Err: errors.New("connection reset by peer")}

// linkMapper is a driver's error mapper with that link rule.
var linkMapper = &driverkit.ErrorMapper{
	Driver: "test",
	Rules: map[string][]driverkit.Rule{
		mq.OpPublish: {{
			Match:     func(err error) bool { var ne net.Error; return errors.As(err, &ne) },
			Code:      mq.CodeUnavailable,
			Retryable: true,
		}},
	},
}

// publishFailures maps the transport failures of a publication, with
// sentAlready saying whether the request may have reached the broker.
func publishFailures(sentAlready bool) map[string]error {
	plain := &driverkit.ErrorMapper{Driver: "test"}
	return map[string]error{
		"EOF":                    plain.MapNet(mq.OpPublish, "orders", "", io.EOF, sentAlready),
		"unexpected EOF":         plain.MapNet(mq.OpPublish, "orders", "", io.ErrUnexpectedEOF, sentAlready),
		"closed connection":      plain.MapNet(mq.OpPublish, "orders", "", net.ErrClosed, sentAlready),
		"reset connection":       plain.MapNet(mq.OpPublish, "orders", "", errLinkDown, sentAlready),
		"link rule and a reset":  linkMapper.MapNet(mq.OpPublish, "orders", "", errLinkDown, sentAlready),
		"link rule, wrapped EOF": linkMapper.MapNet(mq.OpPublish, "orders", "", &net.OpError{Op: "read", Net: "tcp", Err: io.EOF}, sentAlready),
	}
}

// requireShape is the instrument check: every failure has to be the shape the
// defect needs -- a retryable CodeUnavailable whose ambiguity is exactly
// sentAlready -- or a green result below says nothing about it. If MapNet's
// classification ever changes, this fails rather than the test passing
// vacuously.
func requireShape(t *testing.T, name string, err error, ambiguous bool) {
	t.Helper()
	var oe *mq.OpError
	if !errors.As(err, &oe) {
		t.Fatalf("%s: MapNet returned %v, which carries no *OpError", name, err)
	}
	if oe.Code != mq.CodeUnavailable || !oe.Retryable || oe.Ambiguous != ambiguous {
		t.Fatalf("%s: MapNet returned Code=%v Retryable=%v Ambiguous=%v, want CodeUnavailable, Retryable, Ambiguous=%v: "+
			"the test no longer exercises the classification it was written for", name, oe.Code, oe.Retryable, oe.Ambiguous, ambiguous)
	}
}

// TestTransientNeverRetriesAnAmbiguousFailureWhateverItsCode is the rule
// for the default predicate: "The core MUST NOT auto-retry an ambiguous
// publish". CodeUnavailable is on Transient's retry list, so a predicate that
// reads only the code retries a publication that may already be in the broker,
// and one message becomes two. The ambiguity has to win, and it has to win
// through %w and through errors.Join -- including a join whose FIRST entry is
// a definite, retryable refusal, which is the shape a handler that published
// twice and failed twice returns.
func TestTransientNeverRetriesAnAmbiguousFailureWhateverItsCode(t *testing.T) {
	definite := publishFailures(false)["EOF"]
	requireShape(t, "the definite control", definite, false)
	for name, err := range publishFailures(true) {
		requireShape(t, name, err, true)
		forms := map[string]error{
			"bare":                   err,
			"wrapped with %w":        fmt.Errorf("handler: publishing the invoice: %w", err),
			"joined alone":           errors.Join(err),
			"joined after a refusal": errors.Join(definite, err),
			"joined, then wrapped":   fmt.Errorf("handler: %w", errors.Join(errors.New("audit write failed"), err)),
		}
		for form, e := range forms {
			if !mq.IsAmbiguous(e) {
				t.Errorf("%s (%s): IsAmbiguous = false for an error that carries an ambiguous *OpError: %v", name, form, e)
			}
			if middleware.Transient(e) {
				t.Errorf("%s (%s): Transient = true for an ambiguous publication (%v, code %v); retrying it is how one message becomes two ",
					name, form, e, mq.CodeOf(e))
			}
		}
	}
}

// TestTransientStillRetriesTheDefiniteForm is the control that keeps the test
// above honest: the SAME failures, reported before the request could have left
// the socket, are definite refusals with a retryable code, and Transient must
// still retry them. A Transient that returned false for everything would pass
// the ambiguity test and fail this one.
func TestTransientStillRetriesTheDefiniteForm(t *testing.T) {
	for name, err := range publishFailures(false) {
		requireShape(t, name, err, false)
		if !middleware.Transient(err) {
			t.Errorf("%s: Transient = false for a definite, retryable CodeUnavailable (%v): the retry list must still apply", name, err)
		}
		if !middleware.Transient(fmt.Errorf("handler: %w", err)) {
			t.Errorf("%s: Transient = false for the wrapped definite failure", name)
		}
	}
}

// TestRetryNeverRepeatsAnAmbiguousMapNetFailure is the same rule end to end,
// through the middleware with its default predicate: the handler is called
// exactly once and its own error comes back unchanged.
func TestRetryNeverRepeatsAnAmbiguousMapNetFailure(t *testing.T) {
	for name, err := range publishFailures(true) {
		for form, e := range map[string]error{
			"bare":            err,
			"wrapped with %w": fmt.Errorf("handler: %w", err),
		} {
			c := &calls{fn: func(int, *mq.Delivery) error { return e }}
			h := middleware.Retry(middleware.RetryConfig{MaxAttempts: 5, Clock: newFakeClock()})(c)
			if got := h.Handle(context.Background(), delivery("x")); !errors.Is(got, err) {
				t.Fatalf("%s (%s): Handle = %v, want the ambiguous failure unchanged", name, form, got)
			}
			if c.count() != 1 {
				t.Errorf("%s (%s): an ambiguous publication was retried %d times, want 0 ", name, form, c.count()-1)
			}
		}
	}
	// And the definite form is retried to the bound, so the one above is not
	// passing because Retry retries nothing.
	definite := publishFailures(false)["EOF"]
	c := &calls{fn: func(int, *mq.Delivery) error { return definite }}
	h := middleware.Retry(middleware.RetryConfig{MaxAttempts: 3, Clock: newFakeClock()})(c)
	_ = h.Handle(context.Background(), delivery("x"))
	if c.count() != 3 {
		t.Fatalf("a definite, retryable failure was attempted %d times, want MaxAttempts (3)", c.count())
	}
}

// TestCircuitBreakerStillCountsAnAmbiguousDependencyFailure pins what the
// Transient change must NOT change: the breaker's default predicate asks a
// different question -- "is the dependency failing?", not "may this be
// repeated?" -- and a connection that died after the write is the dependency
// failing. It used to answer through Transient; were it still to, taking the
// ambiguity out of Transient would have stopped the breaker counting the very
// failures a broken broker produces.
func TestCircuitBreakerStillCountsAnAmbiguousDependencyFailure(t *testing.T) {
	err := publishFailures(true)["EOF"]
	requireShape(t, "EOF", err, true)
	c := &calls{fn: func(int, *mq.Delivery) error { return err }}
	h := middleware.CircuitBreaker(middleware.BreakerConfig{Threshold: 2, Cooldown: time.Second, Clock: newFakeClock()})(c)
	for range 2 {
		if got := h.Handle(context.Background(), delivery("x")); !errors.Is(got, err) {
			t.Fatalf("Handle = %v, want the dependency's own failure while the circuit is closed", got)
		}
	}
	if got := h.Handle(context.Background(), delivery("x")); !errors.Is(got, middleware.ErrCircuitOpen) {
		t.Fatalf("Handle = %v after two ambiguous CodeUnavailable failures, want ErrCircuitOpen: the breaker stopped counting a dependency that is failing", got)
	}
	if c.count() != 2 {
		t.Fatalf("the handler ran %d times, want 2", c.count())
	}
}

// TestTransientNeverRetriesAmbiguityCarriedInTheCode is the same rule for an
// error whose ambiguity is in its CODE and not in its flag: CodeAmbiguous
// ("a publish whose acceptance is unknown") or CodeSettlementUnknown. The
// bare form was already refused, because neither code is on the retry list;
// the joined form was not, because CodeOf answers with the first branch that
// carries a code, so a definite retryable refusal joined in FRONT of it hid
// it -- the same shape the flag had before IsAmbiguous read every branch.
// drivers/redis's short WAITAOF (errcode.Ambiguous, mapped through MapNet) is
// the real source of the code-only shape.
func TestTransientNeverRetriesAmbiguityCarriedInTheCode(t *testing.T) {
	definite := publishFailures(false)["EOF"]
	codeOnly := map[string]error{
		"flagless CodeAmbiguous":            &mq.OpError{Driver: "test", Operation: mq.OpPublish, Code: mq.CodeAmbiguous},
		"flagless CodeSettlementUnknown":    &mq.OpError{Driver: "test", Operation: mq.OpPublish, Code: mq.CodeSettlementUnknown},
		"the ErrSettlementUnknown sentinel": mq.ErrSettlementUnknown,
		"a short WAITAOF through MapNet": (&driverkit.ErrorMapper{Driver: "redis"}).MapNet(mq.OpPublish, "orders", "",
			errcode.New(errcode.Ambiguous, "WAITAOF came back short"), true),
	}
	for name, err := range codeOnly {
		for form, e := range map[string]error{
			"bare":                   err,
			"wrapped with %w":        fmt.Errorf("handler: %w", err),
			"joined after a refusal": errors.Join(definite, err),
		} {
			if !mq.IsAmbiguous(e) {
				t.Errorf("%s (%s): IsAmbiguous = false for %v (code %v)", name, form, e, mq.CodeOf(e))
			}
			if middleware.Transient(e) {
				t.Errorf("%s (%s): Transient = true for %v (code %v); it may already be in the broker ",
					name, form, e, mq.CodeOf(e))
			}
		}
	}
}
