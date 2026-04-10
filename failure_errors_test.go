package iqmq_test

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	mq "bitbucket.org/iqhive/iqmq"
)

// The two errors the runner hands back when a delivery stops it, and the one
// thing they must never do: carry the payload.
//
// run.go states it on PoisonError's fields -- "The body and the headers are
// deliberately not here: the specification keeps payloads out of errors" -- and the reason
// is that these strings go into logs, metrics labels and traces. A renderer
// that reached for the message would put an application's payload into every
// one of those places, and it would do it only on the failure path, where
// nobody is looking until they have to be.
//
// The second thing they must do is unwrap correctly. Halt is
// CodePoisonMessage, and a runner whose PoisonError did not satisfy
// errors.Is(err, ErrPoisonMessage) would be a halt the caller's error handling
// walks straight past.

// errHandler is what a handler returned, with a recognisable message.
var errHandler = errors.New("the handler could not process this")

func TestPoisonErrorNamesTheDeliveryWithoutThePayload(t *testing.T) {
	t.Parallel()
	const (
		secretBody   = "SECRET-BODY-BYTES"
		secretHeader = "SECRET-HEADER-VALUE"
	)
	e := &mq.PoisonError{
		Topic:        "orders",
		Subscription: "billing",
		MessageID:    "msg-42",
		Attempt:      5,
		Err:          errHandler,
	}
	got := e.Error()
	// What an operator needs in order to find the message: which topic, which
	// subscription, which message and which attempt. Each is asserted
	// separately because each is a separate `if` in the renderer, and a
	// dropped one is a halt an operator cannot trace.
	for _, want := range []string{"orders", "billing", "msg-42", "attempt=5", errHandler.Error()} {
		if !strings.Contains(got, want) {
			t.Errorf("Error() = %q, missing %q", got, want)
		}
	}
	if !strings.Contains(got, "poison") {
		t.Errorf("Error() = %q, does not say what kind of failure it is", got)
	}
	// And what it must not carry. The struct has no body or header field at
	// all, so this is a guard against one being added and rendered rather than
	// a check of today's code -- which is exactly what the specification needs, because the
	// day someone adds one is the day the payload starts appearing in logs.
	for _, forbidden := range []string{secretBody, secretHeader} {
		if strings.Contains(got, forbidden) {
			t.Errorf("Error() = %q, carries the payload", got)
		}
	}
	// Every field is optional and an absent one is simply left out, not
	// rendered as an empty `topic=`: a label with an empty value is a metrics
	// series nobody can group by.
	bare := &mq.PoisonError{Attempt: 1}
	got = bare.Error()
	for _, forbidden := range []string{"topic=", "subscription=", "id="} {
		if strings.Contains(got, forbidden) {
			t.Errorf("Error() on an empty PoisonError = %q, renders an empty %q", got, forbidden)
		}
	}
	if !strings.Contains(got, "attempt=1") {
		t.Errorf("Error() = %q, must always name the attempt", got)
	}
	// The attempt is always rendered, including attempt zero: zero is "it never
	// got a delivery attempt", which is different from "the field is missing".
	if got := (&mq.PoisonError{}).Error(); !strings.Contains(got, "attempt=0") {
		t.Errorf("Error() = %q, want attempt=0 rendered", got)
	}
	// Each field in isolation, so a renderer that dropped one arm is caught on
	// that arm rather than hidden by the others.
	for _, tc := range []struct {
		name string
		e    *mq.PoisonError
		want string
	}{
		{"only a topic", &mq.PoisonError{Topic: "t"}, "topic=t"},
		{"only a subscription", &mq.PoisonError{Subscription: "s"}, "subscription=s"},
		{"only a message id", &mq.PoisonError{MessageID: "m"}, "id=m"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := tc.e.Error(); !strings.Contains(got, tc.want) {
				t.Errorf("Error() = %q, missing %q", got, tc.want)
			}
		})
	}
}

func TestPoisonErrorIsAHaltTheCallersErrorHandlingCanSee(t *testing.T) {
	t.Parallel()
	// the specification's "never skip by default" is only observable if the halt is
	// recognisable. A PoisonError that did not satisfy errors.Is against
	// ErrPoisonMessage would be a halt every `if errors.Is(err, ...)` in a
	// caller's shutdown path walks past, and the process would look like a
	// clean exit.
	withErr := &mq.PoisonError{Err: errHandler, Topic: "t"}
	if !errors.Is(withErr, mq.ErrPoisonMessage) {
		t.Error("a PoisonError is not ErrPoisonMessage; a caller cannot tell a halt from a clean return")
	}
	// The handler's own error has to be reachable too, or the operator gets
	// "poison message" and never learns why.
	if !errors.Is(withErr, errHandler) {
		t.Error("a PoisonError does not unwrap to the handler's error; the diagnosis is lost")
	}
	if got := mq.CodeOf(withErr); got != mq.CodePoisonMessage {
		t.Errorf("CodeOf = %v, want CodePoisonMessage", got)
	}
	// A PoisonError with no handler error is still a halt: a message may
	// exhaust its attempts without the last attempt returning anything
	// recognisable, and the halt must not become invisible because of it.
	bare := &mq.PoisonError{Topic: "t"}
	if !errors.Is(bare, mq.ErrPoisonMessage) {
		t.Error("a PoisonError with no wrapped error is not ErrPoisonMessage")
	}
	if got := mq.CodeOf(bare); got != mq.CodePoisonMessage {
		t.Errorf("CodeOf = %v, want CodePoisonMessage", got)
	}
	// Unwrap's shape is the mechanism, and it must not hand back a nil element:
	// errors.Is walks the slice and a nil in it is a tree with a hole in it.
	for _, e := range []*mq.PoisonError{withErr, bare} {
		errs := e.Unwrap()
		if len(errs) == 0 {
			t.Fatalf("Unwrap() on %+v is empty", e)
		}
		for i, u := range errs {
			if u == nil {
				t.Errorf("Unwrap()[%d] is nil", i)
			}
		}
	}
	if n := len(withErr.Unwrap()); n != 2 {
		t.Errorf("Unwrap() with a handler error has %d elements, want 2", n)
	}
	if n := len(bare.Unwrap()); n != 1 {
		t.Errorf("Unwrap() with no handler error has %d elements, want 1", n)
	}
	// And it is NOT the poison error for an unrelated failure, or every
	// transient error would halt the consumer.
	if errors.Is(errHandler, mq.ErrPoisonMessage) {
		t.Error("an ordinary handler error matches ErrPoisonMessage")
	}
}

// panicValue is a non-error, non-string panic value with a recognisable
// rendering, so the default arm of PanicError.Error is observable.
type panicValue struct{ N int }

func (p panicValue) String() string { return "panicValue(" + fmt.Sprint(p.N) + ")" }

func TestPanicErrorRendersEveryPanicValueAHandlerCanProduce(t *testing.T) {
	t.Parallel()
	// A handler may panic with anything. Each arm here is one of the four
	// shapes, and the one that matters most is nil: `panic(nil)` is a real
	// panic that the runner must not render as an empty string, because an
	// empty error message in a log is indistinguishable from no error at all --
	// and the specification says no panic may silently ack.
	for _, tc := range []struct {
		name  string
		value any
		want  string
	}{
		{"a string", "something went wrong", "something went wrong"},
		{"an empty string", "", "iqmq: handler panic: "},
		{"an error", errHandler, errHandler.Error()},
		{"nil", nil, "nil"},
		{"a struct", panicValue{7}, "panicValue(7)"},
		{"an integer", 42, "42"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			e := &mq.PanicError{Value: tc.value, Stack: []byte("goroutine 1 [running]:\nSTACK-BYTES")}
			got := e.Error()
			if !strings.Contains(got, tc.want) {
				t.Errorf("Error() = %q, missing %q", got, tc.want)
			}
			if !strings.Contains(got, "handler panic") {
				t.Errorf("Error() = %q, does not say it was a panic", got)
			}
			if got == "" {
				t.Error("Error() is empty; an empty error message in a log reads as no error")
			}
			// The stack is deliberately NOT in the string: run.go says an error
			// string goes into logs and metrics labels, and a multi-kilobyte
			// stack in a metrics label is a cardinality and size problem in
			// every backend that receives it. It stays reachable on the field.
			if strings.Contains(got, "STACK-BYTES") {
				t.Errorf("Error() = %q, carries the stack into the error string", got)
			}
			if !strings.Contains(string(e.Stack), "STACK-BYTES") {
				t.Error("the stack is not on the field either; it has been lost entirely")
			}
		})
	}
	// A panic with an error value unwraps to it, so a handler that panicked
	// with a recognisable sentinel is still recognisable.
	withErr := &mq.PanicError{Value: errHandler}
	if !errors.Is(withErr, errHandler) {
		t.Error("a PanicError carrying an error does not unwrap to it")
	}
	if got := withErr.Unwrap(); got == nil {
		t.Error("Unwrap() is nil for a panic whose value is an error")
	}
	// A panic with a non-error value unwraps to nil rather than to something
	// invented: errors.As must not match a type the handler never panicked
	// with.
	for _, v := range []any{"a string", nil, 42, panicValue{1}} {
		if got := (&mq.PanicError{Value: v}).Unwrap(); got != nil {
			t.Errorf("Unwrap() for a %T panic = %v, want nil", v, got)
		}
	}
	// A nil panic value and a panic with the string "nil" must not render
	// identically to each other by accident in a way that loses which it was:
	// both are legible, and the point is that neither is empty.
	if (&mq.PanicError{Value: nil}).Error() == "" {
		t.Error("a nil panic renders as the empty string")
	}
}
