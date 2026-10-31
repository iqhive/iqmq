package iqmq_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/internal/fuzzutil"
)

// TestErrorsMatchByCode is the winning discipline, stated as a
// contract: the specification's "errors.Is matches by Code".
//
// It is the half of the error model that is easy to get wrong in the other
// direction: a caller who expects identity matching will be surprised that
// two sentinels sharing a code are indistinguishable, so both halves are
// pinned here.
func TestErrorsMatchByCode(t *testing.T) {
	t.Run("DistinctValuesOneCodeMatch", func(t *testing.T) {
		// Two *OpError values from two drivers, same code, nothing in
		// common. The specification makes them errors.Is-equal, which is what lets a
		// caller compare a driver's error against a root sentinel without
		// the driver importing the sentinel.
		a := &mq.OpError{Driver: "kafka", Operation: mq.OpPublish, Code: mq.CodeClosed}
		b := &mq.OpError{Driver: "memory", Operation: mq.OpAck, Code: mq.CodeClosed}
		if !errors.Is(a, b) {
			t.Fatal("two *OpError values with one code did not match: the specification matches by code")
		}
		if !errors.Is(a, mq.ErrClosed) || !errors.Is(b, mq.ErrClosed) {
			t.Fatal("an *OpError with CodeClosed did not match ErrClosed")
		}
	})

	t.Run("DifferentCodesDoNotMatch", func(t *testing.T) {
		a := &mq.OpError{Code: mq.CodeClosed}
		if errors.Is(a, mq.ErrLeaseLost) {
			t.Fatal("CodeClosed matched ErrLeaseLost")
		}
		if errors.Is(mq.ErrClosed, mq.ErrLeaseLost) {
			t.Fatal("ErrClosed matched ErrLeaseLost")
		}
	})

	t.Run("CodelessSentinelDoesNotMatch", func(t *testing.T) {
		// A plain errors.New carries no code, so it is not equal to any
		// coded error in either direction. Without this, CodeUnknown would
		// act as a wildcard and every uncoded error would match every
		// sentinel.
		plain := errors.New("a driver's own error")
		coded := &mq.OpError{Code: mq.CodeClosed, Err: plain}
		if errors.Is(coded, plain) {
			// errors.Is(coded, plain) is true only through the unwrap chain,
			// which is correct: plain IS in the chain. The wrong answer is
			// matching a codeless sentinel that is NOT in the chain.
			t.Log("plain is in the chain, so this match is by identity and correct")
		}
		other := errors.New("an unrelated error")
		if errors.Is(coded, other) {
			t.Fatal("a coded error matched an unrelated codeless error")
		}
		if errors.Is(other, mq.ErrClosed) {
			t.Fatal("a codeless error matched ErrClosed")
		}
		if got := mq.CodeOf(plain); got != mq.CodeUnknown {
			t.Fatalf("CodeOf(codeless) = %v, want CodeUnknown", got)
		}
	})

	t.Run("UnknownIsNotAWildcard", func(t *testing.T) {
		// CodeUnknown is the zero ErrorCode, so a driver that forgot to
		// classify a failure produces an error carrying it. If the match
		// compared codes without excluding Unknown, that error would be
		// errors.Is-equal to every codeless error in the program — and,
		// since CodeOf of anything unclassified is also Unknown, to every
		// unclassified error too. That is the wildcard the specification must not have.
		unclassified := &mq.OpError{Driver: "d", Operation: mq.OpPublish, Code: mq.CodeUnknown}
		for _, other := range []error{
			errors.New("somebody else's error"),
			&mq.OpError{Driver: "e", Operation: mq.OpAck, Code: mq.CodeUnknown},
		} {
			if errors.Is(unclassified, other) {
				t.Fatalf("an *OpError with CodeUnknown matched %v: CodeUnknown is not a wildcard", other)
			}
		}
		// And it still does not match a coded sentinel.
		if errors.Is(unclassified, mq.ErrClosed) {
			t.Fatal("an *OpError with CodeUnknown matched ErrClosed")
		}
	})

	t.Run("SettlementErrorsAreReachable", func(t *testing.T) {
		// the settlement outcomes have to be reachable from the root, and
		// each has to be its own code: a settlement layer that could not
		// tell "already settled" from "lease lost" would ack the wrong
		// delivery.
		cases := []struct {
			err  error
			code mq.ErrorCode
		}{
			{mq.ErrAlreadySettled, mq.CodeAlreadySettled},
			{mq.ErrLeaseLost, mq.CodeLeaseLost},
			{mq.ErrClosed, mq.CodeClosed},
			{mq.ErrNotSettleable, mq.CodeNotSettleable},
			{mq.ErrSettlementInProgress, mq.CodeSettlementInProgress},
		}
		seen := map[mq.ErrorCode]error{}
		for _, c := range cases {
			if got := mq.CodeOf(c.err); got != c.code {
				t.Errorf("CodeOf(%v) = %v, want %v", c.err, got, c.code)
			}
			if prev, ok := seen[c.code]; ok {
				t.Errorf("%v and %v share code %v: the settlement outcomes must stay distinguishable", prev, c.err, c.code)
			}
			seen[c.code] = c.err
			for _, other := range cases {
				want := other.code == c.code
				if got := errors.Is(c.err, other.err); got != want {
					t.Errorf("errors.Is(%v, %v) = %v, want %v", c.err, other.err, got, want)
				}
			}
		}
	})

	t.Run("EverySentinelHasItsDocumentedCode", func(t *testing.T) {
		cases := []struct {
			name string
			err  error
			code mq.ErrorCode
		}{
			{"ErrClosed", mq.ErrClosed, mq.CodeClosed},
			{"ErrUnsupported", mq.ErrUnsupported, mq.CodeUnsupported},
			{"ErrNotSettleable", mq.ErrNotSettleable, mq.CodeNotSettleable},
			{"ErrAlreadySettled", mq.ErrAlreadySettled, mq.CodeAlreadySettled},
			{"ErrSettlementInProgress", mq.ErrSettlementInProgress, mq.CodeSettlementInProgress},
			{"ErrLeaseLost", mq.ErrLeaseLost, mq.CodeLeaseLost},
			{"ErrMessageTooLarge", mq.ErrMessageTooLarge, mq.CodeMessageTooLarge},
			{"ErrBackpressure", mq.ErrBackpressure, mq.CodeBackpressure},
			{"ErrQueueFull", mq.ErrQueueFull, mq.CodeResourceExhausted},
			{"ErrPoisonMessage", mq.ErrPoisonMessage, mq.CodePoisonMessage},
			{"ErrTopicNotFound", mq.ErrTopicNotFound, mq.CodeNotFound},
			{"ErrInvalidTopic", mq.ErrInvalidTopic, mq.CodeInvalidArgument},
			{"ErrDriverNotRegistered", mq.ErrDriverNotRegistered, mq.CodeDriverNotRegistered},
		}
		if len(cases) != 13 {
			t.Fatalf("the var list names thirteen sentinels; this table has %d", len(cases))
		}
		for _, c := range cases {
			if got := mq.CodeOf(c.err); got != c.code {
				t.Errorf("%s: CodeOf = %v, want %v", c.name, got, c.code)
			}
			if c.err.Error() == "" {
				t.Errorf("%s: empty message", c.name)
			}
		}
	})
}

// TestOpError holds the requirements on the contextual error: the native
// error stays reachable, the address never appears in the message, and the
// context errors stay discoverable.
func TestOpError(t *testing.T) {
	native := errors.New("dial tcp 10.0.0.1:9092: connection refused")
	e := &mq.OpError{
		Driver: "kafka", Operation: mq.OpPublish, Topic: "orders",
		Subscription: "billing",
		Address:      "user:s3cr3t@broker.internal:9092",
		Code:         mq.CodeUnavailable, Retryable: true,
		Err: native,
	}
	t.Run("NativeErrorReachable", func(t *testing.T) {
		if !errors.Is(e, native) {
			t.Fatal("the native error is not reachable through errors.Is")
		}
		var target *mq.OpError
		if !errors.As(error(e), &target) || target != e {
			t.Fatal("errors.As did not find the *OpError")
		}
	})
	t.Run("AddressRedacted", func(t *testing.T) {
		msg := e.Error()
		for _, secret := range []string{"broker.internal", "s3cr3t", "user:"} {
			if strings.Contains(msg, secret) {
				t.Fatalf("Error() leaked %q: the specification forbids addresses and credentials in error strings\n %s", secret, msg)
			}
		}
		if !strings.Contains(msg, "kafka") || !strings.Contains(msg, "orders") {
			t.Fatalf("Error() dropped the context a caller needs: %s", msg)
		}
	})
	t.Run("ContextErrorsDiscoverable", func(t *testing.T) {
		wrapped := &mq.OpError{Operation: mq.OpFetch, Code: mq.CodeCanceled, Err: context.Canceled}
		if !errors.Is(wrapped, context.Canceled) {
			t.Fatal("context.Canceled is not discoverable through an *OpError")
		}
		if got := mq.CodeOf(context.Canceled); got != mq.CodeCanceled {
			t.Fatalf("CodeOf(context.Canceled) = %v, want CodeCanceled", got)
		}
		if got := mq.CodeOf(context.DeadlineExceeded); got != mq.CodeDeadlineExceeded {
			t.Fatalf("CodeOf(context.DeadlineExceeded) = %v, want CodeDeadlineExceeded", got)
		}
	})
	t.Run("RetryabilityIsNotInferred", func(t *testing.T) {
		// The specification: retryability is "operation-specific, not inferred solely
		// from native error type". A sentinel carries no such judgement, so
		// it is not retryable, and nor is a codeless error.
		if mq.IsRetryable(mq.ErrBackpressure) {
			t.Fatal("a bare sentinel reported itself retryable: the specification makes " +
				"retryability operation-specific and a sentinel carries no operation")
		}
		if !mq.IsRetryable(e) {
			t.Fatal("an *OpError marked retryable did not report itself retryable")
		}
		if mq.IsAmbiguous(e) {
			t.Fatal("an *OpError not marked ambiguous reported itself ambiguous")
		}
		// The claim here is that the FLAGS survive, and they do -- but they
		// are read where they live. These two checks used to read the
		// Retryable flag through mq.IsRetryable, which no longer reports it:
		// an ambiguous error is not safe to repeat however the driver marked
		// it , so the predicate's answer is asserted
		// separately and the flag is asserted on the *OpError.
		amb := &mq.OpError{Code: mq.CodeAmbiguous, Ambiguous: true, Retryable: true}
		var got *mq.OpError
		if !mq.IsAmbiguous(amb) || !errors.As(error(amb), &got) || !got.Retryable || !got.Ambiguous {
			t.Fatalf("an ambiguous *OpError lost its flags: %+v", got)
		}
		if mq.IsRetryable(amb) {
			t.Fatal("IsRetryable reported an ambiguous *OpError safe to repeat: the specification forbids repeating one")
		}
		// Both survive wrapping, which is how the runner sees them.
		joined := errors.Join(errors.New("outer"), amb)
		if !mq.IsAmbiguous(joined) || !errors.As(joined, &got) || !got.Retryable || !got.Ambiguous {
			t.Fatalf("the flags did not survive errors.Join: %+v", got)
		}
		if mq.IsRetryable(joined) {
			t.Fatal("IsRetryable reported a wrapped ambiguous error safe to repeat")
		}
	})
}

// TestIsRetryableLetsAmbiguityWin is the precedence between the two
// questions.
//
// The specification lists three outcomes and attaches "may be retryable" to the DEFINITELY
// REJECTED one alone; an ambiguous publication is a third outcome, and the
// core "MUST NOT auto-retry" one because retrying is what turns one message
// into two -- "a disqualifying correctness defect" in the specification's own words.
// IsRetryable therefore answers "safe to repeat", which an ambiguous outcome
// never is, whatever flag the driver set beside it.
//
// The driver's judgement is NOT discarded: the Retryable field is still
// whatever the driver set, and a caller whose broker deduplicates reads it and
// decides to retry deliberately -- which is exactly what the three
// exceptions require of such a caller.
func TestIsRetryableLetsAmbiguityWin(t *testing.T) {
	definite := &mq.OpError{Operation: mq.OpPublish, Code: mq.CodeUnavailable, Retryable: true}
	byFlag := &mq.OpError{Operation: mq.OpPublish, Code: mq.CodeUnavailable, Retryable: true, Ambiguous: true}
	byCode := &mq.OpError{Operation: mq.OpPublish, Code: mq.CodeAmbiguous, Retryable: true}
	bySettlement := &mq.OpError{Operation: mq.OpAck, Code: mq.CodeSettlementUnknown, Retryable: true}
	for _, tc := range []struct {
		name string
		err  error
		want bool
	}{
		{"definitely rejected and retryable", definite, true},
		{"ambiguous by flag", byFlag, false},
		{"ambiguous by code", byCode, false},
		{"settlement unknown by code", bySettlement, false},
		{"not retryable at all", &mq.OpError{Code: mq.CodeInvalidArgument}, false},
		// The precedence has to survive the shapes a runner actually sees.
		{"wrapped", errors.Join(errors.New("outer"), byFlag), false},
		{"wrapped, definite", errors.Join(errors.New("outer"), definite), true},
		// A batch is ambiguous when ANY entry is, so a batch holding one
		// ambiguous message is not safe to repeat even though its first
		// entry was definitely refused and retryable.
		{"a batch with one ambiguous entry", &mq.BatchError{Errs: []error{definite, nil, byFlag}}, false},
		{"a batch every entry of which was refused, retryable", &mq.BatchError{Errs: []error{definite, definite}}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := mq.IsRetryable(tc.err); got != tc.want {
				t.Fatalf("IsRetryable(%v) = %v, want %v (IsAmbiguous = %v)",
					tc.err, got, tc.want, mq.IsAmbiguous(tc.err))
			}
		})
	}
	t.Run("the driver's own flag is untouched", func(t *testing.T) {
		// The predicate's answer changed; the error's contents did not. A
		// caller who can de-duplicate still reads the judgement the driver
		// recorded, which is the only way the exceptions can be taken.
		var e *mq.OpError
		if !errors.As(error(byFlag), &e) || !e.Retryable || !e.Ambiguous {
			t.Fatalf("errors.As reached %+v, want the driver's Retryable and Ambiguous flags both set", e)
		}
	})
}

// TestIsRetryableAnswersForTheWholeBatch is IsRetryable of a *BatchError as
// the call's answer: safe to repeat only when every message was refused and
// each is safe to repeat. It used to read the first *OpError among the
// entries and nothing else, so a batch whose first message was accepted (a
// nil entry) and whose second was refused as Throttled read retryable, and a
// caller who repeated the call published the accepted message twice; and
// [throttled, invalid] read retryable while [invalid, throttled] did not.
func TestIsRetryableAnswersForTheWholeBatch(t *testing.T) {
	throttled := &mq.OpError{Driver: "adv", Operation: mq.OpPublish, Topic: "orders", Code: mq.CodeThrottled, Retryable: true}
	unavailable := &mq.OpError{Driver: "adv", Operation: mq.OpPublish, Topic: "orders", Code: mq.CodeUnavailable, Retryable: true}
	invalid := &mq.OpError{Driver: "adv", Operation: mq.OpPublish, Topic: "orders", Code: mq.CodeInvalidArgument}
	for _, tc := range []struct {
		name string
		errs []error
		want bool
	}{
		{"accepted, throttled", []error{nil, throttled}, false},
		{"throttled, accepted", []error{throttled, nil}, false},
		{"throttled, invalid", []error{throttled, invalid}, false},
		{"invalid, throttled", []error{invalid, throttled}, false},
		{"throttled, unavailable", []error{throttled, unavailable}, true},
		{"empty", nil, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			be := &mq.BatchError{Errs: tc.errs}
			if got := mq.IsRetryable(be); got != tc.want {
				t.Errorf("IsRetryable(%v) = %v, want %v", be, got, tc.want)
			}
			if got := mq.IsRetryable(fmt.Errorf("wrapped: %w", be)); got != tc.want {
				t.Errorf("IsRetryable of the wrapped batch %v = %v, want %v", be, got, tc.want)
			}
		})
	}
	// The same through PublishBatch, over both driver shapes: the generic
	// "if IsRetryable(err) { repeat the call }" must not publish the
	// accepted message a second time.
	fail := func(_ int, m *mq.Message) error {
		if string(m.Body) == "throttle" {
			return throttled
		}
		return nil
	}
	for name, open := range map[string]func(*advBroker, mq.SinkConfig) mq.Sink{"plain": openPlain, "batch": openBatch} {
		t.Run("PublishBatch/"+name, func(t *testing.T) {
			b, p := advClient(t, open, mq.PublisherConfig{})
			b.setFail(fail)
			err := p.PublishBatch(context.Background(), []*mq.Message{{Body: []byte("ok")}, {Body: []byte("throttle")}})
			var be *mq.BatchError
			if !errors.As(err, &be) {
				t.Fatalf("PublishBatch = %v, want a *BatchError", err)
			}
			if mq.IsRetryable(err) {
				t.Fatalf("IsRetryable(%v) = true for a batch whose first message the broker took (bodies %v): repeating the call publishes it twice",
					err, b.bodies())
			}
			if !mq.IsRetryable(be.Errs[1]) {
				t.Fatalf("IsRetryable(Errs[1] = %v) = false, want the refused message's own answer to stay retryable", be.Errs[1])
			}
		})
	}
}

// TestCodeOf covers the chain walk, including the joined form, so that a
// hand-written walk that forgot a case fails here.
func TestCodeOf(t *testing.T) {
	opErr := &mq.OpError{Code: mq.CodeThrottled}
	cases := []struct {
		name string
		err  error
		want mq.ErrorCode
	}{
		{"nil", nil, mq.CodeUnknown},
		{"plain", errors.New("x"), mq.CodeUnknown},
		{"sentinel", mq.ErrLeaseLost, mq.CodeLeaseLost},
		{"operror", opErr, mq.CodeThrottled},
		{"wrapped operror", errors.Join(errors.New("a"), opErr), mq.CodeThrottled},
		{"wrapped sentinel", errors.Join(errors.New("a"), mq.ErrBackpressure), mq.CodeBackpressure},
		{"joined none", errors.Join(errors.New("a"), errors.New("b")), mq.CodeUnknown},
		{"canceled", context.Canceled, mq.CodeCanceled},
	}
	for _, c := range cases {
		if got := mq.CodeOf(c.err); got != c.want {
			t.Errorf("%s: CodeOf = %v, want %v", c.name, got, c.want)
		}
	}
	if got := mq.ErrorCode(9999).String(); got != "Code(9999)" {
		t.Errorf("an out-of-range code renders as %q", got)
	}
	if mq.ErrorCode(9999).Valid() {
		t.Error("an out-of-range code reported valid")
	}
}

// errorFault is one failure a driver has to turn into an error. It is the
// fuzz target's input, decoded from arbitrary bytes.
type errorFault struct {
	op        string
	code      mq.ErrorCode
	native    error
	retryable bool
	ambiguous bool
	address   string
}

// faultOps and faultCodes are the corpora the fault decoder draws from.
var (
	faultOps = []string{
		mq.OpPublish, mq.OpFetch, mq.OpAck, mq.OpNack,
		mq.OpExtend, mq.OpOpen, mq.OpClose, mq.OpAdmin,
	}
	faultCodes = []mq.ErrorCode{
		mq.CodeUnknown, mq.CodeInvalidArgument, mq.CodeUnsupported, mq.CodeNotSettleable,
		mq.CodeClosed, mq.CodeCanceled, mq.CodeDeadlineExceeded, mq.CodeUnavailable,
		mq.CodeUnauthorized, mq.CodeNotFound, mq.CodeResourceExhausted, mq.CodeThrottled,
		mq.CodeMessageTooLarge, mq.CodeBackpressure, mq.CodeAlreadySettled,
		mq.CodeSettlementUnknown, mq.CodeLeaseLost, mq.CodeAmbiguous,
		mq.CodeCorruptMessage, mq.CodePoisonMessage,
	}
	faultNatives = []error{
		nil,
		errors.New("native: broker refused"),
		context.Canceled,
		context.DeadlineExceeded,
		errors.Join(errors.New("native a"), errors.New("native b")),
	}
	// faultSecrets are the strings no rendered error may contain. The specification
	// forbids payloads, credentials, tokens and bulk headers in errors.
	faultSecrets = []string{
		"user:s3cr3t@broker.internal:9092",
		"AKIAIOSFODNN7EXAMPLE",
		"broker.internal",
	}
)

func decodeFault(r *fuzzutil.Reader) errorFault {
	return errorFault{
		op:        faultOps[r.Choice(len(faultOps))],
		code:      faultCodes[r.Choice(len(faultCodes))],
		native:    faultNatives[r.Choice(len(faultNatives))],
		retryable: r.Bool(),
		ambiguous: r.Bool(),
		address:   faultSecrets[r.Choice(len(faultSecrets))],
	}
}

// FuzzErrorMapping is target 10, re-homed to the root from
// x/apibench/b10: stable codes, unwrapping, redaction and the
// retryability/ambiguity classification over arbitrary faults.
//
// The invariants after every fault: the code the error reports is the code it
// was built with; the native error stays reachable; the rendered message
// contains no address and no credential; errors.Is matches exactly the
// sentinels carrying the same code and no others; and a permanent code is
// never reported retryable.
func FuzzErrorMapping(f *testing.F) {
	seed := func(op, code, native int, retry, amb bool) []byte {
		w := fuzzutil.NewWriter()
		w.Choice(len(faultOps), op)
		w.Choice(len(faultCodes), code)
		w.Choice(len(faultNatives), native)
		w.Bool(retry)
		w.Bool(amb)
		w.Choice(len(faultSecrets), 0)
		return w.Data()
	}
	f.Add(seed(0, 4, 0, false, false)) // publish, Closed, no native
	f.Add(seed(0, 7, 1, true, false))  // publish, Unavailable, native, retryable
	f.Add(seed(1, 6, 3, true, true))   // fetch, DeadlineExceeded, deadline, ambiguous
	// The two codes whose meaning IS an unknown outcome, without the flag:
	// IsAmbiguous is true of them all the same.
	f.Add(seed(0, 17, 0, false, false)) // publish, Ambiguous, flag not set
	f.Add(seed(2, 15, 1, false, false)) // ack, SettlementUnknown, flag not set
	f.Add(seed(2, 16, 0, false, false))
	f.Add(seed(5, 8, 4, false, false)) // open, Unauthorized, joined native
	// Retryable AND ambiguous, by the flag and by the code alone: ambiguity
	// wins over the flag in both shapes.
	f.Add(seed(0, 7, 1, true, true))   // publish, Unavailable, retryable, ambiguous
	f.Add(seed(0, 17, 0, true, false)) // publish, Ambiguous by code, retryable
	f.Fuzz(func(t *testing.T, data []byte) {
		r := fuzzutil.NewReader(data)
		fault := decodeFault(r)
		e := &mq.OpError{
			Driver: "fuzz", Operation: fault.op, Topic: "t", Subscription: "s",
			Address: fault.address, Code: fault.code,
			Retryable: fault.retryable, Ambiguous: fault.ambiguous,
			Err: fault.native,
		}
		if got := mq.CodeOf(e); got != fault.code {
			t.Fatalf("CodeOf = %v, want the code the error was built with, %v", got, fault.code)
		}
		if fault.native != nil && !errors.Is(e, fault.native) {
			t.Fatalf("the native error %v is not reachable", fault.native)
		}
		msg := e.Error()
		for _, secret := range faultSecrets {
			if strings.Contains(msg, secret) {
				t.Fatalf("the rendered error leaked %q: %s", secret, msg)
			}
		}
		// Ambiguous when the flag says so, or when the code is one whose
		// meaning is an unknown outcome.
		wantAmbiguous := fault.ambiguous || fault.code == mq.CodeAmbiguous || fault.code == mq.CodeSettlementUnknown
		if mq.IsAmbiguous(e) != wantAmbiguous {
			t.Fatalf("IsAmbiguous = %v, want %v (flag %v, code %v)", mq.IsAmbiguous(e), wantAmbiguous, fault.ambiguous, fault.code)
		}
		// IsRetryable is "safe to repeat", so ambiguity wins over the flag
		// over the whole cross product of code and flags. This pinned `fault.retryable` alone, which was the
		// independent-flags reading: an ambiguous fault whose driver had
		// also marked it retryable was reported safe to repeat, and the specification
		// calls repeating one a disqualifying correctness defect.
		if want := fault.retryable && !wantAmbiguous; mq.IsRetryable(e) != want {
			t.Fatalf("IsRetryable = %v, want %v (flag %v, ambiguous %v)",
				mq.IsRetryable(e), want, fault.retryable, wantAmbiguous)
		}
		// Whatever the predicate answers, the driver's own judgement is
		// still on the error for a caller who can de-duplicate.
		var carried *mq.OpError
		if !errors.As(error(e), &carried) || carried.Retryable != fault.retryable {
			t.Fatalf("the Retryable flag the error was built with (%v) is no longer on it: %+v", fault.retryable, carried)
		}
		// errors.Is must accept exactly the sentinels of the same code. A
		// code that is not CodeUnknown matches its sentinels; CodeUnknown
		// matches none, because Unknown is not a wildcard.
		for _, s := range rootSentinels() {
			want := fault.code != mq.CodeUnknown && mq.CodeOf(s) == fault.code
			if got := errors.Is(e, s); got != want {
				t.Fatalf("errors.Is(code %v, sentinel %v with code %v) = %v, want %v",
					fault.code, s, mq.CodeOf(s), got, want)
			}
		}
		// Wrapping must not change any of it.
		wrapped := errors.Join(errors.New("outer"), e)
		if got := mq.CodeOf(wrapped); got != fault.code {
			t.Fatalf("CodeOf through a wrap = %v, want %v", got, fault.code)
		}
	})
}

// rootSentinels lists the sentinels, for the fuzz target to compare
// against all of them at once.
func rootSentinels() []error {
	return []error{
		mq.ErrClosed, mq.ErrUnsupported, mq.ErrNotSettleable, mq.ErrAlreadySettled,
		mq.ErrSettlementInProgress, mq.ErrLeaseLost, mq.ErrMessageTooLarge,
		mq.ErrBackpressure, mq.ErrQueueFull, mq.ErrPoisonMessage,
		mq.ErrTopicNotFound, mq.ErrInvalidTopic, mq.ErrDriverNotRegistered,
	}
}
