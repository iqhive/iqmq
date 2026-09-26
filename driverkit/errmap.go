package driverkit

import (
	"context"
	"errors"
	"io"
	"net"
	"strings"

	mq "github.com/iqhive/iqmq"
)

// Rule is one ordered mapping from a native error to a portable one (
// ).
type Rule struct {
	// Match reports whether the rule applies to the error. It is the
	// driver's own predicate: errors.Is against a native sentinel,
	// errors.As to a native type, a status-code comparison.
	Match func(error) bool
	// Code is the portable code the failure maps to.
	Code mq.ErrorCode
	// Retryable reports whether the operation may be retried. The specification makes
	// this operation-specific and forbids inferring it from a native error
	// type alone, which is why a rule set is per operation.
	//
	// A rule that is Retryable and Ambiguous TOGETHER is correct and normal,
	// and [mq.IsRetryable] answers false for it because "safe to repeat" is
	// what that predicate means. There is therefore no
	// need to clear Retryable in order to stop a retry: the precedence does
	// that once, for every caller.
	//
	// WHETHER to set it on an ambiguous rule stays the DRIVER's judgement,
	// and this kit does not take it away. A driver that can say the
	// underlying condition is transient should say so, because a caller who
	// can de-duplicate reads this field to take one of the three
	// exceptions. A driver whose broker makes the repeat unsafe on its own
	// terms leaves it false and documents that — drivers/nsq does, and
	// x/kafkatx and x/pulsartx build their errors the same way.
	//
	// Either way the flag is invisible through [mq.IsRetryable] once the
	// rule is ambiguous, so a test that means to pin it must read it off the
	// *mq.OpError. Reading it through the predicate is an assertion that
	// cannot fail.
	Retryable bool
	// Ambiguous reports that the operation may have been accepted and that
	// iqmq cannot tell. It wins over Retryable in every retry decision the
	// library makes or answers.
	Ambiguous bool
}

// ErrorMapper maps a driver's native errors to the portable model.
//
// Rules are ordered and the first match wins, so a driver writes the specific
// rules before the general ones. What the mapper never does is throw the native
// error away: the result wraps it, so errors.As still reaches a *kgo.Error or an
// *smithy.OperationError and a caller who needs the broker's own answer has it.
//
// Rules are per operation because retryability is. A connection reset on a
// publish that has already left the socket is ambiguous; the same reset on a
// fetch is a plain retry. The specification forbids deciding that from the native error type
// alone, and MapNet is the helper that applies the distinction.
type ErrorMapper struct {
	// Driver is the driver's name, which every *OpError carries.
	Driver string
	// Rules are the per-operation rule sets, keyed by the Op* constants of
	// the root package.
	Rules map[string][]Rule
	// Fallback is tried for every operation, after the operation's own
	// rules.
	Fallback []Rule
}

// Map turns a native error into an *OpError carrying the portable code, the
// retryability and the ambiguity the first matching rule names.
//
// A nil error maps to nil. An error that no rule matches is mapped by the
// standard classification — context cancellation, deadline, EOF, a net.Error —
// and otherwise to CodeUnknown, which is the honest answer: the specification says an error
// that carries no judgement is not retryable.
//
// An error that already says its outcome is unknown -- mq.IsAmbiguous of it is
// true -- stays ambiguous whatever rule matches it: a rule decides the code and
// the retryability, and it cannot know that a publication which may be in the
// broker definitely is not.
func (m *ErrorMapper) Map(op, topic, sub string, err error) error {
	if err == nil {
		return nil
	}
	if r, ok := m.match(op, err); ok {
		r.Ambiguous = r.Ambiguous || mq.IsAmbiguous(err)
		return m.opError(op, topic, sub, err, r)
	}
	code, retryable, ambiguous := ClassifyNet(err, false)
	return m.opError(op, topic, sub, err, Rule{Code: code, Retryable: retryable, Ambiguous: ambiguous})
}

// match returns the first rule that claims err, the operation's own rules
// before the fallback. It is shared by Map and MapNet so that the two cannot
// read one rule table two ways: MapNet used to skip Fallback entirely, so a
// driver that put its link rules there alone got one mapping for a publish
// error and another for the same error on a fetch.
func (m *ErrorMapper) match(op string, err error) (Rule, bool) {
	for _, r := range m.Rules[op] {
		if r.Match != nil && r.Match(err) {
			return r, true
		}
	}
	for _, r := range m.Fallback {
		if r.Match != nil && r.Match(err) {
			return r, true
		}
	}
	return Rule{}, false
}

// MapNet is [ErrorMapper.Map] for an error from a round trip whose request may
// already have left the socket.
//
// sentAlready is the distinction the specification asks a driver to make and that no native
// error type can: a timeout before the write is a clean failure the caller may
// retry, and the same timeout after it may have been accepted by the broker, so
// a retry can duplicate. A driver that reports the second as a plain failure
// makes a duplicate look like a loss, which the specification names a settlement
// disqualifier: the caller publishes again and the broker now holds two.
//
// The transport classification is therefore computed FIRST and a matching rule
// can refine it but not contradict it. A rule decides the portable code and the
// retryability, which are the two things a driver knows better than this
// package; what it cannot decide is that a publication which may already have
// reached the broker definitely did not. Consulting the rule table first is the
// defect the specification found: the entirely reasonable link rule every
// driver writes — {Match: isNetFailure, Code: CodeUnavailable, Retryable: true}
// — silently threw the sentAlready argument away and defeated the whole call.
//
// The ambiguity is ORed with ClassifyNet's rather than with sentAlready itself,
// and the difference matters for every existing rule table. A server REPLY that
// a rule matches means the broker processed the command and refused it, so the
// outcome is definite however the caller reached it; ClassifyNet does not
// classify it as a transport failure, so it stays definite. Only an error the
// transport classification recognises — a cancellation, a deadline, an EOF, a
// closed connection, a net.Error — becomes ambiguous, and only when the caller
// says the request may already have left the socket.
func (m *ErrorMapper) MapNet(op, topic, sub string, err error, sentAlready bool) error {
	if err == nil {
		return nil
	}
	code, retryable, ambiguous := ClassifyNet(err, sentAlready)
	if r, ok := m.match(op, err); ok {
		r.Ambiguous = r.Ambiguous || ambiguous
		return m.opError(op, topic, sub, err, r)
	}
	return m.opError(op, topic, sub, err, Rule{Code: code, Retryable: retryable, Ambiguous: ambiguous})
}

// opError builds the contextual error.
func (m *ErrorMapper) opError(op, topic, sub string, err error, r Rule) error {
	return &mq.OpError{
		Driver:       m.Driver,
		Operation:    op,
		Topic:        topic,
		Subscription: sub,
		Code:         r.Code,
		Retryable:    r.Retryable,
		Ambiguous:    r.Ambiguous,
		Err:          err,
	}
}

// ClassifyNet classifies a transport-level error.
//
// sentAlready says whether the request may already have reached the broker. It
// is what turns a post-send timeout, cancellation or EOF from a clean failure
// into an ambiguous one: the broker may have accepted the publication, so a
// retry may duplicate it and reporting the failure as definite is a lie the
// caller cannot detect.
//
// An error that ALREADY says its outcome is unknown is ambiguous whatever
// sentAlready is: one carrying CodeAmbiguous or CodeSettlementUnknown, or an
// *OpError a driver mapped once already with its Ambiguous flag set. The
// portable-code arm used to keep such an error's code and answer ambiguous
// false, so drivers/redis's short WAITAOF (errcode.Ambiguous) was mapped to an
// *OpError with Code=Ambiguous and Ambiguous=false, and a second mapping of an
// ambiguous *OpError hid it behind a definite one.
func ClassifyNet(err error, sentAlready bool) (code mq.ErrorCode, retryable, ambiguous bool) {
	code, retryable, ambiguous = classifyNet(err, sentAlready)
	return code, retryable, ambiguous || mq.IsAmbiguous(err)
}

// classifyNet is ClassifyNet before the error's own ambiguity is added.
func classifyNet(err error, sentAlready bool) (code mq.ErrorCode, retryable, ambiguous bool) {
	switch {
	case err == nil:
		return mq.CodeUnknown, false, false
	case errors.Is(err, context.Canceled):
		return mq.CodeCanceled, false, sentAlready
	case errors.Is(err, context.DeadlineExceeded):
		return mq.CodeDeadlineExceeded, true, sentAlready
	case errors.Is(err, io.EOF), errors.Is(err, io.ErrUnexpectedEOF):
		return mq.CodeUnavailable, true, sentAlready
	case errors.Is(err, net.ErrClosed):
		return mq.CodeUnavailable, true, sentAlready
	}
	var nerr net.Error
	if errors.As(err, &nerr) {
		if nerr.Timeout() {
			return mq.CodeDeadlineExceeded, true, sentAlready
		}
		return mq.CodeUnavailable, true, sentAlready
	}
	var dnsErr *net.DNSError
	if errors.As(err, &dnsErr) {
		return mq.CodeUnavailable, true, false
	}
	if code := mq.CodeOf(err); code != mq.CodeUnknown {
		return code, false, false
	}
	return mq.CodeUnknown, false, false
}

// MatchIs returns a [Rule.Match] that matches by errors.Is, which is the common
// case: a native client with sentinel errors.
func MatchIs(targets ...error) func(error) bool {
	return func(err error) bool {
		for _, t := range targets {
			if errors.Is(err, t) {
				return true
			}
		}
		return false
	}
}

// MatchContains returns a [Rule.Match] that matches a substring of the error's
// message.
//
// It is the last resort and it is here because some clients have no other
// surface: a broker that reports "TOPIC_AUTHORIZATION_FAILED" in a string and
// nothing else leaves a driver nothing to match on. Prefer [MatchIs] and a typed
// predicate; a driver page says which of its mappings rest on a message.
func MatchContains(substrings ...string) func(error) bool {
	return func(err error) bool {
		if err == nil {
			return false
		}
		msg := err.Error()
		for _, s := range substrings {
			if s != "" && strings.Contains(msg, s) {
				return true
			}
		}
		return false
	}
}
