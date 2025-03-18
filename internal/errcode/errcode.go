// Package errcode defines the portable error codes and sentinel errors shared
// by every layer of iqmq: the engine, the drivers and the public API.
//
// The package depends on the standard library only. The root package
// re-exports the sentinel values defined here (for example
// mq.ErrClosed == errcode.ErrClosed), so an error produced deep inside the
// memory engine or a driver satisfies errors.Is against the public sentinels
// without any translation on the hot path.
//
// Matching rule: two errors match under errors.Is when they carry the same
// Code. This is the contract the specification calls "errors.Is matches by
// Code": an *OpError with Code Closed matches ErrClosed even though it is a
// different value.
package errcode

import (
	"context"
	"errors"
	"strconv"
)

// Code identifies the portable category of a failure. Codes are stable across
// releases: new codes are appended, existing codes are never renumbered.
type Code uint16

// The portable error codes, in specification order.
const (
	// Unknown is the zero Code: the failure could not be classified.
	Unknown Code = iota
	// InvalidArgument reports a caller-supplied value that fails validation.
	InvalidArgument
	// InvalidConfig reports configuration rejected at construction time.
	InvalidConfig
	// Unsupported reports a feature the driver or profile cannot provide.
	Unsupported
	// NotSettleable reports a settlement attempt on a delivery that has no
	// settlement semantics (for example a NATS Core delivery).
	NotSettleable
	// Closed reports an operation on a closed handle.
	Closed
	// Canceled corresponds to context.Canceled.
	Canceled
	// DeadlineExceeded corresponds to context.DeadlineExceeded.
	DeadlineExceeded
	// Unavailable reports a broker that could not be reached.
	Unavailable
	// Unauthorized reports missing or invalid credentials.
	Unauthorized
	// Forbidden reports valid credentials that lack permission.
	Forbidden
	// NotFound reports a missing topic, subscription or record.
	NotFound
	// Conflict reports a state conflict such as an existing resource.
	Conflict
	// ResourceExhausted reports a broker- or engine-side capacity limit.
	ResourceExhausted
	// Throttled reports rate limiting by the broker.
	Throttled
	// MessageTooLarge reports a payload exceeding the advertised limit.
	MessageTooLarge
	// Backpressure reports a client-side buffer that is full under
	// BackpressureReject.
	Backpressure
	// AlreadySettled reports a second terminal settlement of one delivery.
	AlreadySettled
	// SettlementInProgress reports a concurrent settlement attempt that lost
	// the race while another attempt is still in flight.
	SettlementInProgress
	// SettlementUnknown reports an ambiguous native settlement result.
	SettlementUnknown
	// LeaseLost reports a settlement with a stale delivery generation.
	LeaseLost
	// Partial reports a batch in which some items failed.
	Partial
	// Ambiguous reports a publish whose acceptance is unknown.
	Ambiguous
	// CorruptMessage reports an undecodable envelope or record.
	CorruptMessage
	// PoisonMessage reports a message that exhausted its attempts without a
	// usable dead-letter destination.
	PoisonMessage
	// DriverNotRegistered reports a DSN scheme with no registered driver.
	DriverNotRegistered

	codeCount
)

var codeNames = [...]string{
	Unknown:              "Unknown",
	InvalidArgument:      "InvalidArgument",
	InvalidConfig:        "InvalidConfig",
	Unsupported:          "Unsupported",
	NotSettleable:        "NotSettleable",
	Closed:               "Closed",
	Canceled:             "Canceled",
	DeadlineExceeded:     "DeadlineExceeded",
	Unavailable:          "Unavailable",
	Unauthorized:         "Unauthorized",
	Forbidden:            "Forbidden",
	NotFound:             "NotFound",
	Conflict:             "Conflict",
	ResourceExhausted:    "ResourceExhausted",
	Throttled:            "Throttled",
	MessageTooLarge:      "MessageTooLarge",
	Backpressure:         "Backpressure",
	AlreadySettled:       "AlreadySettled",
	SettlementInProgress: "SettlementInProgress",
	SettlementUnknown:    "SettlementUnknown",
	LeaseLost:            "LeaseLost",
	Partial:              "Partial",
	Ambiguous:            "Ambiguous",
	CorruptMessage:       "CorruptMessage",
	PoisonMessage:        "PoisonMessage",
	DriverNotRegistered:  "DriverNotRegistered",
}

// Count is the number of defined codes. It is useful for table-driven tests
// and for sizing per-code counters.
const Count = int(codeCount)

// String returns the specification name of the code, or "Code(n)" for a value
// outside the defined range.
func (c Code) String() string {
	if c < codeCount {
		return codeNames[c]
	}
	return "Code(" + strconv.FormatUint(uint64(c), 10) + ")"
}

// Valid reports whether c is one of the defined codes.
func (c Code) Valid() bool { return c < codeCount }

// Coder is implemented by errors that carry a portable Code. The root
// package's *OpError implements it, as does *Error below.
type Coder interface {
	error
	Code() Code
}

// Error is a sentinel error carrying a Code and a fixed message. Sentinels are
// allocated once at package initialisation and are safe to return from hot
// paths without allocating.
//
// Error values match under errors.Is whenever the target implements Coder
// with the same Code.
type Error struct {
	code Code
	msg  string
}

// New returns a new sentinel with the given code and message. It is intended
// for package-level variables in the engine and driver kits, not for
// per-call construction; use the root package's OpError for contextual errors.
func New(code Code, msg string) *Error {
	return &Error{code: code, msg: msg}
}

// Error implements error.
func (e *Error) Error() string { return e.msg }

// Code returns the portable code of the sentinel.
func (e *Error) Code() Code { return e.code }

// Is reports whether target carries the same Code. It is invoked by errors.Is.
func (e *Error) Is(target error) bool {
	c, ok := target.(Coder)
	return ok && c.Code() == e.code
}

// Sentinel errors. Each carries the Code named in its documentation. They are
// re-exported by the root package under the same names.
var (
	// ErrClosed (Closed) is returned by any operation on a closed handle.
	ErrClosed = New(Closed, "iqmq: closed")
	// ErrUnsupported (Unsupported) is returned when a backend, profile or
	// option cannot provide the requested behaviour.
	ErrUnsupported = New(Unsupported, "iqmq: unsupported")
	// ErrNotSettleable (NotSettleable) is returned by Ack, Nack and Extend on
	// deliveries from a backend with no settlement semantics.
	ErrNotSettleable = New(NotSettleable, "iqmq: delivery is not settleable")
	// ErrAlreadySettled (AlreadySettled) is returned by a second terminal
	// settlement of a delivery that was already settled successfully.
	ErrAlreadySettled = New(AlreadySettled, "iqmq: delivery already settled")
	// ErrSettlementInProgress (SettlementInProgress) is returned to the loser
	// of a concurrent settlement race while the winner is still in flight.
	ErrSettlementInProgress = New(SettlementInProgress, "iqmq: settlement in progress")
	// ErrSettlementUnknown (SettlementUnknown) is returned when the native
	// settlement result was ambiguous and the delivery stays in the unknown
	// state.
	ErrSettlementUnknown = New(SettlementUnknown, "iqmq: settlement outcome unknown")
	// ErrLeaseLost (LeaseLost) is returned when a settlement carries a stale
	// delivery generation: the lease expired, a rebalance revoked it, or the
	// delivery was redelivered.
	ErrLeaseLost = New(LeaseLost, "iqmq: lease lost")
	// ErrMessageTooLarge (MessageTooLarge) is returned before any I/O when a
	// payload exceeds the advertised maximum.
	ErrMessageTooLarge = New(MessageTooLarge, "iqmq: message too large")
	// ErrBackpressure (Backpressure) is returned under BackpressureReject when
	// a client-side buffer is full.
	ErrBackpressure = New(Backpressure, "iqmq: backpressure")
	// ErrQueueFull (ResourceExhausted) is returned by the memory engine under
	// OverflowReject when a lane is at capacity.
	ErrQueueFull = New(ResourceExhausted, "iqmq: queue full")
	// ErrPartial (Partial) is returned when an operation succeeded for some
	// of its targets and failed for the others: a batch in which some items
	// failed, or a fan-out publication under the Independent policy that
	// reached some subscriptions and was refused by the rest. It is not the
	// same failure as ErrQueueFull, which means nothing was accepted, and
	// the difference decides who owns an owned payload afterwards.
	ErrPartial = New(Partial, "iqmq: partially completed")
	// ErrPoisonMessage (PoisonMessage) is returned by the runner when a
	// message exhausts its attempts without a usable dead-letter destination.
	ErrPoisonMessage = New(PoisonMessage, "iqmq: poison message")
	// ErrTopicNotFound (NotFound) is returned when a topic does not exist and
	// automatic creation is disabled.
	ErrTopicNotFound = New(NotFound, "iqmq: topic not found")
	// ErrInvalidTopic (InvalidArgument) is returned when a topic name violates
	// the portable grammar or the driver's stricter rules.
	ErrInvalidTopic = New(InvalidArgument, "iqmq: invalid topic name")
	// ErrDriverNotRegistered (DriverNotRegistered) is returned by a DSN
	// registry when no driver claims the scheme.
	ErrDriverNotRegistered = New(DriverNotRegistered, "iqmq: driver not registered")
	// ErrCanceled (Canceled) is the portable sentinel for context.Canceled.
	// CodeOf maps context.Canceled to Canceled, so callers normally compare
	// against context.Canceled directly.
	ErrCanceled = New(Canceled, "iqmq: canceled")
	// ErrDeadlineExceeded (DeadlineExceeded) is the portable sentinel for
	// context.DeadlineExceeded.
	ErrDeadlineExceeded = New(DeadlineExceeded, "iqmq: deadline exceeded")
)

// CodeOf returns the Code carried by err or by the first error in its Unwrap
// chain that implements Coder. context.Canceled and context.DeadlineExceeded
// map to Canceled and DeadlineExceeded. A nil error and any error without a
// code yield Unknown.
//
// CodeOf walks the chain manually rather than through errors.As so that it
// performs no allocation.
func CodeOf(err error) Code {
	for err != nil {
		if c, ok := err.(Coder); ok { //nolint:errorlint // the chain is walked manually below to avoid allocating
			return c.Code()
		}
		switch err { //nolint:errorlint // each link of the chain is compared directly; wrapping is handled by the loop
		case context.Canceled:
			return Canceled
		case context.DeadlineExceeded:
			return DeadlineExceeded
		}
		switch u := err.(type) { //nolint:errorlint // this is the unwrap step itself
		case interface{ Unwrap() error }:
			err = u.Unwrap()
		case interface{ Unwrap() []error }:
			for _, e := range u.Unwrap() {
				if c := CodeOf(e); c != Unknown {
					return c
				}
			}
			return Unknown
		default:
			return Unknown
		}
	}
	return Unknown
}

// ErrNotAttempted is the cause of every not-attempted answer: a message of a
// batch that was never offered to the broker because an earlier message's
// failure stopped the batch. It carries no code of its own, so the answer's
// code is the one NotAttempted chose.
var ErrNotAttempted = errors.New("iqmq: not attempted: an earlier message of the batch stopped it before this one was sent")

// NotAttempted is the code for a message that never reached the broker
// because a failure with code c stopped its batch.
//
// It is c itself, so that errors.Is against the stopping failure's sentinel --
// ErrClosed, context.Canceled, context.DeadlineExceeded -- still holds for the
// messages behind it, EXCEPT when c's meaning is an unknown outcome
// (Ambiguous, SettlementUnknown). A message that never left the process has a
// definite outcome, and a not-attempted answer that copied such a code would
// tell a caller it "may be in the broker"; drivers/activemq/stomp's did, for
// its receipt timeout. Then the code is Unavailable: the batch could not go on,
// and the message may be sent again.
//
// It is here rather than in the root package or in driverkit because both the
// core's fill and a driver's own loop answer the same question, and neither
// can import the other.
func NotAttempted(c Code) Code {
	if c == Ambiguous || c == SettlementUnknown {
		return Unavailable
	}
	return c
}
