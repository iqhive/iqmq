package iqmq

import (
	"context"
	"strings"

	"bitbucket.org/iqhive/iqmq/internal/errcode"
)

// ErrorCode identifies the portable category of a failure (the API contract
// The specification). Codes are stable across releases: new codes are appended, existing
// codes are never renumbered.
//
// A code is how errors are compared. The specification decided the error shape
// (ADR-011, winner PreallocCodes): hot-path errors are built once and
// errors.Is matches by code, so an error raised deep inside a driver or the
// memory engine satisfies errors.Is against a sentinel of the same code
// without any translation and without any allocation.
type ErrorCode uint16

// The portable error codes, in specification order. The constants
// carry the Code prefix the specification uses for them ("halt with CodePoisonMessage")
// because the support levels claim the unprefixed names.
const (
	// CodeUnknown is the zero code: the failure could not be classified.
	CodeUnknown = ErrorCode(errcode.Unknown)
	// CodeInvalidArgument reports a caller-supplied value that fails validation.
	CodeInvalidArgument = ErrorCode(errcode.InvalidArgument)
	// CodeInvalidConfig reports configuration rejected at construction time.
	CodeInvalidConfig = ErrorCode(errcode.InvalidConfig)
	// CodeUnsupported reports a feature the driver or profile cannot provide.
	CodeUnsupported = ErrorCode(errcode.Unsupported)
	// CodeNotSettleable reports a settlement attempt on a delivery with no
	// settlement semantics.
	CodeNotSettleable = ErrorCode(errcode.NotSettleable)
	// CodeClosed reports an operation on a closed handle.
	CodeClosed = ErrorCode(errcode.Closed)
	// CodeCanceled corresponds to context.Canceled.
	CodeCanceled = ErrorCode(errcode.Canceled)
	// CodeDeadlineExceeded corresponds to context.DeadlineExceeded.
	CodeDeadlineExceeded = ErrorCode(errcode.DeadlineExceeded)
	// CodeUnavailable reports a broker that could not be reached.
	CodeUnavailable = ErrorCode(errcode.Unavailable)
	// CodeUnauthorized reports missing or invalid credentials.
	CodeUnauthorized = ErrorCode(errcode.Unauthorized)
	// CodeForbidden reports valid credentials that lack permission.
	CodeForbidden = ErrorCode(errcode.Forbidden)
	// CodeNotFound reports a missing topic, subscription or record.
	CodeNotFound = ErrorCode(errcode.NotFound)
	// CodeConflict reports a state conflict such as an existing resource.
	CodeConflict = ErrorCode(errcode.Conflict)
	// CodeResourceExhausted reports a broker- or engine-side capacity limit.
	CodeResourceExhausted = ErrorCode(errcode.ResourceExhausted)
	// CodeThrottled reports rate limiting by the broker.
	CodeThrottled = ErrorCode(errcode.Throttled)
	// CodeMessageTooLarge reports a payload exceeding the advertised limit.
	CodeMessageTooLarge = ErrorCode(errcode.MessageTooLarge)
	// CodeBackpressure reports a client-side buffer that is full under
	// BackpressureReject.
	CodeBackpressure = ErrorCode(errcode.Backpressure)
	// CodeAlreadySettled reports a second terminal settlement of one delivery.
	CodeAlreadySettled = ErrorCode(errcode.AlreadySettled)
	// CodeSettlementInProgress reports a concurrent settlement attempt that
	// lost the race while another attempt is still in flight.
	CodeSettlementInProgress = ErrorCode(errcode.SettlementInProgress)
	// CodeSettlementUnknown reports an ambiguous native settlement result.
	CodeSettlementUnknown = ErrorCode(errcode.SettlementUnknown)
	// CodeLeaseLost reports a settlement with a stale delivery generation.
	CodeLeaseLost = ErrorCode(errcode.LeaseLost)
	// CodePartial reports a batch in which some items failed.
	CodePartial = ErrorCode(errcode.Partial)
	// CodeAmbiguous reports a publish whose acceptance is unknown.
	CodeAmbiguous = ErrorCode(errcode.Ambiguous)
	// CodeCorruptMessage reports an undecodable envelope or record.
	CodeCorruptMessage = ErrorCode(errcode.CorruptMessage)
	// CodePoisonMessage reports a message that exhausted its attempts without
	// a usable dead-letter destination.
	CodePoisonMessage = ErrorCode(errcode.PoisonMessage)
	// CodeDriverNotRegistered reports a DSN scheme with no registered driver.
	CodeDriverNotRegistered = ErrorCode(errcode.DriverNotRegistered)
)

// String returns the specification name of the code, or "Code(n)" for a value
// outside the defined range.
func (c ErrorCode) String() string { return errcode.Code(c).String() }

// Valid reports whether c is one of the defined codes.
func (c ErrorCode) Valid() bool { return errcode.Code(c).Valid() }

// The operations an *OpError may name. The specification fixes the set:
// "publish|fetch|ack|nack|extend|open|close|admin".
const (
	// OpPublish is a publication.
	OpPublish = "publish"
	// OpFetch is a receive.
	OpFetch = "fetch"
	// OpAck is a terminal positive settlement.
	OpAck = "ack"
	// OpNack is a negative settlement.
	OpNack = "nack"
	// OpExtend is a lease extension.
	OpExtend = "extend"
	// OpOpen is construction.
	OpOpen = "open"
	// OpClose is teardown.
	OpClose = "close"
	// OpAdmin is a topology operation.
	OpAdmin = "admin"
)

// Sentinel errors. Each carries the code named in its documentation, and
// errors.Is matches by code: a driver's *OpError with code CodeClosed
// satisfies errors.Is against ErrClosed even though it is a different value,
// and two distinct sentinels carrying one code are interchangeable under
// errors.Is. Use CodeOf when the distinction between two errors sharing a
// code matters.
//
// The values are the ones internal/errcode defines, so an error produced
// inside the memory engine or a driver kit is the same value the public
// sentinel names. The list is exactly the one declares; a failure with
// no sentinel of its own is matched by its code.
var (
	// ErrClosed (CodeClosed) is returned by any operation on a closed handle.
	ErrClosed error = errcode.ErrClosed
	// ErrUnsupported (CodeUnsupported) is returned when a backend, profile or
	// option cannot provide the requested behaviour.
	ErrUnsupported error = errcode.ErrUnsupported
	// ErrNotSettleable (CodeNotSettleable) is returned by Ack, Nack and Extend
	// on deliveries from a backend with no settlement semantics.
	ErrNotSettleable error = errcode.ErrNotSettleable
	// ErrAlreadySettled (CodeAlreadySettled) is returned by a second terminal
	// settlement of a delivery that was already settled.
	ErrAlreadySettled error = errcode.ErrAlreadySettled
	// ErrSettlementInProgress (CodeSettlementInProgress) is returned to the
	// loser of a concurrent settlement race while the winner is in flight.
	ErrSettlementInProgress error = errcode.ErrSettlementInProgress
	// ErrLeaseLost (CodeLeaseLost) is returned when a settlement carries a
	// stale delivery generation: the lease expired, a rebalance revoked it, or
	// the delivery was redelivered.
	ErrLeaseLost error = errcode.ErrLeaseLost
	// ErrSettlementUnknown (CodeSettlementUnknown) is returned when a native
	// settlement result was ambiguous and the delivery's outcome stays
	// unknown. The sentinel list does not name it and exports it:
	// the settlement surface has to be able to say so, and "sticky unknown"
	// is a rule a caller branches on rather than a code it looks up.
	ErrSettlementUnknown error = errcode.ErrSettlementUnknown
	// ErrPartial (CodePartial) is returned when an operation succeeded for
	// some of its targets and failed for the others: a batch in which some
	// messages were accepted, or a fan-out publication that reached some
	// subscriptions. It is matched by every *BatchError, which is how a
	// caller asks "was this partial?" without knowing the concrete type
	//  -- including the *BatchError of a batch none of
	// which was accepted but whose items' outcomes differ, some refused and
	// some ambiguous: what is partial there is what is KNOWN, and the answer
	// is per item for the same reason.
	ErrPartial error = errcode.ErrPartial
	// ErrMessageTooLarge (CodeMessageTooLarge) is returned before any I/O when
	// a payload exceeds the advertised maximum.
	ErrMessageTooLarge error = errcode.ErrMessageTooLarge
	// ErrBackpressure (CodeBackpressure) is returned under BackpressureReject
	// when a client-side buffer is full.
	ErrBackpressure error = errcode.ErrBackpressure
	// ErrQueueFull (CodeResourceExhausted) is returned by the memory engine
	// under OverflowReject when a lane is at capacity.
	ErrQueueFull error = errcode.ErrQueueFull
	// ErrPoisonMessage (CodePoisonMessage) is returned by the runner when a
	// message exhausts its attempts without a usable dead-letter destination.
	ErrPoisonMessage error = errcode.ErrPoisonMessage
	// ErrTopicNotFound (CodeNotFound) is returned when a topic does not exist
	// and automatic creation is disabled.
	ErrTopicNotFound error = errcode.ErrTopicNotFound
	// ErrInvalidTopic (CodeInvalidArgument) is returned when a topic name
	// violates the portable grammar or the driver's stricter rules.
	ErrInvalidTopic error = errcode.ErrInvalidTopic
	// ErrDriverNotRegistered (CodeDriverNotRegistered) is returned by a DSN
	// registry when no driver claims the scheme.
	ErrDriverNotRegistered error = errcode.ErrDriverNotRegistered
)

// OpError is the contextual error what failed, where, and how a
// caller should react to it.
//
// The zero value is not a useful error; a driver fills the fields it knows.
// the winner preallocates the values a handle can answer without
// per-call content — an operation on a closed handle, a full client-side
// buffer — at construction, so the failure path formats nothing and allocates
// nothing. Everything a shared value cannot carry (a native cause, a topic the
// handle was not built with) is built per call.
type OpError struct {
	// Driver is the driver name.
	Driver string
	// Operation is the failed operation, one of the Op* constants.
	Operation string
	// Topic is the topic the operation named.
	Topic string
	// Subscription is the subscription, empty for a publisher.
	Subscription string
	// Address is the broker address. It is never rendered by Error: the specification
	// forbids credentials and addresses in error strings.
	Address string
	// Code is the portable code.
	Code ErrorCode
	// Retryable reports whether the operation may be retried.:
	// "operation-specific, not inferred solely from native error type".
	//
	// This is the DRIVER's judgement, and it is not what [IsRetryable]
	// answers when the error is also ambiguous: that predicate means "safe
	// to repeat", and ambiguity wins over this flag.
	// Read the field — errors.As to *OpError — when you can de-duplicate and
	// are taking one of the three exceptions deliberately; ask IsRetryable
	// when you want the safe answer. A test that means to pin a driver's
	// judgement about an AMBIGUOUS failure must read it here: through
	// IsRetryable it is an assertion that cannot fail.
	Retryable bool
	// Ambiguous reports that the operation may have been accepted and that
	// iqmq cannot tell. It wins over Retryable in [IsRetryable].
	Ambiguous bool
	// Err is the native error, preserved for errors.As and returned by
	// Unwrap.
	Err error
}

// Error renders the error without the address, the payload or any header.
//
// It concatenates rather than formatting: the specification forbids fmt on a hot path,
// and the whole point of a preallocated error is that its message is built
// once.
func (e *OpError) Error() string {
	var b strings.Builder
	b.Grow(len(e.Driver) + len(e.Operation) + len(e.Topic) + len(e.Subscription) + 32)
	if e.Driver != "" {
		b.WriteString(e.Driver)
		b.WriteString(": ")
	}
	if e.Operation != "" {
		b.WriteString(e.Operation)
	}
	if e.Topic != "" {
		b.WriteString(" topic=")
		b.WriteString(e.Topic)
	}
	if e.Subscription != "" {
		b.WriteString(" subscription=")
		b.WriteString(e.Subscription)
	}
	b.WriteString(": ")
	b.WriteString(e.Code.String())
	if e.Err != nil {
		b.WriteString(": ")
		b.WriteString(e.Err.Error())
	}
	return b.String()
}

// Is reports whether target carries the same portable code, which is the specification's
// matching rule and the winning discipline. It is invoked by errors.Is
// and allocates nothing.
func (e *OpError) Is(target error) bool {
	if target == nil {
		return false
	}
	code := CodeOf(target)
	return code != CodeUnknown && code == e.Code
}

// Unwrap returns the native error, so errors.As reaches a driver's own error
// type and context.Canceled stays discoverable.
func (e *OpError) Unwrap() error { return e.Err }

// CodeOf returns the code carried by err or by the first error in its unwrap
// chain that carries one. context.Canceled and context.DeadlineExceeded map to
// CodeCanceled and CodeDeadlineExceeded. A nil error and any error without a
// code yield CodeUnknown.
//
// CodeOf walks the chain by hand rather than through errors.As so that it
// performs no allocation (rule 1).
func CodeOf(err error) ErrorCode {
	for err != nil {
		switch e := err.(type) { //nolint:errorlint // the chain is walked by hand below to avoid allocating
		case *OpError:
			return e.Code
		case *ControlError:
			// Its Code reads the wrapped error, so this is one step down the
			// chain and not a loop back into CodeOf's argument.
			return e.Code()
		case *BatchError:
			// A batch that was not uniformly accepted is CodePartial,
			// whatever its entries carry. Falling through to the multi-error
			// unwrap below would answer with the first failing item's code
			// and lose the fact that it was a batch.
			return e.Code()
		case errcode.Coder:
			// The sentinels of internal/errcode, and any error a driver kit
			// builds with them. A root type never reaches here: neither
			// *OpError nor *ControlError can carry errcode's own Code type in
			// its public method set.
			return ErrorCode(e.Code())
		}
		switch err { //nolint:errorlint // each link is compared directly; wrapping is handled by the loop
		case context.Canceled:
			return CodeCanceled
		case context.DeadlineExceeded:
			return CodeDeadlineExceeded
		}
		switch u := err.(type) { //nolint:errorlint // this is the unwrap step itself
		case interface{ Unwrap() error }:
			err = u.Unwrap()
		case interface{ Unwrap() []error }:
			for _, one := range u.Unwrap() {
				if c := CodeOf(one); c != CodeUnknown {
					return c
				}
			}
			return CodeUnknown
		default:
			return CodeUnknown
		}
	}
	return CodeUnknown
}

// IsRetryable reports whether err came from an operation that may be retried.
// Only an *OpError says so: the specification makes retryability operation-specific and
// forbids inferring it from a native error type alone, so an error that
// carries no such judgement is not retryable.
//
// AMBIGUITY WINS: an error that is both Retryable and Ambiguous is NOT
// retryable here (which supersedes). The specification lists
// three outcomes and attaches "may be retryable" to the definitely-rejected
// one alone; an ambiguous publication is a third outcome, and retrying one is
// how one message becomes two — "a disqualifying correctness defect" in the specification's own words. So IsRetryable answers "safe to repeat", and an unknown
// outcome never is. An error CAN carry both flags — driverkit.ErrorMapper's
// MapNet reports a connection that died after a publication left the socket
// as Retryable AND Ambiguous — and this predicate is where the precedence
// between them is applied, once, for every caller.
//
// The driver's own judgement is not discarded: the Retryable FIELD is
// untouched, so a caller who can de-duplicate — native idempotent
// publication, a DedupKey inside the broker's dedup window, or an application
// that has explicitly enabled ambiguous retry, which are the three
// exceptions — reads it and decides deliberately:
//
//	var e *mq.OpError
//	if errors.As(err, &e) && e.Retryable { /* the driver's own judgement */ }
//
// A *BatchError is ambiguous when any of its entries is, so a batch holding
// one ambiguous message is not retryable even when the first *OpError in it
// was definitely refused. That is the point: republishing such a batch
// duplicates the message nobody knows the fate of.
func IsRetryable(err error) bool {
	if e := opErrorOf(err); e != nil {
		return e.Retryable && !IsAmbiguous(err)
	}
	return false
}

// IsSubscriptionNotFound reports whether err says the SUBSCRIPTION the
// operation named does not exist, as distinct from its topic.
//
// There is no ErrSubscriptionNotFound sentinel and this predicate is the
// decision that there will not be one. The specification matches a sentinel by CODE, and
// a missing subscription and a missing topic are both CodeNotFound: a second
// sentinel over the same code would make `errors.Is(err, ErrTopicNotFound)`
// true for a missing subscription AND `errors.Is(err, ErrSubscriptionNotFound)`
// true for a missing topic, which is worse than having neither. A code of its
// own would mean a value the enumerated list does not have and that every
// driver's ErrorMapper would have to learn.
//
// What the drivers already carry is the distinction itself: an *OpError names
// the Topic and the Subscription of the operation that failed, and a
// consumer-side operation that could not find its subscription names both.
// So the two predicates below read what is there. A caller that wants the
// older behaviour keeps it: errors.Is(err, ErrTopicNotFound) is true of every
// CodeNotFound, which is what the code matching means.
func IsSubscriptionNotFound(err error) bool {
	e := opErrorOf(err)
	return e != nil && e.Code == CodeNotFound && e.Subscription != ""
}

// IsTopicNotFound reports whether err says the TOPIC does not exist, and not
// merely that something the operation named was missing. It is the complement
// of [IsSubscriptionNotFound]: a publisher's NotFound names no subscription,
// so it is the topic that is gone.
func IsTopicNotFound(err error) bool {
	e := opErrorOf(err)
	return e != nil && e.Code == CodeNotFound && e.Subscription == ""
}

// IsAmbiguous reports whether err came from an operation that may have been
// accepted: the ambiguous outcome, which the specification defines as
// IsAmbiguous(err) == true.
//
// An error says so in either of two ways, and both count: an *OpError's
// Ambiguous flag, or one of the two codes whose meaning IS an unknown
// outcome -- CodeAmbiguous, "a publish whose acceptance is unknown", and
// CodeSettlementUnknown, "an ambiguous native settlement result". An *OpError
// whose code is one of them is ambiguous whatever its flag says, and so is
// any other error carrying one (ErrSettlementUnknown among them). A driver
// that built the code without the flag -- drivers/redis's short WAITAOF did,
// through driverkit's mapper -- used to be answered "not ambiguous", while
// x/outbox, the runner's settlement path and drivers/sqs each read the code
// themselves; this is now the one predicate they all agree with.
//
// Along one unwrap chain the nearest *OpError decides. An error with several
// branches -- a *BatchError, an errors.Join -- is ambiguous when ANY branch
// is: its branches are independent outcomes, and the specification makes reporting an
// ambiguous one as definite a disqualifying defect. A batch whose first
// failed item was refused and whose last may have been accepted is
// ambiguous, and a caller who read it as a refusal and republished would
// duplicate the last item.
//
// It walks the tree by hand, as CodeOf does, and allocates nothing.
func IsAmbiguous(err error) bool {
	for err != nil {
		switch e := err.(type) { //nolint:errorlint // the chain is walked by hand to avoid allocating
		case *OpError:
			return e.Ambiguous || ambiguousCode(e.Code)
		case errcode.Coder:
			if ambiguousCode(ErrorCode(e.Code())) {
				return true
			}
		}
		switch u := err.(type) { //nolint:errorlint // this is the unwrap step itself
		case interface{ Unwrap() error }:
			err = u.Unwrap()
		case interface{ Unwrap() []error }:
			for _, one := range u.Unwrap() {
				if IsAmbiguous(one) {
					return true
				}
			}
			return false
		default:
			return false
		}
	}
	return false
}

// ambiguousCode reports whether c is one of the two codes whose meaning is an
// unknown outcome.
func ambiguousCode(c ErrorCode) bool {
	return c == CodeAmbiguous || c == CodeSettlementUnknown
}

// opErrorOf returns the first *OpError in err's unwrap chain, walked by hand
// so that IsRetryable and the two NotFound predicates allocate nothing.
// IsAmbiguous has a walk of its own, because it reads every branch of a
// multi-error rather than the first one that holds an *OpError.
func opErrorOf(err error) *OpError {
	for err != nil {
		if e, ok := err.(*OpError); ok { //nolint:errorlint // the chain is walked by hand to avoid allocating
			return e
		}
		switch u := err.(type) { //nolint:errorlint // this is the unwrap step itself
		case interface{ Unwrap() error }:
			err = u.Unwrap()
		case interface{ Unwrap() []error }:
			for _, one := range u.Unwrap() {
				if e := opErrorOf(one); e != nil {
					return e
				}
			}
			return nil
		default:
			return nil
		}
	}
	return nil
}
