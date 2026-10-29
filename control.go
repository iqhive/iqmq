package iqmq

import "time"

// Directive is the instruction a handler's error carries to the runner
// The specification names four of the constructors (Postpone is this
// package's own) and not the readback; ControlError and DirectiveOf are what
// make them act on something.
//
// A handler that returns a plain error is nacked with the configured backoff;
// the control errors below override that.
type Directive uint8

// The directives.
const (
	// DirectiveNone is the zero value: the error carries no instruction and
	// the configured failure policy decides.
	DirectiveNone Directive = iota
	// DirectiveRetryAfter asks for redelivery after a stated delay, in place
	// of the configured backoff.
	DirectiveRetryAfter
	// DirectiveDeadLetter asks for the message to be dead-lettered now,
	// without exhausting its attempts. A failed or ambiguous dead-letter
	// publish never acks the original.
	DirectiveDeadLetter
	// DirectiveDiscard asks for the message to be acked without success. It
	// is logged and counted, never silent.
	DirectiveDiscard
	// DirectiveHalt asks the runner to stop consuming.
	DirectiveHalt
	// DirectivePostpone asks for the message to be offered again later
	// without counting a failed attempt: the handler never ran. It is what a
	// middleware that refuses a delivery before the handler (a rate limit,
	// an open circuit, a duplicate still in flight) returns.
	DirectivePostpone

	directiveCount
)

var directiveNames = [directiveCount]string{
	"none", "retryafter", "deadletter", "discard", "halt", "postpone",
}

// String returns the directive's lower-case name.
func (d Directive) String() string {
	if d < directiveCount {
		return directiveNames[d]
	}
	return "directive(?)"
}

// Valid reports whether d names a directive.
func (d Directive) Valid() bool { return d < directiveCount }

// ControlError is a handler error that carries a Directive. The runner reads
// it with errors.As; anything that wraps it keeps the instruction reachable.
//
// A control error is a failure path and not a hot path: it is built per call.
type ControlError struct {
	// Directive is the instruction.
	Directive Directive
	// Delay is how long to wait before redelivery. It is meaningful only for
	// DirectiveRetryAfter and DirectivePostpone.
	Delay time.Duration
	// Err is the handler's own error, or nil when the handler had none.
	Err error
}

// Error implements error.
func (e *ControlError) Error() string {
	msg := "iqmq: " + e.Directive.String()
	if e.Directive == DirectiveRetryAfter || (e.Directive == DirectivePostpone && e.Delay > 0) {
		msg += " " + e.Delay.String()
	}
	if e.Err != nil {
		msg += ": " + e.Err.Error()
	}
	return msg
}

// Unwrap returns the handler's own error, so errors.Is and errors.As reach
// through the directive to what actually failed.
func (e *ControlError) Unwrap() error { return e.Err }

// Code returns the portable code of the wrapped error, or CodeUnknown when
// there is none: the directive is an instruction to the runner and is not
// itself a failure category. DirectiveHalt with no wrapped error reports
// CodePoisonMessage, which is what Halt means to the runner.
func (e *ControlError) Code() ErrorCode {
	if c := CodeOf(e.Err); c != CodeUnknown {
		return c
	}
	if e.Directive == DirectiveHalt {
		return CodePoisonMessage
	}
	return CodeUnknown
}

// Is reports whether target carries the same portable code, which keeps
// the matching rule true through a control error: Halt(ErrMessageTooLarge)
// matches ErrMessageTooLarge both by the chain and by the code, and Halt(nil)
// matches ErrPoisonMessage even though it wraps nothing. A directive with no
// code of its own — Discard of a plain error — matches nothing, because
// CodeUnknown is not a wildcard.
func (e *ControlError) Is(target error) bool {
	if target == nil {
		return false
	}
	code := e.Code()
	return code != CodeUnknown && CodeOf(target) == code
}

// RetryAfter wraps err so that the runner redelivers the message after d
// instead of applying the configured backoff. A negative d is treated
// as zero.
func RetryAfter(err error, d time.Duration) error {
	if d < 0 {
		d = 0
	}
	return &ControlError{Directive: DirectiveRetryAfter, Delay: d, Err: err}
}

// DeadLetter wraps err so that the runner dead-letters the message now,
// without exhausting its attempts.
func DeadLetter(err error) error {
	return &ControlError{Directive: DirectiveDeadLetter, Err: err}
}

// Discard wraps err so that the runner acks the message without success,
// logging and counting the discard. It is never the default: a handler
// has to ask.
func Discard(err error) error {
	return &ControlError{Directive: DirectiveDiscard, Err: err}
}

// Halt wraps err so that the runner stops consuming.
func Halt(err error) error {
	return &ControlError{Directive: DirectiveHalt, Err: err}
}

// Postpone wraps err so that the runner offers the message again after d
// without counting the delivery as a failed attempt, because the handler
// never ran. A d of zero or less waits the run's configured backoff instead,
// and at least a millisecond, for a refusal that cannot say when it will
// accept (offering the message again at once would turn it into a busy
// loop).
//
// It is for a middleware that refuses a delivery before the handler: a rate
// limit, an open circuit breaker, a duplicate whose first copy is still being
// handled. Returning RetryAfter or a plain error there spent one of
// WithMaxAttempts on every refusal, so a backlog behind a rate limit
// exhausted messages nobody had handled and halted the runner, dead-lettered
// them or, under FailureSkip, acked them.
//
// A postponement never exhausts a message and never reaches the failure policy.
// Where it can, the runner holds the delivery on its worker for d and calls the
// chain again, so the broker never hears of it and the attempt count stays
// exact. Run under OrderingStrict and OrderingStopOnFailure always does, so
// nothing overtakes the delivery. Under OrderingRelaxed it does while the lease
// allows: when the run extends the lease (WithLeaseExtension), when the driver
// reports none, or when d is at most half of what is left of it. The wait holds
// the worker as a slow handler would, so the deliveries queued behind it on its
// lane wait as well.
//
// An ordered run whose handler context ends with the lease (WithLeaseDeadline,
// with neither WithLeaseExtension nor WithHandlerTimeout) holds the delivery
// only while the lease allows, as OrderingRelaxed does. Held longer, the
// handler called afterwards was given a context that had already ended, and
// its failure was counted.
//
// A longer wait, and every postponement from RunBatch, sends the message back
// to the broker with d as the redelivery delay (at once, on a driver without
// delayed redelivery), and the deliveries behind it may then overtake it. A
// broker that counts deliveries counts that one, so Delivery.Attempt, and the
// bound a later failure meets, includes it.
//
// SettlementManual honours a postponement too: the handler never saw the
// delivery, so the runner holds it or gives it back as above.
func Postpone(err error, d time.Duration) error {
	return &ControlError{Directive: DirectivePostpone, Delay: d, Err: err}
}

// DirectiveOf returns the directive carried by err, and the delay when it is
// DirectiveRetryAfter or DirectivePostpone. An error carrying no directive
// returns DirectiveNone.
//
// It walks the unwrap chain by hand rather than through errors.As so that the
// runner's failure path performs no allocation, and it returns the outermost
// directive: a control error wrapped in another control error is the decision
// of whoever wrapped it last.
func DirectiveOf(err error) (Directive, time.Duration) {
	for err != nil {
		if e, ok := err.(*ControlError); ok { //nolint:errorlint // the chain is walked by hand to avoid allocating
			return e.Directive, e.Delay
		}
		switch u := err.(type) { //nolint:errorlint // this is the unwrap step itself
		case interface{ Unwrap() error }:
			err = u.Unwrap()
		case interface{ Unwrap() []error }:
			for _, one := range u.Unwrap() {
				if d, delay := DirectiveOf(one); d != DirectiveNone {
					return d, delay
				}
			}
			return DirectiveNone, 0
		default:
			return DirectiveNone, 0
		}
	}
	return DirectiveNone, 0
}
