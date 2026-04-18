// Package middleware ships the API's middleware set: small,
// off by default, each one composable with mq.Chain and each one measured.
//
// # Two directions, one package
//
// The specification declares two types and this package uses both. Most of the set wraps
// a handler and is an mq.Middleware:
//
//	Recover · Timeout · Retry · Logging · CorrelationID · RateLimit ·
//	CircuitBreaker · Filter · Dedup · Validate · Compress · Encrypt · Metrics
//
// Four wrap a publication instead and are mq.PublishMiddleware, because
// stamping an identifier or a timestamp, compressing a body or sealing one is
// something done to a message on its way out:
//
//	ID · Timestamp · PublishCompress · PublishEncrypt
//
// Compress and Encrypt appear on both sides for that reason: Compress
// decompresses an inbound body under a hard limit and PublishCompress
// produces one, Encrypt opens an inbound body and PublishEncrypt seals one.
// WrapPublisher is what makes a publish chain usable, since the core has no
// publisher option that takes one.
//
// # Composition happens once
//
// A chain is composed when the runner starts and never per message (
// rule 6). Nothing in this package composes anything per delivery, and
// BenchmarkMiddleware holds the composed chain of five no-ops to the specification's
// 30 ns budget — a figure only reachable if the composition is hoisted. A
// middleware that is not configured returns the handler it was given
// unchanged, so an unused middleware costs nothing at all: not a branch a
// message, not a frame.
//
// # Settling is the middleware's job when it short-circuits
//
// A middleware that refuses a delivery before the handler has taken the
// handler's place, and the handler is what settles under
// mq.SettlementManual. Filter, Validate and Dedup therefore settle the
// delivery themselves — Filter and Dedup ack, Validate dead-letters — and
// return nil, so the runner's own settlement is a no-op under the automatic
// modes and nothing stalls under the manual one. A middleware that returned
// without settling would hold the lease until it expired and the message
// would be redelivered for ever.
//
// # Clocks and observers
//
// Every middleware that measures or schedules takes an mq.Clock, and the
// default is the wall clock. Nothing reads a clock unless the middleware that
// needs it is installed ("nil hooks ⇒ no event allocation, no
// time.Now").
package middleware

import (
	"context"
	"errors"
	"time"

	mq "bitbucket.org/iqhive/iqmq"
	"bitbucket.org/iqhive/iqmq/internal/errcode"
)

// Errors this package's middlewares return. Each carries a portable code, so
// errors.Is against the root package's sentinels holds.
//
// the matching rule is by CODE, not by identity: errors.Is(err, X) is
// true for any error carrying X's code. So these sentinels tell a caller what
// KIND of failure happened -- a rate limit is CodeThrottled, an open circuit
// is CodeUnavailable -- and two of them that share a code (ErrNotSealed and
// ErrNotAuthentic, both CodeCorruptMessage) are told apart by their message
// rather than by errors.Is. That is the repository's rule and not this
// package's; a caller who needs to distinguish two failures with one code
// compares the messages or branches earlier.
var (
	// ErrRateLimited (CodeThrottled) reports a delivery the rate limiter gave
	// back to the broker.
	ErrRateLimited error = errcode.New(errcode.Throttled, "iqmq/middleware: rate limit exceeded")
	// ErrCircuitOpen (CodeUnavailable) reports a delivery the circuit breaker
	// refused without calling the handler.
	ErrCircuitOpen error = errcode.New(errcode.Unavailable, "iqmq/middleware: circuit is open")
	// ErrFiltered (CodeInvalidArgument) is the cause Filter reports to its
	// observer for a delivery the predicate rejected.
	ErrFiltered error = errcode.New(errcode.InvalidArgument, "iqmq/middleware: delivery filtered out")
	// ErrDuplicate (CodeConflict) is the cause Dedup reports for a delivery
	// whose key the store had already seen.
	ErrDuplicate error = errcode.New(errcode.Conflict, "iqmq/middleware: duplicate delivery")
	// ErrTooLarge (CodeMessageTooLarge) reports a body whose decompressed
	// size would exceed the configured limit. Nothing that large is ever
	// allocated: the limit is applied while decompressing, not after.
	ErrTooLarge error = errcode.New(errcode.MessageTooLarge, "iqmq/middleware: decompressed body exceeds the limit")
	// ErrNotSealed (CodeCorruptMessage) reports a body the encryption
	// middleware cannot open: too short to hold a nonce, or not sealed at
	// all.
	ErrNotSealed error = errcode.New(errcode.CorruptMessage, "iqmq/middleware: body is not sealed")
	// ErrNotAuthentic (CodeCorruptMessage) reports a sealed body whose
	// authentication tag does not verify: it was tampered with, truncated, or
	// sealed under another key.
	ErrNotAuthentic error = errcode.New(errcode.CorruptMessage, "iqmq/middleware: sealed body does not authenticate")
	// ErrNoKey (CodeInvalidConfig) reports a key the provider does not have.
	ErrNoKey error = errcode.New(errcode.InvalidConfig, "iqmq/middleware: no key for this message")
	// ErrInvalidConfig (CodeInvalidConfig) reports a middleware configuration
	// a constructor refuses. Constructors panic with it: a configuration
	// error is discovered once, at construction, and must not become a
	// per-delivery failure.
	ErrInvalidConfig error = errcode.New(errcode.InvalidConfig, "iqmq/middleware: invalid configuration")
)

// IDFunc produces an identifier. It must be safe for concurrent use.
type IDFunc func() string

// wallClock is the default mq.Clock: package time. It is here and not in the
// root because the root exports no clock — WithClock's default is internal —
// and every middleware that measures or schedules needs one it can be given
// instead.
type wallClock struct{}

// Now returns the current time.
func (wallClock) Now() time.Time { return time.Now() }

// NewTimer returns a single-shot timer.
func (wallClock) NewTimer(d time.Duration) mq.Timer { return wallTimer{time.NewTimer(d)} }

// wallTimer adapts *time.Timer to mq.Timer.
type wallTimer struct{ t *time.Timer }

// C returns the expiry channel.
func (w wallTimer) C() <-chan time.Time { return w.t.C }

// Stop prevents the timer from firing.
func (w wallTimer) Stop() bool { return w.t.Stop() }

// Reset re-arms the timer.
func (w wallTimer) Reset(d time.Duration) bool { return w.t.Reset(d) }

// clockOr returns c, or the wall clock when c is nil.
func clockOr(c mq.Clock) mq.Clock {
	if c == nil {
		return wallClock{}
	}
	return c
}

// ackAndStop settles d positively for a middleware that decided the handler
// must not see it, and returns what the middleware should return.
//
// The ack is the point. A middleware that short-circuits the handler has
// taken the handler's place, and under mq.SettlementManual the handler is
// what settles — so a middleware that simply returned nil would leave the
// lease held until it expired and the message would be redelivered for ever.
// Under the automatic modes the runner sees a delivery that is already
// settled and does not overrule it.
//
// ErrAlreadySettled is not a failure here: another goroutine, or an inner
// middleware, may have settled it first, and the delivery is settled either
// way. Any other failure is returned, so that a delivery which could not be
// settled reaches the runner's failure path instead of being forgotten.
func ackAndStop(d *mq.Delivery) error {
	err := d.Ack()
	if err == nil || errors.Is(err, mq.ErrAlreadySettled) {
		return nil
	}
	return err
}

// rejectAndStop settles d terminally for a middleware that refused it, and
// returns what the middleware should return.
//
// Reject is Nack(NackDeadLetter): a message the application refuses is not a
// message to drop (ADR-011's "never skip"). A backend with no dead-letter
// destination answers ErrUnsupported and settles nothing, which leaves the
// delivery settleable — so the decision is handed to the runner as
// mq.DeadLetter(cause), and the runner's own emulated dead-letter route, or
// its halt when there is none, decides.
func rejectAndStop(d *mq.Delivery, cause error) error {
	err := d.Reject()
	switch {
	case err == nil, errors.Is(err, mq.ErrAlreadySettled):
		return nil
	case errors.Is(err, mq.ErrUnsupported):
		return mq.DeadLetter(cause)
	default:
		return err
	}
}

// contextKey is this package's context key type. It is unexported so that no
// other package can collide with it.
type contextKey uint8

const (
	// keyCorrelation carries the delivery's correlation id.
	keyCorrelation contextKey = iota
	// keyTrace carries the parsed W3C trace context.
	keyTrace
	// keyPublishOptions carries a wrapped publication's per-call options
	// past the mq.PublishFunc signature, which has no room for them.
	keyPublishOptions
)

// Compile-time proof that every constructor in this package produces the type
// The specification declares, so a rename of either type is caught here rather than at a
// caller.
var (
	_ mq.Middleware        = Recover(nil)
	_ mq.Middleware        = Timeout(time.Second)
	_ mq.Middleware        = Retry(RetryConfig{})
	_ mq.Middleware        = Logging(nil)
	_ mq.Middleware        = CorrelationID(CorrelationConfig{})
	_ mq.Middleware        = RateLimit(RateLimitConfig{Rate: 1, Burst: 1})
	_ mq.Middleware        = CircuitBreaker(BreakerConfig{Threshold: 1, Cooldown: time.Second})
	_ mq.Middleware        = Filter(nil)
	_ mq.Middleware        = Dedup(DedupConfig{})
	_ mq.Middleware        = Validate(nil)
	_ mq.Middleware        = Compress(CompressConfig{})
	_ mq.Middleware        = Encrypt(EncryptConfig{})
	_ mq.Middleware        = Metrics(nil, nil)
	_ mq.PublishMiddleware = ID(nil)
	_ mq.PublishMiddleware = Timestamp(nil)
	_ mq.PublishMiddleware = PublishCompress(CompressConfig{})
	_ mq.PublishMiddleware = PublishEncrypt(EncryptConfig{})
)

// publishOptionsFrom returns the per-call publication options a wrapped
// publisher put in ctx, if any.
func publishOptionsFrom(ctx context.Context) []mq.PublishOption {
	opts, _ := ctx.Value(keyPublishOptions).([]mq.PublishOption)
	return opts
}
