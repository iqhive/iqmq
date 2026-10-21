package idempotency

import (
	"context"
	"errors"
	"sync/atomic"
	"time"

	mq "github.com/iqhive/iqmq"
)

// KeyFunc derives the idempotency key of one delivery, and reports whether
// there is one.
//
// It has no default. A store keyed on the wrong thing is worse than no store:
// two distinct messages that derive one key make the second a false duplicate,
// which DISCARDS work, and that is the only failure in this package that can
// lose a message. So the caller names the key, and [KeyFromHeader] and
// [KeyFromID] are the two obvious answers rather than the assumed one.
type KeyFunc func(d *mq.Delivery) (string, bool)

// KeyFromHeader reads the key from a header.
//
// This is the shape to use when the PUBLISHER owns the key, which is the usual
// case: the publisher writes the business identifier of the operation — an
// order id, a payment reference — and every retry of that operation carries
// the same one. A header key must not use the reserved prefix, and
// [Config.Validate] refuses one that does rather than leaving a middleware
// that can never find its header on a driver that strips the prefix.
func KeyFromHeader(name string) KeyFunc {
	return func(d *mq.Delivery) (string, bool) {
		v, ok := d.Headers.Get(name)
		if !ok || len(v) == 0 {
			return "", false
		}
		return string(v), true
	}
}

// KeyFromID reads the key from [mq.Message.ID].
//
// It is the right choice only where the identifier is the publisher's and is
// stable across ITS retries. A driver-assigned or broker-assigned identifier
// is not: two publications of one logical operation get two identifiers, both
// read as [StatusFirst], and the store catches nothing while appearing to
// work. IDGenerator is per publication, so a publisher configured with
// one is exactly the case this must not be used for.
func KeyFromID() KeyFunc {
	return func(d *mq.Delivery) (string, bool) {
		if d.ID == "" {
			return "", false
		}
		return d.ID, true
	}
}

// Unavailable is what the middleware does when the store cannot answer.
type Unavailable int

const (
	// OnUnavailableProcess runs the handler. It is the default.
	//
	// The cost is a duplicate side effect for the duration of the outage. The
	// reason it is the default is that the substrate underneath is
	// at-least-once already: this package REDUCES a duplicate rate that was
	// never zero, so failing open returns the system to the guarantee it
	// advertises rather than to a new one. The alternative turns a cache
	// outage into a consumer that makes no progress, and a consumer that
	// makes no progress on a broker with a max-attempts policy dead-letters
	// everything it is sent.
	OnUnavailableProcess Unavailable = iota
	// OnUnavailableRetry nacks the delivery so it is redelivered later.
	//
	// This is the choice for a handler whose effect is genuinely not safe to
	// repeat — a payment capture, an outbound SMS. Its cost is stated so that
	// it cannot be chosen by accident: while the store is unreachable NOTHING
	// is processed, every redelivery consumes an attempt, and a runner
	// configured with [mq.WithMaxAttempts] and a dead-letter topic will
	// dead-letter perfectly good messages because a cache was down.
	OnUnavailableRetry
)

// MissingKey is what the middleware does with a delivery [Config.Key] finds no
// key on.
type MissingKey int

const (
	// MissingKeyProcess runs the handler, unguarded, and counts the delivery
	// in [Counts.NoKey]. It is the default: a topic that mixes keyed and
	// unkeyed messages is ordinary, and refusing the unkeyed ones would make
	// adding this middleware a breaking change to a running service.
	MissingKeyProcess MissingKey = iota
	// MissingKeyFail nacks the delivery. It is the choice for a topic where
	// every message MUST carry a key, and it turns a publisher that forgot
	// one into a visible failure rather than an unguarded handler call.
	MissingKeyFail
)

// Config configures [New].
type Config struct {
	// Store holds the claims. Required.
	Store Store
	// Key derives the key. Required; see [KeyFunc].
	Key KeyFunc
	// Window is how long a committed claim suppresses a duplicate. Required
	// and positive.
	//
	// It is the only number here that is a real engineering choice. It must
	// outlast the longest redelivery the substrate can produce — a lease that
	// expires, a rebalance, a consumer restart, an operator replaying a
	// day — because a duplicate that arrives after the window is a duplicate
	// that is processed. It must also be short enough that the store holds
	// the resulting number of keys.
	Window time.Duration
	// InFlightLease is how long an OPEN claim lasts. Zero takes
	// [DefaultInFlightLease]; negative is refused. It is capped at Window.
	//
	// It is what a worker that dies mid-handler costs: its redelivery reads
	// [StatusInFlight] and is deferred until the lease lapses, so a lease as
	// long as a day-long Window would spend every attempt and dead-letter a
	// message nothing processed. It must still outlast the slowest handler,
	// because a claim that lapses under a running handler lets a redelivery
	// run it a second time, and that handler's commit is [Counts.CommitLost].
	InFlightLease time.Duration
	// OnUnavailable is the policy for a store that cannot answer. The zero
	// value is [OnUnavailableProcess].
	OnUnavailable Unavailable
	// OnMissingKey is the policy for a delivery with no key. The zero value
	// is [MissingKeyProcess].
	OnMissingKey MissingKey
	// InFlightRetryAfter is how long to wait before redelivering a message
	// whose key another worker holds an open claim on. Zero leaves the
	// runner's configured backoff in charge.
	//
	// A collision is NOT a failure of the message, but the runner cannot tell
	// the difference: every redelivery consumes an attempt, so a key two
	// workers keep colliding on can be dead-lettered by
	// [mq.WithMaxAttempts] without its handler ever having been called. That
	// is a real gap and it is recorded rather than
	// papered over here.
	InFlightRetryAfter time.Duration
}

// Validate reports whether the configuration is usable.
func (c Config) Validate() error {
	switch {
	case c.Store == nil:
		return ErrNoStore
	case c.Key == nil:
		return ErrNoKeyFunc
	case c.Window <= 0:
		return ErrBadWindow
	case c.InFlightLease < 0:
		return ErrBadInFlightLease
	}
	return nil
}

// DefaultInFlightLease is the [Config.InFlightLease] a zero value takes: five
// minutes, long enough for an ordinary handler and short enough that a dead
// worker's redelivery is deferred for minutes rather than for the window.
const DefaultInFlightLease = 5 * time.Minute

// lease is the open claim's lifetime: InFlightLease, defaulted and capped at
// Window.
func (c Config) lease() time.Duration {
	lease := c.InFlightLease
	if lease == 0 {
		lease = DefaultInFlightLease
	}
	return min(lease, c.Window)
}

// The configuration errors.
var (
	// ErrNoStore reports a Config with no Store.
	ErrNoStore = errors.New("idempotency: Config.Store is required")
	// ErrNoKeyFunc reports a Config with no Key. There is deliberately no
	// default; see [KeyFunc].
	ErrNoKeyFunc = errors.New("idempotency: Config.Key is required and has no default, because a store keyed on the wrong thing discards work")
	// ErrBadInFlightLease reports a negative Config.InFlightLease.
	ErrBadInFlightLease = errors.New("idempotency: Config.InFlightLease must not be negative; zero takes the default")
	// ErrMissingKey is what [MissingKeyFail] returns to the runner.
	ErrMissingKey = errors.New("idempotency: this delivery carries no idempotency key")
	// ErrInFlight is what the middleware returns to the runner for a key
	// another worker holds an open claim on. It is a RETRY and never a
	// discard: the work may not have happened.
	ErrInFlight = errors.New("idempotency: another worker holds an open claim on this key; redeliver this message")
)

// Counts is what one [Interceptor] has counted.
type Counts struct {
	// Seen is how many deliveries reached the middleware.
	Seen int64
	// Processed is how many reached the handler because the claim was
	// [StatusFirst].
	Processed int64
	// Skipped is how many did not reach the handler because the store
	// reported [StatusDuplicate]. This is the number the middleware exists
	// to make non-zero.
	Skipped int64
	// Deferred is how many were sent back for redelivery because another
	// worker held an open claim.
	Deferred int64
	// Unguarded is how many reached the handler WITHOUT a claim: the store
	// could not answer and the policy is [OnUnavailableProcess]. Every one is
	// a delivery this middleware did not protect, and a non-zero value is the
	// number to alert on.
	Unguarded int64
	// Refused is how many were sent back because the store could not answer
	// and the policy is [OnUnavailableRetry].
	Refused int64
	// NoKey is how many carried no key.
	NoKey int64
	// Failed is how many handler calls returned an error, and whose claim was
	// therefore aborted so that the redelivery is processed.
	Failed int64
	// CommitLost is how many handlers SUCCEEDED and could not record it: the
	// claim had lapsed or been taken by the time Commit ran. The delivery is
	// acked anyway — the work is done — so each one is a future duplicate
	// this store will not catch.
	CommitLost int64
}

// Interceptor is the middleware and its counters.
type Interceptor struct {
	cfg Config
	c   struct {
		seen, processed, skipped, deferred atomic.Int64
		unguarded, refused, noKey          atomic.Int64
		failed, commitLost                 atomic.Int64
	}
}

// New builds the interceptor.
func New(cfg Config) (*Interceptor, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return &Interceptor{cfg: cfg}, nil
}

// Counts reports what this interceptor has counted.
func (i *Interceptor) Counts() Counts {
	return Counts{
		Seen:       i.c.seen.Load(),
		Processed:  i.c.processed.Load(),
		Skipped:    i.c.skipped.Load(),
		Deferred:   i.c.deferred.Load(),
		Unguarded:  i.c.unguarded.Load(),
		Refused:    i.c.refused.Load(),
		NoKey:      i.c.noKey.Load(),
		Failed:     i.c.failed.Load(),
		CommitLost: i.c.commitLost.Load(),
	}
}

// Middleware returns the [mq.Middleware] that guards a handler.
//
// The chain is composed once when the runner starts, so the closure
// below is built once and the per-delivery cost is one claim, one key
// derivation and — on the first delivery of a key — one commit.
func (i *Interceptor) Middleware() mq.Middleware {
	return func(next mq.Handler) mq.Handler {
		return mq.HandlerFunc(func(ctx context.Context, d *mq.Delivery) error {
			return i.handle(ctx, next, d)
		})
	}
}

// handle is one guarded delivery.
func (i *Interceptor) handle(ctx context.Context, next mq.Handler, d *mq.Delivery) error {
	i.c.seen.Add(1)
	key, ok := i.cfg.Key(d)
	if !ok {
		i.c.noKey.Add(1)
		if i.cfg.OnMissingKey == MissingKeyFail {
			return ErrMissingKey
		}
		return next.Handle(ctx, d)
	}
	claim, err := i.cfg.Store.Claim(ctx, key, i.cfg.lease())
	switch {
	case err != nil, claim.Status == StatusUnknown:
		if i.cfg.OnUnavailable == OnUnavailableRetry {
			i.c.refused.Add(1)
			return i.redeliver(err)
		}
		// Fail open: the handler runs with no claim, so nothing is committed
		// afterwards and this delivery is not protected. It is counted
		// separately from Processed precisely so that "the store was down and
		// we carried on" is visible rather than inferred.
		i.c.unguarded.Add(1)
		return next.Handle(ctx, d)
	case claim.Status == StatusDuplicate:
		// The work was done inside the window. Returning nil acks the
		// delivery: the message HAS been processed, and a nack would ask the
		// broker to deliver it again for ever.
		i.c.skipped.Add(1)
		return nil
	case claim.Status == StatusInFlight:
		// Not a duplicate. See the package documentation: skipping this would
		// lose the work if the holder dies.
		i.c.deferred.Add(1)
		return i.redeliver(ErrInFlight)
	}

	i.c.processed.Add(1)
	// A PANICKING handler releases its claim too, and the panic carries on.
	//
	// The specification recovers a panicking handler unless the caller asked it not to, so
	// without this the panic unwinds past the Abort below and the claim is
	// held for the whole lease: the redelivery then reads StatusInFlight, is
	// deferred, and consumes attempts until a max-attempts policy
	// dead-letters a message nothing has processed (the hazard, at its worst). A panic is exactly when the claim should be
	// released.
	//
	// The re-panic is what keeps the runner's own recovery -- and
	// mq.WithoutRecovery -- in charge of what a panic means.
	done := false
	defer func() {
		if done {
			return
		}
		r := recover()
		if r == nil {
			// The handler returned through some other non-local exit, or the
			// goroutine is being torn down. Release the claim either way.
			_ = i.cfg.Store.Abort(ctx, claim)
			return
		}
		i.c.failed.Add(1)
		_ = i.cfg.Store.Abort(ctx, claim)
		panic(r)
	}()
	herr := next.Handle(ctx, d)
	done = true
	if herr != nil {
		i.c.failed.Add(1)
		// Release the claim so the redelivery is processed rather than
		// skipped. An Abort that itself fails is not escalated: the claim
		// lapses on its own at the end of the lease, and turning a failed
		// abort into a different error would hide the handler's own error —
		// which is the one the specification routes on.
		_ = i.cfg.Store.Abort(ctx, claim)
		return herr
	}
	// The open claim was the lease; the committed one is the whole window.
	claim.Window = i.cfg.Window
	if err := i.cfg.Store.Commit(ctx, claim); err != nil {
		// The work is DONE. Acking is the only answer that does not repeat
		// it, so the error is counted and not returned.
		i.c.commitLost.Add(1)
	}
	return nil
}

// redeliver turns a reason into the error the runner should see for a delivery
// that must come back.
func (i *Interceptor) redeliver(cause error) error {
	if cause == nil {
		cause = ErrInFlight
	}
	if i.cfg.InFlightRetryAfter > 0 {
		return mq.RetryAfter(cause, i.cfg.InFlightRetryAfter)
	}
	return cause
}
