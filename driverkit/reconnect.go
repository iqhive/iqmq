package driverkit

import (
	"context"
	"math/rand/v2"
	"sync"
	"time"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/internal/backoff"
	"github.com/iqhive/iqmq/internal/errcode"
)

// DefaultReconnectPolicy is the backoff a [ReconnectConfig] that names none
// gets, and it is deliberately NOT the specification's `Auto`.
//
// the provisional winner is `none` -- no delay at all -- and
// [backoff.Kind]'s zero value is `Auto`, so a driver that passed its caller's
// unset policy through got a dial loop that answers a broker restart with an
// unbroken stream of attempts from every client in the fleet at once. The specification's
// "reconnect with backoff (never busy-spin)" forbids exactly that, and
// [Reconnector.wait] only softens it by yielding for a nanosecond. Three
// drivers had each named a jittered policy to work around the one default
// before this constant existed, which is the argument for moving the default
// rather than documenting the trap.
//
// The specification was measured on the SHAPE of a delay sequence -- one retry decision, one
// interval, one chain -- and not on a dial loop against a live broker, so
// inheriting its winner here reads a benchmark as the answer to a question it
// was not asked. A caller who wants the family's winner spells
// [backoff.AutoKind]; what nobody gets by accident is a busy dial loop.
const DefaultReconnectPolicy = backoff.Full

// ReconnectConfig configures a [Reconnector].
type ReconnectConfig struct {
	// Dial RESTORES THE CLIENT. It is not "open a socket": [Reconnector.Connect]
	// returns when Dial returns and [Health.Ready] is reported the instant it
	// returns nil, so whatever Dial leaves undone is undone in a client that
	// has been announced as ready.
	//
	// On a broker with topology that means Dial does all of it: dial, register
	// the connection's own failure notifications, replay every declaration this
	// client made, and re-attach every handle. A driver that returned from Dial
	// with a live socket and an unreplayed topology would report a client that
	// cannot publish, and the specification's "driver owns bounded reconnection AND topology
	// restoration" is one obligation for that reason.
	//
	// It is called with the reconnector's context and must respect it: a dial
	// that ignores cancellation is a goroutine a closing client cannot get rid
	// of.
	Dial func(ctx context.Context) error
	// Invalidate is the seam for "the connection is gone", and it is the one
	// step that is not a dial.
	//
	// [Reconnector.Lost] calls it once, on the caller's goroutine, AFTER the
	// generation bump and BEFORE the health report and any dial. That order is
	// the whole point: a driver invalidates its handles here -- fail every
	// outstanding publication as ambiguous, invalidate every source's tokens --
	// and a settlement that races the loss therefore finds the tokens already
	// dead rather than lands on whatever took their place.
	//
	// The helper cannot do this itself because it does not know what a handle
	// is, which is why three drivers wrote the same fifteen lines before this
	// field existed. It must not block and must not call
	// back into the reconnector.
	Invalidate func(err error)
	// Base is the first delay after a failure. Zero means the backoff
	// package's own default.
	Base time.Duration
	// Max caps every delay. Zero means the backoff package's default.
	Max time.Duration
	// Growth is the factor between consecutive delays. Below 1 means the
	// default.
	Growth float64
	// Policy is the backoff kind. The zero value is
	// [DefaultReconnectPolicy], a JITTERED policy, and not the specification's
	// `Auto`: see that constant for why a dial loop does not inherit the family's winner. A caller who wants the winner spells
	// [github.com/iqhive/iqmq/internal/backoff.AutoKind].
	Policy backoff.Kind
	// Clock is the time source the waits read. Nil means the wall clock.
	Clock mq.Clock
	// Rand supplies the random word the jitter draws from. Nil means the
	// runtime's own generator.
	Rand func() uint64
	// MaxAttempts bounds ONE [Reconnector.Connect] call -- which is one
	// outage -- and not the life of the reconnector. The counter starts at
	// zero for every call, so a client that has survived twenty outages has
	// the whole budget for the twenty-first; a caller who sets 100 is saying
	// "give up after a hundred attempts at THIS outage", which is the reading
	// every caller in this repository meant.
	//
	// Zero means it retries until the context is done, which is what a
	// long-lived client wants. [Reconnector.Attempts] is the number this
	// bounds; [Reconnector.TotalAttempts] is the other reading.
	MaxAttempts int
	// OnReconnect observes every attempt, successful or not. It runs
	// on the reconnecting goroutine and must not block.
	OnReconnect func(context.Context, mq.ReconnectEvent)
	// Health is the reporter the loop drives, or nil.
	Health *Health
	// Driver is the driver's name, which every event carries.
	Driver string
}

// Reconnector is the dial loop of the specification's "reconnect with backoff (never
// busy-spin)".
//
// It is three obligations in one place:
//
//   - The wait between attempts is a jittered backoff, so a broker restart is
//     not answered by every client in the fleet at the same instant.
//   - A lost connection bumps a generation. Every delivery handed out before the
//     bump is stale, and a settlement carrying one must report
//     [github.com/iqhive/iqmq.ErrLeaseLost] rather than land on whatever took its
//     place. [Reconnector.Generation] is what a driver's [TokenTable] follows.
//   - The loop never busy-spins and never buffers: it waits on a timer from the
//     injected clock and on the context, and it holds nothing.
//
// Every method is safe for concurrent use.
type Reconnector struct {
	cfg    ReconnectConfig
	policy backoff.Policy
	clock  mq.Clock
	rnd    func() uint64

	mu       sync.Mutex
	gen      uint32
	attempt  int
	total    int
	restores uint64
}

// NewReconnector returns a reconnector.
func NewReconnector(cfg ReconnectConfig) (*Reconnector, error) {
	if cfg.Dial == nil {
		return nil, errNoDial
	}
	if cfg.MaxAttempts < 0 || cfg.Base < 0 || cfg.Max < 0 {
		return nil, errReconnectBounds
	}
	if !cfg.Policy.Valid() {
		return nil, errReconnectPolicy
	}
	kind := cfg.Policy
	if kind == backoff.Auto {
		// The zero value is a jittered policy rather than the family's
		// winner: see [DefaultReconnectPolicy].
		kind = DefaultReconnectPolicy
	}
	policy, err := backoff.NewWith(kind, backoff.Config{Base: cfg.Base, Max: cfg.Max, Growth: cfg.Growth})
	if err != nil {
		return nil, err
	}
	r := &Reconnector{
		cfg:    cfg,
		policy: policy,
		clock:  cfg.Clock,
		rnd:    cfg.Rand,
		gen:    1,
	}
	if r.clock == nil {
		r.clock = wallClock{}
	}
	if r.rnd == nil {
		r.rnd = rand.Uint64
	}
	return r, nil
}

// Connect dials, retrying with backoff until it succeeds, ctx is done or
// MaxAttempts is reached.
//
// Every attempt is reported through OnReconnect, failures included, which is the rule that a driver's background failures surface through hooks and
// health rather than only in logs. A successful dial marks the reporter Ready.
func (r *Reconnector) Connect(ctx context.Context) error {
	var prev time.Duration
	for attempt := 1; ; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		if r.cfg.Health != nil {
			r.cfg.Health.Connecting()
		}
		err := r.cfg.Dial(ctx)
		r.record(ctx, attempt, err)
		if err == nil {
			// The restore is counted BEFORE the reporter goes Ready, so that
			// a caller polling [Reconnector.Restores] can never see a ready
			// client it has not been told about. Both happen strictly after
			// Dial returned, which is what makes either of them mean "the
			// client can work again".
			r.restored()
			if r.cfg.Health != nil {
				r.cfg.Health.Ready()
			}
			return nil
		}
		if r.cfg.Health != nil {
			r.cfg.Health.Degraded(err)
		}
		if r.cfg.MaxAttempts > 0 && attempt >= r.cfg.MaxAttempts {
			return err
		}
		prev = r.policy.Next(attempt, prev, r.rnd())
		if werr := r.wait(ctx, prev); werr != nil {
			return werr
		}
	}
}

// wait sleeps for d on the injected clock, or returns when ctx is done. A
// non-positive delay still yields, so the loop cannot become a busy spin even
// with a zero-delay policy.
func (r *Reconnector) wait(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		d = time.Nanosecond
	}
	t := r.clock.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C():
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Lost records that the connection went away, in the one order that is safe:
//
//  1. The generation is bumped, so every token handed out before now is stale.
//  2. [ReconnectConfig.Invalidate] runs, so the driver's own handles are dead
//     too -- every outstanding publication answered as ambiguous, every
//     source's tokens invalidated.
//  3. The reporter goes Degraded.
//
// A driver calls it from the native client's disconnect callback and then dials
// with [Reconnector.Connect]. Step 2 is what makes the sequence complete: it
// used to be the driver's own fifteen lines between the two calls, which is
// three drivers writing the same code and three chances to write it in the
// wrong order.
//
// The pending publications a driver holds at that moment are answered with an
// ambiguous or unavailable error, never with a definite failure: the broker may
// have taken them.
func (r *Reconnector) Lost(err error) uint32 {
	r.mu.Lock()
	r.gen++
	gen := r.gen
	r.mu.Unlock()
	if r.cfg.Invalidate != nil {
		r.cfg.Invalidate(err)
	}
	if r.cfg.Health != nil {
		r.cfg.Health.Degraded(err)
	}
	return gen
}

// Generation is the current connection generation. It changes on every loss, and
// a delivery handed out under an older one is stale.
func (r *Reconnector) Generation() uint32 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.gen
}

// Attempts is how many dials the current -- or, between outages, the most
// recent -- [Reconnector.Connect] has made. It is the number
// [ReconnectConfig.MaxAttempts] bounds and the number
// [github.com/iqhive/iqmq.ReconnectEvent.Attempt] carries, and it starts again
// at one for every outage.
//
// [Reconnector.TotalAttempts] is the other reading, and the two are separate
// accessors because one number answering to both names is what made a caller
// read `MaxReconnects` as a lifetime budget.
func (r *Reconnector) Attempts() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.attempt
}

// TotalAttempts is how many dials this reconnector has made over its whole
// life, across every outage. Nothing bounds it; it is the counter an operator
// plots.
func (r *Reconnector) TotalAttempts() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.total
}

// Restores is how many times [ReconnectConfig.Dial] has reported the client
// restored, first connection included.
//
// It is the observable form of Dial's contract: it rises when Dial returned
// nil, which is after the topology was replayed and every handle re-attached,
// so a test that waits on it waits for a client that can work rather than for
// a socket. A driver that watched the broker's own state
// instead would be racing the replay one declaration at a time, which is the
// defect this counter was added to close.
func (r *Reconnector) Restores() uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.restores
}

// Backoff is the delay the policy would choose for an attempt, which a driver
// reports and a test asserts on without running the loop.
func (r *Reconnector) Backoff(attempt int, prev time.Duration) time.Duration {
	return r.policy.Next(attempt, prev, r.rnd())
}

// restored counts a dial that reported the client back.
func (r *Reconnector) restored() {
	r.mu.Lock()
	r.restores++
	r.mu.Unlock()
}

// record counts an attempt and fires the hook.
func (r *Reconnector) record(ctx context.Context, attempt int, err error) {
	r.mu.Lock()
	r.attempt = attempt
	r.total++
	r.mu.Unlock()
	if r.cfg.OnReconnect != nil {
		// The dial's own context, so a hook that starts a span or records a
		// metric lands under whatever asked for the connection.
		r.cfg.OnReconnect(ctx, mq.ReconnectEvent{Driver: r.cfg.Driver, Attempt: attempt, Err: err})
	}
}

// The reconnector's construction failures.
var (
	errNoDial          = errcode.New(errcode.InvalidConfig, "iqmq/driverkit: a Reconnector needs a Dial function")
	errReconnectBounds = errcode.New(errcode.InvalidConfig, "iqmq/driverkit: a Reconnector's bounds cannot be negative")
	errReconnectPolicy = errcode.New(errcode.InvalidConfig, "iqmq/driverkit: the backoff policy is not a known kind")
)
