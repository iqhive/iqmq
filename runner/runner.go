// Package runner runs a consumer from open to graceful stop in one call.
//
// # What it is for
//
// The root package already has the runner; what it does not have is the four
// steps around it that every program writes identically:
//
//	c, err := cl.Consumer(ctx, mq.ConsumerConfig{Topic: "orders", Subscription: "billing"})
//	if err != nil { return err }
//	defer c.Close()
//	err = c.Run(ctx, h, mq.WithConcurrency(8), mq.WithMiddleware(mw...))
//	// ... and then the drain, which is the step programs get wrong
//
// becomes
//
//	timeout, err := middleware.Timeout(5 * time.Second)
//	if err != nil {
//		return err
//	}
//	err = runner.Run(ctx, cl, h, runner.Config{
//		Consumer:   mq.ConsumerConfig{Topic: "orders", Subscription: "billing"},
//		Middleware: []mq.Middleware{middleware.Recover(nil), timeout},
//		Options:    []mq.RunOption{mq.WithConcurrency(8)},
//		Drain:      30 * time.Second,
//	})
//
// The step it exists for is the last one. The specification gives Close no context — a
// prompt close is not an operation a caller cancels — and Shutdown is the
// bounded form, so a graceful stop is "run until the context ends, then
// Shutdown with a deadline of its own, then Close". A program that writes
// `defer c.Close()` and nothing else settles nothing in flight; a program that
// writes `defer c.Shutdown(ctx)` waits on a context that has, by definition,
// already ended. Both are common and both are silent.
//
// # What it deliberately is not
//
// It is a facade and not a second runner: every option is the root's, the
// chain is the root's mq.Chain, and the failure policy, retry accounting and
// dead-lettering are ConsumerHandle.Run's. Nothing here interposes on the
// delivery path, so a program pays nothing per message for using it.
//
// It also owns the consumer it opens. A caller that already holds a
// ConsumerHandle wants ConsumerHandle.Run directly; this package is for the
// case where the consumer's lifetime is the run's.
package runner

import (
	"context"
	"errors"
	"time"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/internal/errcode"
)

// DefaultDrain is the drain deadline a zero Config.Drain selects: how long a
// stopped run is given to settle what is still in flight before the consumer
// is closed under it.
const DefaultDrain = 30 * time.Second

// ErrInvalidConfig reports a Config that Validate rejects. It is returned by
// Run before anything is opened.
//
// It carries CodeInvalidConfig, which is how the specification names a configuration
// refused at construction: errors.Is(err, ErrInvalidConfig) holds, and so
// does mq.CodeOf(err) == mq.CodeInvalidConfig.
var ErrInvalidConfig error = errcode.New(errcode.InvalidConfig, "iqmq/runner: invalid configuration")

// Config names the consumer to open and how to run over it. The zero value is
// not usable: Consumer.Topic and Consumer.Subscription are required, as they
// are for ConsumerConfig itself.
type Config struct {
	// Consumer is the consumer to open. It is the root package's own
	// configuration and is passed through unchanged.
	Consumer mq.ConsumerConfig
	// Middleware is the chain wrapped around the handler, outermost first.
	// It is composed once, when the run starts, by mq.Chain; a nil entry is
	// skipped. Leaving it empty costs nothing at all — no option is added.
	Middleware []mq.Middleware
	// Options are the root package's run options, applied before Middleware.
	//
	// Middleware and a mq.WithMiddleware option in this slice are two
	// spellings of one setting, and the root's rule is that the last option
	// wins — so a non-empty Middleware REPLACES a mq.WithMiddleware here
	// rather than composing with it, and an empty Middleware leaves these
	// options exactly as they are. Use one or the other; naming both is a
	// chain the caller did not get.
	Options []mq.RunOption
	// Drain is how long the handlers still in flight when ctx ends are given
	// to finish before their contexts are cancelled, and then how long
	// Shutdown is given to settle what is left and close the consumer. Zero
	// selects DefaultDrain; a negative value is rejected by Validate.
	//
	// The first bound is the run's own mq.WithDrainTimeout, and Drain and a
	// mq.WithDrainTimeout in Options are two spellings of it: a non-zero
	// Drain REPLACES the option, as a non-empty Middleware replaces a
	// mq.WithMiddleware, and a zero Drain leaves the option as it is, or the
	// run's own default, mq.DefaultDrainTimeout, which is DefaultDrain. A
	// Drain shorter than that default used to bound only the close, after
	// the handlers had already been given the full default.
	//
	// The drain runs on a context detached from the caller's, because the
	// caller's context is what stopped the run: a drain bounded by an
	// already-cancelled context would settle nothing, which is the defect
	// this package exists to prevent.
	Drain time.Duration
}

// Validate reports whether the configuration is usable. It is pure and
// allocates nothing on the accepting path.
func (cfg Config) Validate() error {
	if cfg.Consumer.Topic == "" || cfg.Consumer.Subscription == "" {
		return ErrInvalidConfig
	}
	if cfg.Drain < 0 {
		return ErrInvalidConfig
	}
	return nil
}

// drain returns the effective drain deadline.
func (cfg Config) drain() time.Duration {
	if cfg.Drain == 0 {
		return DefaultDrain
	}
	return cfg.Drain
}

// Runner binds a client, a handler and a Config. It is safe to call Run on it
// from one goroutine; a second concurrent Run opens a second consumer, which
// is a second subscriber and not a second runner over the same one.
//
// The zero value is not usable; New returns a ready one.
type Runner struct {
	cl  *mq.Client
	h   mq.Handler
	cfg Config
}

// New binds cl, h and cfg into a Runner. It validates nothing: Run does, so
// that a configuration error is reported where it can be returned.
func New(cl *mq.Client, h mq.Handler, cfg Config) *Runner {
	return &Runner{cl: cl, h: h, cfg: cfg}
}

// Run opens the consumer, runs the handler over it until ctx ends or the
// runner halts, drains, and closes the consumer.
//
// It returns what ConsumerHandle.Run returned — context.Canceled for an
// ordinary stop, a *mq.PoisonError for a halt — joined with any failure of
// the drain or the close, so a caller that ignores neither still sees both.
func (r *Runner) Run(ctx context.Context) error {
	if err := r.cfg.Validate(); err != nil {
		return err
	}
	if r.cl == nil || noHandler(r.h) {
		return ErrInvalidConfig
	}
	c, err := r.cl.Consumer(ctx, r.cfg.Consumer)
	if err != nil {
		return err
	}
	runErr := c.Run(ctx, r.h, r.options()...)
	return errors.Join(runErr, r.stop(ctx, c))
}

// noHandler reports whether h is nil or a nil mq.HandlerFunc. A nil func
// converted to mq.Handler is a non-nil interface, so h == nil alone opened
// the consumer for a handler that panicked on its first delivery.
func noHandler(h mq.Handler) bool {
	f, isFunc := h.(mq.HandlerFunc)
	return h == nil || isFunc && f == nil
}

// options builds the run options: the caller's first, then a non-zero
// Config.Drain as one mq.WithDrainTimeout, then Config.Middleware as one
// mq.WithMiddleware. A zero Drain and an empty Middleware add no option at
// all, so a caller who spelled either in Options keeps it.
//
// The drain timeout is the run's because ConsumerHandle.Run drains its own
// handlers before it returns. Bounding only the Shutdown in stop, as this
// package used to, bounded a close with nothing left in flight: the handlers
// had already been given the run's default of 30s, or whatever Options said.
func (r *Runner) options() []mq.RunOption {
	if len(r.cfg.Middleware) == 0 && r.cfg.Drain == 0 {
		return r.cfg.Options
	}
	opts := make([]mq.RunOption, 0, len(r.cfg.Options)+2)
	opts = append(opts, r.cfg.Options...)
	if r.cfg.Drain != 0 {
		opts = append(opts, mq.WithDrainTimeout(r.cfg.Drain))
	}
	if len(r.cfg.Middleware) > 0 {
		opts = append(opts, mq.WithMiddleware(r.cfg.Middleware...))
	}
	return opts
}

// stop drains the consumer and releases it.
//
// Shutdown is the bounded close it stops fetching, lets the
// settlements already in flight finish, and then closes the source. It IS the
// close, and it is idempotent, so nothing calls Close after it — a Close that
// ran here would observe this same outcome (the specification's "a second call observes
// the first one's outcome") and report the one failure a second time.
//
// The drain's context is detached from the caller's; see drainContext.
func (r *Runner) stop(ctx context.Context, c *mq.ConsumerHandle) error {
	dctx, cancel := drainContext(ctx, r.cfg.drain())
	defer cancel()
	return c.Shutdown(dctx)
}

// drainContext returns the context the drain runs under: ctx's values, none
// of its cancellation, and a deadline d from now.
//
// WithoutCancel is the whole point. The caller's context is what ended the
// run, so a drain derived from it is a drain with no time at all: Shutdown
// would return at once and the consumer would be closed under the
// settlements still finishing. Keeping the values means a drain still carries
// whatever a caller put in the context — a trace, a tenant — into the
// settlements it completes.
func drainContext(ctx context.Context, d time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(ctx), d)
}

// Run is the one-call form: New(cl, h, cfg).Run(ctx).
func Run(ctx context.Context, cl *mq.Client, h mq.Handler, cfg Config) error {
	return New(cl, h, cfg).Run(ctx)
}
