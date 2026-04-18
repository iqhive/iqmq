package middleware

import (
	"context"
	"time"

	mq "bitbucket.org/iqhive/iqmq"
)

// Timeout bounds everything inside it by d.
//
// The handler's context is already narrowed by the lease deadline and by
// mq.WithHandlerTimeout; this is the per-chain form, for a caller who
// wants one stage of the chain bounded more tightly than the run.
//
// A handler that returns because its context ended returns
// context.DeadlineExceeded, which is a plain error to the runner: the
// delivery is nacked with backoff and redelivered. A caller who wants a
// timeout to dead-letter says so by wrapping — the middleware does not decide
// that for them.
//
// It costs one context per delivery, which is what a deadline is; the specification measured the alternative (a clock-derived deadline with no context) and
// that is the runner's own path, not a middleware's, because only the runner
// knows the lease.
//
// Timeout panics on a non-positive d: a "timeout" of zero would refuse every
// delivery before the handler ran, which is never what a caller meant.
func Timeout(d time.Duration) mq.Middleware {
	if d <= 0 {
		panic(ErrInvalidConfig)
	}
	return func(next mq.Handler) mq.Handler {
		return mq.HandlerFunc(func(ctx context.Context, del *mq.Delivery) error {
			ctx, cancel := context.WithTimeout(ctx, d)
			defer cancel()
			return next.Handle(ctx, del)
		})
	}
}
