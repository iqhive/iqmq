package middleware

import (
	"context"
	"runtime"

	mq "bitbucket.org/iqhive/iqmq"
)

// panicStackBytes is the stack buffer size, the same the runner uses.
const panicStackBytes = 8192

// Recover turns a panic from anything inside it into a *mq.PanicError
//
// The panic's identity survives: Value is what the handler panicked with and
// Stack is the panicking goroutine's stack, captured while the panic is still
// unwinding, so the frames that panicked are still on it. A recovery that
// returned a flat "handler panicked" error would destroy the only evidence
// there is, and errors.Is and errors.As would no longer reach a handler that
// panicked with an error — *mq.PanicError.Unwrap returns the value when it is
// one.
//
// The delivery then takes the configured failure policy exactly as a returned
// error would, which is the specification's "treated as retryable failure". A
// *mq.PanicError is never nil, so no panic can silently ack.
//
// onPanic, when non-nil, is called with the delivery and the error before the
// error is returned, for a caller that wants the panic logged or counted at
// the moment it happened. It must not panic itself; one that does takes the
// worker with it.
//
// # Why it exists beside the runner's own recovery
//
// The runner already recovers (ConsumerHandle.Run, unless mq.WithoutRecovery
// is given), and that recovery is OUTSIDE the middleware chain: a middleware
// that wanted to log, count or convert the panic never sees it, because the
// stack has already unwound past it. Recover placed at the head of a chain
// recovers first, so every middleware inside it observes an ordinary error
// return and the chain behaves the same way for a panic as for a failure.
// It is also what a chain used outside the runner needs, since nothing else
// is recovering there.
func Recover(onPanic func(ctx context.Context, d *mq.Delivery, err *mq.PanicError)) mq.Middleware {
	return func(next mq.Handler) mq.Handler {
		return mq.HandlerFunc(func(ctx context.Context, d *mq.Delivery) (err error) {
			defer func() {
				if v := recover(); v != nil {
					pe := &mq.PanicError{Value: v, Stack: stack()}
					if onPanic != nil {
						onPanic(ctx, d, pe)
					}
					err = pe
				}
			}()
			return next.Handle(ctx, d)
		})
	}
}

// stack captures the panicking goroutine's stack. It is called from a
// deferred function while the panic is still unwinding.
func stack() []byte {
	buf := make([]byte, panicStackBytes)
	return buf[:runtime.Stack(buf, false)]
}
