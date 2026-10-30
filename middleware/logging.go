package middleware

import (
	"context"
	"log/slog"
	"time"

	mq "github.com/iqhive/iqmq"
)

// LogConfig configures Logging. The zero value logs failures at Error with
// the wall clock.
type LogConfig struct {
	// Level is the level a failure is logged at. The zero value is
	// slog.LevelInfo, so Logging substitutes slog.LevelError unless Level is
	// set deliberately -- see Logging.
	Level slog.Level
	// LevelSet reports whether Level was chosen. A caller who really wants
	// failures at Info sets both.
	LevelSet bool
	// Clock measures the handler's duration. Nil selects the wall clock.
	Clock mq.Clock
}

// Logging logs handler failures to l.
//
// Failures only. The specification is explicit that iqmq never logs per message at Info
// or above, and a middleware that logged every delivery would do exactly
// that: on a topic doing ten thousand messages a second it is the most
// expensive thing in the chain and the least useful line in the log.
//
// A postponement ([mq.Postpone]) is not a failure: a middleware inside this
// one, such as a rate limit or an open circuit, turned the delivery away
// before the handler ran, and the runner offers it again. It is not logged,
// and the offer that reaches the handler is logged if it fails. Logged, a
// backlog behind a rate limit wrote an Error line for every refusal.
//
// What is logged is the topic, the subscription, the attempt, the message's
// application id when it has one, the handler's duration and the error. Never
// the body, never the headers, never a credential.
//
// A nil logger returns the handler unchanged, so Logging(nil) costs the chain
// nothing at all — not a branch a message. That is what makes it safe to
// write `middleware.Logging(cfg.Logger)` with a logger that is usually nil.
func Logging(l *slog.Logger) mq.Middleware { return LoggingWith(l, LogConfig{}) }

// LoggingWith is Logging with an explicit configuration.
func LoggingWith(l *slog.Logger, cfg LogConfig) mq.Middleware {
	if l == nil {
		return func(next mq.Handler) mq.Handler { return next }
	}
	level := slog.LevelError
	if cfg.LevelSet {
		level = cfg.Level
	}
	clk := clockOr(cfg.Clock)
	return func(next mq.Handler) mq.Handler {
		return mq.HandlerFunc(func(ctx context.Context, d *mq.Delivery) error {
			// The clock is read only when the logger is enabled for the
			// level, so an installed-but-disabled logger costs one Enabled
			// call and no time.Now.
			enabled := l.Enabled(ctx, level)
			var start time.Time
			// The metadata is read BEFORE the chain runs: anything inside
			// may settle the delivery, and under BufferReuse its record is
			// then recycled and refilled with another delivery's metadata.
			var info mq.DeliveryInfo
			if enabled {
				start = clk.Now()
				info = d.Info()
			}
			err := next.Handle(ctx, d)
			if err == nil || !enabled || postponed(err) {
				return err
			}
			l.LogAttrs(ctx, level, "iqmq: handler failed",
				slog.String("topic", info.Topic),
				slog.String("subscription", info.Subscription),
				slog.Int("attempt", d.Attempt),
				slog.String("message_id", d.ID),
				slog.Duration("duration", clk.Now().Sub(start)),
				slog.String("error", err.Error()),
			)
			return err
		})
	}
}
