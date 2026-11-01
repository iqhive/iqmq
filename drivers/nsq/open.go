package nsq

import (
	"context"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit"
	"github.com/iqhive/iqmq/drivers/nsq/internal/nsqapi"
)

// Open connects to NSQ and returns a client.
//
// It creates no topology. nsqd creates a topic on the first publication to it
// whatever any client asks, which is a property of the broker and not a choice
// this driver makes — but the CHANNEL a subscription needs is not created that
// way, and a consumer that subscribes before its channel exists receives
// nothing published in between. [Config.EnsureTopology] is the deliberate
// creation, it is off by default, and the rule is about a driver that could
// provision and chooses not to.
//
// The configuration is validated before anything is dialled, so a typo is an
// immediate refusal rather than a five-second timeout.
func Open(ctx context.Context, cfg Config) (*mq.Client, error) {
	b, err := OpenBackend(ctx, cfg)
	if err != nil {
		return nil, err
	}
	return clientFor(b, cfg), nil
}

// OpenBackend connects and returns the [github.com/iqhive/iqmq.Backend]
// itself, for a caller composing the SPI directly.
func OpenBackend(ctx context.Context, cfg Config) (*Backend, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	resolved := cfg.resolve()
	prod, err := newProducer(resolved)
	if err != nil {
		return nil, err
	}
	b, err := newBackend(resolved, prod, driverkit.ClientOwned, nil)
	if err != nil {
		prod.Close()
		return nil, err
	}
	return b, nil
}

// OpenWithClient wraps a producer the caller already has.
//
// own says who closes it, and there is no default: a driver that always closed
// would tear down a client the application still uses, and one that never
// closed would leak the one it built itself.
//
// It is also the seam an in-process fake comes in through, which is why the
// parameter is the narrow [github.com/iqhive/iqmq/drivers/nsq/internal/nsqapi.Producer]
// rather than a `*nsq.Producer`: a driver whose tests can only talk to a real
// nsqd cannot make an expired per-message timeout or a redelivery to a second
// consumer happen on purpose.
func OpenWithClient(ctx context.Context, prod nsqapi.Producer, own driverkit.Ownership, cfg Config) (*mq.Client, error) {
	b, err := BackendWithClient(ctx, prod, own, cfg)
	if err != nil {
		return nil, err
	}
	return clientFor(b, cfg), nil
}

// BackendWithClient wraps a producer the caller already has and returns the
// backend itself.
func BackendWithClient(ctx context.Context, prod nsqapi.Producer, own driverkit.Ownership, cfg Config) (*Backend, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if prod == nil {
		return nil, errNilProducer
	}
	if err := driverkit.CheckOwnership(own); err != nil {
		return nil, err
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return newBackend(cfg.resolve(), prod, own, prod)
}

// clientFor builds the client around a backend, carrying the configuration's
// clock and hooks.
func clientFor(b *Backend, cfg Config) *mq.Client {
	opts := make([]mq.ClientOption, 0, 2)
	if cfg.Clock != nil {
		opts = append(opts, mq.WithClock(cfg.Clock))
	}
	if hooksSet(cfg.Hooks) {
		opts = append(opts, mq.WithHooks(cfg.Hooks))
	}
	return mq.NewClient(b, opts...)
}

// hooksSet reports whether any observation point is wired. mq.Hooks holds
// function fields, so it is not comparable and the question has to be asked
// field by field.
func hooksSet(h mq.Hooks) bool {
	return h.OnPublishStart != nil || h.OnPublishDone != nil || h.OnReceive != nil ||
		h.OnSettle != nil || h.OnReconnect != nil
}
