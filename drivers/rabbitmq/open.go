package rabbitmq

import (
	"context"

	amqp "github.com/rabbitmq/amqp091-go"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit"
	"github.com/iqhive/iqmq/drivers/rabbitmq/internal/client"
)

// Open dials RabbitMQ and returns a client.
//
// It performs exactly as much I/O as it must: one connection, and — only when
// [Config.DetectDelayPlugin] is set — one throwaway channel for the
// delayed-message probe. It creates NO topology unless [Config.Declare] says
// to, which is the rule; with declaration off, a publisher's own open is
// what discovers that its exchange does not exist, through a passive declare
// rather than through a publication that succeeds and goes nowhere.
//
// The configuration is validated before the dial, so a typo is an immediate
// refusal rather than a fifteen-second timeout.
func Open(ctx context.Context, cfg Config) (*mq.Client, error) {
	b, err := OpenBackend(ctx, cfg)
	if err != nil {
		return nil, err
	}
	return clientFor(b, cfg)
}

// OpenBackend is [Open] without the client wrapper, for a caller composing the
// backend itself.
func OpenBackend(ctx context.Context, cfg Config) (*Backend, error) {
	b, err := newBackend(cfg)
	if err != nil {
		return nil, err
	}
	if err := b.start(ctx, driverkit.ClientOwned); err != nil {
		b.cancel()
		return nil, err
	}
	return b, nil
}

// OpenWithClient wraps an *amqp091.Connection the caller already has.
//
// own says who closes it, and there is no default: a driver that always closed
// would tear down a connection the application still uses, and one that never
// closed would leak the connection it dialled itself. It is also the way in
// for an authentication scheme a typed Config cannot carry — an external SASL
// mechanism, a custom dialler, a connection through a proxy.
//
// The driver's own dial loop is NOT disabled by it. A borrowed connection that
// dies is still a connection this driver has to replace, and it replaces it by
// dialling [Config.Addrs] — so a caller who hands over a connection also has
// to say how to make another one. A configuration with no addresses is refused
// for exactly that reason. own covers the connection the caller handed over
// and nothing else: one the dial loop made after it died is the driver's, and
// Close closes it under either ownership. Under [driverkit.ClientBorrowed] an
// open that fails leaves the caller's connection open too.
func OpenWithClient(ctx context.Context, conn *amqp.Connection, own driverkit.Ownership, cfg Config) (*mq.Client, error) {
	b, err := BackendWithClient(ctx, conn, own, cfg)
	if err != nil {
		return nil, err
	}
	return clientFor(b, cfg)
}

// BackendWithClient is [OpenWithClient] without the client wrapper.
func BackendWithClient(ctx context.Context, conn *amqp.Connection, own driverkit.Ownership, cfg Config) (*Backend, error) {
	if conn == nil {
		return nil, errNilConnection
	}
	b, err := newBackend(cfg)
	if err != nil {
		return nil, err
	}
	b.dial = &borrowed{conn: conn, next: client.Real{}}
	if err := b.start(ctx, own); err != nil {
		b.cancel()
		return nil, err
	}
	return b, nil
}

// BackendWithDialer is [OpenBackend] over a caller-supplied dialler.
//
// It is exported because the scripted fake the driver's own critical tests
// drive is in another package, and a driver whose failure paths can only be
// reached through a real broker has failure paths nobody tests: a channel
// close during a confirm, a mandatory return, a broker-side consumer cancel
// and a settlement on a replaced channel are all provoked here and by nothing
// else.
func BackendWithDialer(ctx context.Context, dialer client.Dialer, cfg Config) (*Backend, error) {
	if dialer == nil {
		return nil, errNilDialer
	}
	b, err := newBackend(cfg)
	if err != nil {
		return nil, err
	}
	b.dial = dialer
	if err := b.start(ctx, driverkit.ClientOwned); err != nil {
		b.cancel()
		return nil, err
	}
	return b, nil
}

// OpenWithDialer is [BackendWithDialer] wrapped in a client.
func OpenWithDialer(ctx context.Context, dialer client.Dialer, cfg Config) (*mq.Client, error) {
	b, err := BackendWithDialer(ctx, dialer, cfg)
	if err != nil {
		return nil, err
	}
	return clientFor(b, cfg)
}

// borrowed is the dialler that hands over a connection the caller already
// made, once.
//
// The FIRST dial returns the borrowed connection and every later one dials
// afresh, which is the only coherent answer to "the borrowed connection died":
// the caller's connection cannot be resurrected, and a driver that gave up
// would turn a borrowed connection into a client that works until the first
// network blip.
type borrowed struct {
	conn *amqp.Connection
	next client.Dialer
	used bool
}

// Dial hands over the borrowed connection once, then falls through.
func (b *borrowed) Dial(ctx context.Context, urls []string, cfg client.DialConfig) (client.Conn, error) {
	if !b.used {
		b.used = true
		return client.Wrap(b.conn), nil
	}
	return b.next.Dial(ctx, urls, cfg)
}

// clientFor builds the client around a backend, carrying the configuration's
// clock and hooks.
func clientFor(b *Backend, cfg Config) (*mq.Client, error) {
	opts := make([]mq.ClientOption, 0, 2)
	if cfg.Clock != nil {
		opts = append(opts, mq.WithClock(cfg.Clock))
	}
	if hooksSet(cfg.Hooks) {
		opts = append(opts, mq.WithHooks(cfg.Hooks))
	}
	return mq.NewClient(b, opts...), nil
}

// hooksSet reports whether any hook is set, so that a client is not given an
// empty hook set to call through.
func hooksSet(h mq.Hooks) bool {
	return h.OnPublishStart != nil || h.OnPublishDone != nil || h.OnReceive != nil ||
		h.OnSettle != nil || h.OnReconnect != nil
}
