package amqp10

import (
	"context"
	"time"

	amqp "github.com/Azure/go-amqp"

	mq "bitbucket.org/iqhive/iqmq"
	"bitbucket.org/iqhive/iqmq/driverkit"
	"bitbucket.org/iqhive/iqmq/drivers/amqp10/internal/client"
	"bitbucket.org/iqhive/iqmq/internal/errcode"
)

// Open dials the broker and returns a client.
//
// It performs exactly one piece of I/O — the connection — and creates NO
// topology: AMQP 1.0 cannot create a node, so the specification's "no hidden creation" is a
// property of the protocol here rather than a choice. Whether an ATTACH to an
// address nobody provisioned succeeds is the broker's own policy, which
// [Profile.AutoCreates] reports and the driver page states.
//
// The configuration AND the profile are validated before the dial, so a typo is
// an immediate refusal rather than a ten-second timeout. The dial itself is
// a single attempt: a bad address should fail Open rather than be retried for
// ever behind a caller that is waiting. The dial LOOP starts once the connection
// exists, and from then on a lost connection is re-dialled with jittered backoff
// (reconnect.go).
//
// This is not a driver of its own. It is the AMQP 1.0 core of one, so it cannot
// be opened without a [Profile]: there is no default addressing, no default
// capability claim and no default version gate that would be right for two
// brokers at once.
func Open(ctx context.Context, profile Profile, cfg Config) (*mq.Client, error) {
	b, err := OpenBackend(ctx, profile, cfg)
	if err != nil {
		return nil, err
	}
	return clientFor(b, cfg), nil
}

// OpenWithClient wraps an *amqp.Conn the caller already has.
//
// own says who closes it, and there is no default: a driver that always closed
// would tear down a connection the application still uses, and one that never
// closed would leak the connection it dialled itself.
//
// It is also the way in for everything a typed [Config] cannot carry — a
// Kerberos or OAuth SASL layer, a custom dialler, a connection over a tunnel:
// build the *amqp.Conn however the deployment needs and hand it over. A caller
// who does that usually supervises the connection itself, which is what
// [Config.NoReconnect] is for; with it off, this driver re-dials the ADDRESSES
// in Config when the borrowed connection dies, which is only right when those
// addresses reach the same broker.
func OpenWithClient(ctx context.Context, conn *amqp.Conn, own driverkit.Ownership, profile Profile, cfg Config) (*mq.Client, error) {
	b, err := BackendWithClient(ctx, conn, own, profile, cfg)
	if err != nil {
		return nil, err
	}
	return clientFor(b, cfg), nil
}

// OpenBackend dials the broker and returns the
// [bitbucket.org/iqhive/iqmq.Backend] itself, for a caller composing the SPI
// directly — which is what a broker-specific module built on this core does.
func OpenBackend(ctx context.Context, profile Profile, cfg Config) (*Backend, error) {
	return openBackend(ctx, profile, cfg, client.RealDialer{}, nil, driverkit.ClientOwned)
}

// BackendWithClient is [OpenWithClient] without the client wrapper.
func BackendWithClient(ctx context.Context, conn *amqp.Conn, own driverkit.Ownership, profile Profile, cfg Config) (*Backend, error) {
	if conn == nil {
		return nil, errNilConn
	}
	if err := driverkit.CheckOwnership(own); err != nil {
		return nil, err
	}
	return openBackend(ctx, profile, cfg, client.RealDialer{}, client.WrapConn(conn), own)
}

// BackendWithDialer is [OpenBackend] over a dialler the caller supplies.
//
// It is exported because it is what makes every one of this module's own tests
// runnable with no broker: the scripted peer of internal/client is a Dialer, and
// a driver whose tests could only reach it through an unexported constructor
// could not be tested from another module — which the critical tests are
// (the same reason drivers/sqs exports BackendWithAPI).
//
// It is also the seam a broker-specific module uses to test ITS profile against
// this core, which is the point of the core being a module of its own.
func BackendWithDialer(ctx context.Context, profile Profile, cfg Config, dialer client.Dialer) (*Backend, error) {
	if dialer == nil {
		return nil, errNilDialer
	}
	return openBackend(ctx, profile, cfg, dialer, nil, driverkit.ClientOwned)
}

// openBackend is the one construction path: validate, dial (or take the
// caller's connection), read the peer's facts, build the table, start the
// supervisor.
func openBackend(ctx context.Context, profile Profile, cfg Config, dialer client.Dialer, conn client.Conn, own driverkit.Ownership) (*Backend, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if profile.Driver == "" && profile.SinkAddress == nil && profile.SourceAddress == nil {
		// The whole profile is missing rather than incomplete, which is a
		// different mistake and deserves a different message: somebody has
		// reached for this module expecting a driver.
		return nil, errNilProfileOpen
	}
	if err := profile.Validate(); err != nil {
		return nil, err
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	resolved := cfg.resolve()
	b := &Backend{
		cfg:     resolved,
		profile: profile,
		dialer:  dialer,
		addrs:   resolved.Addrs,
	}
	b.health = driverkit.NewHealth(nil)
	if conn == nil {
		b.health.Connecting()
		b.cmu.Lock()
		b.addrNext = 0
		b.cmu.Unlock()
		dctx, cancel := dialBound(ctx, resolved.DialTimeout)
		c, err := dialer.Dial(dctx, b.nextAddr(), resolved.connOptions(profile))
		cancel()
		if err != nil {
			b.health.Degraded(err)
			b.health.Close()
			// The error mapper needs the profile's name and nothing else, so
			// it can be built before the connection exists — which is what
			// lets a failed dial be reported with the same rules as every
			// other failure.
			return nil, newErrorMapper(profile, resolved.ReceiverSettle).Map(mq.OpOpen, "", "", err)
		}
		conn = c
	}
	if err := b.init(ctx, conn, own); err != nil {
		b.health.Degraded(err)
		b.health.Close()
		if own.Owned() {
			_ = conn.Close()
		}
		return nil, err
	}
	return b, nil
}

// dialBound bounds the initial dial. It is the package-level twin of
// [Backend.dialBound], which the loop uses: the initial dial happens before the
// backend exists.
func dialBound(ctx context.Context, d time.Duration) (context.Context, context.CancelFunc) {
	if d <= 0 {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, d)
}

// clientFor builds the client around a backend, carrying the configuration's
// clock and hooks.
//
// The backend goes through [AdminBackend] first, which is what makes
// Client.Admin reach a profile's own topology surface: the core asserts
// [bitbucket.org/iqhive/iqmq.AdminBackend] once, here, so a backend that acquired
// the surface afterwards would never be asked for it.
func clientFor(b *Backend, cfg Config) *mq.Client {
	opts := make([]mq.ClientOption, 0, 2)
	if cfg.Clock != nil {
		opts = append(opts, mq.WithClock(cfg.Clock))
	}
	if hooksSet(cfg.Hooks) {
		opts = append(opts, mq.WithHooks(cfg.Hooks))
	}
	return mq.NewClient(AdminBackend(b), opts...)
}

// ClientForBackend wraps a backend this package opened in the portable client,
// which is what [Open] does after it has dialled.
//
// It is exported for a caller that needs to KEEP the backend as well as the
// client, and a conformance harness is the case that needs it:
// [Backend.Native] answers the peer connection — the escape hatch, and the
// right answer for a caller reaching past the SPI — so a harness cannot
// recover the backend from the client it was given. Opening the backend and
// building the client in two steps is the only way to hold both.
func ClientForBackend(b *Backend, cfg Config) *mq.Client { return clientFor(b, cfg) }

// hooksSet reports whether any observation point is wired.
//
// [bitbucket.org/iqhive/iqmq.Hooks] holds function fields, so it is not comparable
// and the question has to be asked field by field.
func hooksSet(h mq.Hooks) bool {
	return h.OnPublishStart != nil || h.OnPublishDone != nil || h.OnReceive != nil ||
		h.OnSettle != nil || h.OnReconnect != nil
}

// OpenSink opens a publication channel to one address.
//
// ConfirmDurable is refused HERE and not per publication when the profile
// defines no durable boundary, which is the rule: "ConfirmDurable fails at
// construction when the profile cannot define a meaningful durable boundary".
func (b *Backend) OpenSink(ctx context.Context, cfg mq.SinkConfig) (mq.Sink, error) {
	if err := b.CheckOpen(ctx); err != nil {
		return nil, err
	}
	if cfg.Receipts && !b.caps.Has(mq.FeatureReceipts) {
		return nil, mq.ErrUnsupported
	}
	s, err := b.newSink(cfg)
	if err != nil {
		return nil, err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return nil, mq.ErrClosed
	}
	b.sinks[s] = struct{}{}
	return s, nil
}

// OpenSource opens a subscription.
//
// The subscription's name reaches the broker through the profile's
// [Profile.SourceAddress], which is what makes two consumers of one
// subscription compete and two consumers of different subscriptions each
// receive everything. No link is attached here: the first Fetch attaches
// one, so that a broker that is down at this instant is a retryable fetch
// failure rather than a consumer that could not be constructed.
func (b *Backend) OpenSource(ctx context.Context, cfg mq.SourceConfig) (mq.Source, error) {
	if err := b.CheckOpen(ctx); err != nil {
		return nil, err
	}
	s, err := b.newSource(cfg)
	if err != nil {
		return nil, err
	}
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		// Close takes no context, which is the decision: a prompt close is
		// not an operation a caller cancels, and Shutdown is the bounded form.
		_ = s.Close() //nolint:contextcheck // mq.Source.Close takes no context by design
		return nil, mq.ErrClosed
	}
	b.srcs[s] = struct{}{}
	b.mu.Unlock()
	return s, nil
}

// errNilDialer is [BackendWithDialer] with nothing to dial with.
var errNilDialer = errcode.New(errcode.InvalidArgument,
	"iqmq/amqp10: BackendWithDialer was given no dialler")
