package jetstream

import (
	"context"

	natsgo "github.com/nats-io/nats.go"

	mq "bitbucket.org/iqhive/iqmq"
	"bitbucket.org/iqhive/iqmq/driverkit"
)

// Open dials NATS, binds to the stream and returns a client.
//
// It performs exactly two pieces of I/O: the connection, and one JetStream API
// call for the stream. It creates NO topology unless [Config.ManageStream] says
// to — the rule, and the reason this driver's `topology/no-hidden-creation`
// scenario is a pass rather than a skip: a publication to a subject the bound
// stream does not capture is refused with CodeNotFound.
//
// The configuration is validated before the dial, so a typo is an immediate
// refusal rather than a five-second timeout.
func Open(ctx context.Context, cfg Config) (*mq.Client, error) {
	b, err := OpenBackend(ctx, cfg)
	if err != nil {
		return nil, err
	}
	return clientFor(b, cfg), nil
}

// OpenWithClient wraps a *nats.Conn the caller already has.
//
// own says who closes it, and there is no default: a driver that always closed
// would tear down a connection the application still uses, and one that never
// closed would leak the connection it dialled itself. It is also the way in for
// an authentication scheme a typed Config cannot carry — NKey with a signing
// callback, a custom dialler, a websocket URL: build the *nats.Conn however the
// deployment needs and hand it over.
func OpenWithClient(ctx context.Context, nc *natsgo.Conn, own driverkit.Ownership, cfg Config) (*mq.Client, error) {
	b, err := BackendWithClient(ctx, nc, own, cfg)
	if err != nil {
		return nil, err
	}
	return clientFor(b, cfg), nil
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

// OpenBackend dials NATS and returns the [bitbucket.org/iqhive/iqmq.Backend]
// itself, for a caller composing the SPI directly.
func OpenBackend(ctx context.Context, cfg Config) (*Backend, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	resolved := cfg.resolve()
	b := &Backend{cfg: resolved, errs: newErrorMapper()}
	b.health = driverkit.NewHealth(b.onHealth)
	b.health.Connecting()
	nc, err := connectServers(resolved.Addrs, b.connectOptions(resolved))
	if err != nil {
		b.health.Degraded(err)
		b.health.Close()
		return nil, b.errs.Map(mq.OpOpen, "", "", err)
	}
	if err := b.init(ctx, nc, driverkit.ClientOwned); err != nil {
		b.health.Degraded(err)
		b.health.Close()
		nc.Close()
		return nil, err
	}
	return b, nil
}

// BackendWithClient is [OpenWithClient] without the client wrapper.
func BackendWithClient(ctx context.Context, nc *natsgo.Conn, own driverkit.Ownership, cfg Config) (*Backend, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if nc == nil {
		return nil, errNilConn
	}
	if err := driverkit.CheckOwnership(own); err != nil {
		return nil, err
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	b := &Backend{cfg: cfg.resolve(), errs: newErrorMapper()}
	b.health = driverkit.NewHealth(b.onHealth)
	if err := b.init(ctx, nc, own); err != nil {
		b.health.Degraded(err)
		b.health.Close()
		return nil, err
	}
	return b, nil
}

// connectOptions turns the configuration into the client library's options.
//
// The asynchronous handlers are the point for this driver: a slow
// consumer, a lost link and a reconnection are all reported here and nowhere
// else, so a driver that did not wire them would have a Health that never
// changed and a caller who found out from a log line.
func (b *Backend) connectOptions(cfg Config) []natsgo.Option {
	opts := []natsgo.Option{
		natsgo.Name(cfg.Name),
		natsgo.Timeout(cfg.ConnectTimeout),
		natsgo.ReconnectWait(cfg.ReconnectWait),
		natsgo.DrainTimeout(cfg.DrainTimeout),
		natsgo.ErrorHandler(b.onAsyncError),
		natsgo.DisconnectErrHandler(b.onDisconnect),
		natsgo.ReconnectHandler(b.onReconnect),
		natsgo.ClosedHandler(b.onClosed),
	}
	if cfg.MaxReconnects != 0 {
		opts = append(opts, natsgo.MaxReconnects(cfg.MaxReconnects))
	}
	if cfg.PingInterval > 0 {
		opts = append(opts, natsgo.PingInterval(cfg.PingInterval))
	}
	if cfg.MaxPingsOut > 0 {
		opts = append(opts, natsgo.MaxPingsOutstanding(cfg.MaxPingsOut))
	}
	if cfg.TLS != nil {
		opts = append(opts, natsgo.Secure(cfg.TLS))
	}
	switch {
	case cfg.CredentialsFile != "":
		opts = append(opts, natsgo.UserCredentials(cfg.CredentialsFile))
	case cfg.Token != "":
		opts = append(opts, natsgo.Token(cfg.Token.Reveal()))
	case cfg.Username != "":
		opts = append(opts, natsgo.UserInfo(cfg.Username, cfg.Password.Reveal()))
	}
	return opts
}

// connectServers dials the servers in addrs as a LIST.
//
// natsgo.Connect takes one string and splits it on ",", so an address holding a
// comma used to become two servers and the second was dialled: a typed
// "nats://nowhere.invalid,127.0.0.1:4222" connected to 127.0.0.1, and a DSN host
// that decoded to one did the same. This is what
// natsgo.Connect does after the split -- default options, the servers, then
// each option -- with the list handed over whole.
func connectServers(addrs []string, opts []natsgo.Option) (*natsgo.Conn, error) {
	o := natsgo.GetDefaultOptions()
	o.Servers = append([]string(nil), addrs...)
	for _, opt := range opts {
		if opt == nil {
			continue
		}
		if err := opt(&o); err != nil {
			return nil, err
		}
	}
	return o.Connect()
}
