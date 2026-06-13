package pulsar

import (
	"context"

	"github.com/apache/pulsar-client-go/pulsar"
	plog "github.com/apache/pulsar-client-go/pulsar/log"

	mq "bitbucket.org/iqhive/iqmq"
	"bitbucket.org/iqhive/iqmq/driverkit"
	"bitbucket.org/iqhive/iqmq/drivers/pulsar/internal/pulsarapi"
)

// Open connects to Pulsar and returns a client.
//
// It creates no topology. Pulsar's broker creates a topic on first use when the
// namespace allows it — `allowAutoTopicCreation`, which is a namespace policy
// and not a choice this driver makes — and [Config.EnsureTopology] is the
// deliberate creation, which is off by default. The rule is about a driver
// that could provision and chooses not to.
//
// The configuration is validated before anything is dialled, so a typo is an
// immediate refusal rather than a lookup timeout.
func Open(ctx context.Context, cfg Config) (*mq.Client, error) {
	b, err := OpenBackend(ctx, cfg)
	if err != nil {
		return nil, err
	}
	return clientFor(b, cfg), nil
}

// OpenBackend connects and returns the [bitbucket.org/iqhive/iqmq.Backend]
// itself, for a caller composing the SPI directly.
func OpenBackend(ctx context.Context, cfg Config) (*Backend, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	resolved := cfg.resolve()
	opts, err := clientOptions(resolved)
	if err != nil {
		return nil, err
	}
	cl, err := pulsar.NewClient(opts)
	if err != nil {
		return nil, newErrorMapper().Map(mq.OpOpen, "", "", err)
	}
	b, err := newBackend(ctx, resolved, cl, driverkit.ClientOwned, cl)
	if err != nil {
		cl.Close()
		return nil, err
	}
	return b, nil
}

// OpenWithClient wraps a client the caller already has.
//
// own says who closes it, and there is no default: a driver that always closed
// would tear down a client the application still uses, and one that never
// closed would leak the one it built itself.
func OpenWithClient(ctx context.Context, cl pulsarapi.Client, own driverkit.Ownership, cfg Config) (*mq.Client, error) {
	b, err := BackendWithClient(ctx, cl, own, cfg)
	if err != nil {
		return nil, err
	}
	return clientFor(b, cfg), nil
}

// BackendWithClient wraps a client the caller already has and returns the
// backend itself.
func BackendWithClient(ctx context.Context, cl pulsarapi.Client, own driverkit.Ownership, cfg Config) (*Backend, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if cl == nil {
		return nil, errNilClient
	}
	if err := driverkit.CheckOwnership(own); err != nil {
		return nil, err
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return newBackend(ctx, cfg.resolve(), cl, own, cl)
}

// clientOptions is the pulsar-client-go client configuration.
func clientOptions(cfg Config) (pulsar.ClientOptions, error) {
	o := pulsar.ClientOptions{
		URL:                        cfg.ServiceURL,
		ConnectionTimeout:          cfg.ConnectionTimeout,
		OperationTimeout:           cfg.OperationTimeout,
		KeepAliveInterval:          cfg.KeepAliveInterval,
		MaxConnectionsPerBroker:    cfg.MaxConnectionsPerBroker,
		MemoryLimitBytes:           cfg.MemoryLimitBytes,
		EnableTransaction:          cfg.EnableTransactions,
		ListenerName:               cfg.ListenerName,
		TLSConfig:                  cfg.TLS,
		TLSTrustCertsFilePath:      cfg.TLSTrustCertsFilePath,
		TLSAllowInsecureConnection: cfg.InsecureSkipVerify,
		// pulsar-client-go's default logger is logrus.StandardLogger(),
		// which writes every reconnection and every lookup to the
		// standard error of a process that never asked for it. A library
		// does not own its host's log stream, so the default here is to
		// discard and Config.Logger is how a caller opts in.
		Logger: plog.DefaultNopLogger(),
	}
	if cfg.Logger != nil {
		o.Logger = plog.NewLoggerWithSlog(cfg.Logger)
	}
	switch {
	case cfg.Token != "":
		o.Authentication = pulsar.NewAuthenticationToken(cfg.Token.Reveal())
	case cfg.AuthName != "":
		auth, err := pulsar.NewAuthentication(cfg.AuthName, cfg.AuthParams.Reveal())
		if err != nil {
			// The library's message can echo the parameters, which is
			// where a credential would be, so it is not wrapped.
			return pulsar.ClientOptions{}, errBadAuth
		}
		o.Authentication = auth
	}
	return o, nil
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
