package redis

import (
	"context"
	"net"
	"os"
	"strconv"

	"github.com/redis/rueidis"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit"
	"github.com/iqhive/iqmq/internal/streams"
)

// Open dials Redis and returns a client.
//
// It performs three pieces of I/O and creates no topology unless
// [Config.AutoCreate] says it may: the connection, the version probe (which
// decides three capability claims) and one SCRIPT LOAD per script. The
// configuration is validated before the dial, so a typo is an immediate
// refusal rather than a five-second timeout.
func Open(ctx context.Context, cfg Config) (*mq.Client, error) {
	b, err := OpenBackend(ctx, cfg)
	if err != nil {
		return nil, err
	}
	return clientFor(b, cfg), nil
}

// OpenWithClient wraps a rueidis.Client the caller already has.
//
// own says who closes it, and there is no default: a driver that always closed
// would tear down a client the application still uses, and one that never
// closed would leak the client it dialled itself. It is also the way in for
// anything a typed Config cannot carry — a Sentinel configuration, a custom
// dialler, client-side caching, an authentication callback: build the
// rueidis.Client however the deployment needs and hand it over.
func OpenWithClient(ctx context.Context, client rueidis.Client, own driverkit.Ownership, cfg Config) (*mq.Client, error) {
	b, err := BackendWithClient(ctx, client, own, cfg)
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

// OpenBackend dials Redis and returns the
// [github.com/iqhive/iqmq.Backend] itself, for a caller composing the SPI
// directly.
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
	client, err := rueidis.NewClient(clientOptions(resolved))
	if err != nil {
		b.health.Degraded(err)
		b.health.Close()
		return nil, b.errs.Map(mq.OpOpen, "", "", err)
	}
	cmder := &commander{client: client}
	if err := b.init(ctx, cmder, client, driverkit.ClientOwned); err != nil {
		client.Close()
		b.health.Degraded(err)
		b.health.Close()
		return nil, err
	}
	return b, nil
}

// BackendWithClient is [OpenWithClient] without the client wrapper.
func BackendWithClient(ctx context.Context, client rueidis.Client, own driverkit.Ownership, cfg Config) (*Backend, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if client == nil {
		return nil, errNilClient
	}
	if err := driverkit.CheckOwnership(own); err != nil {
		return nil, err
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	b := &Backend{cfg: cfg.resolve(), errs: newErrorMapper()}
	b.health = driverkit.NewHealth(b.onHealth)
	cmder := &commander{client: client}
	if err := b.init(ctx, cmder, client, own); err != nil {
		b.health.Degraded(err)
		b.health.Close()
		return nil, err
	}
	return b, nil
}

// BackendWithCommander is the seam a second client library plugs into, and the
// seam the driver's own tests use.
func BackendWithCommander(ctx context.Context, cmder streams.Commander, own driverkit.Ownership, cfg Config) (*Backend, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if cmder == nil {
		return nil, errNilClient
	}
	if err := driverkit.CheckOwnership(own); err != nil {
		return nil, err
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	b := &Backend{cfg: cfg.resolve(), errs: newErrorMapper()}
	b.health = driverkit.NewHealth(b.onHealth)
	if err := b.init(ctx, cmder, cmder, own); err != nil {
		b.health.Degraded(err)
		b.health.Close()
		return nil, err
	}
	return b, nil
}

// OpenWithCommander is [BackendWithCommander] wrapped in a client.
func OpenWithCommander(ctx context.Context, cmder streams.Commander, own driverkit.Ownership, cfg Config) (*mq.Client, error) {
	b, err := BackendWithCommander(ctx, cmder, own, cfg)
	if err != nil {
		return nil, err
	}
	return clientFor(b, cfg), nil
}

// ClientOptions turns the configuration into rueidis's options.
//
// It is exported for two callers and neither is a hack. The specification compares this client library against go-redis
// and has to measure it with the options this driver ships — client-side
// caching off, the dial timeout the configuration names — because an arm
// measured with different options measures a client nobody ships; and a caller
// building a rueidis client for [OpenWithClient] gets the driver's own
// starting point rather than guessing at it. `drivers/redis/goredis`'s
// `UniversalOptions` is the same door for the other library.
func ClientOptions(cfg Config) rueidis.ClientOption {
	return clientOptions(cfg.resolve())
}

// clientOptions turns the configuration into rueidis's options.
func clientOptions(cfg Config) rueidis.ClientOption {
	opt := rueidis.ClientOption{
		InitAddress:  cfg.Addrs,
		Username:     cfg.Username,
		Password:     cfg.Password.Reveal(),
		SelectDB:     cfg.DB,
		Dialer:       net.Dialer{Timeout: cfg.DialTimeout, KeepAlive: rueidis.DefaultTCPKeepAlive},
		DisableCache: true,
		// Client-side caching is off deliberately. It caches key VALUES, and
		// a stream is not read by value: every command this driver sends is
		// XADD, XREADGROUP, XACK or a script, none of which is cacheable.
		// Leaving it on would allocate a cache per connection for nothing.
	}
	if cfg.TLS != nil {
		opt.TLSConfig = cfg.TLS
	}
	return opt
}

// onHealth forwards a health transition to the configured hook.
//
// A driver has no access to the client library's own hooks, so this is the
// one place a caller learns that the link went away and came back — which is
// the same gap `drivers/nats` recorded: the SPI gives a
// backend no hook surface, so the reconnection event a driver produces has
// nowhere to go but the health state machine and a hook the CONFIG carried in.
func (b *Backend) onHealth(snap mq.HealthSnapshot) {
	if b.cfg.Hooks.OnReconnect == nil {
		return
	}
	if snap.State == mq.HealthReady {
		b.cfg.Hooks.OnReconnect(context.Background(), mq.ReconnectEvent{Driver: DriverName})
	}
}

// defaultConsumerPrefix is the first part of a consumer name when the
// configuration names none: the host name and this process's identifier.
//
// Both halves are there so that two processes on one host and two hosts in one
// deployment produce different names — which matters because the guarded
// acknowledgement compares consumer names, and two sources that shared one
// would each be able to acknowledge the other's deliveries.
func defaultConsumerPrefix() string {
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "iqmq"
	}
	return host + "-" + strconv.Itoa(os.Getpid())
}

// BackendWithClientFromAddr dials one address and returns the backend.
//
// It exists for the broker-version matrix, which needs the capability table
// and the server profile and nothing else: it starts a container, opens a
// backend, reads what the driver claims, and closes it. Going through
// [OpenBackend] would work and would make the matrix build a Config for every
// row; this is the one line it actually wants.
func BackendWithClientFromAddr(ctx context.Context, addr string, cfg Config) (*Backend, error) {
	cfg.Addrs = []string{addr}
	return OpenBackend(ctx, cfg)
}
