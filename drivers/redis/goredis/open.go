package goredis

import (
	"context"
	"net"
	"time"

	redis "github.com/redis/go-redis/v9"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit"
	rd "github.com/iqhive/iqmq/drivers/redis"
)

// Config is the driver's configuration, and it is the RUEIDIS driver's
// configuration: the same type, not a copy of it.
//
// Everything in it is a statement about Redis Streams or about iqmq -- how
// many shards a topic has, how long a lease is, which nack strategy is bound,
// where a rejection is routed -- and none of it is a statement about a client
// library. An alias is therefore not a shortcut: it is what makes
// [ParseDSN]'s answer, a `Validate()` refusal and a redacted `String()`
// literally the same behaviour under both clients, so that a deployment can
// change which library serves it without re-reading its configuration.
//
// The few settings that ARE about the library -- go-redis's pool size, its
// read and write timeouts, its retry policy -- are reached by building a
// [github.com/redis/go-redis/v9.UniversalClient] yourself and calling
// [OpenWithClient], which is the same door `drivers/redis` opens for rueidis.
type Config = rd.Config

// ParseDSN turns a Redis DSN into a [Config].
//
// It is `drivers/redis`'s parser, and this driver registers NO scheme of its
// own: a DSN naming "redis" has to mean one thing, and which client library
// serves it is a build-time choice a connection string has no business making.
// That is `drivers/redis`'s [github.com/iqhive/iqmq/drivers/redis.Scheme]
// documenting its own decision, and this is the other half of it.
func ParseDSN(s string) (Config, error) { return rd.ParseDSN(s) }

// Open dials Redis over go-redis and returns a client.
func Open(ctx context.Context, cfg Config) (*mq.Client, error) {
	b, err := OpenBackend(ctx, cfg)
	if err != nil {
		return nil, err
	}
	return clientFor(b, cfg), nil
}

// OpenBackend dials Redis over go-redis and returns the
// [github.com/iqhive/iqmq.Backend] itself, for a caller composing the SPI
// directly.
func OpenBackend(ctx context.Context, cfg Config) (*rd.Backend, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	// Validated before the dial, so a typo is an immediate refusal rather
	// than a five-second timeout. It is validated again inside
	// `drivers/redis`; doing it here too is what keeps a bad configuration
	// from opening a connection at all.
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	client := redis.NewUniversalClient(UniversalOptions(cfg))
	b, err := rd.BackendWithCommander(ctx, &commander{client: client, own: true}, driverkit.ClientOwned, cfg)
	if err != nil {
		_ = client.Close()
		return nil, err
	}
	return b, nil
}

// OpenWithClient wraps a go-redis client the caller already has.
//
// own says who closes it, and there is no default: a driver that always closed
// would tear down a client the application still uses, and one that never
// closed would leak the client it dialled itself. It is also the way in for
// anything a typed Config cannot carry -- a Sentinel or Ring configuration, a
// custom dialler, a pool size, an authentication callback, a hook: build the
// UniversalClient however the deployment needs and hand it over.
func OpenWithClient(ctx context.Context, client redis.UniversalClient, own driverkit.Ownership, cfg Config) (*mq.Client, error) {
	b, err := BackendWithClient(ctx, client, own, cfg)
	if err != nil {
		return nil, err
	}
	return clientFor(b, cfg), nil
}

// BackendWithClient is [OpenWithClient] without the client wrapper.
func BackendWithClient(ctx context.Context, client redis.UniversalClient, own driverkit.Ownership, cfg Config) (*rd.Backend, error) {
	if client == nil {
		return nil, errNilClient
	}
	if err := driverkit.CheckOwnership(own); err != nil {
		return nil, err
	}
	// The commander never closes a borrowed client, and `drivers/redis` is
	// told the same ownership so that its own Close does not either. Both
	// halves are needed: the backend closes the commander, and the commander
	// is what holds the client.
	return rd.BackendWithCommander(ctx, &commander{client: client, own: own == driverkit.ClientOwned}, own, cfg)
}

// clientFor builds the client around a backend, carrying the configuration's
// clock and hooks.
func clientFor(b *rd.Backend, cfg Config) *mq.Client {
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

// UniversalOptions turns the configuration into go-redis's options.
//
// Three of the fields are decisions rather than translations.
//
// MaxRetries is -1, which is go-redis's spelling of "do not retry". go-redis
// retries a command by default, and `shouldRetry` returns true for a write
// error and for a read timeout -- both of which happen AFTER the command may
// have reached the server. On this broker a retried XADD is a second copy of
// the message, presented to the caller as one successful publication, which is the duplicate-reported-as-success and the disqualifier. The driver
// reports the ambiguity instead: `ErrorMapper.MapNet` classifies a transport
// failure after a write as Ambiguous, and the retry policy is the CORE,
// applied where a caller can see it. rueidis behaves this way already, which
// is the other reason: the two clients must not differ in whether a
// publication can silently duplicate.
//
// PoolSize is left to go-redis. It is the family- difference itself --
// rueidis multiplexes on one connection and go-redis takes one per command --
// and pinning it would measure a pool this driver invented rather than the
// library's own answer.
//
// ReadTimeout has to clear the BLOCK, and this is the one setting whose
// default is WRONG for this driver rather than merely different.
//
// `XREADGROUP … BLOCK n` parks at the SERVER for up to n, so the socket read
// that waits for its reply legitimately takes that long. go-redis's default
// read timeout is five seconds and [github.com/iqhive/iqmq/drivers/redis.DefaultBlock]
// is five seconds, so the two raced: every idle fetch came back as
// `DeadlineExceeded: read tcp …: i/o timeout` instead of as "nothing
// available", and the scenarios that wait for a redelivery
// (nack/immediate-redelivery, capability/AtLeastOnce, capability/DelayedRedelivery)
// failed against a real container while every unit test passed, because the
// in-process server answers instantly.
//
// [github.com/iqhive/iqmq/internal/streams.Cmd] carries a `Blocking` flag for
// exactly this, and rueidis honours it by routing the command to a connection
// pool of its own. go-redis's per-command read timeout is not reachable from
// outside the package — `setReadTimeout` is unexported and `Cmder`'s
// `readTimeout()` with it — so the only lever is the connection's, and the
// bound it is given is the longest legitimate block plus the dial timeout as
// slack. The cost is that a non-blocking command's safety net is that long
// too; the alternative, `ReadTimeout: -1`, removes the net altogether and a
// dead connection would then hang a fetch for ever.
//
// Protocol is left to go-redis too, which is RESP3. Both clients default to
// RESP3 and both are exercised against a RESP2 server by
// `internal/streams/respsrv`'s own tests.
func UniversalOptions(cfg Config) *redis.UniversalOptions {
	resolved := cfg
	if len(resolved.Addrs) == 0 {
		resolved.Addrs = []string{rd.DefaultAddr}
	}
	dial := resolved.DialTimeout
	if dial <= 0 {
		dial = rd.DefaultDialTimeout
	}
	block := resolved.Block
	if block <= 0 {
		block = rd.DefaultBlock
	}
	opt := &redis.UniversalOptions{
		Addrs:        resolved.Addrs,
		Username:     resolved.Username,
		Password:     resolved.Password.Reveal(),
		DB:           resolved.DB,
		DialTimeout:  dial,
		ReadTimeout:  block + dial,
		WriteTimeout: dial,
		MaxRetries:   -1,
		Dialer: func(ctx context.Context, network, addr string) (net.Conn, error) {
			d := net.Dialer{Timeout: dial, KeepAlive: 30 * time.Second}
			return d.DialContext(ctx, network, addr)
		},
	}
	if resolved.TLS != nil {
		opt.TLSConfig = resolved.TLS
	}
	return opt
}
