package sqs

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"

	mq "bitbucket.org/iqhive/iqmq"
	"bitbucket.org/iqhive/iqmq/driverkit"
	"bitbucket.org/iqhive/iqmq/drivers/sqs/internal/sqsapi"
)

// Open builds an SQS client and returns an iqmq client.
//
// It performs NO I/O at all, which is unusual and is the service's doing:
// SQS is request/response over HTTP, so there is no connection to establish
// and nothing to dial. The configuration is validated, the capability table is
// built, and the first request happens when a sink or a source is opened.
// [Backend.Ping] is what a caller reaches for when it wants to know the
// endpoint answers.
//
// No topology is created: the specification makes provisioning opt-in, and a driver that
// created a queue for a topic it could not resolve would turn a typo into a
// new queue with a monthly bill. [Config.AutoCreate] is the opt-in.
func Open(ctx context.Context, cfg Config) (*mq.Client, error) {
	b, err := OpenBackend(ctx, cfg)
	if err != nil {
		return nil, err
	}
	return clientFor(b, cfg), nil
}

// OpenWithClient wraps a *sqs.Client the caller already has.
//
// own says who closes it, and there is no default: a driver that always closed
// would tear down a client the application still uses, and one that never
// closed would leak the one it built itself. (An *sqs.Client holds an HTTP
// client rather than a connection, so "closing" it is releasing the idle
// connections its transport holds.)
//
// It is also the way in for a credential scheme a typed Config cannot carry.
// The default chain — environment, instance role, SSO, web identity — lives in
// the SDK's config module, which pulls in the sts and sso service modules;
// this driver depends on neither, so a deployment that wants the chain calls
// config.LoadDefaultConfig itself and hands the client over. [Config.
// Credentials] is the lighter half of the same escape hatch.
func OpenWithClient(ctx context.Context, client *awssqs.Client, own driverkit.Ownership, cfg Config) (*mq.Client, error) {
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

// OpenBackend builds the [bitbucket.org/iqhive/iqmq.Backend] itself, for a caller
// composing the SPI directly.
func OpenBackend(ctx context.Context, cfg Config) (*Backend, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	resolved := cfg.resolve()
	client := awssqs.New(clientOptions(resolved))
	return backendOver(client, client, driverkit.ClientOwned, resolved, closeIdle(client))
}

// BackendWithClient is [OpenWithClient] without the client wrapper.
func BackendWithClient(ctx context.Context, client *awssqs.Client, own driverkit.Ownership, cfg Config) (*Backend, error) {
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
	return backendOver(client, client, own, cfg.resolve(), closeIdle(client))
}

// BackendWithAPI builds a backend over any implementation of the API slice
// this driver uses, which is what its own tests drive and what a caller with a
// middleware stack of its own reaches for.
//
// It is exported because the alternative was an unexported hook the tests
// reached through a file in the driver's package, and a surface a test needs
// is a surface. `native` is what
// [bitbucket.org/iqhive/iqmq.Backend.Native] answers, and may be nil.
func BackendWithAPI(ctx context.Context, api sqsapi.API, native any, own driverkit.Ownership, cfg Config) (*Backend, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if api == nil {
		return nil, errNilClient
	}
	if err := driverkit.CheckOwnership(own); err != nil {
		return nil, err
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return backendOver(api, native, own, cfg.resolve(), nil)
}

// backendOver is the one construction path.
func backendOver(api sqsapi.API, native any, own driverkit.Ownership, cfg Config, closeNative func() error) (*Backend, error) {
	b := &Backend{cfg: cfg, errs: newErrorMapper()}
	b.health = driverkit.NewHealth(nil)
	if err := b.init(api, native, own, closeNative); err != nil {
		b.health.Degraded(err)
		b.health.Close()
		return nil, err
	}
	return b, nil
}

// clientOptions turns the configuration into the SDK's options.
//
// The credential resolution order is [Config.Credentials] first, then the
// static key pair, then anonymous — which is what an emulator that checks
// nothing accepts and what a real endpoint refuses immediately. There is no
// silent fall-through to the environment: a driver that picked up a stray
// AWS_SECRET_ACCESS_KEY would be talking to an account nobody named.
func clientOptions(cfg Config) awssqs.Options {
	opts := awssqs.Options{Region: cfg.Region}
	switch {
	case cfg.Credentials != nil:
		opts.Credentials = cfg.Credentials
	case cfg.AccessKeyID != "":
		opts.Credentials = credentials.NewStaticCredentialsProvider(
			cfg.AccessKeyID, cfg.SecretAccessKey.Reveal(), cfg.SessionToken.Reveal(),
		)
	default:
		opts.Credentials = aws.AnonymousCredentials{}
	}
	if cfg.Endpoint != "" {
		opts.BaseEndpoint = aws.String(cfg.Endpoint)
	}
	return opts
}

// closeIdle releases the idle connections an owned client's transport holds.
//
// An *sqs.Client has no Close: it is a request builder over an
// http.Client, and what "closing" it means is letting go of the pooled
// connections. A transport that does not offer CloseIdleConnections is left
// alone rather than reached into.
func closeIdle(client *awssqs.Client) func() error {
	return func() error {
		type idleCloser interface{ CloseIdleConnections() }
		if c, ok := client.Options().HTTPClient.(idleCloser); ok {
			c.CloseIdleConnections()
		}
		return nil
	}
}
