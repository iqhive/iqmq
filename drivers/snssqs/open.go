package snssqs

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awssns "github.com/aws/aws-sdk-go-v2/service/sns"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"

	mq "bitbucket.org/iqhive/iqmq"
	"bitbucket.org/iqhive/iqmq/driverkit"
	"bitbucket.org/iqhive/iqmq/drivers/snssqs/internal/snsapi"
	"bitbucket.org/iqhive/iqmq/drivers/sqs"
)

// Open builds the SNS and SQS clients and returns an iqmq client.
//
// It performs NO I/O at all, which is unusual and is the services' doing: both
// are request/response over HTTP, so there is no connection to establish and
// nothing to dial. The configuration is validated, the capability table is
// built, and the first request happens when a sink or a source is opened.
// [Backend.Ping] is what a caller reaches for when it wants to know the
// endpoints answer.
//
// No topology is created: the specification makes provisioning opt-in, and a driver that
// created a topic, a queue and a subscription for a name it could not resolve
// would turn a typo into three resources with a monthly bill.
// [Config.AutoCreate] is the opt-in.
func Open(ctx context.Context, cfg Config) (*mq.Client, error) {
	b, err := OpenBackend(ctx, cfg)
	if err != nil {
		return nil, err
	}
	return clientFor(b, cfg), nil
}

// OpenWithClients wraps clients the caller already has.
//
// There are TWO, because this driver speaks to two services, and `own` applies
// to both: a caller that built one client itself and borrowed the other has a
// configuration this signature cannot express, and the answer for it is
// [BackendWithAPI]. (An *sns.Client and an *sqs.Client each hold an HTTP
// client rather than a connection, so "closing" one is releasing the idle
// connections its transport holds.)
//
// It is also the way in for a credential scheme a typed Config cannot carry.
// The default chain — environment, instance role, SSO, web identity — lives in
// the SDK's config module, which pulls in the sts and sso service modules;
// this driver depends on neither, so a deployment that wants the chain calls
// config.LoadDefaultConfig itself and hands the clients over.
// [Config.Credentials] is the lighter half of the same escape hatch.
func OpenWithClients(ctx context.Context, snsClient *awssns.Client, sqsClient *awssqs.Client, own driverkit.Ownership, cfg Config) (*mq.Client, error) {
	b, err := BackendWithClients(ctx, snsClient, sqsClient, own, cfg)
	if err != nil {
		return nil, err
	}
	return clientFor(b, cfg), nil
}

// clientFor builds the client around a backend, carrying the configuration's
// clock and hooks.
//
// The hooks are wired HERE and nowhere else, which is why [Config.sqsConfig]
// leaves them off the inner driver: the inner backend is the SPI under this
// one, not a second client, and a second wiring would fire every observation
// point twice for every message.
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
	snsClient := awssns.New(snsOptions(resolved))
	sqsClient := awssqs.New(sqsOptions(resolved))
	return backendOver(ctx, snsClient, sqsClient, snsClient, driverkit.ClientOwned, resolved,
		closeIdle(snsClient.Options().HTTPClient, sqsClient.Options().HTTPClient),
		func() (*innerBackend, error) {
			return openInner(ctx, sqsClient, driverkit.ClientBorrowed, resolved)
		})
}

// BackendWithClients is [OpenWithClients] without the client wrapper.
func BackendWithClients(ctx context.Context, snsClient *awssns.Client, sqsClient *awssqs.Client, own driverkit.Ownership, cfg Config) (*Backend, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	switch {
	case snsClient == nil:
		return nil, errNilSNSClient
	case sqsClient == nil:
		return nil, errNilSQSClient
	}
	if err := driverkit.CheckOwnership(own); err != nil {
		return nil, err
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	resolved := cfg.resolve()
	return backendOver(ctx, snsClient, sqsClient, snsClient, own, resolved,
		closeIdle(snsClient.Options().HTTPClient, sqsClient.Options().HTTPClient),
		func() (*innerBackend, error) {
			// The inner driver BORROWS the SQS client whatever this
			// backend's ownership is: there is one client and one owner,
			// and letting both halves close it would close it twice.
			return openInner(ctx, sqsClient, driverkit.ClientBorrowed, resolved)
		})
}

// BackendWithAPI builds a backend over any implementations of the two API
// slices this driver uses, which is what its own tests drive and what a caller
// with a middleware stack of its own reaches for.
//
// It is exported for the reason `drivers/sqs`'s equivalent is: the scripted servers this driver's tests need are fixtures, and a
// driver whose tests can only reach them through an unexported field cannot be
// tested from another module — which `conformance/suites` is.
//
// `queueAPI` is what `drivers/sqs` is built over, and `queues` is the two SQS
// calls this driver makes itself. They are two parameters and not one because
// a test can then give the inner driver a faulty client and this one a healthy
// one; in every real deployment they are the same object. `native` is what
// [bitbucket.org/iqhive/iqmq.Backend.Native] answers, and may be nil.
func BackendWithAPI(ctx context.Context, snsAPI snsapi.SNS, queueAPI sqs.API, queues snsapi.Queues, native any, own driverkit.Ownership, cfg Config) (*Backend, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	switch {
	case snsAPI == nil:
		return nil, errNilSNSClient
	case queueAPI == nil, queues == nil:
		return nil, errNilSQSClient
	}
	if err := driverkit.CheckOwnership(own); err != nil {
		return nil, err
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	resolved := cfg.resolve()
	return backendOver(ctx, snsAPI, queues, native, own, resolved, nil,
		func() (*innerBackend, error) {
			return openInnerAPI(ctx, queueAPI, native, driverkit.ClientBorrowed, resolved)
		})
}

// backendOver is the one construction path.
func backendOver(
	ctx context.Context,
	sns snsapi.SNS, queues snsapi.Queues, native any,
	own driverkit.Ownership, cfg Config, closeNative func() error,
	inner func() (*innerBackend, error),
) (*Backend, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	b := &Backend{cfg: cfg, sns: sns, queues: queues, errs: newErrorMapper()}
	b.health = driverkit.NewHealth(nil)
	in, err := inner()
	if err != nil {
		b.health.Degraded(err)
		b.health.Close()
		return nil, err
	}
	b.inner = in
	if err := b.init(native, own, closeNative); err != nil {
		// The inner backend was built and this one was not, so it is the
		// only thing holding resources and it has to be released here:
		// BaseBackend.Close never ran, so teardown never will.
		_ = in.Close()
		b.health.Degraded(err)
		b.health.Close()
		return nil, err
	}
	return b, nil
}

// snsOptions turns the configuration into the SNS SDK's options.
//
// The credential resolution order is [Config.Credentials] first, then the
// static key pair, then anonymous — which is what an emulator that checks
// nothing accepts and what a real endpoint refuses immediately. There is no
// silent fall-through to the environment: a driver that picked up a stray
// AWS_SECRET_ACCESS_KEY would be talking to an account nobody named.
func snsOptions(cfg Config) awssns.Options {
	opts := awssns.Options{Region: cfg.Region, Credentials: credentialsFor(cfg)}
	if cfg.Endpoint != "" {
		opts.BaseEndpoint = aws.String(cfg.Endpoint)
	}
	return opts
}

// sqsOptions is [snsOptions] for the queue half. The two clients are built
// separately and from one configuration, so a deployment cannot end up signing
// for two accounts.
func sqsOptions(cfg Config) awssqs.Options {
	opts := awssqs.Options{Region: cfg.Region, Credentials: credentialsFor(cfg)}
	if cfg.Endpoint != "" {
		opts.BaseEndpoint = aws.String(cfg.Endpoint)
	}
	return opts
}

// credentialsFor is the credential provider both clients are built with.
func credentialsFor(cfg Config) aws.CredentialsProvider {
	switch {
	case cfg.Credentials != nil:
		return cfg.Credentials
	case cfg.AccessKeyID != "":
		return credentials.NewStaticCredentialsProvider(
			cfg.AccessKeyID, cfg.SecretAccessKey.Reveal(), cfg.SessionToken.Reveal(),
		)
	default:
		return aws.AnonymousCredentials{}
	}
}

// closeIdle releases the idle connections the owned clients' transports hold.
//
// An SDK client has no Close: it is a request builder over an http.Client, and
// what "closing" it means is letting go of the pooled connections. A transport
// that does not offer CloseIdleConnections is left alone rather than reached
// into.
func closeIdle(clients ...any) func() error {
	return func() error {
		type idleCloser interface{ CloseIdleConnections() }
		for _, c := range clients {
			if ic, ok := c.(idleCloser); ok {
				ic.CloseIdleConnections()
			}
		}
		return nil
	}
}
