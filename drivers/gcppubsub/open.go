package gcppubsub

import (
	"context"

	"cloud.google.com/go/pubsub/v2"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit"
	"github.com/iqhive/iqmq/drivers/gcppubsub/internal/psapi"
	"github.com/iqhive/iqmq/drivers/gcppubsub/internal/psv2"
)

// API is the slice of a Pub/Sub client library this driver is built over,
// exported so that a driver built on this one — `v1compat`, which is family
// the other arm — can hand a client of its own through
// [BackendWithAPI].
//
// It is an alias rather than a declaration because it has to be the same type
// [BackendWithAPI] takes: an internal package's name is not reachable from
// another module, and a value of type `any` is not assignable to an interface
// type.
type API = psapi.Client

// Open builds a Pub/Sub client and returns an iqmq client.
//
// It performs no I/O beyond what the client library's constructor does, which
// for an emulator endpoint is nothing and for the real service is credential
// discovery: a Pub/Sub publisher and subscriber are handles, and the first RPC
// is the first publication or the first streaming pull. [Backend.Ping] is what
// a caller reaches for when it wants to know the endpoint answers.
//
// No topology is created. The specification makes provisioning opt-in, and a driver that
// created a subscription for a topic it could not resolve would turn a typo
// into a backlog nobody reads and a bill nobody expects.
// [Config.AutoCreate] is the opt-in.
func Open(ctx context.Context, cfg Config) (*mq.Client, error) {
	b, err := OpenBackend(ctx, cfg)
	if err != nil {
		return nil, err
	}
	return clientFor(b, cfg), nil
}

// OpenWithClient wraps a *pubsub.Client the caller already has.
//
// own says who closes it, and there is no default: a driver that always closed
// would tear down a client the application still uses, and one that never
// closed would leak the one it built itself.
//
// It is also the way in for a credential scheme a typed Config cannot carry —
// workload identity federation, an impersonated service account, a token
// source of the deployment's own — which lives in modules this driver
// deliberately does not depend on.
func OpenWithClient(ctx context.Context, client *pubsub.Client, own driverkit.Ownership, cfg Config) (*mq.Client, error) {
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

// OpenBackend builds the [github.com/iqhive/iqmq.Backend] itself, for a caller
// composing the SPI directly.
func OpenBackend(ctx context.Context, cfg Config) (*Backend, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	resolved := cfg.resolve()
	client, err := pubsub.NewClient(ctx, resolved.ProjectID, clientOptions(resolved)...)
	if err != nil {
		return nil, newErrorMapper().Map(mq.OpOpen, "", "", err)
	}
	api := psv2.Wrap(client)
	b, berr := backendOver(api, driverkit.ClientOwned, resolved)
	if berr != nil {
		_ = client.Close()
		return nil, berr
	}
	return b, nil
}

// BackendWithClient is [OpenWithClient] without the client wrapper.
func BackendWithClient(ctx context.Context, client *pubsub.Client, own driverkit.Ownership, cfg Config) (*Backend, error) {
	if client == nil {
		return nil, errNilClient
	}
	return BackendWithAPI(ctx, psv2.Wrap(client), own, cfg)
}

// BackendWithAPI builds a backend over any implementation of the client slice
// this driver uses, which is what its own tests drive and what the specification's
// second arm is.
//
// It is exported because the alternative was an unexported hook the tests
// reached through a file in the driver's package, and a surface a test needs
// is a surface. The implementation's own
// [API.Native] is what [github.com/iqhive/iqmq.Backend.Native] answers.
func BackendWithAPI(ctx context.Context, api API, own driverkit.Ownership, cfg Config) (*Backend, error) {
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
	return backendOver(api, own, cfg.resolve())
}

// ClientFor builds an iqmq client around a backend built with
// [BackendWithAPI], which is what the second arm opens.
func ClientFor(b *Backend, cfg Config) *mq.Client { return clientFor(b, cfg) }

// backendOver is the one construction path.
func backendOver(api psapi.Client, own driverkit.Ownership, cfg Config) (*Backend, error) {
	b := &Backend{cfg: cfg, errs: newErrorMapper()}
	b.health = driverkit.NewHealth(nil)
	if err := b.init(api, own); err != nil {
		b.health.Degraded(err)
		b.health.Close()
		return nil, err
	}
	return b, nil
}

// clientOptions turns the configuration into the client library's options.
//
// The credential resolution order is CredentialsJSON, then CredentialsFile,
// then — only for an explicitly insecure endpoint — no credentials at all.
// There is no silent fall-through for a secure endpoint: the library's own
// application-default discovery is what a deployment with no explicit
// credential gets, which is the documented Google Cloud behaviour and is not
// something a driver should reimplement.
//
// InsecureTransport is what an emulator needs and what a real endpoint must
// never be given, which is why [Config.Validate] refuses it without an
// endpoint.
func clientOptions(cfg Config) []option.ClientOption {
	opts := make([]option.ClientOption, 0, 6)
	if cfg.Endpoint != "" {
		opts = append(opts, option.WithEndpoint(cfg.Endpoint))
	}
	switch {
	case cfg.CredentialsJSON != "":
		// WithAuthCredentialsJSON and not WithCredentialsJSON, and the
		// credential TYPE is pinned to a service-account key. The older
		// functions are deprecated for a security reason worth restating:
		// they accept any credential configuration, including an
		// externally-sourced one whose URLs decide where a token is fetched
		// from — so a configuration from an untrusted source could point the
		// client at somebody else's token endpoint. Naming the type refuses
		// everything but the key this driver's fields document, and
		// [OpenWithClient] is the way in for every other credential scheme.
		opts = append(opts, option.WithAuthCredentialsJSON(option.ServiceAccount, []byte(cfg.CredentialsJSON.Reveal())))
	case cfg.CredentialsFile != "":
		opts = append(opts, option.WithAuthCredentialsFile(option.ServiceAccount, cfg.CredentialsFile))
	case cfg.InsecureTransport:
		opts = append(opts,
			option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())),
			option.WithoutAuthentication(),
			option.WithTelemetryDisabled(),
		)
	}
	return opts
}
