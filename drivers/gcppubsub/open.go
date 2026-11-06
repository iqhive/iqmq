package gcppubsub

import (
	"context"
	"runtime"
	"sync"
	"weak"

	"cloud.google.com/go/pubsub/v2"
	pubsubpb "cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit"
	"github.com/iqhive/iqmq/drivers/gcppubsub/internal/psapi"
	"github.com/iqhive/iqmq/drivers/gcppubsub/internal/psv2"
	"github.com/iqhive/iqmq/internal/errcode"
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
//
// The client's connections stay as the caller configured them. A client Open
// dials carries an interceptor that reports a publication refused on a retry,
// after an attempt that may have been stored, as ambiguous; this one does not,
// so there such a refusal reads as definite (docs/drivers/gcppubsub.md,
// Publish path).
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
	// The client is this driver's own, so its Publish attempts are marked
	// ([retriedPublishes]).
	retried := newRetriedPublishes()
	client, err := pubsub.NewClient(ctx, resolved.ProjectID,
		append(clientOptions(resolved), option.WithGRPCDialOption(grpc.WithChainUnaryInterceptor(retried.intercept)))...)
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

// retriedPublishes marks, on a client this driver dials itself, the failure of
// a Publish RPC attempt that the client library made after an earlier attempt
// of the same call had failed.
//
// The library's publisher repeats a Publish RPC inside its own retry loop, on
// DEADLINE_EXCEEDED, UNAVAILABLE, INTERNAL, RESOURCE_EXHAUSTED, ABORTED,
// CANCELLED and UNKNOWN, and its result carries the last attempt's status
// alone. The service can have stored an attempt whose answer was lost and then
// refused the retry, because the topic was deleted, an IAM binding was removed
// or a credential lapsed in between. The publication rules read that refusal
// as definite while the message was on the topic, and a caller that repeated
// the definite failure published it twice. Every code the library repeats on
// is one the publication rules already call ambiguous, or a cancellation, so
// an attempt that failed and was repeated is one that may have been stored,
// and the failure of any later attempt is reported as a retriedFailure, which
// the mapping reads as ambiguous. A refusal of a first attempt stays definite.
//
// The driver cannot see the library's attempts through its API, and the
// library's own Publish call options replace any gRPC call option a client's
// configuration adds, so the attempts of one call are recognised by their
// request, which the library sends unchanged on every attempt. reqs holds a
// weak reference to each request one of whose attempts has failed, and a
// cleanup the runtime runs once the request is unreachable removes it, so a
// call whose last attempt failed and was not repeated leaves nothing behind.
//
// A client the caller passes to OpenWithClient is the caller's to configure,
// and is not marked.
type retriedPublishes struct {
	mu   sync.Mutex
	reqs map[weak.Pointer[pubsubpb.PublishRequest]]struct{}
}

// newRetriedPublishes returns an empty record.
func newRetriedPublishes() *retriedPublishes {
	return &retriedPublishes{reqs: map[weak.Pointer[pubsubpb.PublishRequest]]struct{}{}}
}

// intercept is the unary client interceptor on the connections of the client.
func (r *retriedPublishes) intercept(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
	err := invoker(ctx, method, req, reply, cc, opts...)
	in, ok := req.(*pubsubpb.PublishRequest)
	if !ok {
		return err
	}
	if err == nil && r.none() {
		return nil
	}
	key := weak.Make(in)
	r.mu.Lock()
	_, failedBefore := r.reqs[key]
	switch {
	case err == nil:
		delete(r.reqs, key)
	case !failedBefore:
		r.reqs[key] = struct{}{}
		runtime.AddCleanup(in, r.forget, key)
	}
	r.mu.Unlock()
	if err != nil && failedBefore {
		return &retriedFailure{err: err}
	}
	return err
}

// none reports whether no request is recorded, which is the case for every
// successful attempt while nothing is failing, so that it costs no weak
// reference.
func (r *retriedPublishes) none() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.reqs) == 0
}

// forget removes a request that is no longer reachable.
func (r *retriedPublishes) forget(key weak.Pointer[pubsubpb.PublishRequest]) {
	r.mu.Lock()
	delete(r.reqs, key)
	r.mu.Unlock()
}

// retriedFailure is the failure of a Publish RPC attempt the library made after
// an earlier attempt of the same call had failed, so after one the service may
// have stored. It keeps the attempt's gRPC status, so the library's retry
// decision and the rules' code see it as they did, and it reports
// errcode.Ambiguous, so the mapping marks the publication ambiguous.
type retriedFailure struct{ err error }

// Error is the attempt's own message.
func (e *retriedFailure) Error() string { return e.err.Error() }

// Unwrap returns the attempt's error.
func (e *retriedFailure) Unwrap() error { return e.err }

// GRPCStatus is the attempt's status, or nil when the attempt's error carries
// none: status.FromError then reads the mark exactly as it reads the attempt's
// own error (codes.Unknown, not a status), so the library's retry decision
// does not change. A made-up Unknown status would read as one, and the
// library repeats a Publish on UNKNOWN.
func (e *retriedFailure) GRPCStatus() *status.Status {
	if st, ok := status.FromError(e.err); ok {
		return st
	}
	return nil
}

// Code reports that the publication's outcome is unknown.
func (e *retriedFailure) Code() errcode.Code { return errcode.Ambiguous }
