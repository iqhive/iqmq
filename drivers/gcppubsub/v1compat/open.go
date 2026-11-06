package v1compat

import (
	"context"
	"runtime"
	"sync"
	"weak"

	//nolint:staticcheck // as in client.go: the deprecated v1 client library is what this package exists to wrap
	"cloud.google.com/go/pubsub"
	pubsubpb "cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit"
	gps "github.com/iqhive/iqmq/drivers/gcppubsub"
	"github.com/iqhive/iqmq/internal/errcode"
)

// Open builds a v1 Pub/Sub client and returns an iqmq client.
//
// The configuration is [github.com/iqhive/iqmq/drivers/gcppubsub.Config] and
// not a type of this package's own, which is the whole point of the arm: the
// two client generations are configured identically, so a deployment moving
// between them changes an import and nothing else.
func Open(ctx context.Context, cfg gps.Config) (*mq.Client, error) {
	b, err := OpenBackend(ctx, cfg)
	if err != nil {
		return nil, err
	}
	return gps.ClientFor(b, cfg), nil
}

// OpenWithClient wraps a *pubsub.Client the caller already has.
//
// own says who closes it, and there is no default: a driver that always closed
// would tear down a client the application still uses, and one that never
// closed would leak the one it built itself.
//
// This is the migration path this package exists for: a deployment that
// already holds a v1 client — with its credential plumbing, its interceptors
// and its instrumentation — gets an iqmq client with the same semantics and
// the same capability table as the v2 driver without moving any of that first.
//
// The client's connections stay as the caller configured them. A client Open
// dials carries an interceptor that reports a publication refused on a retry,
// after an attempt that may have been stored, as ambiguous; this one does not,
// so there such a refusal reads as definite (docs/drivers/gcppubsub.md,
// Publish path).
func OpenWithClient(ctx context.Context, client *pubsub.Client, own driverkit.Ownership, cfg gps.Config) (*mq.Client, error) {
	b, err := gps.BackendWithAPI(ctx, Wrap(client), own, cfg)
	if err != nil {
		return nil, err
	}
	return gps.ClientFor(b, cfg), nil
}

// OpenBackend builds the [github.com/iqhive/iqmq.Backend] itself, for a caller
// composing the SPI directly.
func OpenBackend(ctx context.Context, cfg gps.Config) (*gps.Backend, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	// The client is this arm's own, so its Publish attempts are marked
	// ([retriedPublishes]).
	retried := newRetriedPublishes()
	client, err := pubsub.NewClient(ctx, cfg.ProjectID,
		append(clientOptions(cfg), option.WithGRPCDialOption(grpc.WithChainUnaryInterceptor(retried.intercept)))...)
	if err != nil {
		return nil, err
	}
	b, berr := gps.BackendWithAPI(ctx, Wrap(client), driverkit.ClientOwned, cfg)
	if berr != nil {
		_ = client.Close()
		return nil, berr
	}
	return b, nil
}

// OpenDSN opens a client from a Pub/Sub DSN, through the v1 library.
//
// The DSN grammar is the v2 driver's, parsed by its own ParseDSN: two DSN
// dialects for one service would be two places for a parameter to mean
// something different.
func OpenDSN(ctx context.Context, s string) (*mq.Client, error) {
	cfg, err := gps.ParseDSN(s)
	if err != nil {
		return nil, err
	}
	return Open(ctx, cfg)
}

// clientOptions turns the configuration into the client library's options. It
// is the v2 driver's resolution order, applied to the v1 constructor.
func clientOptions(cfg gps.Config) []option.ClientOption {
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

// retriedPublishes marks, on a client this arm dials itself, the failure of a
// Publish RPC attempt that the v1 client library made after an earlier attempt
// of the same call had failed, which the mapping then reads as ambiguous: the
// earlier attempt may have been stored. It is drivers/gcppubsub's, copied, and
// that type's comment has the reasons. The v1 library's PublishRequest is the
// v2 one under an alias, and it too sends one request unchanged on every
// attempt. A client passed to OpenWithClient is the caller's, and is not
// marked.
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

// retriedFailure is the failure of a Publish RPC attempt made after a failed
// one. It keeps the attempt's gRPC status and reports errcode.Ambiguous.
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
