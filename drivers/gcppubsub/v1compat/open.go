package v1compat

import (
	"context"

	//nolint:staticcheck // as in client.go: the deprecated v1 client library is what this package exists to wrap
	"cloud.google.com/go/pubsub"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	mq "bitbucket.org/iqhive/iqmq"
	"bitbucket.org/iqhive/iqmq/driverkit"
	gps "bitbucket.org/iqhive/iqmq/drivers/gcppubsub"
)

// Open builds a v1 Pub/Sub client and returns an iqmq client.
//
// The configuration is [bitbucket.org/iqhive/iqmq/drivers/gcppubsub.Config] and
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
func OpenWithClient(ctx context.Context, client *pubsub.Client, own driverkit.Ownership, cfg gps.Config) (*mq.Client, error) {
	b, err := gps.BackendWithAPI(ctx, Wrap(client), own, cfg)
	if err != nil {
		return nil, err
	}
	return gps.ClientFor(b, cfg), nil
}

// OpenBackend builds the [bitbucket.org/iqhive/iqmq.Backend] itself, for a caller
// composing the SPI directly.
func OpenBackend(ctx context.Context, cfg gps.Config) (*gps.Backend, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	client, err := pubsub.NewClient(ctx, cfg.ProjectID, clientOptions(cfg)...)
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
