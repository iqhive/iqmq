package v1compat_test

import (
	"context"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	//nolint:staticcheck // the second arm is the deprecated v1 client library, which is the whole point of the comparison
	v1 "cloud.google.com/go/pubsub"
	v2 "cloud.google.com/go/pubsub/v2"
	"cloud.google.com/go/pubsub/v2/pstest"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit"
	gps "github.com/iqhive/iqmq/drivers/gcppubsub"
	"github.com/iqhive/iqmq/drivers/gcppubsub/v1compat"
)

// The Pub/Sub client-generation bake-off.
//
// # Why it lives here
//
// the arms are two CLIENT LIBRARIES in two modules, which is the one shape
// could not express: it runs one `go test` in one package,
// and a bake-off needs both arms in one benchmark so that the campaign's
// per-count interleaving measures them adjacently (step 2). The two arms
// can only be in one package if that package imports both, and the only
// package in this repository that does is THIS one's test binary — the
// v1compat driver's own tests import drivers/gcppubsub for the shared
// semantics and for the cross-generation equivalence suite.
//
// # What the broker is, and what that costs the result
//
// `pstest`, the in-process fake service that ships with
// cloud.google.com/go/pubsub/v2 — not the emulator container the evidence
// column would suggest, and that is a declared gap rather than a substitution
// (`CRITERION_GAP` in.env). Three reasons it is the right service for THIS
// measurement and one reason it is not the whole of the evidence:
//
//   - the question is what the CLIENT costs. A container's service — its
//     gRPC stack, its storage, its own CPU — is common-mode cost that both
//     arms pay identically, so it adds variance to the difference without
//     adding information about it.
//   - The campaign runs inside `make ci`'s ceilings and on a shared host:
//     `make bench-lint` lists every family's rows at `-benchtime=1x` on every
//     CI run, and a family whose rows need Docker would either start
//     containers in every agent's `make ci` or select no benchmark at all and
//     fail the lint.
//   - pstest is a real Pub/Sub gRPC server on a real socket, and BOTH arms
//     speak to the same instance of it over the same connection.
//   - What it does NOT measure is what the specification also asks for: "correctness" and
//     "maintenance". Those are declared as the criterion's gap and named with
//     the evidence that does establish them — the cross-generation
//     equivalence suite and TestModel in this package for correctness, and
//     the upstream module's own deprecation notice for maintenance.
//
// # How the two arms are configured
//
// Identically, field for field, through one
// [github.com/iqhive/iqmq/drivers/gcppubsub.Config]: that is the point of the
// arm. The bundler is pinned to one message and a millisecond for the
// per-message leaves, so a publication's confirmation boundary is the
// service's answer rather than the bundler's timer, and to a hundred messages
// for the batch leaf.

// a34Arm is one arm of A client generation behind the driver
// contract.
type a34Arm struct {
	slug  string
	ident string
	open  func(tb testing.TB, conn *grpc.ClientConn, cfg gps.Config) *mq.Client
}

// a34Arms are the family's kinds, in declaration order. v2 is the reference:
// The specification expects it to be the default.
func a34Arms() []a34Arm {
	return []a34Arm{
		{slug: "v2", ident: "V2", open: openV2Arm},
		{slug: "v1compat", ident: "V1Compat", open: openV1Arm},
	}
}

// openV2Arm opens the driver over cloud.google.com/go/pubsub/v2.
func openV2Arm(tb testing.TB, conn *grpc.ClientConn, cfg gps.Config) *mq.Client {
	tb.Helper()
	native, err := v2.NewClient(context.Background(), cfg.ProjectID, option.WithGRPCConn(conn))
	if err != nil {
		tb.Fatalf("opening the v2 arm: %v", err)
	}
	c, err := gps.OpenWithClient(context.Background(), native, driverkit.ClientBorrowed, cfg)
	if err != nil {
		tb.Fatalf("opening the v2 arm: %v", err)
	}
	// The native client is NOT closed here, which is what ClientBorrowed
	// means and what the shared connection requires: a *pubsub.Client's Close
	// closes the admin clients built over the connection, and the connection
	// is the OTHER arm's too. Closing it after the first arm finished is what
	// made every v1compat row fail with "the client connection is closing".
	// The connection is released once, by [service]'s cleanup.
	tb.Cleanup(func() { _ = c.Close() })
	return c
}

// openV1Arm opens the driver over the deprecated cloud.google.com/go/pubsub.
func openV1Arm(tb testing.TB, conn *grpc.ClientConn, cfg gps.Config) *mq.Client {
	tb.Helper()
	native, err := v1.NewClient(context.Background(), cfg.ProjectID, option.WithGRPCConn(conn))
	if err != nil {
		tb.Fatalf("opening the v1compat arm: %v", err)
	}
	c, err := v1compat.OpenWithClient(context.Background(), native, driverkit.ClientBorrowed, cfg)
	if err != nil {
		tb.Fatalf("opening the v1compat arm: %v", err)
	}
	// As in the v2 arm: the connection is shared, so the native client is
	// borrowed and not closed here.
	tb.Cleanup(func() { _ = c.Close() })
	return c
}

// a34Project is the project both arms are opened against.
const a34Project = "iqmq-a34"

// service starts one pstest server and one connection to it, which is what
// both arms share.
func service(tb testing.TB) *grpc.ClientConn {
	tb.Helper()
	srv := pstest.NewServer()
	conn, err := grpc.NewClient(srv.Addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		_ = srv.Close()
		tb.Fatalf("dialling pstest: %v", err)
	}
	tb.Cleanup(func() {
		_ = conn.Close()
		_ = srv.Close()
	})
	return conn
}

// armConfig is the configuration both arms are opened with.
func armConfig(batch int) gps.Config {
	cfg := gps.Config{
		ProjectID:             a34Project,
		AutoCreate:            true,
		PublishCountThreshold: batch,
		PublishDelayThreshold: time.Millisecond,
		AckDeadline:           10 * time.Second,
		Prefetch:              1024,
	}
	return cfg
}

// names are unique per run: pstest keeps everything in one process, so a topic
// reused across leaves would carry the previous leaf's backlog.
var nameSeq atomic.Uint64

// newName returns a fresh resource name.
func newName(purpose string) string {
	return "a34-" + purpose + "-" + strconv.FormatUint(nameSeq.Add(1), 36)
}

// provision creates a topic and a subscription through the portable admin.
func provision(tb testing.TB, c *mq.Client, topic, sub string) {
	tb.Helper()
	admin, err := c.Admin()
	if err != nil {
		tb.Fatalf("Admin: %v", err)
	}
	ctx := context.Background()
	if err := admin.EnsureTopic(ctx, mq.TopicSpec{Name: topic}); err != nil {
		tb.Fatalf("EnsureTopic(%s): %v", topic, err)
	}
	if err := admin.EnsureSubscription(ctx, mq.SubscriptionSpec{Topic: topic, Name: sub}); err != nil {
		tb.Fatalf("EnsureSubscription(%s/%s): %v", topic, sub, err)
	}
}
