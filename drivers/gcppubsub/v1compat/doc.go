// Package v1compat is the iqmq Pub/Sub driver over the DEPRECATED v1 client
// library, cloud.google.com/go/pubsub.
//
// It exists for two reasons and it is honest about both.
//
// # It is the second arm
//
// The specification is a bake-off between two CLIENT GENERATIONS:
// `cloud.google.com/go/pubsub/v2` and "a v1 compatibility adapter (while
// buildable)", decided on "correctness, maintenance, native comparison" with
// v2 as the expected default. A bake-off needs both arms behind one contract,
// and that contract is
// [github.com/iqhive/iqmq/drivers/gcppubsub/internal/psapi]: everything above
// it — the capability table, the metadata mapping, the completion pool, the
// push adapter, the settlement state machine, the admin surface — is
// [github.com/iqhive/iqmq/drivers/gcppubsub]'s and is shared, so the two arms
// differ by their client library and by nothing else. That is what makes the
// comparison a comparison of the libraries rather than of two drivers.
//
// # It is a migration path
//
// A deployment that already holds a *pubsub.Client from the v1 library can
// hand it to [OpenWithClient] and get an iqmq client with the same semantics
// and the same capability table as the v2 driver, without migrating its
// credential plumbing, its interceptors or its instrumentation first.
//
// # What it is not
//
// It is EXPERIMENTAL and it is not the default. The upstream module is marked
// deprecated ("use cloud.google.com/go/pubsub/v2 instead") and will eventually
// stop receiving fixes; when it stops building, this package goes rather than
// being carried. DriverInfo.Profile reports "v1compat", so a conformance
// report or a log line made through this package can never be mistaken for
// one made through v2.
//
// # What differs from v2, and what does not
//
// Nothing in the driver's behaviour differs. The v1 API's Topic and
// Subscription are the v2 API's Publisher and Subscriber under other names,
// its Message is the same type, and its administration is method calls on a
// Topic and a Subscription where v2 exposes the generated admin clients. The
// one real difference a caller can see is the shape of the native handle
// [github.com/iqhive/iqmq.Backend.Native] answers with: a v1 *pubsub.Client
// rather than a v2 one.
//
//	client, err := v1compat.Open(ctx, gcppubsub.Config{ProjectID: "my-project"})
package v1compat
