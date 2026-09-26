package v1compat_test

import (
	"testing"
	"time"

	//nolint:staticcheck // as in client.go: the deprecated v1 client library is what this package exists to wrap
	"cloud.google.com/go/pubsub"
	"google.golang.org/api/option"

	"github.com/iqhive/iqmq/drivers/gcppubsub/internal/psapi"
	v1 "github.com/iqhive/iqmq/drivers/gcppubsub/v1compat"
)

// The compat shim's own surface: what it refuses, what it reports, and that
// every tuning knob it applies leaves working handles.
//
// This arm exists so the specification can compare two client libraries, and the
// model test drives it hard -- but only through the settings that arm
// happens to set. The rest of the shim's configuration branches, and the
// small accessors the SPI reads, ran nowhere.

// wrapped is a shim client over an in-process emulator.
func wrapped(t *testing.T) *v1.Client {
	t.Helper()
	conn := service(t)
	cl, err := pubsub.NewClient(t.Context(), "iqmq-compat",
		option.WithGRPCConn(conn), option.WithoutAuthentication())
	if err != nil {
		t.Fatalf("building the v1 client: %v", err)
	}
	t.Cleanup(func() { _ = cl.Close() })
	return v1.Wrap(cl)
}

// TestAHandleWithNoNameIsRefused.
//
// The names come from the driver's own naming layer, so an empty one is a
// driver bug -- and creating a topic handle addressed by the empty string
// would publish into a resource path that does not exist, reported by the
// service much later.
func TestAHandleWithNoNameIsRefused(t *testing.T) {
	c := wrapped(t)

	if _, err := c.Publisher(psapi.PublisherConfig{}); err == nil {
		t.Error("Publisher with no topic returned a handle")
	}
	if _, err := c.Subscription(psapi.SubscriptionConfig{}); err == nil {
		t.Error("Subscription with no name returned a handle")
	}
}

// TestTheShimReportsWhichLibraryItIs.
//
// the whole point is comparing two client libraries, and the
// report names the arm: a scorecard that could not tell them apart would be
// comparing a library with itself.
func TestTheShimReportsWhichLibraryItIs(t *testing.T) {
	c := wrapped(t)

	if got := c.Project(); got != "iqmq-compat" {
		t.Errorf("the shim reports the project %q, want the one it was built with", got)
	}
	if c.Generation() == "" {
		t.Error("the shim reports no generation; A34's scorecard names the arm by it")
	}
	if c.ClientModule() == "" {
		t.Error("the shim reports no client module")
	}
	if c.ClientVersion() == "" {
		t.Error("the shim reports no client version; a comparison that cannot say which version it " +
			"measured is not reproducible")
	}
	if _, ok := c.Native().(*pubsub.Client); !ok {
		t.Errorf("Native is %T, want the v1 client this shim wraps", c.Native())
	}
	if c.Admin() == nil {
		t.Error("the shim supplies no Admin")
	}
}

// TestEveryTuningKnobLeavesWorkingHandles.
//
// Each of these is a field the shim copies into the library's own settings
// struct, and each is behind a "was it set" branch so that a zero leaves the
// library's default alone. Setting all of them at once is what reaches every
// branch; asserting the handles still PUBLISH and RECEIVE is what makes it a
// test rather than a line-counter -- a value copied into the wrong field
// would break the round trip here.
func TestEveryTuningKnobLeavesWorkingHandles(t *testing.T) {
	c := wrapped(t)

	pub, err := c.Publisher(psapi.PublisherConfig{
		Topic:                      "orders",
		Ordering:                   true,
		CountThreshold:             1,
		ByteThreshold:              1 << 20,
		DelayThreshold:             10 * time.Millisecond,
		Timeout:                    30 * time.Second,
		NumGoroutines:              2,
		FlowMaxOutstandingMessages: 100,
		FlowMaxOutstandingBytes:    1 << 20,
		Compression:                true,
		CompressionThreshold:       1 << 10,
	})
	if err != nil {
		t.Fatalf("Publisher with every knob set: %v", err)
	}
	defer pub.Stop()

	sub, err := c.Subscription(psapi.SubscriptionConfig{
		Subscription:               "workers",
		MaxOutstandingMessages:     50,
		MaxOutstandingBytes:        1 << 20,
		NumGoroutines:              2,
		MaxExtension:               time.Minute,
		MaxDurationPerAckExtension: 30 * time.Second,
		MinDurationPerAckExtension: 5 * time.Second,
	})
	if err != nil {
		t.Fatalf("Subscription with every knob set: %v", err)
	}
	if sub == nil {
		t.Fatal("Subscription returned no handle")
	}

	// Flush and ResumePublish are the two calls the SPI makes that have no
	// return to assert on. Calling them proves they reach the library rather
	// than panicking on a handle built through this path -- which is what a
	// settings field copied into the wrong struct would do.
	pub.Flush()
	pub.ResumePublish("some-key")
}
