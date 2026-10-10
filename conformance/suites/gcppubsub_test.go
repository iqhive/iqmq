//go:build integration

package suites

import (
	"os"
	"testing"
	"time"

	"go.uber.org/goleak"

	"github.com/iqhive/iqmq/conformance/harness"
	gps "github.com/iqhive/iqmq/drivers/gcppubsub"
	"github.com/iqhive/iqmq/testkit"
)

// TestConformance_gcppubsub runs the whole suite against the pinned
// Pub/Sub emulator and writes the report conformance/cmd/confgen turns into
// capabilities.json and docs/drivers/matrix.md.
//
// The emulator is EvidenceEmulator and never anything stronger. The specification layer 4
// is explicit that an emulator does not stand in for a managed service, and
// this emulator in particular does not implement everything: what it could not
// exercise is an `unverified` claim in the matrix with its reason on the driver
// page, which is the distinction the capability matrix exists to draw.
//
// The profile is the CLIENT GENERATION ("v2"), because the specification is a
// bake-off between two client libraries behind one capability table: a caller
// who read "gcppubsub" without knowing which library answered would not know
// which set of upstream bugs it has. The v1 adapter's own suite is not run
// here — the two arms answer one table, which
// drivers/gcppubsub/v1compat's TestBothArmsAnswerOneCapabilityTable holds them
// to — so a second 156-scenario run would publish the same evidence twice
// under a different key.
func TestConformance_gcppubsub(t *testing.T) {
	b := harness.PubSub(t)
	// goleak is on the DRIVER. A source runs the client library's streaming
	// pull and a delayed negative settlement runs a goroutine per held
	// delivery, so a Close that did not end either would leak across 150-odd
	// scenarios and be invisible in every other test.
	defer goleak.VerifyNone(t, conformanceLeakIgnores(
		// Both client generations' per-iterator stream keep-alive handler
		// exits on its own tickers rather than on the iterator's stop signal,
		// so it lingers for up to ten seconds after a source has closed. It
		// is a shutdown LAG in the library rather than a leak, and it is
		// recorded in the maintenance notes.
		goleak.IgnoreAnyFunction("cloud.google.com/go/pubsub/v2.(*messageIterator).streamKeepAliveHandler"),
	)...)
	project := b.Endpoint("project")
	if project == "" {
		project = harness.PubSubProject
	}
	cfg := gps.Config{
		ProjectID: project,
		Endpoint:  b.Addr,
		// The emulator speaks plaintext gRPC and checks no credentials, which
		// is what InsecureTransport is named for. A real endpoint must never
		// be given it, and Config.Validate refuses it without an endpoint.
		InsecureTransport: true,
		AutoCreate:        true,
		// Ten seconds is the service's own default and its documented
		// minimum. Scenarios that ask for less get less — the emulator grants
		// a one-second deadline where the managed service refuses it — and
		// the harness caps the client library's automatic extension to match,
		// because otherwise the library keeps the lease alive and no lease
		// ever expires.
		AckDeadline: 10 * time.Second,
	}
	h := gps.ConformanceHarness(cfg, testkit.EvidenceEmulator, testkit.Broker{
		Image: b.Image, Version: b.Version,
	})
	h.Commit = os.Getenv("IQMQ_COMMIT")
	rep := testkit.RunConformance(t, h)
	pass, fail, skip := rep.Counts()
	t.Logf("conformance %s: %d passed, %d failed, %d skipped, of %d scenarios",
		rep.Key(), pass, fail, skip, len(rep.Results))

	proven := 0
	for _, v := range rep.Verify() {
		if v.Failed {
			t.Errorf("capability %s (%s) has a failing scenario", v.Feature, v.Claimed)
		}
		if v.Proven() {
			proven++
			continue
		}
		// An unproven claim is not a failure — the evidence rule is that
		// it does not RENDER — but it is worth naming in the log, because the
		// generated matrix silently omits it and the driver page has to say
		// why.
		t.Logf("claim UNVERIFIED (positive %v, negative %v): %-22s %s",
			v.Positive, v.Negative, v.Feature, v.Claimed)
	}
	t.Logf("capability claims proved in both directions: %d of %d", proven, len(rep.Verify()))

	dir := os.Getenv("IQMQ_CONFORMANCE_REPORTS")
	if dir == "" {
		dir = os.Getenv("IQMQ_CONFORMANCE_REPORT_DIR")
	}
	if dir == "" {
		dir = t.TempDir()
	}
	path, err := rep.Save(dir)
	if err != nil {
		t.Fatalf("saving the report: %v", err)
	}
	t.Logf("report written to %s", path)
}
