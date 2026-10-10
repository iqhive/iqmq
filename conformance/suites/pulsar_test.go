//go:build integration

package suites

import (
	"os"
	"testing"
	"time"

	"go.uber.org/goleak"

	"github.com/iqhive/iqmq/conformance/harness"
	pd "github.com/iqhive/iqmq/drivers/pulsar"
	"github.com/iqhive/iqmq/dsn"
	"github.com/iqhive/iqmq/testkit"
)

// TestConformance_pulsar runs the suite against the pinned
// apachepulsar/pulsar image, once per subscription type the driver publishes
// evidence for.
//
// ONE container and TWO reports, which is the natural shape here and not a
// saving: the subscription type decides every ordering claim, so two
// subscription types are two capability tables — and a Shared subscription
// claims no ordering at all while a Key_Shared one claims per-key ordering
// conditionally. Neither report is the other's, and the matrix carries both
// (see pd.ConformanceProfiles for why these two and not all four).
func TestConformance_pulsar(t *testing.T) {
	// goleak is on the DRIVER: every source runs a lease sweeper of its own,
	// every sink waits for the asynchronous publications it admitted, and
	// pulsar-client-go runs a connection goroutine, a dispatch goroutine and
	// a partition-discovery ticker per handle. A Close that forgot to wake
	// its own sweeper, or one that returned before the native callbacks had
	// run, leaves a goroutine holding the caller's message.
	defer goleak.VerifyNone(t, conformanceLeakIgnores(
		// pulsar-client-go starts one failTimeoutMessages goroutine per
		// partition producer, and it exits only when its one-shot timer
		// next fires AND the producer is closing — so it outlives a
		// closed producer by up to one ProducerOptions.SendTimeout and
		// the library offers no way to wake it. Keeping SendTimeout is
		// what makes a publication bounded and its ambiguity
		// classifiable, so the goroutine is ignored rather than
		// the timeout removed; it is recorded in the maintenance notes
		// against pulsar-client-go.
		goleak.IgnoreAnyFunction("github.com/apache/pulsar-client-go/pulsar.(*partitionProducer).failTimeoutMessages"),
	)...)
	b := harness.Pulsar(t)
	// A run that pulls BY DIGEST, which is what a pinned run must do, has no
	// version in its reference at all: Version() strips the digest and finds
	// no tag. The harness now supplies the declared tag for the image it
	// resolved through images.lock itself , which is what
	// closes the half that was open here; this fallback
	// remains for the case the harness cannot answer — an IQMQ_PULSAR_IMAGE
	// override that is digest-only, whose version this repository does not
	// know — and the report has to carry a version either way.
	version := b.Version
	if version == "" || version == "latest" {
		version = harness.Version(harness.Images["pulsar"])
	}
	dir := os.Getenv("IQMQ_CONFORMANCE_REPORTS")
	if dir == "" {
		dir = os.Getenv("IQMQ_CONFORMANCE_REPORT_DIR")
	}
	if dir == "" {
		dir = t.TempDir()
	}
	for _, kind := range pd.ConformanceProfiles() {
		t.Run(kind.String(), func(t *testing.T) {
			cfg := pd.Config{
				ServiceURL: "pulsar://" + b.Addr,
				// Pulsar's topology API is HTTP on a different
				// port from the binary protocol, and the driver
				// does not guess it. It is also what the driver
				// reads the broker's own maxMessageSize and
				// acknowledgmentAtBatchIndexLevelEnabled from at
				// open, which is the one limit it can learn
				// rather than assume.
				AdminURL:         "http://" + b.Addrs["admin"],
				SubscriptionType: kind,
				// The standalone image runs without
				// authentication, and pulsar-client-go sends the
				// token in its CONNECT frame whether the broker
				// asks for one or not — so a configured token is
				// never validated here. It is configured anyway,
				// and that is the point: it is what makes
				// `secrets/redaction` RUN rather than skip, and
				// the scenario's question is whether any rendered
				// string or driver-built error carries it.
				Token: dsn.Secret("iqmq-pulsar-conformance-token-not-a-real-credential"),
				// Pulsar standalone is slow to start and slow to
				// create a topic the first time; the default
				// thirty seconds is not enough for a cold
				// namespace.
				OperationTimeout: 45 * time.Second,
			}
			h := pd.ConformanceHarness(cfg, testkit.EvidenceContainer, testkit.Broker{
				Image: b.Image, Version: version,
			})
			h.Commit = os.Getenv("IQMQ_COMMIT")
			rep := testkit.RunConformance(t, h)
			pass, fail, skip := rep.Counts()
			t.Logf("conformance %s: %d passed, %d failed, %d skipped, of %d scenarios",
				rep.Key(), pass, fail, skip, len(rep.Results))

			proven := 0
			for _, v := range rep.Verify() {
				switch {
				case v.Failed:
					t.Errorf("capability %s (%s) has a failing scenario", v.Feature, v.Claimed)
				case v.Proven():
					proven++
				case v.Unrefuted():
					// A true denial of a GUARANTEE that
					// nothing in this run broke. It is not
					// work outstanding: no number of runs
					// can observe a guarantee's absence, so
					// this is the state of the world and the
					// driver page says which claims are in
					// it.
					t.Logf("claim UNREFUTED: %-22s %s (a guarantee's absence is not observable in one run)",
						v.Feature, v.Claimed)
				default:
					t.Logf("claim UNVERIFIED (positive %v, negative %v): %-22s %s",
						v.Positive, v.Negative, v.Feature, v.Claimed)
				}
			}
			t.Logf("capability claims proved in both directions: %d of %d", proven, len(rep.Verify()))

			path, err := rep.Save(dir)
			if err != nil {
				t.Fatalf("saving the report: %v", err)
			}
			t.Logf("report written to %s", path)
		})
	}
}
