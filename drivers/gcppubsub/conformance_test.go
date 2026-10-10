package gcppubsub_test

import (
	"os"
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
	gps "github.com/iqhive/iqmq/drivers/gcppubsub"
	"github.com/iqhive/iqmq/testkit"
)

// TestConformance runs the suite from THIS module, against an endpoint
// named by IQMQ_PUBSUB_ADDR.
//
// The shipped evidence is `conformance/suites/gcppubsub_test.go`, which starts
// the pinned emulator container and writes the report the capability matrix is
// built from. This is the same harness reached from the driver's own module, so
// that somebody with an endpoint — their own emulator, or a real project —
// can run the suite without the conformance module, its testcontainers
// dependency or Docker.
//
//	IQMQ_PUBSUB_ADDR=127.0.0.1:8085 IQMQ_PUBSUB_PROJECT=my-project \
//	  go test -run TestConformance ./drivers/gcppubsub
//
// It is env-gated and skips by default, which is the shape for a
// driver's own conformance test: `make test-race` runs this package on every CI
// run and must not need a broker.
func TestConformance(t *testing.T) {
	addr := os.Getenv("IQMQ_PUBSUB_ADDR")
	if addr == "" {
		t.Skip("IQMQ_PUBSUB_ADDR is not set; conformance/suites/gcppubsub_test.go is the shipped run, against the pinned emulator")
	}
	project := os.Getenv("IQMQ_PUBSUB_PROJECT")
	if project == "" {
		project = "iqmq-test"
	}
	// The evidence is what the endpoint IS, and it decides more than the
	// report's label: `limits/advertised` fails a driver that advertises
	// nothing against container or managed evidence, and this driver's
	// emulator-only skips are keyed on it. A run against
	// a real project sets IQMQ_PUBSUB_MANAGED and gets the stricter reading.
	ev := testkit.EvidenceEmulator
	if os.Getenv("IQMQ_PUBSUB_MANAGED") != "" {
		ev = testkit.EvidenceManaged
	}
	cfg := gps.Config{
		ProjectID:         project,
		Endpoint:          addr,
		InsecureTransport: ev == testkit.EvidenceEmulator,
		AutoCreate:        true,
		AckDeadline:       10 * time.Second,
	}
	h := gps.ConformanceHarness(cfg, ev, testkit.Broker{
		Image:   os.Getenv("IQMQ_PUBSUB_IMAGE"),
		Version: os.Getenv("IQMQ_PUBSUB_VERSION"),
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
		t.Logf("claim UNVERIFIED (positive %v, negative %v): %-22s %s",
			v.Positive, v.Negative, v.Feature, v.Claimed)
	}
	t.Logf("capability claims proved in both directions: %d of %d", proven, len(rep.Verify()))
	if dir := os.Getenv("IQMQ_CONFORMANCE_REPORTS"); dir != "" {
		path, err := rep.Save(dir)
		if err != nil {
			t.Fatalf("saving the report: %v", err)
		}
		t.Logf("report written to %s", path)
	}
	_ = mq.ErrClosed
}
