//go:build integration

package suites

import (
	"os"
	"testing"

	"go.uber.org/goleak"

	"github.com/iqhive/iqmq/conformance/harness"
	jq "github.com/iqhive/iqmq/drivers/jetstream"
	"github.com/iqhive/iqmq/testkit"
)

// TestConformance_jetstream runs the whole suite against the pinned NATS
// container with JetStream enabled, and writes the report
// conformance/cmd/confgen turns into capabilities.json and
// docs/drivers/matrix.md.
//
// The driver under test is JetStream. One image serves both NATS drivers —
// harness.NATS starts nats-server with --jetstream — and the two suites are
// separate because the two profiles are separate modules with separate
// capability tables (ADR-010): a Core driver that quietly used a stream
// would advertise capabilities it does not have, and a JetStream driver that
// quietly did not would advertise capabilities it cannot honour.
func TestConformance_jetstream(t *testing.T) {
	// goleak is on the DRIVER: a consume loop, a parked Push or a settler
	// that outlives Close fails this. The ignores are the test harness's
	// own — testcontainers keeps a connection to its reaper for the life of
	// the process, and net/http keeps its idle connections — and none of them
	// is anything this driver started.
	defer goleak.VerifyNone(t, conformanceLeakIgnores()...)
	b := harness.NATS(t)
	h := jq.ConformanceHarness(
		jq.Config{Addrs: []string{"nats://" + b.Addr}},
		testkit.EvidenceContainer,
		testkit.Broker{Image: b.Image, Version: b.Version},
	)
	h.Commit = os.Getenv("IQMQ_COMMIT")
	rep := testkit.RunConformance(t, h)
	pass, fail, skip := rep.Counts()
	t.Logf("conformance: %d passed, %d failed, %d skipped, of %d scenarios", pass, fail, skip, len(rep.Results))

	proven := 0
	for _, v := range rep.Verify() {
		if v.Failed {
			t.Errorf("capability %s (%s) has a failing scenario", v.Feature, v.Claimed)
		}
		if v.Proven() {
			proven++
			continue
		}
		t.Logf("claim UNVERIFIED (positive %v, negative %v): %-22s %s", v.Positive, v.Negative, v.Feature, v.Claimed)
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
