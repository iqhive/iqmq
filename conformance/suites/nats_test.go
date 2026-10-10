//go:build integration

// Package suites runs the conformance kit against containerised brokers.
//
// It is the evidence the generated capability matrix is built from: a claim
// renders there only when a scenario proved it in both directions against the
// broker image the project ships against, which is why these tests are
// build-tagged and run by `make integration` rather than by `make ci`.
package suites

import (
	"os"
	"testing"

	"go.uber.org/goleak"

	"github.com/iqhive/iqmq/conformance/harness"
	nq "github.com/iqhive/iqmq/drivers/nats"
	"github.com/iqhive/iqmq/testkit"
)

// TestConformance_nats runs the whole suite against the pinned NATS
// container and writes the report conformance/cmd/confgen turns into
// capabilities.json and docs/drivers/matrix.md.
//
// The driver under test is NATS **Core**. The container is started with
// JetStream enabled because one image serves both drivers, and this suite never
// touches it: a Core driver that quietly used a stream would advertise
// capabilities it does not have (ADR-010).
func TestConformance_nats(t *testing.T) {
	// goleak is on the DRIVER: a subscription goroutine, a parked Push or a
	// flusher that outlives Close fails this. The ignores are the test
	// harness's own — testcontainers keeps a connection to its reaper for
	// the life of the process, and net/http keeps its idle connections —
	// and none of them is anything this driver started.
	defer goleak.VerifyNone(t, conformanceLeakIgnores()...)
	b := harness.NATS(t)
	h := nq.ConformanceHarness(
		nq.Config{Addrs: []string{"nats://" + b.Addr}},
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
		}
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
