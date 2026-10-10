//go:build integration

package suites

import (
	"os"
	"testing"

	"github.com/testcontainers/testcontainers-go"
	"go.uber.org/goleak"

	"github.com/iqhive/iqmq/conformance/harness"
	rd "github.com/iqhive/iqmq/drivers/redis"
	"github.com/iqhive/iqmq/testkit"
)

// TestConformance_redis runs the whole suite against the pinned Redis
// container and writes the report conformance/cmd/confgen turns into
// capabilities.json and docs/drivers/matrix.md.
//
// The container is started with `appendonly yes`, and that is not a
// convenience: WAITAOF is this driver's ConfirmDurable and its condition is an
// append-only file, so a run against a server without one would report
// FeatureDurableConfirm as unsupported and the generated matrix would say this
// driver has no durable boundary. The version matrix
// (conformance/matrix/redis_test.go) is where the other side of that is
// proved.
func TestConformance_redis(t *testing.T) {
	// goleak is on the DRIVER. This driver starts two goroutines per source —
	// the mandatory reclaim sweep and, under the zset strategy, the delayed
	// mover — so a Close that did not stop them would leak one per
	// subscription across 150-odd scenarios. The ignores are the test
	// harness's own.
	defer goleak.VerifyNone(t, conformanceLeakIgnores()...)
	b := harness.Redis(t, testcontainers.WithCmdArgs("--appendonly", "yes"))
	h := rd.ConformanceHarness(
		rd.Config{Addrs: []string{b.Addr}},
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
