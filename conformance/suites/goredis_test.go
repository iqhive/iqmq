//go:build integration

package suites

import (
	"os"
	"testing"

	"github.com/testcontainers/testcontainers-go"
	"go.uber.org/goleak"

	"github.com/iqhive/iqmq/conformance/harness"
	rd "github.com/iqhive/iqmq/drivers/redis"
	"github.com/iqhive/iqmq/drivers/redis/goredis"
	"github.com/iqhive/iqmq/testkit"
)

// TestConformance_goredis runs the whole suite against the pinned Redis
// container over go-redis, and writes the report conformance/cmd/confgen turns
// into capabilities.json and docs/drivers/matrix.md.
//
// The interesting result is not that it passes: it is the DIFF against
// redis-streams.json. The two reports are produced by one driver, one command
// layer and one container, differing only in the client library, so every
// scenario and every capability claim should be identical — and any that is
// not is either a real difference between the libraries (worth recording on
// the driver page) or a defect in one of the two adapters. docs/drivers/goredis.md
// carries the comparison.
//
// The container is started with `appendonly yes` for the same reason the
// rueidis suite is: WAITAOF is this driver's ConfirmDurable and its condition
// is an append-only file, so a run against a server without one would report
// FeatureDurableConfirm as unsupported and the generated matrix would say this
// driver has no durable boundary.
func TestConformance_goredis(t *testing.T) {
	// goleak is on the DRIVER. It starts two goroutines per source — the
	// mandatory reclaim sweep and, under the zset strategy, the delayed mover
	// — so a Close that did not stop them would leak one per subscription
	// across 150-odd scenarios. The ignores are the test harness's own.
	defer goleak.VerifyNone(t, conformanceLeakIgnores()...)
	b := harness.Redis(t, testcontainers.WithCmdArgs("--appendonly", "yes"))
	h := goredis.ConformanceHarness(
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
