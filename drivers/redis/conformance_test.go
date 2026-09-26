package redis_test

import (
	"os"
	"path/filepath"
	"testing"

	rd "github.com/iqhive/iqmq/drivers/redis"
	"github.com/iqhive/iqmq/testkit"
)

// TestConformance runs the whole suite against a Redis server.
//
// It is env-gated with IQMQ_REDIS_ADDR set it runs against that
// server, and with nothing set it is skipped. There is deliberately no
// in-process fallback — the in-memory Commander this package's unit tests use
// is not a Redis, and running the conformance kit against it would produce a
// report that looks like evidence and is not. The shipped evidence is
// The conformance suite against the pinned container image,
// because the specification asks what a driver does against the broker it ships against.
func TestConformance(t *testing.T) {
	addr := os.Getenv("IQMQ_REDIS_ADDR")
	if addr == "" {
		t.Skip("set IQMQ_REDIS_ADDR to run the suite against a server; the shipped evidence is the conformance suite")
	}
	h := rd.ConformanceHarness(
		rd.Config{Addrs: []string{addr}},
		testkit.EvidenceContainer,
		testkit.Broker{Image: addr},
	)
	rep := testkit.RunConformance(t, h)
	pass, fail, skip := rep.Counts()
	t.Logf("conformance: %d passed, %d failed, %d skipped, of %d scenarios", pass, fail, skip, len(rep.Results))
	proven, unproven := 0, 0
	for _, v := range rep.Verify() {
		if v.Failed {
			t.Errorf("capability %s (%s) has a failing scenario", v.Feature, v.Claimed)
		}
		if v.Proven() {
			proven++
			t.Logf("claim proved in both directions: %-22s %s", v.Feature, v.Claimed)
			continue
		}
		unproven++
		t.Logf("claim UNVERIFIED (positive %v, negative %v): %-22s %s", v.Positive, v.Negative, v.Feature, v.Claimed)
	}
	t.Logf("claims: %d proved in both directions, %d unverified", proven, unproven)
	if dir := os.Getenv("IQMQ_CONFORMANCE_REPORTS"); dir != "" {
		path, err := rep.Save(dir)
		if err != nil {
			t.Fatalf("saving the report: %v", err)
		}
		t.Logf("report written to %s", path)
	}
	if t.Failed() {
		if path, err := rep.Save(filepath.Join(t.TempDir(), "reports")); err == nil {
			t.Logf("the failing run's report is at %s", path)
		}
	}
}
