package jetstream_test

import (
	"os"
	"path/filepath"
	"testing"

	nq "bitbucket.org/iqhive/iqmq/drivers/jetstream"
	"bitbucket.org/iqhive/iqmq/testkit"
)

// TestConformance runs the whole suite against a JetStream server.
//
// It is env-gated with IQMQ_NATS_ADDR set it runs against that
// server, and with nothing set it runs against this module's own embedded one,
// which is fast enough to be useful while developing but is NOT the evidence
// the capability matrix is generated from. That evidence is
// The conformance suite against the pinned container image,
// because the specification asks what a driver does against the broker it ships against and
// not against a library the test compiled in.
//
// It is skipped in the ordinary test run: the suite opens a client per scenario
// and `make ci` runs the package several times.
func TestConformance(t *testing.T) {
	if os.Getenv("IQMQ_JETSTREAM_CONFORMANCE") == "" {
		t.Skip("set IQMQ_JETSTREAM_CONFORMANCE=1 to run the suite against an embedded server; the shipped evidence is the conformance suite")
	}
	addr := os.Getenv("IQMQ_NATS_ADDR")
	broker := testkit.Broker{Image: "embedded nats-server", Version: "2.14.6"}
	evidence := testkit.EvidenceInProcess
	if addr == "" {
		addr = freshServer(t)
	} else {
		broker, evidence = testkit.Broker{Image: addr}, testkit.EvidenceContainer
	}
	h := nq.ConformanceHarness(nq.Config{Addrs: []string{addr}}, evidence, broker)
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
