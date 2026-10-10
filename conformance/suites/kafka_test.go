//go:build integration

package suites

import (
	"os"
	"strings"
	"testing"

	"go.uber.org/goleak"

	"github.com/iqhive/iqmq/conformance/harness"
	kq "github.com/iqhive/iqmq/drivers/kafka"
	"github.com/iqhive/iqmq/testkit"
)

// TestConformance_kafka runs the whole suite against the pinned Kafka
// container and writes the report conformance/cmd/confgen turns into
// capabilities.json and docs/drivers/matrix.md.
//
// The driver under test is the franz-go one. The container is Apache Kafka in
// KRaft mode with auto topic creation ON, which the harness does not rely on:
// every scenario's topic is created through the portable administrative API
// before its first publication, because a Kafka topic's partition count is
// fixed when the topic is created and a topic auto-created by a produce has
// the cluster's default.
func TestConformance_kafka(t *testing.T) {
	// goleak is on the DRIVER: a fetch loop, a commit ticker or a parked
	// poll that outlives Close fails this. The ignores are the test
	// harness's own — testcontainers keeps a connection to its reaper for
	// the life of the process, and net/http keeps its idle connections —
	// and none of them is anything this driver started.
	defer goleak.VerifyNone(t, conformanceLeakIgnores()...)
	b := harness.Kafka(t)
	h := kq.ConformanceHarness(
		kq.Config{
			Brokers: strings.Split(b.Addr, ","),
			// The container is a single broker, so a topic created with
			// the cluster's default replication factor would be refused
			// if that default were higher than one.
			ReplicationFactor: 1,
		},
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
