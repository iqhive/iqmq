package snssqs_test

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	sns "github.com/iqhive/iqmq/drivers/snssqs"
	"github.com/iqhive/iqmq/dsn"
	"github.com/iqhive/iqmq/testkit"
)

// TestConformance runs the suite against an endpoint named by
// IQMQ_SNSSQS_ADDR.
//
// It is the dev-loop half. The shipped evidence — the report the generated
// capability matrix is built from — comes from
// The conformance suite, which starts the pinned LocalStack
// container itself; both run the same [sns.ConformanceHarness], so the two
// cannot drift.
//
// Two environment variables, because they answer different questions:
// IQMQ_SNSSQS_CONFORMANCE says whether to run at all (the suite takes minutes
// and creates a topic, a queue and a subscription per scenario), and
// IQMQ_SNSSQS_ADDR says where.
func TestConformance(t *testing.T) {
	if os.Getenv("IQMQ_SNSSQS_CONFORMANCE") == "" {
		t.Skip("IQMQ_SNSSQS_CONFORMANCE is not set; the suite creates a topic, a queue and a subscription per scenario")
	}
	addr := os.Getenv("IQMQ_SNSSQS_ADDR")
	if addr == "" {
		t.Skip("IQMQ_SNSSQS_ADDR is not set; the specification needs an endpoint, and an in-process server is not one")
	}
	cfg := sns.Config{
		Region:            "localstack",
		Endpoint:          "http://" + addr,
		InsecureTransport: true,
		AccessKeyID:       "AKIACONFORMANCE",
		SecretAccessKey:   dsn.Secret("iqmq-conformance-secret-not-a-real-credential"),
		AccountID:         "000000000000",
		AutoCreate:        true,
		NoQueuePolicy:     true,
		WaitTime:          time.Second,
		AckDeadline:       30 * time.Second,
	}
	h := sns.ConformanceHarness(cfg, testkit.EvidenceEmulator, testkit.Broker{
		Image:   os.Getenv("IQMQ_SNSSQS_IMAGE"),
		Version: os.Getenv("IQMQ_SNSSQS_VERSION"),
	})
	h.Commit = os.Getenv("IQMQ_COMMIT")
	rep := testkit.RunConformance(t, h)
	pass, fail, skip := rep.Counts()
	t.Logf("conformance %s: %d passed, %d failed, %d skipped", rep.Key(), pass, fail, skip)
	proven := 0
	for _, v := range rep.Verify() {
		switch {
		case v.Failed:
			t.Errorf("claim %s (%s) FAILED", v.Feature, v.Claimed)
		case v.Proven():
			proven++
		default:
			t.Logf("claim %s (%s) UNVERIFIED (positive %v, negative %v)", v.Feature, v.Claimed, v.Positive, v.Negative)
		}
	}
	t.Logf("%d claim(s) proved in both directions", proven)
	if dir := os.Getenv("IQMQ_CONFORMANCE_REPORTS"); dir != "" {
		path, err := rep.Save(dir)
		if err != nil {
			t.Fatalf("saving the report: %v", err)
		}
		t.Logf("report: %s", path)
	}
	if t.Failed() {
		// A failing run is worth keeping even when nobody asked for a
		// report: it is the only record of which scenario said what.
		if path, err := rep.Save(filepath.Join(t.TempDir(), "reports")); err == nil {
			t.Logf("failing report: %s", path)
		}
	}
}
