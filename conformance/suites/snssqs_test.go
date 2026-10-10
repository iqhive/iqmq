//go:build integration

package suites

import (
	"os"
	"testing"
	"time"

	"go.uber.org/goleak"

	"github.com/iqhive/iqmq/conformance/harness"
	sns "github.com/iqhive/iqmq/drivers/snssqs"
	"github.com/iqhive/iqmq/testkit"
)

// TestConformance_snssqs runs the whole suite against the pinned
// LocalStack container and writes the report conformance/cmd/confgen turns
// into capabilities.json and docs/drivers/matrix.md.
//
// LocalStack and nothing else, and that is not a preference: ElasticMQ has no
// SNS at all. So this driver has ONE emulator where `drivers/sqs` has two, and
// the cross-check that found two of the defects — a claim one emulator
// proves and the other does not — is not available here. It is recorded in
// the maintenance notes; the compensation is that the consume half of this driver
// IS `drivers/sqs`, whose report against both emulators covers it.
//
// There is one report and one profile. SNS's FIFO topic is a different
// capability table and this driver refuses a ".fifo" name rather than
// implementing it , so a second column would describe
// nothing.
func TestConformance_snssqs(t *testing.T) {
	b := harness.LocalStack(t)
	// goleak is on the DRIVER. Every subscription is an SQS source running
	// Config.Pollers goroutines parked in a long poll, and a fan-out opens
	// one set per subscription — so a Close that did not cancel their
	// requests would leak N goroutines per topic across 150-odd scenarios
	// and be invisible in every other test.
	defer goleak.VerifyNone(t, conformanceLeakIgnores()...)
	cfg := sns.Config{
		// The region is the emulator's own name rather than a real one: the
		// SDKs need a region to sign a request and LocalStack does not
		// check it, so naming a real one would suggest the report says
		// something about that region.
		Region: "localstack",
		// One endpoint for both services, which is what LocalStack's edge
		// endpoint is.
		Endpoint:          "http://" + b.Addr,
		InsecureTransport: true,
		// LocalStack accepts any credentials. They are supplied anyway, and
		// they are why `secrets/redaction` runs on this driver: the
		// scenario needs a harness that declares a secret.
		AccessKeyID:     "AKIACONFORMANCE",
		SecretAccessKey: "iqmq-conformance-secret-not-a-real-credential",
		AccountID:       "000000000000",
		AutoCreate:      true,
		// The queue access policy is not written: LocalStack does not
		// enforce it, so it would be two API calls per queue for no
		// observable effect. A real account needs it, which is what
		// Config.NoQueuePolicy's default being false is for.
		NoQueuePolicy: true,
		// One second rather than twenty: every scenario that expects
		// NOTHING to arrive waits for the poll to come back, and a
		// twenty-second long poll would make each of those scenarios twenty
		// seconds long. A production consumer wants the twenty (see
		// docs/drivers/snssqs.md).
		WaitTime:    time.Second,
		AckDeadline: 30 * time.Second,
	}
	h := sns.ConformanceHarness(cfg, testkit.EvidenceEmulator, testkit.Broker{
		Image: b.Image, Version: b.Version,
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
		// An unproven claim is not a failure — the evidence rule is that
		// it does not RENDER — but it is worth naming in the log, because
		// the generated matrix silently omits it and the driver page has to
		// say why.
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
