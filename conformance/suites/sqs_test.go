//go:build integration

package suites

import (
	"os"
	"testing"
	"time"

	"go.uber.org/goleak"

	"github.com/iqhive/iqmq/conformance/harness"
	sq "github.com/iqhive/iqmq/drivers/sqs"
	"github.com/iqhive/iqmq/testkit"
)

// TestConformance_sqs runs the whole suite against the pinned ElasticMQ
// container and writes the report conformance/cmd/confgen turns into
// capabilities.json and docs/drivers/matrix.md.
//
// ElasticMQ rather than LocalStack for the shipped evidence, and the reason is
// about what the report MEANS rather than about speed. Both are emulators and
// neither is evidence for the managed service (layer 4), so the report's
// Evidence is EvidenceEmulator either way; ElasticMQ starts in seconds where
// LocalStack takes a minute, and it implements the whole surface this driver
// uses. TestConformance_sqs_localstack below runs the same harness against
// LocalStack, so a claim the two disagree about is a test failure rather than
// a surprise in production.
func TestConformance_sqs(t *testing.T) {
	runSQSConformance(t, harness.ElasticMQ(t), false)
}

// TestConformance_sqs_fifo runs the same suite against the FIFO profile, whose
// capability table differs in seven claims.
//
// It is a separate report — the profile is part of the report's key — because
// The specification makes a capability table constant per CLIENT and the two profiles are
// two clients: a caller who read the standard profile's matrix row and
// deployed a FIFO queue would be planning against a different queue.
func TestConformance_sqs_fifo(t *testing.T) {
	runSQSConformance(t, harness.ElasticMQ(t), true)
}

// TestConformance_sqs_localstack runs the same harness against LocalStack.
//
// It exists to answer one question: do the two emulators agree about what this
// driver can do? A claim one of them proves and the other does not is either
// an emulator's gap or a driver's assumption about a behaviour only one of them
// has, and both are worth knowing before the driver is pointed at the managed
// service.
//
// Its report is its own file — profile "standard-localstack", so
// sqs-standard-localstack.json — and not capability evidence: a report saved
// under the ElasticMQ arm's name would replace the shipped sqs-standard.json
// with this emulator's answers under the other one's provenance, and
// The conformance suites manifest marks this entry cross_check and names no report for
// it.
func TestConformance_sqs_localstack(t *testing.T) {
	if os.Getenv("IQMQ_SQS_LOCALSTACK") == "" {
		t.Skip("IQMQ_SQS_LOCALSTACK is not set; LocalStack takes about a minute to start and ElasticMQ is the shipped evidence")
	}
	runSQSConformance(t, harness.LocalStack(t), false)
}

// runSQSConformance is the one body all three share, so that the profiles and
// the emulators cannot drift apart.
func runSQSConformance(t *testing.T, b *harness.Broker, fifo bool) {
	// goleak is on the DRIVER. A source runs Config.Pollers goroutines parked
	// in a long poll, so a Close that did not cancel their request would leak
	// one per subscription across 150-odd scenarios and be invisible in every
	// other test.
	defer goleak.VerifyNone(t, conformanceLeakIgnores()...)
	cfg := sq.Config{
		// The region is the emulator's own name rather than a real one: the
		// SDK needs a region to sign a request and neither emulator checks
		// it, so naming a real one would suggest the report says something
		// about that region.
		Region:            "elasticmq",
		Endpoint:          "http://" + b.Addr,
		InsecureTransport: true,
		// Both emulators accept any credentials. They are supplied anyway,
		// and they are why `secrets/redaction` runs on this driver and skips
		// on every other one: the scenario needs a harness that declares a
		// secret, and SQS is the first driver whose configuration carries
		// one.
		AccessKeyID:     "AKIACONFORMANCE",
		SecretAccessKey: "iqmq-conformance-secret-not-a-real-credential",
		AutoCreate:      true,
		FIFO:            fifo,
		// One second rather than twenty: every scenario that expects NOTHING
		// to arrive waits for the poll to come back, and a twenty-second
		// long poll would make each of those scenarios twenty seconds long.
		// A production consumer wants the twenty (see docs/drivers/sqs.md).
		WaitTime:    time.Second,
		AckDeadline: 30 * time.Second,
	}
	// sqsHarness rather than sq.ConformanceHarness directly: the emulator is
	// part of the report's KEY and the driver's own Config.Profile() cannot
	// know which emulator it is pointed at. It lives in sqs_harness_test.go,
	// which carries no build tag, so the key every arm of this suite writes is
	// asserted by `make test` without a container.
	h := sqsHarness(b, cfg)
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
