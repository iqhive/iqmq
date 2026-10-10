//go:build integration

package suites

import (
	"os"
	"testing"
	"time"

	"go.uber.org/goleak"

	"github.com/iqhive/iqmq/conformance/harness"
	nq "github.com/iqhive/iqmq/drivers/nsq"
	"github.com/iqhive/iqmq/testkit"
)

func TestConformance_nsq(t *testing.T) {
	// goleak is on the DRIVER: go-nsq runs a handler pool, a lookupd poll
	// loop and an RDY loop per subscription, and every one of them outlives a
	// Close that forgot to wait for the consumer's stop channel. go-nsq's own
	// teardown is also conditional on the driver having responded to every
	// in-flight message, so a Close that did not requeue what it was holding
	// leaves a connection draining for thirty seconds — which goleak sees as
	// a goroutine and a person sees as a hang.
	defer goleak.VerifyNone(t, conformanceLeakIgnores()...)
	b := harness.NSQ(t)
	cfg := nq.Config{
		// The nsqd TCP address is where publications go; nsqlookupd is
		// discovery for CONSUMERS and cannot be published through.
		Addrs: []string{b.Addr},
		// Discovery through nsqlookupd rather than straight to the nsqd,
		// because that is what a cluster uses and it is the path a direct
		// connection cannot cover: a consumer wired to the addresses never
		// learns about an nsqd that joins later. The harness starts the
		// pair precisely so this is the path under test.
		LookupdAddrs: []string{"http://" + b.Addrs["lookupd-http"]},
		// nsqd's administrative API is HTTP and on its own port, and this
		// driver would otherwise guess it as the TCP port plus one.
		AdminAddrs: []string{b.Addrs["http"]},
		// nsqd runs here without authentication, and go-nsq only sends AUTH
		// when nsqd asks for it — so a configured secret is never put on
		// the wire. It is configured anyway, and that is the point: it is
		// what makes `secrets/redaction` RUN rather than skip, and the
		// scenario's question is whether any rendered string or
		// driver-built error carries it.
		AuthSecret: "iqmq-nsq-conformance-secret-not-a-real-credential",
		// Five seconds rather than nsqd's own minute. The harness supplies
		// no Advance — there is no way to move a real nsqd's clock — so a
		// scenario that waits for a lease to expire waits in real seconds,
		// and a minute of that is a minute per scenario.
		AckDeadline: 5 * time.Second,
		LeaseGuard:  time.Second,
		// nsqd may hold a write to this client for its output-buffer
		// timeout, and go-nsq's default is 250 ms: a quarter of a second
		// added to the latency of every otherwise-idle delivery, which is
		// most of them in a conformance run. nsqd's own floor is 25 ms and
		// it refuses less in the IDENTIFY handshake.
		OutputBufferTimeout: nq.MinOutputBufferTimeout,
	}
	h := nq.ConformanceHarness(cfg, testkit.EvidenceContainer, testkit.Broker{
		Image: b.Image, Version: b.Version,
	})
	h.Commit = os.Getenv("IQMQ_COMMIT")
	rep := testkit.RunConformance(t, h)
	pass, fail, skip := rep.Counts()
	t.Logf("conformance: %d passed, %d failed, %d skipped, of %d scenarios", pass, fail, skip, len(rep.Results))

	proven := 0
	for _, v := range rep.Verify() {
		switch {
		case v.Failed:
			t.Errorf("capability %s (%s) has a failing scenario", v.Feature, v.Claimed)
		case v.Proven():
			proven++
		case v.Unrefuted():
			// A true denial of a GUARANTEE that nothing in this run broke.
			// It is not work outstanding: no number of runs can observe a
			// guarantee's absence, so this is the state of the world and
			// the driver page says which claims are in it.
			t.Logf("claim UNREFUTED: %-22s %s (a guarantee's absence is not observable in one run)",
				v.Feature, v.Claimed)
		default:
			// An unproven claim is not a failure — the evidence rule is
			// that it does not RENDER — but it is worth naming in the log,
			// because the generated matrix silently omits it and the driver
			// page has to say why.
			t.Logf("claim UNVERIFIED (positive %v, negative %v): %-22s %s",
				v.Positive, v.Negative, v.Feature, v.Claimed)
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
