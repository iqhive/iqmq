package nsq_test

import (
	"os"
	"strings"
	"testing"
	"time"

	mq "bitbucket.org/iqhive/iqmq"
	nq "bitbucket.org/iqhive/iqmq/drivers/nsq"
	"bitbucket.org/iqhive/iqmq/testkit"
)

// TestConformance runs the suite against an nsqd named by IQMQ_NSQ_ADDR.
//
// It is the dev-loop half. The shipped evidence — the report the generated
// capability matrix is built from — comes from the conformance suite,
// which starts the pinned nsqd/nsqlookupd pair itself; both run the same
// [nq.ConformanceHarness], so the two cannot drift.
//
// Two environment variables, because they answer different questions:
// IQMQ_NSQ_CONFORMANCE says whether to run at all (the suite takes minutes and
// creates a topic per scenario), and IQMQ_NSQ_ADDR says where.
func TestConformance(t *testing.T) {
	if os.Getenv("IQMQ_NSQ_CONFORMANCE") == "" {
		t.Skip("IQMQ_NSQ_CONFORMANCE is not set; the suite creates a topic per scenario")
	}
	addr := os.Getenv("IQMQ_NSQ_ADDR")
	if addr == "" {
		t.Skip("IQMQ_NSQ_ADDR is not set; the specification needs an nsqd and an in-process model is not one")
	}
	cfg := nq.Config{Addrs: []string{addr}, AckDeadline: 5 * time.Second}
	if http := os.Getenv("IQMQ_NSQ_HTTP_ADDR"); http != "" {
		cfg.AdminAddrs = []string{http}
	}
	if lookupd := os.Getenv("IQMQ_NSQ_LOOKUPD_ADDR"); lookupd != "" {
		cfg.LookupdAddrs = strings.Split(lookupd, ",")
	}
	h := nq.ConformanceHarness(cfg, testkit.EvidenceContainer, testkit.Broker{
		Image:   os.Getenv("IQMQ_NSQ_IMAGE"),
		Version: os.Getenv("IQMQ_NSQ_VERSION"),
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
		case v.Unrefuted():
			t.Logf("claim %s (%s) UNREFUTED: a guarantee's ABSENCE is not observable in one run",
				v.Feature, v.Claimed)
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
}

// TestTheHarnessConfiguresTheEnvelope is what the conformance run depends on
// and what nobody would notice going wrong.
//
// The kit builds every publisher and consumer at the zero metadata mode, which
// on this broker means "refuse a key, refuse an identifier, refuse an empty
// body". The specification says the envelope is this broker's portable-metadata story and makes it an emulation a deployment asks for once, so the harness is
// where the ask lives — and if it stopped asking, the suite would still be
// green with `publish/key-roundtrip` and `publish/roundtrip-bodies` failing
// for a reason that reads as a driver defect.
func TestTheHarnessConfiguresTheEnvelope(t *testing.T) {
	h := nq.ConformanceHarness(nq.Config{Addrs: []string{"127.0.0.1:4150"}},
		testkit.EvidenceContainer, testkit.Broker{Image: "nsqio/nsq:v1.3.0"})
	if h.Driver != "nsq" || h.Evidence != testkit.EvidenceContainer {
		t.Fatalf("the harness names driver %q with evidence %q", h.Driver, h.Evidence)
	}
	if h.Open == nil || h.Wildcard == nil || h.Quiesce == nil {
		t.Fatal("the harness leaves Open, Wildcard or Quiesce nil; a nil Wildcard skips both wildcard " +
			"scenarios and leaves the claim unverified in BOTH directions")
	}
	if _, skipped := h.Skip["metadata/native-strict-rejection"]; !skipped {
		t.Error("metadata/native-strict-rejection is not documented as inapplicable, but the harness " +
			"configures a metadata mode that puts no handle in strict mode")
	}
	// Every name the harness produces has to be a legal NSQ topic, or the
	// scenario fails at open with a message about a topic name.
	for _, purpose := range []string{
		"publish/roundtrip-bodies", "capability/ExactlyOnceDelivery/refused",
		"lifecycle/graceful-shutdown-drains", strings.Repeat("very-long-scenario-name/", 8),
	} {
		name := h.TopicName(purpose)
		if err := nq.CheckTopic(name); err != nil {
			t.Errorf("the harness produced %q for %q, which nsqd refuses: %v", name, purpose, err)
		}
	}
	// And two calls never collide, or two scenarios share a topic.
	if a, b := h.TopicName("x"), h.TopicName("x"); a == b {
		t.Errorf("two calls produced the same topic %q", a)
	}
	_ = mq.MetadataAdaptive
}
