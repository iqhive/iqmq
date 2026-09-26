package amqp10_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	aq "github.com/iqhive/iqmq/drivers/amqp10"
	"github.com/iqhive/iqmq/testkit"
)

// TestConformance runs the whole suite against an AMQP 1.0 broker.
//
// It is env-gated with IQMQ_AMQP10_ADDR set it runs against that
// broker, and with nothing set it SKIPS. This module starts no container and
// needs none — every other test in the package runs against the scripted peer of
// internal/client — and the shipped evidence for the capability matrix is a
// PROFILE's own conformance suite against the profile's pinned image, because
// The specification asks what a driver does against the broker it ships against.
//
// The profile here is the generic one plain addresses and nothing
// broker-specific claimed. A run against Artemis or RabbitMQ 4 through this
// profile tests the CORE and will skip or fail whatever the generic addressing
// cannot reach, which is what makes a broker-specific module's own suite the
// evidence rather than this.
func TestConformance(t *testing.T) {
	addr := os.Getenv("IQMQ_AMQP10_ADDR")
	if addr == "" {
		t.Skip("set IQMQ_AMQP10_ADDR to run the suite against an AMQP 1.0 broker; this module's own tests " +
			"need none, and the shipped evidence is a profile's own suite against its pinned image")
	}
	cfg := aq.Config{Addrs: strings.Split(addr, ",")}
	h := aq.ConformanceHarness(cfg, genericProfile(), testkit.EvidenceContainer, testkit.Broker{Image: addr})
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

// TestConformanceHarnessIsWellFormed is what makes the skip above safe.
//
// A harness nobody has built is a harness that fails on the day somebody sets
// the environment variable, and the failure then looks like the broker's. This
// builds it and checks the parts the kit validates, with no broker at all.
func TestConformanceHarnessIsWellFormed(t *testing.T) {
	h := aq.ConformanceHarness(aq.Config{}, testProfile(), testkit.EvidenceContainer,
		testkit.Broker{Image: "apache/activemq-artemis:2.44.0-alpine"})
	if h.Driver != "amqp10test" {
		t.Errorf("the harness names driver %q, want the profile's own", h.Driver)
	}
	if h.Profile != "artemislike" {
		t.Errorf("the harness names profile %q, want the profile's own variant", h.Profile)
	}
	if h.Open == nil {
		t.Error("the harness has no Open, so the kit would refuse it")
	}
	if h.TopicName == nil {
		t.Fatal("the harness has no TopicName")
	}
	// The same purpose gives the same name within one run, and two purposes
	// give different ones: the rule, and what keeps two scenarios from
	// sharing an address.
	a1, a2 := h.TopicName("publish-fidelity"), h.TopicName("settle-ack")
	if a1 == a2 {
		t.Errorf("two purposes produced the same topic name %q", a1)
	}
	for _, name := range []string{a1, a2} {
		if strings.ContainsAny(name, " /:?&") {
			t.Errorf("the topic name %q contains a character an address grammar may refuse", name)
		}
	}
	if !h.Evidence.Valid() {
		t.Error("the harness does not say what kind of evidence its run is")
	}
	if h.Fidelity <= 0 {
		t.Error("the harness lowers the fidelity count to nothing")
	}
	if len(h.Skip) == 0 {
		t.Error("the harness documents no skips; the start positions have no meaning on this protocol and a skip " +
			"with a reason is what the report publishes")
	}
	// The one Need this protocol cannot express is refused with something the
	// kit turns into a skip rather than a failure.
	if _, err := h.Open(sinkCtx(), testkit.Need{Topic: "t", Subscriptions: []string{"s"}, Partitions: 4}); err == nil {
		t.Error("the harness accepted a Need for several partitions; everything would then arrive in order because " +
			"there is only ever one lane, and the scenario would pass for the wrong reason")
	}
}
