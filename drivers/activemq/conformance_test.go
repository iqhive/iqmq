package activemq_test

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"

	mq "github.com/iqhive/iqmq"
	amq "github.com/iqhive/iqmq/drivers/activemq"
	"github.com/iqhive/iqmq/dsn"
	"github.com/iqhive/iqmq/internal/activemq"
	"github.com/iqhive/iqmq/testkit"
)

// TestConformance runs the suite against a broker named by the
// environment, which is how this module's own test reaches a deployment
// somebody already has.
//
// The SHIPPED evidence is not this: it is `conformance/suites`, which runs the
// same harness against the pinned images and writes the four reports the
// generated capability matrix is built from. This exists so that a developer
// with a broker of their own can run the suite without the container harness,
// and so that the harness itself is exercised by two callers rather than one.
func TestConformance(t *testing.T) {
	addr := os.Getenv("IQMQ_ARTEMIS_ADDR")
	broker := activemq.Artemis
	if addr == "" {
		addr, broker = os.Getenv("IQMQ_ACTIVEMQ_CLASSIC_ADDR"), activemq.Classic
	}
	if addr == "" {
		t.Skip("set IQMQ_ARTEMIS_ADDR or IQMQ_ACTIVEMQ_CLASSIC_ADDR to run the suite against a broker; " +
			"this module's own tests need none, and the shipped evidence is conformance/suites against the " +
			"pinned images")
	}
	cfg := amq.Config{
		Broker:            broker,
		Addrs:             []string{addr},
		Username:          os.Getenv("IQMQ_ACTIVEMQ_USER"),
		Password:          dsn.Secret(os.Getenv("IQMQ_ACTIVEMQ_PASSWORD")),
		Prefix:            "iqmq.",
		InsecurePlainSASL: true,
		Scheduler:         os.Getenv("IQMQ_ACTIVEMQ_SCHEDULER") != "",
	}
	h := amq.ConformanceHarness(cfg, testkit.EvidenceContainer, testkit.Broker{Image: addr})
	h.Commit = os.Getenv("IQMQ_COMMIT")
	rep := testkit.RunConformance(t, h)
	pass, fail, skip := rep.Counts()
	t.Logf("conformance %s: %d passed, %d failed, %d skipped", rep.Key(), pass, fail, skip)
	for _, v := range rep.Verify() {
		if v.Failed {
			t.Errorf("capability %s (%s) has a failing scenario", v.Feature, v.Claimed)
		}
	}
	if dir := os.Getenv("IQMQ_CONFORMANCE_REPORTS"); dir != "" {
		if _, err := rep.Save(dir); err != nil {
			t.Fatalf("saving the report: %v", err)
		}
	}
}

// TestConformanceHarnessIsWellFormed holds the harness to what the kit needs
// of it, with no broker.
//
// It exists because every one of these is a mistake that shows up as a
// conformance run that PASSES for the wrong reason: a nil Wildcard leaves the
// negative claim unverified, two scenarios sharing a topic share a queue on
// Artemis and the second consumes the first's messages, and a Skip map that
// grew silently is a suite that stopped checking things.
func TestConformanceHarnessIsWellFormed(t *testing.T) {
	t.Parallel()
	for _, broker := range activemq.Brokers() {
		cfg := amq.Config{
			Broker: broker, Addrs: []string{"localhost:5672"},
			InsecurePlainSASL: true, Password: dsn.Secret("p"),
		}
		h := amq.ConformanceHarness(cfg, testkit.EvidenceContainer,
			testkit.Broker{Image: "apache/activemq-artemis:2.44.0-alpine"})
		if h.Driver != "activemq" {
			t.Errorf("%v: the harness names the driver %q", broker, h.Driver)
		}
		if h.Profile == "" {
			t.Errorf("%v: the harness names no profile, so two of the four reports would share a key and "+
				"overwrite each other", broker)
		}
		if h.Open == nil || h.TopicName == nil {
			t.Fatalf("%v: the harness has no Open or no TopicName", broker)
		}
		if h.Wildcard == nil {
			t.Errorf("%v: the harness supplies no wildcard pattern. This driver does not CLAIM a wildcard "+
				"subscription, and without a pattern the kit's negative probe skips -- which leaves the claim "+
				"unverified in both directions", broker)
		}
		if !h.Evidence.Valid() {
			t.Errorf("%v: the harness does not say what kind of evidence a run is", broker)
		}
		if h.Fidelity <= 0 {
			t.Errorf("%v: the harness's fidelity is %d", broker, h.Fidelity)
		}
		if len(h.Skip) == 0 {
			t.Errorf("%v: the harness skips nothing, which for this protocol cannot be right: AMQP 1.0 has no "+
				"committed position, so `start/positions` is inapplicable", broker)
		}
		if len(h.Secrets) == 0 {
			t.Errorf("%v: the harness declares no credential, so the redaction scenario skips rather than "+
				"checking anything", broker)
		}
		// Two purposes must give two names, and the names must be usable as
		// an Artemis QUEUE name as well as a topic -- the derived queue is
		// `<topic>.<subscription>` and a queue name is unique across the
		// whole broker, so two scenarios sharing a topic share a queue.
		seen := map[string]bool{}
		for _, purpose := range []string{"a", "b", "c", "publish/fidelity", "never-provisioned"} {
			name := h.TopicName(purpose)
			if seen[name] {
				t.Errorf("%v: TopicName produced %q twice; on Artemis two scenarios sharing a topic share a queue "+
					"and the second consumes the first's messages", broker, name)
			}
			seen[name] = true
			if strings.ContainsAny(name, " /:?&*#>,") {
				t.Errorf("%v: TopicName produced %q, which neither broker can address", broker, name)
			}
			if _, err := cfg.Names(name, "conformance"); err != nil {
				t.Errorf("%v: the topic %q this harness generates is refused by the driver's own naming: %v",
					broker, name, err)
			}
		}
	}
}

// TestHarnessRefusesTheNeedsThisProtocolCannotBuild is the other half of the
// harness's honesty: a Need it accepts but cannot build would make a scenario
// prove something FALSE, and a refusal has to reach ErrUnsupported or the kit
// reports a failure instead of a skip with a reason.
func TestHarnessRefusesTheNeedsThisProtocolCannotBuild(t *testing.T) {
	t.Parallel()
	cfg := amq.Config{Broker: activemq.Artemis, Addrs: []string{"localhost:5672"}, InsecurePlainSASL: true}
	h := amq.ConformanceHarness(cfg, testkit.EvidenceInProcess, testkit.Broker{Image: "none"})
	for name, need := range map[string]testkit.Need{
		"several partitions": {Topic: "t", Subscriptions: []string{"s"}, Partitions: 4},
		"a retained topic":   {Topic: "t", Subscriptions: []string{"s"}, Retain: true},
	} {
		_, err := h.Open(context.Background(), need)
		if err == nil {
			t.Errorf("the harness accepted %s", name)
			continue
		}
		if !errors.Is(err, mq.ErrUnsupported) {
			t.Errorf("the refusal of %s is %v, which the kit turns into a failure rather than a skip with a reason",
				name, err)
		}
	}
}

// TestSkipsAreStatedAndConditional is what keeps the skip list honest.
//
// A skip is never a pass: the claim a skipped scenario would have proved stays
// unverified in the generated matrix. So every entry must have a reason, and
// the entries that depend on the CONFIGURATION must disappear when the
// configuration changes -- otherwise a suite that fixed the condition would go
// on skipping the scenario and nobody would notice.
func TestSkipsAreStatedAndConditional(t *testing.T) {
	t.Parallel()
	base := amq.Config{Broker: activemq.Artemis, Addrs: []string{"localhost:5672"}, InsecurePlainSASL: true}
	withRoute := base
	withRoute.DeadLetterTopic = "dead"
	for name, cfg := range map[string]amq.Config{"base": base, "with a dead-letter route": withRoute} {
		h := amq.ConformanceHarness(cfg, testkit.EvidenceInProcess, testkit.Broker{Image: "none"})
		for scenario, reason := range h.Skip {
			if strings.TrimSpace(reason) == "" {
				t.Errorf("%s: the skip of %q states no reason; the report publishes it and the driver page rests "+
					"on it", name, scenario)
			}
		}
	}
	// The dead-letter skip is the conditional one: it is there when the
	// driver publishes no dead letter and gone when it does.
	if _, ok := amq.ConformanceHarness(base, testkit.EvidenceInProcess,
		testkit.Broker{Image: "none"}).Skip["dlq/failed-publish-never-acks"]; !ok {
		t.Error("the base configuration publishes no dead letter and does not skip dlq/failed-publish-never-acks; " +
			"the scenario is about a driver whose own publication fails, and the broker's route is not one")
	}
	if _, ok := amq.ConformanceHarness(withRoute, testkit.EvidenceInProcess,
		testkit.Broker{Image: "none"}).Skip["dlq/failed-publish-never-acks"]; ok {
		t.Error("a configuration with DeadLetterTopic set still skips dlq/failed-publish-never-acks; the driver " +
			"does publish the dead letter there, so the scenario is exactly what it is about")
	}
}
