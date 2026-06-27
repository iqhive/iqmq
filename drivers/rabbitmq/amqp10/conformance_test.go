package amqp10_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	mq "bitbucket.org/iqhive/iqmq"
	rmq10 "bitbucket.org/iqhive/iqmq/drivers/rabbitmq/amqp10"
	"bitbucket.org/iqhive/iqmq/dsn"
	"bitbucket.org/iqhive/iqmq/testkit"
)

// TestConformance runs the whole suite against a RabbitMQ 4 broker.
//
// It is env-gated with IQMQ_RABBITMQ_ADDR set it runs against
// that broker, and with nothing set it SKIPS. This module starts no container
// and needs none — every other test in the package runs against the scripted
// peer of peer_test.go — and the shipped evidence for the generated capability
// matrix is `conformance/suites`' run against the pinned image.
//
// **The topology has to exist first.** This profile declares nothing (see
// Config.Declare), so a run whose exchanges, queues and bindings nobody created
// fails at the first attach with `amqp:not-found` rather than skipping. The way
// to run it is either against an endpoint whose topology an operator has
// created, or through
// [rmq10.ConformanceHarnessWithProvisioner] with a step that creates each
// scenario's Need — which is what `conformance/suites` is expected to do, using
// the AMQP 0-9-1 driver's Admin surface or the container's management API.
// IQMQ_RABBITMQ_DECLARED is the acknowledgement that somebody has done it, so
// that a run against an empty broker skips with a reason instead of failing
// forty scenarios for one cause.
func TestConformance(t *testing.T) {
	addr := os.Getenv("IQMQ_RABBITMQ_ADDR")
	if addr == "" {
		t.Skip("set IQMQ_RABBITMQ_ADDR to run the suite against a RabbitMQ 4 broker; this module's own tests " +
			"need none, and the shipped evidence is conformance/suites' run against the pinned image")
	}
	if os.Getenv("IQMQ_RABBITMQ_DECLARED") == "" {
		t.Skip("the AMQP 1.0 profile creates no topology, so every scenario needs its exchange, queues and bindings " +
			"to exist before it opens. Set IQMQ_RABBITMQ_DECLARED=1 once they do, or run the suite through " +
			"ConformanceHarnessWithProvisioner, which creates them")
	}
	cfg := rmq10.Config{
		Addrs:      strings.Split(addr, ","),
		Username:   os.Getenv("IQMQ_RABBITMQ_USER"),
		Vhost:      os.Getenv("IQMQ_RABBITMQ_VHOST"),
		DeadLetter: true,
	}
	if pw := os.Getenv("IQMQ_RABBITMQ_PASSWORD"); pw != "" {
		cfg.Password = dsn.Secret(pw)
	}
	h := rmq10.ConformanceHarness(cfg, testkit.EvidenceContainer, testkit.Broker{Image: addr})
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
	cfg := rmq10.Config{Addrs: []string{"amqp://127.0.0.1:5672"}, DeadLetter: true}
	h := rmq10.ConformanceHarness(cfg, testkit.EvidenceContainer, testkit.Broker{Image: "rabbitmq:4.1-management"})
	if h.Driver != "rabbitmq" || h.Profile != "amqp10" {
		t.Errorf("the harness names %q/%q, want rabbitmq/amqp10: the generated matrix keys a row on the pair, so a "+
			"profile that named itself something else would appear as another broker", h.Driver, h.Profile)
	}
	if h.Open == nil || h.TopicName == nil {
		t.Fatal("the harness has no Open or no TopicName, so the kit would refuse it")
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
	if h.Wildcard == nil {
		t.Error("the harness supplies no wildcard pattern. This profile does not CLAIM a wildcard subscription, and " +
			"without a pattern the kit's negative probe skips — which leaves the claim unverified in both directions")
	}
	// Every generated topic name has to be one `internal/rabbit` accepts,
	// because the name is the exchange's and half of every queue's.
	names := map[string]bool{}
	for _, purpose := range [...]string{"publish-fidelity", "settle/ack", "dead letter", "publish-fidelity"} {
		name := h.TopicName(purpose)
		if _, _, err := rmq10.AddressForTest(cfg, name, "workers"); err != nil {
			t.Errorf("the harness produced the topic name %q, which this profile cannot address: %v", name, err)
		}
		names[name] = true
	}
	if len(names) != 4 {
		t.Errorf("the harness produced %d distinct names for four purposes; two scenarios sharing a topic share a "+
			"queue, and the second would consume the first's messages", len(names))
	}
}

// TestHarnessRefusesTheNeedsThisProtocolCannotBuild holds the harness to the specification's
// narrow rule: refuse a Need only when accepting it would make a scenario prove
// something FALSE.
//
// A refusal becomes a skip with a reason in the report, and a skip is never a
// pass — the claim the scenario would have proved stays unverified, which is
// the only honest answer for something nobody checked. Accepting one of these
// would be worse: a partitioned scenario would find everything in order because
// there is only ever one lane.
func TestHarnessRefusesTheNeedsThisProtocolCannotBuild(t *testing.T) {
	cfg := rmq10.Config{Addrs: []string{"amqp://127.0.0.1:5672"}}
	h := rmq10.ConformanceHarness(cfg, testkit.EvidenceContainer, testkit.Broker{Image: "rabbitmq:4.1"})
	base := testkit.Need{Topic: "iqmq-conf-x", Subscriptions: []string{"workers"}}
	for name, need := range map[string]testkit.Need{
		"several partitions":    {Topic: base.Topic, Subscriptions: base.Subscriptions, Partitions: 4},
		"a retained topic":      {Topic: base.Topic, Subscriptions: base.Subscriptions, Retain: true},
		"broker de-duplication": {Topic: base.Topic, Subscriptions: base.Subscriptions, DedupWindow: 300},
		"a wildcard topology":   {Topic: base.Topic, Subscriptions: base.Subscriptions, Wildcards: true},
	} {
		client, err := h.Open(context.Background(), need)
		if client != nil {
			_ = client.Close()
		}
		if err == nil {
			t.Errorf("the harness accepted a Need for %s", name)
			continue
		}
		if !errors.Is(err, mq.ErrUnsupported) {
			t.Errorf("the refusal of %s is %v, which the kit turns into a failure rather than a skip with a reason",
				name, err)
		}
	}
}
