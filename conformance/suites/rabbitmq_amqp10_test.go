//go:build integration

package suites

import (
	"context"
	"os"
	"testing"

	"go.uber.org/goleak"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/conformance/harness"
	rq "github.com/iqhive/iqmq/drivers/rabbitmq"
	rq10 "github.com/iqhive/iqmq/drivers/rabbitmq/amqp10"
	"github.com/iqhive/iqmq/dsn"
	"github.com/iqhive/iqmq/testkit"
)

// TestConformance_rabbitmq_amqp10 runs the whole suite against the same
// pinned RabbitMQ container through the AMQP 1.0 profile.
//
// It is the other half and the reason the profile exists
// as a driver rather than as a flag: the two profiles' capability tables differ
// in eleven claims, and a caller who read the 0-9-1 row and deployed the 1.0
// one would be planning against a different broker. The specification makes a capability
// table per CLIENT, so two clients over one broker are two reports.
//
// **The topology is provisioned by the AMQP 0-9-1 driver.** RabbitMQ 4's AMQP
// 1.0 endpoint creates no exchange, queue or binding — an attach to an address
// nothing provisioned is answered with `amqp:not-found` — so every scenario's
// Need has to exist before its client opens. Doing it with the 0-9-1 driver's
// own Admin surface rather than with the management HTTP API is what keeps the
// two profiles on ONE topology: the names come from `internal/rabbit` either
// way, so a mistake in the addressing shows up as a scenario that cannot find
// its queue instead of as two topologies that happen to work separately.
func TestConformance_rabbitmq_amqp10(t *testing.T) {
	defer goleak.VerifyNone(t, conformanceLeakIgnores()...)
	b := harness.RabbitMQ(t)
	const prefix = "iqmq10."

	// The provisioner is opened PER NEED rather than once for the run, and
	// the reason is the dead-letter destination. A subscription's route is a
	// queue ARGUMENT, fixed when the queue is declared, and the 0-9-1
	// driver's `Config.DeadLetterTopic` is what decides where it points — so
	// a scenario whose Need names a dead-letter topic needs its queues
	// declared by a client configured for that topic. One client for the run
	// would declare every queue against the derived `<queue>.dlq`, and the
	// dead-letter probe would then wait for a message that went somewhere it
	// cannot read. A connection per scenario for three declarations is the
	// price.
	provisioner := func(ctx context.Context, n testkit.Need) error {
		cfg := rq.Config{
			Addrs:          []string{b.Addr},
			Username:       b.Auth.User,
			Password:       dsn.Secret(b.Auth.Password),
			Prefix:         prefix,
			Declare:        true,
			ConnectionName: "iqmq-conformance-amqp10-provisioner",
		}
		if n.DeadLetterTopic != "" {
			cfg.DeadLetter, cfg.DeadLetterTopic = true, n.DeadLetterTopic
		}
		admin, err := rq.Open(ctx, cfg)
		if err != nil {
			return err
		}
		defer func() { _ = admin.Close() }()
		adm, err := admin.Admin()
		if err != nil {
			return err
		}
		topics := []string{n.Topic}
		if n.DeadLetterTopic != "" {
			topics = append(topics, n.DeadLetterTopic)
		}
		for _, topic := range topics {
			if topic == "" {
				continue
			}
			if err := adm.EnsureTopic(ctx, mq.TopicSpec{Name: topic}); err != nil {
				return err
			}
			for _, sub := range n.Subscriptions {
				if sub == "" {
					continue
				}
				if err := adm.EnsureSubscription(ctx, mq.SubscriptionSpec{Topic: topic, Name: sub}); err != nil {
					return err
				}
			}
		}
		return nil
	}

	h := rq10.ConformanceHarnessWithProvisioner(
		rq10.Config{
			Addrs:      []string{b.Addr},
			Username:   b.Auth.User,
			Password:   dsn.Secret(b.Auth.Password),
			Prefix:     prefix,
			DeadLetter: true,
			// The pinned container speaks plain AMQP on 5672, so SASL PLAIN
			// is in the clear and the profile refuses it unless the caller
			// says so deliberately. A conformance run against a container on
			// a loopback port is exactly the deliberate case, and saying it
			// here rather than weakening the default is the point of the
			// flag.
			InsecurePlainSASL: true,
		},
		testkit.EvidenceContainer,
		testkit.Broker{Image: b.Image, Version: b.Version},
		provisioner,
	)
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
	path, serr := rep.Save(dir)
	if serr != nil {
		t.Fatalf("saving the report: %v", serr)
	}
	t.Logf("report written to %s", path)
}
