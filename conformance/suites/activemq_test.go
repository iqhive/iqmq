//go:build integration

package suites

import (
	"context"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"go.uber.org/goleak"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/conformance/harness"
	amq "github.com/iqhive/iqmq/drivers/activemq"
	"github.com/iqhive/iqmq/dsn"
	"github.com/iqhive/iqmq/internal/activemq"
	"github.com/iqhive/iqmq/testkit"
)

// The four ActiveMQ suites, and why there are four.
//
// This file holds the two AMQP 1.0 suites; `activemq_stomp_test.go` holds the
// two STOMP ones.

// activemqPrefix keeps every suite's topology under one name, so that a run
// that died leaves something an operator can find and delete.
const activemqPrefix = "iqmq."

// artemisPassword is the password the Artemis suites start the broker with.
//
// It is not the image's default, and the reason is a failure worth
// reading: the default is `artemis`, which is also this driver's PROFILE NAME,
// so `secrets/redaction` failed on `DriverInfo` -- the string
// "activemq artemis github.com/Azure/go-amqp v1.7.0 2.44.0" contains the
// credential. Nothing leaked and the check was right anyway: a password that
// is a word the driver says about itself cannot be told from one it leaked,
// and a deployment using it cannot audit its own logs.
const artemisPassword = "iqmq-conformance-Zx7Q"

// classicPassword is the password the Classic suites start the broker with,
// and its default is a worse version of the same trap: the harness configures
// `iqmq`/`iqmq`, and "iqmq" is the prefix of every error string this library
// renders -- so `secrets/redaction` failed on "iqmq: invalid topic name".
const classicPassword = "iqmq-conformance-Yw4R"

// classicBroker starts Classic the way every Classic suite here needs it: the
// message scheduler on, prioritised dispatch on its queues, and a password
// that is not also a word this library prints. All three are things the image
// does not do and does not say it does not do.
func classicBroker(t *testing.T) *harness.Broker {
	t.Helper()
	b := harness.ActiveMQClassic(t,
		harness.ActiveMQClassicWithScheduler(),
		harness.ActiveMQClassicPassword(classicPassword))
	if !b.External {
		b.Auth.Password = classicPassword
	}
	return b
}

// artemisBroker starts Artemis the way every Artemis suite here needs it:
// auto-creation off, and a password that is not also a word the driver says.
func artemisBroker(t *testing.T) *harness.Broker {
	t.Helper()
	b := harness.Artemis(t, harness.ArtemisNoAutoCreate(), harness.ArtemisPassword(artemisPassword))
	if !b.External {
		b.Auth.Password = artemisPassword
	}
	return b
}

// TestConformance_activemq_artemis runs the suite against Apache
// ActiveMQ Artemis over AMQP 1.0.
//
// **The broker's auto-creation is turned OFF**, and that is the point rather
// than tidiness. The specification makes implicit topology creation a defect, and Artemis
// creates an address from an attach unless it is told not to — so a driver
// that silently created topology would pass every scenario here and a
// deployment's typo would become an address that receives every publication
// and delivers nothing. With auto-creation off, a destination that does not
// exist is refused with `amqp:not-found`, which is what makes this driver's
// "creates no topology" claim provable. The topology every scenario needs is
// created deliberately, by the driver's own Admin — Artemis's management
// address over the same connection.
func TestConformance_activemq_artemis(t *testing.T) {
	defer goleak.VerifyNone(t, conformanceLeakIgnores()...)
	b := artemisBroker(t)
	cfg := amq.Config{
		Broker:   activemq.Artemis,
		Addrs:    []string{b.Addr},
		Username: b.Auth.User,
		Password: dsn.Secret(b.Auth.Password),
		Prefix:   activemqPrefix,
		// The pinned container speaks plain AMQP on 5672, so SASL PLAIN is
		// in the clear and the driver refuses it unless the caller says so
		// deliberately. A conformance run against a container on a loopback
		// port is exactly the deliberate case, and saying it here rather
		// than weakening the default is the point of the flag.
		InsecurePlainSASL: true,
		// The broker was started with --no-autocreate, so the driver's page
		// and its Profile.AutoCreates report what is true of this run.
		BrokerAutoCreates: false,
	}
	h := amq.ConformanceHarness(cfg, testkit.EvidenceContainer,
		testkit.Broker{Image: b.Image, Version: b.Version})
	t.Run("admin-queue-ownership", func(t *testing.T) { artemisAdminOwnership(t, b, cfg) })
	runActiveMQ(t, h)
}

// artemisAdminOwnership is what the driver's Admin destroys and what it
// reports as ensured on a broker that holds other applications' queues under
// names the driver derives.
//
// An Artemis queue name is unique across the whole broker, so a fan-out
// subscription's queue `orders.billing` and a JMS queue `orders.billing` on
// its own address cannot both exist, and a dead-letter queue `orders.dlq` or a
// JMS durable subscription `orders.audit` reads like one of topic orders'
// subscriptions. Admin asks the broker which address a queue is bound to before
// it destroys it or reports it as ensured. The reading of that answer has a
// table test in drivers/activemq (TestOwnershipOfReadsTheBrokersAnswer); this
// is the asking, against the pinned broker, which no unit test of the module
// can reach.
//
// Two Admins share the run's own prefix: a fan-out one, and an AddressQueue
// one standing for the other application, whose topics are JMS queues on
// addresses of their own names.
func artemisAdminOwnership(t *testing.T, b *harness.Broker, suite amq.Config) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	nonce := strconv.FormatInt(time.Now().UnixNano(), 36)
	fan := suite
	fan.Prefix = activemqPrefix + "own." + nonce + "."
	jms := fan
	jms.Addressing = amq.AddressQueue
	fanAdm, _ := ownershipClient(ctx, t, fan)
	jmsAdm, jmsClient := ownershipClient(ctx, t, jms)
	// Each delete is registered on the test that opened the Admin, after the
	// client's own close, so that it runs first: the cleanups of one test run
	// last in, first out, and a delete on a closed client deletes nothing.
	cleanup := func(t *testing.T, adm mq.Admin, topics ...string) {
		t.Cleanup(func() {
			for _, topic := range topics {
				_ = adm.DeleteTopic(context.WithoutCancel(ctx), topic)
			}
		})
	}
	cleanup(t, fanAdm, "orders", "invoices", "payments")
	cleanup(t, jmsAdm, "orders.dlq", "orders.audit", "orders", "ledger", "invoices.billing", "payments.billing")

	t.Run("DeleteTopic destroys only the queues on the topic's address", func(t *testing.T) {
		for _, sub := range []string{"workers", "billing"} {
			mustAdmin(t, "EnsureSubscription(orders, "+sub+")",
				fanAdm.EnsureSubscription(ctx, mq.SubscriptionSpec{Topic: "orders", Name: sub}))
		}
		for _, topic := range []string{"orders.dlq", "orders.audit"} {
			mustAdmin(t, "the other application's EnsureTopic("+topic+")",
				jmsAdm.EnsureTopic(ctx, mq.TopicSpec{Name: topic}))
		}
		if err := jmsClient.Publish(ctx, "orders.dlq", &mq.Message{Body: []byte("a dead letter")}); err != nil {
			t.Fatalf("publishing to the other application's queue orders.dlq: %v", err)
		}
		mustAdmin(t, "DeleteTopic(orders)", fanAdm.DeleteTopic(ctx, "orders"))
		for _, sub := range []string{"workers", "billing"} {
			if _, ok := queueDepth(ctx, t, fanAdm, "orders", sub); ok {
				t.Errorf("DeleteTopic(orders) left the subscription queue orders.%s, which is bound to orders", sub)
			}
		}
		if depth, ok := queueDepth(ctx, t, jmsAdm, "orders.dlq", "x"); !ok || depth != 1 {
			t.Errorf("after DeleteTopic(orders) the JMS queue orders.dlq exists=%v with %d messages, want it there "+
				"with its one message: it is bound to its own address, whatever its name says", ok, depth)
		}
		if _, ok := queueDepth(ctx, t, jmsAdm, "orders.audit", "x"); !ok {
			t.Error("DeleteTopic(orders) destroyed the JMS queue orders.audit, which is bound to its own address")
		}
	})

	t.Run("DeleteSubscription leaves a queue on another address alone", func(t *testing.T) {
		for _, sub := range []string{"dlq", "gone"} {
			if err := fanAdm.DeleteSubscription(ctx, "orders", sub); err != nil {
				t.Errorf("DeleteSubscription(orders, %s): %v, want nil: there is no such subscription", sub, err)
			}
		}
		if depth, ok := queueDepth(ctx, t, jmsAdm, "orders.dlq", "x"); !ok || depth != 1 {
			t.Errorf("after DeleteSubscription(orders, dlq) the JMS queue orders.dlq exists=%v with %d messages, "+
				"want it there with its one message", ok, depth)
		}
	})

	t.Run("an AddressQueue DeleteTopic checks the queue's address", func(t *testing.T) {
		// A fan-out subscription whose queue has the JMS queue's name: topic
		// <nonce> and subscription orders under the shorter prefix make the
		// queue `<prefix>orders` on the address `<prefix without its dot>`.
		outer := suite
		outer.Prefix = activemqPrefix + "own."
		outerAdm, _ := ownershipClient(ctx, t, outer)
		cleanup(t, outerAdm, nonce)
		mustAdmin(t, "EnsureSubscription("+nonce+", orders)",
			outerAdm.EnsureSubscription(ctx, mq.SubscriptionSpec{Topic: nonce, Name: "orders"}))
		mustAdmin(t, "DeleteTopic(orders) under AddressQueue", jmsAdm.DeleteTopic(ctx, "orders"))
		if _, ok := queueDepth(ctx, t, outerAdm, nonce, "orders"); !ok {
			t.Errorf("DeleteTopic(orders) under AddressQueue destroyed the queue %sorders, which is a subscription "+
				"bound to the address %s", fan.Prefix, strings.TrimSuffix(fan.Prefix, "."))
		}
		// And the JMS queue that is the topic's own goes, with its address.
		mustAdmin(t, "EnsureTopic(ledger) under AddressQueue", jmsAdm.EnsureTopic(ctx, mq.TopicSpec{Name: "ledger"}))
		mustAdmin(t, "DeleteTopic(ledger) under AddressQueue", jmsAdm.DeleteTopic(ctx, "ledger"))
		if _, ok := queueDepth(ctx, t, jmsAdm, "ledger", "x"); ok {
			t.Error("DeleteTopic(ledger) under AddressQueue left the JMS queue ledger, which is the topic's own")
		}
	})

	t.Run("EnsureSubscription is not satisfied by a queue on another address", func(t *testing.T) {
		mustAdmin(t, "the other application's EnsureTopic(invoices.billing)",
			jmsAdm.EnsureTopic(ctx, mq.TopicSpec{Name: "invoices.billing"}))
		err := fanAdm.EnsureSubscription(ctx, mq.SubscriptionSpec{Topic: "invoices", Name: "billing"})
		holder := fan.Prefix + "invoices.billing"
		switch code := mq.CodeOf(err); {
		case code != mq.CodeConflict:
			t.Errorf("EnsureSubscription(invoices, billing) over the JMS queue invoices.billing returned %v (code "+
				"%v), want Conflict: the subscription does not exist and its queue name is another application's",
				err, code)
		case !strings.Contains(err.Error(), "on address "+holder):
			t.Errorf("the Conflict %v does not name %s, the address that holds the queue", err, holder)
		}

		sub := mq.SubscriptionSpec{Topic: "payments", Name: "billing"}
		mustAdmin(t, "EnsureSubscription(payments, billing)", fanAdm.EnsureSubscription(ctx, sub))
		mustAdmin(t, "EnsureSubscription(payments, billing) a second time", fanAdm.EnsureSubscription(ctx, sub))
		err = jmsAdm.EnsureTopic(ctx, mq.TopicSpec{Name: "payments.billing"})
		if code := mq.CodeOf(err); code != mq.CodeConflict {
			t.Errorf("EnsureTopic(payments.billing) under AddressQueue over the subscription queue "+
				"payments.billing returned %v (code %v), want Conflict", err, code)
		}
	})
}

// ownershipClient opens cfg's client and its Admin, both closed with the test.
func ownershipClient(ctx context.Context, t *testing.T, cfg amq.Config) (mq.Admin, *mq.Client) {
	t.Helper()
	c, err := amq.Open(ctx, cfg)
	if err != nil {
		t.Fatalf("Open (prefix %s, addressing %v): %v", cfg.Prefix, cfg.Addressing, err)
	}
	t.Cleanup(func() { _ = c.Close() })
	adm, err := c.Admin()
	if err != nil {
		t.Fatalf("Admin: %v", err)
	}
	return adm, c
}

// mustAdmin stops the subtest when an Admin call that sets the scene fails.
func mustAdmin(t *testing.T, what string, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("%s: %v", what, err)
	}
}

// queueDepth is the depth of the queue topic and sub derive, and whether the
// broker has it. A queue it does not have is Artemis's AMQ229067, "Cannot find
// resource", for the queue's management resource.
func queueDepth(ctx context.Context, t *testing.T, adm mq.Admin, topic, sub string) (int64, bool) {
	t.Helper()
	st, err := adm.Stats(ctx, topic, sub)
	switch {
	case err == nil:
		return st.Depth.Value, true
	case strings.Contains(err.Error(), "AMQ229067"):
		return 0, false
	}
	t.Fatalf("Stats(%s, %s): %v", topic, sub, err)
	return 0, false
}

// TestConformance_activemq_classic runs the suite against Apache
// ActiveMQ Classic over AMQP 1.0.
//
// Classic has no address model, so a topic is a VIRTUAL TOPIC: a publication
// to `topic://VirtualTopic.<t>` is copied by the broker into every queue
// called `Consumer.<sub>.VirtualTopic.<t>`, and a subscription is therefore a
// queue whose name encodes the topic. Two consequences shape this run.
//
// The first is that a consumer queue must exist BEFORE the first publication:
// a virtual topic copies a publication into the queues that exist at that
// moment, so a queue created afterwards receives nothing. The driver's own
// `Provision` does it, by attaching a receiver and letting go again — which is
// the whole of Classic's topology API over this protocol.
//
// The second is that the broker's MESSAGE SCHEDULER has to be turned on for
// the delayed-publication claim to be true at all. It is off in the image's
// own configuration and its absence is silent: a publication carrying
// `AMQ_SCHEDULED_DELAY` is delivered immediately and the property travels
// through to the consumer. So the container is started with the scheduler on
// and `Config.Scheduler` says so; a client that did not would have the claim
// withdrawn from its table, which is the other half of the same honesty.
func TestConformance_activemq_classic(t *testing.T) {
	defer goleak.VerifyNone(t, conformanceLeakIgnores()...)
	b := classicBroker(t)
	cfg := amq.Config{
		Broker:            activemq.Classic,
		Addrs:             []string{b.Addrs["amqp"]},
		Username:          b.Auth.User,
		Password:          dsn.Secret(b.Auth.Password),
		Prefix:            activemqPrefix,
		InsecurePlainSASL: true,
		Scheduler:         true,
		// The container is started with prioritised dispatch on its queues,
		// which the image does not do: a virtual topic's consumer queues
		// are QUEUES and the image's only policy entry is for topics.
		Prioritised: true,
		// Classic creates a destination when a link attaches and cannot be
		// told not to without an authorization plugin, so the driver's page
		// reports what is true rather than what would be tidier.
		BrokerAutoCreates: true,
	}
	h := amq.ConformanceHarness(cfg, testkit.EvidenceContainer,
		testkit.Broker{Image: b.Image, Version: b.Version})
	runActiveMQ(t, h)
}

// runActiveMQ runs one harness and writes its report.
//
// It is shared by all four suites, and the three-way verdict in it is the one
// `drivers/nsq`'s suite established: a FAILING claim is an error, a PROVEN one
// is counted, an UNREFUTED one is a true denial of a GUARANTEE that this run
// happened not to see broken — which is not work outstanding, because no
// number of runs can observe a guarantee's absence — and anything else is
// UNVERIFIED and named in the log, because the generated matrix omits it
// silently and the driver page has to say why.
func runActiveMQ(t *testing.T, h testkit.Harness) {
	t.Helper()
	h.Commit = os.Getenv("IQMQ_COMMIT")
	rep := testkit.RunConformance(t, h)
	pass, fail, skip := rep.Counts()
	t.Logf("conformance %s: %d passed, %d failed, %d skipped, of %d scenarios",
		rep.Key(), pass, fail, skip, len(rep.Results))

	proven, unrefuted, unverified := 0, 0, 0
	for _, v := range rep.Verify() {
		switch {
		case v.Failed:
			t.Errorf("capability %s (%s) has a failing scenario", v.Feature, v.Claimed)
		case v.Proven():
			proven++
		case v.Unrefuted():
			unrefuted++
			t.Logf("claim UNREFUTED: %-22s %s (a guarantee's absence is not observable in one run)",
				v.Feature, v.Claimed)
		default:
			unverified++
			t.Logf("claim UNVERIFIED (positive %v, negative %v): %-22s %s",
				v.Positive, v.Negative, v.Feature, v.Claimed)
		}
	}
	t.Logf("capability claims: %d proved in both directions, %d unrefuted denials, %d unverified, of %d",
		proven, unrefuted, unverified, len(rep.Verify()))

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

// conformanceLeakIgnores are the goroutines testcontainers and net/http keep
// alive for their own reasons, which are neither this driver's nor iqmq's.
// artemisProvisioner creates the topology a Need names through
// `drivers/activemq`'s Admin, for a suite whose own driver has none.
//
// It is the STOMP suites' provisioner and it is here rather than there because
// the names it creates are the ones BOTH protocols derive: they come from
// `internal/activemq` either way, so a mistake in the addressing shows up as a
// scenario that cannot find its queue instead of as two topologies that happen
// to work separately.
func artemisProvisioner(b *harness.Broker, prefix string) func(context.Context, testkit.Need) error {
	return func(ctx context.Context, n testkit.Need) error {
		cfg := amq.Config{
			Broker:            activemq.Artemis,
			Addrs:             []string{b.Addr},
			Username:          b.Auth.User,
			Password:          dsn.Secret(b.Auth.Password),
			Prefix:            prefix,
			InsecurePlainSASL: true,
			ContainerID:       "iqmq-conformance-stomp-provisioner",
		}
		if n.DeadLetterTopic != "" {
			cfg.DeadLetterTopic = n.DeadLetterTopic
		}
		c, err := amq.Open(ctx, cfg)
		if err != nil {
			return err
		}
		defer func() { _ = c.Close() }()
		adm, err := c.Admin()
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
			for i := range max(1, len(n.Subscriptions)) {
				sub := n.Sub(i)
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
}
