package stomp_test

import (
	"context"
	"os"
	"strconv"
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
	stomp "github.com/iqhive/iqmq/drivers/activemq/stomp"
	"github.com/iqhive/iqmq/dsn"
	"github.com/iqhive/iqmq/internal/activemq"
)

// The real-broker twins of foreignqueue_test.go. They need an Artemis whose
// auto-creation is on, because the topology the collision is made of has to
// be created, and STOMP has no frame that creates it except a SUBSCRIBE or a
// SEND to a destination that does not exist yet (conformance_test.go says the
// same of its own run).

// artemisAutoCreating is the Artemis IQMQ_ARTEMIS_ADDR names, with a prefix of
// the run's own, or a skip.
func artemisAutoCreating(t *testing.T) stomp.Config {
	t.Helper()
	addr := os.Getenv("IQMQ_ARTEMIS_ADDR")
	if addr == "" || os.Getenv("IQMQ_ARTEMIS_AUTOCREATE") == "" {
		t.Skip("set IQMQ_ARTEMIS_ADDR (and IQMQ_ARTEMIS_USER, IQMQ_ARTEMIS_PASSWORD) and IQMQ_ARTEMIS_AUTOCREATE=1 " +
			"to run this against an Artemis whose auto-creation is on; what the broker attaches to is the question")
	}
	cfg := stomp.Config{
		Broker:         activemq.Artemis,
		Addrs:          []string{addr},
		Login:          envOr("IQMQ_ARTEMIS_USER", "artemis"),
		ReceiptTimeout: 5 * time.Second,
		// A prefix of the run's own, so that the two applications collide
		// with each other and with nothing a previous run left.
		Prefix: "iqmq.collision." + strconv.FormatInt(time.Now().UnixNano(), 36) + ".",
	}
	if pw := os.Getenv("IQMQ_ARTEMIS_PASSWORD"); pw != "" {
		cfg.Passcode = dsn.Secret(pw)
		cfg.InsecurePlainLogin = true
	}
	return cfg
}

// collisionBackend opens a backend and closes it with the test.
func collisionBackend(ctx context.Context, t *testing.T, cfg stomp.Config) *stomp.Backend {
	t.Helper()
	b, err := stomp.OpenBackend(ctx, cfg)
	if err != nil {
		t.Fatalf("OpenBackend: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })
	return b
}

// publishTo publishes one message to topic and waits for the broker's receipt.
func publishTo(ctx context.Context, t *testing.T, b *stomp.Backend, topic, body string) {
	t.Helper()
	snk, err := b.OpenSink(ctx, mq.SinkConfig{Topic: topic, Confirmation: mq.ConfirmAccepted})
	if err != nil {
		t.Fatalf("OpenSink(%s): %v", topic, err)
	}
	defer func() { _ = snk.Close() }()
	if err := snk.Publish(ctx, &mq.Message{Body: []byte(body)}, nil); err != nil {
		t.Fatalf("Publish(%s): %v", topic, err)
	}
}

// fetchFrom opens a source and fetches once within d.
func fetchFrom(ctx context.Context, t *testing.T, b *stomp.Backend, topic, sub string,
	d time.Duration,
) (mq.Source, []mq.DriverDelivery, error) {
	t.Helper()
	src, err := b.OpenSource(ctx, mq.SourceConfig{Topic: topic, Subscription: sub, Prefetch: 4})
	if err != nil {
		t.Fatalf("OpenSource(%s, %s): %v", topic, sub, err)
	}
	t.Cleanup(func() { _ = src.Close() })
	fctx, cancel := context.WithTimeout(ctx, d)
	defer cancel()
	dst := make([]mq.DriverDelivery, 1)
	n, err := src.Fetch(fctx, dst)
	return src, dst[:n], err
}

// TestArtemisSubscriptionDoesNotConsumeAnotherApplicationsQueue is the
// collision both ways round on the pinned Artemis.
//
// A fan-out subscription "orders"/"billing" SUBSCRIBEs to
// `orders::orders.billing`, and Artemis attached it to another application's
// JMS queue `orders.billing` on its own address: the source handed out and
// acknowledged that application's messages. An AddressQueue consumer of topic
// "orders.billing" did the same to the subscription's queue. Each now fails
// with a Conflict, and the message is still there for its owner.
func TestArtemisSubscriptionDoesNotConsumeAnotherApplicationsQueue(t *testing.T) {
	cfg := artemisAutoCreating(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	qcfg := cfg
	qcfg.Addressing = stomp.AddressQueue
	fanout, jms := collisionBackend(ctx, t, cfg), collisionBackend(ctx, t, qcfg)

	t.Run("fan-out subscription over a JMS queue", func(t *testing.T) {
		publishTo(ctx, t, jms, "orders.billing", "an invoice for the billing service")
		src, got, err := fetchFrom(ctx, t, fanout, "orders", "billing", 3*time.Second)
		if len(got) > 0 {
			t.Fatalf("topic orders subscription billing (%s) received %q, a message another application sent to "+
				"its own JMS queue; Ack: %v", src.(*stomp.Source).Destination(), got[0].Message.Body,
				src.Settler().Ack(ctx, &got[0]))
		}
		if code := mq.CodeOf(err); code != mq.CodeConflict || mq.IsRetryable(err) {
			t.Errorf("the fetch returned %v (code %v), want a Conflict that is not retryable", err, code)
		}
		_, owned, err := fetchFrom(ctx, t, jms, "orders.billing", "billing", 5*time.Second)
		if len(owned) != 1 || string(owned[0].Message.Body) != "an invoice for the billing service" {
			t.Fatalf("the JMS queue's own consumer got %d messages (%v), want the invoice back", len(owned), err)
		}
		if owned[0].Info.Redelivered {
			t.Errorf("the invoice came back to its owner marked redelivered: the refused subscription counted " +
				"a delivery attempt against it")
		}
	})

	t.Run("fan-out subscription named like its topic over a JMS queue", func(t *testing.T) {
		// With no Prefix the JMS queue `<t>.<t>` sits on an address that ends
		// with the topic's address `<t>`, which a check that accepts any text
		// in front of the address (an acceptor's prefix) took for the
		// subscription's own. The run's nonce goes in the topic instead.
		bare, qbare := cfg, qcfg
		bare.Prefix, qbare.Prefix = "", ""
		topic := "amq" + strconv.FormatInt(time.Now().UnixNano(), 36)
		bfan, bjms := collisionBackend(ctx, t, bare), collisionBackend(ctx, t, qbare)
		publishTo(ctx, t, bjms, topic+"."+topic, "an invoice for the "+topic+" service")
		src, got, err := fetchFrom(ctx, t, bfan, topic, topic, 3*time.Second)
		if len(got) > 0 {
			t.Fatalf("topic %s subscription %s (%s) received %q from another application's JMS queue; Ack: %v",
				topic, topic, src.(*stomp.Source).Destination(), got[0].Message.Body, src.Settler().Ack(ctx, &got[0]))
		}
		if code := mq.CodeOf(err); code != mq.CodeConflict || mq.IsRetryable(err) {
			t.Errorf("the fetch returned %v (code %v), want a Conflict that is not retryable", err, code)
		}
		_, owned, err := fetchFrom(ctx, t, bjms, topic+"."+topic, "x", 5*time.Second)
		if len(owned) != 1 {
			t.Fatalf("the JMS queue's own consumer got %d messages (%v), want the invoice back", len(owned), err)
		}
	})

	t.Run("AddressQueue consumer over a subscription's queue", func(t *testing.T) {
		// The subscription's queue, made by its own SUBSCRIBE and left
		// holding one publication once its consumer has gone.
		sub, _, err := fetchFrom(ctx, t, fanout, "orders2", "billing", 200*time.Millisecond)
		if err != nil && mq.CodeOf(err) != mq.CodeDeadlineExceeded {
			t.Fatalf("the subscription's first fetch: %v", err)
		}
		if err := sub.Close(); err != nil {
			t.Fatalf("closing the subscription's source: %v", err)
		}
		publishTo(ctx, t, fanout, "orders2", "a fan-out message for billing")
		src, got, err := fetchFrom(ctx, t, jms, "orders2.billing", "x", 3*time.Second)
		if len(got) > 0 {
			t.Fatalf("the AddressQueue consumer of orders2.billing (%s) received %q from the subscription's "+
				"queue; Ack: %v", src.(*stomp.Source).Destination(), got[0].Message.Body,
				src.Settler().Ack(ctx, &got[0]))
		}
		if code := mq.CodeOf(err); code != mq.CodeConflict || mq.IsRetryable(err) {
			t.Errorf("the fetch returned %v (code %v), want a Conflict that is not retryable", err, code)
		}
	})
}
