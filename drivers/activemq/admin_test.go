package activemq_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	mq "github.com/iqhive/iqmq"
	amq "github.com/iqhive/iqmq/drivers/activemq"
	"github.com/iqhive/iqmq/internal/activemq"
)

// TestManagementRequestEncoding pins the encoding Artemis's management address
// turned out to need.
//
// It is a test rather than a comment because the encoding is not the obvious
// one and THREE were tried against the pinned image before one worked. A data
// section carrying the JSON bytes fails with
//
//	Error reading in simpleString, length=16777216 is greater than readableBytes=32
//
// and an AMQP value holding a real LIST fails the same way, because the broker
// decodes the body with its own SimpleString codec. What works is an AMQP
// value holding the JSON array as a STRING.
//
// A container proves it once; this proves it stays that way, and it is the
// only part of Admin a test without a broker can reach.
func TestManagementRequestEncoding(t *testing.T) {
	t.Parallel()
	value, props, err := amq.ManagementRequestForTest("broker", "createAddress", "reply-to-1", "iqmq.orders", "MULTICAST")
	if err != nil {
		t.Fatalf("ManagementRequestForTest: %v", err)
	}
	// The body is a STRING holding a JSON array, not a list and not bytes.
	if value == "" {
		t.Fatal("the request's body is empty; the broker decodes it with its own SimpleString codec and an AMQP " +
			"value of any other type fails with \"Error reading in simpleString\"")
	}
	var args []any
	if err := json.Unmarshal([]byte(value), &args); err != nil {
		t.Fatalf("the request's body %q is not a JSON array: %v", value, err)
	}
	if len(args) != 2 || args[0] != "iqmq.orders" || args[1] != "MULTICAST" {
		t.Errorf("the request's arguments are %v, want [iqmq.orders MULTICAST]", args)
	}
	// The two application properties are what route the request. A request
	// with neither is not a management request at all; a request with the
	// wrong resource reaches a different MBean.
	if got := props["_AMQ_ResourceName"]; got != "broker" {
		t.Errorf("_AMQ_ResourceName is %v, want \"broker\"", got)
	}
	if got := props["_AMQ_OperationName"]; got != "createAddress" {
		t.Errorf("_AMQ_OperationName is %v, want \"createAddress\"", got)
	}
}

// TestManagementRequestWithNoArguments is the case a naive encoder gets wrong:
// an operation with no arguments needs an EMPTY ARRAY and not a null.
//
// Measured: `getQueueNames` with a `null` body fails with "readerIndex(0) +
// length(1) exceeds writerIndex(0)", which is the broker reading a string it
// was not sent.
func TestManagementRequestWithNoArguments(t *testing.T) {
	t.Parallel()
	value, _, err := amq.ManagementRequestForTest("broker", "getQueueNames", "reply-to-1")
	if err != nil {
		t.Fatalf("ManagementRequestForTest: %v", err)
	}
	if strings.TrimSpace(value) != "[]" {
		t.Errorf("an operation with no arguments encodes its body as %q, want \"[]\": a null body makes the broker "+
			"read a string it was not sent and fail with \"readerIndex(0) + length(1) exceeds writerIndex(0)\"", value)
	}
}

// TestQueueConfigurationIsSpelledOut is what makes a subscription DURABLE and
// SHARED rather than the other thing.
//
// Artemis auto-creates a queue for a JMS topic subscriber with
// `purge-on-no-consumers: true`, which is a NON-durable subscription: the
// queue and everything in it go when the last consumer leaves. The durable
// shared subscription is the opposite, and the difference is four fields that
// a caller would never see -- so every one of them is stated rather than left
// to the broker's default.
func TestQueueConfigurationIsSpelledOut(t *testing.T) {
	t.Parallel()
	cfg := amq.Config{Broker: activemq.Artemis, Addrs: []string{"localhost:5672"}, Prefix: "iqmq."}
	value, err := amq.QueueConfigurationForTest(cfg, "orders", "workers")
	if err != nil {
		t.Fatalf("QueueConfigurationForTest: %v", err)
	}
	for field, why := range map[string]string{
		`"durable":true`:                "a subscription that is not durable loses its messages when the broker restarts",
		`"max-consumers":-1`:            "a queue with one consumer makes the \"two consumers of one subscription compete\" impossible",
		`"purge-on-no-consumers":false`: "true is what Artemis defaults to for an auto-created topic subscriber's queue, and it destroys the subscription when the last consumer leaves",
		`"auto-create-address":false`:   "a create that made its own address would be the implicit topology the specification refuses",
	} {
		if !strings.Contains(value, field) {
			t.Errorf("the queue configuration does not state %s: %s", field, why)
		}
	}
}

// TestAdminFailureNamesTheCallersTopicAndSubscription is the attribution of
// every Admin method's failure.
//
// The management calls classify a failure through the core's error table,
// whose only entry point this module can reach names a placeholder topic "t"
// and subscription "s". Every failure used to carry them, so a failed
// Purge("orders", "billing") sent whoever read it to a topic that does not
// exist. The scripted peer is not an *amqp.Conn, so every call here fails
// before any I/O, which is the failure each method has to name.
func TestAdminFailureNamesTheCallersTopicAndSubscription(t *testing.T) {
	t.Parallel()
	cfg := artemisConfig()
	b, _ := openPeer(t, cfg, artemisProps())
	c, err := amq.ClientForBackend(b, cfg)
	if err != nil {
		t.Fatalf("ClientForBackend: %v", err)
	}
	adm, err := c.Admin()
	if err != nil {
		t.Fatalf("Admin: %v", err)
	}
	ctx := context.Background()
	for _, tc := range []struct {
		name       string
		call       func() error
		topic, sub string
	}{
		{"EnsureTopic", func() error { return adm.EnsureTopic(ctx, mq.TopicSpec{Name: "orders"}) }, "orders", ""},
		{"DeleteTopic", func() error { return adm.DeleteTopic(ctx, "orders") }, "orders", ""},
		{"ListTopics", func() error {
			for _, err := range adm.ListTopics(ctx) {
				return err
			}
			return nil
		}, "", ""},
		{"EnsureSubscription", func() error {
			return adm.EnsureSubscription(ctx, mq.SubscriptionSpec{Topic: "orders", Name: "billing"})
		}, "orders", "billing"},
		{"DeleteSubscription", func() error { return adm.DeleteSubscription(ctx, "orders", "billing") }, "orders", "billing"},
		{"Purge", func() error { return adm.Purge(ctx, "orders", "billing") }, "orders", "billing"},
		{"Stats", func() error { _, err := adm.Stats(ctx, "orders", "billing"); return err }, "orders", "billing"},
	} {
		err := tc.call()
		var oe *mq.OpError
		if !errors.As(err, &oe) {
			t.Errorf("%s: the failure is %v (%T), want an *mq.OpError", tc.name, err, err)
			continue
		}
		if oe.Topic != tc.topic || oe.Subscription != tc.sub {
			t.Errorf("%s: the failure names topic %q subscription %q, want %q and %q: %v",
				tc.name, oe.Topic, oe.Subscription, tc.topic, tc.sub, err)
		}
	}
}
