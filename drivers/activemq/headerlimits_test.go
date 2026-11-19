package activemq_test

import (
	"bytes"
	"context"
	"errors"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/Azure/go-amqp"

	mq "github.com/iqhive/iqmq"
	amq "github.com/iqhive/iqmq/drivers/activemq"
	core "github.com/iqhive/iqmq/drivers/amqp10"
	"github.com/iqhive/iqmq/dsn"
	"github.com/iqhive/iqmq/internal/activemq"
	"github.com/iqhive/iqmq/testkit"
)

// TestASizeRefusalIsMessageTooLarge is the broker's own answer to a message
// too large for its store, now that the profile refuses nothing before I/O.
//
// Neither bound is one the driver can know in advance. Artemis refuses a
// durable message whose encoded record is over its journal's buffer
// (`journal-buffer-size`, 490 KiB unless configured), and Classic a
// persistent message with a string over 65535 octets in a field its store
// writes as Java modified UTF-8: an application-property key (one sentence),
// or the message key's `group-id`, the correlation ID, the message ID or the
// subject (another). All answer with a `rejected{failed}` disposition,
// measured on Artemis 2.44.0 and Classic 6.2.0 with the sentences here, which
// the core reads as Unknown. It is MessageTooLarge and not retryable, since the
// same message meets the same bound. Classic's second sentence comes from a
// store write it makes while it dispatches, and the message it rejects reaches
// a consumer anyway (measured, every time), so that one is ambiguous; the other
// two are definite.
func TestASizeRefusalIsMessageTooLarge(t *testing.T) {
	t.Parallel()
	classic := artemisConfig()
	classic.Broker = activemq.Classic
	for _, tc := range []struct {
		name      string
		cfg       amq.Config
		props     map[string]any
		sentence  string
		ambiguous bool
	}{
		{
			"artemis", artemisConfig(), artemisProps(), "AMQ149005: Message of 1048818 bytes is bigger than the max " +
				"record size of 501760 bytes. You should try to move large application properties to the message body.",
			false,
		},
		{"classic, a property key", classic, nil, "encoded string (kkkkkkkk...kkkkkkkk) too long: 65536 bytes", false},
		{
			"classic, a typed field", classic, nil,
			"java.io.UTFDataFormatException: encoded string too long: 65536 bytes", true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			b, p := openPeer(t, tc.cfg, tc.props)
			snk := openSink(t, b, "orders", mq.ConfirmAccepted)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			p.PlanSendStates(&amqp.StateRejected{Error: &amqp.Error{Condition: "failed", Description: tc.sentence}})
			err := snk.Publish(ctx, &mq.Message{Body: []byte("large")}, nil)
			if mq.CodeOf(err) != mq.CodeMessageTooLarge || mq.IsAmbiguous(err) != tc.ambiguous || mq.IsRetryable(err) {
				t.Errorf("the broker's size refusal is %v (code %v, ambiguous %v, retryable %v), want "+
					"MessageTooLarge, ambiguous %v and not retryable", err, mq.CodeOf(err), mq.IsAmbiguous(err),
					mq.IsRetryable(err), tc.ambiguous)
			}
			// Not retryable for a caller who de-duplicates either: the same
			// message meets the same bound.
			if oe, ok := errors.AsType[*mq.OpError](err); !ok || oe.Retryable {
				t.Errorf("the broker's size refusal is %#v, want an OpError that is not Retryable", err)
			}
		})
	}
}

// TestHeadersReadBackOnABroker publishes, through the driver, headers the
// profile used to refuse, and reads each back from the broker that
// IQMQ_ARTEMIS_ADDR or IQMQ_ACTIVEMQ_CLASSIC_ADDR names, with the credentials
// TestConformance reads.
//
// Up to v1.1.2 the profile advertised MaxHeaderBytes = 255, which the core
// holds the whole header set to, so every message here was refused with
// MessageTooLarge. Neither product bounds an application property near
// there. What each does bound is in its store, for a durable message, which
// this driver publishes unless Config.NonDurable: Artemis a record over its
// journal's buffer (501760 bytes in the pinned image) and Classic a key over
// 65535 octets. Each refuses one with a `rejected` disposition, which the
// driver reports as MessageTooLarge, and the connection goes on serving.
func TestHeadersReadBackOnABroker(t *testing.T) {
	addr := os.Getenv("IQMQ_ARTEMIS_ADDR")
	broker := activemq.Artemis
	if addr == "" {
		addr, broker = os.Getenv("IQMQ_ACTIVEMQ_CLASSIC_ADDR"), activemq.Classic
	}
	if addr == "" {
		t.Skip("set IQMQ_ARTEMIS_ADDR or IQMQ_ACTIVEMQ_CLASSIC_ADDR (and IQMQ_ACTIVEMQ_USER, " +
			"IQMQ_ACTIVEMQ_PASSWORD) to run this against a broker; what it carries is the question")
	}
	cfg := amq.Config{
		Broker:            broker,
		Addrs:             []string{addr},
		Username:          os.Getenv("IQMQ_ACTIVEMQ_USER"),
		Password:          dsn.Secret(os.Getenv("IQMQ_ACTIVEMQ_PASSWORD")),
		InsecurePlainSASL: true,
		Prefix:            "iqmq.headers." + strconv.FormatInt(time.Now().UnixNano(), 36) + ".",
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if err := amq.Provision(ctx, cfg, testkit.Need{Topic: "orders", Subscriptions: []string{"workers"}}); err != nil {
		t.Fatalf("Provision: %v", err)
	}
	b, err := amq.OpenBackend(ctx, cfg)
	if err != nil {
		t.Fatalf("OpenBackend: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })
	src := openSource(t, b, "orders", "workers", 8)
	if err := core.WarmForTest(ctx, b); err != nil {
		t.Fatalf("attaching the receiver: %v", err)
	}
	snk := openSink(t, b, "orders", mq.ConfirmAccepted)

	traced := &mq.Message{Body: []byte("traced")}
	traced.Headers.Add("traceparent", []byte("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"))
	traced.Headers.Add("baggage", []byte(strings.Repeat("v", 300)))
	several := &mq.Message{Body: []byte("several")}
	for i := range 4 {
		several.Headers.Add("header-"+strconv.Itoa(i), []byte(strings.Repeat("v", 62)))
	}
	long := &mq.Message{Body: []byte("long")}
	long.Headers.Add(strings.Repeat("k", 65535), []byte(strings.Repeat("v", 400000)))
	var published []*mq.Message
	for _, m := range []*mq.Message{traced, several, long} {
		if err := snk.Publish(ctx, m, nil); err != nil {
			t.Errorf("publishing %q with %d bytes of headers: %v", m.Body, headerBytes(m), err)
			continue
		}
		published = append(published, m)
	}
	stored := &mq.Message{Body: []byte("over")}
	if broker == activemq.Classic {
		stored.Headers.Add(strings.Repeat("k", 65536), []byte("v"))
	} else {
		stored.Headers.Add("k", []byte(strings.Repeat("v", 502000)))
	}
	err = snk.Publish(ctx, stored, nil)
	if mq.CodeOf(err) != mq.CodeMessageTooLarge || mq.IsAmbiguous(err) || mq.IsRetryable(err) {
		t.Errorf("publishing a message over the store's bound = %.300v (code %v, ambiguous %v), want "+
			"MessageTooLarge, definite and not retryable", err, mq.CodeOf(err), mq.IsAmbiguous(err))
	}
	after := &mq.Message{Body: []byte("after")}
	if err := snk.Publish(ctx, after, nil); err != nil {
		t.Errorf("publishing after the store's refusal: %v", err)
	} else {
		published = append(published, after)
	}
	for _, m := range published {
		d := fetchOne(t, src, 20*time.Second)
		if !bytes.Equal(d.Message.Body, m.Body) {
			t.Errorf("the delivery is %q, want %q", d.Message.Body, m.Body)
		}
		for k, v := range m.Headers.All() {
			if back, _ := d.Message.Headers.Get(k); !bytes.Equal(back, v) {
				t.Errorf("%q: the header with a %d-byte key came back as %d bytes, want the %d published", m.Body,
					len(k), len(back), len(v))
			}
		}
		if err := src.Settler().Ack(ctx, &d); err != nil {
			t.Errorf("Ack: %v", err)
		}
	}
}

// TestAClassicStoreRefusalOfATypedFieldIsAmbiguousOnABroker publishes durable
// messages whose message key, correlation ID or message ID is over the 65535
// octets Classic's store holds, to the broker IQMQ_ACTIVEMQ_CLASSIC_ADDR names,
// through a JMS queue with a consumer attached.
//
// Classic answers each with `rejected`, "java.io.UTFDataFormatException:
// encoded string too long", from the store write it makes while it dispatches,
// and the consumer receives the message anyway: measured, every time on a JMS
// queue, and on one of a virtual topic's two subscriptions. Up to round 2 of the
// hunt 9 fixes the driver reported it as a definite Unknown, which told a caller
// the message had gone nowhere. It is MessageTooLarge and ambiguous.
func TestAClassicStoreRefusalOfATypedFieldIsAmbiguousOnABroker(t *testing.T) {
	addr := os.Getenv("IQMQ_ACTIVEMQ_CLASSIC_ADDR")
	if addr == "" {
		t.Skip("set IQMQ_ACTIVEMQ_CLASSIC_ADDR (and IQMQ_ACTIVEMQ_USER, IQMQ_ACTIVEMQ_PASSWORD) to run this " +
			"against ActiveMQ Classic; what its store does is the question")
	}
	cfg := amq.Config{
		Broker:            activemq.Classic,
		Addrs:             []string{addr},
		Username:          os.Getenv("IQMQ_ACTIVEMQ_USER"),
		Password:          dsn.Secret(os.Getenv("IQMQ_ACTIVEMQ_PASSWORD")),
		InsecurePlainSASL: true,
		Addressing:        amq.AddressQueue,
		Prefix:            "iqmq.typed." + strconv.FormatInt(time.Now().UnixNano(), 36) + ".",
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if err := amq.Provision(ctx, cfg, testkit.Need{Topic: "orders", Subscriptions: []string{"workers"}}); err != nil {
		t.Fatalf("Provision: %v", err)
	}
	b, err := amq.OpenBackend(ctx, cfg)
	if err != nil {
		t.Fatalf("OpenBackend: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })
	src := openSource(t, b, "orders", "workers", 8)
	if err := core.WarmForTest(ctx, b); err != nil {
		t.Fatalf("attaching the receiver: %v", err)
	}
	snk := openSink(t, b, "orders", mq.ConfirmAccepted)
	for what, m := range map[string]*mq.Message{
		"a 65536-byte message key":    {Body: []byte("key"), Key: []byte(strings.Repeat("k", 65536))},
		"a 70000-byte correlation ID": {Body: []byte("correlation"), Properties: mq.Properties{CorrelationID: strings.Repeat("c", 70000)}},
		"a 65536-byte message ID":     {Body: []byte("id"), ID: strings.Repeat("i", 65536)},
	} {
		err := snk.Publish(ctx, m, nil)
		if mq.CodeOf(err) != mq.CodeMessageTooLarge || !mq.IsAmbiguous(err) || mq.IsRetryable(err) {
			t.Errorf("publishing a message with %s = %.300v (code %v, ambiguous %v), want MessageTooLarge, ambiguous "+
				"and not retryable", what, err, mq.CodeOf(err), mq.IsAmbiguous(err))
		}
	}
	fctx, fcancel := context.WithTimeout(ctx, 3*time.Second)
	defer fcancel()
	dst := make([]mq.DriverDelivery, 4)
	n, _ := src.Fetch(fctx, dst)
	for i := range n {
		t.Logf("the consumer received %q, which the broker had rejected", dst[i].Message.Body)
		_ = src.Settler().Ack(ctx, &dst[i])
	}
}
