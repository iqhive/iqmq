package stomp_test

import (
	"bytes"
	"context"
	"errors"
	"strconv"
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
	stomp "github.com/iqhive/iqmq/drivers/activemq/stomp"
	"github.com/iqhive/iqmq/drivers/activemq/stomp/internal/stompsrv"
	"github.com/iqhive/iqmq/internal/activemq"
	"github.com/iqhive/iqmq/testkit"
)

// brokerReadHeaders are a caller's headers under names, or with values, a
// broker acts on when they arrive on a SEND frame, with what each did when it
// was carried as an ordinary header up to v1.1.3. Measured through this driver
// under MetadataNativeStrict against apache/activemq-classic:6.2.0 with its
// scheduler on and apache/activemq-artemis:2.44.0-alpine
// (TestBrokerReadHeadersOnABroker).
var brokerReadHeaders = []struct {
	name, value string
	// also is a second header the first needs to do anything, or none.
	also [2]string
	// since is the root module release whose shared mapping reserves the
	// name; a build against an older one carries it.
	since string
	// classic is a value only ActiveMQ Classic acts on: Artemis carries it
	// as an ordinary property, and the driver does too.
	classic bool
	// did is what the broker did with it.
	did string
}{
	{"AMQ_SCHEDULED_REPEAT", "3", [2]string{"AMQ_SCHEDULED_PERIOD", "200"}, "v1.1.4", false, "Classic delivered one publication four times"},
	{"AMQ_SCHEDULED_CRON", "0 0 1 1 *", [2]string{}, "v1.1.4", false, "Classic held the message until its schedule"},
	{"JMSExpiration", "1", [2]string{}, "v1.1.4", false, "Classic expired the message on arrival and receipted it"},
	{"destination-type", "ANYCAST", [2]string{}, "v1.1.4", false, "Artemis routed the message past every subscription of a fan-out topic"},
	{"transaction", "tx1", [2]string{}, "v1.1.4", false, "Artemis receipted the message and never delivered it; Classic closed the connection"},
	{"JMS_AMQP_NATIVE", "true", [2]string{}, "v1.1.4", false, "Classic failed every AMQP 1.0 consumer of the subscription"},
	{"JMSPriority", "high", [2]string{}, "v1.1.4", true, "Classic closed the connection"},
	{"JMSXGroupSeq", "abc", [2]string{}, "v1.1.4", true, "Classic closed the connection"},
	{"JMSReplyTo", "", [2]string{}, "v1.1.4", true, "Classic closed the connection"},
	{"JMSReplyTo", "queue://a,topic://", [2]string{}, "v1.1.4", true, "Classic closed the connection"},
	{"JMSDeliveryMode", "true", [2]string{}, "v1.1.4", true, "Classic closed the connection"},
	{"JMSXDeliveryCount", "5", [2]string{}, "v1.1.4", false, "Classic delivered it as attempt 5, redelivered, on its first delivery"},
	{"JMSRedelivered", "true", [2]string{}, "v1.1.4", false, "Classic delivered it as attempt 2, redelivered, on its first delivery"},
	{"JMSXGroupID", "g", [2]string{}, "", false, "both read it as the message group"},
	{"_AMQ_DUPL_ID", "d", [2]string{}, "", false, "Artemis read it as its de-duplication ID"},
}

// jmsHeadersAsBefore are JMS names a caller sets on purpose, which worked, or
// did no harm, before the reserved lists grew and have to go on doing so.
// Measured on both pinned images: `JMSType`, `JMSCorrelationID`,
// `JMSReplyTo` and `JMSTimestamp` set the JMS type, correlation ID, reply-to
// and creation time; Classic set the priority from `JMSPriority: 7`, ended a
// message group on `JMSXGroupSeq: -1` and published `JMSDeliveryMode: 1`
// non-persistently, which on a sink that does not promise durability is what
// a caller sets it for; and the rest were carried or taken without effect.
var jmsHeadersAsBefore = [][2]string{
	{"JMSType", "OrderCreated"},
	{"JMSCorrelationID", "c-1"},
	{"JMSReplyTo", "/queue/replies"},
	{"JMSPriority", "7"},
	{"JMSXGroupSeq", "-1"},
	{"JMSDeliveryMode", "2"},
	{"JMSDeliveryMode", "1"},
	{"JMSTimestamp", "1"},
	{"JMSXUserID", "producer"},
	{"JMS_vendor", "v"},
}

// TestANameTheBrokerReadsIsNotCarried publishes each of brokerReadHeaders in
// each metadata mode, to a server that answers as each broker, and reads what
// reached it and what came back.
//
// A header a broker acts on is refused before any I/O under
// MetadataNativeStrict, as the names the shared mapping reserved before were;
// MetadataPortableEnvelope and MetadataAdaptive carry it in the envelope, in the
// body, where no broker reads it, and it comes back as it went out; and
// MetadataLenientDrop leaves it out of the frame. Under MetadataAdaptive the
// older names were refused too, though the envelope carries them. A value only
// Classic acts on goes out natively to Artemis.
func TestANameTheBrokerReadsIsNotCarried(t *testing.T) {
	t.Parallel()
	for _, broker := range activemq.Brokers() {
		for _, h := range brokerReadHeaders {
			for _, mode := range []mq.MetadataMode{
				mq.MetadataNativeStrict, mq.MetadataPortableEnvelope, mq.MetadataAdaptive, mq.MetadataLenientDrop,
			} {
				t.Run(broker.String()+"/"+h.name+"/"+mode.String(), func(t *testing.T) {
					t.Parallel()
					if v, old := rootModuleBefore(t, h.since); h.since != "" && old {
						t.Skipf("this build uses root module %s, whose shared mapping carries %q", v, h.name)
					}
					srv := brokerServer(t, broker)
					cfg := settings(broker)
					cfg.Addrs = []string{srv.Addr()}
					cfg.Metadata = mode
					b := openBackend(t, cfg)
					src, consume := openSource(t, b, cfg, srv, "orders", "workers", 4)
					snk := openSink(t, b, "orders", mq.ConfirmAccepted)
					m := withHeader(withHeader(&mq.Message{Body: []byte("b")}, "tenant", "acme"), h.name, h.value)
					if h.also[0] != "" {
						m = withHeader(m, h.also[0], h.also[1])
					}
					err := snk.Publish(t.Context(), m, nil)
					sends := srv.FramesOf("SEND")
					if h.classic && broker != activemq.Classic {
						native := len(sends) == 1 && sends[0].Has(h.name) && sends[0].Get(h.name) == h.value
						if err != nil || len(sends) != 1 || mode != mq.MetadataPortableEnvelope && !native {
							t.Fatalf("%q = %q under %v: Publish = %v and %d SEND frames, want it carried as it was: "+
								"only Classic acts on that value", h.name, h.value, mode, err, len(sends))
						}
						return
					}
					if mode == mq.MetadataNativeStrict {
						if mq.CodeOf(err) != mq.CodeInvalidArgument || len(sends) != 0 {
							t.Fatalf("a caller's %q header under MetadataNativeStrict: Publish = %v and %d SEND frames, "+
								"want InvalidArgument before any I/O (%s)", h.name, err, len(sends), h.did)
						}
						return
					}
					if err != nil {
						t.Fatalf("a caller's %q header under %v: %v, want it published", h.name, mode, err)
					}
					if len(sends) != 1 {
						t.Fatalf("the server received %d SEND frames, want one", len(sends))
					}
					if sends[0].Has(h.name) || h.also[0] != "" && sends[0].Has(h.also[0]) {
						t.Fatalf("the SEND frame carries %q as a frame header under %v (%s)", h.name, mode, h.did)
					}
					enveloped := bytes.HasPrefix(sends[0].Body, []byte("IQMQ"))
					if enveloped != (mode != mq.MetadataLenientDrop) {
						t.Fatalf("under %v the SEND frame's body is %q, enveloped %v", mode, sends[0].Body, enveloped)
					}
					if mode == mq.MetadataLenientDrop {
						if sends[0].Get("tenant") != "acme" {
							t.Errorf("MetadataLenientDrop dropped the ordinary header as well: %v", sends[0].Headers())
						}
						return
					}
					publish, _ := destinations(t, cfg, "orders", "workers")
					if _, err := srv.Forward(publish, consume); err != nil {
						t.Fatal(err)
					}
					got := fetchN(t, src, 1)[0].Message
					if back, there := got.Headers.Get(h.name); string(back) != h.value || !there {
						t.Errorf("under %v %q came back as %q (present %v), want %q from the envelope", mode, h.name,
							back, there, h.value)
					}
					if tenant, _ := got.Headers.Get("tenant"); string(tenant) != "acme" {
						t.Errorf("the ordinary header came back as %q", tenant)
					}
				})
			}
		}
	}
}

// TestJMSHeadersGoOutAsBefore publishes each of jmsHeadersAsBefore under
// MetadataNativeStrict and MetadataAdaptive, to a server that answers as each
// broker, and reads the SEND frame: each goes out natively, as it did before.
func TestJMSHeadersGoOutAsBefore(t *testing.T) {
	t.Parallel()
	for _, broker := range activemq.Brokers() {
		for _, mode := range []mq.MetadataMode{mq.MetadataNativeStrict, mq.MetadataAdaptive} {
			t.Run(broker.String()+"/"+mode.String(), func(t *testing.T) {
				t.Parallel()
				srv := brokerServer(t, broker)
				cfg := settings(broker)
				cfg.Addrs = []string{srv.Addr()}
				cfg.Metadata = mode
				b := openBackend(t, cfg)
				snk := openSink(t, b, "orders", mq.ConfirmAccepted)
				for _, h := range jmsHeadersAsBefore {
					if err := snk.Publish(t.Context(), withHeader(&mq.Message{Body: []byte(h[0])}, h[0], h[1]), nil); err != nil {
						t.Errorf("%q = %q under %v: %v, want it published as before", h[0], h[1], mode, err)
					}
				}
				sends := srv.FramesOf("SEND")
				for i, h := range jmsHeadersAsBefore {
					if i >= len(sends) || !sends[i].Has(h[0]) || sends[i].Get(h[0]) != h[1] {
						t.Errorf("%q = %q under %v did not go out natively, as it did before", h[0], h[1], mode)
					}
				}
			})
		}
	}
}

// TestADurableSinkKeepsItsPersistence publishes `JMSDeliveryMode: 1` and `2`
// on a ConfirmDurable sink, in each metadata mode, to a server that answers as
// each broker.
//
// Classic sets the delivery mode from the header over the driver's own
// `persistent: true`, and measured on Classic 6.2.0, a message with
// `JMSDeliveryMode: 1` was receipted under ConfirmDurable and lost when the
// broker restarted. So on Classic a durable sink refuses it before any I/O
// under MetadataNativeStrict, envelopes it under MetadataAdaptive and drops it
// under MetadataLenientDrop. `2` goes out as before, and so does `1` to
// Artemis, which kept the message.
func TestADurableSinkKeepsItsPersistence(t *testing.T) {
	t.Parallel()
	for _, broker := range activemq.Brokers() {
		for _, mode := range []mq.MetadataMode{mq.MetadataNativeStrict, mq.MetadataAdaptive, mq.MetadataLenientDrop} {
			t.Run(broker.String()+"/"+mode.String(), func(t *testing.T) {
				t.Parallel()
				srv := brokerServer(t, broker)
				cfg := settings(broker)
				cfg.Addrs = []string{srv.Addr()}
				cfg.Metadata = mode
				b := openBackend(t, cfg)
				snk := openSink(t, b, "orders", mq.ConfirmDurable)
				for _, v := range []string{"1", "2"} {
					err := snk.Publish(t.Context(), withHeader(&mq.Message{Body: []byte(v)}, "JMSDeliveryMode", v), nil)
					refused := broker == activemq.Classic && v == "1" && mode == mq.MetadataNativeStrict
					if refused != (mq.CodeOf(err) == mq.CodeInvalidArgument) || !refused && err != nil {
						t.Errorf("JMSDeliveryMode: %s on a durable sink under %v: %v, want refused %v", v, mode, err, refused)
					}
				}
				for _, f := range srv.FramesOf("SEND") {
					native := !bytes.HasPrefix(f.Body, []byte("IQMQ"))
					carried := native && f.Get("JMSDeliveryMode") == string(f.Body)
					want := broker != activemq.Classic || string(f.Body) == "2"
					if native && carried != want || !native && (mode != mq.MetadataAdaptive || want) {
						t.Errorf("%v: the SEND frame went out enveloped %v with JMSDeliveryMode %q; want it carried "+
							"natively %v", mode, !native, f.Get("JMSDeliveryMode"), want)
					}
				}
			})
		}
	}
}

// TestADeliveryDoesNotHandBackANameTheBrokerReads delivers frames carrying the
// names a broker wrote or a JMS producer set, and reads the caller's headers.
//
// Up to v1.1.3 each reserved one came back as an application header, so a
// forwarder, or the runner's dead-letter copy, published it again:
// `AMQ_SCHEDULED_PERIOD` to Classic's scheduler, `destination-type` to
// Artemis's routing and `JMSXDeliveryCount` to Classic's delivery count. The
// JMS names whose republication does no harm still come back: `JMSXUserID`,
// which Classic adds for an authenticated producer and strips from a SEND,
// `JMSType` and a vendor's `JMS_` property.
func TestADeliveryDoesNotHandBackANameTheBrokerReads(t *testing.T) {
	t.Parallel()
	if v, old := rootModuleBefore(t, "v1.1.4"); old {
		t.Skipf("this build uses root module %s, whose shared mapping hands these back", v)
	}
	for _, broker := range activemq.Brokers() {
		t.Run(broker.String(), func(t *testing.T) {
			t.Parallel()
			srv := brokerServer(t, broker)
			cfg := settings(broker)
			cfg.Addrs = []string{srv.Addr()}
			b := openBackend(t, cfg)
			src, consume := openSource(t, b, cfg, srv, "orders", "workers", 4)
			kept := map[string]string{
				"JMSXUserID": "producer", "JMSType": "t1", "JMS_vendor": "v", "type": "t1", "tenant": "acme",
			}
			headers := make([][2]string, 0, 9+len(kept))
			headers = append(headers,
				[2]string{"AMQ_SCHEDULED_PERIOD", "200"},
				[2]string{"AMQ_SCHEDULED_DELAY", "300"},
				[2]string{"scheduledJobId", "j1"},
				[2]string{"_AMQ_SCHED_DELIVERY", "1795399107882"},
				[2]string{"destination-type", "MULTICAST"},
				[2]string{"JMS_AMQP_NATIVE", "true"},
				[2]string{"JMSExpiration", "1"},
				[2]string{"JMSXDeliveryCount", "5"},
				[2]string{"JMSRedelivered", "true"},
			)
			for k, v := range kept {
				headers = append(headers, [2]string{k, v})
			}
			deliver(t, srv, consume, stompsrv.Message{Body: []byte("b"), Headers: headers})
			got := fetchN(t, src, 1)[0].Message
			for k, v := range got.Headers.All() {
				if kept[k] != string(v) {
					t.Errorf("the delivery handed the caller %q = %q, a name the broker reads", k, v)
				}
			}
			if got.Headers.Len() != len(kept) {
				t.Errorf("the delivery has %d headers, want the %d whose republication does no harm",
					got.Headers.Len(), len(kept))
			}
		})
	}
}

// TestBrokerReadHeadersOnABroker publishes each of brokerReadHeaders to the
// brokers IQMQ_ACTIVEMQ_CLASSIC_ADDR (with its scheduler on) and
// IQMQ_ARTEMIS_ADDR name, under MetadataNativeStrict and then
// MetadataAdaptive, with a control message after each; and then each of
// jmsHeadersAsBefore under MetadataNativeStrict.
//
// Each Strict publication of a header the broker acts on is refused before any
// I/O and the connection is the same one afterwards; each Adaptive one is
// delivered once, with the header as it was published, and the control after
// it once. A value only Classic acts on is carried to Artemis natively. And
// each JMS header that worked before still does: `JMSType` arrives as the JMS
// type and `JMSCorrelationID` as the correlation ID, on both brokers.
func TestBrokerReadHeadersOnABroker(t *testing.T) {
	for name, cfg := range brokersAt(t) {
		t.Run(name, func(t *testing.T) {
			if v, old := rootModuleBefore(t, "v1.1.4"); old {
				t.Skipf("this build uses root module %s, whose shared mapping carries most of these", v)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
			defer cancel()
			for i, h := range brokerReadHeaders {
				for _, mode := range []mq.MetadataMode{mq.MetadataNativeStrict, mq.MetadataAdaptive} {
					c := cfg
					c.Metadata = mode
					c.Prefix += strconv.Itoa(i) + mode.String() + "."
					m := withHeader(&mq.Message{Body: []byte("named")}, h.name, h.value)
					if h.also[0] != "" {
						m = withHeader(m, h.also[0], h.also[1])
					}
					got := publishAndRead(ctx, t, c, m, h.name, h.did)
					switch {
					case h.classic && cfg.Broker != activemq.Classic, mode != mq.MetadataNativeStrict:
						// Artemis takes a JMSReplyTo as the reply-to rather than
						// carrying it as a header.
						back := got.header
						if h.name == "JMSReplyTo" && h.classic && cfg.Broker == activemq.Artemis {
							back = got.replyTo
						}
						if got.err != nil || got.counts["named"] != 1 || back != h.value {
							t.Errorf("%s under %v: Publish = %v, %d deliveries, header %q; want one delivery with %q "+
								"(%s)", h.name, mode, got.err, got.counts["named"], got.header, h.value, h.did)
						}
					default:
						if mq.CodeOf(got.err) != mq.CodeInvalidArgument || got.counts["named"] != 0 {
							t.Errorf("%s under MetadataNativeStrict: Publish = %v and %d deliveries, want "+
								"InvalidArgument before any I/O (%s)", h.name, got.err, got.counts["named"], h.did)
						}
					}
				}
			}
			for i, h := range jmsHeadersAsBefore {
				c := cfg
				c.Prefix += "before" + strconv.Itoa(i) + "."
				got := publishAndRead(ctx, t, c, withHeader(&mq.Message{Body: []byte("named")}, h[0], h[1]), "type",
					"it worked before")
				if got.err != nil || got.counts["named"] != 1 {
					t.Errorf("%s = %q under MetadataNativeStrict: Publish = %v and %d deliveries, want it published "+
						"and delivered once, as before", h[0], h[1], got.err, got.counts["named"])
				}
				switch h[0] {
				case "JMSType":
					if got.header != h[1] {
						t.Errorf("JMSType arrived as the JMS type %q, want %q", got.header, h[1])
					}
				case "JMSCorrelationID":
					if got.correlation != h[1] {
						t.Errorf("JMSCorrelationID arrived as the correlation ID %q, want %q", got.correlation, h[1])
					}
				}
			}
		})
	}
}

// readBack is what [publishAndRead] saw.
type readBack struct {
	// err is the publication's answer.
	err error
	// counts is how many times each body was delivered.
	counts map[string]int
	// header is the value of the header asked for on the first "named"
	// delivery, and correlation and replyTo its correlation ID and reply-to.
	header, correlation, replyTo string
}

// publishAndRead publishes m and then a control message on a backend of its
// own, and reads deliveries for three seconds. The control has to arrive once
// on the same connection, whatever m did.
func publishAndRead(ctx context.Context, t *testing.T, cfg stomp.Config, m *mq.Message, header, did string) readBack {
	t.Helper()
	if err := stomp.Provision(ctx, cfg, testkit.Need{Topic: "named", Subscriptions: []string{"workers"}}); err != nil {
		t.Fatalf("Provision: %v", err)
	}
	b := collisionBackend(ctx, t, cfg)
	snk, err := b.OpenSink(ctx, mq.SinkConfig{Topic: "named", Confirmation: mq.ConfirmAccepted})
	if err != nil {
		t.Fatalf("OpenSink: %v", err)
	}
	defer func() { _ = snk.Close() }()
	src, err := b.OpenSource(ctx, mq.SourceConfig{Topic: "named", Subscription: "workers", Prefetch: 8})
	if err != nil {
		t.Fatalf("OpenSource: %v", err)
	}
	defer func() { _ = src.Close() }()
	before := b.Reconnects()
	got := readBack{err: snk.Publish(ctx, m, nil), counts: map[string]int{}}
	if err := snk.Publish(ctx, &mq.Message{Body: []byte("control")}, nil); err != nil {
		t.Errorf("%s under %v: the control publication after it: %v (%s)", header, cfg.Metadata, err, did)
	}
	deadline := time.Now().Add(3 * time.Second)
	dst := make([]mq.DriverDelivery, 8)
	for time.Now().Before(deadline) {
		fctx, fcancel := context.WithDeadline(ctx, deadline)
		n, ferr := src.Fetch(fctx, dst)
		fcancel()
		for i := range dst[:n] {
			body := string(dst[i].Message.Body)
			if body == "named" && got.counts[body] == 0 {
				v, _ := dst[i].Message.Headers.Get(header)
				got.header, got.correlation = string(v), dst[i].Message.Properties.CorrelationID
				got.replyTo = dst[i].Message.Properties.ReplyTo
			}
			got.counts[body]++
			_ = src.Settler().Ack(ctx, &dst[i])
		}
		if ferr != nil && !errors.Is(ferr, context.DeadlineExceeded) && mq.CodeOf(ferr) != mq.CodeDeadlineExceeded {
			t.Errorf("%s under %v: Fetch: %v (%s)", header, cfg.Metadata, ferr, did)
			return got
		}
	}
	if b.Reconnects() != before || got.counts["control"] != 1 {
		t.Errorf("%s under %v: reconnects %d -> %d and the control delivered %d times, want the same connection "+
			"and once (%s)", header, cfg.Metadata, before, b.Reconnects(), got.counts["control"], did)
	}
	return got
}
