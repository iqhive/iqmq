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
	"github.com/iqhive/iqmq/internal/activemq"
	"github.com/iqhive/iqmq/testkit"
)

// typedReplyTo are Properties.ReplyTo values, whether ActiveMQ Classic closed
// the connection on each as the SEND frame's `reply-to` header up to v1.1.3,
// and whether the driver refuses it on Classic now. Measured with raw SEND
// frames and through this driver against apache/activemq-classic:6.2.0;
// apache/activemq-artemis:2.44.0-alpine read every one.
//
// The first sixteen are the review's. Classic reads `reply-to` with its STOMP
// destination translator, so these are not the `JMSReplyTo` header's values:
// `/queue/` closes the connection here and `queue://` does not.
var typedReplyTo = []struct {
	value           string
	closes, refused bool
}{
	{"/queue/", true, true},
	{"/topic/", true, true},
	{"/queue/ ", true, true},
	{"/queue/a,/queue/", true, true},
	{"/remote-temp-queue/", true, true},
	{"/queue/a?x=%zz", true, true},
	{"/queue/ok", false, false},
	{"/temp-queue/", false, false},
	{"/temp-topic/", false, false},
	{"/queue/?x", false, false},
	{"/queue/a,/topic/b", false, false},
	{"queue://", false, false},
	{"topic://", false, false},
	{"abc", false, false},
	{"", false, false},
	{" ", false, false},

	// The value is trimmed and split on commas, Java's way, which drops the
	// empty parts at the end; a part with no prefix Classic knows makes it
	// keep the whole value as a property.
	{"/remote-temp-topic/", true, true},
	{"\t/queue/", true, true},
	{"/queue/,", true, true},
	{"/queue/ ,", true, true},
	{"/queue/a,/queue/ ", true, true},
	{"/queue/a,/topic/", true, true},
	{"/temp-queue/,/queue/", true, true},
	{"/queue/a,/remote-temp-queue/", true, true},
	{"/remote-temp-queue/a", false, false},
	{"/remote-temp-topic/a", false, false},
	{"/queue/,/queue/a", false, false},
	{"/queue/ ,/queue/a", false, false},
	{"/queue/a, /queue/", false, false},
	{"/queue/a,,/queue/", false, false},
	{",/queue/", false, false},
	{"/queue/a,/queue/b,", false, false},
	{"/queue/a,/temp-queue/", false, false},
	{"/temp-queue/x,/queue/b", false, false},
	{"/queue/queue://", false, false},
	{"/queue/a,/queue/ queue://", false, false},
	{"/QUEUE/", false, false},
	{"/topic/>", false, false},
	{"/queue/%zz", false, false},

	// What follows the first `?` is options: from their last `?`, split on
	// `&`, a parameter with an `=` is URL-decoded and one without is not.
	{"/queue/a?x=%4", true, true},
	{"/queue/a?x=%", true, true},
	{"/queue/a?x=%aG", true, true},
	{"/queue/a?x=%-1", true, true},
	{"/queue/a?=%zz", true, true},
	{"/queue/a?y?x=%zz", true, true},
	{"/queue/a?x=1&y=%zz", true, true},
	{"/queue/a?x=1;y=%zz", true, true},
	{"/queue/a?%zz;x=1", true, true},
	{"/queue/a ?x=%zz", true, true},
	{"/queue/a?x=%zz,/queue/b", true, true},
	{"/queue/a,/queue/b?x=%zz", true, true},
	{"/queue/a,/queue/,/queue/b?x", true, true},
	{"/queue/a?%zz", false, false},
	{"/queue/a?%zz&x=1", false, false},
	{"/queue/a?x=%zz?y", false, false},
	{"/queue/a?x=1,/queue/", false, false},
	{"/queue/?x,/queue/", false, false},
	{"/queue/? ", false, false},
	{"/queue/a?x=%41", false, false},
	{"/queue/a?x=%E2%82%AC", false, false},

	// Java's URL decoder takes a sign in an escape, and the driver's rule
	// does not: a refusal Classic would not have needed.
	{"/queue/a?x=%+f", false, true},
	{"/queue/a?x=%-0", false, true},
}

// TestATypedReplyToClassicCannotBuildIsRefused publishes each of typedReplyTo
// in each metadata mode to a server that answers as each broker, and reads the
// SEND frames.
//
// On Classic a value it cannot build is refused before any I/O with
// InvalidArgument in every mode that puts it on the frame, because a typed
// field has no other place there; MetadataPortableEnvelope carries it in the
// envelope and sends no `reply-to` header. Every other value, and every value
// to Artemis, goes out as the `reply-to` header, as it did before.
func TestATypedReplyToClassicCannotBuildIsRefused(t *testing.T) {
	t.Parallel()
	for _, row := range typedReplyTo {
		if row.closes && !row.refused {
			t.Errorf("ReplyTo %q closed Classic's connection, and the table has the driver send it", row.value)
		}
	}
	for _, broker := range activemq.Brokers() {
		for _, mode := range []mq.MetadataMode{
			mq.MetadataNativeStrict, mq.MetadataPortableEnvelope, mq.MetadataAdaptive, mq.MetadataLenientDrop,
		} {
			t.Run(broker.String()+"/"+mode.String(), func(t *testing.T) {
				t.Parallel()
				srv := brokerServer(t, broker)
				cfg := settings(broker)
				cfg.Addrs = []string{srv.Addr()}
				cfg.Metadata = mode
				b := openBackend(t, cfg)
				snk := openSink(t, b, "orders", mq.ConfirmAccepted)
				var sent []string
				for i, row := range typedReplyTo {
					m := &mq.Message{Body: []byte(strconv.Itoa(i)), Properties: mq.Properties{ReplyTo: row.value}}
					err := snk.Publish(t.Context(), m, nil)
					refused := broker == activemq.Classic && mode != mq.MetadataPortableEnvelope && row.refused
					if refused != (mq.CodeOf(err) == mq.CodeInvalidArgument) || !refused && err != nil {
						t.Errorf("ReplyTo %q: Publish = %v, want refused %v", row.value, err, refused)
					}
					if !refused {
						sent = append(sent, row.value)
					}
				}
				sends := srv.FramesOf("SEND")
				if len(sends) != len(sent) {
					t.Fatalf("the server received %d SEND frames, want %d", len(sends), len(sent))
				}
				for i, f := range sends {
					enveloped := bytes.HasPrefix(f.Body, []byte("IQMQ"))
					want := sent[i] != "" && mode != mq.MetadataPortableEnvelope
					if f.Has("reply-to") != want || want && f.Get("reply-to") != sent[i] ||
						enveloped != (mode == mq.MetadataPortableEnvelope) {
						t.Errorf("ReplyTo %q under %v: reply-to %q (present %v), enveloped %v; want it present %v",
							sent[i], mode, f.Get("reply-to"), f.Has("reply-to"), enveloped, want)
					}
				}
			})
		}
	}
}

// TestTypedReplyToOnABroker publishes each of typedReplyTo, under
// MetadataNativeStrict, to the brokers IQMQ_ACTIVEMQ_CLASSIC_ADDR and
// IQMQ_ARTEMIS_ADDR name, and then a control message, on one connection.
//
// On Classic each refused value is refused before any I/O and each other one
// is delivered once, and the connection is the same one at the end: none of
// the values the driver sends closes it. On Artemis every value is delivered.
func TestTypedReplyToOnABroker(t *testing.T) {
	for name, cfg := range brokersAt(t) {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cancel()
			cfg.Prefix += "replyto."
			if err := stomp.Provision(ctx, cfg, testkit.Need{Topic: "named", Subscriptions: []string{"workers"}}); err != nil {
				t.Fatalf("Provision: %v", err)
			}
			b := collisionBackend(ctx, t, cfg)
			snk, err := b.OpenSink(ctx, mq.SinkConfig{Topic: "named", Confirmation: mq.ConfirmAccepted})
			if err != nil {
				t.Fatalf("OpenSink: %v", err)
			}
			defer func() { _ = snk.Close() }()
			src, err := b.OpenSource(ctx, mq.SourceConfig{Topic: "named", Subscription: "workers", Prefetch: 16})
			if err != nil {
				t.Fatalf("OpenSource: %v", err)
			}
			defer func() { _ = src.Close() }()
			before := b.Reconnects()
			want := map[string]int{"control": 1}
			for i, row := range typedReplyTo {
				body := strconv.Itoa(i)
				err := snk.Publish(ctx, &mq.Message{Body: []byte(body), Properties: mq.Properties{ReplyTo: row.value}}, nil)
				refused := cfg.Broker == activemq.Classic && row.refused
				if refused != (mq.CodeOf(err) == mq.CodeInvalidArgument) || !refused && err != nil {
					t.Errorf("ReplyTo %q: Publish = %v, want refused %v", row.value, err, refused)
				}
				if !refused {
					want[body] = 1
				}
			}
			if err := snk.Publish(ctx, &mq.Message{Body: []byte("control")}, nil); err != nil {
				t.Errorf("the control publication: %v", err)
			}
			got := map[string]int{}
			deadline := time.Now().Add(5 * time.Second)
			dst := make([]mq.DriverDelivery, 16)
			for time.Now().Before(deadline) && got["control"] == 0 {
				fctx, fcancel := context.WithDeadline(ctx, deadline)
				n, ferr := src.Fetch(fctx, dst)
				fcancel()
				for i := range dst[:n] {
					got[string(dst[i].Message.Body)]++
					_ = src.Settler().Ack(ctx, &dst[i])
				}
				if ferr != nil && !errors.Is(ferr, context.DeadlineExceeded) && mq.CodeOf(ferr) != mq.CodeDeadlineExceeded {
					t.Fatalf("Fetch: %v", ferr)
				}
			}
			for i, row := range typedReplyTo {
				if body := strconv.Itoa(i); got[body] != want[body] {
					t.Errorf("ReplyTo %q was delivered %d times, want %d", row.value, got[body], want[body])
				}
			}
			if b.Reconnects() != before || got["control"] != 1 {
				t.Errorf("reconnects %d -> %d and the control delivered %d times, want the same connection and once",
					before, b.Reconnects(), got["control"])
			}
		})
	}
}
