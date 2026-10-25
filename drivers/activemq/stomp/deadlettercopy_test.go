package stomp_test

import (
	"context"
	"testing"

	"github.com/go-stomp/stomp/v3/frame"

	mq "github.com/iqhive/iqmq"
	stomp "github.com/iqhive/iqmq/drivers/activemq/stomp"
	"github.com/iqhive/iqmq/drivers/activemq/stomp/internal/stompsrv"
)

// TestADeadLetteredCopyIsTheMessageTheCallerReceived reads a dead-lettered
// message back from the dead-letter topic and compares it with the original.
//
// The copy was built from the frame's raw body alone, so every header, the key
// and the ID were lost on the hop, and under MetadataPortableEnvelope -- where
// the raw body IS the envelope -- the dead-letter sink wrapped it a second
// time: a consumer of the dead-letter topic got the original's envelope bytes
// as its body.
func TestADeadLetteredCopyIsTheMessageTheCallerReceived(t *testing.T) {
	for _, mode := range []mq.MetadataMode{mq.MetadataAdaptive, mq.MetadataPortableEnvelope} {
		t.Run(mode.String(), func(t *testing.T) {
			srv := artemisServer(t)
			cfg := artemisConfig(srv)
			cfg.Metadata = mode
			cfg.DeadLetterTopic = "orders-dead"
			b := openBackend(t, cfg)
			src, consume := openSource(t, b, cfg, srv, "orders", "workers", 4)
			dlSrc, dlConsume := openSource(t, b, cfg, srv, "orders-dead", "audit", 4)
			snk := openSink(t, b, "orders", mq.ConfirmAccepted)

			orig := &mq.Message{Key: []byte("order-42"), Body: []byte("poison"), ID: "msg-1"}
			orig.Headers.Set("tenant", []byte("acme"))
			orig.Properties.CorrelationID = "corr-7"
			if err := snk.Publish(t.Context(), orig, nil); err != nil {
				t.Fatalf("Publish: %v", err)
			}
			publish, _ := destinations(t, cfg, "orders", "workers")
			relay(t, srv, publish, consume)
			d := fetchN(t, src, 1)[0]
			ctx, cancel := context.WithTimeout(context.Background(), budget)
			defer cancel()
			if err := src.Settler().Nack(ctx, &d, nackOptions(t, mq.NackDeadLetter(), mq.NackReason("bad"))); err != nil {
				t.Fatalf("Nack(NackDeadLetter()): %v", err)
			}

			dlPublish, _ := destinations(t, cfg, "orders-dead", "audit")
			relay(t, srv, dlPublish, dlConsume)
			got := fetchN(t, dlSrc, 1)[0].Message
			if string(got.Body) != "poison" {
				t.Fatalf("the dead-lettered body is %q, want %q", got.Body, "poison")
			}
			if string(got.Key) != "order-42" || got.ID != "msg-1" || got.Properties.CorrelationID != "corr-7" {
				t.Errorf("the dead-lettered copy has key %q, ID %q, correlation ID %q; want the original's %q, %q, %q",
					got.Key, got.ID, got.Properties.CorrelationID, "order-42", "msg-1", "corr-7")
			}
			if v, ok := got.Headers.Get("tenant"); !ok || string(v) != "acme" {
				t.Errorf("the original's header did not survive the hop: tenant = %q, %v", v, ok)
			}
			if v, ok := got.Headers.Get(stomp.DeadLetterReasonHeader); !ok || string(v) != "bad" {
				t.Errorf("the dead-letter reason is %q, %v; want %q", v, ok, "bad")
			}
			if _, ok := got.Headers.Get(stomp.DeadLetterAttemptHeader); !ok {
				t.Error("the dead-lettered copy carries no attempt header")
			}
		})
	}
}

// relay delivers every SEND to one destination to another, headers and all.
// [stompsrv.Server.Forward] keeps only the body and content type, which is
// the one part of a message that survived the old dead-letter copy.
func relay(t *testing.T, srv *stompsrv.Server, from, to string) {
	t.Helper()
	var msgs []stompsrv.Message
	for _, f := range srv.FramesOf(frame.SEND) {
		if f.Get(frame.Destination) != from {
			continue
		}
		m := stompsrv.Message{Body: f.Body}
		for i := range f.Len() {
			k, v := f.At(i)
			switch k {
			case frame.Destination, frame.Receipt, frame.ContentLength:
			case frame.ContentType:
				m.ContentType = v
			case frame.MessageId:
				m.MessageID = v
			default:
				m.Headers = append(m.Headers, [2]string{k, v})
			}
		}
		msgs = append(msgs, m)
	}
	if len(msgs) != 1 {
		t.Fatalf("%d SEND frame(s) to %s, want 1", len(msgs), from)
	}
	deliver(t, srv, to, msgs...)
}

// TestADeadLetterCopyAsksArtemisForAnycastInQueueMode is the dead-letter SEND
// under queue addressing: like any other publication it has to ask for the
// ANYCAST routing type, or an auto-created or mixed address routes the copy
// multicast with no queue behind it while the original is acknowledged.
func TestADeadLetterCopyAsksArtemisForAnycastInQueueMode(t *testing.T) {
	cfg := settings(stomp.Artemis)
	cfg.Addressing = stomp.AddressQueue
	cfg.DeadLetterTopic = "ORDERS.DLQ"
	f, d := settleFixture(t, cfg)
	mark := f.srv.Next()
	ctx, cancel := context.WithTimeout(context.Background(), budget)
	defer cancel()
	if err := f.src.Settler().Nack(ctx, &d, nackOptions(t, mq.NackDeadLetter())); err != nil {
		t.Fatalf("Nack(NackDeadLetter()): %v", err)
	}
	sends := f.srv.FramesOf(frame.SEND)
	if len(sends) == 0 {
		t.Fatalf("no dead-letter SEND; frames since the nack: %v", f.srv.Commands(mark))
	}
	dl := sends[len(sends)-1]
	if dl.Get(frame.Destination) != "ORDERS.DLQ" || dl.Get("destination-type") != "ANYCAST" {
		t.Fatalf("the dead-letter SEND went to %q with destination-type %q, want ORDERS.DLQ and ANYCAST",
			dl.Get(frame.Destination), dl.Get("destination-type"))
	}
}
