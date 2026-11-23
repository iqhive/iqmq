package gcppubsub_test

import (
	"bytes"
	"context"
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
	gps "github.com/iqhive/iqmq/drivers/gcppubsub"
	"github.com/iqhive/iqmq/wire"
)

// forgeEnvelope is a whole envelope of layout k, which is what a payload a
// third party controls can be.
func forgeEnvelope(tb testing.TB, k wire.Kind, m *mq.Message) []byte {
	tb.Helper()
	layout, err := wire.New(k)
	if err != nil {
		tb.Fatalf("wire.New(%v): %v", k, err)
	}
	body, err := layout.Encode(nil, m)
	if err != nil {
		tb.Fatalf("encoding a %v envelope: %v", k, err)
	}
	if got, ok := wire.NewDetector().DetectFull(body); !ok || got != k {
		tb.Fatalf("the forged body is not a whole %v envelope (%v, %v)", k, got, ok)
	}
	return body
}

// TestAdaptiveNeverGuessesTheEnvelopeFromTheBody: this driver writes the
// out-of-band marker (attribute iqmq-env) on every message it envelopes, and
// driverkit's rule for a driver with attributes is that it decodes by that
// marker and never sniffs the body: a marker inside the body is forgeable by
// any payload. The MetadataAdaptive consumer decoded through the kit's
// Adaptive Unwrap, which decides from the body, and read the marker only
// afterwards. So a native publication whose payload was a whole envelope (a
// relay forwarding what a NativeStrict consumer received, a stored record
// replayed, an upload published as it came) was delivered as the forged
// message inside it, with its body, key and headers and the native attributes
// merged in, or, for the layout this consumer does not decode, refused as
// undecodable and never delivered at all.
func TestAdaptiveNeverGuessesTheEnvelopeFromTheBody(t *testing.T) {
	l := newLive(t)
	c := l.open(t)
	tp := newTopic("adaptive-marker")
	provision(t, c, tp, "workers")
	cons, err := c.Consumer(t.Context(), mq.ConsumerConfig{
		Topic: tp, Subscription: "workers", Metadata: mq.MetadataAdaptive,
	})
	if err != nil {
		t.Fatalf("Consumer: %v", err)
	}
	defer func() { _ = cons.Close() }()
	pub, err := c.Publisher(t.Context(), mq.PublisherConfig{Topic: tp, Metadata: mq.MetadataAdaptive})
	if err != nil {
		t.Fatalf("Publisher: %v", err)
	}
	defer func() { _ = pub.Close() }()

	// The message the forged bodies encode. Every field of it is something a
	// mis-decode would hand the application.
	decoy := &mq.Message{Body: []byte("the payload nobody published"), ID: "forged-id", Key: []byte("forged-key")}
	decoy.Headers.Set("forged-header", []byte("forged-value"))

	for _, k := range wire.Kinds() {
		whole := forgeEnvelope(t, k, decoy)
		for _, tc := range []struct {
			name string
			body []byte
		}{
			{"the-six-bytes-a-sniff-reads", whole[:6]},
			{"one-whole-envelope", whole},
		} {
			t.Run(k.String()+"/"+tc.name, func(t *testing.T) {
				m := &mq.Message{Body: tc.body, ID: "native-id"}
				m.Headers.Set("native-header", []byte("native-value"))
				if err := pub.Publish(t.Context(), m); err != nil {
					t.Fatalf("publishing a native body: %v", err)
				}
				ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
				defer cancel()
				dst := make([]mq.Delivery, 1)
				if n, rerr := cons.Receive(ctx, dst); n == 0 {
					t.Fatalf("a native publication whose body is %d bytes of a %v envelope was never delivered "+
						"(%v): the consumer read the payload as an envelope and refused it", len(tc.body), k, rerr)
				}
				d := dst[0]
				defer func() { _ = d.Ack() }()
				if !bytes.Equal(d.Body, tc.body) {
					t.Errorf("the body came back as %q, want the %d bytes that were published", d.Body, len(tc.body))
				}
				if d.ID != "native-id" {
					t.Errorf("the identifier came back as %q, want %q: the PAYLOAD was read as metadata", d.ID, "native-id")
				}
				if got, ok := d.Headers.Get("native-header"); !ok || string(got) != "native-value" {
					t.Errorf("the native header came back as %q (present %v)", got, ok)
				}
				if got, ok := d.Headers.Get("forged-header"); ok {
					t.Errorf("the delivery carries the payload's header %q: a payload decided its own metadata", got)
				}
				if d.Key != nil {
					t.Errorf("the delivery carries key %q, which only the payload named", d.Key)
				}
			})
		}
	}

	t.Run("an-adaptive-publication-that-needs-the-envelope", func(t *testing.T) {
		m := &mq.Message{Body: []byte("binary-metadata"), ID: "binary-id"}
		m.Headers.Set("trace-state", []byte{0x00, 0xff, 0xfe, 0x80})
		if err := pub.Publish(t.Context(), m); err != nil {
			t.Fatalf("publishing a message that needs the envelope: %v", err)
		}
		d := receiveOne(t, cons, 5*time.Second)
		defer func() { _ = d.Ack() }()
		if !bytes.Equal(d.Body, []byte("binary-metadata")) {
			t.Errorf("the body came back as %q, want %q", d.Body, "binary-metadata")
		}
		if d.ID != "binary-id" {
			t.Errorf("the identifier came back as %q, want %q: the envelope was delivered as a payload", d.ID, "binary-id")
		}
		if got, ok := d.Headers.Get("trace-state"); !ok || !bytes.Equal(got, []byte{0x00, 0xff, 0xfe, 0x80}) {
			t.Errorf("the binary header came back as %q (present %v)", got, ok)
		}
	})
}

// TestARejectedEnvelopeKeepsItsMarker: a MetadataNativeStrict consumer is
// handed an envelope another publisher made as its body, and a Reject
// republishes that body to the dead-letter topic. The copy carries the marker
// the original arrived with, so an Adaptive reader of the dead-letter topic
// reads the message the publisher sent. Without it the reader, which decodes
// by the marker, would be handed the envelope's bytes as the payload.
func TestARejectedEnvelopeKeepsItsMarker(t *testing.T) {
	dlq := newTopic("dlq")
	c, _ := scripted(t, func(cfg *gps.Config) {
		cfg.DeadLetterTopic = dlq
		cfg.MaxDeliveryAttempts = 5
		cfg.AckDeadline = time.Minute
	})
	tp := newTopic("reject-envelope")
	provision(t, c, tp, "s")
	provision(t, c, dlq, "d")
	pub, err := c.Publisher(t.Context(), mq.PublisherConfig{Topic: tp, Metadata: mq.MetadataAdaptive})
	if err != nil {
		t.Fatalf("Publisher: %v", err)
	}
	defer func() { _ = pub.Close() }()
	m := &mq.Message{Body: []byte("binary-metadata"), ID: "binary-id"}
	m.Headers.Set("trace-state", []byte{0x00, 0xff})
	if err := pub.Publish(t.Context(), m); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	strict, err := c.Consumer(t.Context(), mq.ConsumerConfig{Topic: tp, Subscription: "s"})
	if err != nil {
		t.Fatalf("Consumer: %v", err)
	}
	defer func() { _ = strict.Close() }()
	d := receiveOne(t, strict, 5*time.Second)
	if bytes.Equal(d.Body, m.Body) {
		t.Fatalf("the NativeStrict consumer unwrapped the envelope, so this test proves nothing")
	}
	if err := d.Reject(); err != nil {
		t.Fatalf("Reject: %v", err)
	}
	reader, err := c.Consumer(t.Context(), mq.ConsumerConfig{
		Topic: dlq, Subscription: "d", Metadata: mq.MetadataAdaptive,
	})
	if err != nil {
		t.Fatalf("Consumer(dlq): %v", err)
	}
	defer func() { _ = reader.Close() }()
	got := receiveOne(t, reader, 5*time.Second)
	defer func() { _ = got.Ack() }()
	if !bytes.Equal(got.Body, m.Body) || got.ID != "binary-id" {
		t.Errorf("the dead-letter copy reads as body %q, identifier %q; want %q, %q: the copy lost the "+
			"marker its original arrived with", got.Body, got.ID, m.Body, "binary-id")
	}
	if v, ok := got.Headers.Get("trace-state"); !ok || !bytes.Equal(v, []byte{0x00, 0xff}) {
		t.Errorf("the dead-letter copy's binary header came back as %q (present %v)", v, ok)
	}
}
