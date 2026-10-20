package jetstream_test

import (
	"bytes"
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit"
	nq "github.com/iqhive/iqmq/drivers/jetstream"
	"github.com/iqhive/iqmq/wire"
)

// forgeEnvelope returns the bytes of one canonical portable envelope for m.
//
// It is not a fixture: it is what a NATIVE publication is entitled to carry.
// The specification makes a body an opaque byte string, so an application publishing
// iqmq-encoded bytes as its payload — a store-and-forward relay, a test
// harness, a backup tool — is doing nothing wrong, and a consumer that read
// those bytes as ITS OWN metadata would hand the application a key, an
// identifier and headers nobody published and a payload that came out of the
// middle of the real one.
func forgeEnvelope(tb testing.TB, k wire.Kind, m *mq.Message) []byte {
	tb.Helper()
	body, err := mustLayout(tb, k).Encode(nil, m)
	if err != nil {
		tb.Fatalf("encoding a %v envelope: %v", k, err)
	}
	if got, ok := wire.NewDetector().DetectFull(body); !ok || got != k {
		tb.Fatalf("the forged body is not a whole %v envelope (%v, %v)", k, got, ok)
	}
	return body
}

// TestEnvelopeMarkerIsTheKitsName pins this driver's envelope marker to the
// kit's, which is the name every driver with somewhere out of band to keep the
// answer spells.
//
// A name that drifted would not be an error anywhere: the marker would simply
// be missing, and an enveloped message would be delivered as a payload with
// nothing reporting it. drivers/sqs and drivers/snssqs have spelled it since.
func TestEnvelopeMarkerIsTheKitsName(t *testing.T) {
	if nq.HeaderEnvelope != driverkit.EnvelopeMarker {
		t.Errorf("this driver marks the envelope with %q, the kit says %q", nq.HeaderEnvelope, driverkit.EnvelopeMarker)
	}
	if nq.EnvelopeMarkerValue != driverkit.EnvelopeMarkerValue {
		t.Errorf("the marker value is %q, the kit says %q", nq.EnvelopeMarkerValue, driverkit.EnvelopeMarkerValue)
	}
}

// TestAdaptiveNeverGuessesTheEnvelopeFromTheBody is on this
// driver: MetadataAdaptive decides from the reserved header and never from the
// payload.
//
// The bodies it publishes are the two a sniff cannot tell from an envelope —
// exactly the six bytes wire.Detect reads, and exactly one whole canonical
// envelope, for both layouts — and every one of them is published NATIVELY,
// with native headers of its own. On this driver the old sniff was worse than a
// mis-decode: a payload the forced decoder refused failed the whole Fetch, so
// one such message took the subscription down.
//
// The other two halves are the round trips the marker has to keep working: an
// adaptive publication that really does need the envelope (a binary header,
// which NATS text headers cannot carry) and a MetadataPortableEnvelope
// publisher on the same subject.
func TestAdaptiveNeverGuessesTheEnvelopeFromTheBody(t *testing.T) {
	c := openClient(t)
	topic := newTopic("adaptive-marker")
	cons := consumerWith(t, c, mq.ConsumerConfig{
		Topic: topic, Subscription: "workers", Prefetch: 16, Metadata: mq.MetadataAdaptive,
	})
	pub, err := c.Publisher(t.Context(), mq.PublisherConfig{Topic: topic, Metadata: mq.MetadataAdaptive})
	if err != nil {
		t.Fatalf("opening a publisher: %v", err)
	}
	defer func() { _ = pub.Close() }()

	// The message the forged bodies encode. Every field of it is something a
	// mis-decode would hand the application.
	decoy := &mq.Message{Body: []byte("the payload nobody published"), ID: "forged-id"}
	decoy.Headers.Set("forged-header", []byte("forged-value"))
	decoy.Key = []byte("forged-key")

	for _, k := range wire.Kinds() {
		whole := forgeEnvelope(t, k, decoy)
		for _, tc := range []struct {
			name string
			body []byte
		}{
			{"the-six-bytes-a-sniff-reads", whole[:6]},
			{"one-whole-canonical-envelope", whole},
		} {
			t.Run(k.String()+"/"+tc.name, func(t *testing.T) {
				m := &mq.Message{Body: tc.body, ID: "native-id"}
				m.Headers.Set("native-header", []byte("native-value"))
				if err := pub.Publish(t.Context(), m); err != nil {
					t.Fatalf("publishing a native body: %v", err)
				}
				d := receiveOne(t, cons, 10*time.Second)
				if !bytes.Equal(d.Body, tc.body) {
					t.Errorf("the body came back as %q, want the %d bytes that were published",
						d.Body, len(tc.body))
				}
				if d.ID != "native-id" {
					t.Errorf("the identifier came back as %q, want %q: the PAYLOAD was read as metadata",
						d.ID, "native-id")
				}
				if got, ok := d.Headers.Get("native-header"); !ok || !bytes.Equal(got, []byte("native-value")) {
					t.Errorf("the native header came back as %q (present %v)", got, ok)
				}
				if got, ok := d.Headers.Get("forged-header"); ok {
					t.Errorf("the delivery carries the forged envelope's header %q: a payload decided its own metadata", got)
				}
				if err := d.Ack(); err != nil {
					t.Fatalf("Ack: %v", err)
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
		d := receiveOne(t, cons, 10*time.Second)
		if !bytes.Equal(d.Body, []byte("binary-metadata")) {
			t.Errorf("the body came back as %q, want %q", d.Body, "binary-metadata")
		}
		if d.ID != "binary-id" {
			t.Errorf("the identifier came back as %q, want %q: the envelope was delivered as a payload", d.ID, "binary-id")
		}
		if got, ok := d.Headers.Get("trace-state"); !ok || !bytes.Equal(got, []byte{0x00, 0xff, 0xfe, 0x80}) {
			t.Errorf("the binary header came back as %q (present %v)", got, ok)
		}
		if got, ok := d.Headers.Get(nq.HeaderEnvelope); ok {
			t.Errorf("the marker %q was surfaced to the caller as a header (%q): it is framing, not metadata",
				nq.HeaderEnvelope, got)
		}
		if err := d.Ack(); err != nil {
			t.Fatalf("Ack: %v", err)
		}
	})

	t.Run("the-portable-publishers-round-trip-still-works", func(t *testing.T) {
		env, err := c.Publisher(t.Context(), mq.PublisherConfig{
			Topic: topic, Metadata: mq.MetadataPortableEnvelope,
		})
		if err != nil {
			t.Fatalf("opening an enveloping publisher: %v", err)
		}
		defer func() { _ = env.Close() }()
		m := &mq.Message{Body: []byte("enveloped"), ID: "real-id", Key: []byte("real-key")}
		m.Headers.Set("real-header", []byte("real-value"))
		if err := env.Publish(t.Context(), m); err != nil {
			t.Fatalf("publishing an enveloped message: %v", err)
		}
		d := receiveOne(t, cons, 10*time.Second)
		if !bytes.Equal(d.Body, []byte("enveloped")) {
			t.Errorf("the body came back as %q, want %q", d.Body, "enveloped")
		}
		if d.ID != "real-id" {
			t.Errorf("the identifier came back as %q, want %q: the envelope was delivered as a payload", d.ID, "real-id")
		}
		if !bytes.Equal(d.Key, []byte("real-key")) {
			t.Errorf("the key came back as %q, want %q", d.Key, "real-key")
		}
		if got, ok := d.Headers.Get("real-header"); !ok || !bytes.Equal(got, []byte("real-value")) {
			t.Errorf("the enveloped header came back as %q (present %v)", got, ok)
		}
		if err := d.Ack(); err != nil {
			t.Fatalf("Ack: %v", err)
		}
	})
}

func mustLayout(tb testing.TB, k wire.Kind) wire.Layout {
	tb.Helper()
	l, err := wire.New(k)
	if err != nil {
		tb.Fatalf("wire.New(%v): %v", k, err)
	}
	return l
}
