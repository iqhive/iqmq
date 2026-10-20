package kafkago_test

import (
	"bytes"
	"testing"
	"time"

	segkafka "github.com/segmentio/kafka-go"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit"
	kq "github.com/iqhive/iqmq/drivers/kafka/kafkago"
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
// be missing, and an enveloped record would be delivered as a payload with
// nothing reporting it. drivers/sqs and drivers/snssqs have spelled it since
// The specification, and TestCrossDriverReservedHeaders holds this driver and the franz-go
// one to the same list.
func TestEnvelopeMarkerIsTheKitsName(t *testing.T) {
	if kq.HeaderEnvelope != driverkit.EnvelopeMarker {
		t.Errorf("this driver marks the envelope with %q, the kit says %q", kq.HeaderEnvelope, driverkit.EnvelopeMarker)
	}
	if kq.EnvelopeMarkerValue != driverkit.EnvelopeMarkerValue {
		t.Errorf("the marker value is %q, the kit says %q", kq.EnvelopeMarkerValue, driverkit.EnvelopeMarkerValue)
	}
}

// TestAdaptiveNeverGuessesTheEnvelopeFromTheBody is on this
// driver: MetadataAdaptive decides from the reserved header and never from the
// record's value.
//
// The bodies it publishes are the two a sniff cannot tell from an envelope —
// exactly the six bytes wire.Detect reads, and exactly one whole canonical
// envelope, for both layouts — and every one of them is published NATIVELY,
// with native headers of its own. A driver that read the value would deliver
// the forged envelope's identifier and headers in place of the caller's and
// the forged envelope's payload in place of the caller's body.
//
// The second half is the round trip the marker has to keep working: a
// MetadataPortableEnvelope publisher writes the marker, so an adaptive
// consumer reading the same topic still gets its metadata back.
func TestAdaptiveNeverGuessesTheEnvelopeFromTheBody(t *testing.T) {
	topic := newTopic("adaptive-marker")
	c := cluster(t, 1, []string{topic})
	cl := openOn(t, c)
	cons, err := cl.Consumer(t.Context(), mq.ConsumerConfig{
		Topic: topic, Subscription: "workers", Metadata: mq.MetadataAdaptive,
	})
	if err != nil {
		t.Fatalf("opening a consumer: %v", err)
	}
	defer func() { _ = cons.Close() }()
	pub, err := cl.Publisher(t.Context(), mq.PublisherConfig{Topic: topic, Metadata: mq.MetadataAdaptive})
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
					t.Errorf("the identifier came back as %q, want %q: the record's VALUE was read as metadata",
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

	t.Run("the-enveloped-round-trip-still-works", func(t *testing.T) {
		// The publisher whose records really ARE envelopes, read by the same
		// adaptive consumer. This is what the marker buys and what a body
		// sniff was standing in for.
		env, err := cl.Publisher(t.Context(), mq.PublisherConfig{
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
		if got, ok := d.Headers.Get(kq.HeaderEnvelope); ok {
			t.Errorf("the marker %q was surfaced to the caller as a header (%q): it is framing, not metadata",
				kq.HeaderEnvelope, got)
		}
		if err := d.Ack(); err != nil {
			t.Fatalf("Ack: %v", err)
		}
	})
}

// TestAMarkedRecordThatWillNotDecodeIsReported is the other direction, and it is the half a marker makes possible.
//
// Once the marker is authoritative, a record that CARRIES it and will not
// decode is a corrupt record, or one written under wider envelope limits than
// this consumer accepts. There is no settlement in the SPI that says
// "undecodable", so the specification makes it a background failure and the record is
// surfaced as it arrived — and that is only the honest answer because it is
// REPORTED. A marked record handed to the application as its own bytes with
// nothing counting it is the mis-decode the marker exists to prevent, wearing
// the other hat, and the adaptive path used to do exactly that silently while
// the portable path next to it reported.
//
// The record is produced through the NATIVE writer, because that is the only
// way to make one: the specification reserves the `iqmq-` prefix and this driver refuses a
// caller's header under it, so no publication through the portable API can
// forge a marker.
func TestAMarkedRecordThatWillNotDecodeIsReported(t *testing.T) {
	topic := newTopic("marked-undecodable")
	c := cluster(t, 1, []string{topic})
	cl, b := openPair(t, c)
	cons, err := cl.Consumer(t.Context(), mq.ConsumerConfig{
		Topic: topic, Subscription: "workers", Metadata: mq.MetadataAdaptive,
	})
	if err != nil {
		t.Fatalf("opening a consumer: %v", err)
	}
	defer func() { _ = cons.Close() }()

	body := []byte("this is not an envelope, whatever the marker says")
	if err := b.Writer().WriteMessages(t.Context(), segkafka.Message{
		Topic: topic,
		Value: body,
		Headers: []segkafka.Header{
			{Key: kq.HeaderEnvelope, Value: []byte(kq.EnvelopeMarkerValue)},
		},
	}); err != nil {
		t.Fatalf("producing a marked record natively: %v", err)
	}

	d := receiveOne(t, cons, 10*time.Second)
	if !bytes.Equal(d.Body, body) {
		t.Errorf("the body came back as %q, want the bytes that were produced", d.Body)
	}
	if err := d.Ack(); err != nil {
		t.Fatalf("Ack: %v", err)
	}
	if got := b.DegradedTransitions(); got == 0 {
		t.Error("a record that carried the envelope marker and could not be decoded was delivered as its own bytes with nothing reporting it: the specification makes an undecodable record a background failure")
	}
}

func mustLayout(tb testing.TB, k wire.Kind) wire.Layout {
	tb.Helper()
	l, err := wire.New(k)
	if err != nil {
		tb.Fatalf("wire.New(%v): %v", k, err)
	}
	return l
}
