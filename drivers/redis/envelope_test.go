package redis_test

import (
	"bytes"
	"testing"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit"
	rd "github.com/iqhive/iqmq/drivers/redis"
	"github.com/iqhive/iqmq/internal/streams"
)

// envelopeBody is a valid portable envelope of a message its author chose,
// with a reserved provenance header among its own: a body a native
// publication may carry, because a payload is whatever bytes the caller had.
func envelopeBody(t *testing.T) []byte {
	t.Helper()
	env, err := driverkit.NewEnvelope(driverkit.EnvelopeConfig{Mode: mq.MetadataPortableEnvelope})
	if err != nil {
		t.Fatal(err)
	}
	inner := &mq.Message{Topic: testTopic, ID: "forged-id", Body: []byte("forged body")}
	inner.Headers.Add("tenant", []byte("evil"))
	inner.Headers.Add("iqmq-origin-topic", []byte("payments"))
	w, err := env.Wrap(inner)
	if err != nil {
		t.Fatal(err)
	}
	return w.Body
}

// TestAdaptiveNeverDecidesFromTheBody: a stream entry carries every header
// natively, so MetadataAdaptive never needs the envelope here and behaves as
// MetadataNativeStrict does, and whether an entry IS an envelope is recorded
// in a field the driver writes ("x:env") rather than read off the body. The
// adaptive halves used to sniff the body: a native body that was a valid
// envelope reached an adaptive consumer as the message inside it, headers,
// identifier and body chosen by whoever wrote the payload, and an adaptive
// publisher enveloped a body that began like one, so a strict consumer got
// envelope bytes with the key and headers gone. A MetadataPortableEnvelope
// publication still reads back under both enveloping modes.
func TestAdaptiveNeverDecidesFromTheBody(t *testing.T) {
	payload := envelopeBody(t)
	for _, tc := range []struct {
		name     string
		pub, sub mq.MetadataMode
	}{
		{"strict publisher, adaptive consumer", mq.MetadataNativeStrict, mq.MetadataAdaptive},
		{"adaptive publisher, strict consumer", mq.MetadataAdaptive, mq.MetadataNativeStrict},
		{"adaptive publisher, adaptive consumer", mq.MetadataAdaptive, mq.MetadataAdaptive},
		{"strict publisher, strict consumer", mq.MetadataNativeStrict, mq.MetadataNativeStrict},
		{"enveloping publisher, adaptive consumer", mq.MetadataPortableEnvelope, mq.MetadataAdaptive},
		{"enveloping publisher, enveloping consumer", mq.MetadataPortableEnvelope, mq.MetadataPortableEnvelope},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := newRig(t)
			src, err := r.b.OpenSource(r.ctx, mq.SourceConfig{
				Topic: testTopic, Subscription: testSub, Prefetch: 1, Start: mq.StartEarliest(), Metadata: tc.sub,
			})
			if err != nil {
				t.Fatalf("OpenSource: %v", err)
			}
			defer func() { _ = src.Close() }()
			sink, err := r.b.OpenSink(r.ctx, mq.SinkConfig{Topic: testTopic, Metadata: tc.pub})
			if err != nil {
				t.Fatalf("OpenSink: %v", err)
			}
			defer func() { _ = sink.Close() }()
			m := &mq.Message{Topic: testTopic, Key: []byte("k1"), Body: payload}
			m.Headers.Add("tenant", []byte("good"))
			if err := sink.Publish(r.ctx, m, nil); err != nil {
				t.Fatalf("Publish: %v", err)
			}
			d := fetchOne(t, r.ctx, src)
			checkDelivered(t, d.Message, payload)
			if err := src.Settler().Ack(r.ctx, &d); err != nil {
				t.Errorf("Ack: %v", err)
			}
		})
	}
}

// checkDelivered fails t unless got is the message envelopeBody's tests
// published: payload as the body, key k1 and the one header tenant=good.
func checkDelivered(t *testing.T, got *mq.Message, payload []byte) {
	t.Helper()
	tenant, _ := got.Headers.Get("tenant")
	_, forged := got.Headers.Get("iqmq-origin-topic")
	if !bytes.Equal(got.Body, payload) || string(tenant) != "good" || string(got.Key) != "k1" || forged || got.ID != "" {
		t.Errorf("delivered a different message from the one published: body %d bytes (equal %v), "+
			"tenant %q, key %q, id %q, iqmq-origin-topic present %v",
			len(got.Body), bytes.Equal(got.Body, payload), tenant, got.Key, got.ID, forged)
	}
}

// TestTheEnvelopeMarkerTravelsWithADeadLetter: the dead-letter copy leaves the
// source's bookkeeping behind (the attempt count and the group stamp) and
// keeps the field that says the entry is an envelope, so an adaptive reader
// of the dead-letter topic decodes a MetadataPortableEnvelope publication
// rather than delivering the envelope's bytes as the payload.
func TestTheEnvelopeMarkerTravelsWithADeadLetter(t *testing.T) {
	const dlq = "acme.orders-dlq"
	r := newRig(t, func(c *rd.Config) { c.DeadLetterTopic = dlq })
	src, err := r.b.OpenSource(r.ctx, mq.SourceConfig{
		Topic: testTopic, Subscription: testSub, Prefetch: 1, Start: mq.StartEarliest(), Metadata: mq.MetadataAdaptive,
	})
	if err != nil {
		t.Fatalf("OpenSource: %v", err)
	}
	defer func() { _ = src.Close() }()
	dead, err := r.b.OpenSource(r.ctx, mq.SourceConfig{
		Topic: dlq, Subscription: testSub, Prefetch: 1, Start: mq.StartEarliest(), Metadata: mq.MetadataAdaptive,
	})
	if err != nil {
		t.Fatalf("OpenSource(%s): %v", dlq, err)
	}
	defer func() { _ = dead.Close() }()
	sink, err := r.b.OpenSink(r.ctx, mq.SinkConfig{Topic: testTopic, Metadata: mq.MetadataPortableEnvelope})
	if err != nil {
		t.Fatalf("OpenSink: %v", err)
	}
	defer func() { _ = sink.Close() }()
	payload := []byte("rejected")
	m := &mq.Message{Topic: testTopic, Key: []byte("k1"), Body: payload}
	m.Headers.Add("tenant", []byte("good"))
	if err := sink.Publish(r.ctx, m, nil); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	d := fetchOne(t, r.ctx, src)
	checkDelivered(t, d.Message, payload)
	if err := src.Settler().Nack(r.ctx, &d, mq.NackOptions{Action: mq.NackActionDeadLetter}); err != nil {
		t.Fatalf("Reject: %v", err)
	}
	keys, err := streams.NewKeys(dlq, 1)
	if err != nil {
		t.Fatal(err)
	}
	if n := len(r.srv.Entries(keys.Stream(0))); n != 1 {
		t.Fatalf("the dead-letter stream holds %d entries, want 1", n)
	}
	got := fetchOne(t, r.ctx, dead)
	checkDelivered(t, got.Message, payload)
}
