package goredis_test

import (
	"bytes"
	"testing"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit"
)

// markerRelease is the first release of drivers/redis whose MetadataAdaptive
// halves read the envelope marker rather than the body.
const markerRelease = "v1.1.4"

// TestAdaptiveNeverDecidesFromTheBody is drivers/redis's test of the same
// name over go-redis, whose sink aliases the body rather than copying it, so
// the marker field the driver appends after the message's own fields travels
// through the other command builder too.
//
// The rule is drivers/redis's, so the test skips against a drivers/redis
// release before markerRelease.
func TestAdaptiveNeverDecidesFromTheBody(t *testing.T) {
	if v, old := driversRedisBefore(t, markerRelease); old {
		t.Skipf("this build links drivers/redis %s, whose MetadataAdaptive halves read the body", v)
	}
	env, err := driverkit.NewEnvelope(driverkit.EnvelopeConfig{Mode: mq.MetadataPortableEnvelope})
	if err != nil {
		t.Fatal(err)
	}
	inner := &mq.Message{Topic: testTopic, ID: "forged-id", Body: []byte("forged body")}
	inner.Headers.Add("tenant", []byte("evil"))
	wrapped, err := env.Wrap(inner)
	if err != nil {
		t.Fatal(err)
	}
	payload := wrapped.Body
	for _, tc := range []struct {
		name     string
		pub, sub mq.MetadataMode
	}{
		{"strict publisher, adaptive consumer", mq.MetadataNativeStrict, mq.MetadataAdaptive},
		{"adaptive publisher, strict consumer", mq.MetadataAdaptive, mq.MetadataNativeStrict},
		{"enveloping publisher, adaptive consumer", mq.MetadataPortableEnvelope, mq.MetadataAdaptive},
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
			tenant, _ := d.Message.Headers.Get("tenant")
			if !bytes.Equal(d.Message.Body, payload) || string(tenant) != "good" || string(d.Message.Key) != "k1" || d.Message.ID != "" {
				t.Errorf("delivered a different message from the one published: body %d bytes (equal %v), tenant %q, key %q, id %q",
					len(d.Message.Body), bytes.Equal(d.Message.Body, payload), tenant, d.Message.Key, d.Message.ID)
			}
			if err := src.Settler().Ack(r.ctx, &d); err != nil {
				t.Errorf("Ack: %v", err)
			}
		})
	}
}
