package memory_test

import (
	"bytes"
	"context"
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/memory"
	"github.com/iqhive/iqmq/wire"
)

// This file is ADR-008's data: what this driver does in each of the four
// metadata modes.
//
// Three of them come out the same, and that is the finding rather than an
// omission: the engine stores the *mq.Message itself, so it has a native
// field for every piece of metadata a message carries — binary values,
// duplicate keys, the identifier and the typed properties — and nothing is
// ever unsupported. MetadataAdaptive therefore never needs the envelope and
// MetadataLenientDrop never drops anything. The fourth mode is explicit and
// is honoured as written.

// richMessage is a message using every piece of metadata the specification is about: a
// binary header value, a duplicate key, an identifier and every typed
// property.
func richMessage() *mq.Message {
	m := &mq.Message{
		Key:  []byte("cust-42"),
		Body: []byte("payload"),
		ID:   "msg-1",
		Properties: mq.Properties{
			ContentType:     "application/json",
			ContentEncoding: "gzip",
			CorrelationID:   "corr-1",
			CausationID:     "cause-1",
			ReplyTo:         "replies",
			CreatedAt:       time.Unix(1767225600, 123456789).UTC(),
		},
	}
	m.Headers.Add("trace", []byte{0x00, 0xff, 0x7f, 0x80})
	m.Headers.Add("tag", []byte("a"))
	m.Headers.Add("tag", []byte("b"))
	return m
}

// assertRich checks that every field of richMessage survived the round trip.
func assertRich(t *testing.T, got *mq.Message) {
	t.Helper()
	want := richMessage()
	if !bytes.Equal(got.Key, want.Key) {
		t.Errorf("key %q, want %q", got.Key, want.Key)
	}
	if !bytes.Equal(got.Body, want.Body) {
		t.Errorf("body %q, want %q", got.Body, want.Body)
	}
	if got.ID != want.ID {
		t.Errorf("id %q, want %q", got.ID, want.ID)
	}
	if got.Properties != want.Properties {
		t.Errorf("properties %+v, want %+v", got.Properties, want.Properties)
	}
	if got.Headers.Len() != want.Headers.Len() {
		t.Errorf("%d headers, want %d", got.Headers.Len(), want.Headers.Len())
	}
	if v, ok := got.Headers.Get("trace"); !ok || !bytes.Equal(v, []byte{0x00, 0xff, 0x7f, 0x80}) {
		t.Errorf("binary header value %v (present %v)", v, ok)
	}
	vals := got.Headers.Values("tag")
	if len(vals) != 2 || string(vals[0]) != "a" || string(vals[1]) != "b" {
		t.Errorf("duplicate header values %q, want [a b] in order", vals)
	}
}

// TestEveryMetadataModeCarriesEveryField is ADR-008's first claim for this
// driver: in all four modes a message comes back whole. Three carry it
// natively and one carries it in the envelope, and the mode is not allowed to
// lose anything either way — the specification forbids dropping unsupported metadata in
// every mode.
func TestEveryMetadataModeCarriesEveryField(t *testing.T) {
	ctx := context.Background()
	for _, mode := range []mq.MetadataMode{
		mq.MetadataNativeStrict,
		mq.MetadataPortableEnvelope,
		mq.MetadataAdaptive,
		mq.MetadataLenientDrop,
	} {
		t.Run(mode.String(), func(t *testing.T) {
			client, _ := memory.NewForTest(t)
			c, err := client.Consumer(ctx, mq.ConsumerConfig{
				Topic: "orders", Subscription: "billing", Metadata: mode,
			})
			if err != nil {
				t.Fatalf("Consumer: %v", err)
			}
			p, err := client.Publisher(ctx, mq.PublisherConfig{Topic: "orders", Metadata: mode})
			if err != nil {
				t.Fatalf("Publisher: %v", err)
			}
			if err := p.Publish(ctx, richMessage()); err != nil {
				t.Fatalf("Publish: %v", err)
			}
			ds := receiveN(t, ctx, c, 1)
			assertRich(t, &ds[0].Message)
			if err := ds[0].Ack(); err != nil {
				t.Fatalf("Ack: %v", err)
			}
		})
	}
}

// TestTheEnvelopeModeChangesTheBytes is ADR-008's other claim: the enveloping
// mode is interop-affecting, and this is the interop it affects. A source in
// native mode on the same topic sees the envelope as the body, exactly as a
// consumer written against the broker's own client would — which is why the specification
// makes the mode explicit and never reaches it by accident.
func TestTheEnvelopeModeChangesTheBytes(t *testing.T) {
	ctx := context.Background()
	client, _ := memory.NewForTest(t)
	// The native reader is opened first so that the subscription exists.
	native, err := client.Consumer(ctx, mq.ConsumerConfig{Topic: "orders", Subscription: "raw"})
	if err != nil {
		t.Fatalf("Consumer: %v", err)
	}
	p, err := client.Publisher(ctx, mq.PublisherConfig{
		Topic: "orders", Metadata: mq.MetadataPortableEnvelope,
	})
	if err != nil {
		t.Fatalf("Publisher: %v", err)
	}
	if err := p.Publish(ctx, richMessage()); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	ds := receiveN(t, ctx, native, 1)
	kind, ok := wire.Detect(ds[0].Body)
	if !ok {
		t.Fatalf("a native reader received %d bytes that are not an iqmq envelope", len(ds[0].Body))
	}
	if kind != wire.Auto.Resolve() {
		t.Fatalf("the envelope is layout %v, want the recorded default %v", kind, wire.Auto.Resolve())
	}
	// The metadata is in the envelope and not in the native fields, which is
	// the whole of what "the bytes change" means.
	if ds[0].Headers.Len() != 0 || ds[0].ID != "" || len(ds[0].Key) != 0 {
		t.Fatalf("an enveloped publication also filled the native fields: %d headers, id %q, key %q",
			ds[0].Headers.Len(), ds[0].ID, ds[0].Key)
	}
	// And it decodes back to the message that was published, which is what
	// makes the envelope portable rather than merely opaque.
	var back mq.Message
	if err := mustLayout(t, wire.Auto).Decode(ds[0].Body, &back, nil); err != nil {
		t.Fatalf("Decode: %v", err)
	}
	assertRich(t, &back)
	if err := ds[0].Ack(); err != nil {
		t.Fatalf("Ack: %v", err)
	}
}

// TestTheEnvelopeModeKeepsThePartitionKey: an enveloped message carries its
// key in the envelope, and the engine must still partition by it. A keyless
// publication is spread over the partitions, which breaks per-key order on a
// topic with more than one.
func TestTheEnvelopeModeKeepsThePartitionKey(t *testing.T) {
	ctx := context.Background()
	_, b := memory.NewForTest(t, memory.TestTopology(memory.TopologyConfig{Partitions: 4}), memory.TestTopics("orders"))
	src, err := b.OpenSource(ctx, mq.SourceConfig{
		Topic: "orders", Subscription: "billing", Prefetch: mq.DefaultPrefetch,
		Metadata: mq.MetadataPortableEnvelope,
	})
	if err != nil {
		t.Fatalf("OpenSource: %v", err)
	}
	t.Cleanup(func() { _ = src.Close() })
	snk, err := b.OpenSink(ctx, mq.SinkConfig{Topic: "orders", Metadata: mq.MetadataPortableEnvelope})
	if err != nil {
		t.Fatalf("OpenSink: %v", err)
	}
	t.Cleanup(func() { _ = snk.Close() })
	const n = 8
	for i := range n {
		if err := snk.Publish(ctx, &mq.Message{Key: []byte("cust-42"), Body: []byte{byte(i)}}, &mq.PublishOptions{}); err != nil {
			t.Fatalf("Publish %d: %v", i, err)
		}
	}
	dst := make([]mq.DriverDelivery, n)
	got, err := src.Fetch(ctx, dst)
	if err != nil || got != n {
		t.Fatalf("Fetch: n=%d err=%v", got, err)
	}
	for i := range got {
		if dst[i].Info.Partition != dst[0].Info.Partition || !bytes.Equal(dst[i].Message.Key, []byte("cust-42")) {
			t.Fatalf("delivery %d: partition %d key %q; the first was on partition %d",
				i, dst[i].Info.Partition, dst[i].Message.Key, dst[0].Info.Partition)
		}
	}
}

// TestACorruptEnvelopeIsReported pins the other side of the mode: a source
// reading in envelope mode from a publisher that was not enveloping reports
// the body as undecodable rather than handing over nonsense.
func TestACorruptEnvelopeIsReported(t *testing.T) {
	ctx := context.Background()
	_, b := memory.NewForTest(t, memory.TestTopics("orders"))
	src, err := b.OpenSource(ctx, mq.SourceConfig{
		Topic:        "orders",
		Subscription: "billing",
		Prefetch:     mq.DefaultPrefetch,
		Metadata:     mq.MetadataPortableEnvelope,
	})
	if err != nil {
		t.Fatalf("OpenSource: %v", err)
	}
	t.Cleanup(func() { _ = src.Close() })
	publish(t, ctx, b, "orders", &mq.Message{Body: []byte("not an envelope at all")})
	n, err := src.Fetch(ctx, make([]mq.DriverDelivery, 1))
	if n != 0 {
		t.Fatalf("Fetch handed over %d deliveries it could not decode", n)
	}
	if mq.CodeOf(err) != mq.CodeCorruptMessage {
		t.Fatalf("Fetch of an undecodable body: %v (code %v), want CodeCorruptMessage", err, mq.CodeOf(err))
	}
}

// TestACorruptEnvelopeDoesNotStrandTheBatch pins that the deliveries after
// an undecodable one go back to their lane: with no ack deadline nothing
// would ever expire their leases, and the good publication behind the
// corrupt one would never be delivered.
func TestACorruptEnvelopeDoesNotStrandTheBatch(t *testing.T) {
	ctx := context.Background()
	_, b := memory.NewForTest(t, memory.TestTopics("orders"))
	src, err := b.OpenSource(ctx, mq.SourceConfig{
		Topic:        "orders",
		Subscription: "billing",
		Prefetch:     mq.DefaultPrefetch,
		Metadata:     mq.MetadataPortableEnvelope,
		AckDeadline:  -1,
	})
	if err != nil {
		t.Fatalf("OpenSource: %v", err)
	}
	t.Cleanup(func() { _ = src.Close() })
	sink, err := b.OpenSink(ctx, mq.SinkConfig{Topic: "orders", Metadata: mq.MetadataPortableEnvelope})
	if err != nil {
		t.Fatalf("OpenSink: %v", err)
	}
	t.Cleanup(func() { _ = sink.Close() })
	if err := sink.Publish(ctx, &mq.Message{Body: []byte("first")}, &mq.PublishOptions{}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	publish(t, ctx, b, "orders", &mq.Message{Body: []byte("not an envelope at all")})
	if err := sink.Publish(ctx, &mq.Message{Body: []byte("last")}, &mq.PublishOptions{}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	dst := make([]mq.DriverDelivery, 3)
	n, err := src.Fetch(ctx, dst)
	if n != 1 || mq.CodeOf(err) != mq.CodeCorruptMessage {
		t.Fatalf("Fetch = (%d, %v), want the one good delivery before the corrupt one and CodeCorruptMessage", n, err)
	}
	if string(dst[0].Message.Body) != "first" {
		t.Fatalf("Fetch handed over %q, want %q", dst[0].Message.Body, "first")
	}
	for range 3 {
		poll, cancel := context.WithTimeout(ctx, time.Second)
		n, _ = src.Fetch(poll, dst)
		cancel()
		for _, d := range dst[:n] {
			if string(d.Message.Body) == "last" {
				return
			}
		}
	}
	t.Fatal("the delivery after a corrupt envelope was stranded under its lease")
}

// TestAnInvalidMetadataModeIsRefused pins the boundary check: a mode outside
// the enumeration is a configuration failure rather than a silent native.
func TestAnInvalidMetadataModeIsRefused(t *testing.T) {
	ctx := context.Background()
	_, b := memory.NewForTest(t, memory.TestTopics("orders"))
	if _, err := b.OpenSink(ctx, mq.SinkConfig{Topic: "orders", Metadata: mq.MetadataMode(99)}); mq.CodeOf(err) != mq.CodeInvalidConfig {
		t.Fatalf("OpenSink with mode 99: %v (code %v)", err, mq.CodeOf(err))
	}
	if _, err := b.OpenSource(ctx, mq.SourceConfig{
		Topic: "orders", Subscription: "s", Prefetch: 1, Metadata: mq.MetadataMode(99),
	}); mq.CodeOf(err) != mq.CodeInvalidConfig {
		t.Fatalf("OpenSource with mode 99: %v (code %v)", err, mq.CodeOf(err))
	}
	// And an unresolved prefetch, which the core always resolves before it
	// opens a source: a driver that saw zero here was handed a config
	// nothing resolved.
	if _, err := b.OpenSource(ctx, mq.SourceConfig{
		Topic: "orders", Subscription: "s", Prefetch: 0,
	}); mq.CodeOf(err) != mq.CodeInvalidConfig {
		t.Fatalf("OpenSource with no prefetch: %v (code %v)", err, mq.CodeOf(err))
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
