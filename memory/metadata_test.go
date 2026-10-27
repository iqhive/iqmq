package memory_test

import (
	"bytes"
	"context"
	"strings"
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

// TestTheEnvelopeModeRefusesWhatItCouldNotReadBack pins the admission check
// of the enveloping mode. The encoder bounds only the layout's own widths and
// the source decodes under the decoder's stricter limits, so an identifier or
// a header key over 256 bytes, or an identifier that is not UTF-8, was
// accepted, stored, and then failed every Fetch: the record went back to its
// lane each time, for ever without an attempt limit, and the publication
// behind it was never delivered. It is refused at publish now, with the code
// the decode would have failed with, and nothing reaches the subscription.
func TestTheEnvelopeModeRefusesWhatItCouldNotReadBack(t *testing.T) {
	long := strings.Repeat("k", 300)
	var longKey mq.Headers
	longKey.Add(long, []byte("v"))
	cases := []struct {
		name string
		m    *mq.Message
		want mq.ErrorCode
	}{
		{"an identifier over the bound", &mq.Message{ID: long, Body: []byte("x")}, mq.CodeMessageTooLarge},
		{"a header key over the bound", &mq.Message{Headers: longKey, Body: []byte("x")}, mq.CodeMessageTooLarge},
		{"an identifier that is not UTF-8", &mq.Message{ID: "\xff\xfe", Body: []byte("x")}, mq.CodeCorruptMessage},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_, b := memory.NewForTest(t, memory.TestTopics("orders"))
			src, err := b.OpenSource(ctx, mq.SourceConfig{
				Topic: "orders", Subscription: "billing", Prefetch: 1,
				Metadata: mq.MetadataPortableEnvelope,
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
			o := &mq.PublishOptions{}
			if err := sink.Publish(ctx, tc.m, o); mq.CodeOf(err) != tc.want {
				t.Fatalf("Publish: %v (code %v), want %v: a record its own source cannot decode was admitted",
					err, mq.CodeOf(err), tc.want)
			}
			bs, ok := sink.(mq.BatchSink)
			if !ok {
				t.Fatal("the sink offers no native batch")
			}
			if err := bs.PublishBatch(ctx, []*mq.Message{{Body: []byte("fine")}, tc.m}, o); mq.CodeOf(err) != tc.want {
				t.Fatalf("PublishBatch: %v (code %v), want %v", err, mq.CodeOf(err), tc.want)
			}
			if snap, _ := b.Inspect("orders", "billing"); snap.Published != 0 {
				t.Fatalf("the subscription admitted %d records from refused publications", snap.Published)
			}
			// The lane is not blocked: the next publication is delivered.
			if err := sink.Publish(ctx, &mq.Message{Body: []byte("next")}, o); err != nil {
				t.Fatalf("Publish: %v", err)
			}
			dst := make([]mq.DriverDelivery, 1)
			if n, err := src.Fetch(ctx, dst); n != 1 || err != nil {
				t.Fatalf("Fetch = (%d, %v), want the next publication", n, err)
			}
			if got := string(dst[0].Message.Body); got != "next" {
				t.Fatalf("Fetch delivered %q, want %q", got, "next")
			}
		})
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

// TestACorruptEnvelopeSpendsNoAttemptOfTheBatch pins that the deliveries
// after an undecodable one go back without spending an attempt: they were
// never handed out, so under a one-attempt limit the valid publication
// behind a corrupt one must be delivered rather than dead-lettered unseen.
// The corrupt one itself still spends its attempt and is dead-lettered.
func TestACorruptEnvelopeSpendsNoAttemptOfTheBatch(t *testing.T) {
	ctx := context.Background()
	_, b := memory.NewForTest(t,
		memory.TestTopology(memory.TopologyConfig{DeadLetterTopic: "dlq", MaxAttempts: 1}),
		memory.TestTopics("orders", "dlq"))
	dlq, err := b.OpenSource(ctx, mq.SourceConfig{Topic: "dlq", Subscription: "audit", Prefetch: 4})
	if err != nil {
		t.Fatalf("OpenSource(dlq): %v", err)
	}
	t.Cleanup(func() { _ = dlq.Close() })
	src, err := b.OpenSource(ctx, mq.SourceConfig{
		Topic:        "orders",
		Subscription: "billing",
		Prefetch:     4,
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
	publish(t, ctx, b, "orders", &mq.Message{Body: []byte("not an envelope at all")})
	if err := sink.Publish(ctx, &mq.Message{Body: []byte("valid")}, &mq.PublishOptions{}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	dst := make([]mq.DriverDelivery, 4)
	n, err := src.Fetch(ctx, dst)
	if n != 0 || mq.CodeOf(err) != mq.CodeCorruptMessage {
		t.Fatalf("Fetch = (%d, %v), want nothing and CodeCorruptMessage", n, err)
	}
	poll, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	n, err = src.Fetch(poll, dst)
	if n != 1 || string(dst[0].Message.Body) != "valid" {
		t.Fatalf("Fetch = (%d, %v), want the valid publication behind the corrupt one", n, err)
	}
	if dst[0].Info.Attempt != 1 {
		t.Fatalf("the valid publication reports attempt %d, want 1", dst[0].Info.Attempt)
	}
	snap, err := b.Inspect("orders", "billing")
	if err != nil {
		t.Fatalf("Inspect: %v", err)
	}
	if snap.DeadLettered != 1 {
		t.Fatalf("snapshot %+v, want only the corrupt publication dead-lettered", snap)
	}
	n, err = dlq.Fetch(poll, dst)
	if n != 1 || string(dst[0].Message.Body) != "not an envelope at all" {
		t.Fatalf("dead-letter Fetch = (%d, %v), want the corrupt publication", n, err)
	}
}

// TestAnExplicitDeadLetterKeepsTheEnvelope pins that a dead-letter action
// publishes in the format the engine's own attempt-limit route does: the
// stored, still-enveloped message. A source reading the dead-letter topic in
// envelope mode then decodes what either route sent.
func TestAnExplicitDeadLetterKeepsTheEnvelope(t *testing.T) {
	ctx := context.Background()
	_, b := memory.NewForTest(t,
		memory.TestTopology(memory.TopologyConfig{DeadLetterTopic: "dlq"}),
		memory.TestTopics("orders", "dlq"))
	open := func(topic, sub string) mq.Source {
		t.Helper()
		src, err := b.OpenSource(ctx, mq.SourceConfig{
			Topic:        topic,
			Subscription: sub,
			Prefetch:     1,
			Metadata:     mq.MetadataPortableEnvelope,
		})
		if err != nil {
			t.Fatalf("OpenSource(%s): %v", topic, err)
		}
		t.Cleanup(func() { _ = src.Close() })
		return src
	}
	dlq := open("dlq", "audit")
	src := open("orders", "billing")
	sink, err := b.OpenSink(ctx, mq.SinkConfig{Topic: "orders", Metadata: mq.MetadataPortableEnvelope})
	if err != nil {
		t.Fatalf("OpenSink: %v", err)
	}
	t.Cleanup(func() { _ = sink.Close() })
	if err := sink.Publish(ctx, &mq.Message{Key: []byte("k"), Body: []byte("poison")}, &mq.PublishOptions{}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	dst := make([]mq.DriverDelivery, 1)
	if n, err := src.Fetch(ctx, dst); n != 1 {
		t.Fatalf("Fetch = (%d, %v), want the publication", n, err)
	}
	if err := src.Settler().Nack(ctx, &dst[0], mq.NackOptions{Action: mq.NackActionDeadLetter}); err != nil {
		t.Fatalf("Nack(DeadLetter): %v", err)
	}
	poll, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	n, err := dlq.Fetch(poll, dst)
	if n != 1 || err != nil {
		t.Fatalf("dead-letter Fetch = (%d, %v), want the enveloped publication", n, err)
	}
	if string(dst[0].Message.Body) != "poison" || string(dst[0].Message.Key) != "k" {
		t.Fatalf("the dead-letter topic received key %q body %q", dst[0].Message.Key, dst[0].Message.Body)
	}
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
