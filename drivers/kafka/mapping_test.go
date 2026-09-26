package kafka_test

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	mq "github.com/iqhive/iqmq"
	kq "github.com/iqhive/iqmq/drivers/kafka"
)

// TestTopicMapping: the mapping is the identity plus the prefix, and a name
// Kafka cannot carry is refused rather than transformed.
func TestTopicMapping(t *testing.T) {
	for _, tc := range []struct {
		name, prefix, topic, want string
		code                      mq.ErrorCode
	}{
		{"plain", "", "orders", "orders", 0},
		{"dots and dashes", "", "orders.eu-west_1", "orders.eu-west_1", 0},
		{"prefixed", "acme.", "orders", "acme.orders", 0},
		{"empty", "", "", "", mq.CodeInvalidArgument},
		{"slash", "", "orders/eu", "", mq.CodeInvalidArgument},
		{"space", "", "orders eu", "", mq.CodeInvalidArgument},
		{"dot", "", ".", "", mq.CodeInvalidArgument},
		{"dotdot", "", "..", "", mq.CodeInvalidArgument},
		{"too long", "", strings.Repeat("x", 250), "", mq.CodeInvalidArgument},
		{"prefix pushes it over", strings.Repeat("p", 245), "orders", "", mq.CodeInvalidArgument},
		// A regular-expression metacharacter is somebody asking for a
		// pattern subscription, which this driver does not offer: the specification
		// makes that ErrUnsupported and not "invalid topic", because the
		// capability table says the feature is absent rather than that the
		// name is wrong.
		{"pattern", "", `^orders\..*$`, "", mq.CodeUnsupported},
		{"star", "", "orders.*", "", mq.CodeUnsupported},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := kq.Topic(tc.prefix, tc.topic)
			if tc.code == 0 {
				if err != nil || got != tc.want {
					t.Fatalf("Topic(%q, %q) = %q, %v", tc.prefix, tc.topic, got, err)
				}
				if back := kq.TopicOf(tc.prefix, got); back != tc.topic {
					t.Fatalf("TopicOf(%q, %q) = %q, want %q", tc.prefix, got, back, tc.topic)
				}
				return
			}
			if mq.CodeOf(err) != tc.code {
				t.Fatalf("Topic(%q, %q) = %q, %v (code %v), want code %v",
					tc.prefix, tc.topic, got, err, mq.CodeOf(err), tc.code)
			}
		})
	}
}

// TestRecordRoundTrip is the mapping's whole contract: everything the specification names
// survives Encode then Decode, byte for byte, including the distinctions
// The specification makes observable.
func TestRecordRoundTrip(t *testing.T) {
	created := time.Date(2026, 3, 4, 5, 6, 7, 890123456, time.UTC)
	for _, tc := range []struct {
		name string
		m    mq.Message
	}{
		{"empty", mq.Message{}},
		{"body only", mq.Message{Body: []byte("hi")}},
		{"nil body", mq.Message{Key: []byte("k")}},
		{"empty non-nil body", mq.Message{Body: []byte{}}},
		{"nil key", mq.Message{Body: []byte("b")}},
		{"empty non-nil key", mq.Message{Key: []byte{}, Body: []byte("b")}},
		{"binary key and body", mq.Message{Key: []byte{0, 1, 0xff}, Body: []byte{0xfe, 0, 0x80}}},
		{"identifier", mq.Message{ID: "01JABCDEF", Body: []byte("b")}},
		{"properties", mq.Message{
			Body: []byte("b"),
			Properties: mq.Properties{
				ContentType:     "application/json",
				ContentEncoding: "gzip",
				CorrelationID:   "c-1",
				CausationID:     "c-0",
				ReplyTo:         "replies",
				CreatedAt:       created,
			},
		}},
		{"created only", mq.Message{Body: []byte("b"), Properties: mq.Properties{CreatedAt: created}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var rec kgo.Record
			kq.Encode(&rec, &tc.m, "orders", nil)
			var got mq.Message
			kq.Decode(&got, &rec, "")
			assertSameMessage(t, &tc.m, &got)
		})
	}
}

// TestHeaderRoundTrip covers the three promises on a broker that keeps all
// three natively: order, duplicates and binary values.
func TestHeaderRoundTrip(t *testing.T) {
	var m mq.Message
	m.Body = []byte("b")
	m.Headers.Add("b-first", []byte("1"))
	m.Headers.Add("a-second", []byte{0, 0xff, 0x7f})
	m.Headers.Add("b-first", []byte("2"))
	m.Headers.Add("nil-value", nil)
	m.Headers.Add("empty-value", []byte{})

	var rec kgo.Record
	kq.Encode(&rec, &m, "orders", nil)
	var got mq.Message
	kq.Decode(&got, &rec, "")

	if got.Headers.Len() != m.Headers.Len() {
		t.Fatalf("%d headers back, want %d", got.Headers.Len(), m.Headers.Len())
	}
	wantOrder := []string{"b-first", "a-second", "b-first", "nil-value", "empty-value"}
	i := 0
	for k := range got.Headers.All() {
		if k != wantOrder[i] {
			t.Fatalf("header %d is %q, want %q: the specification keeps the publication order", i, k, wantOrder[i])
		}
		i++
	}
	vals := got.Headers.Values("b-first")
	if len(vals) != 2 || string(vals[0]) != "1" || string(vals[1]) != "2" {
		t.Fatalf("duplicate values %q: the specification keeps both, in order", vals)
	}
	if v, ok := got.Headers.Get("a-second"); !ok || !bytes.Equal(v, []byte{0, 0xff, 0x7f}) {
		t.Fatalf("binary header value %v, ok=%v", v, ok)
	}
}

// TestRepresentableRefusesTheReservedPrefix: the specification reserves iqmq- for iqmq's
// own metadata, and a caller's header under it is refused rather than renamed
// — renaming it would make the message decode to something else.
func TestRepresentableRefusesTheReservedPrefix(t *testing.T) {
	var m mq.Message
	m.Headers.Add(mq.ReservedHeaderPrefix+"id", []byte("mine"))
	if err := kq.Representable(&m); mq.CodeOf(err) != mq.CodeInvalidArgument {
		t.Fatalf("Representable = %v (code %v)", err, mq.CodeOf(err))
	}
	var empty mq.Message
	empty.Headers.Add("", []byte("v"))
	if err := kq.Representable(&empty); mq.CodeOf(err) != mq.CodeInvalidArgument {
		t.Fatalf("Representable with an empty key = %v", err)
	}
	// Everything else Kafka can carry, which is why strict mode refuses
	// almost nothing on this driver.
	var fine mq.Message
	fine.Headers.Add("content-kind", []byte{0, 1, 2})
	fine.Headers.Add("content-kind", []byte("again"))
	if err := kq.Representable(&fine); err != nil {
		t.Fatalf("Representable = %v; Kafka headers are ordered, binary and duplicate-preserving", err)
	}
	if err := kq.Representable(nil); err == nil {
		t.Fatal("Representable(nil) was accepted")
	}
}

// TestCreatedAtNeedsItsMarker: Kafka stamps every record with a timestamp
// whether the publisher set one or not, so a decoder that read it as
// Properties.CreatedAt would invent a creation time for every message that
// carried none.
func TestCreatedAtNeedsItsMarker(t *testing.T) {
	var m mq.Message
	m.Body = []byte("b")
	var rec kgo.Record
	kq.Encode(&rec, &m, "orders", nil)
	// What franz-go does at produce time, and what a non-iqmq producer
	// leaves behind.
	rec.Timestamp = time.Now()
	var got mq.Message
	kq.Decode(&got, &rec, "")
	if !got.Properties.CreatedAt.IsZero() {
		t.Fatalf("CreatedAt came back as %v for a message that set none", got.Properties.CreatedAt)
	}
}

// TestAttemptHeader: the attempt count survives a republication, because
// Kafka's own attempt count does not — a freshly produced record is a first
// delivery as far as the broker knows.
func TestAttemptHeader(t *testing.T) {
	var rec kgo.Record
	if got := kq.Attempt(&rec); got != 0 {
		t.Fatalf("Attempt of a record with no header = %d, want 0 (the broker does not count)", got)
	}
	for _, tc := range []struct {
		val  string
		want uint32
	}{
		{"1", 1},
		{"7", 7},
		{"4294967295", 4294967295},
		{"", 0},
		{"x", 0},
		{"-1", 0},
		{"4294967296", 0},
	} {
		rec.Headers = []kgo.RecordHeader{{Key: "other", Value: []byte("1")}, {Key: kq.HeaderAttempts, Value: []byte(tc.val)}}
		if got := kq.Attempt(&rec); got != tc.want {
			t.Fatalf("Attempt(%q) = %d, want %d", tc.val, got, tc.want)
		}
	}
}

// TestRouteToPartition: a caller that named a partition gets it, and the
// marker survives franz-go writing the chosen partition into the record's own
// Partition field — which is the whole reason the marker is not that field.
func TestRouteToPartition(t *testing.T) {
	var rec kgo.Record
	m := mq.Message{Body: []byte("b")}
	kq.Encode(&rec, &m, "orders", nil)
	p := kq.Partitioner().ForTopic("orders")
	if got := p.Partition(&rec, 8); got < 0 || got >= 8 {
		t.Fatalf("an unrouted keyless record went to %d", got)
	}
	kq.RouteToPartition(context.Background(), &rec, 5)
	if got := p.Partition(&rec, 8); got != 5 {
		t.Fatalf("Partition = %d, want the 5 the caller named", got)
	}
	if !p.RequiresConsistency(&rec) {
		t.Fatal("a routed record does not require consistency, so franz-go could move it off the partition the caller named")
	}
	// franz-go writes the partition it picked into the record before it
	// buffers it, and asks again on a batch boundary. The marker has to
	// survive that.
	rec.Partition = 3
	if got := p.Partition(&rec, 8); got != 5 {
		t.Fatalf("after franz-go wrote Partition=3, Partition = %d, want the 5 the caller named", got)
	}
}

// assertSameMessage compares two messages field by field, including the nil
// versus empty distinctions the specification makes observable.
func assertSameMessage(t *testing.T, want, got *mq.Message) {
	t.Helper()
	if !sameBytes(want.Key, got.Key) {
		t.Errorf("Key %v (nil=%v) came back as %v (nil=%v)", want.Key, want.Key == nil, got.Key, got.Key == nil)
	}
	if !sameBytes(want.Body, got.Body) {
		t.Errorf("Body %v (nil=%v) came back as %v (nil=%v)", want.Body, want.Body == nil, got.Body, got.Body == nil)
	}
	if want.ID != got.ID {
		t.Errorf("ID %q came back as %q", want.ID, got.ID)
	}
	if want.Properties != got.Properties {
		t.Errorf("Properties %+v came back as %+v", want.Properties, got.Properties)
	}
	if want.Headers.Len() != got.Headers.Len() {
		t.Errorf("%d headers came back as %d", want.Headers.Len(), got.Headers.Len())
	}
}

// sameBytes compares two payloads, distinguishing nil from empty.
func sameBytes(a, b []byte) bool {
	if (a == nil) != (b == nil) {
		return false
	}
	return bytes.Equal(a, b)
}

// TestCheckGroup: a subscription is the consumer group, and Kafka has no
// anonymous one.
func TestCheckGroup(t *testing.T) {
	if err := kq.CheckGroup(""); mq.CodeOf(err) != mq.CodeInvalidArgument {
		t.Fatalf("CheckGroup(\"\") = %v, want an invalid-argument refusal", err)
	}
	if err := kq.CheckGroup("orders-worker"); err != nil {
		t.Fatalf("CheckGroup = %v", err)
	}
	if err := kq.CheckGroup("with\nnewline"); mq.CodeOf(err) != mq.CodeInvalidArgument {
		t.Fatalf("CheckGroup with a newline = %v", err)
	}
	if err := kq.CheckGroup(strings.Repeat("g", 256)); mq.CodeOf(err) != mq.CodeInvalidArgument {
		t.Fatalf("CheckGroup with 256 characters = %v", err)
	}
}
