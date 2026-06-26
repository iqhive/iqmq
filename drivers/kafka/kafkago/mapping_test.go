package kafkago_test

import (
	"bytes"
	"testing"
	"time"

	segkafka "github.com/segmentio/kafka-go"

	mq "bitbucket.org/iqhive/iqmq"
	kq "bitbucket.org/iqhive/iqmq/drivers/kafka/kafkago"
)

// TestEncodeDecodeRoundTrip is the strict mode as a pure function: every
// field the specification names comes back exactly as it was published.
func TestEncodeDecodeRoundTrip(t *testing.T) {
	created := time.Now().UTC().Truncate(time.Millisecond)
	for _, tc := range []struct {
		name string
		m    mq.Message
	}{
		{"empty", mq.Message{}},
		{"body-only", mq.Message{Body: []byte("b")}},
		{"nil-body", mq.Message{Key: []byte("k")}},
		{"empty-body", mq.Message{Key: []byte("k"), Body: []byte{}}},
		{"all-properties", mq.Message{
			ID:   "id",
			Key:  []byte("k"),
			Body: []byte("b"),
			Properties: mq.Properties{
				ContentType:     "application/json",
				ContentEncoding: "gzip",
				CorrelationID:   "corr",
				CausationID:     "cause",
				ReplyTo:         "reply",
				CreatedAt:       created,
			},
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			src := tc.m
			var msg segkafka.Message
			_ = kq.Encode(&msg, &src, "topic", nil)
			var got mq.Message
			kq.Decode(&got, &msg, "")
			if got.ID != src.ID {
				t.Errorf("ID %q, want %q", got.ID, src.ID)
			}
			if !bytes.Equal(got.Key, src.Key) || (got.Key == nil) != (src.Key == nil) {
				t.Errorf("Key %q (nil %v), want %q (nil %v)", got.Key, got.Key == nil, src.Key, src.Key == nil)
			}
			if !bytes.Equal(got.Body, src.Body) || (got.Body == nil) != (src.Body == nil) {
				t.Errorf("Body %q (nil %v), want %q (nil %v)", got.Body, got.Body == nil, src.Body, src.Body == nil)
			}
			if got.Properties.ContentType != src.Properties.ContentType ||
				got.Properties.ContentEncoding != src.Properties.ContentEncoding ||
				got.Properties.CorrelationID != src.Properties.CorrelationID ||
				got.Properties.CausationID != src.Properties.CausationID ||
				got.Properties.ReplyTo != src.Properties.ReplyTo {
				t.Errorf("properties %+v, want %+v", got.Properties, src.Properties)
			}
			if !got.Properties.CreatedAt.Equal(src.Properties.CreatedAt) {
				t.Errorf("CreatedAt %v, want %v", got.Properties.CreatedAt, src.Properties.CreatedAt)
			}
		})
	}
}

// TestCreatedAtIsNotInvented is the reason the iqmq-created marker exists.
//
// Kafka stamps every record with a timestamp whether the publisher set one or
// not, so a decoder that read the native timestamp as CreatedAt would invent a
// creation time for every message that carried none — which the strict mode
// forbids, because the field would not come back as it was published.
func TestCreatedAtIsNotInvented(t *testing.T) {
	var msg segkafka.Message
	src := mq.Message{Body: []byte("b")}
	_ = kq.Encode(&msg, &src, "topic", nil)
	// What the broker does: it stamps the record.
	msg.Time = time.Now()
	var got mq.Message
	kq.Decode(&got, &msg, "")
	if !got.Properties.CreatedAt.IsZero() {
		t.Fatalf("CreatedAt came back as %v for a message that carried none: the native timestamp is the produce time unless the publisher marked it", got.Properties.CreatedAt)
	}
	for _, h := range msg.Headers {
		if h.Key == kq.HeaderCreated {
			t.Fatalf("the marker %q was written for a message with no CreatedAt", h.Key)
		}
	}
}

// TestHeadersAreOrderedAndDuplicatePreserving is what makes
// FeatureDuplicateHeaders a native claim: Kafka's headers are a list, not a
// map, so two values under one key survive and keep their order.
func TestHeadersAreOrderedAndDuplicatePreserving(t *testing.T) {
	src := mq.Message{Body: []byte("b")}
	src.Headers.Add("a", []byte("1"))
	src.Headers.Add("b", []byte("2"))
	src.Headers.Add("a", []byte("3"))
	var msg segkafka.Message
	_ = kq.Encode(&msg, &src, "topic", nil)
	var got mq.Message
	kq.Decode(&got, &msg, "")
	var seen []string
	for k, v := range got.Headers.All() {
		seen = append(seen, k+"="+string(v))
	}
	want := []string{"a=1", "b=2", "a=3"}
	if len(seen) != len(want) {
		t.Fatalf("headers came back as %q, want %q", seen, want)
	}
	for i := range want {
		if seen[i] != want[i] {
			t.Fatalf("headers came back as %q, want %q", seen, want)
		}
	}
}

// TestRepresentableRefusesReservedHeaders is a caller's header under the
// reserved prefix is refused rather than silently renamed, because renaming it
// would make the driver's own mapping ambiguous.
func TestRepresentableRefusesReservedHeaders(t *testing.T) {
	var m mq.Message
	m.Headers.Add(mq.ReservedHeaderPrefix+"mine", []byte("x"))
	if err := kq.Representable(&m); err == nil {
		t.Fatal("Representable accepted a caller's header under the reserved prefix")
	} else if mq.CodeOf(err) != mq.CodeInvalidArgument {
		t.Errorf("Representable refused with %v, want CodeInvalidArgument", mq.CodeOf(err))
	}
	if err := kq.Representable(nil); err == nil {
		t.Error("Representable accepted a nil message")
	}
	var ok mq.Message
	ok.Headers.Add("mine", []byte("x"))
	if err := kq.Representable(&ok); err != nil {
		t.Errorf("Representable refused an ordinary header: %v", err)
	}
}

// TestTopicPrefixIsStrippedOnTheWayBack is the resolved topic: a delivery
// names the topic the CALLER published to, not the prefixed one on the wire.
func TestTopicPrefixIsStrippedOnTheWayBack(t *testing.T) {
	name, err := kq.Topic("acme.", "orders")
	if err != nil {
		t.Fatalf("Topic: %v", err)
	}
	if name != "acme.orders" {
		t.Fatalf("Topic produced %q", name)
	}
	if got := kq.TopicOf("acme.", name); got != "orders" {
		t.Fatalf("TopicOf produced %q, want %q", got, "orders")
	}
	// A prefix that pushes a legal name over Kafka's 249 characters is
	// refused, because the prefixed name is what the broker sees.
	long := string(bytes.Repeat([]byte("a"), 249))
	if _, err := kq.Topic("p.", long); err == nil {
		t.Fatal("a prefixed name longer than 249 characters was accepted")
	}
}

// TestAttemptIsOnlyWhatTheDriverWrote is the rule that a zero attempt means
// "the broker does not count attempts", which is the truth for Kafka: a record
// redelivered because its offset was never committed looks exactly like a
// first delivery.
func TestAttemptIsOnlyWhatTheDriverWrote(t *testing.T) {
	var msg segkafka.Message
	if got := kq.Attempt(&msg); got != 0 {
		t.Errorf("Attempt of a plain record = %d, want 0", got)
	}
	msg.Headers = append(msg.Headers, segkafka.Header{Key: kq.HeaderAttempts, Value: []byte("3")})
	if got := kq.Attempt(&msg); got != 3 {
		t.Errorf("Attempt = %d, want 3", got)
	}
	msg.Headers[0].Value = []byte("not-a-number")
	if got := kq.Attempt(&msg); got != 0 {
		t.Errorf("Attempt of an unreadable count = %d, want 0", got)
	}
}

// TestRouteTravelsInWriterData is the reason Partition control works at
// all on this client.
//
// kafka-go documents Message.Partition as "read-only and MUST NOT be set when
// writing", and the writer overwrites it with the partition the balancer
// picked before the completion runs — so a driver that used it could not tell
// "partition 0 because the caller said so" from "partition 0 because the
// balancer chose it". WriterData is the field that survives.
func TestRouteTravelsInWriterData(t *testing.T) {
	p := kq.Partitioner(1)
	var plain segkafka.Message
	plain.Key = nil
	// Without a route, a keyless record rotates: over four partitions the
	// first four calls must not all answer the same one.
	first := p.Balance(plain, 0, 1, 2, 3)
	rotated := false
	for range 4 {
		if p.Balance(plain, 0, 1, 2, 3) != first {
			rotated = true
		}
	}
	if !rotated {
		t.Error("a keyless record went to the same partition five times running; the balancer is meant to rotate every BatchMaxRecords records")
	}
	// With a route, it goes where the caller said, every time.
	var routed segkafka.Message
	kq.Route(&routed, 2)
	for range 5 {
		if got := p.Balance(routed, 0, 1, 2, 3); got != 2 {
			t.Fatalf("a routed record went to partition %d, want 2", got)
		}
	}
	// Setting Message.Partition instead must NOT route it, which is what
	// proves the driver is not relying on the field kafka-go reserves.
	var viaField segkafka.Message
	viaField.Partition = 3
	viaField.Key = []byte("k")
	if got, want := p.Balance(viaField, 0, 1, 2, 3), kq.PartitionFor([]byte("k"), 4); got != want {
		t.Errorf("a record with Partition set went to %d; the key's own partition is %d and Message.Partition is not a routing instruction", got, want)
	}
}

// TestBalancerReturnsAPartitionIDNotAnIndex is the bug a balancer written
// against the obvious reading of kafka-go's interface would have: Balance is
// handed the topic's partition IDs and its answer is used as an ID, so a
// balancer that returned an index would be right only for a topic whose
// partitions happen to be numbered from zero.
func TestBalancerReturnsAPartitionIDNotAnIndex(t *testing.T) {
	p := kq.Partitioner(1)
	ids := []int{10, 11, 12, 13}
	for _, key := range [][]byte{[]byte("a"), []byte("b"), []byte("key-1")} {
		var msg segkafka.Message
		msg.Key = key
		got := p.Balance(msg, ids...)
		want := ids[kq.PartitionFor(key, len(ids))]
		if got != want {
			t.Errorf("key %q over partitions %v: Balance said %d, want the ID %d", key, ids, got, want)
		}
	}
}
