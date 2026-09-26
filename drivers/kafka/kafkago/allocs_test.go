package kafkago_test

import (
	"testing"

	segkafka "github.com/segmentio/kafka-go"

	mq "github.com/iqhive/iqmq"
	kq "github.com/iqhive/iqmq/drivers/kafka/kafkago"
)

// The allocation budget of the parts of this driver that are on a message's
// path and are the driver's own ("AllocsPerRun for mapping").
//
// The mapping and the balancer are where a driver can quietly cost a caller an
// allocation per message, and they are the only two pieces of this driver that
// run per message and belong to iqmq rather than to kafka-go: the produce and
// fetch paths themselves are the client library's, and measuring them here
// would measure kafka-go.
//
// One allocation on the delivery path is the CLIENT's and is recorded rather
// than budgeted: kafka-go's Reader serves a [segkafka.Message] by value, and a
// DriverDelivery's Native is a pointer, so the value is copied to the heap
// once per delivery. The franz-go driver gets a pointer from its client and
// pays nothing (recorded in the maintenance notes).

// TestAllocsEncode: encoding into a reused message allocates only the header
// slice's growth, and nothing at all once it has grown.
func TestAllocsEncode(t *testing.T) {
	m := mq.Message{Key: []byte("k"), Body: []byte("body"), ID: "id-1"}
	m.Headers.Add("content-kind", []byte("order"))
	m.Headers.Add("tenant", []byte("acme"))
	m.Properties.ContentType = "application/json"
	var msg segkafka.Message
	// Grow both the header slice and the scratch buffer once, which is what
	// the sink's pooled message buffer does for the life of a process.
	scratch := kq.Encode(&msg, &m, "orders", nil)
	if a := testing.AllocsPerRun(200, func() { scratch = kq.Encode(&msg, &m, "orders", scratch) }); a != 0 {
		t.Fatalf("Encode into a reused message and buffer: %.1f allocs/op", a)
	}
}

// TestAllocsBalance: the partition decision is a hash and a modulo, and a
// keyed record's partition must cost nothing — it is computed for every record
// a producer sends, from kafka-go's own publish path.
func TestAllocsBalance(t *testing.T) {
	p := kq.Partitioner(1)
	msg := segkafka.Message{Key: []byte("user-42")}
	// The answer is accumulated as a bit per partition rather than summed:
	// a key whose partition happens to be zero would make a sum stay zero,
	// and the sanity check would then read as "the balancer did nothing".
	var sink uint
	if a := testing.AllocsPerRun(200, func() { sink |= 1 << p.Balance(msg, 0, 1, 2, 3) }); a != 0 {
		t.Fatalf("Balance of a keyed record: %.1f allocs/op", a)
	}
	if want := uint(1) << kq.PartitionFor(msg.Key, 4); sink != want {
		t.Fatalf("the balancer answered %b for a keyed record, want only bit %b", sink, want)
	}
	keyless := segkafka.Message{}
	sink = 0
	if a := testing.AllocsPerRun(200, func() { sink |= 1 << p.Balance(keyless, 0, 1, 2, 3) }); a != 0 {
		t.Fatalf("Balance of a keyless record: %.1f allocs/op", a)
	}
	if sink != 0b1111 {
		t.Errorf("a keyless record reached partitions %b of four; at a run length of one the balancer rotates through all of them", sink)
	}
	// And the routed path, which reads the driver's own marker out of
	// WriterData rather than allocating a context value as the franz-go
	// driver's partitioner has to.
	routed := segkafka.Message{}
	kq.Route(&routed, 2)
	sink = 0
	if a := testing.AllocsPerRun(200, func() { sink |= 1 << p.Balance(routed, 0, 1, 2, 3) }); a != 0 {
		t.Fatalf("Balance of a routed record: %.1f allocs/op", a)
	}
	if sink != 1<<2 {
		t.Errorf("a routed record reached partitions %b, want only partition 2", sink)
	}
}

// TestAllocsPartitionFor is the pure form, which the cross-driver tests and a
// caller computing a partition ahead of a publication both use.
func TestAllocsPartitionFor(t *testing.T) {
	key := []byte("user-42")
	var sink uint
	if a := testing.AllocsPerRun(200, func() { sink |= 1 << kq.PartitionFor(key, 12) }); a != 0 {
		t.Fatalf("PartitionFor: %.1f allocs/op", a)
	}
	if sink == 0 || sink != sink&-sink {
		t.Fatalf("PartitionFor answered %b for one key; it is a pure function of the key and the count", sink)
	}
}

// TestAllocsAttempt: reading the attempt header is on the delivery path of
// every retried record.
func TestAllocsAttempt(t *testing.T) {
	msg := &segkafka.Message{Headers: []segkafka.Header{
		{Key: "content-kind", Value: []byte("order")},
		{Key: kq.HeaderAttempts, Value: []byte("3")},
	}}
	var sink uint32
	if a := testing.AllocsPerRun(200, func() { sink += kq.Attempt(msg) }); a != 0 {
		t.Fatalf("Attempt: %.1f allocs/op", a)
	}
	if sink == 0 {
		t.Fatal("Attempt returned zero every time")
	}
}

// TestAllocsRepresentable: strict mode's check runs before every publication
// in the default metadata mode.
func TestAllocsRepresentable(t *testing.T) {
	var m mq.Message
	m.Body = []byte("b")
	m.Headers.Add("a", []byte("1"))
	m.Headers.Add("b", []byte("2"))
	if a := testing.AllocsPerRun(200, func() {
		if err := kq.Representable(&m); err != nil {
			t.Fatal(err)
		}
	}); a != 0 {
		t.Fatalf("Representable: %.1f allocs/op", a)
	}
}

// TestAllocsDecodeIsOnlyTheStrings: decoding hands the message's own key, body
// and header values straight through — the specification makes received header values
// read-only and the specification makes the storage borrowed until settlement — so the
// only allocations are the strings the typed properties need.
func TestAllocsDecodeIsOnlyTheStrings(t *testing.T) {
	msg := &segkafka.Message{
		Topic: "orders",
		Key:   []byte("k"),
		Value: []byte("body"),
		Headers: []segkafka.Header{
			{Key: "content-kind", Value: []byte("order")},
			{Key: "tenant", Value: []byte("acme")},
		},
	}
	var m mq.Message
	kq.Decode(&m, msg, "") // grow the header storage once
	if a := testing.AllocsPerRun(200, func() { kq.Decode(&m, msg, "") }); a != 0 {
		t.Fatalf("Decode of a message with no typed properties: %.1f allocs/op", a)
	}
	// And the values are the message's own storage, not copies.
	v, _ := m.Headers.Get("content-kind")
	if len(v) > 0 && &v[0] != &msg.Headers[0].Value[0] {
		t.Fatal("Decode copied a header value; the specification surfaces a received value without a copy")
	}
	if len(m.Body) > 0 && &m.Body[0] != &msg.Value[0] {
		t.Fatal("Decode copied the body")
	}
}
