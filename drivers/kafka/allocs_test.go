package kafka_test

import (
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"

	mq "bitbucket.org/iqhive/iqmq"
	kq "bitbucket.org/iqhive/iqmq/drivers/kafka"
)

// The allocation budget of the parts of this driver that are on a message's
// path and are the driver's own ("AllocsPerRun for mapping").
//
// The mapping and the partitioner are where a driver can quietly cost a
// caller an allocation per message, and they are the only two pieces of this
// driver that run per message and belong to iqmq rather than to franz-go: the
// produce path itself is the client library's, and measuring it here would
// measure franz-go.

// TestAllocsEncode: encoding into a reused record allocates only the header
// slice's growth, and nothing at all once it has grown.
func TestAllocsEncode(t *testing.T) {
	m := mq.Message{Key: []byte("k"), Body: []byte("body"), ID: "id-1"}
	m.Headers.Add("content-kind", []byte("order"))
	m.Headers.Add("tenant", []byte("acme"))
	m.Properties.ContentType = "application/json"
	var rec kgo.Record
	// Grow both the header slice and the scratch buffer once, which is what
	// the sink's pooled record buffer does for the life of a process.
	scratch := kq.Encode(&rec, &m, "orders", nil)
	if a := testing.AllocsPerRun(200, func() { scratch = kq.Encode(&rec, &m, "orders", scratch) }); a != 0 {
		t.Fatalf("Encode into a reused record and buffer: %.1f allocs/op", a)
	}
}

// TestAllocsPartition: the partition decision is a hash and a modulo, and a
// keyed record's partition must cost nothing — it is computed for every
// record a producer sends.
func TestAllocsPartition(t *testing.T) {
	p := kq.Partitioner().ForTopic("orders")
	rec := &kgo.Record{Key: []byte("user-42")}
	var sink int
	if a := testing.AllocsPerRun(200, func() { sink += p.Partition(rec, 12) }); a != 0 {
		t.Fatalf("Partition of a keyed record: %.1f allocs/op", a)
	}
	if sink == 0 {
		t.Fatal("the partitioner returned zero every time")
	}
	keyless := &kgo.Record{}
	if a := testing.AllocsPerRun(200, func() { sink += p.Partition(keyless, 12) }); a != 0 {
		t.Fatalf("Partition of a keyless record: %.1f allocs/op", a)
	}
}

// TestAllocsAttempt: reading the attempt header is on the delivery path of
// every retried record.
func TestAllocsAttempt(t *testing.T) {
	rec := &kgo.Record{Headers: []kgo.RecordHeader{
		{Key: "content-kind", Value: []byte("order")},
		{Key: kq.HeaderAttempts, Value: []byte("3")},
	}}
	var sink uint32
	if a := testing.AllocsPerRun(200, func() { sink += kq.Attempt(rec) }); a != 0 {
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

// TestAllocsDecodeIsOnlyTheStrings: decoding hands the record's own key, body
// and header values straight through — the specification makes received header values
// read-only and the specification makes the storage borrowed until settlement — so the
// only allocations are the strings the typed properties need.
func TestAllocsDecodeIsOnlyTheStrings(t *testing.T) {
	rec := &kgo.Record{
		Topic: "orders",
		Key:   []byte("k"),
		Value: []byte("body"),
		Headers: []kgo.RecordHeader{
			{Key: "content-kind", Value: []byte("order")},
			{Key: "tenant", Value: []byte("acme")},
		},
	}
	var m mq.Message
	kq.Decode(&m, rec, "") // grow the header storage once
	if a := testing.AllocsPerRun(200, func() { kq.Decode(&m, rec, "") }); a != 0 {
		t.Fatalf("Decode of a record with no typed properties: %.1f allocs/op", a)
	}
	// And the values are the record's own storage, not copies.
	v, _ := m.Headers.Get("content-kind")
	if len(v) > 0 && &v[0] != &rec.Headers[0].Value[0] {
		t.Fatal("Decode copied a header value; the specification surfaces a received value without a copy")
	}
	if len(m.Body) > 0 && &m.Body[0] != &rec.Value[0] {
		t.Fatal("Decode copied the body")
	}
}
