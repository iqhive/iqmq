package amqp10_test

import (
	"testing"
	"time"

	amqp "github.com/Azure/go-amqp"

	mq "github.com/iqhive/iqmq"
	aq "github.com/iqhive/iqmq/drivers/amqp10"
)

// The mapping's allocation counts (rule 1, the specification's "AllocsPerRun for
// mapping").
//
// The numbers are EXACT rather than ceilings wherever they can be, because a
// ceiling is a number nobody notices growing: the specification rule 1 asks for "a
// testing.AllocsPerRun test asserting the exact count", and the point of the
// test is that a change to the mapping is a diff in this file.

// TestAllocsEncodeReusesItsBuffer is what [aq.Buf] exists for.
//
// An AMQP message's properties section takes *string and *time.Time, so a naive
// encode allocates once per populated property; the buffer holds the strings and
// the times the pointers point at, and is reset rather than rebuilt. What is
// left after that is only what Go cannot avoid — a []byte becoming a string, and
// a string being boxed into an `any` — which is what the exact count below
// names one by one.
func TestAllocsEncodeReusesItsBuffer(t *testing.T) {
	m := mapperFor(t, mq.MetadataNativeStrict)
	buf := aq.NewBuf()
	msg := &mq.Message{
		Key:  []byte("order-42"),
		Body: []byte("payload"),
		ID:   "msg-1",
		Properties: mq.Properties{
			ContentType:     "application/json",
			ContentEncoding: "gzip",
			CorrelationID:   "corr-1",
			ReplyTo:         "replies",
			CreatedAt:       time.Unix(1_700_000_000, 0),
		},
	}
	msg.Headers.Add("one", []byte("1"))
	msg.Headers.Add("two", []byte("2"))
	opts := &mq.PublishOptions{TTL: time.Minute, Priority: 3, PrioritySet: true}
	// Warm the buffer's maps, which grow once.
	for range 4 {
		if _, err := m.Encode(buf, msg, opts); err != nil {
			t.Fatalf("Encode: %v", err)
		}
	}
	got := testingAllocsPerRun(200, func() {
		if _, err := m.Encode(buf, msg, opts); err != nil {
			t.Fatalf("Encode: %v", err)
		}
	})
	// Five, and each one is named because the point of an exact count is that
	// a change to the mapping is a diff in this file:
	//
	//   - the message key becoming the group-id property, which is a
	//     []byte-to-string copy;
	//   - two allocations per header, twice over. An AMQP application
	//     property is an `any`, and putting a string into one boxes it (a
	//     string does not fit in an interface word), while turning the
	//     header's []byte into that string copies it.
	//
	// Neither can be removed without unsafe, which rule 8 confines to
	// two packages that are not this one. What this number WOULD catch is a
	// property, a map, a slice or the message itself being rebuilt per
	// publication, which is the defect rule 1 is about — every one of
	// those is what [aq.Buf] exists to hold.
	if got != 5 {
		t.Fatalf("encoding a two-header keyed message into a reused buffer allocates %.1f times, want exactly 5: "+
			"the group-id string, and a boxed string plus its copy for each of the two headers. A larger number "+
			"means a property, a map or a slice is being rebuilt per message, which rule 1 forbids", got)
	}
}

// TestAllocsEncodeABareMessageIsFree is the floor, and it is what makes a
// high-throughput publisher of bare payloads free at this layer: a message with
// no key, no identifier and no headers encodes into a reused buffer with no
// allocation at all.
func TestAllocsEncodeABareMessageIsFree(t *testing.T) {
	m := mapperFor(t, mq.MetadataNativeStrict)
	buf := aq.NewBuf()
	msg := &mq.Message{Body: []byte("payload")}
	opts := &mq.PublishOptions{TTL: time.Minute, Priority: 3, PrioritySet: true}
	for range 4 {
		if _, err := m.Encode(buf, msg, opts); err != nil {
			t.Fatalf("Encode: %v", err)
		}
	}
	got := testingAllocsPerRun(200, func() {
		if _, err := m.Encode(buf, msg, opts); err != nil {
			t.Fatalf("Encode: %v", err)
		}
	})
	if got != 0 {
		t.Fatalf("encoding a bare payload into a reused buffer allocates %.1f times, want none", got)
	}
}

// TestAllocsEncodeWithNoKeyCostsOnlyTheHeader isolates the per-header price
// from the per-key one, so that a change to either is a separate diff.
func TestAllocsEncodeWithNoKeyCostsOnlyTheHeader(t *testing.T) {
	m := mapperFor(t, mq.MetadataNativeStrict)
	buf := aq.NewBuf()
	msg := &mq.Message{Body: []byte("payload"), ID: "msg-1"}
	msg.Headers.Add("one", []byte("1"))
	for range 4 {
		if _, err := m.Encode(buf, msg, nil); err != nil {
			t.Fatalf("Encode: %v", err)
		}
	}
	got := testingAllocsPerRun(200, func() {
		if _, err := m.Encode(buf, msg, nil); err != nil {
			t.Fatalf("Encode: %v", err)
		}
	})
	// Two, and both belong to the one header: the boxed `any` an AMQP
	// application property is, and the string copy the header's []byte
	// becomes. The key's own allocation is gone, which is what this case
	// isolates.
	if got != 2 {
		t.Fatalf("encoding a keyless one-header message into a reused buffer allocates %.1f times, want exactly 2: "+
			"the boxed application-property value and the string copy of the header's bytes", got)
	}
}

// TestAllocsCheckPublishIsFree holds the pre-I/O check to zero, because it
// runs on every publication whether or not the message is ever encoded — a
// refusal that allocated would make the rejected path more expensive than the
// accepted one.
func TestAllocsCheckPublishIsFree(t *testing.T) {
	m := mapperFor(t, mq.MetadataNativeStrict)
	msg := &mq.Message{Body: []byte("payload"), Key: []byte("k")}
	msg.Headers.Add("one", []byte("1"))
	msg.Headers.Add("two", []byte("2"))
	opts := &mq.PublishOptions{TTL: time.Minute}
	if err := m.CheckPublish(msg, opts); err != nil {
		t.Fatalf("CheckPublish: %v", err)
	}
	got := testingAllocsPerRun(200, func() {
		if err := m.CheckPublish(msg, opts); err != nil {
			t.Fatalf("CheckPublish: %v", err)
		}
	})
	// The duplicate-key check builds a set, which is one allocation for a
	// message that has any headers at all. It is the price of answering
	// "would this message survive a map?" without changing the message.
	if got > 2 {
		t.Fatalf("CheckPublish allocates %.1f times for a two-header message, want at most the duplicate-key set", got)
	}
	t.Logf("CheckPublish allocations: %.1f", got)
}

// TestAllocsDecodeReusesTheHeaderStorage is the receive path's own count.
//
// What it cannot avoid is one allocation per string-valued application
// property, because iqmq's header value is a []byte and converting a string to
// one copies — and the alternative is unsafe, which rule 8 confines to two
// packages that are not this one. What it MUST avoid is a per-delivery message
// or a per-delivery header slice, which reusing dst is what buys.
func TestAllocsDecodeReusesTheHeaderStorage(t *testing.T) {
	m := mapperFor(t, mq.MetadataNativeStrict)
	group := "order-42"
	ctype := "application/json"
	src := &amqp.Message{
		Header:     &amqp.MessageHeader{Durable: true, DeliveryCount: 1},
		Properties: &amqp.MessageProperties{MessageID: "msg-1", GroupID: &group, ContentType: &ctype},
		ApplicationProperties: map[string]any{
			"one": "1",
			"two": "2",
		},
		Data: [][]byte{[]byte("payload")},
	}
	var dst mq.Message
	for range 8 {
		if _, err := m.Decode(&dst, src); err != nil {
			t.Fatalf("Decode: %v", err)
		}
	}
	got := testingAllocsPerRun(200, func() {
		if _, err := m.Decode(&dst, src); err != nil {
			t.Fatalf("Decode: %v", err)
		}
	})
	// Three: the group-id string becoming a key, and the two string-valued
	// application properties becoming header values. The header SLICE itself
	// is reused, which is what Headers.Reset buys and what this number would
	// catch the loss of.
	if got != 3 {
		t.Fatalf("decoding into reused storage allocates %.1f times, want exactly 3 — the key and the two "+
			"string-valued application properties, each of which is a string-to-[]byte copy. A larger number means "+
			"the header slice or the message is being rebuilt per delivery", got)
	}
}

// TestAllocsDecodeAHeaderlessMessageIsFree is the floor: a delivery with no
// metadata at all costs nothing to decode, which is what makes a
// high-throughput consumer of bare payloads free at this layer.
func TestAllocsDecodeAHeaderlessMessageIsFree(t *testing.T) {
	m := mapperFor(t, mq.MetadataNativeStrict)
	src := &amqp.Message{Header: &amqp.MessageHeader{Durable: true}, Data: [][]byte{[]byte("payload")}}
	var dst mq.Message
	for range 8 {
		if _, err := m.Decode(&dst, src); err != nil {
			t.Fatalf("Decode: %v", err)
		}
	}
	got := testingAllocsPerRun(200, func() {
		if _, err := m.Decode(&dst, src); err != nil {
			t.Fatalf("Decode: %v", err)
		}
	})
	if got != 0 {
		t.Fatalf("decoding a bare payload into reused storage allocates %.1f times, want none", got)
	}
}

// TestAllocsPreallocatedHotPathErrors is the specification's "hot-path *OpError values
// preallocated per handle".
//
// A closed handle is the error a shutting-down process produces most of, and a
// driver that formatted one per call would allocate on the one path that is
// already going wrong.
func TestAllocsPreallocatedHotPathErrors(t *testing.T) {
	f := open(t, testProfile(), aq.Config{})
	snk := f.sink(mq.SinkConfig{Topic: "topic"})
	src := f.source(mq.SourceConfig{Prefetch: 2})
	if err := snk.Close(); err != nil {
		t.Fatalf("closing the sink: %v", err)
	}
	if err := src.Close(); err != nil {
		t.Fatalf("closing the source: %v", err)
	}
	msg := &mq.Message{Body: []byte("x")}
	dst := make([]mq.DriverDelivery, 1)
	got := testingAllocsPerRun(200, func() {
		_ = snk.Publish(sinkCtx(), msg, nil)
		_, _ = src.Fetch(sinkCtx(), dst)
	})
	if got != 0 {
		t.Fatalf("a publication and a fetch on closed handles allocate %.1f times; the specification asks for the hot-path "+
			"*OpError values to be preallocated per handle, so the failure path formats nothing", got)
	}
	// And the value really is the root package's sentinel by code, which is
	// what a caller matches on.
	assertIs(t, snk.Publish(sinkCtx(), msg, nil), mq.ErrClosed, "a publication on a closed sink")
}
