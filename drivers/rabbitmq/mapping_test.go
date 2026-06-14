package rabbitmq_test

import (
	"bytes"
	"strings"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	rq "bitbucket.org/iqhive/iqmq/drivers/rabbitmq"
	"bitbucket.org/iqhive/iqmq/internal/rabbit/protocol"
)

// TestDeliveryCountIsReadWhateverWidthItArrivesIn is the case a type assertion
// gets wrong, and the consequence is MaxAttempts never firing.
//
// RabbitMQ sends `x-delivery-count` as a signed 64-bit integer, and the AMQP
// field table can carry it in five widths. A driver that asserted to the one
// width it happened to see in a test would read zero from a broker that chose
// another — so every delivery would report Attempt 1, the core's own count
// would never rise, and a poison message would be redelivered for ever with
// nothing in the deployment able to notice.
func TestDeliveryCountIsReadWhateverWidthItArrivesIn(t *testing.T) {
	for _, tc := range []struct {
		name string
		v    any
		want uint32
	}{
		{"int64", int64(3), 4},
		{"int32", int32(3), 4},
		{"int16", int16(3), 4},
		{"int8", int8(3), 4},
		{"int", 3, 4},
		{"float64", float64(3), 4},
		{"a decimal string", "3", 4},
		{"zero is the first delivery", int64(0), 1},
		// A quorum queue COUNTS, so a header whose value no integer can be
		// read from leaves the attempt at the first-delivery default of one
		// rather than dropping to zero: zero is the specification's "the broker does not
		// count attempts", which is false of a quorum queue.
		{"a value no integer can be read from", []string{"x"}, 1},
		{"no header at all is the first delivery", nil, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			headers := amqp.Table{"x-delivery-count": tc.v}
			if tc.v == nil {
				// RabbitMQ omits the header entirely on a FIRST delivery
				// and sends it from the second onwards, which is the case a
				// driver that keyed the attempt on the header's presence got
				// wrong: it reported zero for every first delivery.
				headers = nil
			}
			info := decodeInfo(t, &amqp.Delivery{
				Exchange:    "orders",
				RoutingKey:  "orders",
				DeliveryTag: 7,
				Headers:     headers,
				Body:        []byte("b"),
			}, true)
			if info.Attempt != tc.want {
				t.Fatalf("Attempt = %d, want %d", info.Attempt, tc.want)
			}
		})
	}
}

// TestClassicQueueReportsNoAttempt is the specification's `(verify)` in the mapping rather
// than in the table: a classic queue sends no `x-delivery-count`, and the specification
// makes an attempt of zero "the broker does not count attempts" rather than
// "this is the first".
func TestClassicQueueReportsNoAttempt(t *testing.T) {
	d := &amqp.Delivery{Exchange: "orders", DeliveryTag: 1, Headers: amqp.Table{"x-delivery-count": int64(5)}, Body: []byte("b")}
	if info := decodeInfo(t, d, false); info.Attempt != 0 {
		t.Fatalf("a classic queue reported Attempt %d: the header is a quorum-queue field and a driver that read it off a classic queue would be reading somebody else's number", info.Attempt)
	}
	if info := decodeInfo(t, d, true); info.Attempt != 6 {
		t.Fatalf("a quorum queue reported Attempt %d, want 6", info.Attempt)
	}
}

// TestDriverHeadersDoNotReachTheApplication is the fidelity requirement
// applied to the driver's own bookkeeping.
//
// This driver puts two entries of its own in the same table as the
// application's headers: the message key, when it does not travel as the
// routing key, and the publisher-confirm correlation token. A round trip that
// handed either back would make a message arrive with a header the publisher
// never set — and worse, an application that echoed its headers would publish
// a confirm token of somebody else's.
func TestDriverHeadersDoNotReachTheApplication(t *testing.T) {
	msg, _ := decodeMessage(t, &amqp.Delivery{
		Exchange:    "orders",
		RoutingKey:  "orders",
		DeliveryTag: 1,
		Headers: amqp.Table{
			"trace":                  "abc",
			rq.KeyHeader:             []byte("k"),
			rq.ConfirmHeader:         "p41",
			"x-iqmq-future":          int64(1),
			"x-delivery-count":       int64(0),
			protocol.CausationHeader: "cause-1",
		},
		Body: []byte("b"),
	}, true)
	for key := range msg.Headers.All() {
		if strings.HasPrefix(key, "x-iqmq-") {
			t.Errorf("the delivery's reserved entry %q reached the application as a header", key)
		}
		if key == "x-delivery-count" {
			t.Error("the broker's own delivery counter reached the application as a header")
		}
	}
	if got, ok := msg.Headers.Get("trace"); !ok || string(got) != "abc" {
		t.Errorf("the application's own header did not survive: %q (present=%v)", got, ok)
	}
	// The two the driver DOES read are read, so the filter is not simply
	// dropping everything.
	if string(msg.Key) != "k" {
		t.Errorf("the key did not come back: %q", msg.Key)
	}
	if msg.Properties.CausationID != "cause-1" {
		t.Errorf("the causation identifier did not come back: %q", msg.Properties.CausationID)
	}
}

// TestForeignHeaderShapesAreDropped is the honest answer to a value the specification's
// byte-string headers have no form for.
//
// A nested field table, a list and a decimal can all arrive from a publisher
// that is not this driver. There is nothing to hand a caller — the headers
// are bytes — and inventing a rendering would make a round trip through iqmq
// change the message, so they are dropped. Everything that CAN be rendered as
// bytes is.
func TestForeignHeaderShapesAreDropped(t *testing.T) {
	msg, _ := decodeMessage(t, &amqp.Delivery{
		Exchange: "orders", DeliveryTag: 1,
		Headers: amqp.Table{
			"nested":  amqp.Table{"a": "b"},
			"list":    []any{1, 2},
			"decimal": amqp.Decimal{Scale: 2, Value: 100},
			"text":    "t",
			"binary":  []byte{0x00, 0xff},
			"number":  int64(42),
			"flag":    true,
			"stamp":   time.Unix(0, 0).UTC(),
			"void":    nil,
		},
		Body: []byte("b"),
	}, true)
	for _, key := range []string{"nested", "list", "decimal"} {
		if _, ok := msg.Headers.Get(key); ok {
			t.Errorf("the header %q was given a rendering: a round trip through iqmq would change the message", key)
		}
	}
	for key, want := range map[string]string{
		"text": "t", "number": "42", "flag": "true", "void": "",
	} {
		got, ok := msg.Headers.Get(key)
		if !ok || string(got) != want {
			t.Errorf("header %q = %q (present=%v), want %q", key, got, ok, want)
		}
	}
	if got, ok := msg.Headers.Get("binary"); !ok || !bytes.Equal(got, []byte{0x00, 0xff}) {
		t.Errorf("the binary header did not survive byte for byte: %x", got)
	}
	if got, ok := msg.Headers.Get("stamp"); !ok || !strings.HasPrefix(string(got), "1970-01-01T00:00:00") {
		t.Errorf("the timestamp header rendered as %q", got)
	}
}

// TestSequenceIsTheDeliveryTagAndNotAPosition pins the one piece of
// DeliveryInfo a caller could read as something it is not.
//
// A delivery tag is per CHANNEL and monotonic within it; it is not a
// broker-wide position and it restarts at 1 on every new channel. The specification's
// Sequence is the nearest field and the driver page says what it means here.
func TestSequenceIsTheDeliveryTagAndNotAPosition(t *testing.T) {
	info := decodeInfo(t, &amqp.Delivery{Exchange: "orders", DeliveryTag: 42, Body: []byte("b")}, true)
	if !info.HasSequence || info.Sequence != 42 {
		t.Fatalf("Sequence = (%d, %v), want (42, true)", info.Sequence, info.HasSequence)
	}
	if info.HasOffset || info.HasPartition {
		t.Errorf("a queue has no partition and no offset, and the driver reported partition=%v offset=%v", info.HasPartition, info.HasOffset)
	}
	// A tag of zero is not a sequence: the protocol's first tag is 1, so
	// zero means the field was never set.
	none := decodeInfo(t, &amqp.Delivery{Exchange: "orders", Body: []byte("b")}, true)
	if none.HasSequence {
		t.Error("a delivery tag of zero was reported as a sequence; the protocol's first tag is 1")
	}
}
