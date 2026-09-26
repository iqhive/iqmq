package kafka_test

import (
	"bytes"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"

	mq "github.com/iqhive/iqmq"
	kq "github.com/iqhive/iqmq/drivers/kafka"
	"github.com/iqhive/iqmq/internal/fuzzutil"
)

// The mapping is the one part of a driver that is pure and total, and it is
// the part a caller's data goes through twice. What the fuzzer is looking for
// is the mapping losing something the API distinguishes: a nil key read back
// as empty, a header order rearranged, a reserved header a caller's own
// header collided with, a property that came back on a different field.
func FuzzKafkaRecordMapping(f *testing.F) {
	seedRecordMapping(f)
	f.Fuzz(func(t *testing.T, data []byte) {
		r := fuzzutil.NewReader(data)
		m := messageFrom(r)
		if err := kq.Representable(&m); err != nil {
			// A message strict mode refuses is refused before any I/O,
			// which is the contract; it is not a mapping failure.
			if mq.CodeOf(err) != mq.CodeInvalidArgument {
				t.Fatalf("Representable refused %+v with %v, want an invalid-argument refusal", m, err)
			}
			return
		}
		var rec kgo.Record
		kq.Encode(&rec, &m, "orders", nil)
		if rec.Topic != "orders" {
			t.Fatalf("Encode put the record on %q", rec.Topic)
		}
		var got mq.Message
		kq.Decode(&got, &rec, "")

		if !sameBytes(m.Key, got.Key) {
			t.Fatalf("key %v (nil=%v) came back as %v (nil=%v)", m.Key, m.Key == nil, got.Key, got.Key == nil)
		}
		if !sameBytes(m.Body, got.Body) {
			t.Fatalf("body %v (nil=%v) came back as %v (nil=%v)", m.Body, m.Body == nil, got.Body, got.Body == nil)
		}
		if m.ID != got.ID {
			t.Fatalf("id %q came back as %q", m.ID, got.ID)
		}
		if m.Properties != got.Properties {
			t.Fatalf("properties %+v came back as %+v", m.Properties, got.Properties)
		}
		assertSameHeaders(t, &m, &got)
		// Encoding twice into the same record produces the same record:
		// the header slice is reused, so a mapping that appended rather
		// than truncated would grow it.
		var again kgo.Record
		again.Headers = make([]kgo.RecordHeader, 7)
		kq.Encode(&again, &m, "orders", nil)
		if len(again.Headers) != len(rec.Headers) {
			t.Fatalf("encoding into a reused record produced %d headers, want %d", len(again.Headers), len(rec.Headers))
		}
	})
}

// messageFrom decodes a message from fuzz input.
func messageFrom(r *fuzzutil.Reader) mq.Message {
	var m mq.Message
	if r.Bool() {
		m.Key = maybeBytes(r)
	}
	m.Body = maybeBytes(r)
	m.ID = r.String(24)
	m.Properties = mq.Properties{
		ContentType:     r.String(16),
		ContentEncoding: r.String(8),
		CorrelationID:   r.String(16),
		CausationID:     r.String(16),
		ReplyTo:         r.String(16),
	}
	if r.Bool() {
		// Kafka's timestamp is milliseconds since the epoch, so a
		// creation time only round-trips at that resolution — which is
		// what the driver page says and what the mapping has to hold to.
		m.Properties.CreatedAt = time.UnixMilli(int64(r.Int(0, 1<<40))).UTC()
	}
	for range r.Int(0, 8) {
		m.Headers.Add(r.String(12), maybeBytes(r))
	}
	return m
}

// maybeBytes reads a byte slice that may be nil, empty or populated, because
// The specification makes all three different.
func maybeBytes(r *fuzzutil.Reader) []byte {
	switch r.Int(0, 2) {
	case 0:
		return nil
	case 1:
		return []byte{}
	default:
		return bytes.Clone(r.Bytes(64))
	}
}

// assertSameHeaders compares header sets pair by pair, in order.
func assertSameHeaders(t *testing.T, want, got *mq.Message) {
	t.Helper()
	if want.Headers.Len() != got.Headers.Len() {
		t.Fatalf("%d headers came back as %d", want.Headers.Len(), got.Headers.Len())
	}
	type kv struct {
		k string
		v []byte
	}
	var a, b []kv
	for k, v := range want.Headers.All() {
		a = append(a, kv{k, v})
	}
	for k, v := range got.Headers.All() {
		b = append(b, kv{k, v})
	}
	for i := range a {
		if a[i].k != b[i].k {
			t.Fatalf("header %d is %q, want %q: the specification keeps the publication order", i, b[i].k, a[i].k)
		}
		if !sameBytes(a[i].v, b[i].v) {
			t.Fatalf("header %q's value %v came back as %v", a[i].k, a[i].v, b[i].v)
		}
	}
}

// seedRecordMapping commits the corners: the nil and empty distinctions, a
// reserved-prefix collision, a binary header, a duplicate key and every typed
// property at once.
func seedRecordMapping(f *testing.F) {
	f.Helper()
	add := func(build func(*fuzzutil.Writer)) {
		w := fuzzutil.NewWriter()
		build(w)
		f.Add(w.Data())
	}
	str := func(w *fuzzutil.Writer, max int, s string) { w.String(max, s) }
	writeMaybe := func(w *fuzzutil.Writer, kind int, b []byte) {
		w.Int(0, 2, kind)
		if kind == 2 {
			w.Bytes(64, b)
		}
	}
	// A message with nothing set at all.
	add(func(w *fuzzutil.Writer) {
		w.Bool(false)         // no key
		writeMaybe(w, 0, nil) // nil body
		str(w, 24, "")        // no id
		for range 5 {
			str(w, 16, "")
		}
		w.Bool(false)  // no created time
		w.Int(0, 8, 0) // no headers
	})
	// Every field populated, with binary values and a duplicate header.
	add(func(w *fuzzutil.Writer) {
		w.Bool(true)
		writeMaybe(w, 2, []byte{0, 1, 0xff})
		writeMaybe(w, 2, []byte{0xfe, 0, 0x80})
		str(w, 24, "01JABCDEF")
		str(w, 16, "application/json")
		str(w, 8, "gzip")
		str(w, 16, "corr-1")
		str(w, 16, "cause-0")
		str(w, 16, "replies")
		w.Bool(true)
		w.Int(0, 1<<40, 1_700_000_000_000)
		w.Int(0, 8, 3)
		str(w, 12, "dup")
		writeMaybe(w, 2, []byte("a"))
		str(w, 12, "dup")
		writeMaybe(w, 2, []byte("b"))
		str(w, 12, "bin")
		writeMaybe(w, 2, []byte{0x80, 0})
	})
	// The empty non-nil key and body, which are NOT the nil ones.
	add(func(w *fuzzutil.Writer) {
		w.Bool(true)
		writeMaybe(w, 1, nil)
		writeMaybe(w, 1, nil)
		str(w, 24, "")
		for range 5 {
			str(w, 16, "")
		}
		w.Bool(false)
		w.Int(0, 8, 0)
	})
	// A caller's own header under the reserved prefix, which strict mode
	// refuses rather than renaming.
	add(func(w *fuzzutil.Writer) {
		w.Bool(false)
		writeMaybe(w, 2, []byte("b"))
		str(w, 24, "")
		for range 5 {
			str(w, 16, "")
		}
		w.Bool(false)
		w.Int(0, 8, 1)
		str(w, 12, mq.ReservedHeaderPrefix+"id")
		writeMaybe(w, 2, []byte("mine"))
	})
	// A header with an empty key, also refused.
	add(func(w *fuzzutil.Writer) {
		w.Bool(false)
		writeMaybe(w, 2, []byte("b"))
		str(w, 24, "")
		for range 5 {
			str(w, 16, "")
		}
		w.Bool(false)
		w.Int(0, 8, 1)
		str(w, 12, "")
		writeMaybe(w, 2, []byte("v"))
	})
}

// This is the defect the target exists for. franz-go's promises fire in
// whatever order the partitions resolve, so a driver that collected results in
// completion order would report message 7's failure against message 2 — and a
// caller reading an index-aligned result would retry the wrong message and
// believe the right one succeeded. Neither half of that is visible without a
// test that scrambles the order on purpose.
func FuzzKafkaBatchOutcome(f *testing.F) {
	seedBatchOutcome(f)
	f.Fuzz(func(t *testing.T, data []byte) {
		r := fuzzutil.NewReader(data)
		n := r.Int(1, 16)
		// Which messages the broker refuses, and with what.
		outcomes := make([]*kerr.Error, n)
		failures := 0
		for i := range outcomes {
			switch r.Int(0, 3) {
			case 1:
				outcomes[i] = kerr.TopicAuthorizationFailed
				failures++
			case 2:
				outcomes[i] = kerr.MessageTooLarge
				failures++
			case 3:
				outcomes[i] = kerr.InvalidRecord
				failures++
			}
		}
		native := make([]error, n)
		for i, ke := range outcomes {
			if ke != nil {
				native[i] = ke
			}
		}
		errs := kq.BatchOutcome(native)
		if len(errs) != n {
			t.Fatalf("%d outcomes for %d messages", len(errs), n)
		}
		got := 0
		for i := range errs {
			switch {
			case outcomes[i] == nil && errs[i] != nil:
				t.Fatalf("message %d succeeded and its outcome is %v", i, errs[i])
			case outcomes[i] != nil && errs[i] == nil:
				t.Fatalf("message %d was refused with %v and its outcome is nil", i, outcomes[i])
			case outcomes[i] == nil:
				continue
			}
			got++
			// The outcome at index i has to be the error for index i,
			// which is the whole contract.
			var kerror *kerr.Error
			if !errors.As(errs[i], &kerror) || kerror.Code != outcomes[i].Code {
				t.Fatalf("message %d was refused with %v and its outcome carries %v", i, outcomes[i], errs[i])
			}
		}
		if got != failures {
			t.Fatalf("%d outcomes carry an error for %d refusals", got, failures)
		}
		// And the summary the SPI returns: nil for all-success, the single
		// error for all-failure with one outcome -- every refusal here is
		// definite -- and a *BatchError otherwise.
		summary := kq.BatchSummary(errs)
		switch failures {
		case 0:
			if summary != nil {
				t.Fatalf("a batch with no failures summarised as %v", summary)
			}
		case n:
			var be *mq.BatchError
			if errors.As(summary, &be) {
				t.Fatalf("a batch in which every message was definitely refused summarised as a *BatchError, want the single error")
			}
			if summary == nil {
				t.Fatal("a batch in which every message failed summarised as nil")
			}
		default:
			var be *mq.BatchError
			if !errors.As(summary, &be) {
				t.Fatalf("a partially failed batch summarised as %v, want a *BatchError", summary)
			}
			if len(be.Errs) != n {
				t.Fatalf("the *BatchError carries %d outcomes for %d messages", len(be.Errs), n)
			}
			for i := range be.Errs {
				if (be.Errs[i] == nil) != (outcomes[i] == nil) {
					t.Fatalf("the *BatchError's outcome %d does not match the broker's answer for that message", i)
				}
			}
		}
	})
}

// seedBatchOutcome commits the shapes that matter: all success, all failure,
// the first one failing, the last one failing, and an interleaving.
func seedBatchOutcome(f *testing.F) {
	f.Helper()
	add := func(n int, kinds ...int) {
		w := fuzzutil.NewWriter()
		w.Int(1, 16, n)
		for _, k := range kinds {
			w.Int(0, 3, k)
		}
		f.Add(w.Data())
	}
	add(1, 0)
	add(1, 1)
	add(3, 0, 0, 0)
	add(3, 1, 2, 3)
	add(4, 1, 0, 0, 0)
	add(4, 0, 0, 0, 2)
	add(6, 0, 1, 0, 2, 0, 3)
	add(16, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1)
}

// FuzzKafkaTopicMapping is the topic grammar in both directions: a name the
// driver accepts is a name Kafka accepts, and a name it maps round-trips.
func FuzzKafkaTopicMapping(f *testing.F) {
	for _, seed := range []string{
		"orders", "orders.eu", "a_b-c.d", "", ".", "..", "orders/eu",
		"orders eu", "ORDERS", "0", strings.Repeat("x", 249), strings.Repeat("x", 250),
		`^orders\..*$`, "orders.*", "orders[0]", "orders\x00",
	} {
		f.Add(seed, "")
		f.Add(seed, "acme.")
	}
	f.Fuzz(func(t *testing.T, topic, prefix string) {
		name, err := kq.Topic(prefix, topic)
		if err != nil {
			if code := mq.CodeOf(err); code != mq.CodeInvalidArgument && code != mq.CodeUnsupported {
				t.Fatalf("Topic(%q, %q) refused with %v (code %v)", prefix, topic, err, code)
			}
			return
		}
		if err := kq.CheckTopic(name); err != nil {
			t.Fatalf("Topic(%q, %q) produced %q, which CheckTopic then refuses: %v", prefix, topic, name, err)
		}
		if len(name) == 0 || len(name) > kq.MaxTopicLen {
			t.Fatalf("Topic(%q, %q) produced a %d-byte name", prefix, topic, len(name))
		}
		if !strings.HasSuffix(name, topic) {
			t.Fatalf("Topic(%q, %q) produced %q, which does not end in the topic", prefix, topic, name)
		}
		if back := kq.TopicOf(prefix, name); back != topic {
			t.Fatalf("TopicOf(%q, %q) = %q, want %q", prefix, name, back, topic)
		}
	})
}
