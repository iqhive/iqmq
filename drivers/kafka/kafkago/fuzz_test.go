package kafkago_test

import (
	"bytes"
	"errors"
	"testing"
	"time"

	segkafka "github.com/segmentio/kafka-go"

	mq "github.com/iqhive/iqmq"
	kq "github.com/iqhive/iqmq/drivers/kafka/kafkago"
)

// The differential fuzz targets the specification asks of a Kafka driver: header packing,
// record mapping, the offset tracker's contract as this driver drives it, and
// the batch outcome mapping.
//
// Two of them are differential against the franz-go driver and live in
// crossdriver_test.go (the topic grammar and the partition assignment). The
// four here are differential against the PROPERTY rather than against another
// implementation: a round trip that loses a field, an alignment that moves an
// outcome, a watermark that passes an incomplete offset.

// FuzzRecordMapping is the strict mode as a property: everything the specification names
// survives Encode and Decode byte for byte, whatever it is.
//
// The interesting inputs are the ones a person would not write: a nil key
// beside an empty one (Kafka's key is a NULLABLE byte string and the
// difference is observable), a header value that is empty, a property that
// contains the reserved prefix as a VALUE rather than a key, a creation time
// at the epoch.
func FuzzRecordMapping(f *testing.F) {
	f.Add("", "", []byte(nil), []byte(nil), "", "", int64(0))
	f.Add("id", "ct", []byte("k"), []byte("b"), "corr", "reply", int64(1))
	f.Add("", "", []byte{}, []byte{}, "", "", int64(-1))
	f.Add("iqmq-id", mq.ReservedHeaderPrefix, []byte("\x00\xff"), []byte("\xfe"), "\x00", "\n", int64(1<<40))
	f.Fuzz(func(t *testing.T, id, ctype string, key, body []byte, corr, reply string, createdMilli int64) {
		src := mq.Message{
			ID:   id,
			Key:  key,
			Body: body,
			Properties: mq.Properties{
				ContentType:   ctype,
				CorrelationID: corr,
				ReplyTo:       reply,
			},
		}
		if createdMilli > 0 {
			src.Properties.CreatedAt = time.UnixMilli(createdMilli).UTC()
		}
		var msg segkafka.Message
		_ = kq.Encode(&msg, &src, "t", nil)
		// Kafka stamps every record, so the decoder must see a timestamp on
		// the way back whether or not the publisher set one. That is exactly
		// the case the iqmq-created marker exists for.
		if msg.Time.IsZero() {
			msg.Time = time.Now()
		}
		var got mq.Message
		kq.Decode(&got, &msg, "")
		if got.ID != src.ID {
			t.Fatalf("ID %q, want %q", got.ID, src.ID)
		}
		if !bytes.Equal(got.Key, src.Key) || (got.Key == nil) != (src.Key == nil) {
			t.Fatalf("Key %q (nil %v), want %q (nil %v)", got.Key, got.Key == nil, src.Key, src.Key == nil)
		}
		if !bytes.Equal(got.Body, src.Body) || (got.Body == nil) != (src.Body == nil) {
			t.Fatalf("Body %q (nil %v), want %q (nil %v)", got.Body, got.Body == nil, src.Body, src.Body == nil)
		}
		if got.Properties.ContentType != src.Properties.ContentType ||
			got.Properties.CorrelationID != src.Properties.CorrelationID ||
			got.Properties.ReplyTo != src.Properties.ReplyTo {
			t.Fatalf("properties %+v, want %+v", got.Properties, src.Properties)
		}
		if !got.Properties.CreatedAt.Equal(src.Properties.CreatedAt) {
			t.Fatalf("CreatedAt %v, want %v", got.Properties.CreatedAt, src.Properties.CreatedAt)
		}
	})
}

// FuzzHeaderPacking is the other half of the mapping, and it is where a
// scratch-buffer bug lives: Encode writes every reserved header's value into
// ONE growing buffer and the record's headers point into it, so a value
// appended after the buffer has grown into a new array must still be the value
// that was written.
func FuzzHeaderPacking(f *testing.F) {
	f.Add([]byte("a\x00b\x001\x002"), "id", "ct")
	f.Add([]byte(""), "", "")
	f.Add([]byte("\x00\x00"), "x", "y")
	f.Fuzz(func(t *testing.T, packed []byte, id, ctype string) {
		// The input is a NUL-separated list of alternating keys and values,
		// which is how a fuzzer can produce a header set at all.
		parts := bytes.Split(packed, []byte{0})
		src := mq.Message{Body: []byte("b"), ID: id}
		src.Properties.ContentType = ctype
		type pair struct {
			k string
			v []byte
		}
		var want []pair
		for i := 0; i+1 < len(parts); i += 2 {
			k := string(parts[i])
			if k == "" || len(k) > 128 {
				continue
			}
			if bytes.HasPrefix(parts[i], []byte(mq.ReservedHeaderPrefix)) {
				// The specification refuses a caller's header under the reserved prefix,
				// so a message carrying one is never encoded.
				continue
			}
			src.Headers.Add(k, parts[i+1])
			want = append(want, pair{k, parts[i+1]})
		}
		var msg segkafka.Message
		_ = kq.Encode(&msg, &src, "t", make([]byte, 0, 1))
		var got mq.Message
		kq.Decode(&got, &msg, "")
		var seen []pair
		for k, v := range got.Headers.All() {
			seen = append(seen, pair{k, v})
		}
		if len(seen) != len(want) {
			t.Fatalf("%d headers came back, want %d (%v vs %v)", len(seen), len(want), seen, want)
		}
		for i := range want {
			if seen[i].k != want[i].k || !bytes.Equal(seen[i].v, want[i].v) {
				t.Fatalf("header %d is %q=%q, want %q=%q", i, seen[i].k, seen[i].v, want[i].k, want[i].v)
			}
		}
		// And the reserved values survived the shared scratch buffer.
		if got.ID != id {
			t.Fatalf("ID %q, want %q: the reserved header values share one growing buffer", got.ID, id)
		}
		if got.Properties.ContentType != ctype {
			t.Fatalf("ContentType %q, want %q", got.Properties.ContentType, ctype)
		}
	})
}

// FuzzBatchOutcome is the alignment the specification requires and the one that loses a
// message silently when it is wrong.
//
// The outcomes are scrambled on purpose. A driver that collected results in
// COMPLETION order rather than by index would report message seven's failure
// against message two, and a caller reading an index-aligned result would
// retry the wrong message and believe the right one succeeded — which on this
// client is a duplicate the broker does not de-duplicate.
func FuzzBatchOutcome(f *testing.F) {
	f.Add([]byte{0})
	f.Add([]byte{0, 1, 0})
	f.Add([]byte{19, 20, 0, 7})
	f.Add([]byte{19, 20}) // every message failed: one refused, one appended
	f.Add(bytes.Repeat([]byte{0, 20}, 8))
	f.Fuzz(func(t *testing.T, codes []byte) {
		if len(codes) == 0 || len(codes) > 256 {
			return
		}
		native := make([]error, len(codes))
		failed := 0
		for i, c := range codes {
			if c == 0 {
				continue
			}
			native[i] = kafkaGoError(int16(c))
			failed++
		}
		out := kq.BatchOutcome(native)
		if len(out) != len(native) {
			t.Fatalf("BatchOutcome returned %d outcomes for %d messages", len(out), len(native))
		}
		for i := range native {
			if (native[i] == nil) != (out[i] == nil) {
				t.Fatalf("outcome %d: native %v, mapped %v; an accepted message must stay accepted at its own index", i, native[i], out[i])
			}
			if out[i] == nil {
				continue
			}
			// The native error is reachable at the SAME index, which is what
			// alignment means.
			if !errors.Is(out[i], native[i]) {
				t.Fatalf("outcome %d does not wrap its own native error: %v vs %v", i, out[i], native[i])
			}
		}
		summary := kq.BatchSummary(out)
		switch {
		case failed == 0:
			if summary != nil {
				t.Fatalf("BatchSummary of an accepted batch = %v", summary)
			}
		case failed == len(out):
			// One error only when it is every message's outcome: a batch
			// in which some were refused and some may be in the log is
			// answered per message, and is ambiguous.
			amb, mixed := mq.IsAmbiguous(out[0]), false
			for _, e := range out[1:] {
				mixed = mixed || mq.IsAmbiguous(e) != amb
			}
			var be *mq.BatchError
			if errors.As(summary, &be) != mixed {
				t.Fatalf("BatchSummary of a wholly failed batch = %v: a *BatchError is right exactly when the outcomes differ (mixed=%v)", summary, mixed)
			}
			if mq.IsAmbiguous(summary) != (amb || mixed) {
				t.Fatalf("BatchSummary of a wholly failed batch = %v: IsAmbiguous = %v", summary, mq.IsAmbiguous(summary))
			}
		default:
			var be *mq.BatchError
			if !errors.As(summary, &be) {
				t.Fatalf("BatchSummary of a partial failure = %v; a caller has no way to learn WHICH messages to retry", summary)
			}
			if len(be.Errs) != len(out) {
				t.Fatalf("the *BatchError carries %d outcomes for %d messages", len(be.Errs), len(out))
			}
			for i := range out {
				if (be.Errs[i] == nil) != (out[i] == nil) {
					t.Fatalf("the *BatchError moved outcome %d", i)
				}
			}
		}
	})
}

// FuzzDSNRoundTrip is the parser's own property: anything [Config.DSN] renders
// parses back into a configuration that renders the same way.
//
// It is not a byte-for-byte round trip of the caller's string — the password
// is deliberately gone from the rendering, which is the whole reason the
// rendering exists — so the fixed point is the RENDERING, which is what ends
// up in a log and in an error.
func FuzzDSNRoundTrip(f *testing.F) {
	f.Add("kafkago://host:9092")
	f.Add("kafkago://u:p@h1:9092,h2:9093/acme.?balancer=roundrobin&commitinterval=1s")
	f.Add("kafkago://host:9092?tracker=bitset&nack=stop&partitions=4")
	f.Fuzz(func(t *testing.T, s string) {
		cfg, err := kq.ParseDSN(s)
		if err != nil {
			return
		}
		rendered := cfg.DSN()
		again, err := kq.ParseDSN(rendered)
		if err != nil {
			t.Fatalf("a configuration's own rendering does not parse:\n  input:    %q\n  rendered: %q\n  error:    %v", s, rendered, err)
		}
		if got := again.DSN(); got != rendered {
			t.Fatalf("the rendering is not a fixed point:\n  first:  %q\n  second: %q", rendered, got)
		}
	})
}
