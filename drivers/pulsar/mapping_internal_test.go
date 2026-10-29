package pulsar

import (
	"bytes"
	"errors"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/apache/pulsar-client-go/pulsar"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit"
	"github.com/iqhive/iqmq/internal/fuzzutil"
	"github.com/iqhive/iqmq/wire"
)

// FuzzPulsarProperties holds the metadata decision to its contract, in all
// four modes.
//
// The property is the one that matters to a caller: a message this driver
// ACCEPTS comes back exactly as it was published, and a message it cannot carry
// is refused rather than silently changed. Pulsar's properties are a map of
// protobuf strings, so five kinds of metadata have nowhere native to go — and
// two of those five are the message key, which the inventory has no word
// for at all.
func FuzzPulsarProperties(f *testing.F) {
	seedProperties(f)
	f.Fuzz(func(t *testing.T, data []byte) {
		r := fuzzutil.NewReader(data)
		mode := mq.MetadataMode(r.Int(0, 3))
		m := fuzzMessage(r)
		caps, err := capabilities(Config{Metadata: mode}.resolve())
		if err != nil {
			t.Fatalf("building the capability table: %v", err)
		}
		lim := wire.DefaultLimits()
		env, err := driverkit.NewEnvelope(driverkit.EnvelopeConfig{Mode: mode, Limits: &lim, Capabilities: caps})
		if err != nil {
			t.Fatalf("NewEnvelope: %v", err)
		}
		forced, err := driverkit.NewEnvelope(driverkit.EnvelopeConfig{
			Mode: mq.MetadataPortableEnvelope, Limits: &lim, Capabilities: caps,
		})
		if err != nil {
			t.Fatalf("NewEnvelope(portable): %v", err)
		}
		out, err := wrapForPublish(env, forced, &lim, mode, m)
		if err != nil {
			// Every refusal has to be classified: the specification makes an
			// unclassified error the one a caller cannot act on.
			if mq.CodeOf(err) == mq.CodeUnknown {
				t.Fatalf("the publication was refused with an unclassified error: %v", err)
			}
			// And strict mode must only refuse what Pulsar cannot
			// carry.
			if mode == mq.MetadataNativeStrict && Representable(m) == nil {
				t.Fatalf("strict mode refused a message Pulsar can carry natively: %v", err)
			}
			return
		}
		var pm pulsar.ProducerMessage
		Encode(&pm, out, map[string]string{})
		// The broker routes, dispatches and compacts by the native key, and it
		// cannot read an envelope, so every mode that keeps the key publishes
		// the native key the message has, enveloped or not.
		if mode != mq.MetadataLenientDrop && pm.Key != nativeKey(m.Key) {
			t.Fatalf("the published partition key is %q, want %q: an enveloped publication went out without "+
				"the key Pulsar routes by", pm.Key, nativeKey(m.Key))
		}
		var decoded mq.Message
		Decode(&decoded, &fuzzMsg{pm: &pm, topic: "persistent://public/default/orders"}, "orders")
		var got mq.Message
		if err := env.Unwrap(&got, &decoded); err != nil {
			t.Fatalf("a message this driver published cannot be read back: %v", err)
		}
		switch mode {
		case mq.MetadataNativeStrict, mq.MetadataPortableEnvelope, mq.MetadataAdaptive:
			// The three modes that promise fidelity.
			if (got.Body == nil) != (m.Body == nil) {
				t.Fatalf("the body's nil-ness changed: %#v -> %#v; the specification makes the distinction "+
					"observable and Pulsar's null-value flag keeps it", m.Body, got.Body)
			}
			if !bytes.Equal(got.Body, m.Body) {
				t.Fatalf("the body came back as %q, want %q", got.Body, m.Body)
			}
			if (got.Key == nil) != (m.Key == nil) || !bytes.Equal(got.Key, m.Key) {
				t.Fatalf("the key came back as %#v, want %#v", got.Key, m.Key)
			}
			if got.ID != m.ID {
				t.Fatalf("the identifier came back as %q, want %q", got.ID, m.ID)
			}
			// Every string property compared exactly, and the
			// creation time compared as an INSTANT: Pulsar's event
			// time is milliseconds since the epoch and the
			// envelope's is nanoseconds, so neither carries a
			// location and a CreatedAt comes back in UTC. It is a
			// property of the carrier rather than a choice, and the
			// driver page says so.
			gp, wp := got.Properties, m.Properties
			if gp.ContentType != wp.ContentType || gp.ContentEncoding != wp.ContentEncoding ||
				gp.CorrelationID != wp.CorrelationID || gp.CausationID != wp.CausationID ||
				gp.ReplyTo != wp.ReplyTo {
				t.Fatalf("the typed properties came back as %+v, want %+v", gp, wp)
			}
			if !gp.CreatedAt.Equal(wp.CreatedAt) {
				t.Fatalf("the creation time came back as %v, want the same instant as %v",
					gp.CreatedAt, wp.CreatedAt)
			}
			if got.Headers.Len() != m.Headers.Len() {
				t.Fatalf("the headers came back as %d values, want %d", got.Headers.Len(), m.Headers.Len())
			}
			for k, v := range m.Headers.All() {
				found := false
				for _, g := range got.Headers.Values(k) {
					if bytes.Equal(g, v) {
						found = true
					}
				}
				if !found {
					t.Fatalf("header %q lost the value %q", k, v)
				}
			}
		case mq.MetadataLenientDrop:
			// the lenient mode drops metadata and keeps the body,
			// and that is the whole promise.
			if !bytes.Equal(got.Body, m.Body) {
				t.Fatalf("lenient mode changed the body: %q -> %q", m.Body, got.Body)
			}
		}
	})
}

// fuzzMessage builds a message out of the reader.
func fuzzMessage(r *fuzzutil.Reader) *mq.Message {
	m := &mq.Message{}
	switch r.Int(0, 3) {
	case 0:
	case 1:
		m.Body = []byte{}
	default:
		m.Body = r.Bytes(64)
	}
	switch r.Int(0, 3) {
	case 0:
	case 1:
		m.Key = []byte{}
	default:
		m.Key = r.Bytes(16)
	}
	if r.Bool() {
		m.ID = r.String(16)
	}
	for range r.Int(0, 4) {
		m.Headers.Add(r.String(8), r.Bytes(8))
	}
	if r.Bool() {
		m.Properties.ContentType = r.String(8)
	}
	if r.Bool() {
		m.Properties.CorrelationID = r.String(8)
	}
	if r.Bool() {
		m.Properties.ReplyTo = r.String(8)
	}
	if r.Bool() {
		// A whole number of milliseconds, because Pulsar's event time is
		// milliseconds since the epoch and a driver cannot carry more.
		m.Properties.CreatedAt = time.UnixMilli(int64(r.Int(1, 1<<40)))
	}
	return m
}

// seedProperties commits one shape per mode and per loss: the plain message,
// the empty and nil bodies and keys, the binary key, the binary header value
// and the duplicate key.
func seedProperties(f *testing.F) {
	f.Helper()
	add := func(mode mq.MetadataMode, build func(*fuzzutil.Writer)) {
		w := fuzzutil.NewWriter()
		w.Int(0, 3, int(mode))
		build(w)
		f.Add(w.Data())
	}
	// A message, written in the order fuzzMessage reads it.
	msg := func(w *fuzzutil.Writer, bodyKind int, body []byte, keyKind int, key []byte,
		id string, headers [][2]string, created bool,
	) {
		w.Int(0, 3, bodyKind)
		if bodyKind > 1 {
			w.Bytes(64, body)
		}
		w.Int(0, 3, keyKind)
		if keyKind > 1 {
			w.Bytes(16, key)
		}
		w.Bool(id != "")
		if id != "" {
			w.String(16, id)
		}
		w.Int(0, 4, len(headers))
		for _, h := range headers {
			w.String(8, h[0])
			w.Bytes(8, []byte(h[1]))
		}
		w.Bool(false)
		w.Bool(false)
		w.Bool(false)
		w.Bool(created)
		if created {
			w.Int(1, 1<<40, 1_700_000_000_000)
		}
	}
	for mode := mq.MetadataNativeStrict; mode.Valid(); mode++ {
		add(mode, func(w *fuzzutil.Writer) {
			msg(w, 2, []byte("payload"), 2, []byte("k"), "id-1", [][2]string{{"kind", "order"}}, true)
		})
		add(mode, func(w *fuzzutil.Writer) { msg(w, 0, nil, 0, nil, "", nil, false) })
		add(mode, func(w *fuzzutil.Writer) { msg(w, 1, nil, 1, nil, "", nil, false) })
		add(mode, func(w *fuzzutil.Writer) {
			msg(w, 2, []byte{0x00, 0xff}, 2, []byte{0xff}, "", nil, false)
		})
		add(mode, func(w *fuzzutil.Writer) {
			msg(w, 2, []byte("b"), 0, nil, "", [][2]string{{"t", "\xff"}}, false)
		})
		add(mode, func(w *fuzzutil.Writer) {
			msg(w, 2, []byte("b"), 0, nil, "", [][2]string{{"t", "a"}, {"t", "b"}}, false)
		})
		// A textual key on a message the adaptive mode envelopes for its
		// headers.
		add(mode, func(w *fuzzutil.Writer) {
			msg(w, 2, []byte("b"), 2, []byte("customer-7"), "", [][2]string{{"t", "a"}, {"t", "b"}}, false)
		})
	}
}

// nativeKey is the partition key Pulsar can carry for key: the key itself, or
// none when it is empty or not valid UTF-8.
func nativeKey(key []byte) string {
	if len(key) == 0 || !utf8.Valid(key) {
		return ""
	}
	return string(key)
}

// TestAnEnvelopedMessageKeepsItsPartitionKey: an enveloped publication still
// carries the key Pulsar routes by.
//
// The envelope carries the key back to an iqmq consumer, and the broker cannot
// read it: Pulsar picks the partition, a Key_Shared subscription dispatches,
// and compaction keeps the latest message, by the native key alone. The message
// driverkit's Wrap returns has no key of its own, so every enveloped
// publication went out keyless while FeatureOrderedByKey was claimed. A key
// with no native form travels in the envelope alone.
func TestAnEnvelopedMessageKeepsItsPartitionKey(t *testing.T) {
	lim := wire.DefaultLimits()
	dup := func(m *mq.Message) *mq.Message {
		// A repeated header key, which a property map cannot hold, so the
		// adaptive mode envelopes the message too.
		m.Headers.Add("t", []byte("a"))
		m.Headers.Add("t", []byte("b"))
		return m
	}
	for _, mode := range []mq.MetadataMode{mq.MetadataPortableEnvelope, mq.MetadataAdaptive} {
		caps, err := capabilities(Config{Metadata: mode}.resolve())
		if err != nil {
			t.Fatalf("building the capability table: %v", err)
		}
		env, err := driverkit.NewEnvelope(driverkit.EnvelopeConfig{Mode: mode, Limits: &lim, Capabilities: caps})
		if err != nil {
			t.Fatalf("NewEnvelope: %v", err)
		}
		forced, err := driverkit.NewEnvelope(driverkit.EnvelopeConfig{
			Mode: mq.MetadataPortableEnvelope, Limits: &lim, Capabilities: caps,
		})
		if err != nil {
			t.Fatalf("NewEnvelope(portable): %v", err)
		}
		for _, tc := range []struct {
			name string
			key  []byte
			want string
		}{
			{"a textual key", []byte("customer-7"), "customer-7"},
			{"a key that is not valid UTF-8", []byte{0xff, 0x00}, ""},
			{"an empty key", []byte{}, ""},
			{"no key", nil, ""},
		} {
			m := dup(&mq.Message{Body: []byte("b"), Key: tc.key})
			out, err := wrapForPublish(env, forced, &lim, mode, m)
			if err != nil {
				t.Fatalf("%v, %s: %v", mode, tc.name, err)
			}
			if out == m {
				t.Fatalf("%v, %s: the message was not enveloped, so this test is not about an envelope", mode, tc.name)
			}
			var pm pulsar.ProducerMessage
			Encode(&pm, out, map[string]string{})
			if pm.Key != tc.want {
				t.Errorf("%v, %s: the enveloped publication's partition key is %q, want %q", mode, tc.name, pm.Key, tc.want)
			}
			var got mq.Message
			if err := forced.Unwrap(&got, &mq.Message{Body: pm.Payload}); err != nil {
				t.Fatalf("%v, %s: the envelope does not decode: %v", mode, tc.name, err)
			}
			if (got.Key == nil) != (tc.key == nil) || !bytes.Equal(got.Key, tc.key) {
				t.Errorf("%v, %s: the envelope's key came back as %#v, want %#v", mode, tc.name, got.Key, tc.key)
			}
		}
	}
}

// fuzzMsg adapts a pulsar.ProducerMessage to the pulsar.Message interface, so
// that the encoder and the decoder can be held to each other without a broker.
type fuzzMsg struct {
	pm    *pulsar.ProducerMessage
	topic string
}

func (e *fuzzMsg) Topic() string                 { return e.topic }
func (e *fuzzMsg) ProducerName() string          { return "fuzz" }
func (e *fuzzMsg) Properties() map[string]string { return e.pm.Properties }
func (e *fuzzMsg) Payload() []byte               { return e.pm.Payload }
func (e *fuzzMsg) IsNullValue() bool             { return e.pm.Payload == nil }
func (e *fuzzMsg) ID() pulsar.MessageID          { return pulsar.EarliestMessageID() }
func (e *fuzzMsg) PublishTime() time.Time        { return time.Unix(0, 0) }
func (e *fuzzMsg) EventTime() time.Time          { return e.pm.EventTime }
func (e *fuzzMsg) Key() string                   { return e.pm.Key }
func (e *fuzzMsg) OrderingKey() string           { return e.pm.OrderingKey }
func (e *fuzzMsg) RedeliveryCount() uint32       { return 0 }
func (e *fuzzMsg) IsReplicated() bool            { return false }
func (e *fuzzMsg) GetReplicatedFrom() string     { return "" }
func (e *fuzzMsg) GetSchemaValue(any) error      { return errors.New("no schema") }
func (e *fuzzMsg) SchemaVersion() []byte         { return nil }
func (e *fuzzMsg) Index() *uint64                { return nil }
func (e *fuzzMsg) BrokerPublishTime() *time.Time { return nil }

func (e *fuzzMsg) GetEncryptionContext() *pulsar.EncryptionContext { return nil }
