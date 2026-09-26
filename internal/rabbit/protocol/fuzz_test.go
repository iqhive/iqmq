package protocol

import (
	"bytes"
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/internal/rabbit"
)

// FuzzRabbitProtocol holds both arms to the fidelity the specification asks
// of a driver, over arbitrary messages.
//
// The property is the one a caller depends on and the one a property mapping is
// most likely to break on an input nobody thought of: a message that is
// CARRIED and then RESTORED is the message that was published, byte for byte,
// or the carry refused it. There is no third outcome — a mapping that accepted
// a message and gave back a different one is worse than one that refused it,
// because nothing in a deployment reports it.
//
// The second property is the reserved prefix. This driver puts its own entries
// in the same table as the application's, so a round trip must not hand any of
// them back as application headers: a message that arrived with an
// `x-iqmq-key` header the publisher never set would fail the fidelity
// scenario, and worse, an application that echoed its headers would publish a
// key it did not choose.
func FuzzRabbitProtocol(f *testing.F) {
	f.Add(uint8(0), uint8(0), "", "", []byte(""), []byte(""), int64(0))
	f.Add(uint8(1), uint8(0), "id", "text/plain", []byte("k"), []byte("v"), int64(1000))
	f.Add(uint8(0), uint8(3), "id", "", []byte{0x00, 0xff}, []byte{0x00, 0xff}, int64(0))
	f.Add(uint8(0), uint8(0), "id", "", []byte(""), []byte("x-iqmq-key"), int64(0))
	f.Add(uint8(1), uint8(1), "\xff", "\xff", []byte("\xff"), []byte("\xff"), int64(-1))
	f.Add(uint8(0), uint8(0), "id", "ct", []byte("k"), []byte(""), int64(1<<40))

	f.Fuzz(func(t *testing.T, kindByte, modeByte uint8, id, contentType string, key, headerValue []byte, ttlMS int64) {
		k := Kinds()[int(kindByte)%len(Kinds())]
		p, err := New(k)
		if err != nil {
			t.Fatalf("New(%v) = %v", k, err)
		}
		modes := [...]mq.MetadataMode{
			mq.MetadataNativeStrict, mq.MetadataPortableEnvelope,
			mq.MetadataAdaptive, mq.MetadataLenientDrop,
		}
		mode := modes[int(modeByte)%len(modes)]

		src := &mq.Message{
			Key:  key,
			Body: []byte("body"),
			ID:   id,
			Properties: mq.Properties{
				ContentType:   contentType,
				CorrelationID: id,
				CausationID:   contentType,
				ReplyTo:       id,
				CreatedAt:     time.Unix(0, 0).UTC(),
			},
		}
		// The header key is derived from the value so that the fuzzer can
		// reach the reserved prefix, an over-long key and an empty one
		// without needing a seventh parameter.
		headerKey := string(headerValue)
		if headerKey != "" {
			src.Headers.Add(headerKey, headerValue)
		}

		o := Options{Metadata: mode, KeyHeader: ReservedHeaderPrefix + "key"}
		if ttlMS > 0 && ttlMS < int64(time.Hour/time.Millisecond) {
			o.Publish.TTL = time.Duration(ttlMS) * time.Millisecond
		}

		var fields Fields
		if cerr := p.Carry(&fields, src, o); cerr != nil {
			// A refusal has to be one of the codes a caller acts on.
			switch code := mq.CodeOf(cerr); code {
			case mq.CodeUnsupported, mq.CodeInvalidArgument, mq.CodeInvalidConfig, mq.CodeMessageTooLarge:
			default:
				t.Fatalf("Carry refused with code %v: %v", code, cerr)
			}
			return
		}

		// Every entry the mapping produced is one the protocol can encode:
		// a field-table key is a shortstr, so a key over 255 bytes cannot
		// be put on the wire at all and the client library's answer to it is
		// a channel exception.
		for i := range fields.Entries {
			e := &fields.Entries[i]
			if e.Key == "" {
				t.Fatalf("entry %d has an empty key", i)
			}
			if len(e.Key) > rabbit.MaxNameLen {
				t.Fatalf("entry %d has a %d-byte key, which an AMQP shortstr cannot carry", i, len(e.Key))
			}
			if !e.Kind.valid() {
				t.Fatalf("entry %d has the value kind %d", i, e.Kind)
			}
		}
		// Exactly one TTL spelling, and it is this protocol's.
		if fields.Expiration != "" && fields.TTL != 0 {
			t.Fatalf("both TTL spellings are filled: %q and %v", fields.Expiration, fields.TTL)
		}

		// A DELIVERY's entries come from the broker, not from Carry, so the
		// fuzz target feeds Restore one reserved entry that Carry never
		// produces -- the publisher-confirm correlation token a windowed
		// strategy puts on a `mandatory` publication. Without it the
		// reserved-prefix filter in Restore is unreachable from this target,
		// which is how a mutation that deleted it survived every other check.
		fields.Entries = append(fields.Entries, Entry{Key: ReservedHeaderPrefix + "pub", Text: "p1", Kind: Text})

		var back mq.Message
		if rerr := p.Restore(&back, &fields, o); rerr != nil {
			t.Fatalf("Restore refused a Fields that Carry produced: %v", rerr)
		}

		// (1) The message came back.
		if back.ID != src.ID {
			t.Fatalf("the identifier round-tripped as %q, want %q", back.ID, src.ID)
		}
		if back.Properties != src.Properties {
			t.Fatalf("the properties round-tripped as %+v, want %+v", back.Properties, src.Properties)
		}
		if !bytes.Equal(back.Key, src.Key) && (len(back.Key) != 0 || len(src.Key) != 0) {
			t.Fatalf("the key round-tripped as %x, want %x", back.Key, src.Key)
		}
		if mode != mq.MetadataLenientDrop || fields.Dropped == 0 {
			if back.Headers.Len() != src.Headers.Len() {
				t.Fatalf("%d headers round-tripped, want %d", back.Headers.Len(), src.Headers.Len())
			}
			for key, want := range src.Headers.All() {
				got, ok := back.Headers.Get(key)
				if !ok || !bytes.Equal(got, want) {
					t.Fatalf("header %q round-tripped as %x (present=%v), want %x", key, got, ok, want)
				}
			}
		}

		// (2) None of this driver's own entries reached the application.
		for key := range back.Headers.All() {
			if hasPrefix(key, ReservedHeaderPrefix) {
				t.Fatalf("the restored message carries this driver's own header %q", key)
			}
		}
	})
}

// valid reports whether v names a value kind, for the fuzz target's check that
// the mapping never produces one it does not.
func (v ValueKind) valid() bool { return v < valueKindCount }
