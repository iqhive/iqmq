package driverkit_test

import (
	"bytes"
	"strconv"
	"strings"
	"testing"
	"time"

	mq "bitbucket.org/iqhive/iqmq"
	"bitbucket.org/iqhive/iqmq/driverkit"
	"bitbucket.org/iqhive/iqmq/internal/fuzzutil"
	"bitbucket.org/iqhive/iqmq/wire"
)

// envelopeLimits is the limit set handed to BOTH halves of the envelope, which
// is what a driver author does not do by accident: EnvelopeConfig.Limits reads
// "bounds what Decode accepts" and says nothing about the encoder.
//
// It is wire.DefaultLimits itself rather than a copy of its numbers. The specification
// recorded streams.DefaultLimits.MaxHeaderKey = 259 against
// wire.DefaultLimits.MaxHeaderKey = 256 as a defect of exactly the kind this
// target exists to find, so a hand-written bound here would be a second one.
func envelopeLimits() *wire.Limits {
	lim := wire.DefaultLimits
	return &lim
}

// fuzzEnvelopeMessage builds a message from the fuzzer's bytes, deliberately
// reaching PAST the decoder's limits and past UTF-8 validity: the interesting
// inputs are the ones one byte over a bound and the ones with a lone
// continuation byte in a field the decoder validates.
func fuzzEnvelopeMessage(r *fuzzutil.Reader) *mq.Message {
	m := &mq.Message{}
	switch r.Choice(3) {
	case 0:
		m.Body = nil
	case 1:
		m.Body = []byte{}
	default:
		m.Body = r.Bytes(512)
	}
	switch r.Choice(3) {
	case 0:
		m.Key = nil
	case 1:
		m.Key = []byte{}
	default:
		m.Key = r.Bytes(300)
	}
	m.Topic = r.String(64)
	m.ID = r.String(300)
	for n := r.Int(0, 6); n > 0; n-- {
		m.Headers.Add(r.String(300), r.Bytes(300))
	}
	if r.Bool() {
		m.Properties.ContentType = r.String(64)
	}
	if r.Bool() {
		m.Properties.ContentEncoding = r.String(64)
	}
	if r.Bool() {
		m.Properties.CorrelationID = r.String(64)
	}
	if r.Bool() {
		m.Properties.CausationID = r.String(64)
	}
	if r.Bool() {
		m.Properties.ReplyTo = r.String(64)
	}
	if r.Bool() {
		m.Properties.CreatedAt = time.Unix(int64(r.Uint64()>>2)-(1<<40), int64(r.Uint32()%1e9)).UTC()
	}
	return m
}

// seedEnvelope commits the inputs that found the hole in its three skins, plus
// the framing corners of the layouts.
func seedEnvelope(f *testing.F) {
	repeat := func(c byte, n int) string { return strings.Repeat(string([]byte{c}), n) }
	layouts := len(wire.Kinds())
	seeds := []func(*fuzzutil.Writer){
		// An ordinary message: a body, no key, one header, no properties.
		func(w *fuzzutil.Writer) {
			w.Choice(3, 2)
			w.Bytes(512, []byte("payload"))
			w.Choice(3, 0)
			w.String(64, "acme.orders")
			w.String(300, "")
			w.Int(0, 6, 1)
			w.String(300, "trace")
			w.Bytes(300, []byte("abc"))
			for range 6 {
				w.Bool(false)
			}
		},
		// A header key one byte over wire.DefaultLimits.MaxHeaderKey: the
		// input the fuzz target found in five seconds against its toy
		// driver's envelope.
		func(w *fuzzutil.Writer) {
			w.Choice(3, 2)
			w.Bytes(512, []byte("x"))
			w.Choice(3, 0)
			w.String(64, "t")
			w.String(300, "")
			w.Int(0, 6, 1)
			w.String(300, repeat('k', wire.DefaultLimits.MaxHeaderKey+1))
			w.Bytes(300, []byte("v"))
			for range 6 {
				w.Bool(false)
			}
		},
		// A message identifier one byte over MaxID: the input it found in
		// the next sixty-seven seconds, against a fix that bounded only the
		// headers.
		func(w *fuzzutil.Writer) {
			w.Choice(3, 2)
			w.Bytes(512, []byte("x"))
			w.Choice(3, 0)
			w.String(64, "t")
			w.String(300, repeat('i', wire.DefaultLimits.MaxID+1))
			w.Int(0, 6, 0)
			for range 6 {
				w.Bool(false)
			}
		},
		// Invalid UTF-8 in the identifier: the third skin, which is not a
		// length at all. FuzzRedisFields found it in a tenth of a second.
		func(w *fuzzutil.Writer) {
			w.Choice(3, 2)
			w.Bytes(512, []byte("x"))
			w.Choice(3, 0)
			w.String(64, "t")
			w.String(300, "\xff")
			w.Int(0, 6, 0)
			for range 6 {
				w.Bool(false)
			}
		},
		// Invalid UTF-8 in a header key, whose value is binary and therefore
		// legal: only the key is validated.
		func(w *fuzzutil.Writer) {
			w.Choice(3, 2)
			w.Bytes(512, []byte("x"))
			w.Choice(3, 0)
			w.String(64, "t")
			w.String(300, "")
			w.Int(0, 6, 1)
			w.String(300, "k\x80")
			w.Bytes(300, []byte{0, 0xff})
			for range 6 {
				w.Bool(false)
			}
		},
		// Invalid UTF-8 in a string property.
		func(w *fuzzutil.Writer) {
			w.Choice(3, 2)
			w.Bytes(512, []byte("x"))
			w.Choice(3, 0)
			w.String(64, "t")
			w.String(300, "")
			w.Int(0, 6, 0)
			w.Bool(true)
			w.String(64, "text/\xc3")
			for range 5 {
				w.Bool(false)
			}
		},
		// A key one byte over MaxID's neighbour is not reachable at these
		// sizes, so the length corner covered here is the header COUNT: more
		// headers than the fuzz message can hold naturally, each empty.
		func(w *fuzzutil.Writer) {
			w.Choice(3, 1)
			w.Choice(3, 1)
			w.String(64, "t")
			w.String(300, "")
			w.Int(0, 6, 6)
			for range 6 {
				w.String(300, "")
				w.Bytes(300, nil)
			}
			for range 6 {
				w.Bool(false)
			}
		},
		// Every property set, with a created-at in the far past.
		func(w *fuzzutil.Writer) {
			w.Choice(3, 2)
			w.Bytes(512, []byte("x"))
			w.Choice(3, 2)
			w.Bytes(300, []byte("k"))
			w.String(64, "t")
			w.String(300, "01J8ZQ2K3M4N5P6Q7R8S9T0V1W")
			w.Int(0, 6, 0)
			for range 5 {
				w.Bool(true)
				w.String(64, "v")
			}
			w.Bool(true)
			w.Uint64(0)
			w.Uint32(999999999)
		},
	}
	for ki := range layouts {
		for _, seed := range seeds {
			w := fuzzutil.NewWriter()
			w.Choice(layouts, ki)
			seed(w)
			f.Add(w.Data())
		}
	}
	f.Add([]byte{})
	f.Add([]byte{0, 0, 0, 0})
}

// FuzzEnvelopeCheck holds the one property a driver cannot check for itself:
// the portable envelope never encodes a message it cannot decode.
//
// Wrap encodes whatever it is given and Unwrap decodes under wire.Limits, and
// the decoder validates more than the encoder ever did — the key, identifier
// and header bounds, the header count, the string-property bounds, and UTF-8 in
// the identifier, every header key and every string property. A driver that
// accepted a publication and enveloped it could therefore store a record that
// breaks every subsequent Fetch on that subscription for ever: the record
// cannot be decoded, so it cannot be delivered, so it cannot be settled. That
// is the disqualifier reached through a message the driver made itself.
//
// The target asserts both halves of the fix. Anything Wrap produces must
// Unwrap, and Envelope.Check must answer the same question before any I/O —
// neither refusing a message the envelope would have carried, nor accepting one
// it would not.
func FuzzEnvelopeCheck(f *testing.F) {
	seedEnvelope(f)
	f.Fuzz(func(t *testing.T, data []byte) {
		r := fuzzutil.NewReader(data)
		layouts := wire.Kinds()
		layout := layouts[r.Choice(len(layouts))]
		env, err := driverkit.NewEnvelope(driverkit.EnvelopeConfig{
			Mode:   mq.MetadataPortableEnvelope,
			Layout: layout,
			Limits: envelopeLimits(),
		})
		if err != nil {
			t.Fatalf("NewEnvelope: %v", err)
		}
		m := fuzzEnvelopeMessage(r)
		shape := func() string {
			return "layout=" + layout.String() + " id=" + itoa(len(m.ID)) +
				"B headers=" + itoa(m.Headers.Len()) + " key=" + itoa(len(m.Key)) +
				"B body=" + itoa(len(m.Body)) + "B"
		}

		cerr := env.Check(m)
		wrapped, werr := env.Wrap(m)
		switch {
		case cerr == nil && werr != nil:
			t.Fatalf("Check accepted a message Wrap refused with %v: a driver that checks "+
				"before it admits would fail after its own admission\n%s", werr, shape())
		case cerr != nil && werr == nil:
			t.Fatalf("Check refused a message Wrap carried (%v): the check is stricter than "+
				"the envelope, so a driver that trusts it refuses publications the broker "+
				"would have taken\n%s", cerr, shape())
		case cerr != nil:
			// Refused before any I/O, which is the whole point: the
			// publication is never admitted and no record is written.
			return
		}
		var back mq.Message
		if uerr := env.Unwrap(&back, wrapped); uerr != nil {
			t.Fatalf("the portable envelope encoded a message it cannot decode: %v\n%s",
				uerr, shape())
		}
		// The round trip is the reason the bound exists, so check it rather
		// than only the absence of an error.
		if back.ID != m.ID || back.Headers.Len() != m.Headers.Len() ||
			back.Properties != m.Properties || !bytes.Equal(back.Body, m.Body) ||
			!bytes.Equal(back.Key, m.Key) {
			t.Fatalf("the envelope round trip changed the message\n%s", shape())
		}
	})
}

// itoa is strconv.Itoa under a shorter name, for the failure messages above.
func itoa(n int) string { return strconv.Itoa(n) }

// TestEnvelopeCheckReadsTheConfiguredLimits holds Check to the limits the
// envelope was BUILT with rather than to wire.DefaultLimits.
//
// A driver that tightens EnvelopeConfig.Limits — because its broker's entry
// size is smaller than iqmq's default, which is the usual reason to set the
// field at all — gets a decoder that refuses more, and a Check that answered
// from the defaults would hand it back exactly the undecodable record the
// function exists to prevent.
func TestEnvelopeCheckReadsTheConfiguredLimits(t *testing.T) {
	tight := &wire.Limits{MaxHeaderKey: 8, MaxID: 4, MaxProp: 4}
	env, err := driverkit.NewEnvelope(driverkit.EnvelopeConfig{
		Mode:   mq.MetadataPortableEnvelope,
		Limits: tight,
	})
	if err != nil {
		t.Fatalf("NewEnvelope: %v", err)
	}
	for _, tc := range []struct {
		name string
		m    *mq.Message
		want mq.ErrorCode
	}{
		{"a header key at the bound", headerMessage(strings.Repeat("k", 8)), mq.CodeUnknown},
		{"a header key one over", headerMessage(strings.Repeat("k", 9)), mq.CodeMessageTooLarge},
		{"an identifier at the bound", &mq.Message{ID: "abcd"}, mq.CodeUnknown},
		{"an identifier one over", &mq.Message{ID: "abcde"}, mq.CodeMessageTooLarge},
		{"a property one over", &mq.Message{Properties: mq.Properties{ReplyTo: "abcde"}}, mq.CodeMessageTooLarge},
		{"an identifier that is not UTF-8", &mq.Message{ID: "a\xff"}, mq.CodeCorruptMessage},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cerr := env.Check(tc.m)
			if tc.want == mq.CodeUnknown {
				if cerr != nil {
					t.Fatalf("Check refused a message inside the configured limits: %v", cerr)
				}
				if _, werr := env.Wrap(tc.m); werr != nil {
					t.Fatalf("Wrap refused a message Check accepted: %v", werr)
				}
				return
			}
			if mq.CodeOf(cerr) != tc.want {
				t.Fatalf("Check answered %v (%v), want %v: the check is not reading the "+
					"limits the envelope was built with", mq.CodeOf(cerr), cerr, tc.want)
			}
			if _, werr := env.Wrap(tc.m); mq.CodeOf(werr) != tc.want {
				t.Fatalf("Wrap answered %v, want %v: Wrap must refuse what Check refuses",
					mq.CodeOf(werr), tc.want)
			}
		})
	}
}

// headerMessage is a message with one header of the given key.
func headerMessage(key string) *mq.Message {
	m := &mq.Message{Body: []byte("x")}
	m.Headers.Add(key, []byte("v"))
	return m
}
