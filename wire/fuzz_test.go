package wire_test

import (
	"bytes"
	"testing"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/wire"
)

// FuzzEnvelope is this package's target for the parser rule: all parsing is
// pure with explicit limits, never panics and never allocates unboundedly,
// and the bytes of an envelope come off a wire a stranger can write to.
//
// It checks the two properties that matter about a decoder:
//
//   - it never panics, whatever the bytes are, in either layout and under
//     either copy mode;
//   - the encoding of an envelope it ACCEPTED is a FIXED POINT: encoding the
//     message it decoded to, decoding that and encoding again gives the same
//     bytes and an equal message. That is the canonical-encoding rule as it
//     can be stated at this layer — one message has one valid encoding per
//     layout, so a decoder that accepted a second spelling of one message
//     would let two peers disagree about whether two envelopes are the same
//     message.
//
// It is a fixed point rather than "re-encodes to exactly the bytes it came
// from" for one reason, and it is a property of this package rather than of
// the layouts: the layouts distinguish a nil header table from an empty one
// and an mq.Message cannot — Headers.Len is 0 either way — so this package
// normalises the empty table to nil. An envelope
// declaring an empty table therefore re-encodes to the nil spelling, which is
// one step of normalisation and not a disagreement; every OTHER
// non-canonicality survives the fixed-point check, because a second spelling
// of one message would have to be a fixed point of its own.
func FuzzEnvelope(f *testing.F) {
	for _, m := range corpus() {
		for _, k := range wire.Kinds() {
			buf, err := wire.New(k).Encode(nil, m)
			if err != nil {
				continue
			}
			f.Add(buf)
		}
	}
	f.Add([]byte{})
	f.Add([]byte("IQMQ"))
	f.Add([]byte("not an envelope at all"))
	f.Fuzz(func(t *testing.T, src []byte) {
		for _, k := range wire.Kinds() {
			l := wire.New(k)
			for _, zeroCopy := range []bool{false, true} {
				var m mq.Message
				err := l.Decode(src, &m, &wire.Limits{ZeroCopy: zeroCopy})
				if err != nil {
					// Every refusal carries a code a caller can branch on.
					switch mq.CodeOf(err) {
					case mq.CodeCorruptMessage, mq.CodeMessageTooLarge:
					default:
						t.Fatalf("%v: Decode refused %d bytes with %v (code %v), want a corrupt or too-large code",
							k, len(src), err, mq.CodeOf(err))
					}
					continue
				}
				// Canonical: the encoding of the message this envelope
				// decoded to is a fixed point.
				again, eerr := l.Encode(nil, &m)
				if eerr != nil {
					t.Fatalf("%v: a decoded message would not re-encode: %v", k, eerr)
				}
				if got := l.Size(&m); got != len(again) {
					t.Fatalf("%v: Size said %d and Encode wrote %d", k, got, len(again))
				}
				var round mq.Message
				if rerr := l.Decode(again, &round, &wire.Limits{ZeroCopy: zeroCopy}); rerr != nil {
					t.Fatalf("%v: an envelope this layout wrote would not decode: %v", k, rerr)
				}
				if !equal(&m, &round) {
					t.Fatalf("%v: the round trip is not stable:\n got %+v\nwant %+v", k, &round, &m)
				}
				third, terr := l.Encode(nil, &round)
				if terr != nil {
					t.Fatalf("%v: a decoded message would not re-encode: %v", k, terr)
				}
				if !bytes.Equal(again, third) {
					t.Fatalf("%v: the encoding is not canonical: %d bytes re-encoded to %d different bytes",
						k, len(again), len(third))
				}
				// Detect agrees with the layout that accepted it.
				if dk, ok := wire.Detect(src); !ok || dk != k {
					t.Fatalf("%v: Detect returned (%v, %v) for an envelope this layout accepted", k, dk, ok)
				}
			}
		}
	})
}
