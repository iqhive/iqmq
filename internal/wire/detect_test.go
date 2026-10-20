package wire

import (
	"bytes"
	"testing"
	"time"

	"github.com/iqhive/iqmq/internal/payload"
)

// TestDetectFullClaimsOnlyAWholeEnvelope is the property Detect does not have
// and the one an adaptive driver needs.
//
// Detect reads six bytes, so it answers true for anything that begins like an
// envelope. On a broker that can record "this record is enveloped" out of band
// that is harmless; on one that cannot — NSQ has no headers at all — the body
// is the only evidence there is, and a caller that published one of those byte
// strings had it decoded as metadata or left a record nothing could decode.
func TestDetectFullClaimsOnlyAWholeEnvelope(t *testing.T) {
	whole := map[string][]byte{}
	for _, k := range Kinds() {
		l := mustNew(t, k)
		for name, p := range map[string]*payload.Payload{
			"empty":      {},
			"key body":   {Key: []byte("k"), Body: []byte("b")},
			"headers":    {Headers: []payload.KV{{Key: "content-type", Value: []byte("application/json")}}, ID: "id"},
			"properties": {Props: payload.Props{CreatedAt: time.Unix(1757160000, 1).UTC(), ContentType: "text/plain"}},
		} {
			b, err := l.Encode(nil, p)
			if err != nil {
				t.Fatalf("%v/%s: Encode: %v", k, name, err)
			}
			whole[k.String()+"/"+name] = b
		}
	}
	for name, env := range whole {
		if k, ok := NewDetector().DetectFull(env); !ok {
			t.Errorf("%s: DetectFull refused a whole envelope", name)
		} else if _, want := Detect(env); !want {
			t.Errorf("%s: Detect and DetectFull disagree (%v)", name, k)
		}
	}

	// Every one of these begins with the six bytes Detect reads, and none of
	// them is an envelope.
	forgeries := map[string][]byte{
		"the prefix alone":          []byte("IQMQ\x11\x00"),
		"the prefix and a payload":  []byte("IQMQ\x01\x00 and then some ordinary bytes"),
		"a truncated fixed header":  bytes.Repeat([]byte("IQMQ\x11\x00"), 3),
		"the other layout's prefix": []byte("IQMQ\x01\x0f\x00\x00\x00\x00"),
	}
	for _, k := range Kinds() {
		b, err := mustNew(t, k).Encode(nil, &payload.Payload{Key: []byte("k"), Body: []byte("body")})
		if err != nil {
			t.Fatalf("Encode: %v", err)
		}
		forgeries[k.String()+"/one byte short"] = bytes.Clone(b[:len(b)-1])
		forgeries[k.String()+"/one byte over"] = append(bytes.Clone(b), 0)
	}
	for name, body := range forgeries {
		if _, sniffed := Detect(body); !sniffed {
			t.Fatalf("%s: the test's own forgery does not even pass the six-byte sniff, "+
				"so it proves nothing about DetectFull", name)
		}
		if k, ok := NewDetector().DetectFull(body); ok {
			t.Errorf("%s: DetectFull claimed %v for a body that is not a whole envelope: "+
				"a native publication would be decoded as metadata", name, k)
		}
	}
}

// TestDetectFullIgnoresTheCallersLimits pins the one thing DetectFull must not
// do: depend on what a particular consumer will accept.
//
// A consumer that read an envelope too large for its own limits as a NATIVE
// body would deliver the envelope's own bytes as a payload, silently, instead
// of producing the refusal the limit exists for. So the question "is this an
// envelope" is answered with no limits at all, and the limits are applied
// afterwards by Decode.
func TestDetectFullIgnoresTheCallersLimits(t *testing.T) {
	for _, k := range Kinds() {
		l := mustNew(t, k)
		p := &payload.Payload{Body: bytes.Repeat([]byte("b"), 4096)}
		b, err := l.Encode(nil, p)
		if err != nil {
			t.Fatalf("%v: Encode: %v", k, err)
		}
		tight := &Limits{MaxBody: 16}
		if derr := l.Decode(b, &payload.Payload{}, tight); derr == nil {
			t.Fatalf("%v: the tight limits accepted a 4 KiB body, so the test proves nothing", k)
		}
		if _, ok := NewDetector().DetectFull(b); !ok {
			t.Errorf("%v: DetectFull refused an envelope that only exceeds one consumer's limits", k)
		}
	}
}

// FuzzDetectFull holds DetectFull to the decoder it is meant to agree with:
// it claims src exactly when an unbounded Decode of src succeeds.
//
// That equivalence is the whole contract. Written as its own walk over the
// framing it would be a second reading of the decoder's rules, which is the
// defect class wire.Check exists for; written as the decode itself it cannot
// drift.
func FuzzDetectFull(f *testing.F) {
	seeds := [][]byte{
		nil,
		{},
		[]byte("IQMQ"), []byte("IQMQ\x11\x00"), []byte("IQMQ\x01\x00"),
		[]byte("IQMQ\x01\x00 and then some ordinary bytes"),
		bytes.Repeat([]byte{0xff}, 32),
	}
	for _, k := range Kinds() {
		l := mustNew(f, k)
		for _, p := range []*payload.Payload{
			{},
			{Key: []byte("k"), Body: []byte("b")},
			{Headers: []payload.KV{{Key: "trace", Value: []byte{0x00, 0xff}}}, ID: "id"},
			{Props: payload.Props{CreatedAt: time.Unix(1757160000, 1).UTC(), ReplyTo: "r"}},
		} {
			b, err := l.Encode(nil, p)
			if err != nil {
				continue
			}
			seeds = append(seeds, b)
			if len(b) > 8 {
				seeds = append(seeds, bytes.Clone(b[:len(b)-1]), append(bytes.Clone(b), 0))
			}
		}
	}
	for _, s := range seeds {
		f.Add(s)
	}
	unbounded := &Limits{
		MaxKey: -1, MaxBody: -1, MaxID: -1, MaxHeaders: -1,
		MaxHeaderKey: -1, MaxHeaderValue: -1, MaxProp: -1,
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		k, claimed := NewDetector().DetectFull(data)
		sniffed, prefix := Detect(data)
		if claimed && !prefix {
			t.Fatalf("DetectFull claimed %v for something Detect refuses", k)
		}
		var decoded bool
		var kind Kind
		if prefix {
			var p payload.Payload
			if err := mustNew(t, sniffed).Decode(data, &p, unbounded); err == nil {
				decoded, kind = true, sniffed
			}
		}
		if claimed != decoded {
			t.Fatalf("DetectFull says %v and an unbounded decode says %v", claimed, decoded)
		}
		if claimed && k != kind {
			t.Fatalf("DetectFull says %v and the decode says %v", k, kind)
		}
	})
}
