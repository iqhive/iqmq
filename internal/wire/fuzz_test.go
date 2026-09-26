package wire

import (
	"bytes"
	"runtime"
	"testing"
	"time"

	"github.com/iqhive/iqmq/internal/errcode"
	"github.com/iqhive/iqmq/internal/fuzzutil"
	"github.com/iqhive/iqmq/internal/payload"
)

// fuzzPayload decodes a payload from fuzz input: every field is optional
// and each byte-valued field can be nil, empty or arbitrary.
func fuzzPayload(r *fuzzutil.Reader) *payload.Payload {
	p := &payload.Payload{}
	switch r.Choice(3) {
	case 1:
		p.Key = []byte{}
	case 2:
		p.Key = bytes.Clone(r.Bytes(512))
	}
	switch r.Choice(3) {
	case 1:
		p.Body = []byte{}
	case 2:
		p.Body = bytes.Clone(r.Bytes(4096))
	}
	switch r.Choice(3) {
	case 1:
		p.Headers = []payload.KV{}
	case 2:
		n := r.Int(0, 16)
		p.Headers = make([]payload.KV, n)
		for i := range p.Headers {
			p.Headers[i].Key = sanitise(r.String(64))
			if r.Bool() {
				p.Headers[i].Value = bytes.Clone(r.Bytes(256))
			}
		}
	}
	p.ID = sanitise(r.String(64))
	p.Props.ContentType = sanitise(r.String(64))
	p.Props.ContentEncoding = sanitise(r.String(32))
	p.Props.CorrelationID = sanitise(r.String(64))
	p.Props.CausationID = sanitise(r.String(64))
	p.Props.ReplyTo = sanitise(r.String(64))
	if r.Bool() {
		sec := int64(r.Uint64()%(1<<34)) - (1 << 33)
		p.Props.CreatedAt = time.Unix(sec, int64(r.Uint64()%1e9)).UTC()
	}
	return p
}

// sanitise replaces invalid UTF-8 with a marker: string fields carry text,
// and the encoder's contract is that a payload it accepts round trips.
func sanitise(s string) string {
	if validString([]byte(s)) {
		return s
	}
	out := make([]rune, 0, len(s))
	for _, r := range s {
		if r == '�' {
			r = '?'
		}
		out = append(out, r)
	}
	return string(out)
}

// FuzzWireRoundTrip encodes fuzz-built payloads with every layout and
// checks the envelope contract: Size is exact, Encode appends, Decode
// reproduces the payload (nil versus empty included), re-encoding is
// canonical, decoding into a reused destination matches a fresh one, and
// zero-copy decoding aliases the envelope while producing equal content.
func FuzzWireRoundTrip(f *testing.F) {
	for ki := range Kinds() {
		w := fuzzutil.NewWriter()
		w.Choice(len(Kinds()), ki)
		w.Choice(3, 2)
		w.Bytes(512, []byte("orders/42"))
		w.Choice(3, 2)
		w.Bytes(4096, []byte("hello wire"))
		w.Choice(3, 2)
		w.Int(0, 16, 2)
		w.String(64, "content-type")
		w.Bool(true)
		w.Bytes(256, []byte("application/json"))
		w.String(64, "iqmq-attempts")
		w.Bool(false)
		w.String(64, "01J8ZQ2K3M4N5P6Q7R8S9T0V1W")
		w.String(64, "application/json")
		w.String(32, "gzip")
		w.String(64, "corr-1")
		w.String(64, "cause-1")
		w.String(64, "orders.reply")
		w.Bool(true)
		w.Uint64(1 << 32)
		w.Uint64(500)
		f.Add(w.Data())
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		r := fuzzutil.NewReader(data)
		k := Kinds()[r.Choice(len(Kinds()))]
		l := New(k)
		p := fuzzPayload(r)

		size := l.Size(p)
		enc, err := l.Encode(nil, p)
		if err != nil {
			// Only the Fixed layout's widths can reject a payload.
			if k != Fixed || errcode.CodeOf(err) != errcode.MessageTooLarge {
				t.Fatalf("Encode: %v", err)
			}
			return
		}
		if len(enc) != size {
			t.Fatalf("Size = %d, encoded %d", size, len(enc))
		}
		prefixed, err := l.Encode([]byte("xy"), p)
		if err != nil || !bytes.Equal(prefixed, append([]byte("xy"), enc...)) {
			t.Fatalf("append form differs (%v)", err)
		}

		var got payload.Payload
		if err := l.Decode(enc, &got, nil); err != nil {
			t.Fatalf("Decode: %v\n%s", err, show(p))
		}
		if !payload.Equal(p, &got) {
			t.Fatalf("round trip differs\n have %s\n want %s", show(&got), show(p))
		}
		again, err := l.Encode(nil, &got)
		if err != nil || !bytes.Equal(again, enc) {
			t.Fatalf("re-encoding is not canonical (%v)", err)
		}
		// Reuse: a second decode into the same destination is identical.
		if err := l.Decode(enc, &got, nil); err != nil || !payload.Equal(p, &got) {
			t.Fatalf("reused destination: %v %s", err, show(&got))
		}
		// Zero copy: equal content, aliasing the envelope.
		var zc payload.Payload
		if err := l.Decode(enc, &zc, &Limits{ZeroCopy: true}); err != nil {
			t.Fatalf("zero-copy Decode: %v", err)
		}
		if !payload.Equal(p, &zc) {
			t.Fatalf("zero-copy differs: %s", show(&zc))
		}
		// Every prefix of the envelope is rejected, and no prefix panics.
		for i := range enc {
			var x payload.Payload
			if err := l.Decode(enc[:i], &x, nil); err == nil {
				t.Fatalf("prefix of %d bytes decoded", i)
			}
		}
		// The other layout rejects this envelope.
		other := New(otherKind(k))
		var x payload.Payload
		if err := other.Decode(enc, &x, nil); err == nil {
			t.Fatalf("%v decoded a %v envelope", other.Kind(), k)
		}
	})
}

// FuzzWireDecode feeds arbitrary bytes to both decoders. Nothing may panic;
// every rejection carries CorruptMessage or MessageTooLarge; a successful
// decode must re-encode to exactly its input (unless it skipped an optional
// field) and must not have allocated more than the envelope's own size
// allows — the bound that makes a hostile length harmless.
func FuzzWireDecode(f *testing.F) {
	seeds := [][]byte{
		nil,
		{},
		[]byte("IQMQ"), []byte("IQMQ\x01\x00"), []byte("IQMQ\x11\x00"),
		[]byte("IQMQ\x01\xff\x00\x00"), bytes.Repeat([]byte{0xff}, 32),
	}
	for _, k := range Kinds() {
		l := New(k)
		for _, p := range []*payload.Payload{
			{},
			{Key: []byte("k"), Body: []byte("b")},
			{Headers: []payload.KV{{Key: "content-type", Value: []byte("application/json")}}, ID: "id"},
			{Props: payload.Props{CreatedAt: time.Unix(1757160000, 1).UTC(), ContentType: "text/plain"}},
		} {
			b, err := l.Encode(nil, p)
			if err != nil {
				continue
			}
			seeds = append(seeds, b)
			if len(b) > 8 {
				trunc := bytes.Clone(b[:len(b)-1])
				bad := bytes.Clone(b)
				bad[6] ^= 0xff
				seeds = append(seeds, trunc, bad, append(bytes.Clone(b), 0))
			}
		}
		for _, env := range hostileEnvelopes(k) {
			seeds = append(seeds, env)
		}
	}
	for _, s := range seeds {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		for _, k := range Kinds() {
			l := New(k)
			var got payload.Payload
			before := heapAlloc()
			skipped, err := decodeSkipped(l, data, &got, nil)
			grew := heapAlloc() - before
			if err != nil {
				switch code := errcode.CodeOf(err); code {
				case errcode.CorruptMessage, errcode.MessageTooLarge:
				default:
					t.Fatalf("%v: code %v for %x", k, code, data)
				}
				continue
			}
			// A successful decode allocates in proportion to its input,
			// never to a declared length: the destination's fields, the
			// header slice and the interned names together stay within a
			// small multiple of the envelope (the constant term covers
			// Go's size classes and the runtime's own bookkeeping).
			if limit := uint64(4096 + 64*len(data)); grew > limit {
				t.Fatalf("%v: decode of %d bytes allocated %d bytes (limit %d)", k, len(data), grew, limit)
			}
			again, err := l.Encode(nil, &got)
			if err != nil {
				t.Fatalf("%v: re-encode failed: %v", k, err)
			}
			if skipped == 0 && !bytes.Equal(again, data) {
				t.Fatalf("%v: decode of %x is not canonical: %x", k, data, again)
			}
			// The re-encoded envelope decodes to an equal payload.
			var round payload.Payload
			if err := l.Decode(again, &round, nil); err != nil {
				t.Fatalf("%v: re-encoded envelope rejected: %v", k, err)
			}
			if !payload.Equal(&got, &round) {
				t.Fatalf("%v: re-encode changed the payload", k)
			}
			// Decoding with tight limits either succeeds or reports
			// MessageTooLarge; it never reports corruption.
			var tight payload.Payload
			lim := Limits{MaxKey: 1, MaxBody: 1, MaxID: 1, MaxHeaders: 1, MaxHeaderKey: 1, MaxHeaderValue: 1, MaxProp: 1}
			if err := l.Decode(data, &tight, &lim); err != nil && errcode.CodeOf(err) != errcode.MessageTooLarge {
				t.Fatalf("%v: tight limits gave %v", k, err)
			}
		}
	})
}

// heapAlloc returns the number of bytes allocated by this goroutine's
// process so far.
func heapAlloc() uint64 {
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	return ms.TotalAlloc
}
