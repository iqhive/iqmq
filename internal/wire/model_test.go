package wire

import (
	"bytes"
	"encoding/binary"
	"math/rand/v2"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"bitbucket.org/iqhive/iqmq/internal/errcode"
	"bitbucket.org/iqhive/iqmq/internal/payload"
)

// TestModel checks every layout against the envelope contract: exact Size,
// round trip through an independently built reference encoding, canonical
// re-encoding, destination reuse, limit enforcement, zero-copy aliasing and
// robustness against every single-byte mutation of a valid envelope.
func TestModel(t *testing.T) {
	for _, k := range Kinds() {
		t.Run(k.String(), func(t *testing.T) {
			t.Parallel()
			t.Run("size", func(t *testing.T) { modelSize(t, k) })
			t.Run("roundtrip", func(t *testing.T) { modelRoundTrip(t, k) })
			t.Run("reference", func(t *testing.T) { modelReference(t, k) })
			t.Run("canonical", func(t *testing.T) { modelCanonical(t, k) })
			t.Run("reuse", func(t *testing.T) { modelReuse(t, k) })
			t.Run("limits", func(t *testing.T) { modelLimits(t, k) })
			t.Run("zerocopy", func(t *testing.T) { modelZeroCopy(t, k) })
			t.Run("mutation", func(t *testing.T) { modelMutation(t, k) })
			t.Run("truncation", func(t *testing.T) { modelTruncation(t, k) })
			t.Run("cross-layout", func(t *testing.T) { modelCrossLayout(t, k) })
		})
	}
}

// corpusPayloads returns the payload corpus every model subtest uses: the
// empty payload, every nil/empty combination of key, body and headers, a
// nil header value, unicode and binary content, all five properties, a
// timestamp, many headers, and a few sizes.
func corpusPayloads() []*payload.Payload {
	ts := time.Date(2026, 9, 6, 12, 34, 56, 789012345, time.UTC)
	ps := make([]*payload.Payload, 0, 240)
	ps = append(ps, []*payload.Payload{
		{},
		{Key: []byte{}, Body: []byte{}, Headers: []payload.KV{}},
		{Key: nil, Body: []byte("body only")},
		{Key: []byte("k"), Body: nil},
		{Headers: []payload.KV{{Key: "a", Value: nil}}},
		{Headers: []payload.KV{{Key: "a", Value: []byte{}}}},
		{Headers: []payload.KV{{Key: "", Value: []byte("")}, {Key: "", Value: nil}}},
		{ID: "01J8ZQ2K3M4N5P6Q7R8S9T0V1W"},
		{Props: payload.Props{CreatedAt: ts}},
		{Props: payload.Props{CreatedAt: time.Unix(0, 1).UTC()}},
		{Props: payload.Props{CreatedAt: time.Unix(-62135596800, 0).UTC()}}, // year 1
		{
			Key:  []byte("orders/42"),
			Body: []byte(strings.Repeat("payload ", 128)),
			Headers: []payload.KV{
				{Key: "content-type", Value: []byte("application/json")},
				{Key: "traceparent", Value: []byte("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")},
				{Key: "iqmq-attempts", Value: []byte("3")},
				{Key: "binary", Value: []byte{0, 1, 2, 0xff, 0xfe}},
				{Key: "dup", Value: []byte("1")},
				{Key: "dup", Value: []byte("2")},
			},
			ID: "msg-1",
			Props: payload.Props{
				ContentType:     "application/json",
				ContentEncoding: "gzip",
				CorrelationID:   "corr-9",
				CausationID:     "cause-8",
				ReplyTo:         "orders.reply",
				CreatedAt:       ts,
			},
		},
		{Key: []byte("ünïcödé-ключ"), Body: []byte{0xff, 0xfe, 0x00}, Props: payload.Props{ContentType: "text/plain; charset=utf-8"}},
	}...)
	// Many headers and a range of body sizes.
	big := &payload.Payload{Key: []byte("big"), Body: bytes.Repeat([]byte{7}, 70000)}
	for i := range 300 {
		big.Headers = append(big.Headers, payload.KV{Key: "h-" + strconv.Itoa(i), Value: []byte(strconv.Itoa(i * i))})
	}
	ps = append(ps, big)
	for _, n := range []int{1, 63, 64, 127, 128, 1023, 1024, 65535, 65536} {
		ps = append(ps, &payload.Payload{Key: []byte("k"), Body: bytes.Repeat([]byte("x"), n)})
	}
	// A pseudo-random selection, deterministic across runs.
	r := rand.New(rand.NewPCG(17, 18))
	for range 200 {
		ps = append(ps, randomPayload(r))
	}
	return ps
}

// randomPayload builds a payload with random shape, exercising nil versus
// empty and the full property set.
func randomPayload(r *rand.Rand) *payload.Payload {
	p := &payload.Payload{}
	switch r.IntN(3) {
	case 1:
		p.Key = []byte{}
	case 2:
		p.Key = randomBytes(r, 40)
	}
	switch r.IntN(3) {
	case 1:
		p.Body = []byte{}
	case 2:
		p.Body = randomBytes(r, 2000)
	}
	switch r.IntN(3) {
	case 1:
		p.Headers = []payload.KV{}
	case 2:
		// The count crosses inlineHeaders and the values cross 127 bytes,
		// so the model covers both the inline window and the second pass
		// over a long list, and both the one-byte and the multi-byte
		// length encodings of the TLV layout.
		n := r.IntN(14)
		p.Headers = make([]payload.KV, n)
		for i := range p.Headers {
			p.Headers[i].Key = randomName(r)
			if r.IntN(4) > 0 {
				p.Headers[i].Value = randomBytes(r, 200)
			}
		}
	}
	if r.IntN(2) == 0 {
		p.ID = randomName(r)
	}
	if r.IntN(2) == 0 {
		p.Props.ContentType = randomName(r)
	}
	if r.IntN(3) == 0 {
		p.Props.ContentEncoding = randomName(r)
	}
	if r.IntN(3) == 0 {
		p.Props.CorrelationID = randomName(r)
	}
	if r.IntN(4) == 0 {
		p.Props.CausationID = randomName(r)
	}
	if r.IntN(4) == 0 {
		p.Props.ReplyTo = randomName(r)
	}
	if r.IntN(2) == 0 {
		p.Props.CreatedAt = time.Unix(r.Int64N(4e9), r.Int64N(1e9)).UTC()
	}
	return p
}

func randomBytes(r *rand.Rand, max int) []byte {
	b := make([]byte, r.IntN(max+1))
	for i := range b {
		b[i] = byte(r.Uint32())
	}
	return b
}

func randomName(r *rand.Rand) string {
	const alpha = "abcdefghijklmnopqrstuvwxyz-0123456789"
	n := 1 + r.IntN(20)
	var sb strings.Builder
	for range n {
		sb.WriteByte(alpha[r.IntN(len(alpha))])
	}
	return sb.String()
}

// encode is Encode with the test's error handling.
func encode(t testing.TB, l Layout, p *payload.Payload) []byte {
	t.Helper()
	b, err := l.Encode(nil, p)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	return b
}

func modelSize(t *testing.T, k Kind) {
	l := New(k)
	for i, p := range corpusPayloads() {
		want := l.Size(p)
		b, err := l.Encode(nil, p)
		if err != nil {
			t.Fatalf("payload %d: Encode: %v", i, err)
		}
		if len(b) != want {
			t.Fatalf("payload %d: Size = %d, encoded %d bytes", i, want, len(b))
		}
		// Appending to an existing buffer appends exactly Size bytes and
		// leaves the prefix untouched.
		prefix := []byte("head")
		b2, err := l.Encode(prefix, p)
		if err != nil {
			t.Fatal(err)
		}
		if len(b2) != len(prefix)+want || !bytes.Equal(b2[:4], []byte("head")) || !bytes.Equal(b2[4:], b) {
			t.Fatalf("payload %d: append form differs", i)
		}
	}
}

func modelRoundTrip(t *testing.T, k Kind) {
	l := New(k)
	for i, p := range corpusPayloads() {
		b := encode(t, l, p)
		var got payload.Payload
		if err := l.Decode(b, &got, nil); err != nil {
			t.Fatalf("payload %d: Decode: %v", i, err)
		}
		if !payload.Equal(p, &got) {
			t.Fatalf("payload %d: round trip differs\n have %s\n want %s", i, show(&got), show(p))
		}
		// The decoded payload owns its storage: mutating the envelope must
		// not change it.
		for j := range b {
			b[j] ^= 0xff
		}
		if !payload.Equal(p, &got) {
			t.Fatalf("payload %d: decoded payload aliases the envelope", i)
		}
	}
}

// modelReference compares Encode against an independent, deliberately
// naive encoder written from the layout documentation.
func modelReference(t *testing.T, k Kind) {
	l := New(k)
	for i, p := range corpusPayloads() {
		want, ok := refEncode(k, p)
		if !ok {
			continue
		}
		got := encode(t, l, p)
		if !bytes.Equal(got, want) {
			t.Fatalf("payload %d: encoding differs from reference\n got  %x\n want %x", i, got, want)
		}
	}
}

// modelCanonical: re-encoding a decoded envelope reproduces it byte for byte.
func modelCanonical(t *testing.T, k Kind) {
	l := New(k)
	for i, p := range corpusPayloads() {
		b := encode(t, l, p)
		var got payload.Payload
		if err := l.Decode(b, &got, nil); err != nil {
			t.Fatal(err)
		}
		again, err := l.Encode(nil, &got)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(b, again) {
			t.Fatalf("payload %d: re-encoding is not canonical", i)
		}
	}
}

// modelReuse decodes a stream into one destination payload and checks that
// every message equals a fresh decode: no field survives from the previous
// message.
func modelReuse(t *testing.T, k Kind) {
	l := New(k)
	ps := corpusPayloads()
	var dst payload.Payload
	for i, p := range ps {
		b := encode(t, l, p)
		if err := l.Decode(b, &dst, nil); err != nil {
			t.Fatalf("payload %d: %v", i, err)
		}
		if !payload.Equal(p, &dst) {
			t.Fatalf("payload %d into reused destination: %s, want %s", i, show(&dst), show(p))
		}
	}
	// The reverse order too: shrinking every field.
	slices.Reverse(ps)
	for i, p := range ps {
		b := encode(t, l, p)
		if err := l.Decode(b, &dst, nil); err != nil {
			t.Fatalf("reverse payload %d: %v", i, err)
		}
		if !payload.Equal(p, &dst) {
			t.Fatalf("reverse payload %d: %s, want %s", i, show(&dst), show(p))
		}
	}
}

func modelLimits(t *testing.T, k Kind) {
	l := New(k)
	p := &payload.Payload{
		Key:  bytes.Repeat([]byte("k"), 100),
		Body: bytes.Repeat([]byte("b"), 1000),
		ID:   strings.Repeat("i", 50),
		Headers: []payload.KV{
			{Key: strings.Repeat("h", 40), Value: bytes.Repeat([]byte("v"), 200)},
			{Key: "x", Value: []byte("y")},
		},
		Props: payload.Props{ContentType: strings.Repeat("c", 30)},
	}
	b := encode(t, l, p)
	cases := []struct {
		name string
		lim  Limits
	}{
		{"key", Limits{MaxKey: 99}},
		{"body", Limits{MaxBody: 999}},
		{"id", Limits{MaxID: 49}},
		{"headers", Limits{MaxHeaders: 1}},
		{"header-key", Limits{MaxHeaderKey: 39}},
		{"header-value", Limits{MaxHeaderValue: 199}},
		{"prop", Limits{MaxProp: 29}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var got payload.Payload
			err := l.Decode(b, &got, &c.lim)
			if err == nil {
				t.Fatal("limit not enforced")
			}
			if code := errcode.CodeOf(err); code != errcode.MessageTooLarge {
				t.Fatalf("code %v, want MessageTooLarge", code)
			}
		})
	}
	// The exact limits accept.
	exact := Limits{MaxKey: 100, MaxBody: 1000, MaxID: 50, MaxHeaders: 2, MaxHeaderKey: 40, MaxHeaderValue: 200, MaxProp: 30}
	var got payload.Payload
	if err := l.Decode(b, &got, &exact); err != nil {
		t.Fatalf("exact limits rejected: %v", err)
	}
	if !payload.Equal(p, &got) {
		t.Fatal("exact limits changed the payload")
	}
	// Negative means unbounded.
	none := Limits{MaxKey: -1, MaxBody: -1, MaxID: -1, MaxHeaders: -1, MaxHeaderKey: -1, MaxHeaderValue: -1, MaxProp: -1}
	if err := l.Decode(b, &got, &none); err != nil {
		t.Fatalf("unbounded limits rejected: %v", err)
	}
}

func modelZeroCopy(t *testing.T, k Kind) {
	l := New(k)
	p := &payload.Payload{
		Key:     []byte("key"),
		Body:    []byte("body"),
		Headers: []payload.KV{{Key: "h", Value: []byte("value")}},
	}
	b := encode(t, l, p)
	var got payload.Payload
	if err := l.Decode(b, &got, &Limits{ZeroCopy: true}); err != nil {
		t.Fatal(err)
	}
	if !payload.Equal(p, &got) {
		t.Fatalf("zero-copy decode: %s", show(&got))
	}
	// Every byte slice must point into the envelope.
	for _, s := range [][]byte{got.Key, got.Body, got.Headers[0].Value} {
		if len(s) == 0 {
			continue
		}
		if !aliases(b, s) {
			t.Fatal("zero-copy decode copied a field")
		}
		if cap(s) != len(s) {
			t.Fatal("zero-copy slice exposes capacity beyond its field")
		}
	}
	// Mutating the envelope is visible through the payload (the documented
	// consequence of the mode).
	b[len(b)-1] ^= 0xff
	if payload.Equal(p, &got) {
		t.Fatal("zero-copy payload did not observe the envelope mutation")
	}
}

// aliases reports whether sub points into buf.
func aliases(buf, sub []byte) bool {
	if len(sub) == 0 {
		return true
	}
	for i := range buf {
		if len(buf)-i >= len(sub) && &buf[i] == &sub[0] {
			return true
		}
	}
	return false
}

// modelMutation flips every byte of a valid envelope to every one of a few
// values and requires the decoder either to fail with a portable code or to
// produce a payload that re-encodes to exactly the mutated envelope.
func modelMutation(t *testing.T, k Kind) {
	l := New(k)
	p := &payload.Payload{
		Key:     []byte("orders/42"),
		Body:    []byte("hello wire"),
		Headers: []payload.KV{{Key: "content-type", Value: []byte("text/plain")}, {Key: "n", Value: nil}},
		ID:      "id-7",
		Props:   payload.Props{ContentType: "text/plain", CorrelationID: "c", CreatedAt: time.Unix(1757000000, 5).UTC()},
	}
	base := encode(t, l, p)
	buf := make([]byte, len(base))
	for i := range base {
		for _, v := range []byte{0x00, 0x01, 0x7f, 0x80, 0xff} {
			copy(buf, base)
			if buf[i] == v {
				continue
			}
			buf[i] = v
			checkDecodeInvariants(t, l, buf)
		}
	}
}

// modelTruncation checks every prefix of a valid envelope: none may panic
// and none may decode (the envelope is self-delimiting).
func modelTruncation(t *testing.T, k Kind) {
	l := New(k)
	p := &payload.Payload{Key: []byte("k"), Body: []byte("body"), Headers: []payload.KV{{Key: "h", Value: []byte("v")}}, ID: "i"}
	b := encode(t, l, p)
	for i := range b {
		var got payload.Payload
		if err := l.Decode(b[:i], &got, nil); err == nil {
			t.Fatalf("prefix of %d bytes decoded", i)
		} else if errcode.CodeOf(err) != errcode.CorruptMessage {
			t.Fatalf("prefix of %d bytes: code %v", i, errcode.CodeOf(err))
		}
	}
	// Trailing bytes are rejected too.
	var got payload.Payload
	if err := l.Decode(append(slices.Clone(b), 0), &got, nil); err == nil {
		t.Fatal("trailing byte accepted")
	}
}

// decodeSkipped decodes and reports how many optional (odd-tagged) fields
// the layout skipped; only TLV can skip any.
func decodeSkipped(l Layout, src []byte, p *payload.Payload, lim *Limits) (int, error) {
	if tl, ok := l.(*tlvLayout); ok {
		return tl.decode(src, p, lim)
	}
	return 0, l.Decode(src, p, lim)
}

// checkDecodeInvariants: a decode either fails with CorruptMessage or
// MessageTooLarge, or succeeds and re-encodes to exactly its input —
// unless it skipped an optional field from a later version, which by
// design does not survive a re-encode.
func checkDecodeInvariants(t *testing.T, l Layout, src []byte) {
	t.Helper()
	var got payload.Payload
	skipped, err := decodeSkipped(l, src, &got, nil)
	if err != nil {
		switch code := errcode.CodeOf(err); code {
		case errcode.CorruptMessage, errcode.MessageTooLarge:
		default:
			t.Fatalf("decode of %x: code %v", src, code)
		}
		return
	}
	again, err := l.Encode(nil, &got)
	if err != nil {
		t.Fatalf("re-encode of decoded %x: %v", src, err)
	}
	if skipped > 0 {
		return
	}
	if !bytes.Equal(again, src) {
		t.Fatalf("decode of %x is not canonical: re-encodes to %x (%s)", src, again, show(&got))
	}
}

// modelCrossLayout: every layout carries the same information, so a payload
// encoded by one and decoded, then encoded by the other, survives.
func modelCrossLayout(t *testing.T, k Kind) {
	src := New(k)
	for _, other := range Kinds() {
		dstLayout := New(other)
		for i, p := range corpusPayloads() {
			b := encode(t, src, p)
			var mid payload.Payload
			if err := src.Decode(b, &mid, nil); err != nil {
				t.Fatal(err)
			}
			b2 := encode(t, dstLayout, &mid)
			var end payload.Payload
			if err := dstLayout.Decode(b2, &end, nil); err != nil {
				t.Fatal(err)
			}
			if !payload.Equal(p, &end) {
				t.Fatalf("payload %d through %v then %v: %s", i, k, other, show(&end))
			}
			// The other layout must reject this envelope outright.
			if other != k {
				var x payload.Payload
				if err := dstLayout.Decode(b, &x, nil); err == nil {
					t.Fatalf("%v decoded a %v envelope", other, k)
				}
			}
		}
	}
}

// show renders a payload for failure messages.
func show(p *payload.Payload) string {
	var sb strings.Builder
	sb.WriteString("{Key:")
	sb.WriteString(showBytes(p.Key))
	sb.WriteString(" Body:")
	sb.WriteString(showBytes(p.Body))
	sb.WriteString(" ID:" + strconv.Quote(p.ID))
	if p.Headers == nil {
		sb.WriteString(" Headers:nil")
	} else {
		sb.WriteString(" Headers:[")
		for i := range p.Headers {
			if i > 0 {
				sb.WriteByte(' ')
			}
			sb.WriteString(strconv.Quote(p.Headers[i].Key) + "=" + showBytes(p.Headers[i].Value))
		}
		sb.WriteByte(']')
	}
	sb.WriteString(" Props:" + strconv.Quote(p.Props.ContentType+"|"+p.Props.ContentEncoding+"|"+
		p.Props.CorrelationID+"|"+p.Props.CausationID+"|"+p.Props.ReplyTo) + " At:" + p.Props.CreatedAt.String() + "}")
	return sb.String()
}

func showBytes(b []byte) string {
	if b == nil {
		return "nil"
	}
	return strconv.Quote(string(b))
}

// refEncode is an independent encoder written from the layout comments. It
// reports false for payloads the layout cannot represent.
func refEncode(k Kind, p *payload.Payload) ([]byte, bool) {
	var flags byte
	if p.Key != nil {
		flags |= 1
	}
	if p.Body != nil {
		flags |= 2
	}
	if p.Headers != nil {
		flags |= 4
	}
	if !p.Props.CreatedAt.IsZero() {
		flags |= 8
	}
	out := append([]byte("IQMQ"), 0, flags)
	pr := &p.Props
	tsBytes := func() []byte {
		b := binary.LittleEndian.AppendUint64(nil, uint64(pr.CreatedAt.Unix()))
		return binary.LittleEndian.AppendUint32(b, uint32(pr.CreatedAt.Nanosecond()))
	}
	switch k {
	case Fixed:
		if len(p.Headers) > 0xffff {
			return nil, false
		}
		out[4] = 0x11
		out = binary.LittleEndian.AppendUint16(out, uint16(len(p.Headers)))
		out = binary.LittleEndian.AppendUint32(out, uint32(len(p.Key)))
		out = binary.LittleEndian.AppendUint32(out, uint32(len(p.ID)))
		for _, s := range []string{pr.ContentType, pr.ContentEncoding, pr.CorrelationID, pr.CausationID, pr.ReplyTo} {
			if len(s) > 0xffff {
				return nil, false
			}
			out = binary.LittleEndian.AppendUint16(out, uint16(len(s)))
		}
		if pr.CreatedAt.IsZero() {
			out = append(out, make([]byte, 12)...)
		} else {
			out = append(out, tsBytes()...)
		}
		out = binary.LittleEndian.AppendUint32(out, uint32(len(p.Body)))
		out = append(out, p.Key...)
		out = append(out, p.ID...)
		for _, s := range []string{pr.ContentType, pr.ContentEncoding, pr.CorrelationID, pr.CausationID, pr.ReplyTo} {
			out = append(out, s...)
		}
		for i := range p.Headers {
			h := &p.Headers[i]
			if len(h.Key) > 0xffff {
				return nil, false
			}
			var hf byte
			if h.Value != nil {
				hf = 1
			}
			out = append(out, hf)
			out = binary.LittleEndian.AppendUint16(out, uint16(len(h.Key)))
			out = binary.LittleEndian.AppendUint32(out, uint32(len(h.Value)))
			out = append(out, h.Key...)
			out = append(out, h.Value...)
		}
		out = append(out, p.Body...)
		return out, true
	case TLV:
		out[4] = 0x01
		var meta []byte
		field := func(tag int, val []byte) {
			meta = refVarint(meta, uint64(tag))
			meta = refVarint(meta, uint64(len(val)))
			meta = append(meta, val...)
		}
		if p.Key != nil {
			field(tagKey, p.Key)
		}
		if p.ID != "" {
			field(tagID, []byte(p.ID))
		}
		for i := range p.Headers {
			h := &p.Headers[i]
			var hf byte
			if h.Value != nil {
				hf = 1
			}
			inner := append([]byte{hf}, refVarint(nil, uint64(len(h.Key)))...)
			inner = append(inner, h.Key...)
			inner = append(inner, h.Value...)
			field(tagHeader, inner)
		}
		for tag, s := range map[int]string{
			tagContentType: pr.ContentType, tagContentEncoding: pr.ContentEncoding,
			tagCorrelationID: pr.CorrelationID, tagCausationID: pr.CausationID, tagReplyTo: pr.ReplyTo,
		} {
			_ = tag
			_ = s
		}
		for _, e := range []struct {
			tag int
			s   string
		}{
			{tagContentType, pr.ContentType},
			{tagContentEncoding, pr.ContentEncoding},
			{tagCorrelationID, pr.CorrelationID},
			{tagCausationID, pr.CausationID},
			{tagReplyTo, pr.ReplyTo},
		} {
			if e.s != "" {
				field(e.tag, []byte(e.s))
			}
		}
		if !pr.CreatedAt.IsZero() {
			field(tagCreatedAt, tsBytes())
		}
		out = refVarint(out, uint64(len(meta)))
		out = refVarint(out, uint64(len(p.Body)))
		out = append(out, meta...)
		out = append(out, p.Body...)
		return out, true
	default:
		return nil, false
	}
}

func refVarint(dst []byte, v uint64) []byte {
	for {
		b := byte(v & 0x7f)
		v >>= 7
		if v != 0 {
			dst = append(dst, b|0x80)
			continue
		}
		return append(dst, b)
	}
}
