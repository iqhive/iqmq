package wire

import (
	"bytes"
	"encoding/binary"
	"errors"
	"strings"
	"testing"
	"time"

	"bitbucket.org/iqhive/iqmq/internal/errcode"
	"bitbucket.org/iqhive/iqmq/internal/payload"
	"bitbucket.org/iqhive/iqmq/internal/testguard"
)

// TestKindsList announces the family's kinds to benchmarks/cmd/bakeoff.
func TestKindsList(t *testing.T) {
	parts := make([]string, 0, len(Kinds()))
	for _, k := range Kinds() {
		parts = append(parts, k.String()+"="+k.Ident())
	}
	t.Logf("kinds: %s", strings.Join(parts, ","))
}

func TestKindProperties(t *testing.T) {
	if len(Kinds()) != 2 {
		t.Fatalf("Kinds() = %v", Kinds())
	}
	seen := map[string]bool{}
	for _, k := range Kinds() {
		if !k.Valid() || k == Auto || k.Resolve() != k || k.Ident() == "" {
			t.Fatalf("%v: basic properties", k)
		}
		if seen[k.String()] {
			t.Fatalf("duplicate slug %q", k)
		}
		seen[k.String()] = true
		if p, ok := ParseKind(k.String()); !ok || p != k {
			t.Fatalf("ParseKind(%q) = %v %v", k, p, ok)
		}
		if New(k).Kind() != k {
			t.Fatalf("New(%v).Kind() = %v", k, New(k).Kind())
		}
	}
	if Auto.Resolve() != AutoKind || !AutoKind.Valid() || AutoKind == Auto || Auto.String() != "auto" {
		t.Fatal("Auto binding")
	}
	if New(Auto).Kind() != AutoKind {
		t.Fatal("New(Auto) does not resolve")
	}
	if Fixed.ForwardCompatible() || !TLV.ForwardCompatible() {
		t.Fatal("ForwardCompatible")
	}
	if Kind(9).Valid() || Kind(9).Ident() != "" || Kind(9).String() != "kind(?)" {
		t.Fatal("invalid kind")
	}
	if _, ok := ParseKind("x"); ok {
		t.Fatal("ParseKind junk")
	}
	func() {
		defer func() {
			if recover() == nil {
				t.Fatal("New(invalid) should panic")
			}
		}()
		New(Kind(9))
	}()
}

func TestAutoBindingMatchesADR(t *testing.T) {
	slug, bound, err := testguard.ADRAutoBinding("../..", "wire")
	if err != nil {
		t.Fatal(err)
	}
	if !bound {
		t.Logf("no ADR binds wire yet; AutoKind=%v (provisional=%v)", AutoKind, AutoProvisional)
		return
	}
	if slug != AutoKind.String() {
		t.Fatalf("ADR binds wire to %q but AutoKind is %q", slug, AutoKind)
	}
}

// TestDetect reads the layout out of an envelope prefix without decoding.
func TestDetect(t *testing.T) {
	p := &payload.Payload{Key: []byte("k"), Body: []byte("b")}
	for _, k := range Kinds() {
		b := encode(t, New(k), p)
		if got, ok := Detect(b); !ok || got != k {
			t.Fatalf("Detect(%v envelope) = %v %v", k, got, ok)
		}
		if _, ok := Detect(b[:5]); ok {
			t.Fatal("Detect accepted a truncated prefix")
		}
	}
	for _, bad := range [][]byte{nil, []byte("IQM"), []byte("XQMQ\x01\x00"), []byte("IQMQ\x99\x00")} {
		if _, ok := Detect(bad); ok {
			t.Fatalf("Detect(%q) accepted", bad)
		}
	}
}

var allocSink int

// TestAllocs pins Encode into a caller-owned buffer and Decode into a
// reused destination at zero allocations, for both layouts and the payload
// shapes of the budget (64 B, 1 KiB and 64 KiB bodies with 0 and 4
// headers).
func TestAllocs(t *testing.T) {
	for _, k := range Kinds() {
		t.Run(k.String(), func(t *testing.T) {
			l := New(k)
			for _, size := range benchSizes {
				for _, nh := range []int{0, 4} {
					p := benchPayload(size.bytes, nh)
					buf := make([]byte, 0, l.Size(p)+64)
					var dst payload.Payload
					// Warm the destination's storage and the intern table.
					enc, err := l.Encode(buf, p)
					if err != nil {
						t.Fatal(err)
					}
					if err := l.Decode(enc, &dst, nil); err != nil {
						t.Fatal(err)
					}
					if a := testing.AllocsPerRun(100, func() {
						out, err := l.Encode(buf, p)
						if err != nil {
							t.Fatal(err)
						}
						allocSink += len(out)
					}); a != 0 {
						t.Fatalf("Encode %s headers=%d: %.1f allocs/op", size.name, nh, a)
					}
					if a := testing.AllocsPerRun(100, func() {
						if err := l.Decode(enc, &dst, nil); err != nil {
							t.Fatal(err)
						}
						allocSink += len(dst.Body)
					}); a != 0 {
						t.Fatalf("Decode %s headers=%d: %.1f allocs/op", size.name, nh, a)
					}
					if a := testing.AllocsPerRun(100, func() { allocSink += l.Size(p) }); a != 0 {
						t.Fatalf("Size %s headers=%d: %.1f allocs/op", size.name, nh, a)
					}
				}
			}
		})
	}
}

// TestRejectionAllocsBounded: a hostile envelope that declares gigantic
// lengths is rejected without allocating anything at all.
func TestRejectionAllocsBounded(t *testing.T) {
	for _, k := range Kinds() {
		t.Run(k.String(), func(t *testing.T) {
			l := New(k)
			var dst payload.Payload
			for name, env := range hostileEnvelopes(k) {
				if a := testing.AllocsPerRun(50, func() {
					if err := l.Decode(env, &dst, nil); err == nil {
						t.Fatalf("%s was accepted", name)
					}
				}); a != 0 {
					t.Fatalf("%s: rejection cost %.1f allocs/op", name, a)
				}
			}
		})
	}
}

// hostileEnvelopes returns envelopes of kind k that declare enormous
// lengths in a handful of bytes (the classic decompression-bomb shape).
func hostileEnvelopes(k Kind) map[string][]byte {
	out := map[string][]byte{}
	switch k {
	case Fixed:
		base := func() []byte { return append([]byte("IQMQ\x11\x07"), make([]byte, fixedHeaderLen-prefixLen)...) }
		b := base()
		binary.LittleEndian.PutUint32(b[8:], 0xffffffff) // key length
		out["fixed/huge-key"] = b
		b = base()
		binary.LittleEndian.PutUint32(b[38:], 0xffffffff) // body length
		out["fixed/huge-body"] = b
		b = base()
		binary.LittleEndian.PutUint16(b[6:], 0xffff) // header flood
		out["fixed/header-flood"] = b
		b = base()
		binary.LittleEndian.PutUint32(b[12:], 0xffffffff) // id length
		out["fixed/huge-id"] = b
	case TLV:
		// metadata length 2^60, body length 2^60.
		out["tlv/huge-meta"] = append([]byte("IQMQ\x01\x07"), 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x02, 0x00)
		out["tlv/huge-body"] = append([]byte("IQMQ\x01\x07"), 0x00, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x02)
		// One header field claiming 2^35 bytes.
		out["tlv/huge-header"] = append([]byte("IQMQ\x01\x07"), 0x0a, 0x00, 0x06, 0x80, 0x80, 0x80, 0x80, 0x02, 0x00, 0x00, 0x00, 0x00)
		// Ten continuation bytes: an overflowing varint.
		out["tlv/varint-overflow"] = append([]byte("IQMQ\x01\x07"),
			0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x00)
	default: // no hostile envelopes for a kind that does not exist
	}
	return out
}

// TestSecurity is the row-33 security set for the wire envelope: the
// hostile shapes a broker or a peer can put on the network. Every one is
// rejected with a portable code, none panics, and no error message repeats
// attacker-controlled bytes (so a credential-like string in a header cannot
// reach a log through an error).
func TestSecurity(t *testing.T) {
	secret := "AKIAIOSFODNN7EXAMPLE/wJalrXUtnFEMI"
	for _, k := range Kinds() {
		t.Run(k.String(), func(t *testing.T) {
			l := New(k)
			var dst payload.Payload
			for name, env := range securityCases(t, k, secret) {
				t.Run(name, func(t *testing.T) {
					err := l.Decode(env, &dst, nil)
					if err == nil {
						t.Fatal("accepted")
					}
					switch code := errcode.CodeOf(err); code {
					case errcode.CorruptMessage, errcode.MessageTooLarge:
					default:
						t.Fatalf("code %v", code)
					}
					if strings.Contains(err.Error(), secret) {
						t.Fatalf("error message repeats attacker content: %q", err)
					}
					if !errors.Is(err, errcode.ErrMessageTooLarge) && errcode.CodeOf(err) != errcode.CorruptMessage {
						t.Fatalf("error %v is neither corrupt nor too large", err)
					}
				})
			}
		})
	}
}

// securityCases builds the hostile envelope set for one layout.
func securityCases(t *testing.T, k Kind, secret string) map[string][]byte {
	t.Helper()
	l := New(k)
	valid := encode(t, l, &payload.Payload{
		Key:     []byte("k"),
		Body:    []byte("body"),
		Headers: []payload.KV{{Key: "authorization", Value: []byte(secret)}},
		ID:      "id",
		Props:   payload.Props{ContentType: "text/plain", CreatedAt: time.Unix(1, 2).UTC()},
	})
	cases := map[string][]byte{
		"empty":            {},
		"magic":            []byte("XQMQ\x01\x00\x00\x00"),
		"prefix-only":      valid[:prefixLen],
		"truncated-half":   valid[:len(valid)/2],
		"trailing":         append(bytes.Clone(valid), 0x00),
		"reserved-flags":   flipByte(valid, 5, 0xf0),
		"wrong-version":    flipByte(valid, 4, 0x42),
		"other-layout":     encode(t, New(otherKind(k)), &payload.Payload{Body: []byte(secret)}),
		"all-ff":           bytes.Repeat([]byte{0xff}, 64),
		"prefix-plus-ff":   append([]byte("IQMQ"), bytes.Repeat([]byte{0xff}, 60)...),
		"invalid-utf8-id":  invalidUTF8ID(t, k),
		"header-flood":     headerFlood(t, k),
		"bad-timestamp":    badTimestamp(t, k),
		"zero-len-payload": nil,
	}
	for name, env := range hostileEnvelopes(k) {
		cases[name] = env
	}
	if k == TLV {
		cases["tlv/duplicate-key"] = append([]byte("IQMQ\x01\x01"), 0x06, 0x00, 0x02, 0x01, 'a', 0x02, 0x01, 'b')
		cases["tlv/unknown-critical"] = append([]byte("IQMQ\x01\x00"), 0x03, 0x00, 0x28, 0x01, 'x')
		cases["tlv/overlong-varint"] = append([]byte("IQMQ\x01\x00"), 0x80, 0x00, 0x00)
		cases["tlv/empty-string-field"] = append([]byte("IQMQ\x01\x00"), 0x02, 0x00, 0x08, 0x00)
		cases["tlv/header-without-flag"] = append([]byte("IQMQ\x01\x00"), 0x05, 0x00, 0x06, 0x03, 0x01, 0x01, 'a')
	}
	return cases
}

func otherKind(k Kind) Kind {
	if k == Fixed {
		return TLV
	}
	return Fixed
}

func flipByte(b []byte, i int, v byte) []byte {
	out := bytes.Clone(b)
	out[i] = v
	return out
}

// invalidUTF8ID builds a valid envelope and corrupts the ID's bytes.
func invalidUTF8ID(t *testing.T, k Kind) []byte {
	t.Helper()
	b := encode(t, New(k), &payload.Payload{ID: "abc"})
	i := bytes.Index(b, []byte("abc"))
	if i < 0 {
		t.Fatal("ID not found in envelope")
	}
	out := bytes.Clone(b)
	copy(out[i:], []byte{0xff, 0xfe, 0x80})
	return out
}

// headerFlood declares more headers than DefaultLimits.MaxHeaders in a
// short envelope.
func headerFlood(t *testing.T, k Kind) []byte {
	t.Helper()
	p := &payload.Payload{Headers: make([]payload.KV, DefaultLimits.MaxHeaders+1)}
	for i := range p.Headers {
		p.Headers[i] = payload.KV{Key: "h", Value: []byte{}}
	}
	return encode(t, New(k), p)
}

// badTimestamp sets the nanosecond field out of range.
func badTimestamp(t *testing.T, k Kind) []byte {
	t.Helper()
	b := encode(t, New(k), &payload.Payload{Props: payload.Props{CreatedAt: time.Unix(1, 2).UTC()}})
	out := bytes.Clone(b)
	// The nanosecond word is the last four bytes of the timestamp field,
	// which is the last metadata field of both layouts for this payload.
	i := bytes.LastIndex(out, []byte{0x02, 0x00, 0x00, 0x00})
	if i < 0 {
		t.Fatal("timestamp not found")
	}
	binary.LittleEndian.PutUint32(out[i:], 1e9)
	return out
}

// TestForwardCompatibility: a TLV decoder skips optional (odd-tagged)
// fields written by a later minor version and rejects unknown critical
// (even-tagged) ones; the Fixed layout accepts neither.
func TestForwardCompatibility(t *testing.T) {
	l := New(TLV)
	p := &payload.Payload{Key: []byte("k"), Body: []byte("body"), ID: "id"}
	base := encode(t, l, p)
	extended := withExtraTLVField(t, base, 7, []byte("from v2"))
	var got payload.Payload
	skipped, err := l.(*tlvLayout).decode(extended, &got, nil)
	if err != nil {
		t.Fatalf("optional field rejected: %v", err)
	}
	if skipped != 1 {
		t.Fatalf("skipped %d optional fields, want 1", skipped)
	}
	if !payload.Equal(p, &got) {
		t.Fatalf("payload changed: %s", show(&got))
	}
	critical := withExtraTLVField(t, base, 40, []byte("from v2"))
	if err := l.Decode(critical, &got, nil); !errors.Is(err, ErrUnknownField) {
		t.Fatalf("unknown critical field: %v, want ErrUnknownField", err)
	}
}

// withExtraTLVField inserts a field at the end of the metadata section.
func withExtraTLVField(t *testing.T, env []byte, tag int, val []byte) []byte {
	t.Helper()
	pos := prefixLen
	metaLen, n := uvarint(env[pos:])
	pos += n
	bodyLen, n := uvarint(env[pos:])
	metaStart := pos + n
	field := appendUvarint(nil, uint64(tag))
	field = appendUvarint(field, uint64(len(val)))
	field = append(field, val...)
	out := append([]byte{}, env[:prefixLen]...)
	out = appendUvarint(out, metaLen+uint64(len(field)))
	out = appendUvarint(out, bodyLen)
	out = append(out, env[metaStart:metaStart+int(metaLen)]...)
	out = append(out, field...)
	return append(out, env[metaStart+int(metaLen):]...)
}

// TestEncodeUnencodable: the Fixed layout refuses payloads whose fields do
// not fit its widths, with MessageTooLarge, and reports Size 0 for them.
func TestEncodeUnencodable(t *testing.T) {
	l := New(Fixed)
	big := &payload.Payload{Props: payload.Props{ContentType: strings.Repeat("c", 1<<16)}}
	if n := l.Size(big); n != 0 {
		t.Fatalf("Size = %d, want 0", n)
	}
	if _, err := l.Encode(nil, big); !errors.Is(err, ErrUnencodable) || errcode.CodeOf(err) != errcode.MessageTooLarge {
		t.Fatalf("Encode = %v", err)
	}
	many := &payload.Payload{Headers: make([]payload.KV, 1<<16)}
	if _, err := l.Encode(nil, many); !errors.Is(err, ErrUnencodable) {
		t.Fatalf("Encode(65536 headers) = %v", err)
	}
	// TLV has no such width limit.
	if _, err := New(TLV).Encode(nil, big); err != nil {
		t.Fatalf("TLV Encode: %v", err)
	}
}

// TestInterner checks the bounded intern table: equal content yields equal
// strings, a hit does not allocate, long strings bypass the table, and
// collisions never return the wrong content.
func TestInterner(t *testing.T) {
	var in interner
	if got := in.get(nil); got != "" {
		t.Fatalf("get(nil) = %q", got)
	}
	names := []string{"content-type", "traceparent", "iqmq-id", strings.Repeat("x", internMaxLen+1)}
	for _, n := range names {
		if got := in.get([]byte(n)); got != n {
			t.Fatalf("get(%q) = %q", n, got)
		}
	}
	hot := []byte("content-type")
	if a := testing.AllocsPerRun(100, func() { allocSink += len(in.get(hot)) }); a != 0 {
		t.Fatalf("intern hit: %.1f allocs/op", a)
	}
	// Every distinct byte string must come back exactly, whatever the
	// table does with its slots.
	for i := range 4096 {
		s := "key-" + string(rune('a'+i%26)) + strings.Repeat("z", i%40)
		if got := in.get([]byte(s)); got != s {
			t.Fatalf("get(%q) = %q", s, got)
		}
	}
}

// TestStdlibOnly keeps the envelope inside the layering rules.
func TestStdlibOnly(t *testing.T) {
	testguard.StdlibOnly(t, ".")
	testguard.NoRootImport(t, ".")
}
