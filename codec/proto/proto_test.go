package proto_test

import (
	"bytes"
	"errors"
	"strings"
	"testing"

	pb "google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"bitbucket.org/iqhive/iqmq/codec"
	"bitbucket.org/iqhive/iqmq/codec/codectest"
	iqproto "bitbucket.org/iqhive/iqmq/codec/proto"
)

// nested returns a ListValue nested depth levels deep. It is the input the
// recursion limit is checked with: two bytes of body buy one level of decoder
// recursion, which is why protobuf-go's own 10,000 default is not a limit this
// codec is willing to inherit.
func nested(depth int) *structpb.ListValue {
	inner := &structpb.ListValue{Values: []*structpb.Value{
		structpb.NewStringValue("leaf"),
	}}
	for range depth {
		inner = &structpb.ListValue{Values: []*structpb.Value{
			structpb.NewListValue(inner),
		}}
	}
	return inner
}

func mustMarshal(m pb.Message) []byte {
	b, err := pb.Marshal(m)
	if err != nil {
		panic(err)
	}
	return b
}

func TestContract(t *testing.T) {
	var nilMsg *wrapperspb.StringValue
	codectest.Suite{
		Codec:       iqproto.New(),
		ContentType: iqproto.ContentType,
		Cases: []codectest.Case{
			{
				Name:      "StringValue",
				Value:     wrapperspb.String("hello"),
				NewTarget: func() any { return new(wrapperspb.StringValue) },
				Check: func(target any) error {
					if got := target.(*wrapperspb.StringValue).GetValue(); got != "hello" {
						return errors.New("got " + got)
					}
					return nil
				},
				// field 1, LEN, 5 bytes, "hello".
				Wire: []byte{0x0a, 0x05, 'h', 'e', 'l', 'l', 'o'},
			},
			{
				Name: "Struct",
				Value: func() *structpb.Struct {
					s, err := structpb.NewStruct(map[string]any{"id": "a", "qty": 2.0})
					if err != nil {
						panic(err)
					}
					return s
				}(),
				NewTarget: func() any { return new(structpb.Struct) },
				Check: func(target any) error {
					s := target.(*structpb.Struct)
					if s.GetFields()["id"].GetStringValue() != "a" || s.GetFields()["qty"].GetNumberValue() != 2 {
						return errors.New("round trip lost a field")
					}
					return nil
				},
			},
			{
				Name:      "empty message",
				Value:     &wrapperspb.StringValue{},
				NewTarget: func() any { return new(wrapperspb.StringValue) },
				Check: func(target any) error {
					if got := target.(*wrapperspb.StringValue).GetValue(); got != "" {
						return errors.New("got " + got)
					}
					return nil
				},
				Wire: []byte{},
			},
		},
		Unencodable: []any{
			make(chan int),
			"a string is not a message",
			nilMsg,
			// Invalid UTF-8 in a string field: the value protobuf's own
			// MarshalAppend returns a partially encoded buffer for.
			wrapperspb.String(string([]byte{0xff, 0xfe})),
		},
		Corrupt: [][]byte{
			{0x0a, 0x05, 'h', 'i'},               // length prefix past the end
			{0x0a, 0xff, 0xff, 0xff, 0xff, 0x7f}, // hostile length prefix
			{0x08},                               // a tag with no value
			{0x0d, 0x01},                         // fixed32 field, one byte
			mustMarshal(nested(200)),             // deeper than the recursion limit
		},
	}.Run(t)
}

// TestBodyOverTheLimitIsRejectedBeforeTheDecoder is the specification's "decoders enforce
// size limits before allocation" for this codec.
//
// The body used is a VALID encoding that decodes cleanly when the limit
// permits it, so what the test observes is the limit rejecting it and not the
// body being bad. A check that could only ever see a malformed body would pass
// with the limit removed.
func TestBodyOverTheLimitIsRejectedBeforeTheDecoder(t *testing.T) {
	big := wrapperspb.String(strings.Repeat("x", 4096))
	body, err := pb.Marshal(big)
	if err != nil {
		t.Fatal(err)
	}

	small := iqproto.NewWithConfig(iqproto.Config{MaxBytes: 1024})
	var got wrapperspb.StringValue
	err = small.Unmarshal(body, &got)
	if !errors.Is(err, iqproto.ErrTooLarge) {
		t.Fatalf("Unmarshal of a %d-byte body at MaxBytes=1024 = %v, want ErrTooLarge", len(body), err)
	}
	// The limit is not a decode failure: a body bigger than this codec will
	// decode is a different fact from a malformed one, and the poison
	// policy wants to tell them apart.
	var de *codec.DecodeError
	if errors.As(err, &de) {
		t.Fatalf("an over-size body is a *codec.DecodeError (%v); it is not corrupt, it is too big", err)
	}
	// Nothing was written into the target, because the decoder never ran.
	if got.GetValue() != "" {
		t.Fatalf("the decoder ran anyway: target holds %d bytes", len(got.GetValue()))
	}

	// The same body at a limit that permits it decodes cleanly. Without this
	// half the test above would pass against a codec that rejected every
	// body.
	roomy := iqproto.NewWithConfig(iqproto.Config{MaxBytes: 8192})
	var ok wrapperspb.StringValue
	if err := roomy.Unmarshal(body, &ok); err != nil {
		t.Fatalf("Unmarshal at MaxBytes=8192: %v", err)
	}
	if len(ok.GetValue()) != 4096 {
		t.Fatalf("round trip lost the body: %d bytes", len(ok.GetValue()))
	}

	// A negative limit is unlimited, for a body the caller produced itself.
	if err := iqproto.NewWithConfig(iqproto.Config{MaxBytes: -1}).Unmarshal(body, &ok); err != nil {
		t.Fatalf("Unmarshal at MaxBytes=-1: %v", err)
	}
}

// TestRecursionLimitIsTighterThanTheLibrarys checks the second half of the specification's
// pre-allocation rule for this format: nesting depth, which an attacker buys
// at two bytes per level.
//
// Both halves matter. The default rejects a 200-deep body; protobuf-go's own
// default ACCEPTS it, which is what makes the tightened limit load-bearing
// rather than decoration.
func TestRecursionLimitIsTighterThanTheLibrarys(t *testing.T) {
	body, err := pb.Marshal(nested(200))
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("a 200-level nesting is %d bytes of body", len(body))

	var got structpb.ListValue
	err = iqproto.New().Unmarshal(body, &got)
	if err == nil {
		t.Fatal("the default configuration decoded a 200-level nesting; DefaultRecursionLimit is not applied")
	}
	var de *codec.DecodeError
	if !errors.As(err, &de) {
		t.Fatalf("a too-deep body = %v, want a *codec.DecodeError", err)
	}

	// protobuf-go's own default would take it, so the limit is ours.
	var lib structpb.ListValue
	if err := pb.Unmarshal(body, &lib); err != nil {
		t.Fatalf("protobuf-go rejected the 200-level body itself (%v), so this test cannot show that the tightened limit does anything", err)
	}

	// A body inside the limit still decodes.
	shallow, err := pb.Marshal(nested(8))
	if err != nil {
		t.Fatal(err)
	}
	var fine structpb.ListValue
	if err := iqproto.New().Unmarshal(shallow, &fine); err != nil {
		t.Fatalf("an 8-level nesting was rejected too: %v", err)
	}
}

// TestAnAbsentMapValueDeepensTheReEncoding pins the one normalisation that
// makes a body this codec ACCEPTS re-encode into a body the same codec
// REFUSES, which is why FuzzProto's round trip re-decodes one level deeper
// than it decoded (issue #51).
//
// Decoding this format is not depth-preserving. A map entry whose value field
// is absent is not decoded as an absent value: protobuf's map decoding
// materialises the value message, and the encoder then writes it out. So the
// re-encoding is one message level deeper than the body it came from, and a
// body sitting exactly AT DefaultRecursionLimit re-encodes one level past it.
//
// The deepening is exactly one level, and this test is what says so: the
// materialised value is an empty structpb.Value, which encodes to no bytes at
// all and so nests nothing further, and an absent map value is always a leaf,
// so the extra level cannot stack. A protobuf-go upgrade that made it more
// than one fails here, rather than FuzzProto rediscovering issue #51 against a
// +1 that had silently become too small.
func TestAnAbsentMapValueDeepensTheReEncoding(t *testing.T) {
	// A Struct carrying a single map entry that has neither a key nor a
	// value: field 1 (fields), LEN, zero bytes.
	body := []byte{0x0a, 0x00}

	var s structpb.Struct
	if err := iqproto.New().Unmarshal(body, &s); err != nil {
		t.Fatalf("Unmarshal(%x): %v", body, err)
	}
	v, ok := s.GetFields()[""]
	if !ok || v == nil {
		t.Fatalf("an absent map value did not materialise: %#v", s.GetFields())
	}
	if empty, err := iqproto.New().Marshal(v); err != nil || len(empty) != 0 {
		t.Fatalf("the materialised value encodes to %x (%v), want no bytes: it would nest further", empty, err)
	}

	out, err := iqproto.New().Marshal(&s)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("%x re-encodes to %x", body, out)

	// need reports the smallest RecursionLimit that decodes b.
	need := func(b []byte) int {
		for lim := 1; lim <= 16; lim++ {
			if err := iqproto.NewWithConfig(iqproto.Config{RecursionLimit: lim}).Unmarshal(b, new(structpb.Struct)); err == nil {
				return lim
			}
		}
		return -1
	}
	in, re := need(body), need(out)
	t.Logf("the body needs RecursionLimit %d; its re-encoding needs %d", in, re)
	if in < 1 || re != in+1 {
		t.Fatalf("the re-encoding needs RecursionLimit %d where the body needed %d; FuzzProto gives "+
			"the round trip DefaultRecursionLimit+1, which is no longer the right headroom", re, in)
	}
}

// TestHostileLengthPrefixIsRejectedByTheLibrary pins the property this
// package note relies on: protobuf-go validates a length prefix against
// the bytes that remain BEFORE it allocates, so a five-byte body claiming a
// four-gigabyte field is a parse error rather than a four-gigabyte allocation.
//
// It is here because the note in the package comment is a claim about a
// dependency. A library upgrade that lost the property would make this codec a
// denial-of-service surface silently; this test is what makes it loud.
func TestHostileLengthPrefixIsRejectedByTheLibrary(t *testing.T) {
	hostile := [][]byte{
		{0x0a, 0xff, 0xff, 0xff, 0xff, 0x7f}, // bytes field, ~34 GiB claimed
		{0x0a, 0x80, 0x80, 0x80, 0x80, 0x20}, // string field, 8 GiB claimed
		{0x0a, 0xff, 0xff, 0xff, 0x7f},       // 256 MiB claimed
	}
	for _, in := range hostile {
		var m wrapperspb.StringValue
		if err := pb.Unmarshal(in, &m); err == nil {
			t.Fatalf("protobuf-go accepted %x, so a length prefix can drive an allocation the input does not justify", in)
		}
		var through wrapperspb.StringValue
		err := iqproto.New().Unmarshal(in, &through)
		var de *codec.DecodeError
		if !errors.As(err, &de) {
			t.Fatalf("Unmarshal(%x) = %v, want a *codec.DecodeError", in, err)
		}
	}
}

// TestAppendFailureLeavesTheCallerBufferAlone is the pinned contract applied
// to the one place a dependency breaks it. codectest checks the contract; this
// test shows that the wrapper is what satisfies it, by measuring the library
// beside it.
func TestAppendFailureLeavesTheCallerBufferAlone(t *testing.T) {
	bad := wrapperspb.String(string([]byte{0xff, 0xfe}))
	dst := append(make([]byte, 0, 64), "keep-me"...)

	raw, rerr := pb.MarshalOptions{Deterministic: true}.MarshalAppend(dst, bad)
	if rerr == nil {
		t.Fatal("protobuf accepted invalid UTF-8, so this test cannot show what the wrapper fixes")
	}
	if string(raw) == "keep-me" {
		t.Fatal("protobuf's MarshalAppend already leaves the caller's buffer alone, so the wrapper's truncation is dead code and this test proves nothing")
	}
	t.Logf("google.golang.org/protobuf MarshalAppend on failure: %q (%v)", raw, rerr)

	got, err := iqproto.New().(codec.Appender).AppendMarshal(dst, bad)
	if err == nil {
		t.Fatal("AppendMarshal accepted invalid UTF-8")
	}
	if string(got) != "keep-me" {
		t.Fatalf("AppendMarshal returned %q, want the caller's buffer %q", got, "keep-me")
	}
}

// TestTypedShapeThroughAdapt is from the caller's side: the
// generic shape over a pointer message type.
func TestTypedShapeThroughAdapt(t *testing.T) {
	c := iqproto.Typed[*wrapperspb.StringValue]()
	if c.ContentType() != iqproto.ContentType {
		t.Fatalf("ContentType = %q", c.ContentType())
	}
	b, err := c.Encode(wrapperspb.String("x"))
	if err != nil {
		t.Fatalf("Encode: %v (codec.Indirect did not resolve the **StringValue the adapter builds)", err)
	}
	v, err := c.Decode(b)
	if err != nil {
		t.Fatalf("Decode: %v (codec.Target did not allocate through the nil *StringValue)", err)
	}
	if v.GetValue() != "x" {
		t.Fatalf("Decode = %q", v.GetValue())
	}
	ac, ok := c.(codec.AppendingTyped[*wrapperspb.StringValue])
	if !ok {
		t.Fatal("Adapt did not carry the append path")
	}
	ab, err := ac.AppendEncode([]byte("P"), wrapperspb.String("x"))
	if err != nil || !bytes.Equal(ab, append([]byte("P"), b...)) {
		t.Fatalf("AppendEncode = %x, %v", ab, err)
	}
}

func TestRegister(t *testing.T) {
	r := codec.NewRegistry()
	if err := iqproto.Register(r); err != nil {
		t.Fatal(err)
	}
	for _, ct := range []string{iqproto.ContentType, iqproto.ContentTypeAlias, "application/vnd.acme.order+protobuf"} {
		c, err := r.For(ct)
		if err != nil {
			t.Fatalf("For(%q): %v", ct, err)
		}
		if c.ContentType() != iqproto.ContentType {
			t.Fatalf("For(%q) = %q", ct, c.ContentType())
		}
	}
	if err := iqproto.Register(r); !errors.Is(err, codec.ErrDuplicateCodec) {
		t.Fatalf("second Register = %v, want ErrDuplicateCodec", err)
	}
}

// TestDiscardUnknownIsOffByDefault pins the retention default: a round trip
// through a binary that does not know a field keeps it, which is the property
// a rolling upgrade depends on.
func TestDiscardUnknownIsOffByDefault(t *testing.T) {
	// A StringValue body with an extra field 15 carrying a varint.
	body := []byte{0x0a, 0x01, 'x', 0x78, 0x2a}
	var keep wrapperspb.StringValue
	if err := iqproto.New().Unmarshal(body, &keep); err != nil {
		t.Fatal(err)
	}
	out, err := iqproto.New().Marshal(&keep)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(out, []byte{0x78, 0x2a}) {
		t.Fatalf("re-encoding dropped the unknown field: %x", out)
	}

	var drop wrapperspb.StringValue
	dc := iqproto.NewWithConfig(iqproto.Config{DiscardUnknown: true})
	if err := dc.Unmarshal(body, &drop); err != nil {
		t.Fatal(err)
	}
	out, err = dc.Marshal(&drop)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(out, []byte{0x78, 0x2a}) {
		t.Fatalf("DiscardUnknown kept the unknown field: %x", out)
	}
}
