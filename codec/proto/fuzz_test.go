package proto_test

import (
	"bytes"
	"errors"
	"testing"

	pb "google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"bitbucket.org/iqhive/iqmq/codec"
	iqproto "bitbucket.org/iqhive/iqmq/codec/proto"
	"bitbucket.org/iqhive/iqmq/internal/errcode"
)

// FuzzProto drives the decoder with arbitrary bytes against two message
// shapes, and holds it to the four properties that matter for an untrusted
// body (the security checks):
//
//  1. nothing panics, and nothing runs out of memory — the second is why the
//     codec's own size and recursion limits are what the fuzzer exercises
//     rather than the library's defaults;
//  2. every rejection is classified: a *codec.DecodeError carrying
//     CorruptMessage for a malformed body, ErrTooLarge for an over-size one,
//     and nothing else;
//  3. a body that decodes re-encodes, and the second round trip is identical
//     to the first — a decoder that accepted a body it cannot reproduce would
//     silently change a message that passed through. The re-decode is given
//     ONE extra level of recursion, because decoding this format is not
//     depth-preserving: a map entry whose value field is absent decodes to a
//     PRESENT empty message, which the encoder then writes out, so the
//     re-encoding is one message level deeper than the body it came from and a
//     body accepted AT the recursion limit re-encodes one level past it
//     (issue #51). TestAnAbsentMapValueDeepensTheReEncoding pins that the
//     deepening is exactly one level, so this headroom cannot quietly widen
//     into a hole;
//  4. the append path produces exactly what Marshal produces.
func FuzzProto(f *testing.F) {
	seeds := [][]byte{
		{},
		{0x0a, 0x00},
		{0x0a, 0x01, 'x'},
		{0x0a, 0x05, 'h', 'i'},               // length past the end
		{0x0a, 0xff, 0xff, 0xff, 0xff, 0x7f}, // hostile length prefix
		{0x0a, 0x80, 0x80, 0x80, 0x80, 0x20}, // 8 GiB claimed
		{0x08, 0x01},                         // varint in field 1
		{0x08},                               // tag with no value
		{0x0d, 0x01, 0x02, 0x03, 0x04},       // fixed32
		{0x09, 0, 0, 0, 0, 0, 0, 0, 0},       // fixed64
		{0x0b, 0x0c},                         // deprecated group tags
		{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}, // an unterminated varint tag
		{0x0a, 0x02, 0xff, 0xfe},                   // invalid UTF-8 in a string field
		mustMarshal(wrapperspb.String("hello")),
		mustMarshal(nested(4)),
		mustMarshal(nested(64)),
		mustMarshal(nested(200)),
	}
	for _, s := range seeds {
		f.Add(s)
	}
	c := iqproto.New()
	a := c.(codec.Appender)
	// rt re-decodes the round trip. It differs from c in the recursion limit
	// and in nothing else, so the untrusted body is still decoded under every
	// default limit; only the codec's OWN output is read one level deeper, for
	// the reason property 3 gives.
	rt := iqproto.NewWithConfig(iqproto.Config{RecursionLimit: iqproto.DefaultRecursionLimit + 1})
	f.Fuzz(func(t *testing.T, data []byte) {
		for _, shape := range []struct {
			name   string
			target func() pb.Message
		}{
			{"StringValue", func() pb.Message { return new(wrapperspb.StringValue) }},
			{"Struct", func() pb.Message { return new(structpb.Struct) }},
			{"ListValue", func() pb.Message { return new(structpb.ListValue) }},
		} {
			m := shape.target()
			if err := c.Unmarshal(data, m); err != nil {
				classify(t, shape.name, data, err)
				continue
			}
			first, err := c.Marshal(m)
			if err != nil {
				t.Fatalf("%s: a decoded body does not re-encode: %v (body %x)", shape.name, err, data)
			}
			again := shape.target()
			if err := rt.Unmarshal(first, again); err != nil {
				t.Fatalf("%s: the codec cannot decode its own output %x: %v", shape.name, first, err)
			}
			second, err := c.Marshal(again)
			if err != nil || !bytes.Equal(first, second) {
				t.Fatalf("%s: the second round trip differs: %x vs %x (%v)", shape.name, second, first, err)
			}
			ap, err := a.AppendMarshal([]byte("P"), m)
			if err != nil || !bytes.Equal(ap, append([]byte("P"), first...)) {
				t.Fatalf("%s: AppendMarshal = %x, want %x (%v)", shape.name, ap, append([]byte("P"), first...), err)
			}
		}
	})
}

// classify holds a rejection to the error model: an over-size body is
// ErrTooLarge and everything else is a *codec.DecodeError carrying
// CorruptMessage, with the decoder's own error preserved for errors.As.
func classify(t *testing.T, shape string, data []byte, err error) {
	t.Helper()
	if errors.Is(err, iqproto.ErrTooLarge) {
		if len(data) <= iqproto.DefaultMaxBytes {
			t.Fatalf("%s: a %d-byte body was ErrTooLarge at a %d-byte limit", shape, len(data), iqproto.DefaultMaxBytes)
		}
		return
	}
	var de *codec.DecodeError
	if !errors.As(err, &de) {
		t.Fatalf("%s: %v (%T) is neither ErrTooLarge nor a *codec.DecodeError (body %x)", shape, err, err, data)
	}
	if de.Err == nil || de.ContentType != iqproto.ContentType {
		t.Fatalf("%s: incomplete *codec.DecodeError %+v", shape, de)
	}
	if code := errcode.CodeOf(err); code != errcode.CorruptMessage {
		t.Fatalf("%s: %v carries %v, want CorruptMessage", shape, err, code)
	}
}
