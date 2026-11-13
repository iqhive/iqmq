package avro_test

import (
	"bytes"
	"errors"
	"math"
	"testing"

	"github.com/iqhive/iqmq/codec"
	iqavro "github.com/iqhive/iqmq/codec/avro"
	"github.com/iqhive/iqmq/internal/errcode"
)

// fuzzMaxBytes is the byte limit the fuzz arms are configured with.
const fuzzMaxBytes = 1 << 16

// FuzzAvro drives the decoder with arbitrary bytes under eight schemas and
// both framings, and holds it to the properties that matter for an untrusted
// body (the security checks):
//
//  1. nothing panics and nothing runs out of memory — which needs the element
//     and byte limits, because hamba/avro's own are 1<<48 and an array block
//     count is an attacker-chosen varint;
//  2. every rejection is ErrTooLarge or a *codec.DecodeError carrying
//     CorruptMessage, and nothing else;
//  3. a body that decodes re-encodes, and the second round trip is identical
//     to the first — the property hamba/avro's silent zero-filling of a
//     truncated body would otherwise break, because the zeroes re-encode to
//     something LONGER than the body they came from;
//  4. the append path produces exactly what Marshal produces, under both
//     framings, and single-object framing adds exactly ten bytes;
//  5. a body decoded into a target that a long earlier message filled
//     re-encodes as it does from a fresh target, so a null or an empty list
//     leaves no slice of the earlier message behind (review 9 N1).
//
// Property 3 is also what holds the schema walk Unmarshal runs first to the
// codec's own output: a re-encoded body the walk refused is a fatal "cannot
// decode its own output". everySchema is there so that the output covers
// every type the walk reads, the zero-byte ones and a recursive one included.
//
// The specification names FuzzProto and FuzzMsgpack. This target exists because the
// answer to "why not Avro too" is "no reason": Avro's decode path has more
// attacker-chosen lengths in it than either of those.
func FuzzAvro(f *testing.F) {
	seeds := [][]byte{
		{},
		{0x00},
		{0x02},
		{0x02, 'a'},
		{0x04, 'a', 'b'},
		{0x01},
		{0xff},
		{0xff, 0xff, 0xff, 0xff, 0xff, 0x7f},
		{0x80, 0x80, 0x80, 0x01}, // a block count of 1,048,576
		{0x80, 0x80, 0x80, 0x80, 0x80, 0x20},
		{0xc3, 0x01}, // the single-object marker and nothing else
		append([]byte{0xc3, 0x01}, make([]byte, 8)...),
		bytes.Repeat([]byte{0x02, 'a'}, 32),
		bytes.Repeat([]byte{0x01}, 256),
		// The advisories' shapes: a block count of MinInt64, a block of one
		// then one of MaxInt64, nested arrays of zero-byte elements, and a
		// recursive record deeper than the walk allows.
		append(zigzagVarint(math.MinInt64), 0x00, 0x00),
		append(append([]byte{0x02, 0x00}, zigzagVarint(math.MaxInt64)...), 0x00),
		nestedBody(4, 128),
		nestedBody(2, 300),
		append(bytes.Repeat([]byte{0x02}, 10_001), 0x00),
	}
	for _, s := range seeds {
		f.Add(s)
	}
	seedCodec, err := iqavro.New(iqavro.Config{Schema: everySchema})
	if err != nil {
		f.Fatal(err)
	}
	values := everyValues()
	for i := range values {
		b, err := seedCodec.Marshal(&values[i])
		if err != nil {
			f.Fatal(err)
		}
		f.Add(b)
	}
	// Bodies of the two union arms: a record, a null, and a holder with
	// nullable records in a field, a list and a map.
	nullableCodec, err := iqavro.New(iqavro.Config{Schema: nullableSchema})
	if err != nil {
		f.Fatal(err)
	}
	optionalCodec, err := iqavro.New(iqavro.Config{Schema: optionalSchema})
	if err != nil {
		f.Fatal(err)
	}
	for _, v := range []any{&nullable{N: 7, ID: "x"}, (*nullable)(nil)} {
		b, err := nullableCodec.Marshal(v)
		if err != nil {
			f.Fatal(err)
		}
		f.Add(b)
	}
	for _, v := range []holder{{}, {Inner: &inner{N: -1, ID: "a"}, List: []*inner{nil, {N: 2}}, ByName: map[string]*inner{"k": nil}}} {
		b, err := optionalCodec.Marshal(&v)
		if err != nil {
			f.Fatal(err)
		}
		f.Add(b)
	}
	type arm struct {
		name string
		c    codec.Codec
		sobj bool
	}
	// The limits are deliberately TIGHTER than the defaults, because the
	// limit logic is the part the specification is about and a fuzzer reaches a 512-element
	// bound far more often than a 65,536-element one. It also keeps one
	// execution's allocation small, which matters with eight workers on a
	// shared host.
	mk := func(schema string, framing iqavro.Framing) codec.Codec {
		c, err := iqavro.New(iqavro.Config{
			Schema:           schema,
			Framing:          framing,
			MaxBytes:         1 << 16,
			MaxSliceElements: 512,
			MaxStringBytes:   4096,
		})
		if err != nil {
			panic(err)
		}
		return c
	}
	arms := []arm{
		{"order/datum", mk(orderSchema, iqavro.FramingDatum), false},
		{"order/sobj", mk(orderSchema, iqavro.FramingSingleObject), true},
		{"longs/datum", mk(longsSchema, iqavro.FramingDatum), false},
		{"map/datum", mk(mapSchema, iqavro.FramingDatum), false},
		{"every/datum", mk(everySchema, iqavro.FramingDatum), false},
		{"nested/datum", mk(nestedEmptiesSchema, iqavro.FramingDatum), false},
		{"list/datum", mk(listSchema, iqavro.FramingDatum), false},
		// The union arms decode into pointers, as Typed[*T] does. A union
		// decoded into the wrong memory re-encodes to a different body, which
		// property 3 catches.
		{"nullable/datum", mk(nullableSchema, iqavro.FramingDatum), false},
		{"optional/datum", mk(optionalSchema, iqavro.FramingDatum), false},
		{"reuse/datum", mk(reuseSchema, iqavro.FramingDatum), false},
	}
	// Property 5's earlier messages: long lists wherever the arm's schema has
	// one, within the arms' 512-element bound. An arm with a map is left
	// out: the library adds a body's entries to a map it reuses, as
	// encoding/json does.
	tags := make([]string, 300)
	attrs := make([]attr, 300)
	long := make([]int64, 300)
	items := []reuseItem{{Tags: tags[:3], Opt: long[:4]}, {Tags: tags[:2], Opt: long[:5]}}
	earlier := map[string]any{
		"order/datum":  &order{ID: "earlier", Qty: 9, Tags: tags, Attrs: attrs},
		"order/sobj":   &order{ID: "earlier", Qty: 9, Tags: tags, Attrs: attrs},
		"longs/datum":  &long,
		"nested/datum": &[][]struct{}{make([]struct{}, 7), make([]struct{}, 3)},
		"reuse/datum":  &reuseRec{L: long, B: []byte("earlier"), List: items},
	}
	dirty := map[string][]byte{}
	for _, a := range arms {
		if v, ok := earlier[a.name]; ok {
			b, err := a.c.Marshal(v)
			if err != nil {
				f.Fatalf("%s: %v", a.name, err)
			}
			dirty[a.name] = b
			f.Add(b)
		}
	}
	// And what such a message leaves behind when nothing fixes it: nulls and
	// empty lists.
	if b, err := iqavro.New(iqavro.Config{Schema: reuseSchema}); err == nil {
		if body, err := b.Marshal(&reuseRec{List: []reuseItem{{}}}); err == nil {
			f.Add(body)
		}
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		for _, a := range arms {
			target, fresh := targets(a.name)
			if err := a.c.Unmarshal(data, target); err != nil {
				classify(t, a.name, data, err)
				continue
			}
			first, err := a.c.Marshal(target)
			if err != nil {
				t.Fatalf("%s: a decoded body does not re-encode: %v (body %x)", a.name, err, data)
			}
			if a.sobj {
				if len(first) < 10 || first[0] != 0xc3 || first[1] != 0x01 {
					t.Fatalf("%s: single-object output has no marker: %x", a.name, first)
				}
			}
			if body, ok := dirty[a.name]; ok {
				reused := fresh()
				if err := a.c.Unmarshal(body, reused); err != nil {
					t.Fatalf("%s: the earlier message: %v", a.name, err)
				}
				if err := a.c.Unmarshal(data, reused); err != nil {
					t.Fatalf("%s: a body that decodes into a fresh target is refused into a reused one: %v (body %x)", a.name, err, data)
				}
				if got, err := a.c.Marshal(reused); err != nil || !bytes.Equal(got, first) {
					t.Fatalf("%s: decoded into a reused target the body re-encodes as %x, %v; from a fresh one as %x (body %x)",
						a.name, got, err, first, data)
				}
			}
			again := fresh()
			if err := a.c.Unmarshal(first, again); err != nil {
				t.Fatalf("%s: the codec cannot decode its own output %x: %v", a.name, first, err)
			}
			second, err := a.c.Marshal(again)
			if err != nil {
				t.Fatalf("%s: re-encoding the second decode failed: %v", a.name, err)
			}
			// A map field is not a function of the value (see
			// TestMapFieldsAreNotDeterministic), so only the length is
			// compared where the schema has one.
			if a.name == "map/datum" || a.name == "every/datum" || a.name == "optional/datum" {
				if len(second) != len(first) {
					t.Fatalf("%s: the second round trip changed length: %d vs %d", a.name, len(second), len(first))
				}
				continue
			}
			if !bytes.Equal(first, second) {
				t.Fatalf("%s: the second round trip differs: %x vs %x", a.name, second, first)
			}
			ap, err := a.c.(codec.Appender).AppendMarshal([]byte("P"), target)
			if err != nil || !bytes.Equal(ap, append([]byte("P"), first...)) {
				t.Fatalf("%s: AppendMarshal = %x, want %x (%v)", a.name, ap, append([]byte("P"), first...), err)
			}
		}
	})
}

// targets returns a decode target for one arm and a way to make another.
func targets(name string) (any, func() any) {
	switch name {
	case "longs/datum":
		fresh := func() any { return new([]int64) }
		return fresh(), fresh
	case "map/datum":
		fresh := func() any { return new(mapped) }
		return fresh(), fresh
	case "every/datum":
		fresh := func() any { return new(every) }
		return fresh(), fresh
	case "nested/datum":
		fresh := func() any { return new([][]struct{}) }
		return fresh(), fresh
	case "list/datum":
		fresh := func() any { return new(listNode) }
		return fresh(), fresh
	case "nullable/datum":
		// A **nullable, which is what Typed[*nullable] hands the codec.
		fresh := func() any { return new(*nullable) }
		return fresh(), fresh
	case "optional/datum":
		fresh := func() any { return new(holder) }
		return fresh(), fresh
	case "reuse/datum":
		fresh := func() any { return new(reuseRec) }
		return fresh(), fresh
	default:
		fresh := func() any { return new(order) }
		return fresh(), fresh
	}
}

// classify holds a rejection to the error model.
func classify(t *testing.T, arm string, data []byte, err error) {
	t.Helper()
	if err == iqavro.ErrTooLarge { //nolint:errorlint // identity, see reasonIs
		if len(data) <= fuzzMaxBytes {
			t.Fatalf("%s: a %d-byte body was ErrTooLarge at a %d-byte limit", arm, len(data), fuzzMaxBytes)
		}
		return
	}
	var de *codec.DecodeError
	if !errors.As(err, &de) {
		t.Fatalf("%s: %v (%T) is neither ErrTooLarge nor a *codec.DecodeError (body %x)", arm, err, err, data)
	}
	if de.Err == nil || de.ContentType != iqavro.ContentType {
		t.Fatalf("%s: incomplete *codec.DecodeError %+v", arm, de)
	}
	if code := errcode.CodeOf(err); code != errcode.CorruptMessage {
		t.Fatalf("%s: %v carries %v, want CorruptMessage", arm, err, code)
	}
}
