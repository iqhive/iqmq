package avro_test

import (
	"errors"
	"math/big"
	"strings"
	"testing"
	"time"

	"github.com/iqhive/iqmq/codec"
	iqavro "github.com/iqhive/iqmq/codec/avro"
	"github.com/iqhive/iqmq/internal/errcode"
)

// nullableSchema is a bare nullable record: the schema a Typed[*T] is for.
const nullableSchema = `["null", {"type":"record","name":"Nullable","namespace":"iqmq.test","fields":[
  {"name":"n","type":"long"},{"name":"id","type":"string"}]}]`

type nullable struct {
	N  int64  `avro:"n"`
	ID string `avro:"id"`
}

// optionalSchema holds a nullable record, a list of nullable records, a map of
// them and a nullable timestamp in a record.
const optionalSchema = `{"type":"record","name":"Holder","namespace":"iqmq.test","fields":[
  {"name":"inner","type":["null",{"type":"record","name":"Inner","fields":[{"name":"n","type":"long"},{"name":"id","type":"string"}]}]},
  {"name":"list","type":{"type":"array","items":["null","Inner"]}},
  {"name":"byname","type":{"type":"map","values":["null","Inner"]}},
  {"name":"at","type":["null",{"type":"long","logicalType":"timestamp-millis"}]}
]}`

type inner struct {
	N  int64  `avro:"n"`
	ID string `avro:"id"`
}

type holder struct {
	Inner  *inner            `avro:"inner"`
	List   []*inner          `avro:"list"`
	ByName map[string]*inner `avro:"byname"`
	At     *time.Time        `avro:"at"`
}

// TestATopLevelNullableUnionDecodesIntoAPointer is the reported defect.
// codec.Target allocated through the nil *T that Typed[*T].Decode hands the
// codec, so hamba/avro was given a *T where its nullable-union decoder takes a
// **T: it wrote a freshly allocated *T over the first word of the struct (the
// int64 came back as a heap address and the rest of the record was lost), and
// a null came back as a zero record. Neither returned an error.
func TestATopLevelNullableUnionDecodesIntoAPointer(t *testing.T) {
	tc, err := iqavro.Typed[*nullable](iqavro.Config{Schema: nullableSchema})
	if err != nil {
		t.Fatal(err)
	}
	t.Run("a record", func(t *testing.T) {
		in := &nullable{N: 7, ID: "x"}
		b, err := tc.Encode(in)
		if err != nil {
			t.Fatal(err)
		}
		got, err := tc.Decode(b)
		if err != nil {
			t.Fatalf("Decode(%x): %v", b, err)
		}
		if got == nil || *got != *in {
			t.Fatalf("Decode(Encode(%+v)) = %+v (body %x), want the value back", *in, got, b)
		}
	})
	t.Run("a null", func(t *testing.T) {
		b, err := tc.Encode(nil)
		if err != nil {
			t.Fatal(err)
		}
		got, err := tc.Decode(b)
		if err != nil {
			t.Fatalf("Decode(%x): %v", b, err)
		}
		if got != nil {
			t.Fatalf("Decode(Encode(nil)) = %+v (body %x), want nil: a null became a value", got, b)
		}
	})
	t.Run("into a pointer that holds a value", func(t *testing.T) {
		c := newCodec(t, iqavro.Config{Schema: nullableSchema})
		b, err := c.Marshal(&nullable{N: 9, ID: "new"})
		if err != nil {
			t.Fatal(err)
		}
		p := &nullable{N: 1, ID: "old"}
		if err := c.Unmarshal(b, &p); err != nil {
			t.Fatal(err)
		}
		if p == nil || *p != (nullable{N: 9, ID: "new"}) {
			t.Fatalf("Unmarshal into a *T holding a value = %+v, want {N:9 ID:new}", p)
		}
		null, err := c.Marshal((*nullable)(nil))
		if err != nil {
			t.Fatal(err)
		}
		if err := c.Unmarshal(null, &p); err != nil {
			t.Fatal(err)
		}
		if p != nil {
			t.Fatalf("Unmarshal of a null into a *T holding a value = %+v, want nil", p)
		}
	})
	t.Run("a nullable string", func(t *testing.T) {
		ts, err := iqavro.Typed[*string](iqavro.Config{Schema: `["null","string"]`})
		if err != nil {
			t.Fatal(err)
		}
		s := "x"
		for _, in := range []*string{&s, nil} {
			b, err := ts.Encode(in)
			if err != nil {
				t.Fatal(err)
			}
			got, err := ts.Decode(b)
			if err != nil {
				t.Fatalf("Decode(%x): %v", b, err)
			}
			if (got == nil) != (in == nil) || (got != nil && *got != *in) {
				t.Fatalf("Decode(Encode(%v)) = %v (body %x), want it back", in, got, b)
			}
		}
	})
}

// TestAUnionIsNeverDecodedIntoAStruct is the same defect wherever the
// schema has a union: hamba/avro decodes a union into a struct by running the
// decoder of a POINTER to it on the struct's own address, so it writes a
// pointer over the struct's first word, or decodes into whatever address that
// word already held. A struct there is refused as the caller's error, before
// the library is handed anything, and the struct is left as it was.
func TestAUnionIsNeverDecodedIntoAStruct(t *testing.T) {
	type badInner struct {
		Inner inner `avro:"inner"`
	}
	type badList struct {
		List []inner `avro:"list"`
	}
	type badMap struct {
		ByName map[string]inner `avro:"byname"`
	}
	type badTime struct {
		At time.Time `avro:"at"`
	}
	type embedded struct {
		badInner
	}
	full := holder{
		Inner:  &inner{N: 1, ID: "a"},
		List:   []*inner{{N: 2, ID: "b"}, nil},
		ByName: map[string]*inner{"k": {N: 3, ID: "c"}},
		At:     ptr(time.UnixMilli(1_700_000_000_000).UTC()),
	}
	c := newCodec(t, iqavro.Config{Schema: optionalSchema})
	body, err := c.Marshal(&full)
	if err != nil {
		t.Fatal(err)
	}
	top := newCodec(t, iqavro.Config{Schema: nullableSchema})
	record, err := top.Marshal(&nullable{N: 7, ID: "x"})
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name   string
		c      codec.Codec
		body   []byte
		target func() any
	}{
		{"a struct under a top-level union", top, record, func() any { return &nullable{} }},
		{"a struct field under a union", c, body, func() any { return &badInner{} }},
		{"an embedded struct field under a union", c, body, func() any { return &embedded{} }},
		{"array items under a union", c, body, func() any { return &badList{} }},
		{"map values under a union", c, body, func() any { return &badMap{} }},
		{"a time.Time under a union", c, body, func() any { return &badTime{} }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			target := tc.target()
			err := tc.c.Unmarshal(tc.body, target)
			if err == nil {
				t.Fatalf("Unmarshal into %T = %+v and no error: a struct under a union was decoded", target, target)
			}
			if code := errcode.CodeOf(err); code != errcode.InvalidArgument {
				t.Fatalf("Unmarshal into %T = %v (%v), want InvalidArgument: the target is the caller's error", target, err, code)
			}
			var de *codec.DecodeError
			if errors.As(err, &de) {
				t.Fatalf("Unmarshal into %T = %v, a *codec.DecodeError; the body is not at fault", target, err)
			}
		})
	}
	t.Run("pointers there decode", func(t *testing.T) {
		var got holder
		if err := c.Unmarshal(body, &got); err != nil {
			t.Fatal(err)
		}
		if *got.Inner != *full.Inner || len(got.List) != 2 || *got.List[0] != *full.List[0] || got.List[1] != nil ||
			*got.ByName["k"] != *full.ByName["k"] || !got.At.Equal(*full.At) {
			t.Fatalf("Unmarshal = %+v, want %+v", got, full)
		}
	})
	t.Run("a refused type stays refused", func(t *testing.T) {
		for range 2 {
			if err := c.Unmarshal(body, &badInner{}); errcode.CodeOf(err) != errcode.InvalidArgument {
				t.Fatalf("Unmarshal = %v, want InvalidArgument again", err)
			}
		}
	})
	t.Run("the advice fits the union", func(t *testing.T) {
		if err := c.Unmarshal(body, &badInner{}); !strings.Contains(err.Error(), "use a *avro_test.inner there") {
			t.Fatalf("Unmarshal = %v; under a nullable union the advice is a pointer", err)
		}
		// A union with two non-null branches does not decode into a pointer
		// to a struct either: hamba/avro takes an any, a map[string]any or a
		// UnionConverter there.
		two := newCodec(t, iqavro.Config{Schema: `["string", {"type":"record","name":"Two","fields":[{"name":"n","type":"long"}]}]`})
		err := two.Unmarshal([]byte{0x00, 0x02, 'x'}, &nullable{})
		if errcode.CodeOf(err) != errcode.InvalidArgument || !strings.Contains(err.Error(), "avro.UnionConverter") {
			t.Fatalf("Unmarshal of a two-branch union into a struct = %v; want InvalidArgument naming the shapes that decode", err)
		}
	})
}

func ptr[T any](v T) *T { return &v }

// decimal is a bytes decimal of scale 2.
const decimal = `{"type":"bytes","logicalType":"decimal","precision":10,"scale":2}`

// money is a type the library converts to big.Rat.
type money big.Rat

// TestADecimalDecodesIntoAPointerToABigRat: hamba/avro decodes a bytes decimal
// into a big.Rat VALUE by writing a *big.Rat over the value's first word
// (codec_native.go, bytesDecimalCodec), so 123.45 came back as 0 with no
// error, as a field, as a *big.Rat under a nullable union, and in every Typed
// shape of a decimal at the top level of a schema. A decimal decodes into a
// *big.Rat field and a nullable one into a **big.Rat field; every other shape
// is refused, never decoded to 0.
func TestADecimalDecodesIntoAPointerToABigRat(t *testing.T) {
	want := big.NewRat(12345, 100)
	field := `{"type":"record","name":"Price","namespace":"iqmq.test","fields":[{"name":"p","type":` + decimal + `}]}`
	nullable := `{"type":"record","name":"Price","namespace":"iqmq.test","fields":[{"name":"p","type":["null",` + decimal + `]}]}`
	type ratField struct {
		P *big.Rat `avro:"p"`
	}
	type nullableField struct {
		P **big.Rat `avro:"p"`
	}
	fc := newCodec(t, iqavro.Config{Schema: field})
	fieldBody, err := fc.Marshal(&ratField{P: want})
	if err != nil {
		t.Fatal(err)
	}
	nc := newCodec(t, iqavro.Config{Schema: nullable})
	nullableBody, err := nc.Marshal(&nullableField{P: &want})
	if err != nil {
		t.Fatal(err)
	}
	nullBody, err := nc.Marshal(&nullableField{})
	if err != nil {
		t.Fatal(err)
	}
	top := newCodec(t, iqavro.Config{Schema: decimal})
	topBody, err := top.Marshal(want)
	if err != nil {
		t.Fatal(err)
	}
	topNullable := newCodec(t, iqavro.Config{Schema: `["null",` + decimal + `]`})
	topNullableBody, err := topNullable.Marshal(&want)
	if err != nil {
		t.Fatal(err)
	}

	t.Run("a *big.Rat field", func(t *testing.T) {
		var got ratField
		if err := fc.Unmarshal(fieldBody, &got); err != nil {
			t.Fatal(err)
		}
		if got.P == nil || got.P.Cmp(want) != 0 {
			t.Fatalf("Unmarshal = %v, want %v", got.P, want)
		}
	})
	t.Run("a **big.Rat field under a nullable union", func(t *testing.T) {
		var got nullableField
		if err := nc.Unmarshal(nullableBody, &got); err != nil {
			t.Fatal(err)
		}
		if got.P == nil || *got.P == nil || (*got.P).Cmp(want) != 0 {
			t.Fatalf("Unmarshal = %v, want %v", got.P, want)
		}
		if err := nc.Unmarshal(nullBody, &got); err != nil || got.P != nil {
			t.Fatalf("Unmarshal of a null = %v, %v; want nil", got.P, err)
		}
	})
	type ratValue struct {
		P big.Rat `avro:"p"`
	}
	type moneyValue struct {
		P money `avro:"p"`
	}
	type ratUnderUnion struct {
		P *big.Rat `avro:"p"`
	}
	type ratValueUnderUnion struct {
		P big.Rat `avro:"p"`
	}
	for _, tc := range []struct {
		name string
		c    codec.Codec
		body []byte
		into func() any
	}{
		{"a big.Rat field", fc, fieldBody, func() any { return &ratValue{} }},
		{"a field of a type converted to big.Rat", fc, fieldBody, func() any { return &moneyValue{} }},
		{"a *big.Rat field under a nullable union", nc, nullableBody, func() any { return &ratUnderUnion{} }},
		{"a big.Rat field under a nullable union", nc, nullableBody, func() any { return &ratValueUnderUnion{} }},
		{"Typed[*big.Rat] at the top level", top, topBody, func() any { return new(*big.Rat) }},
		{"Typed[**big.Rat] at the top level", top, topBody, func() any { return new(**big.Rat) }},
		{"Typed[big.Rat] at the top level", top, topBody, func() any { return new(big.Rat) }},
		{"Typed[*big.Rat] under a top-level nullable union", topNullable, topNullableBody, func() any { return new(*big.Rat) }},
		{"Typed[**big.Rat] under a top-level nullable union", topNullable, topNullableBody, func() any { return new(**big.Rat) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			target := tc.into()
			err := tc.c.Unmarshal(tc.body, target)
			if err == nil {
				t.Fatalf("Unmarshal into %T = %+v and no error: a decimal was decoded into a big.Rat value", target, target)
			}
			if code := errcode.CodeOf(err); code != errcode.InvalidArgument {
				t.Fatalf("Unmarshal into %T = %v (%v), want InvalidArgument", target, err, code)
			}
			if !strings.Contains(err.Error(), "**big.Rat") {
				t.Fatalf("Unmarshal into %T = %v; the error does not say what decodes a decimal", target, err)
			}
		})
	}
}
