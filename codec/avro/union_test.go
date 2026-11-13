package avro_test

import (
	"bytes"
	"encoding/hex"
	"errors"
	"math/big"
	"slices"
	"strings"
	"sync"
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

// heldSchema is a record holding a record that holds a nullable record, and
// heldListSchema a list of those middle records.
const (
	heldSchema = `{"type":"record","name":"Outer","namespace":"iqmq.test","fields":[
  {"name":"p","type":{"type":"record","name":"Mid","fields":[
    {"name":"u","type":["null",{"type":"record","name":"Leaf","fields":[{"name":"n","type":"long"},{"name":"id","type":"string"}]}]}]}}]}`
	heldListSchema = `{"type":"record","name":"Outer","namespace":"iqmq.test","fields":[
  {"name":"list","type":{"type":"array","items":{"type":"record","name":"Mid","fields":[
    {"name":"u","type":["null",{"type":"record","name":"Leaf","fields":[{"name":"n","type":"long"},{"name":"id","type":"string"}]}]}]}}}]}`
	heldDeepSchema = `{"type":"record","name":"Outer","namespace":"iqmq.test","fields":[
  {"name":"p","type":{"type":"record","name":"Mid","fields":[
    {"name":"q","type":{"type":"record","name":"Low","fields":[
      {"name":"u","type":["null",{"type":"record","name":"Leaf","fields":[{"name":"n","type":"long"},{"name":"id","type":"string"}]}]}]}}]}}]}`
	heldNodeSchema = `{"type":"record","name":"Node","namespace":"iqmq.test","fields":[
  {"name":"kids","type":{"type":"array","items":"Node"}}]}`
)

// midPtr is the middle record decoded as it should be, midValue with the
// struct where the schema has a union, and midder an interface both
// implement.
type (
	midPtr struct {
		U *inner `avro:"u"`
	}
	midValue struct {
		U inner `avro:"u"`
	}
	midder  interface{ mid() }
	heldAny struct {
		P any `avro:"p"`
	}
	heldIfc struct {
		P midder `avro:"p"`
	}
	heldPtr struct {
		P *any `avro:"p"`
	}
	heldList struct {
		List []any `avro:"list"`
	}
	lowAny struct {
		Q any `avro:"q"`
	}
	heldNode struct {
		Kids []any `avro:"kids"`
	}
)

func (*midPtr) mid()   {}
func (*midValue) mid() {}

// TestAnInterfaceThatHoldsAPointerIsCheckedToo is the reported defect, the
// mx7-1 refusal reached through an interface (Incomplete mx7-1). When an
// interface the library decodes into already holds a pointer, hamba/avro does
// not build a value of its own: it decodes into that pointer with the
// pointer's type (efaceDecoder for an any, the record decoder for an
// interface with methods), and for a nil pointer of a type it decodes into a
// fresh value of that type. The check walked only the static type, so a
// struct under a union inside the pointed-at type was decoded with no error:
// watched, the long came back holding a heap address and the string was lost.
// The value is now walked to every such interface, the type it holds is
// checked like any other, and it is refused before the library sees anything.
func TestAnInterfaceThatHoldsAPointerIsCheckedToo(t *testing.T) {
	type outerMid struct {
		P midValue `avro:"p"`
	}
	leaf := &inner{N: 7, ID: "x"}
	c := newCodec(t, iqavro.Config{Schema: heldSchema})
	body, err := c.Marshal(&struct {
		P midPtr `avro:"p"`
	}{P: midPtr{U: leaf}})
	if err != nil {
		t.Fatal(err)
	}
	lc := newCodec(t, iqavro.Config{Schema: heldListSchema})
	listBody, err := lc.Marshal(&struct {
		List []midPtr `avro:"list"`
	}{List: []midPtr{{U: leaf}}})
	if err != nil {
		t.Fatal(err)
	}
	dc := newCodec(t, iqavro.Config{Schema: heldDeepSchema})
	deepBody, err := dc.Marshal(&struct {
		P struct {
			Q midPtr `avro:"q"`
		} `avro:"p"`
	}{})
	if err != nil {
		t.Fatal(err)
	}
	rc := newCodec(t, iqavro.Config{Schema: `{"type":"record","name":"Price","namespace":"iqmq.test","fields":[{"name":"p","type":` + decimal + `}]}`})
	ratBody, err := rc.Marshal(&struct {
		P *big.Rat `avro:"p"`
	}{P: big.NewRat(12345, 100)})
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name   string
		c      codec.Codec
		body   []byte
		target func() any
	}{
		{"an any holding a pointer", c, body, func() any { return &heldAny{P: &midValue{}} }},
		{"an any holding a nil pointer", c, body, func() any { return &heldAny{P: (*midValue)(nil)} }},
		{"an interface with methods holding a pointer", c, body, func() any { return &heldIfc{P: &midValue{}} }},
		{"a pointer to an any holding a pointer", c, body, func() any { var a any = &midValue{}; return &heldPtr{P: &a} }},
		{"an any at the top level holding a nil pointer", c, body, func() any { var a any = (*outerMid)(nil); return &a }},
		{"an any in a list", lc, listBody, func() any { return &heldList{List: []any{&midValue{}}} }},
		{"an any in a list's spare capacity", lc, listBody, func() any { return &heldList{List: []any{&midPtr{}, &midValue{}}[:1:2]} }},
		{"an any held by an any", dc, deepBody, func() any { return &heldAny{P: &lowAny{Q: &midValue{}}} }},
		{"a big.Rat held by an any under a decimal", rc, ratBody, func() any { return &heldAny{P: new(big.Rat)} }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			target := tc.target()
			err := tc.c.Unmarshal(tc.body, target)
			if err == nil {
				t.Fatalf("Unmarshal into %T = %+v and no error: the library decoded into the pointer an interface held", target, target)
			}
			if code := errcode.CodeOf(err); code != errcode.InvalidArgument || !strings.Contains(err.Error(), "the interface at ") {
				t.Fatalf("Unmarshal into %T = %v (%v), want InvalidArgument naming the interface", target, err, code)
			}
			t.Log(err)
		})
	}
	t.Run("the refused value is left as it was", func(t *testing.T) {
		mid := &midValue{U: inner{N: 1, ID: "old"}}
		if err := c.Unmarshal(body, &heldAny{P: mid}); err == nil || mid.U != (inner{N: 1, ID: "old"}) {
			t.Fatalf("Unmarshal = %v, and the value it held is now %+v", err, mid.U)
		}
	})
	t.Run("pointers there decode", func(t *testing.T) {
		mid := &midPtr{}
		if err := c.Unmarshal(body, &heldAny{P: mid}); err != nil || mid.U == nil || *mid.U != *leaf {
			t.Fatalf("Unmarshal into an any holding a *midPtr = %v, %+v; want %+v", err, mid.U, *leaf)
		}
		got := heldAny{P: (*midPtr)(nil)}
		if err := c.Unmarshal(body, &got); err != nil {
			t.Fatal(err)
		}
		if p, ok := got.P.(*midPtr); !ok || p.U == nil || *p.U != *leaf {
			t.Fatalf("Unmarshal into an any holding a nil *midPtr = %#v; want a *midPtr holding %+v", got.P, *leaf)
		}
		ifc := &midPtr{}
		if err := c.Unmarshal(body, &heldIfc{P: ifc}); err != nil || ifc.U == nil || *ifc.U != *leaf {
			t.Fatalf("Unmarshal into an interface holding a *midPtr = %v, %+v", err, ifc.U)
		}
		list := heldList{List: []any{&midPtr{}}}
		if err := lc.Unmarshal(listBody, &list); err != nil || *list.List[0].(*midPtr).U != *leaf {
			t.Fatalf("Unmarshal into a list holding a *midPtr = %v, %+v", err, list.List[0])
		}
		var empty heldAny
		if err := c.Unmarshal(body, &empty); err != nil {
			t.Fatalf("Unmarshal into an empty any: %v", err)
		}
		if _, ok := empty.P.(map[string]any); !ok {
			t.Fatalf("Unmarshal into an empty any = %#v; want the library's own map", empty.P)
		}
	})
	t.Run("concurrent first decodes", func(t *testing.T) {
		// A fresh codec, so that the answers are worked out while the
		// goroutines race for them. Run it under -race.
		fresh := newCodec(t, iqavro.Config{Schema: heldSchema})
		var wg sync.WaitGroup
		for range 8 {
			wg.Go(func() {
				for range 32 {
					mid := &midPtr{}
					if err := fresh.Unmarshal(body, &heldAny{P: mid}); err != nil || mid.U == nil || *mid.U != *leaf {
						t.Errorf("concurrent Unmarshal = %v, %+v", err, mid.U)
						return
					}
					if err := fresh.Unmarshal(body, &heldAny{P: &midValue{}}); errcode.CodeOf(err) != errcode.InvalidArgument {
						t.Errorf("concurrent Unmarshal of a refused value = %v", err)
						return
					}
				}
			})
		}
		wg.Wait()
	})
	t.Run("a value that holds itself", func(t *testing.T) {
		nc := newCodec(t, iqavro.Config{Schema: heldNodeSchema})
		n := &heldNode{}
		n.Kids = []any{n}
		// A node with no kids: a block count of 0.
		if err := nc.Unmarshal([]byte{0x00}, n); err != nil {
			t.Fatalf("Unmarshal into a value that holds itself: %v", err)
		}
	})
}

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

// TestATopLevelNullableArrayEncodesANilSliceAsNull is the reported defect:
// codec.Adapt hands the codec the address of a Typed[[]T]'s argument, and
// hamba/avro's nullable-union encoder for that pointer takes the non-null
// branch for any pointer that is not nil. So under a top-level ["null",
// {array}] a nil slice was written as an empty array (02 00) and came back as
// [], where the same nil in a record field is the null branch. The package
// comment names a slice as what a nullable array decodes into.
func TestATopLevelNullableArrayEncodesANilSliceAsNull(t *testing.T) {
	for _, tc := range []struct {
		schema, null string
	}{
		{`["null", {"type":"array","items":"long"}]`, "00"},
		{`[{"type":"array","items":"long"}, "null"]`, "02"},
	} {
		c, err := iqavro.Typed[[]int64](iqavro.Config{Schema: tc.schema})
		if err != nil {
			t.Fatal(err)
		}
		for _, in := range [][]int64{nil, {}, {1, 2}} {
			b, err := c.Encode(in)
			if err != nil {
				t.Fatal(err)
			}
			if in == nil && hex.EncodeToString(b) != tc.null {
				t.Fatalf("%s: Encode(nil) = %x, want %s, the null branch", tc.schema, b, tc.null)
			}
			got, err := c.Decode(b)
			if err != nil {
				t.Fatalf("%s: Decode(%x): %v", tc.schema, b, err)
			}
			if (got == nil) != (in == nil) || !slices.Equal(got, in) {
				t.Fatalf("%s: Decode(Encode(%#v)) = %#v (body %x), want it back", tc.schema, in, got, b)
			}
		}
	}
	// The same rule for nullable bytes, and for the pointer shape.
	b, err := iqavro.Typed[[]byte](iqavro.Config{Schema: `["null", "bytes"]`})
	if err != nil {
		t.Fatal(err)
	}
	for _, in := range [][]byte{nil, {}, {7}} {
		body, err := b.Encode(in)
		if err != nil {
			t.Fatal(err)
		}
		got, err := b.Decode(body)
		if err != nil || (got == nil) != (in == nil) || !slices.Equal(got, in) {
			t.Fatalf(`["null","bytes"]: Decode(Encode(%#v)) = %#v, %v (body %x), want it back`, in, got, err, body)
		}
	}
	p, err := iqavro.Typed[*[]int64](iqavro.Config{Schema: `["null", {"type":"array","items":"long"}]`})
	if err != nil {
		t.Fatal(err)
	}
	if body, err := p.Encode(nil); err != nil || hex.EncodeToString(body) != "00" {
		t.Fatalf("Typed[*[]int64].Encode(nil) = %x, %v; want 00", body, err)
	}
	body, err := p.Encode(&[]int64{3})
	if err != nil {
		t.Fatal(err)
	}
	if got, err := p.Decode(body); err != nil || got == nil || !slices.Equal(*got, []int64{3}) {
		t.Fatalf("Typed[*[]int64]: Decode(Encode(&[3])) = %v, %v (body %x), want &[3]", got, err, body)
	}
}

// reuseSchema has a nullable array, nullable bytes, and a list of records that
// hold an array and a nullable array; reuseHolderSchema holds one in a record.
const (
	reuseSchema = `{"type":"record","name":"R","namespace":"iqmq.test","fields":[
  {"name":"l","type":["null",{"type":"array","items":"long"}]},
  {"name":"b","type":["null","bytes"]},
  {"name":"list","type":{"type":"array","items":{"type":"record","name":"Item","fields":[
    {"name":"tags","type":{"type":"array","items":"string"}},
    {"name":"opt","type":["null",{"type":"array","items":"long"}]}]}}}]}`
	reuseHolderSchema = `{"type":"record","name":"H","namespace":"iqmq.test","fields":[{"name":"p","type":` + reuseSchema + `}]}`
)

type (
	reuseItem struct {
		Tags []string `avro:"tags"`
		Opt  []int64  `avro:"opt"`
	}
	reuseRec struct {
		L    []int64     `avro:"l"`
		B    []byte      `avro:"b"`
		List []reuseItem `avro:"list"`
	}
)

// TestAReusedSliceTakesWhatTheBodySays is the reported defect (review 9 N1):
// decoding into a reused target is this codec's zero-allocation path, and
// hamba/avro left a reused slice wrong in two ways. A null under a nullable
// union wrote nil over the slice's data word only, so the slice read as nil
// with its old length: index 0 panicked, and an index past the first page was
// a fatal fault no recover catches, from two messages (a long list, then a
// null). And an empty array left a slice that was not nil as it was, so the
// message read as the previous message's list. A reused slice now holds what
// the body says, and keeps its memory for a list that fits.
func TestAReusedSliceTakesWhatTheBodySays(t *testing.T) {
	c := newCodec(t, iqavro.Config{Schema: reuseSchema})
	full := reuseRec{
		L:    make([]int64, 1024),
		B:    []byte("abc"),
		List: []reuseItem{{Tags: []string{"a", "b"}, Opt: []int64{1, 2}}, {Tags: []string{"c"}, Opt: []int64{3}}},
	}
	fullBody, err := c.Marshal(&full)
	if err != nil {
		t.Fatal(err)
	}
	empty := reuseRec{List: []reuseItem{{}}}
	emptyBody, err := c.Marshal(&empty)
	if err != nil {
		t.Fatal(err)
	}
	check := func(t *testing.T, v *reuseRec) {
		t.Helper()
		if len(v.L) != 0 || cap(v.L) != 0 || len(v.B) != 0 || cap(v.B) != 0 {
			t.Fatalf("after a null: L len %d cap %d, B len %d cap %d; want nil slices", len(v.L), cap(v.L), len(v.B), cap(v.B))
		}
		if len(v.List) != 1 || len(v.List[0].Tags) != 0 || len(v.List[0].Opt) != 0 || cap(v.List[0].Opt) != 0 {
			t.Fatalf("after an empty list and a null in an element: %+v; want one empty element", v.List)
		}
		if got, err := c.Marshal(v); err != nil || !bytes.Equal(got, emptyBody) {
			t.Fatalf("the reused value re-encodes as %x, %v; want %x, what the body said", got, err, emptyBody)
		}
	}
	t.Run("in a record", func(t *testing.T) {
		var v reuseRec
		if err := c.Unmarshal(fullBody, &v); err != nil {
			t.Fatal(err)
		}
		if err := c.Unmarshal(emptyBody, &v); err != nil {
			t.Fatal(err)
		}
		check(t, &v)
	})
	t.Run("through an interface", func(t *testing.T) {
		hc := newCodec(t, iqavro.Config{Schema: reuseHolderSchema})
		held := &reuseRec{}
		if err := hc.Unmarshal(fullBody, &heldAny{P: held}); err != nil {
			t.Fatal(err)
		}
		if err := hc.Unmarshal(emptyBody, &heldAny{P: held}); err != nil {
			t.Fatal(err)
		}
		check(t, held)
	})
	t.Run("a list that fits keeps the memory", func(t *testing.T) {
		var v reuseRec
		if err := c.Unmarshal(fullBody, &v); err != nil {
			t.Fatal(err)
		}
		before, capacity := &v.L[0], cap(v.L)
		short, err := c.Marshal(&reuseRec{L: []int64{7, 8}})
		if err != nil {
			t.Fatal(err)
		}
		if err := c.Unmarshal(short, &v); err != nil {
			t.Fatal(err)
		}
		if !slices.Equal(v.L, []int64{7, 8}) || cap(v.L) != capacity || &v.L[0] != before {
			t.Fatalf("L = %v (cap %d); want [7 8] in the memory it had (cap %d)", v.L, cap(v.L), capacity)
		}
	})
	t.Run("at the top level", func(t *testing.T) {
		nc := newCodec(t, iqavro.Config{Schema: `["null", {"type":"array","items":"long"}]`})
		s := make([]int64, 1024)
		if err := nc.Unmarshal([]byte{0x00}, &s); err != nil || len(s) != 0 || cap(s) != 0 {
			t.Fatalf("a null into a reused []int64: len %d cap %d, %v; want a nil slice", len(s), cap(s), err)
		}
		ac := newCodec(t, iqavro.Config{Schema: `{"type":"array","items":"long"}`})
		s = []int64{4, 5}
		if err := ac.Unmarshal([]byte{0x00}, &s); err != nil || len(s) != 0 {
			t.Fatalf("an empty array into a reused []int64 = %v, %v; want it empty", s, err)
		}
	})
	t.Run("a slice the target reaches twice", func(t *testing.T) {
		// The repair after the decode is skipped when no slice can have
		// capacity when a null meets it. Here the slice is nil before the
		// decode, but an any earlier in the record holds a pointer to it, so
		// the decode gives it capacity as an array and then meets the null.
		type twice struct {
			A any     `avro:"a"`
			L []int64 `avro:"l"`
		}
		tc := newCodec(t, iqavro.Config{Schema: `{"type":"record","name":"Twice","namespace":"iqmq.test","fields":[
  {"name":"a","type":{"type":"array","items":"long"}},{"name":"l","type":["null",{"type":"array","items":"long"}]}]}`})
		body, err := tc.Marshal(&struct {
			A []int64 `avro:"a"`
			L []int64 `avro:"l"`
		}{A: make([]int64, 1024)})
		if err != nil {
			t.Fatal(err)
		}
		var v twice
		v.A = &v.L
		if err := tc.Unmarshal(body, &v); err != nil {
			t.Fatal(err)
		}
		if len(v.L) != 0 || cap(v.L) != 0 {
			t.Fatalf("after an array then a null into the same slice: len %d cap %d; want a nil slice", len(v.L), cap(v.L))
		}
	})
	t.Run("a refused body leaves the target as it was", func(t *testing.T) {
		v := reuseRec{L: []int64{1, 2}}
		if err := c.Unmarshal([]byte{0x02, 0xff}, &v); err == nil || !slices.Equal(v.L, []int64{1, 2}) {
			t.Fatalf("Unmarshal of a malformed body = %v, and L is now %v", err, v.L)
		}
	})
}

// The targets of TestASliceOneDecodeFillsAndNullsIsRepaired: each has a
// nullable []byte that something earlier in the same decode fills.
type (
	fillAny struct {
		P any    `avro:"p"`
		B []byte `avro:"b"`
	}
	fillSelf struct {
		Q *fillSelf `avro:"q"`
		B []byte    `avro:"b"`
	}
	fillOnly struct {
		B []byte `avro:"b"`
	}
	fillText struct {
		P any      `avro:"p"`
		B textFill `avro:"b"`
	}
	// textFill is a []byte the caller decodes from a string itself, into
	// 9,000 bytes.
	textFill []byte
)

func (f *textFill) UnmarshalText([]byte) error {
	*f = make(textFill, 9000)
	return nil
}

// TestASliceOneDecodeFillsAndNullsIsRepaired is the reported defect (review 9
// R3-1): round 3 skipped the repair after the decode unless it saw a pointer or
// interface leading to an array or a nullable slice, but hamba/avro gives a
// slice capacity in other ways too: here plain bytes decoded into a []byte, and
// two schema fields decoded into one Go field because one's alias names the
// other. Each target's nullable []byte is filled with 9,000 bytes and then
// meets a null in the same decode, which left it with no data and a length of
// 9,000 (a fatal fault past the first page). The fourth target is fresh, so
// no rule about the caller's memory could have caught it.
func TestASliceOneDecodeFillsAndNullsIsRepaired(t *testing.T) {
	big := make([]byte, 9000)
	inner := `{"type":"record","name":"Inner","fields":[{"name":"b","type":"bytes"}]}`
	type innerBytes struct {
		B []byte `avro:"b"`
	}
	for _, tc := range []struct {
		name, fields string
		value        any
		target       func() (any, *[]byte)
	}{
		{
			"an any holding the target's own slice", `{"name":"p","type":"bytes"}`,
			&struct {
				P []byte `avro:"p"`
				B []byte `avro:"b"`
			}{P: big},
			func() (any, *[]byte) { v := &fillAny{}; v.P = &v.B; return v, &v.B },
		},
		{
			"a pointer to the target itself", `{"name":"q","type":["null",` + inner + `]}`,
			&struct {
				Q *innerBytes `avro:"q"`
				B []byte      `avro:"b"`
			}{Q: &innerBytes{B: big}},
			func() (any, *[]byte) { v := &fillSelf{}; v.Q = v; return v, &v.B },
		},
		{
			"an any holding the target itself", `{"name":"p","type":` + inner + `}`,
			&struct {
				P innerBytes `avro:"p"`
				B []byte     `avro:"b"`
			}{P: innerBytes{B: big}},
			func() (any, *[]byte) { v := &fillAny{}; v.P = v; return v, &v.B },
		},
		{
			"a field alias naming another field, into a fresh target", `{"name":"x","aliases":["b"],"type":"bytes"}`,
			&struct {
				X []byte `avro:"x"`
				B []byte `avro:"b"`
			}{X: big},
			func() (any, *[]byte) { v := &fillOnly{}; return v, &v.B },
		},
		{
			"an any holding the target's own slice, filled by the caller's UnmarshalText", `{"name":"p","type":"string"}`,
			&struct {
				P string `avro:"p"`
				B []byte `avro:"b"`
			}{P: "x"},
			func() (any, *[]byte) {
				v := &fillText{}
				v.P = &v.B
				return v, (*[]byte)(&v.B)
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			schema := `{"type":"record","name":"Fill","namespace":"iqmq.test","fields":[` + tc.fields +
				`,{"name":"b","type":["null","bytes"]}]}`
			c := newCodec(t, iqavro.Config{Schema: schema})
			// The body: the first field as the value gives it, then a null.
			body, err := c.Marshal(tc.value)
			if err != nil {
				t.Fatal(err)
			}
			target, b := tc.target()
			if err := c.Unmarshal(body, target); err != nil {
				t.Fatal(err)
			}
			if len(*b) != 0 || cap(*b) != 0 {
				t.Fatalf("after the slice was filled and then met a null: nil %v, len %d, cap %d; want a nil slice",
					*b == nil, len(*b), cap(*b))
			}
		})
	}
}

// TestAListALaterBlockOutgrowsIsNotLeftBroken is the reported defect (review 9
// N4-1): hamba/avro grows a list block by block, so when a body's first array
// block fits a reused list and a later one outgrows it, the first block is
// decoded in the old backing array and then copied to a new one. A null there
// broke an element's nullable []byte in the old array; the repair fixed the
// copy, but an application that kept the previous message's list (old :=
// v.Items) read a slice with no data and a length of 9,000, a fatal fault. No
// self-referencing target is needed: the body chooses its blocks.
func TestAListALaterBlockOutgrowsIsNotLeftBroken(t *testing.T) {
	type item struct {
		Raw []byte `avro:"raw"`
	}
	type list struct {
		Items []item `avro:"items"`
	}
	c := newCodec(t, iqavro.Config{Schema: `{"type":"record","name":"O","namespace":"iqmq.test","fields":[{"name":"items",
  "type":{"type":"array","items":{"type":"record","name":"OI","fields":[{"name":"raw","type":["null","bytes"]}]}}}]}`})
	first, err := c.Marshal(&list{Items: []item{{Raw: make([]byte, 9000)}, {Raw: make([]byte, 9000)}}})
	if err != nil {
		t.Fatal(err)
	}
	one, err := c.Marshal(&list{Items: []item{{}}})
	if err != nil {
		t.Fatal(err)
	}
	two, err := c.Marshal(&list{Items: []item{{}, {}}})
	if err != nil {
		t.Fatal(err)
	}
	// Two blocks of nulls, two elements then one: the first block's end
	// marker dropped. The first block fills the kept list.
	body := slices.Concat(two[:len(two)-1], one)
	var v list
	if err := c.Unmarshal(first, &v); err != nil {
		t.Fatal(err)
	}
	old := v.Items
	v.Items = v.Items[:2:2]
	if err := c.Unmarshal(body, &v); err != nil {
		t.Fatalf("Unmarshal(%x): %v", body, err)
	}
	if len(v.Items) != 3 {
		t.Fatalf("decoded %d items, want 3", len(v.Items))
	}
	for i, it := range old {
		if it.Raw == nil && (len(it.Raw) != 0 || cap(it.Raw) != 0) {
			t.Fatalf("the kept list's item %d: no data, len %d cap %d; a read past the first page is a fatal fault",
				i, len(it.Raw), cap(it.Raw))
		}
	}
	for i, it := range v.Items {
		if len(it.Raw) != 0 || cap(it.Raw) != 0 {
			t.Fatalf("item %d after a null: len %d cap %d; want a nil slice", i, len(it.Raw), cap(it.Raw))
		}
	}
}

// deepLeaf holds a nullable array; deep8 and deep9 reach one through eight and
// nine pointers in a row, and selfPointer is a pointer type defined in terms
// of itself.
type (
	deepLeaf struct {
		L []int64 `avro:"l"`
	}
	deep8 struct {
		P ********deepLeaf `avro:"p"`
	}
	deep9 struct {
		P *********deepLeaf `avro:"p"`
	}
	selfPointer *selfPointer
)

// TestAPointerChainPastTheBoundIsRefused is the reported defect (review 9
// R2-1): the check of a target type follows at most eight pointers in a row,
// and hamba/avro any number, so behind nine every check in this file was off.
// A nullable array there was left with no data and its old length after a
// null (the fault of TestAReusedSliceTakesWhatTheBodySays), and a struct
// under a union was not refused. And a pointer type defined in terms of itself
// overflowed the stack inside hamba/avro's decoder construction. Such a type
// is now refused with an InvalidArgument error at the bound.
func TestAPointerChainPastTheBoundIsRefused(t *testing.T) {
	schema := `{"type":"record","name":"Deep","namespace":"iqmq.test","fields":[{"name":"p","type":
  {"type":"record","name":"Leaf","fields":[{"name":"l","type":["null",{"type":"array","items":"long"}]}]}}]}`
	c := newCodec(t, iqavro.Config{Schema: schema})
	type flat struct {
		P deepLeaf `avro:"p"`
	}
	long, err := c.Marshal(&flat{P: deepLeaf{L: make([]int64, 1024)}})
	if err != nil {
		t.Fatal(err)
	}
	null, err := c.Marshal(&flat{})
	if err != nil {
		t.Fatal(err)
	}
	t.Run("eight pointers decode", func(t *testing.T) {
		var v deep8
		if err := c.Unmarshal(long, &v); err != nil {
			t.Fatal(err)
		}
		if err := c.Unmarshal(null, &v); err != nil {
			t.Fatal(err)
		}
		if l := (*******v.P).L; len(l) != 0 || cap(l) != 0 {
			t.Fatalf("after a null behind eight pointers: len %d cap %d; want a nil slice", len(l), cap(l))
		}
	})
	t.Run("nine are refused", func(t *testing.T) {
		var v deep9
		err := c.Unmarshal(long, &v)
		if errcode.CodeOf(err) != errcode.InvalidArgument || !strings.Contains(err.Error(), "more than eight pointers") {
			t.Fatalf("Unmarshal behind nine pointers = %v; want InvalidArgument naming the bound", err)
		}
		if v.P != nil {
			t.Fatal("the refused target was written")
		}
	})
	t.Run("a pointer to itself is refused", func(t *testing.T) {
		var p selfPointer
		err := c.Unmarshal(long, &p)
		if errcode.CodeOf(err) != errcode.InvalidArgument || !strings.Contains(err.Error(), "more than eight pointers") {
			t.Fatalf("Unmarshal into a type P *P = %v; want InvalidArgument naming the bound", err)
		}
	})
}
