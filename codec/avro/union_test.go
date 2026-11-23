package avro_test

import (
	"bytes"
	"encoding"
	"encoding/hex"
	"errors"
	"fmt"
	"math/big"
	"os"
	"os/exec"
	"regexp"
	"runtime/debug"
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
		// A *big.Rat itself is the library's own value there, which the any
		// is emptied of instead (TestAReusedAnyUnderADecimalDecodesTheNextMessage).
		{"a type converted to big.Rat held by an any under a decimal", rc, ratBody, func() any { return &heldAny{P: new(money)} }},
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

// convUnion is a union of a string and a long that the caller decodes itself,
// through hamba/avro's avro.UnionConverter; convField and convList hold one
// under a union with a null.
type (
	convUnion struct {
		S *string
		L *int64
	}
	convField struct {
		U *convUnion `avro:"u"`
	}
	convList struct {
		List []*convUnion `avro:"list"`
	}
)

func (u *convUnion) FromAny(p any) error {
	switch x := p.(type) {
	case string:
		u.S = &x
	case int64:
		u.L = &x
	default:
		return errors.New("convUnion: a branch that is neither a string nor a long")
	}
	return nil
}

// ToAny returns a pointer to the branch's value, which is what hamba/avro's
// encoder takes from it.
func (u *convUnion) ToAny() (any, error) {
	switch {
	case u.S != nil:
		return u.S, nil
	case u.L != nil:
		return u.L, nil
	}
	return nil, errors.New("convUnion: empty")
}

// convString describes a *convUnion for a failure message.
func convString(u *convUnion) string {
	switch {
	case u == nil:
		return "nil"
	case u.S != nil:
		return "the string " + *u.S
	case u.L != nil:
		return "a long"
	}
	return "an empty union"
}

// TestAReusedUnionConverterTakesANull is the reported defect (hunt 9 mx9-1):
// hamba/avro decodes a union into a pointer to an avro.UnionConverter by
// allocating a fresh value for every branch but null, and for a null it
// leaves the pointer as it was. So a target reused for the next message kept
// the previous message's union value where the body said null, with no
// error. Such a pointer is now set to nil before the decode, as a reused list
// is emptied.
func TestAReusedUnionConverterTakesANull(t *testing.T) {
	c := newCodec(t, iqavro.Config{Schema: `{"type":"record","name":"Conv","namespace":"iqmq.test","fields":[
  {"name":"u","type":["null","string","long"]}]}`})
	withString, err := c.Marshal(&convField{U: &convUnion{S: ptr("first")}})
	if err != nil {
		t.Fatal(err)
	}
	withLong, err := c.Marshal(&convField{U: &convUnion{L: ptr(int64(7))}})
	if err != nil {
		t.Fatal(err)
	}
	null, err := c.Marshal(&convField{})
	if err != nil {
		t.Fatal(err)
	}
	t.Run("in a record", func(t *testing.T) {
		var v convField
		if err := c.Unmarshal(withString, &v); err != nil {
			t.Fatal(err)
		}
		if err := c.Unmarshal(null, &v); err != nil || v.U != nil {
			t.Fatalf("a null into a reused target = %v, %s; want nil, what the body says", err, convString(v.U))
		}
		if err := c.Unmarshal(withLong, &v); err != nil || v.U == nil || v.U.L == nil || *v.U.L != 7 || v.U.S != nil {
			t.Fatalf("a long after a null = %v, %s; want the long 7 alone", err, convString(v.U))
		}
	})
	t.Run("in a list", func(t *testing.T) {
		lc := newCodec(t, iqavro.Config{Schema: `{"type":"record","name":"Convs","namespace":"iqmq.test","fields":[
  {"name":"list","type":{"type":"array","items":["null","string","long"]}}]}`})
		full, err := lc.Marshal(&convList{List: []*convUnion{{S: ptr("a")}, {L: ptr(int64(2))}}})
		if err != nil {
			t.Fatal(err)
		}
		nulls, err := lc.Marshal(&convList{List: []*convUnion{nil, nil}})
		if err != nil {
			t.Fatal(err)
		}
		var v convList
		if err := lc.Unmarshal(full, &v); err != nil {
			t.Fatal(err)
		}
		if err := lc.Unmarshal(nulls, &v); err != nil || len(v.List) != 2 || v.List[0] != nil || v.List[1] != nil {
			t.Fatalf("a list of nulls into a reused list = %v, %d elements, the first %s; want two nils", err, len(v.List), convString(v.List[0]))
		}
	})
	t.Run("at the top level", func(t *testing.T) {
		tc := newCodec(t, iqavro.Config{Schema: `["null","string","long"]`})
		s, err := tc.Marshal(&convUnion{S: ptr("first")})
		if err != nil {
			t.Fatal(err)
		}
		var u *convUnion
		if err := tc.Unmarshal(s, &u); err != nil || u == nil || u.S == nil {
			t.Fatalf("Unmarshal = %v, %s; want the string", err, convString(u))
		}
		if err := tc.Unmarshal([]byte{0x00}, &u); err != nil || u != nil {
			t.Fatalf("a null into a reused *convUnion = %v, %s; want nil", err, convString(u))
		}
	})
	t.Run("a refused body leaves the target as it was", func(t *testing.T) {
		kept := &convUnion{S: ptr("kept")}
		v := convField{U: kept}
		// A union index past the branches.
		if err := c.Unmarshal([]byte{0x10}, &v); err == nil || v.U != kept {
			t.Fatalf("Unmarshal of a malformed body = %v, and the union is now %s", err, convString(v.U))
		}
	})
}

// fixedDecimal is a fixed decimal of scale 2.
const fixedDecimal = `{"type":"fixed","name":"Fixed","size":8,"logicalType":"decimal","precision":10,"scale":2}`

// decimalAny holds a decimal field and a list of decimals in interfaces, and
// decimalRat the same as the *big.Rat values they decode to.
type (
	decimalAny struct {
		P    any   `avro:"p"`
		List []any `avro:"list"`
	}
	decimalRat struct {
		P    *big.Rat   `avro:"p"`
		List []*big.Rat `avro:"list"`
	}
)

// TestAReusedAnyUnderADecimalDecodesTheNextMessage is the reported defect
// (hunt 9 mx9-2, Incomplete mx8-2): hamba/avro decodes a decimal into an empty
// any as a *big.Rat of its own, and the next decode into the same target
// decodes into what that pointer points at, as into any pointer an any holds:
// under a bytes decimal by writing a pointer over the big.Rat, under a fixed
// decimal not at all. Hunt 8 refused such a pointer under a bytes decimal, so
// every message after the first was refused, and under a fixed decimal the
// library refused it as a corrupt body. An any under a decimal that holds a
// *big.Rat is now emptied before the decode, so the library builds a fresh
// *big.Rat each time, and the one the any held is not written.
func TestAReusedAnyUnderADecimalDecodesTheNextMessage(t *testing.T) {
	for _, tc := range []struct{ name, p, items string }{
		{"bytes", decimal, decimal},
		{"fixed", fixedDecimal, `"Fixed"`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := newCodec(t, iqavro.Config{Schema: `{"type":"record","name":"Price","namespace":"iqmq.test","fields":[
  {"name":"p","type":` + tc.p + `},{"name":"list","type":{"type":"array","items":` + tc.items + `}}]}`})
			first, err := c.Marshal(&decimalRat{P: big.NewRat(12345, 100), List: []*big.Rat{big.NewRat(1, 1), big.NewRat(2, 1)}})
			if err != nil {
				t.Fatal(err)
			}
			second, err := c.Marshal(&decimalRat{P: big.NewRat(-67890, 100), List: []*big.Rat{big.NewRat(3, 1)}})
			if err != nil {
				t.Fatal(err)
			}
			want := func(t *testing.T, v *decimalAny) {
				t.Helper()
				if p, ok := v.P.(*big.Rat); !ok || p.Cmp(big.NewRat(-67890, 100)) != 0 {
					t.Fatalf("p = %T %v; want the *big.Rat -678.90", v.P, v.P)
				}
				if len(v.List) != 1 {
					t.Fatalf("list = %v; want [3]", v.List)
				}
				if r, ok := v.List[0].(*big.Rat); !ok || r.Cmp(big.NewRat(3, 1)) != 0 {
					t.Fatalf("list = %v; want [3]", v.List)
				}
			}
			t.Run("after a decode into the same target", func(t *testing.T) {
				var v decimalAny
				if err := c.Unmarshal(first, &v); err != nil {
					t.Fatalf("the first decode into a zero target: %v", err)
				}
				held, ok := v.P.(*big.Rat)
				if !ok {
					t.Fatalf("the first decode left p holding %T; want the library's *big.Rat", v.P)
				}
				was := new(big.Rat).Set(held)
				if err := c.Unmarshal(second, &v); err != nil {
					t.Fatalf("the second decode into the reused target: %v", err)
				}
				want(t, &v)
				if held.Cmp(was) != 0 {
					t.Fatalf("the *big.Rat p held before the decode is now %v; want it left at %v", held, was)
				}
			})
			t.Run("a *big.Rat the caller put there", func(t *testing.T) {
				mine := big.NewRat(5, 1)
				v := decimalAny{P: mine, List: []any{(*big.Rat)(nil), big.NewRat(6, 1)}}
				if err := c.Unmarshal(second, &v); err != nil {
					t.Fatal(err)
				}
				want(t, &v)
				if mine.Cmp(big.NewRat(5, 1)) != 0 {
					t.Fatalf("the caller's *big.Rat is now %v; want it left at 5", mine)
				}
			})
		})
	}
}

// mapAny holds a map whose values the library decodes into an any, and
// mapRat the same as the *big.Rat values a decimal decodes to.
type (
	mapAny struct {
		M map[string]any `avro:"m"`
	}
	mapRat struct {
		M map[string]*big.Rat `avro:"m"`
	}
)

// TestAReusedMapDecodesEachValueAfresh is the reported defect (hunt 10 mx10-1
// to mx10-4, a regression from the move to iqhive/avro v2.36.0). That release
// decodes the value of a key a reused map already holds into the value held
// there, where hamba/avro decoded each value into a new one, and this
// package's plan takes every map value to be new, so it checks and prepares
// nothing inside one. So the *big.Rat a fixed decimal left in an any was
// refused as a corrupt body on every later message, the one a bytes decimal
// left was written over, a nested map kept the previous message's entries, an
// any holding a nil pointer to a type with an empty text interface panicked,
// and one holding its own address ended the process with a stack overflow.
// The release after v2.36.0 decodes each map value into a new one again, and
// each case decodes as into a fresh target.
func TestAReusedMapDecodesEachValueAfresh(t *testing.T) {
	if v, before := avroLibraryBefore(t, "v2.36.1"); before {
		t.Skipf("iqhive/avro %s decodes a reused map's values in place; the release after v2.36.0 does not", v)
	}
	for _, tc := range []struct{ name, values string }{
		{"under a fixed decimal", fixedDecimal},
		{"under a bytes decimal", decimal},
	} {
		t.Run("an any "+tc.name, func(t *testing.T) {
			c := newCodec(t, iqavro.Config{Schema: `{"type":"record","name":"Prices","namespace":"iqmq.test","fields":[
  {"name":"m","type":{"type":"map","values":` + tc.values + `}}]}`})
			first, err := c.Marshal(&mapRat{M: map[string]*big.Rat{"a": big.NewRat(12345, 100)}})
			if err != nil {
				t.Fatal(err)
			}
			second, err := c.Marshal(&mapRat{M: map[string]*big.Rat{"a": big.NewRat(-67890, 100)}})
			if err != nil {
				t.Fatal(err)
			}
			var v mapAny
			if err := c.Unmarshal(first, &v); err != nil {
				t.Fatalf("the first decode into a zero target: %v", err)
			}
			held, ok := v.M["a"].(*big.Rat)
			if !ok {
				t.Fatalf("the first decode left m[a] holding %T; want the library's *big.Rat", v.M["a"])
			}
			if err := c.Unmarshal(second, &v); err != nil {
				t.Fatalf("the second decode into the reused target: %v (%v)", err, errcode.CodeOf(err))
			}
			if r, ok := v.M["a"].(*big.Rat); !ok || r.Cmp(big.NewRat(-67890, 100)) != 0 {
				t.Fatalf("m[a] = %T %v; want the *big.Rat -678.90", v.M["a"], v.M["a"])
			}
			if held.Cmp(big.NewRat(12345, 100)) != 0 {
				t.Fatalf("the *big.Rat m[a] held before the decode is now %v; want it left at 123.45", held.FloatString(2))
			}
		})
	}
	t.Run("a nested map", func(t *testing.T) {
		type nested struct {
			M map[string]map[string]string `avro:"m"`
		}
		c := newCodec(t, iqavro.Config{Schema: `{"type":"record","name":"Nested","namespace":"iqmq.test","fields":[
  {"name":"m","type":{"type":"map","values":{"type":"map","values":"string"}}}]}`})
		first, err := c.Marshal(&nested{M: map[string]map[string]string{"a": {"x": "1"}}})
		if err != nil {
			t.Fatal(err)
		}
		second, err := c.Marshal(&nested{M: map[string]map[string]string{"a": {"y": "2"}}})
		if err != nil {
			t.Fatal(err)
		}
		var v nested
		if err := c.Unmarshal(first, &v); err != nil {
			t.Fatal(err)
		}
		if err := c.Unmarshal(second, &v); err != nil {
			t.Fatal(err)
		}
		if got := v.M["a"]; len(got) != 1 || got["y"] != "2" {
			t.Fatalf("the second message names m[a] = {y: 2}, and the reused target holds m[a] = %v", got)
		}
	})
	t.Run("an any holding a nil pointer to a type with an empty text interface", func(t *testing.T) {
		c := newCodec(t, iqavro.Config{Schema: `{"type":"map","values":{"type":"record","name":"MapText","namespace":"iqmq.test",
  "fields":[{"name":"s","type":"string"}]}}`})
		m := map[string]any{"k": (*textHolder)(nil)}
		body := []byte{0x02, 0x02, 'k', 0x04, 'h', 'i', 0x00} // {"k": {"s": "hi"}}
		var err error
		func() {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("Unmarshal panicked: %v", r)
				}
			}()
			err = c.Unmarshal(body, &m)
		}()
		if err != nil {
			t.Fatalf("Unmarshal = %v; a new value takes the record", err)
		}
		if got, ok := m["k"].(map[string]any); !ok || got["s"] != "hi" {
			t.Fatalf("m[k] = %#v; want the record as a map[string]any", m["k"])
		}
	})
	t.Run("an any holding its own address", func(t *testing.T) {
		// A library that decoded into the held value again would overflow the
		// stack, which ends the whole test binary, so this runs in a child
		// with the stack capped for it to come at once.
		inChild(t, func(t *testing.T) {
			debug.SetMaxStack(16 << 20)
			c := newCodec(t, iqavro.Config{Schema: `{"type":"map","values":"long"}`})
			var x any
			x = &x
			m := map[string]any{"k": &x}
			if err := c.Unmarshal([]byte{0x02, 0x02, 'k', 0x0e, 0x00}, &m); err != nil { // {"k": 7}
				t.Fatalf("Unmarshal = %v; a new value takes the long", err)
			}
			if m["k"] != int64(7) {
				t.Fatalf("m[k] = %#v; want int64(7)", m["k"])
			}
		})
	})
}

// inChild runs fn as t in a fresh copy of the test binary, which runs only
// t, and fails t unless that copy passes. A stack overflow ends the process,
// and recover cannot catch it, so a test that would otherwise end the test
// binary runs its body here.
func inChild(t *testing.T, fn func(t *testing.T)) {
	t.Helper()
	const env = "IQMQ_AVRO_TEST_IN_CHILD"
	if os.Getenv(env) == t.Name() {
		fn(t)
		return
	}
	cmd := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^"+regexp.QuoteMeta(t.Name())+"$", "-test.count=1")
	cmd.Env = append(os.Environ(), env+"="+t.Name())
	if out, err := cmd.CombinedOutput(); err != nil {
		if len(out) > 4000 {
			out = out[:4000]
		}
		t.Fatalf("%s failed in a child process (%v):\n%s", t.Name(), err, out)
	}
}

// selfAny holds an any, and selfList a list of them, for
// TestAnInterfaceThatLeadsBackToItselfIsRefused.
type (
	selfAny struct {
		P any `avro:"p"`
	}
	selfList struct {
		List []any `avro:"list"`
	}
)

// TestAnInterfaceThatLeadsBackToItselfIsRefused is the reported defect (hunt
// 9 mx9-3): hamba/avro decodes into what a pointer an any holds points at, so
// an any that holds its own address, directly or through other pointers and
// interfaces, sent it round for ever without reading a byte, and the process
// died of a stack overflow. Such a target is refused with an InvalidArgument
// error, at the top level and inside a record or a list.
func TestAnInterfaceThatLeadsBackToItselfIsRefused(t *testing.T) {
	long := newCodec(t, iqavro.Config{Schema: `"long"`})
	longBody, err := long.Marshal(int64(7))
	if err != nil {
		t.Fatal(err)
	}
	rec := newCodec(t, iqavro.Config{Schema: `{"type":"record","name":"Self","namespace":"iqmq.test","fields":[{"name":"p","type":"long"}]}`})
	recBody, err := rec.Marshal(&struct {
		P int64 `avro:"p"`
	}{P: 7})
	if err != nil {
		t.Fatal(err)
	}
	list := newCodec(t, iqavro.Config{Schema: `{"type":"record","name":"Selves","namespace":"iqmq.test","fields":[
  {"name":"list","type":{"type":"array","items":"long"}}]}`})
	listBody, err := list.Marshal(&struct {
		List []int64 `avro:"list"`
	}{List: []int64{7}})
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name   string
		c      codec.Codec
		body   []byte
		target func() any
	}{
		{"an any holding its own address", long, longBody, func() any { var x any; x = &x; return &x }},
		{"two anys holding each other's address", long, longBody, func() any { var a, b any; a, b = &b, &a; return &a }},
		{"an any holding a pointer to a pointer to it, under a record", rec, recBody, func() any {
			var x any
			p := &x
			x = &p
			return &x
		}},
		{"a field holding its own address", rec, recBody, func() any { v := &selfAny{}; v.P = &v.P; return v }},
		{"a list element holding its own address", list, listBody, func() any {
			v := &selfList{List: make([]any, 1)}
			v.List[0] = &v.List[0]
			return v
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.c.Unmarshal(tc.body, tc.target())
			if errcode.CodeOf(err) != errcode.InvalidArgument {
				t.Fatalf("Unmarshal = %v (%v); want InvalidArgument", err, errcode.CodeOf(err))
			}
			t.Log(err)
		})
	}
	t.Run("a chain that ends decodes", func(t *testing.T) {
		var end any
		var x any = &end
		if err := long.Unmarshal(longBody, &x); err != nil || end != int64(7) {
			t.Fatalf("Unmarshal into an any holding the address of an empty any = %v, %#v; want 7", err, end)
		}
	})
}

// textHolder holds an interface the library hands a string to through its
// UnmarshalText; upperText is a value of a type that implements it with a
// pointer receiver. pointerText (one pointer field) and labelsText (a map
// that fills itself) implement it with a value receiver, and Go stores a value
// of either in an interface's data word, so a zero one leaves that word nil,
// which is what hamba/avro takes for nothing. ptrLabels is labelsText with a
// pointer receiver, which the library hands a pointer.
type (
	textHolder struct {
		S encoding.TextUnmarshaler `avro:"s"`
	}
	upperText   struct{ s string }
	pointerText struct{ p *string }
	labelsText  map[string]string
	ptrLabels   map[string]string
)

func (u *upperText) UnmarshalText(b []byte) error {
	u.s = strings.ToUpper(string(b))
	return nil
}

func (pointerText) UnmarshalText([]byte) error { return nil }

func (l labelsText) UnmarshalText(b []byte) error {
	l["text"] = string(b)
	return nil
}

func (l *ptrLabels) UnmarshalText(b []byte) error {
	*l = ptrLabels{"text": string(b)}
	return nil
}

// TestAnEmptyTextUnmarshalerIsRefused is the reported defect (hunt 9 mx9-4,
// and mx9-4b from review 10): hamba/avro decodes a string or an enum into a
// type with an UnmarshalText method by calling it on what it holds, and when
// what it holds is nothing to it (a nil data word: a nil interface, a nil
// pointer, map or function in one, or the zero value of a pointer-shaped type
// such as a struct of one pointer) and the type is not a pointer, the library
// panics (an assertion on its own type descriptor) instead of returning an
// error. That happens for an interface type with that method, and for a map
// or pointer-shaped type whose UnmarshalText has a value receiver: when the
// field or target is empty, and in every value the library allocates itself
// (behind a nil pointer, in a list, as a map value or key). Each is refused
// with an InvalidArgument error before the library sees anything, a list or
// a map of them and a nil pointer to one whatever the body holds; an
// interface holding a pointer, a map that is not nil and a type whose
// UnmarshalText has a pointer receiver still decode.
func TestAnEmptyTextUnmarshalerIsRefused(t *testing.T) {
	type (
		plainText struct {
			S string `avro:"s"`
		}
		textPtr struct {
			P *textHolder `avro:"p"`
		}
		textList struct {
			List []textHolder `avro:"list"`
		}
		textMap struct {
			M map[string]textHolder `avro:"m"`
		}
		textKeys struct {
			M map[encoding.TextUnmarshaler]string `avro:"m"`
		}
		textEmbedded struct {
			*textHolder
		}
		labelsField struct {
			S labelsText `avro:"s"`
		}
		pointerField struct {
			S pointerText `avro:"s"`
		}
		ptrLabelsField struct {
			S ptrLabels `avro:"s"`
		}
		labelsPtr struct {
			P *labelsField `avro:"p"`
		}
		labelsList struct {
			List []labelsText `avro:"list"`
		}
		pointerKeys struct {
			M map[pointerText]string `avro:"m"`
		}
	)
	txt := `{"type":"record","name":"Txt","fields":[{"name":"s","type":"string"}]}`
	outer := func(fields string) string {
		return `{"type":"record","name":"Outer","namespace":"iqmq.test","fields":[` + fields + `]}`
	}
	encode := func(c codec.Codec, v any) []byte {
		b, err := c.Marshal(v)
		if err != nil {
			t.Fatal(err)
		}
		return b
	}
	field := newCodec(t, iqavro.Config{Schema: outer(`{"name":"s","type":"string"}`)})
	fieldBody := encode(field, &plainText{S: "hello"})
	top := newCodec(t, iqavro.Config{Schema: `"string"`})
	topBody := encode(top, "hello")
	enum := newCodec(t, iqavro.Config{Schema: outer(`{"name":"s","type":{"type":"enum","name":"Word","symbols":["hello"]}}`)})
	enumBody := encode(enum, &plainText{S: "hello"})
	ptrc := newCodec(t, iqavro.Config{Schema: outer(`{"name":"p","type":` + txt + `}`)})
	ptrBody := encode(ptrc, &struct {
		P plainText `avro:"p"`
	}{P: plainText{S: "hello"}})
	nullc := newCodec(t, iqavro.Config{Schema: outer(`{"name":"p","type":["null",` + txt + `]}`)})
	nullBody := encode(nullc, &struct {
		P *plainText `avro:"p"`
	}{})
	listc := newCodec(t, iqavro.Config{Schema: outer(`{"name":"list","type":{"type":"array","items":` + txt + `}}`)})
	listBody := encode(listc, &struct {
		List []plainText `avro:"list"`
	}{List: []plainText{{S: "a"}, {S: "b"}}})
	stringsc := newCodec(t, iqavro.Config{Schema: outer(`{"name":"list","type":{"type":"array","items":"string"}}`)})
	stringsBody := encode(stringsc, &struct {
		List []string `avro:"list"`
	}{List: []string{"a", "b"}})
	mapc := newCodec(t, iqavro.Config{Schema: outer(`{"name":"m","type":{"type":"map","values":` + txt + `}}`)})
	mapBody := encode(mapc, &struct {
		M map[string]plainText `avro:"m"`
	}{M: map[string]plainText{"k": {S: "v"}}})
	keyc := newCodec(t, iqavro.Config{Schema: outer(`{"name":"m","type":{"type":"map","values":"string"}}`)})
	keyBody := encode(keyc, &struct {
		M map[string]string `avro:"m"`
	}{M: map[string]string{"k": "v"}})
	for _, tc := range []struct {
		name   string
		c      codec.Codec
		body   []byte
		target func() any
	}{
		// An interface type with UnmarshalText (mx9-4).
		{"a nil field under a string", field, fieldBody, func() any { return &textHolder{} }},
		{"a field holding a nil pointer", field, fieldBody, func() any { return &textHolder{S: (*upperText)(nil)} }},
		{"a field holding a nil map", field, fieldBody, func() any { return &textHolder{S: labelsText(nil)} }},
		{"a field holding a pointer-shaped zero", field, fieldBody, func() any { return &textHolder{S: pointerText{}} }},
		{"a nil target under a string", top, topBody, func() any { return new(encoding.TextUnmarshaler) }},
		{"a nil field under an enum", enum, enumBody, func() any { return &textHolder{} }},
		{"behind a nil pointer", ptrc, ptrBody, func() any { return &textPtr{} }},
		{"behind a nil pointer under a nullable union, the body null", nullc, nullBody, func() any { return &textPtr{} }},
		{"behind a nil embedded pointer", field, fieldBody, func() any { return &textEmbedded{} }},
		{"in an any holding a nil pointer", ptrc, ptrBody, func() any { return &heldAny{P: (*textHolder)(nil)} }},
		{"in a list past its capacity", listc, listBody, func() any { return &textList{List: []textHolder{{S: &upperText{}}}} }},
		{"in a list the body fits", listc, listBody, func() any {
			return &textList{List: []textHolder{{S: &upperText{}}, {S: &upperText{}}, {S: &upperText{}}, {S: &upperText{}}}}
		}},
		{"in a map value", mapc, mapBody, func() any { return &textMap{} }},
		{"as a map key", keyc, keyBody, func() any { return &textKeys{} }},
		// A map or pointer-shaped type whose UnmarshalText has a value
		// receiver (mx9-4b).
		{"a nil map field under a string", field, fieldBody, func() any { return &labelsField{} }},
		{"a nil map target under a string", top, topBody, func() any { return new(labelsText) }},
		{"a nil map field under an enum", enum, enumBody, func() any { return &labelsField{} }},
		{"a zero pointer-shaped field under a string", field, fieldBody, func() any { return &pointerField{} }},
		{"a map field behind a nil pointer", ptrc, ptrBody, func() any { return &labelsPtr{} }},
		{"in a list of maps", stringsc, stringsBody, func() any { return &labelsList{} }},
		{"a pointer-shaped map key", keyc, keyBody, func() any { return &pointerKeys{} }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			target := tc.target()
			err := unmarshalOrPanic(tc.c, tc.body, target)
			if errcode.CodeOf(err) != errcode.InvalidArgument {
				t.Fatalf("Unmarshal into %T = %v (%v); want InvalidArgument", target, err, errcode.CodeOf(err))
			}
			t.Log(err)
		})
	}
	t.Run("what holds something decodes", func(t *testing.T) {
		u := &upperText{}
		if err := unmarshalOrPanic(field, fieldBody, &textHolder{S: u}); err != nil || u.s != "HELLO" {
			t.Fatalf("Unmarshal into a field holding a *upperText = %v, %q; want HELLO", err, u.s)
		}
		u = &upperText{}
		if err := unmarshalOrPanic(ptrc, ptrBody, &textPtr{P: &textHolder{S: u}}); err != nil || u.s != "HELLO" {
			t.Fatalf("Unmarshal behind a pointer = %v, %q; want HELLO", err, u.s)
		}
		u = &upperText{}
		var held encoding.TextUnmarshaler = u
		if err := unmarshalOrPanic(top, topBody, &held); err != nil || u.s != "HELLO" {
			t.Fatalf("Unmarshal into a target holding a *upperText = %v, %q; want HELLO", err, u.s)
		}
		l := labelsText{}
		if err := unmarshalOrPanic(field, fieldBody, &textHolder{S: l}); err != nil || l["text"] != "hello" {
			t.Fatalf("Unmarshal into a field holding a map = %v, %v; want text:hello", err, l)
		}
		v := labelsField{S: labelsText{}}
		if err := unmarshalOrPanic(field, fieldBody, &v); err != nil || v.S["text"] != "hello" {
			t.Fatalf("Unmarshal into a map field = %v, %v; want text:hello", err, v.S)
		}
		if err := unmarshalOrPanic(field, fieldBody, &pointerField{S: pointerText{p: ptr("set")}}); err != nil {
			t.Fatalf("Unmarshal into a pointer-shaped field that is set = %v", err)
		}
		var p ptrLabelsField
		if err := unmarshalOrPanic(field, fieldBody, &p); err != nil || p.S["text"] != "hello" {
			t.Fatalf("Unmarshal into a nil map whose UnmarshalText has a pointer receiver = %v, %v; want text:hello", err, p.S)
		}
	})
}

// unmarshalOrPanic is c.Unmarshal, with a panic reported as an error.
func unmarshalOrPanic(c codec.Codec, body []byte, target any) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("Unmarshal panicked: %v", r)
		}
	}()
	return c.Unmarshal(body, target)
}
