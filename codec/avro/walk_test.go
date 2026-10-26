package avro_test

import (
	"bytes"
	"errors"
	"math"
	"math/big"
	"reflect"
	"runtime"
	"strconv"
	"testing"
	"time"

	hamba "github.com/hamba/avro/v2"

	"github.com/iqhive/iqmq/codec"
	iqavro "github.com/iqhive/iqmq/codec/avro"
	"github.com/iqhive/iqmq/internal/errcode"
)

// nestedNullsSchema and nestedEmptiesSchema are the two shapes whose elements
// cost no bytes at all, so a per-array element limit multiplies with nesting.
const (
	nestedNullsSchema   = `{"type":"array","items":{"type":"array","items":"null"}}`
	nestedEmptiesSchema = `{"type":"array","items":{"type":"array","items":{"type":"record","name":"Empty","fields":[]}}}`
)

// listSchema is a recursive record: a linked list, one record per node.
const listSchema = `{
  "type": "record", "name": "Node", "namespace": "iqmq.test",
  "fields": [{"name": "next", "type": ["null", "Node"]}]
}`

type listNode struct {
	Next *listNode `avro:"next"`
}

// everySchema holds every type hamba/avro supports, a named type used again by
// reference, the zero-width types, a recursive type, and logical types held in
// plain Go types (logicalSchema has their own). It is what the walk is held to
// accept: whatever Marshal writes under it.
const everySchema = `{
  "type": "record", "name": "Every", "namespace": "iqmq.test",
  "fields": [
    {"name": "n",   "type": "null"},
    {"name": "b",   "type": "boolean"},
    {"name": "i",   "type": "int"},
    {"name": "l",   "type": "long"},
    {"name": "f",   "type": "float"},
    {"name": "d",   "type": "double"},
    {"name": "by",  "type": "bytes"},
    {"name": "s",   "type": "string"},
    {"name": "e",   "type": {"type": "enum", "name": "Colour", "symbols": ["RED", "GREEN", "BLUE"]}},
    {"name": "fx",  "type": {"type": "fixed", "name": "Four", "size": 4}},
    {"name": "fx0", "type": {"type": "fixed", "name": "None", "size": 0}},
    {"name": "empty", "type": {"type": "record", "name": "Empty", "fields": []}},
    {"name": "unit",  "type": {"type": "record", "name": "Unit", "fields": [
      {"name": "a", "type": "null"}, {"name": "b", "type": "Empty"}, {"name": "c", "type": "None"}
    ]}},
    {"name": "longs",   "type": {"type": "array", "items": "long"}},
    {"name": "nulls",   "type": {"type": "array", "items": "null"}},
    {"name": "units",   "type": {"type": "array", "items": "Unit"}},
    {"name": "fours",   "type": {"type": "array", "items": "Four"}},
    {"name": "grid",    "type": {"type": "array", "items": {"type": "array", "items": "int"}}},
    {"name": "colours", "type": {"type": "map", "values": "Colour"}},
    {"name": "opt",     "type": ["null", "long"]},
    {"name": "pick",    "type": ["null", "string", "long"]},
    {"name": "maybe",   "type": ["null", "Four"]},
    {"name": "date", "type": {"type": "int", "logicalType": "date"}},
    {"name": "tms",  "type": {"type": "int", "logicalType": "time-millis"}},
    {"name": "tsu",  "type": {"type": "long", "logicalType": "timestamp-micros"}},
    {"name": "dec",  "type": {"type": "bytes", "logicalType": "decimal", "precision": 9, "scale": 2}},
    {"name": "fdec", "type": {"type": "fixed", "name": "Dec8", "size": 8, "logicalType": "decimal", "precision": 18, "scale": 3}},
    {"name": "uuid", "type": {"type": "string", "logicalType": "uuid"}},
    {"name": "dur",  "type": {"type": "fixed", "name": "Dur", "size": 12, "logicalType": "duration"}},
    {"name": "tree", "type": {"type": "record", "name": "Tree", "fields": [
      {"name": "v", "type": "int"},
      {"name": "kids", "type": {"type": "array", "items": "Tree"}}
    ]}}
  ]
}`

type unit struct {
	A struct{} `avro:"a"`
	B struct{} `avro:"b"`
	C [0]byte  `avro:"c"`
}

type tree struct {
	V    int32  `avro:"v"`
	Kids []tree `avro:"kids"`
}

// every is everySchema with plain Go types, so that a decoded value
// re-encodes byte for byte: it is the fuzz arm's target.
type every struct {
	N       struct{}          `avro:"n"`
	B       bool              `avro:"b"`
	I       int32             `avro:"i"`
	L       int64             `avro:"l"`
	F       float32           `avro:"f"`
	D       float64           `avro:"d"`
	By      []byte            `avro:"by"`
	S       string            `avro:"s"`
	E       string            `avro:"e"`
	Fx      [4]byte           `avro:"fx"`
	Fx0     [0]byte           `avro:"fx0"`
	Empty   struct{}          `avro:"empty"`
	Unit    unit              `avro:"unit"`
	Longs   []int64           `avro:"longs"`
	Nulls   []struct{}        `avro:"nulls"`
	Units   []unit            `avro:"units"`
	Fours   [][4]byte         `avro:"fours"`
	Grid    [][]int32         `avro:"grid"`
	Colours map[string]string `avro:"colours"`
	Opt     *int64            `avro:"opt"`
	Pick    any               `avro:"pick"`
	Maybe   *[4]byte          `avro:"maybe"`
	Date    int32             `avro:"date"`
	TMs     int32             `avro:"tms"`
	TSu     int64             `avro:"tsu"`
	Dec     []byte            `avro:"dec"`
	FDec    [8]byte           `avro:"fdec"`
	UUID    string            `avro:"uuid"`
	Dur     [12]byte          `avro:"dur"`
	Tree    tree              `avro:"tree"`
}

// logicalSchema is every logical type hamba/avro supports, written through
// the logical types' own Go types, so that what their encoders write is
// walked too.
const logicalSchema = `{
  "type": "record", "name": "Logical", "namespace": "iqmq.test",
  "fields": [
    {"name": "date", "type": {"type": "int", "logicalType": "date"}},
    {"name": "tms",  "type": {"type": "int", "logicalType": "time-millis"}},
    {"name": "tmu",  "type": {"type": "long", "logicalType": "time-micros"}},
    {"name": "tsm",  "type": {"type": "long", "logicalType": "timestamp-millis"}},
    {"name": "tsu",  "type": {"type": "long", "logicalType": "timestamp-micros"}},
    {"name": "dec",  "type": {"type": "bytes", "logicalType": "decimal", "precision": 9, "scale": 2}},
    {"name": "fdec", "type": {"type": "fixed", "name": "Dec8", "size": 8, "logicalType": "decimal", "precision": 18, "scale": 3}},
    {"name": "uuid", "type": {"type": "string", "logicalType": "uuid"}},
    {"name": "dur",  "type": {"type": "fixed", "name": "Dur", "size": 12, "logicalType": "duration"}},
    {"name": "when", "type": ["null", {"type": "long", "logicalType": "timestamp-millis"}]}
  ]
}`

type logical struct {
	Date      time.Time             `avro:"date"`
	Clock     time.Duration         `avro:"tms"`
	ClockFine time.Duration         `avro:"tmu"`
	TSm       time.Time             `avro:"tsm"`
	TSu       time.Time             `avro:"tsu"`
	Dec       *big.Rat              `avro:"dec"`
	FDec      *big.Rat              `avro:"fdec"`
	UUID      string                `avro:"uuid"`
	Dur       hamba.LogicalDuration `avro:"dur"`
	When      *time.Time            `avro:"when"`
}

// nestedBody is an array of outer arrays, each declaring inner elements in one
// block: a block count and a terminator per inner array, and nothing else.
func nestedBody(outer, inner int) []byte {
	b := zigzagVarint(int64(outer))
	for range outer {
		b = append(b, zigzagVarint(int64(inner))...)
		b = append(b, 0x00)
	}
	return append(b, 0x00)
}

// refused holds err to the error model and to one reason.
func refused(t *testing.T, what string, err, want error) {
	t.Helper()
	if err == nil {
		t.Fatalf("%s decoded; want %v", what, want)
	}
	var de *codec.DecodeError
	if !errors.As(err, &de) || de.ContentType != iqavro.ContentType {
		t.Fatalf("%s = %v (%T), want a *codec.DecodeError", what, err, err)
	}
	if code := errcode.CodeOf(err); code != errcode.CorruptMessage {
		t.Fatalf("%s = %v carrying %v, want CorruptMessage", what, err, code)
	}
	if !reasonIs(err, want) {
		t.Fatalf("%s = %v, want %v", what, err, want)
	}
}

// TestBlockCountOverflowIsRefused is GO-2026-5047. hamba/avro negates a
// negative block count and adds it to its running total without an overflow
// check, so a count of MinInt64 (whose negation is itself), or a block of one
// followed by a block of MaxInt64, wraps the total negative. The library then
// passes its MaxSliceAllocSize check, grows the slice to that negative length
// and reports success: watched as err=nil and len(v) == MinInt64, which the
// next append on v panics over. Under either element limit, the default or the
// restored 1<<48.
func TestBlockCountOverflowIsRefused(t *testing.T) {
	bodies := []struct {
		name string
		body []byte
		want error
	}{
		// A count of MinInt64, a byte size of zero, the end of the array: 12 bytes.
		{"MinInt64", append(zigzagVarint(math.MinInt64), 0x00, 0x00), iqavro.ErrBlockForTest},
		// A block of one long, then a block of MaxInt64, then the end.
		{"1+MaxInt64", append(append([]byte{0x02, 0x00}, zigzagVarint(math.MaxInt64)...), 0x00), iqavro.ErrElementsForTest},
	}
	for _, elems := range []int{0, -1} {
		c := newCodec(t, iqavro.Config{Schema: longsSchema, MaxSliceElements: elems})
		for _, b := range bodies {
			var got []int64
			err := c.Unmarshal(b.body, &got)
			if err == nil {
				t.Fatalf("MaxSliceElements=%d: the %d-byte %s body decoded, len=%d", elems, len(b.body), b.name, len(got))
			}
			refused(t, b.name, err, b.want)
			if len(got) != 0 {
				t.Fatalf("MaxSliceElements=%d: the %s body was refused but left len=%d", elems, b.name, len(got))
			}
		}
	}
}

// TestZeroWidthElementsShareOneBudget is GO-2026-5048 for arrays.
// MaxSliceElements bounds ONE array, and an element that costs no bytes costs
// nothing against MaxBytes either, so nesting multiplies the bound: 64 inner
// arrays of 65,536 nulls is 259 bytes of body, decoded into [][]any it was
// watched allocating 100 MB, and 256 KiB of body would ask for 100 GB. The
// walk gives zero-width elements one budget for the whole body.
func TestZeroWidthElementsShareOneBudget(t *testing.T) {
	c := newCodec(t, iqavro.Config{Schema: nestedNullsSchema})
	body := nestedBody(64, iqavro.DefaultMaxSliceElements)
	if len(body) != 259 {
		t.Fatalf("the body is %d bytes, want the 259 this test is about", len(body))
	}
	var got [][]any
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	err := c.Unmarshal(body, &got)
	runtime.ReadMemStats(&after)
	refused(t, "64 arrays of 65,536 nulls", err, iqavro.ErrZeroWidthForTest)
	if grew := after.TotalAlloc - before.TotalAlloc; grew > 1<<20 {
		t.Fatalf("refusing the %d-byte body allocated %d bytes", len(body), grew)
	}

	// The budget is the body's, not each array's: every inner array here is
	// within the per-array limit and the sum is not.
	tight := newCodec(t, iqavro.Config{Schema: nestedNullsSchema, MaxSliceElements: 1024})
	err = tight.Unmarshal(nestedBody(2, 1024), &got)
	refused(t, "two arrays of 1,024 nulls at MaxSliceElements=1024", err, iqavro.ErrZeroWidthForTest)
	err = tight.Unmarshal(nestedBody(1, 1025), &got)
	refused(t, "one array of 1,025 nulls at MaxSliceElements=1024", err, iqavro.ErrElementsForTest)

	// Exactly the budget decodes, and so does what Marshal writes.
	if err := tight.Unmarshal(nestedBody(4, 256), &got); err != nil || len(got) != 4 || len(got[3]) != 256 {
		t.Fatalf("four arrays of 256 nulls at MaxSliceElements=1024 = %d arrays, %v", len(got), err)
	}
	in := [][]any{{nil, nil}, {}, {nil}}
	b, err := tight.Marshal(&in)
	if err != nil {
		t.Fatal(err)
	}
	// A fresh target: hamba/avro leaves an inner slice it is decoding into as
	// it was when the body's array is empty.
	var back [][]any
	if err := tight.Unmarshal(b, &back); err != nil || !reflect.DeepEqual(back, in) {
		t.Fatalf("round trip of %v = %v, %v", in, back, err)
	}
}

// TestZeroWidthElementsCannotBuyDecodeTime is GO-2026-5046. An empty record
// is an element of no bytes the library still visits one by one, so 4,095
// inner arrays of 65,536 of them is 16 KiB of body and 268 million decode
// steps (watched taking 0.94 s; 256 KiB is about 15 s per message). The walk
// counts zero-width elements without visiting them, so refusing the body
// costs one pass over its 16 KiB.
func TestZeroWidthElementsCannotBuyDecodeTime(t *testing.T) {
	c := newCodec(t, iqavro.Config{Schema: nestedEmptiesSchema})
	body := nestedBody(4095, iqavro.DefaultMaxSliceElements)
	var got [][]struct{}
	start := time.Now()
	err := c.Unmarshal(body, &got)
	took := time.Since(start)
	refused(t, "4,095 arrays of 65,536 empty records", err, iqavro.ErrZeroWidthForTest)
	t.Logf("a %d-byte body declaring %d empty records was refused in %v", len(body), 4095*iqavro.DefaultMaxSliceElements, took)

	in := [][]struct{}{{{}, {}}, {{}}}
	b, err := c.Marshal(&in)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Unmarshal(b, &got); err != nil || !reflect.DeepEqual(got, in) {
		t.Fatalf("round trip of %v = %v, %v", in, got, err)
	}
}

// TestMapElementCountIsBounded is GO-2026-5048 for maps, and the limit
// Config.MaxSliceElements has always documented for one. hamba/avro checks
// MaxSliceAllocSize for arrays only, so before the walk a map had no element
// bound at all: MaxSliceElements: 4 decoded a 100,000-entry map without an
// error.
func TestMapElementCountIsBounded(t *testing.T) {
	c := newCodec(t, iqavro.Config{Schema: mapSchema, MaxSliceElements: 4})
	entries := func(n int) map[string]string {
		m := make(map[string]string, n)
		for i := range n {
			m["k"+strconv.Itoa(i)] = ""
		}
		return m
	}
	for _, n := range []int{5, 100_000} {
		b, err := c.Marshal(&mapped{Meta: entries(n)})
		if err != nil {
			t.Fatal(err)
		}
		var got mapped
		err = c.Unmarshal(b, &got)
		refused(t, strconv.Itoa(n)+" entries at MaxSliceElements=4", err, iqavro.ErrElementsForTest)
		if len(got.Meta) != 0 {
			t.Fatalf("%d entries were refused but %d were decoded first", n, len(got.Meta))
		}
	}

	// The count is the map's, across its blocks: three entries, then two.
	split := []byte{0x06, 0x02, 'a', 0x00, 0x02, 'b', 0x00, 0x02, 'c', 0x00, 0x04, 0x02, 'd', 0x00, 0x02, 'e', 0x00, 0x00}
	var got mapped
	refused(t, "a map of 3+2 entries at MaxSliceElements=4", c.Unmarshal(split, &got), iqavro.ErrElementsForTest)

	b, err := c.Marshal(&mapped{Meta: entries(4)})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Unmarshal(b, &got); err != nil || len(got.Meta) != 4 {
		t.Fatalf("four entries at MaxSliceElements=4 = %v, %v", got.Meta, err)
	}
}

// TestRecordNestingIsBounded is the gap the package comment used to state
// rather than fix: a recursive schema had no nesting bound but MaxBytes, which
// at one byte a level allowed more depth than a goroutine has stack.
func TestRecordNestingIsBounded(t *testing.T) {
	c := newCodec(t, iqavro.Config{Schema: listSchema})
	// A list of n nodes is n-1 union indices of 1, then the last node's null.
	list := func(n int) []byte { return append(bytes.Repeat([]byte{0x02}, n-1), 0x00) }

	var got listNode
	refused(t, "a list one node deeper than the bound", c.Unmarshal(list(iqavro.MaxRecordDepthForTest+1), &got), iqavro.ErrDepthForTest)
	refused(t, "a 1 MiB list", c.Unmarshal(list(1<<20), &got), iqavro.ErrDepthForTest)

	// At the bound it decodes, and what Marshal writes for it decodes.
	if err := c.Unmarshal(list(iqavro.MaxRecordDepthForTest), &got); err != nil {
		t.Fatalf("a list of exactly %d nodes = %v", iqavro.MaxRecordDepthForTest, err)
	}
	head := &listNode{}
	for range iqavro.MaxRecordDepthForTest - 1 {
		head = &listNode{Next: head}
	}
	b, err := c.Marshal(head)
	if err != nil {
		t.Fatal(err)
	}
	got = listNode{}
	if err := c.Unmarshal(b, &got); err != nil {
		t.Fatalf("Marshal's own %d-node list = %v", iqavro.MaxRecordDepthForTest, err)
	}
	n := 0
	for p := &got; p != nil; p = p.Next {
		n++
	}
	if n != iqavro.MaxRecordDepthForTest {
		t.Fatalf("the list came back with %d nodes, want %d", n, iqavro.MaxRecordDepthForTest)
	}
}

// optSchema holds a union in a record, because codec.Target strips a
// top-level pointer that a bare nullable union would need.
const optSchema = `{"type":"record","name":"Opt","fields":[{"name":"v","type":["null","long"]}]}`

type opt struct {
	V *int64 `avro:"v"`
}

// TestTheWalkRefusesMalformedBodies is each rule the walk adds, one body
// apiece. Some the library refused already; the walk must refuse them too,
// because it is what decides whether the library sees the body at all.
func TestTheWalkRefusesMalformedBodies(t *testing.T) {
	ff := func(n int, last byte) []byte { return append(bytes.Repeat([]byte{0xff}, n), last) }
	cases := []struct {
		name   string
		schema string
		body   []byte
		into   func() any
		want   error
	}{
		{"an 11-byte long", `"long"`, ff(10, 0x01), func() any { return new(int64) }, iqavro.ErrVarintForTest},
		{"a long past 64 bits", `"long"`, ff(9, 0x02), func() any { return new(int64) }, iqavro.ErrVarintForTest},
		{"a 6-byte int", `"int"`, ff(5, 0x01), func() any { return new(int32) }, iqavro.ErrVarintForTest},
		// hamba/avro drops the excess bits of this one and decodes a number.
		{"an int past 32 bits", `"int"`, ff(4, 0x1f), func() any { return new(int32) }, iqavro.ErrVarintForTest},
		{"a varint that runs off the end", `"long"`, []byte{0x80}, func() any { return new(int64) }, iqavro.ErrTruncated},
		{"a boolean of 2", `"boolean"`, []byte{0x02}, func() any { return new(bool) }, iqavro.ErrValueForTest},
		{"a union index past the branches", optSchema, []byte{0x04}, func() any { return new(opt) }, iqavro.ErrValueForTest},
		{"a negative union index", optSchema, []byte{0x01}, func() any { return new(opt) }, iqavro.ErrValueForTest},
		{
			"an enum index past the symbols", `{"type":"enum","name":"E","symbols":["A","B"]}`,
			[]byte{0x04},
			func() any { return new(string) }, iqavro.ErrValueForTest,
		},
		{"a negative string length", `"string"`, []byte{0x01}, func() any { return new(string) }, iqavro.ErrLengthForTest},
		{"a string past the end", `"string"`, []byte{0x08, 'a'}, func() any { return new(string) }, iqavro.ErrTruncated},
		{"a fixed past the end", `{"type":"fixed","name":"F","size":4}`, []byte{1, 2, 3}, func() any { return new([4]byte) }, iqavro.ErrTruncated},
		{"a float past the end", `"float"`, []byte{1, 2, 3}, func() any { return new(float32) }, iqavro.ErrTruncated},
		// A block of -1 items with a byte size of -1.
		{"a negative block size", longsSchema, []byte{0x01, 0x01, 0x00, 0x00}, func() any { return new([]int64) }, iqavro.ErrBlockForTest},
		{"a block size past the end", longsSchema, []byte{0x01, 0x7e, 0x00, 0x00}, func() any { return new([]int64) }, iqavro.ErrTruncated},
		// One one-byte item that claims two bytes: the library's skip decoder
		// would jump by the size and read the next block from inside this one.
		{"a block size its items disagree with", longsSchema, []byte{0x01, 0x04, 0x00, 0x00}, func() any { return new([]int64) }, iqavro.ErrBlockForTest},
		{"two datums", `"long"`, []byte{0x00, 0x00}, func() any { return new(int64) }, iqavro.ErrTrailingBytes},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := newCodec(t, iqavro.Config{Schema: tc.schema})
			refused(t, tc.name, c.Unmarshal(tc.body, tc.into()), tc.want)
		})
	}
}

// everyValues are the values TestTheWalkAcceptsEveryMarshalOutput and the
// fuzz seeds encode: empty, small, and past the library's 100-item block
// length, so that an array and a map are written as several blocks with a
// negative count and a byte size. The large one stays inside the fuzz arms'
// limits (500 zero-byte elements against 512) so that it round-trips there
// too.
func everyValues() []every {
	seven := int64(-7)
	large := every{
		B: true, I: math.MinInt32, L: math.MaxInt64, F: -1.5, D: math.Inf(-1),
		By: bytes.Repeat([]byte{0xc3, 0x01}, 300), S: "héllo", E: "BLUE", Fx: [4]byte{0xc3, 0x01, 0, 0xff},
		Longs: make([]int64, 250), Nulls: make([]struct{}, 200), Units: make([]unit, 300),
		Fours: make([][4]byte, 101), Grid: [][]int32{{1, 2, 3}, {}, make([]int32, 150)},
		Colours: map[string]string{}, Opt: &seven, Pick: "x",
		Date: -1, TMs: 86_399_999, TSu: math.MinInt64, Dec: []byte{0x80, 0x00},
		UUID: "00000000-0000-0000-0000-000000000000", Dur: [12]byte{1, 2, 3},
		Tree: tree{V: 1, Kids: []tree{{V: 2, Kids: []tree{{V: 3}}}, {V: 4}}},
	}
	for i := range large.Longs {
		large.Longs[i] = int64(i-125) << 40
	}
	for i := range 150 {
		large.Colours["c"+strconv.Itoa(i)] = []string{"RED", "GREEN", "BLUE"}[i%3]
	}
	return []every{
		{E: "RED"},
		{
			B: true, I: 1, L: -98, S: "a", E: "GREEN", Longs: []int64{1}, Nulls: []struct{}{{}}, Units: []unit{{}},
			Grid: [][]int32{{-1}}, Colours: map[string]string{"k": "RED"}, Pick: int64(-98), Maybe: &[4]byte{1, 2, 3, 4},
		},
		large,
	}
}

// TestTheWalkAcceptsEveryMarshalOutput holds the walk to the codec's own
// output, under both framings, for every type it supports.
func TestTheWalkAcceptsEveryMarshalOutput(t *testing.T) {
	for _, framing := range []iqavro.Framing{iqavro.FramingDatum, iqavro.FramingSingleObject} {
		c := newCodec(t, iqavro.Config{Schema: everySchema, Framing: framing})
		values := everyValues()
		for i := range values {
			b, err := c.Marshal(&values[i])
			if err != nil {
				t.Fatalf("%s/%d: %v", framing, i, err)
			}
			var got every
			if err := c.Unmarshal(b, &got); err != nil {
				t.Fatalf("%s/%d: the codec refused its own %d-byte output: %v", framing, i, len(b), err)
			}
			again, err := c.Marshal(&got)
			if err != nil || len(again) != len(b) {
				t.Fatalf("%s/%d: re-encoding the decode = %d bytes, %v; want %d", framing, i, len(again), err, len(b))
			}
		}

		// The logical types' own Go types, through their own encoders.
		lc := newCodec(t, iqavro.Config{Schema: logicalSchema, Framing: framing})
		when := time.UnixMilli(-1).UTC()
		lv := logical{
			Date:      time.Date(1969, 12, 31, 0, 0, 0, 0, time.UTC),
			Clock:     time.Hour,
			ClockFine: -time.Microsecond,
			TSm:       time.UnixMilli(1 << 41).UTC(),
			TSu:       time.Date(2026, 10, 26, 1, 2, 3, 4000, time.UTC),
			Dec:       big.NewRat(-12345, 100),
			FDec:      big.NewRat(7, 1000),
			UUID:      "123e4567-e89b-12d3-a456-426614174000",
			Dur:       hamba.LogicalDuration{Months: 1, Days: 2, Milliseconds: 3},
			When:      &when,
		}
		b, err := lc.Marshal(&lv)
		if err != nil {
			t.Fatalf("%s/logical: %v", framing, err)
		}
		var got logical
		if err := lc.Unmarshal(b, &got); err != nil {
			t.Fatalf("%s/logical: the codec refused its own output: %v", framing, err)
		}
		if !got.Date.Equal(lv.Date) || !got.TSm.Equal(lv.TSm) || !got.TSu.Equal(lv.TSu) || got.Clock != lv.Clock ||
			got.ClockFine != lv.ClockFine || got.Dec.Cmp(lv.Dec) != 0 || got.FDec.Cmp(lv.FDec) != 0 || got.UUID != lv.UUID ||
			got.Dur != lv.Dur || got.When == nil || !got.When.Equal(when) {
			t.Fatalf("%s/logical: round trip = %+v", framing, got)
		}
	}
}
