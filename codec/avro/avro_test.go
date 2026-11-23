package avro_test

import (
	"bytes"
	"errors"
	"fmt"
	"go/parser"
	"go/token"
	"math/big"
	"reflect"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	avrolib "github.com/iqhive/avro/v2"
	"github.com/iqhive/avro/v2/pkg/crc64"

	"github.com/iqhive/iqmq/codec"
	iqavro "github.com/iqhive/iqmq/codec/avro"
	"github.com/iqhive/iqmq/codec/codectest"
	"github.com/iqhive/iqmq/internal/errcode"
)

// orderSchema models the metadata block as an ARRAY of key/value records
// rather than an Avro `map`, which is the workaround the package comment
// names: a map field makes the body depend on Go's map iteration order and no
// option can fix that. Every other Avro type is deterministic here, and this
// schema is therefore a body that is a function of the value.
const orderSchema = `{
  "type": "record",
  "name": "Order",
  "namespace": "iqmq.test",
  "fields": [
    {"name": "id",   "type": "string"},
    {"name": "qty",  "type": "long"},
    {"name": "tags", "type": {"type": "array", "items": "string"}},
    {"name": "attrs", "type": {"type": "array", "items": {
       "type": "record", "name": "Attr",
       "fields": [{"name": "k", "type": "string"}, {"name": "v", "type": "string"}]
    }}}
  ]
}`

// mapSchema is the same record with an Avro `map`, used only to pin the
// non-determinism the package comment states.
const mapSchema = `{
  "type": "record",
  "name": "Mapped",
  "namespace": "iqmq.test",
  "fields": [{"name": "meta", "type": {"type": "map", "values": "string"}}]
}`

const longsSchema = `{"type":"array","items":"long"}`

type attr struct {
	K string `avro:"k"`
	V string `avro:"v"`
}

type order struct {
	ID    string   `avro:"id"`
	Qty   int64    `avro:"qty"`
	Tags  []string `avro:"tags"`
	Attrs []attr   `avro:"attrs"`
}

type mapped struct {
	Meta map[string]string `avro:"meta"`
}

func newCodec(t *testing.T, c iqavro.Config) codec.Codec {
	t.Helper()
	got, err := iqavro.New(c)
	if err != nil {
		t.Fatal(err)
	}
	return got
}

func suite(t *testing.T, cfg iqavro.Config) codectest.Suite {
	t.Helper()
	cfg.Schema = orderSchema
	c := newCodec(t, cfg)
	want := order{ID: "a", Qty: 2, Tags: []string{"x", "y"}, Attrs: []attr{{"a", "2"}, {"z", "1"}}}
	return codectest.Suite{
		Codec:       c,
		ContentType: iqavro.ContentType,
		Cases: []codectest.Case{{
			Name:      "order",
			Value:     &want,
			NewTarget: func() any { return new(order) },
			Check: func(target any) error {
				got := target.(*order)
				if got.ID != want.ID || got.Qty != want.Qty ||
					strings.Join(got.Tags, ",") != "x,y" ||
					len(got.Attrs) != 2 || got.Attrs[0] != want.Attrs[0] || got.Attrs[1] != want.Attrs[1] {
					return errors.New("round trip lost a field: " + got.ID + "/" + strconv.FormatInt(got.Qty, 10))
				}
				return nil
			},
		}, {
			Name:      "empty strings and containers",
			Value:     &order{Tags: []string{}, Attrs: []attr{}},
			NewTarget: func() any { return new(order) },
			Check: func(target any) error {
				got := target.(*order)
				if got.ID != "" || got.Qty != 0 || len(got.Tags) != 0 || len(got.Attrs) != 0 {
					return errors.New("round trip invented a value")
				}
				return nil
			},
		}},
		Unencodable: []any{
			make(chan int),
			"a string is not this record",
			42,
		},
		Corrupt: [][]byte{
			{},                                   // truncated: the whole record
			{0x02},                               // a length prefix and no string
			{0xff, 0xff, 0xff, 0xff, 0xff, 0x7f}, // a hostile string length
			{0x02, 'a'},                          // the record stops after one field
		},
	}
}

func TestContractDatum(t *testing.T) { suite(t, iqavro.Config{}).Run(t) }

func TestContractSingleObject(t *testing.T) {
	suite(t, iqavro.Config{Framing: iqavro.FramingSingleObject}).Run(t)
}

// TestTruncatedBodyIsCorruptAndNotZeroes is the finding that made this package
// drive avro.Reader itself instead of calling API.Unmarshal.
//
// hamba/avro's frozenConfig.Unmarshal returned nil when the reader's error was
// io.EOF, so every field that ran off the end of the body was left at its
// zero value and the caller was told the decode succeeded. iqhive/avro
// reports io.ErrUnexpectedEOF; this codec refuses the body either way.
func TestTruncatedBodyIsCorruptAndNotZeroes(t *testing.T) {
	c := newCodec(t, iqavro.Config{Schema: orderSchema})

	for _, body := range [][]byte{{}, {0x02, 'a'}, {0x02, 'a', 0x02}} {
		var got order
		err := c.Unmarshal(body, &got)
		if !reasonIs(err, iqavro.ErrTruncated) {
			t.Fatalf("Unmarshal(%x) = %v, want ErrTruncated", body, err)
		}
		var de *codec.DecodeError
		if !errors.As(err, &de) {
			t.Fatalf("Unmarshal(%x) = %v, want a *codec.DecodeError", body, err)
		}
	}

	// A complete body is not flagged: the truncation check must not refuse
	// every message.
	full, err := c.Marshal(&order{ID: "a", Qty: 1, Tags: []string{"t"}, Attrs: []attr{{"k", "v"}}})
	if err != nil {
		t.Fatal(err)
	}
	var round order
	if err := c.Unmarshal(full, &round); err != nil {
		t.Fatalf("a complete body was rejected: %v", err)
	}
	if round.ID != "a" || len(round.Tags) != 1 || len(round.Attrs) != 1 {
		t.Fatalf("round trip = %+v", round)
	}
}

// TestTrailingBytesAreCorrupt: one body is one datum. Avro's binary encoding
// is not self-delimiting for every schema, so a second datum appended to the
// first is silently ignored by the library.
func TestTrailingBytesAreCorrupt(t *testing.T) {
	c := newCodec(t, iqavro.Config{Schema: orderSchema})
	body, err := c.Marshal(&order{ID: "a", Tags: []string{}, Attrs: []attr{}})
	if err != nil {
		t.Fatal(err)
	}
	doubled := append(append([]byte{}, body...), body...)
	var got order
	if err := c.Unmarshal(doubled, &got); !reasonIs(err, iqavro.ErrTrailingBytes) {
		t.Fatalf("two datums in one body = %v, want ErrTrailingBytes", err)
	}
	// And the library takes it.
	var lib order
	if err := avrolib.Unmarshal(avrolib.MustParse(orderSchema), doubled, &lib); err != nil {
		t.Fatalf("the library rejected two datums itself (%v), so this check is not the thing refusing them", err)
	}
}

// TestArrayElementCountIsBoundedBeforeAllocation is the specification's "count limits before
// allocation" for Avro. An array block count is an attacker-chosen zigzag
// varint, hamba/avro's own bound was 1<<48, and it allocated before
// any element was read. The walk refuses the count before the library sees it;
// the library's MaxSliceAllocSize, set from the same limit, stands behind it.
func TestArrayElementCountIsBoundedBeforeAllocation(t *testing.T) {
	c := newCodec(t, iqavro.Config{Schema: longsSchema})
	hostile := zigzagVarint(1 << 20) // 1,048,576 elements from four bytes
	t.Logf("a block count of %d is %d bytes of body: %x", 1<<20, len(hostile), hostile)

	var got []int64
	err := c.Unmarshal(hostile, &got)
	if err == nil {
		t.Fatalf("a %d-byte body claiming %d elements decoded to %d elements", len(hostile), 1<<20, len(got))
	}
	if !reasonIs(err, iqavro.ErrElementsForTest) {
		t.Fatalf("Unmarshal(%x) = %v; the rejection did not come from the element-count limit, so this test "+
			"is not showing what it claims", hostile, err)
	}
	var de *codec.DecodeError
	if !errors.As(err, &de) {
		t.Fatalf("Unmarshal(%x) = %v, want a *codec.DecodeError", hostile, err)
	}

	// An array the body can justify still decodes.
	fine, err := c.Marshal(&[]int64{1, 2, 3})
	if err != nil {
		t.Fatal(err)
	}
	var ok []int64
	if err := c.Unmarshal(fine, &ok); err != nil || len(ok) != 3 {
		t.Fatalf("a three-element array = %v, %v", ok, err)
	}
}

// TestTheLibraryDefaultAllocatesFromTheBlockCount pins what the library's
// DefaultConfig does with an array block count, which is why this package
// never uses it, at a size that is safe to allocate. The 8.8-terabyte
// measurement in the package comment is not repeated as a test, because a test
// that allocates 8.8 TB kills the host.
//
// hamba/avro allocated a block's elements before reading any of them.
// iqhive/avro grows the slice as elements arrive, which bounds an array of
// longs by the body; but a null takes no bytes, so up to v2.36.0 a four-byte
// body still bought a million of them. The release after v2.36.0 bounds those
// too, by the bytes the Unmarshal has read. This package's own bound stays:
// the walk applies MaxSliceElements before the library sees the body.
//
// The library is driven directly. Through the codec, even with its bound
// lifted, the walk refuses this body before the library sees it, so the
// codec cannot show what the library does.
func TestTheLibraryDefaultAllocatesFromTheBlockCount(t *testing.T) {
	nulls := avrolib.MustParse(`{"type":"array","items":"null"}`)
	hostile := zigzagVarint(1 << 20)
	var got []any
	err := avrolib.Unmarshal(nulls, hostile, &got)
	if v, before := avroLibraryBefore(t, "v2.36.1"); before {
		if err != nil && strings.Contains(err.Error(), "MaxSliceAllocSize") {
			t.Fatalf("iqhive/avro %s bounds array allocation at its own default (%v); re-read the package comment", v, err)
		}
		if cap(got) < 1<<20 {
			t.Fatalf("iqhive/avro %s's default allocated cap=%d from a %d-byte body claiming %d nulls; "+
				"re-read the package comment", v, cap(got), len(hostile), 1<<20)
		}
		t.Logf("library %s default: a %d-byte body claiming %d nulls allocated cap=%d, then %v",
			v, len(hostile), 1<<20, cap(got), err)
	} else if err == nil || cap(got) >= 1<<20 {
		t.Fatalf("iqhive/avro %s's default decoded a %d-byte body claiming %d nulls (cap=%d, %v); "+
			"re-read the package comment", v, len(hostile), 1<<20, cap(got), err)
	}
	// Through the codec, MaxSliceElements refuses the same body, and one
	// element past it, before the library sees either.
	c := newCodec(t, iqavro.Config{Schema: `{"type":"array","items":"null"}`})
	for _, body := range [][]byte{hostile, append(zigzagVarint(iqavro.DefaultMaxSliceElements+1), 0)} {
		var nullsGot []any
		if err := c.Unmarshal(body, &nullsGot); !reasonIs(err, iqavro.ErrElementsForTest) {
			t.Fatalf("Unmarshal(%x) through the codec = %v, want MaxSliceElements' refusal", body, err)
		}
	}

	// The codec with the library's bound restored: a long costs at least a
	// byte, the body has none to give, and nothing is allocated.
	unbounded := newCodec(t, iqavro.Config{Schema: longsSchema, MaxSliceElements: -1})
	var mine []int64
	if err := unbounded.Unmarshal(hostile, &mine); !reasonIs(err, iqavro.ErrTruncated) || cap(mine) != 0 {
		t.Fatalf("MaxSliceElements=-1: Unmarshal(%x) = %v with cap=%d, want ErrTruncated and nothing allocated",
			hostile, err, cap(mine))
	}
}

// TestBodyOverTheLimitIsRejectedBeforeTheDecoder is the byte-count half of the rule, with a body that is VALID and decodes when the limit permits it.
func TestBodyOverTheLimitIsRejectedBeforeTheDecoder(t *testing.T) {
	roomy := newCodec(t, iqavro.Config{Schema: orderSchema, MaxBytes: 8192})
	body, err := roomy.Marshal(&order{ID: strings.Repeat("x", 4096), Tags: []string{}, Attrs: []attr{}})
	if err != nil {
		t.Fatal(err)
	}
	tight := newCodec(t, iqavro.Config{Schema: orderSchema, MaxBytes: 1024})
	var got order
	if err := tight.Unmarshal(body, &got); err != iqavro.ErrTooLarge { //nolint:errorlint // identity: errors.Is matches on the portable code, see reasonIs
		t.Fatalf("a %d-byte body at MaxBytes=1024 = %v, want ErrTooLarge", len(body), err)
	}
	if got.ID != "" {
		t.Fatal("the decoder ran anyway")
	}
	if errcode.CodeOf(iqavro.ErrTooLarge) != errcode.MessageTooLarge {
		t.Fatalf("ErrTooLarge carries %v, want MessageTooLarge: a body that is too big is not corrupt",
			errcode.CodeOf(iqavro.ErrTooLarge))
	}
	if err := roomy.Unmarshal(body, &got); err != nil || len(got.ID) != 4096 {
		t.Fatalf("the same body at MaxBytes=8192 = %d bytes, %v", len(got.ID), err)
	}
}

// TestStringLengthIsBounded covers the other pre-allocation limit: one
// `string` or `bytes` value's own length prefix.
func TestStringLengthIsBounded(t *testing.T) {
	c := newCodec(t, iqavro.Config{Schema: `"string"`, MaxStringBytes: 64})
	body := append(zigzagVarint(1<<20), make([]byte, 8)...)
	var s string
	err := c.Unmarshal(body, &s)
	if err == nil {
		t.Fatalf("a %d-byte body claiming a 1 MiB string decoded to %d bytes", len(body), len(s))
	}
	if !reasonIs(err, iqavro.ErrStringLimitForTest) {
		t.Fatalf("the rejection did not come from the string limit: %v", err)
	}
	// Inside the limit it decodes.
	ok := newCodec(t, iqavro.Config{Schema: `"string"`})
	good, err := ok.Marshal("hello")
	if err != nil {
		t.Fatal(err)
	}
	if err := ok.Unmarshal(good, &s); err != nil || s != "hello" {
		t.Fatalf("round trip = %q, %v", s, err)
	}
}

// TestFramingIsConfiguredNotDetected is the specification's "envelopes are never
// auto-detected unless configured", applied in both directions.
//
// The directions are not symmetrical, and the datum half used to pretend they
// were. A single-object codec VERIFIES its framing, because the framing says
// what it is: no marker, no body. A datum codec cannot, because nothing in a
// datum says it is one -- C3 01 is also the zigzag long -98 -- so it reads the
// body as a datum. Refusing a body that began with the marker refused the
// codec's own output (TestADatumMayBeginWithTheMarker).
// What "not detected" means for a datum codec is that it never UNWRAPS: a
// single-object body is misread as a datum, here as a string of length -98,
// and never decoded as the value inside the envelope.
func TestFramingIsConfiguredNotDetected(t *testing.T) {
	datum := newCodec(t, iqavro.Config{Schema: orderSchema})
	sobj := newCodec(t, iqavro.Config{Schema: orderSchema, Framing: iqavro.FramingSingleObject})
	v := &order{ID: "a", Tags: []string{}, Attrs: []attr{}}
	db, err := datum.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	sb, err := sobj.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	if len(sb) != len(db)+10 {
		t.Fatalf("single-object framing added %d bytes, want 10", len(sb)-len(db))
	}
	if sb[0] != 0xc3 || sb[1] != 0x01 {
		t.Fatalf("single-object body does not start with the C3 01 marker: %x", sb[:2])
	}

	var got order
	if err := sobj.Unmarshal(db, &got); !reasonIs(err, iqavro.ErrFraming) {
		t.Fatalf("a bare datum under single-object framing = %v, want ErrFraming", err)
	}
	got = order{}
	switch err := datum.Unmarshal(sb, &got); {
	case err == nil && reflect.DeepEqual(got, *v):
		t.Fatalf("a single-object body under datum framing decoded as the value inside the envelope, " +
			"so the datum codec detected and stripped a framing it was not configured for")
	case reasonIs(err, iqavro.ErrFraming):
		t.Fatalf("a single-object body under datum framing = %v: a datum codec has no framing to verify, "+
			"and refusing C3 01 refuses its own output", err)
	case err == nil:
		t.Fatalf("a single-object body under datum framing decoded, as %+v; under this schema the marker is a "+
			"negative string length, measured as a refusal", got)
	}
	if err := datum.Unmarshal(db, &got); err != nil {
		t.Fatal(err)
	}
	if err := sobj.Unmarshal(sb, &got); err != nil {
		t.Fatal(err)
	}
}

// TestADatumMayBeginWithTheMarker is FuzzAvro's nightly crasher as a rule
// C3 01 is the single-object marker and it is also the
// zigzag long -98, so a datum codec's own output begins with it whenever the
// first thing encoded is -98: hamba/avro writes an array or a map as a block
// whose count is NEGATED when a byte size follows, so every 98-entry array
// starts that way, and so does a bare long of -98. The codec used to refuse
// all of them as ErrFraming -- it encoded what it could not decode.
func TestADatumMayBeginWithTheMarker(t *testing.T) {
	longs := newCodec(t, iqavro.Config{Schema: longsSchema})
	in := make([]int64, 98)
	for i := range in {
		in[i] = int64(i) * 7
	}
	body, err := longs.Marshal(&in)
	if err != nil {
		t.Fatal(err)
	}
	if len(body) < 2 || body[0] != 0xc3 || body[1] != 0x01 {
		t.Fatalf("a 98-entry array encoded as %x, which does not begin C3 01, so this test tests nothing", body[:min(len(body), 4)])
	}
	var out []int64
	if err := longs.Unmarshal(body, &out); err != nil {
		t.Fatalf("the codec cannot decode its own encoding of a 98-entry array: %v", err)
	}
	if !reflect.DeepEqual(out, in) {
		t.Fatalf("a 98-entry array came back as %v", out)
	}

	long := newCodec(t, iqavro.Config{Schema: `"long"`})
	b, err := long.Marshal(int64(-98))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(b, []byte{0xc3, 0x01}) {
		t.Fatalf("the long -98 encoded as %x, want c301", b)
	}
	var back int64
	if err := long.Unmarshal(b, &back); err != nil || back != -98 {
		t.Fatalf("the long -98 came back as %d, %v", back, err)
	}
}

// TestSingleObjectDetectsAForeignSchema is the one thing datum framing cannot
// do: a body written by a different schema is an error rather than a
// misreading.
func TestSingleObjectDetectsAForeignSchema(t *testing.T) {
	mine := newCodec(t, iqavro.Config{Schema: orderSchema, Framing: iqavro.FramingSingleObject})
	// The same shape with one field renamed, so the DATUM is byte-identical
	// and only the fingerprint differs.
	otherSchema := strings.Replace(orderSchema, `"name": "id"`, `"name": "identifier"`, 1)
	other := newCodec(t, iqavro.Config{Schema: otherSchema, Framing: iqavro.FramingSingleObject})
	if mine.(codec.Schemer).SchemaID() == other.(codec.Schemer).SchemaID() {
		t.Fatal("the two schemas have the same fingerprint, so this test cannot show a mismatch")
	}
	type renamed struct {
		ID    string   `avro:"identifier"`
		Qty   int64    `avro:"qty"`
		Tags  []string `avro:"tags"`
		Attrs []attr   `avro:"attrs"`
	}
	body, err := other.Marshal(&renamed{ID: "a", Tags: []string{}, Attrs: []attr{}})
	if err != nil {
		t.Fatal(err)
	}
	var got order
	if err := mine.Unmarshal(body, &got); !reasonIs(err, iqavro.ErrSchemaMismatch) {
		t.Fatalf("a body from a different schema = %v, want ErrSchemaMismatch", err)
	}

	// The same bytes under datum framing decode silently into the wrong
	// field's slot, which is the whole reason single-object framing is
	// offered. Measured rather than asserted.
	datum := newCodec(t, iqavro.Config{Schema: orderSchema})
	if err := datum.Unmarshal(body[10:], &got); err != nil {
		t.Fatalf("a datum from a differently named schema failed to decode (%v), so the point this test "+
			"makes about silent misreading no longer holds", err)
	}
	if got.ID != "a" {
		t.Fatalf("got %q; the datum was expected to decode silently under the wrong schema", got.ID)
	}
}

// TestSchemerIsImplemented is the root contract's schema hook, used here for
// the first time in the repository.
func TestSchemerIsImplemented(t *testing.T) {
	c := newCodec(t, iqavro.Config{Schema: orderSchema})
	s, ok := c.(codec.Schemer)
	if !ok {
		t.Fatal("the Avro codec does not implement codec.Schemer, so a publisher cannot stamp schema-id")
	}
	id := s.SchemaID()
	want, err := avrolib.MustParse(orderSchema).FingerprintUsing(avrolib.CRC64AvroLE)
	if err != nil {
		t.Fatal(err)
	}
	if id != hexOf(want) {
		t.Fatalf("SchemaID() = %q, want %q", id, hexOf(want))
	}
	if len(id) != 16 {
		t.Fatalf("SchemaID() = %q, want the 8-byte CRC-64-AVRO fingerprint in hex", id)
	}
}

// TestSortingAGoMapDoesNotOrderIt is why Config has no DeterministicMaps
// option. The obvious implementation -- rebuild the map inserting keys in
// sorted order -- does not work, because a Go map's iteration order is not its
// insertion order. This is the measurement that decided it.
func TestSortingAGoMapDoesNotOrderIt(t *testing.T) {
	in := map[string]string{}
	for i := range 16 {
		in["k"+strconv.Itoa(i)] = strconv.Itoa(i)
	}
	keys := make([]string, 0, len(in))
	for k := range in {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	rebuilt := reflect.MakeMapWithSize(reflect.TypeOf(in), len(keys))
	for _, k := range keys {
		rebuilt.SetMapIndex(reflect.ValueOf(k), reflect.ValueOf(in[k]))
	}
	orders := map[string]bool{}
	for range 200 {
		var b strings.Builder
		for _, k := range rebuilt.MapKeys() {
			b.WriteString(k.String())
			b.WriteByte(',')
		}
		orders[b.String()] = true
	}
	if len(orders) == 1 {
		t.Fatal("a Go map rebuilt with sorted insertions now iterates in one order, so codec/avro could " +
			"offer a DeterministicMaps option after all; re-read the package comment")
	}
	t.Logf("a 16-key map rebuilt in sorted order iterated %d different ways over 200 walks", len(orders))
}

// TestMapFieldsAreNotDeterministic pins the limitation the package comment
// states, and the workaround beside it.
func TestMapFieldsAreNotDeterministic(t *testing.T) {
	c := newCodec(t, iqavro.Config{Schema: mapSchema})
	meta := map[string]string{}
	for i := range 16 {
		meta["k"+strconv.Itoa(i)] = strconv.Itoa(i)
	}
	seen := map[string]bool{}
	for range 200 {
		b, err := c.Marshal(&mapped{Meta: meta})
		if err != nil {
			t.Fatal(err)
		}
		seen[string(b)] = true
	}
	if len(seen) == 1 {
		t.Fatal("a sixteen-key Avro map encoded identically 200 times, so hamba/avro now orders map keys: " +
			"the package comment's determinism limitation is stale")
	}
	t.Logf("map field: %d distinct encodings of one value over 200 calls", len(seen))

	// It still round-trips; the body is just not a function of the value.
	var got mapped
	b, err := c.Marshal(&mapped{Meta: meta})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Unmarshal(b, &got); err != nil || len(got.Meta) != 16 || got.Meta["k7"] != "7" {
		t.Fatalf("round trip = %+v, %v", got.Meta, err)
	}

	// The workaround: the same data as an array of key/value records is a
	// function of the value, which is what orderSchema uses.
	arr := newCodec(t, iqavro.Config{Schema: orderSchema})
	attrs := make([]attr, 0, 16)
	for i := range 16 {
		attrs = append(attrs, attr{K: "k" + strconv.Itoa(i), V: strconv.Itoa(i)})
	}
	first, err := arr.Marshal(&order{Tags: []string{}, Attrs: attrs})
	if err != nil {
		t.Fatal(err)
	}
	for range 200 {
		again, err := arr.Marshal(&order{Tags: []string{}, Attrs: attrs})
		if err != nil || !bytes.Equal(first, again) {
			t.Fatalf("an array of key/value records is not deterministic either: %x vs %x (%v)", first, again, err)
		}
	}
}

// TestThePackageDocNamesOnlyConfigFieldsThatExist is the reported defect (hunt
// 9 mx9-6): the package comment's account of determinism named a
// Config.DeterministicMaps option, which TestSortingAGoMapDoesNotOrderIt shows
// cannot work and Config never had, and left out the workaround
// TestMapFieldsAreNotDeterministic pins.
func TestThePackageDocNamesOnlyConfigFieldsThatExist(t *testing.T) {
	f, err := parser.ParseFile(token.NewFileSet(), "avro.go", nil, parser.ParseComments|parser.PackageClauseOnly)
	if err != nil {
		t.Fatal(err)
	}
	doc := strings.Join(strings.Fields(f.Doc.Text()), " ")
	names := regexp.MustCompile(`\bConfig\.([A-Z]\w*)`).FindAllStringSubmatch(doc, -1)
	for _, m := range names {
		if _, ok := reflect.TypeFor[iqavro.Config]().FieldByName(m[1]); !ok {
			t.Errorf("the package comment names Config.%s, and Config has no such field", m[1])
		}
	}
	if !strings.Contains(doc, "array of key/value records") {
		t.Error("the package comment does not name the workaround for a body that must be a function of the value")
	}
}

// TestAFailedEncodeDoesNotPoisonTheCodec is hamba/avro's
// Writer keeps its error and Encoder.Reset does not clear it, so a pooled
// encoder that has once failed reports that failure for ever.
func TestAFailedEncodeDoesNotPoisonTheCodec(t *testing.T) {
	// The library, first. Up to iqhive/avro v2.36.0, as in hamba/avro, a
	// second Encode after a failure and a Reset still reports the first
	// failure, which the discard-on-error in AppendMarshal is for; the
	// release after v2.36.0 clears it on Reset. If either stops being true,
	// re-read AppendMarshal.
	schema := avrolib.MustParse(orderSchema)
	var buf []byte
	w := &sliceWriter{&buf}
	e := avrolib.NewEncoderForSchema(schema, w)
	if err := e.Encode(make(chan int)); err == nil {
		t.Fatal("the library encoded a chan")
	}
	e.Reset(w)
	err := e.Encode(order{ID: "a", Tags: []string{}, Attrs: []attr{}})
	v, keeps := avroLibraryBefore(t, "v2.36.1")
	switch {
	case keeps && err == nil:
		t.Fatalf("iqhive/avro %s's Encoder no longer keeps its error across Reset; re-read AppendMarshal", v)
	case !keeps && err != nil:
		t.Fatalf("iqhive/avro %s's Encoder keeps its error across Reset (%v); re-read AppendMarshal", v, err)
	default:
		t.Logf("library %s, a good value after a failure and a Reset: %v", v, err)
	}

	// This codec: a failure, then a hundred good values.
	c := newCodec(t, iqavro.Config{Schema: orderSchema})
	good := &order{ID: "a", Qty: 1, Tags: []string{"t"}, Attrs: []attr{{"k", "v"}}}
	want, err := c.Marshal(good)
	if err != nil {
		t.Fatal(err)
	}
	for i := range 100 {
		if _, err := c.Marshal(make(chan int)); err == nil {
			t.Fatal("Marshal encoded a chan")
		}
		got, err := c.Marshal(good)
		if err != nil || !bytes.Equal(got, want) {
			t.Fatalf("round %d: a good value after a failure = %x, %v", i, got, err)
		}
	}
}

func TestBadConfig(t *testing.T) {
	if _, err := iqavro.New(iqavro.Config{}); !errors.Is(err, iqavro.ErrNoSchema) {
		t.Fatalf("New with no schema = %v, want ErrNoSchema", err)
	}
	if _, err := iqavro.New(iqavro.Config{Schema: "{not json"}); err == nil {
		t.Fatal("New accepted a schema that will not parse")
	}
}

// schemaOf is the schema text a codec is bound to.
func schemaOf(c codec.Codec) string { return c.(interface{ Schema() string }).Schema() }

// TestNewParsesTheSchemaOnItsOwn is the reported defect (hunt 10 mx10-5): New
// parsed Config.Schema into the library's process-wide schema cache. So a
// schema could name a type it never defines once anything in the process had
// parsed one, bound to whichever definition came last, and the parse of a
// schema naming another codec's types rewrote that codec's schema in place,
// racing with its decodes. Each Config.Schema is parsed on its own now: a name
// it does not define is an InvalidConfig error, and no New touches another
// codec's schema. A second New of a schema with aliases is checked too: the
// fork at cd41f68 refuses an alias already in the cache it parses into, so
// through the shared cache that New failed.
func TestNewParsesTheSchemaOnItsOwn(t *testing.T) {
	const aliased = `{"type":"record","name":"Aliased","namespace":"iqmq.cache","aliases":["Old"],"fields":[
  {"name":"n","type":"long","aliases":["count"]}]}`
	for range 2 {
		if _, err := iqavro.New(iqavro.Config{Schema: aliased}); err != nil {
			t.Fatalf("New of a schema with aliases, again: %v", err)
		}
	}
	a := newCodec(t, iqavro.Config{Schema: `{"type":"record","name":"T","namespace":"iqmq.cache","fields":[
  {"name":"a","type":{"type":"record","name":"X","fields":[{"name":"n","type":"long"}]}},
  {"name":"b","type":"X"}]}`})
	before := schemaOf(a)
	avrolib.MustParse(`{"type":"record","name":"Elsewhere","namespace":"iqmq.cache","fields":[{"name":"n","type":"long"}]}`)
	for _, schema := range []string{
		`{"type":"array","items":"iqmq.cache.X"}`,
		`{"type":"array","items":"iqmq.cache.Elsewhere"}`,
		`{"type":"record","name":"U","namespace":"iqmq.cache","fields":[{"name":"x","type":"X"},{"name":"t","type":"T"}]}`,
	} {
		got, err := iqavro.New(iqavro.Config{Schema: schema})
		if err == nil {
			t.Errorf("New(%s), which names a type it does not define, bound it to %s", schema, schemaOf(got))
		} else if errcode.CodeOf(err) != errcode.InvalidConfig {
			t.Errorf("New(%s) = %v (%v), want InvalidConfig", schema, err, errcode.CodeOf(err))
		}
	}
	if after := schemaOf(a); after != before {
		t.Fatalf("New of another codec changed this codec's schema:\n  before %s\n  after  %s", before, after)
	}
}

// TestNewRefusesADecimalTheLibraryDrops is the reported defect (hunt 10
// mx10-6): iqhive/avro bounds a fixed's precision one digit below where
// hamba/avro did (a fixed of n bytes holds floor(log10(2^(8n-1)-1)) digits),
// and from the release after v2.36.0 caps every decimal's precision at 1000,
// and it parses a decimal past those bounds as the plain bytes or fixed under
// it, with no error. So a schema v1.1.3 bound to a decimal at hamba/avro's
// bound (fixed sizes 3, 5, 8, 10 and 15 among them) was bound to a plain fixed
// after the move, and every message decoded into a *big.Rat under it was
// refused as a corrupt body. New refuses such a decimal with an InvalidConfig
// error that names the bound. A decimal the specification calls invalid (no
// precision, a scale past it, a negative scale, a decimal on a long) is
// ignored as the specification says, and the schema keeps its plain type, as
// on v1.1.3; so is one the library keeps, and a logical type it does not know.
func TestNewRefusesADecimalTheLibraryDrops(t *testing.T) {
	fixed := func(size int, prec any, scale int) string {
		return fmt.Sprintf(`{"type":"record","name":"P","namespace":"iqmq.dec","fields":[{"name":"p","type":`+
			`{"type":"fixed","name":"D","size":%d,"logicalType":"decimal","precision":%v,"scale":%d}}]}`, size, prec, scale)
	}
	// A decimal deep in a recursive record, under an array, a map and a union.
	const nested = `{"type":"record","name":"R","namespace":"iqmq.dec","fields":[{"name":"next","type":["null","R"]},` +
		`{"name":"list","type":{"type":"array","items":{"type":"map","values":["null",` +
		`{"type":"fixed","name":"F","size":8,"logicalType":"decimal","precision":19}]}}}]}`
	refused := []struct{ schema, want string }{
		{fixed(3, 7, 2), "field p: a decimal on a fixed of size 3 takes a precision of 1 to 6, the library parses this one (precision 7) as the plain fixed"},
		{fixed(5, 12, 2), "field p: a decimal on a fixed of size 5 takes a precision of 1 to 11,"},
		{fixed(8, 19, 2), "field p: a decimal on a fixed of size 8 takes a precision of 1 to 18,"},
		{fixed(10, 24, 2), "field p: a decimal on a fixed of size 10 takes a precision of 1 to 23,"},
		{fixed(15, 36, 2), "field p: a decimal on a fixed of size 15 takes a precision of 1 to 35,"},
		// Past both libraries' bounds, which v1.1.3 parsed as a plain fixed too.
		{fixed(1, 3, 0), "field p: a decimal on a fixed of size 1 takes a precision of 1 to 2,"},
		{nested, "field list: a decimal on a fixed of size 8 takes a precision of 1 to 18,"},
	}
	if v, before := avroLibraryBefore(t, "v2.36.1"); before {
		t.Logf("iqhive/avro %s caps no decimal's precision, so a precision of 1001 is not refused", v)
	} else {
		refused = append(refused, struct{ schema, want string }{
			`{"type":"bytes","logicalType":"decimal","precision":1001,"scale":2}`,
			"the top level: a decimal takes a precision of 1 to 1000, the library parses this one (precision 1001) as the plain bytes",
		})
	}
	for _, tc := range refused {
		_, err := iqavro.New(iqavro.Config{Schema: tc.schema})
		if errcode.CodeOf(err) != errcode.InvalidConfig || !strings.Contains(fmt.Sprint(err), tc.want) {
			t.Errorf("New(%s) = %v; want InvalidConfig saying %q", tc.schema, err, tc.want)
		}
	}
	for _, schema := range []string{
		fixed(8, 18, 2),
		fixed(16, 38, 2),
		`{"type":"bytes","logicalType":"decimal","precision":10,"scale":2}`,
		`{"type":"string","logicalType":"iqmq-unknown"}`,
		`{"type":"fixed","name":"U","namespace":"iqmq.dec","size":16,"logicalType":"uuid"}`,
		// Invalid by the specification, so ignored: the plain type, as on v1.1.3.
		`{"type":"bytes","logicalType":"decimal"}`,
		`{"type":"bytes","logicalType":"decimal","precision":0}`,
		`{"type":"bytes","logicalType":"decimal","precision":-3}`,
		`{"type":"bytes","logicalType":"decimal","precision":2,"scale":3}`,
		`{"type":"bytes","logicalType":"decimal","precision":4,"scale":-1}`,
		`{"type":"long","logicalType":"decimal","precision":4}`,
		fixed(8, 19, 25),
		fixed(8, "10.5", 2),
	} {
		c, err := iqavro.New(iqavro.Config{Schema: schema})
		if err != nil {
			t.Errorf("New(%s) = %v; want it accepted", schema, err)
			continue
		}
		t.Logf("New(%s) binds %s", schema, schemaOf(c))
	}
	// The plain type is what such a schema decodes as.
	c := newCodec(t, iqavro.Config{Schema: `{"type":"bytes","logicalType":"decimal","precision":2,"scale":3}`})
	body, err := c.Marshal([]byte{0x30, 0x39})
	if err != nil {
		t.Fatal(err)
	}
	var got []byte
	if err := c.Unmarshal(body, &got); err != nil || !bytes.Equal(got, []byte{0x30, 0x39}) {
		t.Fatalf("an invalid decimal's plain bytes = %x, %v", got, err)
	}
}

// TestTheLibrarysTruncationIsErrTruncated is the reported defect (hunt 10
// mx10-7): Unmarshal took the library's error for a body that ran out to be
// io.EOF, as hamba/avro's was, where iqhive/avro reports io.ErrUnexpectedEOF
// ("reading long: unexpected EOF"). The walk refuses every short body before
// the library reads it, so nobody saw it, but a body the two disagreed on
// would have had codec.Reason give the library's error, not ErrTruncated.
// This drives the library's own reader on short bodies and hands its error to
// what Unmarshal makes of it.
func TestTheLibrarysTruncationIsErrTruncated(t *testing.T) {
	schema := avrolib.MustParse(orderSchema)
	for _, body := range [][]byte{{}, {0x02, 'a'}, {0x02, 'a', 0x02}, {0x02, 'a', 0x80}} {
		r := avrolib.NewReader(nil, 0)
		r.Reset(body)
		var got order
		r.ReadVal(schema, &got)
		if r.Error == nil {
			t.Fatalf("the library decoded the short body %x", body)
		}
		if err := iqavro.ReaderErrorForTest(r.Error); err != iqavro.ErrTruncated { //nolint:errorlint // identity, see reasonIs
			t.Errorf("the library's %q for the short body %x is reported as %v, want ErrTruncated", r.Error, body, err)
		}
	}
}

// specOrder is a record with logical types, for
// TestASingleObjectBodyFromAnotherImplementationDecodes.
type (
	specLine struct {
		Qty int32 `avro:"qty"`
	}
	specOrder struct {
		ID    string     `avro:"id"`
		At    time.Time  `avro:"at"`
		Price *big.Rat   `avro:"price"`
		FX    *big.Rat   `avro:"fx"`
		State string     `avro:"state"`
		Lines []specLine `avro:"lines"`
		Next  *specOrder `avro:"next"`
		Same  *big.Rat   `avro:"same"`
	}
)

// TestASingleObjectBodyFromJavaDecodes is the reported defect (hunt 10
// mx10-11): the single-object fingerprint this codec checks is the CRC-64-AVRO
// of iqhive/avro's canonical form, which keeps logical types, where the
// Parsing Canonical Form Java's SchemaNormalization computes, and so the
// header Java writes, leaves them out. For any schema with a logical type,
// every body Java wrote was refused as ErrSchemaMismatch. Both fingerprints
// are accepted now. The one written, and SchemaID, are unchanged, so an iqmq
// consumer on an older release still reads this codec's bodies. (Python's
// avro and goavro write a primitive with a logical type as {"type":"long"} in
// the form, and their bodies for such a schema are still refused.)
func TestASingleObjectBodyFromJavaDecodes(t *testing.T) {
	// The value b71df49344e154d0 for "long" was worked out by the hunt 10
	// hunter's own CRC-64-AVRO, independent of the library's pkg/crc64 that
	// javaFingerprint uses: it pins that helper.
	if got := hexOf(javaFingerprint(`"long"`)); got != "b71df49344e154d0" {
		t.Fatalf("the CRC-64-AVRO of \"long\" = %s, want b71df49344e154d0", got)
	}
	// Each value is given as itself; a new one of its type is decoded into.
	fresh := func(v any) reflect.Value { return reflect.New(reflect.TypeOf(v)) }
	at := time.UnixMilli(1700000000123).UTC()
	for _, tc := range []struct {
		name, schema, canonical string
		value                   any
	}{
		{"timestamp-millis", `{"type":"long","logicalType":"timestamp-millis"}`, `"long"`, at},
		{
			"record",
			`{"type":"record","name":"Order","namespace":"iqmq.spec","doc":"an order","aliases":["Old"],"fields":[
  {"name":"id","type":{"type":"string","logicalType":"uuid"},"doc":"the id"},
  {"name":"at","type":{"type":"long","logicalType":"timestamp-micros"},"aliases":["when"]},
  {"name":"price","type":{"type":"bytes","logicalType":"decimal","precision":10,"scale":2}},
  {"name":"fx","type":{"type":"fixed","name":"Money","size":8,"logicalType":"decimal","precision":18,"scale":4}},
  {"name":"state","type":{"type":"enum","name":"State","symbols":["NEW","DONE"],"default":"NEW"}},
  {"name":"lines","type":{"type":"array","items":{"type":"record","name":"Line","fields":[
    {"name":"qty","type":"int","order":"descending","default":1}]}}},
  {"name":"next","type":["null","Order"],"default":null},
  {"name":"same","type":"iqmq.spec.Money"}]}`,
			`{"name":"iqmq.spec.Order","type":"record","fields":[{"name":"id","type":"string"},{"name":"at","type":"long"},` +
				`{"name":"price","type":"bytes"},{"name":"fx","type":{"name":"iqmq.spec.Money","type":"fixed","size":8}},` +
				`{"name":"state","type":{"name":"iqmq.spec.State","type":"enum","symbols":["NEW","DONE"]}},` +
				`{"name":"lines","type":{"type":"array","items":{"name":"iqmq.spec.Line","type":"record","fields":[` +
				`{"name":"qty","type":"int"}]}}},{"name":"next","type":["null","iqmq.spec.Order"]},` +
				`{"name":"same","type":"iqmq.spec.Money"}]}`,
			specOrder{
				ID: "6ba7b810-9dad-11d1-80b4-00c04fd430c8", At: time.UnixMicro(1700000000123456).UTC(),
				Price: big.NewRat(12345, 100), FX: big.NewRat(-5, 4), State: "DONE", Lines: []specLine{{2}, {3}},
				Next: &specOrder{Price: new(big.Rat), FX: new(big.Rat), Same: new(big.Rat), State: "NEW", Lines: []specLine{}},
				Same: big.NewRat(7, 1),
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := newCodec(t, iqavro.Config{Schema: tc.schema, Framing: iqavro.FramingSingleObject})
			body, err := c.Marshal(tc.value)
			if err != nil {
				t.Fatal(err)
			}
			if got, want := hexOf(body[2:10]), c.(codec.Schemer).SchemaID(); got != want {
				t.Fatalf("the codec wrote the fingerprint %s; SchemaID is %s", got, want)
			}
			spec := javaFingerprint(tc.canonical)
			if hexOf(spec) == c.(codec.Schemer).SchemaID() {
				t.Fatal("the schema's two fingerprints are the same, so this case shows nothing")
			}
			theirs := slices.Concat(body[:2], spec, body[10:])
			got := fresh(tc.value)
			if err := c.Unmarshal(theirs, got.Interface()); err != nil {
				t.Fatalf("a body with Java's fingerprint = %v", err)
			}
			again, err := c.Marshal(got.Elem().Interface())
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(again, body) {
				t.Fatalf("the body decoded to a value that encodes as %x, want %x", again, body)
			}
			other := slices.Concat(body[:2], javaFingerprint(`"bytes"`), body[10:])
			if err := c.Unmarshal(other, fresh(tc.value).Interface()); !reasonIs(err, iqavro.ErrSchemaMismatch) {
				t.Fatalf("a body with another schema's fingerprint = %v, want ErrSchemaMismatch", err)
			}
		})
	}
}

// javaFingerprint is the single-object fingerprint Java writes for a schema
// whose Parsing Canonical Form is canonical: its little-endian CRC-64-AVRO.
func javaFingerprint(canonical string) []byte {
	h := crc64.SumWithByteOrder([]byte(canonical), crc64.LittleEndian)
	return h[:]
}

// TestABadTargetIsReportedBeforeTheBody: a target that is not a non-nil
// pointer is the caller's error (codec.ErrNilTarget, InvalidArgument), and it
// is reported as one whatever the body holds. A body over the limit or one
// without its single-object header was refused first, as a message fault, so
// a handler with a bad target saw its corrupt messages dead-lettered and its
// good ones refused as its own bug.
func TestABadTargetIsReportedBeforeTheBody(t *testing.T) {
	c := newCodec(t, iqavro.Config{Schema: orderSchema, Framing: iqavro.FramingSingleObject, MaxBytes: 16})
	var nilPtr *order
	for _, body := range [][]byte{{0x00}, bytes.Repeat([]byte{0x02}, 17)} {
		for _, target := range []any{nil, order{}, nilPtr} {
			if err := c.Unmarshal(body, target); err != codec.ErrNilTarget { //nolint:errorlint // identity, see reasonIs
				t.Fatalf("Unmarshal(%x, %#v) = %v, want codec.ErrNilTarget", body, target, err)
			}
		}
	}
}

func TestTypedShape(t *testing.T) {
	c, err := iqavro.Typed[order](iqavro.Config{Schema: orderSchema})
	if err != nil {
		t.Fatal(err)
	}
	b, err := c.Encode(order{ID: "x", Qty: 1, Tags: []string{}, Attrs: []attr{}})
	if err != nil {
		t.Fatal(err)
	}
	v, err := c.Decode(b)
	if err != nil || v.ID != "x" || v.Qty != 1 {
		t.Fatalf("Decode = %+v, %v", v, err)
	}
}

func TestRegister(t *testing.T) {
	r := codec.NewRegistry()
	if err := iqavro.Register(r, iqavro.Config{Schema: orderSchema}); err != nil {
		t.Fatal(err)
	}
	if c, err := r.For(iqavro.ContentType); err != nil || c.ContentType() != iqavro.ContentType {
		t.Fatalf("For = %v, %v", c, err)
	}
}

type sliceWriter struct{ b *[]byte }

func (w *sliceWriter) Write(p []byte) (int, error) { *w.b = append(*w.b, p...); return len(p), nil }

// zigzagVarint encodes n as Avro does: zigzag, then a little-endian
// 7-bit-group varint.
func zigzagVarint(n int64) []byte {
	u := uint64(n) << 1
	if n < 0 {
		u = ^(uint64(n) << 1)
	}
	out := make([]byte, 0, 10)
	for {
		b := byte(u & 0x7f)
		u >>= 7
		if u == 0 {
			return append(out, b)
		}
		out = append(out, b|0x80)
	}
}

func hexOf(b []byte) string {
	const digits = "0123456789abcdef"
	out := make([]byte, 0, 2*len(b))
	for _, c := range b {
		out = append(out, digits[c>>4], digits[c&0xf])
	}
	return string(out)
}

// reasonIs reports whether err is a *codec.DecodeError whose own error IS
// want, by identity.
//
// errors.Is cannot answer this question. *codec.DecodeError.Is matches any
// target that carries the same portable code, and every sentinel in this
// package except ErrTooLarge and ErrNoSchema carries errcode.CorruptMessage --
// so errors.Is(<any corrupt body>, ErrFraming) is TRUE for every corrupt body,
// and an assertion written that way passes whatever the codec did. Watched:
// with the datum codec's refusal of a single-object body removed,
// TestFramingIsConfiguredNotDetected still PASSED, because the length error
// the record decoder produced instead was also a CorruptMessage.
// That refusal has since been removed on purpose -- it
// refused the codec's own output, -- and the test now
// asserts that a datum codec never unwraps a single-object body; the identity
// comparison is still what keeps its ErrFraming checks honest on the
// single-object side.
func reasonIs(err, want error) bool {
	return codec.Reason(err) == want //nolint:errorlint // identity is the question; errors.Is answers a different one
}
