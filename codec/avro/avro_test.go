package avro_test

import (
	"bytes"
	"errors"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"

	hamba "github.com/hamba/avro/v2"

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
// hamba/avro's frozenConfig.Unmarshal returns nil when the reader's error is
// io.EOF, so every field that ran off the end of the body is left at its zero
// value and the caller is told the decode succeeded. Both halves are here: the
// library doing it, and this codec refusing to.
func TestTruncatedBodyIsCorruptAndNotZeroes(t *testing.T) {
	schema := hamba.MustParse(orderSchema)
	c := newCodec(t, iqavro.Config{Schema: orderSchema})

	for _, body := range [][]byte{{}, {0x02, 'a'}, {0x02, 'a', 0x02}} {
		// The library: a successful decode of a body that ended early.
		var lib order
		if err := hamba.Unmarshal(schema, body, &lib); err != nil {
			t.Logf("library rejected %x: %v", body, err)
			continue
		}
		t.Logf("library accepted the %d-byte body %x as %+v", len(body), body, lib)

		// This codec: corrupt.
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
	if err := hamba.Unmarshal(hamba.MustParse(orderSchema), doubled, &lib); err != nil {
		t.Fatalf("the library rejected two datums itself (%v), so this check is not the thing refusing them", err)
	}
}

// TestArrayElementCountIsBoundedBeforeAllocation is the specification's "count limits before
// allocation" for Avro. An array block count is an attacker-chosen zigzag
// varint, hamba/avro's own bound is 1<<48, and the allocation happens before
// any element is read. The walk refuses the count before the library sees it;
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

// TestTheLibraryDefaultAllocatesFromTheBlockCount pins the reason this package
// never uses hamba/avro's DefaultConfig, at a size that is safe to allocate.
// The 8.8-terabyte measurement in the package comment is not repeated as a
// test, because a test that allocates 8.8 TB kills the host.
//
// The library is driven directly. Through the codec, even with its 1<<48
// restored, the walk now refuses this body before the library sees it, so the
// codec can no longer show what the library does.
func TestTheLibraryDefaultAllocatesFromTheBlockCount(t *testing.T) {
	hostile := zigzagVarint(1 << 20)
	var got []int64
	err := hamba.Unmarshal(hamba.MustParse(longsSchema), hostile, &got)
	if err != nil && strings.Contains(err.Error(), "MaxSliceAllocSize") {
		t.Fatalf("hamba/avro now bounds array allocation at its own default (%v), so "+
			"Config.MaxSliceElements may be redundant; re-read the package comment", err)
	}
	// The body has no payload, so the library runs off its end, but only
	// AFTER it has allocated the slice, which is the point.
	if cap(got) < 1<<20 {
		t.Fatalf("the library's default allocated cap=%d from a %d-byte body claiming %d elements; "+
			"if the library no longer allocates ahead of the payload, the limit is redundant",
			cap(got), len(hostile), 1<<20)
	}
	t.Logf("library default: a %d-byte body claiming %d elements allocated cap=%d, then %v",
		len(hostile), 1<<20, cap(got), err)

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
	want, err := hamba.MustParse(orderSchema).FingerprintUsing(hamba.CRC64AvroLE)
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

// TestAFailedEncodeDoesNotPoisonTheCodec is hamba/avro's
// Writer keeps its error and Encoder.Reset does not clear it, so a pooled
// encoder that has once failed reports that failure for ever.
func TestAFailedEncodeDoesNotPoisonTheCodec(t *testing.T) {
	// The library, first: a second Encode after a failure still reports the
	// first failure. If this stops being true the discard-on-error in
	// AppendMarshal is dead code.
	schema := hamba.MustParse(orderSchema)
	var buf []byte
	w := &sliceWriter{&buf}
	e := hamba.NewEncoderForSchema(schema, w)
	if err := e.Encode(make(chan int)); err == nil {
		t.Fatal("the library encoded a chan")
	}
	e.Reset(w)
	if err := e.Encode(order{ID: "a", Tags: []string{}, Attrs: []attr{}}); err == nil {
		t.Fatal("hamba/avro's Encoder no longer keeps its error across Reset, so codec/avro's " +
			"discard-on-error is dead code; re-read ")
	} else {
		t.Logf("library, good value after a failure and a Reset: %v", err)
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
