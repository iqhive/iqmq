package msgpack_test

import (
	"bytes"
	"errors"
	"runtime"
	"strconv"
	"strings"
	"testing"

	mp "github.com/vmihailenco/msgpack/v5"

	"github.com/iqhive/iqmq/codec"
	"github.com/iqhive/iqmq/codec/codectest"
	iqmsgpack "github.com/iqhive/iqmq/codec/msgpack"
	"github.com/iqhive/iqmq/internal/errcode"
)

type order struct {
	ID   string            `msgpack:"id"`
	Qty  int               `msgpack:"qty"`
	Tags []string          `msgpack:"tags"`
	Meta map[string]string `msgpack:"meta"`
}

func TestContract(t *testing.T) {
	want := order{
		ID:   "a",
		Qty:  2,
		Tags: []string{"x", "y"},
		Meta: map[string]string{"z": "1", "a": "2"},
	}
	codectest.Suite{
		Codec:       iqmsgpack.New(),
		ContentType: iqmsgpack.ContentType,
		Cases: []codectest.Case{
			{
				Name:      "order",
				Value:     &want,
				NewTarget: func() any { return new(order) },
				Check: func(target any) error {
					got := target.(*order)
					if got.ID != want.ID || got.Qty != want.Qty ||
						strings.Join(got.Tags, ",") != "x,y" ||
						got.Meta["a"] != "2" || got.Meta["z"] != "1" {
						return errors.New("round trip lost a field: " + got.ID + "/" + strconv.Itoa(got.Qty))
					}
					return nil
				},
			},
			{
				Name:      "nil",
				Value:     nil,
				NewTarget: func() any { return new(any) },
				Check: func(target any) error {
					if v := *target.(*any); v != nil {
						return errors.New("nil decoded as non-nil")
					}
					return nil
				},
				Wire: []byte{0xc0},
			},
			{
				Name:      "bytes",
				Value:     []byte{1, 2, 3},
				NewTarget: func() any { return new([]byte) },
				Check: func(target any) error {
					if !bytes.Equal(*target.(*[]byte), []byte{1, 2, 3}) {
						return errors.New("round trip lost the bytes")
					}
					return nil
				},
				Wire: []byte{0xc4, 0x03, 1, 2, 3},
			},
		},
		Unencodable: []any{make(chan int), func() {}},
		Corrupt: [][]byte{
			{},
			{0xc1},                         // the reserved header byte
			{0xdd, 0xff, 0xff, 0xff, 0xff}, // array32 of 4,294,967,295
			{0xdf, 0xff, 0xff, 0xff, 0xff}, // map32 of 4,294,967,295
			{0xc6, 0xff, 0xff, 0xff, 0xff}, // bin32 of 4 GiB
			{0xdb, 0x7f, 0xff, 0xff, 0xff}, // str32 of 2 GiB
			{0xa5, 'h', 'i'},               // fixstr with a truncated payload
			{0x91},                         // fixarray with no element
			{0xc0, 0xc0},                   // a second body after the first
		},
	}.Run(t)
}

// hostile are the bodies that made the library allocate. Each is at most five
// bytes.
var hostile = []struct {
	name string
	body []byte
}{
	{"array32/4G", []byte{0xdd, 0xff, 0xff, 0xff, 0xff}},
	{"map32/4G", []byte{0xdf, 0xff, 0xff, 0xff, 0xff}},
	{"bin32/4GiB", []byte{0xc6, 0xff, 0xff, 0xff, 0xff}},
	{"str32/2GiB", []byte{0xdb, 0x7f, 0xff, 0xff, 0xff}},
	{"array16/64K", []byte{0xdc, 0xff, 0xff}},
	{"map16/64K", []byte{0xde, 0xff, 0xff}},
	{"nested array32", []byte{0x91, 0xdd, 0xff, 0xff, 0xff, 0xff}},
}

// TestHostileCountIsRefusedAtTheHeader is the specification's "decoders enforce size and
// count limits before allocation" for MessagePack, which is the format where
// it is not a formality.
//
// The check is the validator's `pending > remaining` rule: every value costs
// at least one byte, so a container claiming more elements than the input can
// hold is refused at the header. Measured, on
// github.com/vmihailenco/msgpack/v5 v5.4.1, with the validator removed:
// `dd ff ff ff ff` allocated 64 GiB before returning EOF, and the map32 of the
// same shape ended the process with `signal: killed`.
//
// TestTheLibraryStillAllocatesFromAnUnvalidatedCount is the other half — it
// shows the library doing it, at a size that is safe to run — so that a
// library upgrade making the validator redundant is visible instead of
// leaving a check that cannot fail.
func TestHostileCountIsRefusedAtTheHeader(t *testing.T) {
	c := iqmsgpack.New()
	for _, h := range hostile {
		t.Run(h.name, func(t *testing.T) {
			var before, after runtime.MemStats
			runtime.ReadMemStats(&before)
			var v any
			err := c.Unmarshal(h.body, &v)
			runtime.ReadMemStats(&after)

			var de *codec.DecodeError
			if !errors.As(err, &de) {
				t.Fatalf("Unmarshal(%x) = %v, want a *codec.DecodeError", h.body, err)
			}
			if !reasonIs(err, iqmsgpack.ErrMalformed) {
				t.Fatalf("Unmarshal(%x) = %v, want ErrMalformed", h.body, err)
			}
			// A %d-byte body must not have cost a measurable allocation. The
			// bound is generous (1 MiB) because the runtime allocates for its
			// own reasons; the defect it excludes is measured in gigabytes.
			if grew := after.TotalAlloc - before.TotalAlloc; grew > 1<<20 {
				t.Fatalf("a %d-byte body allocated %d bytes: the count was not refused at the header",
					len(h.body), grew)
			}
			if v != nil {
				t.Fatalf("the decoder ran anyway: target holds %#v", v)
			}
		})
	}
}

// TestTheLibraryStillAllocatesFromAnUnvalidatedCount is the reason the
// validator exists, kept as a running test at a size that cannot hurt the
// host: an array16 claiming 65,535 elements from a three-byte body.
//
// If a future version of github.com/vmihailenco/msgpack/v5 stops allocating
// from a count the input cannot justify, this test fails and the package
// comment's claim has to be rewritten. That is the point: the alternative is a
// validator nobody can tell is still needed.
func TestTheLibraryStillAllocatesFromAnUnvalidatedCount(t *testing.T) {
	body := []byte{0xdc, 0xff, 0xff} // array16, 65,535 elements, no payload
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	var v any
	err := mp.Unmarshal(body, &v)
	runtime.ReadMemStats(&after)
	grew := after.TotalAlloc - before.TotalAlloc
	t.Logf("library: %d-byte body, err=%v, allocated %d bytes", len(body), err, grew)
	if err == nil {
		t.Fatal("the library accepted a 3-byte body claiming 65,535 elements")
	}
	// 65,535 interface values is about 1 MiB; anything of that order proves
	// the allocation happened before the count was checked.
	if grew < 512<<10 {
		t.Fatalf("the library allocated only %d bytes from a count of 65,535, so it now checks the count "+
			"against the input and codec/msgpack's validator may be redundant; re-read the package comment", grew)
	}
}

// TestLimitsAreCheckedBeforeTheDecoder covers the other three limits with
// bodies that are VALID and would decode cleanly if the limit permitted them,
// so what the test observes is each limit and not a malformed body.
func TestLimitsAreCheckedBeforeTheDecoder(t *testing.T) {
	// A 4 KiB string body.
	big, err := iqmsgpack.New().Marshal(strings.Repeat("x", 4096))
	if err != nil {
		t.Fatal(err)
	}
	var s string
	if err := iqmsgpack.NewWithConfig(iqmsgpack.Config{MaxBytes: 1024}).Unmarshal(big, &s); err != iqmsgpack.ErrTooLarge { //nolint:errorlint // identity, see reasonIs
		t.Fatalf("a %d-byte body at MaxBytes=1024 = %v, want ErrTooLarge", len(big), err)
	}
	if s != "" {
		t.Fatal("the decoder ran anyway")
	}
	if err := iqmsgpack.NewWithConfig(iqmsgpack.Config{MaxBytes: 8192}).Unmarshal(big, &s); err != nil || len(s) != 4096 {
		t.Fatalf("the same body at MaxBytes=8192 = %d bytes, %v", len(s), err)
	}

	// 64 elements in one array.
	list := make([]int, 64)
	body, err := iqmsgpack.New().Marshal(list)
	if err != nil {
		t.Fatal(err)
	}
	var got []int
	if err := iqmsgpack.NewWithConfig(iqmsgpack.Config{MaxElements: 8}).Unmarshal(body, &got); err != iqmsgpack.ErrTooManyElements { //nolint:errorlint // identity, see reasonIs
		t.Fatalf("65 values at MaxElements=8 = %v, want ErrTooManyElements", err)
	}
	if got != nil {
		t.Fatal("the decoder ran anyway")
	}
	if err := iqmsgpack.NewWithConfig(iqmsgpack.Config{MaxElements: 128}).Unmarshal(body, &got); err != nil || len(got) != 64 {
		t.Fatalf("the same body at MaxElements=128 = %d values, %v", len(got), err)
	}

	// 40 levels of nesting.
	deep := make([]byte, 0, 41)
	for range 40 {
		deep = append(deep, 0x91)
	}
	deep = append(deep, 0xc0)
	var any1 any
	if err := iqmsgpack.NewWithConfig(iqmsgpack.Config{MaxDepth: 8}).Unmarshal(deep, &any1); err != iqmsgpack.ErrTooDeep { //nolint:errorlint // identity, see reasonIs
		t.Fatalf("40 levels at MaxDepth=8 = %v, want ErrTooDeep", err)
	}
	if err := iqmsgpack.NewWithConfig(iqmsgpack.Config{MaxDepth: 64}).Unmarshal(deep, &any1); err != nil {
		t.Fatalf("the same body at MaxDepth=64: %v", err)
	}

	// The three count limits are MessageTooLarge and not corrupt-message: a
	// body the codec declines to decode is a different fact from one it
	// cannot.
	for _, e := range []error{iqmsgpack.ErrTooLarge, iqmsgpack.ErrTooManyElements, iqmsgpack.ErrTooDeep} {
		if code := errcode.CodeOf(e); code != errcode.MessageTooLarge {
			t.Fatalf("%v carries %v, want MessageTooLarge: a limit is not corruption", e, code)
		}
	}
}

// TestValidateIsIterative pins the property that makes the depth check safe:
// the validator does not recurse, so a body nested a million levels deep is
// refused rather than exhausting the stack while the depth limit is being
// applied.
func TestValidateIsIterative(t *testing.T) {
	deep := make([]byte, 1<<20)
	for i := range deep {
		deep[i] = 0x91 // fixarray of 1
	}
	if err := iqmsgpack.Validate(deep, iqmsgpack.Config{MaxDepth: -1, MaxElements: -1}); err != iqmsgpack.ErrMalformed { //nolint:errorlint // identity, see reasonIs
		// The body is truncated (a million opens and no leaf), so ErrMalformed
		// is the right answer; what matters is that it is AN ANSWER.
		t.Fatalf("Validate of a 1 Mi-level nesting = %v, want ErrMalformed", err)
	}
}

func TestDeterministicMapOrder(t *testing.T) {
	c := iqmsgpack.New()
	m := map[string]any{"z": 1, "a": 2, "m": 3, "b": 4, "y": 5, "c": 6}
	first, err := c.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	for range 64 {
		again, err := c.Marshal(m)
		if err != nil || !bytes.Equal(first, again) {
			t.Fatalf("map encoding is not deterministic: %x vs %x (%v)", first, again, err)
		}
	}
	// The keys really are sorted, not merely stable.
	if i, j := bytes.IndexByte(first, 'a'), bytes.IndexByte(first, 'z'); i > j {
		t.Fatalf("map keys are not sorted: %x", first)
	}
}

func TestTypedShape(t *testing.T) {
	c := iqmsgpack.Typed[order]()
	b, err := c.Encode(order{ID: "x", Qty: 1})
	if err != nil {
		t.Fatal(err)
	}
	v, err := c.Decode(b)
	if err != nil || v.ID != "x" || v.Qty != 1 {
		t.Fatalf("Decode = %+v, %v", v, err)
	}
	pc := iqmsgpack.Typed[*order]()
	pb, err := pc.Encode(&order{ID: "y"})
	if err != nil {
		t.Fatalf("Encode through a pointer T: %v", err)
	}
	pv, err := pc.Decode(pb)
	if err != nil || pv == nil || pv.ID != "y" {
		t.Fatalf("Decode through a pointer T = %+v, %v", pv, err)
	}
}

func TestRegister(t *testing.T) {
	r := codec.NewRegistry()
	if err := iqmsgpack.Register(r); err != nil {
		t.Fatal(err)
	}
	for _, ct := range []string{iqmsgpack.ContentType, iqmsgpack.ContentTypeVendor, iqmsgpack.ContentTypeX} {
		if c, err := r.For(ct); err != nil || c.ContentType() != iqmsgpack.ContentType {
			t.Fatalf("For(%q) = %v, %v", ct, c, err)
		}
	}
}

// TestValidateAcceptsEveryHeaderTheEncoderProduces is the validator's
// completeness check: a validator that refused a body its own encoder writes
// would make the codec unable to decode itself, and a table of hand-written
// bodies would not notice.
func TestValidateAcceptsEveryHeaderTheEncoderProduces(t *testing.T) {
	values := make([]any, 0, 40)
	values = append(values,
		nil, true, false, 0, 1, -1, 127, -32, 128, 255, 256, 65535, 65536,
		int64(1)<<40, int64(-1)<<40, 1.5, float32(1.5),
		"", "x", strings.Repeat("s", 40), strings.Repeat("s", 400), strings.Repeat("s", 70000),
		[]byte{},
		[]byte{1},
		make([]byte, 300), make([]byte, 70000),
		[]any{},
		[]any{1, 2, 3},
		make([]any, 40), make([]any, 400),
		map[string]any{},
		map[string]any{"a": 1},
		map[string]any{"a": []any{1, map[string]any{"b": 2}}},
	)
	big := map[string]any{}
	for i := range 40 {
		big["k"+strconv.Itoa(i)] = i
	}
	values = append(values, big)
	c := iqmsgpack.New()
	for i, v := range values {
		body, err := c.Marshal(v)
		if err != nil {
			t.Fatalf("value %d (%T): Marshal: %v", i, v, err)
		}
		if err := iqmsgpack.Validate(body, iqmsgpack.Config{}); err != nil {
			t.Fatalf("value %d (%T): the validator refused its own encoder's output %x: %v", i, v, body, err)
		}
		var into any
		if err := c.Unmarshal(body, &into); err != nil {
			t.Fatalf("value %d (%T): %v", i, v, err)
		}
	}
	// A time.Time exercises the ext family, which is the one header group the
	// encoder reaches only for a special type.
	ext, err := mp.Marshal(map[string]any{"at": mp.RawMessage{0xd6, 0xff, 0, 0, 0, 0}})
	if err != nil {
		t.Fatal(err)
	}
	if err := iqmsgpack.Validate(ext, iqmsgpack.Config{}); err != nil {
		t.Fatalf("the validator refused a fixext4 timestamp %x: %v", ext, err)
	}
}

// TestMapDeterminismStopsAtTheMapTypesTheLibrarySorts pins the limitation the
// package comment states, from both sides.
//
// github.com/vmihailenco/msgpack/v5's SetSortMapKeys documents support for
// map[string]string, map[string]bool and map[string]interface{} and for
// nothing else. So a map[string]int encodes in Go's randomised iteration
// order, and this codec cannot promise a body that is a function of the value
// for one. Asserting the negative is deliberate: a library release that
// widened the support would fail this test and force the doc comment to be
// rewritten, which is the only way a stale limitation gets noticed.
func TestMapDeterminismStopsAtTheMapTypesTheLibrarySorts(t *testing.T) {
	c := iqmsgpack.New()

	// The supported three are stable over many calls.
	supported := []any{
		map[string]string{"z": "1", "a": "2", "m": "3", "b": "4", "y": "5", "c": "6", "d": "7", "e": "8"},
		map[string]bool{"z": true, "a": false, "m": true, "b": false, "y": true, "c": false, "d": true, "e": false},
		map[string]any{"z": 1, "a": 2, "m": 3, "b": 4, "y": 5, "c": 6, "d": 7, "e": 8},
	}
	for _, m := range supported {
		first, err := c.Marshal(m)
		if err != nil {
			t.Fatal(err)
		}
		for range 200 {
			again, err := c.Marshal(m)
			if err != nil || !bytes.Equal(first, again) {
				t.Fatalf("%T is documented as sorted and is not: %x vs %x (%v)", m, first, again, err)
			}
		}
	}

	// A map type the library does not sort. Sixteen keys have 16! orderings,
	// so 200 identical encodings would not happen by chance.
	unsupported := map[string]int{}
	for i := range 16 {
		unsupported["k"+strconv.Itoa(i)] = i
	}
	seen := map[string]bool{}
	for range 200 {
		b, err := c.Marshal(unsupported)
		if err != nil {
			t.Fatal(err)
		}
		seen[string(b)] = true
	}
	if len(seen) == 1 {
		t.Fatalf("map[string]int encoded identically 200 times, so github.com/vmihailenco/msgpack/v5 now sorts it: "+
			"widen the determinism claim in the package comment (saw %d distinct encodings)", len(seen))
	}
	t.Logf("map[string]int: %d distinct encodings of one value over 200 calls", len(seen))
}

// reasonIs reports whether err is a *codec.DecodeError whose own error IS
// want, by identity.
//
// errors.Is cannot answer this question: *codec.DecodeError.Is matches any
// target carrying the same portable code, so errors.Is(<any corrupt body>,
// ErrMalformed) is TRUE for every corrupt body and an assertion written that
// way passes whatever the codec did. The same holds among this package's three
// MessageTooLarge sentinels, which are compared by identity for the same
// reason.
func reasonIs(err, want error) bool {
	return codec.Reason(err) == want //nolint:errorlint // identity is the question; errors.Is answers a different one
}
