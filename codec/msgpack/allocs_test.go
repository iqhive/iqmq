package msgpack_test

import (
	"strings"
	"testing"

	"github.com/iqhive/iqmq/codec"
	iqmsgpack "github.com/iqhive/iqmq/codec/msgpack"
)

var allocSink []byte

// TestAllocs pins the allocation counts of the two paths whose cost this
// package is responsible for.
//
// **Validate is asserted at exactly zero**, and that is the load-bearing half:
// it is the pre-allocation check, it runs on every delivery before the
// decoder, and a validator that allocated in proportion to the body it is
// protecting against would defeat its own purpose. It walks the bytes with one
// fixed-size array on the stack for the container stack, so zero is a
// property and not a coincidence.
//
// The append path is asserted as an upper bound with the measured figure in
// the message, because the allocations in it belong to
// github.com/vmihailenco/msgpack/v5's struct encoder rather than to this
// package, and a library upgrade that moved the number by one should not be a
// red gate — while one that moved it by ten should.
func TestAllocs(t *testing.T) {
	c := iqmsgpack.New()
	a := c.(codec.Appender)
	v := &order{ID: "a", Qty: 2, Tags: []string{"x"}, Meta: map[string]string{"k": "v"}}
	big := &order{ID: strings.Repeat("x", 4096)}
	body, err := c.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	deep := make([]byte, 0, 80)
	for range 40 {
		deep = append(deep, 0x91)
	}
	deep = append(deep, 0xc0)
	buf := make([]byte, 0, 8192)
	cfg := iqmsgpack.Config{}

	exact := []struct {
		name string
		fn   func()
	}{
		{"Validate/order", func() { _ = iqmsgpack.Validate(body, cfg) }},
		{"Validate/40-deep", func() { _ = iqmsgpack.Validate(deep, cfg) }},
		{"Validate/hostile", func() { _ = iqmsgpack.Validate([]byte{0xdd, 0xff, 0xff, 0xff, 0xff}, cfg) }},
		{"ContentType", func() { _ = c.ContentType() }},
	}
	for _, tc := range exact {
		t.Run(tc.name, func(t *testing.T) {
			if got := testing.AllocsPerRun(200, tc.fn); got != 0 {
				t.Fatalf("%s: %.1f allocs/op, want 0: the validator must not allocate from the body "+
					"it is protecting against", tc.name, got)
			}
		})
	}

	bounded := []struct {
		name     string
		measured int
		max      float64
		fn       func()
	}{
		{"AppendMarshal/order", 2, 6, func() { allocSink, _ = a.AppendMarshal(buf[:0], v) }},
		{"AppendMarshal/4KiB", 1, 6, func() { allocSink, _ = a.AppendMarshal(buf[:0], big) }},
	}
	for _, tc := range bounded {
		t.Run(tc.name, func(t *testing.T) {
			got := testing.AllocsPerRun(200, tc.fn)
			if got > tc.max {
				t.Fatalf("%s: %.1f allocs/op, over the %.0f ceiling (it measured %d when this test was "+
					"written; the allocations belong to the library's struct encoder)",
					tc.name, got, tc.max, tc.measured)
			}
			t.Logf("%s: %.1f allocs/op (%d when written, ceiling %.0f)", tc.name, got, tc.measured, tc.max)
		})
	}
}
