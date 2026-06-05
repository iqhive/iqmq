package avro_test

import (
	"strings"
	"testing"

	"bitbucket.org/iqhive/iqmq/codec"
	iqavro "bitbucket.org/iqhive/iqmq/codec/avro"
)

var allocSink []byte

// TestAllocs pins the allocation counts of the append and decode paths under
// both framings.
//
// Both are asserted at exactly zero, which is the strongest result of the four
// codec modules and is worth stating plainly: hamba/avro encodes through a
// pooled Writer straight into the caller's buffer and decodes a record into a
// caller-owned struct without allocating, so a publish and a receive through
// this codec cost nothing beyond the body. Single-object framing's ten-byte
// header is a memcpy of a precomputed slice and does not change that.
//
// Zero exactly, not a bound: there is no library allocation to absorb here, so
// a bound would hide the day one appears.
func TestAllocs(t *testing.T) {
	if allocsRaceDetector {
		t.Skip("the race detector perturbs allocation counts; `make allocs` runs without it")
	}

	datum := newCodecFor(t, iqavro.FramingDatum)
	sobj := newCodecFor(t, iqavro.FramingSingleObject)
	v := &order{ID: "a", Qty: 2, Tags: []string{"x"}, Attrs: []attr{{"k", "v"}}}
	big := &order{ID: strings.Repeat("x", 4096), Tags: []string{}, Attrs: []attr{}}
	buf := make([]byte, 0, 8192)

	type arm struct {
		name string
		c    codec.Codec
	}
	// The tight-limit codec is built ONCE, outside every measured closure:
	// the first version of this test called iqavro.New inside the function
	// AllocsPerRun measures and reported 840 allocs/op for a path that
	// allocates none -- it was measuring a schema parse.
	tight := newCodecWith(t, iqavro.Config{Schema: orderSchema, MaxBytes: 1})

	for _, a := range []arm{{"datum", datum}, {"sobj", sobj}} {
		app := a.c.(codec.Appender)
		body, err := a.c.Marshal(v)
		if err != nil {
			t.Fatal(err)
		}
		var into order
		cases := []struct {
			name string
			fn   func()
		}{
			{"AppendMarshal/order", func() { allocSink, _ = app.AppendMarshal(buf[:0], v) }},
			{"AppendMarshal/4KiB", func() { allocSink, _ = app.AppendMarshal(buf[:0], big) }},
			{"Unmarshal/order", func() { _ = a.c.Unmarshal(body, &into) }},
			// A refused body must not cost an allocation either, or a hostile
			// sender buys one per rejected message.
			{"Unmarshal/refused", func() { _ = tight.Unmarshal(body, &into) }},
			{"ContentType", func() { _ = a.c.ContentType() }},
			{"SchemaID", func() { _ = a.c.(codec.Schemer).SchemaID() }},
		}
		for _, tc := range cases {
			t.Run(a.name+"/"+tc.name, func(t *testing.T) {
				if got := testing.AllocsPerRun(200, tc.fn); got != 0 {
					t.Fatalf("%s/%s: %.1f allocs/op, want 0", a.name, tc.name, got)
				}
			})
		}
	}
}

func newCodecFor(t *testing.T, f iqavro.Framing) codec.Codec {
	t.Helper()
	return newCodecWith(t, iqavro.Config{Schema: orderSchema, Framing: f})
}

func newCodecWith(t *testing.T, c iqavro.Config) codec.Codec {
	t.Helper()
	got, err := iqavro.New(c)
	if err != nil {
		t.Fatal(err)
	}
	return got
}
