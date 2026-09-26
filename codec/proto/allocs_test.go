package proto_test

import (
	"strings"
	"testing"

	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/iqhive/iqmq/codec"
	iqproto "github.com/iqhive/iqmq/codec/proto"
)

// allocSink keeps a result live so the compiler cannot elide the work.
var allocSink []byte

// TestAllocs pins the allocation count of the append path, which is the reason
// codec.Appender exists: a publisher encodes into a pooled buffer, and a codec
// that allocated per message would put the specification's "core-attributable allocations
// per message <= 2 (0 with reuse)" out of reach before the core had spent one.
//
// Zero is asserted exactly rather than as a bound. protobuf's MarshalAppend
// writes straight into the caller's slice and the size check ahead of it is
// arithmetic, so there is nothing here that should ever allocate; a bound
// would hide the day that stops being true.
//
// `make allocs` runs this in every module, which is how a nested module's
// allocation test gets run at all.
func TestAllocs(t *testing.T) {
	c := iqproto.New()
	a := c.(codec.Appender)
	small := wrapperspb.String("hello")
	large := wrapperspb.String(strings.Repeat("x", 4096))
	buf := make([]byte, 0, 8192)
	body, err := c.Marshal(small)
	if err != nil {
		t.Fatal(err)
	}
	var into wrapperspb.StringValue
	// Built once, outside the measured closure: a constructor inside it
	// measures the constructor.
	tight := iqproto.NewWithConfig(iqproto.Config{MaxBytes: 1})

	cases := []struct {
		name string
		want float64
		fn   func()
	}{
		{"AppendMarshal/64B", 0, func() { allocSink, _ = a.AppendMarshal(buf[:0], small) }},
		{"AppendMarshal/4KiB", 0, func() { allocSink, _ = a.AppendMarshal(buf[:0], large) }},
		// An over-size body is refused before the decoder runs, so the
		// refusal itself must not allocate either: a hostile sender would
		// otherwise buy an allocation per rejected message.
		{"Unmarshal/refused", 0, func() { _ = tight.Unmarshal(body, &into) }},
		{"ContentType", 0, func() { _ = c.ContentType() }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := testing.AllocsPerRun(200, tc.fn); got != tc.want {
				t.Fatalf("%s: %.1f allocs/op, want %.0f", tc.name, got, tc.want)
			}
		})
	}
}
