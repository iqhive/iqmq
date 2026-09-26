package cloudevents_test

import (
	"testing"

	"github.com/cloudevents/sdk-go/v2/event"

	"github.com/iqhive/iqmq/codec"
	ce "github.com/iqhive/iqmq/codec/cloudevents"
)

var allocSink []byte

// TestAllocs pins this codec's allocation counts as BOUNDS with the measured
// figures named, and deliberately asserts nothing at zero for the encode or
// decode paths — because nothing here can be zero and saying so is the honest
// result.
//
// The figures named are what `testing.AllocsPerRun` reports, which is a
// couple higher than the `-benchmem` column for the same call: AllocsPerRun
// runs at GOMAXPROCS=1 and counts the closure's own boxing. The ceiling is
// about twice the measured figure, so a doubling is a finding and a rounding
// is not.
//
// Structured mode is `event.Event.MarshalJSON`, which builds a JSON object
// through the SDK's own writer; binary mode builds a header slice and formats
// every attribute through `types.Format`. Both allocate by construction, an
// order of magnitude more than the other three codec modules, and that is the
// cost of CloudEvents rather than a defect: a body that carries eight context
// attributes and a data content type has eight strings in it wherever they
// come from.
//
// What IS asserted at zero is the pair of refusals: an over-size body and an
// over-attribute delivery are rejected before anything is built, so a hostile
// sender does not buy an allocation per rejected message. That is the specification's
// requirement stated as a cost rather than as a control flow.
func TestAllocs(t *testing.T) {
	structured := ce.New()
	app := structured.(codec.Appender)
	binding := ce.NewBinding()
	e := sample(t)
	body, err := structured.Marshal(e)
	if err != nil {
		t.Fatal(err)
	}
	bin, headers, err := binding.Encode(e)
	if err != nil {
		t.Fatal(err)
	}
	tight := ce.NewWithConfig(ce.Config{MaxBytes: 1})
	tightBinding := ce.NewBindingWithConfig(ce.Config{MaxBytes: 1, MaxAttributes: 1})
	buf := make([]byte, 0, 4096)
	var into event.Event

	zero := []struct {
		name string
		fn   func()
	}{
		{"Unmarshal/over-size", func() { _ = tight.Unmarshal(body, &into) }},
		{"Binding.Decode/over-size", func() { _ = tightBinding.Decode(bin, headers, &into) }},
		{"ContentType", func() { _ = structured.ContentType() }},
		{"Prefix", func() { _ = binding.Prefix() }},
	}
	for _, tc := range zero {
		t.Run(tc.name, func(t *testing.T) {
			if got := testing.AllocsPerRun(200, tc.fn); got != 0 {
				t.Fatalf("%s: %.1f allocs/op, want 0: a refusal must not cost an allocation, or a hostile "+
					"sender buys one per rejected message", tc.name, got)
			}
		})
	}

	bounded := []struct {
		name     string
		measured int
		max      float64
		fn       func()
	}{
		{"Marshal", 13, 28, func() { allocSink, _ = structured.Marshal(e) }},
		{"AppendMarshal", 13, 28, func() { allocSink, _ = app.AppendMarshal(buf[:0], e) }},
		{"Unmarshal", 42, 88, func() { _ = structured.Unmarshal(body, &into) }},
		{"Binding.Encode", 23, 48, func() { allocSink, _, _ = binding.Encode(e) }},
		{"Binding.Decode", 15, 32, func() { _ = binding.Decode(bin, headers, &into) }},
	}
	for _, tc := range bounded {
		t.Run(tc.name, func(t *testing.T) {
			got := testing.AllocsPerRun(200, tc.fn)
			if got > tc.max {
				t.Fatalf("%s: %.1f allocs/op, over the %.0f ceiling (it measured %d when this test was "+
					"written; the ceiling is about twice that, so a doubling is a finding and a rounding is not)",
					tc.name, got, tc.max, tc.measured)
			}
			t.Logf("%s: %.1f allocs/op (%d when written, ceiling %.0f)", tc.name, got, tc.measured, tc.max)
		})
	}

	// The comparison the page makes, asserted rather than left to a reader:
	// CloudEvents costs an order of magnitude more per message than the other
	// three codecs, and that has to stay visible.
	if got := testing.AllocsPerRun(200, func() { allocSink, _ = structured.Marshal(e) }); got < 4 {
		t.Fatalf("structured Marshal now costs %.1f allocs/op; if the SDK has become allocation-light, "+
			"the package comment's comparison with the other three codec modules is stale", got)
	}
}
