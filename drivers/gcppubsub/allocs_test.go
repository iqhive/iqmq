package gcppubsub_test

import (
	"strings"
	"testing"

	mq "bitbucket.org/iqhive/iqmq"
	gps "bitbucket.org/iqhive/iqmq/drivers/gcppubsub"
)

// TestAllocsMappingIsBoundedPerPublication pins what the pure mapping costs.
//
// It is a BOUND rather than a budget of zero, and the difference is the
// broker's: a Pub/Sub publication is a protobuf message with a `map<string,
// string>` of attributes, so the attribute map is allocated per publication by
// construction and nothing a driver does removes it. What the bound is for is
// the other direction — a mapping that started allocating per HEADER, or that
// built a second message, or that formatted a string on the hot path, would
// cross it and this is what notices.
//
// the zero-allocation rows are the core's (`internal/wire`,
// `driverkit/offset`, the middleware chain); the specification asks a driver for its own
// numbers and this is one of them. Each bound is the measured figure plus a
// little headroom rather than an exact count, because an exact count would
// fail on a Go release that changes how a small map is built — and rather than
// a generous round number, because a bound nothing can cross is a check that
// cannot fail. The measured figures are logged on every run.
func TestAllocsMappingIsBoundedPerPublication(t *testing.T) {
	if testing.Short() {
		t.Skip("allocation counts are perturbed by -short's shorter warm-up")
	}
	for _, tc := range []struct {
		name string
		msg  func() *mq.Message
		max  float64
	}{
		{
			// A body and nothing else: one attribute map, one marker-free
			// publication.
			name: "body-only",
			msg:  func() *mq.Message { return &mq.Message{Body: []byte("payload")} },
			max:  4,
		},
		{
			// A key and an identifier: two more attribute values, and the
			// key's is a string conversion the SPI's []byte key makes
			// unavoidable at this boundary (ADR-006).
			name: "key-and-id",
			msg: func() *mq.Message {
				return &mq.Message{Body: []byte("payload"), Key: []byte("customer-7"), ID: "app-1"}
			},
			max: 8,
		},
		{
			// Eight headers with values long enough to need a real string
			// conversion each. The bound grows with the headers, because an
			// attribute value IS a string and the SPI's header value is a
			// []byte; what it must not do is grow faster than that.
			//
			// The values are sixteen bytes on purpose: a ONE-byte conversion
			// allocates nothing at all (the runtime returns a pointer into
			// its static byte table), so a test written with short values
			// measures the map and calls it the per-header cost.
			name: "eight-headers",
			msg: func() *mq.Message {
				m := &mq.Message{Body: []byte("payload")}
				for i := range 8 {
					m.Headers.Add("h"+string(rune('a'+i)), []byte(strings.Repeat("v", 16)))
				}
				return m
			},
			max: 16,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			enc := gps.NewEncoder(t, gps.Config{ProjectID: "p"}, mq.MetadataNativeStrict)
			msg := tc.msg()
			// One warm-up outside the measurement, so that a lazily built
			// table inside the encoder is not counted as a per-publication
			// cost.
			if _, err := enc.Encode(msg, nil); err != nil {
				t.Fatalf("Encode: %v", err)
			}
			got := testing.AllocsPerRun(200, func() {
				if _, err := enc.Encode(msg, nil); err != nil {
					t.Fatalf("Encode: %v", err)
				}
			})
			if got > tc.max {
				t.Errorf("Encode allocated %.0f times, over the bound of %.0f: "+
					"the mapping has started allocating per header, or per call where it used to allocate once",
					got, tc.max)
			}
			t.Logf("allocs/publication: %.0f (bound %.0f)", got, tc.max)
		})
	}
}

// TestAllocsRefusalPathDoesNotFormat is the rule about the failure path:
// a refusal is a preallocated value rather than a formatted string, so a
// publisher under a storm of refusals does not allocate its way through them.
func TestAllocsRefusalPathDoesNotFormat(t *testing.T) {
	if testing.Short() {
		t.Skip("allocation counts are perturbed by -short's shorter warm-up")
	}
	enc := gps.NewEncoder(t, gps.Config{ProjectID: "p"}, mq.MetadataNativeStrict)
	// A header key over the 256 bytes both the attributes and the envelope
	// bound: refused in every mode, before any I/O.
	msg := &mq.Message{Body: []byte("x")}
	msg.Headers.Add(strings.Repeat("k", 300), []byte("v"))
	if _, err := enc.Encode(msg, nil); err == nil {
		t.Fatal("the oversized header key was accepted")
	}
	got := testing.AllocsPerRun(200, func() {
		if _, err := enc.Encode(msg, nil); err == nil {
			t.Fatal("the oversized header key was accepted")
		}
	})
	// The refusal itself allocates nothing: it is a package-level errcode
	// value. What the call still pays for is the attribute map the encoder
	// builds before it refuses, which is a handful.
	if got > 8 {
		t.Errorf("a refused publication allocated %.0f times: the refusals are preallocated values, not formatted strings", got)
	}
	t.Logf("allocs/refusal: %.0f", got)
}
