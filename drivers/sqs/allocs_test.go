package sqs

import (
	"testing"

	mq "github.com/iqhive/iqmq"
)

// TestAllocs holds the mapping's allocation count, which is the part of this
// driver that runs per message and is not dominated by an HTTP request.
//
// The ceilings are not zero and cannot be: the SDK's request types are maps of
// pointer-valued structs, so an attribute is an allocation whatever the driver
// does, and the encoding has to produce a string the SDK will serialise. What
// the ceilings are for is the DIRECTION — a change that doubled the per-message
// cost of the mapping would be caught here rather than in a benchmark nobody
// reads — and they are stated as the numbers the specification asks a driver to
// measure rather than as a budget the specification sets.
func TestAllocs(t *testing.T) {
	for _, tc := range []struct {
		name string
		mode mq.MetadataMode
		msg  func() *mq.Message
		max  float64
	}{
		{"bare body", mq.MetadataNativeStrict, func() *mq.Message {
			return &mq.Message{Body: []byte("payload")}
		}, 8},
		{"key and identifier", mq.MetadataNativeStrict, func() *mq.Message {
			return &mq.Message{Key: []byte("k"), Body: []byte("payload"), ID: "id"}
		}, 16},
		{"three headers", mq.MetadataNativeStrict, func() *mq.Message {
			m := &mq.Message{Body: []byte("payload")}
			m.Headers.Add("a", []byte("1"))
			m.Headers.Add("b", []byte("2"))
			m.Headers.Add("c", []byte("3"))
			return m
		}, 24},
		{"envelope", mq.MetadataPortableEnvelope, func() *mq.Message {
			m := &mq.Message{Key: []byte("k"), Body: []byte("payload")}
			m.Headers.Add("a", []byte("1"))
			return m
		}, 24},
	} {
		t.Run(tc.name, func(t *testing.T) {
			enc := encoderFor(t, Config{}, tc.mode)
			m := tc.msg()
			opts := mq.PublishOptions{}
			// One call first, so that a lazily built table inside the
			// encoder is not counted as a per-message allocation.
			if _, err := enc.encode(m, &opts); err != nil {
				t.Fatalf("encode: %v", err)
			}
			got := testing.AllocsPerRun(200, func() {
				if _, err := enc.encode(m, &opts); err != nil {
					t.Fatalf("encode: %v", err)
				}
			})
			t.Logf("%s: %.1f allocs/encode", tc.name, got)
			if got > tc.max {
				t.Fatalf("%s costs %.1f allocations per encode, over the %.0f this driver measures", tc.name, got, tc.max)
			}
		})
	}
}

// TestDecodeAllocsAreBounded is the receive side. A fetch reuses the caller's
// buffer, so what a delivery costs is the decoding, and it is bounded by the
// message's own shape rather than by the number of deliveries.
func TestDecodeAllocsAreBounded(t *testing.T) {
	enc := encoderFor(t, Config{}, mq.MetadataNativeStrict)
	m := &mq.Message{Key: []byte("k"), Body: []byte("payload"), ID: "id"}
	m.Headers.Add("a", []byte("1"))
	out, err := enc.encode(m, &mq.PublishOptions{})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	src := received(out)
	var dst mq.Message
	if err := enc.decode(src, "t", &dst); err != nil {
		t.Fatalf("decode: %v", err)
	}
	got := testing.AllocsPerRun(200, func() {
		if err := enc.decode(src, "t", &dst); err != nil {
			t.Fatalf("decode: %v", err)
		}
	})
	t.Logf("%.1f allocs/decode", got)
	if got > 12 {
		t.Fatalf("decoding one delivery costs %.1f allocations, over the 12 this driver measures", got)
	}
}
