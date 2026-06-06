package msgpack_test

import (
	"strings"
	"testing"

	"bitbucket.org/iqhive/iqmq/codec"
	iqmsgpack "bitbucket.org/iqhive/iqmq/codec/msgpack"
)

// BenchmarkCodec is layer 1 with the codec kind as a path segment:
//
//	BenchmarkCodec/msgpack/{Marshal,AppendMarshal,Unmarshal,Validate}/{64B,1KiB,64KiB}
//
// Validate is a row of its own because it is work this codec does that the
// library does not, and the requirement is only affordable if it is cheap.
func BenchmarkCodec(b *testing.B) {
	c := iqmsgpack.New()
	a := c.(codec.Appender)
	sizes := []struct {
		name string
		n    int
	}{{"64B", 64}, {"1KiB", 1024}, {"64KiB", 64 << 10}}
	b.Run("msgpack", func(b *testing.B) {
		for _, s := range sizes {
			v := &order{ID: strings.Repeat("x", s.n), Qty: 7, Tags: []string{"a", "b"}, Meta: map[string]string{"k": "v"}}
			body, err := c.Marshal(v)
			if err != nil {
				b.Fatal(err)
			}
			b.Run("Marshal/"+s.name, func(b *testing.B) {
				b.SetBytes(int64(len(body)))
				b.ReportAllocs()
				for b.Loop() {
					if _, err := c.Marshal(v); err != nil {
						b.Fatal(err)
					}
				}
			})
			b.Run("AppendMarshal/"+s.name, func(b *testing.B) {
				buf := make([]byte, 0, len(body))
				b.SetBytes(int64(len(body)))
				b.ReportAllocs()
				for b.Loop() {
					out, err := a.AppendMarshal(buf[:0], v)
					if err != nil {
						b.Fatal(err)
					}
					buf = out
				}
			})
			b.Run("Unmarshal/"+s.name, func(b *testing.B) {
				var into order
				b.SetBytes(int64(len(body)))
				b.ReportAllocs()
				for b.Loop() {
					if err := c.Unmarshal(body, &into); err != nil {
						b.Fatal(err)
					}
				}
			})
			b.Run("Validate/"+s.name, func(b *testing.B) {
				cfg := iqmsgpack.Config{}
				b.SetBytes(int64(len(body)))
				b.ReportAllocs()
				for b.Loop() {
					if err := iqmsgpack.Validate(body, cfg); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	})
}
