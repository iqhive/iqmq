package avro_test

import (
	"strconv"
	"strings"
	"testing"

	"github.com/iqhive/iqmq/codec"
	iqavro "github.com/iqhive/iqmq/codec/avro"
)

// BenchmarkCodec is layer 1 with the codec kind as a path segment:
//
//	BenchmarkCodec/avro/{datum,sobj}/{Marshal,AppendMarshal,Unmarshal}/{64B,1KiB,64KiB}
//
// The two framings are separate arms because single-object framing's cost is
// the question a reader has: it is ten bytes and a memcpy of a precomputed
// header, and the numbers are what say so.
func BenchmarkCodec(b *testing.B) {
	sizes := []struct {
		name string
		n    int
	}{{"64B", 64}, {"1KiB", 1024}, {"64KiB", 64 << 10}}
	b.Run("avro", func(b *testing.B) {
		for _, framing := range []iqavro.Framing{iqavro.FramingDatum, iqavro.FramingSingleObject} {
			c, err := iqavro.New(iqavro.Config{Schema: orderSchema, Framing: framing})
			if err != nil {
				b.Fatal(err)
			}
			a := c.(codec.Appender)
			b.Run(framing.String(), func(b *testing.B) {
				for _, s := range sizes {
					attrs := make([]attr, 0, 4)
					for i := range 4 {
						attrs = append(attrs, attr{K: "k" + strconv.Itoa(i), V: strconv.Itoa(i)})
					}
					v := &order{ID: strings.Repeat("x", s.n), Qty: 7, Tags: []string{"a", "b"}, Attrs: attrs}
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
				}
			})
		}
	})
}
