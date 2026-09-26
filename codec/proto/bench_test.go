package proto_test

import (
	"strings"
	"testing"

	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/iqhive/iqmq/codec"
	iqproto "github.com/iqhive/iqmq/codec/proto"
)

// BenchmarkCodec is the shape codec's own benchmark uses, with the codec kind
// as a path segment:
//
//	BenchmarkCodec/proto/{Marshal,AppendMarshal,Unmarshal}/{64B,1KiB,64KiB}
//
// It is not a bake-off family: there is one protobuf implementation, so there
// is nothing to decide. The numbers exist so that a change to the append path
// or the limit checks is visible.
func BenchmarkCodec(b *testing.B) {
	c := iqproto.New()
	a := c.(codec.Appender)
	sizes := []struct {
		name string
		n    int
	}{{"64B", 64}, {"1KiB", 1024}, {"64KiB", 64 << 10}}
	b.Run("proto", func(b *testing.B) {
		for _, s := range sizes {
			msg := wrapperspb.String(strings.Repeat("x", s.n))
			body, err := c.Marshal(msg)
			if err != nil {
				b.Fatal(err)
			}
			b.Run("Marshal/"+s.name, func(b *testing.B) {
				b.SetBytes(int64(len(body)))
				b.ReportAllocs()
				for b.Loop() {
					if _, err := c.Marshal(msg); err != nil {
						b.Fatal(err)
					}
				}
			})
			b.Run("AppendMarshal/"+s.name, func(b *testing.B) {
				buf := make([]byte, 0, len(body))
				b.SetBytes(int64(len(body)))
				b.ReportAllocs()
				for b.Loop() {
					out, err := a.AppendMarshal(buf[:0], msg)
					if err != nil {
						b.Fatal(err)
					}
					buf = out
				}
			})
			b.Run("Unmarshal/"+s.name, func(b *testing.B) {
				var into wrapperspb.StringValue
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
