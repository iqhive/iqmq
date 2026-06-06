package cloudevents_test

import (
	"strings"
	"testing"
	"time"

	"github.com/cloudevents/sdk-go/v2/event"

	"bitbucket.org/iqhive/iqmq/codec"
	ce "bitbucket.org/iqhive/iqmq/codec/cloudevents"
)

// BenchmarkCodec is layer 1 with the codec kind as a path segment:
//
//	BenchmarkCodec/cloudevents/structured/{Marshal,AppendMarshal,Unmarshal}/{64B,1KiB,64KiB}
//	BenchmarkCodec/cloudevents/binary/{Encode,Decode}/{64B,1KiB,64KiB}
//
// The two content modes are separate arms with DIFFERENT operation names,
// because they are not two encodings of one thing: structured mode marshals a
// body and binary mode splits an event into a body and a header block, which
// is why one is a codec.Codec and the other cannot be.
func BenchmarkCodec(b *testing.B) {
	sizes := []struct {
		name string
		n    int
	}{{"64B", 64}, {"1KiB", 1024}, {"64KiB", 64 << 10}}
	mk := func(n int) *event.Event {
		e := event.New(ce.SpecVersion)
		e.SetID("id-1")
		e.SetSource("https://example.test/orders")
		e.SetType("test.order.created")
		e.SetSubject("order/42")
		e.SetTime(time.Date(2026, 9, 15, 10, 0, 0, 0, time.UTC))
		if err := e.SetData("text/plain", strings.Repeat("x", n)); err != nil {
			panic(err)
		}
		ce.SetPartitionKey(&e, "cust-42")
		return &e
	}
	b.Run("cloudevents", func(b *testing.B) {
		c := ce.New()
		a := c.(codec.Appender)
		b.Run("structured", func(b *testing.B) {
			for _, s := range sizes {
				e := mk(s.n)
				body, err := c.Marshal(e)
				if err != nil {
					b.Fatal(err)
				}
				b.Run("Marshal/"+s.name, func(b *testing.B) {
					b.SetBytes(int64(len(body)))
					b.ReportAllocs()
					for b.Loop() {
						if _, err := c.Marshal(e); err != nil {
							b.Fatal(err)
						}
					}
				})
				b.Run("AppendMarshal/"+s.name, func(b *testing.B) {
					buf := make([]byte, 0, len(body))
					b.SetBytes(int64(len(body)))
					b.ReportAllocs()
					for b.Loop() {
						out, err := a.AppendMarshal(buf[:0], e)
						if err != nil {
							b.Fatal(err)
						}
						buf = out
					}
				})
				b.Run("Unmarshal/"+s.name, func(b *testing.B) {
					var into event.Event
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
		bind := ce.NewBinding()
		b.Run("binary", func(b *testing.B) {
			for _, s := range sizes {
				e := mk(s.n)
				body, headers, err := bind.Encode(e)
				if err != nil {
					b.Fatal(err)
				}
				b.Run("Encode/"+s.name, func(b *testing.B) {
					b.SetBytes(int64(len(body)))
					b.ReportAllocs()
					for b.Loop() {
						if _, _, err := bind.Encode(e); err != nil {
							b.Fatal(err)
						}
					}
				})
				b.Run("Decode/"+s.name, func(b *testing.B) {
					var into event.Event
					b.SetBytes(int64(len(body)))
					b.ReportAllocs()
					for b.Loop() {
						if err := bind.Decode(body, headers, &into); err != nil {
							b.Fatal(err)
						}
					}
				})
			}
		})
	})
}
