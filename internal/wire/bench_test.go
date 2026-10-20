package wire

import (
	"bytes"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/iqhive/iqmq/internal/payload"
)

// benchSizes are the body sizes.
var benchSizes = []struct {
	name  string
	bytes int
}{
	{"64B", 64},
	{"1KiB", 1024},
	{"64KiB", 64 << 10},
}

// benchPayload is a message of the given body size with n headers: the
// shape the specification budgets (a key, an ID, the usual properties and the headers
// a driver maps).
func benchPayload(size, headers int) *payload.Payload {
	p := &payload.Payload{
		Key:  []byte("orders/2025-04-11/42"),
		Body: bytes.Repeat([]byte("iqmq"), (size+3)/4)[:size],
		ID:   "01J8ZQ2K3M4N5P6Q7R8S9T0V1W",
		Props: payload.Props{
			ContentType:   "application/json",
			CorrelationID: "corr-0123456789abcdef",
			CreatedAt:     time.Unix(1727641242, 123456789).UTC(),
		},
	}
	names := []string{"traceparent", "iqmq-attempts", "x-tenant", "x-schema-id"}
	values := []string{
		"00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
		"1", "tenant-42", "orders.v3",
	}
	if headers > 0 {
		p.Headers = make([]payload.KV, headers)
		for i := range p.Headers {
			p.Headers[i] = payload.KV{
				Key:   names[i%len(names)] + strconv.Itoa(i/len(names)),
				Value: []byte(values[i%len(values)]),
			}
		}
	}
	return p
}

var benchSink int

// BenchmarkWire/<layout>/{Encode,Decode}/{64B,1KiB,64KiB}/headers={0,4}
// measures one envelope operation with a caller-owned destination buffer
// (Encode) and a reused destination payload (Decode), the mode the specification
// budgets at 1 KiB: ≤ 150 ns encode, ≤ 200 ns decode, zero allocations.
// Size is measured alongside because publishers call it to size a batch.
func BenchmarkWire(b *testing.B) {
	for _, k := range Kinds() {
		b.Run(k.String(), func(b *testing.B) {
			l := mustNew(b, k)
			for _, op := range []string{"Encode", "Decode", "Size"} {
				b.Run(op, func(b *testing.B) {
					for _, size := range benchSizes {
						for _, nh := range []int{0, 4} {
							name := fmt.Sprintf("%s/headers=%d", size.name, nh)
							b.Run(name, func(b *testing.B) {
								benchOne(b, l, op, size.bytes, nh)
							})
						}
					}
				})
			}
		})
	}
}

func benchOne(b *testing.B, l Layout, op string, size, headers int) {
	p := benchPayload(size, headers)
	buf := make([]byte, 0, l.Size(p)+64)
	enc, err := l.Encode(buf, p)
	if err != nil {
		b.Fatal(err)
	}
	var dst payload.Payload
	if err := l.Decode(enc, &dst, nil); err != nil {
		b.Fatal(err)
	}
	b.SetBytes(int64(len(enc)))
	b.ReportAllocs()
	b.ReportMetric(float64(len(enc)-size), "overhead-B")
	switch op {
	case "Encode":
		for b.Loop() {
			out, err := l.Encode(buf, p)
			if err != nil {
				b.Fatal(err)
			}
			benchSink += len(out)
		}
	case "Decode":
		for b.Loop() {
			if err := l.Decode(enc, &dst, nil); err != nil {
				b.Fatal(err)
			}
			benchSink += len(dst.Body)
		}
	case "Size":
		for b.Loop() {
			benchSink += l.Size(p)
		}
	}
}

// BenchmarkWireZeroCopy compares the copying decode with the aliasing one
// at the sizes where the copy dominates.
func BenchmarkWireZeroCopy(b *testing.B) {
	for _, k := range Kinds() {
		b.Run(k.String(), func(b *testing.B) {
			l := mustNew(b, k)
			for _, size := range benchSizes {
				p := benchPayload(size.bytes, 4)
				enc := encode(b, l, p)
				var dst payload.Payload
				lim := &Limits{ZeroCopy: true}
				b.Run(size.name, func(b *testing.B) {
					b.SetBytes(int64(len(enc)))
					b.ReportAllocs()
					for b.Loop() {
						if err := l.Decode(enc, &dst, lim); err != nil {
							b.Fatal(err)
						}
						benchSink += len(dst.Body)
					}
				})
			}
		})
	}
}
