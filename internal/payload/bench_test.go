package payload

import (
	"strconv"
	"testing"
)

// BenchmarkPayload/<op>/body=<n>/headers=<n> measures the copy modes the
// engine uses: Clone (safe publish), CloneInto (pooled destination), Alias
// (borrowed / zero-copy) and Aliases (debug alias check).
func BenchmarkPayload(b *testing.B) {
	for _, body := range []int{0, 64, 1024, 65536} {
		for _, headers := range []int{0, 4} {
			p := benchPayload(body, headers)
			dims := "body=" + strconv.Itoa(body) + "/headers=" + strconv.Itoa(headers)
			b.Run("Clone/"+dims, func(b *testing.B) {
				b.SetBytes(int64(p.Size()))
				b.ReportAllocs()
				for b.Loop() {
					if p.Clone() == nil {
						b.Fatal("nil clone")
					}
				}
			})
			b.Run("CloneInto/"+dims, func(b *testing.B) {
				dst := p.Clone()
				b.SetBytes(int64(p.Size()))
				b.ReportAllocs()
				for b.Loop() {
					p.CloneInto(dst)
				}
			})
			b.Run("Alias/"+dims, func(b *testing.B) {
				var dst Payload
				b.ReportAllocs()
				for b.Loop() {
					p.AliasInto(&dst)
				}
			})
			b.Run("Aliases/"+dims, func(b *testing.B) {
				q := p.Clone()
				b.ReportAllocs()
				for b.Loop() {
					if Aliases(p, q) {
						b.Fatal("clone aliases")
					}
				}
			})
		}
	}
}

func benchPayload(body, headers int) *Payload {
	p := &Payload{Key: []byte("order-42"), Body: make([]byte, body), ID: "id"}
	for i := range p.Body {
		p.Body[i] = byte(i)
	}
	if headers > 0 {
		p.Headers = make([]KV, headers)
		for i := range p.Headers {
			p.Headers[i] = KV{Key: "h" + strconv.Itoa(i), Value: []byte("value-" + strconv.Itoa(i))}
		}
	}
	return p
}
