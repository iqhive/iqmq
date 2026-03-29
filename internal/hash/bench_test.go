package hash

import (
	"fmt"
	"testing"
)

// benchSeed is a fixed non-zero seed so every kind pays for seeding.
const benchSeed = 0x5eed

var benchSink uint64

// BenchmarkHash/<kind>/<size>B/entry=<bytes|string> hashes one fixed key
// of 8, 32 or 256 bytes through the Bytes or String entry point;
// BenchmarkHash/Lane measures the range reduction alone and
// BenchmarkHash/KafkaPartition the Kafka partitioner.
func BenchmarkHash(b *testing.B) {
	key := katBuffer()
	for _, k := range Kinds() {
		b.Run(k.String(), func(b *testing.B) {
			h := New(k)
			for _, size := range []int{8, 32, 256} {
				kb := key[:size]
				ks := string(kb)
				b.Run(fmt.Sprintf("%dB/entry=bytes", size), func(b *testing.B) {
					b.SetBytes(int64(size))
					b.ReportAllocs()
					var sink uint64
					for b.Loop() {
						sink += h.Bytes(benchSeed, kb)
					}
					benchSink = sink
				})
				b.Run(fmt.Sprintf("%dB/entry=string", size), func(b *testing.B) {
					b.SetBytes(int64(size))
					b.ReportAllocs()
					var sink uint64
					for b.Loop() {
						sink += h.String(benchSeed, ks)
					}
					benchSink = sink
				})
			}
		})
	}
	b.Run("Lane", func(b *testing.B) {
		b.ReportAllocs()
		var sink uint64
		h := uint64(0x123456789abcdef)
		for b.Loop() {
			sink += uint64(Lane(h, 64))
			h += 0x9E3779B97F4A7C15
		}
		benchSink = sink
	})
	b.Run("KafkaPartition", func(b *testing.B) {
		kb := key[:32]
		b.SetBytes(32)
		b.ReportAllocs()
		var sink uint64
		for b.Loop() {
			sink += uint64(KafkaPartition(kb, 12))
		}
		benchSink = sink
	})
}
