package topicname

import (
	"strconv"
	"strings"
	"testing"
)

// BenchmarkTopicName/Validate/len=<n>/hier=<bool> measures validation of a
// valid name; Check/invalid measures the early exit on the first bad byte.
func BenchmarkTopicName(b *testing.B) {
	for _, n := range []int{8, 64, 255} {
		for _, hier := range []bool{false, true} {
			name := makeName(n, hier)
			b.Run("Validate/len="+strconv.Itoa(n)+"/hier="+strconv.FormatBool(hier), func(b *testing.B) {
				b.SetBytes(int64(len(name)))
				b.ReportAllocs()
				for b.Loop() {
					if Validate(name) != nil {
						b.Fatal("valid name rejected")
					}
				}
			})
		}
	}
	bad := makeName(64, true)
	bad = bad[:32] + " " + bad[33:]
	b.Run("Check/invalid/len=64", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			if Check(bad).Kind != InvalidByte {
				b.Fatal("invalid name accepted")
			}
		}
	})
	name := makeName(64, true)
	b.Run("Segments/len=64", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			n := 0
			for seg := range Segments(name) {
				n += len(seg)
			}
			if n == 0 {
				b.Fatal("no segments")
			}
		}
	})
}

// makeName returns a valid name of exactly n bytes; with hier it contains a
// separator every 16 bytes (every 4 for short names).
func makeName(n int, hier bool) string {
	every := 16
	if n < 32 {
		every = 4
	}
	var sb strings.Builder
	for i := range n {
		if hier && i%every == every-1 && i != n-1 && i != 0 {
			sb.WriteByte('/')
			continue
		}
		sb.WriteByte("abcdefghijklmnopqrstuvwxyz0123456789._-"[i%39])
	}
	s := sb.String()
	if Validate(s) != nil {
		panic("makeName produced an invalid name: " + s)
	}
	return s
}
