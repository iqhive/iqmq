package corpus

import (
	"bytes"
	"testing"
)

func TestCorpus(t *testing.T) {
	if SizeName(0) != "0B" || SizeName(64) != "64B" || SizeName(1024) != "1KiB" || SizeName(64*1024) != "64KiB" || SizeName(1<<20) != "1MiB" {
		t.Error("SizeName")
	}
	b := Body(1024)
	if len(b) != 1024 || &b[0] != &Body(1024)[0] {
		t.Error("Body not cached/deterministic")
	}
	if len(Body(0)) != 0 {
		t.Error("Body(0)")
	}
	h := Headers(8)
	if len(h) != 8 || h[0].Key != "h0" || h[3].Value[0] != 0 || string(h[1].Value) != "00000001" {
		t.Errorf("Headers = %v", h)
	}
	k := Keys(3, 16)
	if len(k) != 3 || len(k[0]) != 16 || string(k[1][:5]) != "key-1" || bytes.Equal(k[0], k[1]) {
		t.Errorf("Keys = %q", k)
	}
	z := ZipfKeys(100, 1.2, 1000)
	if len(z) != 1000 {
		t.Error("ZipfKeys length")
	}
	hist := map[int]int{}
	for _, i := range z {
		if i < 0 || i >= 100 {
			t.Fatalf("zipf index %d out of range", i)
		}
		hist[i]++
	}
	if hist[0] < hist[50] {
		t.Error("zipf not skewed towards low indices")
	}
}
