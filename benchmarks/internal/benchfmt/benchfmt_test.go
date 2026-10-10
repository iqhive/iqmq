package benchfmt

import (
	"strings"
	"testing"
)

const sample = `goos: linux
goarch: amd64
pkg: example
cpu: Fake CPU
BenchmarkMemory/Queue/mpmc/MPMC/P=4-8   	 1000000	       123.4 ns/op	      16 B/op	       0 allocs/op	     12.5 p99-ns
BenchmarkMemory/Queue/mpmc/MPMC/P=4-8   	 1000000	       125.0 ns/op	      16 B/op	       0 allocs/op	     13.0 p99-ns
BenchmarkHeaders/Get   	 5000000	        10 ns/op
not a benchmark line
BenchmarkBroken	x
PASS
ok  	example	1.0s
`

func TestParseAndGroup(t *testing.T) {
	rs, err := Parse(strings.NewReader(sample))
	if err != nil {
		t.Fatal(err)
	}
	if len(rs) != 3 {
		t.Fatalf("parsed %d results, want 3", len(rs))
	}
	r := rs[0]
	if r.Base() != "BenchmarkMemory/Queue/mpmc/MPMC/P=4" || r.Procs != 8 || r.Iters != 1000000 {
		t.Errorf("unexpected result %+v", r)
	}
	if r.Values["ns/op"] != 123.4 || r.Values["allocs/op"] != 0 || r.Values["p99-ns"] != 12.5 {
		t.Errorf("values = %v", r.Values)
	}
	if v, ok := Dim(r.Name, "P"); !ok || v != "4" {
		t.Errorf("Dim P = %q,%v", v, ok)
	}
	if _, ok := Dim(r.Name, "nope"); ok {
		t.Error("Dim found missing key")
	}
	if rs[2].Procs != 1 || rs[2].Base() != "BenchmarkHeaders/Get" {
		t.Errorf("unsuffixed result %+v", rs[2])
	}
	groups := GroupByName(rs)
	if len(groups) != 2 {
		t.Fatalf("groups = %d", len(groups))
	}
	if med, ok := groups[0].Median("ns/op"); !ok || med != 124.2 {
		t.Errorf("median = %v,%v", med, ok)
	}
	if _, ok := groups[0].Median("missing"); ok {
		t.Error("median of missing unit reported ok")
	}
	if got := Format(rs[2]); got != "BenchmarkHeaders/Get\t5000000\t10 ns/op" {
		t.Errorf("Format = %q", got)
	}
	back, _ := ParseBytes([]byte(Format(rs[0])))
	if len(back) != 1 || back[0].Values["ns/op"] != 123.4 || len(back[0].Units) != 4 {
		t.Errorf("round trip = %+v", back)
	}
}

func TestBase(t *testing.T) {
	for in, want := range map[string]string{
		"BenchmarkX-8":     "BenchmarkX",
		"BenchmarkX":       "BenchmarkX",
		"BenchmarkX/n=1-4": "BenchmarkX/n=1",
		"BenchmarkX/a-b":   "BenchmarkX/a-b",
	} {
		if got := Base(in); got != want {
			t.Errorf("Base(%q) = %q, want %q", in, got, want)
		}
	}
}

func FuzzParse(f *testing.F) {
	f.Add([]byte(sample))
	f.Add([]byte("BenchmarkA-1 1 1 ns/op\n"))
	f.Add([]byte("Benchmark 1e999 ns/op\n"))
	f.Fuzz(func(t *testing.T, data []byte) {
		rs, err := ParseBytes(data)
		if err != nil {
			return
		}
		for _, r := range rs {
			if len(r.Units) != len(r.Values) {
				t.Fatalf("units/values mismatch in %+v", r)
			}
			back, err := ParseBytes([]byte(Format(r) + "\n"))
			if err != nil || len(back) != 1 {
				t.Fatalf("Format not parseable: %q (%v)", Format(r), err)
			}
		}
		GroupByName(rs)
	})
}

// TestResultRow: a measurement's identity includes the rung it was taken at.
// Base() alone collapses -1, -4, -8 and -32 into one key, and a median over
// that mixture describes no configuration.
func TestResultRow(t *testing.T) {
	rs, err := ParseBytes([]byte(
		"BenchmarkMemory/Queue/mpmc/MPMC/P=4-1\t10\t400.0 ns/op\n" +
			"BenchmarkMemory/Queue/mpmc/MPMC/P=4-32\t10\t100.0 ns/op\n",
	))
	if err != nil {
		t.Fatal(err)
	}
	if len(rs) != 2 {
		t.Fatalf("parsed %d result(s)", len(rs))
	}
	b0, p0 := rs[0].Row()
	b1, p1 := rs[1].Row()
	if b0 != b1 {
		t.Errorf("base names differ: %q %q", b0, b1)
	}
	if p0 != 1 || p1 != 32 {
		t.Errorf("rungs = %d, %d; want 1, 32", p0, p1)
	}
}
