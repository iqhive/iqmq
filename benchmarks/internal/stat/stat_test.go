package stat

import (
	"math"
	"testing"
)

func TestBasics(t *testing.T) {
	if Median(nil) != 0 || Mean(nil) != 0 || Geomean(nil) != 0 {
		t.Error("empty inputs")
	}
	if Median([]float64{3, 1, 2}) != 2 || Median([]float64{4, 1, 3, 2}) != 2.5 {
		t.Error("median")
	}
	if Mean([]float64{1, 2, 3}) != 2 {
		t.Error("mean")
	}
	if g := Geomean([]float64{1, 100, -5}); math.Abs(g-10) > 1e-9 {
		t.Errorf("geomean = %v", g)
	}
	lo, hi := MedianCI([]float64{5, 1, 3})
	if lo != 1 || hi != 5 {
		t.Errorf("small CI = %v,%v", lo, hi)
	}
	xs := make([]float64, 10)
	for i := range xs {
		xs[i] = float64(i + 1)
	}
	lo, hi = MedianCI(xs)
	if lo != 2 || hi != 9 {
		t.Errorf("CI(1..10) = %v,%v, want 2,9", lo, hi)
	}
}

func TestMannWhitney(t *testing.T) {
	a := []float64{1000, 1001, 1002, 1003, 1004, 1005, 1006, 1007, 1008, 1009}
	b := []float64{1040, 1041, 1042, 1043, 1044, 1045, 1046, 1047, 1048, 1049}
	if _, p := MannWhitney(a, b); p > 0.001 {
		t.Errorf("separated samples p = %v", p)
	}
	if _, p := MannWhitney(a, a); p < 0.9 {
		t.Errorf("identical samples p = %v", p)
	}
	// Interleaved samples are indistinguishable.
	c := []float64{1000, 1002, 1004, 1006, 1008, 1010, 1012, 1014, 1016, 1018}
	d := []float64{1001, 1003, 1005, 1007, 1009, 1011, 1013, 1015, 1017, 1019}
	if _, p := MannWhitney(c, d); p < 0.5 {
		t.Errorf("interleaved samples p = %v", p)
	}
	if _, p := MannWhitney(nil, a); p != 1 {
		t.Error("empty sample p != 1")
	}
	// Ties fall back to the normal approximation and still separate clearly.
	e := []float64{1, 1, 1, 1, 1, 1, 1, 1, 1, 1}
	f := []float64{2, 2, 2, 2, 2, 2, 2, 2, 2, 2}
	if _, p := MannWhitney(e, f); p > 0.001 {
		t.Errorf("tied separated samples p = %v", p)
	}
	if _, p := MannWhitney(e, e); p != 1 {
		t.Errorf("all-equal samples p = %v", p)
	}
	// Large samples use the approximation.
	big1 := make([]float64, 40)
	big2 := make([]float64, 40)
	for i := range big1 {
		big1[i] = float64(i)
		big2[i] = float64(i) + 0.5
	}
	if _, p := MannWhitney(big1, big2); p < 0.5 {
		t.Errorf("large overlapping samples p = %v", p)
	}
	// Exact CDF sanity: for m=n=1, P(U<=0)=0.5 and P(U<=1)=1.
	if v := exactCDF(1, 1, 0); v != 0.5 {
		t.Errorf("exactCDF(1,1,0) = %v", v)
	}
	if v := exactCDF(1, 1, 1); v != 1 {
		t.Errorf("exactCDF(1,1,1) = %v", v)
	}
}

// TestGeomeanRatioCI holds the bootstrap interval to the three things the
// bake-off reads off it: an interval that brackets a real difference, one
// that straddles 1 when the two samples cannot be told apart, and the same
// numbers on a second run.
func TestGeomeanRatioCI(t *testing.T) {
	// Two rows, the candidate reliably half the reference's cost.
	fast := [][]float64{{50, 51, 49, 50, 52, 48, 50, 51, 49, 50}, {100, 102, 98, 101, 99, 100, 103, 97, 100, 101}}
	slow := [][]float64{{100, 101, 99, 100, 102, 98, 100, 101, 99, 100}, {200, 202, 198, 201, 199, 200, 203, 197, 200, 201}}
	lo, mid, hi := GeomeanRatioCI(fast, slow, 500, 7)
	if !(lo > 0.45 && hi < 0.55) {
		t.Errorf("clear win: CI = [%.4f, %.4f] mid %.4f; want a tight interval around 0.5", lo, hi, mid)
	}
	if hi >= 1 {
		t.Errorf("clear win: CI upper bound %.4f does not exclude parity", hi)
	}
	// One row, heavily overlapping samples about 4% apart: the interval must
	// include 1, which is what stops a 4% median difference from being read
	// as a win.
	a := [][]float64{{96, 97, 98, 99, 100, 101, 102, 103, 104, 105}}
	b := [][]float64{{100, 101, 102, 103, 104, 105, 106, 107, 108, 109}}
	lo, _, hi = GeomeanRatioCI(a, b, 2000, 11)
	if !(lo < 1 && hi >= 1) {
		t.Errorf("noise: CI = [%.4f, %.4f]; overlapping samples must not exclude parity", lo, hi)
	}
	// Reproducible: the ADR is rendered from the scorecard and must be a
	// function of the data.
	lo2, mid2, hi2 := GeomeanRatioCI(fast, slow, 500, 7)
	lo1, mid1, hi1 := GeomeanRatioCI(fast, slow, 500, 7)
	if lo1 != lo2 || mid1 != mid2 || hi1 != hi2 {
		t.Errorf("not reproducible: [%v %v %v] vs [%v %v %v]", lo1, mid1, hi1, lo2, mid2, hi2)
	}
	if lo, mid, hi := GeomeanRatioCI(nil, nil, 100, 1); lo != 0 || mid != 0 || hi != 0 {
		t.Errorf("empty = %v %v %v", lo, mid, hi)
	}
	if lo, _, _ := GeomeanRatioCI([][]float64{{1}}, [][]float64{{1}, {2}}, 100, 1); lo != 0 {
		t.Errorf("mismatched row counts = %v", lo)
	}
}
