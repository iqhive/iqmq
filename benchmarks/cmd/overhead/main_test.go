package main

import (
	"math"
	"testing"
)

// TestLossIsTheRateNotTheDuration pins the one piece of arithmetic every figure
// on docs/performance/overhead.md rests on. The specification budgets a "throughput loss",
// and an arm that takes 10 % LONGER per operation does not move 10 % fewer
// messages a second -- it moves 9.09 % fewer. Quoting the duration increase
// would overstate every row in the table, and in the wrong direction: it would
// report a breach on a row that meets the budget.
func TestLossIsTheRateNotTheDuration(t *testing.T) {
	for _, tc := range []struct {
		name               string
		driverNs, nativeNs float64
		want               float64
	}{
		{"identical arms lose nothing", 100, 100, 0},
		{"10 % longer per op is 9.09 % of the rate", 110, 100, 9.0909},
		{"5 % longer per op is 4.76 % of the rate", 105, 100, 4.7619},
		{"twice as slow is half the throughput", 200, 100, 50},
		{"faster than the raw client is a negative loss", 90, 100, -11.1111},
	} {
		if got := loss(tc.driverNs, tc.nativeNs); math.Abs(got-tc.want) > 0.001 {
			t.Errorf("%s: loss(%g, %g) = %.4f, want %.4f", tc.name, tc.driverNs, tc.nativeNs, got, tc.want)
		}
	}

	// The figure the batched budget really permits: a 5 % throughput loss
	// is a 5.26 % longer operation, so a row at exactly 5 % longer MEETS the
	// budget and the duration reading would have failed it.
	if got := loss(105, 100); got > 5 {
		t.Errorf("a 5 %%-longer operation reports %.4f %% throughput loss, which would breach the 5 %% budget", got)
	}
	if got := loss(105.264, 100); got < 5 {
		t.Errorf("a 5.264 %%-longer operation reports %.4f %%, which should be at the 5 %% budget", got)
	}
}

// TestBudgetsFollowTheSpecification pins which row each operation is
// held to, and that the two operations containing a receive are measured and
// not compared. EndToEnd was compared in the first version of the tool and the
// first measurement refuted it; this is what stops it coming back.
func TestBudgetsFollowTheSpecification(t *testing.T) {
	for op, want := range map[string]struct {
		pct      float64
		compared bool
	}{
		"PublishBatch": {5, true},
		"Publish":      {10, true},
		// Both of these contain, or are, a receive, and the two arms'
		// receive paths are not the same experiment -- measured in both
		// directions and at two orders of magnitude.
		"Receive":  {0, false},
		"EndToEnd": {0, false},
	} {
		pct, compared := budget(op)
		if pct != want.pct || compared != want.compared {
			t.Errorf("budget(%q) = %v, %v; want %v, %v", op, pct, compared, want.pct, want.compared)
		}
	}
}

func TestSummariseReportsTheSampleCountAndIntervalWidth(t *testing.T) {
	med, half, n := summarise(nil)
	if med != 0 || half != 0 || n != 0 {
		t.Errorf("summarise(nil) = %v, %v, %v; want zeroes", med, half, n)
	}
	med, half, n = summarise([]float64{100, 100, 100, 100, 100})
	if med != 100 || n != 5 {
		t.Errorf("summarise(five identical) = %v (n=%d), want 100 (n=5)", med, n)
	}
	if half != 0 {
		t.Errorf("five identical samples report a %.2f %% interval, want 0", half)
	}
	if _, half, _ = summarise([]float64{50, 100, 150, 200, 250}); half <= 0 {
		t.Error("a spread sample reports a zero interval half-width")
	}
}
