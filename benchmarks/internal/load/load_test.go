package load

import (
	"testing"
	"time"
)

func TestPacerSchedule(t *testing.T) {
	p := NewPacer(1000) // 1ms interval
	t0 := p.Next()
	t1 := p.Next()
	if d := t1.Sub(t0); d != time.Millisecond {
		t.Errorf("interval = %v", d)
	}
	if b := p.Backlog(p.start.Add(10 * time.Millisecond)); b != 8 {
		t.Errorf("backlog = %d, want 8", b)
	}
	if b := p.Backlog(p.start); b != 0 {
		t.Errorf("backlog at start = %d", b)
	}
	free := NewPacer(0)
	if free.Backlog(time.Now().Add(time.Hour)) != 0 {
		t.Error("unpaced backlog")
	}
	free.Wait()
	start := time.Now()
	fast := NewPacer(10000)
	for range 5 {
		fast.Wait()
	}
	if el := time.Since(start); el < 300*time.Microsecond {
		t.Errorf("Wait did not pace: %v", el)
	}
}

func TestCalibrateAndRates(t *testing.T) {
	c := Calibrate(20*time.Millisecond, func() {})
	if c <= 0 {
		t.Errorf("capacity = %v", c)
	}
	r := Rates(1000)
	if r["rate=50pct"] != 500 || r["rate=120pct"] != 1200 || len(r) != 4 {
		t.Errorf("rates = %v", r)
	}
}
