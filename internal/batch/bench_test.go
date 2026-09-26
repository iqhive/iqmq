package batch

import (
	"fmt"
	"testing"
	"time"

	"github.com/iqhive/iqmq/internal/clock"
)

var benchSink int

// benchRates are publish rates in messages per second: a low-rate
// publisher is where the linger trigger decides latency, a high-rate one
// is where it decides throughput.
var benchRates = []int{100, 10000, 1000000}

// benchLingers are the linger settings exposes.
var benchLingers = []time.Duration{time.Millisecond, 10 * time.Millisecond}

// BenchmarkBatch/<kind>/rate=<msgs/s>/linger=<d> publishes at a fixed
// simulated rate through one batcher and reports what the family is
// compared on: timers/op (timers created per message), batches/op,
// msgs/batch and the mean delay a message waits before its batch leaves.
// Time comes from a fake clock advanced by 1/rate per message, so the
// numbers describe the trigger, not the host's timer resolution.
func BenchmarkBatch(b *testing.B) {
	for _, k := range Kinds() {
		b.Run(k.String(), func(b *testing.B) {
			for _, rate := range benchRates {
				for _, linger := range benchLingers {
					b.Run(fmt.Sprintf("rate=%d/linger=%s", rate, linger), func(b *testing.B) {
						benchRate(b, k, rate, linger)
					})
				}
			}
			b.Run("Add", func(b *testing.B) { benchAdd(b, k) })
		})
	}
}

// benchRate drives one batcher at a simulated publish rate.
func benchRate(b *testing.B, k Kind, rate int, linger time.Duration) {
	f := clock.NewFake(modelStart)
	counting := clock.NewCounting(f)
	step := time.Duration(int64(time.Second) / int64(rate))
	bat := New[int](k, Config{MaxMessages: 100, MaxBytes: 1 << 20, Linger: linger, Clock: counting})
	defer func() { _ = bat.Close() }()
	dst := make([]int, 0, 128)
	sent := make([]time.Time, 0, 128)
	batches, msgs := 0, 0
	var totalWait time.Duration
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		f.Advance(step)
		now := f.Now()
		ready, err := bat.Add(msgs, 100)
		if err != nil {
			b.Fatal(err)
		}
		sent = append(sent, now)
		msgs++
		if !ready && !bat.Ready(now) {
			continue
		}
		var took bool
		dst, took = bat.TakeReady(now, dst[:0])
		if !took {
			continue
		}
		batches++
		for _, s := range sent[:len(dst)] {
			totalWait += now.Sub(s)
		}
		sent = sent[:0]
	}
	b.StopTimer()
	if msgs == 0 {
		return
	}
	b.ReportMetric(float64(counting.Created())/float64(msgs), "timers/op")
	b.ReportMetric(float64(batches)/float64(msgs), "batches/op")
	if batches > 0 {
		b.ReportMetric(float64(msgs)/float64(batches), "msgs/batch")
	}
	b.ReportMetric(float64(totalWait.Nanoseconds())/float64(msgs), "wait-ns/op")
	benchSink += len(dst)
}

// benchAdd isolates the cost of accumulating one item into a batch that is
// nowhere near its thresholds: the per-message cost every publisher pays.
func benchAdd(b *testing.B, k Kind) {
	f := clock.NewFake(modelStart)
	bat := New[int](k, Config{MaxMessages: 1 << 20, Linger: time.Millisecond, Clock: f})
	defer func() { _ = bat.Close() }()
	dst := make([]int, 0, 1<<20)
	b.ReportAllocs()
	n := 0
	for b.Loop() {
		if _, err := bat.Add(n, 64); err != nil {
			b.Fatal(err)
		}
		n++
		if n%(1<<20) == 0 {
			dst = bat.Flush(dst[:0])
		}
	}
	benchSink += len(dst)
}
