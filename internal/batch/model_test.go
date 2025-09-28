package batch

import (
	"slices"
	"testing"
	"testing/synctest"
	"time"

	"bitbucket.org/iqhive/iqmq/internal/clock"
	"bitbucket.org/iqhive/iqmq/internal/errcode"
)

// TestModel checks every kind against the batching contract: the size
// thresholds, the linger promise, conservation (nothing is stranded or
// duplicated), the wake-up channel, closing, and concurrent use.
func TestModel(t *testing.T) {
	for _, k := range Kinds() {
		t.Run(k.String(), func(t *testing.T) {
			t.Run("count", func(t *testing.T) { modelCount(t, k) })
			t.Run("bytes", func(t *testing.T) { modelBytes(t, k) })
			t.Run("linger", func(t *testing.T) { modelLinger(t, k) })
			t.Run("conservation", func(t *testing.T) { modelConservation(t, k) })
			t.Run("close", func(t *testing.T) { modelClose(t, k) })
			t.Run("wake", func(t *testing.T) { modelWake(t, k) })
			t.Run("concurrent", func(t *testing.T) { modelConcurrent(t, k) })
		})
	}
}

var modelStart = time.Date(2026, 9, 6, 12, 0, 0, 0, time.UTC)

// newTestBatcher returns a batcher of kind k on a fake clock.
func newTestBatcher(t *testing.T, k Kind, cfg Config) (Batcher[int], *clock.Fake) {
	t.Helper()
	f := clock.NewFake(modelStart)
	cfg.Clock = f
	b := New[int](k, cfg)
	t.Cleanup(func() {
		if err := b.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	})
	return b, f
}

// modelCount: a batch is ready exactly when it holds MaxMessages items.
func modelCount(t *testing.T, k Kind) {
	b, f := newTestBatcher(t, k, Config{MaxMessages: 4, Linger: time.Hour})
	for i := range 3 {
		ready, err := b.Add(i, 1)
		if err != nil {
			t.Fatal(err)
		}
		if ready {
			t.Fatalf("ready after %d of 4 items", i+1)
		}
		if b.Len() != i+1 {
			t.Fatalf("Len = %d, want %d", b.Len(), i+1)
		}
	}
	ready, err := b.Add(3, 1)
	if err != nil || !ready {
		t.Fatalf("Add(4th) = %v %v", ready, err)
	}
	if !b.Ready(f.Now()) {
		t.Fatal("Ready is false with a full batch")
	}
	out, took := b.TakeReady(f.Now(), nil)
	if !took || !slices.Equal(out, []int{0, 1, 2, 3}) {
		t.Fatalf("TakeReady = %v %v", out, took)
	}
	if b.Len() != 0 || b.Bytes() != 0 || b.Ready(f.Now()) {
		t.Fatal("batcher not empty after TakeReady")
	}
}

// modelBytes: the byte threshold works the same way, and one oversized
// item makes its batch ready on its own.
func modelBytes(t *testing.T, k Kind) {
	b, f := newTestBatcher(t, k, Config{MaxBytes: 100, Linger: time.Hour})
	if ready, _ := b.Add(1, 60); ready {
		t.Fatal("ready at 60 of 100 bytes")
	}
	if b.Bytes() != 60 {
		t.Fatalf("Bytes = %d", b.Bytes())
	}
	if ready, _ := b.Add(2, 40); !ready {
		t.Fatal("not ready at 100 bytes")
	}
	if out, took := b.TakeReady(f.Now(), nil); !took || len(out) != 2 {
		t.Fatalf("TakeReady = %v %v", out, took)
	}
	if ready, _ := b.Add(3, 1000); !ready {
		t.Fatal("an item larger than MaxBytes did not make its batch ready")
	}
	// A negative size counts as zero rather than corrupting the total.
	b.Flush(nil)
	if ready, _ := b.Add(4, -5); ready {
		t.Fatal("negative size made the batch ready")
	}
	if b.Bytes() != 0 {
		t.Fatalf("negative size counted %d bytes", b.Bytes())
	}
}

// modelLinger: a timed kind promises a partly filled batch leaves within
// Linger (rounded up to the tick for SharedTicker); SizeOnly promises
// nothing and must never claim readiness on time alone.
func modelLinger(t *testing.T, k Kind) {
	const linger = 10 * time.Millisecond
	b, f := newTestBatcher(t, k, Config{MaxMessages: 1000, Linger: linger})
	if _, err := b.Add(1, 1); err != nil {
		t.Fatal(err)
	}
	deadline, timed := b.Deadline()
	if timed != k.Timed() {
		t.Fatalf("Deadline timed = %v, want %v", timed, k.Timed())
	}
	if !k.Timed() {
		f.Advance(time.Hour)
		if b.Ready(f.Now()) {
			t.Fatal("SizeOnly claimed readiness from the clock")
		}
		if _, took := b.TakeReady(f.Now(), nil); took {
			t.Fatal("SizeOnly took a batch that was not full")
		}
		if out := b.Flush(nil); len(out) != 1 {
			t.Fatalf("Flush returned %v", out)
		}
		return
	}
	want := modelStart.Add(linger)
	switch {
	case k.Exact() && !deadline.Equal(want):
		t.Fatalf("deadline %v, want %v", deadline, want)
	case !k.Exact() && (deadline.Before(want) || deadline.After(want.Add(linger))):
		t.Fatalf("deadline %v, want within one tick of %v", deadline, want)
	}
	// One nanosecond before the deadline the batch waits; at the deadline
	// it is ready.
	f.Set(deadline.Add(-time.Nanosecond))
	if b.Ready(f.Now()) {
		t.Fatalf("ready %v before the deadline", deadline.Sub(f.Now()))
	}
	f.Set(deadline)
	if !b.Ready(f.Now()) {
		t.Fatal("not ready at the deadline")
	}
	out, took := b.TakeReady(f.Now(), nil)
	if !took || len(out) != 1 {
		t.Fatalf("TakeReady = %v %v", out, took)
	}
	// The next batch gets a fresh deadline measured from its own first
	// item, not from the previous batch.
	f.Advance(time.Second)
	if _, err := b.Add(2, 1); err != nil {
		t.Fatal(err)
	}
	second, _ := b.Deadline()
	if !second.After(deadline) {
		t.Fatalf("second batch deadline %v is not after the first %v", second, deadline)
	}
	if b.Ready(f.Now()) {
		t.Fatal("the second batch inherited the first deadline")
	}
}

// modelConservation: over a long random-ish run, every item added comes
// back exactly once, in order.
func modelConservation(t *testing.T, k Kind) {
	b, f := newTestBatcher(t, k, Config{MaxMessages: 8, MaxBytes: 64, Linger: 5 * time.Millisecond})
	var got []int
	next := 0
	for step := range 2000 {
		switch step % 7 {
		case 0, 1, 2, 3:
			ready, err := b.Add(next, 1+step%9)
			if err != nil {
				t.Fatal(err)
			}
			next++
			if ready {
				got, _ = b.TakeReady(f.Now(), got)
			}
		case 4:
			f.Advance(time.Millisecond)
			got, _ = b.TakeReady(f.Now(), got)
		case 5:
			f.Advance(3 * time.Millisecond)
			got, _ = b.TakeReady(f.Now(), got)
		case 6:
			if step%21 == 6 {
				got = b.Flush(got)
			}
		}
		if b.Len() > 8 {
			t.Fatalf("step %d: %d pending items exceed MaxMessages", step, b.Len())
		}
	}
	got = b.Flush(got)
	if len(got) != next {
		t.Fatalf("%d items in, %d out", next, len(got))
	}
	for i, v := range got {
		if v != i {
			t.Fatalf("item %d came back as %d: order or conservation broken", i, v)
		}
	}
}

// modelClose: Close is idempotent, rejects further items and strands none.
func modelClose(t *testing.T, k Kind) {
	f := clock.NewFake(modelStart)
	b := New[int](k, Config{MaxMessages: 100, Linger: time.Millisecond, Clock: f})
	for i := range 3 {
		if _, err := b.Add(i, 1); err != nil {
			t.Fatal(err)
		}
	}
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	if err := b.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	if _, err := b.Add(9, 1); err == nil {
		t.Fatal("Add after Close succeeded")
	} else if errcode.CodeOf(err) != errcode.Closed {
		t.Fatalf("Add after Close: code %v", errcode.CodeOf(err))
	}
	if b.Len() != 3 {
		t.Fatalf("Close discarded items: Len = %d", b.Len())
	}
	if out := b.Flush(nil); !slices.Equal(out, []int{0, 1, 2}) {
		t.Fatalf("Flush after Close = %v", out)
	}
	if b.Wait() != nil {
		t.Fatal("Wait after Close is not nil")
	}
}

// modelWake: a caller blocked on Wait is woken within the linger, and the
// batch is ready when it wakes. It runs in a synctest bubble, where the
// real clock is virtual, so the timer and the shared ticker are exercised
// exactly as they are in production.
func modelWake(t *testing.T, k Kind) {
	synctest.Test(t, func(t *testing.T) {
		const linger = 20 * time.Millisecond
		b := New[int](k, Config{MaxMessages: 1000, Linger: linger})
		defer func() {
			if err := b.Close(); err != nil {
				t.Error(err)
			}
		}()
		start := time.Now()
		if _, err := b.Add(1, 1); err != nil {
			t.Fatal(err)
		}
		ch := b.Wait()
		if !k.Timed() {
			if ch != nil {
				t.Fatal("SizeOnly armed a wake-up")
			}
			return
		}
		if ch == nil {
			t.Fatal("no wake-up channel for a timed kind")
		}
		select {
		case <-ch:
		case <-time.After(10 * linger):
			t.Fatal("no wake-up within ten lingers")
		}
		waited := time.Since(start)
		if waited < linger {
			t.Fatalf("woken after %v, before the linger %v", waited, linger)
		}
		if max := linger + tickPeriod(linger); waited > max {
			t.Fatalf("woken after %v, later than %v", waited, max)
		}
		if !b.Ready(time.Now()) {
			t.Fatal("woken with a batch that is not ready")
		}
		if out, took := b.TakeReady(time.Now(), nil); !took || len(out) != 1 {
			t.Fatalf("TakeReady after the wake-up = %v %v", out, took)
		}
	})
}

// modelConcurrent: many producers and one consumer lose nothing under the
// race detector.
func modelConcurrent(t *testing.T, k Kind) {
	synctest.Test(t, func(t *testing.T) {
		b := New[int](k, Config{MaxMessages: 16, Linger: time.Millisecond})
		defer func() { _ = b.Close() }()
		const producers, per = 8, 500
		done := make(chan struct{})
		var got []int
		go func() {
			defer close(done)
			for len(got) < producers*per {
				if out, took := b.TakeReady(time.Now(), got); took {
					got = out
					continue
				}
				if ch := b.Wait(); ch != nil {
					select {
					case <-ch:
					case <-time.After(time.Millisecond):
					}
					continue
				}
				time.Sleep(100 * time.Microsecond)
				got = b.Flush(got)
			}
		}()
		for p := range producers {
			go func() {
				for i := range per {
					if _, err := b.Add(p*per+i, 1); err != nil {
						t.Error(err)
						return
					}
					if i%13 == 0 {
						time.Sleep(50 * time.Microsecond)
					}
				}
			}()
		}
		select {
		case <-done:
		case <-time.After(time.Minute):
			t.Fatal("consumer did not collect every item")
		}
		seen := make(map[int]bool, producers*per)
		for _, v := range got {
			if seen[v] {
				t.Fatalf("item %d delivered twice", v)
			}
			seen[v] = true
		}
		if len(seen) != producers*per {
			t.Fatalf("%d of %d items delivered", len(seen), producers*per)
		}
	})
}
