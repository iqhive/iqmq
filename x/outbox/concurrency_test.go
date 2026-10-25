package outbox_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/x/outbox"
)

// TestConcurrencyAboveOnePublishesEveryRow covers the path
// TestConcurrencyOneKeepsRowOrder cannot reach.
//
// `Relay.publishRows` branches on `Concurrency > 1` into a separate function
// that starts a goroutine per row and writes its results into a shared
// `[]bool` by index. Every other test in this file runs at Concurrency 1, so
// that branch -- the only concurrent code in the package -- was exercised by
// nothing, and `-race` cannot report a race in a path no test enters. The
// gap was found by reading the branch rather than by a failure, which is why
// this test says so.
//
// What it holds: every row is published exactly once, the sent/unsent
// partition is still right when some rows fail, and the marking rule from
// TestAPartialBatchMarksOnlyWhatWasPublished survives concurrency.
func TestConcurrencyAboveOnePublishesEveryRow(t *testing.T) {
	for _, concurrency := range []int{2, 4, 16} {
		t.Run(name(concurrency), func(t *testing.T) {
			s, err := outbox.NewMemoryStore(outbox.MemoryOptions{})
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = s.Close() }()

			const rows = 64
			for i := range rows {
				if _, err := s.Append(outbox.Row{Topic: "t", Body: []byte{byte(i)}}); err != nil {
					t.Fatal(err)
				}
			}

			// Every fifth row fails, so the partition is non-trivial.
			var mu sync.Mutex
			seen := map[byte]int{}
			var calls atomic.Int64
			publish := func(_ context.Context, _ string, ms []*mq.Message, _ []mq.PublishOption) error {
				calls.Add(1)
				mu.Lock()
				defer mu.Unlock()
				for _, m := range ms {
					seen[m.Body[0]]++
					if m.Body[0]%5 == 0 {
						return rowFault("the broker refused this one")
					}
				}
				return nil
			}

			r, err := outbox.New(outbox.Config{
				Store: s, Publish: publish, Batch: rows, Concurrency: concurrency,
			})
			if err != nil {
				t.Fatal(err)
			}
			n, err := r.Drain(context.Background())
			if err != nil {
				t.Fatal(err)
			}

			// Every row was attempted exactly once.
			mu.Lock()
			attempted := len(seen)
			var twice []byte
			for b, c := range seen {
				if c != 1 {
					twice = append(twice, b)
				}
			}
			mu.Unlock()
			if attempted != rows {
				t.Errorf("%d distinct row(s) reached the publisher, want %d", attempted, rows)
			}
			if len(twice) > 0 {
				t.Errorf("%d row(s) were published more than once in a single pass: %v", len(twice), twice)
			}
			if got := calls.Load(); got != rows {
				t.Errorf("the publisher was called %d time(s), want %d", got, rows)
			}

			// 13 of 64 bodies are divisible by five (0, 5, …, 60), so 51
			// succeed. The number is computed rather than written down, so
			// the test does not encode an arithmetic slip of its own.
			wantSent := 0
			for i := range rows {
				if byte(i)%5 != 0 {
					wantSent++
				}
			}
			if n != wantSent {
				t.Errorf("Drain published %d, want %d", n, wantSent)
			}
			if got := len(s.Sent()); got != wantSent {
				t.Errorf("%d row(s) marked sent, want %d", got, wantSent)
			}
			if got := s.Unsent(); got != rows-wantSent {
				t.Errorf("%d row(s) unsent, want %d", got, rows-wantSent)
			}
			// And no row that failed was marked: the rule the serial path
			// is held to, under concurrency.
			for _, id := range s.Sent() {
				if err := s.FailureOf(id); err != nil {
					t.Errorf("row %s is marked sent and was recorded as failed with %v", id, err)
				}
			}
			if st := r.Stats(); st.Sent != uint64(wantSent) || st.Failed != uint64(rows-wantSent) {
				t.Errorf("Stats = %+v, want Sent %d and Failed %d", st, wantSent, rows-wantSent)
			}
		})
	}
}

// name renders a concurrency for a subtest name without strconv, which this
// package's own code also avoids.
func name(n int) string {
	if n < 10 {
		return "concurrency=" + string(rune('0'+n))
	}
	return "concurrency=" + string(rune('0'+n/10)) + string(rune('0'+n%10))
}

// TestConcurrencyBoundsTheRowsInFlight holds the concurrent path to its
// bound in both directions: it reaches Concurrency publications at once, and
// it never exceeds it. A relay that overshot would open more broker
// round trips than the caller allowed; one that undershot would make the
// option a lie.
//
// The publisher parks every call until Concurrency of them are in flight at
// the same moment, so the peak is reached deterministically rather than by
// luck of scheduling, and a bound that admitted one more would show as a
// peak above it.
func TestConcurrencyBoundsTheRowsInFlight(t *testing.T) {
	for _, concurrency := range []int{2, 4} {
		t.Run(name(concurrency), func(t *testing.T) {
			s, err := outbox.NewMemoryStore(outbox.MemoryOptions{})
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = s.Close() }()

			const rows = 32
			for i := range rows {
				if _, err := s.Append(outbox.Row{Topic: "t", Body: []byte{byte(i)}}); err != nil {
					t.Fatal(err)
				}
			}

			var (
				mu       sync.Mutex
				inFlight int
				peak     int
				full     = make(chan struct{})
				fullOnce sync.Once
			)
			publish := func(ctx context.Context, _ string, _ []*mq.Message, _ []mq.PublishOption) error {
				mu.Lock()
				inFlight++
				peak = max(peak, inFlight)
				reached := inFlight == concurrency
				mu.Unlock()
				if reached {
					fullOnce.Do(func() { close(full) })
				}
				select {
				case <-full:
				case <-ctx.Done():
				}
				mu.Lock()
				inFlight--
				mu.Unlock()
				return nil
			}

			r, err := outbox.New(outbox.Config{
				Store: s, Publish: publish, Batch: rows, Concurrency: concurrency,
			})
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			n, err := r.Drain(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if n != rows {
				t.Errorf("Drain published %d, want %d", n, rows)
			}
			mu.Lock()
			defer mu.Unlock()
			if peak != concurrency {
				t.Errorf("%d publications were in flight at once, want exactly %d", peak, concurrency)
			}
		})
	}
}
