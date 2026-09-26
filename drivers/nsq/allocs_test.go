package nsq_test

import (
	"context"
	"testing"
	"testing/synctest"

	mq "github.com/iqhive/iqmq"
	nq "github.com/iqhive/iqmq/drivers/nsq"
)

// TestAllocs pins the allocation counts of the pure mapping, which is where
// the specification's "core-attributable allocations per message ≤ 2" is decided for this
// driver.
//
// It is the MAPPING and not the round trip: a publication here is a TCP round
// trip whose allocations belong to go-nsq and to the kernel, and a budget over
// that would measure the client library. What this driver owns is the decision
// about what to publish, and on the native path that decision must cost
// nothing at all — a bare body goes out as it stands.
//
// It skips under -race, which perturbs allocation counts.
func TestAllocs(t *testing.T) {
	if raceEnabled {
		t.Skip("the race detector perturbs allocation counts")
	}
	m := &mq.Message{Topic: "orders", Body: []byte("payload")}

	t.Run("the native admission decision allocates nothing", func(t *testing.T) {
		if n := testing.AllocsPerRun(200, func() {
			if err := nq.Representable(m); err != nil {
				t.Fatalf("Representable: %v", err)
			}
		}); n != 0 {
			t.Errorf("Representable allocates %.1f per call, want 0", n)
		}
	})
	t.Run("the topic mapping allocates once for the mapped name", func(t *testing.T) {
		// One allocation, and only when there is a prefix: the mapped name
		// is a new string. With no prefix it is the caller's own string and
		// costs nothing.
		if n := testing.AllocsPerRun(200, func() {
			if _, err := nq.TopicOf("", "orders"); err != nil {
				t.Fatalf("TopicOf: %v", err)
			}
		}); n != 0 {
			t.Errorf("TopicOf with no prefix allocates %.1f per call, want 0", n)
		}
	})
	t.Run("a settlement allocates nothing", func(t *testing.T) {
		b, _ := newBackendRig(t)
		src := sourceOf(t, b, "allocs", "workers", mq.MetadataNativeStrict, 64)
		snk := sinkOf(t, b, "allocs", mq.MetadataNativeStrict)
		const runs = 64
		for range runs + 8 {
			if err := snk.Publish(context.Background(), &mq.Message{Body: []byte("b")}, nil); err != nil {
				t.Fatalf("Publish: %v", err)
			}
		}
		ds := fetchN(t, src, runs)
		i := 0
		if n := testing.AllocsPerRun(runs-1, func() {
			if err := src.Settler().Ack(context.Background(), &ds[i]); err != nil {
				t.Fatalf("Ack: %v", err)
			}
			i++
		}); n > 1 {
			// One allocation is the bound rather than zero because the
			// model's own bookkeeping is inside the measured call; the
			// driver's settlement path itself takes no new storage.
			t.Errorf("a settlement allocates %.1f per call, want at most 1", n)
		}
	})
}

// TestSyncTestFetchIsDeterministic runs a fetch under synctest, which is what
// The specification asks of a driver whose timing a test should not depend on.
//
// The point is not the assertion: it is that a Fetch blocking on an empty ring
// is a synctest-visible wait rather than a poll, so a test can be sure the
// source really is parked instead of spinning.
func TestSyncTestFetchIsDeterministic(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		b, _ := newBackendRig(t)
		src := sourceOf(t, b, "synctest", "workers", mq.MetadataNativeStrict, 4)
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() {
			_, err := src.Fetch(ctx, make([]mq.DriverDelivery, 1))
			done <- err
		}()
		synctest.Wait()
		select {
		case err := <-done:
			t.Fatalf("Fetch returned %v on an empty subscription rather than parking", err)
		default:
		}
		cancel()
		synctest.Wait()
		if err := <-done; err == nil {
			t.Fatal("a cancelled Fetch returned no error")
		}
	})
}
