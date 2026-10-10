package opbench

import "testing"

// TestOutstandingBoundKeepsAPublishLeafInsideABoundedBroker pins the numbers
// The specification rests on. The Publish and PublishBatch leaves publish
// without waiting for anybody to read, and an in-process broker's lane is
// bounded with an overflow policy of BLOCK -- so a leaf whose outstanding
// count can reach the lane's capacity deadlocks against its own backpressure.
// Watched: at the memory driver's default 65,536-record lane capacity,
// `BenchmarkDriver/memory/PublishBatch/64B` published about eighty thousand
// hundred-message batches into a lane nothing was reading and stopped for
// ever, with the load guard still holding the host lock.
func TestOutstandingBoundKeepsAPublishLeafInsideABoundedBroker(t *testing.T) {
	// The memory driver's DefaultCapacityMessages. Hard-coded rather than
	// imported: this module depends on nothing, which is its reason to be a
	// module.
	const laneCapacity = 65536

	for _, size := range Sizes() {
		n := outstandingBound(size)
		if n < BatchSize {
			t.Errorf("%s: bound %d is below one batch of %d, so a leaf would drain inside every iteration and measure the drain",
				size.Name, n, BatchSize)
		}
		if n >= laneCapacity {
			t.Errorf("%s: bound %d reaches an in-process lane's capacity of %d, which is the deadlock",
				size.Name, n, laneCapacity)
		}
		if bytes := n * size.Bytes; bytes > drainBudgetBytes && n > BatchSize {
			t.Errorf("%s: bound %d holds %d bytes, over the %d-byte budget", size.Name, n, bytes, drainBudgetBytes)
		}
	}

	// The three canonical sizes, stated, so a change to either constant has to
	// be made deliberately.
	for _, tc := range []struct {
		size Size
		want int
	}{
		{Size{"64B", 64}, 8192},
		{Size{"1KiB", 1024}, 8192},
		{Size{"64KiB", 64 << 10}, 512},
	} {
		if got := outstandingBound(tc.size); got != tc.want {
			t.Errorf("outstandingBound(%s) = %d, want %d", tc.size.Name, got, tc.want)
		}
	}

	// A zero-byte payload has no byte bound to apply.
	if got := outstandingBound(Size{"0B", 0}); got != maxOutstanding {
		t.Errorf("outstandingBound(0 bytes) = %d, want %d", got, maxOutstanding)
	}
}

func TestNamesAndOpsFollowTheSpecification(t *testing.T) {
	if got, want := len(Ops())*len(Sizes()), 12; got != want {
		t.Fatalf("%d leaves, want %d", got, want)
	}
	native := Names("BenchmarkNative", "rueidis")
	driver := Names("BenchmarkDriver", "redis/streams")
	if len(native) != 12 || len(driver) != 12 {
		t.Fatalf("names: %d native, %d driver", len(native), len(driver))
	}
	if native[0] != "BenchmarkNative/rueidis/Publish/64B" {
		t.Errorf("first native name = %q", native[0])
	}
	if driver[11] != "BenchmarkDriver/redis/streams/EndToEnd/64KiB" {
		t.Errorf("last driver name = %q", driver[11])
	}
}
