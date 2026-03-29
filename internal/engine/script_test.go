package engine

import (
	"testing"
	"testing/synctest"
	"time"
)

// scriptConfig is the topology every canonical script runs on: two lanes, a
// small capacity so overflow is reachable, and a short lease so expiry is.
func scriptConfig(ks KindSet) harnessConfig {
	return harnessConfig{
		kinds:       ks,
		partitions:  2,
		capMessages: 4,
		ackDeadline: 10 * tick,
	}
}

// runScript drives one engine of the given kind set through the canonical
// script on the base topology.
func runScript(t *testing.T, ks KindSet) {
	t.Helper()
	runScriptOn(t, scriptConfig(ks))
}

// runScriptOn drives one engine through every operation of the engine slices
// and compares each of them with the reference model. It runs inside a
// synctest bubble, so the clock is deterministic and a goroutine the engine
// forgot would surface as a bubble deadlock.
func runScriptOn(t *testing.T, hc harnessConfig) {
	t.Helper()
	synctest.Test(t, func(t *testing.T) {
		h := newHarness(t, hc)
		defer h.close()

		// Publishing before anyone subscribes discards and releases — or,
		// on a stream, retains: the log is the topic's, not the
		// subscription set's (engine Topic.retainOnly).
		h.publish(-1, 8, 0)
		h.check()

		h.subscribe(0)
		h.subscribe(1)
		h.subscribe(0) // idempotent
		h.attach(0)
		h.attach(0)
		h.attach(1)
		h.check()

		// Fan-out: every subscription gets its own copy.
		for i := range 4 {
			h.publish(i%2, 8+i, 0)
		}
		h.check()

		// Round-robin partition selection and an explicit partition.
		h.publish(-1, 16, 0)
		h.publish(1, 16, 0)
		h.check()

		// Claim, settle, and settle again with the same and a forged
		// generation.
		h.fetch(0, 0, 3)
		h.check()
		h.settle(0, true, 0, 0)
		h.check()
		h.settle(0, true, 0, 0) // already settled
		h.settle(0, true, -1, 0)
		h.settle(0, true, 1, 0)
		h.check()

		// Negative settlement, immediate and delayed.
		h.settle(1, false, 0, 0)
		h.settle(2, false, 0, 4*tick)
		h.check()
		h.fetch(0, 1, 2)
		h.check()

		// The delayed record is not delivered before its due time.
		h.fetch(0, 0, 4)
		h.check()
		h.advance(4 * tick)
		h.fetch(0, 0, 4)
		h.check()

		// Lease extension keeps a record in flight across an expiry sweep.
		h.extend(0, 0, 20*tick)
		h.check()
		h.advance(11 * tick)
		h.expire()
		h.check()

		// A closed consumer returns its leases to the lane.
		h.fetch(1, 0, 4)
		h.check()
		h.closeConsumer(1, 0)
		h.check()

		// Capacity: the lane holds four records, so a fifth is refused for
		// every subscription.
		for range 8 {
			h.publish(0, 4, 0)
		}
		h.check()

		// Extending and settling through a stale handle after redelivery.
		h.advance(30 * tick)
		h.expire()
		h.check()
		h.extend(0, 0, tick)
		h.settle(0, true, 0, 0)
		h.check()

		// The slice-4 operations. Each is a no-op on a topology that does
		// not offer it, exactly as the engine ignores the field, so the
		// same script runs on every topology of the matrix.
		h.publishFull(0, 4, 0, PriorityLevels-1, "dk")
		h.publishFull(0, 4, 0, 0, "dk") // deduplicated where the window is on
		h.publishFull(1, 4, 0, 1, "")
		h.check()
		h.rebalance(0)
		h.check()
		h.fetch(0, 0, 4)
		h.check()
		h.seek(0, StartEarliest, 0, 0)
		h.check()
		h.seek(0, StartAtSeq, 2, 0)
		h.check()
		// Subscription 1 carries a narrowing subject filter on a wildcard
		// run, so its replay must admit only the retained records the
		// filter matches.
		h.seek(1, StartEarliest, 0, 0)
		h.check()
		// A time position that is not the beginning of the log: with a zero
		// At, StartAtTime takes everything and is indistinguishable from
		// StartEarliest on both sides.
		h.seek(0, StartAtTime, 0, 8*tick)
		h.check()
		h.fetch(0, 1, 4)
		h.settle(0, true, 0, 0)
		h.check()

		// Closing one subscription leaves the other untouched.
		h.closeSub(1)
		h.check()
		h.publish(-1, 8, 0)
		h.check()

		// Closing the engine drops and releases everything left.
		h.closeEngine()
		h.check()
		h.publish(-1, 8, 0)
		h.fetch(0, 0, 2)
		h.check()
	})
}

// TestEngineScript runs the canonical script on the default kind set.
func TestEngineScript(t *testing.T) {
	runScript(t, KindSet{})
}

// TestEngineScriptSafe runs it on the Safe reference set.
func TestEngineScriptSafe(t *testing.T) {
	runScript(t, KindSet{Safe: true})
}

// TestScriptClockAlignment guards the assumption the differential rests on:
// the bubble clock starts on a scheduler tick, so a due time computed from it
// needs no rounding.
func TestScriptClockAlignment(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		if n := time.Now().UnixNano(); n%int64(tick) != 0 {
			t.Fatalf("bubble clock starts at %d, which is not a multiple of %v", n, tick)
		}
	})
}
