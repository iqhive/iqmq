package iqmq

import "testing"

// TestStatsCountersHoldOnlyCellsTheyWrite: the root counts with Add and never
// with AddAt, so a counter layout whose Add lands on one shared cell has
// nothing to put in any other cell, and every other cell is allocated and
// summed on every Load for nothing. stats.go said the counters used the
// striped layout, which spreads Add over its cells. They were built with
// stats.Auto, the per-worker layout, whose Add writes one shared cell beside
// eight that only AddAt writes.
func TestStatsCountersHoldOnlyCellsTheyWrite(t *testing.T) {
	if cells := statsKind.Cells(1); statsKind.SharedAdd() && cells != 1 {
		t.Fatalf("the root counters are %v: Add writes one shared cell and each counter holds %d", statsKind.Resolve(), cells)
	}
}
