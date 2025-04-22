package linear

import (
	"strconv"
	"strings"
	"testing"
)

// regState is the model state of a one-register test: the value held.
type regState struct{ v int }

// regModel is a read/write register: write(v) always succeeds, read returns
// the value written by the most recent linearized write.
func regModel() Model[regState] {
	return Model[regState]{
		Init: func(string) regState { return regState{} },
		Step: func(s regState, op Op) (regState, bool) {
			switch op.Kind {
			case "write":
				return regState{v: op.Arg.(int)}, true
			default:
				return s, s.v == op.Ret.(int)
			}
		},
		Key: func(s regState) string { return strconv.Itoa(s.v) },
	}
}

func op(client int, kind string, arg, ret any, start, end int64) Op {
	return Op{Client: client, Kind: kind, Arg: arg, Ret: ret, Start: start, End: end}
}

func TestCheckSequential(t *testing.T) {
	m := regModel()
	m.Partition = nil
	h := []Op{
		op(0, "write", 1, nil, 1, 2),
		op(0, "read", 0, 1, 3, 4),
		op(0, "write", 2, nil, 5, 6),
		op(0, "read", 0, 2, 7, 8),
	}
	r := Check(m, h, 0)
	if !r.OK {
		t.Fatalf("sequential history rejected: %+v\n%s", r, FormatHistory(h))
	}
	if len(r.Witness) != len(h) {
		t.Fatalf("witness has %d of %d operations", len(r.Witness), len(h))
	}
}

// TestCheckConcurrentAccepted: a read concurrent with a write may be ordered
// before or after it, so both returns are linearizable.
func TestCheckConcurrentAccepted(t *testing.T) {
	m := regModel()
	m.Partition = nil
	for _, ret := range []int{0, 1} {
		h := []Op{
			op(0, "write", 1, nil, 1, 10),
			op(1, "read", 0, ret, 2, 9),
		}
		if r := Check(m, h, 0); !r.OK {
			t.Fatalf("read=%d rejected: %+v", ret, r)
		}
	}
}

// TestCheckRejectsImpossible: a read that returns a value no write produced,
// and a read ordered strictly after a write that returns the stale value, are
// both refused.
func TestCheckRejectsImpossible(t *testing.T) {
	m := regModel()
	m.Partition = nil
	bogus := []Op{
		op(0, "write", 1, nil, 1, 2),
		op(0, "read", 0, 7, 3, 4),
	}
	if r := Check(m, bogus, 0); r.OK {
		t.Fatalf("history with an invented value accepted: %+v", r)
	}
	stale := []Op{
		op(0, "write", 1, nil, 1, 2),
		op(1, "read", 0, 0, 3, 4),
	}
	r := Check(m, stale, 0)
	if r.OK {
		t.Fatalf("stale read accepted: %+v", r)
	}
	if r.Witness == nil {
		t.Fatal("no witness prefix for the rejected history")
	}
}

// TestCheckPartitionsAreIndependent: two registers interleave freely, and a
// violation in one partition is still caught.
func TestCheckPartitionsAreIndependent(t *testing.T) {
	m := regModel()
	m.Partition = func(op Op) string {
		if op.Kind == "write" {
			return strconv.Itoa(op.Arg.(int) % 2)
		}
		return strconv.Itoa(op.Ret.(int) % 2)
	}
	h := make([]Op, 0, 5)
	h = append(h,
		op(0, "write", 2, nil, 1, 2),
		op(1, "write", 3, nil, 1, 2),
		op(0, "read", 0, 2, 3, 4),
		op(1, "read", 0, 3, 3, 4))
	if r := Check(m, h, 0); !r.OK {
		t.Fatalf("independent partitions rejected: %+v\n%s", r, FormatHistory(h))
	}
	h = append(h, op(0, "read", 0, 4, 5, 6))
	if r := Check(m, h, 0); r.OK {
		t.Fatalf("violation inside a partition accepted: %+v", r)
	}
}

// TestCheckMemoisationBoundsSearch: a wholly concurrent history of n writes
// and reads has n! orders; memoisation must keep the search far below that.
func TestCheckMemoisationBoundsSearch(t *testing.T) {
	m := regModel()
	m.Partition = nil
	const n = 9
	h := make([]Op, 0, 2*n)
	for i := range n {
		h = append(h,
			op(i, "write", i+1, nil, 1, 100),
			op(i, "read", 0, i+1, 1, 100))
	}
	r := Check(m, h, 1<<18)
	if !r.OK {
		t.Fatalf("concurrent history rejected: %+v", r)
	}
	if r.Limited {
		t.Fatalf("search hit its limit after %d steps", r.Explored)
	}
	if r.Explored > 5000 {
		t.Fatalf("search explored %d states; memoisation is not pruning", r.Explored)
	}
}

// TestCheckLimitIsInconclusive: a tiny budget reports Limited rather than a
// false verdict.
func TestCheckLimitIsInconclusive(t *testing.T) {
	m := regModel()
	m.Partition = nil
	h := make([]Op, 0, 40)
	for i := range 20 {
		h = append(h,
			op(i%4, "write", i+1, nil, 1, 100),
			op(i%4, "read", 0, i+1, 1, 100))
	}
	r := Check(m, h, 4)
	if !r.Limited {
		t.Fatalf("a four-step budget was not reported as limited: %+v", r)
	}
	if r.OK {
		t.Fatal("a limited search must not claim the history is linearizable")
	}
}

// TestNowIsMonotone checks the global logical clock.
func TestNowIsMonotone(t *testing.T) {
	prev := Now()
	for range 1000 {
		n := Now()
		if n <= prev {
			t.Fatalf("clock went %d -> %d", prev, n)
		}
		prev = n
	}
}

// TestFormatHistory renders every value kind it supports.
func TestFormatHistory(t *testing.T) {
	s := FormatHistory([]Op{
		op(1, "publish", 7, nil, 1, 2),
		op(2, "fetch", "k", uint64(9), 3, 4),
		op(3, "ack", true, []int{1, 2}, 5, 6),
	})
	for _, want := range []string{"c1 [1,2] publish(7) = -", "c2 [3,4] fetch(k) = 9", "c3 [5,6] ack(true) = [1 2]"} {
		if !strings.Contains(s, want) {
			t.Fatalf("history %q does not contain %q", s, want)
		}
	}
}

// TestEmptyHistory is trivially linearizable.
func TestEmptyHistory(t *testing.T) {
	m := regModel()
	m.Partition = nil
	if r := Check(m, nil, 0); !r.OK {
		t.Fatalf("empty history rejected: %+v", r)
	}
}
