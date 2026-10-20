// Package linear checks recorded operation histories for linearizability
// against a sequential model (the specification C, the test suite).
//
// A history is a list of Ops, each with the interval [Start, End) during which
// it was in flight and the value it returned. The history is linearizable when
// there is a total order of the operations that respects the real-time order
// of non-overlapping operations and that the model accepts. Check searches for
// such an order with the Wing and Gong depth-first algorithm, extended with
// memoisation on the pair (set of linearized operations, model state key) so
// that the many interleavings which reach the same state are explored once.
//
// Histories are partitioned first: operations whose Partition differs cannot
// influence one another — in iqmq that is one partition per subscription and
// routing key — and each partition is checked on its own, which is what keeps
// the search tractable.
//
// The package is stdlib-only and imports nothing from iqmq, so it can be used
// by the engine, by the public API and by driver conformance alike.
package linear

import (
	"strconv"
	"strings"
	"sync/atomic"
)

// Clock is a logical clock. It is monotonic, so intervals recorded by
// different goroutines sharing one Clock are comparable. Under
// testing/synctest the wall clock does not advance while goroutines run,
// which is why the ordering comes from this counter rather than time. The
// zero value is ready; a history's recorders share one.
type Clock struct{ n atomic.Int64 }

// Now returns the clock's next value. Every call returns a distinct,
// increasing value.
func (c *Clock) Now() int64 { return c.n.Add(1) }

// Op is one recorded operation.
type Op struct {
	// Client identifies the goroutine that issued the operation. Operations
	// of one client never overlap.
	Client int
	// Kind names the operation, for example "publish" or "ack".
	Kind string
	// Arg is the operation's argument.
	Arg any
	// Ret is what the operation returned.
	Ret any
	// Start and End bracket the operation on the logical clock: Start is
	// taken before the call and End after it returns.
	Start int64
	End   int64
}

// Model is the sequential specification a history is checked against.
//
// Step must be pure: it must not modify s, and it must return an independent
// state. Key must map states that are equivalent for the rest of the history
// to the same string, and states that differ to different strings.
type Model[S any] struct {
	// Init returns the initial state of one partition.
	Init func(partition string) S
	// Step applies op to s. It returns the resulting state and whether the
	// model accepts op's return value in that state.
	Step func(s S, op Op) (S, bool)
	// Key returns the memoisation key of a state.
	Key func(s S) string
	// Partition groups operations that can be checked independently. A nil
	// Partition puts every operation in one partition.
	Partition func(op Op) string
}

// Result is the outcome of a check.
type Result struct {
	// OK reports whether a linearization was found for every partition.
	OK bool
	// Witness is the linearization found for the first partition that has
	// one when OK, and the longest prefix explored otherwise. It is a
	// diagnostic, not a proof.
	Witness []Op
	// Explored counts the model steps the search took.
	Explored int
	// Limited reports that the search hit the step limit before it could
	// decide, which makes OK inconclusive rather than a proof.
	Limited bool
	// Partition names the partition the witness or the failure belongs to.
	Partition string
}

// Check reports whether h is linearizable with respect to m. limit bounds the
// number of model steps per partition; a non-positive limit means 1e6.
func Check[S any](m Model[S], h []Op, limit int) Result {
	if limit <= 0 {
		limit = 1 << 20
	}
	parts := partition(m, h)
	out := Result{OK: true}
	for _, name := range sortedKeys(parts) {
		ops := parts[name]
		r := checkOne(m, name, ops, limit)
		out.Explored += r.Explored
		out.Limited = out.Limited || r.Limited
		if !r.OK {
			r.Explored = out.Explored
			r.Limited = out.Limited
			return r
		}
		if len(r.Witness) > len(out.Witness) {
			out.Witness = r.Witness
			out.Partition = name
		}
	}
	return out
}

// partition groups the history.
func partition[S any](m Model[S], h []Op) map[string][]Op {
	parts := map[string][]Op{}
	for _, op := range h {
		key := ""
		if m.Partition != nil {
			key = m.Partition(op)
		}
		parts[key] = append(parts[key], op)
	}
	return parts
}

func sortedKeys(m map[string][]Op) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	for i := 1; i < len(out); i++ {
		for j := i; j > 0 && out[j] < out[j-1]; j-- {
			out[j], out[j-1] = out[j-1], out[j]
		}
	}
	return out
}

// entry is one operation in the doubly linked list the search consumes.
type entry struct {
	op   Op
	idx  int
	prev *entry
	next *entry
}

// checkOne runs the Wing and Gong search over one partition.
func checkOne[S any](m Model[S], name string, ops []Op, limit int) Result {
	if len(ops) == 0 {
		return Result{OK: true, Partition: name}
	}
	if len(ops) > maxOps {
		// The bitmask memo needs one bit per operation; a longer history is
		// checked in prefix, which is still a sound refutation.
		ops = ops[:maxOps]
	}
	sorted := byStart(ops)
	head := buildList(sorted)
	sr := &searchState{
		seen:    map[memoKey]bool{},
		witness: make([]Op, 0, len(sorted)),
		limit:   limit,
	}
	ok := search(m, head, m.Init(name), 0, sr)
	res := Result{OK: ok, Explored: sr.steps, Partition: name, Limited: sr.steps >= limit}
	if ok {
		res.Witness = append([]Op(nil), sr.witness...)
	} else {
		res.Witness = sr.best
	}
	return res
}

// searchState carries the mutable part of the depth-first search: the memo
// table, the current candidate linearization, the longest prefix reached (a
// diagnostic for failures) and the step budget.
type searchState struct {
	seen    map[memoKey]bool
	witness []Op
	best    []Op
	steps   int
	limit   int
}

// maxOps bounds the operations checked in one partition, because the memo key
// carries one bit per operation.
const maxOps = 62

// memoKey identifies a search node: which operations are linearized and what
// the model state is.
type memoKey struct {
	mask uint64
	key  string
}

// byStart returns ops ordered by Start.
func byStart(ops []Op) []Op {
	out := append([]Op(nil), ops...)
	for i := 1; i < len(out); i++ {
		for j := i; j > 0 && out[j].Start < out[j-1].Start; j-- {
			out[j], out[j-1] = out[j-1], out[j]
		}
	}
	return out
}

// buildList links the operations into a list with a sentinel head.
func buildList(ops []Op) *entry {
	head := &entry{idx: -1}
	at := head
	for i, op := range ops {
		e := &entry{op: op, idx: i, prev: at}
		at.next = e
		at = e
	}
	return head
}

// lift removes e from the list; drop puts it back.
func lift(e *entry) {
	e.prev.next = e.next
	if e.next != nil {
		e.next.prev = e.prev
	}
}

func drop(e *entry) {
	e.prev.next = e
	if e.next != nil {
		e.next.prev = e
	}
}

// search tries to linearize the remaining operations. An operation may be
// linearized next only when it starts before the earliest end among the
// remaining ones: any later operation must be concurrent with it or after it.
func search[S any](m Model[S], head *entry, st S, mask uint64, sr *searchState) bool {
	if head.next == nil {
		return true
	}
	if sr.steps >= sr.limit {
		return false
	}
	if len(sr.witness) > len(sr.best) {
		sr.best = append([]Op(nil), sr.witness...)
	}
	k := memoKey{mask: mask, key: m.Key(st)}
	if sr.seen[k] {
		return false
	}
	sr.seen[k] = true
	minEnd := int64(1<<63 - 1)
	for e := head.next; e != nil; e = e.next {
		if e.op.End < minEnd {
			minEnd = e.op.End
		}
	}
	for e := head.next; e != nil; e = e.next {
		if e.op.Start > minEnd {
			break
		}
		sr.steps++
		if sr.steps > sr.limit {
			return false
		}
		next, ok := m.Step(st, e.op)
		if !ok {
			continue
		}
		lift(e)
		sr.witness = append(sr.witness, e.op)
		if search(m, head, next, mask|1<<uint(e.idx), sr) {
			return true
		}
		sr.witness = sr.witness[:len(sr.witness)-1]
		drop(e)
	}
	return false
}

// FormatHistory renders a history for a failure message.
func FormatHistory(h []Op) string {
	var b strings.Builder
	for _, op := range h {
		b.WriteString("c")
		b.WriteString(strconv.Itoa(op.Client))
		b.WriteString(" [")
		b.WriteString(strconv.FormatInt(op.Start, 10))
		b.WriteString(",")
		b.WriteString(strconv.FormatInt(op.End, 10))
		b.WriteString("] ")
		b.WriteString(op.Kind)
		b.WriteString("(")
		b.WriteString(format(op.Arg))
		b.WriteString(") = ")
		b.WriteString(format(op.Ret))
		b.WriteString("\n")
	}
	return b.String()
}

// format renders an argument or return value without reflection-heavy
// formatting, so that the package stays dependency-free and cheap.
func format(v any) string {
	switch t := v.(type) {
	case nil:
		return "-"
	case string:
		return t
	case int:
		return strconv.Itoa(t)
	case int64:
		return strconv.FormatInt(t, 10)
	case uint64:
		return strconv.FormatUint(t, 10)
	case bool:
		return strconv.FormatBool(t)
	case []int:
		var b strings.Builder
		b.WriteByte('[')
		for i, v := range t {
			if i > 0 {
				b.WriteByte(' ')
			}
			b.WriteString(strconv.Itoa(v))
		}
		b.WriteByte(']')
		return b.String()
	case error:
		return t.Error()
	case interface{ String() string }:
		return t.String()
	default:
		return "?"
	}
}
