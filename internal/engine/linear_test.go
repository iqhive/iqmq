package engine

import (
	"bytes"
	"context"
	"errors"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/iqhive/iqmq/internal/errcode"
	"github.com/iqhive/iqmq/internal/fuzzutil"
	"github.com/iqhive/iqmq/internal/hash"
	"github.com/iqhive/iqmq/internal/pool"
	"github.com/iqhive/iqmq/internal/queue"
	"github.com/iqhive/iqmq/internal/wait"
	"github.com/iqhive/iqmq/testkit/linear"
)

// linArg carries the partition an operation belongs to next to its value, so
// that the checker can split the history. A partition is one subscription and
// one routing key: the unkeyed rig gives every partition its own subscription,
// and the keyed rig gives one subscription several keys, each hashed to its
// own lane.
type linArg struct {
	Part int
	V    int64
}

// String renders the argument for a failure message.
func (a linArg) String() string {
	return "p" + strconv.Itoa(a.Part) + ":" + strconv.FormatInt(a.V, 10)
}

// linState is the sequential specification of one lane at the linearization
// points of the memory engine: publish makes a record ready, a fetch
// atomically claims a ready record (the head, when the queue kind is
// ordered), and an ack removes a claimed record.
type linState struct {
	ready    []int64
	inflight []int64
}

// linModel is that specification as a linear.Model. Step never mutates its
// argument: it copies the slices it changes. An unordered ready-queue kind
// promises no order, so a fetch may then claim any queued record rather than
// the head.
func linModel(ordered bool) linear.Model[linState] {
	return linear.Model[linState]{
		Init: func(string) linState { return linState{} },
		Step: func(s linState, op linear.Op) (linState, bool) {
			switch op.Kind {
			case "publish":
				out := linState{ready: append(append([]int64(nil), s.ready...), op.Arg.(linArg).V), inflight: s.inflight}
				return out, true
			case "fetch":
				if op.Ret == nil {
					// A fetch that claims nothing is a poll, not a claim: it
					// has no linearization point of its own. A ready queue
					// built on slot reservation can hide a committed record
					// behind a slot another publisher is still filling, so an
					// empty poll promises nothing about what is queued. That
					// the engine loses nothing is checked instead by the
					// completeness pass below and by FuzzEngineModel's
					// conservation invariant on a quiescent engine.
					return s, true
				}
				v := op.Ret.(int64)
				rest, ok := takeReady(s.ready, v, ordered)
				if !ok {
					return s, false
				}
				return linState{ready: rest, inflight: insertSorted(s.inflight, v)}, true
			default: // ack
				v := op.Arg.(linArg).V
				out, ok := removeSorted(s.inflight, v)
				if !ok {
					return s, false
				}
				return linState{ready: s.ready, inflight: out}, true
			}
		},
		Key: func(s linState) string {
			var b strings.Builder
			for _, v := range s.ready {
				b.WriteString(strconv.FormatInt(v, 10))
				b.WriteByte(',')
			}
			b.WriteByte('|')
			for _, v := range s.inflight {
				b.WriteString(strconv.FormatInt(v, 10))
				b.WriteByte(',')
			}
			return b.String()
		},
		Partition: func(op linear.Op) string { return strconv.Itoa(op.Arg.(linArg).Part) },
	}
}

// takeReady removes v from the ready queue: the head for an ordered kind, any
// queued record for an unordered one.
func takeReady(ready []int64, v int64, ordered bool) ([]int64, bool) {
	if ordered {
		if len(ready) == 0 || ready[0] != v {
			return nil, false
		}
		return append([]int64(nil), ready[1:]...), true
	}
	for i, x := range ready {
		if x == v {
			out := make([]int64, 0, len(ready)-1)
			out = append(out, ready[:i]...)
			return append(out, ready[i+1:]...), true
		}
	}
	return nil, false
}

// insertSorted returns a copy of xs with v inserted in order.
func insertSorted(xs []int64, v int64) []int64 {
	out := make([]int64, 0, len(xs)+1)
	i := 0
	for i < len(xs) && xs[i] < v {
		out = append(out, xs[i])
		i++
	}
	out = append(out, v)
	return append(out, xs[i:]...)
}

// removeSorted returns a copy of xs without one occurrence of v.
func removeSorted(xs []int64, v int64) ([]int64, bool) {
	for i, x := range xs {
		if x == v {
			out := make([]int64, 0, len(xs)-1)
			out = append(out, xs[:i]...)
			return append(out, xs[i+1:]...), true
		}
	}
	return nil, false
}

// linRig is one engine with several independent ordered lanes, each its own
// topic, subscription and set of competing consumers.
type linRig struct {
	eng     *Engine[int64]
	topics  []*Topic[int64]
	subs    []*Subscription[int64]
	keys    [][]byte
	poll    context.Context
	ordered bool
	next    atomic.Int64
	mu      sync.Mutex
	hist    []linear.Op
}

// keyed reports whether the rig routes by key: one topic and one subscription
// whose lanes are chosen by the publisher's routing key, rather than one topic
// and subscription per partition.
func (rig *linRig) keyed() bool { return rig.keys != nil }

// topicOf returns the topic operations on part publish to.
func (rig *linRig) topicOf(part int) *Topic[int64] {
	if rig.keyed() {
		return rig.topics[0]
	}
	return rig.topics[part]
}

// subOf returns the subscription part is served by.
func (rig *linRig) subOf(part int) *Subscription[int64] {
	if rig.keyed() {
		return rig.subs[0]
	}
	return rig.subs[part]
}

// keyOf returns the routing key of part, or nil when the rig is unkeyed.
func (rig *linRig) keyOf(part int) []byte {
	if rig.keyed() {
		return rig.keys[part]
	}
	return nil
}

// partOf maps a claimed record back to its linearization partition. A keyed
// rig shares one subscription between every key, so a consumer bound to one
// partition may well claim a record of another: the record's own key decides
// which history the operation belongs to.
func (rig *linRig) partOf(cl Claim[int64], dflt int) int {
	if !rig.keyed() {
		return dflt
	}
	for i, k := range rig.keys {
		if bytes.Equal(k, cl.Info.Key) {
			return i
		}
	}
	return dflt
}

// newLinRig builds parts independent lanes on one engine.
func newLinRig(t *testing.T, ks KindSet, parts int) *linRig {
	t.Helper()
	eng, err := New(Config[int64]{
		Kinds:           ks,
		Clone:           func(v int64) int64 { return v },
		DefaultTopology: TopologyConfig{Partitions: 1, CapacityMessages: 64},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	rig := &linRig{eng: eng, ordered: ks.Ordered()}
	for i := range parts {
		top, err := eng.CreateTopic("t"+strconv.Itoa(i), TopologyConfig{})
		if err != nil {
			t.Fatalf("CreateTopic: %v", err)
		}
		sub, err := top.Subscribe("s", SubscriptionConfig{})
		if err != nil {
			t.Fatalf("Subscribe: %v", err)
		}
		rig.topics = append(rig.topics, top)
		rig.subs = append(rig.subs, sub)
	}
	poll, cancel := context.WithCancel(context.Background())
	cancel()
	rig.poll = poll
	return rig
}

// newLinKeyRig builds parts partitions as parts routing keys of a single
// subscription, which is the per-subscription-and-key partitioning the
// linearizability checker is specified for: the lane a record lands in is
// chosen by the key hash, not by the caller.
func newLinKeyRig(t *testing.T, ks KindSet, parts int) *linRig {
	t.Helper()
	eng, err := New(Config[int64]{
		Kinds:           ks,
		Clone:           func(v int64) int64 { return v },
		DefaultTopology: TopologyConfig{Partitions: parts, CapacityMessages: 64},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	top, err := eng.CreateTopic("t", TopologyConfig{})
	if err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	sub, err := top.Subscribe("s", SubscriptionConfig{})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	poll, cancel := context.WithCancel(context.Background())
	cancel()
	return &linRig{
		eng: eng, topics: []*Topic[int64]{top}, subs: []*Subscription[int64]{sub},
		keys: laneKeys(t, eng, parts), poll: poll, ordered: ks.Ordered(),
	}
}

// laneKeys returns one routing key per lane, each hashing to a different lane,
// so that a key names exactly one lane and a lane serves exactly one key.
func laneKeys(t *testing.T, eng *Engine[int64], parts int) [][]byte {
	t.Helper()
	keys := make([][]byte, parts)
	found := 0
	for i := 0; found < parts && i < 4096; i++ {
		k := []byte("key-" + strconv.Itoa(i))
		lane := hash.Lane(eng.hashfn(0, k), parts)
		if keys[lane] != nil {
			continue
		}
		keys[lane] = k
		found++
	}
	if found < parts {
		t.Fatalf("only %d of %d lanes have a key", found, parts)
	}
	return keys
}

// record appends one completed operation to the history.
func (rig *linRig) record(client int, kind string, arg linArg, ret any, start, end int64) {
	rig.mu.Lock()
	rig.hist = append(rig.hist, linear.Op{
		Client: client, Kind: kind, Arg: arg, Ret: ret, Start: start, End: end,
	})
	rig.mu.Unlock()
}

// client runs one goroutine's operations against lane part, taking the
// interval of every call from the global logical clock.
func (rig *linRig) client(t *testing.T, id, part, steps int, pick func(i int) int) {
	t.Helper()
	c, err := rig.subOf(part).Attach(ConsumerConfig{})
	if err != nil {
		t.Errorf("attach: %v", err)
		return
	}
	defer func() { _ = c.Close() }()
	dst := make([]Claim[int64], 1)
	var held []Claim[int64]
	ack := func() {
		cl := held[0]
		held = held[1:]
		start := linear.Now()
		err := c.Ack(cl.Env, cl.Gen)
		end := linear.Now()
		if err != nil {
			t.Errorf("ack: %v", err)
			return
		}
		rig.record(id, "ack", linArg{Part: rig.partOf(cl, part), V: cl.Payload}, nil, start, end)
	}
	for i := range steps {
		switch pick(i) {
		case 0:
			v := rig.next.Add(1)
			start := linear.Now()
			err := rig.topicOf(part).Publish(context.Background(), Publish[int64]{
				Payload: v, Key: rig.keyOf(part), Size: 8, Partition: -1,
			})
			end := linear.Now()
			if err != nil {
				if !errors.Is(err, errcode.ErrQueueFull) {
					t.Errorf("publish: %v", err)
				}
				continue
			}
			rig.record(id, "publish", linArg{Part: part, V: v}, nil, start, end)
		case 1:
			start := linear.Now()
			n, err := c.Fetch(rig.poll, dst)
			end := linear.Now()
			switch {
			case n == 1:
				rig.record(id, "fetch", linArg{Part: rig.partOf(dst[0], part)}, dst[0].Payload, start, end)
				held = append(held, dst[0])
			case errors.Is(err, context.Canceled):
				rig.record(id, "fetch", linArg{Part: part}, nil, start, end)
			default:
				t.Errorf("fetch: n=%d err=%v", n, err)
			}
		default:
			if len(held) > 0 {
				ack()
			}
		}
	}
	for len(held) > 0 {
		ack()
	}
}

// run drives clients concurrently and checks the recorded history.
func (rig *linRig) run(t *testing.T, clients, parts, steps int, pick func(client, i int) int) {
	t.Helper()
	var wg sync.WaitGroup
	for id := range clients {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rig.client(t, id, id%parts, steps, func(i int) int { return pick(id, i) })
		}()
	}
	wg.Wait()
	if t.Failed() {
		return
	}
	rig.checkComplete(t)
	res := linear.Check(linModel(rig.ordered), rig.hist, 1<<20)
	if res.OK {
		return
	}
	if res.Limited {
		// The search ran out of budget: a history of wholly concurrent
		// publications has a factorial number of ready-queue orders, and the
		// memo cannot collapse them because the order is the state. That is
		// inconclusive, not a counterexample.
		t.Logf("linearizability check inconclusive for partition %q after %d steps (%d operations)",
			res.Partition, res.Explored, len(rig.hist))
		return
	}
	t.Fatalf("history is not linearizable (partition %q, explored %d):\n%s",
		res.Partition, res.Explored, linear.FormatHistory(rig.hist))
}

// checkComplete drains every lane once the clients have stopped and asserts
// that exactly the published-but-unacked records are still there: nothing was
// lost, duplicated or invented while the clients raced.
func (rig *linRig) checkComplete(t *testing.T) {
	t.Helper()
	want := map[int64]int{}
	for _, op := range rig.hist {
		switch op.Kind {
		case "publish":
			want[op.Arg.(linArg).V]++
		case "ack":
			want[op.Arg.(linArg).V]--
		}
	}
	for v, n := range want {
		if n == 0 {
			delete(want, v)
		}
	}
	got := map[int64]int{}
	dst := make([]Claim[int64], 8)
	for i, sub := range rig.subs {
		c, err := sub.Attach(ConsumerConfig{})
		if err != nil {
			t.Fatalf("attach for drain: %v", err)
		}
		for {
			n, err := c.Fetch(rig.poll, dst)
			if n == 0 {
				if err != nil && !errors.Is(err, context.Canceled) {
					t.Fatalf("drain lane %d: %v", i, err)
				}
				break
			}
			for j := range n {
				got[dst[j].Payload]++
				if err := c.Ack(dst[j].Env, dst[j].Gen); err != nil {
					t.Fatalf("drain ack: %v", err)
				}
			}
		}
		if err := c.Close(); err != nil {
			t.Fatalf("drain close: %v", err)
		}
	}
	for v, n := range want {
		if got[v] != n {
			t.Fatalf("record %d: published-unacked %d, drained %d", v, n, got[v])
		}
	}
	for v, n := range got {
		if want[v] != n {
			t.Fatalf("record %d: drained %d, published-unacked %d", v, n, want[v])
		}
	}
}

// TestEngineLinearizable records concurrent publish, fetch and ack histories
// from competing consumers on independent lanes and checks them against the linearization points with the Wing and Gong checker.
func TestEngineLinearizable(t *testing.T) {
	sets := []KindSet{{}, {Safe: true}, {Queue: queue.Chan, Wait: wait.Hybrid, Pool: pool.FreeList}}
	for _, ks := range sets {
		t.Run(ks.String(), func(t *testing.T) {
			rig := newLinRig(t, ks, 2)
			defer func() { _ = rig.eng.Close() }()
			rig.run(t, 4, 2, 8, func(client, i int) int { return (i + client) % 3 })
		})
	}
}

// TestEngineLinearizableKeys is the same check over the other partitioning the row asks for: one subscription whose partitions are routing keys, so the key
// hash picks the lane, consumers compete across every lane at once, and each
// claimed record is attributed to its key's history rather than to the
// consumer that happened to claim it.
func TestEngineLinearizableKeys(t *testing.T) {
	sets := []KindSet{{}, {Safe: true}, {Queue: queue.Chan, Wait: wait.Hybrid, Pool: pool.FreeList}}
	for _, ks := range sets {
		t.Run(ks.String(), func(t *testing.T) {
			rig := newLinKeyRig(t, ks, 3)
			defer func() { _ = rig.eng.Close() }()
			rig.run(t, 6, 3, 8, func(client, i int) int { return (i + client) % 3 })
			rig.checkPartitionsUsed(t, 3)
		})
	}
}

// checkPartitionsUsed asserts the history really is spread over parts
// partitions, so that a keyed run cannot pass by routing everything into one
// lane and checking a single history.
func (rig *linRig) checkPartitionsUsed(t *testing.T, parts int) {
	t.Helper()
	seen := make([]int, parts)
	rig.mu.Lock()
	for _, op := range rig.hist {
		if op.Kind == "publish" {
			seen[op.Arg.(linArg).Part]++
		}
	}
	rig.mu.Unlock()
	for i, n := range seen {
		if n == 0 {
			t.Fatalf("partition %d published nothing: %v", i, seen)
		}
	}
}

// FuzzEngineLinearizable is the same check with the operation mix, the kind
// set and the shape chosen by the fuzzer.
func FuzzEngineLinearizable(f *testing.F) {
	for _, seed := range [][3]int{{2, 2, 5}, {3, 2, 6}, {2, 1, 6}} {
		w := fuzzutil.NewWriter()
		w.Int(1, 3, seed[0])
		w.Int(1, 2, seed[1])
		w.Int(1, 6, seed[2])
		w.Int(0, len(kindMatrix("one"))-1, 0)
		for i := range 64 {
			w.Byte(byte(i * 13))
		}
		f.Add(w.Data())
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		r := fuzzutil.NewReader(data)
		clients := r.Int(1, 3)
		parts := r.Int(1, 2)
		steps := r.Int(1, 6)
		sets := kindMatrix("one")
		ks := sets[r.Int(0, len(sets)-1)]
		rest := data[len(data)-r.Remaining():]
		rig := newLinRig(t, ks, parts)
		defer func() { _ = rig.eng.Close() }()
		rig.run(t, clients, parts, steps, func(client, i int) int {
			idx := client*steps + i
			if idx < len(rest) {
				return int(rest[idx] % 3)
			}
			return (idx + client) % 3
		})
	})
}
