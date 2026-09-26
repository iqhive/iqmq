package engine

import (
	"context"
	"errors"
	"testing"

	"github.com/iqhive/iqmq/internal/errcode"
	"github.com/iqhive/iqmq/internal/model/fanout"
	"github.com/iqhive/iqmq/internal/queue"
)

// fanConfig is the shape of one fan-out differential run: the architecture
// that stores the records, the policy that admits them, how many
// subscriptions the topic starts with and how much room each of them has.
type fanConfig struct {
	arch     Arch
	policy   FanoutPolicy
	subs     int
	capacity int
	capBytes int64
}

// fanSub pairs an engine subscription with its consumer and the deliveries a
// script is holding for it.
type fanSub struct {
	name  string
	sub   *Subscription[int64]
	c     *Consumer[int64]
	held  []Claim[int64]
	alive bool
}

// fanHarness runs one op script against the engine and against
// internal/model/fanout at once.
type fanHarness struct {
	t    testing.TB
	eng  *Engine[int64]
	top  *Topic[int64]
	mod  *fanout.Model
	subs []*fanSub
	buf  []Claim[int64]
	poll context.Context
	next int
}

// maxFanSubs bounds the subscriptions a fan-out script may create.
const maxFanSubs = 6

// modelPolicy maps an engine policy onto the model's.
func modelPolicy(p FanoutPolicy) fanout.Policy {
	switch p {
	case Independent:
		return fanout.Independent
	case BestEffort:
		return fanout.BestEffort
	case DropSlow:
		return fanout.DropSlow
	default:
		return fanout.Atomic
	}
}

// newFanHarness builds an engine and a model with matching configuration. The
// ready queue is pinned to the ordered reference kind: what varies here is
// the architecture and the policy, and an unordered queue would only blur the
// comparison of which records reached which subscription.
func newFanHarness(t testing.TB, cfg fanConfig) *fanHarness {
	t.Helper()
	if cfg.capacity <= 0 {
		cfg.capacity = 2
	}
	eng, err := New(Config[int64]{
		Kinds: KindSet{Arch: cfg.arch, Queue: queue.MutexRing},
		Clone: func(v int64) int64 { return v },
		DefaultTopology: TopologyConfig{
			Kind:             TopologyTopic,
			Fanout:           cfg.policy,
			Partitions:       1,
			CapacityMessages: cfg.capacity,
			CapacityBytes:    cfg.capBytes,
			AckDeadline:      -1, // leases never expire: admission is the subject
		},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	top, err := eng.CreateTopic("fan", TopologyConfig{})
	if err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	poll, cancel := context.WithCancel(context.Background())
	cancel()
	h := &fanHarness{
		t: t, eng: eng, top: top, buf: make([]Claim[int64], 1), poll: poll,
		mod: fanout.New(fanout.Config{
			Policy:        modelPolicy(cfg.policy),
			Capacity:      cfg.capacity,
			CapacityBytes: cfg.capBytes,
			Shared:        cfg.arch.Shared(),
		}),
	}
	for range cfg.subs {
		h.subscribe()
	}
	return h
}

// close shuts the engine down.
func (h *fanHarness) close() { _ = h.eng.Close() }

// subscribe attaches one more subscription with one consumer.
func (h *fanHarness) subscribe() {
	h.t.Helper()
	if len(h.subs) >= maxFanSubs {
		return
	}
	name := "s" + string(rune('0'+h.next))
	h.next++
	sub, err := h.top.Subscribe(name, SubscriptionConfig{})
	if err != nil {
		h.t.Fatalf("subscribe %s: %v", name, err)
	}
	c, err := sub.Attach(ConsumerConfig{})
	if err != nil {
		h.t.Fatalf("attach %s: %v", name, err)
	}
	h.subs = append(h.subs, &fanSub{name: name, sub: sub, c: c, alive: true})
	h.mod.Step(fanout.Op{Kind: fanout.Subscribe})
}

// publish admits one record of the given size and returns what the publisher
// was told, having checked that the model says the same.
func (h *fanHarness) publish(size int) error {
	h.t.Helper()
	err := h.top.Publish(context.Background(), Publish[int64]{
		Payload: int64(h.mod.Steps()), Size: size, Partition: 0,
	})
	res := h.mod.Step(fanout.Op{Kind: fanout.Publish, Size: int64(size)})
	if !errors.Is(err, res.Err) && (err != nil || res.Err != nil) {
		h.t.Fatalf("publish(size=%d): engine %v, model %v", size, err, res.Err)
	}
	return err
}

// claim takes one record for a subscription's consumer.
func (h *fanHarness) claim(i int) {
	h.t.Helper()
	if i >= len(h.subs) || !h.subs[i].alive {
		return
	}
	fs := h.subs[i]
	n, err := fs.c.Fetch(h.poll, h.buf[:1])
	res := h.mod.Step(fanout.Op{Kind: fanout.Claim, Sub: i})
	if err != nil && !errors.Is(err, context.Canceled) {
		h.t.Fatalf("claim %s: %v", fs.name, err)
	}
	if (n == 1) != (res.Seq != 0) {
		h.t.Fatalf("claim %s: engine claimed %d, model %d", fs.name, n, res.Seq)
	}
	if n == 0 {
		return
	}
	if got := h.buf[0].Info.Seq; got != res.Seq {
		h.t.Fatalf("claim %s: engine record %d, model record %d", fs.name, got, res.Seq)
	}
	fs.held = append(fs.held, h.buf[0])
}

// settle acknowledges the oldest delivery a subscription is holding.
func (h *fanHarness) settle(i int) {
	h.t.Helper()
	if i >= len(h.subs) || !h.subs[i].alive || len(h.subs[i].held) == 0 {
		return
	}
	fs := h.subs[i]
	cl := fs.held[0]
	fs.held = fs.held[1:]
	err := fs.c.Ack(cl.Env, cl.Gen)
	res := h.mod.Step(fanout.Op{Kind: fanout.Settle, Sub: i, Seq: cl.Info.Seq})
	if err != nil {
		h.t.Fatalf("ack %s record %d: %v (model %v)", fs.name, cl.Info.Seq, err, res.Err)
	}
	if res.Err != nil {
		h.t.Fatalf("ack %s record %d: engine accepted it, model says %v", fs.name, cl.Info.Seq, res.Err)
	}
}

// closeSub detaches one subscription.
func (h *fanHarness) closeSub(i int) {
	h.t.Helper()
	if i >= len(h.subs) || !h.subs[i].alive {
		return
	}
	fs := h.subs[i]
	if err := fs.sub.Close(); err != nil {
		h.t.Fatalf("close %s: %v", fs.name, err)
	}
	fs.alive = false
	fs.held = nil
	h.mod.Step(fanout.Op{Kind: fanout.Close, Sub: i})
}

// check compares every observable of the engine with the model and asserts
// the model's own invariants.
func (h *fanHarness) check() {
	h.t.Helper()
	if err := h.mod.Invariants(); err != nil {
		h.t.Fatalf("model invariant: %v", err)
	}
	if h.mod.Subs() != len(h.subs) {
		h.t.Fatalf("model has %d subscriptions, harness %d", h.mod.Subs(), len(h.subs))
	}
	for i, fs := range h.subs {
		snap := fs.sub.Inspect()
		published, dropped, rejected, settled := h.mod.Counters(i)
		if snap.Published != published || snap.Dropped != dropped ||
			snap.Rejected != rejected || snap.Acked != settled {
			h.t.Fatalf("%s: engine published=%d dropped=%d rejected=%d acked=%d, model %d/%d/%d/%d",
				fs.name, snap.Published, snap.Dropped, snap.Rejected, snap.Acked,
				published, dropped, rejected, settled)
		}
		if got, want := snap.Ready+snap.InFlight, h.mod.Depth(i); got != want {
			h.t.Fatalf("%s: engine holds %d records (ready=%d inflight=%d), model %d",
				fs.name, got, snap.Ready, snap.InFlight, want)
		}
		if !snap.Conserved() {
			h.t.Fatalf("%s is not conserved: %+v", fs.name, snap)
		}
	}
}

// refused reports whether err is the admission refusal both sides use: no
// subscription took the record. A partial admission is errcode.ErrPartial and
// is deliberately not this.
func refused(err error) bool { return errors.Is(err, errcode.ErrQueueFull) }
