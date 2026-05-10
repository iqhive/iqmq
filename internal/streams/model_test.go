package streams_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	mq "bitbucket.org/iqhive/iqmq"
	"bitbucket.org/iqhive/iqmq/internal/streams"
	"bitbucket.org/iqhive/iqmq/internal/streams/fakestreams"
	"bitbucket.org/iqhive/iqmq/internal/testguard"
)

// TestModel holds every kind to the contract a negative
// settlement has, whatever strategy is bound (the message contract).
//
// The contract is deliberately not "they all behave the same": they do not,
// and the differences are what the bake-off measures. What every kind has to
// do is
//
//   - never report success for a settlement it did not perform;
//   - never make the message undeliverable;
//   - refuse a delay it cannot honour, rather than redelivering at some other
//     time and reporting success;
//   - carry the attempt count forward, so that MaxAttempts terminates;
//   - be a pure function of its inputs, so that the bake-off measures the
//     strategy and not a clock;
//   - agree with what it says about itself: a kind whose Delays is false must
//     refuse a delay, and one whose Reorders is false must not change the
//     entry's identifier.
func TestModel(t *testing.T) {
	for _, k := range streams.Kinds() {
		t.Run(k.String(), func(t *testing.T) {
			t.Parallel()
			t.Run("pure", func(t *testing.T) { modelPure(t, k) })
			t.Run("self-description", func(t *testing.T) { modelSelfDescription(t, k) })
			t.Run("requeue-redelivers", func(t *testing.T) { modelRequeue(t, k) })
			t.Run("delay-honoured-or-refused", func(t *testing.T) { modelDelay(t, k) })
			t.Run("attempt-carried", func(t *testing.T) { modelAttempt(t, k) })
			t.Run("drop-destroys", func(t *testing.T) { modelDrop(t, k) })
			t.Run("deadletter-needs-a-destination", func(t *testing.T) { modelDeadLetter(t, k) })
			t.Run("guard-refuses-a-stolen-entry", func(t *testing.T) { modelGuard(t, k) })
		})
	}
}

// modelStrategy builds a strategy of kind k over a one-shard topic.
func modelStrategy(t testing.TB, k streams.Kind, dlq string) streams.Strategy {
	t.Helper()
	keys, err := streams.NewKeys(testTopic, 1)
	if err != nil {
		t.Fatalf("NewKeys: %v", err)
	}
	s, err := streams.NewNack(k, streams.NackConfig{
		Keys:             keys,
		Group:            testGroup,
		Consumer:         "source-a",
		AckDeadline:      testLease,
		DeadLetterStream: dlq,
	})
	if err != nil {
		t.Fatalf("NewNack(%v): %v", k, err)
	}
	return s
}

// modelEntry is a plausible entry to nack.
func modelEntry() streams.Entry {
	m := &mq.Message{Topic: testTopic, Body: []byte("payload")}
	return streams.Entry{
		ID:     streams.ID{Ms: 1700000000000, Seq: 3},
		Shard:  0,
		Fields: streams.Encode(nil, m),
	}
}

func modelPure(t *testing.T, k streams.Kind) {
	s := modelStrategy(t, k, "")
	req := streams.NackRequest{
		Entry:   modelEntry(),
		Options: mq.NackOptions{},
		Now:     time.Unix(1700000000, 0),
		Attempt: 3,
	}
	first, err1 := s.Plan(req)
	second, err2 := s.Plan(req)
	if (err1 == nil) != (err2 == nil) {
		t.Fatalf("Plan is not pure: %v then %v", err1, err2)
	}
	if err1 != nil {
		return
	}
	if len(first.Cmds) != len(second.Cmds) {
		t.Fatalf("Plan produced %d commands then %d", len(first.Cmds), len(second.Cmds))
	}
	for i := range first.Cmds {
		if strings.Join(first.Cmds[i].Tokens, "\x00") != strings.Join(second.Cmds[i].Tokens, "\x00") {
			t.Fatalf("command %d differs between two identical plans", i)
		}
	}
	if first.Settled != second.Settled || first.HoldShard != second.HoldShard ||
		first.RedeliverAfter != second.RedeliverAfter {
		t.Fatalf("plan flags differ between two identical plans: %+v vs %+v", first, second)
	}
}

func modelSelfDescription(t *testing.T, k streams.Kind) {
	s := modelStrategy(t, k, "")
	if s.Kind() != k {
		t.Fatalf("Kind() = %v, want %v", s.Kind(), k)
	}
	if s.Kind() == streams.Auto {
		t.Fatal("a constructed strategy must report a concrete kind, never Auto")
	}
	// A kind that does not reorder must not change the entry's identifier,
	// which means its requeue plan must not add a new entry.
	plan, err := s.Plan(streams.NackRequest{Entry: modelEntry(), Now: time.Unix(1, 0), Attempt: 1})
	if err != nil {
		t.Fatalf("Plan of a plain requeue: %v", err)
	}
	adds := 0
	for _, c := range plan.Cmds {
		if c.Name() == "XADD" {
			adds++
		}
		// A re-adding strategy does it inside a script, so the script's own
		// identity is what says so.
		if c.Name() == "EVALSHA" && len(c.Tokens) > 1 {
			switch c.Tokens[1] {
			case streams.SHAOf(streams.ScriptReAdd), streams.SHAOf(streams.ScriptDelayPark):
				adds++
			}
		}
	}
	if !s.Reorders() && adds > 0 {
		t.Fatalf("%v reports Reorders()=false but its requeue plan adds a new entry, "+
			"which moves the message to the end of its shard", k)
	}
	if s.Reorders() && adds == 0 {
		t.Fatalf("%v reports Reorders()=true but its requeue plan adds nothing, "+
			"so the entry keeps its position and the claim is wrong", k)
	}
}

func modelRequeue(t *testing.T, k streams.Kind) {
	// End to end against the fake: a nacked message must come back.
	r := newRig(t, "source-a", 1, func(c *streams.ConsumerConfig) { c.Nack = k })
	r.publish(t, "payload")
	ds := r.read(t)
	if len(ds) != 1 {
		t.Fatalf("Read returned %d deliveries, want 1", len(ds))
	}
	if err := r.cons.Nack(r.ctx, r.srv, ds[0], mq.NackOptions{}, r.srv.Now()); err != nil {
		t.Fatalf("Nack: %v", err)
	}
	got := redeliver(t, r, k)
	if len(got) != 1 {
		t.Fatalf("%v: a nacked message was not redelivered (%d deliveries); a requeue that "+
			"loses the message is the one thing no strategy may do", k, len(got))
	}
	if got[0].Entry.Fields[1] != "payload" {
		t.Fatalf("%v: the redelivered entry's body is %q", k, got[0].Entry.Fields[1])
	}
}

// redeliver drives whatever each kind needs in order to redeliver: a sweep
// for the pending kinds, a mover pass for the zset, a plain read for the
// re-add.
func redeliver(t testing.TB, r *rig, k streams.Kind) []streams.Delivery {
	t.Helper()
	switch k.Resolve() {
	case streams.ZSetMover:
		movePark(t, r)
		ds, _, err := r.cons.Read(r.ctx, r.srv, nil, nil)
		if err != nil {
			t.Fatalf("Read: %v", err)
		}
		return ds
	case streams.ReAdd:
		ds, _, err := r.cons.Read(r.ctx, r.srv, nil, nil)
		if err != nil {
			t.Fatalf("Read: %v", err)
		}
		return ds
	case streams.LeavePending, streams.Stop:
		r.srv.Advance(testLease + time.Second)
		ds, _, err := r.cons.Sweep(r.ctx, r.srv, nil, nil)
		if err != nil {
			t.Fatalf("Sweep: %v", err)
		}
		return ds
	case streams.Auto:
		t.Fatal("Auto is not a concrete kind")
	}
	return nil
}

// movePark runs one pass of the delayed mover: read the due members and
// release each one.
//
// It is the loop the driver's source runs on a ticker, written out here so
// that the model test drives the same two commands in the same order.
func movePark(t testing.TB, r *rig) {
	t.Helper()
	zset := r.keys.Delayed(0)
	v, err := r.srv.Do(r.ctx, streams.ZRangeDue(zset, r.srv.Now().UnixMilli(), 32))
	if err != nil {
		t.Fatalf("ZRANGEBYSCORE: %v", err)
	}
	members, err := streams.ParseMembers(v)
	if err != nil {
		t.Fatalf("ParseMembers: %v", err)
	}
	for _, member := range members {
		d, uerr := streams.UnpackDelayed(member)
		if uerr != nil {
			t.Fatalf("UnpackDelayed: %v", uerr)
		}
		args := append([]string{member, "0"}, d.Fields...)
		if _, rerr := r.srv.Do(r.ctx, streams.EvalSHA(
			streams.SHAOf(streams.ScriptDelayRelease),
			[]string{zset, r.keys.Stream(d.Shard)}, args,
		)); rerr != nil {
			t.Fatalf("release: %v", rerr)
		}
	}
}

func modelDelay(t *testing.T, k streams.Kind) {
	s := modelStrategy(t, k, "")
	const long = time.Hour
	plan, err := s.Plan(streams.NackRequest{
		Entry:   modelEntry(),
		Options: mq.NackOptions{Delay: long},
		Now:     time.Unix(1700000000, 0),
		Attempt: 1,
	})
	if s.Delays() {
		if err != nil {
			t.Fatalf("%v reports Delays()=true and refused a one-hour delay: %v", k, err)
		}
		if plan.RedeliverAfter != long {
			t.Fatalf("%v arranged a redelivery after %v for a one-hour delay", k, plan.RedeliverAfter)
		}
		return
	}
	if err == nil {
		t.Fatalf("%v reports Delays()=false and accepted a one-hour delay, arranging a redelivery "+
			"after %v: the specification requires ErrUnsupported rather than a delay that is not honoured",
			k, plan.RedeliverAfter)
	}
	if !errors.Is(err, mq.ErrUnsupported) {
		t.Fatalf("%v refused a delay with %v, which is not ErrUnsupported", k, err)
	}
}

func modelAttempt(t *testing.T, k streams.Kind) {
	// MaxAttempts terminates only if the attempt count rises on every
	// redelivery. Two of the four kinds re-add the entry under a new
	// identifier, which starts the broker's own delivery counter again at
	// one — so for those the count has to travel in the entry, and a kind
	// that lost it would retry a message that always fails for ever.
	//
	// The check is five rounds rather than one, because a count that rises
	// once and then sticks is exactly as fatal as one that never rises, and
	// a single round cannot tell them apart.
	r := newRig(t, "source-a", 1, func(c *streams.ConsumerConfig) { c.Nack = k })
	r.publish(t, "payload")
	ds := r.read(t)
	if len(ds) != 1 {
		t.Fatalf("Read returned %d deliveries, want 1", len(ds))
	}
	d := ds[0]
	if d.Attempt != 1 {
		t.Fatalf("%v: the first delivery reports attempt %d, want 1", k, d.Attempt)
	}
	for round := 2; round <= 6; round++ {
		if err := r.cons.Nack(r.ctx, r.srv, d, mq.NackOptions{}, r.srv.Now()); err != nil {
			t.Fatalf("%v: Nack in round %d: %v", k, round, err)
		}
		got := redeliver(t, r, k)
		if len(got) != 1 {
			t.Fatalf("%v: round %d redelivered %d messages, want 1", k, round, len(got))
		}
		if got[0].Attempt < uint32(round) {
			t.Fatalf("%v: round %d's delivery reports attempt %d, want at least %d; "+
				"an attempt count that does not rise on every redelivery means MaxAttempts "+
				"never dead-letters a message that always fails",
				k, round, got[0].Attempt, round)
		}
		d = got[0]
	}
}

func modelDrop(t *testing.T, k streams.Kind) {
	r := newRig(t, "source-a", 1, func(c *streams.ConsumerConfig) { c.Nack = k })
	r.publish(t, "payload")
	ds := r.read(t)
	if err := r.cons.Nack(r.ctx, r.srv, ds[0],
		mq.NackOptions{Action: mq.NackActionDrop}, r.srv.Now()); err != nil {
		t.Fatalf("Nack(drop): %v", err)
	}
	if n := len(r.srv.Pending(r.keys.Stream(0), testGroup)); n != 0 {
		t.Fatalf("%v: a dropped message is still pending (%d entries), so it will be redelivered", k, n)
	}
	r.srv.Advance(testLease + time.Second)
	got, _, err := r.cons.Sweep(r.ctx, r.srv, nil, nil)
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("%v: a dropped message came back from the sweep", k)
	}
}

func modelDeadLetter(t *testing.T, k streams.Kind) {
	// Without a destination, the specification requires ErrUnsupported and no settlement.
	s := modelStrategy(t, k, "")
	if _, err := s.Plan(streams.NackRequest{
		Entry:   modelEntry(),
		Options: mq.NackOptions{Action: mq.NackActionDeadLetter},
		Now:     time.Unix(1, 0),
	}); !errors.Is(err, mq.ErrUnsupported) {
		t.Fatalf("%v: a dead-letter with no destination returned %v, want ErrUnsupported", k, err)
	}
	// With one that shares the topic's hash slot, it is the atomic Lua move.
	keys, err := streams.NewKeys(testTopic, 1)
	if err != nil {
		t.Fatalf("NewKeys: %v", err)
	}
	sameSlot := keys.Stream(0) + ":dlq"
	withDLQ := modelStrategy(t, k, sameSlot)
	plan, err := withDLQ.Plan(streams.NackRequest{
		Entry:   modelEntry(),
		Options: mq.NackOptions{Action: mq.NackActionDeadLetter},
		Now:     time.Unix(1, 0),
		Attempt: 2,
	})
	if err != nil {
		t.Fatalf("%v: a dead-letter to a co-located destination: %v", k, err)
	}
	if !plan.Settled {
		t.Fatalf("%v: a dead-letter plan that does not settle leaves the message pending as well as dead-lettered", k)
	}
	if len(plan.Cmds) != 1 || plan.Cmds[0].Name() != "EVALSHA" {
		t.Fatalf("%v: a co-located dead-letter is %d command(s) (%v), want one script: "+
			"a copy and an acknowledgement that are two commands duplicate the message on a crash between them",
			k, len(plan.Cmds), cmdNames(plan.Cmds))
	}
	// A destination on another slot cannot be a script at all, so the plan
	// is two commands and the duplicate window is the documented cost.
	crossSlot := modelStrategy(t, k, "{other.topic}")
	crossPlan, err := crossSlot.Plan(streams.NackRequest{
		Entry:   modelEntry(),
		Options: mq.NackOptions{Action: mq.NackActionDeadLetter},
		Now:     time.Unix(1, 0),
	})
	if err != nil {
		t.Fatalf("%v: a cross-slot dead-letter: %v", k, err)
	}
	if len(crossPlan.Cmds) != 2 {
		t.Fatalf("%v: a cross-slot dead-letter is %d command(s) (%v), want two: a cluster refuses "+
			"a script whose keys are on two slots", k, len(crossPlan.Cmds), cmdNames(crossPlan.Cmds))
	}
}

func cmdNames(cmds []streams.Cmd) []string {
	out := make([]string, 0, len(cmds))
	for _, c := range cmds {
		out = append(out, c.Name())
	}
	return out
}

func modelGuard(t *testing.T, k streams.Kind) {
	// Every strategy's write route is guarded: a nack of a delivery another
	// consumer now holds must not move, drop or dead-letter that consumer's
	// entry.
	r := newRig(t, "source-a", 1, func(c *streams.ConsumerConfig) { c.Nack = k })
	id := r.publish(t, "payload")
	ds := r.read(t)
	if !r.srv.Deliver(r.keys.Stream(0), testGroup, "source-b", id) {
		t.Fatal("hand-off failed")
	}
	err := r.cons.Nack(r.ctx, r.srv, ds[0], mq.NackOptions{Action: mq.NackActionDrop}, r.srv.Now())
	if !errors.Is(err, mq.ErrLeaseLost) {
		t.Fatalf("%v: dropping a delivery another consumer holds returned %v, want ErrLeaseLost", k, err)
	}
	pending := r.srv.Pending(r.keys.Stream(0), testGroup)
	if len(pending) != 1 || pending[0].Consumer != "source-b" {
		t.Fatalf("%v: the other consumer's entry was destroyed by our drop (pending=%v)", k, pending)
	}
}

// TestStopHoldsTheShard is the one behaviour only [streams.Stop] has, and the
// reason it is an arm of the family.
func TestStopHoldsTheShard(t *testing.T) {
	r := newRig(t, "source-a", 1, func(c *streams.ConsumerConfig) { c.Nack = streams.Stop })
	r.publish(t, "first")
	r.publish(t, "second")
	ds := r.read(t)
	if len(ds) != 2 {
		t.Fatalf("Read returned %d deliveries, want 2", len(ds))
	}
	// Nack the first. Under Stop the shard stops, so nothing published after
	// it is delivered until it is settled.
	if err := r.cons.Nack(r.ctx, r.srv, ds[0], mq.NackOptions{}, r.srv.Now()); err != nil {
		t.Fatalf("Nack: %v", err)
	}
	if r.cons.Held() != 1 {
		t.Fatalf("Held() = %d after a Stop nack, want 1", r.cons.Held())
	}
	r.publish(t, "third")
	more, _, err := r.cons.Read(r.ctx, r.srv, nil, nil)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if len(more) != 0 {
		t.Fatalf("the held shard delivered %d new entries; Stop's whole purpose is that nothing "+
			"behind the failed message moves", len(more))
	}
	// The sweep still redelivers the held entry itself — holding that back
	// too would stop the shard for ever.
	r.srv.Advance(testLease + time.Second)
	got, _, err := r.cons.Sweep(r.ctx, r.srv, nil, nil)
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if len(got) == 0 {
		t.Fatal("the held entry was not redelivered by the sweep, so the shard can never move again")
	}
	var held streams.Delivery
	for _, d := range got {
		if d.Key == ds[0].Key {
			held = d
		}
	}
	if held.Gen == 0 {
		t.Fatalf("the sweep did not redeliver the held entry %v", ds[0].Key)
	}
	if errs := r.cons.Ack(r.ctx, r.srv, []streams.Delivery{held}); errs[0] != nil {
		t.Fatalf("Ack of the held entry: %v", errs[0])
	}
	if r.cons.Held() != 0 {
		t.Fatalf("Held() = %d after the held entry was acknowledged, want 0", r.cons.Held())
	}
	after, _, err := r.cons.Read(r.ctx, r.srv, nil, nil)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if len(after) == 0 {
		t.Fatal("the shard did not resume after its held entry was settled")
	}
}

// TestZSetMoverHonoursTheDelay is the one behaviour only
// [streams.ZSetMover] has.
func TestZSetMoverHonoursTheDelay(t *testing.T) {
	r := newRig(t, "source-a", 1, func(c *streams.ConsumerConfig) { c.Nack = streams.ZSetMover })
	r.publish(t, "payload")
	ds := r.read(t)
	const delay = 10 * time.Minute
	if err := r.cons.Nack(r.ctx, r.srv, ds[0], mq.NackOptions{Delay: delay}, r.srv.Now()); err != nil {
		t.Fatalf("Nack: %v", err)
	}
	if n := r.srv.ZLen(r.keys.Delayed(0)); n != 1 {
		t.Fatalf("the delayed set holds %d members, want 1", n)
	}
	if n := len(r.srv.Pending(r.keys.Stream(0), testGroup)); n != 0 {
		t.Fatalf("the entry is still pending (%d) as well as parked: the park and the "+
			"acknowledgement were not one step", n)
	}
	// Before the delay is up, the mover releases nothing.
	r.srv.Advance(delay - time.Second)
	movePark(t, r)
	if n := r.srv.ZLen(r.keys.Delayed(0)); n != 1 {
		t.Fatalf("the mover released a member %v early", time.Second)
	}
	got, _, err := r.cons.Read(r.ctx, r.srv, nil, nil)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("the message was redelivered before its delay elapsed")
	}
	// After it, exactly once.
	r.srv.Advance(2 * time.Second)
	movePark(t, r)
	movePark(t, r) // a second pass must release nothing: ZREM decides
	if n := r.srv.ZLen(r.keys.Delayed(0)); n != 0 {
		t.Fatalf("the delayed set still holds %d members after the delay elapsed", n)
	}
	got, _, err = r.cons.Read(r.ctx, r.srv, nil, nil)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("the delayed message was redelivered %d times, want exactly 1", len(got))
	}
}

// TestZSetMemberIsUniquePerEntry pins the reason a parked member carries its
// source identifier: a sorted set is a set.
//
// Two identical messages nacked for the same instant have the same fields and
// the same score. Without the identifier in the member they would be one
// member, and the second nack would discard a message while reporting success.
func TestZSetMemberIsUniquePerEntry(t *testing.T) {
	r := newRig(t, "source-a", 1, func(c *streams.ConsumerConfig) { c.Nack = streams.ZSetMover })
	r.publish(t, "same")
	r.publish(t, "same")
	ds := r.read(t)
	if len(ds) != 2 {
		t.Fatalf("Read returned %d deliveries, want 2", len(ds))
	}
	now := r.srv.Now()
	for i := range ds {
		if err := r.cons.Nack(r.ctx, r.srv, ds[i], mq.NackOptions{Delay: time.Minute}, now); err != nil {
			t.Fatalf("Nack %d: %v", i, err)
		}
	}
	if n := r.srv.ZLen(r.keys.Delayed(0)); n != 2 {
		t.Fatalf("two identical messages parked for the same instant produced %d member(s), want 2: "+
			"a sorted set is a set, so a member that did not carry the entry's identifier would collide "+
			"and one of the messages would be silently discarded", n)
	}
	r.srv.Advance(2 * time.Minute)
	movePark(t, r)
	got, _, err := r.cons.Read(r.ctx, r.srv, nil, nil)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("%d of the 2 parked messages came back", len(got))
	}
}

// TestKindsList announces the family's kinds to benchmarks/cmd/bakeoff.
func TestKindsList(t *testing.T) {
	parts := make([]string, 0, len(streams.Kinds()))
	for _, k := range streams.Kinds() {
		parts = append(parts, k.String()+"="+k.Ident())
	}
	t.Logf("kinds: %s", strings.Join(parts, ","))
}

// TestKindProperties holds the family's identifiers to the shape the specification
// requires of every variant family.
func TestKindProperties(t *testing.T) {
	if got := len(streams.Kinds()); got != 4 {
		t.Fatalf("Kinds() has %d kinds, want 4: the arms are leave-pending, "+
			"the zset mover, re-add and stop", got)
	}
	seen := map[string]bool{}
	for _, k := range streams.Kinds() {
		if !k.Valid() || k == streams.Auto || k.Resolve() != k || k.Ident() == "" {
			t.Fatalf("%v: basic properties", k)
		}
		if seen[k.String()] {
			t.Fatalf("duplicate slug %q", k)
		}
		seen[k.String()] = true
		parsed, ok := streams.ParseKind(k.String())
		if !ok || parsed != k {
			t.Fatalf("ParseKind(%q) = %v, %v", k.String(), parsed, ok)
		}
	}
	if _, ok := streams.ParseKind("nosuchkind"); ok {
		t.Fatal("ParseKind accepted a slug the family does not have")
	}
	if streams.Auto.Resolve() != streams.AutoKind {
		t.Fatalf("Auto resolves to %v, not AutoKind %v", streams.Auto.Resolve(), streams.AutoKind)
	}
	if names := streams.KindNames(); len(names) != len(streams.Kinds()) {
		t.Fatalf("KindNames() has %d entries, Kinds() has %d", len(names), len(streams.Kinds()))
	}
}

// TestFakeXAckDoesNotCheckOwnership pins the one property of the fake that
// the reclaim tests depend on.
//
// Redis's XACK removes a pending entry whoever holds it. A fake that checked
// the owner would refuse the very acknowledgement the guard exists to refuse,
// and every reclaim test would pass against a driver with no guard at all.
func TestFakeXAckDoesNotCheckOwnership(t *testing.T) {
	r := newRig(t, "source-a", 1)
	id := r.publish(t, "payload")
	r.read(t)
	if !r.srv.Deliver(r.keys.Stream(0), testGroup, "source-b", id) {
		t.Fatal("hand-off failed")
	}
	v, err := r.srv.Do(r.ctx, streams.XAck(r.keys.Stream(0), testGroup, []streams.ID{id}))
	if err != nil {
		t.Fatalf("XACK: %v", err)
	}
	n, err := streams.ParseCount(v)
	if err != nil {
		t.Fatalf("ParseCount: %v", err)
	}
	if n != 1 {
		t.Fatalf("the fake's XACK refused to acknowledge another consumer's entry (n=%d); "+
			"a real server does not, so every test of the guard would pass vacuously", n)
	}
}

// TestFakeRefusesAnUnknownScript pins the other property the fake needs: a
// script it cannot execute is NOSCRIPT and not a silent success.
func TestFakeRefusesAnUnknownScript(t *testing.T) {
	srv := fakestreams.New(fakestreams.Options{})
	t.Cleanup(func() { _ = srv.Close() })
	_, err := srv.Do(rigContext(), streams.Eval("return 1", []string{"{k}"}, nil))
	if !errors.Is(err, fakestreams.ErrNoScript) {
		t.Fatalf("the fake accepted a script it cannot execute: %v", err)
	}
}

func rigContext() context.Context { return context.Background() }

// TestAutoBindingMatchesADR holds `auto.go`'s constant to whatever ADR-013
// records for this family, once one is rendered.
//
// ADR-013 collects the ten rows that need a broker (-, -, -) and `bakeoff adr` renders it from the stored per-family
// scorecards, so until a campaign has measured more than the specification there is no file
// and this test logs rather than fails. What it catches is the case that
// costs most: a campaign that bound a different kind and an `auto.go` that
// still names the old one, which would make the driver's default silently
// disagree with the record of the decision.
func TestAutoBindingMatchesADR(t *testing.T) {
	slug, bound, err := testguard.ADRAutoBinding("../..", "streamsnack")
	if err != nil {
		t.Fatal(err)
	}
	if !bound {
		t.Logf("no ADR binds streamsnack yet; AutoKind=%v (provisional=%v)",
			streams.AutoKind, streams.AutoProvisional)
		return
	}
	if slug != streams.AutoKind.String() {
		t.Fatalf("ADR binds streamsnack to %q but AutoKind is %q", slug, streams.AutoKind)
	}
}
