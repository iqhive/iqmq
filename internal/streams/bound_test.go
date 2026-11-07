package streams_test

import (
	"context"
	"slices"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/internal/streams"
	"github.com/iqhive/iqmq/internal/streams/fakestreams"
)

// readBlocks is the fake keeping the BLOCK argument of every XREADGROUP it is
// sent, "" for a read that polls, with the error the read ended with.
type readBlocks struct {
	*fakestreams.Server

	mu     sync.Mutex
	blocks []string
	errs   []error
}

func (s *readBlocks) Do(ctx context.Context, c streams.Cmd) (streams.Value, error) {
	v, err := s.Server.Do(ctx, c)
	if c.Name() == "XREADGROUP" {
		block := ""
		if i := slices.Index(c.Tokens, "BLOCK"); i >= 0 {
			block = c.Tokens[i+1]
		}
		s.mu.Lock()
		s.blocks = append(s.blocks, block)
		s.errs = append(s.errs, err)
		s.mu.Unlock()
	}
	return v, err
}

// TestReadEndsItsBlockBeforeItsContext is a read under a context whose
// deadline is nearer than Block. It used to send BLOCK Block whatever the
// deadline, so the client gave up on a read the server still held: both
// clients close the connection such a read waited on, and an entry the server
// handed it at the last moment was pending to this consumer and never
// delivered, until the sweep claimed it a deadline later as attempt 2.
func TestReadEndsItsBlockBeforeItsContext(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r := newRig(t, "source-a", 1, func(c *streams.ConsumerConfig) { c.Block = 5 * time.Second })
		log := &readBlocks{Server: r.srv}
		for _, tc := range []struct {
			name  string
			left  time.Duration
			block string
		}{
			{"no deadline", 0, "5000"},
			{"a deadline past the block", time.Minute, "5000"},
			{"a deadline inside the block", time.Second, "850"},
			{"a short deadline", 200 * time.Millisecond, "50"},
			{"a deadline 2ms past the margin", 152 * time.Millisecond, "2"},
			{"a deadline within the margin", 100 * time.Millisecond, ""},
		} {
			ctx, cancel := r.ctx, context.CancelFunc(func() {})
			if tc.left > 0 {
				ctx, cancel = context.WithTimeout(r.ctx, tc.left)
			}
			_, _, err := r.cons.Read(ctx, log, nil, nil)
			cancel()
			log.mu.Lock()
			block, rerr := log.blocks[len(log.blocks)-1], log.errs[len(log.errs)-1]
			log.mu.Unlock()
			if block != tc.block {
				t.Errorf("%s: the read sent BLOCK %q, want %q", tc.name, block, tc.block)
			}
			if err != nil || rerr != nil {
				t.Errorf("%s: the read ended with %v (the server's answer: %v), want the server's empty answer", tc.name, err, rerr)
			}
		}
	})
}

// TestSweepAtMostClaimsNoMoreThanItIsAsked is the bound a caller with room
// for fewer entries than ClaimCount puts on a round: n per shard, and none
// at all for n of zero. A plain Sweep still claims up to ClaimCount.
func TestSweepAtMostClaimsNoMoreThanItIsAsked(t *testing.T) {
	crashed := newRig(t, "crashed", 1)
	for range 5 {
		crashed.publish(t, "x")
	}
	if got := crashed.read(t); len(got) != 5 {
		t.Fatalf("the consumer that crashes read %d entries, want 5", len(got))
	}
	cons, err := streams.NewConsumer(streams.ConsumerConfig{
		Keys: crashed.keys, Group: testGroup, Name: "live", Prefetch: 16, AckDeadline: testLease,
	})
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	crashed.srv.Advance(testLease + time.Second)
	owned := func() int {
		n := 0
		for _, p := range crashed.srv.Pending(crashed.keys.Stream(0), testGroup) {
			if p.Consumer == "live" {
				n++
			}
		}
		return n
	}
	for _, step := range []struct {
		n, want int
	}{{0, 0}, {2, 2}, {-1, 0}} {
		got, _, err := cons.SweepAtMost(crashed.ctx, crashed.srv, step.n, nil, nil)
		if err != nil {
			t.Fatalf("SweepAtMost(%d): %v", step.n, err)
		}
		if len(got) != step.want {
			t.Errorf("SweepAtMost(%d) claimed %d entries, want %d", step.n, len(got), step.want)
		}
	}
	if n := owned(); n != 2 {
		t.Errorf("the consumer owns %d entries after the bounded rounds, want 2", n)
	}
	got, _, err := cons.Sweep(crashed.ctx, crashed.srv, nil, nil)
	if err != nil || len(got) != 3 {
		t.Errorf("Sweep claimed %d entries, %v, want the other 3", len(got), err)
	}
}

// consumerOn is another consumer of r's group, as another source of the
// subscription would be.
func consumerOn(t *testing.T, r *rig, name string, prefetch int) *streams.Consumer {
	t.Helper()
	cons, err := streams.NewConsumer(streams.ConsumerConfig{
		Keys: r.keys, Group: testGroup, Name: name, Prefetch: prefetch, AckDeadline: testLease,
	})
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	return cons
}

// readAll has cons read until it holds n entries.
func readAll(t *testing.T, r *rig, cons *streams.Consumer, n int) []streams.Delivery {
	t.Helper()
	var held []streams.Delivery
	for len(held) < n {
		ds, _, err := cons.Read(r.ctx, r.srv, nil, nil)
		if err != nil || len(ds) == 0 {
			t.Fatalf("Read after %d of %d: %d, %v", len(held), n, len(ds), err)
		}
		held = append(held, ds...)
	}
	return held
}

// keepFresh extends every lease of held, as a handler with lease extension
// does while it works.
func keepFresh(t *testing.T, r *rig, cons *streams.Consumer, held []streams.Delivery) {
	t.Helper()
	for i, err := range cons.Extend(r.ctx, r.srv, held) {
		if err != nil {
			t.Fatalf("Extend of delivery %d: %v", i, err)
		}
	}
}

// TestASmallSweepFindsAnEntryPastItsScanWindow is an entry whose consumer
// died, behind more pending rows than one XAUTOCLAIM looks at, which another
// consumer keeps fresh. Redis looks at no more than ten rows for each entry
// COUNT lets a call claim, and a round's COUNT is the room its caller has, so
// a round with room for one looked at ten rows and the next round at the next
// ten: the entry came back only after rows/10 rounds, 400 rows taking 40
// sweep intervals, where the worst case is AckDeadline and one interval.
func TestASmallSweepFindsAnEntryPastItsScanWindow(t *testing.T) {
	const fresh = 400
	r := newRig(t, "live", 1, func(c *streams.ConsumerConfig) { c.Prefetch = fresh })
	for range fresh {
		r.publish(t, "fresh")
	}
	live := readAll(t, r, r.cons, fresh)
	lost := r.publish(t, "lost")
	readAll(t, r, consumerOn(t, r, "crashed", 1), 1)
	worker := consumerOn(t, r, "worker", 1)
	r.srv.Advance(testLease + time.Second)
	keepFresh(t, r, r.cons, live)
	got, _, err := worker.SweepAtMost(r.ctx, r.srv, 1, nil, nil)
	if err != nil {
		t.Fatalf("SweepAtMost: %v", err)
	}
	if len(got) != 1 || got[0].Key.ID != lost {
		t.Fatalf("a round with room for one claimed %d entries (want the expired one, %v): the entry of a "+
			"consumer that died, behind %d fresh rows, is not found by a round that looks at ten rows",
			len(got), lost, fresh)
	}
	if got[0].Attempt != 2 || !got[0].Redelivered {
		t.Errorf("the redelivery reports attempt %d (redelivered %v), want 2", got[0].Attempt, got[0].Redelivered)
	}
	if more, _, err := worker.SweepAtMost(r.ctx, r.srv, 1, nil, nil); err != nil || len(more) != 0 {
		t.Errorf("the next round claimed %d, %v, want nothing: every other row is fresh", len(more), err)
	}
}

// publishTo adds one entry to the given shard and returns its identifier.
func publishTo(t *testing.T, r *rig, shard int, body string) streams.ID {
	t.Helper()
	v, err := r.srv.Do(r.ctx, streams.XAdd(streams.XAddArgs{
		Key:    r.keys.Stream(shard),
		ID:     streams.IDAuto,
		Fields: streams.Encode(nil, &mq.Message{Topic: testTopic, Body: []byte(body)}),
	}))
	if err != nil {
		t.Fatalf("XADD: %v", err)
	}
	id, err := streams.ParseXAdd(v)
	if err != nil {
		t.Fatalf("ParseXAdd: %v", err)
	}
	return id
}

// TestASmallSweepFindsAnEntryPastItsScanWindowOnEveryShard is the same on the
// second shard of a round whose first shard used its room within its scan:
// the room left is each shard's own.
func TestASmallSweepFindsAnEntryPastItsScanWindowOnEveryShard(t *testing.T) {
	const fresh = 400
	r := newRig(t, "live", 2, func(c *streams.ConsumerConfig) { c.Prefetch = fresh })
	crashed := consumerOn(t, r, "crashed", 1)
	first := publishTo(t, r, 0, "lost on 0")
	readAll(t, r, crashed, 1)
	for range fresh {
		publishTo(t, r, 1, "fresh")
	}
	live := readAll(t, r, r.cons, fresh)
	second := publishTo(t, r, 1, "lost on 1")
	readAll(t, r, crashed, 1)
	worker := consumerOn(t, r, "worker", 1)
	r.srv.Advance(testLease + time.Second)
	keepFresh(t, r, r.cons, live)
	got, _, err := worker.SweepAtMost(r.ctx, r.srv, 1, nil, nil)
	if err != nil {
		t.Fatalf("SweepAtMost: %v", err)
	}
	ids := make([]streams.ID, 0, len(got))
	for _, d := range got {
		ids = append(ids, d.Key.ID)
	}
	if len(ids) != 2 || !slices.Contains(ids, first) || !slices.Contains(ids, second) {
		t.Fatalf("a round with room for one per shard claimed %v, want %v on shard 0 and %v on shard 1, "+
			"behind %d fresh rows", ids, first, second, fresh)
	}
}

// beforeClaim is the fake running a hook just before the first pipeline that
// starts with an XCLAIM, which is a sweep's second round trip.
type beforeClaim struct {
	*fakestreams.Server
	hook func()
}

func (b *beforeClaim) DoMulti(ctx context.Context, cmds []streams.Cmd) ([]streams.Result, error) {
	if len(cmds) > 0 && cmds[0].Name() == "XCLAIM" && b.hook != nil {
		hook := b.hook
		b.hook = nil
		hook()
	}
	return b.Server.DoMulti(ctx, cmds)
}

// TestASmallSweepLeavesAnEntryItsHolderExtendedMeanwhile is an entry past the
// scan window whose holder is slow rather than dead: it extends its lease
// after the round's XPENDING listed the entry and before the round claims it.
// The claim keeps the round's idle bound, so the entry stays with its holder,
// whose settlement would otherwise be refused while a second handler ran it.
func TestASmallSweepLeavesAnEntryItsHolderExtendedMeanwhile(t *testing.T) {
	const fresh = 400
	r := newRig(t, "live", 1, func(c *streams.ConsumerConfig) { c.Prefetch = fresh })
	for range fresh {
		r.publish(t, "fresh")
	}
	live := readAll(t, r, r.cons, fresh)
	r.publish(t, "slow")
	slow := consumerOn(t, r, "slow", 1)
	held := readAll(t, r, slow, 1)
	worker := consumerOn(t, r, "worker", 1)
	r.srv.Advance(testLease + time.Second)
	keepFresh(t, r, r.cons, live)
	cmder := &beforeClaim{Server: r.srv, hook: func() { keepFresh(t, r, slow, held) }}
	got, _, err := worker.SweepAtMost(r.ctx, cmder, 1, nil, nil)
	if err != nil {
		t.Fatalf("SweepAtMost: %v", err)
	}
	if cmder.hook != nil {
		t.Fatal("the round sent no XCLAIM, so the test proves nothing")
	}
	if len(got) != 0 {
		t.Fatalf("the round claimed %d entries, want none: the slow holder extended its lease before the claim", len(got))
	}
	if errs := slow.Ack(r.ctx, r.srv, held); errs[0] != nil {
		t.Errorf("the slow holder's acknowledgement: %v", errs[0])
	}
}

// slowPipeline is the fake answering a pipeline one command at a time, with
// its clock moved on by step after each XPENDING, as a server's clock moves
// between two commands of one pipeline.
type slowPipeline struct {
	*fakestreams.Server
	step time.Duration
}

func (p *slowPipeline) DoMulti(ctx context.Context, cmds []streams.Cmd) ([]streams.Result, error) {
	out := make([]streams.Result, len(cmds))
	for i, c := range cmds {
		v, err := p.Do(ctx, c)
		out[i] = streams.Result{Value: v, Err: err}
		if c.Name() == "XPENDING" {
			p.Advance(p.step)
		}
	}
	return out, nil
}

// pastWindowRig is a one-shard topic whose pending list holds first, then 30
// entries another consumer keeps fresh, then an entry whose consumer died,
// expired: past the scan of a round with room for one. first is pending to
// holder, delivered held ago.
func pastWindowRig(t *testing.T, holder string, held time.Duration) (r *rig, first, expired streams.ID) {
	t.Helper()
	r = newRig(t, "live", 1)
	key := r.keys.Stream(0)
	first = r.publish(t, "first")
	fresh := make([]streams.ID, 0, 30)
	for range cap(fresh) {
		fresh = append(fresh, r.publish(t, "fresh"))
	}
	expired = r.publish(t, "expired")
	r.srv.Deliver(key, testGroup, "crashed", expired)
	r.srv.Advance(testLease + time.Second - held)
	r.srv.Deliver(key, testGroup, holder, first)
	r.srv.Advance(held)
	for _, id := range fresh {
		r.srv.Deliver(key, testGroup, "live", id)
	}
	return r, first, expired
}

// TestASmallSweepClaimsNoMoreThanItsRoomWhenAnEntryExpiresMidRound is an
// entry that goes idle between the round's XPENDING and its XAUTOCLAIM: the
// XAUTOCLAIM claims it although the XPENDING did not list it. A round that
// then claimed everything the XPENDING listed past the scan would claim more
// than its room.
func TestASmallSweepClaimsNoMoreThanItsRoomWhenAnEntryExpiresMidRound(t *testing.T) {
	r, first, _ := pastWindowRig(t, "crashed", testLease-time.Second)
	worker := consumerOn(t, r, "worker", 1)
	got, _, err := worker.SweepAtMost(r.ctx, &slowPipeline{Server: r.srv, step: 2 * time.Second}, 1, nil, nil)
	if err != nil {
		t.Fatalf("SweepAtMost: %v", err)
	}
	if len(got) != 1 || got[0].Key.ID != first {
		t.Fatalf("a round with room for one claimed %d entries, want only %v, which its XAUTOCLAIM took", len(got), first)
	}
}

// TestASmallSweepIsNotKeptFromAnEntryByADeletedRow is a round whose
// XAUTOCLAIM spends its COUNT dropping the pending row of an entry gone from
// the stream, as Redis 7.0 and later do however fresh the row is. That row is
// no delivery, so the round still has room for the expired entry past its
// scan, and takes it.
func TestASmallSweepIsNotKeptFromAnEntryByADeletedRow(t *testing.T) {
	r, first, expired := pastWindowRig(t, "live", 0)
	if _, err := r.srv.Do(r.ctx, streams.XDel(r.keys.Stream(0), []streams.ID{first})); err != nil {
		t.Fatalf("XDEL: %v", err)
	}
	worker := consumerOn(t, r, "worker", 1)
	got, _, err := worker.SweepAtMost(r.ctx, r.srv, 1, nil, nil)
	if err != nil {
		t.Fatalf("SweepAtMost: %v", err)
	}
	if len(got) != 1 || got[0].Key.ID != expired {
		t.Fatalf("a round whose XAUTOCLAIM dropped the row of deleted %v claimed %d entries, want %v",
			first, len(got), expired)
	}
}

// TestASmallSweepGoesBackForAnEntryBehindItsCursor is the same entry in front
// of the fresh rows, expiring after a round has already scanned past it. The
// next round resumed where the last one's scan stopped, and the cursor walked
// on ten rows a round, so the entry waited for the cursor to wrap. A round
// whose XPENDING has seen every expired row from its cursor to the end starts
// the next one from the beginning.
func TestASmallSweepGoesBackForAnEntryBehindItsCursor(t *testing.T) {
	const fresh = 400
	r := newRig(t, "live", 1, func(c *streams.ConsumerConfig) { c.Prefetch = fresh })
	lost := r.publish(t, "lost")
	readAll(t, r, consumerOn(t, r, "crashed", 1), 1)
	for range fresh {
		r.publish(t, "fresh")
	}
	live := readAll(t, r, r.cons, fresh)
	worker := consumerOn(t, r, "worker", 1)
	// A round before anything has expired scans past the entry.
	if got, _, err := worker.SweepAtMost(r.ctx, r.srv, 1, nil, nil); err != nil || len(got) != 0 {
		t.Fatalf("a round with nothing expired claimed %d, %v", len(got), err)
	}
	r.srv.Advance(testLease + time.Second)
	keepFresh(t, r, r.cons, live)
	got, _, err := worker.SweepAtMost(r.ctx, r.srv, 1, nil, nil)
	if err != nil {
		t.Fatalf("SweepAtMost: %v", err)
	}
	if len(got) != 1 || got[0].Key.ID != lost {
		t.Fatalf("the round after the entry expired claimed %d entries (want %v): it resumed past the entry, "+
			"and the cursor comes back to it only once it has walked the other %d rows", len(got), lost, fresh)
	}
}
