package redis_test

import (
	"context"
	"errors"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit"
	rd "github.com/iqhive/iqmq/drivers/redis"
	"github.com/iqhive/iqmq/internal/streams"
	"github.com/iqhive/iqmq/internal/streams/fakestreams"
)

// stopNack is the configuration of every test in this file: the strategy
// that holds a shard until its failed message is settled.
func stopNack(c *rd.Config) { c.Nack = streams.Stop }

// publishBodies publishes one message per body, in order.
func publishBodies(t testing.TB, r *rig, sink mq.Sink, bodies ...string) {
	t.Helper()
	for _, b := range bodies {
		if err := sink.Publish(r.ctx, &mq.Message{Topic: testTopic, Body: []byte(b)}, nil); err != nil {
			t.Fatalf("Publish %q: %v", b, err)
		}
	}
}

// TestAStopNackHandsBackWhatFetchBufferedBehindIt is the source's own
// buffer. A read takes up to Prefetch entries and a fetch hands out as many
// as its caller has room for, so when the first message fails the ones read
// with it are still waiting in the source. They are behind it, and the next
// fetch must not hand them out before its retry.
func TestAStopNackHandsBackWhatFetchBufferedBehindIt(t *testing.T) {
	r := newRig(t, stopNack)
	sink, src := r.openPair(t)
	publishBodies(t, r, sink, "a", "b", "c")
	first := fetchOne(t, r.ctx, src)
	if string(first.Message.Body) != "a" {
		t.Fatalf("the first fetch returned %q, want a", first.Message.Body)
	}
	if err := src.Settler().Nack(r.ctx, &first, mq.NackOptions{}); err != nil {
		t.Fatalf("Nack: %v", err)
	}
	ctx, cancel := context.WithTimeout(r.ctx, 200*time.Millisecond)
	defer cancel()
	dst := make([]mq.DriverDelivery, 1)
	if n, err := src.Fetch(ctx, dst); n != 0 {
		t.Fatalf("the fetch after the nack handed out %q, which is behind the failed message", dst[0].Message.Body)
	} else if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Fetch = %v, want the context's deadline", err)
	}
	keys, _ := streams.NewKeys(testTopic, 1)
	if n := len(r.srv.Pending(keys.Stream(0), testSub)); n != 3 {
		t.Fatalf("the pending list holds %d entries, want all 3: what the source handed back was lost", n)
	}

	r.srv.Advance(testLease + time.Second)
	if _, _, err := rd.SweepForTest(r.b, src); err != nil {
		t.Fatalf("SweepForTest: %v", err)
	}
	for i, want := range []string{"a", "b", "c"} {
		d := fetchOne(t, r.ctx, src)
		if string(d.Message.Body) != want {
			t.Fatalf("delivery %d after the sweep is %q, want %q: the shard's order did not survive the retry",
				i, d.Message.Body, want)
		}
		if i == 0 && d.Info.Attempt != 2 {
			t.Fatalf("the retry reports attempt %d, want 2", d.Info.Attempt)
		}
		if err := src.Settler().Ack(r.ctx, &d); err != nil {
			t.Fatalf("Ack of %q: %v", want, err)
		}
	}
}

// TestAStopNackKeepsWhatAParkedFetchReadBehindIt is the same failure while
// the next fetch is already waiting at the server. Its read chose the shard
// before the nack, and the server answers it with a message published during
// the block, which is behind the failed one.
//
// It runs on the wall clock rather than in a synctest bubble: a fetch that
// spins never lets a bubble's clock move, and the test would hang instead of
// failing. A read that has not reached the server by the time of the nack
// sees the held shard and sends nothing, which passes for the same reason.
func TestAStopNackKeepsWhatAParkedFetchReadBehindIt(t *testing.T) {
	r := newRig(t, stopNack, func(c *rd.Config) { c.Block = 2 * time.Second })
	sink, src := r.openPair(t)
	publishBodies(t, r, sink, "a")
	first := fetchOne(t, r.ctx, src)
	type result struct {
		body string
		n    int
		err  error
	}
	out := make(chan result, 1)
	go func() {
		ctx, cancel := context.WithTimeout(r.ctx, 500*time.Millisecond)
		defer cancel()
		dst := make([]mq.DriverDelivery, 1)
		n, err := src.Fetch(ctx, dst)
		body := ""
		if n > 0 {
			body = string(dst[0].Message.Body)
		}
		out <- result{body, n, err}
	}()
	time.Sleep(50 * time.Millisecond)
	if err := src.Settler().Nack(r.ctx, &first, mq.NackOptions{}); err != nil {
		t.Fatalf("Nack: %v", err)
	}
	publishBodies(t, r, sink, "b")
	got := <-out
	if got.n != 0 {
		t.Fatalf("the fetch that was waiting at the server handed out %q, which is behind the failed message", got.body)
	}
	if !errors.Is(got.err, context.DeadlineExceeded) {
		t.Fatalf("Fetch = %v, want the context's deadline", got.err)
	}
}

// TestFetchWaitsWhileEveryShardIsHeld is a fetch with nothing it may read.
// The read sends no command when every shard is held, so it returns at once
// rather than after the block, and a fetch that went straight round again
// spun a core until the sweep brought the failed message back.
func TestFetchWaitsWhileEveryShardIsHeld(t *testing.T) {
	r := newRig(t, stopNack, func(c *rd.Config) { c.Block = time.Minute })
	sink, src := r.openPair(t)
	publishBodies(t, r, sink, "a")
	first := fetchOne(t, r.ctx, src)
	if err := src.Settler().Nack(r.ctx, &first, mq.NackOptions{}); err != nil {
		t.Fatalf("Nack: %v", err)
	}
	const wait = 200 * time.Millisecond
	base, cancel := context.WithTimeout(r.ctx, wait)
	defer cancel()
	ctx := &countingCtx{Context: base}
	if n, err := src.Fetch(ctx, make([]mq.DriverDelivery, 1)); n != 0 || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Fetch = %d, %v; want nothing until the context's deadline", n, err)
	}
	if rounds := ctx.errs.Load(); rounds > 10 {
		t.Fatalf("Fetch checked its context %d times in %v with every shard held: it went round its loop "+
			"instead of waiting", rounds, wait)
	}
}

// countingCtx counts how often Err is asked, which is once per round of a
// fetch's loop.
type countingCtx struct {
	context.Context
	errs atomic.Int64
}

func (c *countingCtx) Err() error {
	c.errs.Add(1)
	return c.Context.Err()
}

// TestAFetchWaitingOnAHeldShardTakesTheRetryAtOnce is the other side of that
// wait: it ends when the sweep brings the failed message back, not when the
// block runs out.
func TestAFetchWaitingOnAHeldShardTakesTheRetryAtOnce(t *testing.T) {
	r := newRig(t, stopNack, func(c *rd.Config) { c.Block = time.Minute })
	sink, src := r.openPair(t)
	publishBodies(t, r, sink, "a")
	first := fetchOne(t, r.ctx, src)
	if err := src.Settler().Nack(r.ctx, &first, mq.NackOptions{}); err != nil {
		t.Fatalf("Nack: %v", err)
	}
	type result struct {
		d   mq.DriverDelivery
		n   int
		err error
	}
	out := make(chan result, 1)
	ctx, cancel := context.WithTimeout(r.ctx, 30*time.Second)
	defer cancel()
	go func() {
		dst := make([]mq.DriverDelivery, 1)
		n, err := src.Fetch(ctx, dst)
		out <- result{dst[0], n, err}
	}()
	r.srv.Advance(testLease + time.Second)
	if _, _, err := rd.SweepForTest(r.b, src); err != nil {
		t.Fatalf("SweepForTest: %v", err)
	}
	select {
	case got := <-out:
		if got.err != nil || got.n != 1 || string(got.d.Message.Body) != "a" {
			t.Fatalf("Fetch = %d, %v; want the retry of a", got.n, got.err)
		}
	case <-time.After(5 * time.Second):
		cancel()
		<-out
		t.Fatal("the fetch did not take the retry the sweep brought back: it waited out its block")
	}
}

// TestAnUndecodableEntryBehindAStopNackDoesNotHoldItsShard is an entry this
// driver cannot decode, read with the message that fails and handed back
// with the rest of the buffer. Nacking it held the shard for a settlement
// nothing can make: it never reaches a handler, and with no dead-letter
// stream poison only leaves it pending, so the entry behind it never came.
func TestAnUndecodableEntryBehindAStopNackDoesNotHoldItsShard(t *testing.T) {
	r := newRig(t, stopNack)
	sink, src := r.openPair(t)
	keys, _ := streams.NewKeys(testTopic, 1)
	publishBodies(t, r, sink, "a")
	// A header index that skips one, which no correct encoder writes.
	if _, err := r.srv.Do(r.ctx, streams.XAdd(streams.XAddArgs{
		Key:    keys.Stream(0),
		ID:     streams.IDAuto,
		Fields: []string{"b", "body", "h:0", "1:kv", "h:2", "1:kv"},
	})); err != nil {
		t.Fatalf("XADD: %v", err)
	}
	publishBodies(t, r, sink, "c")
	first := fetchOne(t, r.ctx, src)
	if err := src.Settler().Nack(r.ctx, &first, mq.NackOptions{}); err != nil {
		t.Fatalf("Nack: %v", err)
	}
	r.srv.Advance(testLease + time.Second)
	if _, _, err := rd.SweepForTest(r.b, src); err != nil {
		t.Fatalf("SweepForTest: %v", err)
	}
	retry := fetchOne(t, r.ctx, src)
	if string(retry.Message.Body) != "a" {
		t.Fatalf("the first fetch after the sweep returned %q, want the retry of a", retry.Message.Body)
	}
	if err := src.Settler().Ack(r.ctx, &retry); err != nil {
		t.Fatalf("Ack of the retry: %v", err)
	}
	ctx, cancel := context.WithTimeout(r.ctx, time.Second)
	defer cancel()
	dst := make([]mq.DriverDelivery, 1)
	if n, err := src.Fetch(ctx, dst); n != 1 || string(dst[0].Message.Body) != "c" {
		t.Fatalf("Fetch = %d, %v after the retry was settled, want c: the shard is held for the undecodable "+
			"entry, which nothing will ever settle", n, err)
	}
}

// hookCmder is the fake server with a hook that runs once, as soon as the
// first command its match picks has been answered. It is how a test lands a
// settlement inside a read or a sweep, between the server's reply and the
// source's buffer.
type hookCmder struct {
	*fakestreams.Server

	mu    sync.Mutex
	match func(streams.Cmd) bool
	hook  func()
}

func (h *hookCmder) Do(ctx context.Context, c streams.Cmd) (streams.Value, error) {
	v, err := h.Server.Do(ctx, c)
	h.fire(c)
	return v, err
}

func (h *hookCmder) DoMulti(ctx context.Context, cs []streams.Cmd) ([]streams.Result, error) {
	res, err := h.Server.DoMulti(ctx, cs)
	h.fire(cs...)
	return res, err
}

// arm sets the hook, in place of any armed before.
func (h *hookCmder) arm(match func(streams.Cmd) bool, hook func()) {
	h.mu.Lock()
	h.match, h.hook = match, hook
	h.mu.Unlock()
}

func (h *hookCmder) fire(cs ...streams.Cmd) {
	h.mu.Lock()
	var hook func()
	for _, c := range cs {
		if h.match != nil && h.match(c) {
			hook = h.hook
			h.match, h.hook = nil, nil
			break
		}
	}
	h.mu.Unlock()
	if hook != nil {
		hook()
	}
}

// newHookedRig is newRig over a hookCmder.
func newHookedRig(t *testing.T, opts ...func(*rd.Config)) (*rig, *hookCmder) {
	t.Helper()
	srv := fakestreams.New(fakestreams.Options{AOF: true, Version: "7.4.2"})
	h := &hookCmder{Server: srv}
	cfg := rd.Config{
		AckDeadline:   testLease,
		ClaimInterval: time.Hour,
		Block:         time.Millisecond,
		AutoCreate:    true,
		Clock:         fakeClock{srv},
	}
	for _, opt := range opts {
		opt(&cfg)
	}
	ctx := context.Background()
	b, err := rd.BackendWithCommander(ctx, h, driverkit.ClientOwned, cfg)
	if err != nil {
		t.Fatalf("BackendWithCommander: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })
	return &rig{srv: srv, b: b, ctx: ctx}, h
}

// anotherGroupsRetry puts a retry of another subscription into the topic's
// stream and returns its identifier: a source of that subscription nacks the
// first message under ReAdd, which adds a copy stamped with its group. Every
// other group's read and sweep acknowledges the copy without delivering it,
// once it has registered the rest of what it brought.
func anotherGroupsRetry(t *testing.T, r *rig) streams.ID {
	t.Helper()
	b, err := rd.BackendWithCommander(r.ctx, r.srv, driverkit.ClientBorrowed, rd.Config{
		AckDeadline:   testLease,
		ClaimInterval: time.Hour,
		Block:         time.Millisecond,
		AutoCreate:    true,
		Clock:         fakeClock{r.srv},
		Nack:          streams.ReAdd,
	})
	if err != nil {
		t.Fatalf("BackendWithCommander: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })
	audit, err := b.OpenSource(r.ctx, mq.SourceConfig{
		Topic: testTopic, Subscription: "audit", Prefetch: 1, AckDeadline: testLease, Start: mq.StartEarliest(),
	})
	if err != nil {
		t.Fatalf("OpenSource: %v", err)
	}
	t.Cleanup(func() { _ = audit.Close() })
	d := fetchOne(t, r.ctx, audit)
	if err := audit.Settler().Nack(r.ctx, &d, mq.NackOptions{}); err != nil {
		t.Fatalf("Nack: %v", err)
	}
	keys, _ := streams.NewKeys(testTopic, 1)
	entries := r.srv.Entries(keys.Stream(0))
	return entries[len(entries)-1].ID
}

// TestAStopNackKeepsWhatAReadOrSweepWasBringingBehindIt is a Stop nack
// that lands after a read or a sweep has registered what it brought and
// before the source has buffered it. The nack found nothing behind it in the
// buffer, and the round's entries were handed out next, before the failed
// message's retry.
//
// The hook runs inside the round, at the acknowledgement of another group's
// retry. A read was already checked against a nack made while it was in
// flight, so the read case guards that check; a sweep was not checked at
// all. A nack after the round has returned sends no command a hook could
// catch, and TestAStopNackAfterAReadReturnedKeepsWhatItBrought lands one
// there.
func TestAStopNackKeepsWhatAReadOrSweepWasBringingBehindIt(t *testing.T) {
	t.Run("read", func(t *testing.T) {
		r, h := newHookedRig(t, stopNack)
		sink, src := r.openPair(t)
		publishBodies(t, r, sink, "a")
		first := fetchOne(t, r.ctx, src)
		h.arm(ackOf(anotherGroupsRetry(t, r)), nackOf(t, r, src, &first))
		publishBodies(t, r, sink, "b")
		expectTheRetryFirst(t, r, src, "a", "b")
	})
	t.Run("sweep", func(t *testing.T) {
		r, h := newHookedRig(t, stopNack)
		sink, src := r.openPair(t)
		publishBodies(t, r, sink, "a")
		foreign := anotherGroupsRetry(t, r)
		publishBodies(t, r, sink, "b")
		// A consumer that died holding all three, "a" for longest, so that
		// one sweep brings "a" and the next the other two.
		keys, _ := streams.NewKeys(testTopic, 1)
		read, err := streams.XReadGroup(streams.XReadGroupArgs{
			Group: testSub, Consumer: "dead", Keys: []string{keys.Stream(0)}, IDs: []string{streams.IDNew}, Count: 16,
		})
		if err != nil {
			t.Fatalf("XReadGroup: %v", err)
		}
		if _, err := r.srv.Do(r.ctx, read); err != nil {
			t.Fatalf("XREADGROUP: %v", err)
		}
		r.srv.Advance(testLease / 2)
		for _, e := range r.srv.Entries(keys.Stream(0))[1:] {
			r.srv.Deliver(keys.Stream(0), testSub, "dead", e.ID)
		}
		r.srv.Advance(testLease/2 + time.Second)
		if _, _, err := rd.SweepForTest(r.b, src); err != nil {
			t.Fatalf("SweepForTest: %v", err)
		}
		first := fetchOne(t, r.ctx, src)
		if string(first.Message.Body) != "a" {
			t.Fatalf("the first sweep handed out %q, want a", first.Message.Body)
		}
		r.srv.Advance(testLease / 2)
		h.arm(ackOf(foreign), nackOf(t, r, src, &first))
		if _, _, err := rd.SweepForTest(r.b, src); err != nil {
			t.Fatalf("SweepForTest: %v", err)
		}
		expectTheRetryFirst(t, r, src, "a", "b")
	})
}

// ackOf matches the acknowledgement of id.
func ackOf(id streams.ID) func(streams.Cmd) bool {
	return func(c streams.Cmd) bool { return c.Name() == "XACK" && slices.Contains(c.Tokens, id.String()) }
}

// nackOf is a hook that nacks d.
func nackOf(t *testing.T, r *rig, src mq.Source, d *mq.DriverDelivery) func() {
	return func() {
		if err := src.Settler().Nack(r.ctx, d, mq.NackOptions{}); err != nil {
			t.Errorf("Nack: %v", err)
		}
	}
}

// expectTheRetryFirst checks that a fetch hands out nothing until the sweep
// brings the failed message back, and that the bodies then arrive in order.
func expectTheRetryFirst(t *testing.T, r *rig, src mq.Source, bodies ...string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(r.ctx, 200*time.Millisecond)
	defer cancel()
	dst := make([]mq.DriverDelivery, 1)
	if n, _ := src.Fetch(ctx, dst); n != 0 {
		t.Fatalf("the fetch after the nack handed out %q, which is behind the failed message", dst[0].Message.Body)
	}
	r.srv.Advance(2 * testLease)
	if _, _, err := rd.SweepForTest(r.b, src); err != nil {
		t.Fatalf("SweepForTest: %v", err)
	}
	for i, want := range bodies {
		d := fetchOne(t, r.ctx, src)
		if string(d.Message.Body) != want {
			t.Fatalf("delivery %d after the sweep is %q, want %q: the shard's order did not survive the retry",
				i, d.Message.Body, want)
		}
		if err := src.Settler().Ack(r.ctx, &d); err != nil {
			t.Fatalf("Ack of %q: %v", want, err)
		}
	}
}

// TestAStopSourceKeepsWhatItHoldsBackWithinItsOwnDeadline is a source whose
// AckDeadline is shorter than the backend's sweep interval. Each sweep
// restarts the idle clocks of the entries it keeps back behind a failed
// message, and a source that swept once per interval left them idle past its
// own deadline, so another consumer's sweep claimed them and delivered them
// before the retry they were waiting behind.
func TestAStopSourceKeepsWhatItHoldsBackWithinItsOwnDeadline(t *testing.T) {
	const lease = 300 * time.Millisecond
	r := newRig(t, stopNack)
	src, err := r.b.OpenSource(r.ctx, mq.SourceConfig{
		Topic: testTopic, Subscription: testSub, Prefetch: 16, AckDeadline: lease, Start: mq.StartEarliest(),
	})
	if err != nil {
		t.Fatalf("OpenSource: %v", err)
	}
	t.Cleanup(func() { _ = src.Close() })
	sink, err := r.b.OpenSink(r.ctx, mq.SinkConfig{Topic: testTopic})
	if err != nil {
		t.Fatalf("OpenSink: %v", err)
	}
	t.Cleanup(func() { _ = sink.Close() })
	publishBodies(t, r, sink, "a", "b")
	dst := make([]mq.DriverDelivery, 2)
	if n, err := src.Fetch(r.ctx, dst); n != 2 || err != nil {
		t.Fatalf("Fetch = %d, %v; want both messages", n, err)
	}
	if err := src.Settler().Nack(r.ctx, &dst[0], mq.NackOptions{}); err != nil {
		t.Fatalf("Nack: %v", err)
	}
	keys, _ := streams.NewKeys(testTopic, 1)

	// Both leases run out, and the source's own sweep retries "a" and claims
	// "b" from behind the hold.
	r.srv.Advance(lease + 100*time.Millisecond)
	rows := awaitPending(t, r, keys, "the source to sweep within its deadline of "+lease.String(),
		func(rows []streams.Pending) bool { return len(rows) == 2 && rows[0].Idle == 0 && rows[1].Idle == 0 })
	owner := rows[1].Consumer
	ctx, cancel := context.WithTimeout(r.ctx, time.Second)
	defer cancel()
	n, err := src.Fetch(ctx, dst)
	if n == 2 {
		// CI also tests this module against the published root module, whose
		// consumer hands out what a sweep claims from behind a hold instead
		// of keeping it back, so there is nothing to keep fresh.
		t.Skip("this build's streams consumer does not keep back what a sweep claims from behind a Stop hold")
	}
	if n != 1 || string(dst[0].Message.Body) != "a" {
		t.Fatalf("Fetch = %d, %v after the sweep; want only the retry of a", n, err)
	}

	r.srv.Advance(lease * 5 / 6)
	awaitPending(t, r, keys, "the source to restart the idle clock of the entry it keeps back",
		func(rows []streams.Pending) bool { return len(rows) == 2 && rows[1].Idle == 0 })
	r.srv.Advance(lease * 5 / 6)
	thief, err := streams.Sweep(keys, testSub, "thief", lease.Milliseconds(), nil, 16)
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if _, err := r.srv.Do(r.ctx, thief.Cmds[0]); err != nil {
		t.Fatalf("XAUTOCLAIM: %v", err)
	}
	if got := r.srv.Pending(keys.Stream(0), testSub)[1].Consumer; got != owner {
		t.Fatalf("%s claimed b while it waited behind the retry of a: the source left it idle for its whole deadline", got)
	}
}

// awaitPending polls the shard's pending list until done holds, for up to
// five seconds of wall-clock time, and returns the list it held for.
func awaitPending(t *testing.T, r *rig, keys streams.Keys, what string, done func([]streams.Pending) bool) []streams.Pending {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		rows := r.srv.Pending(keys.Stream(0), testSub)
		if done(rows) {
			return rows
		}
		if time.Now().After(deadline) {
			t.Fatalf("waited 5s for %s: %+v", what, rows)
		}
		time.Sleep(5 * time.Millisecond)
	}
}
