package redis_test

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/rueidis"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit"
	rd "github.com/iqhive/iqmq/drivers/redis"
	"github.com/iqhive/iqmq/internal/streams"
	"github.com/iqhive/iqmq/internal/streams/fakestreams"
	"github.com/iqhive/iqmq/internal/streams/respsrv"
)

// skipBeforeAtMostSweeps skips a test of the sweep's bound against a root
// module whose streams consumer has no SweepAtMost: built against v1.1.1, the
// source claims as it used to.
func skipBeforeAtMostSweeps(t *testing.T) {
	t.Helper()
	if v, old := rd.RootModuleBeforeForTest(t, "v1.1.2"); old {
		t.Skipf("this build uses root module %s, whose streams consumer cannot bound a sweep's claim, "+
			"so the source claims whatever the round finds", v)
	}
}

// ownersOf counts the pending entries of the test topic's shard 0 by the
// consumer that holds them.
func ownersOf(r *rig) map[string]int {
	keys, _ := streams.NewKeys(testTopic, 1)
	out := map[string]int{}
	for _, p := range r.srv.Pending(keys.Stream(0), testSub) {
		out[p.Consumer]++
	}
	return out
}

// crashWithAll publishes n messages and has a source fetch them all and die
// holding them, as a crashed consumer does. It returns that source, still
// open.
func crashWithAll(t *testing.T, r *rig, n int) mq.Source {
	t.Helper()
	sink, crashed := r.openPair(t)
	for i := range n {
		if err := sink.Publish(r.ctx, &mq.Message{Topic: testTopic, Body: []byte(strconv.Itoa(i))}, nil); err != nil {
			t.Fatalf("Publish: %v", err)
		}
	}
	dst := make([]mq.DriverDelivery, n)
	ctx, cancel := context.WithTimeout(r.ctx, 2*time.Second)
	defer cancel()
	if got, err := crashed.Fetch(ctx, dst); got != n {
		t.Fatalf("the consumer that crashes fetched %d of %d: %v", got, n, err)
	}
	return crashed
}

// openPrefetch opens a second source on the test subscription with the given
// Prefetch.
func openPrefetch(t *testing.T, r *rig, prefetch int) mq.Source {
	t.Helper()
	src, err := r.b.OpenSource(r.ctx, mq.SourceConfig{
		Topic: testTopic, Subscription: testSub, Prefetch: prefetch, AckDeadline: testLease, Start: mq.StartEarliest(),
	})
	if err != nil {
		t.Fatalf("OpenSource: %v", err)
	}
	t.Cleanup(func() { _ = src.Close() })
	return src
}

// fetchNothing runs a Fetch that finds nothing, which is what a caller that
// is between messages does.
func fetchNothing(t *testing.T, r *rig, src mq.Source) {
	t.Helper()
	ctx, cancel := context.WithTimeout(r.ctx, 50*time.Millisecond)
	defer cancel()
	if n, err := src.Fetch(ctx, make([]mq.DriverDelivery, 1)); n != 0 {
		t.Fatalf("Fetch on an idle subscription = %d, %v", n, err)
	}
}

// TestASourceNobodyFetchesFromClaimsNothing is a source whose caller is not
// fetching, as a runner's is while every MaxInFlight slot is busy. Its sweep
// used to claim another consumer's expired entries into a buffer nothing
// drained, up to ClaimCount per shard whatever Prefetch said, and to claim
// them again each deadline they waited there, each claim an attempt: a
// fetching source never got them, and the busy one handed them out, once it
// had room, already past MaxAttempts.
func TestASourceNobodyFetchesFromClaimsNothing(t *testing.T) {
	skipBeforeAtMostSweeps(t)
	r := newRig(t)
	crashWithAll(t, r, 16)
	busy := openPrefetch(t, r, 1)
	r.srv.Advance(testLease + time.Second)
	buffered, live, err := rd.LoopSweepForTest(busy)
	if err != nil {
		t.Fatalf("LoopSweepForTest: %v", err)
	}
	if buffered != 0 || live != 0 {
		t.Errorf("a source whose caller has never fetched claimed %d entries and holds %d (owners now %v): "+
			"they are kept from the sources that are fetching", buffered, live, ownersOf(r))
	}
	if owners := ownersOf(r); len(owners) != 1 {
		t.Errorf("the expired entries changed hands with nobody fetching: %v", owners)
	}
}

// TestTheSweepLoopClaimsOnlyWhileItsCallerFetches is the same through the
// loop itself: rounds every 20ms claim nothing for a caller that is busy, and
// the first Fetch gets a redelivery within a round or two.
func TestTheSweepLoopClaimsOnlyWhileItsCallerFetches(t *testing.T) {
	skipBeforeAtMostSweeps(t)
	r := newRig(t, func(c *rd.Config) { c.ClaimInterval = 20 * time.Millisecond })
	crashed := crashWithAll(t, r, 16)
	if err := crashed.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	busy := openPrefetch(t, r, 1)
	r.srv.Advance(testLease + time.Second)
	time.Sleep(200 * time.Millisecond) // about ten rounds
	if owners := ownersOf(r); len(owners) != 1 {
		t.Fatalf("the expired entries changed hands while nobody was fetching: %v", owners)
	}
	ctx, cancel := context.WithTimeout(r.ctx, 2*time.Second)
	defer cancel()
	dst := make([]mq.DriverDelivery, 1)
	if n, err := busy.Fetch(ctx, dst); n != 1 {
		t.Fatalf("Fetch = %d, %v; want a redelivery once the caller fetches", n, err)
	}
	if dst[0].Info.Attempt != 2 {
		t.Errorf("the redelivery reports attempt %d, want 2", dst[0].Info.Attempt)
	}
	if err := busy.Settler().Ack(r.ctx, &dst[0]); err != nil {
		t.Fatalf("Ack: %v", err)
	}
}

// TestASweepClaimsNoMoreThanPrefetch is the sweep's claim for a caller that is
// fetching: at most Prefetch less what the source already holds, as the
// specification's SourceConfig.Prefetch says ("how many messages the source
// may hold unclaimed"). It used to be ClaimCount per shard, 128 by default.
func TestASweepClaimsNoMoreThanPrefetch(t *testing.T) {
	skipBeforeAtMostSweeps(t)
	r := newRig(t)
	crashWithAll(t, r, 16)
	src := openPrefetch(t, r, 1)
	fetchNothing(t, r, src)
	r.srv.Advance(testLease + time.Second)
	buffered, live, err := rd.LoopSweepForTest(src)
	if err != nil {
		t.Fatalf("LoopSweepForTest: %v", err)
	}
	if buffered != 1 || live != 1 {
		t.Fatalf("a Prefetch-1 source's sweep claimed %d entries and holds %d, want 1 (owners now %v)",
			buffered, live, ownersOf(r))
	}
	d := fetchOne(t, r.ctx, src)
	if d.Info.Attempt != 2 || !d.Info.Redelivered {
		t.Errorf("the redelivery reports attempt %d (redelivered %v), want 2", d.Info.Attempt, d.Info.Redelivered)
	}
	if err := src.Settler().Ack(r.ctx, &d); err != nil {
		t.Fatalf("Ack: %v", err)
	}
	// Room again, and a caller that fetched since the previous round.
	if buffered, _, err := rd.LoopSweepForTest(src); err != nil || buffered != 1 {
		t.Fatalf("the next round claimed %d, %v, want 1 now that the source has room", buffered, err)
	}
}

// TestARoundWithAFullBufferStillAsksTheServer is a round for a caller that is
// fetching while the source already holds Prefetch deliveries, which a read
// of a partitioned topic can bring (its COUNT is per shard). The round has no
// room to claim, and it sent no command whose failure it reported, so with
// the server failing every command it reported a good round, and a degraded
// Health went back to Ready for that round.
func TestARoundWithAFullBufferStillAsksTheServer(t *testing.T) {
	r := newRig(t, func(c *rd.Config) { c.Partitions = 2 })
	sink, err := r.b.OpenSink(r.ctx, mq.SinkConfig{Topic: testTopic})
	if err != nil {
		t.Fatalf("OpenSink: %v", err)
	}
	t.Cleanup(func() { _ = sink.Close() })
	src := openPrefetch(t, r, 2)
	for i := range 8 {
		msg := &mq.Message{Topic: testTopic, Key: []byte(strconv.Itoa(i)), Body: []byte(strconv.Itoa(i))}
		if err := sink.Publish(r.ctx, msg, nil); err != nil {
			t.Fatalf("Publish: %v", err)
		}
	}
	// The read takes up to two from each shard and hands out one.
	_ = fetchOne(t, r.ctx, src)
	keys, _ := streams.NewKeys(testTopic, 2)
	held := len(r.srv.Pending(keys.Stream(0), testSub)) + len(r.srv.Pending(keys.Stream(1), testSub))
	if held < 3 {
		t.Fatalf("the read brought %d entries, so the buffer is not full and the test proves nothing", held)
	}
	loading := errors.New("LOADING Redis is loading the dataset in memory")
	for _, verb := range []string{"XAUTOCLAIM", "XPENDING", "EVALSHA", "XCLAIM"} {
		r.srv.Fail(verb, loading)
	}
	if buffered, _, err := rd.LoopSweepForTest(src); err == nil {
		t.Errorf("a round for a fetching caller holding %d deliveries (Prefetch 2) claimed %d and reported "+
			"success while the server refused every command", held, buffered)
	}
}

// TestAWaitingRedeliveryIsNotClaimedAgain is a redelivery that waits in the
// buffer while its caller fetches something else, then while it is busy. A
// sweep round used to claim it again each deadline it waited, so it reached
// its handler counted as several attempts no handler made.
func TestAWaitingRedeliveryIsNotClaimedAgain(t *testing.T) {
	skipBeforeAtMostSweeps(t)
	r := newRig(t)
	sink, src := r.openPair(t)
	publishBodies(t, r, sink, "a", "b")
	dst := make([]mq.DriverDelivery, 2)
	ctx, cancel := context.WithTimeout(r.ctx, 2*time.Second)
	n, err := src.Fetch(ctx, dst)
	cancel()
	if n != 2 {
		t.Fatalf("Fetch = %d, %v", n, err)
	}
	// Both handlers die, and the sweep brings both back into the buffer.
	r.srv.Advance(testLease + time.Second)
	if buffered, _, err := rd.SweepForTest(r.b, src); err != nil || buffered != 2 {
		t.Fatalf("SweepForTest = %d, %v; want both redeliveries", buffered, err)
	}
	// The caller takes one and is then busy for three deadlines; a round runs
	// in each, the first of them right after the caller fetched.
	first := fetchOne(t, r.ctx, src)
	if err := src.Settler().Ack(r.ctx, &first); err != nil {
		t.Fatalf("Ack: %v", err)
	}
	for range 3 {
		r.srv.Advance(testLease + time.Second)
		if _, _, err := rd.LoopSweepForTest(src); err != nil {
			t.Fatalf("LoopSweepForTest: %v", err)
		}
	}
	second := fetchOne(t, r.ctx, src)
	if second.Info.Attempt != 2 {
		t.Errorf("a message delivered once, to a handler that died, reached its second handler as attempt %d, "+
			"want 2: the sweep claimed it again while it waited in the buffer", second.Info.Attempt)
	}
	if left := second.Info.LeaseExpires.Sub(r.srv.Now()); left < testLease-time.Second {
		t.Errorf("the delivery that waited reached its handler with %v of its lease left, want about %v", left, testLease)
	}
	if err := src.Settler().Ack(r.ctx, &second); err != nil {
		t.Fatalf("Ack of the delivery that waited: %v", err)
	}
}

// TestAWaitingDeliveryAnotherConsumerTookIsNotHandedOut is a source whose
// caller stopped fetching with a redelivery still in its buffer, which
// another source's sweep then claims and delivers. The first source used to
// hand out its copy when its caller came back, so a second handler ran the
// message under a lease that had run out, and its settlement was refused.
func TestAWaitingDeliveryAnotherConsumerTookIsNotHandedOut(t *testing.T) {
	r := newRig(t)
	sink, slow := r.openPair(t)
	publishBodies(t, r, sink, "a", "b")
	dst := make([]mq.DriverDelivery, 2)
	ctx, cancel := context.WithTimeout(r.ctx, 2*time.Second)
	n, err := slow.Fetch(ctx, dst)
	cancel()
	if n != 2 {
		t.Fatalf("Fetch = %d, %v", n, err)
	}
	r.srv.Advance(testLease + time.Second)
	if buffered, _, err := rd.SweepForTest(r.b, slow); err != nil || buffered != 2 {
		t.Fatalf("SweepForTest = %d, %v; want both redeliveries", buffered, err)
	}
	first := fetchOne(t, r.ctx, slow)
	if err := slow.Settler().Ack(r.ctx, &first); err != nil {
		t.Fatalf("Ack: %v", err)
	}
	// The slow source's caller is busy now. Another source is fetching, and
	// its sweep takes what the slow one holds once it has waited a deadline.
	other := openPrefetch(t, r, 16)
	r.srv.Advance(testLease + time.Second)
	fetchNothing(t, r, other)
	if buffered, _, err := rd.SweepForTest(r.b, other); err != nil || buffered != 1 {
		t.Fatalf("the fetching source's sweep claimed %d, %v; want the delivery the busy source holds", buffered, err)
	}
	taken := fetchOne(t, r.ctx, other)
	ctx, cancel = context.WithTimeout(r.ctx, 200*time.Millisecond)
	defer cancel()
	got := make([]mq.DriverDelivery, 1)
	if n, _ := slow.Fetch(ctx, got); n != 0 {
		err := slow.Settler().Ack(r.ctx, &got[0])
		t.Errorf("the busy source handed out %q, which the other source had claimed and delivered as attempt %d; "+
			"its settlement: %v", got[0].Message.Body, taken.Info.Attempt, err)
	}
	if err := other.Settler().Ack(r.ctx, &taken); err != nil {
		t.Fatalf("Ack on the source that took it: %v", err)
	}
}

// TestARedeliveryDoesNotWaitOutAParkedRead is a redelivery the sweep makes
// while the source's Fetch is parked in XREADGROUP. It waits in the buffer
// until the read returns, and the read used to block for the whole
// Config.Block: 5 s by default, longer than the sweep interval whenever the
// deadline is under 15 s, and with a deadline shorter than the block the
// sweep claimed the waiting entry again, counting an attempt each time.
func TestARedeliveryDoesNotWaitOutAParkedRead(t *testing.T) {
	const block = 3 * time.Second
	r := newRig(t, func(c *rd.Config) {
		c.Block = block
		c.ClaimInterval = 50 * time.Millisecond
	})
	sink, src := r.openPair(t)
	publishBodies(t, r, sink, "m")
	_ = fetchOne(t, r.ctx, src) // its handler dies
	r.srv.Advance(testLease + time.Second)
	start := time.Now()
	ctx, cancel := context.WithTimeout(r.ctx, 2*block)
	defer cancel()
	dst := make([]mq.DriverDelivery, 1)
	n, err := src.Fetch(ctx, dst)
	took := time.Since(start)
	if n != 1 {
		t.Fatalf("Fetch = %d, %v", n, err)
	}
	if took >= block/2 {
		t.Errorf("the redelivery took %v to reach the caller, with a sweep every 50ms: it waited for the "+
			"read parked in XREADGROUP BLOCK %v", took.Round(time.Millisecond), block)
	}
	if dst[0].Info.Attempt != 2 {
		t.Errorf("the redelivery reports attempt %d, want 2", dst[0].Info.Attempt)
	}
}

// readLog is a commander over the fake that keeps every XREADGROUP it was
// handed, with how much of its context was left when it was and what the
// server answered.
type readLog struct {
	*fakestreams.Server

	mu    sync.Mutex
	reads []loggedRead
}

type loggedRead struct {
	cmd  streams.Cmd
	left time.Duration
	err  error
}

func (l *readLog) Do(ctx context.Context, c streams.Cmd) (streams.Value, error) {
	if c.Name() != "XREADGROUP" {
		return l.Server.Do(ctx, c)
	}
	var left time.Duration
	if deadline, ok := ctx.Deadline(); ok {
		left = time.Until(deadline)
	}
	v, err := l.Server.Do(ctx, c)
	l.mu.Lock()
	l.reads = append(l.reads, loggedRead{cmd: c, left: left, err: err})
	l.mu.Unlock()
	return v, err
}

func (l *readLog) logged() []loggedRead {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]loggedRead(nil), l.reads...)
}

// blockOf is a read's BLOCK argument, or "" when it polls.
func blockOf(c streams.Cmd) string {
	for i, tok := range c.Tokens {
		if tok == "BLOCK" && i+1 < len(c.Tokens) {
			return c.Tokens[i+1]
		}
	}
	return ""
}

// TestAFetchDeadlineInsideTheBlockEndsTheReadFirst is a Fetch whose context
// ends before Config.Block would. Its read used to block for the whole
// Config.Block, so the client gave up on a read the server still held: both
// clients close the connection such a read waited on, so every short pull
// cost a dial, and whatever the server handed the read in its last moment
// was pending to this consumer and never delivered until the sweep claimed it
// a deadline later, as attempt 2. The read now asks the server to give up
// first, and the server's empty answer is what ends it.
func TestAFetchDeadlineInsideTheBlockEndsTheReadFirst(t *testing.T) {
	if v, old := rd.RootModuleBeforeForTest(t, "v1.1.2"); old {
		t.Skipf("this build uses root module %s, whose streams consumer reads with the whole block", v)
	}
	srv := fakestreams.New(fakestreams.Options{AOF: true, Version: "7.4.2"})
	log := &readLog{Server: srv}
	r := newRecordingRig(t, log, srv)
	src := openPrefetch(t, r, 4)
	const wait = time.Second
	ctx, cancel := context.WithTimeout(r.ctx, wait)
	defer cancel()
	start := time.Now()
	n, err := src.Fetch(ctx, make([]mq.DriverDelivery, 4))
	took := time.Since(start)
	if n != 0 || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Fetch on an idle subscription = %d, %v; want its deadline", n, err)
	}
	if took > wait+100*time.Millisecond {
		t.Errorf("Fetch returned %v after a deadline of %v", took.Round(time.Millisecond), wait)
	}
	reads := log.logged()
	if len(reads) == 0 {
		t.Fatal("Fetch sent no XREADGROUP")
	}
	if reads[0].err != nil {
		t.Errorf("the read under a %v deadline (BLOCK %q, Config.Block %v) ended with %v: the client gave up on "+
			"it while the server held it", wait, blockOf(reads[0].cmd), rd.DefaultBlock, reads[0].err)
	}
	for i, read := range reads {
		ms, _ := strconv.Atoi(blockOf(read.cmd))
		if block := time.Duration(ms) * time.Millisecond; block >= read.left {
			t.Errorf("read %d of %d sent BLOCK %v with %v of its context left", i+1, len(reads), block, read.left)
		}
	}
	// A read ends 150ms before the deadline, so a Fetch that read again until
	// its deadline would poll the server in a loop for those 150ms. It waits
	// them out instead.
	if len(reads) > 4 {
		t.Errorf("a Fetch under a %v deadline sent %d reads, want a few: it polled the server until its deadline",
			wait, len(reads))
	}
}

// TestShortFetchesKeepTheirConnection is what a Fetch whose deadline ended
// inside the block cost on the wire. rueidis closes the connection of a
// blocking command its context ended rather than reuse it with the server's
// answer still to come, so every idle pull with a deadline shorter than
// Config.Block dialled again, and over TLS shook hands again.
func TestShortFetchesKeepTheirConnection(t *testing.T) {
	if v, old := rd.RootModuleBeforeForTest(t, "v1.1.2"); old {
		t.Skipf("this build uses root module %s, whose streams consumer reads with the whole block", v)
	}
	fake := fakestreams.New(fakestreams.Options{AOF: true, Version: "7.4.2"})
	srv, err := respsrv.Serve(fake, respsrv.Options{Version: "7.4.2"})
	if err != nil {
		t.Fatalf("respsrv.Serve: %v", err)
	}
	t.Cleanup(func() { _ = srv.Close() })
	var dials atomic.Int32
	client, err := rueidis.NewClient(rueidis.ClientOption{
		InitAddress:  []string{srv.Addr()},
		DisableCache: true,
		DialCtxFn: func(ctx context.Context, addr string, d *net.Dialer, _ *tls.Config) (net.Conn, error) {
			dials.Add(1)
			return d.DialContext(ctx, "tcp", addr)
		},
	})
	if err != nil {
		t.Fatalf("rueidis.NewClient: %v", err)
	}
	cmder, err := rd.NewCommander(client)
	if err != nil {
		client.Close()
		t.Fatalf("NewCommander: %v", err)
	}
	b, err := rd.BackendWithCommander(context.Background(), cmder, driverkit.ClientOwned, rd.Config{AutoCreate: true})
	if err != nil {
		_ = cmder.Close()
		t.Fatalf("BackendWithCommander: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })
	src, err := b.OpenSource(context.Background(), mq.SourceConfig{
		Topic: testTopic, Subscription: testSub, Prefetch: 4, Start: mq.StartEarliest(),
	})
	if err != nil {
		t.Fatalf("OpenSource: %v", err)
	}
	t.Cleanup(func() { _ = src.Close() })
	fetch := func() {
		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		defer cancel()
		if n, err := src.Fetch(ctx, make([]mq.DriverDelivery, 4)); n != 0 || !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("an idle Fetch = %d, %v; want its deadline", n, err)
		}
	}
	fetch() // the first takes a connection for blocking reads
	before := dials.Load()
	const fetches = 10
	for range fetches {
		fetch()
	}
	if n := dials.Load() - before; n >= fetches/2 {
		t.Errorf("%d idle Fetches with a 200ms deadline (Config.Block %v) dialled %d new connections", fetches, rd.DefaultBlock, n)
	}
}
