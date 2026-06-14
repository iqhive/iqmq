package redis_test

import (
	"context"
	"errors"
	"testing"
	"time"

	mq "bitbucket.org/iqhive/iqmq"
	"bitbucket.org/iqhive/iqmq/driverkit"
	rd "bitbucket.org/iqhive/iqmq/drivers/redis"
	"bitbucket.org/iqhive/iqmq/internal/streams"
	"bitbucket.org/iqhive/iqmq/internal/streams/fakestreams"
)

const (
	testTopic = "acme.orders"
	testSub   = "workers"
	testLease = 5 * time.Second
)

// rig is a fake server and a backend over it.
//
// Almost every test in this package uses one, and the reason is the whole
// point of [streams.Commander]: the fake is a Commander, so the driver under
// test is the real driver — the real sink, the real source, the real settler,
// the real reclaim loop — with no container, no network and a clock the test
// sets. The conformance suite against a real container is what proves the
// Lua and the protocol; this is what proves the driver.
type rig struct {
	srv *fakestreams.Server
	b   *rd.Backend
	ctx context.Context
}

func newRig(t testing.TB, opts ...func(*rd.Config)) *rig {
	t.Helper()
	srv := fakestreams.New(fakestreams.Options{AOF: true, Version: "7.4.2"})
	cfg := rd.Config{
		AckDeadline:   testLease,
		ClaimInterval: time.Hour, // the tests drive the sweep themselves
		Block:         time.Millisecond,
		AutoCreate:    true,
		Clock:         fakeClock{srv},
	}
	for _, opt := range opts {
		opt(&cfg)
	}
	ctx := context.Background()
	b, err := rd.BackendWithCommander(ctx, srv, driverkit.ClientOwned, cfg)
	if err != nil {
		t.Fatalf("BackendWithCommander: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })
	return &rig{srv: srv, b: b, ctx: ctx}
}

// fakeClock reads the fake server's clock, so that the driver's notion of now
// and the server's idle times cannot drift apart.
type fakeClock struct{ srv *fakestreams.Server }

func (c fakeClock) Now() time.Time { return c.srv.Now() }

func (c fakeClock) NewTimer(d time.Duration) mq.Timer { return realTimer{time.NewTimer(d)} }

type realTimer struct{ t *time.Timer }

func (r realTimer) C() <-chan time.Time        { return r.t.C }
func (r realTimer) Stop() bool                 { return r.t.Stop() }
func (r realTimer) Reset(d time.Duration) bool { return r.t.Reset(d) }

// openPair opens a sink and a source on one topic.
func (r *rig) openPair(t testing.TB) (mq.Sink, mq.Source) {
	t.Helper()
	src, err := r.b.OpenSource(r.ctx, mq.SourceConfig{
		Topic:        testTopic,
		Subscription: testSub,
		Prefetch:     16,
		AckDeadline:  testLease,
		Start:        mq.StartEarliest(),
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
	return sink, src
}

// fetchOne fetches exactly one delivery, failing if none arrives.
func fetchOne(t testing.TB, ctx context.Context, src mq.Source) mq.DriverDelivery {
	t.Helper()
	dst := make([]mq.DriverDelivery, 1)
	fctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	n, err := src.Fetch(fctx, dst)
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if n != 1 {
		t.Fatalf("Fetch returned %d deliveries, want 1", n)
	}
	return dst[0]
}

// TestPublishAndSettleRoundTrip is the driver's basic shape: a message goes
// in through the SPI, comes back out with its metadata intact, and settling
// it removes the pending entry.
func TestPublishAndSettleRoundTrip(t *testing.T) {
	r := newRig(t)
	sink, src := r.openPair(t)
	want := &mq.Message{
		Topic:      testTopic,
		Key:        []byte("order-4711"),
		Body:       []byte("payload"),
		ID:         "01JQZ8V3K5T2WQ9X7YB6C4D2E1",
		Properties: mq.Properties{ContentType: "application/json"},
	}
	want.Headers.Add("trace", []byte{0, 1, 0xff})
	want.Headers.Add("trace", []byte("second"))
	if err := sink.Publish(r.ctx, want, nil); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	d := fetchOne(t, r.ctx, src)
	if string(d.Message.Body) != "payload" {
		t.Fatalf("body = %q", d.Message.Body)
	}
	if string(d.Message.Key) != "order-4711" {
		t.Fatalf("key = %q", d.Message.Key)
	}
	if d.Message.ID != want.ID {
		t.Fatalf("id = %q", d.Message.ID)
	}
	if got := d.Message.Headers.Values("trace"); len(got) != 2 ||
		string(got[0]) != "\x00\x01\xff" || string(got[1]) != "second" {
		t.Fatalf("duplicate binary headers did not survive: %q", got)
	}
	if d.Info.Attempt != 1 {
		t.Fatalf("attempt = %d, want 1", d.Info.Attempt)
	}
	if d.Info.Redelivered {
		t.Fatal("a first delivery reported Redelivered")
	}
	if !d.Info.HasSequence {
		t.Fatal("the delivery reports no sequence; a stream identifier's millisecond half is one")
	}
	if d.Info.LeaseExpires.IsZero() {
		t.Fatal("the delivery reports no lease deadline; this driver grants one even though Redis does not")
	}
	if err := src.Settler().Ack(r.ctx, &d); err != nil {
		t.Fatalf("Ack: %v", err)
	}
}

// TestPublishRefusesWhatItCannotHonour is enforced before I/O: a
// publication control this broker has no way to honour is refused, and
// nothing is written.
//
// The negative half is what makes it mean something: the stream must still be
// empty afterwards. A driver that refused the option AFTER the XADD would
// pass an assertion on the error and have published the message anyway.
func TestPublishRefusesWhatItCannotHonour(t *testing.T) {
	r := newRig(t)
	sink, _ := r.openPair(t)
	cases := []struct {
		name string
		opts mq.PublishOptions
	}{
		{"delay", mq.PublishOptions{Delay: time.Minute}},
		{"deliverat", mq.PublishOptions{DeliverAt: time.Unix(1767225600, 0)}},
		{"ttl", mq.PublishOptions{TTL: time.Minute}},
		{"priority", mq.PublishOptions{PrioritySet: true, Priority: 5}},
		{"dedupkey", mq.PublishOptions{DedupKey: "k"}},
	}
	keys, err := streams.NewKeys(testTopic, 1)
	if err != nil {
		t.Fatalf("NewKeys: %v", err)
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			o := tc.opts
			err := sink.Publish(r.ctx, &mq.Message{Topic: testTopic, Body: []byte("x")}, &o)
			if !errors.Is(err, mq.ErrUnsupported) {
				t.Fatalf("Publish with %s returned %v, want ErrUnsupported", tc.name, err)
			}
			if n := len(r.srv.Entries(keys.Stream(0))); n != 0 {
				t.Fatalf("the stream holds %d entries after a refused publication: the option was "+
					"refused after the XADD, so the message was published anyway", n)
			}
		})
	}
}

// TestPublishRefusesAMessageItCouldNotDecode is at the SPI:
// the limit check happens before any I/O, in every metadata mode, so a
// publication the driver accepted is one the driver can read back.
func TestPublishRefusesAMessageItCouldNotDecode(t *testing.T) {
	keys, err := streams.NewKeys(testTopic, 1)
	if err != nil {
		t.Fatalf("NewKeys: %v", err)
	}
	for _, mode := range []mq.MetadataMode{
		mq.MetadataNativeStrict,
		mq.MetadataLenientDrop,
		mq.MetadataPortableEnvelope,
		mq.MetadataAdaptive,
	} {
		t.Run(mode.String(), func(t *testing.T) {
			r := newRig(t)
			sink, err := r.b.OpenSink(r.ctx, mq.SinkConfig{Topic: testTopic, Metadata: mode})
			if err != nil {
				t.Fatalf("OpenSink: %v", err)
			}
			defer func() { _ = sink.Close() }()
			m := &mq.Message{Topic: testTopic, Body: []byte("x")}
			m.Headers.Add(repeat('k', streams.DefaultLimits.MaxHeaderKey+1), []byte("v"))
			if err := sink.Publish(r.ctx, m, nil); err == nil {
				t.Fatalf("Publish accepted a message whose header key is one byte over the "+
					"envelope's decode limit in %s mode; it would be stored and then break every "+
					"Fetch on the subscription for ever ", mode)
			}
			if n := len(r.srv.Entries(keys.Stream(0))); n != 0 {
				t.Fatalf("the stream holds %d entries after the refusal", n)
			}
			// A non-UTF-8 header key is the same hole in its third skin: the
			// envelope's decoder validates UTF-8 in the identifier, header
			// keys and string properties and its encoder validates nothing.
			bad := &mq.Message{Topic: testTopic, Body: []byte("x")}
			bad.Headers.Add("\xff\xfe", []byte("v"))
			if err := sink.Publish(r.ctx, bad, nil); err == nil {
				t.Fatalf("Publish accepted a message with a non-UTF-8 header key in %s mode", mode)
			}
		})
	}
}

func repeat(c byte, n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = c
	}
	return string(b)
}

// TestAckOfAStolenDeliveryIsRefused is the disqualifier at the SPI boundary:
// the guard has to be reachable through Settler.Ack, not only through the
// command layer.
func TestAckOfAStolenDeliveryIsRefused(t *testing.T) {
	r := newRig(t)
	sink, src := r.openPair(t)
	if err := sink.Publish(r.ctx, &mq.Message{Topic: testTopic, Body: []byte("x")}, nil); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	d := fetchOne(t, r.ctx, src)
	keys, _ := streams.NewKeys(testTopic, 1)
	pending := r.srv.Pending(keys.Stream(0), testSub)
	if len(pending) != 1 {
		t.Fatalf("pending = %d, want 1", len(pending))
	}
	// Another process claims it.
	if !r.srv.Deliver(keys.Stream(0), testSub, "someone-else", pending[0].ID) {
		t.Fatal("hand-off failed")
	}
	if err := src.Settler().Ack(r.ctx, &d); !errors.Is(err, mq.ErrLeaseLost) {
		t.Fatalf("Ack of a stolen delivery returned %v, want ErrLeaseLost", err)
	}
	after := r.srv.Pending(keys.Stream(0), testSub)
	if len(after) != 1 || after[0].Consumer != "someone-else" {
		t.Fatalf("the other consumer's entry was acknowledged anyway: pending=%v", after)
	}
}

// TestExtendDoesNotStealTheEntry is at the SPI boundary.
func TestExtendDoesNotStealTheEntry(t *testing.T) {
	r := newRig(t)
	sink, src := r.openPair(t)
	if err := sink.Publish(r.ctx, &mq.Message{Topic: testTopic, Body: []byte("x")}, nil); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	d := fetchOne(t, r.ctx, src)
	keys, _ := streams.NewKeys(testTopic, 1)
	pending := r.srv.Pending(keys.Stream(0), testSub)
	if !r.srv.Deliver(keys.Stream(0), testSub, "someone-else", pending[0].ID) {
		t.Fatal("hand-off failed")
	}
	ext, ok := src.Settler().(mq.DeadlineExtender)
	if !ok {
		t.Fatal("the settler is not a DeadlineExtender; FeatureExtendLease is advertised")
	}
	err := ext.Extend(r.ctx, &d, time.Minute)
	if !errors.Is(err, mq.ErrLeaseLost) {
		t.Fatalf("Extend of an entry another consumer holds returned %v, want ErrLeaseLost", err)
	}
	after := r.srv.Pending(keys.Stream(0), testSub)
	if len(after) != 1 || after[0].Consumer != "someone-else" {
		t.Fatalf("Extend stole the entry from the other consumer: pending=%v", after)
	}
}

// TestBatchResultsAreIndexAligned is the batch contract for both directions:
// a publication batch and a settlement batch.
//
// Alignment is the whole contract: a result aligned with anything but the
// input reports one message's outcome for another, which is how a batch
// publish loses a message silently.
func TestBatchResultsAreIndexAligned(t *testing.T) {
	r := newRig(t)
	sink, src := r.openPair(t)
	batch, ok := sink.(mq.BatchSink)
	if !ok {
		t.Fatal("the sink is not a BatchSink; FeatureBatchPublish is advertised as native")
	}
	// The middle message is refused before any I/O: its header key is one
	// byte over the limit.
	bad := &mq.Message{Topic: testTopic, Body: []byte("bad")}
	bad.Headers.Add(repeat('k', streams.DefaultLimits.MaxHeaderKey+1), []byte("v"))
	ms := []*mq.Message{
		{Topic: testTopic, Body: []byte("first")},
		bad,
		{Topic: testTopic, Body: []byte("third")},
	}
	err := batch.PublishBatch(r.ctx, ms, nil)
	// The whole batch is refused before any I/O, because check runs over
	// every message first: the specification puts the refusal before the write, and a
	// batch that published two of three and then refused would have written
	// half a caller's batch.
	if err == nil {
		t.Fatal("PublishBatch accepted a batch containing a message it cannot decode")
	}
	keys, _ := streams.NewKeys(testTopic, 1)
	if n := len(r.srv.Entries(keys.Stream(0))); n != 0 {
		t.Fatalf("the stream holds %d entries after a batch refused before I/O", n)
	}
	// Now a good batch, and a settlement batch over it.
	good := []*mq.Message{
		{Topic: testTopic, Body: []byte("a")},
		{Topic: testTopic, Body: []byte("b")},
		{Topic: testTopic, Body: []byte("c")},
	}
	if err := batch.PublishBatch(r.ctx, good, nil); err != nil {
		t.Fatalf("PublishBatch: %v", err)
	}
	dst := make([]mq.DriverDelivery, 3)
	fctx, cancel := context.WithTimeout(r.ctx, 2*time.Second)
	defer cancel()
	n, err := src.Fetch(fctx, dst)
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if n != 3 {
		t.Fatalf("Fetch returned %d of 3 published messages", n)
	}
	bs, ok := src.Settler().(mq.BatchSettler)
	if !ok {
		t.Fatal("the settler is not a BatchSettler; FeatureBatchSettle is advertised as native")
	}
	// Steal the middle one, so exactly one of three must fail and at exactly
	// the right index.
	pending := r.srv.Pending(keys.Stream(0), testSub)
	if len(pending) != 3 {
		t.Fatalf("pending = %d, want 3", len(pending))
	}
	stolen := streams.Unpack(dst[1].Info.Sequence)
	if !r.srv.Deliver(keys.Stream(0), testSub, "someone-else", stolen) {
		t.Fatal("hand-off failed")
	}
	ptrs := []*mq.DriverDelivery{&dst[0], &dst[1], &dst[2]}
	err = bs.AckBatch(r.ctx, ptrs)
	var berr *mq.BatchError
	if !errors.As(err, &berr) {
		t.Fatalf("AckBatch with one stolen delivery returned %v, want a *BatchError", err)
	}
	if len(berr.Errs) != 3 {
		t.Fatalf("the batch error holds %d results for 3 deliveries", len(berr.Errs))
	}
	if berr.Errs[0] != nil || berr.Errs[2] != nil {
		t.Fatalf("the batch error blames deliveries that settled: %v", berr.Errs)
	}
	if !errors.Is(berr.Errs[1], mq.ErrLeaseLost) {
		t.Fatalf("the stolen delivery's result is %v at index 1, want ErrLeaseLost", berr.Errs[1])
	}
}

// TestSweepRedeliversAnUnsettledDelivery is the mandatory reclaim loop at the
// SPI: a delivery whose handler died comes back.
//
// The sweep runs on a ticker in production; here the test drives the clock and
// the interval, which is what the fake clock is for.
func TestSweepRedeliversAnUnsettledDelivery(t *testing.T) {
	r := newRig(t, func(c *rd.Config) { c.ClaimInterval = 20 * time.Millisecond })
	sink, src := r.openPair(t)
	if err := sink.Publish(r.ctx, &mq.Message{Topic: testTopic, Body: []byte("x")}, nil); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	first := fetchOne(t, r.ctx, src)
	// The handler dies: the delivery is never settled, and the lease runs
	// out.
	r.srv.Advance(testLease + time.Second)
	second := fetchOne(t, r.ctx, src)
	if second.Gen == first.Gen {
		t.Fatalf("the redelivered message carries the same generation (%d) as the delivery it "+
			"superseded", second.Gen)
	}
	if second.Info.Attempt < 2 {
		t.Fatalf("the redelivered message reports attempt %d, want at least 2", second.Info.Attempt)
	}
	if !second.Info.Redelivered {
		t.Fatal("the redelivered message does not report Redelivered")
	}
	// The superseded delivery is no longer settleable.
	if err := src.Settler().Ack(r.ctx, &first); !errors.Is(err, mq.ErrLeaseLost) {
		t.Fatalf("Ack of the superseded delivery returned %v, want ErrLeaseLost", err)
	}
	if err := src.Settler().Ack(r.ctx, &second); err != nil {
		t.Fatalf("Ack of the current delivery: %v", err)
	}
}

// TestConfirmDurableIsRefusedWithoutTheCondition is the rule: a publisher
// asking for a boundary this profile cannot define fails at CONSTRUCTION.
//
// Both conditions are tested, because both can be absent and they are
// different failures: a 6.2 server has no WAITAOF at all, and a 7.4 server
// with `appendonly no` has the command and nothing for it to wait on.
func TestConfirmDurableIsRefusedWithoutTheCondition(t *testing.T) {
	t.Run("no-append-only-file", func(t *testing.T) {
		srv := fakestreams.New(fakestreams.Options{Version: "7.4.2", AOF: false})
		b, err := rd.BackendWithCommander(context.Background(), srv, driverkit.ClientOwned,
			rd.Config{AutoCreate: true, Clock: fakeClock{srv}})
		if err != nil {
			t.Fatalf("BackendWithCommander: %v", err)
		}
		defer func() { _ = b.Close() }()
		_, err = b.OpenSink(context.Background(), mq.SinkConfig{
			Topic: testTopic, Confirmation: mq.ConfirmDurable,
		})
		if !errors.Is(err, mq.ErrUnsupported) {
			t.Fatalf("OpenSink with ConfirmDurable on a server with no AOF returned %v, want ErrUnsupported", err)
		}
		caps := b.Capabilities()
		if caps.Has(mq.FeatureDurableConfirm) {
			t.Fatalf("the capability table claims a durable confirmation this server cannot provide "+
				"(level %v): Capabilities.Has would then answer true for a boundary OpenSink refuses",
				caps.Support(mq.FeatureDurableConfirm).Level)
		}
	})
	t.Run("waitaof-exists", func(t *testing.T) {
		srv := fakestreams.New(fakestreams.Options{Version: "7.4.2", AOF: true})
		b, err := rd.BackendWithCommander(context.Background(), srv, driverkit.ClientOwned,
			rd.Config{AutoCreate: true, Clock: fakeClock{srv}})
		if err != nil {
			t.Fatalf("BackendWithCommander: %v", err)
		}
		defer func() { _ = b.Close() }()
		sink, err := b.OpenSink(context.Background(), mq.SinkConfig{
			Topic: testTopic, Confirmation: mq.ConfirmDurable,
		})
		if err != nil {
			t.Fatalf("OpenSink with ConfirmDurable on a server with an AOF: %v", err)
		}
		defer func() { _ = sink.Close() }()
		if err := sink.Publish(context.Background(), &mq.Message{Topic: testTopic, Body: []byte("x")}, nil); err != nil {
			t.Fatalf("a durable publication: %v", err)
		}
		caps := b.Capabilities()
		if !caps.Has(mq.FeatureDurableConfirm) {
			t.Fatal("the capability table does not claim a durable confirmation this server can provide")
		}
		if sup := caps.Support(mq.FeatureDurableConfirm); sup.Level == mq.Conditional && sup.Note == "" {
			t.Fatal("a conditional claim with no condition tells a program nothing ")
		}
	})
}

// TestServerTooOldIsRefusedAtOpen is the version floor: XAUTOCLAIM is the
// reclaim loop, and without the loop nothing redelivers.
//
// Opening anyway would advertise at-least-once delivery on a server that
// cannot provide it, which is the one claim a message queue must not get
// wrong.
func TestServerTooOldIsRefusedAtOpen(t *testing.T) {
	srv := fakestreams.New(fakestreams.Options{Version: "6.0.20"})
	_, err := rd.BackendWithCommander(context.Background(), srv, driverkit.ClientOwned, rd.Config{})
	if !errors.Is(err, mq.ErrUnsupported) {
		t.Fatalf("opening against Redis 6.0 returned %v, want ErrUnsupported", err)
	}
	if err == nil || !contains(err.Error(), "XAUTOCLAIM") {
		t.Fatalf("the refusal does not say why: %v", err)
	}
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}

// TestAutoCreateOffRefusesAMissingStream is nothing creates topology
// behind an operator's back.
func TestAutoCreateOffRefusesAMissingStream(t *testing.T) {
	srv := fakestreams.New(fakestreams.Options{AOF: true})
	b, err := rd.BackendWithCommander(context.Background(), srv, driverkit.ClientOwned,
		rd.Config{AutoCreate: false, Clock: fakeClock{srv}})
	if err != nil {
		t.Fatalf("BackendWithCommander: %v", err)
	}
	defer func() { _ = b.Close() }()
	if _, err := b.OpenSink(context.Background(), mq.SinkConfig{Topic: testTopic}); err == nil {
		t.Fatal("OpenSink created a stream with AutoCreate off")
	}
	if _, err := b.OpenSource(context.Background(), mq.SourceConfig{
		Topic: testTopic, Subscription: testSub, Prefetch: 8,
	}); err == nil {
		t.Fatal("OpenSource created a consumer group with AutoCreate off")
	}
	// And the negative direction: with it on, both open.
	b2, err := rd.BackendWithCommander(context.Background(), srv, driverkit.ClientBorrowed,
		rd.Config{AutoCreate: true, Clock: fakeClock{srv}})
	if err != nil {
		t.Fatalf("BackendWithCommander: %v", err)
	}
	defer func() { _ = b2.Close() }()
	src, err := b2.OpenSource(context.Background(), mq.SourceConfig{
		Topic: testTopic, Subscription: testSub, Prefetch: 8,
	})
	if err != nil {
		t.Fatalf("OpenSource with AutoCreate on: %v", err)
	}
	_ = src.Close()
}

// TestCloseWakesAParkedFetch is the specification's "Close unblocks blocked operations"
// reaching this driver.
func TestCloseWakesAParkedFetch(t *testing.T) {
	r := newRig(t, func(c *rd.Config) { c.Block = 2 * time.Second })
	_, src := r.openPair(t)
	done := make(chan error, 1)
	go func() {
		dst := make([]mq.DriverDelivery, 1)
		_, err := src.Fetch(context.Background(), dst)
		done <- err
	}()
	// Give the fetch time to park at the server.
	time.Sleep(50 * time.Millisecond)
	if err := src.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	select {
	case err := <-done:
		if !errors.Is(err, mq.ErrClosed) && !errors.Is(err, context.Canceled) {
			t.Fatalf("the parked Fetch returned %v, want ErrClosed", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Close did not wake the parked Fetch")
	}
}

// TestPartitionRoutingIsStable is the per-key ordering requirement reduced
// to the one property the driver has to have: a key always goes to the same
// shard.
//
// A key that hashed differently in two publishers, or differently after a
// restart, would put one key's messages in two shards and break per-key order
// with nothing failing.
func TestPartitionRoutingIsStable(t *testing.T) {
	const shards = 4
	keys := [][]byte{[]byte("a"), []byte("order-4711"), {0, 1, 2}, nil, {}}
	first := make([]int, len(keys))
	for i, k := range keys {
		first[i] = rd.ShardOf(k, shards)
		if first[i] < 0 || first[i] >= shards {
			t.Fatalf("ShardOf(%q) = %d, outside [0,%d)", k, first[i], shards)
		}
	}
	for range 100 {
		for i, k := range keys {
			if got := rd.ShardOf(k, shards); got != first[i] {
				t.Fatalf("ShardOf(%q) returned %d then %d: a key that moves between shards breaks "+
					"per-key ordering with nothing failing", k, first[i], got)
			}
		}
	}
	// A keyless message goes to shard 0, which the specification allows because a message
	// with no key is unordered.
	if got := rd.ShardOf(nil, shards); got != 0 {
		t.Fatalf("ShardOf(nil) = %d, want 0", got)
	}
	// And the hash really does spread: a mapping that sent everything to one
	// shard would satisfy every assertion above.
	seen := map[int]bool{}
	for i := range 64 {
		seen[rd.ShardOf([]byte{byte(i)}, shards)] = true
	}
	if len(seen) < 2 {
		t.Fatalf("64 different keys reached %d shard(s) of %d: the mapping is not a hash", len(seen), shards)
	}
}

// TestPartitionOutOfRangeIsRefused: a publication naming a shard the topic
// does not have is an error and not a silent shard 0.
func TestPartitionOutOfRangeIsRefused(t *testing.T) {
	r := newRig(t, func(c *rd.Config) { c.Partitions = 2 })
	sink, _ := r.openPair(t)
	o := mq.PublishOptions{PartitionSet: true, Partition: 7}
	if err := sink.Publish(r.ctx, &mq.Message{Topic: testTopic, Body: []byte("x")}, &o); err == nil {
		t.Fatal("Publish accepted a partition this topic does not have")
	}
}

// TestRepeatedSweepsDoNotStrandABufferedDelivery is a
// delivery the sweep produced but Fetch has not handed out yet must not be
// superseded into unsettleability.
//
// The sweep runs on its own schedule, so a redelivery can sit in the source's
// buffer while the sweep runs again. A buffer that appended would then hold
// two generations of one entry and hand out the older first, whose settlement
// is refused with ErrLeaseLost — and to the caller that is a message that
// arrived and cannot be acknowledged.
//
// Against a real server this broke three scenarios
// (lease/expiry-redelivery, settle/stale-rejection,
// capability/AtLeastOnce/works) while every unit test passed, because the
// unit tests drove one sweep at a time.
func TestRepeatedSweepsDoNotStrandABufferedDelivery(t *testing.T) {
	r := newRig(t, func(c *rd.Config) { c.ClaimInterval = time.Hour })
	sink, src := r.openPair(t)
	if err := sink.Publish(r.ctx, &mq.Message{Topic: testTopic, Body: []byte("abandoned")}, nil); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	first := fetchOne(t, r.ctx, src)
	_ = first
	// The handler dies. Now several sweeps run before anything fetches, which
	// is what a source whose ClaimInterval is shorter than a slow handler's
	// run does all the time.
	for range 4 {
		r.srv.Advance(testLease + time.Second)
		if _, _, err := rd.SweepForTest(r.b, src); err != nil {
			t.Fatalf("sweep: %v", err)
		}
	}
	// The delivery the caller is handed must be settleable. Anything else is
	// a message that arrived and cannot be acknowledged.
	again := fetchOne(t, r.ctx, src)
	if string(again.Message.Body) != "abandoned" {
		t.Fatalf("the redelivery carries %q", again.Message.Body)
	}
	if err := src.Settler().Ack(r.ctx, &again); err != nil {
		t.Fatalf("the redelivery after four sweeps is not settleable: %v — a buffer holding two "+
			"generations of one entry hands out the superseded one first", err)
	}
	keys, _ := streams.NewKeys(testTopic, 1)
	if n := len(r.srv.Pending(keys.Stream(0), testSub)); n != 0 {
		t.Fatalf("the entry is still pending after the acknowledgement (%d)", n)
	}
}

// TestEmptyMessageIsPublishable is a message with NOTHING
// in it is still a message.
//
// XADD refuses an entry with no field-value pairs, and a message with a nil
// body, a nil key, no topic, no headers and no properties encodes to nothing
// — so the entry layout has to write something. The
// publish/roundtrip-bodies scenario sends exactly that message first, and it
// failed against a real server with "wrong number of arguments for 'xadd'
// command" while every unit test passed: the layout's own round-trip test
// never needed a command.
//
// The nil-versus-empty distinction has to survive it, which is why the marker
// is a bookkeeping field rather than an empty body.
func TestEmptyMessageIsPublishable(t *testing.T) {
	r := newRig(t)
	sink, src := r.openPair(t)
	for _, tc := range []struct {
		name string
		msg  mq.Message
	}{
		{"nothing-at-all", mq.Message{}},
		{"empty-body", mq.Message{Body: []byte{}}},
		{"empty-key-no-body", mq.Message{Key: []byte{}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			msg := tc.msg
			if err := sink.Publish(r.ctx, &msg, nil); err != nil {
				t.Fatalf("Publish of a message with nothing in it: %v — XADD refuses an entry "+
					"with no fields ", err)
			}
			d := fetchOne(t, r.ctx, src)
			if (d.Message.Body == nil) != (tc.msg.Body == nil) {
				t.Fatalf("the body's nil-ness did not survive: published nil=%v, received nil=%v",
					tc.msg.Body == nil, d.Message.Body == nil)
			}
			if (d.Message.Key == nil) != (tc.msg.Key == nil) {
				t.Fatalf("the key's nil-ness did not survive: published nil=%v, received nil=%v",
					tc.msg.Key == nil, d.Message.Key == nil)
			}
			if err := src.Settler().Ack(r.ctx, &d); err != nil {
				t.Fatalf("Ack: %v", err)
			}
		})
	}
}
