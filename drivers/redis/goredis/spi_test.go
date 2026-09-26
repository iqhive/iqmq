package goredis_test

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
	rd "github.com/iqhive/iqmq/drivers/redis"
	"github.com/iqhive/iqmq/drivers/redis/goredis"
	"github.com/iqhive/iqmq/internal/streams"
)

// TestPublishAndSettleRoundTripThroughGoRedis is the driver's basic shape over
// this client: a message goes in through the SPI, comes back out with its
// metadata intact, and settling it removes the pending entry.
func TestPublishAndSettleRoundTripThroughGoRedis(t *testing.T) {
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
	if !d.Info.HasSequence {
		t.Fatal("the delivery reports no sequence")
	}
	if d.Info.LeaseExpires.IsZero() {
		t.Fatal("the delivery reports no lease deadline")
	}
	if err := src.Settler().Ack(r.ctx, &d); err != nil {
		t.Fatalf("Ack: %v", err)
	}
	keys, err := streams.NewKeys(testTopic, 1)
	if err != nil {
		t.Fatalf("NewKeys: %v", err)
	}
	if p := r.fake.Pending(keys.Stream(0), testSub); len(p) != 0 {
		t.Fatalf("the pending list still holds %d rows after the Ack", len(p))
	}
}

// TestTheFieldLayoutRoundTripsEveryMessageThisClientAccepts is the specification's
// promise, re-proved over this client and over the wire.
//
// The promise is that a message the driver ACCEPTED comes back byte for byte,
// bounded by the decoder's own limits — a message published and then
// undecodable is a record that can be neither delivered nor settled, which is the disqualifier. It is re-proved here rather than inherited because
// this is where it could break for a reason `drivers/redis`'s own test cannot
// see: the field layout is a list of BINARY-SAFE strings, and the layer that
// has to keep them binary-safe is the client library. A client that sent a
// field as a simple string, trimmed it, or transcoded it would lose exactly
// the bytes the specification promises to return.
func TestTheFieldLayoutRoundTripsEveryMessageThisClientAccepts(t *testing.T) {
	limits := streams.DefaultLimits
	cases := []struct {
		name string
		msg  mq.Message
	}{
		{"nothing-at-all", mq.Message{}},
		{"empty-body", mq.Message{Body: []byte{}}},
		{"nil-key-empty-body", mq.Message{Key: nil, Body: []byte{}}},
		{"protocol-bytes-in-the-body", mq.Message{Body: []byte("a\r\n*3\r\n$4\r\nPING\r\nb")}},
		{"nul-and-high-bytes", mq.Message{Body: []byte{0, 1, 0x7f, 0x80, 0xff}, Key: []byte{0xff, 0}}},
		{"utf8-everywhere", mq.Message{Body: []byte("🙂 a b"), Key: []byte("клавиша"), ID: "идентификатор"}},
		{"id-at-the-limit", mq.Message{Body: []byte("x"), ID: strings.Repeat("i", limits.MaxID)}},
		{"header-key-at-the-limit", mq.Message{Body: []byte("x")}},
		{"a-header-value-with-crlf", mq.Message{Body: []byte("x")}},
		{"many-headers", mq.Message{Body: []byte("x")}},
		{"properties", mq.Message{
			Body: []byte("x"),
			Properties: mq.Properties{
				ContentType:     "application/x-thing",
				ContentEncoding: "gzip",
				CorrelationID:   "corr\x00id",
				ReplyTo:         "reply.to",
			},
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := newRig(t)
			sink, src := r.openPair(t)
			msg := tc.msg
			msg.Topic = testTopic
			switch tc.name {
			case "header-key-at-the-limit":
				msg.Headers.Add(strings.Repeat("k", limits.MaxHeaderKey), []byte("v"))
			case "a-header-value-with-crlf":
				msg.Headers.Add("h", []byte("one\r\ntwo\x00three"))
			case "many-headers":
				for i := range 16 {
					msg.Headers.Add("h"+string(rune('a'+i)), []byte{byte(i), '\r', '\n'})
				}
			}
			if err := sink.Publish(r.ctx, &msg, nil); err != nil {
				t.Fatalf("Publish: %v", err)
			}
			d := fetchOne(t, r.ctx, src)
			if (d.Message.Body == nil) != (msg.Body == nil) {
				t.Errorf("the body's nil-ness did not survive: published nil=%v, received nil=%v",
					msg.Body == nil, d.Message.Body == nil)
			}
			if !bytes.Equal(d.Message.Body, msg.Body) {
				t.Errorf("body = %q, want %q", d.Message.Body, msg.Body)
			}
			if (d.Message.Key == nil) != (msg.Key == nil) {
				t.Errorf("the key's nil-ness did not survive: published nil=%v, received nil=%v",
					msg.Key == nil, d.Message.Key == nil)
			}
			if !bytes.Equal(d.Message.Key, msg.Key) {
				t.Errorf("key = %q, want %q", d.Message.Key, msg.Key)
			}
			if d.Message.ID != msg.ID {
				t.Errorf("id = %q, want %q", d.Message.ID, msg.ID)
			}
			if d.Message.Properties != msg.Properties {
				t.Errorf("properties = %+v, want %+v", d.Message.Properties, msg.Properties)
			}
			if got, want := renderHeaders(&d.Message.Headers), renderHeaders(&msg.Headers); got != want {
				t.Errorf("headers did not round-trip:\n got %s\nwant %s", got, want)
			}
			if err := src.Settler().Ack(r.ctx, &d); err != nil {
				t.Errorf("Ack: %v", err)
			}
		})
	}
}

// TestPublishRefusesAMessageThisClientCouldSendAndTheDecoderCouldNot is the
// other half of the bound is the DECODER, and it is applied before
// any I/O in every metadata mode.
//
// A Redis stream entry can carry anything, and so can this client library, so
// nothing below the driver would refuse a header key one byte over the
// envelope's limit — it would be stored and would then break every Fetch on
// the subscription for ever.
func TestPublishRefusesAMessageThisClientCouldSendAndTheDecoderCouldNot(t *testing.T) {
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
			over := &mq.Message{Topic: testTopic, Body: []byte("x")}
			over.Headers.Add(strings.Repeat("k", streams.DefaultLimits.MaxHeaderKey+1), []byte("v"))
			if err := sink.Publish(r.ctx, over, nil); err == nil {
				t.Errorf("Publish accepted a header key one byte over the decode limit in %s mode", mode)
			}
			bad := &mq.Message{Topic: testTopic, Body: []byte("x")}
			bad.Headers.Add("\xff\xfe", []byte("v"))
			if err := sink.Publish(r.ctx, bad, nil); err == nil {
				t.Errorf("Publish accepted a non-UTF-8 header key in %s mode", mode)
			}
			if n := len(r.fake.Entries(keys.Stream(0))); n != 0 {
				t.Errorf("the stream holds %d entries after the refusals: the bound was applied "+
					"after the XADD rather than before it", n)
			}
		})
	}
}

// TestAckOfAStolenDeliveryIsRefused is the disqualifier over this client.
//
// `XACK key group id` removes the pending row whoever holds it, so a driver
// that sent one for a delivery another consumer has reclaimed acknowledges
// that consumer's work — and if that consumer then dies, the message is lost.
// The guard is a per-source generation table AND a Lua check on the pending
// row's consumer, and it is re-proved here because both halves travel over
// the client library: the generation table decides without a command, and the
// Lua check is an EVALSHA whose reply this adapter parses.
func TestAckOfAStolenDeliveryIsRefused(t *testing.T) {
	r := newRig(t)
	sink, src := r.openPair(t)
	if err := sink.Publish(r.ctx, &mq.Message{Topic: testTopic, Body: []byte("x")}, nil); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	d := fetchOne(t, r.ctx, src)
	keys, err := streams.NewKeys(testTopic, 1)
	if err != nil {
		t.Fatalf("NewKeys: %v", err)
	}
	pending := r.fake.Pending(keys.Stream(0), testSub)
	if len(pending) != 1 {
		t.Fatalf("pending = %d, want 1", len(pending))
	}
	// Another process claims it.
	if !r.fake.Deliver(keys.Stream(0), testSub, "someone-else", pending[0].ID) {
		t.Fatal("hand-off failed")
	}
	if err := src.Settler().Ack(r.ctx, &d); !errors.Is(err, mq.ErrLeaseLost) {
		t.Fatalf("Ack of a stolen delivery returned %v, want ErrLeaseLost", err)
	}
	after := r.fake.Pending(keys.Stream(0), testSub)
	if len(after) != 1 || after[0].Consumer != "someone-else" {
		t.Fatalf("the other consumer's entry was acknowledged anyway: pending=%v", after)
	}
}

// TestExtendDoesNotStealTheEntry is over this client:
// `XCLAIM min-idle 0` is a CLAIM, so an extension written as a bare XCLAIM
// takes the entry from whoever holds it and reports success.
func TestExtendDoesNotStealTheEntry(t *testing.T) {
	r := newRig(t)
	sink, src := r.openPair(t)
	if err := sink.Publish(r.ctx, &mq.Message{Topic: testTopic, Body: []byte("x")}, nil); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	d := fetchOne(t, r.ctx, src)
	keys, err := streams.NewKeys(testTopic, 1)
	if err != nil {
		t.Fatalf("NewKeys: %v", err)
	}
	pending := r.fake.Pending(keys.Stream(0), testSub)
	if !r.fake.Deliver(keys.Stream(0), testSub, "someone-else", pending[0].ID) {
		t.Fatal("hand-off failed")
	}
	ext, ok := src.Settler().(mq.DeadlineExtender)
	if !ok {
		t.Fatal("the settler is not a DeadlineExtender; FeatureExtendLease is advertised")
	}
	if err := ext.Extend(r.ctx, &d, time.Minute); !errors.Is(err, mq.ErrLeaseLost) {
		t.Fatalf("Extend of an entry another consumer holds returned %v, want ErrLeaseLost", err)
	}
	after := r.fake.Pending(keys.Stream(0), testSub)
	if len(after) != 1 || after[0].Consumer != "someone-else" {
		t.Fatalf("Extend stole the entry from the other consumer: pending=%v", after)
	}
}

// TestPipelinedResultsAreIndexAligned is the batch contract over the one
// piece of code in this module the rueidis driver shares none of.
//
// go-redis's pipeline returns its own command objects and its Exec returns the
// FIRST error among them, so an adapter that read Exec's error as the batch's
// would report one delivery's outcome for another: a batch of settlements
// where the first met NOGROUP would report all of them failed, and the core
// would retry settlements that already happened.
func TestPipelinedResultsAreIndexAligned(t *testing.T) {
	r := newRig(t)
	cmder := goredisCommander(t, r.srv.Addr())
	keys, err := streams.NewKeys("pipeline", 1)
	if err != nil {
		t.Fatalf("NewKeys: %v", err)
	}
	stream := keys.Stream(0)
	if _, err := cmder.Do(r.ctx, streams.Cmd{Tokens: []string{"XGROUP", "CREATE", stream, "g", "0", "MKSTREAM"}}); err != nil {
		t.Fatalf("setup: %v", err)
	}
	cmds := []streams.Cmd{
		{Tokens: []string{"XLEN", stream}, KeyStart: 1, KeyCount: 1},
		{Tokens: []string{"XACK"}},                                                                  // refused: wrong arity
		{Tokens: []string{"XADD", stream, "*", "b", "one"}, KeyStart: 1, KeyCount: 1},               // succeeds
		{Tokens: []string{"XREADGROUP", "GROUP", "g", "c", "COUNT", "1", "STREAMS", "nosuch", ">"}}, // refused: NOGROUP
		{Tokens: []string{"XLEN", stream}, KeyStart: 1, KeyCount: 1},                                // succeeds
	}
	results, err := cmder.DoMulti(r.ctx, cmds)
	if err != nil {
		t.Fatalf("DoMulti: %v", err)
	}
	if len(results) != len(cmds) {
		t.Fatalf("DoMulti returned %d results for %d commands", len(results), len(cmds))
	}
	wantErr := []bool{false, true, false, true, false}
	for i, res := range results {
		if (res.Err != nil) != wantErr[i] {
			t.Errorf("result %d (%v): err = %v, want err=%v", i, cmds[i].Tokens[0], res.Err, wantErr[i])
		}
	}
	if n, ok := results[0].Value.Int64(); !ok || n != 0 {
		t.Errorf("result 0 (XLEN before the XADD) = %v, want int(0)", results[0].Value)
	}
	if n, ok := results[4].Value.Int64(); !ok || n != 1 {
		t.Errorf("result 4 (XLEN after the XADD) = %v, want int(1): the succeeding commands behind "+
			"a failing one did not run, or their results were not aligned", results[4].Value)
	}
	if _, ok := results[2].Value.Str(); !ok {
		t.Errorf("result 2 (XADD) = %v, want the new entry's identifier", results[2].Value)
	}
}

// TestAnEmptyPipelineIsNotARoundTrip holds DoMulti's own edge: go-redis's
// Exec returns (nil, nil) for an empty pipeline, which must not be read as a
// failure and must not be a command.
func TestAnEmptyPipelineIsNotARoundTrip(t *testing.T) {
	r := newRig(t)
	cmder := goredisCommander(t, r.srv.Addr())
	results, err := cmder.DoMulti(r.ctx, nil)
	if err != nil {
		t.Fatalf("DoMulti(nil): %v", err)
	}
	if len(results) != 0 {
		t.Fatalf("DoMulti(nil) returned %d results", len(results))
	}
}

// TestSweepRedeliversAnUnsettledDelivery is the mandatory reclaim loop over
// this client. Redis grants no lease, so this loop is the only thing that
// redelivers: without it the driver's at-least-once claim is false.
func TestSweepRedeliversAnUnsettledDelivery(t *testing.T) {
	r := newRig(t)
	sink, src := r.openPair(t)
	if err := sink.Publish(r.ctx, &mq.Message{Topic: testTopic, Body: []byte("x")}, nil); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	first := fetchOne(t, r.ctx, src)
	// The holder never settles, and the entry outlives its lease.
	r.fake.Advance(2 * testLease)
	claimed, live, err := rd.SweepForTest(r.b, src)
	if err != nil {
		t.Fatalf("SweepForTest: %v", err)
	}
	if claimed != 1 {
		t.Fatalf("the sweep claimed %d entries, want 1 (live=%d)", claimed, live)
	}
	again := fetchOne(t, r.ctx, src)
	if again.Info.Attempt < 2 {
		t.Errorf("the redelivery reports attempt %d, want at least 2", again.Info.Attempt)
	}
	if !again.Info.Redelivered {
		t.Error("the redelivery does not report Redelivered")
	}
	// The superseded holder must not be able to settle the new holder's row.
	if err := src.Settler().Ack(r.ctx, &first); !errors.Is(err, mq.ErrLeaseLost) {
		t.Errorf("the superseded delivery's Ack returned %v, want ErrLeaseLost", err)
	}
	if err := src.Settler().Ack(r.ctx, &again); err != nil {
		t.Errorf("the current delivery's Ack returned %v", err)
	}
}

// TestCloseWakesAParkedFetch is a Fetch parked in XREADGROUP BLOCK
// must be woken rather than held for ever.
//
// The bound is ONE BLOCK and not zero, and it is worth being exact about
// because the first version of this test asked for zero and failed. Neither
// client aborts an in-flight blocking read when the source closes: the fetch
// loop returns ErrClosed when the block expires and it looks again, so
// Config.Block is what bounds Close's latency (its own documentation says so).
// The reason the first version passed before the read timeout was fixed is
// worse than the reason it failed after: go-redis's socket read was timing out
// inside the block, and the fetch was being "woken" by a transport failure.
func TestCloseWakesAParkedFetch(t *testing.T) {
	r := newRig(t, func(c *rd.Config) { c.Block = 500 * time.Millisecond })
	_, src := r.openPair(t)
	started := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		close(started)
		dst := make([]mq.DriverDelivery, 1)
		_, err := src.Fetch(context.Background(), dst)
		done <- err
	}()
	<-started
	// Long enough for the fetch to be parked at the server rather than
	// between two reads.
	time.Sleep(100 * time.Millisecond)
	if err := src.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	select {
	case err := <-done:
		if !errors.Is(err, mq.ErrClosed) && !errors.Is(err, context.Canceled) {
			t.Fatalf("the parked Fetch returned %v, want ErrClosed", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Close did not wake the parked Fetch within several blocks")
	}
}

// TestCapabilitiesAreTheSameUnderBothClients is what "two clients behind one
// contract" has to mean at the capability table: every claim is a
// statement about Redis Streams and about this driver, and none of them is a
// statement about a client library. A table that differed would make the
// generated matrix carry two answers for one broker.
func TestCapabilitiesAreTheSameUnderBothClients(t *testing.T) {
	r := newRig(t)
	rueidisRig := newRueidisRig(t)
	got, want := r.b.Capabilities(), rueidisRig.Capabilities()
	for i := range mq.FeatureCount {
		f := mq.Feature(i)
		if g, w := got.Support(f), want.Support(f); g != w {
			t.Errorf("%s: goredis claims %+v, rueidis claims %+v", f, g, w)
		}
	}
	if got.Limits() != want.Limits() {
		t.Errorf("the two clients report different limits:\n goredis %+v\n rueidis %+v", got.Limits(), want.Limits())
	}
}

// renderHeaders writes a header set out in order, values included, so that a
// difference in one byte of one value fails the comparison.
func renderHeaders(h *mq.Headers) string {
	out := ""
	for name, val := range h.All() {
		out += quote(name) + "=" + quote(string(val)) + " "
	}
	return out
}

// TestABlockingReadWaitsOutTheBlockRatherThanTimingOut is the
// client-library defect this module was written to find, and the one its unit
// tests almost could not see.
//
// `XREADGROUP … BLOCK n` parks at the SERVER, so the socket read waiting for
// its reply legitimately takes n. go-redis's default read timeout is five
// seconds and the driver's default BLOCK is five seconds, so the two raced:
// every idle fetch came back as `read tcp …: i/o timeout` — a fetch reported
// as a transport failure — and three scenarios failed against a
// container while every test in this package passed, because the in-process
// server answers instantly and only a BLOCK with nothing to return waits at
// all.
//
// The BLOCK here is deliberately longer than go-redis's own default read
// timeout, and the test therefore takes that long. A shorter one would pass
// whether or not the option is set, and a check that cannot fail is exactly
// what let this reach a container run.
func TestABlockingReadWaitsOutTheBlockRatherThanTimingOut(t *testing.T) {
	const block = 6 * time.Second // > go-redis's 5s default ReadTimeout
	r := newRig(t, func(c *rd.Config) { c.Block = block })
	cmder := goredisCommander(t, r.srv.Addr())
	keys, err := streams.NewKeys("blocking", 1)
	if err != nil {
		t.Fatalf("NewKeys: %v", err)
	}
	stream := keys.Stream(0)
	group, err := streams.Group("workers")
	if err != nil {
		t.Fatalf("Group: %v", err)
	}
	if _, err := cmder.Do(r.ctx, streams.XGroupCreate(stream, group, streams.IDZero, true)); err != nil {
		t.Fatalf("XGROUP CREATE: %v", err)
	}
	read, err := streams.XReadGroup(streams.XReadGroupArgs{
		Group: group, Consumer: "c1", Keys: []string{stream},
		IDs: []string{streams.IDNew}, Count: 8, Block: block,
	})
	if err != nil {
		t.Fatalf("XReadGroup: %v", err)
	}
	if !read.Blocking {
		t.Fatal("streams.XReadGroup did not mark the command Blocking, so this test proves nothing")
	}
	start := time.Now()
	v, err := cmder.Do(r.ctx, read)
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("a BLOCK of %v failed after %v: %v\nthe socket read timed out inside the block, so "+
			"a fetch with nothing to return is reported as a transport failure", block, elapsed, err)
	}
	if !v.IsNil() {
		t.Fatalf("the read returned %v from an empty stream, want the null reply", v)
	}
	if elapsed < block/2 {
		t.Errorf("the read returned after %v, less than half the %v block: it did not park at the "+
			"server at all, so this test is not exercising the timeout it exists for", elapsed, block)
	}
}

// TestTheReadTimeoutClearsTheBlock is the same property as a pure check on the
// options, so that a change to either default is caught without waiting six
// seconds for it.
func TestTheReadTimeoutClearsTheBlock(t *testing.T) {
	for _, block := range []time.Duration{0, time.Second, 30 * time.Second, 5 * time.Minute} {
		opt := goredis.UniversalOptions(rd.Config{Block: block})
		want := block
		if want <= 0 {
			want = rd.DefaultBlock
		}
		if opt.ReadTimeout <= want {
			t.Errorf("Block %v gives ReadTimeout %v, which does not clear the block: an idle fetch "+
				"would be reported as an i/o timeout", block, opt.ReadTimeout)
		}
	}
}
