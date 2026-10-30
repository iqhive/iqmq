package main

import (
	"context"
	"encoding/json"
	"errors"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit"
	"github.com/iqhive/iqmq/dsn"
	"github.com/iqhive/iqmq/memory"
)

// The two schemes the end-to-end bridge test registers, over two SEPARATE
// memory brokers. Two brokers and not one: a bridge whose ends were the same
// engine would not have a seam, and the seam is the whole subject.
const (
	srcScheme = "bsrc"
	dstScheme = "bdst"
)

// newBridgeHarness registers a scheme over each of two memory brokers.
func newBridgeHarness(t *testing.T) (*harness, *memory.Broker, *memory.Broker) {
	t.Helper()
	mk := func(name string) *memory.Broker {
		b, err := memory.NewBroker(memory.Config{AutoCreate: false})
		if err != nil {
			t.Fatalf("memory.NewBroker(%s): %v", name, err)
		}
		t.Cleanup(func() { _ = b.Close() })
		return b
	}
	source, dest := mk("source"), mk("destination")
	reg := dsn.NewRegistry()
	for scheme, broker := range map[string]*memory.Broker{srcScheme: source, dstScheme: dest} {
		b := broker
		if err := reg.Register(scheme, func(ctx context.Context, _ string) (*mq.Client, error) {
			return memory.OpenWithClient(ctx, b, memory.ClientBorrowed)
		}); err != nil {
			t.Fatalf("registering %q: %v", scheme, err)
		}
	}
	return harnessWith(t, reg, nil), source, dest
}

// clientOver opens a client over one broker, for the test's own publishing and
// consuming. The CLI is what is under test; the library is how the test sets
// the broker up and reads the result, so neither half is checking itself.
func clientOver(t *testing.T, b *memory.Broker) *mq.Client {
	t.Helper()
	c, err := memory.OpenWithClient(context.Background(), b, memory.ClientBorrowed)
	if err != nil {
		t.Fatalf("opening a client: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

// ensure creates a topic, and a subscription on it when sub is not empty.
func ensure(t *testing.T, b *memory.Broker, topic, sub string) {
	t.Helper()
	admin, err := clientOver(t, b).Admin()
	if err != nil {
		t.Fatalf("Admin: %v", err)
	}
	if err := admin.EnsureTopic(context.Background(), mq.TopicSpec{Name: topic}); err != nil {
		t.Fatalf("EnsureTopic(%q): %v", topic, err)
	}
	if sub == "" {
		return
	}
	if err := admin.EnsureSubscription(context.Background(),
		mq.SubscriptionSpec{Topic: topic, Name: sub}); err != nil {
		t.Fatalf("EnsureSubscription(%q/%q): %v", topic, sub, err)
	}
}

// publish puts messages into a broker with the library.
func publish(t *testing.T, b *memory.Broker, topic string, msgs ...*mq.Message) {
	t.Helper()
	pub, err := clientOver(t, b).Publisher(context.Background(), mq.PublisherConfig{Topic: topic})
	if err != nil {
		t.Fatalf("Publisher(%q): %v", topic, err)
	}
	defer func() { _ = pub.Close() }()
	for _, m := range msgs {
		if err := pub.Publish(context.Background(), m); err != nil {
			t.Fatalf("Publish: %v", err)
		}
	}
	if err := pub.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}
}

// drainWait is how long drain waits for a message that may not be there.
//
// It is a real wait and not a fake clock because the thing being waited on is
// a driver's Receive, which blocks until a message arrives: the question drain
// asks is "is the destination EMPTY", and the only honest way to ask it is to
// wait a little and find out. It is short, and it is only ever reached by a
// test that expects fewer messages than it asked for.
const drainWait = 2 * time.Second

// drain consumes up to want messages from a subscription and acknowledges
// them, returning what it saw. It returns early rather than blocking when the
// subscription holds fewer.
//
// The bound is the correction to this helper's first version, which called
// Receive with a background context: every test that expected the destination
// to be EMPTY -- the dry-run test above all -- then blocked until the whole
// package timed out.
//
//	panic: test timed out after 2m0s
//	running tests:
//	  TestBridgeDryRunPublishesNothingAndKeepsTheSource (2m0s)
func drain(t *testing.T, b *memory.Broker, topic, sub string, want int) []*mq.Message {
	t.Helper()
	con, err := clientOver(t, b).Consumer(context.Background(),
		mq.ConsumerConfig{Topic: topic, Subscription: sub})
	if err != nil {
		t.Fatalf("Consumer(%q/%q): %v", topic, sub, err)
	}
	defer func() { _ = con.Close() }()
	out := make([]*mq.Message, 0, want)
	dst := make([]mq.Delivery, want)
	for len(out) < want {
		ctx, cancel := context.WithTimeout(context.Background(), drainWait)
		n, rerr := con.Receive(ctx, dst)
		cancel()
		for i := range n {
			out = append(out, dst[i].CloneMessage())
			if err := dst[i].Ack(); err != nil {
				t.Fatalf("Ack: %v", err)
			}
		}
		if rerr != nil || n == 0 {
			break
		}
	}
	return out
}

// depth is what a subscription still holds, read through the Admin surface.
func depth(t *testing.T, b *memory.Broker, topic, sub string) int64 {
	t.Helper()
	admin, err := clientOver(t, b).Admin()
	if err != nil {
		t.Fatalf("Admin: %v", err)
	}
	st, err := admin.Stats(context.Background(), topic, sub)
	if err != nil {
		t.Fatalf("Stats(%q/%q): %v", topic, sub, err)
	}
	if !st.Depth.Known {
		t.Fatal("the memory driver does not report a depth, so this test proves nothing")
	}
	return st.Depth.Value
}

// TestBridgeMovesMessagesBetweenTwoBrokers is the end-to-end arm: two real
// engines, the real CLI, and the messages read back out of the destination.
func TestBridgeMovesMessagesBetweenTwoBrokers(t *testing.T) {
	h, source, dest := newBridgeHarness(t)
	ensure(t, source, "orders", "bridge")
	ensure(t, dest, "mirror", "reader")

	m := &mq.Message{Body: []byte("one"), Key: []byte("cust-7")}
	m.Headers.Add("trace", []byte("abc"))
	publish(t, source, "orders", m, &mq.Message{Body: []byte("two")})

	code := h.run("bridge", "--max", "2", srcScheme+"://", "orders", "bridge",
		dstScheme+"://", "mirror")
	if code != exitOK {
		t.Fatalf("bridge = %d, want %d\nstdout:\n%s\nstderr:\n%s",
			code, exitOK, h.stdout(), h.stderr())
	}
	got := drain(t, dest, "mirror", "reader", 2)
	if len(got) != 2 {
		t.Fatalf("the destination holds %d message(s), want 2", len(got))
	}
	if string(got[0].Body) != "one" || string(got[1].Body) != "two" {
		t.Errorf("the destination holds %q and %q, want \"one\" and \"two\"",
			got[0].Body, got[1].Body)
	}
	if string(got[0].Key) != "cust-7" {
		t.Errorf("the key was not carried: %q", got[0].Key)
	}
	if v, ok := got[0].Headers.Get("trace"); !ok || string(v) != "abc" {
		t.Errorf("the header was not carried: %q (present=%v)", v, ok)
	}
	if d := depth(t, source, "orders", "bridge"); d != 0 {
		t.Errorf("the source subscription still holds %d message(s); every one moved", d)
	}
	if !strings.Contains(h.stdout(), "moved 2") {
		t.Errorf("the summary does not report two moved:\n%s", h.stdout())
	}
}

// TestBridgeDropsHeadersAndKeyWhenAsked covers the other half of --headers and
// --key, so "pass" is a choice rather than the only thing the code can do.
func TestBridgeDropsHeadersAndKeyWhenAsked(t *testing.T) {
	h, source, dest := newBridgeHarness(t)
	ensure(t, source, "orders", "bridge")
	ensure(t, dest, "mirror", "reader")
	m := &mq.Message{Body: []byte("one"), Key: []byte("cust-7")}
	m.Headers.Add("authorization", []byte("Bearer xyz"))
	publish(t, source, "orders", m)

	if code := h.run("bridge", "--max", "1", "--headers", "drop", "--key", "drop",
		srcScheme+"://", "orders", "bridge", dstScheme+"://", "mirror"); code != exitOK {
		t.Fatalf("bridge = %d\nstderr:\n%s", code, h.stderr())
	}
	got := drain(t, dest, "mirror", "reader", 1)
	if len(got) != 1 {
		t.Fatalf("the destination holds %d message(s), want 1", len(got))
	}
	if got[0].Headers.Len() != 0 {
		t.Errorf("--headers drop carried %d header(s)", got[0].Headers.Len())
	}
	if len(got[0].Key) != 0 {
		t.Errorf("--key drop carried the key %q", got[0].Key)
	}
	if string(got[0].Body) != "one" {
		t.Errorf("the body was changed: %q", got[0].Body)
	}
}

// TestBridgeDryRunPublishesNothingAndKeepsTheSource: --dry-run is the probe,
// and a probe that consumed the source would be worse than no probe.
func TestBridgeDryRunPublishesNothingAndKeepsTheSource(t *testing.T) {
	h, source, dest := newBridgeHarness(t)
	ensure(t, source, "orders", "bridge")
	ensure(t, dest, "mirror", "reader")
	publish(t, source, "orders", &mq.Message{Body: []byte("one")})

	if code := h.run("bridge", "--max", "1", "--dry-run",
		srcScheme+"://", "orders", "bridge", dstScheme+"://", "mirror"); code != exitOK {
		t.Fatalf("bridge --dry-run = %d\nstderr:\n%s", code, h.stderr())
	}
	if n := len(drain(t, dest, "mirror", "reader", 1)); n != 0 {
		t.Errorf("--dry-run published %d message(s)", n)
	}
	if d := depth(t, source, "orders", "bridge"); d != 1 {
		t.Errorf("the source holds %d message(s) after a dry run, want 1", d)
	}
	if !strings.Contains(h.stdout(), "moved 0") {
		t.Errorf("the summary claims a dry run moved something:\n%s", h.stdout())
	}
}

// TestBridgeDryRunRefusesADestructiveOnStop: a dry run that acked or rejected
// what it received would leave the message on neither broker.
func TestBridgeDryRunRefusesADestructiveOnStop(t *testing.T) {
	for _, action := range []string{"ack", "reject"} {
		t.Run(action, func(t *testing.T) {
			h, source, dest := newBridgeHarness(t)
			ensure(t, source, "orders", "bridge")
			ensure(t, dest, "mirror", "reader")
			publish(t, source, "orders", &mq.Message{Body: []byte("one")})

			if code := h.run("bridge", "--max", "1", "--dry-run", "--on-stop", action,
				srcScheme+"://", "orders", "bridge", dstScheme+"://", "mirror"); code != exitUsage {
				t.Fatalf("bridge --dry-run --on-stop %s = %d, want %d\nstderr:\n%s",
					action, code, exitUsage, h.stderr())
			}
			if d := depth(t, source, "orders", "bridge"); d != 1 {
				t.Errorf("the source holds %d message(s) after a refused dry run, want 1", d)
			}
		})
	}
}

// TestBridgeMaxReceivesNoMoreThanItMoves: --max N with --on-stop ack used to
// receive a whole batch, move N and ack the rest unpublished.
func TestBridgeMaxReceivesNoMoreThanItMoves(t *testing.T) {
	h, source, dest := newBridgeHarness(t)
	ensure(t, source, "orders", "bridge")
	ensure(t, dest, "mirror", "reader")
	publish(t, source, "orders",
		&mq.Message{Body: []byte("one")}, &mq.Message{Body: []byte("two")},
		&mq.Message{Body: []byte("three")})

	if code := h.run("bridge", "--max", "1", "--on-stop", "ack",
		srcScheme+"://", "orders", "bridge", dstScheme+"://", "mirror"); code != exitOK {
		t.Fatalf("bridge = %d\nstderr:\n%s", code, h.stderr())
	}
	if n := len(drain(t, dest, "mirror", "reader", 3)); n != 1 {
		t.Errorf("the destination holds %d message(s), want 1", n)
	}
	if d := depth(t, source, "orders", "bridge"); d != 2 {
		t.Errorf("the source holds %d message(s) after moving one of three, want 2", d)
	}
}

// TestBridgeDryRunOnStopNoneStopsAtAFullPrefetchWindow: a dry run with
// --on-stop none leaves every delivery unsettled, and each one holds a
// prefetch slot until its lease expires. The loop fetched again anyway, so on a
// broker that enforces the window (RabbitMQ's basic.qos) the probe waited with
// no stop reason until --idle, --timeout or Ctrl-C, and the memory broker,
// which does not enforce it, delivered past it. sub --settle none stops at the
// window and says so, and a dry run now does the same.
//
// --idle bounds the failure only: a run that stops at the window never waits.
func TestBridgeDryRunOnStopNoneStopsAtAFullPrefetchWindow(t *testing.T) {
	h, source, dest := newBridgeHarness(t)
	ensure(t, source, "orders", "bridge")
	ensure(t, dest, "mirror", "reader")
	bodies := []string{"a", "b", "c", "d", "e", "f"}
	msgs := make([]*mq.Message, 0, len(bodies))
	for _, b := range bodies {
		msgs = append(msgs, &mq.Message{Body: []byte(b)})
	}
	publish(t, source, "orders", msgs...)

	code := h.run("bridge", "--dry-run", "--on-stop", "none", "--prefetch", "4", "--idle", "2s",
		srcScheme+"://", "orders", "bridge", dstScheme+"://", "mirror")
	if code != exitOK {
		t.Fatalf("bridge --dry-run --on-stop none = %d\nstdout:\n%s\nstderr:\n%s",
			code, h.stdout(), h.stderr())
	}
	for _, want := range []string{"received 4", "returned 4", "stopped: the prefetch window is full"} {
		if !strings.Contains(h.stdout(), want) {
			t.Errorf("bridge --dry-run --on-stop none --prefetch 4 does not report %q:\n%s", want, h.stdout())
		}
	}
}

// TestBridgeDryRunStopsAtMaxWhenMaxEqualsTheWindow: when --max and the window
// are reached by the same delivery, the run stopped because the caller asked
// for that many, and sub says so. The dry run named the window, and told the
// caller to raise --prefetch for a stop --max had chosen.
func TestBridgeDryRunStopsAtMaxWhenMaxEqualsTheWindow(t *testing.T) {
	h, source, dest := newBridgeHarness(t)
	ensure(t, source, "orders", "bridge")
	ensure(t, dest, "mirror", "reader")
	bodies := []string{"a", "b", "c", "d", "e", "f"}
	msgs := make([]*mq.Message, 0, len(bodies))
	for _, b := range bodies {
		msgs = append(msgs, &mq.Message{Body: []byte(b)})
	}
	publish(t, source, "orders", msgs...)

	code := h.run("bridge", "--dry-run", "--on-stop", "none", "--prefetch", "4", "--max", "4",
		srcScheme+"://", "orders", "bridge", dstScheme+"://", "mirror")
	if code != exitOK {
		t.Fatalf("bridge = %d\nstdout:\n%s\nstderr:\n%s", code, h.stdout(), h.stderr())
	}
	for _, want := range []string{"received 4", "stopped: the --max count was reached"} {
		if !strings.Contains(h.stdout(), want) {
			t.Errorf("bridge --dry-run --on-stop none --prefetch 4 --max 4 does not report %q:\n%s",
				want, h.stdout())
		}
	}
}

// TestBridgeDryRunFetchesNoMoreThanTheWindowHasLeft: a source may answer a
// Receive with fewer deliveries than it was asked for. The next Receive asks
// only for what the window has left, so a dry run with --on-stop none never
// holds more unsettled deliveries than the window. This source hands out
// three at a time against a window of four.
func TestBridgeDryRunFetchesNoMoreThanTheWindowHasLeft(t *testing.T) {
	log := &seamLog{}
	source := &fakeSource{
		log: log, bodies: []string{"a", "b", "c", "d", "e", "f"}, perFetch: 3,
		settler: &fakeSettler{log: log}, closed: make(chan struct{}),
	}
	src := &seamBackend{
		caps:   seamCaps(t, mq.FeatureNack, mq.FeatureAtLeastOnce, mq.FeatureExplicitAck),
		source: source,
	}
	dst := &seamBackend{caps: seamCaps(t, mq.FeaturePublisherConfirm), sink: &fakeSink{log: log}}
	reg := dsn.NewRegistry()
	for scheme, b := range map[string]*seamBackend{"seamsrc": src, "seamdst": dst} {
		backend := b
		if err := reg.Register(scheme, func(context.Context, string) (*mq.Client, error) {
			return mq.NewClient(backend), nil
		}); err != nil {
			t.Fatalf("registering %q: %v", scheme, err)
		}
	}
	h := harnessWith(t, reg, nil)
	code := h.run("bridge", "--dry-run", "--on-stop", "none", "--prefetch", "4", "-o", "json",
		"seamsrc://", "orders", "bridge", "seamdst://", "mirror")
	if code != exitOK {
		t.Fatalf("bridge = %d\nstdout:\n%s\nstderr:\n%s", code, h.stdout(), h.stderr())
	}
	var rep bridgeReport
	if err := json.Unmarshal([]byte(h.stdout()), &rep); err != nil {
		t.Fatalf("the JSON form does not parse: %v\n%s", err, h.stdout())
	}
	if r := &rep.Result; r.Received != 4 || r.Returned != 4 || !strings.Contains(r.StopReason, "prefetch window is full") {
		t.Errorf("received %d, returned %d, stopped %q; want 4, 4 and a full window",
			r.Received, r.Returned, r.StopReason)
	}
}

// TestBridgeStatesItsSettlement: the specification forbids an unqualified guarantee, and a
// bridge is exactly where a reader would infer one from silence.
func TestBridgeStatesItsSettlement(t *testing.T) {
	h, source, dest := newBridgeHarness(t)
	ensure(t, source, "orders", "bridge")
	ensure(t, dest, "mirror", "reader")
	publish(t, source, "orders", &mq.Message{Body: []byte("one")})

	if code := h.run("bridge", "--max", "1", srcScheme+"://", "orders", "bridge",
		dstScheme+"://", "mirror"); code != exitOK {
		t.Fatalf("bridge = %d\nstderr:\n%s", code, h.stderr())
	}
	for _, want := range []string{"at-least-once", "duplicates possible on the destination"} {
		if !strings.Contains(h.stdout(), want) {
			t.Errorf("the run does not state %q:\n%s", want, h.stdout())
		}
	}

	// And in the machine-readable form, where a tool reads it.
	h2 := harnessWith(t, h.env.registry, nil)
	publish(t, source, "orders", &mq.Message{Body: []byte("two")})
	if code := h2.run("bridge", "--max", "1", "-o", "json", srcScheme+"://", "orders",
		"bridge", dstScheme+"://", "mirror"); code != exitOK {
		t.Fatalf("bridge -o json = %d\nstderr:\n%s", code, h2.stderr())
	}
	var rep bridgeReport
	if err := json.Unmarshal([]byte(h2.stdout()), &rep); err != nil {
		t.Fatalf("the JSON form does not parse: %v\n%s", err, h2.stdout())
	}
	if !strings.Contains(rep.Settlement, "at-least-once") {
		t.Errorf("the JSON settlement statement is %q", rep.Settlement)
	}
	if rep.Result.Moved != 1 {
		t.Errorf("moved = %d, want 1", rep.Result.Moved)
	}
	if strings.Contains(rep.Source.DSN, "?") && !strings.Contains(rep.Source.DSN, "not shown") {
		t.Errorf("the source DSN carries a rendered parameter: %q", rep.Source.DSN)
	}
}

// ---------------------------------------------------------------------------
// The fake pair: a source and a sink that record the ORDER of what happened.
//
// The ordering is the bridge's whole contract and no real broker can show it:
// against two memory engines, "published then acked" and "acked then
// published" both end with the message in the destination and gone from the
// source. Only a recorder that sees both sides in one sequence can tell them
// apart, which is why these fakes exist and why the two failure injections
// live here too.

// seamLog is the shared, ordered record of what both ends were asked to do.
type seamLog struct {
	mu   sync.Mutex
	rows []string
}

// add appends one event.
func (l *seamLog) add(s string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.rows = append(l.rows, s)
}

// events returns a copy of the sequence.
func (l *seamLog) events() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]string, len(l.rows))
	copy(out, l.rows)
	return out
}

// fakeSink records every publication and can refuse, either at Publish or at
// Flush.
//
// The Flush half is what makes the CONFIRMATION half of the bridge's
// ordering observable. A sink that implements [mq.FlushSink] is a sink that
// buffers, so Publish returning nil means "admitted" and not "confirmed"
// -- and a bridge that settled the source on an admission would lose
// every message the buffer then failed to send.
type fakeSink struct {
	log       *seamLog
	failOn    int // the 1-based publication that fails; 0 never fails
	failAll   bool
	failErr   error // what a refusal answers; nil is "the destination is full"
	flushErr  error
	published int
}

// Flush is [mq.FlushSink]: it records and maybe refuses.
func (s *fakeSink) Flush(context.Context) error {
	if s.flushErr != nil {
		s.log.add("flush-refused")
		return s.flushErr
	}
	s.log.add("flush")
	return nil
}

// Publish records and maybe refuses.
func (s *fakeSink) Publish(_ context.Context, m *mq.Message, _ *mq.PublishOptions) error {
	s.published++
	if s.failAll || s.failOn == s.published {
		s.log.add("publish-refused " + string(m.Body))
		if s.failErr != nil {
			return s.failErr
		}
		return errors.New("the destination is full")
	}
	s.log.add("publish " + string(m.Body))
	return nil
}

// Close releases nothing.
func (s *fakeSink) Close() error { return nil }

// fakeSettler records every settlement and can refuse the acks.
type fakeSettler struct {
	log    *seamLog
	ackErr error
}

// Ack records and maybe refuses.
func (s *fakeSettler) Ack(_ context.Context, d *mq.DriverDelivery) error {
	if s.ackErr != nil {
		s.log.add("ack-refused " + string(d.Message.Body))
		return s.ackErr
	}
	s.log.add("ack " + string(d.Message.Body))
	return nil
}

// Nack records.
func (s *fakeSettler) Nack(_ context.Context, d *mq.DriverDelivery, _ mq.NackOptions) error {
	s.log.add("nack " + string(d.Message.Body))
	return nil
}

// fakeSource hands out a fixed list of messages once and then blocks until the
// context is done, which is what a real source does when the broker has
// nothing.
type fakeSource struct {
	log      *seamLog
	bodies   []string
	next     int
	settler  *fakeSettler
	closed   chan struct{}
	closeOne sync.Once
	// stripIDs hands out deliveries with no message id, which is what a
	// broker with no message identity gives a bridge.
	stripIDs bool
	// perFetch caps what one Fetch hands out, as a broker that delivers in
	// smaller batches than it was asked for does. 0 is no cap.
	perFetch int
}

// Fetch fills dst with what is left.
func (s *fakeSource) Fetch(ctx context.Context, dst []mq.DriverDelivery) (int, error) {
	if s.next >= len(s.bodies) {
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-s.closed:
			return 0, mq.ErrClosed
		}
	}
	limit := len(dst)
	if s.perFetch > 0 {
		limit = min(limit, s.perFetch)
	}
	n := 0
	for n < limit && s.next < len(s.bodies) {
		body := s.bodies[s.next]
		id := "id-" + body
		if s.stripIDs {
			id = ""
		}
		dst[n] = mq.DriverDelivery{
			Message: &mq.Message{Topic: "orders", Body: []byte(body), ID: id},
			Info:    mq.DeliveryInfo{Topic: "orders", Subscription: "bridge", Attempt: 1},
			Gen:     uint32(s.next + 1),
		}
		s.next++
		n++
	}
	return n, nil
}

// Settler is the source's settler.
func (s *fakeSource) Settler() mq.Settler { return s.settler }

// Close unblocks a parked Fetch.
func (s *fakeSource) Close() error {
	s.closeOne.Do(func() { close(s.closed) })
	return nil
}

// seamBackend is a Backend that is a source, a sink, or both.
type seamBackend struct {
	caps   mq.Capabilities
	source *fakeSource
	sink   *fakeSink
}

// Info names the fake.
func (b *seamBackend) Info() mq.DriverInfo { return mq.DriverInfo{Name: "seam"} }

// Capabilities is the table the test built.
func (b *seamBackend) Capabilities() mq.Capabilities { return b.caps }

// OpenSink hands out the recording sink.
func (b *seamBackend) OpenSink(context.Context, mq.SinkConfig) (mq.Sink, error) {
	if b.sink == nil {
		return nil, mq.ErrUnsupported
	}
	return b.sink, nil
}

// OpenSource hands out the recording source.
func (b *seamBackend) OpenSource(context.Context, mq.SourceConfig) (mq.Source, error) {
	if b.source == nil {
		return nil, mq.ErrUnsupported
	}
	return b.source, nil
}

// Ping succeeds.
func (b *seamBackend) Ping(context.Context) error { return nil }

// Native offers no escape hatch.
func (b *seamBackend) Native() any { return nil }

// Close releases nothing.
func (b *seamBackend) Close() error { return nil }

// seamCaps is a capability table with the features a bridge end needs.
func seamCaps(tb testing.TB, features ...mq.Feature) mq.Capabilities {
	b := driverkit.NewCapabilities()
	for _, f := range features {
		b.Native(f, mq.ScopeTopic)
	}
	return mustBuild(tb, b)
}

// seam builds a harness whose source and destination are the recording fakes,
// and returns the shared log.
func seam(t *testing.T, bodies []string, sinkFailOn int, ackErr error, dstFeatures ...mq.Feature) (
	*harness, *seamLog,
) {
	t.Helper()
	return seamWithFlush(t, bodies, sinkFailOn, nil, ackErr, dstFeatures...)
}

// seamWithFlush is seam with a Flush that can refuse too.
func seamWithFlush(
	t *testing.T, bodies []string, sinkFailOn int, flushErr, ackErr error,
	dstFeatures ...mq.Feature,
) (*harness, *seamLog) {
	t.Helper()
	log := &seamLog{}
	settler := &fakeSettler{log: log, ackErr: ackErr}
	source := &fakeSource{
		log: log, bodies: bodies, settler: settler, closed: make(chan struct{}),
	}
	srcBackend := &seamBackend{
		caps:   seamCaps(t, mq.FeatureNack, mq.FeatureAtLeastOnce),
		source: source,
	}
	dstBackend := &seamBackend{
		caps: seamCaps(t, append([]mq.Feature{mq.FeaturePublisherConfirm}, dstFeatures...)...),
		sink: &fakeSink{log: log, failOn: sinkFailOn, flushErr: flushErr},
	}
	reg := dsn.NewRegistry()
	for scheme, b := range map[string]*seamBackend{"seamsrc": srcBackend, "seamdst": dstBackend} {
		backend := b
		if err := reg.Register(scheme, func(context.Context, string) (*mq.Client, error) {
			return mq.NewClient(backend), nil
		}); err != nil {
			t.Fatalf("registering %q: %v", scheme, err)
		}
	}
	return harnessWith(t, reg, nil), log
}

// TestBridgePublishesBeforeItSettles is the ordering the whole command rests
// on, pinned as one interleaved sequence.
//
// Publish-then-ack is at-least-once; ack-then-publish loses the message
// whenever the publication fails. Both orders leave a real broker pair in the
// same final state, so this is the only place the difference is observable.
func TestBridgePublishesBeforeItSettles(t *testing.T) {
	h, log := seam(t, []string{"a", "b"}, 0, nil)
	if code := h.run("bridge", "--max", "2", "seamsrc://", "orders", "bridge",
		"seamdst://", "mirror"); code != exitOK {
		t.Fatalf("bridge = %d\nstdout:\n%s\nstderr:\n%s", code, h.stdout(), h.stderr())
	}
	// publish -> flush -> ack, per message. The flush is in the sequence
	// because the fake sink implements mq.FlushSink, which is what makes the
	// "confirmed, not merely admitted" half of the rule observable at all.
	want := []string{"publish a", "flush", "ack a", "publish b", "flush", "ack b"}
	got := log.events()
	if len(got) != len(want) {
		t.Fatalf("the seam recorded %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("the seam recorded %v, want %v", got, want)
		}
	}
}

// TestBridgeTreatsAnUnconfirmedPublicationAsAFailure is the other half of the
// ordering: not merely "publish before settle" but "CONFIRM before settle".
//
// A sink that buffers returns nil from Publish the moment it has admitted the
// message, and the specification makes admission and acceptance different facts. This sink
// admits and then fails its Flush, which is a publication the destination
// never took -- so the source delivery has to come back, exactly as it does
// for an outright refusal.
func TestBridgeTreatsAnUnconfirmedPublicationAsAFailure(t *testing.T) {
	h, log := seamWithFlush(t, []string{"a"}, 0, errors.New("the buffer could not be sent"), nil)
	code := h.run("bridge", "--max", "1", "seamsrc://", "orders", "bridge",
		"seamdst://", "mirror")
	if code != exitFailure {
		t.Fatalf("bridge = %d, want %d\nstdout:\n%s\nstderr:\n%s",
			code, exitFailure, h.stdout(), h.stderr())
	}
	got := log.events()
	want := []string{"publish a", "flush-refused", "nack a"}
	if strings.Join(got, "|") != strings.Join(want, "|") {
		t.Fatalf("the seam recorded %v, want %v", got, want)
	}
	for _, e := range got {
		if e == "ack a" {
			t.Error("the source was settled against an ADMISSION rather than a confirmation")
		}
	}
	// And it is reported as AMBIGUOUS, not as a clean refusal: Publish
	// already returned nil, so the destination may hold the message the
	// source is about to redeliver.
	h2, log2 := seamWithFlush(t, []string{"a"}, 0, errors.New("the buffer could not be sent"), nil)
	if code := h2.run("bridge", "--max", "1", "-o", "json", "seamsrc://", "orders", "bridge",
		"seamdst://", "mirror"); code != exitFailure {
		t.Fatalf("bridge -o json = %d, want %d", code, exitFailure)
	}
	var rep bridgeReport
	if err := json.Unmarshal([]byte(h2.stdout()), &rep); err != nil {
		t.Fatalf("the JSON form does not parse: %v\n%s", err, h2.stdout())
	}
	if rep.Result.Ambiguous != 1 {
		t.Errorf("ambiguous = %d, want 1: a failed Flush leaves the acceptance unknown",
			rep.Result.Ambiguous)
	}
	if rep.Result.Published != 0 {
		t.Errorf("published = %d: nothing was confirmed", rep.Result.Published)
	}
	_ = log2
}

// TestBridgeReturnsTheSourceDeliveryWhenThePublishFails is the no-loss
// property: a message the destination refused is a message the source still
// owes, and the bridge has to hand it back rather than drop it.
func TestBridgeReturnsTheSourceDeliveryWhenThePublishFails(t *testing.T) {
	h, log := seam(t, []string{"a", "b"}, 2, nil)
	code := h.run("bridge", "--max", "2", "seamsrc://", "orders", "bridge",
		"seamdst://", "mirror")
	if code != exitFailure {
		t.Fatalf("bridge = %d, want %d\nstdout:\n%s\nstderr:\n%s",
			code, exitFailure, h.stdout(), h.stderr())
	}
	got := log.events()
	want := []string{"publish a", "flush", "ack a", "publish-refused b", "nack b"}
	if strings.Join(got, "|") != strings.Join(want, "|") {
		t.Fatalf("the seam recorded %v, want %v", got, want)
	}
	// An ELEMENT comparison and not a substring one: "nack b" contains
	// "ack b", so strings.Contains(joined, "ack b") is true of the correct
	// sequence as well as of the broken one -- a check that could only fail.
	for _, e := range got {
		if e == "ack b" {
			t.Error("the message the destination refused was acknowledged on the source anyway")
		}
	}
	if !strings.Contains(h.stderr(), "nothing is lost") {
		t.Errorf("the failure does not say the delivery was returned:\n%s", h.stderr())
	}
	// An outright refusal is NOT ambiguous: the sink never took the message,
	// so returning the delivery cannot produce a duplicate. If every failure
	// were reported as ambiguous the bucket would say nothing.
	if strings.Contains(h.stderr(), "MAY") {
		t.Errorf("an outright refusal was reported as ambiguous:\n%s", h.stderr())
	}
}

// TestBridgeStopsAtAFailedPublicationRatherThanReorder: the bridge is
// single-flight so that the destination sees the source's order. A refused
// publication nacked its delivery and went on to publish the ones fetched
// after it in the same batch, so the destination took b and c ahead of the a
// the source was about to redeliver, and --max counted the refusal as one of
// its three.
func TestBridgeStopsAtAFailedPublicationRatherThanReorder(t *testing.T) {
	h, log := seam(t, []string{"a", "b", "c"}, 1, nil)
	code := h.run("bridge", "--max", "3", "-o", "json", "seamsrc://", "orders", "bridge",
		"seamdst://", "mirror")
	if code != exitFailure {
		t.Fatalf("bridge = %d, want %d\nstdout:\n%s\nstderr:\n%s",
			code, exitFailure, h.stdout(), h.stderr())
	}
	got := log.events()
	want := []string{"publish-refused a", "nack a", "nack b", "nack c"}
	if strings.Join(got, "|") != strings.Join(want, "|") {
		t.Fatalf("the seam recorded %v, want %v: nothing fetched after a refused publication "+
			"may reach the destination ahead of it", got, want)
	}
	var rep bridgeReport
	if err := json.Unmarshal([]byte(h.stdout()), &rep); err != nil {
		t.Fatalf("the JSON form does not parse: %v\n%s", err, h.stdout())
	}
	r := &rep.Result
	if r.Received != 3 || r.PublishFailures != 1 || r.Returned != 2 || r.Published != 0 {
		t.Errorf("received %d, publish failures %d, returned %d, published %d; want 3, 1, 2 and 0",
			r.Received, r.PublishFailures, r.Returned, r.Published)
	}
	if !strings.Contains(r.StopReason, "ahead of it") {
		t.Errorf("the stop reason %q does not say why the run ended at the refusal", r.StopReason)
	}
}

// TestBridgeCountsTheDuplicateWindow: a destination that confirmed and a
// source that would not settle is the window the command's documentation is
// about, and the run has to report it rather than call the message moved.
func TestBridgeCountsTheDuplicateWindow(t *testing.T) {
	h, log := seam(t, []string{"a"}, 0, errors.New("the lease has expired"), mq.FeatureDeduplication)
	code := h.run("bridge", "--max", "1", "-o", "json", "seamsrc://", "orders", "bridge",
		"seamdst://", "mirror")
	if code != exitFailure {
		t.Fatalf("bridge = %d, want %d\nstderr:\n%s", code, exitFailure, h.stderr())
	}
	var rep bridgeReport
	if err := json.Unmarshal([]byte(h.stdout()), &rep); err != nil {
		t.Fatalf("the JSON form does not parse: %v\n%s", err, h.stdout())
	}
	if rep.Result.DuplicateRisk != 1 {
		t.Errorf("duplicate_risk = %d, want 1", rep.Result.DuplicateRisk)
	}
	if rep.Result.Moved != 0 {
		t.Errorf("moved = %d: the source never settled it, so nothing is complete",
			rep.Result.Moved)
	}
	if rep.Result.Published != 1 {
		t.Errorf("published = %d, want 1: the destination did take it", rep.Result.Published)
	}
	if e := strings.Join(log.events(), "|"); e != "publish a|flush|ack-refused a" {
		t.Errorf("the seam recorded %q", e)
	}
	if !strings.Contains(h.stderr(), "will redeliver") {
		t.Errorf("the failure does not say what happens next:\n%s", h.stderr())
	}
}

// TestBridgeExitsOneWhenTheSourceReceiveFails: a run that ended because the
// source's Receive failed said so in its stop reason and exited 0, so a script
// read a broken source as a run that moved everything. sub exits 1 for the
// same stop.
func TestBridgeExitsOneWhenTheSourceReceiveFails(t *testing.T) {
	log := &seamLog{}
	// A source the broker has closed: it hands out what it holds, and the
	// next Receive fails.
	source := &fakeSource{log: log, bodies: []string{"a"}, settler: &fakeSettler{log: log}, closed: make(chan struct{})}
	_ = source.Close()
	src := &seamBackend{caps: seamCaps(t, mq.FeatureNack, mq.FeatureAtLeastOnce), source: source}
	dst := &seamBackend{caps: seamCaps(t, mq.FeaturePublisherConfirm), sink: &fakeSink{log: log}}
	reg := dsn.NewRegistry()
	for scheme, b := range map[string]*seamBackend{"seamsrc": src, "seamdst": dst} {
		backend := b
		if err := reg.Register(scheme, func(context.Context, string) (*mq.Client, error) {
			return mq.NewClient(backend), nil
		}); err != nil {
			t.Fatalf("registering %q: %v", scheme, err)
		}
	}
	h := harnessWith(t, reg, nil)
	code := h.run("bridge", "seamsrc://", "orders", "bridge", "seamdst://", "mirror")
	if code != exitFailure {
		t.Errorf("bridge = %d after a failed Receive, want %d\nstdout:\n%s\nstderr:\n%s",
			code, exitFailure, h.stdout(), h.stderr())
	}
	if !strings.Contains(h.stdout(), "stopped: Receive failed") {
		t.Errorf("the run does not say the Receive failed:\n%s", h.stdout())
	}
	if !strings.Contains(h.stdout(), "moved 1") {
		t.Errorf("the run lost count of what it moved before the failure:\n%s", h.stdout())
	}
}

// TestBridgeRefusesASourceThatCannotNack: a source with no Nack turns every
// refused publication into a lost message, so the bridge refuses to start
// rather than finding out per message.
func TestBridgeRefusesASourceThatCannotNack(t *testing.T) {
	log := &seamLog{}
	src := &seamBackend{
		caps: seamCaps(t, mq.FeatureAtMostOnce),
		source: &fakeSource{
			log: log, bodies: []string{"a"},
			settler: &fakeSettler{log: log}, closed: make(chan struct{}),
		},
	}
	dst := &seamBackend{caps: seamCaps(t, mq.FeaturePublisherConfirm), sink: &fakeSink{log: log}}
	reg := dsn.NewRegistry()
	for scheme, b := range map[string]*seamBackend{"nonack": src, "anydst": dst} {
		backend := b
		if err := reg.Register(scheme, func(context.Context, string) (*mq.Client, error) {
			return mq.NewClient(backend), nil
		}); err != nil {
			t.Fatalf("registering %q: %v", scheme, err)
		}
	}
	h := harnessWith(t, reg, nil)
	if code := h.run("bridge", "--max", "1", "nonack://", "orders", "bridge",
		"anydst://", "mirror"); code != exitUnsupported {
		t.Fatalf("bridge = %d, want %d\nstderr:\n%s", code, exitUnsupported, h.stderr())
	}
	if !strings.Contains(h.stderr(), "Nack") {
		t.Errorf("the refusal does not name the feature:\n%s", h.stderr())
	}
	if n := len(log.events()); n != 0 {
		t.Errorf("the bridge did %d thing(s) before refusing: %v", n, log.events())
	}
}

// TestBridgeDedupRequiresTheFeatureAndSaysWhatItRests(on) covers both halves
// of --dedup-from id: refused where the destination cannot de-duplicate, and
// explicit about what the claim is worth where it can.
func TestBridgeDedupRequiresTheFeature(t *testing.T) {
	t.Run("refused without FeatureDeduplication", func(t *testing.T) {
		h, log := seam(t, []string{"a"}, 0, nil)
		if code := h.run("bridge", "--max", "1", "--dedup-from", "id",
			"seamsrc://", "orders", "bridge", "seamdst://", "mirror"); code != exitUnsupported {
			t.Fatalf("bridge = %d, want %d\nstderr:\n%s", code, exitUnsupported, h.stderr())
		}
		if n := len(log.events()); n != 0 {
			t.Errorf("the bridge published %d message(s) before refusing: %v", n, log.events())
		}
	})
	t.Run("says what the claim is worth", func(t *testing.T) {
		h, _ := seam(t, []string{"a"}, 0, nil, mq.FeatureDeduplication)
		if code := h.run("bridge", "--max", "1", "--dedup-from", "id",
			"seamsrc://", "orders", "bridge", "seamdst://", "mirror"); code != exitOK {
			t.Fatalf("bridge = %d\nstderr:\n%s", code, h.stderr())
		}
		out := h.stdout()
		for _, want := range []string{"Deduplication=native", "AS ADVERTISED", "WINDOW"} {
			if !strings.Contains(out, want) {
				t.Errorf("the run does not say %q:\n%s", want, out)
			}
		}
	})
	t.Run("--dry-run and --dedup-from id contradict each other", func(t *testing.T) {
		h, _ := seam(t, []string{"a"}, 0, nil)
		if code := h.run("bridge", "--dry-run", "--dedup-from", "id",
			"seamsrc://", "orders", "bridge", "seamdst://", "mirror"); code != exitUsage {
			t.Fatalf("bridge = %d, want %d", code, exitUsage)
		}
	})
}

// TestBridgeUnkeyedPublicationsAreCounted: a source delivery with no message
// id gets no de-duplication key, and a run that asked for de-duplication and
// silently got none for half its messages would be the wrong kind of quiet.
func TestBridgeUnkeyedPublicationsAreCounted(t *testing.T) {
	log := &seamLog{}
	settler := &fakeSettler{log: log}
	// A source whose deliveries carry no ID at all, which is what a broker
	// with no message identity gives you.
	src := &seamBackend{
		caps: seamCaps(t, mq.FeatureNack, mq.FeatureAtLeastOnce),
		source: &fakeSource{
			log: log, bodies: []string{"a"}, settler: settler, closed: make(chan struct{}),
		},
	}
	src.source.stripIDs = true
	dst := &seamBackend{
		caps: seamCaps(t, mq.FeaturePublisherConfirm, mq.FeatureDeduplication),
		sink: &fakeSink{log: log},
	}
	reg := dsn.NewRegistry()
	for scheme, b := range map[string]*seamBackend{"noid": src, "dedupdst": dst} {
		backend := b
		if err := reg.Register(scheme, func(context.Context, string) (*mq.Client, error) {
			return mq.NewClient(backend), nil
		}); err != nil {
			t.Fatalf("registering %q: %v", scheme, err)
		}
	}
	h := harnessWith(t, reg, nil)
	if code := h.run("bridge", "--max", "1", "--dedup-from", "id", "-o", "json",
		"noid://", "orders", "bridge", "dedupdst://", "mirror"); code != exitOK {
		t.Fatalf("bridge = %d\nstderr:\n%s", code, h.stderr())
	}
	var rep bridgeReport
	if err := json.Unmarshal([]byte(h.stdout()), &rep); err != nil {
		t.Fatalf("the JSON form does not parse: %v\n%s", err, h.stdout())
	}
	if rep.Result.Unkeyed != 1 {
		t.Errorf("unkeyed = %d, want 1: the delivery carried no id", rep.Result.Unkeyed)
	}
	if rep.Result.Moved != 1 {
		t.Errorf("moved = %d, want 1: an unkeyed publication is still a publication",
			rep.Result.Moved)
	}
}

// TestBridgePrintsNoCredential is over BOTH targets: either end's driver
// can put its own DSN into a message, and the report names them both.
func TestBridgePrintsNoCredential(t *testing.T) {
	const (
		srcPass  = "source-pa55word"
		dstToken = "dest-tok-987654"
	)
	h, _, _ := newBridgeHarness(t)
	source := srcScheme + "://reader:" + srcPass + "@host:1234?region=eu-west-1"
	dest := dstScheme + "://host:5678?apikey=" + dstToken
	// The topics do not exist, so the run fails -- which is the harder case,
	// because the failure path carries the driver's own message.
	if code := h.run("bridge", "--max", "1", source, "orders", "bridge",
		dest, "mirror"); code == exitOK {
		t.Fatalf("the bridge succeeded against topics that do not exist")
	}
	both := h.stdout() + h.stderr()
	for _, secret := range []string{srcPass, dstToken} {
		if strings.Contains(both, secret) {
			t.Errorf("a credential reached the terminal:\n%s", both)
		}
	}
	if !strings.Contains(both, dsn.Redacted) {
		t.Errorf("no target was rendered redacted:\n%s", both)
	}
	// the two layers are two separate claims, and the assertions above hold
	// only the SCRUB: with redactTarget removed and the raw DSN rendered, the
	// scrub still replaced both secrets and this test still passed. "region"
	// is not a credential name, so the scrub does not hold its value and the
	// only thing that can keep "eu-west-1" out of the output is the
	// structural layer -- which drops every parameter rather than deciding
	// per parameter which ones are secret.
	if strings.Contains(both, "eu-west-1") {
		t.Errorf("a DSN parameter's VALUE was rendered; the structural layer drops every "+
			"parameter and the scrub does not hold a non-credential one:\n%s", both)
	}
	if !strings.Contains(both, "parameter, not shown") {
		t.Errorf("the parameter count is not reported at all:\n%s", both)
	}
}

// bridgeRaceRounds is how many interrupted bridges the race test runs.
//
// Three thousand, because this repository's instances of the
// place-versus-close defect class appeared at five to twenty failures per two
// or three thousand rounds: a hundred rounds proves nothing (the contributor notes,
// "The engine's recurring defect class").
const bridgeRaceRounds = 3000

// TestBridgeLosesNothingWhenInterruptedMidTransfer races an interrupt against
// the transfer, three thousand times, and holds the seam to the one promise it
// makes: a message may be duplicated on the destination and may never be
// acknowledged on the source, but it is never in NEITHER place.
//
// The interrupt is the realistic race -- Ctrl-C during a bridge -- and it lands
// somewhere different every round: between the publish and the flush, between
// the flush and the ack, or before the receive. Each of those is a window in
// which the wrong order would lose the message, and none of them is reachable
// deterministically.
//
// It also holds the run's own accounting: every delivery a run received is in
// exactly one bucket. A delivery counted twice would flatter the summary, and
// one counted nowhere is the "dropped on the floor" shape that leaves a
// message held until its lease expires with nothing in the output saying so.
func TestBridgeLosesNothingWhenInterruptedMidTransfer(t *testing.T) {
	h, source, dest := newBridgeHarness(t)
	ensure(t, source, "orders", "bridge")
	ensure(t, dest, "mirror", "reader")

	msgs := make([]*mq.Message, bridgeRaceRounds)
	for i := range msgs {
		msgs[i] = &mq.Message{Body: []byte("m" + itoa(i))}
	}
	publish(t, source, "orders", msgs...)

	var duplicateRisk, ambiguous, received, stopped int
	for round := range bridgeRaceRounds {
		ctx, cancel := context.WithCancel(context.Background())
		hr := harnessWith(t, h.env.registry, nil)
		hr.env.ctx = ctx
		// The cancel races the transfer from another goroutine. The delay
		// walks across the work a single transfer does, so over three
		// thousand rounds it lands in every window rather than always in the
		// same one.
		go func() {
			for range round % 7 {
				runtime.Gosched()
			}
			cancel()
		}()
		// --prefetch 1, so the source driver holds ONE message per round:
		// with the default window it buffers 128, and the 127 the run never
		// receives are requeued only at Close, which buries the interesting
		// path in bookkeeping.
		code := hr.run("bridge", "--max", "1", "--prefetch", "1", "-o", "json",
			srcScheme+"://", "orders", "bridge", dstScheme+"://", "mirror")
		cancel()
		switch code {
		case exitOK, exitFailure:
		default:
			t.Fatalf("round %d: bridge = %d\nstdout:\n%s\nstderr:\n%s",
				round, code, hr.stdout(), hr.stderr())
		}
		var rep bridgeReport
		if err := json.Unmarshal([]byte(hr.stdout()), &rep); err != nil {
			t.Fatalf("round %d: the JSON form does not parse: %v\n%s", round, err, hr.stdout())
		}
		r := &rep.Result
		if got := r.Moved + r.PublishFailures + r.DuplicateRisk + r.Returned; got != r.Received {
			t.Fatalf("round %d: %d deliveries received and %d accounted for: %+v",
				round, r.Received, got, r)
		}
		received += r.Received
		duplicateRisk += r.DuplicateRisk
		ambiguous += r.Ambiguous
		if r.Received == 0 {
			stopped++
		}
	}

	// Nothing is lost. Whatever the interrupts did, every message is either
	// on the destination or still owed by the source -- and the total may
	// EXCEED the count only by the duplicates the seam is documented to
	// allow.
	onDest := len(drain(t, dest, "mirror", "reader", bridgeRaceRounds+1))
	left := int(depth(t, source, "orders", "bridge"))
	if onDest+left < bridgeRaceRounds {
		t.Errorf("%d message(s) on the destination and %d still owed by the source, "+
			"from %d published: %d have been LOST",
			onDest, left, bridgeRaceRounds, bridgeRaceRounds-onDest-left)
	}
	// And no duplicate the run did not ACCOUNT for. Every message that is
	// both on the destination and still owed by the source has to correspond
	// to a window the summary named -- a confirmed publication the source
	// would not settle, or a publication whose acceptance was unknown. An
	// unexplained duplicate is a bridge that is quietly less reliable than
	// its own output claims, and this is the check that caught the whole of
	// the "ambiguous" bucket: before it existed the totals came out at 3002
	// from 3000 published with 0 duplicate risks reported.
	overlap := onDest + left - bridgeRaceRounds
	if overlap > duplicateRisk+ambiguous {
		t.Errorf("%d message(s) are on the destination AND still owed by the source, and the "+
			"runs reported only %d duplicate risk(s) and %d ambiguous publication(s)",
			overlap, duplicateRisk, ambiguous)
	}
	t.Logf("%d rounds: %d deliveries received, %d runs stopped before receiving anything, "+
		"%d duplicate risk(s), %d ambiguous; %d on the destination, %d still owed by the "+
		"source, overlap %d",
		bridgeRaceRounds, received, stopped, duplicateRisk, ambiguous, onDest, left, overlap)
	if received == 0 {
		t.Error("no round received a delivery at all, so this test proves nothing: the " +
			"interrupt is landing before every receive")
	}
	if stopped == 0 {
		t.Error("no round was interrupted before it received anything, so the interrupt " +
			"is landing too late to race anything")
	}
}

// TestBridgeUsage holds the invocation contract.
func TestBridgeUsage(t *testing.T) {
	cases := []struct {
		args []string
		want string
	}{
		{[]string{"bridge"}, "are required"},
		{[]string{"bridge", "a://b", "t", "s", "c://d"}, "are required"},
		{[]string{"bridge", "a://b", "t", "s", "c://d", "u", "extra"}, "are required"},
		{[]string{"bridge", "--headers", "maybe", "a://b", "t", "s", "c://d", "u"}, "pass or drop"},
		{[]string{"bridge", "--key", "maybe", "a://b", "t", "s", "c://d", "u"}, "pass or drop"},
		{[]string{"bridge", "--dedup-from", "hash", "a://b", "t", "s", "c://d", "u"}, "id or none"},
		{[]string{"bridge", "--on-stop", "maybe", "a://b", "t", "s", "c://d", "u"}, "ack, nack"},
		{[]string{"bridge", "--dry-run", "--on-stop", "ack", "a://b", "t", "s", "c://d", "u"}, "leave the source"},
		{[]string{"bridge", "--dry-run", "--on-stop", "reject", "a://b", "t", "s", "c://d", "u"}, "leave the source"},
		{[]string{"bridge", "--start", "middle", "a://b", "t", "s", "c://d", "u"}, "latest or earliest"},
		{[]string{"bridge", "--max", "-1", "a://b", "t", "s", "c://d", "u"}, "non-negative"},
		{[]string{"bridge", "-o", "yaml", "a://b", "t", "s", "c://d", "u"}, "text or json"},
		{[]string{"bridge", "--confirm", "maybe", "a://b", "t", "s", "c://d", "u"}, "default, accepted"},
	}
	for _, c := range cases {
		h := newFullHarness(t)
		if got := h.run(c.args...); got != exitUsage {
			t.Errorf("run(%v) = %d, want %d\nstderr:\n%s", c.args, got, exitUsage, h.stderr())
		}
		if !strings.Contains(h.stderr(), c.want) {
			t.Errorf("run(%v) stderr does not contain %q:\n%s", c.args, c.want, h.stderr())
		}
	}
}
