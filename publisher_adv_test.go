package iqmq_test

import (
	"context"
	"sync"
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit"
)

// This file is the driver the advanced publication surface is tested
// against: the optional interfaces that the specification left unused —
// AsyncSink and OwnedSink — plus the receipt protocol.
//
// It follows spi_test.go's rule about optional interfaces, and for the same
// reason: each optional method lives on a distinct type, so the core's
// type assertion really fails for a driver that does not have it. A single
// sink type with a boolean would satisfy every interface at compile time and
// the adapters the core promises a minimal driver would never run.
//
//	advSink        Sink, and nothing else
//	advAsyncSink   Sink + AsyncSink
//	advOwnedSink   Sink + OwnedSink
//	advBatchSink   Sink + BatchSink
//
// What it adds beyond a real driver is the bookkeeping the tests read: what
// each publication carried, when it was read, and the faults that make a
// rejection, a held publication or a late completion reachable at all.

// advCaps is a capability set with every feature native and every limit
// advertised, minus the features a test wants missing.
func advCaps(without ...mq.Feature) mq.Capabilities {
	var caps mq.Capabilities
	skip := make(map[mq.Feature]bool, len(without))
	for _, f := range without {
		skip[f] = true
	}
	for f := mq.Feature(0); int(f) < mq.FeatureCount; f++ {
		if skip[f] {
			continue
		}
		if err := caps.Set(f, mq.Support{Level: mq.Native, Scope: mq.ScopeTopic}); err != nil {
			panic(err)
		}
	}
	caps.SetLimits(mq.Limits{
		MaxMessageBytes:  mq.Limit[int64]{Value: 1 << 20, Known: true},
		MaxBatchMessages: mq.Limit[int]{Value: 1000, Known: true},
		MaxHeaderCount:   mq.Limit[int]{Value: 64, Known: true},
		MaxDelay:         mq.Limit[time.Duration]{Value: time.Hour, Known: true},
	})
	return caps
}

// advRec is one publication as the driver saw it.
type advRec struct {
	body  string
	id    string
	dedup string
	delay time.Duration
	part  int32
	made  time.Time
}

// advBroker is the fake broker. open decides which sink type it hands out,
// which is how a test chooses the optional interfaces its driver has.
type advBroker struct {
	caps mq.Capabilities
	open func(*advBroker, mq.SinkConfig) mq.Sink

	mu       sync.Mutex
	sink     mq.Sink
	sinkCfg  mq.SinkConfig
	recs     []advRec
	calls    int
	inflight int
	maxSeen  int
	// fail decides a publication's fate; nil accepts everything.
	fail func(n int, m *mq.Message) error
}

func newAdvBroker(open func(*advBroker, mq.SinkConfig) mq.Sink, without ...mq.Feature) *advBroker {
	return &advBroker{caps: advCaps(without...), open: open}
}

func (b *advBroker) Info() mq.DriverInfo           { return mq.DriverInfo{Name: "adv", DriverVersion: "v0"} }
func (b *advBroker) Capabilities() mq.Capabilities { return b.caps }

func (b *advBroker) OpenSink(_ context.Context, cfg mq.SinkConfig) (mq.Sink, error) {
	sink := b.open(b, cfg)
	b.mu.Lock()
	b.sinkCfg = cfg
	b.sink = sink
	b.mu.Unlock()
	return sink, nil
}

func (b *advBroker) OpenSource(context.Context, mq.SourceConfig) (mq.Source, error) {
	return nil, mq.ErrUnsupported
}

func (b *advBroker) Ping(context.Context) error { return nil }
func (b *advBroker) Native() any                { return b }
func (b *advBroker) Close() error               { return nil }

// accept is the native client: it records the publication, or refuses it.
func (b *advBroker) accept(m *mq.Message, o *mq.PublishOptions, receipts bool) error {
	b.mu.Lock()
	b.calls++
	n := b.calls
	fail := b.fail
	b.mu.Unlock()
	if fail != nil {
		if err := fail(n, m); err != nil {
			return err
		}
	}
	rec := advRec{body: string(m.Body), id: m.ID, made: m.Properties.CreatedAt}
	if o != nil {
		rec.dedup, rec.delay, rec.part = o.DedupKey, o.Delay, o.Partition
	}
	if receipts && m.Receipt != nil {
		// the receipt protocol: the storage is the caller's and the
		// driver fills it in place, on success.
		m.Receipt.MessageID = "srv-" + rec.body
		m.Receipt.Partition = 7
		m.Receipt.Offset = int64(n)
		m.Receipt.Sequence = uint64(n)
	}
	b.mu.Lock()
	b.recs = append(b.recs, rec)
	b.mu.Unlock()
	return nil
}

// enter and leave record how many publications the driver held at once,
// which is what a bounded buffer is a claim about.
func (b *advBroker) enter() {
	b.mu.Lock()
	b.inflight++
	if b.inflight > b.maxSeen {
		b.maxSeen = b.inflight
	}
	b.mu.Unlock()
}

func (b *advBroker) leave() {
	b.mu.Lock()
	b.inflight--
	b.mu.Unlock()
}

func (b *advBroker) records() []advRec {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make([]advRec, len(b.recs))
	copy(out, b.recs)
	return out
}

func (b *advBroker) bodies() []string {
	recs := b.records()
	out := make([]string, 0, len(recs))
	for _, r := range recs {
		out = append(out, r.body)
	}
	return out
}

func (b *advBroker) nativeCalls() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.calls
}

func (b *advBroker) concurrentMax() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.maxSeen
}

func (b *advBroker) setFail(f func(n int, m *mq.Message) error) {
	b.mu.Lock()
	b.fail = f
	b.mu.Unlock()
}

// advSink is the whole of a driver with nothing optional: one method and a
// close.
type advSink struct {
	b        *advBroker
	receipts bool

	mu     sync.Mutex
	closed bool
}

func openPlain(b *advBroker, cfg mq.SinkConfig) mq.Sink {
	return &advSink{b: b, receipts: cfg.Receipts}
}

func (s *advSink) Publish(ctx context.Context, m *mq.Message, o *mq.PublishOptions) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	closed := s.closed
	s.mu.Unlock()
	if closed {
		return mq.ErrClosed
	}
	if o == nil {
		return mq.ErrUnsupported
	}
	return s.b.accept(m, o, s.receipts)
}

func (s *advSink) Close() error {
	s.mu.Lock()
	s.closed = true
	s.mu.Unlock()
	return nil
}

func (s *advSink) isClosed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closed
}

// advBatchSink is the same driver with the one optional method a native
// batcher adds.
type advBatchSink struct{ advSink }

func openBatch(b *advBroker, cfg mq.SinkConfig) mq.Sink {
	return &advBatchSink{advSink{b: b, receipts: cfg.Receipts}}
}

// PublishBatch answers per message through driverkit.CollapseBatch, the
// helper a driver author is given for the batch result.
func (s *advBatchSink) PublishBatch(ctx context.Context, ms []*mq.Message, o *mq.PublishOptions) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if s.isClosed() {
		return mq.ErrClosed
	}
	errs := make([]error, len(ms))
	for i, m := range ms {
		errs[i] = s.b.accept(m, o, s.receipts)
	}
	return driverkit.CollapseBatch(errs)
}

// advAsyncCall is one publication the asynchronous sink is holding: the
// message, the option set the core handed over, and the callback that
// finishes it.
type advAsyncCall struct {
	m    *mq.Message
	o    *mq.PublishOptions
	done func(error)
	// seenDedup is what the driver read out of o, from its own goroutine,
	// after PublishAsync had already returned. A core that pooled the option
	// set would have handed it to another message by then.
	seenDedup string
	seenDelay time.Duration
}

// advAsyncSink is Sink plus AsyncSink. It holds every publication until the
// test releases it, which is how a test reaches the state the specification is about: a
// publication admitted, the caller returned, and the outcome not yet known.
type advAsyncSink struct {
	advSink

	amu      sync.Mutex
	held     []*advAsyncCall
	refuse   error
	admitted int
}

func openAsync(b *advBroker, cfg mq.SinkConfig) mq.Sink {
	return &advAsyncSink{advSink: advSink{b: b, receipts: cfg.Receipts}}
}

func (s *advAsyncSink) PublishAsync(ctx context.Context, m *mq.Message, o *mq.PublishOptions, done func(error)) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if s.isClosed() {
		return mq.ErrClosed
	}
	s.amu.Lock()
	if s.refuse != nil {
		err := s.refuse
		s.amu.Unlock()
		return err
	}
	s.admitted++
	s.held = append(s.held, &advAsyncCall{m: m, o: o, done: done})
	s.amu.Unlock()
	s.b.enter()
	return nil
}

// setRefuse makes every further PublishAsync refuse with err, which the specification says
// means the callback will not run.
func (s *advAsyncSink) setRefuse(err error) {
	s.amu.Lock()
	s.refuse = err
	s.amu.Unlock()
}

// heldCount is how many publications the driver is holding.
func (s *advAsyncSink) heldCount() int {
	s.amu.Lock()
	defer s.amu.Unlock()
	return len(s.held)
}

// takeHeld removes up to n held publications and returns them WITHOUT
// completing any of them, so a test can choose the instant and the goroutine
// each outcome arrives on.
//
// It is what [advAsyncSink.complete] cannot give: complete waits for its own
// goroutine, so the call that hands an outcome over is always ordered before
// whatever the test does next. A test about a completion that RACES something
// — a close, another completion — needs the hand-over itself to be
// concurrent.
func (s *advAsyncSink) takeHeld(n int) []*advAsyncCall {
	s.amu.Lock()
	defer s.amu.Unlock()
	if n > len(s.held) {
		n = len(s.held)
	}
	take := s.held[:n:n]
	s.held = s.held[n:]
	return take
}

// complete finishes up to n held publications on a goroutine of the driver's
// own, reading each option set there — after the call that handed it over has
// long returned. It waits for that goroutine, so a test that calls it has a
// definite state afterwards.
func (s *advAsyncSink) complete(n int, err error) []*advAsyncCall {
	s.amu.Lock()
	if n > len(s.held) {
		n = len(s.held)
	}
	take := s.held[:n:n]
	s.held = s.held[n:]
	s.amu.Unlock()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for _, c := range take {
			// The read that matters: the driver's own goroutine, after the
			// call returned, through the pointer it was given.
			c.seenDedup, c.seenDelay = c.o.DedupKey, c.o.Delay
			if err == nil {
				_ = s.b.accept(c.m, c.o, s.receipts)
			}
			s.b.leave()
			c.done(err)
		}
	}()
	wg.Wait()
	return take
}

// advOwnedSink is Sink plus OwnedSink: it takes ownership at invocation and
// owes the release.
type advOwnedSink struct {
	advSink

	omu       sync.Mutex
	refuse    error
	noRelease bool
}

func openOwned(b *advBroker, cfg mq.SinkConfig) mq.Sink {
	return &advOwnedSink{advSink: advSink{b: b, receipts: cfg.Receipts}}
}

func (s *advOwnedSink) PublishOwned(ctx context.Context, om mq.OwnedMessage, o *mq.PublishOptions) error {
	// The specification: the release is this sink's obligation from the moment it is
	// called, on every path out — including the refusal below.
	if !s.noRelease && om.Release != nil {
		defer om.Release(om.Message)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if s.isClosed() {
		return mq.ErrClosed
	}
	s.omu.Lock()
	refuse := s.refuse
	s.omu.Unlock()
	if refuse != nil {
		return refuse
	}
	return s.b.accept(om.Message, o, s.receipts)
}

func (s *advOwnedSink) setRefuse(err error) {
	s.omu.Lock()
	s.refuse = err
	s.omu.Unlock()
}

// advClient opens a client and a publisher over one of the sink shapes.
func advClient(tb testing.TB, open func(*advBroker, mq.SinkConfig) mq.Sink, cfg mq.PublisherConfig, opts ...mq.ClientOption) (*advBroker, *mq.PublisherHandle) {
	tb.Helper()
	b := newAdvBroker(open)
	return b, advPublisher(tb, b, cfg, opts...)
}

// advPublisher opens a publisher over an existing broker.
func advPublisher(tb testing.TB, b *advBroker, cfg mq.PublisherConfig, opts ...mq.ClientOption) *mq.PublisherHandle {
	tb.Helper()
	cl := mq.NewClient(b, opts...)
	tb.Cleanup(func() { _ = cl.Close() })
	if cfg.Topic == "" {
		cfg.Topic = "orders"
	}
	p, err := cl.Publisher(context.Background(), cfg)
	if err != nil {
		tb.Fatalf("opening the publisher: %v", err)
	}
	return p
}

// advSinkOf returns the sink the broker last opened, as its concrete type.
func advSinkOf[T mq.Sink](tb testing.TB, b *advBroker) T {
	tb.Helper()
	b.mu.Lock()
	got := b.sink
	b.mu.Unlock()
	s, ok := got.(T)
	if !ok {
		var zero T
		tb.Fatalf("the publisher's sink is %T, want %T", got, zero)
	}
	return s
}

// staticID is an IDGenerator that allocates nothing, so that a publisher
// configured with one still measures the core's own cost and not a
// generator's.
type staticID struct{ id string }

func (s staticID) NextID() string { return s.id }

// countingID hands out a distinct identifier per call, which is what a test
// of "only a message with no identifier is stamped" needs.
type countingID struct {
	mu sync.Mutex
	n  int
}

func (c *countingID) NextID() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.n++
	return "gen-" + string(rune('a'+c.n-1))
}

// fixedClock is a clock a test drives by hand, so that a linger deadline and
// a timestamp are decided by the test and not by the wall.
type fixedClock struct {
	mu  sync.Mutex
	now time.Time
	// timers are the timers handed out, woken by advance.
	timers []*fixedTimer
}

func newFixedClock() *fixedClock {
	return &fixedClock{now: time.Date(2026, 9, 12, 10, 0, 0, 0, time.UTC)}
}

func (c *fixedClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fixedClock) NewTimer(d time.Duration) mq.Timer {
	c.mu.Lock()
	defer c.mu.Unlock()
	t := &fixedTimer{c: make(chan time.Time, 1), at: c.now.Add(d)}
	c.timers = append(c.timers, t)
	return t
}

// liveTimers is how many timers the clock has handed out and not yet fired or
// stopped, which is how a test knows something is really waiting on it.
func (c *fixedClock) liveTimers() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := 0
	for _, t := range c.timers {
		if !t.stopped() {
			n++
		}
	}
	return n
}

// advance moves the clock and fires every timer that is due.
func (c *fixedClock) advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	now := c.now
	due := c.timers[:0:0]
	keep := c.timers[:0]
	for _, t := range c.timers {
		if !t.stopped() && !now.Before(t.at) {
			due = append(due, t)
			continue
		}
		keep = append(keep, t)
	}
	c.timers = keep
	c.mu.Unlock()
	for _, t := range due {
		t.fire(now)
	}
}

type fixedTimer struct {
	c    chan time.Time
	at   time.Time
	mu   sync.Mutex
	dead bool
}

func (t *fixedTimer) C() <-chan time.Time { return t.c }

func (t *fixedTimer) Stop() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	was := !t.dead
	t.dead = true
	return was
}

func (t *fixedTimer) Reset(time.Duration) bool { return true }

func (t *fixedTimer) stopped() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.dead
}

func (t *fixedTimer) fire(now time.Time) {
	select {
	case t.c <- now:
	default:
	}
}
