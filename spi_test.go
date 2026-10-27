package iqmq_test

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit"
)

//
// It therefore implements the mandatory surface and no more than it has to:
//
//   - fakeSink implements Sink and nothing else. It is a distinct type from
//     fakeBatchSink precisely so that the core's assertion really fails for
//     it — a sink that merely returned an error from PublishBatch would still
//     satisfy BatchSink and the adapter would never run (the lesson, x/apibench/a36/shapes.go).
//   - fakeBatchSink adds the one optional method a driver with native
//     batching writes.
//   - fakeSource fills the caller's slice, hands out a new generation per
//     delivery, and requeues on Close.
//   - fakeSettler answers for the native action only: the core owns the specification's
//     state machine, and this settler is allowed to be as simple as a
//     broker's own protocol.
//
// What it adds beyond a real driver is bookkeeping the tests read: the
// conservation (published == acked + ready + inflight + dropped +
// dead-lettered) and the faults that make an ambiguous or stale settlement
// reachable at all.

// fakeConfig is what the fake broker is opened with. Every field is a driver
// property the tests need to vary, not a knob a real driver would have.
type fakeConfig struct {
	// openSinkErr makes OpenSink fail, which is the failure a misconfigured
	// deployment meets before any message is published.
	openSinkErr error
	// batching makes OpenSink return a sink that implements BatchSink.
	batching bool
	// deadLetter gives the broker a dead-letter destination, without which
	// a dead-lettering nack answers ErrUnsupported.
	deadLetter bool
	// delayed gives it native delayed redelivery.
	delayed bool
	// extend makes Source.Settler return a settler that also implements
	// the optional DeadlineExtender, which is what makes Delivery.Extend
	// reach a driver instead of answering ErrUnsupported.
	extend bool
	// noSettler makes Source.Settler return nil, which is AckNone
	// model: the deliveries are not settleable.
	noSettler bool
	// maxBatch is the MaxBatchMessages the broker advertises. Zero
	// advertises none.
	maxBatch int
	// maxBytes is the MaxMessageBytes it advertises. Zero advertises none.
	maxBytes int64
	// maxHeaders is the MaxHeaderCount it advertises.
	maxHeaders int
	// scheduled advertises FeatureScheduledPublish.
	scheduled bool
	// lease is how long a delivery's lease lasts. Zero is fakeLease; a
	// negative value makes the broker grant no lease at all, which is what
	// DeliveryInfo.LeaseExpires being the zero time means and what a driver
	// with no redelivery deadline reports.
	lease time.Duration
	// countless makes the broker report no attempt count, which is the specification's
	// "the broker does not count attempts" and what a runner retrying by
	// nack has nothing to bound.
	countless bool
	// partitions is how many source partitions the broker spreads its
	// records over. Zero puts every delivery in fakePartition, which is what
	// a single-partition topic looks like.
	partitions int
	// settlePermissive makes the settler acknowledge anything it is handed,
	// including a delivery of a source that has closed and a token it has
	// never seen.
	//
	// It is a badly behaved driver on purpose. The rules are the API's
	// and hold "whatever the driver": a test whose driver refuses a
	// settlement after close cannot tell the core's rule from the driver's,
	// and would pass for a core that had no rule at all.
	settlePermissive bool
}

// fakeBroker is the fake driver's broker: topics, subscriptions and the
// records in them.
type fakeBroker struct {
	cfg fakeConfig

	mu     sync.Mutex
	topics map[string]*fakeTopic
	closed bool

	// seq counts the publications the native client accepted, so a test can
	// see the order a batch reached the broker in.
	seq  int
	sent []int
	// refuse decides a publication's fate. It is the fault injector the
	// batch tests drive; nil accepts everything.
	refuse func(seq int, m *mq.Message) error
	// lose is asked AFTER a publication has been stored, and a non-nil
	// answer is returned in place of the success: the broker has the message
	// and the client was never told, which is the ambiguous outcome and
	// the one in which republishing duplicates. nil loses nothing.
	lose func(seq int, m *mq.Message) error
	// ambiguous makes the settlement of a token answer "unknown".
	ambiguous map[uint64]bool
	// settlerErr, when set, is what every settlement answers with before
	// the broker is consulted at all.
	settlerErr error
	// settlerFails is how many of the next settlements answer
	// errSettleRefused, a definite refusal the driver would accept on a
	// retry.
	settlerFails int
	// extendGate is gate for extensions alone, which is how a test parks an
	// extension inside the driver while a settlement of the same delivery
	// completes.
	extendGate *settleGate
	// calls counts the native calls: one per single publish, one per
	// batch.
	calls int
	// events is an ordered log of the driver-facing events a test needs to
	// see the ORDER of: a settlement entering and leaving the driver, and
	// the source closing. The specification puts the close after the settlements in
	// flight, and an order is the only way to check that.
	events []string
	// gate, when set, holds every settlement inside the driver until it is
	// released, which is how a test reaches the state the specification calls settling:
	// one attempt in flight and the broker not yet told anything.
	gate *settleGate
	// sinkConfirm is the confirmation boundary each sink was opened with,
	// which is how a test sees that a PublisherConfig reached the driver.
	sinkConfirm map[string]mq.ConfirmationMode
}

// settleGate holds a settlement inside the driver. entered announces the
// token of a settlement that has arrived; release lets every waiting
// settlement through.
type settleGate struct {
	entered chan uint64
	release chan struct{}
}

// The broker-side values the fake reports in its delivery metadata. They are
// constants so that a test can name what it expects rather than read it back
// out of the thing it is checking.
const (
	// fakeStream is the prefix of DeliveryInfo.Source: the broker-side
	// origin, which for this broker is a stream per topic.
	fakeStream = "stream-"
	// fakePartition is the partition every delivery comes from.
	fakePartition = int32(3)
	// fakeLease is how long a delivery's lease lasts.
	fakeLease = 30 * time.Second
)

// fakeTopic is one topic and its subscriptions.
type fakeTopic struct {
	subs map[string]*fakeSub
}

// fakeSub is one subscription: a ready queue, an in-flight table and the
// counters the conservation invariant is checked against.
type fakeSub struct {
	ready    []*fakeRecord
	inflight map[uint64]*fakeRecord
	nextTok  uint64
	// wake is the fetch's parking signal, buffered so that a publication
	// never blocks on a consumer that is not parked (the wait strategy:
	// register, re-check, park).
	wake chan struct{}
	// gone is closed when the source closes, which is what unblocks a
	// parked Fetch — the specification makes that the driver's obligation.
	gone   chan struct{}
	closed bool

	published    int
	acked        int
	requeued     int
	dropped      int
	deadLettered int
	// extended counts the lease extensions the driver applied. It moves no
	// record, so it is not part of the conservation.
	extended int
	// extendedBy is the total the leases were moved by, so that a test can
	// see that the driver was handed the duration it was asked for.
	extendedBy time.Duration
}

// fakeRecord is one message in one subscription.
type fakeRecord struct {
	msg     *mq.Message
	tok     uint64
	gen     uint32
	attempt uint32
	// lease is when this record's current delivery stops being the
	// consumer's, which is what DeliveryInfo.LeaseExpires reports and what
	// fakeBroker.expireLeases sweeps against. It is set at every fetch and
	// MOVED by Extend, so that a test can see whether an extension bought
	// anything rather than only that the driver was called. The zero time is a broker that grants no lease at all.
	lease time.Time
}

// newFakeBroker builds a broker with the given topics.
func newFakeBroker(cfg fakeConfig, topics ...string) *fakeBroker {
	b := &fakeBroker{
		cfg:         cfg,
		topics:      map[string]*fakeTopic{},
		ambiguous:   map[uint64]bool{},
		sinkConfirm: map[string]mq.ConfirmationMode{},
	}
	for _, t := range topics {
		b.topics[t] = &fakeTopic{subs: map[string]*fakeSub{}}
	}
	return b
}

// Info describes the driver.
func (b *fakeBroker) Info() mq.DriverInfo {
	return mq.DriverInfo{
		Name:          "fake",
		DriverVersion: "v0",
		ClientName:    "none",
	}
}

// Capabilities is what the broker advertises. A driver builds this
// once, at open, from its profile.
func (b *fakeBroker) Capabilities() mq.Capabilities {
	var caps mq.Capabilities
	native := []mq.Feature{
		mq.FeatureAtLeastOnce, mq.FeatureExplicitAck, mq.FeatureNack,
		mq.FeatureConsumerGroups, mq.FeatureFanOut, mq.FeatureBatchReceive,
	}
	if b.cfg.batching {
		native = append(native, mq.FeatureBatchPublish)
	}
	if b.cfg.deadLetter {
		native = append(native, mq.FeatureDeadLetter)
	}
	if b.cfg.delayed {
		native = append(native, mq.FeatureDelayedRedelivery)
	}
	if b.cfg.scheduled {
		native = append(native, mq.FeatureScheduledPublish)
	}
	for _, f := range native {
		if err := caps.Set(f, mq.Support{Level: mq.Native, Scope: mq.ScopeTopic}); err != nil {
			panic(err)
		}
	}
	caps.SetLimits(mq.Limits{
		MaxMessageBytes:  mq.Limit[int64]{Value: b.cfg.maxBytes, Known: b.cfg.maxBytes > 0},
		MaxBatchMessages: mq.Limit[int]{Value: b.cfg.maxBatch, Known: b.cfg.maxBatch > 0},
		MaxHeaderCount:   mq.Limit[int]{Value: b.cfg.maxHeaders, Known: b.cfg.maxHeaders > 0},
	})
	return caps
}

// OpenSink opens a publication channel. A driver with native batching
// returns the type that has the optional method; one without returns the
// type that does not, and the core's single assertion at open is what tells
// them apart.
func (b *fakeBroker) OpenSink(_ context.Context, cfg mq.SinkConfig) (mq.Sink, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.cfg.openSinkErr != nil {
		return nil, b.cfg.openSinkErr
	}
	if b.closed {
		return nil, mq.ErrClosed
	}
	if _, ok := b.topics[cfg.Topic]; !ok {
		return nil, mq.ErrTopicNotFound
	}
	b.sinkConfirm[cfg.Topic] = cfg.Confirmation
	if b.cfg.batching {
		return &fakeBatchSink{fakeSink{b: b, topic: cfg.Topic}}, nil
	}
	return &fakeSink{b: b, topic: cfg.Topic}, nil
}

// OpenSource opens a subscription, creating it on first use.
func (b *fakeBroker) OpenSource(_ context.Context, cfg mq.SourceConfig) (mq.Source, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return nil, mq.ErrClosed
	}
	t, ok := b.topics[cfg.Topic]
	if !ok {
		return nil, mq.ErrTopicNotFound
	}
	if cfg.Prefetch <= 0 {
		// The core resolves the default before opening a source, so a
		// driver that sees zero here has been handed an unresolved config.
		return nil, mq.ErrUnsupported
	}
	sub, ok := t.subs[cfg.Subscription]
	if !ok {
		sub = &fakeSub{
			inflight: map[uint64]*fakeRecord{},
			wake:     make(chan struct{}, 1),
			gone:     make(chan struct{}),
		}
		t.subs[cfg.Subscription] = sub
	}
	return &fakeSource{b: b, topic: cfg.Topic, sub: cfg.Subscription, s: sub}, nil
}

// Ping is the liveness check: no I/O, no topology, no consumption.
func (b *fakeBroker) Ping(_ context.Context) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return mq.ErrClosed
	}
	return nil
}

// Native is the escape hatch.
func (b *fakeBroker) Native() any { return b }

// Close releases the broker.
func (b *fakeBroker) Close() error {
	b.mu.Lock()
	first := !b.closed
	b.closed = true
	b.mu.Unlock()
	if first {
		// The specification closes native clients LAST, and an order is the only way to
		// check that, so the backend's own close is in the event log too.
		b.event("backend-close")
	}
	return nil
}

// sinkConfirmation is the confirmation boundary the sink for topic was
// opened with, and whether one was opened at all.
func (b *fakeBroker) sinkConfirmation(topic string) (mq.ConfirmationMode, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	c, ok := b.sinkConfirm[topic]
	return c, ok
}

// --- publication ----------------------------------------------------------

// fakeSink is the whole of a driver with no native batching: one method and
// a close (the specification's "a driver that cannot batch implements one method").
type fakeSink struct {
	b     *fakeBroker
	topic string

	mu     sync.Mutex
	closed bool
}

// Publish admits one message.
func (s *fakeSink) Publish(ctx context.Context, m *mq.Message, o *mq.PublishOptions) error {
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
	s.b.mu.Lock()
	s.b.calls++
	err := s.b.sendLocked(s.topic, m)
	s.b.mu.Unlock()
	return err
}

// Close releases the sink.
func (s *fakeSink) Close() error {
	s.mu.Lock()
	first := !s.closed
	s.closed = true
	s.mu.Unlock()
	if first {
		s.b.event("sink-close")
	}
	return nil
}

// fakeBatchSink is the same driver with the one optional method a native
// batcher adds (BatchSink).
type fakeBatchSink struct{ fakeSink }

// PublishBatch admits a batch and answers per message through
// driverkit.CollapseBatch, which is what a driver author is given for it:
// nil when every message was accepted, a *BatchError index-aligned with ms
// when some were or when none were but their outcomes differ, and a single
// error when none were and every message's outcome is the same.
func (s *fakeBatchSink) PublishBatch(ctx context.Context, ms []*mq.Message, o *mq.PublishOptions) error {
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
	s.b.mu.Lock()
	s.b.calls++
	errs := make([]error, len(ms))
	for i, m := range ms {
		errs[i] = s.b.sendLocked(s.topic, m)
	}
	s.b.mu.Unlock()
	return driverkit.CollapseBatch(errs)
}

// sendLocked is the native client: it asks the fault injector, records the
// sequence it accepted and fans the message out to every subscription.
//
// It clones the message, which is what the specification requires of a driver that
// retains one: "drivers encode synchronously without copying or copy if they
// retain".
func (b *fakeBroker) sendLocked(topic string, m *mq.Message) error {
	b.seq++
	seq := b.seq
	if b.refuse != nil {
		if err := b.refuse(seq, m); err != nil {
			return err
		}
	}
	if b.closed {
		return mq.ErrClosed
	}
	t, ok := b.topics[topic]
	if !ok {
		return mq.ErrTopicNotFound
	}
	b.sent = append(b.sent, seq)
	for _, sub := range t.subs {
		if sub.closed {
			continue
		}
		sub.nextTok++
		sub.ready = append(sub.ready, &fakeRecord{msg: m.Clone(), tok: sub.nextTok})
		sub.published++
		sub.signal()
	}
	if b.lose != nil {
		return b.lose(seq, m)
	}
	return nil
}

// event appends to the ordered event log.
func (b *fakeBroker) event(name string) {
	b.mu.Lock()
	b.events = append(b.events, name)
	b.mu.Unlock()
}

// eventLog returns a copy of the ordered event log.
func (b *fakeBroker) eventLog() []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make([]string, len(b.events))
	copy(out, b.events)
	return out
}

// signal wakes a parked fetch without blocking on one that is not parked.
func (s *fakeSub) signal() {
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

// --- consumption ----------------------------------------------------------

// fakeSource is one open subscription.
type fakeSource struct {
	b     *fakeBroker
	topic string
	sub   string
	s     *fakeSub
	once  sync.Once
}

// Fetch blocks until at least one delivery is available, ctx is done or the
// source is closed, and fills dst(n].
func (src *fakeSource) Fetch(ctx context.Context, dst []mq.DriverDelivery) (int, error) {
	for {
		src.b.mu.Lock()
		if src.s.closed || src.b.closed {
			src.b.mu.Unlock()
			return 0, mq.ErrClosed
		}
		n := min(len(dst), len(src.s.ready))
		for i := range n {
			rec := src.s.ready[i]
			// A new generation for every delivery and redelivery, which is
			// what makes a stale settlement detectable here.
			rec.gen++
			rec.attempt++
			src.s.inflight[rec.tok] = rec
			now := time.Now()
			var expires time.Time
			switch {
			case src.b.cfg.lease < 0:
			case src.b.cfg.lease > 0:
				expires = now.Add(src.b.cfg.lease)
			default:
				expires = now.Add(fakeLease)
			}
			rec.lease = expires
			attempt := rec.attempt
			if src.b.cfg.countless {
				attempt = 0
			}
			part := fakePartition
			if src.b.cfg.partitions > 0 {
				part = int32(rec.tok % uint64(src.b.cfg.partitions))
			}
			dst[i] = mq.DriverDelivery{
				Message: rec.msg,
				// Every field the specification gives a broker somewhere to put, so
				// that the metadata a caller reads is the driver's and not
				// a default: the driver pages document the partition
				// and the offset, and a Has flag is what keeps a
				// partition of zero from meaning "the broker did not say".
				Info: mq.DeliveryInfo{
					Topic:        src.topic,
					Source:       fakeStream + src.topic,
					Subscription: src.sub,
					ReceivedAt:   now,
					LeaseExpires: expires,
					Attempt:      attempt,
					// A broker that counts no attempts may still flag a
					// redelivery, which is the one thing that tells it apart
					// from a first delivery.
					Redelivered:  rec.attempt > 1,
					Partition:    part,
					HasPartition: true,
					Offset:       int64(rec.tok),
					HasOffset:    true,
					Sequence:     rec.tok,
					HasSequence:  true,
				},
				Token: rec.tok,
				Gen:   rec.gen,
				// The driver's own delivery object, which is what
				// Delivery.Native and NativeAs hand back.
				Native: rec,
			}
		}
		if n > 0 {
			src.s.ready = src.s.ready[n:]
			src.b.mu.Unlock()
			return n, nil
		}
		src.b.mu.Unlock()
		select {
		case <-src.s.wake:
		case <-src.s.gone:
			return 0, mq.ErrClosed
		case <-ctx.Done():
			return 0, ctx.Err()
		}
	}
}

// Settler settles this source's deliveries. A nil settler is AckNone
// model and the core answers every settlement with ErrNotSettleable.
//
// A broker with a visibility timeout answers with a settler that also
// implements DeadlineExtender. It is a distinct type for the reason
// fakeBatchSink is: a settler that merely returned ErrUnsupported from
// Extend would still satisfy the interface, so the core's assertion would
// never fail for it and the ErrUnsupported half of Delivery.Extend would be
// the driver's answer rather than the core's.
func (src *fakeSource) Settler() mq.Settler {
	if src.b.cfg.noSettler {
		return nil
	}
	st := &fakeSettler{b: src.b, s: src.s}
	if src.b.cfg.extend {
		return fakeExtendingSettler{fakeSettler: st}
	}
	return st
}

// Close requeues everything the source handed out and never had settled,
// and unblocks a parked Fetch. Both are the obligations, and the requeue is the specification's "closing releases/requeues all records per policy": a message the
// core was told ErrClosed about is redelivered rather than lost.
func (src *fakeSource) Close() error {
	src.once.Do(func() {
		src.b.event("source-close")
		src.b.mu.Lock()
		src.s.closed = true
		for tok, rec := range src.s.inflight {
			delete(src.s.inflight, tok)
			src.s.ready = append(src.s.ready, rec)
			src.s.requeued++
		}
		src.b.mu.Unlock()
		close(src.s.gone)
	})
	return nil
}

// fakeSettler is the driver's settlement surface. It answers for the native
// action only: the core has already decided that this attempt is the one
// that may proceed.
type fakeSettler struct {
	b *fakeBroker
	s *fakeSub
}

// Ack settles a delivery positively.
func (st *fakeSettler) Ack(ctx context.Context, d *mq.DriverDelivery) error {
	st.b.event("settle-enter")
	defer st.b.event("settle-leave")
	if err := st.hold(ctx, d); err != nil {
		return err
	}
	st.b.mu.Lock()
	defer st.b.mu.Unlock()
	if st.b.cfg.settlePermissive {
		st.s.acked++
		return nil
	}
	rec, err := st.claimLocked(ctx, d)
	if err != nil {
		return err
	}
	delete(st.s.inflight, rec.tok)
	st.s.acked++
	return nil
}

// Nack settles a delivery negatively, under the action it was given.
func (st *fakeSettler) Nack(ctx context.Context, d *mq.DriverDelivery, o mq.NackOptions) error {
	st.b.event("settle-enter")
	defer st.b.event("settle-leave")
	if err := st.hold(ctx, d); err != nil {
		return err
	}
	st.b.mu.Lock()
	defer st.b.mu.Unlock()
	rec, err := st.claimLocked(ctx, d)
	if err != nil {
		return err
	}
	switch o.Action {
	case mq.NackActionDeadLetter:
		if !st.b.cfg.deadLetter {
			// The specification: a driver that can neither dead-letter natively nor
			// emulate it answers ErrUnsupported and settles nothing.
			return mq.ErrUnsupported
		}
		delete(st.s.inflight, rec.tok)
		st.s.deadLettered++
	case mq.NackActionDrop:
		delete(st.s.inflight, rec.tok)
		st.s.dropped++
	case mq.NackActionRequeue:
		if o.Delay > 0 && !st.b.cfg.delayed {
			return mq.ErrUnsupported
		}
		delete(st.s.inflight, rec.tok)
		// Redelivery first, the way a broker puts a nacked message back.
		st.s.ready = append([]*fakeRecord{rec}, st.s.ready...)
		st.s.requeued++
		st.s.signal()
	default:
		return mq.ErrUnsupported
	}
	return nil
}

// fakeExtendingSettler is the settler of a broker whose deliveries have a
// lease: fakeSettler plus the one optional method.
type fakeExtendingSettler struct {
	*fakeSettler
}

// Extend moves the delivery's lease. Like every other driver call it holds
// at the gate when one is installed and honours the context, which is what
// lets a test see whether an extension can still be inside the driver while
// the source is being closed.
func (st fakeExtendingSettler) Extend(ctx context.Context, d *mq.DriverDelivery, by time.Duration) error {
	st.b.event("extend-enter")
	defer st.b.event("extend-leave")
	if err := st.hold(ctx, d); err != nil {
		return err
	}
	st.b.mu.Lock()
	g := st.b.extendGate
	st.b.mu.Unlock()
	if err := waitGate(ctx, g, d); err != nil {
		return err
	}
	st.b.mu.Lock()
	defer st.b.mu.Unlock()
	// The staleness check is the settlement's: an extension of a delivery
	// the broker has taken away is refused with ErrLeaseLost, exactly as an
	// ack of it would be.
	rec, err := st.claimLocked(ctx, d)
	if err != nil {
		return err
	}
	st.s.extended++
	st.s.extendedBy += by
	// DeadlineExtender moves the deadline "by at least by", so the
	// deadline becomes by from now and an extension never SHORTENS a lease
	// that already runs longer -- the defect drivers/sqs had. Without this the extension is a counter and no test can tell a
	// scheduler that keeps a lease alive from one that does not.
	if until := time.Now().Add(by); until.After(rec.lease) {
		rec.lease = until
	}
	return nil
}

// hold is the gate: it announces the settlement and waits, so that a test
// can observe a delivery in the settling state. It holds no lock while it
// waits, and it honours the context — which is what lets a consumer's close
// unblock a settlement parked in a driver.
func (st *fakeSettler) hold(ctx context.Context, d *mq.DriverDelivery) error {
	st.b.mu.Lock()
	g := st.b.gate
	st.b.mu.Unlock()
	return waitGate(ctx, g, d)
}

// waitGate announces d at g and waits for g's release; a nil gate holds
// nothing.
func waitGate(ctx context.Context, g *settleGate, d *mq.DriverDelivery) error {
	if g == nil {
		return nil
	}
	select {
	case g.entered <- d.Token:
	default:
	}
	select {
	case <-g.release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// claimLocked is the driver's own staleness check: the token must name an
// in-flight record and the generation must be the one that record was last
// delivered under. It is where the specification's "stale tokens cannot settle newer
// deliveries" lives for a real broker.
func (st *fakeSettler) claimLocked(ctx context.Context, d *mq.DriverDelivery) (*fakeRecord, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if st.b.settlerErr != nil {
		return nil, st.b.settlerErr
	}
	if st.b.settlerFails > 0 {
		st.b.settlerFails--
		return nil, errSettleRefused
	}
	if st.b.ambiguous[d.Token] {
		// The round trip may or may not have been applied: the broker
		// cannot say, so it says so (CodeSettlementUnknown).
		return nil, mq.ErrSettlementUnknown
	}
	if st.s.closed {
		return nil, mq.ErrClosed
	}
	rec, ok := st.s.inflight[d.Token]
	if !ok || rec.gen != d.Gen {
		return nil, mq.ErrLeaseLost
	}
	return rec, nil
}

// --- the bookkeeping the tests read ---------------------------------------

// fakeCounts is one subscription's record accounting.
type fakeCounts struct {
	published    int
	ready        int
	inflight     int
	acked        int
	requeued     int
	dropped      int
	deadLettered int
	extended     int
	extendedBy   time.Duration
}

// conserved reports the conservation for one subscription: every
// published record is in exactly one place.
func (c fakeCounts) conserved() bool {
	return c.published == c.acked+c.ready+c.inflight+c.dropped+c.deadLettered
}

// counts reads one subscription's accounting.
func (b *fakeBroker) counts(topic, sub string) fakeCounts {
	b.mu.Lock()
	defer b.mu.Unlock()
	t, ok := b.topics[topic]
	if !ok {
		return fakeCounts{}
	}
	s, ok := t.subs[sub]
	if !ok {
		return fakeCounts{}
	}
	return fakeCounts{
		published:    s.published,
		ready:        len(s.ready),
		inflight:     len(s.inflight),
		acked:        s.acked,
		requeued:     s.requeued,
		dropped:      s.dropped,
		deadLettered: s.deadLettered,
		extended:     s.extended,
		extendedBy:   s.extendedBy,
	}
}

// expire redelivers every in-flight record of a subscription under a new
// generation, which is what a lease expiry or a rebalance does. A settlement
// holding the previous generation is then stale.
func (b *fakeBroker) expire(topic, sub string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	s := b.topics[topic].subs[sub]
	for tok, rec := range s.inflight {
		delete(s.inflight, tok)
		s.ready = append(s.ready, rec)
		s.requeued++
	}
	s.signal()
}

// expireLeases is the broker's own visibility sweep: every in-flight record
// whose lease deadline has passed goes back to ready, and one whose lease the
// consumer has extended stays where it is. It returns how many it took back.
//
// It is what makes the scheduler observable from outside. The
// unconditional expire above models a rebalance, which takes a delivery back
// whatever its lease says; this one models the deadline, so a test can hold a
// handler past its lease and watch the delivery survive only when something
// is extending it.
func (b *fakeBroker) expireLeases(topic, sub string) int {
	b.mu.Lock()
	defer b.mu.Unlock()
	s := b.topics[topic].subs[sub]
	now := time.Now()
	took := 0
	for tok, rec := range s.inflight {
		if rec.lease.IsZero() || rec.lease.After(now) {
			continue
		}
		delete(s.inflight, tok)
		s.ready = append(s.ready, rec)
		s.requeued++
		took++
	}
	if took > 0 {
		s.signal()
	}
	return took
}

// setGate installs or removes the settlement gate.
func (b *fakeBroker) setGate(g *settleGate) {
	b.mu.Lock()
	b.gate = g
	b.mu.Unlock()
}

// setAmbiguous makes the next settlement of tok answer "unknown".
func (b *fakeBroker) setAmbiguous(tok uint64, on bool) {
	b.mu.Lock()
	b.ambiguous[tok] = on
	b.mu.Unlock()
}

// setRefuse installs the publication fault injector.
func (b *fakeBroker) setRefuse(f func(seq int, m *mq.Message) error) {
	b.mu.Lock()
	b.refuse = f
	b.mu.Unlock()
}

// setLose installs the fault injector that stores a publication and then
// loses its answer.
func (b *fakeBroker) setLose(f func(seq int, m *mq.Message) error) {
	b.mu.Lock()
	b.lose = f
	b.mu.Unlock()
}

// nativeCalls reports how many calls the sinks made to the native client,
// which is what "one batch, one call" means.
func (b *fakeBroker) nativeCalls() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.calls
}

// sentSeqs reports the sequences the native client accepted, in order.
func (b *fakeBroker) sentSeqs() []int {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make([]int, len(b.sent))
	copy(out, b.sent)
	return out
}

// --- the harness ----------------------------------------------------------

// openFake builds a client over a fake broker with one topic and returns
// both. The client is closed by the test's cleanup, which is also where the
// "no goroutines remain" half is checked.
func openFake(tb testing.TB, cfg fakeConfig) (*mq.Client, *fakeBroker) {
	tb.Helper()
	b := newFakeBroker(cfg, "orders")
	cl := mq.NewClient(b)
	tb.Cleanup(func() {
		if err := cl.Close(); err != nil {
			tb.Errorf("closing the client: %v", err)
		}
	})
	return cl, b
}

// consume opens a consumer on the fake's topic.
func consume(tb testing.TB, cl *mq.Client, cfg mq.ConsumerConfig) *mq.ConsumerHandle {
	tb.Helper()
	if cfg.Topic == "" {
		cfg.Topic = "orders"
	}
	if cfg.Subscription == "" {
		cfg.Subscription = "billing"
	}
	c, err := cl.Consumer(context.Background(), cfg)
	if err != nil {
		tb.Fatalf("opening the consumer: %v", err)
	}
	return c
}

// publishN publishes n messages whose ID is their index.
func publishN(tb testing.TB, cl *mq.Client, n int) {
	tb.Helper()
	ctx := context.Background()
	for i := range n {
		m := &mq.Message{ID: strconv.Itoa(i), Body: []byte("body")}
		if err := cl.Publish(ctx, "orders", m); err != nil {
			tb.Fatalf("publishing %d: %v", i, err)
		}
	}
}

// receiveOne receives exactly one delivery.
func receiveOne(tb testing.TB, c *mq.ConsumerHandle) mq.Delivery {
	tb.Helper()
	dst := make([]mq.Delivery, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	n, err := c.Receive(ctx, dst)
	if err != nil || n != 1 {
		tb.Fatalf("Receive = (%d, %v), want (1, nil)", n, err)
	}
	return dst[0]
}

// TestSPIMandatorySurfaceIsOneMethod pins the finding at the type
// level: a driver with no native batching implements Sink and nothing else,
// and the core's one assertion at open is what tells the two apart.
//
// The negative half is the half that matters. A test that only checked the
// batching sink would pass for a core that never used the adapter at all.
func TestSPIMandatorySurfaceIsOneMethod(t *testing.T) {
	ctx := context.Background()
	plain := newFakeBroker(fakeConfig{}, "orders")
	sink, err := plain.OpenSink(ctx, mq.SinkConfig{Topic: "orders"})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := sink.(mq.BatchSink); ok {
		t.Fatal("the non-batching driver's sink satisfies BatchSink; the core's assertion at open can never fail and the adapter is dead code")
	}
	if _, ok := sink.(mq.FlushSink); ok {
		t.Fatal("the non-batching driver's sink satisfies FlushSink")
	}
	batching := newFakeBroker(fakeConfig{batching: true}, "orders")
	bsink, err := batching.OpenSink(ctx, mq.SinkConfig{Topic: "orders"})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := bsink.(mq.BatchSink); !ok {
		t.Fatal("the batching driver's sink does not satisfy BatchSink")
	}
}

// TestSPIPublishDoesNotRetainTheCallersMessage pins the synchronous
// publish contract at the SPI boundary: the caller may reuse and mutate
// everything it handed over the moment Publish returns, so a driver that
// keeps the message has to have copied it.
func TestSPIPublishDoesNotRetainTheCallersMessage(t *testing.T) {
	cl, b := openFake(t, fakeConfig{})
	c := consume(t, cl, mq.ConsumerConfig{})
	m := &mq.Message{Body: []byte("original"), Key: []byte("k1")}
	if err := cl.Publish(context.Background(), "orders", m); err != nil {
		t.Fatal(err)
	}
	// The specification: "Caller may reuse/mutate referenced data after return".
	copy(m.Body, "mutated!")
	m.Key[0] = 'X'
	d := receiveOne(t, c)
	if string(d.Body) != "original" {
		t.Fatalf("the delivered body is %q: the driver kept the caller's storage", d.Body)
	}
	if string(d.Key) != "k1" {
		t.Fatalf("the delivered key is %q: the driver kept the caller's storage", d.Key)
	}
	if err := d.Ack(); err != nil {
		t.Fatal(err)
	}
	if got := b.counts("orders", "billing"); got.acked != 1 {
		t.Fatalf("the broker counted %d acks, want 1", got.acked)
	}
}

// TestSPIFetchFillsTheCallersSlice pins the winner at the SPI boundary:
// a fetch fills a slice the core owns and reuses, so neither the fetch nor
// the receive above it allocates a container per batch.
func TestSPIFetchFillsTheCallersSlice(t *testing.T) {
	cl, _ := openFake(t, fakeConfig{})
	c := consume(t, cl, mq.ConsumerConfig{})
	publishN(t, cl, 5)
	dst := make([]mq.Delivery, 3)
	n, err := c.Receive(context.Background(), dst)
	if err != nil {
		t.Fatal(err)
	}
	if n != 3 {
		t.Fatalf("Receive filled %d of 3", n)
	}
	for i := range n {
		if dst[i].Attempt != 1 {
			t.Fatalf("delivery %d is attempt %d, want 1", i, dst[i].Attempt)
		}
		if dst[i].ID != strconv.Itoa(i) {
			t.Fatalf("delivery %d is message %q, want %q", i, dst[i].ID, strconv.Itoa(i))
		}
		if err := dst[i].Ack(); err != nil {
			t.Fatalf("acking delivery %d: %v", i, err)
		}
	}
	n, err = c.Receive(context.Background(), dst)
	if err != nil || n != 2 {
		t.Fatalf("the second Receive = (%d, %v), want (2, nil)", n, err)
	}
	for i := range n {
		if err := dst[i].Ack(); err != nil {
			t.Fatal(err)
		}
	}
	if got := c.Stats(); got.Received != 5 || got.Acked != 5 || got.InFlight != 0 {
		t.Fatalf("Stats = %+v, want 5 received, 5 acked, 0 in flight", got)
	}
}

// TestSPIRedeliveryCountsAttempts pins the specification's "attempts increment per
// delivery": a nacked message comes back, and it comes back knowing it has
// been here before.
func TestSPIRedeliveryCountsAttempts(t *testing.T) {
	cl, _ := openFake(t, fakeConfig{})
	c := consume(t, cl, mq.ConsumerConfig{})
	publishN(t, cl, 1)
	first := receiveOne(t, c)
	if first.Attempt != 1 {
		t.Fatalf("the first delivery is attempt %d, want 1", first.Attempt)
	}
	if err := first.Nack(); err != nil {
		t.Fatalf("Nack: %v", err)
	}
	second := receiveOne(t, c)
	if second.Attempt != 2 {
		t.Fatalf("the redelivery is attempt %d, want 2", second.Attempt)
	}
	if err := second.Ack(); err != nil {
		t.Fatal(err)
	}
	if got := c.Stats(); got.Redelivered != 1 || got.Nacked != 1 || got.Acked != 1 {
		t.Fatalf("Stats = %+v, want 1 redelivered, 1 nacked, 1 acked", got)
	}
}

// TestSPINackActionsReachTheDriver pins the nack actions, including the
// one a driver cannot honour: dead-lettering on a broker with no
// dead-letter destination answers ErrUnsupported and settles nothing, so the
// delivery is still settleable afterwards.
func TestSPINackActionsReachTheDriver(t *testing.T) {
	t.Run("requeue", func(t *testing.T) {
		cl, b := openFake(t, fakeConfig{})
		c := consume(t, cl, mq.ConsumerConfig{})
		publishN(t, cl, 1)
		d := receiveOne(t, c)
		if err := d.Nack(mq.NackRequeue()); err != nil {
			t.Fatal(err)
		}
		if got := b.counts("orders", "billing"); got.ready != 1 || got.inflight != 0 {
			t.Fatalf("after a requeue the broker holds %+v, want 1 ready and 0 in flight", got)
		}
	})
	t.Run("drop", func(t *testing.T) {
		cl, b := openFake(t, fakeConfig{})
		c := consume(t, cl, mq.ConsumerConfig{})
		publishN(t, cl, 1)
		d := receiveOne(t, c)
		if err := d.Nack(mq.NackDrop(), mq.NackReason("poison")); err != nil {
			t.Fatal(err)
		}
		if got := b.counts("orders", "billing"); got.dropped != 1 || got.ready != 0 {
			t.Fatalf("after a drop the broker holds %+v, want 1 dropped and 0 ready", got)
		}
	})
	t.Run("deadletter", func(t *testing.T) {
		cl, b := openFake(t, fakeConfig{deadLetter: true})
		c := consume(t, cl, mq.ConsumerConfig{})
		publishN(t, cl, 1)
		d := receiveOne(t, c)
		if err := d.Reject(); err != nil {
			t.Fatal(err)
		}
		if got := b.counts("orders", "billing"); got.deadLettered != 1 {
			t.Fatalf("after a Reject the broker holds %+v, want 1 dead-lettered", got)
		}
		if got := c.Stats(); got.DeadLettered != 1 || got.Nacked != 1 {
			t.Fatalf("Stats = %+v, want 1 dead-lettered and 1 nacked", got)
		}
	})
	t.Run("deadletterunsupported", func(t *testing.T) {
		cl, b := openFake(t, fakeConfig{})
		c := consume(t, cl, mq.ConsumerConfig{})
		publishN(t, cl, 1)
		d := receiveOne(t, c)
		if err := d.Reject(); !errors.Is(err, mq.ErrUnsupported) {
			t.Fatalf("Reject on a broker with no dead-letter destination = %v, want ErrUnsupported", err)
		}
		if d.Settled() {
			t.Fatal("a refused settlement reported the delivery settled")
		}
		// The specification: a definite refusal leaves the delivery settleable.
		if err := d.Ack(); err != nil {
			t.Fatalf("acking after a refused Reject = %v, want nil", err)
		}
		if got := b.counts("orders", "billing"); got.acked != 1 || got.deadLettered != 0 {
			t.Fatalf("the broker holds %+v, want 1 acked and 0 dead-lettered", got)
		}
	})
	t.Run("delayunsupported", func(t *testing.T) {
		cl, _ := openFake(t, fakeConfig{})
		c := consume(t, cl, mq.ConsumerConfig{})
		publishN(t, cl, 1)
		d := receiveOne(t, c)
		if err := d.Nack(mq.NackDelay(time.Minute)); !errors.Is(err, mq.ErrUnsupported) {
			t.Fatalf("a delayed Nack on a broker without native delay = %v, want ErrUnsupported", err)
		}
		if err := d.Nack(); err != nil {
			t.Fatalf("an undelayed Nack afterwards = %v, want nil", err)
		}
	})
}

// TestSPIOptionsFailBeforeIO pins the rule that an unsupported populated
// control fails before any I/O: the broker is never told about a publication
// carrying a control it cannot honour.
func TestSPIOptionsFailBeforeIO(t *testing.T) {
	cl, b := openFake(t, fakeConfig{maxBytes: 8, maxHeaders: 2})
	ctx := context.Background()
	p, err := cl.Publisher(ctx, mq.PublisherConfig{Topic: "orders"})
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name string
		call func() error
		want error
	}{
		{"delay", func() error {
			return p.Publish(ctx, &mq.Message{Body: []byte("x")}, mq.Delay(time.Minute))
		}, mq.ErrUnsupported},
		{"ttl", func() error {
			return p.Publish(ctx, &mq.Message{Body: []byte("x")}, mq.TTL(time.Minute))
		}, mq.ErrUnsupported},
		{"priority", func() error {
			return p.Publish(ctx, &mq.Message{Body: []byte("x")}, mq.Priority(3))
		}, mq.ErrUnsupported},
		{"dedup", func() error {
			return p.Publish(ctx, &mq.Message{Body: []byte("x")}, mq.DedupKey("k"))
		}, mq.ErrUnsupported},
		{"toolarge", func() error {
			return p.Publish(ctx, &mq.Message{Body: []byte("123456789")})
		}, mq.ErrMessageTooLarge},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			before := b.nativeCalls()
			if err := tc.call(); !errors.Is(err, tc.want) {
				t.Fatalf("= %v, want %v", err, tc.want)
			}
			if after := b.nativeCalls(); after != before {
				t.Fatalf("the driver was called %d times: the specification says the control fails before any I/O", after-before)
			}
		})
	}
	if err := p.Publish(ctx, &mq.Message{Body: []byte("ok")}); err != nil {
		t.Fatalf("a publication with no unsupported control = %v", err)
	}
}
