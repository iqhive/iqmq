package pulsar

import (
	"context"
	"math"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit"
	"github.com/iqhive/iqmq/drivers/pulsar/internal/pulsarapi"
	"github.com/iqhive/iqmq/internal/errcode"
)

// The compile-time proof of what this source is.
var _ mq.Source = (*source)(nil)

// minSweepInterval bounds how often the source's one scheduler runs, so that a
// very short lease guard does not turn into a busy loop.
const minSweepInterval = 25 * time.Millisecond

// defaultSweepInterval is how often the scheduler runs when there is no lease
// to guard and it is only there for the delayed negative settlements.
const defaultSweepInterval = 100 * time.Millisecond

// The settlement states, which are the as one source sees them.
const (
	stateActive  uint32 = iota // handed out, not settled
	stateSettled               // the settlement was written
	stateLost                  // this source no longer vouches for it
	stateUnknown               // the settlement's outcome is unknown, and stays so
)

// source is one open subscription: the Pulsar consumer, the bounded channel it
// pushes into, and the fetch that drains it.
//
// There is no [driverkit.PushAdapter] here and the absence is deliberate. The
// kit's adapter exists to turn a native CALLBACK into a bounded ring, and
// Pulsar's consumer does not call back — pulsar-client-go writes into a channel
// the caller supplies (`ConsumerOptions.MessageChannel`), which already IS the
// bounded hand-off, and behind it sits the per-partition receiver queue and the
// protocol's own flow-control permits. Putting a ring in front of that would be
// a second buffer of the same messages: twice the memory a prefetch asks for,
// and a delivery whose lease is running down in a queue the broker cannot see.
// The channel's capacity is the prefetch, so a consumer that stops fetching
// stops the permits, which is what the specification asks of backpressure.
type source struct {
	b       *Backend
	cons    pulsarapi.Consumer
	ch      chan pulsar.ConsumerMessage
	topic   string
	iqTopic string
	sub     string
	lease   time.Duration
	guard   time.Duration
	meta    mq.MetadataMode
	env     *driverkit.Envelope
	rules   AckRules
	pattern bool
	// volatile marks a non-persistent topic, whose positions do not name a
	// message: see [source.slotOf].
	volatile bool
	// retry is the fully qualified retry topic the client republishes a
	// delayed requeue to, and empty without Config.RetryEnable. A requeue
	// names it on the message ([carriedAttempts]), which is how [AttemptOf]
	// tells the hop from a dead-letter copy.
	retry string

	// done is closed by Close, which is what unblocks a parked Fetch:
	// pulsar-client-go's Close does not close the message channel the caller
	// supplied, so the channel alone cannot wake a fetch.
	done   chan struct{}
	timer  mq.Timer
	once   sync.Once
	wg     sync.WaitGroup
	closed atomic.Bool

	gen atomic.Uint32

	// mu guards inflight and held.
	//
	// inflight maps a message's POSITION ([source.slotOf]) to the delivery
	// this source currently vouches for. It is the stale-settlement defence, and Pulsar
	// needs one: a redelivered message carries the SAME (ledger, entry,
	// batch index), and the broker will accept an acknowledgement through
	// either delivery's identifier. So the older delivery is marked lost the
	// moment the newer one arrives, and a settlement for it reports
	// ErrLeaseLost rather than acknowledging a message this process is
	// working on under a different delivery.
	mu       sync.Mutex
	inflight map[BatchPosition]*delivery
	held     []heldNack

	settleFn *settler
}

// heldNack is a delayed negative settlement waiting for its due time, which is EmulatedVolatile delay for a subscription with no retry topic.
type heldNack struct {
	d   *delivery
	due time.Time
}

// OpenSource opens a subscription: a Pulsar subscription on a topic or on a
// pattern.
func (b *Backend) OpenSource(ctx context.Context, cfg mq.SourceConfig) (mq.Source, error) {
	if err := b.CheckOpen(ctx); err != nil {
		return nil, err
	}
	if cfg.Prefetch <= 0 {
		return nil, errUnresolvedPrefetch
	}
	if !cfg.Metadata.Valid() {
		return nil, errMetadataMode
	}
	sub, err := SubscriptionOf(cfg.Subscription)
	if err != nil {
		return nil, err
	}
	start := cfg.Start.Resolve()
	if start.Kind() == mq.StartKindSequence {
		return nil, errNoStartSequence
	}
	pattern := HasPattern(cfg.Topic)
	if start.Kind() == mq.StartKindTime && (pattern || b.cfg.RetryEnable) {
		// pulsar-client-go refuses a seek on a multi-topic consumer, and
		// a pattern subscription and a retry-topic subscription are both
		// multi-topic. Refusing here is and it is also what makes
		// the capability table's denial provable.
		return nil, errNoSeekMultiTopic
	}
	mode := b.metadataMode(cfg.Metadata)
	env, err := driverkit.NewEnvelope(driverkit.EnvelopeConfig{Mode: mode, Limits: b.limit, Capabilities: b.caps})
	if err != nil {
		return nil, err
	}
	lease, guard := b.leaseFor(cfg.AckDeadline)

	opts, topic, err := b.consumerOptions(cfg, sub, start, pattern)
	if err != nil {
		return nil, err
	}
	if b.cfg.EnsureTopology && !pattern {
		t, terr := TopicOf(b.cfg, cfg.Topic)
		if terr != nil {
			return nil, terr
		}
		if err := b.ensureTopic(ctx, t); err != nil {
			return nil, err
		}
	}
	s := &source{
		b: b, ch: opts.MessageChannel, topic: topic, iqTopic: cfg.Topic, sub: sub,
		lease: lease, guard: guard, meta: mode, env: env, pattern: pattern,
		volatile: strings.HasPrefix(topic, DomainNonPersistent+Separator),
		rules:    b.ackRules(),
		done:     make(chan struct{}),
		inflight: map[BatchPosition]*delivery{},
	}
	s.settleFn = &settler{src: s}
	if opts.DLQ != nil {
		s.retry = opts.DLQ.RetryLetterTopic
	}

	cons, err := b.cl.Subscribe(opts)
	if err != nil {
		return nil, b.errs.Map(mq.OpOpen, cfg.Topic, sub, err)
	}
	s.cons = cons
	if t, ok := cons.(pulsarapi.BatchTracker); ok {
		// A consumer that can say whether its identifiers carry the
		// batch bookkeeping is asked; one that cannot is read as
		// "tracked", which is what pulsar-client-go v0.21's own consumer
		// does. See pulsarapi.BatchTracker for why the question cannot
		// be answered from the MessageID itself.
		s.rules.Tracked = t.BatchTracked()
	}
	if start.Kind() == mq.StartKindTime {
		if err := cons.SeekByTime(start.Time()); err != nil {
			cons.Close()
			return nil, b.errs.Map(mq.OpOpen, cfg.Topic, sub, err)
		}
	}
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		_ = s.Close()
		return nil, mq.ErrClosed
	}
	b.srcs[s] = struct{}{}
	if b.pingTopic == "" && !pattern {
		b.pingTopic = topic
	}
	b.mu.Unlock()
	// The scheduler's timer is created HERE and not inside the goroutine,
	// and the difference is a race a test found rather than a review: with
	// the timer created by the goroutine, a caller that moved an injected
	// clock immediately after OpenSource could fire nothing, because the
	// timer did not exist yet — and the sweeper would then be waiting on a
	// deadline in the clock's future for ever.
	s.timer = b.newTimer(s.sweepInterval())
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		s.sweep()
	}()
	return s, nil
}

// leaseFor is the delivery lease this source grants: the consumer's own ask
// when it made one, and the configuration's default otherwise.
//
// A negative AckDeadline is the specification's "leases that never expire", and the honest
// answer for an emulated lease is no lease at all rather than a very long one:
// the driver can simply not track a deadline.
func (b *Backend) leaseFor(ask time.Duration) (lease, guard time.Duration) {
	switch {
	case b.cfg.DisableAckTimeout, ask < 0:
		return 0, 0
	case ask > 0:
		g := min(b.cfg.LeaseGuard, ask/5)
		if g <= 0 {
			g = minSweepInterval
		}
		return ask, g
	default:
		return b.cfg.AckTimeout, b.cfg.LeaseGuard
	}
}

// ackRules are the conditions this backend's acknowledgements are subject to.
func (b *Backend) ackRules() AckRules {
	return AckRules{
		Tracked:             true,
		ClientBatchIndexAck: b.cfg.EnableBatchIndexAck,
		BrokerBatchIndexAck: b.broker.batchIndexAck,
	}
}

// consumerOptions is the pulsar-client-go consumer configuration for one
// subscription, and the topic or pattern it is opened on.
func (b *Backend) consumerOptions(cfg mq.SourceConfig, sub string, start mq.StartPosition, pattern bool) (pulsar.ConsumerOptions, string, error) {
	queue := b.cfg.ReceiverQueueSize
	if queue <= 0 {
		queue = cfg.Prefetch
	}
	o := pulsar.ConsumerOptions{
		SubscriptionName: sub,
		Type:             b.cfg.SubscriptionType.Native(),
		// The bounded hand-off the specification asks for. Its capacity is the
		// prefetch, so a consumer that stops fetching stops the
		// protocol's permits rather than buffering without limit.
		MessageChannel:                 make(chan pulsar.ConsumerMessage, cfg.Prefetch),
		ReceiverQueueSize:              queue,
		NackRedeliveryDelay:            b.cfg.NackRedeliveryDelay,
		ReadCompacted:                  b.cfg.ReadCompacted,
		RetryEnable:                    b.cfg.RetryEnable,
		EnableBatchIndexAcknowledgment: b.cfg.EnableBatchIndexAck,
		AckWithResponse:                b.cfg.AckWithResponse,
		SubscriptionMode:               pulsar.Durable,
		AutoDiscoveryPeriod:            b.cfg.AutoDiscoveryPeriod,
		AckGroupingOptions: &pulsar.AckGroupingOptions{
			MaxSize: uint32(b.cfg.AckGroupingMaxSize),
			MaxTime: b.cfg.AckGroupingMaxTime,
		},
	}
	switch start.Kind() {
	case mq.StartKindEarliest, mq.StartKindTime:
		// A time is reached by creating the subscription at the
		// beginning and then seeking, so its initial position is the
		// earliest too.
		o.SubscriptionInitialPosition = pulsar.SubscriptionPositionEarliest
	default:
		o.SubscriptionInitialPosition = pulsar.SubscriptionPositionLatest
	}
	if b.cfg.SubscriptionType == SubscriptionKeyShared {
		o.KeySharedPolicy = &pulsar.KeySharedPolicy{
			Mode:                    pulsar.KeySharedPolicyModeAutoSplit,
			AllowOutOfOrderDelivery: b.cfg.AllowOutOfOrderDelivery,
		}
	}
	if b.cfg.HasDeadLetter() {
		dlq, err := TopicOf(b.cfg, b.cfg.DeadLetterTopic)
		if err != nil {
			return pulsar.ConsumerOptions{}, "", err
		}
		o.DLQ = &pulsar.DLQPolicy{
			MaxDeliveries:   b.cfg.MaxAttempts,
			DeadLetterTopic: dlq.String(),
		}
	}
	if b.cfg.RetryEnable && o.DLQ == nil {
		// A retry topic with no dead-letter destination still needs a
		// policy, because pulsar-client-go makes one up when it is given
		// none: `<topic>-<subscription>-DLQ` with MaxDeliveries 16, so
		// the sixteenth redelivery of a message went to a topic nobody
		// named, which Config.DeadLetterTopic says never happens. A
		// policy whose bound no count can pass keeps the client's
		// dead-letter route shut: the broker's redelivery count never
		// reaches math.MaxUint32, and the reconsume count is compared
		// with ">" as a uint32. The destination is spelled out only so
		// that the client does not probe the broker for a legacy name
		// on every subscribe; nothing is ever published to it.
		dlq, err := TopicOf(b.cfg, b.cfg.TopicPrefix+cfg.Topic+"-"+sub+pulsar.DlqTopicSuffix)
		if err != nil {
			return pulsar.ConsumerOptions{}, "", err
		}
		o.DLQ = &pulsar.DLQPolicy{
			MaxDeliveries:   math.MaxUint32,
			DeadLetterTopic: dlq.String(),
		}
	}
	if b.cfg.RetryEnable {
		retry := b.cfg.RetryTopic
		if retry == "" {
			// Pulsar's own convention, spelled out rather than left
			// to the client's topic resolution: the client would
			// otherwise probe the broker for a legacy name on every
			// subscribe. It used to be set only beside a dead-letter
			// topic, so without one Config.RetryTopic was ignored and
			// the client probed.
			retry = b.cfg.TopicPrefix + cfg.Topic + "-" + sub + pulsar.RetryTopicSuffix
		}
		rt, err := TopicOf(b.cfg, retry)
		if err != nil {
			return pulsar.ConsumerOptions{}, "", err
		}
		o.DLQ.RetryLetterTopic = rt.String()
	}
	if pattern {
		p, ns, err := PatternOf(b.cfg, cfg.Topic)
		if err != nil {
			return pulsar.ConsumerOptions{}, "", err
		}
		_ = ns
		o.TopicsPattern = p
		return o, p, nil
	}
	t, err := TopicOf(b.cfg, cfg.Topic)
	if err != nil {
		return pulsar.ConsumerOptions{}, "", err
	}
	o.Topic = t.String()
	return o, t.String(), nil
}

// Fetch fills dst(n] with the deliveries the channel holds.
//
// One blocking wait for the first delivery, then a non-blocking drain of
// whatever else is there, which is what makes a batch receive a batch. A
// delivery that cannot be decoded is given back rather than handed out, and a
// delivery a later one has already superseded is dropped: both are messages no
// settlement could succeed on, and handing one to a handler would make its work
// certainly wasted.
func (s *source) Fetch(ctx context.Context, dst []mq.DriverDelivery) (int, error) {
	if len(dst) == 0 {
		return 0, nil
	}
	for {
		var first pulsar.ConsumerMessage
		select {
		case <-s.done:
			return 0, mq.ErrClosed
		case <-ctx.Done():
			return 0, ctx.Err()
		case cm := <-s.ch:
			first = cm
		}
		n := s.admit(first, dst, 0)
	drain:
		for n < len(dst) {
			select {
			case cm := <-s.ch:
				n += s.admit(cm, dst, n)
			default:
				break drain
			}
		}
		if n > 0 {
			return n, nil
		}
		if s.closed.Load() {
			return 0, mq.ErrClosed
		}
		// Everything in this batch was undecodable. Wait for the next
		// one rather than returning zero with no error, which the SPI
		// does not allow.
	}
}

// admit turns one native message into a delivery at dst[i], and reports whether
// it did.
func (s *source) admit(cm pulsar.ConsumerMessage, dst []mq.DriverDelivery, i int) int {
	msg := cm.Message
	if msg == nil {
		return 0
	}
	topic := s.resolvedTopic(msg)
	var m mq.Message
	var raw mq.Message
	Decode(&raw, msg, topic)
	if err := s.env.Unwrap(&m, &raw); err != nil {
		// There is no settlement that means "this is not a message I can
		// read". Acknowledging it would discard somebody's message, so
		// it goes back negatively and the failure surfaces as a count
		// and a health transition, which is the answer for a
		// driver's background failure.
		s.b.undecodable.Add(1)
		s.b.degrade(s.b.errs.Map(mq.OpFetch, s.iqTopic, s.sub, err))
		s.cons.NackID(msg.ID())
		return 0
	}
	m.Topic = topic
	now := s.b.now()
	d := &delivery{
		msg: msg, id: msg.ID(), pos: PositionOf(msg.ID()), src: s, gen: s.gen.Add(1),
	}
	if s.lease > 0 {
		d.expires.Store(now.Add(s.lease).UnixNano())
	}
	s.supersede(d)
	info := mq.DeliveryInfo{
		Topic:        topic,
		Subscription: s.sub,
		ReceivedAt:   now,
		Attempt:      AttemptOf(msg),
		Redelivered:  msg.RedeliveryCount() > 0,
	}
	if s.lease > 0 {
		info.LeaseExpires = now.Add(s.lease)
	}
	if p := d.pos.Partition; p >= 0 {
		info.Partition, info.HasPartition = p, true
	}
	if src := msg.Topic(); src != "" && src != topic {
		info.Source = src
	}
	dst[i] = mq.DriverDelivery{
		Message: &m, Info: info, Ref: d, Native: msg, Gen: d.gen,
		Token: uint64(d.gen),
	}
	return 1
}

// resolvedTopic is the portable topic name a delivery reports: the CONCRETE
// topic it arrived on, never the pattern the subscription named.
//
// It also undoes the configured prefix, and it undoes Pulsar's retry-topic
// suffix: a message that came back through the retry topic is the same
// application message, and reporting `orders-workers-RETRY` as its topic would
// make a handler that switches on the topic see a topic nobody published to.
//
// A DEAD-LETTER COPY reports the dead-letter topic it arrived on, as a copy does
// on the memory and RabbitMQ drivers and as the core's mq.WithDeadLetter
// publication does. The copy carries REAL_TOPIC whenever its source had been
// round the retry topic or the client's redelivery bound moved it, because both
// dead-letter routes keep the properties they find and the bound records
// REAL_TOPIC where there is none. Reading it reported the source's topic on
// those routes and the dead-letter topic on a Reject of a message that never
// hopped. A consumer of the dead-letter topic saw one topic
// or the other depending on how the copy was made. [deadLetterCopy] is the
// classification [AttemptOf] counts by, so a copy's topic and its attempt
// agree. REAL_TOPIC stays on the native message for a consumer that wants the
// source's topic.
func (s *source) resolvedTopic(msg pulsar.Message) string {
	name := msg.Topic()
	props := msg.Properties()
	if real := props[pulsar.SysPropertyRealTopic]; real != "" && !deadLetterCopy(name, props) {
		name = real
	}
	t, err := ParseTopic(name)
	if err != nil {
		if !s.pattern {
			return s.iqTopic
		}
		return name
	}
	local := t.Local
	if s.b.cfg.TopicPrefix != "" && len(local) > len(s.b.cfg.TopicPrefix) &&
		local[:len(s.b.cfg.TopicPrefix)] == s.b.cfg.TopicPrefix {
		local = local[len(s.b.cfg.TopicPrefix):]
	}
	return local
}

// supersede records d as the delivery this source vouches for and takes any
// older delivery of the same message out of circulation.
//
// Pulsar redelivers a message under the SAME identifier, and the broker will
// accept an acknowledgement through either delivery's copy of it — so the older
// one has to stop being settleable the moment the newer arrives, or an
// acknowledgement written for work that was abandoned lands on the work that
// replaced it (the specification's "lets a stale ack settle a later redelivery").
func (s *source) supersede(d *delivery) {
	s.mu.Lock()
	defer s.mu.Unlock()
	slot := s.slotOf(d)
	prev, ok := s.inflight[slot]
	s.inflight[slot] = d
	if !ok || prev == d {
		return
	}
	// Counted only when this supersession is what took it out of
	// circulation: a delivery the core had already settled went nowhere
	// stale.
	if prev.lose() {
		s.b.stale.Add(1)
	}
}

// forget drops a delivery this source has finished with.
func (s *source) forget(d *delivery) {
	s.mu.Lock()
	defer s.mu.Unlock()
	slot := s.slotOf(d)
	if cur, ok := s.inflight[slot]; ok && cur == d {
		delete(s.inflight, slot)
	}
}

// slotOf is d's key in the in-flight map: its position, which on a persistent
// topic names one message and is what a redelivery shares with the delivery it
// supersedes.
//
// A non-persistent topic's positions name nothing. The broker dispatches every
// message of one as entry 0:0, so two different messages share a position, and
// keying them by it made each delivery supersede the one before: every Ack but
// the last answered ErrLeaseLost. Such a topic has nothing stored to redeliver
// either, so each of its deliveries gets a key of its own, made from the
// delivery's generation and a ledger no persistent entry has.
func (s *source) slotOf(d *delivery) BatchPosition {
	if !s.volatile {
		return d.pos
	}
	return BatchPosition{Ledger: -1, Entry: int64(d.gen), Partition: d.pos.Partition, Index: -1}
}

// hold records a delayed negative settlement, which the source's one scheduler
// carries out when it is due.
//
// It is the emulated delay, and the shape is the specification: ONE scheduler per
// source over a list bounded by the prefetch, never a timer per delivery. The
// closed flag is re-read after the placement and the list re-drained, because a
// hold placed while the source is closing is exactly the defect class
// the contributor notes records — a record put somewhere while the thing that owns
// it is being torn down, whose obligation then never runs.
func (s *source) hold(d *delivery, delay time.Duration) {
	due := s.b.now().Add(delay)
	s.mu.Lock()
	s.held = append(s.held, heldNack{d: d, due: due})
	s.mu.Unlock()
	s.b.held.Add(1)
	if s.closed.Load() {
		// Close may have drained the list before this append landed. Take
		// whatever is there now and give it back at once: a held nack the
		// scheduler will never run is a delivery Pulsar keeps waiting for
		// until the consumer disconnects.
		s.releaseHeld()
	}
}

// releaseHeld gives back every held delayed nack at once, which is what Close
// does with them.
func (s *source) releaseHeld() {
	s.mu.Lock()
	held := s.held
	s.held = nil
	s.mu.Unlock()
	for _, h := range held {
		s.b.held.Add(-1)
		s.nackNow(h.d)
	}
}

// sweepInterval is how often the source's one scheduler runs: half the lease
// guard, bounded below so that a very short guard does not become a busy loop,
// and a fixed period when there is no lease and the scheduler is only there for
// the delayed negative settlements.
func (s *source) sweepInterval() time.Duration {
	if s.guard > 0 {
		return max(s.guard/2, minSweepInterval)
	}
	return defaultSweepInterval
}

// sweep is the source's one scheduler: it fires the delayed negative
// settlements that are due and gives back the deliveries whose lease is about
// to run out.
//
// Giving a delivery back one guard EARLY is what makes the emulated lease a
// prevention rather than a diagnosis. Without it the normal course of a slow
// handler is: the deadline passes, the driver refuses the handler's
// acknowledgement with ErrLeaseLost — correctly — and nothing ever gives the
// message back, because Pulsar redelivers an unsettled message only when the
// consumer disconnects. The message would then sit unsettled until the process
// ended.
//
// Its timer is the INJECTED clock's and not time.NewTicker, because this is the
// one place the driver's behaviour depends on time passing: a driver whose lease
// sweeper read the wall clock could only be tested by waiting, and the four
// things this driver has to make happen on purpose all involve a deadline
// (rule 4).
func (s *source) sweep() {
	interval := s.sweepInterval()
	t := s.timer
	defer t.Stop()
	for {
		select {
		case <-s.done:
			return
		case <-t.C():
		}
		t.Reset(interval)
		now := s.b.now()
		s.mu.Lock()
		var due []*delivery
		keep := s.held[:0]
		for _, h := range s.held {
			if !h.due.After(now) {
				due = append(due, h.d)
				continue
			}
			keep = append(keep, h)
		}
		s.held = keep
		if s.lease > 0 {
			for _, d := range s.inflight {
				if d.expiringSoon(now, s.guard) {
					due = append(due, d)
				}
			}
		}
		s.mu.Unlock()
		// Outside the mutex: each of these is a frame written to the
		// broker.
		for _, d := range due {
			held := d.held.Load()
			gave := s.nackNow(d)
			switch {
			case held:
				s.b.held.Add(-1)
			case gave:
				// Counted only when the sweeper is what took the
				// delivery: one collected above can be settled, or
				// refused by its own late settlement, before it
				// gets here. It used to be counted before nackNow
				// whatever nackNow then found.
				s.b.leaseLost.Add(1)
			}
		}
	}
}

// nackNow gives a delivery back to Pulsar, whatever its state, and takes it out
// of circulation. It reports whether it wrote the negative acknowledgement.
func (s *source) nackNow(d *delivery) bool {
	terminal := d.state.CompareAndSwap(stateActive, stateLost)
	if !terminal && !d.held.Load() {
		// Already settled or already lost, and in both cases the frame
		// was written or the delivery was superseded by one that will
		// answer for it.
		return false
	}
	if !terminal && !s.vouchesFor(d) {
		// A held delivery whose message has been redelivered since, which
		// happens when the consumer reconnects during the hold. The
		// redelivery carries the same identifier and is being worked on
		// under its own delivery, so a negative acknowledgement now would
		// give back that work rather than this. The hold used to fire
		// regardless, and the live redelivery came back a third time and
		// lost its lease.
		return false
	}
	s.cons.NackID(d.id)
	s.forget(d)
	return true
}

// vouchesFor reports whether d is still the delivery this source holds for its
// message.
func (s *source) vouchesFor(d *delivery) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.inflight[s.slotOf(d)] == d
}

// Settler settles this source's deliveries.
func (s *source) Settler() mq.Settler { return s.settleFn }

// Close releases the source, gives back everything it has handed out that was
// never settled, and unblocks a parked Fetch.
//
// The order matters and each step is load-bearing:
//
//  1. close done, which unparks a blocked Fetch with ErrClosed and stops the
//     scheduler;
//  2. fire every held delayed negative settlement at once, because each of them
//     is a delivery Pulsar is still waiting for and a timer that is about to
//     stop existing;
//  3. mark everything this source handed out as lost, so a settlement that
//     arrives afterwards is refused rather than applied to whatever the message
//     becomes next;
//  4. close the consumer, which is what actually gives the messages back:
//     Pulsar makes every unacknowledged message of a disconnected consumer
//     available again at once, so the specification's "requeues or abandons everything it has
//     handed out" is the broker's here and not a loop of this driver's.
func (s *source) Close() error {
	s.once.Do(func() {
		s.closed.Store(true)
		close(s.done)
		s.wg.Wait()
		s.releaseHeld()
		s.mu.Lock()
		held := s.inflight
		s.inflight = map[BatchPosition]*delivery{}
		s.mu.Unlock()
		for _, d := range held {
			_ = d.lose()
		}
		if s.cons != nil {
			s.cons.Close()
		}
		s.b.forget(s, nil)
	})
	return nil
}

// delivery is one delivery's bookkeeping: the native message and its
// identifier, the local view of its lease, and the state machine answers a
// second settlement from.
type delivery struct {
	// msg is the native message. It is kept because two of Pulsar's
	// settlement verbs take the MESSAGE and not the identifier:
	// ReconsumeLater has to copy its payload, key and properties onto the
	// retry topic.
	msg pulsar.Message
	// id is the identifier the consumer handed out, kept for the delivery's
	// whole life and never rebuilt. See AckRules.Tracked: an identifier
	// that has been through Serialize and DeserializeMessageID has lost the
	// client's batch bookkeeping, and acknowledging one message of a batch
	// through it settles every sibling.
	id  pulsar.MessageID
	pos BatchPosition
	src *source
	gen uint32

	// expires is the local view of the emulated lease, as Unix nanoseconds.
	// It is atomic because the lease check reads it from whichever
	// goroutine is settling while an extension writes it from another, and
	// because a time.Time is two words and cannot be read atomically.
	expires atomic.Int64
	// held marks a delivery whose delayed negative settlement is waiting in
	// this process, which is a state the lease check must not confuse with
	// an active one.
	held  atomic.Bool
	state atomic.Uint32
}

// expiresAt is the local view of the lease deadline.
func (d *delivery) expiresAt() time.Time { return time.Unix(0, d.expires.Load()) }

// extend moves the local view of the lease to at, unless it already runs later.
//
// An extension never shortens a lease: DeadlineExtender moves the deadline by
// at least by, so an Extend shorter than what is left must leave it alone. It
// used to store at unconditionally, so an Extend of 1.5s on a fresh 10s lease
// handed the delivery to the sweeper within a second. The compare-and-swap
// keeps two concurrent extensions from storing the earlier of the two.
func (d *delivery) extend(at time.Time) {
	next := at.UnixNano()
	for {
		cur := d.expires.Load()
		if next <= cur || d.expires.CompareAndSwap(cur, next) {
			return
		}
	}
}

// lose marks an ACTIVE delivery one this source no longer vouches for,
// reporting whether it did.
//
// It is a compare-and-swap and not a store: a store would clobber a terminal
// state, and a caller whose settlement then arrived would be told ErrLeaseLost
// where the specification requires ErrAlreadySettled. One says "your work was never
// accounted for and the message is coming back"; the other says "it was
// accounted for once", and a caller that retried on the first would do the work
// twice for no reason.
func (d *delivery) lose() bool { return d.state.CompareAndSwap(stateActive, stateLost) }

// claim takes an active delivery terminal, reporting whether it was still
// active. A second claim is refused, which is what makes a settlement happen at
// most once.
func (d *delivery) claim() bool { return d.state.CompareAndSwap(stateActive, stateSettled) }

// pastGuard reports whether the delivery is inside guard of its lease expiring.
// It asks nothing about the delivery's state, because folding the two questions
// together turns a concurrent settlement into a lost lease.
func (d *delivery) pastGuard(now time.Time, guard time.Duration) bool {
	if d.expires.Load() == 0 {
		return false
	}
	return !now.Add(guard).Before(d.expiresAt())
}

// expiringSoon reports whether the delivery is active, inside guard of its lease
// expiring, and not already held for a delayed settlement.
func (d *delivery) expiringSoon(now time.Time, guard time.Duration) bool {
	if d.state.Load() != stateActive || d.held.Load() {
		return false
	}
	if d.expires.Load() == 0 {
		return false
	}
	expires := d.expiresAt()
	return !now.Add(guard).Before(expires)
}

// stateError maps a delivery's current state to the answer the specification gives a second
// settlement.
func stateError(state uint32) error {
	switch state {
	case stateSettled:
		return mq.ErrAlreadySettled
	case stateLost:
		return mq.ErrLeaseLost
	case stateUnknown:
		// The specification makes an ambiguous settlement permanent: nothing later
		// turns it into a success, and a second attempt is refused with
		// the same answer rather than being retried against a broker
		// that may already have taken it.
		return mq.ErrSettlementUnknown
	case stateActive:
		return nil
	default:
		return mq.ErrLeaseLost
	}
}

// The source's refusals.
var (
	errUnresolvedPrefetch = errcode.New(errcode.InvalidConfig, "iqmq/pulsar: SourceConfig.Prefetch is not positive; the core resolves it before opening a source")
	errNoStartSequence    = errcode.New(errcode.Unsupported, "iqmq/pulsar: a Pulsar position is a (ledger, entry, batch index) triple and a StartPosition's sequence is one number, so StartAtSequence cannot be honoured; StartAt, which seeks by time, can")
	errNoSeekMultiTopic   = errcode.New(errcode.Unsupported, "iqmq/pulsar: pulsar-client-go refuses a seek on a multi-topic consumer, and both a pattern subscription and Config.RetryEnable make one, so StartAt cannot be honoured on this subscription")
)
