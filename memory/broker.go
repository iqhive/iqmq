package memory

import (
	"context"
	"sync"
	"time"

	mq "bitbucket.org/iqhive/iqmq"
	"bitbucket.org/iqhive/iqmq/internal/clock"
	"bitbucket.org/iqhive/iqmq/internal/engine"
	"bitbucket.org/iqhive/iqmq/internal/errcode"
)

// Broker is one in-process broker: the memory engine, the topology it creates
// topics with, and the test affordances of the memory engine.
//
// It is what [bitbucket.org/iqhive/iqmq.Client.Native] returns, so a caller that
// needs to inspect the engine, wait for it to go idle or inject a fault gets
// there through the escape hatch rather than through a second API:
//
//	b, _ := client.Native().(*memory.Broker)
//	snap, _ := b.Inspect("orders", "billing")
//
// Every method is safe for concurrent use, and Close is idempotent.
type Broker struct {
	cfg  Config
	eng  *engine.Engine[*mq.Message]
	caps mq.Capabilities
	// topo is the resolved default topology every topic is created with.
	topo engine.TopologyConfig
	// fake is the controllable clock the broker owns, or nil when the
	// broker reads a clock it cannot move. It is what Advance needs.
	fake *clock.Fake
	// resolution is the granularity the selected scheduler kind provides.
	resolution time.Duration
	// autoCreate is Config.AutoCreate, compiled into a field.
	autoCreate bool
	// faults is the injector, built once so that every caller of
	// Faults gets the same one and an armed fault stays armed.
	faults *Faults

	// mu guards the open handle list, which Close walks.
	mu      sync.Mutex
	sources []*source
	sinks   []*sink
	closed  bool
}

// NewBroker builds a broker without a client (the memory engine).
//
// It performs no I/O and creates no topology: with AutoCreate false — the
// default, and what every network driver does — a topic has to be created
// through the admin surface before a publisher or a consumer can name it.
func NewBroker(cfg Config) (*Broker, error) {
	topo, err := cfg.DefaultTopology.resolve()
	if err != nil {
		return nil, err
	}
	switch {
	case cfg.MaxTotalMessages < 0:
		return nil, errTotalMessages
	case cfg.MaxTotalBytes < 0:
		return nil, errTotalBytes
	case cfg.SchedulerResolution < 0:
		return nil, errSchedulerResolution
	case !cfg.Engine.Valid():
		return nil, errEngineKinds
	}
	kinds := cfg.Engine.kindSet()
	res := kinds.Resolve().Timer.Resolution()
	if cfg.SchedulerResolution > 0 && res > cfg.SchedulerResolution {
		// A resolution the selected scheduler cannot provide is refused
		// rather than ignored: a delay that fires a millisecond late when
		// the caller asked for microseconds is exactly the kind of silent
		// approximation the specification forbids.
		return nil, errSchedulerTooCoarse
	}
	b := &Broker{
		cfg:        cfg,
		topo:       topo,
		resolution: res,
		autoCreate: cfg.AutoCreate,
	}
	ec := engine.Config[*mq.Message]{
		Kinds:            kinds,
		DefaultTopology:  topo,
		MaxTotalMessages: cfg.MaxTotalMessages,
		MaxTotalBytes:    cfg.MaxTotalBytes,
		Clone:            cloneMessage,
		CopyMode:         engine.CopySafe,
		Receive:          engine.ReceiveShared,
		DebugOwnership:   cfg.DebugOwnership,
	}
	if cfg.ZeroOnRelease {
		ec.Zero = zeroMessage
	}
	switch c := cfg.Clock.(type) {
	case nil:
		ec.Clock = clock.Real{}
	case *TestClock:
		// The engine's own clock, not a bridge over it: Advance moves time
		// and runs the sweeps the move implies, and it can only do that for
		// a clock the engine recognises.
		b.fake = c.fake
		ec.Clock = c.fake
	default:
		ec.Clock = clockBridge{c: c}
	}
	eng, err := engine.New(ec)
	if err != nil {
		return nil, err
	}
	b.eng = eng
	b.caps = b.capabilities()
	b.faults = &Faults{b: b, f: eng.Faults()}
	return b, nil
}

// Open opens a client over a new broker (the API).
//
// It performs no I/O and creates no production topology, which is the rule
// for every driver. Closing the client closes the broker.
func Open(ctx context.Context, cfg Config) (*mq.Client, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	b, err := NewBroker(cfg)
	if err != nil {
		return nil, err
	}
	return b.client(true), nil
}

// Ownership says whether a client that was handed an existing broker closes
// it (the API). There is no default: a lifetime is not something a
// caller should acquire by omission.
type Ownership uint8

// The ownership modes.
const (
	// ownershipUnset is the zero value and names neither mode.
	ownershipUnset Ownership = iota
	// ClientOwned transfers the broker to the client: closing the client
	// closes the broker.
	ClientOwned
	// ClientBorrowed leaves the broker with the caller: closing the client
	// closes the publishers and consumers it opened and leaves the broker
	// running, so a second client can go on using it.
	ClientBorrowed

	ownershipCount
)

var ownershipNames = [ownershipCount]string{"unset", "owned", "borrowed"}

// String returns the mode's lower-case name.
func (o Ownership) String() string {
	if o < ownershipCount {
		return ownershipNames[o]
	}
	return "Ownership(?)"
}

// Valid reports whether o names a mode.
func (o Ownership) Valid() bool { return o > ownershipUnset && o < ownershipCount }

// OpenWithClient opens a client over an existing broker (the API's
// native-injection form, with the broker as the native client).
//
// Two clients over one broker is what makes a test that needs two processes
// possible in one, which is why the ownership has to be explicit: the second
// client must not close the broker the first one is still using.
func OpenWithClient(ctx context.Context, b *Broker, own Ownership) (*mq.Client, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if b == nil {
		return nil, errNilBroker
	}
	if !own.Valid() {
		return nil, errOwnership
	}
	return b.client(own == ClientOwned), nil
}

// client builds the client over this broker. own decides whether closing the
// client closes the broker.
func (b *Broker) client(own bool) *mq.Client {
	var backend mq.Backend = b
	if !own {
		backend = borrowed{b}
	}
	opts := []mq.ClientOption{mq.WithHooks(b.cfg.Hooks)}
	if b.cfg.Clock != nil {
		opts = append(opts, mq.WithClock(b.cfg.Clock))
	}
	return mq.NewClient(backend, opts...)
}

// borrowed is the broker seen by a client that does not own it: the whole
// backend surface, with a Close that closes nothing.
//
// The client still closes every publisher and consumer it opened before it
// reaches here, so what this Close skips is exactly the broker.
type borrowed struct{ *Broker }

// Close closes nothing: the broker belongs to whoever built it.
func (borrowed) Close() error { return nil }

// Inspect returns the observable state of one subscription (the API contract
// The specification). It fails with ErrTopicNotFound for a topic the broker does not know
// and a CodeNotFound error for a subscription it does not have.
func (b *Broker) Inspect(topic, sub string) (Snapshot, error) {
	snap, err := b.eng.Inspect(topic, sub)
	if err != nil {
		return Snapshot{}, err
	}
	return snapshotOf(snap), nil
}

// WaitIdle blocks until the broker holds no record a consumer could take —
// and, in IdleIncludingDelayed, no record at all — or ctx is done
// (the memory engine).
//
// It is driven by the settlement path rather than by polling, so "advance the
// clock, then wait" is deterministic under testing/synctest.
func (b *Broker) WaitIdle(ctx context.Context, mode IdleMode) error {
	if !mode.Valid() {
		return errIdleMode
	}
	return b.eng.WaitIdle(ctx, mode.engineMode())
}

// Faults is the fault injector of the memory engine.
func (b *Broker) Faults() *Faults { return b.faults }

// Advance moves the broker's clock forward by d and runs the sweeps the move
// implies: records past their time to live, retained records past their age
// bound, and leases past their deadline.
//
// It is Faults().Advance and is here because it is the one fault a test
// reaches for constantly. It fails with ErrUnsupported on a broker that does
// not own a controllable clock, because a fault that silently did nothing
// would make a test that relies on it pass for the wrong reason.
func (b *Broker) Advance(d time.Duration) error { return b.faults.Advance(d) }

// Engine returns the resolved variant selection the broker is running, which
// is what a test logs and an ADR quotes.
func (b *Broker) Engine() EngineConfig { return b.cfg.Engine.Resolve() }

// SchedulerResolution is the granularity the selected scheduler kind
// provides. Zero means the scheduler fires at the exact due time.
func (b *Broker) SchedulerResolution() time.Duration { return b.resolution }

// Totals returns the records and accounted bytes admitted across the whole
// broker, plus the number of publications discarded because their topic had
// no subscription.
func (b *Broker) Totals() (messages, bytes, discarded int64) { return b.eng.Totals() }

// Clock returns the broker's time source.
func (b *Broker) Clock() mq.Clock {
	if b.cfg.Clock != nil {
		return b.cfg.Clock
	}
	return realClock{}
}

// Close shuts the broker down promptly: no further publication or
// attachment is admitted, every parked consumer and publisher is woken with
// ErrClosed, and every record a lane still holds is dropped, which releases
// its payload exactly once.
//
// It is idempotent and safe for concurrent use.
func (b *Broker) Close() error {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return nil
	}
	b.closed = true
	sources, sinks := b.sources, b.sinks
	b.sources, b.sinks = nil, nil
	b.mu.Unlock()
	// The handles are closed outside the lock: a source's close detaches a
	// consumer, which runs the release callbacks of the records it gives
	// back, and no callback of the caller's may run under a lock of ours
	for _, s := range sources {
		_ = s.Close()
	}
	for _, s := range sinks {
		_ = s.Close()
	}
	return b.eng.Close()
}

// Shutdown closes the broker gracefully, bounded by ctx: the drain
// waits for the deliveries in flight and the settling phase for the
// dead-letter publications still in flight.
func (b *Broker) Shutdown(ctx context.Context) error {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return nil
	}
	b.closed = true
	sources, sinks := b.sources, b.sinks
	b.sources, b.sinks = nil, nil
	b.mu.Unlock()
	for _, s := range sources {
		_ = s.Close()
	}
	for _, s := range sinks {
		_ = s.Close()
	}
	return b.eng.Shutdown(ctx)
}

// Closed reports whether the broker is closing or closed.
func (b *Broker) Closed() bool { return b.eng.Closed() }

// topicFor resolves a topic name, creating the topic when AutoCreate is set.
//
// It is the one place the specification's "Open never creates production topology" is
// enforced: without AutoCreate an unknown name is ErrTopicNotFound, whatever
// the caller was trying to do with it.
func (b *Broker) topicFor(name string) (*engine.Topic[*mq.Message], error) {
	if err := mq.ValidateTopic(name); err != nil {
		return nil, err
	}
	if t, err := b.eng.Topic(name); err == nil {
		return t, nil
	}
	if !b.autoCreate {
		return nil, mq.ErrTopicNotFound
	}
	return b.eng.CreateTopic(name, b.topo)
}

// track registers an open handle so that closing the broker closes it.
func (b *Broker) track(src *source, snk *sink) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed || b.eng.Closed() {
		return mq.ErrClosed
	}
	if src != nil {
		b.sources = append(b.sources, src)
	}
	if snk != nil {
		b.sinks = append(b.sinks, snk)
	}
	return nil
}

// forget drops a handle that closed itself, so that a long-lived broker does
// not accumulate handles nobody holds.
func (b *Broker) forget(src *source, snk *sink) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if src != nil {
		b.sources = dropHandle(b.sources, src)
	}
	if snk != nil {
		b.sinks = dropHandle(b.sinks, snk)
	}
}

// dropHandle removes one handle from a slice without keeping a reference to
// the element it moved away from.
func dropHandle[T comparable](s []T, v T) []T {
	for i := range s {
		if s[i] != v {
			continue
		}
		var zero T
		s[i] = s[len(s)-1]
		s[len(s)-1] = zero
		return s[:len(s)-1]
	}
	return s
}

// Snapshot is the observable state of one subscription (the memory engine).
//
// The gauges are exact while the subscription is quiescent, which is the
// condition every model and invariant check runs under.
type Snapshot struct {
	// Ready counts records waiting for a consumer.
	Ready int
	// InFlight counts leased records.
	InFlight int
	// Delayed counts records waiting for their due time.
	Delayed int
	// Retained counts the records the subscription's topic keeps for
	// replay. It is a property of the topic, so every subscription of a
	// stream reports the same number.
	Retained int
	// Bytes is the accounted payload size of every record the lane holds.
	Bytes int64
	// Published counts records admitted to the subscription.
	Published int64
	// Acked counts records terminally settled by a consumer. A delivery
	// dropped with NackDrop is counted here too: the engine's native
	// settlement for a drop is the one it takes for an ack, which is what
	// most brokers do as well.
	Acked int64
	// Nacked counts negative settlements.
	Nacked int64
	// Requeued counts records returned to a lane by a negative settlement,
	// a lease expiry or a consumer close.
	Requeued int64
	// Expired counts leases reclaimed by expiry.
	Expired int64
	// DeadLettered counts records published to a dead-letter topic because
	// they used up their delivery attempts.
	DeadLettered int64
	// Overflowed counts publications the overflow policy discarded before
	// admission.
	Overflowed int64
	// DeadLetterFailures counts dead-letter publications that failed, each
	// of which returned its record to the lane rather than settling it.
	DeadLetterFailures int64
	// Dropped counts admitted records discarded without settlement.
	Dropped int64
	// Rejected counts publications refused before admission.
	Rejected int64
	// Attempts is the total number of deliveries, redeliveries included.
	Attempts int64
	// Deduplicated counts the publications the topic's dedup window
	// discarded. It is a property of the topic.
	Deduplicated int64
	// ActiveConsumers counts attached, open consumers.
	ActiveConsumers int
	// Assignment is the version of the subscription's partition assignment,
	// which every attach, detach and rebalance increments.
	Assignment uint64
}

// Conserved reports whether the conservation invariant of the memory engine
// holds: every admitted record is settled, dead-lettered, dropped, or still
// held in one of the three lane structures.
func (s Snapshot) Conserved() bool {
	return s.Published == s.Acked+int64(s.Ready)+int64(s.InFlight)+int64(s.Delayed)+
		s.DeadLettered+s.Dropped
}

// snapshotOf converts the engine's snapshot.
func snapshotOf(s engine.Snapshot) Snapshot {
	return Snapshot{
		Ready:              s.Ready,
		InFlight:           s.InFlight,
		Delayed:            s.Delayed,
		Retained:           s.Retained,
		Bytes:              s.Bytes,
		Published:          s.Published,
		Acked:              s.Acked,
		Nacked:             s.Nacked,
		Requeued:           s.Requeued,
		Expired:            s.Expired,
		DeadLettered:       s.DeadLettered,
		Overflowed:         s.Overflowed,
		DeadLetterFailures: s.DeadLetterFailures,
		Dropped:            s.Dropped,
		Rejected:           s.Rejected,
		Attempts:           s.Attempts,
		Deduplicated:       s.Deduplicated,
		ActiveConsumers:    s.ActiveConsumers,
		Assignment:         s.Assignment,
	}
}

// IdleMode selects what WaitIdle waits for (the memory engine).
type IdleMode uint8

// The idle modes. The zero value is IdleDelivered.
const (
	// IdleDelivered waits until nothing is ready or in flight: every record
	// the broker could hand to a consumer has been handed over and settled.
	// Records waiting for a due time in the future do not hold it.
	IdleDelivered IdleMode = iota
	// IdleIncludingDelayed waits until the broker holds no record at all,
	// delayed and redelivery-pending records included.
	IdleIncludingDelayed
	// IdleSettled waits until no lease is outstanding, whatever is still
	// queued: it is "every consumer has finished what it took".
	IdleSettled

	idleModeCount
)

var idleModeNames = [idleModeCount]string{"delivered", "includingdelayed", "settled"}

// String returns the mode's lower-case name.
func (m IdleMode) String() string {
	if m < idleModeCount {
		return idleModeNames[m]
	}
	return "IdleMode(?)"
}

// Valid reports whether m names a mode.
func (m IdleMode) Valid() bool { return m < idleModeCount }

// engineMode is the internal form.
func (m IdleMode) engineMode() engine.IdleMode {
	switch m {
	case IdleIncludingDelayed:
		return engine.IdleIncludingDelayed
	case IdleSettled:
		return engine.IdleSettled
	case IdleDelivered, idleModeCount:
		return engine.IdleDelivered
	default:
		return engine.IdleDelivered
	}
}

// cloneMessage is the engine's Clone: the deep copy safe publication makes.
func cloneMessage(m *mq.Message) *mq.Message {
	if m == nil {
		return nil
	}
	return m.Clone()
}

// zeroMessage backs ZeroOnRelease: it overwrites the bytes of a payload the
// engine owns as it releases the record, so that sensitive data does not
// survive in a pooled one.
//
// It writes over the full capacity of Key and Body and of every header value,
// which is only safe on storage the engine owns — which is exactly where the
// engine applies it: a borrowed payload's storage belongs to the caller and
// CopyBorrowed.Scrubs is false.
func zeroMessage(m *mq.Message) {
	if m == nil {
		return
	}
	clear(m.Key[:cap(m.Key)])
	clear(m.Body[:cap(m.Body)])
	for _, v := range m.Headers.All() {
		clear(v[:cap(v)])
	}
	m.Reset()
}

// messageSize is the size the engine accounts a publication at: the key and
// the body.
//
// Header storage is not counted. Counting it means walking the header set on
// every publication, and mq.Headers exposes no allocation-free size accessor
// — the engine's own payload form has one and a *mq.Message cannot reach it —
// so the gauge would be paid for on the hot path the specification protects.
func messageSize(m *mq.Message) int { return len(m.Key) + len(m.Body) }

// The broker's construction failures.
var (
	errNilBroker          = errcode.New(errcode.InvalidArgument, "iqmq/memory: OpenWithClient was given a nil Broker")
	errOwnership          = errcode.New(errcode.InvalidArgument, "iqmq/memory: OpenWithClient needs ClientOwned or ClientBorrowed")
	errIdleMode           = errcode.New(errcode.InvalidArgument, "iqmq/memory: WaitIdle was given no idle mode")
	errSchedulerTooCoarse = errcode.New(errcode.InvalidConfig, "iqmq/memory: Config.SchedulerResolution is finer than the selected Engine.Timer kind provides ")
)

// EngineTopologyForBench resolves a public topology into the engine's own
// form, for a benchmark that measures the engine directly against the same
// topology the driver would have given it.
//
// It exists so that the two sides of BenchmarkMemory/PubSubAck cannot drift:
// a hand-written engine topology beside a public one is a comparison of two
// workloads rather than of two routes, and the number that comes out of it
// means nothing. It is not part of the driver's own surface — nothing in this
// package calls it — and it is exported only because the benchmark is an
// external test package.
func EngineTopologyForBench(t TopologyConfig) (engine.TopologyConfig, error) {
	return t.resolve()
}
