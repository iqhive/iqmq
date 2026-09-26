package toydriver

import (
	"context"
	"errors"
	"sync"
	"time"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit"
	"github.com/iqhive/iqmq/dsn"
)

// DriverName is the driver's registered name, which is what DriverInfo.Name
// answers and what a conformance report is keyed by.
const DriverName = "toy"

// Version is this driver's version. It is a constant rather than read from
// build info because an example module has no tagged version.
const Version = "0.1.0-example"

// The driver's own defaults.
const (
	// DefaultAckDeadline is the lease a delivery gets when neither the
	// topic nor the consumer names one.
	DefaultAckDeadline = 30 * time.Second
	// DefaultMaxMessageBytes is the largest message the broker accepts. It
	// is advertised, so the core refuses an oversized publication before it
	// reaches the driver.
	DefaultMaxMessageBytes = 1 << 20
	// DefaultCapacity bounds one subscription's queue. A publication that
	// would exceed it is refused with CodeResourceExhausted, which is the specification's "every buffer is bounded" reaching a driver: the core owns
	// the PUBLISHER's backpressure and a driver owns the broker's.
	//
	// The figure is the memory default, and it is 65,536 rather than
	// something smaller for a reason worth passing on. At 4,096 the
	// conformance kit's publish/fidelity scenario failed INTERMITTENTLY:
	// its publisher is a tight loop with no I/O and its consumer settles
	// every message through the whole core, so the publisher gets several
	// thousand ahead, and the scenario that then fails is the one about
	// byte-for-byte fidelity rather than the one about capacity. Worse, it
	// fails twenty seconds later, when the collector times out waiting for
	// messages the publisher had already given up on, so the report reads
	// "after 1228 of 5000 messages" and not "the queue is full".
	DefaultCapacity = 1 << 16
)

// TopicSpec is one topic to create.
type TopicSpec struct {
	// Name is the topic's name.
	Name string
	// Subscriptions are created with the topic. More may be created later
	// by opening a consumer, which is how a subscription that outlives its
	// consumers comes to exist at all.
	Subscriptions []string
	// DeadLetterTopic is where an explicit Reject sends a message. It must
	// name a topic this broker also creates; Reject answers ErrUnsupported
	// when it is empty, which is the rule for an action a driver cannot
	// honour.
	DeadLetterTopic string
	// AckDeadline is the lease a delivery from this topic gets. Zero means
	// DefaultAckDeadline and a negative value means leases never expire.
	AckDeadline time.Duration
	// Capacity bounds each of this topic's subscription queues. Zero means
	// DefaultCapacity.
	Capacity int
}

// Config configures a broker.
//
// The zero value is a broker with no topics, which can do nothing: the specification
// forbids Open from creating production topology, and this driver has no
// AutoCreate escape from that rule because a driver that creates a topic
// behind an operator's back cannot be told from one that found it.
type Config struct {
	// Topics are created before the broker is returned, in order, so that a
	// topic may name a dead-letter destination declared before it.
	Topics []TopicSpec
	// MaxMessageBytes is the limit the broker advertises. Zero means
	// DefaultMaxMessageBytes.
	MaxMessageBytes int
	// AckDeadline is the lease a topic that names none gets. Zero means
	// DefaultAckDeadline.
	//
	// It is NOT MaxLease and this broker advertises none: a default
	// lease and the longest lease a broker will grant are different facts,
	// and `settler.Extend` here grants any duration asked for. See
	// `capabilities.go` for what advertising the one as the other costs.
	AckDeadline time.Duration
	// Start is the instant the manual clock reads before anything advances
	// it. The zero value is the wall clock's current time, which is what a
	// seek to "an hour ago" needs in order to name a time in the past.
	Start time.Time
	// Credential is a secret the broker is configured with. It exists so
	// that the redaction rule has something to find: it appears in no
	// error, no DriverInfo field and no rendered configuration, and the
	// conformance kit's redaction scenario is what proves it.
	//
	Credential dsn.Secret
}

// Broker is one in-process toy broker and the iqmq backend over it.
//
// It embeds [driverkit.BaseBackend], which answers Info, Capabilities, Native
// and Health without I/O from any goroutine at any time — including after
// Close, which the specification requires and which a driver that wrote them itself usually
// gets wrong.
//
// Every method is safe for concurrent use, and Close is idempotent.
type Broker struct {
	*driverkit.BaseBackend

	cfg  Config
	errs *driverkit.ErrorMapper
	caps mq.Capabilities

	// mu guards everything below it. One mutex for a whole broker is what
	// the memory engine tells a real driver not to do; it is here because
	// this driver is about the contract and not about throughput.
	mu      sync.Mutex
	topics  map[string]*topicState
	offset  time.Duration
	nextID  uint64
	nextSeq uint64
	closed  bool
}

// topicState is one topic: its declaration and its subscriptions.
type topicState struct {
	spec TopicSpec
	subs map[string]*subState
}

// subState is one subscription: a FIFO queue of ready records, the records it
// has handed out under a lease, and the broadcast channel a parked Fetch waits
// on.
type subState struct {
	name     string
	ready    []*record
	inflight map[uint64]*record
	// wake is closed to release every parked Fetch and replaced under the
	// broker's mutex. Closing a channel is the only broadcast Go offers
	// that a select can wait on alongside a context, and a per-waiter
	// buffered channel would wake one consumer of a group when a second
	// record arrived for another (a lost wake-up, not a slow one).
	wake chan struct{}
}

// record is one message in one subscription.
type record struct {
	id       uint64
	gen      uint32
	attempts uint32
	msg      *mq.Message
	// lease is when this delivery's lease runs out. It is the zero time
	// when the record is not in flight, and also when the topic's deadline
	// is negative, which the specification spells "the broker grants no lease".
	lease  time.Time
	holder *source
	seq    uint64
}

// New builds a broker and creates the topology Config declares.
//
// It performs no I/O, which is trivially true here, and creates nothing the
// configuration did not name, which is the part the specification is about.
func New(cfg Config) (*Broker, error) {
	if cfg.Start.IsZero() {
		cfg.Start = time.Now()
	}
	b := &Broker{cfg: cfg, topics: map[string]*topicState{}}
	b.errs = newErrorMapper()
	caps, err := buildCapabilities(cfg)
	if err != nil {
		return nil, err
	}
	b.caps = caps
	base, err := driverkit.NewBase(driverkit.BaseConfig{
		Info: mq.DriverInfo{
			Name:          DriverName,
			DriverVersion: Version,
			ClientName:    "toydriver (in-process)",
			ClientVersion: Version,
		},
		Capabilities: caps,
		Native:       b,
		// The broker IS the native client, so iqmq owning it would be
		// iqmq closing itself. The escape hatch is borrowed here.
		Ownership: driverkit.ClientBorrowed,
	}, b.teardown)
	if err != nil {
		return nil, err
	}
	b.BaseBackend = base
	for _, spec := range cfg.Topics {
		if err := b.createTopic(spec); err != nil {
			return nil, err
		}
	}
	return b, nil
}

// Open builds a broker and a client over it, which is Open for this
// driver.
func Open(cfg Config) (*mq.Client, error) {
	b, err := New(cfg)
	if err != nil {
		return nil, err
	}
	return mq.NewClient(b), nil
}

// createTopic adds one topic. It is called only from New.
func (b *Broker) createTopic(spec TopicSpec) error {
	if spec.Name == "" {
		return errNoTopicName
	}
	if err := mq.ValidateTopic(spec.Name); err != nil {
		return err
	}
	if spec.AckDeadline == 0 {
		spec.AckDeadline = b.ackDeadline()
	}
	if spec.Capacity <= 0 {
		spec.Capacity = DefaultCapacity
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, dup := b.topics[spec.Name]; dup {
		return errTopicExists
	}
	if spec.DeadLetterTopic != "" {
		if _, ok := b.topics[spec.DeadLetterTopic]; !ok {
			// A dead-letter destination that does not exist would make
			// Reject fail at the moment a poison message is in a
			// handler's hands, which is the worst moment to discover it.
			return errNoDeadLetterTopic
		}
		if spec.DeadLetterTopic == spec.Name {
			return errDeadLetterLoop
		}
	}
	t := &topicState{spec: spec, subs: map[string]*subState{}}
	for _, name := range spec.Subscriptions {
		if name == "" {
			continue
		}
		t.subs[name] = newSub(name)
	}
	b.topics[spec.Name] = t
	return nil
}

// newSub builds an empty subscription.
func newSub(name string) *subState {
	return &subState{name: name, inflight: map[uint64]*record{}, wake: make(chan struct{})}
}

// ackDeadline is the configured default lease.
func (b *Broker) ackDeadline() time.Duration {
	if b.cfg.AckDeadline != 0 {
		return b.cfg.AckDeadline
	}
	return DefaultAckDeadline
}

// Now is the broker's current time. Time moves only through [Broker.Advance].
func (b *Broker) Now() time.Time {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.now()
}

// now reads the manual clock. The caller holds mu.
func (b *Broker) now() time.Time { return b.cfg.Start.Add(b.offset) }

// Advance moves the broker's clock forward by d and runs the sweeps the move
// implies before it returns.
//
// Running the sweeps here rather than in a background goroutine is what makes
// a test deterministic: when Advance returns, every lease the move expired has
// already been reclaimed, so the redelivery a scenario is about to fetch is
// already queued. A clock that moved without sweeping would leave the broker
// in a state no real passage of time produces.
func (b *Broker) Advance(d time.Duration) error {
	if d < 0 {
		return errNegativeAdvance
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return mq.ErrClosed
	}
	b.offset += d
	b.sweepLocked()
	return nil
}

// sweepLocked reclaims every expired lease. The caller holds mu.
func (b *Broker) sweepLocked() {
	now := b.now()
	for _, t := range b.topics {
		for _, s := range t.subs {
			woke := false
			for id, rec := range s.inflight {
				if rec.lease.IsZero() || rec.lease.After(now) {
					continue
				}
				delete(s.inflight, id)
				rec.lease, rec.holder = time.Time{}, nil
				// The front of the queue, so that a reclaimed record
				// keeps its place: this driver claims global ordering,
				// and a redelivery appended at the back would break it
				// for every record already waiting.
				s.ready = append([]*record{rec}, s.ready...)
				woke = true
			}
			if woke {
				s.broadcastLocked()
			}
		}
	}
}

// broadcastLocked releases every parked Fetch on this subscription. The caller
// holds the broker's mutex.
func (s *subState) broadcastLocked() {
	close(s.wake)
	s.wake = make(chan struct{})
}

// Ping is the liveness check: no I/O, no topology, no consumption.
func (b *Broker) Ping(ctx context.Context) error { return b.CheckOpen(ctx) }

// Native returns the broker itself, which is the escape hatch and what a
// harness reaches Advance through.
func (b *Broker) Native() any { return b }

// Close shuts the broker down and releases every parked Fetch. It is
// idempotent, which BaseBackend supplies.
func (b *Broker) Close() error { return b.BaseBackend.Close() }

// teardown is the driver's own close step, run once by BaseBackend.
func (b *Broker) teardown() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.closed = true
	for _, t := range b.topics {
		for _, s := range t.subs {
			s.broadcastLocked()
		}
	}
	return nil
}

// publish admits one message into every subscription of a topic.
//
// The message is CLONED. The specification says a synchronous Publish must not retain the
// caller's Key, Body or header storage after it returns, and the conformance
// kit's ownership scenario scribbles over all three the instant Publish
// returns — so a driver that stored the caller's slices would deliver
// "SCRIBBLED" and fail it.
func (b *Broker) publish(ctx context.Context, topic string, m *mq.Message) error {
	if err := ctx.Err(); err != nil {
		return b.errs.Map(mq.OpPublish, topic, "", err)
	}
	if m == nil {
		return b.errs.Map(mq.OpPublish, topic, "", errNilMessage)
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return b.errs.Map(mq.OpPublish, topic, "", mq.ErrClosed)
	}
	t, ok := b.topics[topic]
	if !ok {
		return b.errs.Map(mq.OpPublish, topic, "", errNoSuchTopic)
	}
	for _, s := range t.subs {
		if len(s.ready)+len(s.inflight) >= t.spec.Capacity {
			return b.errs.Map(mq.OpPublish, topic, s.name, errFull)
		}
	}
	// The clone happens once per subscription, because a subscription may
	// settle its copy while another still holds one and the fan-out
	// isolation is exactly that: one subscription's settlement does not
	// settle another's copy. A real driver with reference-counted payloads
	// would share the body; this one copies, which is the simple thing that
	// cannot be wrong.
	b.nextSeq++
	seq := b.nextSeq
	for _, s := range t.subs {
		b.nextID++
		rec := &record{id: b.nextID, msg: m.Clone(), seq: seq}
		rec.msg.Topic = topic
		s.ready = append(s.ready, rec)
		s.broadcastLocked()
	}
	return nil
}

// errNilMessage and friends are the driver's NATIVE errors: its own sentinels,
// mapped to the portable codes by the ErrorMapper rather than constructed
// as portable errors in place.
//
// That is the shape driverkit is built for and the shape a real driver has no
// choice about — a native client hands it *kgo.Error, not an iqmq code — and
// doing it here means the mapping is written once and is testable, instead of a
// code being chosen at each of a dozen call sites.
var (
	errNoSuchTopic       = errors.New("toy: no such topic")
	errTopicExists       = errors.New("toy: the topic already exists")
	errNoTopicName       = errors.New("toy: a topic needs a name")
	errNoDeadLetterTopic = errors.New("toy: the dead-letter destination does not exist")
	errDeadLetterLoop    = errors.New("toy: a topic cannot dead-letter to itself")
	errFull              = errors.New("toy: the subscription queue is full")
	errNilMessage        = errors.New("toy: the message is nil")
	errNegativeAdvance   = errors.New("toy: time does not run backwards")
	errStaleDelivery     = errors.New("toy: the delivery's lease has been taken back")
	errNoHandle          = errors.New("toy: the delivery carries no toy-broker handle")
	errNoDeadLetter      = errors.New("toy: this topic names no dead-letter destination")
	errNoDelay           = errors.New("toy: this broker has no delayed redelivery")
	errNoRetention       = errors.New("toy: this broker retains nothing, so it cannot start anywhere but at the end of the queue")
	errNoDurableConfirm  = errors.New("toy: this broker has no durable boundary to confirm against")
)

// newErrorMapper builds the native-to-portable mapping.
//
// Every rule names a code, a retryability and an ambiguity, and the native
// error survives inside the *OpError so that errors.Is still reaches the
// sentinels above.
func newErrorMapper() *driverkit.ErrorMapper {
	return &driverkit.ErrorMapper{Driver: DriverName, Fallback: []driverkit.Rule{
		{
			Match: driverkit.MatchIs(errNoSuchTopic),
			Code:  mq.CodeNotFound,
		},
		{
			Match: driverkit.MatchIs(errTopicExists, errDeadLetterLoop, errNoDeadLetterTopic),
			Code:  mq.CodeConflict,
		},
		{
			Match: driverkit.MatchIs(errNoTopicName, errNilMessage, errNegativeAdvance, errNoSubscription),
			Code:  mq.CodeInvalidArgument,
		},
		{
			Match: driverkit.MatchIs(errUnrepresentableSize),
			Code:  mq.CodeMessageTooLarge,
		},
		{
			// A full queue is retryable: the consumer drains it.
			Match:     driverkit.MatchIs(errFull),
			Code:      mq.CodeResourceExhausted,
			Retryable: true,
		},
		{
			Match: driverkit.MatchIs(errStaleDelivery),
			Code:  mq.CodeLeaseLost,
		},
		{
			Match: driverkit.MatchIs(errNoHandle),
			Code:  mq.CodeNotSettleable,
		},
		{
			Match: driverkit.MatchIs(errNoDeadLetter, errNoDelay, errNoRetention, errNoDurableConfirm,
				errUnrepresentable, mq.ErrUnsupported),
			Code: mq.CodeUnsupported,
		},
		{
			Match: driverkit.MatchIs(mq.ErrClosed),
			Code:  mq.CodeClosed,
		},
	}}
}
