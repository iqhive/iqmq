package iqmq

import (
	"context"
	"strconv"
	"time"
)

// Backend is the mandatory driver surface of the SPI: what a driver
// must implement for the core to publish and consume through it.
//
// It is deliberately small. Everything a broker does differently — batching,
// asynchronous publication, ownership transfer, lease extension, batched
// settlement — is an optional interface below, type-asserted once when the
// core opens the sink or source and never on a message's path. A driver that
// implements nothing but this interface works: driverkit supplies the loops
// the optional interfaces would have replaced (ADR-021).
//
// Drivers MUST NOT implement worker pools, retries, dead-letter logic or
// ordering lanes: those are the core's, and a driver that adds a second one
// breaks the specification's "exactly one layer owns redelivery accounting".
//
// Info, Capabilities and Native are answered without I/O and are safe to call
// from any goroutine at any time, including after Close. Every other method
// may be called concurrently, and Close is idempotent.
type Backend interface {
	// Info describes the driver and the client library it wraps.
	Info() DriverInfo
	// Capabilities is what this backend can and cannot do. It is
	// constant for the life of the backend: the core compiles it into
	// booleans at construction (rule 4).
	Capabilities() Capabilities
	// OpenSink opens a publication channel to one destination.
	OpenSink(ctx context.Context, cfg SinkConfig) (Sink, error)
	// OpenSource opens a subscription. Every parameter the source needs is
	// in cfg, so a source never consults the core.
	OpenSource(ctx context.Context, cfg SourceConfig) (Source, error)
	// Ping is a cheap liveness check. It never consumes a message and never
	// creates topology.
	Ping(ctx context.Context) error
	// Native returns the underlying client for callers who need the escape
	// hatch: *kgo.Client, *nats.Conn, *memory.Broker. It may be nil.
	Native() any
	// Close releases the backend and everything it opened.
	Close() error
}

// DriverInfo names the driver, its profile and the versions behind it. Every
// field is empty when it is not known, which the specification requires: an empty string
// is "the driver did not say", never a claim.
type DriverInfo struct {
	// Name is the driver's registered name, such as "kafka" or "memory".
	Name string
	// Profile is the variant of the broker the driver is talking to, such as
	// "jetstream" for NATS or "fifo" for SQS. It is empty when the driver has
	// one profile.
	Profile string
	// DriverVersion is the version of the iqmq driver module.
	DriverVersion string
	// ClientName is the native client library the driver wraps.
	ClientName string
	// ClientVersion is that library's version.
	ClientVersion string
	// BrokerVersion is the broker's version, when the client reports one.
	BrokerVersion string
}

// SinkConfig is what a Sink is opened with.
//
// The specification fixes the Sink's own parameters and the publisher configuration a
// caller writes; this carries the part of that configuration a driver has to
// honour itself. The core keeps the rest — the pending bounds, the
// backpressure mode, the retry policy — because a driver that implemented
// them would be the second owner of a policy the specification gives to one.
type SinkConfig struct {
	// Topic is the destination. A sink is bound to it for its lifetime,
	// which is the winner (ADR-021, Bound).
	Topic string
	// Confirmation is how strong an acknowledgement a publication waits for. ConfirmDurable fails at open when the profile cannot define a
	// durable boundary.
	Confirmation ConfirmationMode
	// Metadata is the metadata mode. It decides what the driver does
	// with metadata it has no native field for.
	Metadata MetadataMode
	// Receipts asks the sink to report what the broker said about each
	// accepted publication. It is true only when the publisher was
	// configured with Receipts and the driver advertises FeatureReceipts, so
	// a sink that reads it here never needs a per-message branch.
	//
	// The receipt's storage is the caller's and travels with the message:
	// see Message.Receipt and Sink.Publish.
	Receipts bool
}

// SourceConfig is what a Source is opened with: the specification's "topic, subscription,
// prefetch, start, ack deadline, metadata mode".
type SourceConfig struct {
	// Topic is the topic or pattern to consume.
	Topic string
	// Subscription is the durable subscription, consumer group or queue name.
	Subscription string
	// Prefetch is how many messages the source may hold unclaimed. It is
	// always positive: the core resolves the portable default before
	// opening the source.
	Prefetch int
	// Start is where a subscription with no committed position begins.
	Start StartPosition
	// AckDeadline is how long a delivery's lease lasts. Zero means the
	// broker's own default.
	AckDeadline time.Duration
	// Metadata is the metadata mode.
	Metadata MetadataMode
}

// Sink is the mandatory publication surface: one message at a time.
//
// A driver that can publish a batch natively also implements BatchSink; one
// that cannot implements this and nothing else, and the core loops
// (ADR-021).
type Sink interface {
	// Publish admits one message. It must not retain m, its Key, its Body or
	// its header storage after it returns: a caller may reuse all of
	// them, so a driver that keeps the message encodes it first or copies it.
	//
	// o is the resolved option set. It is borrowed for the call: a sink must
	// neither retain nor modify it.
	//
	// A sink opened with SinkConfig.Receipts fills m.Receipt on success,
	// when it is not nil: the storage is the caller's, the core has already
	// zeroed it, and filling it in place is what keeps the specification's "no result
	// struct allocation on success" true. It is the one field of m a sink
	// may write.
	Publish(ctx context.Context, m *Message, o *PublishOptions) error
	// Close releases the sink. It is idempotent, and a Publish afterwards
	// returns ErrClosed.
	Close() error
}

// BatchSink is the optional native-batch surface (ADR-021,
// Optional). The core asserts it once, when it opens the sink, and records
// the answer in FeatureBatchPublish.
type BatchSink interface {
	// PublishBatch admits a batch. It returns nil when every message was
	// accepted, a *BatchError whose Errs are index-aligned with ms when some
	// were, and a single error when none were and that one error is every
	// message outcome -- all ambiguous, all refused with one code and one
	// retryability, or a closed sink or an ended context at the first message
	// with every message behind it answered as never attempted (its code,
	// retryable). A batch none of which was accepted but whose messages'
	// outcomes differ is reported as the index-aligned *BatchError too: the
	// core cannot recover an ambiguous message from a single error that calls
	// it refused, and a caller who republished it would duplicate it; nor can
	// a caller tell which refusals to retry from one error with one
	// retryability. driverkit.CollapseBatch implements exactly this.
	//
	// Index alignment is the whole contract: a result aligned with anything
	// but ms — a native sub-batch, the failures only — reports one message's
	// outcome for another, which is how a batch publish loses a message
	// silently. FuzzBatchResults exists for it.
	PublishBatch(ctx context.Context, ms []*Message, o *PublishOptions) error
}

// AsyncSink is the optional asynchronous publication surface.
//
// Its contract is the specification: a non-nil immediate error means done will not run;
// a nil return means done runs exactly once; the message is borrowed until
// done runs; and there is no goroutine per message. A callback must not block
// the driver's progress.
//
// o is borrowed for as long as the message is — that is, until done runs —
// and not merely for the call. That is the one place the asynchronous
// contract differs from Sink.Publish's, and it is stated here because the
// core has to honour it on the other side: the resolved option set of a
// synchronous publication comes from a pool and goes back to it the moment
// the call returns, so an asynchronous publication is given a set of its own
// that lives as long as the publication does. A driver that read a pooled set
// from its I/O goroutine would read another message's options, which is a
// silent cross-message swap under load rather than a crash.
//
// Like Sink.Publish, a sink opened with SinkConfig.Receipts fills m.Receipt
// before it calls done.
type AsyncSink interface {
	// PublishAsync admits one message and reports its outcome to done. m and
	// o are both borrowed until done runs.
	PublishAsync(ctx context.Context, m *Message, o *PublishOptions, done func(error)) error
}

// OwnedSink is the optional ownership-transfer surface.
//
// Ownership of om's message transfers at invocation, so the release is the
// sink's obligation from the moment it is called — including when it refuses
// the publication, when it is closed, and when the failure is permanent. The
// core releases only on the paths that never reach a sink at all.
//
// o follows Sink.Publish's rule and not AsyncSink's: it is borrowed for the
// call. A sink that takes ownership and then publishes in the background
// therefore copies the option set, the same way it would copy anything else
// it retains.
type OwnedSink interface {
	// PublishOwned takes ownership of om's message at invocation and calls
	// om.Release exactly once — including on rejection, on close and on
	// permanent failure — and never while holding an internal lock.
	PublishOwned(ctx context.Context, om OwnedMessage, o *PublishOptions) error
}

// FlushSink is the optional flush surface a sink that buffers
// publications implements it so that a caller can wait for what it has
// admitted.
type FlushSink interface {
	// Flush completes the publications already admitted, bounded by ctx.
	Flush(ctx context.Context) error
}

// OwnedMessage is a message whose storage is transferred to the callee
// Release is called exactly once, with the message it was given.
type OwnedMessage struct {
	// Message is the transferred message.
	Message *Message
	// Release returns the message's storage to whoever owned it. It is
	// called exactly once and never under an internal lock.
	Release func(*Message)
}

// Source is the mandatory consumption surface: a batch-filling fetch, a
// settler and a close.
//
// One Fetch at a time per source, which is the specification's "one active Receive per
// consumer unless the driver documents otherwise"; settlement may happen
// concurrently with a fetch and with other settlements.
type Source interface {
	// Fetch blocks until at least one delivery is available, ctx is done or
	// the source is closed, and fills dst[:n].
	//
	// It allocates nothing per fetch: the caller owns dst and reuses it, so
	// the deliveries in it are only valid until the next Fetch. Whatever a
	// settlement needs later — a native handle, a receipt string — belongs in
	// the DriverDelivery, which the core copies out before reusing dst.
	//
	// Fetch returns ErrClosed once the source is closed, and a Close in
	// another goroutine must unblock a Fetch that is parked: it is how the specification's
	// "Close unblocks blocked operations" reaches a driver.
	Fetch(ctx context.Context, dst []DriverDelivery) (int, error)
	// Settler settles this source's deliveries. There is one per source
	// and it may be called concurrently with Fetch.
	//
	// A nil Settler declares the source non-settleable, which is the specification's
	// AckNone model: the core answers every Ack, Nack and Reject with
	// ErrNotSettleable and never calls the driver.
	Settler() Settler
	// Close releases the source, requeues or abandons everything it has
	// handed out that was never settled, and unblocks a parked Fetch. It is
	// idempotent.
	Close() error
}

// Settler settles the deliveries of one source.
//
// The core has already decided that this settlement is the one that may
// proceed — exactly one terminal attempt per delivery reaches a Settler, and
// a second is refused above it with ErrAlreadySettled — so a Settler answers
// only for the native action: nil when the broker took it, an error carrying
// CodeSettlementUnknown or an *OpError with Ambiguous set when the outcome is
// unknown, and any other error for a definite refusal.
//
// A definite refusal leaves the delivery settleable: the core returns the
// error and the caller may try again. An unknown outcome is sticky, which is
// why a driver must not report an ambiguous round trip as a plain failure.
type Settler interface {
	// Ack settles d positively: the application completed processing and the
	// driver takes the strongest configured native settlement action.
	Ack(ctx context.Context, d *DriverDelivery) error
	// Nack settles d negatively under o. A driver that cannot honour o's
	// action — a delayed requeue or a dead-letter route it has neither
	// natively nor by an enabled emulation — returns ErrUnsupported and
	// settles nothing.
	Nack(ctx context.Context, d *DriverDelivery, o NackOptions) error
}

// BatchSettler is the optional batched-settlement surface a broker
// whose settlement is a round trip implements it so that the core can settle
// many deliveries in one.
type BatchSettler interface {
	// AckBatch settles every delivery in ds positively. Like a batch
	// publication it reports per-delivery outcomes through a *BatchError
	// whose Errs are index-aligned with ds.
	AckBatch(ctx context.Context, ds []*DriverDelivery) error
	// NackBatch settles every delivery in ds negatively under o, with the
	// same index-aligned result contract.
	NackBatch(ctx context.Context, ds []*DriverDelivery, o NackOptions) error
}

// DeadlineExtender is the optional lease-extension surface a broker
// with a visibility timeout or a redelivery deadline implements it so that
// the core's shared lease scheduler can keep a long handler's delivery alive.
//
// The scheduler is one per consumer and never one timer per delivery.
type DeadlineExtender interface {
	// Extend moves d's lease deadline by at least by.
	Extend(ctx context.Context, d *DriverDelivery, by time.Duration) error
}

// AdminBackend is the optional topology surface a backend whose broker
// has an administrative API implements it, and Client.Admin answers
// ErrUnsupported for one that does not.
type AdminBackend interface {
	// Admin returns the topology surface, or an error when this backend
	// cannot provide one.
	Admin() (Admin, error)
}

// HealthReporter is the optional health surface a backend that runs
// background work — a reconnect loop, a heartbeat — implements it so that
// the specification's "driver background failures surface via hooks and health, not only
// logs" has somewhere to surface.
//
// Health is answered without I/O: a client polling it must not become load on
// the broker.
type HealthReporter interface {
	// Health is the backend's current state.
	Health() HealthSnapshot
}

// DriverDelivery is one delivery as a driver hands it over: the message, the
// metadata, and whatever the driver needs in order to settle it later.
//
// Token, Ref, Native and Gen are the driver's own bookkeeping and the core
// never interprets them. The core copies the whole struct out of the fetch
// buffer before reusing it, and hands back a pointer to its copy when it
// settles, so a driver may keep state in any of these fields for a
// delivery's lifetime.
//
// Gen is what makes a stale settlement detectable at the driver: a driver
// that redelivers a record hands out a new generation, and a settlement
// carrying the old one is answered with ErrLeaseLost rather than applied to
// the new delivery.
//
// A driver must not modify the struct the core hands back when it settles or
// extends a delivery. The core writes its copy once, before the delivery
// reaches the caller, and never again, which is what lets Delivery.Info,
// Native and NativeAs answer from any goroutine without a lock — a driver
// that rewrote a field during a settlement would race those reads.
// "The driver may keep state in these fields" means the
// core preserves what the driver put there at fetch, not that a later call
// may change it.
type DriverDelivery struct {
	// Message is the received message. The core copies its value into the
	// Delivery it hands the caller, so the driver may reuse the *Message
	// itself after the fetch — but not the Key, Body or header storage it
	// points at, which stay borrowed until the delivery is settled.
	Message *Message
	// Info is the delivery metadata.
	Info DeliveryInfo
	// Token is a packed driver handle, such as a slot index and a tag.
	Token uint64
	// Ref is a handle no integer can carry, such as an SQS receipt handle.
	// It is the only place the SPI boxes a value per delivery, and it exists
	// because some brokers leave no alternative.
	Ref any
	// Native is the driver's own delivery object, surfaced to callers that
	// ask for the escape hatch.
	Native any
	// Gen is the delivery generation: a new one for every delivery and
	// redelivery of the same record.
	Gen uint32
}

// DeliveryInfo is the metadata of one delivery. Every optional field
// is paired with a Has flag, because a partition of zero and an offset of
// zero are real values and "the broker did not say" must not be the same
// answer.
type DeliveryInfo struct {
	// Topic is the resolved topic: the concrete one, not the pattern a
	// wildcard subscription named.
	Topic string
	// Source is the broker-side origin when it differs from Topic, such as
	// the stream behind a NATS subject.
	Source string
	// Subscription is the subscription the delivery came from.
	Subscription string
	// ReceivedAt is when the driver received the message.
	ReceivedAt time.Time
	// LeaseExpires is when the delivery's lease runs out. It is the zero time
	// when the broker grants no lease.
	LeaseExpires time.Time
	// Attempt is the delivery attempt, 1 on the first delivery. Zero means
	// the broker does not count attempts.
	Attempt uint32
	// Redelivered reports that the broker considers this a redelivery.
	Redelivered bool
	// Partition is the partition the message came from.
	Partition int32
	// HasPartition reports whether Partition was set.
	HasPartition bool
	// Offset is the message's offset within its partition.
	Offset int64
	// HasOffset reports whether Offset was set.
	HasOffset bool
	// Sequence is the message's broker-assigned sequence.
	Sequence uint64
	// HasSequence reports whether Sequence was set.
	HasSequence bool
}

// ConfirmationMode is how strong an acknowledgement a publication waits for. Every driver page states the exact boundary each mode means for it.
type ConfirmationMode uint8

// The confirmation modes.
const (
	// ConfirmDefault is the zero value and resolves to ConfirmAccepted.
	ConfirmDefault ConfirmationMode = iota
	// ConfirmNone waits for local or native-client admission only. It may be
	// lossy.
	ConfirmNone
	// ConfirmAccepted waits for the strongest ordinary acknowledgement the
	// profile offers without extra round trips.
	ConfirmAccepted
	// ConfirmDurable waits for a durable boundary. A publisher asking for it
	// fails at construction on a profile that cannot define one.
	ConfirmDurable

	confirmationModeCount
)

var confirmationModeNames = [confirmationModeCount]string{"default", "none", "accepted", "durable"}

// String returns the mode's lower-case name.
func (c ConfirmationMode) String() string {
	if c < confirmationModeCount {
		return confirmationModeNames[c]
	}
	return "ConfirmationMode(?)"
}

// Valid reports whether c names a confirmation mode.
func (c ConfirmationMode) Valid() bool { return c < confirmationModeCount }

// Resolve maps ConfirmDefault to the mode it stands for and returns every
// other mode unchanged.
func (c ConfirmationMode) Resolve() ConfirmationMode {
	if c == ConfirmDefault {
		return ConfirmAccepted
	}
	return c
}

// MetadataMode is the answer to metadata a broker has no native field for.
// All four modes ship and strict is the default; the three explicit ones
// change the bytes on the wire or lose metadata, so none of them is reached
// by accident.
type MetadataMode uint8

// The metadata modes.
const (
	// MetadataNativeStrict is the zero value and the default: the body is
	// unchanged, metadata maps to native fields, and unsupported populated
	// metadata fails with ErrUnsupported before any I/O.
	MetadataNativeStrict MetadataMode = iota
	// MetadataPortableEnvelope always wraps the message in iqmq's envelope,
	// which carries binary and duplicate headers everywhere and changes body
	// interoperability.
	MetadataPortableEnvelope
	// MetadataAdaptive uses native fields where they suffice and the
	// envelope where they do not.
	MetadataAdaptive
	// MetadataLenientDrop drops unsupported metadata with a hook and a
	// metric. It cannot satisfy a metadata requirement.
	MetadataLenientDrop

	metadataModeCount
)

var metadataModeNames = [metadataModeCount]string{"nativestrict", "portableenvelope", "adaptive", "lenientdrop"}

// String returns the mode's lower-case name.
func (m MetadataMode) String() string {
	if m < metadataModeCount {
		return metadataModeNames[m]
	}
	return "MetadataMode(?)"
}

// Valid reports whether m names a metadata mode.
func (m MetadataMode) Valid() bool { return m < metadataModeCount }

// StartKind is which of the four start positions a StartPosition names.
type StartKind uint8

// The start kinds. StartKindUnset is the zero value and resolves to
// StartKindLatest, which is default.
const (
	// StartKindUnset is the zero value: the caller did not say.
	StartKindUnset StartKind = iota
	// StartKindLatest starts at the end of the topic.
	StartKindLatest
	// StartKindEarliest starts at the oldest retained message.
	StartKindEarliest
	// StartKindTime starts at the first message at or after a time.
	StartKindTime
	// StartKindSequence starts at a broker sequence.
	StartKindSequence

	startKindCount
)

var startKindNames = [startKindCount]string{"unset", "latest", "earliest", "time", "sequence"}

// String returns the kind's lower-case name.
func (s StartKind) String() string {
	if s < startKindCount {
		return startKindNames[s]
	}
	return "StartKind(?)"
}

// Valid reports whether s names a start kind.
func (s StartKind) Valid() bool { return s < startKindCount }

// StartPosition is where a subscription with no committed position begins
// It is opaque because two of its four forms carry a value: a
// StartPosition is built by one of the four constructors and read through
// Kind, Time and Sequence.
//
// The zero value is the caller not having said, and resolves to
// StartLatest — default, so that a consumer of an unbounded
// topic does not replay its history by omission.
type StartPosition struct {
	kind StartKind
	at   time.Time
	seq  uint64
}

// StartLatest starts at the end of the topic: only messages published after
// the subscription exists are delivered.
func StartLatest() StartPosition { return StartPosition{kind: StartKindLatest} }

// StartEarliest starts at the oldest message the broker still retains.
func StartEarliest() StartPosition { return StartPosition{kind: StartKindEarliest} }

// StartAt starts at the first message at or after t. It needs
// FeatureSeekByTime.
func StartAt(t time.Time) StartPosition { return StartPosition{kind: StartKindTime, at: t} }

// StartAtSequence starts at the broker sequence n. It needs
// FeatureSeekBySequence.
func StartAtSequence(n uint64) StartPosition {
	return StartPosition{kind: StartKindSequence, seq: n}
}

// Kind reports which form this is.
func (s StartPosition) Kind() StartKind { return s.kind }

// Time is the time StartAt named, or the zero time for every other form.
func (s StartPosition) Time() time.Time { return s.at }

// Sequence is the sequence StartAtSequence named, or zero for every other
// form.
func (s StartPosition) Sequence() uint64 { return s.seq }

// Resolve maps the zero value to StartLatest and returns every other
// position unchanged. A driver reads a resolved position: the core resolves
// before it opens a source.
func (s StartPosition) Resolve() StartPosition {
	if s.kind == StartKindUnset {
		return StartLatest()
	}
	return s
}

// String returns the position's name, with its value where it has one.
func (s StartPosition) String() string {
	switch s.kind {
	case StartKindTime:
		return "time=" + s.at.Format(time.RFC3339Nano)
	case StartKindSequence:
		return "sequence=" + strconv.FormatUint(s.seq, 10)
	case StartKindUnset, StartKindLatest, StartKindEarliest:
		return s.kind.String()
	default:
		return s.kind.String()
	}
}
