// Package protocol holds the two ActiveMQ protocols as two implementations of
// one interface.
//
// It is in the root module rather than in either driver for the reason
// families, and the specification record — measures a family by running one `go test` in one package, and a root-module `go test`
// cannot reach a nested driver module  — and for a second
// reason that is this family's own: the two arms ARE the two drivers, so a
// package that belonged to either of them could not hold both.
//
// # The family is decided per broker as well as per protocol
//
// criterion is "on Artemis and Classic; feature coverage score
// (delay, groups, priority, TTL)", and the phrase "on Artemis and Classic" is
// not scene-setting: it makes the row's answer FOUR numbers rather than two.
// Every claim in this package therefore takes an [activemq.Broker] — the
// version floor, the support table, the count that summarises it — and
// the four combinations really do differ:
//
//   - the scheduled-delivery slot differs by broker AND by protocol. Over AMQP
//     1.0 Artemis reads a MESSAGE ANNOTATION and Classic reads an APPLICATION
//     PROPERTY, and each ignores the other's; over STOMP both read one frame
//     header. [DelaySlot] is that fact.
//   - de-duplication exists on Artemis (`_AMQ_DUPL_ID`) and not on Classic.
//   - a STOMP `selector` filters on Classic and is ignored on Artemis when the
//     subscription is a fully qualified queue name.
//   - a STOMP `NACK` returns nothing on either broker, while an AMQP 1.0
//     `modified{delivery-failed}` disposition returns the delivery and raises
//     `delivery-count` on both.
//
// # What this family decides, and what it does not
//
// The row's expected outcome is "feature completeness on Artemis, then perf →
// AMQP 1.0 default; STOMP kept for Classic". Its FIRST term decides it, and
// that term is not a benchmark metric:
//
//   - **Feature completeness** is [Protocol.Support] over the whole
//     inventory, and [Protocol.Features] is that table's own summary as a
//     number. It is reported as a benchmark metric so that the scorecard
//     carries it, but it is computed from the table rather than measured, and
//     the four conformance suites are what prove the table in both directions.
//   - **Supported-version compatibility** is [Protocol.MinBroker], which is
//     two versions per protocol because the two products gained the two
//     protocols at different releases.
//
// The second term, perf, is what the benchmark measures, and it measures the
// part of it that is honestly measurable without a broker: the cost of the
// PROPERTY MAPPING each protocol's message model demands. That is a real
// difference and it is on the hot path of every publication and every
// delivery — AMQP 1.0 has a header section, a properties section and typed
// application properties, STOMP has one flat set of TEXT frame headers and no
// slot at all for a message key, a content encoding or a binary value. What it
// is NOT is the two protocols' throughput against the real brokers, which
// needs two brokers and two client libraries; records
// that as the criterion's gap.
//
// # Why the mapping lives here rather than in each driver
//
// Both drivers must map one `mq.Message` onto their protocol's property model,
// and a caller who moves between them must find the same message on the other
// side: the same content type, the same identifier, the same headers, the same
// key. Two independent mappings would drift, and the half that drifted would
// be the one with fewer users. So the decision — which iqmq field goes into
// which protocol slot, on which broker, and what happens to what does not fit
// — is here, in [Protocol.Carry] and [Protocol.Restore].
//
// [Fields] is deliberately a reusable buffer: `Carry` takes a `*Fields` the
// caller owns and truncates its slices rather than allocating, so putting the
// shared mapping in the middle of the publish path costs no allocation per
// message. `TestAllocs` is what holds it to that.
//
// # The clock is not here
//
// Two of ActiveMQ's slots are ABSOLUTE times where the option is relative:
// STOMP's `expires` is an epoch-millisecond instant and has no relative form
// at all, and a delay stated as `PublishOptions.DeliverAt` has to become a
// duration for the STOMP header. Resolving either needs the clock, `internal/*`
// forbids `time.Now` in non-test code, and a mapping that read the clock could
// not be a pure function of its arguments anyway. So [Options] carries the
// values the DRIVER resolved against the injected clock, and a publication
// that needs one and did not bring it is refused with
// [ErrNeedsAbsoluteExpiry] or [ErrNeedsResolvedDelay] rather than silently
// losing its expiry.
package protocol

import (
	"time"
	"unicode/utf8"

	mq "bitbucket.org/iqhive/iqmq"
	"bitbucket.org/iqhive/iqmq/internal/activemq"
	"bitbucket.org/iqhive/iqmq/internal/errcode"
)

// Kind identifies a protocol variant. Auto resolves to AutoKind.
type Kind uint8

// The kinds. AMQP10 is the reference implementation and the
// default, because it is the arm with the higher feature coverage on Artemis,
// which is what criterion is read on first.
const (
	// Auto selects AutoKind, the bake-off winner recorded in auto.go.
	Auto Kind = iota
	// AMQP10 is AMQP 1.0, spoken by Artemis and by Classic 5.9 and newer.
	AMQP10
	// STOMP is STOMP 1.2, spoken by Artemis and by Classic 5.6 and newer.
	STOMP

	kindCount
)

var kindSlugs = [kindCount]string{"auto", "amqp10", "stomp"}

var kindIdents = [kindCount]string{"Auto", "AMQP10", "STOMP"}

// String returns the kind's slug as used in benchmark names, DSN parameters
// and ADRs.
func (k Kind) String() string {
	if k < kindCount {
		return kindSlugs[k]
	}
	return "kind(?)"
}

// Ident returns the kind's Go identifier.
func (k Kind) Ident() string {
	if k < kindCount {
		return kindIdents[k]
	}
	return ""
}

// Valid reports whether k names a concrete kind or Auto.
func (k Kind) Valid() bool { return k < kindCount }

// Resolve maps Auto to AutoKind and returns every other kind unchanged.
func (k Kind) Resolve() Kind {
	if k == Auto {
		return AutoKind
	}
	return k
}

// ParseKind returns the kind with the given slug.
func ParseKind(slug string) (Kind, bool) {
	for i, s := range kindSlugs {
		if s == slug {
			return Kind(i), true
		}
	}
	return 0, false
}

// Kinds lists every concrete kind in declaration order (Auto excluded).
func Kinds() []Kind {
	out := make([]Kind, 0, kindCount-1)
	for k := AMQP10; k < kindCount; k++ {
		out = append(out, k)
	}
	return out
}

// Slugs lists every concrete kind's slug.
func Slugs() []string {
	out := make([]string, 0, kindCount-1)
	for _, k := range Kinds() {
		out = append(out, k.String())
	}
	return out
}

// Role is what a destination is for.
type Role uint8

// The two roles a destination is derived for.
const (
	// Publish is a destination a message is sent to.
	Publish Role = iota
	// Consume is a source a consumer attaches to.
	Consume
)

// ValueKind is the type of one extensible-table entry.
//
// The three are what the two protocols' tables can carry between them: a Go
// string the mapping sourced from a typed property, an opaque byte string
// borrowed from the caller, and an integer for a broker's own numeric
// arguments.
//
// Kind is the value's STORAGE and its type, not the section it lands in. What
// each protocol then does with it is where the two arms differ, and it is what
// the two `mq.FeatureBinaryHeaders` rows record: AMQP 1.0's application
// properties are typed, so a Binary entry goes on the wire as a binary value
// byte for byte, while a STOMP frame header is TEXT and the STOMP arm has
// therefore already refused any header value that is not valid UTF-8 by the
// time it produces the entry.
type ValueKind uint8

// The entry value kinds.
const (
	// Text is a UTF-8 string held in Entry.Text.
	Text ValueKind = iota
	// Binary is an opaque byte string held in Entry.Bin. It BORROWS the
	// caller's storage (a synchronous publish retains nothing past
	// the call), so copying it here would allocate per message for nothing.
	Binary
	// Int is a signed integer held in Entry.Int, for a broker's own numeric
	// arguments such as `AMQ_SCHEDULED_DELAY`.
	Int

	valueKindCount
)

var valueKindNames = [valueKindCount]string{"text", "binary", "int"}

// String returns the kind's lower-case name.
func (v ValueKind) String() string {
	if v < valueKindCount {
		return valueKindNames[v]
	}
	return "ValueKind(?)"
}

// Valid reports whether v names a value kind.
func (v ValueKind) Valid() bool { return v < valueKindCount }

// Entry is one extensible-table entry: an application property on AMQP 1.0, a
// frame header on STOMP.
type Entry struct {
	// Key is the entry's name.
	Key string
	// Text is the value when Kind is Text.
	Text string
	// Bin is the value when Kind is Binary. It borrows the caller's storage.
	Bin []byte
	// Int is the value when Kind is Int.
	Int int64
	// Kind is which of the three the value is.
	Kind ValueKind
}

// value is the entry's value as bytes.
//
// A Binary entry answers without allocating, which is the steady path: every
// header a caller set is Binary, because `mq.Headers` values are `[]byte`. A
// Text or Int entry converts, and the only way one reaches [Protocol.Restore]
// is from a broker that sent it — nothing [Protocol.Carry] produces as an
// application header is either.
func (e *Entry) value() []byte {
	switch e.Kind {
	case Binary:
		return e.Bin
	case Int:
		return []byte(decimal(e.Int))
	case Text, valueKindCount:
	}
	return []byte(e.Text)
}

// text is the entry's value as a string, for the typed properties that are
// strings. A Binary entry converts; a Text one does not.
func (e *Entry) text() string {
	switch e.Kind {
	case Binary:
		return string(e.Bin)
	case Int:
		return decimal(e.Int)
	case Text, valueKindCount:
	}
	return e.Text
}

// DelaySlot is WHERE a scheduled publication's delay is carried, which on
// ActiveMQ is a fact about the broker as well as about the protocol.
//
// Every value below was measured against `apache/activemq-artemis:2.44.0-alpine`
// and `apache/activemq-classic:6.2.0`, and the measurement is the reason the
// slot is a value in this package rather than a constant in each driver: the
// wrong slot is not an error, it is a publication that is DELIVERED AT ONCE.
type DelaySlot uint8

// The delay slots.
const (
	// NoDelay is a publication with no delay, which writes nothing at all.
	NoDelay DelaySlot = iota
	// DelayAnnotation is Artemis's AMQP 1.0 MESSAGE ANNOTATION section.
	//
	// Measured: `x-opt-delivery-delay` (whole milliseconds, int64) and
	// `x-opt-delivery-time` (absolute epoch milliseconds) are both honoured
	// on Artemis as annotations, with a plain `string` key and with an
	// `amqp.Symbol` key alike. Measured on Classic: an annotation is IGNORED
	// and the message is delivered immediately.
	DelayAnnotation
	// DelayProperty is Classic's AMQP 1.0 APPLICATION-PROPERTY section.
	//
	// Measured: `AMQ_SCHEDULED_DELAY` as an application property is honoured
	// on Classic. Measured on Artemis: `AMQ_SCHEDULED_DELAY` as an
	// ANNOTATION is ignored, which is why the section and not only the key
	// is part of this enumeration.
	DelayProperty
	// DelayHeader is the STOMP frame header, on BOTH brokers.
	//
	// Measured: `AMQ_SCHEDULED_DELAY` held a publication back 2.5 s on
	// Artemis (echoed on delivery as `_AMQ_SCHED_DELIVERY`, an absolute
	// epoch time) and 2.6 s on Classic (echoed as `scheduledJobId`). STOMP
	// has no absolute form, so [DelaySlot.Keys] answers with an empty second
	// key and a caller's `DeliverAt` becomes a duration the driver resolved.
	DelayHeader

	delaySlotCount
)

var delaySlotNames = [delaySlotCount]string{"none", "annotation", "property", "header"}

// String returns the slot's lower-case name.
func (s DelaySlot) String() string {
	if s < delaySlotCount {
		return delaySlotNames[s]
	}
	return "DelaySlot(?)"
}

// Valid reports whether s names a slot.
func (s DelaySlot) Valid() bool { return s < delaySlotCount }

// Keys returns the key the relative delay is written under and the key the
// absolute delivery time is written under, both empty for [NoDelay] and the
// second empty for [DelayHeader], which has no absolute form.
func (s DelaySlot) Keys() (delay, at string) {
	switch s {
	case DelayAnnotation:
		return AnnotationDeliveryDelay, AnnotationDeliveryTime
	case DelayProperty:
		return PropertyScheduledDelay, PropertyScheduledTime
	case DelayHeader:
		return PropertyScheduledDelay, ""
	case NoDelay, delaySlotCount:
	}
	return "", ""
}

// The broker's own key names, measured against the two pinned images.
//
// They are constants here rather than in either driver because they are the
// part of a protocol that is a DECISION: which key, in which section, on which
// broker, and what the broker echoes back under a different name again.
const (
	// AnnotationDeliveryDelay is Artemis's relative scheduled-delivery
	// annotation, in whole milliseconds.
	AnnotationDeliveryDelay = "x-opt-delivery-delay"
	// AnnotationDeliveryTime is Artemis's absolute scheduled-delivery
	// annotation, in epoch milliseconds.
	AnnotationDeliveryTime = "x-opt-delivery-time"
	// PropertyScheduledDelay is Classic's relative scheduled-delivery
	// application property and both brokers' STOMP frame header.
	PropertyScheduledDelay = "AMQ_SCHEDULED_DELAY"
	// PropertyScheduledTime is Classic's absolute scheduled-delivery
	// application property.
	PropertyScheduledTime = "AMQ_SCHEDULED_TIME"
	// PropertyDuplicateID is Artemis's de-duplication property. Classic has
	// no equivalent, which is why [Protocol.Support] answers
	// FeatureDeduplication differently for the two brokers.
	PropertyDuplicateID = "_AMQ_DUPL_ID"
	// PropertyGroupID is the message-group key both protocols use, spelled
	// the way JMS spells it.
	//
	// Measured over AMQP 1.0: two competing consumers on one Artemis queue
	// received the `k0` messages exclusively on one and the `k1` messages
	// exclusively on the other, so the group is the key-ordering mechanism
	// The specification calls "groups".
	PropertyGroupID = "JMSXGroupID"
	// PropertyArtemisGroupID is what Artemis ECHOES a group identifier as on
	// delivery.
	//
	// Measured over STOMP: Artemis echoes `_AMQ_GROUP_ID` and Classic echoes
	// nothing at all, so a consumer reads both spellings and is promised
	// neither.
	PropertyArtemisGroupID = "_AMQ_GROUP_ID"
	// PropertyArtemisScheduled is what Artemis echoes a scheduled delivery
	// as: an absolute epoch time, not the delay that was sent.
	PropertyArtemisScheduled = "_AMQ_SCHED_DELIVERY"
	// PropertyClassicScheduledJob is what Classic echoes a scheduled
	// delivery as: the broker's own job identifier.
	PropertyClassicScheduledJob = "scheduledJobId"
)

// Fields is one message's protocol-level form: the slots the protocol has a
// name for, and the entries that travel in its extensible table.
//
// It is a REUSABLE BUFFER and not a value a mapping returns. [Protocol.Carry]
// takes a pointer the caller owns and truncates its slice in place, so a sink
// holds one per sink and the shared mapping costs no allocation per
// publication. `TestAllocs` holds it to that.
//
// The typed fields are everything either protocol has a NAMED slot for — an
// AMQP 1.0 header-section or properties-section field, or a STOMP frame header
// the protocol itself defines — and [Fields.Props] is the extensible table the
// rest travels in. A field one arm has no slot for is left at its zero value
// and the arm's own file says where the value went instead: STOMP has no
// content-encoding header, for one, so that property travels as a reserved
// entry (see [EncodingHeader]).
//
// There is deliberately no second slice for AMQP 1.0's MESSAGE ANNOTATIONS,
// even though the annotation section is where Artemis reads a delay from. The
// only annotation either broker reads is the scheduled delivery, and
// [Fields.Slot] names the section it belongs in — so a second slice would hold
// at most one entry and every driver would have to know which one.
type Fields struct {
	// ContentType is AMQP 1.0's `content-type` property and STOMP's
	// `content-type` frame header.
	ContentType string
	// ContentEncoding is AMQP 1.0's `content-encoding` property. STOMP has
	// no such header, so the STOMP arm leaves this empty and carries the
	// value as the reserved entry [EncodingHeader] instead.
	ContentEncoding string
	// MessageID is the message identifier: AMQP 1.0's `message-id` property,
	// STOMP's `message-id` frame header. On a DELIVERY the STOMP
	// `message-id` is the BROKER's identifier and not the publisher's, which
	// is why the STOMP arm does not restore it over a publisher's own.
	MessageID string
	// CorrelationID and ReplyTo are the request/reply properties. Both
	// protocols have a named slot for each.
	CorrelationID string
	ReplyTo       string
	// Receipt is the publication's own receipt identifier: STOMP's `receipt`
	// header on a SEND frame, for which the broker answers with a RECEIPT
	// frame. It is empty for AMQP 1.0, whose publication is resolved by its
	// own disposition instead.
	Receipt string
	// GroupID is the message group, which on ActiveMQ is how a key is
	// ordered: AMQP 1.0's `group-id` property, STOMP's [PropertyGroupID]
	// frame header.
	//
	// It BORROWS the message's key storage rather than converting it to a
	// string, because a conversion here would allocate on every keyed
	// publication and the specification makes the borrow safe for the life of a
	// synchronous publish. The driver converts once, at the client-library
	// boundary.
	GroupID []byte
	// TTL is AMQP 1.0's header-section `ttl`, a RELATIVE duration. It is
	// filled by the AMQP 1.0 arm and left zero by the STOMP one, which has
	// no relative expiry at all.
	TTL time.Duration
	// Expiry is the ABSOLUTE expiry: AMQP 1.0's `absolute-expiry-time`
	// property and STOMP's `expires` frame header, both epoch milliseconds
	// on the wire. STOMP has only this form, so a relative TTL is the
	// driver's arithmetic against the injected clock and arrives here in
	// [Options.Expiry].
	Expiry time.Time
	// Timestamp is when the application created the message (the specification's
	// Properties.CreatedAt): AMQP 1.0's `creation-time`, STOMP's
	// `timestamp`. It is the zero time when the caller did not say, which
	// The specification makes distinct from a timestamp of zero.
	Timestamp time.Time
	// Delay is the relative scheduled-delivery delay, rounded UP to whole
	// milliseconds so that a publication is never released earlier than the
	// caller asked. Slot says which key and which section it belongs in.
	Delay time.Duration
	// DeliverAt is the absolute scheduled-delivery time, set only when the
	// caller named one and the slot has an absolute form.
	DeliverAt time.Time
	// Slot is where Delay and DeliverAt are written, which differs by broker
	// as well as by protocol. It is [NoDelay] for a publication with no
	// delay, and a driver that writes nothing then is writing the truth.
	Slot DelaySlot
	// DeliveryCount is AMQP 1.0's header-section `delivery-count`: the
	// number of PREVIOUS delivery attempts, so zero on the first transfer.
	// [Attempt] is what turns it into the attempt, which counts from one.
	//
	// It is always zero over STOMP: measured on both brokers, a STOMP
	// delivery carries no `delivery-count`, no `JMSXDeliveryCount` and no
	// `_AMQ_DELIVERY_COUNT`.
	DeliveryCount int
	// Redelivered is STOMP's `redelivered` frame header, which is a BOOLEAN.
	// It is the whole of what a STOMP delivery says about its attempt, and
	// the asymmetry against AMQP 1.0's real count is one of this family's
	// most important measured differences.
	Redelivered bool
	// Priority is the message priority and HasPriority whether the caller
	// named one. The pair exists because a priority of zero is a real
	// priority.
	Priority    uint8
	HasPriority bool
	// Durable is AMQP 1.0's header-section `durable` and STOMP's
	// `persistent: true|false` frame header.
	Durable bool
	// Props are the extensible-table entries: the application properties on
	// AMQP 1.0, the frame headers on STOMP.
	Props []Entry
	// Dropped counts the metadata this mapping left out under
	// MetadataLenientDrop, which the specification requires a driver to report through a
	// hook and a metric rather than silently.
	Dropped int
}

// Reset truncates the buffer for reuse without releasing its storage, which is
// what makes the shared mapping allocation-free on the steady path.
func (f *Fields) Reset() {
	props := f.Props[:0]
	*f = Fields{Props: props}
}

// Options is what a mapping needs beyond the message itself.
type Options struct {
	// Broker is which ActiveMQ the message is going to. It is REQUIRED and
	// there is no Auto: the delay slot and the de-duplication property
	// differ between the two products, and nothing on the wire says which
	// one a caller meant, so a mapping that guessed would put a delay in a
	// section the broker ignores and deliver the message at once. A mapping
	// with no broker is refused with `activemq.ErrNoBroker` rather than
	// defaulted.
	Broker activemq.Broker
	// Metadata is the mode. It decides what happens to metadata the
	// protocol has no native place for: every mode but MetadataLenientDrop
	// refuses it, and the driver's own `driverkit.Envelope` has already
	// wrapped anything unrepresentable under the two enveloping modes, so a
	// refusal here under those modes is a driver defect rather than a
	// caller's.
	Metadata mq.MetadataMode
	// Persistent asks for a durable message: `durable=true` on AMQP 1.0,
	// `persistent: true` on STOMP.
	Persistent bool
	// KeyHeader is the entry the message key travels in, or empty when the
	// key is not carried as an entry.
	//
	// NEITHER protocol has a slot for a message key, so unlike the RabbitMQ
	// pair there is no routing key to put it in and the key always costs an
	// entry (see [KeyHeader], the name it is given). Over STOMP that entry
	// is a TEXT frame header, so a key that is not valid UTF-8 cannot be
	// carried at all and is refused rather than mangled — which is a cost of
	// the STOMP arm and not of the key.
	KeyHeader string
	// KeyAsGroup asks that the key also become the message group, which is
	// what makes per-key ordering work: the broker hands one group to one
	// consumer at a time.
	KeyAsGroup bool
	// Receipt is the receipt identifier a STOMP publication asks the broker
	// to answer with. It is ignored by the AMQP 1.0 arm.
	Receipt string
	// Delay is the relative delay the DRIVER resolved, from
	// PublishOptions.Delay or from PublishOptions.DeliverAt against the
	// injected clock. A publication that asked to be delayed and brought
	// neither is refused with [ErrNeedsResolvedDelay].
	Delay time.Duration
	// Expiry is the absolute expiry the DRIVER resolved from
	// PublishOptions.TTL against the injected clock. STOMP's `expires` has
	// no relative form, so a STOMP publication with a TTL and no resolved
	// expiry is refused with [ErrNeedsAbsoluteExpiry].
	Expiry time.Time
	// Publish is the per-call publication control.
	Publish mq.PublishOptions
	// Limits bounds what the mapping accepts, so that an oversized header is
	// refused before any I/O rather than by the broker with a frame error or
	// a detached link. The zero value bounds nothing.
	Limits mq.Limits
}

// Protocol is one of the two arms.
//
// Every method is pure and safe to call from any goroutine: a protocol holds
// no state, so one value serves every sink and source of a client. Every claim
// takes the broker, because criterion is read "on Artemis and
// Classic" and the two products do not agree.
type Protocol interface {
	// Kind returns the variant.
	Kind() Kind
	// MinBroker is the oldest release of b this protocol can be used
	// against. It is the supported-version compatibility, and it is two
	// versions per protocol rather than one because the two products gained
	// the two protocols at different releases.
	MinBroker(b activemq.Broker) activemq.Version
	// Support is what this protocol can do for one feature of the
	// inventory, on that broker. It is the protocol-and-broker's half of a
	// driver's capability table: the driver narrows a row for what its own
	// code does not implement, and never widens one.
	Support(b activemq.Broker, f mq.Feature) mq.Support
	// Features is how many of the inventory this protocol supports
	// NATIVELY on b, which is the specification's "feature coverage score" as one number.
	// It is computed from the table rather than measured.
	Features(b activemq.Broker) int
	// Destination is the destination in this protocol's own form, taken from
	// a name set the caller derived with the right flags, and refused when
	// the name set is for the other protocol or is empty.
	Destination(n activemq.Names, role Role) (string, error)
	// Carry maps a message into this protocol's property model, filling dst,
	// which it resets first. It allocates nothing when dst's slice already
	// has room.
	Carry(dst *Fields, m *mq.Message, o Options) error
	// Restore maps a delivery's fields back into a message, which is the
	// receive direction of the same mapping. It fills dst's headers and
	// properties and leaves dst's Body alone: the body is the driver's, and
	// copying it here would make the mapping allocate per delivery.
	Restore(dst *mq.Message, f *Fields, o Options) error
	// MaxMessageBytes is the largest message this protocol's framing allows,
	// or zero when there is no protocol-level bound to advertise.
	MaxMessageBytes() int64
}

// New returns the protocol of kind k.
func New(k Kind) (Protocol, error) {
	switch k.Resolve() {
	case AMQP10:
		return amqp10{}, nil
	case STOMP:
		return stomp{}, nil
	case Auto:
		// Unreachable: Resolve maps Auto to a concrete kind. Returning the
		// reference kind rather than panicking keeps a future AutoKind typo
		// from crashing a caller's Open.
		return amqp10{}, nil
	default:
		return nil, ErrInvalidKind
	}
}

// The family's refusals.
var (
	// ErrInvalidKind is a kind outside the enumeration.
	ErrInvalidKind = errcode.New(errcode.InvalidConfig,
		"iqmq/activemq: the protocol is not a known kind")
	// ErrDuplicateHeader is a repeated header key under a mode that does not
	// envelope.
	//
	// Neither protocol preserves one. AMQP 1.0's application properties are
	// a MAP, so the second value replaces the first; a STOMP frame CAN carry
	// a repeated header name, and STOMP 1.2 says the first occurrence wins,
	// so the second value is carried on the wire and then discarded — which
	// is worse than a map, because nothing reports it. The specification promises
	// duplicate-preserving headers, so the honest answer is a refusal and
	// the route that keeps the promise is MetadataPortableEnvelope or
	// MetadataAdaptive.
	ErrDuplicateHeader = errcode.New(errcode.Unsupported,
		"iqmq/activemq: neither ActiveMQ protocol preserves a repeated header key; use MetadataPortableEnvelope or MetadataAdaptive")
	// ErrReservedHeader is a header under the `x-iqmq-` prefix, which these
	// drivers use for the slots neither protocol has.
	ErrReservedHeader = errcode.New(errcode.InvalidArgument,
		"iqmq/activemq: the header key is under the reserved \"x-iqmq-\" prefix, which these drivers use for the slots neither protocol has")
	// ErrProtocolHeader is a header whose name collides with a STOMP frame
	// header or with one of the broker's own.
	//
	// It is refused rather than shadowed because both directions of the
	// shadow are silent: a caller's `expires` header would become the
	// message's expiry, and a caller's `destination` header would address
	// another queue.
	ErrProtocolHeader = errcode.New(errcode.InvalidArgument,
		"iqmq/activemq: the header key is a STOMP frame header or one of the broker's own, which would be shadowed rather than carried")
	// ErrTextOnly is a header value or a key that is not valid UTF-8, over a
	// protocol whose every value is text.
	ErrTextOnly = errcode.New(errcode.Unsupported,
		"iqmq/activemq: a STOMP frame header is text, so a value that is not valid UTF-8 cannot be carried; use MetadataPortableEnvelope or MetadataAdaptive")
	// ErrEmptyHeaderKey is a header with no name.
	//
	// Neither protocol can carry one, and neither fails cleanly on it: an
	// AMQP 1.0 application property keyed on the empty string is a property
	// nothing will show you, and a STOMP frame header with an empty name is
	// a malformed frame the broker answers with an ERROR that closes the
	// connection — taking every other publication in flight with it. So it
	// is refused before any I/O, on both arms.
	ErrEmptyHeaderKey = errcode.New(errcode.InvalidArgument,
		"iqmq/activemq: a header key is empty, and neither protocol can name a value with nothing")
	// ErrHeaderTooLarge is a header key or value over the configured limit.
	ErrHeaderTooLarge = errcode.New(errcode.MessageTooLarge,
		"iqmq/activemq: the header key or value is over the advertised limit")
	// ErrNoDestination is a name set with nothing to address in the role
	// that was asked for.
	ErrNoDestination = errcode.New(errcode.InvalidArgument,
		"iqmq/activemq: the name set has no destination for that role")
	// ErrWrongDestinationForm is a name set derived for the OTHER protocol.
	//
	// On Classic a destination's own shape is what tells the broker whether
	// a name is a queue or a topic — AMQP 1.0 carries `topic://` and
	// `queue://`, STOMP carries `/topic/` and `/queue/` — so a name set
	// derived with the wrong flag does not fail. It publishes to a QUEUE
	// called `VirtualTopic.orders`, which every consumer queue is subscribed
	// to and none of them receives anything from. Refusing the other
	// protocol's form is the only place that defect can be caught before it
	// becomes a topology.
	ErrWrongDestinationForm = errcode.New(errcode.InvalidConfig,
		"iqmq/activemq: the destination is in the other protocol's form; derive the names with activemq.Config.Scheme for AMQP 1.0 and activemq.Config.STOMP for STOMP")
	// ErrNeedsAbsoluteExpiry is a STOMP publication with a relative TTL and
	// no absolute expiry resolved for it.
	ErrNeedsAbsoluteExpiry = errcode.New(errcode.InvalidArgument,
		"iqmq/activemq: STOMP's `expires` header is an absolute instant and this mapping does not read the clock; resolve Options.Expiry from the injected clock")
	// ErrNeedsResolvedDelay is a publication that asked to be delayed with
	// no delay resolved for it.
	ErrNeedsResolvedDelay = errcode.New(errcode.InvalidArgument,
		"iqmq/activemq: a scheduled publication needs Options.Delay resolved from the injected clock; this mapping does not read the clock")
	// ErrNoDeduplication is a de-duplication key on a broker that has none.
	ErrNoDeduplication = errcode.New(errcode.Unsupported,
		"iqmq/activemq: ActiveMQ Classic has no broker-side de-duplication; only Artemis reads _AMQ_DUPL_ID")
	// ErrBadMetadataMode is a metadata mode outside the four.
	ErrBadMetadataMode = errcode.New(errcode.InvalidConfig,
		"iqmq/activemq: the metadata mode is not one of the message contract's four")
)

// ReservedHeaderPrefix is the prefix these drivers reserve for their own
// extensible-table entries.
//
// ActiveMQ needs FOUR of them where RabbitMQ needs two, and the reason is that
// neither ActiveMQ protocol has a slot for a message key at all: there is no
// routing key to put one in, so unlike `internal/rabbit/protocol`'s
// `WithKeyAsRoutingKey` the key ALWAYS costs an entry. STOMP adds two more,
// because it has no `content-encoding` header and its `message-id` belongs to
// the broker. The companion half of the same decision is which names a caller
// may NOT use: see [ReservedProperty] and [ReservedFrameHeader], which are two
// lists rather than one because a broker property clashes on both arms and a
// STOMP frame header on only one.
//
// A caller's header under the prefix is refused rather than shadowed, because
// the alternative is a header that silently means something to the driver: a
// message published with `x-iqmq-key` set by the application would arrive with
// a key the application never chose.
const ReservedHeaderPrefix = "x-iqmq-"

// The reserved entries, one per slot neither protocol has.
const (
	// KeyHeader is the entry the message key travels in. Neither protocol
	// has a key, so this is not a fallback: it is the mapping, and it is
	// what [Options.KeyHeader] is normally set to.
	//
	// What it costs over STOMP is stated once, here: a frame header is text,
	// so a key that is not valid UTF-8 is refused rather than carried. A
	// caller whose keys are binary needs MetadataPortableEnvelope, or the
	// AMQP 1.0 arm, whose application properties are typed.
	KeyHeader = ReservedHeaderPrefix + "key"
	// CausationHeader is the entry CausationID travels in. Neither
	// protocol has a slot for it.
	CausationHeader = ReservedHeaderPrefix + "causation"
	// EncodingHeader is the entry ContentEncoding travels in over
	// STOMP, which has no `content-encoding` frame header. The AMQP 1.0 arm
	// has the property and does not use this.
	EncodingHeader = ReservedHeaderPrefix + "content-encoding"
	// IDHeader is the entry Message.ID travels in over STOMP.
	//
	// STOMP does have a `message-id` header and it is NOT the publisher's: on
	// a delivery it is the BROKER's identifier for that delivery, which is
	// why it is in [ReservedFrameHeader]'s list. So a publisher's identifier
	// sent as `message-id` would be overwritten in transit, and the only way
	// to give it back to a consumer is an entry of these drivers' own. The
	// AMQP 1.0 arm has a real `message-id` property and does not use this.
	IDHeader = ReservedHeaderPrefix + "id"
)

// ReservedHeader reports whether key is one of these drivers' own entries.
//
// A round trip must not hand one back as an application header: a message that
// arrived with an `x-iqmq-key` header the publisher never set would fail
// the fidelity scenario, and an application that echoed its headers would
// publish a key it did not choose.
func ReservedHeader(key string) bool { return hasPrefix(key, ReservedHeaderPrefix) }

// brokerProperties are the MESSAGE-PROPERTY names the brokers themselves read,
// whichever protocol carries them.
//
// They are refused on publication and filtered on delivery, from one list, for
// the same reason in both directions: a caller's header under one of these
// names would MEAN something to the broker on the way out — an application
// property called `JMSXGroupID` really is the message group over AMQP 1.0, and
// one called `_AMQ_DUPL_ID` really is a de-duplication key on Artemis — and
// the broker's own would be handed back as the application's on the way in.
//
// The distinction against [stompFrameHeaders] is not tidiness, and the fuzz
// target is what found it: `destination` is a FRAME header and is nobody's
// property, so refusing it on the AMQP 1.0 arm would refuse a header that arm
// can carry perfectly well, while refusing `JMSXGroupID` there is necessary
// because that one is read. Two lists rather than one, per arm rather than per
// family — which is the other half of what these drivers may not carry, so it
// shares the row with the reserved prefix.
var brokerProperties = map[string]struct{}{
	PropertyGroupID:             {},
	PropertyScheduledDelay:      {},
	PropertyScheduledTime:       {},
	PropertyClassicScheduledJob: {},
}

// stompFrameHeaders are the names STOMP's own FRAMES use, which clash on the
// STOMP arm and on no other.
//
// A Classic delivery really does carry most of this list — `destination`,
// `message-id`, `subscription`, `ack`, `timestamp`, `expires`, `priority`,
// `persistent`, `redelivered`, `content-length` and `scheduledJobId` were all
// measured on one — so the filter is not theoretical either.
var stompFrameHeaders = map[string]struct{}{
	"destination":    {},
	"content-type":   {},
	"content-length": {},
	"receipt":        {},
	"receipt-id":     {},
	"message-id":     {},
	"subscription":   {},
	"ack":            {},
	"redelivered":    {},
	"priority":       {},
	"expires":        {},
	"persistent":     {},
	"timestamp":      {},
	"reply-to":       {},
	"correlation-id": {},
	"selector":       {},
}

// artemisPrefix is the prefix Artemis puts its own message properties under
// (`_AMQ_DUPL_ID`, `_AMQ_GROUP_ID`, `_AMQ_SCHED_DELIVERY`, and every one a
// future release adds). Matching the prefix rather than a list is what keeps a
// new one from reaching an application as a header.
const artemisPrefix = "_AMQ_"

// ReservedProperty reports whether key is a message-property name a broker
// reads on EITHER protocol, and is therefore neither carried from an
// application nor handed back to one on either arm.
func ReservedProperty(key string) bool {
	if hasPrefix(key, artemisPrefix) {
		return true
	}
	_, ok := brokerProperties[key]
	return ok
}

// ReservedFrameHeader reports whether key is a STOMP frame header or one of
// the brokers' own message properties, and is therefore neither carried from
// an application nor handed back to one OVER STOMP.
//
// It is the union: a frame header clashes only on the STOMP arm, and a broker
// property clashes on both, so this is the STOMP arm's whole list and
// [ReservedProperty] is the part of it the AMQP 1.0 arm shares.
func ReservedFrameHeader(key string) bool {
	if ReservedProperty(key) {
		return true
	}
	_, ok := stompFrameHeaders[key]
	return ok
}

// Attempt is the delivery attempt, counted from ONE, and whether the
// protocol can say it exactly.
//
// This is the family's sharpest measured asymmetry after the NACK finding.
// AMQP 1.0 carries `delivery-count` in the header section — the number of
// PREVIOUS attempts, so `delivery-count + 1` is the attempt, measured as 0, 1
// and 2 across three rounds on both brokers — and STOMP carries a BOOLEAN
// `redelivered` and nothing else: measured on both brokers, a STOMP delivery
// has no `delivery-count`, no `JMSXDeliveryCount` and no `_AMQ_DELIVERY_COUNT`.
// So over STOMP the attempt is exactly 1 on a first delivery and AT LEAST 2
// after that, and exact is false for the second case rather than 2 being
// reported as a fact.
//
// NEITHER DRIVER CALLS IT, and that is worth stating rather than hiding. The
// AMQP 1.0 arm takes the number from the core's own `AttemptFromHeader` rule,
// which reads the same `delivery-count` one layer down; and the STOMP arm
// reports ZERO -- the value for "the broker does not count attempts" --
// because the `redelivered` boolean turned out not to be trustworthy either:
// measured on Artemis, the redelivery a REQUEUE produces arrives with
// `redelivered: false`, so "at least 2" is not available and 1 twice running
// is worse than nothing. What this function is, then, is
// the protocol fact stated once and asserted by the model and the fuzz
// target: it is what a caller of this package would get if it trusted the
// header, and the drivers' own answers are the measured corrections to it.
func Attempt(k Kind, f *Fields) (attempt int, exact bool) {
	if f == nil {
		return 1, true
	}
	switch k.Resolve() {
	case STOMP:
		if f.Redelivered {
			return 2, false
		}
		return 1, true
	case AMQP10, Auto, kindCount:
	}
	n := f.DeliveryCount
	if n < 0 {
		n = 0
	}
	return n + 1, true
}

// EchoedGroup reads the message group out of a delivery's entries, in the two
// spellings the brokers echo it under, and reports whether either was there.
//
// Measured over STOMP: Artemis echoes [PropertyArtemisGroupID] and Classic
// echoes nothing at all, so a consumer that needed the group would have to
// read both and would still not be promised one. Over AMQP 1.0 the group is
// the `group-id` property and reaches [Fields.GroupID] directly; this is for
// the protocol that has no such section.
func EchoedGroup(f *Fields) (group []byte, known bool) {
	if f == nil {
		return nil, false
	}
	for _, name := range [...]string{PropertyGroupID, PropertyArtemisGroupID} {
		for i := range f.Props {
			if f.Props[i].Key == name {
				return f.Props[i].value(), true
			}
		}
	}
	return nil, false
}

// carry is the part of the mapping the two protocols share, which is rather
// less of it than the RabbitMQ pair share: the two ActiveMQ protocols agree on
// the fixed properties and on the header walk's rules, and differ on the
// scheduled-delivery slot, on whether a value may be binary, and on whether an
// expiry is relative or absolute.
//
// k is the arm, so that the three differences are decided here rather than
// duplicated in two files that would drift.
func carry(dst *Fields, m *mq.Message, o Options, k Kind) error {
	if !o.Metadata.Valid() {
		return ErrBadMetadataMode
	}
	if !o.Broker.Valid() {
		return activemq.ErrNoBroker
	}
	dst.Reset()
	if m == nil {
		return nil
	}
	text := k.Resolve() == STOMP
	lenient := o.Metadata == mq.MetadataLenientDrop

	dst.ContentType = m.Properties.ContentType
	dst.CorrelationID = m.Properties.CorrelationID
	dst.ReplyTo = m.Properties.ReplyTo
	dst.Timestamp = m.Properties.CreatedAt
	dst.Durable = o.Persistent
	if text {
		dst.Receipt = o.Receipt
	}
	if o.Publish.PrioritySet {
		dst.Priority, dst.HasPriority = o.Publish.Priority, true
	}
	if err := carryExpiry(dst, o, text); err != nil {
		return err
	}
	if err := carryDelay(dst, o, text); err != nil {
		return err
	}

	// The reserved entries: the slots neither protocol has. They are a
	// native mapping and not an emulation -- the value goes on the wire and
	// comes back, and a non-iqmq consumer sees a header it can read.
	if m.Properties.CausationID != "" {
		dst.Props = append(dst.Props, Entry{Key: CausationHeader, Text: m.Properties.CausationID})
	}
	if text {
		// STOMP has no `content-encoding` frame header at all, and its
		// `message-id` belongs to the broker rather than to the publisher, so
		// both properties travel as reserved entries. The AMQP 1.0 arm has a
		// properties-section field for each and fills those instead.
		if m.Properties.ContentEncoding != "" {
			dst.Props = append(dst.Props, Entry{Key: EncodingHeader, Text: m.Properties.ContentEncoding})
		}
		if m.ID != "" {
			dst.Props = append(dst.Props, Entry{Key: IDHeader, Text: m.ID})
		}
	} else {
		dst.ContentEncoding = m.Properties.ContentEncoding
		dst.MessageID = m.ID
	}
	if len(m.Key) > 0 {
		if text && !utf8.Valid(m.Key) {
			// A frame header is text, so a binary key cannot travel at all.
			// Mangling it would give the consumer a key the publisher never
			// chose, which is worse than a refusal nobody can miss.
			if !lenient {
				return ErrTextOnly
			}
			dst.Dropped++
		} else {
			if o.KeyHeader != "" {
				dst.Props = append(dst.Props, Entry{Key: o.KeyHeader, Bin: m.Key, Kind: Binary})
			}
			if o.KeyAsGroup {
				dst.GroupID = m.Key
			}
		}
	}
	if o.Publish.DedupKey != "" {
		if o.Broker != activemq.Artemis {
			if !lenient {
				return ErrNoDeduplication
			}
			dst.Dropped++
		} else {
			dst.Props = append(dst.Props, Entry{Key: PropertyDuplicateID, Text: o.Publish.DedupKey})
		}
	}

	// Everything appended so far is this mapping's OWN, and the count below
	// treats it differently from the application's headers.
	reserved := len(dst.Props)

	// The application's own headers.
	seen := headerSet(m.Headers.Len())
	for key, val := range m.Headers.All() {
		if err := headerRefusal(key, val, o, text); err != nil {
			if lenient {
				dst.Dropped++
				continue
			}
			return err
		}
		if _, dup := seen[key]; dup {
			if lenient {
				dst.Dropped++
				continue
			}
			return ErrDuplicateHeader
		}
		seen[key] = struct{}{}
		dst.Props = append(dst.Props, Entry{Key: key, Bin: val, Kind: Binary})
	}
	if lim := o.Limits.MaxHeaderCount; lim.Known && len(dst.Props) > lim.Value {
		if reserved > lim.Value {
			// The limit does not leave room for the mapping's OWN entries,
			// and those are not metadata the caller asked for: they are how
			// the typed properties reach the wire at all on a protocol
			// with no slot for them -- the message key, the publisher's
			// identifier, the causation identifier, the content encoding. A
			// truncation here would silently return a message with no key
			// and no identifier, so this is a refusal in EVERY mode,
			// MetadataLenientDrop included: a broker configured this tightly
			// cannot carry this driver's messages at all, which is a
			// configuration error rather than per-message metadata loss.
			// The fuzz target found this by truncating the STOMP arm's
			// content-encoding entry away and getting the property back
			// empty.
			return ErrHeaderTooLarge
		}
		if !lenient {
			return ErrHeaderTooLarge
		}
		dst.Dropped += len(dst.Props) - lim.Value
		dst.Props = dst.Props[:lim.Value]
	}
	return nil
}

// carryExpiry puts the per-message TTL in the slot the protocol has for it.
//
// AMQP 1.0's header-section `ttl` is a RELATIVE duration, so the caller's own
// option goes straight in. STOMP's `expires` is an epoch-millisecond INSTANT
// and has no relative form, so the driver must have resolved one against the
// injected clock; a publication with a TTL and no resolved expiry is refused
// rather than published without one, because a message that silently never
// expires is a message that is delivered long after the caller said it was
// worthless.
func carryExpiry(dst *Fields, o Options, text bool) error {
	if !text {
		dst.TTL = o.Publish.TTL
		dst.Expiry = o.Expiry
		return nil
	}
	if o.Publish.TTL <= 0 {
		dst.Expiry = o.Expiry
		return nil
	}
	if o.Expiry.IsZero() {
		return ErrNeedsAbsoluteExpiry
	}
	dst.Expiry = o.Expiry
	return nil
}

// carryDelay puts a scheduled publication's delay in the slot the PROTOCOL AND
// BROKER have for it, which is the one place all four combinations differ.
//
// A publication with no delay writes nothing at all: the slot stays [NoDelay],
// and a driver that then sends no key is sending the truth. That matters more
// than it sounds, because an `AMQ_SCHEDULED_DELAY` of 0 is accepted by both
// brokers and turns an ordinary publication into a scheduled one.
func carryDelay(dst *Fields, o Options, text bool) error {
	asked := o.Publish.Delay > 0 || !o.Publish.DeliverAt.IsZero()
	if !asked {
		dst.Slot = NoDelay
		return nil
	}
	switch {
	case text:
		dst.Slot = DelayHeader
	case o.Broker == activemq.Artemis:
		dst.Slot = DelayAnnotation
	default:
		dst.Slot = DelayProperty
	}
	// The absolute form, where the protocol has one, is preferred for a
	// caller who named an absolute time: it needs no clock and it does not
	// drift with the time the publication spends in the driver.
	if _, at := dst.Slot.Keys(); at != "" && !o.Publish.DeliverAt.IsZero() {
		dst.DeliverAt = o.Publish.DeliverAt
		return nil
	}
	d := o.Delay
	if d <= 0 {
		d = o.Publish.Delay
	}
	if d <= 0 {
		// The caller asked for an absolute delivery time over a protocol
		// with no absolute slot, and the driver did not resolve it into a
		// duration. Reading the clock here is forbidden and would make this
		// mapping impure, so the refusal names what is missing.
		return ErrNeedsResolvedDelay
	}
	dst.Delay = roundUpMillis(d)
	return nil
}

// headerRefusal is every reason a header cannot be carried, in one fixed
// order, so that one header set always produces one verdict.
func headerRefusal(key string, val []byte, o Options, text bool) error {
	if key == "" {
		// Neither protocol can name a value with nothing. An AMQP 1.0
		// application-property map keyed on the empty string is not a
		// property either broker will show you, and a STOMP frame header
		// that begins with its own ":" is a malformed frame -- which
		// go-stomp encodes without complaint and the broker answers with an
		// ERROR frame that closes the connection, taking every other
		// publication in flight with it. The fuzz target found this.
		return ErrEmptyHeaderKey
	}
	if len(key) > activemq.MaxNameLen {
		return ErrHeaderTooLarge
	}
	if lim := o.Limits.MaxHeaderBytes; lim.Known && int64(len(key))+int64(len(val)) > lim.Value {
		return ErrHeaderTooLarge
	}
	if ReservedHeader(key) {
		return ErrReservedHeader
	}
	// A broker's own MESSAGE PROPERTY clashes on both arms; a STOMP FRAME
	// header clashes on the STOMP arm and on no other, because it is not a
	// property at all. See [ReservedProperty].
	if ReservedProperty(key) {
		return ErrProtocolHeader
	}
	if !text {
		return nil
	}
	if ReservedFrameHeader(key) {
		return ErrProtocolHeader
	}
	if !utf8.ValidString(key) || !utf8.Valid(val) {
		return ErrTextOnly
	}
	return nil
}

// restore is the receive direction of [carry]: the fixed slots and the table
// entries back into a message.
//
// It writes the message's headers and properties and leaves its Body alone.
// The body is the driver's — it points into the client library's own buffer,
// which the specification makes borrowed until the delivery is settled — and copying it
// here would allocate per delivery for nothing.
func restore(dst *mq.Message, f *Fields, o Options, k Kind) error {
	if !o.Metadata.Valid() {
		return ErrBadMetadataMode
	}
	if dst == nil || f == nil {
		return nil
	}
	text := k.Resolve() == STOMP
	dst.Properties.ContentType = f.ContentType
	dst.Properties.ContentEncoding = f.ContentEncoding
	dst.Properties.CorrelationID = f.CorrelationID
	dst.Properties.ReplyTo = f.ReplyTo
	dst.Properties.CreatedAt = f.Timestamp
	if !text {
		// Over STOMP `message-id` is the BROKER's identifier for the
		// delivery and not the publisher's for the message, so restoring it
		// would replace an application identifier with a broker one. The
		// publisher's own arrives under [IDHeader] instead, below.
		dst.ID = f.MessageID
	}

	for i := range f.Props {
		e := &f.Props[i]
		switch {
		case e.Key == CausationHeader:
			dst.Properties.CausationID = e.text()
			continue
		case e.Key == EncodingHeader:
			dst.Properties.ContentEncoding = e.text()
			continue
		case e.Key == IDHeader:
			dst.ID = e.text()
			continue
		case o.KeyHeader != "" && e.Key == o.KeyHeader:
			dst.Key = e.value()
			continue
		case ReservedHeader(e.Key):
			// These drivers' own entries are not the application's headers.
			// Handing them back would make a round trip add headers the
			// publisher never set, which is what the fidelity scenario
			// checks.
			continue
		case ReservedProperty(e.Key):
			// The broker's own message properties, which both arms receive:
			// Artemis echoes its group identifier and its scheduled-delivery
			// time, and Classic echoes its scheduled job.
			continue
		case text && ReservedFrameHeader(e.Key):
			// STOMP's own frame headers, of which a Classic delivery carries
			// eleven. They are filtered on THIS arm only: `destination` is
			// not a message property, so on the AMQP 1.0 arm a caller's
			// header of that name is the caller's and comes back.
			continue
		}
		dst.Headers.Add(e.Key, e.value())
	}
	return nil
}

// headerSet is the duplicate-detection set, sized for the message's headers. A
// message with no headers allocates nothing.
func headerSet(n int) map[string]struct{} {
	if n == 0 {
		return nil
	}
	return make(map[string]struct{}, n)
}

// hasPrefix is strings.HasPrefix, local so that this package's hot path does
// not depend on the strings package's inlining.
func hasPrefix(s, prefix string) bool {
	return len(s) >= len(prefix) && s[:len(prefix)] == prefix
}

// roundUpMillis rounds a duration UP to whole milliseconds, which is the unit
// both brokers' scheduled-delivery keys are in.
//
// Up rather than to-nearest, so that a publication is never released before
// the caller asked: a delay of 1500 µs that became 1 ms would deliver half a
// millisecond early, and the same rounding applied to a TTL would expire a
// message the caller still considered live.
func roundUpMillis(d time.Duration) time.Duration {
	if d <= 0 {
		return 0
	}
	return ((d + time.Millisecond - 1) / time.Millisecond) * time.Millisecond
}

// decimal renders a whole number without reaching for strconv's formatting of
// a signed value, which this package's hot path does not need.
func decimal(n int64) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}

// destination picks the name the role needs and refuses one the protocol
// cannot address.
//
// `wrong` is the other protocol's Classic decoration, which is the defect this
// check exists for: on Artemis both protocols address bare names, so there is
// nothing to tell apart, and on Classic a swapped flag produces a destination
// that WORKS and addresses the wrong kind of thing.
func destination(n activemq.Names, role Role, wrong func(string) bool) (string, error) {
	var name string
	switch role {
	case Publish:
		name = n.Publish
	case Consume:
		name = n.Consume
	default:
		return "", ErrNoDestination
	}
	if name == "" {
		return "", ErrNoDestination
	}
	if wrong(name) {
		return "", ErrWrongDestinationForm
	}
	return name, nil
}

// native, conditional and unsupported are the supports a protocol's table is
// written in terms of, so that a row reads as a sentence rather than as a
// struct literal.
//
// The specification requires a conditional support to state its condition and an emulation
// to say what the emulation is, and `driverkit.CapabilityBuilder` refuses a
// table that does not — so `conditional` takes the note as a parameter and the
// driver's own construction is what enforces it.
func native(scope mq.Scope) mq.Support {
	return mq.Support{Level: mq.Native, Scope: scope}
}

// nativeNote is a feature the broker provides itself with something a caller
// still needs to know: which key it is spelled with, or which of the two units
// it is in. The specification does not require a note here; this family's rows carry one
// wherever the measurement found something that would otherwise be a surprise.
func nativeNote(scope mq.Scope, note string) mq.Support {
	return mq.Support{Level: mq.Native, Scope: scope, Note: note}
}

// conditional is a feature that needs a broker configuration, a destination
// policy or a naming convention, with the condition stated.
func conditional(scope mq.Scope, note string) mq.Support {
	return mq.Support{Level: mq.Conditional, Scope: scope, Note: note}
}

// unsupported is the honest zero: the protocol does not have the feature.
func unsupported() mq.Support { return mq.Support{Level: mq.Unsupported} }

// unsupportedBecause is an absent feature whose absence is a MEASUREMENT
// rather than an omission, with what was measured.
//
// FeatureNack is why it exists. "STOMP cannot nack" is not a documentation
// gap: a STOMP NACK frame was measured to ACKNOWLEDGE the message on Artemis
// and to be a poison-ack on Classic, so a driver that mapped `Nack` to it
// would destroy messages while reporting a requeue. A row like that has to
// carry its evidence.
func unsupportedBecause(note string) mq.Support {
	return mq.Support{Level: mq.Unsupported, Note: note}
}

// notConsidered is the note a feature nobody answered carries.
//
// An unanswered feature reads as Unsupported, which is the safe answer and not
// an informative one, and the evidence rule is about what a driver CLAIMS
// rather than about what it forgot. So each arm's switch ends in a default that
// says "nobody looked at this", and `TestEveryFeatureIsConsidered` fails on it
// — which is what makes adding a feature to the inventory a failure here
// rather than a silent absence in four capability tables.
const notConsidered = "not considered by this protocol's table"

// absentFromAllFour is the part of the inventory that NEITHER protocol has
// on EITHER broker, with the reason for each. They are here rather than as
// four identical per-row notes because a note repeated four times drifts three
// ways.
//
//   - FeatureExactlyOnceDelivery: neither broker de-duplicates a REDELIVERY.
//     Artemis's `_AMQ_DUPL_ID` de-duplicates a publication, which is
//     FeatureDeduplication and is a different promise.
//   - FeatureReplay: a queue has no history. A delivered and acknowledged
//     message is gone, so there is nothing to re-read.
//   - FeatureSeekByTime, FeatureSeekBySequence: a queue has no position
//     either, so there is nothing to seek to. Browsing a queue is not a
//     subscription position.
//   - FeatureExtendLease: neither protocol has a frame that extends an
//     acknowledgement deadline. A delivery's lease is the consumer's link or
//     subscription, so there is nothing to extend and nothing to expire.
//   - FeatureTransactions: AMQP 1.0's transactional dispositions need a
//     transaction-coordinator link that `go-amqp` does not expose. Over STOMP
//     go-stomp's BEGIN/COMMIT/ABORT do reach the broker — a transacted SEND
//     that was aborted was correctly discarded on Artemis — but a transacted
//     ACK is acknowledged ANYWAY on Artemis (`getMessagesAcknowledged` went to
//     1 and the queue count to 0 after an ABORT), and Classic refuses to
//     transact a negative acknowledgement at all ("Poison ack cannot be
//     transacted", which arrives as an ERROR frame and closes the connection).
//     So the atomicity the specification names is not there on either arm.
//   - FeatureAtomicBatchPublish: neither protocol has a batch frame, so a
//     batch is N publications and nothing makes them one outcome.
//   - FeatureBatchSettle: `go-amqp` settles one delivery at a time and exposes
//     no ranged disposition; a STOMP ACK settles one message identifier,
//     because these drivers subscribe with `client-individual`.
//   - FeatureNativeTracing: neither broker propagates a trace context.
//   - FeatureWildcardSubscribe: a topic maps to its own address or virtual
//     topic, so a pattern over topics would have to span addresses.
//     `activemq.HasWildcard` refuses one before any I/O, which is what makes
//     this negative claim provable rather than merely stated.
var absentFromAllFour = [...]mq.Feature{
	mq.FeatureExactlyOnceDelivery,
	mq.FeatureReplay,
	mq.FeatureSeekByTime,
	mq.FeatureSeekBySequence,
	mq.FeatureExtendLease,
	mq.FeatureTransactions,
	mq.FeatureAtomicBatchPublish,
	mq.FeatureBatchSettle,
	mq.FeatureNativeTracing,
	mq.FeatureWildcardSubscribe,
}

// absentEverywhere reports whether f is one of [absentFromAllFour].
func absentEverywhere(f mq.Feature) bool {
	for _, a := range absentFromAllFour {
		if a == f {
			return true
		}
	}
	return false
}

// countNative counts the features a protocol supports at Native level on b,
// which is "feature coverage score" reduced to one number.
//
// It counts NATIVE only and not "available at some level", which is the
// distinction that makes the number mean something: a conditional support is a
// feature a deployment may or may not have, and an emulation is iqmq's work
// rather than the broker's, so counting either would let a protocol improve
// its score by adding a caveat.
func countNative(p Protocol, b activemq.Broker) int {
	n := 0
	for f := range mq.FeatureCount {
		if p.Support(b, mq.Feature(f)).Level == mq.Native {
			n++
		}
	}
	return n
}

// SupportTable is a protocol's whole answer for one broker, in inventory
// order.
//
// It is what a driver's capability table is built from and what the four
// combinations are compared on: the driver narrows a row for what its own code
// does not implement and never widens one.
func SupportTable(p Protocol, b activemq.Broker) []mq.Support {
	out := make([]mq.Support, mq.FeatureCount)
	for f := range mq.FeatureCount {
		out[f] = p.Support(b, mq.Feature(f))
	}
	return out
}
