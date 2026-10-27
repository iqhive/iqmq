package amqp10

import (
	"context"
	"strconv"
	"strings"
	"time"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit"
	"github.com/iqhive/iqmq/internal/errcode"
)

// Profile is what a broker-specific module supplies to this core, and it is the
// seam this module exists for.
//
// AMQP 1.0 is a framing and settlement protocol with almost no topology in it.
// Two conforming brokers agree about transfers, dispositions, credit and the
// message format, and agree about nothing else that a message queue needs: what
// an address means, how a shared durable subscription is named, how a
// publication is delayed, where a redelivery delay goes, which field counts
// attempts, what is dead-lettered and how, and which versions work at all. This
// core owns the first list and refuses to guess at the second. A Profile is the
// second list, written once per broker.
//
// # Why a struct of functions rather than an interface
//
// Most of what distinguishes two AMQP 1.0 brokers is DATA — a name, a flag, an
// annotation key — and only three or four things are decisions that need code.
// An interface would make every profile implement a dozen methods in order to
// answer "this broker cannot do that" a dozen times, and the shapes that differ
// between Artemis and ActiveMQ Classic are three fields rather than three
// methods. A struct also lets the core VALIDATE a profile before it dials
// ([Profile.Validate]), which is the specification's "validate config before starting
// background work" applied to the seam itself; an interface can only be
// validated by calling it. The precedent in this repository is
// [github.com/iqhive/iqmq.Hooks], which is a struct of function fields for the
// same reason stated in its own documentation: so that a caller can set one
// without implementing five.
//
// The price is that a new obligation added here is not a compile error for an
// existing profile. That is why [Profile.Validate] refuses a profile that is
// missing anything mandatory, why every optional field's zero value means
// "refused" rather than "guess", and why [Capabilities] is checked against the
// core's own ceiling rather than trusted.
//
// # Writing one
//
// Everything a profile must supply is in the first block below and is refused
// when absent. Everything after it is optional and its zero value is the honest
// "this broker has no way to express that", which the core turns into
// [github.com/iqhive/iqmq.ErrUnsupported] before any I/O.
//
// A minimal profile for a broker whose addresses are plain names, which is
// enough to publish and consume:
//
//	amqp10.Profile{
//		Driver:        "activemq",
//		Name:          "artemis",
//		SinkAddress:   func(topic string) (string, error) { return topic, nil },
//		SourceAddress: func(topic, sub string) (string, error) { return topic + "::" + sub, nil },
//	}
//
// The two addressing functions are the fields a reader should look at first,
// because they are where two brokers that speak the same protocol stop agreeing.
// Apache Artemis addresses a shared durable subscription by the fully qualified
// queue name `address::queue`, so its SourceAddress joins the topic and the
// subscription with "::". RabbitMQ 4's AMQP 1.0 endpoint addresses a queue as
// `/queues/<name>` and an exchange as `/exchanges/<name>/<key>`, so its
// SinkAddress builds an exchange path and its SourceAddress a queue path, and
// neither looks anything like Artemis's. ActiveMQ Classic has virtual topics, so
// its SourceAddress builds `Consumer.<sub>.VirtualTopic.<topic>`. There is no
// intersection of those three that a core could implement, and a core that
// picked one would be wrong on the other two — which is why this is the
// profile's decision and not the core's.
//
// # What the core does with it
//
// A profile is read once, at [Open], and never again: the addresses are computed
// when a sink or a source is opened, the capability table is built at open and
// is constant for the life of the backend, and the error rules are folded
// into the core's table once. Nothing here is on a message's path except the
// annotation keys, which are strings the mapping writes.
type Profile struct {
	// Driver is the driver name reported in
	// [github.com/iqhive/iqmq.DriverInfo.Name] and carried by every
	// *OpError this backend produces. It is the name a caller sees in an
	// error and in the capability matrix, so it is the broker-specific
	// module's name ("rabbitmq", "activemq") and never this core's.
	//
	// Required.
	Driver string

	// Name is the broker variant reported in
	// [github.com/iqhive/iqmq.DriverInfo.Profile]: "artemis", "classic",
	// "amqp10". It is empty only for a driver with one profile.
	Name string

	// Version is the broker-specific module's own version, reported in
	// [github.com/iqhive/iqmq.DriverInfo.DriverVersion]. The specification makes an empty
	// string "the driver did not say" rather than a claim, so an untagged
	// module leaves it empty.
	Version string

	// SinkAddress maps a portable topic to the address a sending link
	// attaches its target to.
	//
	// Required. It must be pure and must refuse a topic it cannot map, with
	// an error the core returns unchanged — so a profile that wants
	// [github.com/iqhive/iqmq.ErrInvalidTopic] to be reachable through
	// errors.Is wraps it.
	SinkAddress func(topic string) (string, error)

	// SourceAddress maps a portable topic and subscription to the address a
	// receiving link attaches its source to.
	//
	// Required, and the single most broker-specific decision in the module.
	// the rule is what it has to deliver: two consumers of ONE
	// subscription compete for a message, and two consumers of DIFFERENT
	// subscriptions each receive it. On Artemis that is the fully qualified
	// queue name of a shared durable subscription on a multicast address
	// (`address::queue`); on RabbitMQ 4 it is a queue path; on Classic it is
	// a virtual-topic consumer queue. A profile whose SourceAddress ignored
	// the subscription would make every consumer compete, which is a
	// fan-out claim silently turned into a queue.
	SourceAddress func(topic, subscription string) (string, error)

	// DeadLetterAddress is where a rejected delivery is routed when the
	// broker does not route `rejected` itself.
	//
	// Nil means the broker's own dead-letter address handles `rejected`,
	// which is the usual case: both Artemis and RabbitMQ have a
	// dead-letter-address policy per queue and `rejected` reaches it. A
	// profile supplies this only when a rejection has to be re-published,
	// and the core then publishes to the address and settles the original
	// only once the publication is durable — the specification's "a failed or ambiguous
	// dead-letter publish never acknowledges the original".
	DeadLetterAddress func(topic, subscription string) (string, error)

	// Capabilities are the claims this profile adds to the core's own table.
	//
	// The core builds a base table from what AMQP 1.0 and this
	// implementation really do, and then applies these, which is how a
	// profile both NARROWS the base (a broker with no dead-letter policy
	// withdraws FeatureDeadLetter) and WIDENS it (a broker with a delivery
	// delay claims FeatureScheduledPublish natively). What it cannot do is
	// claim something the core has no code for: every feature has a ceiling
	// the core enforces ([CoreCeiling]), and a claim above it is refused at
	// [Open] with CodeInvalidConfig rather than published in the capability
	// matrix. The specification makes the table the thing a caller acts on, so a table
	// that can lie is worse than no table.
	//
	// Nil means the base table stands.
	Capabilities func(Facts) []Claim

	// Limits are the numeric limits this broker advertises, merged over the
	// ones the core reads off the connection and the sender link. The specification
	// forbids reading an unadvertised limit as zero, so a profile leaves
	// Known false for anything it does not know.
	//
	// Nil means the core's own limits stand.
	Limits func(Facts, mq.Limits) mq.Limits

	// Delay says how a delayed publication is expressed
	// ([github.com/iqhive/iqmq.PublishOptions.Delay] and DeliverAt).
	//
	// The zero value refuses it, which is what a broker with no scheduling
	// needs: the core answers ErrUnsupported before any I/O. Artemis reads
	// the message annotation `x-opt-delivery-delay` as a delay in
	// milliseconds; ActiveMQ Classic reads `AMQ_SCHEDULED_DELAY`; RabbitMQ 4
	// has no AMQP 1.0 spelling at all and refuses.
	Delay DelayRule

	// NackDelay says how a delayed negative settlement is expressed
	// ([github.com/iqhive/iqmq.NackOptions.Delay]).
	//
	// The zero value refuses it. AMQP 1.0's own answer is the `modified`
	// outcome — `delivery-failed` counts the attempt and
	// `undeliverable-here` keeps the message off this link — and a broker
	// that can carry a delay does it with an annotation merged into the
	// modified outcome. A profile that sets Supported without an annotation
	// key gets `modified` with no delay, which is a redelivery that is not
	// held back; that is a different behaviour from the one the caller asked
	// for, so the core refuses a non-zero delay unless the rule can carry
	// it.
	NackDelay NackDelayRule

	// Attempt says where the delivery attempt count comes from.
	//
	// The zero value reads the AMQP header's `delivery-count`, which every
	// conforming broker maintains and which is the right answer for both
	// Artemis and RabbitMQ 4. A broker that keeps the count in an annotation
	// instead names it here. Zero attempts mean "the broker does not count",
	// which the specification makes distinct from "this is the first delivery".
	Attempt AttemptRule

	// Dedup says how [github.com/iqhive/iqmq.PublishOptions.DedupKey]
	// reaches the broker.
	//
	// The zero value refuses it. Artemis de-duplicates on the
	// `_AMQ_DUPL_ID` application property; RabbitMQ 4 has no de-duplication
	// and refuses.
	Dedup SlotRule

	// Durable says what [github.com/iqhive/iqmq.ConfirmDurable] means here.
	//
	// The zero value refuses it, and a publisher that asks for
	// ConfirmDurable then fails when the sink is opened, which is the specification's
	// rule: "ConfirmDurable fails at construction when the profile cannot
	// define a meaningful durable boundary". A profile that can define one
	// says what it is in the Note, which the capability table publishes and
	// the driver page quotes.
	Durable DurableRule

	// AnnotationPrefix is the header-key prefix that maps to and from AMQP
	// MESSAGE ANNOTATIONS rather than application properties.
	//
	// It is the profile's because the two brokers spell it differently:
	// Artemis's own annotations are `x-opt-*` and RabbitMQ's are `x-*`. A
	// header a caller sets under this prefix becomes an annotation, which is
	// what makes a broker-specific control reachable from the portable API
	// without the core knowing what it means; every other header is an
	// application property. Empty means every header is an application
	// property.
	AnnotationPrefix string

	// AutoCreates reports that the broker creates the address or queue an
	// attach names when it does not exist.
	//
	// It changes what the specification's "no hidden creation" means for this driver: a
	// broker that auto-creates will accept an attach to a topic nobody
	// provisioned, so the driver cannot refuse it and the capability page
	// says so. A broker that does not auto-create refuses the attach itself,
	// and the core's error table turns that into CodeNotFound — which is the
	// answer the specification wants and the one the conformance kit's
	// `topology/no-hidden-creation` scenario looks for.
	AutoCreates bool

	// CheckBroker refuses a broker this profile does not support, from the
	// connection properties the peer sent in its OPEN frame.
	//
	// Nil accepts every peer. It is a profile rule and not a core one
	// because the core has no opinion: RabbitMQ's AMQP 1.0 endpoint exists
	// only from 4.0 and its 3.13 plugin is a different protocol, so
	// "refuse below 4.0" belongs to the RabbitMQ profile; Artemis has
	// spoken AMQP 1.0 for a decade and refuses nothing.
	CheckBroker func(Facts) error

	// ErrorRules are the rules this profile adds to the core table,
	// keyed by the Op* constants of the root package.
	//
	// They are tried BEFORE the core's own for the same operation, because a
	// profile's rules are the specific ones: a broker that answers
	// `amqp:internal-error` with a description only it can interpret is the
	// case this exists for. Nil adds nothing.
	ErrorRules func() map[string][]driverkit.Rule

	// Admin is the topology surface, or nil.
	//
	// AMQP 1.0 has no portable topology API — the protocol can attach to an
	// address and cannot create one — so a backend whose profile supplies
	// nothing here answers [github.com/iqhive/iqmq.Client.Admin] with
	// ErrUnsupported, which is the answer for a backend that cannot provide
	// one. A profile that has a management endpoint (RabbitMQ's HTTP API,
	// Artemis's management address, Azure Service Bus's ARM surface) supplies
	// it, and FeatureAdmin then becomes claimable; a profile that claims
	// FeatureAdmin with no Admin is refused at [Open].
	Admin func(ctx context.Context, b *Backend) (mq.Admin, error)
}

// Claim is one line of a profile's capability table.
type Claim struct {
	// Feature is the feature the claim is about.
	Feature mq.Feature
	// Support is what the profile claims about it.
	Support mq.Support
}

// Native is the claim for a feature the broker provides itself.
func Native(f mq.Feature, scope mq.Scope) Claim {
	return Claim{Feature: f, Support: mq.Support{Level: mq.Native, Scope: scope}}
}

// Conditional is the claim for a feature that needs a profile setting or an
// option, with the condition the specification requires it to state.
func Conditional(f mq.Feature, scope mq.Scope, note string) Claim {
	return Claim{Feature: f, Support: mq.Support{Level: mq.Conditional, Scope: scope, Note: note}}
}

// Withdraw is the claim for a feature the broker does not have, which is how a
// profile narrows the core's base table.
func Withdraw(f mq.Feature) Claim {
	return Claim{Feature: f, Support: mq.Support{Level: mq.Unsupported}}
}

// Slot names the part of an AMQP 1.0 message a broker-specific value is written
// to.
//
// The three that carry one are the three a broker reads: message annotations
// are propagated by intermediaries and are where a broker's own controls live,
// delivery annotations are hop-by-hop, and application properties are the bare
// message's own typed map. The zero value names none, which is what makes an
// unset rule a refusal rather than a value written nowhere.
type Slot uint8

// The slots.
const (
	// SlotNone names no part of the message: the control is refused.
	SlotNone Slot = iota
	// SlotMessageAnnotation is the message-annotations section.
	SlotMessageAnnotation
	// SlotDeliveryAnnotation is the delivery-annotations section.
	SlotDeliveryAnnotation
	// SlotApplicationProperty is the application-properties section.
	SlotApplicationProperty

	slotCount
)

var slotNames = [slotCount]string{"none", "messageannotation", "deliveryannotation", "applicationproperty"}

// String returns the slot's lower-case name.
func (s Slot) String() string {
	if s < slotCount {
		return slotNames[s]
	}
	return "Slot(?)"
}

// Valid reports whether s names a slot.
func (s Slot) Valid() bool { return s < slotCount }

// Supported reports whether the slot names somewhere a value can go.
func (s Slot) Supported() bool { return s != SlotNone && s.Valid() }

// phrase is the slot's name in prose, for a capability note a person reads.
// [Slot.String] is the machine-readable slug and this is the readable one; a
// note that said "the messageannotation x-opt-delivery-delay" would be a
// capability table written for a parser.
func (s Slot) phrase() string {
	switch s {
	case SlotMessageAnnotation:
		return "message annotation"
	case SlotDeliveryAnnotation:
		return "delivery annotation"
	case SlotApplicationProperty:
		return "application property"
	case SlotNone, slotCount:
	}
	return "nothing"
}

// SlotRule is a broker-specific value with one key and one place to put it.
//
// The zero value refuses the control it stands for, which is the whole reason
// the rules are structs rather than strings: an empty key and a zero slot are
// one answer — "this broker has no way to express it" — rather than a value
// written under the empty name.
type SlotRule struct {
	// Slot is where the value goes. SlotNone refuses the control.
	Slot Slot
	// Key is the annotation or property name.
	Key string
}

// Supported reports whether the rule can carry a value.
func (r SlotRule) Supported() bool { return r.Slot.Supported() && r.Key != "" }

// DelayRule says how a delayed publication is expressed.
type DelayRule struct {
	// Slot is where the delay goes. SlotNone refuses every delayed
	// publication with ErrUnsupported, before any I/O.
	Slot Slot
	// Key is the annotation or property name: "x-opt-delivery-delay" on
	// Artemis, "AMQ_SCHEDULED_DELAY" on ActiveMQ Classic.
	Key string
	// Absolute asks for the delay as an absolute delivery TIME rather than a
	// relative duration, for a broker that schedules by instant
	// ("x-opt-delivery-time", "AMQ_SCHEDULED_TIME").
	//
	// Either kind of rule takes both PublishOptions.Delay and DeliverAt. The
	// one the rule does not read is restated against [Config.Clock] before it
	// is written: a relative rule gets the time left until DeliverAt, and an
	// absolute rule gets the instant Delay ends.
	Absolute bool
	// Value encodes the delay. Nil means whole milliseconds as an int64,
	// which is what both Artemis annotations take; a broker that wants
	// seconds, or a string, supplies its own. The argument is the relative
	// delay, or the absolute time as a Unix millisecond count when Absolute
	// is set. On a relative rule it is not called for a DeliverAt that has
	// already passed: that publication is due now and carries no delay.
	Value func(ms int64) any
	// Max is the longest delay the broker accepts, which the core refuses
	// beyond and publishes as Limits.MaxDelay. Zero means the profile does
	// not know, and nothing is enforced from it.
	Max time.Duration
}

// Supported reports whether the rule can express a delay.
func (r DelayRule) Supported() bool { return r.Slot.Supported() && r.Key != "" }

// encode renders a publication's hold as the value that goes under Key, and
// reports false when there is no hold to write.
//
// Delay and DeliverAt are two spellings of one hold and a rule reads only one
// of them, so the other is restated against the clock first. Reading only the
// rule's own field published a DeliverAt on a relative rule as a delay of zero,
// which the broker delivered at once, and a Delay on an absolute rule as the
// zero time, year one; both publications reported success.
//
// A DeliverAt that has passed, or is less than a millisecond away, is due now,
// so a relative rule writes nothing for it rather than a delay of zero: an
// explicit zero is a scheduled publication on some brokers, not an ordinary
// one. An absolute rule writes the caller's instant as given, past or not,
// because a broker that schedules by instant delivers a past one at once and
// needs no clock read to say so.
func (r DelayRule) encode(delay time.Duration, at time.Time, clock mq.Clock) (any, bool) {
	var ms int64
	switch {
	case delay <= 0 && at.IsZero():
		return nil, false
	case r.Absolute && at.IsZero():
		ms = clock.Now().Add(delay).UnixMilli()
	case r.Absolute:
		ms = at.UnixMilli()
	case at.IsZero():
		ms = delay.Milliseconds()
	default:
		ms = at.Sub(clock.Now()).Milliseconds()
		if ms <= 0 {
			return nil, false
		}
	}
	if r.Value != nil {
		return r.Value(ms), true
	}
	return ms, true
}

// NackDelayRule says how a delayed negative settlement is expressed.
//
// AMQP 1.0's `modified` outcome is the protocol's own answer to "I did not
// process this; do something else with it", and the two flags are what a broker
// acts on. The delay itself has no standard spelling, so a profile that can hold
// a message back names the annotation the broker reads.
type NackDelayRule struct {
	// Supported reports that the broker honours a delayed redelivery at all.
	// False refuses a non-zero NackOptions.Delay with ErrUnsupported and
	// settles nothing, which is the rule.
	Supported bool
	// Slot and Key are where the delay goes, in the annotation map merged
	// into the modified outcome. An unsupported slot with Supported set
	// means the broker redelivers immediately, so the core refuses a
	// non-zero delay rather than dropping it.
	Slot Slot
	// Key is the annotation name.
	Key string
	// Value encodes the delay in whole milliseconds. Nil means the count as
	// an int64.
	Value func(ms int64) any
	// DeliveryFailed asks the node to count the delayed redelivery as a
	// failed attempt, which is what makes the attempt count rise
	// across a nack. It is true for both brokers this core was written for.
	DeliveryFailed bool
	// UndeliverableHere asks the node not to redeliver on this link. It is
	// what a broker with no delay support needs in order not to hand the
	// same message straight back, and it is what the core uses for a
	// delivery it could not decode.
	UndeliverableHere bool
	// Max is the longest redelivery delay the broker accepts. Zero means
	// unknown.
	Max time.Duration
}

// carries reports whether the rule can express a non-zero delay.
func (r NackDelayRule) carries() bool { return r.Supported && r.Slot.Supported() && r.Key != "" }

// encode renders a redelivery delay as the value that goes under Key.
func (r NackDelayRule) encode(d time.Duration) any {
	ms := d.Milliseconds()
	if r.Value != nil {
		return r.Value(ms)
	}
	return ms
}

// AttemptSource is where the delivery attempt count comes from.
type AttemptSource uint8

// The attempt sources.
const (
	// AttemptFromHeader reads the AMQP message header's `delivery-count`,
	// which is the protocol's own counter and the zero value. The header
	// counts REDELIVERIES — it is zero on the first transfer — so the core
	// reports delivery-count plus one as Attempt, which is 1 on the
	// first delivery.
	AttemptFromHeader AttemptSource = iota
	// AttemptFromAnnotation reads a broker annotation instead, for a broker
	// that keeps its own count.
	AttemptFromAnnotation
	// AttemptNone reports no attempt count. The specification makes zero "the broker does
	// not count attempts", which is honest and is not the same as "this is
	// the first delivery".
	AttemptNone

	attemptSourceCount
)

var attemptSourceNames = [attemptSourceCount]string{"header", "annotation", "none"}

// String returns the source's lower-case name.
func (a AttemptSource) String() string {
	if a < attemptSourceCount {
		return attemptSourceNames[a]
	}
	return "AttemptSource(?)"
}

// Valid reports whether a names a source.
func (a AttemptSource) Valid() bool { return a < attemptSourceCount }

// AttemptRule says where the attempt count comes from.
type AttemptRule struct {
	// Source is which counter to read. The zero value is the AMQP header's.
	Source AttemptSource
	// Slot and Key name the annotation, for AttemptFromAnnotation.
	Slot Slot
	// Key is the annotation name.
	Key string
	// FirstIsOne reports that the broker's own counter is already 1 on the
	// first delivery, so the core does not add one. It is meaningful only
	// for AttemptFromAnnotation: the AMQP header's delivery-count is
	// defined to be zero on the first transfer.
	FirstIsOne bool
}

// DurableRule says what ConfirmDurable means on this broker.
type DurableRule struct {
	// Supported reports that the profile can define a durable boundary. When
	// it is false a publisher asking for
	// [github.com/iqhive/iqmq.ConfirmDurable] fails when the sink is opened
	Supported bool
	// Note states the boundary, which the specification requires every driver page to do
	// and which the capability table publishes. It is refused when empty and
	// Supported is set: a durable claim whose boundary nobody wrote down is
	// the claim exists to prevent.
	Note string
}

// Facts is what the connected peer and the attached links really said.
//
// Every conditional capability claim and every version gate is read off this
// and off nothing else, which is the rule for this module: a claim read off
// a constant would be wrong on the broker versions the matrix runs against.
// Every field is empty or zero when the peer did not say, which the specification makes "the
// peer did not say" rather than a claim.
type Facts struct {
	// Product is the peer's "product" connection property, which is how
	// every AMQP 1.0 broker names itself: "rabbitmq", "apache-activemq-artemis".
	Product string
	// Version is the peer's "version" connection property.
	Version string
	// Platform is the peer's "platform" connection property, when it sent
	// one.
	Platform string
	// Properties is the whole connection-properties map the peer sent. A
	// profile that needs a property this struct does not name reads it here.
	Properties map[string]any
	// MaxMessageBytes is the largest message the peer will accept, read off
	// the sender link's declared maximum at the first attach. Zero means the
	// peer declared none.
	MaxMessageBytes uint64
	// Address is the address the probe link attached to, which is what a
	// profile's own naming produced.
	Address string
}

// String renders the facts for a log line or a test failure, so that a
// capability answer can be read against what the peer said.
func (f Facts) String() string {
	var b strings.Builder
	b.WriteString("product=")
	b.WriteString(quoteFact(f.Product))
	b.WriteString(" version=")
	b.WriteString(quoteFact(f.Version))
	b.WriteString(" platform=")
	b.WriteString(quoteFact(f.Platform))
	b.WriteString(" maxmessagebytes=")
	b.WriteString(strconv.FormatUint(f.MaxMessageBytes, 10))
	b.WriteString(" properties=")
	b.WriteString(strconv.Itoa(len(f.Properties)))
	return b.String()
}

// quoteFact renders a peer-supplied string so that the result is printable
// whatever the peer sent. A broker's own properties arrive off the wire, so a
// rendering that interpolated them raw could produce a log line that is not
// valid UTF-8 — the failure the specification recorded for DSN parameter names, in the one
// other place this module interpolates bytes a stranger chose.
func quoteFact(s string) string {
	if s == "" {
		return `""`
	}
	for i := range len(s) {
		if c := s[i]; c <= ' ' || c >= 0x7f {
			return strconv.Quote(s)
		}
	}
	return s
}

// Validate refuses a profile that cannot be used, without any I/O.
//
// It is called before the dial, which is the specification's "validate config before starting
// background work" applied to the seam: a profile with no SourceAddress is a
// programming error in the broker-specific module, and finding it after a
// five-second dial and one attach is finding it in the wrong place.
func (p Profile) Validate() error {
	switch {
	case p.Driver == "":
		return errProfileNoDriver
	case p.SinkAddress == nil:
		return errProfileNoSinkAddress
	case p.SourceAddress == nil:
		return errProfileNoSourceAddress
	case !p.Delay.Slot.Valid() || !p.NackDelay.Slot.Valid() || !p.Dedup.Slot.Valid() || !p.Attempt.Slot.Valid():
		return errProfileSlot
	case !p.Attempt.Source.Valid():
		return errProfileAttemptSource
	case p.Attempt.Source == AttemptFromAnnotation && (!p.Attempt.Slot.Supported() || p.Attempt.Key == ""):
		return errProfileAttemptKey
	case p.Delay.Slot.Supported() && p.Delay.Key == "":
		return errProfileDelayKey
	case p.Dedup.Slot.Supported() && p.Dedup.Key == "":
		return errProfileDedupKey
	case p.NackDelay.Slot.Supported() && p.NackDelay.Key == "":
		return errProfileNackDelayKey
	case p.Durable.Supported && p.Durable.Note == "":
		// The specification requires every driver page to state the exact durable
		// boundary, and the specification requires a claim to state its condition. A
		// profile that claims a durable boundary and does not say what it is
		// publishes a promise nobody can check.
		return errProfileDurableNote
	case p.Delay.Max < 0 || p.NackDelay.Max < 0:
		return errProfileNegativeMax
	}
	return nil
}

// profileName is the DriverInfo.Profile this profile reports.
func (p Profile) profileName() string { return p.Name }

// annotationHeader reports whether a portable header key names a message
// annotation on this broker rather than an application property.
func (p Profile) annotationHeader(key string) bool {
	return p.AnnotationPrefix != "" && strings.HasPrefix(key, p.AnnotationPrefix)
}

// The profile's refusals. The specification names no sentinel for a bad configuration and
// errors.Is matches by code, so a caller compares mq.CodeOf(err) against
// mq.CodeInvalidConfig.
var (
	errProfileNoDriver = errcode.New(errcode.InvalidConfig,
		"iqmq/amqp10: the profile names no driver; Profile.Driver is the name every error and every capability row carries")
	errProfileNoSinkAddress = errcode.New(errcode.InvalidConfig,
		"iqmq/amqp10: the profile has no SinkAddress; AMQP 1.0 has no portable topic-to-address mapping, so the profile has to supply one")
	errProfileNoSourceAddress = errcode.New(errcode.InvalidConfig,
		"iqmq/amqp10: the profile has no SourceAddress; a shared durable subscription is named differently on every broker (Artemis address::queue, RabbitMQ /queues/<name>), so the profile has to supply one")
	errProfileSlot = errcode.New(errcode.InvalidConfig,
		"iqmq/amqp10: a profile rule names a message part that is not one of the three AMQP 1.0 sections")
	errProfileAttemptSource = errcode.New(errcode.InvalidConfig,
		"iqmq/amqp10: Profile.Attempt.Source is not one of header, annotation or none")
	errProfileAttemptKey = errcode.New(errcode.InvalidConfig,
		"iqmq/amqp10: Profile.Attempt reads an annotation and names none")
	errProfileDelayKey = errcode.New(errcode.InvalidConfig,
		"iqmq/amqp10: Profile.Delay names a message part and no key")
	errProfileDedupKey = errcode.New(errcode.InvalidConfig,
		"iqmq/amqp10: Profile.Dedup names a message part and no key")
	errProfileNackDelayKey = errcode.New(errcode.InvalidConfig,
		"iqmq/amqp10: Profile.NackDelay names a message part and no key")
	errProfileDurableNote = errcode.New(errcode.InvalidConfig,
		"iqmq/amqp10: Profile.Durable claims a durable confirmation boundary and does not state it; the message contract requires every driver to state the exact boundary")
	errProfileNegativeMax = errcode.New(errcode.InvalidConfig,
		"iqmq/amqp10: a profile rule's Max cannot be negative")
)
