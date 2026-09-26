// Package protocol holds the two RabbitMQ protocols as two implementations of one interface.
//
// It is in the root module rather than in either driver for the reason
// families, and the specification record — measures a family
// by running one `go test` in one package, and a root-module `go test` cannot
// reach a nested driver module  — and for a second reason
// that is this family's own: the two arms ARE the two drivers, so a package
// that belonged to either of them could not hold both.
//
// # What this family decides, and what it does not
//
// the criterion is "supported-version compatibility, feature subset, perf"
// and its expected outcome is "0-9-1 default; 1.0 conditional profile". Two of
// those three terms are not benchmark metrics at all:
//
//   - **Supported-version compatibility** is [Protocol.MinBroker]. RabbitMQ
//     4.0 is the first release that speaks AMQP 1.0 natively; 3.13 needs a
//     plugin and behaves differently enough that forbids substituting one
//     protocol for the other silently. So the 1.0 profile REFUSES a broker
//     below 4.0 and the 0-9-1 driver works on every version in the matrix,
//     which settles the default before any measurement.
//   - **Feature subset** is [Protocol.Support] over the whole inventory,
//     and [Protocol.Features] is that table's own summary as a number. It is
//     reported as a benchmark metric so that the scorecard carries it, but it
//     is computed from the table rather than measured, and the table is what
//     the conformance suites prove in both directions.
//
// The third term, perf, is what the benchmark measures, and it measures the
// part of it that is honestly measurable without a broker: the cost of the
// PROPERTY MAPPING each protocol's message model demands. That is a real
// difference and it is on the hot path of every publication and every
// delivery — AMQP 0-9-1 has fourteen fixed basic properties and one field
// table, AMQP 1.0 has a header section, a properties section, application
// properties and message annotations — and it is what both drivers really run.
// What it is NOT is the two protocols' throughput against a broker, which
// needs a broker and two client libraries; records that
// as the criterion's gap and `docs/drivers/rabbitmq-amqp10.md` says so where a
// reader will look.
//
// # Why the mapping lives here rather than in each driver
//
// Both drivers must map one `mq.Message` onto their protocol's property model,
// and a caller who moves between them must find the same message on the other
// side: the same content type, the same identifier, the same headers, the same
// key. Two independent mappings would drift, and the half that drifted would
// be the one with fewer users. So the decision — which iqmq field goes into
// which protocol slot, and what happens to what does not fit — is here, in
// [Protocol.Carry] and [Protocol.Restore], and each driver's own mapping code
// is the mechanical copy from [Fields] into its client library's struct.
//
// [Fields] is deliberately a reusable buffer: `Carry` takes a `*Fields` the
// caller owns and truncates its slices rather than allocating, so putting the
// shared mapping in the middle of the publish path costs no allocation per
// message. `TestAllocs` is what holds it to that.
package protocol

import (
	"time"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/internal/errcode"
	"github.com/iqhive/iqmq/internal/rabbit"
)

// Kind identifies a protocol variant. Auto resolves to AutoKind.
type Kind uint8

// The kinds. AMQP091 is the reference implementation and the
// default, because it is the only one every supported broker speaks.
const (
	// Auto selects AutoKind, the bake-off winner recorded in auto.go.
	Auto Kind = iota
	// AMQP091 is AMQP 0-9-1, which every RabbitMQ speaks.
	AMQP091
	// AMQP10 is the AMQP 1.0 profile, which needs RabbitMQ 4.0 or newer.
	AMQP10

	kindCount
)

var kindSlugs = [kindCount]string{"auto", "amqp091", "amqp10"}

var kindIdents = [kindCount]string{"Auto", "AMQP091", "AMQP10"}

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
	for k := AMQP091; k < kindCount; k++ {
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

// Role is what an address is for.
type Role uint8

// The two roles an address is derived for.
const (
	// Publish is a destination a message is sent to.
	Publish Role = iota
	// Consume is a source a consumer attaches to.
	Consume
)

// Address is a destination in the form its protocol expresses it.
//
// The two protocols do not name a destination the same way and cannot be made
// to. AMQP 0-9-1 addresses an exchange and a routing key as two separate
// fields of `basic.publish`, and a consumer names a queue in `basic.consume`.
// AMQP 1.0 has one address string per link, and RabbitMQ 4's own address
// format is `/exchanges/<exchange>/<routing-key>` for a publication and
// `/queues/<queue>` for a consumer. So an address carries both shapes and each
// driver reads the fields its protocol has, which is what keeps the derivation
// in one place without pretending the two protocols are the same.
type Address struct {
	// Exchange and RoutingKey are AMQP 0-9-1's two fields.
	Exchange   string
	RoutingKey string
	// Queue is the queue a consumer attaches to.
	Queue string
	// Node is AMQP 1.0's single address string, empty for AMQP 0-9-1.
	Node string
}

// ValueKind is the type of one extensible-table entry.
//
// The three are what both protocols' tables can carry and what the header
// promise needs: a string, an opaque byte string, and an integer for the
// broker's own numeric arguments such as `x-delay`.
type ValueKind uint8

// The entry value kinds.
const (
	// Text is a UTF-8 string.
	Text ValueKind = iota
	// Binary is an opaque byte string. AMQP 0-9-1's field tables carry it as
	// a byte array and AMQP 1.0's application properties as a binary value,
	// so both protocols keep it byte for byte.
	Binary
	// Int is a signed integer, for the broker's own numeric arguments.
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

// Entry is one extensible-table entry: an application property on AMQP 1.0, a
// field-table entry on AMQP 0-9-1.
type Entry struct {
	// Key is the entry's name.
	Key string
	// Text is the value when Kind is Text.
	Text string
	// Bin is the value when Kind is Binary. It BORROWS the caller's storage,
	// for the reason: a synchronous publish must not retain the body or
	// the header storage, and copying every header value in order to hand it
	// straight to a client library would allocate per message for nothing.
	Bin []byte
	// Int is the value when Kind is Int.
	Int int64
	// Kind is which of the three the value is.
	Kind ValueKind
}

// Fields is one message's protocol-level form: the fixed properties the
// protocol has a slot for, and the entries that travel in its extensible
// table.
//
// It is a REUSABLE BUFFER and not a value a mapping returns. [Protocol.Carry]
// takes a pointer the caller owns and truncates its slices in place, so a
// sink holds one per sink and the shared mapping costs no allocation per
// publication. `TestAllocs` holds it to that; a `Fields` returned by value
// would allocate the table on every message and put the shared mapping on the
// wrong side of the budget.
//
// The string fields are the intersection of the two protocols' property
// models, and the two extension slices are where they differ: AMQP 0-9-1 has
// one field table and AMQP 1.0 has application properties AND message
// annotations, the second of which is where a broker's own extensions live
// (the delayed-message exchange's `x-delay`, for one). A protocol that has no
// annotations leaves that slice empty.
type Fields struct {
	// ContentType and ContentEncoding are the typed properties.
	ContentType     string
	ContentEncoding string
	// MessageID is the message identifier (Message.ID).
	MessageID string
	// CorrelationID and ReplyTo are the request/reply properties.
	CorrelationID string
	ReplyTo       string
	// Type is AMQP 0-9-1's `type` and AMQP 1.0's `subject`.
	Type string
	// AppID and UserID are the producer's identity. UserID is validated by
	// RabbitMQ against the connection's own user, so a driver sets it only
	// when it is configured to.
	AppID  string
	UserID string
	// Expiration is AMQP 0-9-1's per-message TTL: a decimal string of
	// milliseconds, which is that protocol's own odd encoding. It is filled
	// by the AMQP 0-9-1 arm and left empty by the AMQP 1.0 one.
	Expiration string
	// TTL is the same value in AMQP 1.0's header section, where it is a
	// duration rather than a string. It is filled by the AMQP 1.0 arm and
	// left zero by the AMQP 0-9-1 one.
	//
	// Exactly one of the two is filled, and which one is the protocol's own
	// choice rather than a convenience: rendering a decimal string that the
	// AMQP 1.0 driver would throw away is an allocation per publication with
	// a TTL, and it is one of the few places where the two arms of family
	// The specification really do different work on the hot path.
	TTL time.Duration
	// Timestamp is when the application created the message (the specification's
	// Properties.CreatedAt). It is the zero time when the caller did not
	// say, which the specification makes distinct from a timestamp of zero.
	Timestamp time.Time
	// Priority is the message priority and HasPriority whether the caller
	// named one. The pair exists because a priority of zero is a real
	// priority.
	Priority    uint8
	HasPriority bool
	// Durable is AMQP 0-9-1's `delivery_mode=2` and AMQP 1.0's header
	// `durable=true`.
	Durable bool
	// Entries are the extensible-table entries: the field table on AMQP
	// 0-9-1, the application properties on AMQP 1.0.
	//
	// There is deliberately no second slice for AMQP 1.0's MESSAGE
	// ANNOTATIONS. The annotation section is for infrastructure metadata and
	// a driver that put its own reserved entries there would break the one
	// property this shared mapping exists for: RabbitMQ converts between the
	// two protocols by mapping application properties to and from the 0-9-1
	// field table, so an entry in the annotation section would be in a
	// different place on the other protocol and a message published through
	// one driver would read back without its key through the other. A driver
	// that needs an annotation of its own adds it to the client's message
	// itself.
	Entries []Entry
	// Dropped counts the metadata this mapping left out under
	// MetadataLenientDrop, which the specification requires a driver to report through a
	// hook and a metric rather than silently.
	Dropped int
}

// Reset truncates the buffer for reuse without releasing its storage, which is
// what makes the shared mapping allocation-free on the steady path.
func (f *Fields) Reset() {
	entries := f.Entries[:0]
	*f = Fields{Entries: entries}
}

// Options is what a mapping needs beyond the message itself.
type Options struct {
	// Metadata is the mode. It decides what happens to metadata the
	// protocol has no native place for: every mode but MetadataLenientDrop
	// refuses it, and the driver's own [driverkit.Envelope] has already
	// wrapped anything unrepresentable under the two enveloping modes, so a
	// refusal here under those modes is a driver defect rather than a
	// caller's.
	Metadata mq.MetadataMode
	// Persistent asks for a durable message: `delivery_mode=2` on AMQP
	// 0-9-1, `durable=true` on AMQP 1.0.
	Persistent bool
	// KeyHeader is the extensible-table entry the message key travels in, or
	// empty when the key is the routing key instead (the specification's
	// WithKeyAsRoutingKey). A key that is the routing key is NOT also put in
	// the table: a consumer reads it back from the delivery's own routing
	// key, and carrying it twice would let the two disagree.
	KeyHeader string
	// Publish is the per-call publication control: the TTL and the
	// priority are the two fields either protocol has a slot for.
	Publish mq.PublishOptions
	// Limits bounds what the mapping accepts, so that an oversized header is
	// refused before any I/O rather than by the broker with a channel
	// exception. The zero value bounds nothing.
	Limits mq.Limits
}

// Protocol is one of the two arms.
//
// Every method is pure and safe to call from any goroutine: a protocol holds
// no state, so one value serves every sink and source of a client.
type Protocol interface {
	// Kind returns the variant.
	Kind() Kind
	// MinBroker is the oldest RabbitMQ this protocol can be used against.
	// It is the specification's "supported-version compatibility" and it is what the
	// AMQP 1.0 profile's refusal is read from.
	MinBroker() rabbit.Version
	// Support is what this protocol can do for one feature of the
	// inventory. It is the protocol's half of a driver's capability table:
	// the driver narrows it further for what its own code does not
	// implement, and never widens it.
	Support(f mq.Feature) mq.Support
	// Features is how many of the inventory this protocol supports
	// natively, which is the specification's "feature subset" as one number. It is
	// computed from the table rather than measured.
	Features() int
	// Address derives the destination in this protocol's own form.
	Address(n rabbit.Names, role Role) (Address, error)
	// Carry maps a message into this protocol's property model, filling dst,
	// which it resets first. It allocates nothing when dst's slices already
	// have room.
	Carry(dst *Fields, m *mq.Message, o Options) error
	// Restore maps a delivery's fields back into a message, which is the
	// receive direction of the same mapping. It fills dst's headers and
	// properties and leaves dst's Body alone: the body is the driver's, and
	// copying it here would make the mapping allocate per delivery.
	Restore(dst *mq.Message, f *Fields, o Options) error
	// MaxMessageBytes is the largest message this protocol's framing allows
	// on RabbitMQ, or zero when the driver has no protocol-level bound to
	// advertise.
	MaxMessageBytes() int64
}

// New returns the protocol of kind k.
func New(k Kind) (Protocol, error) {
	switch k.Resolve() {
	case AMQP091:
		return amqp091{}, nil
	case AMQP10:
		return amqp10{}, nil
	case Auto:
		// Unreachable: Resolve maps Auto to a concrete kind. Returning the
		// reference kind rather than panicking keeps a future AutoKind typo
		// from crashing a caller's Open.
		return amqp091{}, nil
	default:
		return nil, ErrInvalidKind
	}
}

// The family's refusals.
var (
	// ErrInvalidKind is a kind outside the enumeration.
	ErrInvalidKind = errcode.New(errcode.InvalidConfig,
		"iqmq/rabbitmq: the protocol is not a known kind")
	// ErrDuplicateHeader is a repeated header key under a mode that does not
	// envelope.
	//
	// Neither protocol's table can carry one: AMQP 0-9-1's field table and
	// AMQP 1.0's application properties are both maps, so the second value
	// would replace the first and the message would read back with one
	// header where two were published. The specification promises duplicate-preserving
	// headers, so the honest answer is a refusal and the route that keeps
	// the promise is MetadataPortableEnvelope or MetadataAdaptive.
	ErrDuplicateHeader = errcode.New(errcode.Unsupported,
		"iqmq/rabbitmq: neither AMQP protocol's property table can carry a repeated header key; use MetadataPortableEnvelope or MetadataAdaptive")
	// ErrReservedHeader is a header under the `x-iqmq-` prefix, which this
	// driver uses for its own correlation and key entries.
	ErrReservedHeader = errcode.New(errcode.InvalidArgument,
		"iqmq/rabbitmq: the header key is under the reserved \"x-iqmq-\" prefix, which this driver uses for its own entries")
	// ErrHeaderTooLarge is a header key or value over the configured limit.
	ErrHeaderTooLarge = errcode.New(errcode.MessageTooLarge,
		"iqmq/rabbitmq: the header key or value is over the advertised limit")
	// ErrMessageTooLarge is a body over the protocol's own framing limit.
	ErrMessageTooLarge = errcode.New(errcode.MessageTooLarge,
		"iqmq/rabbitmq: the message is larger than this protocol allows on RabbitMQ")
	// ErrNoAddress is a name set with nothing to address.
	ErrNoAddress = errcode.New(errcode.InvalidArgument,
		"iqmq/rabbitmq: the name set has no exchange to publish to and no queue to consume from")
	// ErrBadMetadataMode is a metadata mode outside the four.
	ErrBadMetadataMode = errcode.New(errcode.InvalidConfig,
		"iqmq/rabbitmq: the metadata mode is not one of the message contract's four")
)

// ReservedHeaderPrefix is the prefix this driver reserves for its own
// extensible-table entries: the message key when it does not travel as the
// routing key, and the publisher-confirm correlation token that
// `internal/rabbit/confirm` needs in order to attribute a `basic.return`.
//
// A caller's header under it is refused rather than shadowed, because the
// alternative is a header that silently means something to the driver: a
// message published with `x-iqmq-key` set by the application would arrive with
// a key the application never chose.
const ReservedHeaderPrefix = "x-iqmq-"

// carry is the part of the mapping the two protocols share, which is most of
// it: the fixed properties, the header walk, the key and the limits.
//
// What the two protocols differ about is where the result goes — one table or
// two sections — and how a TTL is spelled, so those are the parameters.
func carry(dst *Fields, m *mq.Message, o Options, ttlAsString bool) error {
	if !o.Metadata.Valid() {
		return ErrBadMetadataMode
	}
	dst.Reset()
	if m == nil {
		return nil
	}
	lenient := o.Metadata == mq.MetadataLenientDrop

	dst.ContentType = m.Properties.ContentType
	dst.ContentEncoding = m.Properties.ContentEncoding
	dst.CorrelationID = m.Properties.CorrelationID
	dst.ReplyTo = m.Properties.ReplyTo
	dst.MessageID = m.ID
	dst.Timestamp = m.Properties.CreatedAt
	dst.Durable = o.Persistent

	if o.Publish.PrioritySet {
		dst.Priority, dst.HasPriority = o.Publish.Priority, true
	}
	if o.Publish.TTL > 0 {
		// One spelling, not both: see Fields.TTL.
		if ttlAsString {
			dst.Expiration = millisString(o.Publish.TTL)
		} else {
			dst.TTL = o.Publish.TTL
		}
	}

	// CausationID has no slot in either protocol's property model, so
	// it travels in the table under the reserved prefix. That is a native
	// mapping and not an emulation: the value goes on the wire and comes
	// back, and a non-iqmq consumer sees a header it can read.
	if m.Properties.CausationID != "" {
		dst.Entries = append(dst.Entries, Entry{Key: CausationHeader, Text: m.Properties.CausationID})
	}
	if len(m.Key) > 0 && o.KeyHeader != "" {
		dst.Entries = append(dst.Entries, Entry{Key: o.KeyHeader, Bin: m.Key, Kind: Binary})
	}

	seen := headerSet(m.Headers.Len())
	for k, v := range m.Headers.All() {
		if err := checkHeaderLimits(k, v, o.Limits); err != nil {
			if lenient {
				dst.Dropped++
				continue
			}
			return err
		}
		if hasPrefix(k, ReservedHeaderPrefix) {
			if lenient {
				dst.Dropped++
				continue
			}
			return ErrReservedHeader
		}
		if _, dup := seen[k]; dup {
			if lenient {
				dst.Dropped++
				continue
			}
			return ErrDuplicateHeader
		}
		seen[k] = struct{}{}
		dst.Entries = append(dst.Entries, Entry{Key: k, Bin: v, Kind: Binary})
	}
	if lim := o.Limits.MaxHeaderCount; lim.Known && len(dst.Entries) > lim.Value {
		if !lenient {
			return ErrHeaderTooLarge
		}
		dst.Dropped += len(dst.Entries) - lim.Value
		dst.Entries = dst.Entries[:lim.Value]
	}
	return nil
}

// CausationHeader is the table entry CausationID travels in.
const CausationHeader = ReservedHeaderPrefix + "causation"

// restore is the receive direction of [carry]: the fixed properties and the
// table entries back into a message.
//
// It writes the message's headers and properties and leaves its Body alone.
// The body is the driver's — it points into the client library's own buffer,
// which the specification makes borrowed until the delivery is settled — and copying it
// here would allocate per delivery for nothing.
func restore(dst *mq.Message, f *Fields, o Options) error {
	if !o.Metadata.Valid() {
		return ErrBadMetadataMode
	}
	if dst == nil || f == nil {
		return nil
	}
	dst.Properties.ContentType = f.ContentType
	dst.Properties.ContentEncoding = f.ContentEncoding
	dst.Properties.CorrelationID = f.CorrelationID
	dst.Properties.ReplyTo = f.ReplyTo
	dst.Properties.CreatedAt = f.Timestamp
	dst.ID = f.MessageID

	for i := range f.Entries {
		e := &f.Entries[i]
		switch {
		case e.Key == CausationHeader:
			dst.Properties.CausationID = valueText(e)
			continue
		case e.Key == o.KeyHeader && o.KeyHeader != "":
			dst.Key = e.Bin
			continue
		case hasPrefix(e.Key, ReservedHeaderPrefix):
			// This driver's own entries are not the application's headers.
			// Handing them back would make a round trip through this driver
			// add headers the publisher never set, which is exactly what
			// the fidelity scenario checks.
			continue
		}
		dst.Headers.Add(e.Key, entryBytes(e))
	}
	return nil
}

// entryBytes is an entry's value as bytes, whichever kind it is.
func entryBytes(e *Entry) []byte {
	switch e.Kind {
	case Binary:
		return e.Bin
	case Int:
		return []byte(millisStringOf(e.Int))
	case Text, valueKindCount:
	}
	return []byte(e.Text)
}

// valueText is an entry's value as a string.
func valueText(e *Entry) string {
	if e.Kind == Binary {
		return string(e.Bin)
	}
	return e.Text
}

// checkHeaderLimits refuses a header the broker would refuse, before any I/O.
//
// The 255-byte key ceiling is the PROTOCOL's and is checked whatever the
// capability table advertises: a field-table key is a shortstr on the wire, so
// a longer one cannot be encoded at all and the client library's own answer to
// it is a channel exception -- which on a publisher-confirm channel takes every
// publication whose confirm has not yet arrived with it.
func checkHeaderLimits(k string, v []byte, lim mq.Limits) error {
	if len(k) > rabbit.MaxNameLen {
		return ErrHeaderTooLarge
	}
	if lim.MaxHeaderBytes.Known && int64(len(k))+int64(len(v)) > lim.MaxHeaderBytes.Value {
		return ErrHeaderTooLarge
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

// millisString is a duration as AMQP 0-9-1's `expiration` property: a decimal
// string of milliseconds, rounded UP so that a TTL is never shorter than the
// caller asked for.
func millisString(d time.Duration) string {
	if d <= 0 {
		return ""
	}
	return millisStringOf(int64((d + time.Millisecond - 1) / time.Millisecond))
}

// millisStringOf renders a whole number without reaching for strconv's
// formatting of a signed value, which the hot path does not need.
func millisStringOf(n int64) string {
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

// native, conditional, emulated and unsupported are the four supports a
// protocol's table is written in terms of, so that a row reads as a sentence
// rather than as a struct literal.
//
// The specification requires a conditional support to state its condition and an emulation
// to say what the emulation is, and `driverkit.CapabilityBuilder` refuses a
// table that does not — so these take the note as a parameter and the driver's
// own construction is what enforces it.
func native(scope mq.Scope) mq.Support {
	return mq.Support{Level: mq.Native, Scope: scope}
}

// conditional is a feature that needs a profile, a queue argument or a plugin,
// with the condition stated.
func conditional(scope mq.Scope, note string) mq.Support {
	return mq.Support{Level: mq.Conditional, Scope: scope, Note: note}
}

// unsupported is the honest zero: the protocol does not have the feature.
func unsupported() mq.Support { return mq.Support{Level: mq.Unsupported} }

// countNative counts the features a protocol supports at Native level, which
// is the specification's "feature subset" reduced to one number.
//
// It counts NATIVE only and not "available at some level", which is the
// distinction that makes the number mean something: a conditional support is a
// feature a deployment may or may not have, and an emulation is iqmq's work
// rather than the protocol's, so counting either would let a protocol improve
// its score by adding a caveat.
func countNative(p Protocol) int {
	n := 0
	for f := range mq.FeatureCount {
		if p.Support(mq.Feature(f)).Level == mq.Native {
			n++
		}
	}
	return n
}

// SupportTable is a protocol's whole answer, in inventory order.
//
// It is what a driver's capability table is built from and what the two arms
// are compared on: the driver narrows a row for what its own code does not
// implement and never widens one, and `TestDriverNarrowsAndNeverWidens` in
// each driver holds it to that.
func SupportTable(p Protocol) []mq.Support {
	out := make([]mq.Support, mq.FeatureCount)
	for f := range mq.FeatureCount {
		out[f] = p.Support(mq.Feature(f))
	}
	return out
}
