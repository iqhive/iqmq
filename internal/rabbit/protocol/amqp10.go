package protocol

import (
	mq "bitbucket.org/iqhive/iqmq"
	"bitbucket.org/iqhive/iqmq/internal/rabbit"
)

// amqp10 is the AMQP 1.0 profile, for RabbitMQ 4.0 and newer.
//
// It is not the default and it is not a substitute for the other arm: the specification
// says the profile is "never silently substituted", and [amqp10.MinBroker] is
// the mechanism — a client that asks for this protocol against RabbitMQ 3.13
// is refused with `mq.ErrUnsupported` rather than quietly given AMQP 0-9-1.
// The reason is that the two protocols are not the same broker seen through
// two windows: the settlement model, the unroutable-message outcome and the
// request/reply mechanism all differ, so a caller who was silently moved
// between them would find different behaviour under one name.
//
// # What it gains over AMQP 0-9-1
//
// The message model is richer and typed. AMQP 1.0 carries a header section, a
// properties section, application properties AND message annotations, with
// typed values throughout, so a binary property and a numeric one are both
// native rather than encoded into a byte array. The `delivery-count` field of
// the header section is the attempt counter Attempt reads, and it is a
// protocol field rather than RabbitMQ's `x-delivery-count` header extension.
// Settlement is a per-delivery DISPOSITION — accepted, rejected, released,
// modified — which maps onto the four actions more directly than
// `basic.ack`/`basic.nack`/`basic.reject` do, and `modified` carries
// `delivery-failed` and `undeliverable-here` as separate flags.
//
// # What it loses
//
// There is no `mandatory` flag and no `basic.return`, so the specification's "Mandatory +
// NotifyReturn = Rejected" has no equivalent: an unroutable message on this
// protocol is RELEASED by the broker, which this driver reports as Rejected
// too but which reaches the driver as a disposition rather than as a returned
// message. There is no confirm MODE and therefore no at all: every
// publication on an unsettled link gets its own disposition, so the
// per-message, windowed and asynchronous strategies collapse into one
// mechanism — the link's credit — and the choice a caller makes is a credit
// window rather than a confirm strategy. And there is no direct reply-to, so
// native request/reply is gone.
//
// Those three absences are the whole of the specification's "feature subset" criterion for
// this family, and TestFeatureSubsetsDiffer is what keeps them from being
// quietly equalised.
type amqp10 struct{}

var _ Protocol = amqp10{}

// Kind returns AMQP10.
func (amqp10) Kind() Kind { return AMQP10 }

// MinBroker is RabbitMQ 4.0, the first release that speaks AMQP 1.0 natively.
//
// A broker below it is refused. On 3.13 AMQP 1.0 is a plugin whose feature
// set, address syntax and settlement behaviour differ from the native
// implementation, so a driver that accepted it would be claiming this
// profile's behaviour for something else.
func (amqp10) MinBroker() rabbit.Version { return rabbit.MinBrokerAMQP10 }

// MaxMessageBytes is zero: an AMQP 1.0 message is framed in as many transfer
// frames as the negotiated frame size needs, so the protocol imposes no
// ceiling and the real one is the broker's `max_message_size`.
func (amqp10) MaxMessageBytes() int64 { return 0 }

// The AMQP 1.0 address prefixes RabbitMQ 4 uses.
//
// They are RabbitMQ's own v2 address format, and they are here rather than in
// `internal/rabbit` because they are the PROTOCOL's spelling of a destination
// and not the destination itself: the same exchange has one name and two
// addresses.
const (
	// ExchangePrefix addresses an exchange and a routing key:
	// "/exchanges/<exchange>/<routing-key>".
	ExchangePrefix = "/exchanges/"
	// QueuePrefix addresses a queue: "/queues/<queue>".
	QueuePrefix = "/queues/"
)

// Address is RabbitMQ 4's AMQP 1.0 address for the destination.
//
// A publication addresses the exchange and the routing key together, because
// an AMQP 1.0 link has one address and no second field to put a routing key
// in; a consumer addresses the queue. The 0-9-1 fields are filled in too, so
// that a driver reporting a delivery's origin has the same names whichever
// protocol it speaks.
func (amqp10) Address(n rabbit.Names, role Role) (Address, error) {
	switch role {
	case Consume:
		if n.Queue == "" {
			return Address{}, ErrNoAddress
		}
		return Address{
			Queue:      n.Queue,
			Exchange:   n.Exchange,
			RoutingKey: n.RoutingKey,
			Node:       QueuePrefix + escapeAddress(n.Queue),
		}, nil
	case Publish:
		if n.Exchange == "" {
			return Address{}, ErrNoAddress
		}
		return Address{
			Exchange:   n.Exchange,
			RoutingKey: n.RoutingKey,
			Node:       ExchangePrefix + escapeAddress(n.Exchange) + "/" + escapeAddress(n.RoutingKey),
		}, nil
	default:
		return Address{}, ErrNoAddress
	}
}

// escapeAddress percent-encodes the characters that would otherwise end a
// segment of an AMQP 1.0 address.
//
// RabbitMQ's v2 address format is path-shaped, so an exchange or routing key
// containing "/" would split into two segments and address something else
// entirely — which for a routing key is not a theoretical case at all, because
// a routing key is arbitrary caller-supplied text. Percent-encoding it is what
// RabbitMQ's own documentation asks for, and it is done here rather than in
// the driver so that both address forms come from one function.
func escapeAddress(s string) string {
	needed := 0
	for i := range len(s) {
		if mustEscape(s[i]) {
			needed++
		}
	}
	if needed == 0 {
		return s
	}
	out := make([]byte, 0, len(s)+2*needed)
	const hex = "0123456789ABCDEF"
	for i := range len(s) {
		c := s[i]
		if !mustEscape(c) {
			out = append(out, c)
			continue
		}
		out = append(out, '%', hex[c>>4], hex[c&0xf])
	}
	return string(out)
}

// mustEscape reports whether a byte would change an address's meaning.
//
// "/" separates segments and "%" introduces an escape, so an unescaped "%" in
// a name would be read as the start of one. Everything else is left alone: a
// broker that accepted the name accepts it here too, and escaping more than
// necessary would make the address unreadable in a log.
func mustEscape(c byte) bool { return c == '/' || c == '%' }

// Carry maps the message into the header, properties and application-property
// sections.
func (amqp10) Carry(dst *Fields, m *mq.Message, o Options) error {
	return carry(dst, m, o, false)
}

// Restore is the receive direction.
func (amqp10) Restore(dst *mq.Message, f *Fields, o Options) error {
	return restore(dst, f, o)
}

// Support is this protocol's half of the capability table.
//
// Where it agrees with AMQP 0-9-1 it says so by returning the same support;
// where it differs, the row carries the reason. The rows that differ are the family's "feature subset" criterion and are listed at the top of this file.
func (p amqp10) Support(f mq.Feature) mq.Support {
	switch f {
	// --- The three rows this protocol LOSES.
	case mq.FeatureRequestReply:
		// Direct reply-to (`amq.rabbitmq.reply-to`) is a RabbitMQ extension
		// of AMQP 0-9-1 and has no AMQP 1.0 form. A reply address can of
		// course be carried in the `reply-to` property, but that is the
		// portable emulation the core provides rather than a native
		// request/reply mechanism, and the specification forbids advertising one as the
		// other.
		return unsupported()
	case mq.FeaturePublisherConfirm:
		// There is no confirm MODE: a publication on an unsettled link is
		// resolved by its own disposition, which is stronger and simpler
		// than `confirm.select` and is not the same feature. It is
		// advertised as native because the guarantee a caller wants from
		// FeaturePublisherConfirm -- "the broker told me it took this
		// message" -- is exactly what a disposition is; what is gone is the choice of strategy.
		return native(mq.ScopeTopic)
	case mq.FeatureBatchSettle:
		// A disposition carries a first and last delivery id, so a range of
		// deliveries is settled in one frame -- which is the same shape as
		// `basic.ack{multiple}` and carries the same ordering caveat.
		return conditional(mq.ScopeGroup,
			"a disposition settles a range of delivery ids in one frame, which is safe only when the deliveries are settled in the order they arrived; this driver settles individually unless cumulative settlement is proven safe for the subscription")

	// --- The row this protocol GAINS.
	case mq.FeatureBinaryHeaders:
		// Application properties are typed, so a binary value is a binary
		// value rather than a byte array inside a field table. Both
		// protocols keep the bytes; this one does not have to encode them.
		return native(mq.ScopeTopic)

	// --- Delayed redelivery, which is the disposition rather than a plugin.
	case mq.FeatureDelayedRedelivery:
		return conditional(mq.ScopeGroup,
			"a `modified` disposition returns the delivery to the queue immediately; a DELAY still needs a TTL queue that dead-letters back or the rabbitmq_delayed_message_exchange plugin ")

	default:
		// Everything else is the same broker seen through another window, so
		// the answer is AMQP 0-9-1's. Deriving it rather than repeating it
		// is what keeps the two tables from drifting in the rows that are
		// NOT the family's subject: a copied table would have to be edited
		// twice for every change to the broker's own behaviour, and the
		// half that was forgotten would be this one.
		return amqp091{}.Support(f)
	}
}

// Features counts the natively supported features.
func (p amqp10) Features() int { return countNative(p) }
