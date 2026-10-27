package rabbitmq

import (
	"strconv"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/internal/rabbit/protocol"
)

// The extensible-table entries this driver reserves for itself. They are under
// `internal/rabbit/protocol`'s reserved prefix, which the shared mapping
// refuses a caller's header under, so a message never arrives with one the
// application set.
const (
	// KeyHeader carries the message key when it does not travel as the
	// routing key. A key is bytes and a routing key is a
	// shortstr, so a binary key cannot be a routing key at all — and even a
	// textual one changes the routing when it is used as one, which is why
	// [Config.KeyAsRoutingKey] is a decision and not the default.
	KeyHeader = protocol.ReservedHeaderPrefix + "key"
	// ConfirmHeader carries the publisher-confirm correlation token that
	// `internal/rabbit/confirm` needs in order to attribute a
	// `basic.return`: the frame carries the returned message but not its
	// delivery tag, so with more than one publication outstanding there is
	// nothing else to match on.
	//
	// It is set ONLY on a `mandatory` publication, under every strategy: the
	// per-message strategy keeps one publication outstanding per caller, and
	// concurrent callers on one publisher share its channel.
	ConfirmHeader = protocol.ReservedHeaderPrefix + "pub"
	// NilBodyHeader records that the published body was NIL rather than
	// empty.
	//
	// AMQP 0-9-1 cannot tell the two apart: a body is a sequence of content
	// frames and a zero-length body is no content frames, which is what a nil
	// body is too. The specification makes the distinction observable -- a caller that
	// published nil gets nil back -- so the driver carries the one bit the
	// protocol drops. It is set only for a nil body, so an empty one costs
	// nothing.
	NilBodyHeader = protocol.ReservedHeaderPrefix + "nil"
	// AttemptHeader carries the attempt count across a delayed
	// redelivery.
	//
	// A delayed redelivery is a RE-PUBLICATION (see [settler.requeueLater]),
	// and `x-delivery-count` belongs to the QUEUE ENTRY rather than to the
	// message: the parked copy is a new entry, so the broker's count restarts
	// at zero and every delayed redelivery would report attempt 1 for ever.
	// A caller whose policy is "dead-letter after three attempts" would then
	// retry a poison message without end, which is the failure written out
	// in full. So the driver carries the count it knows across the hop, and
	// the delivery's Attempt is the larger of what the broker counted and
	// what the driver carried.
	AttemptHeader = protocol.ReservedHeaderPrefix + "attempt"
)

// The two names RabbitMQ's redelivery counter has had, which is the specification's
// Attempt.
//
// Both are QUORUM-QUEUE fields. A classic queue carries neither, so a driver
// reading one off a classic queue would report every delivery as the first —
// which makes MaxAttempts silently ineffective, because the count it
// compares never rises. The capability table states the condition.
//
// There are TWO because the broker renamed the header, and the rename is the
// single most valuable thing the version matrix found. Measured against three
// real brokers (`conformance/matrix/rabbitmq_test.go` asserts it, and
// docs/drivers/rabbitmq.md records it as a verified fact):
//
//	RabbitMQ 3.13.7   x-delivery-count
//	RabbitMQ 4.1.8    x-delivery-count
//	RabbitMQ 4.3.5    x-acquired-count
//
// A driver that read only the documented name would report Attempt 1 on every
// delivery from a 4.3 broker — so MaxAttempts would never fire, a
// poison message would be retried for ever, and nothing would say why. Reading
// EITHER is what makes the attempt count a fact about the message rather than
// about the broker's minor version.
const (
	// DeliveryCountHeader is the name RabbitMQ 3.8 through 4.1 uses.
	DeliveryCountHeader = "x-delivery-count"
	// AcquiredCountHeader is the name RabbitMQ 4.3 uses. The two never
	// arrive together and mean the same thing: how many times this queue
	// entry has been handed to a consumer before now.
	AcquiredCountHeader = "x-acquired-count"
)

// emptyBody is the body an EMPTY publication comes back as: non-nil and zero
// length, which is what tells it apart from a nil one.
//
// It is shared by every delivery, which is safe because it has no bytes: there
// is nothing for a caller to read and nothing for one to write. A per-delivery
// allocation would be one allocation per empty message for nothing.
var emptyBody = []byte{}

// The delivery modes of `basic.properties`. The names are the specification's.
const (
	deliveryTransient  = 1
	deliveryPersistent = 2
)

// publishing is a sink's reusable AMQP publication.
//
// The [amqp.Table] is reused across publications, which is safe for one
// specific reason: `Channel.PublishWithContext` encodes the table into the
// frame and writes it before it returns, so the library never retains the map.
// A sink that allocated a fresh table per publication would allocate once per
// message for nothing, and the specification budgets the publish path. Publications on one
// sink are serialised by the confirm lock, so one table per sink is enough.
type publishing struct {
	pub    amqp.Publishing
	fields protocol.Fields
}

// encode fills p from the message, the resolved options and the protocol's own
// mapping.
//
// The two-step shape — the shared [protocol.Protocol] first, then the copy
// into the client library's struct — is what keeps the two RabbitMQ drivers
// mapping one message the same way. Everything that is a DECISION (which iqmq
// field goes into which protocol slot, what happens to a header the table
// cannot carry, how a TTL is spelled) is in the shared mapping; everything
// here is a field copy.
func (p *publishing) encode(proto protocol.Protocol, m *mq.Message, o protocol.Options, token string) error {
	if err := proto.Carry(&p.fields, m, o); err != nil {
		return err
	}
	f := &p.fields
	p.pub = amqp.Publishing{
		Headers:         p.table(f, token, m.Body == nil),
		ContentType:     f.ContentType,
		ContentEncoding: f.ContentEncoding,
		DeliveryMode:    deliveryTransient,
		Priority:        f.Priority,
		CorrelationId:   f.CorrelationID,
		ReplyTo:         f.ReplyTo,
		Expiration:      f.Expiration,
		MessageId:       f.MessageID,
		Timestamp:       f.Timestamp,
		Body:            m.Body,
	}
	if f.Durable {
		p.pub.DeliveryMode = deliveryPersistent
	}
	return nil
}

// table refills the reusable header table from the mapped entries.
//
// A nil table is left nil when there is nothing to carry: `basic.properties`
// omits the header field entirely then, which is two bytes smaller on the wire
// and, more usefully, means a consumer that reads `d.Headers == nil` can tell
// "no headers" from "an empty header table".
func (p *publishing) table(f *protocol.Fields, token string, nilBody bool) amqp.Table {
	n := len(f.Entries)
	if token != "" {
		n++
	}
	if nilBody {
		n++
	}
	if n == 0 {
		return nil
	}
	if p.pub.Headers == nil {
		p.pub.Headers = make(amqp.Table, n)
	}
	t := p.pub.Headers
	for k := range t {
		delete(t, k)
	}
	for i := range f.Entries {
		e := &f.Entries[i]
		switch e.Kind {
		case protocol.Binary:
			t[e.Key] = e.Bin
		case protocol.Int:
			t[e.Key] = e.Int
		case protocol.Text:
			t[e.Key] = e.Text
		}
	}
	if token != "" {
		t[ConfirmHeader] = token
	}
	if nilBody {
		// The value is not read: the KEY's presence is the bit. A boolean is
		// the smallest field-table value AMQP 0-9-1 has.
		t[NilBodyHeader] = true
	}
	return t
}

// decode fills a message and its delivery metadata from one AMQP delivery.
//
// dst's message storage is reused: the core copies the DriverDelivery out of
// the fetch buffer before it hands it to a caller, so a source keeps one
// message per buffer slot and refills it. The Body is NOT copied — it points
// into the library's own decoded frame, which the specification makes borrowed until the
// delivery is settled — and a consumer that wants an independent copy asks the
// core for one.
func decode(
	proto protocol.Protocol,
	dst *mq.Message,
	fields *protocol.Fields,
	d *amqp.Delivery,
	o protocol.Options,
	sub string,
	quorum bool,
	now time.Time,
) (mq.DeliveryInfo, error) {
	dst.Reset()
	fields.Reset()
	fields.ContentType = d.ContentType
	fields.ContentEncoding = d.ContentEncoding
	fields.CorrelationID = d.CorrelationId
	fields.ReplyTo = d.ReplyTo
	fields.MessageID = d.MessageId
	fields.Timestamp = d.Timestamp
	fields.Priority = d.Priority
	fields.HasPriority = d.Priority > 0
	fields.Durable = d.DeliveryMode == deliveryPersistent
	fields.Expiration = d.Expiration

	// Attempt. A QUORUM queue counts redeliveries, so the attempt is
	// one more than the count -- and the count's ABSENCE is what a driver
	// gets wrong: RabbitMQ omits `x-delivery-count` on a FIRST delivery and
	// sends it from the second onwards. A driver that keyed the attempt on
	// the header's presence therefore reported zero -- "the broker does not
	// count attempts", which the specification makes a different statement -- for every
	// first delivery, and MaxAttempts could then not tell a first
	// delivery from an uncounted one. So the attempt is 1 whenever the queue
	// counts at all, and the header raises it.
	//
	// A CLASSIC queue does not carry the header ever, and the attempt then
	// stays zero, which is the honest answer: the broker is not counting.
	attempt := uint32(0)
	if quorum {
		attempt = 1
	}
	var (
		carried uint32
		nilBody bool
	)
	for k, v := range d.Headers {
		switch k {
		case DeliveryCountHeader, AcquiredCountHeader:
			if n, ok := asInt(v); ok && quorum && n >= 0 {
				attempt = uint32(n) + 1
			}
			continue
		case AttemptHeader:
			// The driver's own count across a delayed redelivery. See
			// [AttemptHeader].
			if n, ok := asInt(v); ok && n > 0 && n <= int64(^uint32(0)>>1) {
				carried = uint32(n)
			}
			continue
		case NilBodyHeader:
			nilBody = true
			continue
		}
		e, ok := entryOf(k, v)
		if !ok {
			// A value of a kind no byte string can carry — a nested table, a
			// list, a decimal — came from a publisher that is not this
			// driver. The headers are bytes, so there is nothing to hand
			// the caller, and inventing a rendering would make a round trip
			// through iqmq change the message. It is dropped, and
			// `foreign-header` on the driver's own counters is where it is
			// reported.
			continue
		}
		fields.Entries = append(fields.Entries, e)
	}
	if carried > 0 && carried >= attempt {
		// The parked copy's own entry has been delivered `attempt` times and
		// the message had been delivered `carried` times before it was
		// parked, so this delivery is at least the one after that. The MAX
		// rather than a sum, because a requeue of the parked copy raises the
		// broker's count too and adding both would double-count it.
		attempt = carried + 1
	}
	if err := proto.Restore(dst, fields, o); err != nil {
		return mq.DeliveryInfo{}, err
	}
	dst.Body = d.Body
	if len(d.Body) == 0 {
		// the nil-versus-empty distinction, in BOTH directions. The
		// protocol has one representation for the two -- a body is a
		// sequence of content frames and a zero-length body is none of them
		// -- so the library hands over whichever of nil and empty it happens
		// to have built, and the driver decides from the carrier which one
		// the publisher meant.
		//
		// The guard on the length is what keeps a forged header from
		// discarding a body: the header is under the reserved prefix, so no
		// application can set it through iqmq, but a native publisher can set
		// anything.
		if nilBody {
			dst.Body = nil
		} else {
			dst.Body = emptyBody
		}
	}
	if o.KeyHeader == "" && d.RoutingKey != "" && len(dst.Key) == 0 {
		// With the key travelling as the routing key there is no header to
		// read it back from, so the routing key IS the key. It is only done
		// when the configuration says so, because on a topic exchange the
		// routing key of an ordinary publication is the TOPIC and handing
		// that back as a message key would invent one.
		dst.Key = []byte(d.RoutingKey)
	}

	info := mq.DeliveryInfo{
		Topic:        d.Exchange,
		Source:       d.RoutingKey,
		Subscription: sub,
		ReceivedAt:   now,
		Attempt:      attempt,
		Redelivered:  d.Redelivered,
	}
	if d.DeliveryTag > 0 {
		// A delivery tag is per channel and monotonic, which is what the specification's
		// Sequence describes, and it is NOT a broker-wide position: a caller
		// must not read it as one, which is why the driver page says so.
		info.Sequence, info.HasSequence = d.DeliveryTag, true
	}
	return info, nil
}

// entryOf converts one AMQP field-table value into a protocol entry, reporting
// false for a value no byte string can carry.
//
// The conversions are the ones that keep a value's own bytes: a string and a
// byte array pass through, and an integer or a boolean is rendered in the form
// a caller would have published it in. What is refused is a nested table, a
// list and a decimal — three shapes the byte-string headers have no form
// for, and which no iqmq publisher produces.
func entryOf(key string, v any) (protocol.Entry, bool) {
	switch x := v.(type) {
	case string:
		return protocol.Entry{Key: key, Text: x, Kind: protocol.Text}, true
	case []byte:
		return protocol.Entry{Key: key, Bin: x, Kind: protocol.Binary}, true
	case bool:
		return protocol.Entry{Key: key, Text: strconv.FormatBool(x), Kind: protocol.Text}, true
	case int8:
		return protocol.Entry{Key: key, Int: int64(x), Kind: protocol.Int}, true
	case int16:
		return protocol.Entry{Key: key, Int: int64(x), Kind: protocol.Int}, true
	case int32:
		return protocol.Entry{Key: key, Int: int64(x), Kind: protocol.Int}, true
	case int64:
		return protocol.Entry{Key: key, Int: x, Kind: protocol.Int}, true
	case int:
		return protocol.Entry{Key: key, Int: int64(x), Kind: protocol.Int}, true
	case float32:
		return protocol.Entry{Key: key, Text: strconv.FormatFloat(float64(x), 'g', -1, 32), Kind: protocol.Text}, true
	case float64:
		return protocol.Entry{Key: key, Text: strconv.FormatFloat(x, 'g', -1, 64), Kind: protocol.Text}, true
	case time.Time:
		return protocol.Entry{Key: key, Text: x.UTC().Format(time.RFC3339Nano), Kind: protocol.Text}, true
	case nil:
		// An AMQP void. It is a header the publisher set with no value,
		// which the specification can represent as an empty byte string.
		return protocol.Entry{Key: key, Kind: protocol.Binary}, true
	default:
		return protocol.Entry{}, false
	}
}

// asInt reads an AMQP field-table integer whatever width it arrived in.
//
// `x-delivery-count` is the reason it exists: RabbitMQ sends it as a signed
// 64-bit integer, and a driver that type-asserted to the one width it happened
// to see in a test would read zero from a broker that chose another — so
// Attempt would be one for every delivery and MaxAttempts would never
// fire.
func asInt(v any) (int64, bool) {
	switch x := v.(type) {
	case int64:
		return x, true
	case int32:
		return int64(x), true
	case int16:
		return int64(x), true
	case int8:
		return int64(x), true
	case int:
		return int64(x), true
	case uint8:
		return int64(x), true
	case uint16:
		return int64(x), true
	case uint32:
		return int64(x), true
	case uint64:
		if x > 1<<62 {
			return 0, false
		}
		return int64(x), true
	case float64:
		return int64(x), true
	case string:
		n, err := strconv.ParseInt(x, 10, 64)
		return n, err == nil
	default:
		return 0, false
	}
}

// confirmToken is the correlation token publication n carries, which is a
// counter rendered in base 36.
//
// A counter and not a random value, for two reasons. It cannot collide, so
// `confirm.Tracker`'s duplicate-token refusal is unreachable in practice; and
// it is short — five characters for a million publications — which matters
// because it is a header on every `mandatory` publication under a windowed
// strategy.
func confirmToken(n uint64) string {
	if n == 0 {
		return "p0"
	}
	const digits = "0123456789abcdefghijklmnopqrstuvwxyz"
	var buf [16]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = digits[n%36]
		n /= 36
	}
	return "p" + string(buf[i:])
}

// tokenOfReturn reads the correlation token off a returned message.
//
// It is the only way to attribute a `basic.return` when more than one
// publication is outstanding: the frame carries the message but not its
// delivery tag. A return with no token is a publication made under the
// per-message strategy, or one made by something other than this driver, and
// the caller of this function decides which.
func tokenOfReturn(r *amqp.Return) string {
	v, ok := r.Headers[ConfirmHeader]
	if !ok {
		return ""
	}
	switch x := v.(type) {
	case string:
		return x
	case []byte:
		return string(x)
	default:
		return ""
	}
}
