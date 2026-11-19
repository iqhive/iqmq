package rabbitmq

import (
	"strconv"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/drivers/rabbitmq/internal/client"
	"github.com/iqhive/iqmq/internal/rabbit"
	"github.com/iqhive/iqmq/internal/rabbit/delay"
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
	//
	// Under KeyAsRoutingKey every publication carries it as well, EMPTY when
	// there is no key, because the routing key a delivery arrives with is
	// often not the key: a keyless publication and a request are routed by
	// their topic, a delayed redelivery's parked copy by the subscription's
	// queue, and a message the broker dead-letters by the dead-letter routing
	// key. Without it the consumer read the topic, the queue's name or the
	// dead-letter topic's name as the key. A reply carries it whatever the
	// configuration, because it is routed by the reply address and the
	// requester may read keys either way. A park carries the key the delivery
	// had when the publisher did not set it.
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
	// the delivery's Attempt is what the driver carried plus what the broker
	// has counted on the parked copy's own entry.
	AttemptHeader = protocol.ReservedHeaderPrefix + "attempt"
)

// The two entries that carry a message's own TTL across a delay queue (see
// [parkExpiration]). The dead-letter hop out of the queue removes the
// `expiration` property, so the parked copy goes without it and these say
// what it was and when the copy is back.
const (
	// ttlHeader is the TTL in milliseconds, as `expiration` spelled it.
	ttlHeader = protocol.ReservedHeaderPrefix + "ttl"
	// dueHeader is when the copy leaves its delay queue, in Unix
	// milliseconds on the clock of the client that parked it. The TTL runs
	// from then.
	dueHeader = protocol.ReservedHeaderPrefix + "due"
)

// The broker's own record of a dead-lettered message, which says whether the
// two entries above apply (see [carriedTTL]).
const (
	// deathHeader is RabbitMQ's `x-death` array, one table per queue and
	// reason the message was dead-lettered from, the latest first.
	deathHeader = "x-death"
	// deathExpired is an `x-death` entry's reason for a message whose TTL
	// ran out in the queue it names.
	deathExpired = "expired"
)

// The two names RabbitMQ's redelivery counter has had, which is the specification's
// Attempt.
//
// Both are QUORUM-QUEUE fields. A classic queue carries neither, so a driver
// reading one off a classic queue would report every delivery as the first —
// which makes MaxAttempts silently ineffective, because the count it
// compares never rises. The capability table states the condition.
//
// There are TWO because RabbitMQ 4.3 changed what `x-delivery-count` counts
// and added `x-acquired-count` beside it. Measured against real brokers, one
// message on a quorum queue taken with `basic.get` and handed back in turn by
// two requeueing `basic.nack`s, a requeueing `basic.reject` and two channel
// closes (`conformance/matrix/rabbitmq_test.go` checks the nack and the
// channel close):
//
//	                  after nack, nack, reject, close, close
//	3.13.7 to 4.2.9   x-delivery-count 1, 2, 3, 4, 5
//	4.3.5             x-acquired-count 1, 2, 3, 4, 5
//	                  x-delivery-count -, -, 1, 2, 3
//
// Up to 4.2 `x-delivery-count` counts every return. From 4.3
// `x-acquired-count` counts every time a consumer was handed the entry, and
// `x-delivery-count` only the failed deliveries (a reject, a return by a
// channel or connection close), so after either both arrive with different
// values. On every version the larger of the two is how many times this queue
// entry has been handed to a consumer before now, which is the count Attempt
// is one more than (see [decode]).
//
// A driver that read only `x-delivery-count` would report Attempt 1 on every
// delivery a 4.3 broker requeued with `basic.nack`, which is the driver's own
// requeue, so MaxAttempts would never fire on a poison message. Up to v1.1.2
// the driver read whichever of the two the header table yielded last, which
// is Go's random map order, so on 4.3 after one return Attempt mostly stayed
// at 2 and MaxAttempts did not fire either.
const (
	// DeliveryCountHeader is the count RabbitMQ 3.8 to 4.2 raises on every
	// return of a quorum queue's entry, and 4.3 on a failed delivery only.
	DeliveryCountHeader = "x-delivery-count"
	// AcquiredCountHeader is the count RabbitMQ 4.3 raises every time it
	// hands a quorum queue's entry to a consumer. It is never smaller than
	// [DeliveryCountHeader] when both arrive.
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
//
// carryKey puts key in [KeyHeader], empty when there is none, for the
// publications whose routing key may not be their key. The key is passed
// rather than read from m because m may be an enveloped copy, which carries no
// key of its own.
func (p *publishing) encode(proto protocol.Protocol, m *mq.Message, o protocol.Options, token string, carryKey bool, key []byte) error {
	if err := proto.Carry(&p.fields, m, o); err != nil {
		return err
	}
	f := &p.fields
	p.pub = amqp.Publishing{
		Headers:         p.table(f, token, m.Body == nil, carryKey, key),
		ContentType:     f.ContentType,
		ContentEncoding: f.ContentEncoding,
		DeliveryMode:    deliveryTransient,
		Priority:        f.Priority,
		CorrelationId:   f.CorrelationID,
		// The escaped reply address is how the broker's address appears
		// inside the process (see [replyTopic]); on the wire it is the
		// broker's own again. A relay that forwards a request's Properties
		// otherwise sent the escaped form, which a responder that is not
		// iqmq replied to in vain and an iqmq one escaped a second time.
		ReplyTo:    replyKey(f.ReplyTo),
		Expiration: f.Expiration,
		MessageId:  f.MessageID,
		Timestamp:  f.Timestamp,
		Type:       f.Type,
		AppId:      f.AppID,
		Body:       m.Body,
	}
	if f.Durable {
		p.pub.DeliveryMode = deliveryPersistent
	}
	return nil
}

// identify fills the `app-id` and `type` properties the message left empty
// with the client's own, [Config.AppID] and [Config.PublishType].
func (p *publishing) identify(appID, typ string) {
	if p.pub.AppId == "" {
		p.pub.AppId = appID
	}
	if p.pub.Type == "" {
		p.pub.Type = typ
	}
}

// table refills the reusable header table from the mapped entries.
//
// A nil table is left nil when there is nothing to carry: `basic.properties`
// omits the header field entirely then, which is two bytes smaller on the wire
// and, more usefully, means a consumer that reads `d.Headers == nil` can tell
// "no headers" from "an empty header table".
func (p *publishing) table(f *protocol.Fields, token string, nilBody, carryKey bool, key []byte) amqp.Table {
	n := len(f.Entries)
	if token != "" {
		n++
	}
	if nilBody {
		n++
	}
	if carryKey {
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
	if carryKey {
		t[KeyHeader] = carriedKey(key)
	}
	return t
}

// frameOverhead is a frame's header and end octet, which the negotiated frame
// size counts and its payload does not.
const frameOverhead = 8

// requeueHeadroom is what RabbitMQ adds to a message's header table when it
// hands the message out again after a return: `x-acquired-count` and, after a
// requeueing reject or a return by a channel close, `x-delivery-count`, each a
// shortstr key of 16 octets and a signed 64-bit value, 1 + 16 + 1 + 8 octets.
// 3.13 to 4.2 add only `x-delivery-count`.
const requeueHeadroom = 2 * (1 + 16 + 1 + 8)

// fitsFrame refuses pub when its content header frame, with room for the
// headers a requeue adds, is larger than cn's negotiated frame size allows,
// before any I/O.
//
// The headers travel in that frame with the other properties, and one frame
// cannot be split. The client library writes a larger one without a word, and
// RabbitMQ answers a payload over the frame size with 501 FRAME_ERROR, which
// closes the CONNECTION, so every publication outstanding on it became
// ambiguous and every handle on it reconnected
// (TestAHeaderSetPastTheFrameSizeIsRefusedBeforeAnyIO). RabbitMQ stores one up
// to 8 octets over the frame size less 8, and a consumer's client library on
// the same frame size refuses its delivery with 501
// (TestHeaderFrameSizeIsExactOnABroker), so the frame size less 8 is the most a
// delivery may be. RabbitMQ 4.3 adds [requeueHeadroom] to a quorum queue's
// redelivery, so a publication at that bound came back in a frame its consumer
// could not read, which closed the consumer's connection on every redelivery;
// the headroom is kept (TestARequeuedPublicationAtTheBoundIsDeliveredOnABroker).
// The headers a dead-lettering adds, `x-death` and the `x-first-death-*` and
// `x-last-death-*` ones, are sized by queue and exchange names and are not;
// a delayed nack, whose copy a delay queue dead-letters, checks its own
// ([parkFits]). It is the only bound on the header set, and it is not
// advertised as MaxHeaderBytes because what it leaves for the headers depends
// on their count and on the other properties.
func fitsFrame(pub *amqp.Publishing, cn client.Conn) error {
	size := cn.FrameSize()
	if size <= 0 || headerFrameSize(pub) <= size-frameOverhead-requeueHeadroom {
		return nil
	}
	return errHeaderFrameTooLarge
}

// parkFits refuses a delayed nack's park, pub, before any I/O when the copy
// that comes back from the delay will not fit cn's negotiated frame size with
// [requeueHeadroom] spare.
//
// The copy comes back larger than the delivery it parks. On the plugin arm it
// is the park as published, which adds `x-delay`, [AttemptHeader] and
// [ConfirmHeader] to the delivery's headers. On the TTL and dead-letter arm the
// delay queue dead-letters it back, and RabbitMQ adds what [deadLetterGrowth]
// sizes. A delivery near the bound, parked, came back in a frame its consumer's
// client library refused with 501, so the consumer's connection closed, the
// delivery went back to its queue, and the next delivery closed it again: on a
// classic queue for ever, on a quorum queue until its delivery limit dropped
// the message (TestADelayedNackThatWouldComeBackTooLargeIsRefusedOnABroker).
// The park of a delivery the consumer cannot map ([source.parkUnmappable]) is
// checked the same way, and one refused is left unsettled, as a park that fails
// leaves it.
//
// plan is the park's, and a plan with no queue to declare is not
// dead-lettered; v is the broker's version, zero when it is not known.
func parkFits(pub *amqp.Publishing, plan *delay.Plan, cn client.Conn, v rabbit.Version) error {
	size := cn.FrameSize()
	if size <= 0 {
		return nil
	}
	n := headerFrameSize(pub)
	if plan.Declare != nil {
		n += deadLetterGrowth(pub.Headers, plan.Declare.Name, plan.Exchange, plan.RoutingKey, v)
	}
	if n <= size-frameOverhead-requeueHeadroom {
		return nil
	}
	return errParkTooLarge
}

// The headers RabbitMQ 3.13 and later add to a message they dead-letter,
// besides [deathHeader].
const (
	firstDeathReason   = "x-first-death-reason"
	firstDeathQueue    = "x-first-death-queue"
	firstDeathExchange = "x-first-death-exchange"
	lastDeathReason    = "x-last-death-reason"
	lastDeathQueue     = "x-last-death-queue"
	lastDeathExchange  = "x-last-death-exchange"
)

// deathsRebuilt is the first RabbitMQ release measured to build a
// dead-lettered message's death headers from its own record of the message,
// replacing any it was published with: 4.1.8 does, 3.13.7 does not, and 4.0 was
// not measured.
var deathsRebuilt = rabbit.Version{Major: 4, Minor: 1}

// deadLetterGrowth is how many octets RabbitMQ v adds to the header table h
// of a message published to exchange with routingKey when queue dead-letters
// it for expiry, as amqp091 encodes them; negative when it takes octets away.
//
// It sets seven headers: [deathHeader], whose entry for the queue and the
// reason [deathEntrySize] sizes, the three `x-first-death-*` headers and the
// three `x-last-death-*` ones. How it treats those the message was published
// with, as a delivery parked again carries them from its last dead-lettering,
// depends on the version ([deathsRebuilt]). For a version not known, or not
// measured, it is the larger of the two. Measured on RabbitMQ 3.13.7, 4.1.8,
// 4.2.9 and 4.3.5: a first park into a delay queue of 47 octets came back 471
// octets larger, and parks again into the same queue and into another came
// back exactly as this says (TestDeadLetterGrowthIsWhatRabbitMQAddsOnABroker).
func deadLetterGrowth(h amqp.Table, queue, exchange, routingKey string, v rabbit.Version) int {
	rebuilt := deathsRebuiltGrowth(h, queue, exchange, routingKey)
	if v.AtLeast(deathsRebuilt) {
		return rebuilt
	}
	return max(rebuilt, deathsKeptGrowth(h, queue, exchange, routingKey))
}

// deathFields are the six headers besides [deathHeader] that a dead-lettering
// for expiry from queue of a message published to exchange sets, the three
// `x-first-death-*` ones first.
func deathFields(queue, exchange string) [6][2]string {
	return [6][2]string{
		{firstDeathReason, deathExpired},
		{firstDeathQueue, queue},
		{firstDeathExchange, exchange},
		{lastDeathReason, deathExpired},
		{lastDeathQueue, queue},
		{lastDeathExchange, exchange},
	}
}

// deathsRebuiltGrowth is [deadLetterGrowth] on RabbitMQ 4.1 and later, which
// replace all seven headers: [deathHeader] becomes an array of the one entry.
func deathsRebuiltGrowth(h amqp.Table, queue, exchange, routingKey string) int {
	n := 1 + len(deathHeader) + 1 + 4 + deathEntrySize(queue, exchange, routingKey) - headerEntrySize(h, deathHeader)
	for _, f := range deathFields(queue, exchange) {
		n += 1 + len(f[0]) + 1 + 4 + len(f[1]) - headerEntrySize(h, f[0])
	}
	return n
}

// deathsKeptGrowth is [deadLetterGrowth] on RabbitMQ 3.13, which keeps the
// published [deathHeader] and `x-first-death-*` headers: it adds the entry to
// the front of the array, or raises the count of the entry already there for
// the queue and the reason, and it sets the `x-first-death-*` headers only
// when they are not there, and the `x-last-death-*` ones always.
func deathsKeptGrowth(h amqp.Table, queue, exchange, routingKey string) int {
	entry := deathEntrySize(queue, exchange, routingKey)
	n := 0
	switch deaths, ok := h[deathHeader].([]any); {
	case ok && diedIn(deaths, queue):
	case ok:
		n += entry
	default:
		n += 1 + len(deathHeader) + 1 + 4 + entry - headerEntrySize(h, deathHeader)
	}
	for i, f := range deathFields(queue, exchange) {
		_, there := h[f[0]]
		if i < 3 && there {
			continue
		}
		n += 1 + len(f[0]) + 1 + 4 + len(f[1]) - headerEntrySize(h, f[0])
	}
	return n
}

// deathEntrySize is the encoding of one entry of [deathHeader], for expiry
// from queue of a message published to exchange with routingKey: the table's
// type octet and length, and per key its shortstr, the value's type octet and
// the value, of `count` (a long), `reason`, `queue`, `time` (a timestamp),
// `exchange` and `routing-keys` (an array of the one key).
func deathEntrySize(queue, exchange, routingKey string) int {
	return 1 + 4 +
		(1 + len("count") + 1 + 8) +
		(1 + len("reason") + 1 + 4 + len(deathExpired)) +
		(1 + len("queue") + 1 + 4 + len(queue)) +
		(1 + len("time") + 1 + 8) +
		(1 + len("exchange") + 1 + 4 + len(exchange)) +
		(1 + len("routing-keys") + 1 + 4 + (1 + 4 + len(routingKey)))
}

// diedIn reports whether an `x-death` array has an entry for expiry from
// queue.
func diedIn(deaths []any, queue string) bool {
	for _, d := range deaths {
		t, ok := d.(amqp.Table)
		if !ok {
			continue
		}
		q, _ := t["queue"].(string)
		r, _ := t["reason"].(string)
		if q == queue && r == deathExpired {
			return true
		}
	}
	return false
}

// headerEntrySize is what the header key takes in h's encoding, nothing when
// it is not there.
func headerEntrySize(h amqp.Table, key string) int {
	v, ok := h[key]
	if !ok {
		return 0
	}
	return 1 + len(key) + fieldSize(v)
}

// headerFrameSize is the payload of the content header frame amqp091 v1.16.0
// writes for pub (headerFrame.write): the class, the weight, the body size and
// the property flags, then each property that is set, a shortstr as a length
// octet and its bytes, the header table as a longstr of its entries, and the
// fixed-width ones at their width. It walks the table and allocates nothing.
func headerFrameSize(pub *amqp.Publishing) int {
	n := 2 + 2 + 8 + 2
	n += shortstrSize(pub.ContentType) + shortstrSize(pub.ContentEncoding)
	if len(pub.Headers) > 0 {
		n += tableSize(pub.Headers)
	}
	if pub.DeliveryMode > 0 {
		n++
	}
	if pub.Priority > 0 {
		n++
	}
	n += shortstrSize(pub.CorrelationId) + shortstrSize(pub.ReplyTo) + shortstrSize(pub.Expiration) +
		shortstrSize(pub.MessageId)
	if !pub.Timestamp.IsZero() {
		n += 8
	}
	return n + shortstrSize(pub.Type) + shortstrSize(pub.UserId) + shortstrSize(pub.AppId)
}

// shortstrSize is an optional shortstr property's encoding: nothing when it is
// empty, which leaves its flag clear.
func shortstrSize(s string) int {
	if s == "" {
		return 0
	}
	return 1 + len(s)
}

// tableSize is a field table's encoding: its length, and per entry the key as
// a shortstr and the value with its type octet.
func tableSize(t amqp.Table) int {
	n := 4
	for k, v := range t {
		n += 1 + len(k) + fieldSize(v)
	}
	return n
}

// fieldSize is one field value's encoding, type octet included, for every type
// amqp091's writeField encodes. The table has been validated
// ([amqp.Table.Validate]), so no other type reaches it.
func fieldSize(v any) int {
	switch v := v.(type) {
	case bool, byte, int8:
		return 2
	case int16, uint16:
		return 3
	case int, int32, uint32, float32:
		return 5
	case amqp.Decimal:
		return 6
	case int64, float64, time.Time:
		return 9
	case string:
		return 5 + len(v)
	case []byte:
		return 5 + len(v)
	case amqp.Table:
		return 1 + tableSize(v)
	case []any:
		n := 5
		for _, e := range v {
			n += fieldSize(e)
		}
		return n
	default:
		// nil, 'V', the one valid value left.
		return 1
	}
}

// carriedKey is key as [KeyHeader]'s value: the key, or an empty byte array
// that says there is none. The empty one is shared, because it has no bytes
// for anyone to write.
func carriedKey(key []byte) []byte {
	if len(key) == 0 {
		return emptyBody
	}
	return key
}

// decode fills a message and its delivery metadata from one AMQP delivery.
//
// dst's message storage is reused: the core copies the DriverDelivery out of
// the fetch buffer before it hands it to a caller, so a source keeps one
// message per buffer slot and refills it. The Body is NOT copied — it points
// into the library's own decoded frame, which the specification makes borrowed until the
// delivery is settled — and a consumer that wants an independent copy asks the
// core for one.
//
// topic and sub are the consumer's own, unprefixed. The topic is not read off
// the delivery, because its exchange is the topic's only on a first delivery,
// and prefixed under Config.Prefix even then: a TTL+DLX redelivery comes back
// through the default exchange, "", and a plugin redelivery through
// `<exchange>.delayed.direct`.
func decode(
	proto protocol.Protocol,
	dst *mq.Message,
	fields *protocol.Fields,
	d *amqp.Delivery,
	o protocol.Options,
	topic, sub string,
	quorum bool,
	now time.Time,
) (mq.DeliveryInfo, error) {
	dst.Reset()
	fields.Reset()
	fields.ContentType = d.ContentType
	fields.ContentEncoding = d.ContentEncoding
	fields.CorrelationID = d.CorrelationId
	// A direct reply-to address is escaped into a valid topic name, so that
	// a responder can publish its reply there through the core (see
	// [replyTopic]). Any other `reply_to` is passed on as it is.
	fields.ReplyTo = replyTopic(d.ReplyTo)
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
	//
	// The count is the LARGER of the two headers, for the reason on
	// [AcquiredCountHeader]: RabbitMQ 4.3 sends both after a return, with
	// different values, and the header table is a map.
	attempt := uint32(0)
	if quorum {
		attempt = 1
	}
	var (
		counted = int64(-1)
		carried uint32
		nilBody bool
	)
	for k, v := range d.Headers {
		switch k {
		case DeliveryCountHeader, AcquiredCountHeader:
			if n, ok := asInt(v); ok && n > counted {
				counted = n
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
	if quorum && counted >= 0 {
		attempt = uint32(counted) + 1
	}
	if carried > 0 {
		// The message had been delivered `carried` times before it was
		// parked, and the broker's count on the parked copy is that NEW
		// entry's own: [parkHeaders] strips the old entry's count, so the
		// two cover different deliveries and the attempt is their SUM. It
		// was the larger of the two, on the theory that the parked copy's
		// count included the deliveries before the park, and it does not:
		// after two delayed redeliveries an immediate requeue reported
		// 3, 3, 3, 4, so MaxAttempts compared a number that stood still
		// until the new entry's count caught up with the carried one.
		//
		// The floor of one is a classic queue, which counts nothing and
		// leaves `attempt` at zero: this delivery is still one more than
		// the carried count.
		attempt = carried + max(attempt, 1)
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
	switch {
	case o.KeyHeader == "" && len(dst.Key) == 0:
		// With the key travelling as the routing key, the key is
		// [KeyHeader]'s when the publisher set it and the routing key
		// otherwise (see [routedKey]). It is only done when the configuration
		// says so, because on a topic exchange the routing key of an ordinary
		// publication is the TOPIC and handing that back as a message key
		// would invent one.
		dst.Key = routedKey(d)
	case len(dst.Key) == 0:
		// An empty [KeyHeader] is a publication with no key, which a
		// publisher writes under KeyAsRoutingKey and for a reply, and no key
		// is nil.
		dst.Key = nil
	}

	info := mq.DeliveryInfo{
		Topic:        topic,
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

// routedKey is the message key of a delivery published under
// [Config.KeyAsRoutingKey]: the key [KeyHeader] carries, which every such
// publication of this driver's sets (empty when there is no key), and
// otherwise the routing key. A delivery from a publisher that never sets the
// header, an older iqmq or a native client, is read by its routing key as
// before.
func routedKey(d *amqp.Delivery) []byte {
	v, carried := d.Headers[KeyHeader]
	if !carried {
		if d.RoutingKey == "" {
			return nil
		}
		return []byte(d.RoutingKey)
	}
	var key []byte
	switch x := v.(type) {
	case []byte:
		key = x
	case string:
		key = []byte(x)
	}
	if len(key) == 0 {
		return nil
	}
	return key
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
