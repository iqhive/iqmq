package iqmq

import (
	"errors"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

// The provenance headers a dead-lettered message carries.
//
// They are exported because the consumer of a dead-letter topic reads them,
// and a name a caller has to copy out of a specification is a name that goes
// wrong. Each uses ReservedHeaderPrefix, which the specification reserves for iqmq's own
// use; the runner is iqmq, and it is the only writer of them in this package.
//
// The route's copy carries these five and no driver's provenance from an
// earlier hop: a Kafka driver's iqmq-origin-partition and iqmq-origin-offset,
// an iqmq-dead-letter- header the JetStream or ActiveMQ STOMP driver's own
// dead-letter route wrote, or the iqmq-dlq-origin and iqmq-dlq-reason of the
// SQS driver's, is left out, because it describes that hop and not this one.
//
// # Every driver has to carry these five
//
// The reserved-prefix rule is about a CALLER's headers. These are the
// core's, so a driver's own admission check has to let them through: the
// dead-letter route publishes the stamped copy through the ordinary publish
// path, so a driver that refuses every key under ReservedHeaderPrefix refuses
// iqmq's own provenance, and mq.WithDeadLetter cannot work on it at all. The
// publication fails, the runner requeues the message and counts a
// DeadLetterFailures, and the message is redelivered for ever. Seven drivers
// did that until they exempted these five keys and no others, drivers/gcppubsub
// the last of them.
//
// A driver that refuses a caller's key under the prefix exempts exactly these
// five through driverkit.ReservedHeaderAllowed: the memory broker,
// drivers/kafka, drivers/kafka/kafkago, drivers/nats and drivers/jetstream
// (and so drivers/natsunified), drivers/pulsar, drivers/amqp10 (and so
// drivers/rabbitmq/amqp10) and drivers/gcppubsub. The other drivers refuse
// only names of their own and carry the five as ordinary headers:
// drivers/sqs and drivers/snssqs refuse their own attribute names and carry
// the five as far as the attribute limit allows (fitHeaderCount),
// drivers/rabbitmq and the ActiveMQ drivers their own x-iqmq- prefix, and
// drivers/redis and drivers/nsq nothing under it, NSQ carrying every header
// in the envelope. A driver added later needs the same exemption, which
// TestProvenanceHeadersAreExactlyWhatTheRouteWrites keeps to these five.
const (
	// HeaderOriginTopic is the topic the message was consumed from.
	HeaderOriginTopic = ReservedHeaderPrefix + "origin-topic"
	// HeaderOriginSubscription is the subscription it was consumed by.
	HeaderOriginSubscription = ReservedHeaderPrefix + "origin-subscription"
	// HeaderAttempts is the delivery attempt that failed, in decimal.
	//
	// It is the RUNNER's count and not the broker's: attemptOf floors the
	// broker's attempt at 1 and adds the attempts this runner made without
	// giving the message back, which is what makes the number exact on a
	// broker that counts no attempts at all. Under OrderingStrict every
	// retry is held in process, so the broker's counter never moves and
	// reading it would have said "1" however many attempts were made.
	// It is therefore always present, which is what
	// the specification lists it as, unless the dead-letter publisher's
	// MaxHeaderCount leaves no room for it (fitHeaderCount).
	HeaderAttempts = ReservedHeaderPrefix + "attempts"
	// HeaderFailure is the error the handler reported, rendered by its own
	// Error method.
	//
	// It is the one provenance header whose value is arbitrary, and several
	// brokers frame a header as a line of text (a NATS or JetStream header
	// block is HTTP-shaped), so every ASCII control character in the reason
	// is rendered as a space and the result is trimmed of leading and
	// trailing spaces and tabs. Without that a handler reporting a
	// multi-line error would have its dead-letter publication refused
	// before any I/O on two of the drivers the route has to work on. It is
	// also made valid UTF-8, each run of invalid bytes becoming U+FFFD, and
	// a reason longer than 1,024 bytes is cut at a character boundary and
	// ends in "...", the three bytes counted inside the 1,024: Pub/Sub
	// refuses an attribute value that is longer or not UTF-8, and the route
	// publishes in MetadataNativeStrict, so such an error looped the message
	// there. Every driver gets the same value. Valid bytes at or above 0x80
	// are untouched, so a reason in any language arrives intact; a reason
	// with nothing printable in it leaves the header out rather than
	// setting it empty, and removes one the delivery already carried.
	HeaderFailure = ReservedHeaderPrefix + "failure"
	// HeaderFirstDeliveredAt is when the message was first delivered, in
	// RFC 3339 with nanoseconds.
	//
	// It is written only when this delivery IS the first one — attempt 1, or
	// a broker that counts no attempts and did not flag a redelivery —
	// because that is the only case in which the runner knows the answer: a
	// redelivery may reach a different process entirely, DeliveryInfo carries no
	// first-delivery time, and nothing in the SPI can carry one back.
	// A missing header says "not known"; a header
	// holding this attempt's receive time would say something false, and so
	// would one the delivery already carried, which is removed.
	HeaderFirstDeliveredAt = ReservedHeaderPrefix + "first-delivered-at"
)

// deadLetter routes one message to the dead-letter topic.
//
// The order is the whole point and is the single worst thing in this
// increment to get wrong: publish to the dead-letter destination, confirm it,
// and only THEN settle the original. A runner that acked first, or that acked
// after a publish it could not confirm, would destroy the message in exactly
// the case the dead-letter topic exists for.
//
// So a publish that fails for any reason — a refusal, a cancelled context, or
// an ambiguous outcome the driver cannot resolve — leaves the original
// unacked and requeues it, which is the rule "a failed or ambiguous DLQ
// publish never acks (the message is requeued and counted)". The message is
// then redelivered and tried again; a dead-letter destination that stays
// broken keeps the message alive rather than losing it.
//
// An ambiguous publish may therefore produce a duplicate in the dead-letter
// topic. That is the deliberate direction: the at-least-once boundary
// makes a duplicate a thing a consumer must already handle, and a lost poison
// message is a thing nobody can recover.
//
// A partial fan-out is not a failed publish. A dead-letter topic whose
// subscriptions take a copy independently (memory's FanoutIndependent)
// answers ErrPartial when some took it and the others had no room: the copy
// is in the dead-letter topic, so it is counted and the original is acked,
// as the memory driver's own routes do. Requeueing it published the copy
// again, to the subscriptions that already had it, on every redelivery.
//
// The message goes back with the retry backoff and not at once. A
// destination that refuses the copy, such as one whose size limit it exceeds,
// usually refuses the next one too, and an immediate redelivery ran the
// handler and the refused publication again as fast as the broker could hand
// the message over, for as long as the destination stayed broken.
func (r *runner) deadLetter(it *runItem, d *Delivery, cause error) {
	if r.cfg.nativeDLQ {
		r.nativeDeadLetter(it, d)
		return
	}
	m := r.deadLetterMessage(it, d, cause)
	if err := r.dlq.Publish(r.dlqCtx, m); err != nil && !partlyAccepted(err) {
		// NOT acked. The delivery goes back to the broker so that the
		// message is redeliverable rather than sitting on a lease nobody
		// will settle.
		r.deadLetterRefused(it, d)
		return
	}
	// Counted BEFORE the ack, so a reader of Stats never sees the ack
	// without the reason for it. The route settles positively, so this
	// count is disjoint from DeadLettered, which is the native route's.
	r.c.counters.dlqPublished.Add(1)
	r.ack(d)
}

// nativeDeadLetter settles the delivery through the driver's own dead-letter
// route (Nack(NackDeadLetter())).
//
// There is no publication and therefore no publish-then-ack window: the
// broker moves the message, and the delivery is settled by the same call. The
// provenance headers are not written, because nothing in this path
// writes a message — what the dead-letter destination receives is what the
// broker puts there.
//
// A driver that advertised the route and then refused the action leaves the
// delivery unsettled, exactly as a failed publication does: the message is
// requeued with the backoff and counted rather than lost.
func (r *runner) nativeDeadLetter(it *runItem, d *Delivery) {
	err := d.Nack(NackDeadLetter())
	if err == nil || errors.Is(err, ErrAlreadySettled) {
		return
	}
	r.deadLetterRefused(it, d)
}

// partlyAccepted reports whether err is ErrPartial for one publication: a
// fan-out the broker took for some of the topic's subscriptions and refused
// for the others. A *BatchError carries CodePartial too, for a batch whose
// messages' outcomes differ, and says nothing about any one message.
func partlyAccepted(err error) bool {
	var be *BatchError
	return CodeOf(err) == CodePartial && !errors.As(err, &be)
}

// deadLetterRefused gives back a message whose dead-lettering failed, after
// the backoff its next attempt would wait, and counts the failure.
func (r *runner) deadLetterRefused(it *runItem, d *Delivery) {
	r.requeueAfter(it, r.cfg.backoff.Delay(attemptOf(it, d)+1))
	r.dlqFailed()
}

// deadLetterMessage builds the message that is published to the dead-letter
// topic: an independent copy of the original plus the provenance headers.
//
// The copy is CloneMessage and not the delivery's own Message, because under
// ConsumerConfig.BufferReuse the delivery's storage returns to the consumer
// the moment it is settled, and because adding headers to a received message
// would mutate storage the specification says is borrowed.
//
// d carries the message as it was received, because failAs put it back: the
// copy of a message a middleware decrypted, decompressed or checked out is
// the sealed, compressed or claim-checked one the broker delivered, so a
// dead-letter topic never holds a plaintext the publisher sealed, nor a
// payload larger than the broker accepted in the first place.
//
// The copy keeps the message's headers but not an earlier hop's provenance
// (earlierHop), which the route's own replaces.
func (r *runner) deadLetterMessage(it *runItem, d *Delivery, cause error) *Message {
	m := d.CloneMessage()
	m.Topic = r.cfg.deadLetter
	dropEarlierHops(&m.Headers)
	info := d.Info()
	topic := info.Topic
	if topic == "" {
		topic = r.c.cfg.Topic
	}
	sub := info.Subscription
	if sub == "" {
		sub = r.c.cfg.Subscription
	}
	m.Headers.Set(HeaderOriginTopic, []byte(topic))
	m.Headers.Set(HeaderOriginSubscription, []byte(sub))
	m.Headers.Set(HeaderAttempts, []byte(strconv.Itoa(attemptOf(it, d))))
	// The copy starts with the delivery's own headers, which may already
	// hold the two below: a publisher may set the five provenance keys, and
	// a dead-letter copy an operator sends back to its origin topic carries
	// an earlier route's. A header left out here is therefore deleted, so
	// that the copy never passes off one the runner did not write as its own.
	reason := ""
	if cause != nil {
		reason = framableReason(cause.Error())
	}
	if reason != "" {
		m.Headers.Set(HeaderFailure, []byte(reason))
	} else {
		m.Headers.Delete(HeaderFailure)
	}
	if firstDelivery(d.Attempt, info.Redelivered) && !info.ReceivedAt.IsZero() {
		m.Headers.Set(HeaderFirstDeliveredAt, []byte(info.ReceivedAt.Format(time.RFC3339Nano)))
	} else {
		m.Headers.Delete(HeaderFirstDeliveredAt)
	}
	r.fitHeaderCount(m)
	return m
}

// fitHeaderCount gives up the route's own provenance, in the reverse of the
// order deadLetterMessage writes it, so the origin topic last, while the copy
// holds more headers than the dead-letter publisher's MaxHeaderCount allows.
// The publisher refuses such a copy before any I/O (ErrMessageTooLarge), and
// the runner then requeued the message: drivers/sqs advertises nine, so a
// message with six or more headers of its own could never be dead-lettered
// there, and one with five only on a redelivery, whose copy has no
// HeaderFirstDeliveredAt. The message's own headers stay; a copy they
// alone overfill is still refused, as the message itself would be.
func (r *runner) fitHeaderCount(m *Message) {
	limit := r.dlq.maxHeaders
	if limit <= 0 {
		return
	}
	for _, k := range [...]string{
		HeaderFirstDeliveredAt, HeaderFailure, HeaderAttempts, HeaderOriginSubscription, HeaderOriginTopic,
	} {
		if m.Headers.Len() <= limit {
			return
		}
		m.Headers.Delete(k)
	}
}

// earlierHop reports whether key is provenance a driver wrote on a copy it
// moved or dead-lettered itself: a key under ReservedHeaderPrefix+"origin-"
// other than the two the route writes, or one under
// ReservedHeaderPrefix+"dead-letter-" or ReservedHeaderPrefix+"dlq-".
// drivers/kafka and drivers/kafka/kafkago stamp iqmq-origin-partition and
// iqmq-origin-offset on a record they move to a retry or dead-letter topic,
// drivers/jetstream and the ActiveMQ STOMP driver stamp iqmq-dead-letter-*,
// and drivers/sqs (so drivers/snssqs too) iqmq-dlq-origin and
// iqmq-dlq-reason, on their own dead-letter copies, and each hands them to
// whoever consumes that topic.
//
// The route's copy drops them. They describe the hop before this one: Kafka's
// partition and offset name a record of the topic that hop consumed, not of
// the one the copy's HeaderOriginTopic names, and each driver's own next hop
// replaces them, as the route replaces its five. Kafka and JetStream also
// refuse a caller's key under the prefix other than the five, and SQS holds
// ten message attributes, its own reserved ones among them, so a copy that
// kept them could be refused before any I/O: the runner requeued the message
// and counted a DeadLetterFailures on every delivery, and a Run with
// WithDeadLetter over a retry or dead-letter topic of those drivers never
// dead-lettered anything. Every other key under the prefix stays, because
// some drivers map reserved names of their own to native fields and read
// them back when the copy is published (drivers/amqp10's iqmq-amqp- names).
func earlierHop(key string) bool {
	rest, ok := strings.CutPrefix(key, ReservedHeaderPrefix)
	if !ok || key == HeaderOriginTopic || key == HeaderOriginSubscription {
		return false
	}
	return strings.HasPrefix(rest, "origin-") || strings.HasPrefix(rest, "dead-letter-") ||
		strings.HasPrefix(rest, "dlq-")
}

// dropEarlierHops removes every pair whose key is an earlier hop's
// provenance (earlierHop), keeping the order of the rest. h is the copy's
// own storage.
func dropEarlierHops(h *Headers) {
	out := h.pairs[:0]
	for _, p := range h.pairs {
		if !earlierHop(p.Key) {
			out = append(out, p)
		}
	}
	clearTail(h.pairs, len(out))
	h.pairs = out
}

// firstDelivery reports that the delivery is known to be the message's first:
// the broker counted it as attempt 1, or counts nothing and did not flag it
// as a redelivery. Attempt zero alone says only that the broker does not
// count, which a redelivery says too.
func firstDelivery(attempt int, redelivered bool) bool {
	return attempt == 1 || (attempt == 0 && !redelivered)
}

// maxFailureBytes is the longest HeaderFailure value the dead-letter route
// writes, the marker of a cut reason included. It is Pub/Sub's limit on an
// attribute value, the tightest any shipped driver has.
const maxFailureBytes = 1024

// failureCut ends a reason cut to maxFailureBytes.
const failureCut = "..."

// framableReason renders a handler's error text as something every broker can
// carry in a header value: every ASCII control character becomes a space, the
// leading and trailing spaces and tabs are trimmed, each run of bytes that is
// not valid UTF-8 becomes U+FFFD, and a reason still longer than
// maxFailureBytes is cut at a character boundary and ends in failureCut, the
// marker counted inside the bound.
//
// It exists because HeaderFailure is the one provenance header whose value is
// arbitrary (the other four are a topic, a subscription, a decimal and an
// RFC 3339 timestamp), and several brokers frame a header as a line of text. A
// NATS or JetStream header block is HTTP-shaped, so a carriage return in the
// reason ends the header early and a newline injects one; drivers/nats and
// drivers/jetstream refuse such a value before any I/O, which would leave
// the route refused on two of the very drivers repairs,
// for messages whose handler happened to report a multi-line error. A leading
// space or tab is refused for the same framing reason, so it is trimmed too.
// The bound and the UTF-8 rule are Pub/Sub's: the route's publisher publishes
// in MetadataNativeStrict, where drivers/gcppubsub refuses an attribute value
// over 1,024 bytes or one that is not valid UTF-8, so a longer handler error,
// or one carrying arbitrary bytes, failed the route there and the message was
// redelivered for ever. Every driver gets the same reason, so a dead-letter
// topic reads the same whichever broker it is on.
//
// The transformation is of the CORE's own diagnostic string and not of a
// caller's metadata, which is why the rule against dropping metadata
// silently does not govern it, and it is documented on HeaderFailure, so it
// is not silent either. Valid bytes at or above 0x80 are left alone: every
// byte of a multi-byte UTF-8 sequence is one of those, so a reason in any
// language survives intact.
//
// A reason that is already short, printable and valid UTF-8 is returned as a
// slice of s, so the usual dead-lettering pays no allocation for it.
//
// A reason that is nothing but control characters and spaces renders empty, and
// an empty HeaderFailure would say "the handler reported an empty error" rather
// than "the handler reported something unprintable"; deadLetterMessage leaves
// the header out instead, which is the same answer it gives for a nil cause.
func framableReason(s string) string {
	s = trimReason(s)
	if framed(s) {
		return s
	}
	b := make([]byte, 0, min(len(s), maxFailureBytes))
	invalid := false
	for i := 0; i < len(s); {
		c := s[i]
		if c < utf8.RuneSelf {
			if c < 0x20 || c == 0x7f {
				c = ' '
			}
			b = append(b, c)
			i++
			invalid = false
			continue
		}
		r, n := utf8.DecodeRuneInString(s[i:])
		switch {
		case r != utf8.RuneError || n > 1:
			b = append(b, s[i:i+n]...)
			invalid = false
		case !invalid:
			// One replacement for each run of invalid bytes, as
			// strings.ToValidUTF8 makes.
			b = utf8.AppendRune(b, utf8.RuneError)
			invalid = true
		}
		i += n
	}
	// A control character at either end became a space, so trim again.
	// Tabs are spaces by now, like every other control character.
	for len(b) > 0 && b[0] == ' ' {
		b = b[1:]
	}
	for len(b) > 0 && b[len(b)-1] == ' ' {
		b = b[:len(b)-1]
	}
	if len(b) > maxFailureBytes {
		// b is valid UTF-8 now, so the cut backs off to the start of the
		// rune it would split.
		n := maxFailureBytes - len(failureCut)
		for n > 0 && !utf8.RuneStart(b[n]) {
			n--
		}
		b = append(b[:n], failureCut...)
	}
	return string(b)
}

// trimReason trims the leading and trailing spaces and tabs.
func trimReason(s string) string {
	start, end := 0, len(s)
	for start < end && (s[start] == ' ' || s[start] == '\t') {
		start++
	}
	for end > start && (s[end-1] == ' ' || s[end-1] == '\t') {
		end--
	}
	return s[start:end]
}

// framed reports whether a trimmed reason needs nothing more: no control
// character, valid UTF-8 and within maxFailureBytes.
func framed(s string) bool {
	if len(s) > maxFailureBytes {
		return false
	}
	for i := range len(s) {
		if c := s[i]; c < 0x20 || c == 0x7f {
			return false
		}
	}
	return utf8.ValidString(s)
}

// dlqFailed records a dead-letter publication the runner could not confirm.
//
// There is nothing to undo — deadLetter has already requeued the original, so
// the message is redeliverable — and the rule's other half is this count:
// "a failed or ambiguous DLQ publish never acks (the message is requeued and
// COUNTED)". It is a counter of its own and not ConsumerStats.DeadLettered,
// which counts the native dead-lettering nacks a driver performed rather than
// this emulation's publications.
func (r *runner) dlqFailed() { r.c.counters.dlqFailures.Add(1) }
