package iqmq

import (
	"errors"
	"strconv"
	"time"
)

// The provenance headers a dead-lettered message carries.
//
// They are exported because the consumer of a dead-letter topic reads them,
// and a name a caller has to copy out of a specification is a name that goes
// wrong. Each uses ReservedHeaderPrefix, which the specification reserves for iqmq's own
// use; the runner is iqmq, and it is the only writer of them in this package.
//
// # A driver has to accept these five, and six shipped drivers do not
//
// the reserved-prefix rule is about a CALLER's headers. These are the
// core's, so a driver's own Representable predicate has to let them through:
// the dead-letter route publishes the stamped copy through the ordinary
// publish path, so a driver that refuses any key under ReservedHeaderPrefix
// refuses iqmq's own provenance and mq.WithDeadLetter cannot work on it at
// all. `drivers/kafka`, `drivers/kafka/kafkago`, `drivers/nats`,
// `drivers/jetstream`, `drivers/amqp10` and `drivers/pulsar` refuse them
// today: the publication fails, the runner requeues the message and counts a
// DeadLetterFailures, and the message is redelivered for ever. Those six
// drivers' callers use WithNativeDeadLetter instead, and the refusal is pinned
// by a test in examples/kafka-orders that fails the day it is fixed.
//
// The fix is to exempt exactly these five keys and no others, in driverkit's
// shared predicate and in the six drivers that carry their own. It widens what
// a driver accepts, so it breaks no caller; it is still a change to every
// driver's publish path and needs each driver's conformance suite re-run
// against its broker, which is why the specification recorded it rather than made it.
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
	// The specification lists it as.
	HeaderAttempts = ReservedHeaderPrefix + "attempts"
	// HeaderFailure is the error the handler reported, rendered by its own
	// Error method.
	//
	// It is the one provenance header whose value is arbitrary, and several
	// brokers frame a header as a line of text — a NATS or JetStream header
	// block is HTTP-shaped — so every ASCII control character in the reason
	// is rendered as a space and the result is trimmed of leading and
	// trailing spaces and tabs. Without that a handler reporting a
	// multi-line error would have its dead-letter publication refused
	// before any I/O on two of the drivers the route has to work on.
	// Bytes at or above 0x80 are untouched, so a
	// reason in any language arrives intact; a reason with nothing
	// printable in it leaves the header out rather than setting it empty.
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
	// holding this attempt's receive time would say something false.
	HeaderFirstDeliveredAt = ReservedHeaderPrefix + "first-delivered-at"
)

// deadLetter routes one message to the dead-letter topic (ADR-011).
//
// The order is the whole point and is the single worst thing in this
// increment to get wrong: publish to the dead-letter destination, confirm it,
// and only THEN settle the original. A runner that acked first, or that acked
// after a publish it could not confirm, would destroy the message in exactly
// the case the dead-letter topic exists for.
//
// So a publish that fails for any reason — a refusal, a cancelled context, or
// an ambiguous outcome the driver cannot resolve — leaves the original
// unacked and requeues it, which is ADR-011's "a failed or ambiguous DLQ
// publish never acks (the message is requeued and counted)". The message is
// then redelivered and tried again; a dead-letter destination that stays
// broken keeps the message alive rather than losing it.
//
// An ambiguous publish may therefore produce a duplicate in the dead-letter
// topic. That is the deliberate direction: the at-least-once boundary
// makes a duplicate a thing a consumer must already handle, and a lost poison
// message is a thing nobody can recover.
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
	if err := r.dlq.Publish(r.dlqCtx, m); err != nil {
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
// route (Nack(NackDeadLetter()), ADR-011).
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

// deadLetterRefused gives back a message whose dead-lettering failed, after
// the backoff its next attempt would wait, and counts the failure.
func (r *runner) deadLetterRefused(it *runItem, d *Delivery) {
	r.requeue(d, r.cfg.backoff.Delay(attemptOf(it, d)+1))
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
func (r *runner) deadLetterMessage(it *runItem, d *Delivery, cause error) *Message {
	m := d.CloneMessage()
	m.Topic = r.cfg.deadLetter
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
	if cause != nil {
		if reason := framableReason(cause.Error()); reason != "" {
			m.Headers.Set(HeaderFailure, []byte(reason))
		}
	}
	if firstDelivery(d.Attempt, info.Redelivered) && !info.ReceivedAt.IsZero() {
		m.Headers.Set(HeaderFirstDeliveredAt, []byte(info.ReceivedAt.Format(time.RFC3339Nano)))
	}
	return m
}

// firstDelivery reports that the delivery is known to be the message's first:
// the broker counted it as attempt 1, or counts nothing and did not flag it
// as a redelivery. Attempt zero alone says only that the broker does not
// count, which a redelivery says too.
func firstDelivery(attempt int, redelivered bool) bool {
	return attempt == 1 || (attempt == 0 && !redelivered)
}

// framableReason renders a handler's error text as something every broker can
// carry in a header value: every ASCII control character becomes a space, and
// the leading and trailing spaces and tabs are trimmed.
//
// It exists because HeaderFailure is the one provenance header whose value is
// arbitrary — the other four are a topic, a subscription, a decimal and an
// RFC 3339 timestamp — and several brokers frame a header as a line of text. A
// NATS or JetStream header block is HTTP-shaped, so a carriage return in the
// reason ends the header early and a newline injects one; drivers/nats and
// drivers/jetstream refuse such a value before any I/O, which would leave
// the route refused on two of the very drivers repairs,
// for messages whose handler happened to report a multi-line error. A leading
// space or tab is refused for the same framing reason, so it is trimmed too.
//
// The transformation is of the CORE's own diagnostic string and not of a
// caller's metadata, which is why the rule against dropping metadata
// silently does not govern it — and it is documented on HeaderFailure, so it
// is not silent either. Bytes at or above 0x80 are left alone: every byte of a
// multi-byte UTF-8 sequence is one of those, so a reason in any language
// survives intact.
//
// A reason that is nothing but control characters and spaces renders empty, and
// an empty HeaderFailure would say "the handler reported an empty error" rather
// than "the handler reported something unprintable"; deadLetterMessage leaves
// the header out instead, which is the same answer it gives for a nil cause.
func framableReason(s string) string {
	start, end := 0, len(s)
	for start < end && (s[start] == ' ' || s[start] == '\t') {
		start++
	}
	for end > start && (s[end-1] == ' ' || s[end-1] == '\t') {
		end--
	}
	var b []byte
	for i := start; i < end; i++ {
		c := s[i]
		if c >= 0x20 && c != 0x7f {
			if b != nil {
				b = append(b, c)
			}
			continue
		}
		if b == nil {
			b = make([]byte, 0, end-start)
			b = append(b, s[start:i]...)
		}
		b = append(b, ' ')
	}
	if b == nil {
		return s[start:end]
	}
	// A control character at either end became a space, so trim again.
	for len(b) > 0 && (b[0] == ' ' || b[0] == '\t') {
		b = b[1:]
	}
	for len(b) > 0 && (b[len(b)-1] == ' ' || b[len(b)-1] == '\t') {
		b = b[:len(b)-1]
	}
	return string(b)
}

// dlqFailed records a dead-letter publication the runner could not confirm.
//
// There is nothing to undo — deadLetter has already requeued the original, so
// the message is redeliverable — and ADR-011's other half is this count:
// "a failed or ambiguous DLQ publish never acks (the message is requeued and
// COUNTED)". It is a counter of its own and not ConsumerStats.DeadLettered,
// which counts the native dead-lettering nacks a driver performed rather than
// this emulation's publications.
func (r *runner) dlqFailed() { r.c.counters.dlqFailures.Add(1) }
