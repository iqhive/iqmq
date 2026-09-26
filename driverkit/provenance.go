package driverkit

import mq "github.com/iqhive/iqmq"

// ProvenanceHeaderCount is how many keys [ReservedHeaderAllowed] admits.
//
// It is a constant so that a driver's own test can say "exactly these and no
// others" without restating the list, and so that a sixth provenance header
// added cannot be added to the core without every driver's admission
// test noticing.
const ProvenanceHeaderCount = 5

// ProvenanceHeaders returns the dead-letter provenance header keys, in the
// order the core's dead-letter route writes them.
//
// The array is returned by value and holds no allocation, so it is usable from
// a test, a doc example or a driver's own admission table.
func ProvenanceHeaders() [ProvenanceHeaderCount]string {
	return [ProvenanceHeaderCount]string{
		mq.HeaderOriginTopic,
		mq.HeaderOriginSubscription,
		mq.HeaderAttempts,
		mq.HeaderFailure,
		mq.HeaderFirstDeliveredAt,
	}
}

// ReservedHeaderAllowed reports whether key is one of the five dead-letter
// provenance headers — the keys iqmq's OWN dead-letter route writes — and
// therefore a key a driver must carry rather than refuse.
//
// # Why a driver needs this
//
// The specification reserves the "iqmq-" prefix and a driver refuses a CALLER's key under
// it, which is right: the prefix exists so that a caller cannot collide with
// the library. The dead-letter route is the library. It publishes a copy of
// the poison message stamped with `iqmq-origin-topic`,
// `iqmq-origin-subscription`, `iqmq-attempts`, `iqmq-failure` and
// `iqmq-first-delivered-at` through the ORDINARY publish path, so a driver that
// applies the caller's rule to the core's own provenance refuses the
// publication before any I/O — and because the runner requeues and counts a
// ConsumerStats.DeadLetterFailures rather than returning the error, the
// message is redelivered for ever with nothing in the caller's hands saying
// why. That is exactly what mq.WithDeadLetter did on six of the shipped
// drivers.
//
// So a driver's publish-admission predicate reads
//
//	if strings.HasPrefix(k, mq.ReservedHeaderPrefix) && !driverkit.ReservedHeaderAllowed(k) {
//		return errReservedHeader
//	}
//
// which costs nothing on a key that is not under the prefix, and a driver that
// carries a provenance header must also hand it BACK on the read path: the
// point of a dead-letter topic is that an operator reading it later can see
// where the message came from.
//
// # What it deliberately does not admit
//
// Exactly these five and nothing else. A driver's own reserved names — the
// ones it maps to native fields, and driverkit's own [EnvelopeMarker] — are
// framing rather than metadata and stay refused from a caller, because a
// caller that could set one could forge the driver's own decisions. Widening
// the predicate to "any reserved key this driver does not itself map" was
// rejected: it would let a caller invent a namespace under the library's
// prefix, which is what exists to prevent.
//
// The cost of admitting the five is real and is recorded rather than hidden: a
// caller can now publish them too, so a provenance header on a message is
// evidence about where the message says it came from and not proof that iqmq's
// dead-letter route put it there. Six of the shipped drivers — drivers/sqs,
// drivers/snssqs, drivers/gcppubsub, drivers/rabbitmq, drivers/nsq and the
// memory broker — already accepted them, so the widening makes the drivers
// AGREE rather than making one of them weaker.
func ReservedHeaderAllowed(key string) bool {
	switch key {
	case mq.HeaderOriginTopic,
		mq.HeaderOriginSubscription,
		mq.HeaderAttempts,
		mq.HeaderFailure,
		mq.HeaderFirstDeliveredAt:
		return true
	}
	return false
}
