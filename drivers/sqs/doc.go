// Package sqs is the iqmq driver for Amazon SQS, in both of its profiles:
// Standard (at-least-once, unordered, cheap) and FIFO (ordered within a
// message group, deduplicated, throughput-limited).
//
// The two are one module and two profiles because they are one API and two
// sets of guarantees. [Config.FIFO] selects the profile, and it is reported
// through DriverInfo.Profile — never hidden — because seven capability claims
// differ between them: ordering by key, deduplication, idempotent publication
// and scheduled publication are FIFO's, and a caller who read "sqs" and
// planned against the wrong profile's table would be planning against a
// different queue.
//
// # Semantics
//
// SQS is a queue with a visibility timeout. A message is stored redundantly
// before the send responds; a receive makes it invisible for the timeout and
// returns a RECEIPT HANDLE; a delete against that handle removes it; and a
// message nobody deletes becomes visible again when the timeout passes. That
// single mechanism decides almost everything below.
//
//   - **Confirmation boundary.** [github.com/iqhive/iqmq.ConfirmAccepted] and
//     [github.com/iqhive/iqmq.ConfirmDurable] are the SAME boundary and
//     neither costs an extra round trip: the SendMessage or SendMessageBatch
//     response means the service has stored the message redundantly across
//     availability zones. ConfirmNone is accepted and is also that boundary —
//     the SDK has no fire-and-forget send, so there is nothing weaker to
//     offer — and the receipt says which boundary the publication actually
//     reached.
//   - **Acknowledgement boundary.** An Ack is a DeleteMessage or a
//     DeleteMessageBatch, and it returns when the service has taken it. The
//     coalescing in between is [Config.Settle], Under the default
//     `immediate` policy an Ack is one request and returns when that request
//     does; under `count`, `time` or `hybrid` it joins a batch and returns
//     when the batch's request does, which is fewer requests and a later
//     return. Nothing is ever lost by the coalescing — a settlement that has
//     not been sent is one whose Ack has not returned — and Close sends
//     whatever is queued.
//   - **At-least-once window.** From the receive to the delete. A handler that
//     takes longer than the visibility timeout has its message delivered to
//     somebody else, and its own delete then deletes a message another
//     consumer is processing — which is why this driver refuses a settlement
//     whose window has passed BEFORE it calls the service (see "The local
//     lease check" below).
//
// # The local lease check
//
// The service will accept a DeleteMessage for a receipt handle whose message
// another consumer now holds. There is no API that asks "do I still hold
// this?", the delete does not fail, and the message is gone. So the only place
// the refusal can happen is in the driver, before the call: every settlement
// compares the delivery's visibility deadline — as this driver recorded it at
// the receive, on the injected clock — against now, and answers
// [github.com/iqhive/iqmq.ErrLeaseLost] when the window has passed or is
// within [Config.LeaseGuard] of passing.
//
// It is a LOCAL view and the driver says so. It cannot see a window another
// process shortened, and it deliberately refuses on a window this process
// believes has passed even when the service would still have accepted the
// delete. Both directions of that trade are the safe one: the cost is a
// redelivery, and the alternative is deleting somebody else's work.
//
// A second, sharper case is the same message received TWICE by one source —
// because its window expired, or because somebody made it visible again early.
// Both receipt handles are valid at the service. The source marks the earlier
// delivery superseded the moment the later one arrives, so a settlement for
// the earlier handle reports ErrLeaseLost rather than deleting the message the
// later delivery owns.
//
// # An extension is measured from the deadline
//
// [github.com/iqhive/iqmq.Delivery.Extend] moves the deadline "at least by
// further into the future", and the service's ChangeMessageVisibility measures
// its VisibilityTimeout from the moment of the CALL. Those are two different
// origins, so the timeout this driver sends is the REMAINING window plus by,
// and never by alone: sending by alone sets the window to by from now, which
// shortens the lease of any delivery with more than by left, makes the message
// visible to another consumer early and makes the next settlement report
// ErrLeaseLost — the caller having asked for more time.
//
// Two consequences a caller can see. `remaining + by` can pass the service's
// twelve hours where by alone does not, and no timeout grants that deadline:
// the extension is refused with CodeInvalidArgument rather than clamped to the
// ceiling and reported as a success. And the driver cannot tell the caller what
// the extension bought — DeliveryInfo is the snapshot the receive handed over
// and the SPI's Extend returns only an error — so a caller deciding whether it
// has time for more work has to assume the documented minimum, which on this
// driver is now true rather than optimistic.
//
// # An empty poll does not mean an empty queue
//
// On the FIFO profile, SQS delivers nothing else from a message group while one
// of its messages is in flight. That is what makes the order hold, and it means
// a receive on a queue whose every group is blocked returns no messages, no
// error and no code — indistinguishable, through the portable API, from a queue
// that is empty. Reading an empty poll as an empty queue is the mistake this
// paragraph exists to stop.
//
// What distinguishes them is [github.com/iqhive/iqmq.Admin.Stats], and it
// works because a blocked group always has a message IN FLIGHT for
// QueueStats.InFlight to report: depth without in-flight messages means a
// message is available, and depth with them means somebody is holding it. That
// invariant is pinned by TestABlockedFifoGroupIsVisibleInTheQueueStats, because
// it is exactly what the in-process server used to break — a group held by a
// message that had become VISIBLE again reported depth 1 with nothing in flight
// and delivered nothing at all.
//
// # Ten attributes, and the envelope
//
// A message carries at most TEN message attributes, and this driver spends
// some of them: the key, the identifier, the body-encoding marker and the six
// typed properties each take one when they are set. The question
// for this driver is what happens to the eleventh, and the answer is the
// metadata mode:
//
//   - MetadataNativeStrict, the default, refuses it before any I/O.
//   - MetadataAdaptive encodes the whole message into the body as an iqmq
//     envelope, which is also what a repeated header key and a header key
//     that is not a legal attribute name take.
//   - MetadataPortableEnvelope always envelopes.
//   - MetadataLenientDrop drops headers from the end of the list, with a
//     metric.
//
// # The binary-body policy
//
// An SQS body is TEXT: the service accepts only the XML character set, so an
// arbitrary byte string has no native representation. [Config.Body] is the
// policy, with all three answers shipped: `auto` (the default) passes text
// through and base64-encodes what it must, marking the message; `text`
// refuses a non-text body before any I/O; `base64` encodes everything. A nil
// body and an empty body are both carried and are distinguished, because the
// service refuses an empty MessageBody outright.
//
// # Maturity and tier
//
// Tier 1 (core, tested against pinned ElasticMQ and LocalStack containers in
// `make integration`). Numbers taken against either emulator describe the
// client and the container and are never evidence for the managed service
// (layer 4).
//
// # Opening
//
//	client, err := sqs.Open(ctx, sqs.Config{
//	    Region:          "eu-west-1",
//	    AccessKeyID:     "AKIAEXAMPLE",
//	    SecretAccessKey: "…",
//	})
//
// or from a DSN, once the driver is registered with a
// [github.com/iqhive/iqmq/dsn.Registry]:
//
//	sqs://AKIAEXAMPLE:secret@aws?region=eu-west-1&settle=hybrid
//	sqs://x:x@127.0.0.1:9324/dev-?region=elasticmq&insecuretransport=true
//
// [OpenWithClient] takes an *sqs.Client the caller already has, with explicit
// [github.com/iqhive/iqmq/driverkit.Ownership]. It is also the way in for the
// SDK's default credential chain, which lives in a module this driver
// deliberately does not depend on.
//
// # Topics, queues and subscriptions
//
// A topic maps to a queue NAME by [QueueName] — "/" becomes "-", the prefix is
// prepended, ".fifo" is appended on the FIFO profile — and the name maps to a
// URL by one GetQueueUrl call whose answer is cached. The mapping is not
// injective ("orders/eu" and "orders-eu" are one queue), which
// [Config.Resolver] and [Config.QueueURLs] exist to override.
//
// SourceConfig.Subscription is IGNORED. A queue has exactly one set of
// messages, so every consumer of one topic competes for them whatever the
// subscription says; two independent subscriptions need
// github.com/iqhive/iqmq/drivers/snssqs, where the topic is an SNS topic and
// the subscription is a queue.
//
// # Documentation
//
// docs/drivers/sqs.md carries the fixed per-driver sections the specification requires,
// including the ordering statement delegates to each driver page, the
// full table of what maps to what, and the service limits this driver read out
// of the pinned SDK.
package sqs
