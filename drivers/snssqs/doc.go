// Package snssqs is the iqmq driver for Amazon SNS fanning out into Amazon SQS
// queues.
//
// A TOPIC is an SNS topic. A SUBSCRIPTION is an SQS queue subscribed to that
// topic with RawMessageDelivery turned on. One publication reaches every
// subscribed queue, and each queue's consumers compete for their own copy.
// That is the whole model, and every difference from
// [bitbucket.org/iqhive/iqmq/drivers/sqs] follows from it.
//
// # Why it composes the SQS driver rather than reimplementing it
//
// Everything from the queue inward is `drivers/sqs`: this driver opens a whole
// SQS backend over the same credentials and wraps its sources. The long poll,
// the bounded ring, the local lease check, the supersession bookkeeping that
// makes a stale receipt handle detectable, the partial delete batch, the
// dead-letter ordering and the specification flush policy are that driver,
// already tested against a fake and against two emulators. A second copy of
// them here would be a second thing to get right.
//
// What this package is, then, is the publish side — an SNS topic ARN, the
// mapping, PublishBatch — plus the topology that makes a fan-out a fan-out,
// plus one refusal.
//
// # Semantics
//
//   - **Confirmation boundary.** [bitbucket.org/iqhive/iqmq.ConfirmAccepted] and
//     [bitbucket.org/iqhive/iqmq.ConfirmDurable] are the SAME boundary and
//     neither costs an extra round trip: the SNS Publish response means the
//     service has stored the message redundantly across availability zones.
//     ConfirmNone is accepted and is also that boundary — the SDK has no
//     fire-and-forget publication.
//
//     What the response does NOT mean is that a copy is in any queue. The
//     fan-out happens after it, so a consumer may see the message a moment
//     later, and a subscription created after the publication never sees it at
//     all. A publication to a topic with no subscriptions is accepted and
//     delivered nowhere; that is the service's behaviour and it is also what a
//     deployment that subscribes later wants.
//
//   - **Acknowledgement boundary.** Per queue. A delivery's lease is its
//     queue's visibility timeout; an Ack is a DeleteMessage against that
//     queue; a Nack is a ChangeMessageVisibility against that queue. Nothing
//     any subscriber does to its copy is visible to another subscriber.
//
//   - **At-least-once window.** Two windows, in series. SNS retries a delivery
//     it could not make, so a subscriber can see a duplicate of a publication
//     SNS thought had failed; and the queue redelivers a copy whose visibility
//     window passed before it was deleted. Neither has an upper bound a caller
//     can configure away. The answer applies unchanged: handlers are
//     idempotent, and `x/idempotency` is where the deduplication lives.
//
//   - **Settlement is per queue, and that is the trap.** One publication
//     becomes N messages with N receipt handles, N visibility windows and N
//     ApproximateReceiveCounts. A subscriber that acknowledges its copy has
//     settled its copy; the other N-1 are still in flight. A message
//     dead-lettered for one subscription is still being processed by the
//     others. There is no operation anywhere in this driver that settles "the
//     publication", and there must not be.
//
// # The one refusal
//
// A subscription without RawMessageDelivery is refused at
// [Backend.OpenSource], before a single delivery. Without raw delivery the
// body a consumer receives is SNS's JSON notification wrapped AROUND the
// published body —
// `{"Type":"Notification","MessageId":…,"Message":"<the real body>",…}` — with
// every iqmq message attribute moved inside it. A driver that accepted such a
// subscription would hand every application a body that is not the one that
// was published and a message with no headers, no key and no identifier, and
// there is no reliable way to tell the two shapes apart afterwards: a
// publisher whose payload IS an SNS-shaped JSON object would be unwrapped by
// any driver that tried. So the refusal names the attribute and the two ways
// to set it, and it happens once, at open.
//
// [Config.AutoCreate] makes subscriptions with the attribute set;
// [TypedAdmin.EnableRawDelivery] sets it on one that exists. This driver never
// flips it silently: a subscription somebody else made without raw delivery
// has consumers that expect the envelope.
//
// # What SNS takes away
//
// Two things a caller porting a publisher from `drivers/sqs` loses, and both
// are refusals rather than silences (the specification makes a silent drop a
// disqualifier):
//
//   - **no per-message delay.** An SQS queue has DelaySeconds; SNS has no
//     per-message timer of any kind, so [bitbucket.org/iqhive/iqmq.Delay] and
//     [bitbucket.org/iqhive/iqmq.DeliverAt] are refused with ErrUnsupported
//     before any I/O. A REDELIVERY delay is still available — it is a Nack
//     with one, against the queue.
//   - **a quarter of the message size.** SNS takes 256 KiB where a queue takes
//     a mebibyte, so a publication between the two is accepted by the queue
//     driver and refused by this one.
//
// # Maturity and tier
//
// Experimental, tier 1 (an official driver with a conformance report and a
// benchmark). The evidence is LocalStack, which is an emulator: the layer
// 4 — the managed service — needs an AWS account and is the known gap the plan
// records. Every claim in the generated matrix is therefore marked with the
// emulator symbol, and the filter-policy claim is CONDITIONAL and unverified
// in the positive direction, because the specification has no portable probe for a
// server-side filter (see docs/drivers/snssqs.md).
package snssqs
