# Duplicates and idempotency

You will see the same message twice. This page is about where the duplicates
come from, which of them this library can remove, and what to do about the
rest.

## Where a duplicate comes from

| Source | Why | Can the library remove it? |
|---|---|---|
| the crash window between your handler returning and the ack landing | at-least-once, by construction | no — removing it would mean losing messages instead |
| a retried publication after an [ambiguous](ambiguous-publishes.md) outcome | nobody knows whether the first one landed | only where the broker has native idempotent publication, within its documented scope |
| a lease that expired while your handler was still running | the broker gave the message to somebody else | no, but `mq.WithLeaseExtension` narrows it — see [acks and leases](acks-and-leases.md) |
| a rebalance that revoked a partition mid-flight | the new owner starts from the last committed offset | no; `ConsumerStats.Redelivered` counts it |
| an ambiguous **dead-letter** publication | the runner requeues rather than acking, deliberately | no, and the direction is chosen: see below |
| a broker's own redelivery after a nack | you asked for it | no |

## The one the library chooses to create

When the runner dead-letters a message it publishes to the dead-letter
destination, waits for the confirmation, and only then acks the original. A
publication whose outcome is **ambiguous** therefore leaves the original
unacked — so the message comes back, is tried again, and may be dead-lettered
a second time, producing a duplicate in the dead-letter topic.

That is the deliberate direction, and it is worth understanding why: the specification
already makes a duplicate something a consumer has to handle, and a poison
message that was destroyed because its dead-letter publication could not be
confirmed is something nobody can recover. A duplicate in a dead-letter topic
is an inconvenience; a lost one is a bug report you cannot answer.

## What to do about the rest

**Make the handler idempotent.** This is the only answer that works on every
broker, and everything below is an optimisation of it. A handler is idempotent
when running it twice with the same message has the same effect as running it
once — usually because it writes with a natural key, or checks before it acts.

**Use the message's identity, not its content.** `Delivery.ID` is the broker's
own identifier where the broker has one. `Delivery.Attempt` is the broker's
delivery count, and is `0` on a broker that keeps none — a distinction that
matters more than it looks; see the ordering page.

**Deduplicate at the edge where the broker can.** `mq.DedupKey(s)` maps to
`Nats-Msg-Id`, SQS FIFO's `MessageDeduplicationId`, and the equivalents; it
fails with `ErrUnsupported` before any I/O where there is none. What it gives
you is *publication* dedup within the broker's window, not processing dedup.

**Keep a processed-message set when you need cross-broker dedup.**
`x/idempotency` provides memory and Redis stores for this purpose.

## What the library will never do

It will not retry an ambiguous publication behind your back. The specification forbids it
unless native idempotent publication makes the retry safe within its documented
scope, or you asked for it explicitly. A library that quietly retries an
ambiguous publish is a library that quietly duplicates payments.

It will not report an ambiguous outcome as rejected or accepted.
Misclassifying ambiguity is called out as a *disqualifying correctness
defect*, which is the strongest thing the specification says about any bug.
