# Confirmation boundaries

When `Publish` returns `nil`, what has actually happened? Specification.

The answer is *whatever `mq.ConfirmationMode` you asked for, on the broker you
are talking to* — and the second half is why every driver page has to state its
own boundary in words.

## The four modes

| Mode | Boundary | Fails at construction when |
|---|---|---|
| `mq.ConfirmDefault` | resolves to `ConfirmAccepted` | never |
| `mq.ConfirmNone` | the native client admitted it locally. May be lossy. | never |
| `mq.ConfirmAccepted` | the strongest ordinary acknowledgement the profile offers **without extra round trips** | never |
| `mq.ConfirmDurable` | the broker has it durably | the profile cannot define a meaningful durable boundary |

`ConfirmDurable` failing at construction is the important row. On NATS Core
and on the `memory` engine there is no durable boundary that could be honoured
— NATS Core's publish confirmation is a flush, and the memory engine is never
persistent — so asking for one is a config error rather than a promise nobody
keeps. That is the whole design in one behaviour: the library would rather not
compile your intent than pretend to satisfy it.

## What `ConfirmAccepted` means, per broker

These are the boundaries the driver pages state. They are not equivalent, and
the differences are exactly the differences in your loss window:

| Driver | `ConfirmAccepted` is |
|---|---|
| kafka | `acks=all` with idempotence |
| jetstream | the `PubAck` |
| nats (core) | a flush — **never durable** |
| rabbitmq | a publisher confirm |
| sqs / snssqs | the API response |
| gcppubsub | the API response |
| pulsar | the send callback |
| nsq | the `OK` |
| redis | the `XADD` reply, with no `WAIT` |
| memory | the record is visible as ready or delayed |

A flush and a `PubAck` are both "accepted", and one of them survives a broker
restart. This is why a portable API cannot have one word for it and why
`ConfirmNone`, `ConfirmAccepted` and `ConfirmDurable` are three modes rather
than a boolean.

## What `nil` never means

It never means the message was consumed, and it never means a side effect
happened. The specification says so in one line and it is worth repeating: a publication's
outcome is about the broker's custody of bytes, not about anything downstream.

## The three outcomes

Every publish resolves to exactly one of three states, and the API keeps them
distinguishable:

* **accepted** — `nil`. The configured boundary was reached.
* **definitely rejected** — an error whose outcome is rejected. It may be
  retryable; `mq.IsRetryable(err)` reads the operation's own judgement, and
  only an `*mq.OpError` answers, because retryability is operation-specific
  rather than a property of a code. `IsRetryable` means **safe to repeat**, so
  ambiguity wins: a driver reports a connection that died after the write as
  `Retryable` *and* `Ambiguous`, and `IsRetryable` answers false for it. The
  driver's flag is still on the error (`errors.As` to `*mq.OpError`) for a
  caller who can de-duplicate and decides to repeat it deliberately.
* **ambiguous** — `mq.IsAmbiguous(err)` is true. Nobody knows. See
  [ambiguous publishes](ambiguous-publishes.md).

There is **no hidden retry** between those. Native SDK retries may stay on
where they are part of documented confirmation behaviour, their ambiguity
implications are documented, and they are configurable — but nothing in this
library retries a publication you did not ask it to retry.

## Batches are not atomic

Not unless the driver advertises `FeatureAtomicBatchPublish`, which today no
driver in the matrix does — the row is `?` everywhere, meaning nothing
established it either way.

A partially failed batch returns an `*mq.BatchError` whose `Errs` is
**index-aligned with the input**, so item 3's error is `Errs[3]`. That
alignment is the whole contract: a batch error that told you "four failed"
without saying which four would be unusable. `BatchError` carries
`mq.CodePartial`, so `errors.Is(err, mq.ErrPartial)` is true, and `Unwrap`
returns the entries so `errors.Is` and `errors.As` reach a single item's cause.

A batch **none** of which was accepted returns the single error when every
item has the same outcome: all ambiguous, all refused with the same code and
retryability, or stopped at the first item by a closed sink, a cancelled
context or a deadline. So a whole call refused for one reason reads like a
single publication's refusal. When the items' outcomes differ (some refused
and some that may already be in the broker, or refusals with different codes
or retryability) it returns the index-aligned `*mq.BatchError` instead,
because no one error can say both. A batch error therefore does not mean that
anything was accepted. `mq.IsAmbiguous(err)` is true of a batch error when
**any** item is ambiguous, so check it before republishing anything, and then
republish only the items whose own error is a definite refusal that
`mq.IsRetryable` allows. `middleware.Retry`'s default predicate does not count
`mq.CodePartial` as transient, so it does not repeat a handler that returns a
batch error.

When an error the sink cannot continue past — a closed sink, a cancelled
context, a deadline — stops a batch part-way, the items after it were never
offered to the driver. Their errors carry that same code, so
`errors.Is(err, context.DeadlineExceeded)` still holds, but they are
**definite**: they are not in the broker, even when the error that stopped the
batch was itself ambiguous.

Large batches are split into bounded native sub-batches. Splitting is not
atomicity and the library does not present it as such.

On a client that keeps order per key (`FeatureOrderedByKey`), the split holds a
key back once one of its messages has failed. Its messages in the later pieces
are not sent, and each one's error is definite, retryable and says it was
never attempted, so you can retry the failures in order and the key stays in
order. Keyless messages count as one key, because an SQS FIFO queue puts them
in one group. Sent, a later message would have reached the broker ahead of
your retry of the earlier one. One native call to a broker that stops a group
at its first failure, such as an SQS FIFO queue, behaves the same way, and the
split keeps that behaviour across its pieces. `PublishItems` and a `Batching`
flush also send items with different option sets as separate runs, and the
hold crosses those runs too, so giving every message its own `DedupKey` does
not let a later message of a failed key through.

## Receipts

`mq.PublisherConfig.Receipts` asks the driver to fill `Message.Receipt` with
what the broker returned — partition and offset on Kafka, sequence on
JetStream. It is a capability (`FeatureReceipts`) and it costs something on
some drivers, which is why it is opt-in rather than always populated.
