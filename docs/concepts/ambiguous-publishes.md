# Ambiguous publishes

An ambiguous publication is one whose outcome **nobody knows**: the connection
dropped after the bytes went out and before the confirmation came back, or the
context was cancelled while the broker was accepting. Specification the specification makes
this a first-class outcome rather than an error to be lumped in with the
others, and the specification also calls misclassifying it *a disqualifying correctness
defect* — the strongest thing the specification says about any bug.

## Detecting it

```go
if err := pub.Publish(ctx, msg); err != nil {
    switch {
    case mq.IsAmbiguous(err):
        // Nobody knows. See below.
    case mq.IsRetryable(err):
        // Definitely rejected, and the operation may be repeated.
    default:
        // Definitely rejected, and repeating it will not help.
    }
}
```

`mq.IsAmbiguous` and `mq.IsRetryable` are the two questions, and **ambiguity
wins**: `IsRetryable` answers "safe to repeat", and an unknown outcome never
is, because repeating it is what produces the duplicate. The specification attaches "may be
retryable" to the *definitely rejected* outcome and to nothing else, so an
error that is both is not retryable here.

The error can still carry both flags — a driver reports a connection that died
after the publication left the socket as `mq.CodeUnavailable`, `Retryable`
*and* `Ambiguous` — and the precedence between them is applied once, inside
`IsRetryable`, rather than by every caller. The switch above therefore reads
correctly in any order; it is written ambiguity-first because that is the case
you must handle, not because the order changes the answer.

The driver's own judgement is not lost, and a caller who can de-duplicate
needs it — the specification allows an ambiguous retry only when native idempotent
publication, a `DedupKey` inside the broker's dedup window, or an explicit
application decision makes it safe. Read the flag where it lives:

```go
var e *mq.OpError
if errors.As(err, &e) && e.Retryable {
    // The driver's transport judgement, ambiguity aside. Repeating this is
    // YOUR decision, and it duplicates unless your broker deduplicates it.
}
```

`middleware.Retry`'s default predicate, `Transient`, never retries anything
ambiguous whatever its code.

`IsAmbiguous` is true when the error's `Ambiguous` flag is set **or** its code
is `mq.CodeAmbiguous` or `mq.CodeSettlementUnknown`: those two codes mean an
unknown outcome, so an error carrying either is ambiguous whatever its flag
says.

A batch error is ambiguous when **any** of its items is: `IsAmbiguous` reads
every entry of an `*mq.BatchError` and every branch of an `errors.Join`, and a
batch none of which was accepted, but whose items were partly refused and
partly ambiguous, is reported per item rather than as its first error.

A batch is therefore not retryable when any of its items is ambiguous, even
when the first item in it was definitely refused: republishing such a batch
duplicates the item nobody knows the fate of.

Only an `*mq.OpError` answers `IsRetryable`, deliberately: retryability is a
property of the *operation* and not of the code. `mq.CodeUnavailable` on a
publish may be safe to repeat; the same code on an ack may mean the lease is
already somebody else's.

## What the library does about it: nothing

The core **must not** auto-retry an ambiguous publication. The three exceptions
The specification allows are all explicit:

1. native idempotent publication makes the retry safe within its *documented*
   scope — Kafka's producer idempotence, a `DedupKey` inside the broker's
   dedup window;
2. you enabled ambiguous retry yourself;
3. a retry middleware you installed distinguishes rejected from ambiguous and
   decides.

A library that silently retried would silently duplicate, and on a payment
path that is not a trade-off anybody should be making on your behalf in a
default.

## What you do about it

Three answers, in order of how much they cost you:

**Make the publication idempotent at the broker.** `mq.DedupKey("order-42")`
where the broker has publication dedup — `Nats-Msg-Id` on JetStream, SQS
FIFO's `MessageDeduplicationId`, Pulsar's dedup. Then a retry is safe inside
the window, and the window is the broker's and is documented on the driver
page. `ErrUnsupported` before any I/O where there is none.

**Make the consumer idempotent.** Then a duplicate costs nothing and you can
retry freely. This is the answer that works on every broker and it is the one
[the duplicates page](idempotency.md) argues for.

## The library's own ambiguous publication

The runner meets this problem itself, once, and its answer is worth knowing
because it is the same trade-off: when it dead-letters a message it publishes
to the dead-letter destination and waits for the confirmation, and if that
outcome is ambiguous it **requeues the original rather than acking it**. So the
message may be dead-lettered twice.

It chooses the duplicate over the loss, and counts the event —
`ConsumerStats.DeadLetterFailures` — so that an operator can see how often the
choice is being made. [Acks and leases](acks-and-leases.md) has the sequence.
