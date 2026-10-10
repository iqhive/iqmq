# Poison messages

A poison message is one that will never succeed. The library's whole job here
is to stop it from also preventing the messages behind it from succeeding —
without destroying it, because a destroyed poison message is a bug report
nobody can answer.

## What happens by default

Five attempts with bounded exponential jittered backoff. Then, if a
dead-letter destination is configured, the message is dead-lettered; otherwise
the runner **halts** with a `*mq.PoisonError` carrying
`mq.CodePoisonMessage`, having first requeued the message.

The halt is the part worth defending. The three
alternatives are: skip it (silent data loss — forbidden as a default), retry it
for ever (the queue never drains and nobody is told), or auto-create a
dead-letter topic (a production topology change nobody asked for). Halting is
loud, loses nothing, and puts the decision where it belongs.

## Configuring it

```go
err := consumer.Run(ctx, handler,
    mq.WithMaxAttempts(5),
    mq.WithRetryBackoff(mq.ExponentialBackoff{
        Base:   200 * time.Millisecond,
        Max:    30 * time.Second,
        Jitter: mq.FullJitter,
    }),
    mq.WithDeadLetter("orders.dlq"),
    mq.WithFailurePolicy(mq.FailureDeadLetter))
```

| Policy | On exhaustion |
|---|---|
| `mq.FailureDeadLetter` | publish to the destination, confirm, then ack. The default **when a destination exists**. |
| `mq.FailureHalt` | `*mq.PoisonError`, requeue, stop. The default when none does. |
| `mq.FailureSkip` | ack a message nobody handled. Destructive; never a default; logged and counted. |
| `mq.FailureInfiniteRequeue` | give it back with the backoff, for ever |

There is no `FailureCustom` policy. The API exposes typed failure-policy
choices through `RunOption` rather than a custom policy closure.

## Short-circuiting the attempts

A handler that already knows the message is poison should not wait for five
attempts:

```go
if errors.Is(err, ErrSchemaUnknown) {
    return mq.DeadLetter(err)   // now, without exhausting the attempts
}
```

`mq.DeadLetter(err)` dead-letters immediately. With **no** dead-letter
destination it halts — dropping instead is the one answer the specification forbids, so the
directive cannot be used to discard by accident.

`mq.Discard(err)` is the deliberate discard: ack without success, logged and
counted, never silent. It is the handler's decision and never the runner's,
and it is the honest way to say "this message is garbage and I accept the
loss".

`mq.Halt(err)` stops the runner. It stops the **fetch first** and requeues
after — in that order, so the runner cannot hand itself the same message on its
way out.

## The bound that may never be reached

`mq.WithMaxAttempts(5)` bounds `Delivery.Attempt`, which is **the broker's**
count and is `0` on a broker that keeps none. Under the default
`mq.OrderingRelaxed` a message the runner gave back carries nothing else, so
on such a broker the exhaustion branch is never reached and the message is
retried for as long as it fails.

If your broker does not count attempts and you need the bound, use
`mq.OrderingStrict`: the runner holds the message in the process, so the count
is exact. The matrix says which drivers count; the driver pages state it under
*Attempt counting*.

## Consuming the dead-letter topic

It is a topic. Consume it like any other — and read the five provenance
headers, which are the reason the dead-letter topic is useful rather than a
graveyard:

`iqmq-origin-topic`, `iqmq-origin-subscription`, `iqmq-attempts`,
`iqmq-failure`, `iqmq-first-delivered-at`. The names are exported as
`mq.HeaderOriginTopic` and friends, deliberately: a name you have to copy out
of a specification is a name that goes wrong.

`iqmq-first-delivered-at` is present only when the dead-lettered delivery
*was* the first one. A missing header means "not known", because a redelivery
may reach a different process and nothing in the SPI carries a first-delivery
time back.

On `mq.WithNativeDeadLetter()` — the broker's own dead-letter route — there are
**no provenance headers at all**, because nothing on that path writes a
message. That is the trade: the native route has no publish-then-ack window and
no duplicate risk, and it also tells you nothing about why.

## Where this is limited today

`mq.WithDeadLetter` works on all twelve drivers. It did not until six of them refused **any** header under `mq.ReservedHeaderPrefix` on
publish, which is the rule about a *caller's* headers applied to the core's
own five, so the publication failed before any I/O, the runner requeued the
message and counted a `DeadLetterFailures`, and the message was redelivered for
ever.
