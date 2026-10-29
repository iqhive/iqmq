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

## A refusal is not an attempt

A middleware that turns a delivery away before the handler runs has not
tried the message, so it must not spend one of its attempts.
`mq.Postpone(err, d)` says so: the runner offers the message again after `d`
(or after the configured backoff when `d` is zero) and never exhausts it,
dead-letters it or skips it for that. `middleware.RateLimit`,
`middleware.CircuitBreaker`, `middleware.Dedup` (for a duplicate whose first
copy is still in the handler) and `x/idempotency` (for a key another worker
holds an open claim on) all postpone. When they used `mq.RetryAfter` or a plain
error, a backlog behind a rate limit used up `WithMaxAttempts` on messages
nobody had handled, and the runner halted with a `*mq.PoisonError`.

A postponement only means something when no handler ran. When
`middleware.Retry` sits in front of a rate limit or a breaker and a later
attempt is turned away, an earlier attempt has already run the handler and
failed, so `Retry` returns that failure, not the postponement. The runner
counts it, and a breaker outside the `Retry` sees it. Passed on, the
postponement kept a message that failed every time from ever being exhausted. `Dedup` with `KeepOnFailure` forgets the key
of a postponed delivery for the same reason, so the next offer is handled
rather than acked as a duplicate.

The runner holds a postponed delivery on its worker and calls the chain again
after the wait, because most brokers count every redelivery: a message given
back to the broker five times behind a rate limit would meet
`WithMaxAttempts(5)` on its first real failure. While it is held the broker
never hears of it, and the count stays exact. Under `mq.OrderingStrict` and
`mq.OrderingStopOnFailure` it is always held, so nothing overtakes it. Under
the default `mq.OrderingRelaxed` it is held while the lease allows: when you
run with `mq.WithLeaseExtension`, when the driver reports no lease, or when
the wait takes at most half of what is left of it. A held delivery keeps its
in-flight slot and its worker, so the deliveries queued behind it on its lane
wait too, as they would behind a slow handler.

An ordered run with `mq.WithLeaseDeadline` (and neither
`mq.WithLeaseExtension` nor `mq.WithHandlerTimeout`) ends the handler's
context with the lease, so it holds a postponed delivery only while the lease
allows, as a relaxed run does. Held past the lease, the handler called next
was given a context that had already ended, and every attempt after the wait
failed and was counted.

A longer wait, and every postponement from `RunBatch`, goes back to the broker
with the delay (at once, on a driver without delayed redelivery), and the
deliveries behind it on its lane may overtake it. A broker that counts
deliveries counts that one, so `Delivery.Attempt`, and the bound a later
failure meets, includes it. `mq.WithLeaseExtension` keeps the long waits of a
circuit breaker's cooldown out of the bound as well.

`mq.SettlementManual` honours a postponement too. Your handler owns what it
is given, and a postponed delivery was never given to it, so the runner holds
it or gives it back by the same rules. It used to leave it in flight until its
lease ran out, or for ever on a driver with no lease.

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

The copy is the message **as the broker delivered it**, plus those headers, and
not what the middleware chain made of it. A message that `x/encrypt` decrypted
for the handler is dead-lettered sealed, one that `x/compress` or
`middleware.Compress` decompressed is dead-lettered compressed, and a
claim-checked one is dead-lettered as its reference. A strictly ordered retry
starts from the same received message, so a decoder in the chain decodes it
again rather than being handed its own output, and each attempt of
`middleware.Retry` starts from the message it was given. A middleware gets this for
free as long as it **replaces** what it rewrites (`d.Body = plain`) rather than
writing into the received storage, which is borrowed and read-only.

When the dead-letter publication fails, the original is not acked: it goes
back to the broker with the retry backoff and the failure is counted in
`ConsumerStats.DeadLetterFailures`. A destination that keeps refusing the
copy therefore costs one handler call per backoff interval rather than a
tight loop, on a driver that can delay a redelivery; one that cannot hands
the message straight back.

## Where this is limited today

`mq.WithDeadLetter` works on all twelve drivers. It did not until six of them refused **any** header under `mq.ReservedHeaderPrefix` on
publish, which is the rule about a *caller's* headers applied to the core's
own five, so the publication failed before any I/O, the runner requeued the
message and counted a `DeadLetterFailures`, and the message was redelivered for
ever.
