# Acks and leases

What happens between "your handler received this" and "the broker will not send
it again".

![The settlement sequence, and what a failed delivery becomes](../assets/settlement-sequence.svg)

That diagram is the runner's actual control flow — `run.go`'s `settle`, `fail`,
`retry`, `exhaust` and `deadLetter` — rather than the obvious version, and four
of its details are worth reading twice.

## A lease is a deadline, not a lock

When a broker hands you a message it usually hands you a deadline with it:
visibility timeout (SQS), ack deadline (Pub/Sub, JetStream), in-flight timeout
(NSQ). Past that instant the broker considers the delivery abandoned and gives
the message to somebody else — while your handler is still running.

`Delivery.LeaseExpires` is when that happens, where the driver knows.
`Delivery.Extend(by)` pushes it out. `mq.WithLeaseExtension(d)` asks the runner
to do it for you while a handler is slow, and `mq.WithLeaseDeadline()` narrows
the handler's own context to the lease so that a handler cannot keep working
past the point at which its work no longer counts.

`WithLeaseExtension` takes a *lead* — how long before the deadline to ask —
and there is one scheduler and one timer for the whole runner, never a timer
per delivery. What it asks the broker for is the lease's own length, and the
lead decides when: an extension is requested a lead before the deadline it is
renewing.

A lead longer than the lease cannot be honoured, and the runner extends more
often instead of asking for more time. Nothing can buy 30 seconds of notice
out of a 10-second visibility timeout, and asking the broker for 30 seconds
would be refused outright on the three drivers that cap an extension
(`drivers/sqs`, `drivers/gcppubsub`, `drivers/nsq`) — so the runner keeps the
delivery alive on shorter notice rather than losing it altogether. The floor
is half the lease: the deadline is never less than half a lease away, and the
lead is honoured exactly whenever it is under that.

A refused extension is not retried. The lease is either gone — in which case
the broker has the message and the settlement that follows says so — or the
driver refused, and asking again on the same interval turns one broken
delivery into a busy loop. Your handler is not cancelled either: it finds out
at its own settlement, and the `OnSettle` hook fires for the failed extension
with kind `SettleExtend` if you want to know sooner.

Two honest limits. A driver with no `DeadlineExtender` cannot extend at all,
and `Extend` returns `ErrUnsupported` — the matrix's `ExtendLease` row says
which. And extension is a race you can lose: the broker may have already
redelivered, in which case the settlement that follows returns `ErrLeaseLost`
rather than quietly settling somebody else's delivery.

## The generation token is why a stale handle is safe

Every delivery carries a generation. A `*Delivery` you captured in a closure
and settle after the message has been redelivered settles *nothing* and returns
`ErrLeaseLost` — it cannot reach the new delivery, even though the runner has
pooled and reused the struct underneath. This is the invariant that makes
buffer reuse safe to have at all.

## The dead-letter order is the whole point

Publish to the dead-letter destination, **confirm it**, and only then ack the
original. A publication that is refused or ambiguous leaves the original
unacked and requeues it, and increments a counter of its own.

A runner that acked first would destroy the message in exactly the case the
dead-letter topic exists for. A runner that acked after a publication it could
not *confirm* would do the same, less often and less reproducibly.

The five provenance headers a dead-lettered message carries:

| Header | Content |
|---|---|
| `iqmq-origin-topic` | the topic it was consumed from |
| `iqmq-origin-subscription` | the subscription that consumed it |
| `iqmq-attempts` | the failed attempt, in decimal, as the runner counted it: the broker's count (1 where the broker keeps none) plus the retries the runner held in process, so it is always present |
| `iqmq-failure` | the error the handler reported, rendered by its own `Error` |
| `iqmq-first-delivered-at` | when it was **first** delivered — written only when this delivery *is* the first |

The last row is a deliberate absence. A redelivery may reach a different
process entirely and nothing in the SPI carries a first-delivery time back, so
a missing header says "not known" and a header holding this attempt's receive
time would say something false.

On a driver whose broker moves the message itself (`mq.WithNativeDeadLetter()`
over `d.Nack(mq.NackDeadLetter())`), there is no publication, no confirmation
window and **no provenance headers**, because nothing on that path writes a
message. What the destination receives is what the broker put there.

## Who settles

`mq.WithSettlement(mq.SettlementManual)` hands settlement to your handler
entirely: the runner dispatches and then leaves the delivery alone, whatever
the handler returns. Under the default `mq.SettlementAuto` the handler may
still settle for itself, and is not overruled — the runner checks, and every
settlement it performs tolerates `ErrAlreadySettled`, because the handler may
settle on another goroutine and win the race with that check.

Middleware wrapped around your handler can see what the handler did.
`d.Settled()` is true after any successful settlement, and `d.Acked()` only
after an Ack. A handler that nacks or rejects its own delivery and returns nil
has asked for the message again, so a middleware that records the work as done
(an idempotency key, an inbox entry) checks `Acked`, not `Settled`. Under
`BufferReuse` both answer for your delivery for as long as the runner holds
it, which covers the handler, its middleware and the runner's own settlement.

## Where the specification is not met yet
