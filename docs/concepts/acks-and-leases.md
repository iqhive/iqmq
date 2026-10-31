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

An extension does not move `d.Info().LeaseExpires`, which stays the lease the
driver granted. `d.LeaseKept()` reports that the runner is extending the lease
of the delivery your handler holds, so code that measures a wait against
`LeaseExpires` knows the measurement no longer applies. `middleware.Retry`
asks it before and after every wait.

A refused extension is not retried. The lease is either gone — in which case
the broker has the message and the settlement that follows says so — or the
driver refused, and asking again on the same interval turns one broken
delivery into a busy loop. Your handler is not cancelled either. A lost lease
shows at its own settlement, and the `OnSettle` hook fires for the failed
extension with kind `SettleExtend` if you want to know sooner. `d.LeaseKept()`
turns false at the refusal, so `middleware.Retry` and any code that asks it
measure their waits against `LeaseExpires` again. It turns false as well once
the runner lets the delivery go, for a delivery your handler keeps under
`mq.SettlementManual`.

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
unacked and requeues it with the retry backoff, and increments a counter of
its own.

A runner that acked first would destroy the message in exactly the case the
dead-letter topic exists for. A runner that acked after a publication it could
not *confirm* would do the same, less often and less reproducibly.

What is published is the message as the broker delivered it, plus the headers
below. A middleware that decrypted, decompressed or checked out the body for
your handler does not change the copy: it stays sealed, compressed or a
reference, as it was on the original topic.

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
entirely: the runner dispatches and then settles nothing your handler was
given, whatever it returns. Your handler may keep the `*mq.Delivery` and
settle it after it returns, from another goroutine if you like. The pointer
is your handler's own, which costs one allocation per delivery in this mode,
and it stays valid until you settle it. The runner frees the delivery's
in-flight slot as your handler returns, so `mq.WithMaxInFlight` bounds the
deliveries in handlers, not the ones you keep. A middleware that records a
delivery as handled (`middleware.Dedup`, `x/idempotency`, `x/inbox`) decides
as your handler returns and cannot see a later settlement: a kept delivery
your handler returned nil for is recorded, so a Nack after that, or a
redelivery after its lease runs out unsettled, is acked as a duplicate. Settle a failure before returning, or keep
those middlewares off a handler that keeps its deliveries.

The runner still acts on what is not a settlement. A delivery a middleware
turned away before the handler ran (`mq.Postpone`) never reached your
handler, so the runner holds it or gives it back exactly as it does under
`SettlementAuto`. A handler that panics before it settles its delivery leaves
nobody to settle it, so the delivery takes the failure policy, as a panic
does under `SettlementAuto`. `mq.Halt` and a failure under
`mq.OrderingStopOnFailure` (a panic included) stop the runner, whether or not
your handler settled the delivery first, and the delivery stays yours.

Under the default `mq.SettlementAuto` the handler may
still settle for itself, and is not overruled — the runner checks, and every
settlement it performs tolerates `ErrAlreadySettled`, because the handler may
settle on another goroutine and win the race with that check. Stopping the
run is not a settlement, so `mq.Halt`, or a failure under
`mq.OrderingStopOnFailure`, stops the runner after the handler settled its
delivery as well.

Middleware wrapped around your handler can see what the handler did.
`d.Settled()` is true after any successful settlement, and `d.Acked()` only
after an Ack. A handler that nacks or rejects its own delivery and returns nil
has asked for the message again, so a middleware that records the work as done
(an idempotency key, an inbox entry) checks `Acked`, not `Settled`. Under
`BufferReuse` both answer for your delivery for as long as the runner holds
it, which covers the handler, its middleware and the runner's own settlement.

## Where the specification is not met yet
