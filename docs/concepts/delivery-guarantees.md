# Delivery guarantees

Specification. Four guarantees are recognised, and one of the four is not
something this library provides.

| Guarantee | What it means here | Who provides it |
|---|---|---|
| **At-most-once** | the message may be lost and will not be delivered twice | a driver that acknowledges before the handler runs, or a broker with no settlement at all (NATS Core) |
| **At-least-once** | the message is delivered until it is settled; a crash between handler and ack produces a duplicate | the default, on every driver that has explicit acknowledgement |
| **Broker-scoped exactly-once *delivery*** | the broker suppresses a duplicate within a documented scope and window | Pub/Sub exactly-once subscriptions, Kafka transactions with `read_committed`, Pulsar transactions, JetStream dedup |
| **Exactly-once *processing*** | your side effects happen once | **not provided.** `x/idempotency` and `x/inbox` are helpers for building it |

The third row is the one that gets misread, so the specification states it and so does this
page: **a broker dedup or transaction feature does not make an arbitrary side
effect exactly-once.** A payment charged inside a Kafka transaction is still
charged twice if the process dies after the charge and before the commit. What
the broker deduplicates is its own record, within its own window, on its own
key.

## What an ack means

An ack means two things at once, and no more:

1. your handler completed — it returned `nil`, or called `Ack` itself; and
2. the driver accepted the strongest configured native settlement action.

What *that* means is broker-specific, and every driver page states which of
these it is: recorded in the local client, written to the connection, confirmed
by the broker, or included in a broker exactly-once acknowledgement mode. They
are four different durability stories, and the difference is your duplicate
rate after a crash.

Under the at-least-once default the ack happens **after** the handler returns
`nil`. The window between those two events is real and cannot be closed by any
API: a process that dies in it produces a duplicate. That is the cost of not
losing the message, and it is why [the duplicates page](idempotency.md) is not
optional reading.

## Settlement is a state machine, not a boolean

```
active ──► settling ──► settled
              ├─► active              (definite rejection; retry allowed)
              └─► settlement-unknown  (ambiguous native result)
```

The rules the core enforces, all of them observable through errors rather than
through logs:

* exactly one concurrent terminal attempt wins. The loser gets
  `ErrSettlementInProgress` while the winner is in flight.
* a second terminal action after a success returns `ErrAlreadySettled` and is
  **not** silently ignored — that is how a double-settle bug in your handler
  becomes visible. The runner treats it as non-fatal.
* a stale delivery returns `ErrLeaseLost`. Every delivery carries a generation
  token, so a handle you kept past a redelivery cannot settle the new one —
  including after the delivery struct has been pooled and reused.
* settlement after the consumer closed returns `ErrClosed`.
* an ambiguous native result stays **explicitly unknown** rather than being
  guessed either way.

## Non-settleable deliveries

On NATS Core there is nothing to acknowledge: the protocol has no settlement.
`Ack`, `Nack` and `Extend` return `ErrNotSettleable`, and the runner treats a
handler that returned `nil` as locally complete without pretending to have
acknowledged anything. A driver that cannot settle **never advertises
`FeatureExplicitAck`**, so the capability check catches this before your code
depends on it rather than at three in the morning.

## What is verified, and what is asserted

`FeatureAtLeastOnce` and `FeatureExplicitAck` are probed in both directions on
every driver with a conformance report, and the
matrix renders the result. `FeatureAtMostOnce`,
`FeatureExactlyOnceDelivery` and `FeatureDurableStorage` are among the claims
The specification has **no portable probe** for: a guarantee's *absence* is not observable
in one run, and a durability claim needs a broker restart the kit does not
perform. Those render as advertised-and-unverified, in parentheses, and the
matrix says so.
