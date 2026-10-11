# The memory driver as a test double

`github.com/iqhive/iqmq/memory` is in the root module, depends on the standard
library only, and implements the same `mq.Backend` as Kafka does. That last
part is the whole argument for using it in tests: a test written against it
exercises the real runner, the real settlement state machine and the real
capability guards.

It is also the reference target for this repository's own state-machine,
linearizability and differential testing, which means its conformance is
held tighter than any network driver's.

## `NewForTest`

```go
client, broker := memory.NewForTest(t, opts...)
```

Three things it does that `memory.Open` does not:

* **`AutoCreate` on.** Topics appear when you publish to them. `Open` defaults
  it off, to be consistent with network drivers where creating a topic is a
  privileged operation.
* **a fake clock.** Nothing advances until you say so.
* **a goroutine-leak check on `t.Cleanup`.** A consumer you forgot to close
  fails the test rather than slowing down the next one.

`memory.TestRealClock()` hands time back to the wall clock — or, inside a
`synctest` bubble, to the bubble. That is the option
[the synctest page](synctest.md) is about.

## Driving time

```go
broker.Advance(30 * time.Second)   // lease expiry, delayed delivery, TTL, backoff
```

`Advance` moves the fake clock and lets the scheduler fire everything that is
now due: delayed publications, delayed requeues, TTL expiry and lease expiry
all run off the same per-broker scheduler, so one call exercises all four.

It returns an error, and the error is informative rather than decorative: on a
broker left on the real clock there is no fake clock to move, and `Advance`
says so instead of silently doing nothing.

## Faults

```go
f := broker.Faults()
f.RejectPublish(1)         // the next publication is refused
f.FailSettlement(-1)       // every settlement fails until disarmed with 0
f.DuplicateDelivery(2)
f.FailDeadLetter(1)        // the runner must requeue, not ack
f.DelaySettlement(3)
f.Reorder(4)               // only where no order is promised

n := f.ExpireLeases()      // the sweeps happen NOW rather than when due
_ = f.CloseConsumers()
_ = f.Reconnect()

// and the counters that say the path was taken
_ = f.PublishRejections()
_ = f.SettlementFailures()
_ = f.DeadLetterFailures()
_ = f.Duplicates()
```

the list, and the counters are the point. Each armed fault reports how often
it fired, so a test asserts *that the path it wanted was taken* rather than
that nothing crashed — and a fault that fired zero times tells you the test did
not reach the branch. That has caught more broken tests here than broken code.

A positive count fires that many times; a negative count fires until disarmed
with `0`. The sweep methods — `ExpireLeases`, `ExpireRecords`,
`TrimRetention`, `CloseConsumers`, `Reconnect` — are not armed faults but
*immediate* actions returning how many records or consumers they touched: the
scheduler does them on its own, and the fault makes them happen now rather
than being the only way they happen.

## Inspecting

```go
snap, err := broker.Inspect("orders", "billing")   // per-lane state
msgs, bytes, discarded := broker.Totals()
```

`Inspect` is what the engine's model tests assert on, and it is available to
you for the same reason: when a test fails, "there are three records ready and
one in flight" is the fact that explains it.

## The three topologies

`memory.TopologyConfig.Kind` — and they are genuinely different code paths
rather than one with flags, because the specification requires the queue fast path not to
pay for stream bookkeeping:

| Kind | Behaviour |
|---|---|
| `memory.TopologyQueue` | competing consumers; the record is removed after a terminal ack |
| `memory.TopologyTopic` | fan-out to independent subscriptions, each with its own backpressure and settlement state |
| `memory.TopologyStream` | a bounded retained log with replayable cursors and consumer groups |

Pick the one that matches the broker you are standing in for. A test against
`TopologyQueue` will not catch a bug that only appears when a second
subscription is slow, and a test against `TopologyTopic` will not catch one
that depends on replay.

`memory.TopologyForTest` is the shorthand for a topology in a test.

## What it is honest about

**It is never persistent, and never advertises durable storage.** So it cannot
stand in for a durability test, `mq.ConfirmDurable` fails at construction on
it, and `FeatureDurableSubscriptions` renders as *emulated-volatile* rather
than as support. Those are the right answers and they are why you cannot use
the memory driver to prove a durability claim.

**Its capability table is generated from a real conformance run**, like every
other driver's: `docs/drivers/memory.capabilities.md`. The engine is held to
the same evidence standard as a network broker.

## Choosing the engine's internals

`memory.EngineConfig` selects the variant implementations, and every `Auto` is
a recorded bake-off winner. **Leave them on `Auto`** — except for one case:

```go
cfg.Engine = memory.SafeEngine()   // every family forced to its mutex reference kind
```

That is the differential-testing configuration. If a test passes under
`SafeEngine()` and fails under the defaults, you have found a bug in a variant
implementation, and that is a report worth making.
