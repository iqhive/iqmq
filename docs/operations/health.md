# Health

```go
snap := client.Health()
switch snap.State {
case mq.HealthReady:      // serving
case mq.HealthDegraded:   // snap.Err says why
case mq.HealthClosed:     // Shutdown or Close has run
}
```

`mq.HealthSnapshot` is two fields: a `State` and an `Err` which is non-nil
exactly when the state is not ready. The error never carries a payload, a
credential or an address, so the snapshot is safe to serve from an unprotected
endpoint.

## Five states

| State | Means |
|---|---|
| `mq.HealthConfigured` | the client exists and has not connected |
| `mq.HealthConnecting` | a connection is being established |
| `mq.HealthReady` | serving |
| `mq.HealthDegraded` | something is wrong and `Err` says what |
| `mq.HealthClosed` | `Shutdown` or `Close` has run |

## It is a snapshot, not a subscription

`Health` answers with what is true now, cheaply, with no I/O. It does not
block, does not dial, and does not tell you when something changes.

For *changes*, use `mq.Hooks.OnReconnect`, which the driver fires when it
observes one. The two together are the whole story: poll `Health` from your
readiness probe, and hook `OnReconnect` if you want to alert on flapping.

## Readiness and liveness are different questions

**Liveness** — is the process wedged? `client.Health().State != mq.HealthClosed`
is a weak answer and a process that is deadlocked inside a handler will still
pass it. A better liveness signal is your own: a handler that has not completed
anything in *n* seconds while `ConsumerStats.InFlight > 0` is stuck, and
`ConsumerStats.Received` not advancing while the topic has lag is stuck
differently.

**Readiness** — should traffic come here? `mq.HealthReady` plus your own
dependencies. Note that a client is ready before it has consumed anything:
`Consumer.Run` starting is not part of the client's health, because a client
with no consumers is a perfectly healthy publisher.

**`client.Ping(ctx)`** is the deeper check, and it costs a round trip. 
requires it to never consume a message and never create topology, so it is
safe to call on a schedule — but it is a *broker* check, not a client one, and
on some drivers it is the only thing that will notice a silently dead
connection. That is also how `OnReconnect` gets fired at all on such drivers,
which is worth knowing before you decide not to ping.

## `HealthReporter` is what a driver adds

A driver that can say more than the core can infer implements
`mq.HealthReporter`, and the client uses it. A driver that does not gets the
core's own view: configured, ready once open succeeded, degraded once an
operation failed with a connection error, closed after shutdown.

## Draining is a health event and has a timeout

`client.Shutdown(ctx)` flushes publishers, drains consumers and then closes the
backend, bounded by `ctx`. The runner's own wait for handlers already in flight
is `mq.DefaultDrainTimeout` — 30 seconds — or whatever
`mq.WithDrainTimeout(d)` says; past it their contexts are cancelled.

A drain that exceeds its timeout is the production event this library currently
cannot report as a metric, because the specification has no shutdown hook. It is recorded
with as its owner. In the meantime: the `Shutdown` error is the signal, and
it is worth logging at `Error`.

See [graceful shutdown](../reliability/graceful-shutdown.md) for the ordering
guarantees the drain gives you.

## `iqmq doctor`

For the "is this configuration going to work at all" question, the
CLI has `iqmq doctor`, which dials, reports the driver's
capabilities and limits, and exits `3` when the driver lacks a capability the
command needs. It is the thing to run before a deployment, not during one.
