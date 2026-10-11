# Credential rotation

Rotating a secret under a client that is serving traffic. Two facts decide how
this goes, and both are about this library rather than about your secret store.

## Fact one: a credential reaches a driver once

A driver's `Config` — or its DSN — is read at `Open` and is not re-read. There
is no `SetCredentials`, and there is no callback the core will invoke to ask
for a fresh one. That is deliberate: a configuration that can change under a
running client is a configuration whose state nothing can reason about, and the
capability model depends on a client's answers being constant for its life.

So the portable rotation is **open a new client and drain the old one**:

```go
next, err := kafka.Open(ctx, newConfig)   // new credential
if err != nil {
    return err                            // keep serving on the old one
}
swapIn(next)                              // your own pointer swap
if err := prev.Shutdown(drainCtx); err != nil {
    slog.Error("drain after rotation", "err", err)
}
```

The ordering matters. Open first, and only swap when the open succeeded —
otherwise a bad credential takes down a client that was working. `Shutdown`
flushes publishers and drains consumers, bounded by its context, so in-flight
handlers finish rather than being cancelled mid-transaction. See
[graceful shutdown](../reliability/graceful-shutdown.md).

Some client libraries take a credential *provider* rather than a value — the
AWS SDK's credential chain refreshes itself, and a Pub/Sub service-account key
is renewed by the metadata server. Where that is true, rotation is the SDK's
and this page does not apply; the driver page says which.

## Fact two: the credential is never rendered

Every driver's `Config` implements `slog.LogValuer` and renders itself with the
credential redacted, and the same is true of `String`, `GoString` and `DSN`.
Each driver has a test — `TestConfigNeverRendersACredential` or its local name
— that pushes the config through all four *and* through a real `log/slog`
handler and asserts that the secret appears in none of them.

the rule completes it: **an error never carries a credential or an
address**, so a failed open does not leak the thing that failed. A log line
from this library is safe to ship to a log aggregator you do not own.

The CLI holds the same line: `iqmq doctor` prints capabilities and limits and
not the DSN it dialled with, and it takes a password from the environment
rather than from a flag so that it does not reach the process table or a shell
history.

## What to watch while rotating

| Signal | Means |
|---|---|
| the new `Open` returns an error whose `mq.CodeOf` is `mq.CodeUnauthorized` or `mq.CodeForbidden` | the new credential is wrong, or has not propagated. Nothing was swapped. |
| `mq.CodeOf(err)` on the new `Open` is `mq.CodeUnauthorized` or `mq.CodeForbidden` | the new credential is wrong, or has not propagated. Nothing was swapped. || the old client's `Health()` goes `mq.HealthDegraded` **during** the drain | the old credential was revoked before the drain finished. Shorten the overlap next time, or lengthen the drain. |
| the new `Open` fails with `mq.CodeUnauthorized` or `mq.CodeForbidden` | the new credential is wrong, or has not propagated. Nothing was swapped. |
| the old client's `Health()` goes `mq.HealthDegraded` **during** the drain | the old credential was revoked before the drain finished. Shorten the overlap next time, or lengthen the drain. || `ConsumerStats.SettleFailures` rising on the old client | it is failing to settle what it already holds. Those messages will be redelivered to the new client — a duplicate, not a loss. |
| `Shutdown` returns non-nil | the drain hit its deadline and handlers were cancelled. Whatever they were doing is unfinished and unsettled, so it comes back. |

## The gap

There is no shutdown hook, so a drain that exceeded its timeout
during a rotation is not a metric — only the `Shutdown` error and your own log
line. It is recorded with as its owner; see
[logging](logging.md#where-the-gaps-are).
