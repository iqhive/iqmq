# Troubleshooting

Symptom to cause. Every row names the error code or the counter that
distinguishes it, because the codes are the stable interface and the messages
are not.

## At construction

| Symptom | Likely cause |
|---|---|
| `mq.ErrUnsupported` from `client.Publisher` or `client.Consumer` | a `Requirement` the driver does not meet, or a config field it cannot honour. The message names the feature. |
| an error whose `mq.CodeOf` is `mq.CodeInvalidConfig` | a field combination the driver rejects — `ConfirmDurable` on NATS Core or the memory engine, two subscriptions on `drivers/sqs`, a `Start` position needing a capability the driver lacks |
| `mq.CodeOf(err) == mq.CodeInvalidConfig` | a field combination the driver rejects — `ConfirmDurable` on NATS Core or the memory engine, two subscriptions on `drivers/sqs`, a `Start` position needing a capability the driver lacks || `mq.ErrDriverNotRegistered` from `dsn.Open` | the driver's module is imported but its `Register` was never called on the registry you passed. Drivers do not use a global registry. |
| `mq.CodeInvalidConfig` | a field combination the driver rejects — `ConfirmDurable` on NATS Core or the memory engine, two subscriptions on `drivers/sqs`, a `Start` position needing a capability the driver lacks |
| `mq.ErrDriverNotRegistered` from `dsn.Open` | the driver's module is imported but its `Register` was never called on the registry you passed. Drivers do not use a global registry. || a DSN that is refused with a list of what is required | the DSN refusal names what a valid DSN needs rather than asserting what yours lacks; read it as a schema |

## Publishing

| Symptom | Cause | What to do |
|---|---|---|
| `Publish` blocks for ever | `mq.BackpressureBlock` (the default) on a full buffer and a context with no deadline | put a deadline on the context, or choose `mq.BackpressureReject` |
| `mq.ErrBackpressure` | `mq.BackpressureReject` at the bound | raise `MaxPendingMessages`, slow down, or shed |
| `mq.ErrMessageTooLarge` before any I/O | a body, header count or header size over the driver's advertised limit — enforced in the core | check `caps.Limits()`; for header *count*, note that `MetadataAdaptive` will **not** rescue you, because the core refuses first |
| `mq.ErrUnsupported` from a per-message option | `mq.Delay`, `mq.TTL`, `mq.DedupKey` or `mq.Priority` on a broker without it, refused before I/O | the matrix says which broker has which |
| `mq.IsAmbiguous(err)` | the outcome is genuinely unknown | do **not** blind-retry; see [ambiguous publishes](../concepts/ambiguous-publishes.md) |
| `errors.Is(err, mq.ErrPartial)` on a batch | some items failed, or none was accepted and their outcomes differ (some refused, some ambiguous) | `err.(*mq.BatchError).Errs` is index-aligned with your input; `mq.IsAmbiguous` on an entry says whether that item may already be in the broker |

## Consuming

| Symptom | Cause | What to do |
|---|---|---|
| the same message over and over, never dead-lettered | the broker keeps no delivery count, so `Delivery.Attempt` is 0 and under `mq.OrderingRelaxed` the runner can never call it exhausted | `mq.OrderingStrict`, which counts in-process, or a broker with a native count. See [ordering](../concepts/ordering.md#the-attempt-bound-can-be-unreachable). |
| throughput collapses after one failure | `mq.OrderingStrict` at `mq.WithConcurrency(1)`: one worker parked for the whole backoff, up to the configured 30-second cap | raise the concurrency, or shorten `mq.ExponentialBackoff.Max` |
| `mq.ErrLeaseLost` on settle | the lease expired and the broker redelivered while the handler ran | `mq.WithLeaseExtension`, `mq.WithLeaseDeadline`, or a faster handler. `ConsumerStats.Redelivered` counts it. |
| `mq.ErrNotSettleable` | NATS Core: the protocol has no settlement | do not require `FeatureExplicitAck` on that profile |
| `mq.ErrAlreadySettled` | your handler settled and the runner also tried, or the handler settled twice | it is non-fatal; if it surprises you, the double-settle is in your code and this error is how you found it |
| the runner stopped and returned a `*mq.PoisonError` | attempts exhausted with `mq.FailureHalt` (the default with no dead-letter destination), or a `mq.Halt(err)` from the handler | configure a dead-letter destination, or handle the class of failure |
| `ConsumerStats.DeadLetterFailures` rising | dead-letter publications the runner could not confirm. The originals were requeued, not lost. | the dead-letter destination is broken; the messages are alive and will be retried |
| nothing is consumed and no error | the subscription started at `mq.StartLatest` and nothing has been published since; or a position already existed and a `Start` change did nothing | `mq.StartEarliest` needs `FeatureReplay`; a restart resumes rather than re-reads |
| a goroutine leak after `Close` | on some drivers a client library's own goroutine outlives the close by up to one of its timeouts | the driver pages record the ones that are known, with the upstream cause |

## Asking the system

```
iqmq doctor 'kafka://localhost:9092'      # dial, then print capabilities and limits; exit 3 on a missing one
iqmq tail 'kafka://localhost:9092' orders # watch messages without settling anything
```

Exit `3` from `doctor` means the driver lacks a capability the command needed —
the honest answer rather than a crash. The [CLI page](../cli/index.md) has the
rest, including what it deliberately cannot express.

## When it is the library

Two things to check before writing an issue, because both have caught real
bugs and both are cheap:

* **run under `-race`.** The engine's recurring defect class is a record placed
  in a lane while the subscription is closing, and it appears as five to twenty
  failures per two or three thousand rounds — which means a hundred rounds
  proves nothing either way.
* **reproduce on the memory driver.** `memory.NewForTest` gives you a fake
  clock and a leak check, and a failure that reproduces there is a core
  failure rather than a broker one. That halves the search.

A driver bug report wants the `iqmq doctor` dump, which is what the issue form
asks for.
