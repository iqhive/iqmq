# Logging

## The library has no logger

There is no package-level logger, no `SetLogger`, and no default output. This
is not an omission: a library that logs decides your format, your level
mapping and your sink, and gets all three wrong for somebody.

What you get instead:

* **`mq.Hooks`** — `OnPublishStart`, `OnPublishDone`, `OnReceive`, `OnSettle`,
  `OnReconnect`. Five function fields on a struct; set the ones you want.
* **`ConsumerStats` and `PublisherStats`** — snapshots you poll on your own
  schedule.
* **errors** — every one carrying an `mq.ErrorCode`.
* **`middleware.Metrics`** and the two integration packages, for when you want
  the standard thing rather than your own.

A minimal `log/slog` bridge is about eight lines:

```go
client := mq.NewClient(backend, mq.WithHooks(mq.Hooks{
    OnSettle: func(e mq.SettleEvent) {
        if e.Err != nil {
            slog.Error("settle failed",
                "topic", e.Topic, "subscription", e.Subscription,
                "code", mq.CodeOf(e.Err).String(), "err", e.Err)
        }
    },
    OnReconnect: func(e mq.ReconnectEvent) {
        slog.Warn("reconnected", "driver", e.Driver, "attempt", e.Attempt, "err", e.Err)
    },
}))
```

## What the library never puts in an error

The specification is a rule about content, not about wording: **an error never carries a
message body, a key, a header value, a credential or an address.** So an error
is safe to log at any level, and safe to put in an alert, and safe to return
to a caller you do not trust with the payload.

This is checked rather than promised. Every driver has a test that renders its
configuration through `String`, `GoString`, `LogValue`, `DSN` and a real
`log/slog` handler and asserts that no credential appears in any of them — the
`Config` types implement `slog.LogValuer` for exactly that reason.

It also means an error will not tell you *which* message failed. `SettleEvent`
and the delivery carry the identity (`Delivery.ID`, topic, subscription,
partition, offset); the error carries the reason. Log both if you need to
correlate them, which is what the middleware does.

## Levels, if you want the convention

The library does not enforce these; they are what the integrations use and what
the troubleshooting page assumes.

| Event | Level | Why |
|---|---|---|
| a settled delivery | *not logged* | one line per message is a bill, not a log |
| a handler error that will be retried | `Warn` | expected, bounded, and visible in `ConsumerStats.Nacked` |
| a dead-lettered message | `Error` | you have lost the ability to process it in band |
| a `Discard(err)` | `Error` | the specification requires it to be logged **and** counted, never silent |
| a `FailureSkip` ack | `Error` | the one destructive policy; it acked a message nobody handled |
| a reconnect | `Warn` | with the attempt count, because a flapping connection is a pattern rather than an event |
| a poison halt | `Error` | the runner has stopped; something has to page |

## Where the gaps are

**There is no shutdown hook.** the five fields do not include one, so
nothing observes a client, publisher or consumer closing, and neither
integration has anything to say about a drain that exceeded its timeout —
which is exactly the production event an operator wants a metric for. Recorded
with as its owner: either the list is complete and the plan's reading
of it was wrong, or a sixth field is owed.

**`iqmq_dropped_total{policy}` has no source in the core for the policy
label.** `PublisherStats` carries `Drops` and not the mode, so
`prometheus.AddPublisher` takes the `mq.BackpressureMode` from you.
