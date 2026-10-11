# Operations

What an operator needs from this library, and what it deliberately does not
provide.

| Page | |
|---|---|
| [Metrics](metrics.md) | the canonical metric names, through `integrations/prometheus` |
| [Tracing](tracing.md) | the four spans and W3C trace context, through `integrations/otel` |
| [Logging](logging.md) | what the library logs, what it never logs, and where your handler's logs come from |
| [Health](health.md) | five client states, and the difference between "connected" and "working" |
| [Troubleshooting](troubleshooting.md) | symptom to cause, with the error codes |
| [Upgrades](upgrades.md) | broker upgrades, client-library upgrades, and the release train |
| [Credential rotation](credential-rotation.md) | rotating a secret under a live client |

## Three things that are true of all of them

**Observation is opt-in and costs nothing when it is off.** `mq.Hooks` is a
struct of function fields; an unset field is a nil check on a path that is
already compiled to booleans. There is no global registry, no `init()`-time
side effect, and no instrumentation you did not install.

**Nothing in the library logs by itself.** There is no package-level logger and
no default handler. What the library gives you instead is hooks, stats
snapshots and errors; turning those into log lines is `middleware.Metrics`,
`integrations/otel`, or four lines of your own. That is deliberate — a library
that logs has opinions about your log format that it should not have.

**The error is the interface.** Every failure carries one of the specification's
twenty-six `mq.ErrorCode` values, reachable with `mq.CodeOf(err)` and matchable
with `errors.Is` against the sentinel of the same name. An operations runbook
written against those codes keeps working when the message text changes, which
is the point of having them.

## What is not here

There is no built-in admin UI, no metrics endpoint, and no "iqmq agent". The
`iqmq` [command-line tool](../cli/index.md) is a diagnostic — `doctor`, `pub`,
`sub`, `tail`, `bench` — and not a control plane.

Topology management is `mq.Admin` and is deliberately small: ensure a
topic, ensure a subscription, delete either, list, purge, stats. It is
`Conditional` or absent on several drivers, because several brokers have no
portable way to do it and inventing one would mean guessing. The matrix's
`Admin` row says which.
