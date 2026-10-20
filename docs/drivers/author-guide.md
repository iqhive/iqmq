# Driver author guide

How to write an iqmq driver for a broker this repository does not support,
**from outside this repository**. Specification.

`examples/custom-driver` is the worked example: a complete backend implemented
in a module of its own, held to the same conformance suite the memory
driver is held to, with a test — `TestNoInternalImports` — proving it imports
no `internal/` package and uses exactly four iqmq packages. "Written from
outside the repository" is a fact there rather than an assertion.

## The obligation is three interfaces

```go
type Backend interface {
    Info() DriverInfo
    Capabilities() Capabilities
    OpenSink(ctx context.Context, cfg SinkConfig) (Sink, error)
    OpenSource(ctx context.Context, cfg SourceConfig) (Source, error)
    Ping(ctx context.Context) error
    Native() any
    Close() error
}
```

Plus `Sink` (publish) and `Source` (fetch). That is all a driver must
implement. `Info`, `Capabilities` and `Native` are answered **without I/O** and
must be safe to call concurrently.

Everything a broker does differently is an **optional interface**, and the core
type-asserts each one once, when it opens the sink or source:

| Interface | For a broker that |
|---|---|
| `BatchSink` | publishes a batch in one call |
| `AsyncSink` | has an asynchronous publish with a completion callback |
| `OwnedSink` | can take ownership of the caller's buffers |
| `FlushSink` | has a flush distinct from a publish |
| `Settler` | settles a delivery (ack, nack) |
| `BatchSettler` | settles several at once |
| `DeadlineExtender` | can extend a lease |
| `AdminBackend` | has a topology surface |
| `HealthReporter` | knows more about its health than the core can infer |

Implement the ones your broker has. Leave out the ones it does not: the core's
answer to a missing interface is `ErrUnsupported`, which is the correct and
honest behaviour, and the conformance kit will check that it happens.

## What a driver must NOT do

The specification is explicit, and it is the rule that keeps the semantics consistent across
eleven brokers: **a driver must not implement worker pools, retries or
dead-letter logic.** Those belong to the runner, which is what the caller
configures. Two owners of redelivery accounting produce a message retried 25
times when the caller asked for 5.

A `Source` also never consults the core: every parameter it needs is in the
`SourceConfig` it was opened with.

And **`Open` must not create production topology.** The specification. A driver may offer
an explicit `AutoCreate` or an `Admin` surface; it may not quietly create a
topic because somebody published to it.

## Errors: the one question with a definite answer

**Do not reach for `internal/errcode`.** `drivers/nats` imports it — it is
inside this repository and can — and a driver at `github.com/acme/mq-foo`
cannot. A driver author's first instinct is to look for `errcode.New`, find it
in the source, import it, and be unable to explain later why their driver
cannot be built outside this repository.

There are two public answers and both carry the codes:

**`driverkit.ErrorMapper`** — native sentinels plus an ordered rule set. This
is the shape a driver over a real client wants, and it is what the toy driver
uses:

```go
var mapper = driverkit.ErrorMapper{
    Driver: "myq",
    Rules: map[string][]driverkit.Rule{
        mq.OpPublish: {
            {Match: driverkit.MatchIs(myq.ErrTooBig), Code: mq.CodeMessageTooLarge},
            {Match: driverkit.MatchContains("quota"), Code: mq.CodeThrottled, Retryable: true},
        },
    },
    Fallback: []driverkit.Rule{ /* … */ },
}

err = mapper.Map(mq.OpPublish, topic, "", nativeErr)
err = mapper.MapNet(mq.OpPublish, topic, "", nativeErr, sentAlready)
```

Rules are ordered and the first match wins, so write the specific rules before
the general ones. The mapper **never throws the native error away**: the result
wraps it, so `errors.As` still reaches your client library's own type.

Rules are **per operation** because retryability is. A connection reset on a
publish that has already left the socket is *ambiguous*; the same reset on a
fetch is a plain retry. `MapNet`'s `sentAlready` is how you say which, and
`driverkit.ClassifyNet` is the underlying helper.

**An `mq.OpError` literal** — for the cases a rule set does not cover. Fill the
fields you know. Note `Address` is a field and is **never rendered** by
`Error()`: the specification forbids addresses and credentials in error strings.

## What else `driverkit` gives you

Optional, all of it. Nothing in the core depends on `driverkit`, and a driver
that uses none of it is a perfectly good driver.

| | |
|---|---|
| `Envelope`, `EnvelopeConfig` | portable metadata over a broker with no headers, or with limited attributes. `wire`'s framing, with the sniffing decision made for you. |
| `TokenTable` | lease tokens with generations, so a stale settlement cannot settle a newer delivery |
| `Reconnector`, `ReconnectConfig` | bounded redial with topology restoration |
| `PushAdapter`, `PushConfig` | adapt a push-delivering client library to the core's bounded `Fetch` |
| `BatchSettler`, `BatchConfig` | coalesce settlements, with the batching strategy as a typed choice |
| `offset` | the Kafka-shaped contiguous-watermark offset tracker |
| `nack`, `flush` | the nack-strategy and flush-strategy shapes |
| `CapabilityBuilder` | build a `Capabilities` without getting the levels wrong |
| `BaseBackend`, `BaseConfig` | the boilerplate an `Open` needs |

## Capabilities: claim only what you can prove

`Capabilities` is an array of `Support` indexed by `Feature`. Three rules:

* **the zero value advertises nothing**, and that is the right answer for a
  feature you have not thought about. An unadvertised limit is `Known: false`,
  never zero.
* **`Conditional` must state its condition** in `Support.Note`. A conditional
  claim with no condition cannot be planned against.
* **`EmulatedVolatile` never satisfies a durability requirement.** If your
  emulation is lost on a crash, say so with the level rather than in prose.

Then prove it. The conformance kit checks **both** directions: the feature
works when you claim it, and the unsupported path returns `ErrUnsupported`
when you do not.

## Conformance

```go
// conformance_test.go, in package mydriver_test — NOT in the driver's package.
func TestConformance(t *testing.T) {
    testkit.RunConformance(t, testkit.Harness{
        Driver:    "myq",
        Open:      open,
        TopicName: func(p string) string { return "conformance." + p },
        Timeout:   30 * time.Second,
        Evidence:  testkit.EvidenceContainer,
    })
}
```

`package mydriver_test`, not the driver's own package.
`drivers/nats/conformance_test.go` and the toy driver's
`package toydriver_test` are the models. A harness that reaches unexported
helpers works for its own driver and is useless as an example for anybody
else's — which is the state `memory/conformance_test.go` is in, and it is
recorded as such rather than defended.

Set `Evidence` honestly: `EvidenceContainer` for a real broker,
`EvidenceEmulator` for a vendor emulator, `EvidenceInProcess` for a fake,
`EvidenceManaged` for the real service. An emulator's answer is never presented
as the managed service's.

[testkit](../testing/testkit.md) has the rest, including what the kit cannot
probe.

## The page you owe

The specification fixes fifteen sections for a driver page,
[`docs/drivers/_template.md`](_template.md) is the template, and
`make docs-check` holds every driver page in this repository to them **in
order** — reading the list out of the template itself, so the list has one
home and a page and the template cannot disagree about it:

client & version policy · topic/subscription mapping · publish path &
confirmation boundary · consume path · ack/nack/extend mapping · ordering ·
delays · dead-letter queue · headers & limits · reconnect · at-least-once
window · config & DSN · known gotchas · maturity & tier · driver-specific
critical tests

Two of them are the ones readers actually need and the ones authors skip.
**Publish path & confirmation boundary** must say in words what
`mq.ConfirmAccepted` means on your broker — a flush and a durable write are
both "accepted" and only one survives a restart. **At-least-once window** must
say where the duplicate comes from. Both of those are the honest content;
everything else is reference.

**Line 5 is your maturity declaration, and it is the only one.** The template
fixes the shape — `**Maturity:** <level> · **Tier:** <n> · …` — and
`make docs-check` reads it: the level has to be one of the four
(`Experimental`, `Beta`, `Stable`, `Deprecated`, read out of the specification
rather than hard-coded here), the tier has to be 1, 2 or 3, there has to be
exactly one such line, and it has to be on line 5. A new driver starts
**Experimental** and nothing objects. Two further rules bite only when you claim
more:

Do not restate the level anywhere else. `docs/drivers/index.md`'s Maturity
column is held to line 5 by the same check; a driver module's README should link
the page rather than repeat it, because the one that repeated it drifted.

## Registration

A driver exposes `Register(dsn.Registrar)` and **never reaches for a global
registry**, so a program composing two brokers owns the registry both are
registered with. There is no process-wide registry: a program builds one with
`dsn.NewRegistry()` in `main` and passes it to whatever opens clients.

## Checklist

- [ ] `Backend`, `Sink`, `Source`; the optional interfaces your broker has
- [ ] no worker pool, no retry loop, no dead-letter logic
- [ ] `Open` creates no production topology
- [ ] errors through `driverkit.ErrorMapper` or `mq.OpError` — not
      `internal/errcode`
- [ ] `Config` redacts in `String`, `GoString`, `LogValue` and `DSN`, with a
      test that pushes it through a real `log/slog` handler
- [ ] capabilities claim only what a probe can prove; `Conditional` states its
      condition
- [ ] `testkit.RunConformance` in `package <driver>_test`, with an honest
      `Evidence`
- [ ] critical tests for the things only your broker does
- [ ] fuzz targets for your native mapping and any parser
- [ ] a driver page with the fifteen sections, and one `**Maturity:**` line
      on line 5 — `Experimental` until you can cite the evidence for more
- [ ] `Register(dsn.Registrar)`, no global
