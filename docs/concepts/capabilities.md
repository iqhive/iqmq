# Capability negotiation

Specification. This is the mechanism the whole library is built around, so
it is worth understanding before anything else: **you find out what a broker
can do by asking, at construction, and a claim that no probe has verified is
rendered differently from one that has.**

## Five levels, not a boolean

```go
const (
    mq.Unsupported      // absent
    mq.Conditional      // only with a profile or an option, and the condition is stated
    mq.EmulatedVolatile // emulated in process; lost on a crash; NEVER satisfies a durability requirement
    mq.EmulatedDurable  // emulated by iqmq using durable broker features, e.g. a dead-letter topic publish
    mq.Native           // the broker does it itself
)
```

The two emulated levels are the reason a boolean would not do. "This driver
supports delayed redelivery" is true of JetStream (`Native`, a `Nak` with a
delay), true of Redis Streams (`Conditional`, a Lua mover whose durability
depends on the server's persistence settings), and true of a broker where the
core just holds the message in the process (`EmulatedVolatile`, and a crash
redelivers it immediately). Those are three different promises, and
`Level.Durable()` is the one line of code that separates them.

`mq.Conditional` carries its condition in `Support.Note`, because a
conditional support that does not say what the condition is cannot be planned
against.

## Asking

```go
caps := client.Capabilities()
if caps.Has(mq.FeatureDelayedRedelivery) { … }
switch caps.Support(mq.FeatureDeadLetter).Level {
case mq.Native, mq.EmulatedDurable: …
}
```

`Has` is a bounds check and a load — the representation is an array indexed by
`mq.Feature` — so asking is free and you can ask on a path you care about.
Capabilities are **constant for the life of a `Client`**: the driver builds
them at open from its profile plus a cheap probe, and nothing mutates them
afterwards. That constancy is what lets the core compile them into booleans
once instead of consulting them per message.

## Requiring

Asking is for code that adapts. **Requiring** is for code that does not, and it
is the better default:

```go
pub, err := client.Publisher(ctx, mq.PublisherConfig{
    Topic: "orders",
    Requirements: []mq.Requirement{
        mq.RequireAtLeast(mq.FeatureNativeHeaders, mq.Native),
        mq.RequireAny(mq.FeatureScheduledPublish, mq.Native, mq.EmulatedDurable),
    },
})
```

An unmet requirement fails **at construction**, with an error that names the
feature and the level it found. Not on the first publish, and not by quietly
emulating. `RequireAtLeast` uses `Level.Rank()`'s documented ordinal;
`RequireAny` is for the acceptance sets that are not ordinal — "native or
durably emulated, but not volatile" is exactly the case `RequireAtLeast`
cannot express, because the ranks do not line up with what you mean.

## Limits are `Limit[T]`, and unknown is not zero

```go
type Limit[T any] struct{ Value T; Known bool }
```

`caps.Limit(mq.LimitMaxMessageBytes)` returns that, and `Known == false` means
*the driver has not said* — not "the limit is zero". This is a small type with
a large purpose: an unadvertised limit read as a limit of zero refuses every
message, and an unadvertised limit read as "unbounded" accepts one the broker
will reject. The zero `Capabilities` advertises nothing and every limit is
unknown, which is the honest answer for a driver that has not spoken.

Limits are enforced **before I/O**: `MaxHeaderCount`, `MaxHeaderBytes`,
`MaxMessageBytes` and the batch bounds are checked in the core, so a message
that cannot fit is refused locally rather than round-tripped to be rejected.

## Truthfulness is a gate, not a promise

The matrix and `capabilities.json` are **generated** from conformance runs —
`conformance/cmd/confgen`, held by `make generate-check` — and the conformance
kit asserts both directions for each claim:

* advertised support actually works;
* unsupported returns `ErrUnsupported`;
* volatile emulation cannot satisfy a durable requirement;
* conditional support names and tests its condition.

Which is why the matrix has *two* notations for
"supported":

| Glyph | Meaning |
|---|---|
| `N` `C` `ED` `EV` | the level, **verified by a probe in both directions** |
| `(N)` `(C)` `(ED)` `(EV)` | the driver advertises the level and no probe verified it |
| `—` | not offered, and a probe observed the refusal |
| `·` | a denied guarantee that no run contradicted — one run cannot observe an absence |
| `?` | not offered, and nothing established that either |
| `!` | a scenario for the claim **FAILED** |
| `◐` | the evidence is an emulator's, never presented as the managed service's |

## What is not verified, and why that is in the docs

A claim in parentheses is not a lie. It is a claim, marked as a claim.
