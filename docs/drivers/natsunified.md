# natsunified

<!-- Fixed sections per the driver documentation. Every section must be present. -->

**Maturity:** Experimental · **Tier:** 1 · **Owner:** iqmq maintainers

`github.com/iqhive/iqmq/drivers/natsunified` is one entry point to both NATS
profiles: **Core** (`drivers/nats`) and **JetStream**
(`drivers/jetstream`). It is a facade over two drivers and not a third
driver: it dials nothing, publishes nothing, settles nothing and maps no
subject.

Every section below has two columns, because the facade has two profiles and
they disagree about nearly everything a queue is judged on. Where a row says
"the profile's", the page it belongs to is [`nats.md`](nats.md) or
[`jetstream.md`](jetstream.md) and nothing here changes it.

## Client & version policy

| | Core profile | JetStream profile |
|---|---|---|
| Driver module | `drivers/nats` | `drivers/jetstream` |
| `Client.Info().Name` | `nats` | `jetstream` |
| Native client | `github.com/nats-io/nats.go` v1.55.0 | the same, plus its `jetstream` package |
| Server tested against | `nats:2.14.6-alpine` (pinned by digest in `conformance/harness/images.lock`) | the same image, `--jetstream` |

The facade adds no client library of its own and pins no version: it requires
the two driver modules, and they pin `nats.go`. `natsunified.Version` is empty
until the module is tagged, which the specification makes "the driver did not say" rather
than a claim.

`Client.Info().Name` is deliberately **not** `natsunified`. The facade returns
the profile driver's own client, so `Info()` is that driver's answer, and a
caller reading a log line can tell which driver is serving it.
`natsunified.ProfileOf(client.Info())` maps it back to a
`Profile`.

## Topic / subscription mapping

Entirely the selected profile's. The facade passes `Config.Core` or
`Config.JetStream` through untouched, including `SubjectPrefix`, so the subject
a topic maps to and the consumer or queue group a subscription becomes are
exactly what the profile's own page documents.

What the facade adds is the mapping **from a DSN scheme to a profile**, and it
is the whole of its dispatch table:

| DSN | Profile | Driver |
|---|---|---|
| `nats://user:pw@host:4222,host2:4222/prefix?…` | `ProfileCore` | `drivers/nats` |
| `natsjs://user:pw@host:4222/prefix?stream=S&…` | `ProfileJetStream` | `drivers/jetstream` |
| anything else | — | `ErrDriverNotRegistered` |

There is no `profile=` parameter and there will not be one: two spellings for
which broker you are talking to is one too many, and the second would have to be
refused whenever it contradicted the scheme. The refusal a
caller actually meets is better than that. Each scheme carries its profile's own
strict parameter table, so `nats://host?stream=ORDERS` is refused naming
`stream`, and `natsjs://host?noecho=true` naming `noecho`.

## Publish path & confirmation boundary

The selected profile's, unchanged — including the refusal.

| Mode | Core profile | JetStream profile |
|---|---|---|
| `ConfirmNone` | the message is in the write buffer | the asynchronous publication is admitted |
| `ConfirmAccepted` | PING/PONG: the server has parsed it and stored nothing | the stream's PubAck, sent after the message is committed to storage |
| `ConfirmDurable` | **refused at `Client.Publisher`** | the same PubAck as `ConfirmAccepted` |

`ConfirmDurable` is the question a facade is most likely to get wrong, and the
answer is structural rather than defensive: [`Open`](#config-dsn) resolves the
profile, so by the time there is a publisher there is no longer a choice to
defer. The sink is `drivers/nats`'s own sink and the refusal is its own
`errNoDurableConfirm`, at publisher construction, before a single message
exists. A facade that deferred the profile choice to the first publication would
have turned the fail-fast into a per-message failure; this one cannot.

**One refusal earlier still is the facade's own.** `Config.Require` carries 
requirements and is evaluated inside `Open`, against the table the connected
broker really produced:

```go
cfg, err := natsunified.ParseDSN(os.Getenv("QUEUE_DSN"))
cfg.Require = []mq.Requirement{
        mq.RequireAtLeast(mq.FeatureDurableConfirm, mq.Native),
        mq.RequireAtLeast(mq.FeatureExplicitAck, mq.Native),
}
client, err := natsunified.Open(ctx, cfg)   // refused on nats://, at the Open
```

It costs the dial, which is unavoidable — JetStream's table is read off the
bound stream, so there is no honest answer before the connection exists — and a
client it refuses is closed before the error is returned. A DSN cannot carry a
requirement, so this is a two-step and it is the recommended shape for any
program whose profile comes from configuration.

## Consume path

The selected profile's. Core is a subscription with `SetPendingLimits` and a
driver-side ring; JetStream is a durable pull consumer with `MaxAckPending`. The
facade opens no source of its own.

## Ack / nack / extend mapping

| | Core profile | JetStream profile |
|---|---|---|
| `Ack` | `ErrNotSettleable` — `Settler()` is nil (AckNone) | `+ACK`, or a request/reply round trip with `doubleack=true` |
| `Nack` | `ErrNotSettleable` | `-NAK`, with `{"delay":n}` for a delayed requeue |
| `Reject` | `ErrNotSettleable` | publish to `DeadLetterTopic`, await its PubAck, then `+TERM` |
| `Extend` | `ErrNotSettleable` | `+WPI` |

This is the difference the facade cannot smooth over and does not try to. A
program that selects its profile at run time and calls `Ack` gets a no-op
success on one profile and an error on the other, from identical portable code.
It is held by `TestSettlementDiffersByProfile` on both profiles so that nobody
has to find out in production.

## Ordering

The selected profile's, and the two happen to agree in shape: neither broker has
partitions or message keys, so all three ordering features are **conditional**
on both profiles and all three conditions reduce to "one publisher's messages on
one subject, delivered to a single consumer". A queue group with more than one
member interleaves on Core; a subscription with two consumers interleaves on
JetStream; on JetStream a nack additionally puts a message behind the messages
that followed it.

## Delays

| | Core profile | JetStream profile |
|---|---|---|
| `PublishOption` `Delay` / `DeliverAt` | `ErrUnsupported` | a `Nats-Schedule` control message, conditional on a 2.12+ server and `AllowMsgSchedules` |
| Delayed nack | `ErrNotSettleable` | native, `-NAK {"delay":n}` |

## Dead-letter queue

| | Core profile | JetStream profile |
|---|---|---|
| Destination | none; `Reject` is `ErrNotSettleable` | `Config.DeadLetterTopic`, conditional support |
| Route | — | publish, await PubAck, then `+TERM` — never `+TERM` first |

The facade sets neither. `deadlettertopic=` in a `natsjs://` DSN reaches
`drivers/jetstream`; the same parameter in a `nats://` DSN is refused by name.

## Headers & limits

The selected profile's. NATS headers are text lines on both, so binary headers
are unsupported on both and duplicate headers are native on both. The message
ceiling is the server's `max_payload` on Core, and the smaller of that and the
stream's `MaxMsgSize` on JetStream. The facade advertises no limits of its own
and `Capabilities().Limits()` is the profile's, compared field for field by
`TestCapabilitiesAreTheProfilesOwn`.

## Reconnect

The selected profile's `nats.go` reconnect loop and the selected profile's
`driverkit.Health` state machine. The facade wires no handlers and holds no
health state. On a connection handed to `OpenWithClient`, each profile's
backend puts its handlers in front of the ones the connection already has, so
a Core client and a JetStream client sharing one connection both see its lost
links, reconnections and close, and your own handlers still run after theirs.
Open the two one at a time: two opened at the same moment can each read your
handler, and the one that sets last wins. A slow consumer counts in the Core
client only for its own subscriptions, and in the JetStream client wherever
it happens on the connection.

## At-least-once window

| | Core profile | JetStream profile |
|---|---|---|
| Window | none — there is no redelivery, so the failure mode is loss | from the PubAck until the delivery is acknowledged; a lease that expires redelivers |
| Duplicates | never | yes, on lease expiry, reconnection or `Close` |

A caller that selects a profile at run time must be written for the JetStream
column and must additionally tolerate loss on the Core one. `Common()` does not
contain `FeatureAtLeastOnce`, and that is the machine-readable form of this
paragraph.

## Config & DSN

`natsunified.Config` is four fields:

```go
type Config struct {
        Profile   Profile            // ProfileCore or ProfileJetStream; there is NO default
        Core      nats.Config        // read when Profile is ProfileCore
        JetStream jetstream.Config   // read when Profile is ProfileJetStream
        Require   []mq.Requirement   // requirements, checked inside Open
}
```

**There is no default profile**. A `Config` with
`ProfileUnset` is a construction error carrying `CodeInvalidConfig`, and the DSN
scheme is the only other way to set one. The two profiles disagree about
at-most-once versus at-least-once delivery — the most consequential property a
queue has — so a zero value that resolved to either would decide it by
omission, which is the reasoning that already gave `memory.Open` its
`AutoCreate: false`.

**Both profile configurations may be filled in at once** and only the selected
one is read or validated. That is the shape a facade is genuinely good for: an
application deployed against Core in development and JetStream in production
writes both and flips `Profile`. `Config.Validate()` is pure, checks that a
profile was named and delegates the rest to the selected profile's own
`Validate`; the unselected half is not validated, because refusing a program for
a typo in configuration this deployment never reads would make that shape
unusable.

**Why it is not one merged field set**. The two driver
configurations share **19** connection fields — `Addrs`, `Name`, `Username`,
`Password`, `Token`, `CredentialsFile`, `TLS`, `InsecureSkipVerify`,
`SubjectPrefix`, `PendingMessages`, `PendingBytes`, `ConnectTimeout`,
`ReconnectWait`, `MaxReconnects`, `PingInterval`, `MaxPingsOut`,
`DrainTimeout`, `Clock`, `Hooks` — and disagree about **24** more: **2**
Core-only (`FlushTimeout`, `NoEcho`) and **22** JetStream-only. A single field
set would therefore be 19 retyped fields plus two profile-only groups, 38
assignments and two validation paths, all of which have to be kept in step with
two other modules by hand, with nothing able to notice when one of them grows a
field. Carrying the two typed configurations costs none of that.
`TestSharedConfigurationFields` recomputes the partition by reflection and fails
when either driver changes, so the figures in this paragraph are a measurement
rather than a count taken once.

**DSN.** Every parameter is the selected profile's own; see
[`nats.md`](nats.md#config-dsn) and
[`jetstream.md`](jetstream.md#config-dsn) for the two tables. `ParseDSN` reads
the scheme and hands the whole string to that profile's parser, which costs one
extra pass over the string and buys the thing that matters: neither parameter
table is duplicated here to go stale.

```
nats://user:password@host:4222,host2:4222/prefix?creds=/path.creds&tls=true
natsjs://user:password@host:4222/prefix?stream=ORDERS&managestream=true&doubleack=true
```

`Config.DSN()`, `String()` and `LogValue()` are the selected profile's redacted
rendering, so the scheme in the result is the one that would select the same
profile again. A `Config` with no profile renders as
`natsunified://?profile=unset`: there is no connection string for "the caller
did not say which broker".

**Naming and auto-create.** The facade creates no topology.

| | Core profile | JetStream profile |
|---|---|---|
| Topology | none: a subject exists because somebody published to it | a stream |
| Default | nothing to create | `ManageStream: false` |
| A name that does not exist | works immediately | `Open` fails with `CodeNotFound`, **naming the stream** |
| Explicit creation | — | `managestream=true`, in the connection string where it is reviewable |

Naming the stream in that failure is new in this increment: the client library's
own message is `nats: API error: code=404 err_code=10059 description=stream not
found`, which says everything except which stream, and a deployment with five of
them cannot act on it. `drivers/jetstream` now wraps it.

## Known gotchas

- **`Info().Name` is not `natsunified`.** It is `nats` or `jetstream`, on
  purpose. If your logging or metrics key on the driver name, a profile switch
  changes the key — which is the honest signal, and
  `natsunified.ProfileOf(info)` is how to recover the profile.
- **`OpenBackend` returns the interface, not a concrete type.** A function that
  dispatches between two backend types cannot name both in its signature, so
  `*nats.Backend.Conn()`, `*jetstream.Backend.Stream()` and `Facts()` are behind
  a type assertion. `CoreBackend` and `JetStreamBackend` are that assertion
  under a findable name; the profile driver's own `OpenBackend` avoids it
  entirely, and a program that already knows its profile should use that.
- **A feature in `Common()` can still mean two different things.**
  `FeaturePublisherConfirm` is native on both profiles and is the socket on one
  and the disk on the other. `FeatureBatchPublish` is `EmulatedVolatile` on Core
  and `EmulatedDurable` on JetStream, so `RequireAtLeast(FeatureBatchPublish,
  EmulatedDurable)` passes on one profile and fails on the other — from a
  feature both "have". Read `Capabilities().Support(f).Note`, not just `Has(f)`.
- **A DSN cannot carry a requirement.** `OpenDSN` is one call and checks
  nothing beyond the profile's own validation. The two-step —
  `ParseDSN`, set `Require`, `Open` — is the form that fails fast, and it is
  the one to use whenever the DSN is not yours.
- **One `*nats.Conn` serves both profiles.** NATS Core and JetStream are the
  same protocol on the same socket, so `OpenWithClient` with
  `driverkit.ClientBorrowed` can back a Core client and a JetStream client on
  one connection. This is the facade's clearest technical win and the one place
  where a single entry point is not merely cosmetic.
- **`ManageStream` is never turned on for you**, even when the stream the DSN
  names does not exist. See the table above.

## Maturity & tier

See the pinned client version and the module tests when evaluating this driver.

## Driver-specific critical tests

The module's own tests run against two embedded `nats-server` instances, one
plain and one with JetStream, and need no container. The ones that are about the
facade rather than about either driver:

| Test | What it holds |
|---|---|
| `TestCapabilitiesAreTheProfilesOwn` | the facade's table is the profile's, compared per feature on level, scope, note and limits  |
| `TestCapabilitiesAreNeitherTheUnionNorTheIntersection` | eight features that are native on JetStream and absent on Core, and two that are the reverse |
| `TestCommonIsTheLiveIntersection` | `Common()` against the intersection of two live tables, so a claim added to or removed from either driver fails here |
| `TestCommonSharesFeaturesAtDifferentLevels` | the `BatchPublish` and `PublisherConfirm` traps, including the `RequireAtLeast` that passes on one profile and fails on the other |
| `TestConfirmDurableIsRefusedAtConstructionOnCore` | no publisher comes back at all, which is what distinguishes a construction-time refusal from a publish-time one |
| `TestRequireRefusesAtOpenOnTheWrongProfile` | both directions: refused on Core, met on JetStream |
| `TestRequireClosesTheClientItRefuses` | the server's own connection count returns to zero after five refused opens |
| `TestFacadeNamesAStreamItDoesNotCreate` | `CodeNotFound`, the stream named, and the stream still absent afterwards |
| `TestOpenWithClientServesBothProfilesOnOneConnection` | one borrowed `*nats.Conn`, two profiles, and the connection still usable afterwards |
| `TestSharedConfigurationFields` | the 19/2/22 configuration partition, recomputed by reflection |
| `TestConformanceHarnessOpensThroughTheFacade` | the conformance run really goes through the facade's `Open` and not the driver's |
| `TestInfoAndNativeAfterClose` | the kit's `client/info-and-native` scenario, which the kit cannot run here (below) |
| `FuzzUnifiedDSN` | the two parsing passes agree, the unselected half stays zero, the rendering never carries a credential, the input's scheme is the one the grammar reads (case-insensitively), and the rendering parses back to every setting the input filled, compared field by field |
