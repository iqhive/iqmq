# iqmq unified NATS driver

`github.com/iqhive/iqmq/drivers/natsunified` is **one entry point to both NATS
profiles**: NATS Core (`github.com/iqhive/iqmq/drivers/nats`) and JetStream
(`github.com/iqhive/iqmq/drivers/jetstream`).

It is a facade over two drivers, **not a third driver**. It dials nothing,
publishes nothing, settles nothing and maps no subject. What it owns is the
choice.

```go
// The scheme is the profile.
client, err := natsunified.OpenDSN(ctx, os.Getenv("QUEUE_DSN"))
//   nats://host:4222/prefix        -> NATS Core
//   natsjs://host:4222?stream=S    -> JetStream
```

```go
// Or typed, with one configuration per deployment and a flag between them.
client, err := natsunified.Open(ctx, natsunified.Config{
        Profile:   profile,        // ProfileCore or ProfileJetStream — never a default
        Core:      coreConfig,     // read when the profile is Core
        JetStream: streamConfig,   // read when the profile is JetStream
})
```

## What it does, in full

1. **Maps the scheme to a profile.** `nats://` is Core, `natsjs://` is
   JetStream, anything else is `ErrDriverNotRegistered`. There is no `profile=`
   parameter: one spelling, and a parameter that belongs to the other profile is
   refused **by name** by that profile's own strict parameter table
   (`nats://host?stream=ORDERS` says `stream`).
2. **Registers both schemes atomically.** `Register` prefers
   `(*dsn.Registry).RegisterDriver`, so a scheme collision registers neither
   profile rather than leaving one reachable.
3. **Delegates.** `Open` returns the profile driver's own `*mq.Client`. There is
   no wrapper type, so there is nothing that can lose a capability claim, a
   capability note or an optional SPI surface — and `Client.Info().Name` says
   `nats` or `jetstream`, never `natsunified`, because a caller reading a log
   line has to know which driver is serving it.
4. **Checks requirements at `Open`.** `Config.Require` carries requirements,
   evaluated against the table the connected broker really produced. It is the
   only refusal this facade adds that neither driver has, and it is the answer to
   the question a DSN cannot ask.

Everything else is the selected profile's, unchanged.

## The two things a caller has to know

**The capability table is the profile's, and it is neither the union nor the
intersection.** Core cannot settle at all; JetStream settles, leases,
redelivers, replays, seeks and dead-letters. A union would claim
`FeatureExplicitAck` on a broker whose `Settler()` is nil — and worse, it would
make `RequireAtLeast(FeatureExplicitAck, Native)` *pass* there, defeating the
one mechanism the specification gives a caller to check. An intersection would withdraw
fifteen claims JetStream honours natively. So `Capabilities()` is the selected
profile's method on the selected profile's value.

Before you have a client, `Common()` is what you may rely on:

| | |
|---|---|
| **In `Common()`** | consumer groups, fan-out, the three ordering features, native and duplicate headers, batch publish, batch receive, wildcard subscribe, publisher confirm |
| **Not in `Common()`** | everything about settlement, storage, replay, seeking, TTL, schedules, dead-lettering and de-duplication (JetStream's), and at-most-once and request/reply (Core's) |

Two entries in `Common()` are traps and are documented as such:
`FeaturePublisherConfirm` is native on both and means *the server parsed it* on
one and *the stream committed it* on the other; `FeatureBatchPublish` is
`EmulatedVolatile` on Core and `EmulatedDurable` on JetStream, so the same
`RequireAtLeast` passes on one and fails on the other. `Common()` answers "may I
call this at all"; only `Capabilities()` answers "what does it mean here".

**`ConfirmDurable` is a construction error on Core and the normal case on
JetStream**, and the facade does not move it. The profile is resolved at `Open`,
so the sink a publisher opens is the profile's own and the refusal lands where
`drivers/nats` puts it — at `Client.Publisher`, before there is a message.

## Naming and auto-create

## Should you use it?

**If your program knows which profile it wants, import that driver directly.**
You lose nothing and you keep a concrete backend type, so
`*nats.Backend.Conn()` and `*jetstream.Backend.Stream()` need no type assertion.

**Use this facade when the profile is not yours to choose** — it comes from a
DSN, an operator's configuration or a deployment flag. Then write:

```go
cfg, err := natsunified.ParseDSN(s)
cfg.Require = []mq.Requirement{ /* what your application actually needs */ }
client, err := natsunified.Open(ctx, cfg)
```

## Documentation

`docs/drivers/natsunified.md` is the full page, with the fifteen sections
stated per profile.

## Testing

The module's own tests run against **two embedded** `nats-server` instances —
one plain, one with JetStream — and need no container:

```
go test ./...
```

The conformance evidence is `conformance/suites/natsunified_test.go`,
which runs the whole suite **twice, once per profile**, through this facade's own
`Open`, against the pinned `nats:2.14.6-alpine` image:

```
make integration DRIVER=natsunified
```

Its two reports are read against the profile drivers' own, scenario for
scenario. That comparison is the increment's result and it is in
`docs/drivers/natsunified.md`.
