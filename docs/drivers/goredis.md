# goredis

<!-- Fixed sections per the driver documentation. Every section must be present. -->

**Maturity:** Experimental · **Tier:** 2 · **Module:** `github.com/iqhive/iqmq/drivers/redis/goredis` · **Profile:** `goredis`

This is the **same driver** as [`redis`](redis.md) over a different client
library. The specification asks for two Redis drivers "both implement[ing] a small internal
command interface", and that interface is
`internal/streams.Commander`: one method that sends a command and returns a
reply. Everything that decides whether a message is lost or duplicated — the
key naming, the entry field layout, the commands, the reply semantics, the
reclaim accounting, the nack strategies, the capability table, the error table,
the `Config` type and the DSN parser — is shared code, and this module is the
adapter plus its entry points.

So **every section below that is not about the client library says "as
[`redis`](redis.md)" and points at it.** That is not a gap in this page: two
copies of the same prose is two copies to drift, and the half that drifted
would be the half nobody reads. What this page carries in full is what the
client library changes, and the evidence that it changes nothing else.

## Client & version policy

| | |
|---|---|
| Client library | `github.com/redis/go-redis/v9` |
| Pinned version | `v9.23.0` |
| Interface taken | `redis.UniversalClient` (so `*redis.Client`, `*redis.ClusterClient`, `*redis.Ring` and a Sentinel failover client all fit) |
| Minimum server | Redis 6.2, as [`redis`](redis.md) — the reclaim loop is `XAUTOCLAIM` |
| Protocol | RESP3 (go-redis's default); RESP2 is exercised by `internal/streams/respsrv`'s own tests |

### Verified facts (the API contract)

Checked while building this driver; re-verify when the pin moves.

- **go-redis retries a command it has already written.** `Options.MaxRetries`
  defaults to 3 and `shouldRetry` returns true for a write error and for a read
  timeout, both of which happen after the command may have reached the server.
  On this broker a retried `XADD` is a second copy of the message, reported to
  the caller as one successful publication. `MaxRetries: -1`, which this
  driver sets on the client it dials, is not enough on its own: a cluster
  client (what the driver dials for several addresses) re-sends a timed-out
  command through its redirect loop, up to `MaxRedirects` times, whatever
  `MaxRetries` says. And a client you pass to `OpenWithClient` keeps its own
  `MaxRetries`. So every command the driver sends, on every client, its own
  included, is marked as one go-redis must not retry (`Cmder.NoRetry`), which
  every retry loop checks: a single command and a pipeline, on a client, a
  cluster and a Ring. Your own commands on a client you pass in keep the
  retries you chose.
  _Verified against: go-redis v9.22.0 `options.go` (MaxRetries default),
  `redis.go` (`_process`'s retry loop, `shouldRetry`); v9.23.0 `redis.go`
  (`processWithRetry`, `generalProcessPipelineFrom`), `command.go`
  (`cmdsContainNoRetry`), `ring.go` (`Ring.process`), `osscluster.go`
  (`ClusterClient.process`, its `MaxRedirects` loop);
  `TestGoRedisDoesNotRetryAWrittenXAdd`._
- **go-redis reads a context only as a deadline on the socket, and only with
  `ContextTimeoutEnabled`.** Without it the socket gets a background context,
  so a `Publish` past its deadline waited up to `ReadTimeout` for its reply and
  a `Fetch` waited out its block. This driver turns it on. A cancellation
  without a deadline is not seen at all: go-redis watches no context while it
  reads, so the call returns when the server answers.
  _Verified against: go-redis v9.23.0 `redis.go:1892` (`baseClient.context`),
  `internal/pool/conn.go:1382` (`Conn.deadline`);
  `TestPublishReturnsAtItsDeadline`, `TestFetchReturnsAtItsDeadline`._
- **A Ring routes a command it cannot find a key in by its first argument.**
  `WAITAOF` and `SCAN` name no key and are not in go-redis's keyless list, so
  a Ring sends `WAITAOF 1 0 …` to whichever shard owns the key `1`, and each
  page of a `SCAN` to whichever shard owns its cursor. `GetShardClientForKey`
  and `GetShardClients` name the shards directly, and the driver uses them for
  a durable publication and for `ListTopics`.
  _Verified against: go-redis v9.23.0 `command.go:337`
  (`cmdFirstKeyPosWithInfo`), `ring.go:844` (`cmdShard`), `ring.go:1033`,
  `ring.go:1046`; `TestADurablePublicationOnARingWaitsOnTheShardThatWroteIt`,
  `TestListTopicsOnARingListsEveryShardsTopics`._
- **go-redis cannot find the key in a command sent through `Do`.**
  `cmdFirstKeyPos` reads the position from the server's own command table,
  which knows nothing about an arbitrary `Do`, and `cmdSlot` falls back to
  `hashtag.RandomSlot()` — a *random* node. `SetFirstKeyPos` is how a caller
  states it, and this driver states it from `streams.Cmd`'s key span.
  _Verified against: go-redis v9.22.0 `command.go` (`SetFirstKeyPos`),
  `cluster.go`/`ring.go` (`cmdSlot`); `TestKeyRoutingIsDecidedByTheCommandsKeyAndNotAtRandom`._
- **A per-command read timeout is not reachable from outside go-redis.**
  `Cmder.readTimeout()` and `baseCmd.setReadTimeout` are unexported, so the
  only lever for a blocking command is the connection's `ReadTimeout`.
  _Verified against: go-redis v9.22.0 `command.go:553`, `command.go:557`._
- **go-redis's default `ReadTimeout` is five seconds, and so is this driver's
  default `Block`.** They raced: every idle fetch came back as
  `read tcp …: i/o timeout`. `ReadTimeout` is now the block plus the dial
  timeout. A client you pass to `OpenWithClient` keeps its own `ReadTimeout`:
  a `Block` left at its default is shortened to fit inside it, and a `Block`
  you set that does not fit is refused as invalid configuration.
  _Verified against: go-redis v9.22.0 `options.go` (ReadTimeout default);
  `drivers/redis.DefaultBlock`; `TestABlockingReadWaitsOutTheBlockRatherThanTimingOut`._
- **`redis.Nil` is a CONSTANT of a string type.** So `var x = redis.Nil` gives
  a string-typed variable that is boxed into an `error` interface on every use
  — one allocation per call, charged to whatever called it.
  _Verified against: go-redis v9.22.0 `redis.go:28`, `internal/proto/reader.go:44`._
- **go-redis reports a null reply as an ERROR (`redis.Nil`), not as a value.**
  An `XREADGROUP` that timed out and one that failed are different answers, so
  the adapter turns `redis.Nil` into `streams.Nil` and everything else into a
  failure.
  _Verified against: go-redis v9.22.0 `internal/proto/reader.go` (`ReadReply`)._
- **go-redis decodes a reply into `any`**: `string`, `int64`, `float64`,
  `bool`, `*big.Int`, `[]any` (array, set and push), `map[any]any` (RESP3 map),
  and a nested error reply as a `proto.RedisError` VALUE inside the slice.
  _Verified against: go-redis v9.22.0 `internal/proto/reader.go:338` (`ReadReply`),
  `readSlice`; `TestConvertRendersEveryReplyGoRedisCanProduce`._
- **go-redis does NOT strip the `ERR ` prefix from an error reply and rueidis
  DOES** (a kvrocks compatibility workaround in
  `RedisMessage.Error`). The shared error table therefore matches the
  distinctive phrase rather than the prefix.
  _Verified against: rueidis v1.0.77 `message.go:751`; go-redis v9.22.0
  `proto.RedisError`; `TestBothClientsMapOneServerReplyToOneCode`._
- **`Pipeline.Exec` returns the FIRST error among its commands**, and sets a
  transport failure on every command it holds while leaving a server's error
  reply on the command that got it.
  _Verified against: go-redis v9.22.0 `pipeline.go:104`, `redis.go:1607`
  (`generalProcessPipeline`, `setCmdsErr`)._

## Topic / subscription mapping

As [`redis`](redis.md#topic-subscription-mapping), including the entry field
layout (`b`, `k`, `t`, `h:<n>`, `p:<n>`), the hash-tagged `{topic}` /
`{topic}:<i>` shard keys and the `XGROUP CREATE … MKSTREAM` subscription. The
key naming is `internal/streams.Keys` and is shared character for character.

One thing is this module's own: go-redis is **told the first key's position**
in every command, from `streams.Cmd`'s key span. Without it a cluster or a ring
routes every command this driver sends to a random node — correct by accident
on a standalone server and wrong everywhere else. Only the first key's position
is stated because that is all go-redis's routing reads, and it is enough:
every key of a topic carries the topic's hash tag, so all of a command's keys
are on one slot and the first one names it.

## Publish path & confirmation boundary

As [`redis`](redis.md#publish-path-confirmation-boundary): `XADD` with
`MAXLEN ~ N` when configured, `ConfirmAccepted` at the server's reply,
`ConfirmDurable` via `WAITAOF` and conditional on Redis ≥ 7.0 with an
append-only file, and the admission bound applied before any I/O in every
metadata mode.

What differs is what a publication costs and what it may not do:

* **Batching.** A batch is `Pipeline` → `Process` per command → `Exec`, one
  write and one read, with one reply per command. `DoMulti` returns a result
  per command index-aligned with its input, and treats `Exec`'s own error as
  the batch's only when no command carries one of its own — because `Exec`
  returns the *first* command's error, and reading that as the batch's would
  report one message's outcome for another.
* **No retries.** Every command the driver sends is marked `NoRetry`, on every
  client, its own included. `MaxRetries: -1` on the client it dials was not
  enough: with several addresses that client is a cluster client, whose
  redirect loop re-sent a late `XADD` and wrote it twice. The
  retry policy is the core, applied where a caller can see it, and a
  publication whose reply never arrived is reported Ambiguous rather than
  silently sent again. The same goes for a reply that missed the context's
  deadline.
* **The durable boundary on a Ring.** A durable publication's `XADD` and
  `WAITAOF` run as one pipeline on a connection of the shard that owns the
  topic, as they do on a node of a cluster. Sent through the Ring, the
  `WAITAOF` went to the shard that owns the key `1`, which wrote nothing,
  answered at once and confirmed the publication durable.
* **Allocations.** go-redis takes a command as `...any`, so every token of
  every command is boxed: a nine-token `XADD` costs ten allocations before it
  reaches the socket. `TestAllocsAdapting` pins the adapter's own share, and
  The specification measures the difference.
* **The message body is not copied here, and is on [`redis`](redis.md).**
  go-redis writes and reads in the calling goroutine, so a `[]byte` argument
  is on the wire before `Publish` returns; the body therefore goes out as the
  caller's own slice. rueidis queues a command for a background writer and can
  return from a cancelled `Do` with the command still queued, so the same
  trick there would put the caller's LATER bytes on the wire, and that arm
  copies. It is a statement rather than a performance note, and it is
  part of why the batched budget reads differently on the two profiles.

  **One obligation follows for you.** A `redis.Hook` installed through
  `OpenWithClient` that STORES the `Cmder` and reads its arguments after its
  own call returns must copy what it stores: the body argument is the
  publisher's buffer, and the specification has already given it back.

## Consume path

As [`redis`](redis.md#consume-path): `XREADGROUP COUNT prefetch BLOCK`, a
delivery buffer holding at most one delivery per entry, and the generation
re-check in the drain.

`streams.Cmd.Blocking` is honoured as far as this library allows: rueidis
routes a blocking command to a connection pool of its own, and go-redis has no
per-command lever at all (`setReadTimeout` is unexported), so the connection's
bound is what carries it. go-redis takes a connection per command from a pool,
so a blocking read occupies one connection rather than stalling the others —
the head-of-line problem `Blocking` exists for on a multiplexed client does not
arise here.

A `Fetch` returns at its context's deadline, as on rueidis. A `Fetch` whose
context is cancelled without a deadline does not: it returns when the block
ends, because go-redis does not watch a context while it reads. rueidis closes
the blocking connection and returns at once. The same bound applies to a halt
or a `Shutdown` of a `Run` on this arm, so a shorter `Block` is the lever if
that wait matters to you.

## Ack / nack / extend mapping

As [`redis`](redis.md#ack-nack-extend-mapping), including **both halves of
the guarded acknowledgement** — the per-source generation table that refuses a
superseded settlement with no command sent, and the Lua script that checks the
pending row's consumer atomically with the `XACK` — and the `XCLAIM … JUSTID`
extension behind the same ownership check. Both are re-proved over this client
(`TestAckOfAStolenDeliveryIsRefused`, `TestExtendDoesNotStealTheEntry`,
`TestSweepRedeliversAnUnsettledDelivery`), because both travel over the client
library: the generation table decides without a command and the Lua check is an
`EVALSHA` whose reply this adapter parses.

Negative settlement is the four strategies, unchanged and
shared.

## Ordering

As [`redis`](redis.md#ordering): per shard, and a topic with several shards has
no total order. The key-to-shard mapping is FNV-1a over the key bytes
(`redis.ShardOf`) and is the same function, so a key published through one
client and consumed through the other lands in the same shard.

## Delays

As [`redis`](redis.md#delays): only the `zsetmover` nack strategy honours an
arbitrary redelivery delay; the other three refuse a delay they cannot honour
rather than redelivering early. A delayed *publication* is unsupported on this
broker under either client.

## Dead-letter queue

As [`redis`](redis.md#dead-letter-queue): a destination is configuration
(`Config.DeadLetterTopic` or `Config.DeadLetterSuffix`) and is empty by
default, so a `Reject` on a subscription that has none reports
`ErrUnsupported` and settles nothing. The suffix form is one atomic Lua move
(the keys share the topic's hash tag); the topic form is two commands with a
documented duplicate window.

## Headers & limits

As [`redis`](redis.md#headers-limits): native, ordered, binary-safe and
duplicate-preserving, because a stream entry is a field-value list of
binary-safe strings; bounded by the portable envelope's decode limits in every
metadata mode.

The client library is where binary safety could be lost, so it is proved here
rather than assumed:
`TestTheFieldLayoutRoundTripsEveryMessageThisClientAccepts` round-trips a body
carrying `\r\n`, a NUL and high bytes, header values carrying the protocol's
own terminator, sixteen headers, the identifier at the limit, and a message
with nothing in it at all. The capability table's limits are identical under
both clients (`TestCapabilitiesAreTheSameUnderBothClients`).

## Reconnect

As [`redis`](redis.md#reconnect) in shape: the client library owns its
reconnection and reports nothing back, so a lost link shows as the failure of
whatever command met it and as a `Degraded` health transition, not as a
transition of its own. `driverkit.Reconnector` is not used because there is no
dial loop for it to run.

go-redis's own behaviour: a pool connection that fails is discarded and
redialled on the next command, with `DialerRetries` (5) attempts and
`DialerRetryTimeout` (100 ms) between them. Command retries are off
, so a failure reaches the driver's error mapper rather than
being absorbed — which is what lets `MapNet` classify a post-write failure as
Ambiguous.

Maintenance-notification handling (`CLIENT MAINT_NOTIFICATIONS`) is left at
go-redis's default (`auto`): on a server without it the library disables the
feature on the error, and its background worker stops with the client.

## At-least-once window

As [`redis`](redis.md#at-least-once-window). The lease is **this driver's**,
not the broker's: Redis grants none, nothing expires and nothing redelivers, so
the mandatory `XAUTOCLAIM` sweep is the only thing that hands a dead consumer's
entries to another. `Close`'s latency is bounded by `Config.Block` and not by
`Close`: neither client aborts an in-flight blocking read, so the fetch loop
returns `ErrClosed` when the block expires and it looks again.

A `Fetch` cut short by its context's deadline closes the connection its read
was on. An entry the server handed to that read is pending in this consumer's
name and comes back through the sweep after `AckDeadline`, as on rueidis.

## Config & DSN

`goredis.Config` **is** `redis.Config` — a type alias, not a copy — and
`goredis.ParseDSN` is `redis.ParseDSN`. A program that already configures the
Redis driver changes one import and one call.

**This module registers no DSN scheme.** A DSN naming `redis` has to mean one
thing, and which client library serves it is a build-time choice a connection
string has no business making; that is `redis.Scheme`'s own documented
decision and this is the other half of it. `TestTheSchemeBelongsToTheBrokerAndNotToTheClient`
holds it.

`goredis.UniversalOptions(cfg)` is the configuration translated into go-redis's
options, exported so that a caller building its own client starts from the
driver's answer rather than guessing. Four fields in it are decisions:

| Field | Value | Why |
|---|---|---|
| `MaxRetries` | `-1` | a retried `XADD` is a duplicate the caller is told is one publication; a cluster's redirect loop ignores it, so every command is also marked `NoRetry` |
| `ReadTimeout` | `Block + DialTimeout` | a blocking read legitimately takes the whole block  |
| `ContextTimeoutEnabled` | `true` | without it a caller's deadline never reaches the socket |
| `PoolSize` | go-redis's default | the connection model IS what the specification measures; pinning it would measure a pool this driver invented |

Anything the typed configuration cannot carry — a Sentinel or Ring topology, a
pool size, a custom dialler, a hook, instrumentation — goes through
`OpenWithClient`, with `driverkit.ClientBorrowed` or `ClientOwned` saying who
closes it.

The driver does not change a client you pass in. Three of its settings meet
the driver's own. Its `MaxRetries` does not reach the driver's commands, which
are marked `NoRetry`, and your other commands keep it. Its `ReadTimeout` bounds
the block: a `Block` left at its default is shortened to fit, and one you set
that does not fit is refused. Its `ContextTimeoutEnabled` decides whether a
context's deadline reaches the socket, so a client built without it makes
every `Publish` and `Fetch` wait for the server up to `ReadTimeout`, whatever
its deadline. Set it when you build the client.

## Known gotchas

- **A cancelled `Fetch` waits for its block.** A context cancelled without a
  deadline is not seen until the server answers, so it ends a parked `Fetch`
  up to one `Block` later than on rueidis. A deadline is seen.
- **A client you pass in without `ContextTimeoutEnabled` ignores deadlines.**
  The driver cannot turn it on for you without changing your client.
- **A go-redis hook sees the driver's commands under a type of the driver's
  own**, which carries the `NoRetry` mark around a `*redis.Cmd`. A hook that
  type-switches on `*redis.Cmd` does not match them; `Name`, `Args`, `Err` and
  the rest of `Cmder` answer as before. For the same reason a cluster command
  policy you install yourself that fans a keyless command out to every shard
  cannot hand the merged reply back, because go-redis sets it only on a
  `*redis.Cmd`. go-redis's own policies cover only the `ft.` commands, which
  this driver does not send.
- **A hook added with a Ring's `AddHook` does not see every command.** A
  batch, a durable publication and `ListTopics`' `SCAN`s run on the shard's own
  client, so only a hook added to each shard (through `OnNewNode`, as
  redisotel does) sees them. A cluster client's nodes behave the same way, and
  a hook on a single-server client sees everything.
- **`ListTopics` on a Ring scans the shards the Ring reports live.** A shard
  it has marked down is skipped, as the Ring itself skips it for every key, so
  a topic written to that shard is not listed until the shard is back.
- **A client of a type this driver does not know** (your own wrapper of a
  `UniversalClient`, for example) gets a plain pipeline for a durable
  publication's `XADD` and `WAITAOF`. That is correct for a wrapper around a
  `*redis.Client` and not for one around a cluster or a Ring client.

## Maturity & tier

See the pinned client version and the module tests when evaluating this driver.

## Driver-specific critical tests

Beyond the shared suite, this module holds the two clients to each other. That
is the whole reason a second implementation of one contract is worth having.

| Test | What it holds |
|---|---|
| `TestBothClientsMapOneServerReplyToOneCode` | one server reply, two clients, one `mq.ErrorCode` — the divergence that found |
| `TestBothClientsRenderOneReplyAsOneValue` | a script of every reply shape a stream command produces, rendered identically, and the two servers' own state equal afterwards |
| `FuzzRedisClientReplyEquivalence` | an arbitrary reply written on the wire by `internal/streams/respsrv` and read by both shipped adapters (1.06M executions clean) |
| `TestCapabilitiesAreTheSameUnderBothClients` | every claim and every limit, under both |
| `TestKeyRoutingIsDecidedByTheCommandsKeyAndNotAtRandom` | a `redis.Ring` over two servers: one stream's commands must all land on one shard, and the read must reach the shard the writes did |
| `TestADurablePublicationOnARingWaitsOnTheShardThatWroteIt` | a `ConfirmDurable` publication on a `redis.Ring`: the `WAITAOF` runs on the shard that took the `XADD` |
| `TestListTopicsOnARingListsEveryShardsTopics` | `ListTopics` on a `redis.Ring` lists every topic once, whichever shard holds it |
| `TestGoRedisDoesNotRetryAWrittenXAdd` | an `XADD` whose reply is late, through a client with go-redis's default retries, a Ring, the cluster client the driver dials and `DoMulti`: one entry and an error, not two entries and success |
| `TestPublishReturnsAtItsDeadline`, `TestFetchReturnsAtItsDeadline` | a deadline bounds the socket on the client the driver dials |
| `TestABlockingReadWaitsOutTheBlockRatherThanTimingOut` | a 6-second `BLOCK` over go-redis's 5-second default read timeout |
| `TestPipelinedResultsAreIndexAligned` | a pipeline with a refusal in the middle: the commands behind it still ran and their results are where they belong |
| `TestTheFieldLayoutRoundTripsEveryMessageThisClientAccepts` | binary safety through this library, message by message |
| `TestAckOfAStolenDeliveryIsRefused`, `TestExtendDoesNotStealTheEntry` | the disqualifier and, over this client |
| `TestConvertRendersEveryReplyGoRedisCanProduce` | every Go type `proto.Reader.ReadReply` can return, including the ones no command in this driver produces |

`internal/streams/respsrv` is what makes all of that possible with no
container: a real TCP server speaking real RESP whose command semantics are
`internal/streams/fakestreams`, with the fake's settable clock and inspectable
pending list still underneath.

## Conformance: the diff against `redis-streams.json`
