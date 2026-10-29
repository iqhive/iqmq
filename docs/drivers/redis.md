# redis

<!-- Fixed sections per the driver documentation. Every section must be present. -->

**Maturity:** Experimental · **Tier:** 2 · **Module:** `github.com/iqhive/iqmq/drivers/redis` · **Profile:** `streams`

## Client & version policy

The native baselines for this driver live in `benchmarks/native-baselines/` and measure the clients below against the harness's `redis` image (`make bench-native CLIENTS="rueidis goredis"`). A driver number is only comparable with a baseline taken from the same client version, broker version and confirmation boundary, so the pins move together; each baseline's package comment records the settlement boundary it measures as well.

| Baseline | Client | Pinned version | Confirmation boundary |
|---|---|---|---|
| `rueidis` | github.com/redis/rueidis | `github.com/redis/rueidis@v1.0.79` | XADD reply from the server (appendfsync everysec, no WAIT) |
| `goredis` | github.com/redis/go-redis/v9 | `github.com/redis/go-redis/v9@v9.23.0` | XADD reply from the server (appendfsync everysec, no WAIT) |

### Verified facts (the API contract)

Checked while building the native baselines; re-verify when a pin moves.

- **reclaim support.** XAUTOCLAIM, which a stream driver needs to reclaim entries from dead consumers without scanning the pending-entries list, requires Redis 6.2 or newer; the harness pins redis 7.4 and offers valkey 8.1 as a matrix image.
  _Verified against: Redis command documentation (XAUTOCLAIM, since 6.2.0); conformance/harness/images.go._
- **rueidis argument model.** rueidis builds commands from string arguments, so a []byte payload must go through rueidis.BinaryString — the documented conversion that does not copy — and its auto-pipelining lets one connection carry a whole batch through DoMulti.
  _Verified against: rueidis v1.0.77 binary.go:17 and client.go:89._
- **value size.** A Redis string value, and therefore a stream field value, is limited to 512 MiB; the practical limit for a message payload is set by memory and by proto-max-bulk-len (512 MiB by default), not by the client.
  _Verified against: Redis data-type documentation; redis.conf default._
- **`XACK` does not check ownership.** `XACK key group id` removes the entry from the group's pending-entries list whoever is holding it and whatever its delivery counter is. There is no token, no receipt and no lease, which is why this driver's acknowledgement is a guarded script — see *Ack / nack / extend mapping*.
  _Verified against: Redis 7.4 `XACK` documentation and behaviour; the ack guard's own suite._
- **Redis grants no lease and redelivers nothing.** A consumer group hands an entry to a consumer and then does nothing about it for ever. Redelivery is entirely the client's, through `XAUTOCLAIM`/`XCLAIM` on idle time, which is why the reclaim loop is mandatory rather than an optimisation.
  _Verified against: Redis 7.4 consumer-group documentation._
- **`XCLAIM ... JUSTID` does not increment the delivery counter, and a plain `XCLAIM` does.** That is what makes `JUSTID` usable as a lease extension and a plain claim unusable as one.
  _Verified against: Redis 7.4 `XCLAIM` documentation._
- **`XCLAIM` with `min-idle 0` claims an entry however recently it was delivered.** So an extension written as a bare `XCLAIM` *steals* the entry from its current holder; `FuzzRedisReclaimer` found it in 1.3 seconds.
  _Verified against: Redis 7.4 `XCLAIM` semantics._
- **`XGROUP DELCONSUMER` deletes the consumer's pending entries with the consumer.** A tidy-up that removed a consumer still holding entries would delete those messages from the group for ever.
  _Verified against: Redis 7.4 `XGROUP DELCONSUMER` documentation._
- **`WAITAOF` arrived in 7.0 and fails outright when `appendonly` is `no`.** It is therefore the *conditional* durable boundary, and both conditions are facts about the deployment rather than things a caller can turn on.
  _Verified against: Redis 7.0 release notes; `WAITAOF` documentation._
- **`XADD` refuses an entry with no field-value pairs.** So a message with nothing in it still has to produce a field.
  _Verified against: Redis 7.4 `XADD` behaviour; the publish/roundtrip-bodies scenario._
- **A stream entry's fields are an ORDERED list of binary-safe strings**, and a stream identifier's high half is the publication time in milliseconds. The first is why this driver needs no portable envelope; the second is why seek-by-time is a real position rather than an approximation.
  _Verified against: Redis 7.4 streams documentation; RESP bulk-string encoding._
- **Redis Cluster refuses a script whose keys are on two hash slots** (CROSSSLOT). That is why every key of a topic carries the topic's hash tag.
  _Verified against: Redis Cluster specification; `FuzzRedisKeyNaming`._

**Minimum server: Redis 6.2.** The driver refuses to open against anything
older, and the refusal is not a policy preference: the mandatory
pending-entries reclaim loop is `XAUTOCLAIM`, and without that loop **nothing
on this broker redelivers** — so opening against 5.x would advertise
at-least-once delivery on a server that has at-most-once. When the server will
not report a version (a managed Redis with `INFO` behind an ACL) the driver
asks it directly whether `XAUTOCLAIM` exists, rather than refusing a server it
can actually use.

Valkey is supported as the same profile. It reports `valkey_version` beside
`redis_version` and the driver prefers its own key: a Valkey reporting
`redis_version: 7.2.4` for compatibility is not a Redis 7.2.4, and the version
matrix has to tell them apart.

## Topic / subscription mapping

| iqmq | Redis Streams | Notes |
|---|---|---|
| `Topic` | stream key `{topic}` | hash-tagged, so every shard of a topic is on one cluster slot |
| `Topic` shard *i* > 0 | `{topic}:<i>` | shard 0 is the bare tag, so adding a partition never renames the stream that already holds every message |
| `Config.KeyPrefix` | inside the tag: `{prefix.topic}` | a prefix OUTSIDE the tag would leave the tag as the bare topic, so two deployments' same-named topics would collide on one slot |
| `ConsumerConfig.Subscription` | consumer group | two consumers of one subscription compete; two subscriptions each receive everything |
| one `Source` | one consumer name | `<prefix>-<sub>-<nonce>`, unique per source instance — see *Ack / nack / extend mapping* |
| `Message.Key` | chooses the shard (FNV-1a) | and travels in the `k` field, so it survives the round trip |
| dead-letter destination | `Config.DeadLetterTopic` or `{topic}` + `Config.DeadLetterSuffix` | **neither is set by default**; see *Dead-letter queue* |
| delayed redelivery (zset arm) | `{topic}:delayed`, `{topic}:<i>:delayed` | a sorted set per shard, scored by deliver-at time |

**A topic is one cluster slot, and that is a trade rather than an oversight.**
Every key of a topic carries the topic's hash tag, so shards buy ordering
lanes and parallel consumers *within* a subscription and do **not** spread a
topic across a cluster's nodes. The price buys the thing this driver cannot do
without: the dead-letter move, the delayed park and the delayed release are
each one Lua script touching two of a topic's keys, and a cluster refuses a
script whose keys are on two slots. A single very hot topic scales by being
split into several topics, not by adding partitions.

`Config.Partitions` is a **client-side** setting with no server-side record. A
deployment that changes it changes which shard a key hashes to, so messages
already in the old shards stay there and per-key ordering is broken across the
change. Treat it as a topic's shape, decided once.

### Entry field layout

| Field | Content |
|---|---|
| `b` | the body. Absent for a nil body, present-and-empty for an empty one |
| `k` | the key, with the same nil-versus-empty rule |
| `t` | the logical topic |
| `h:<n>` | header *n* in publication order, framed as `<decimal key length>:<key><value>` |
| `p:<n>` | typed property *n*: `0` identifier, `1` content type, `2` content encoding, `3` correlation id, `4` causation id, `5` reply-to, `6` created-at as `<seconds>,<nanoseconds>` |
| `x:*` | the driver's own bookkeeping, never message content: `x:a` the carried attempt count, `x:d` a deliver-at time, `x:e` the marker that makes an otherwise empty message publishable |

The header framing is a **length** and not a separator because every byte —
the separator included — is a byte a header value may contain. The property
indices are part of the wire format and are append-only.

`p:6` is two integers rather than one nanosecond count: a single
unix-nanosecond count overflows an `int64` outside 1678–2262, and a time a
caller can construct must not become a different time on the way back. The one
thing the round trip does not preserve is the LOCATION: an entry carries none,
so a `CreatedAt` set in a named zone comes back as the same instant in UTC.

`x:e` exists because `XADD` refuses an entry with no field-value pairs, and a
message with a nil body, a nil key, no topic, no headers and no properties
encodes to nothing.

## Publish path & confirmation boundary

`Publish` is `XADD <key> [NOMKSTREAM] [MAXLEN ~ n] * <fields...>`, in this
order:

1. the per-call controls this broker cannot honour are refused, **before any
   I/O**;
2. the entry layout's limits are checked, in **every** metadata mode;
3. the shard is chosen: an explicit partition, then the key's hash, then 0;
4. the `XADD` goes to the server;
5. under `ConfirmDurable` only, `WAITAOF` waits for the append-only file.

| Mode | Boundary |
|---|---|
| `ConfirmNone` | the `XADD` is written; its reply is not waited for |
| `ConfirmAccepted` (default) | `XADD`'s reply: the entry is in the server's memory and will be served to a reader. **Nothing about disk is implied.** |
| `ConfirmDurable` | `WAITAOF <local> <replicas> <timeout>`: the write has reached that many append-only files |

`ConfirmDurable` is **conditional** and is refused at **construction** on a
server that cannot define the boundary — Redis below 7.0, or `appendonly no`.
A publisher that discovered it at its first message would already have told
the application it was safe to forget the data.

A `WAITAOF` that comes back **short** of the number asked for is reported as
an **ambiguous** outcome, not a failure: the entry IS in the stream and may yet
be fsynced, so a caller told "failed" would republish and duplicate it.

**Batch publication is native and NOT atomic.** rueidis pipelines the whole
batch into one flush, so a hundred messages cost one round trip; Redis executes
pipelined commands independently, so a partial failure is a partial batch. The
result is index-aligned with the input. `FeatureAtomicBatchPublish` is
unsupported, and a batch containing a message the limits refuse is refused in
full before anything is written — the specification puts the refusal before the write, and
half a caller's batch in the stream is worse than none of it.

**Refused before any I/O:** `Delay`, `DeliverAt`, `TTL`, `Priority`,
`DedupKey`, a partition the topic does not have, and any message over the
entry layout's limits.

### The message body is copied, and the go-redis arm does not copy it

This driver copies the message body into every command it sends;
[`drivers/redis/goredis`](goredis.md) does not. It is a statement rather
than a performance note, and it is the reason for part of the breach on
this profile.

rueidis queues a command for a background writer, and a cancelled `Do` returns
`ctx.Err()` while the command is still in that queue — `_backgroundWrite` then
writes its tokens from another goroutine, after `Publish` has returned. 
gives the caller its buffer back at that moment, so a body that aliased the
caller's slice would put whatever the caller wrote next on the wire: a corrupt
entry in the stream, reported as nothing. go-redis writes in the calling
goroutine, so the body is on the wire before `Publish` returns and aliasing is
safe there.

What this means for you: on this driver you may reuse a body buffer freely.
On the go-redis arm you may too — the specification is the same promise on both — but a
`redis.Hook` you install through its `OpenWithClient` that STORES the `Cmder`
and reads its arguments after its own call returns must copy what it stores,
because the body argument is the publisher's buffer and the publisher has it
back.

`Delay` is refused even though the zset nack arm *can* hold a redelivery back,
and the distinction is not pedantry: a delayed publication is a message that
must not be deliverable yet, and a delayed redelivery is a message that has
already been delivered once. Reusing the retry machinery for the first would
mean acknowledging a message nobody has consumed. `TTL` is refused even though
a stream has `MAXLEN`: `MAXLEN` is the whole stream's retention, measured in
entries, and cannot expire one message out of the middle.

## Consume path

`XREADGROUP GROUP <group> <consumer> COUNT <prefetch> BLOCK <block> STREAMS
<keys...> ><...>`, plus two background loops.

- **`COUNT`** is `ConsumerConfig.Prefetch`, resolved by the core.
- **`BLOCK`** is `Config.Block` (default 5 s) and is **never zero**: `BLOCK 0`
  means *for ever* in Redis, and a fetch that cannot be woken is what 
  forbids. `Close` is therefore prompt to within one block.
- **`NOACK` is never set.** It is at-most-once delivery: the pending-entries
  list is where a redelivery comes from, so a `NOACK` read is a read whose
  messages cannot be recovered from a crash.
- **The reclaim sweep** runs every `Config.ClaimInterval` (default one third of
  the deadline) and is **mandatory** — see the next section. Each round is one
  `XPENDING` and one `XAUTOCLAIM` per shard, pipelined. Under the `stop` nack
  arm a source sweeps at least every third of its own acknowledgement
  deadline, which can be shorter than the backend's (see *Delays*), though
  never more often than every 10ms on that account.
- **The delayed mover** runs every `Config.MoverInterval`, and only under the
  zset nack arm.

A delivery the sweep produced waits in the source's buffer until a `Fetch`
takes it, and the buffer holds **one delivery per entry**: a later sweep that
re-claims a buffered entry replaces it rather than queueing a second
generation, because the older generation's settlement would be refused.

A buffered delivery's lease (`DeliveryInfo.ReceivedAt` and `LeaseExpires`) is
counted from the read or the claim that produced it, not from the `Fetch` that
hands it out. The read or the claim is what started the entry's idle clock, and
another consumer's sweep takes the entry a deadline after it, so a delivery
that waited in the buffer reaches its handler with less than a whole deadline
left.

`DeliveryInfo.Sequence` is the entry identifier **packed**: the millisecond in
the high bits and the within-millisecond sequence in the low twenty. It is
monotone, so comparing two deliveries' sequences gives the publication order,
and exact, so `StartAtSequence` resumes at the entry rather than at the start
of its millisecond — which on a busy stream would re-deliver everything
published in it. Above 2^20 entries in one millisecond the
sequence saturates, which makes a resume start slightly early rather than
slightly late.

| `StartPosition` | Redis |
|---|---|
| `StartLatest` (default) | `$` — the entries added from now on |
| `StartEarliest` | `0-0` — the beginning of **whatever the stream still holds**, which is not "every message ever published" if the stream was trimmed |
| `StartAt(t)` | `<t in ms - 1>-18446744073709551615`, the last identifier before t's millisecond. Redis has no seek-by-time; a stream identifier's high half IS the publication time, so this is a real position at millisecond resolution. `XGROUP CREATE` takes the last entry the group has seen, so the group's first delivery is the first entry of t's millisecond, the one published at t included |
| `StartAtSequence(n)` | the packed identifier, unpacked |

Start positions apply when the consumer group is **created**. A group that
already exists has a committed position and is left alone, which is the specification's
idempotence; moving an existing one is `Admin.Purge` or a replay.

## Ack / nack / extend mapping

| iqmq | Redis |
|---|---|
| `Ack` | a Lua script: check the pending row's consumer, then `XACK` |
| `Ack` with `AckGuard: none` | a bare `XACK` |
| `AckBatch` | the same script over a batch of identifiers, one command per shard, pipelined |
| `Nack` | whichever of the four arms is bound — see *Delays* |
| `Nack(NackActionDrop)` | the guarded `XACK`: on this broker a drop IS an acknowledgement |
| `Nack(NackActionDeadLetter)` | a Lua move — see *Dead-letter queue* |
| `Extend` | a Lua script: check the pending row's consumer, then `XCLAIM ... JUSTID` |

### Why the acknowledgement is guarded

**`XACK` removes a pending entry whoever is holding it.** So

```
consumer A reads entry E          (E pending, owner A)
A's handler is slow
consumer B runs XAUTOCLAIM        (E pending, owner B)
B starts processing E
A finishes and sends XACK E       (E leaves the pending list)
B crashes before finishing
```

**loses the message**: A acknowledged work B was doing, B never finished it,
and nothing will ever deliver E again. The specification calls that a disqualifier.

Two mechanisms close it, because the two cases have different evidence:

1. **Within one source**, a generation table. Every entry handed out is
   registered with a generation, and handing it out again — which is what the
   reclaim sweep does when a handler outlives its lease — supersedes the
   previous one. A settlement carrying the superseded generation is refused
   **before any command is sent**. Exact, and free.
2. **Across processes**, the server's own pending row, which records the
   consumer holding the entry. Every source gets a consumer name unique to
   that instance, so "the row names somebody else" is exactly "somebody else
   claimed it" — and the guard checks it inside the same Lua script as the
   `XACK`, because checking it in the client and acknowledging afterwards
   leaves the window the check was for.

The consumer name's uniqueness is therefore **load-bearing** and is not
configurable: `Config.ConsumerPrefix` sets the prefix and the driver always
appends a nonce. Two sources sharing a name would each believe the other's
deliveries were their own.

`AckGuard: none` turns (2) off: one command instead of one script call, and
the loss window above open. It is correct only when nothing else reclaims from
the same group, which in practice means a single consumer process.

### Why the extension is guarded too

`XCLAIM key group me 0 <id> JUSTID` is a **claim**. `min-idle 0` means "no
matter how recently it was delivered", so an extension written as a bare
`XCLAIM` takes the entry from its current holder and reports success — after
which two consumers each believe the delivery is theirs. `FuzzRedisReclaimer`
found that in 1.3 seconds.

`Extend`'s `by` argument is **not sent**. Redis has no "extend by": the lease
is this driver's, its length is `Config.AckDeadline`, and what the extension
does is reset the idle clock the deadline is measured from. `JUSTID` is what
keeps it from incrementing the delivery counter, so extending a lease twenty
times does not make a slow handler look like a message that keeps failing and
trip the specification's `MaxAttempts`.

So an extension buys one `AckDeadline` from now, and a `by` longer than the
source's `AckDeadline` is refused with `InvalidArgument` before anything is
sent. Accepting it would report the lease as moved, and the sweep would reclaim
the entry one deadline later underneath the handler. A delivery the sweep has
already handed out again answers `ErrLeaseLost` first, whatever it asked for.

### Attempt counting

`DeliveryInfo.Attempt` is the sum of two halves, and it has to be:

- **the broker counts** — `XAUTOCLAIM` increments a pending entry's delivery
  counter, and the sweep reads the counter with an `XPENDING` pipelined
  alongside the claim. `XAUTOCLAIM` returns the entries and **not** their
  counters, so without the `XPENDING` every redelivery would report attempt 2
  and `MaxAttempts` would never terminate;
- **a nack arm that re-adds** the entry starts a new pending row whose counter
  begins again at one, so what was lost travels in the entry's `x:a` field and
  is added back.

## Ordering

`FeatureOrderedByPartition` and `FeatureOrderedByKey` are **conditional**;
`FeatureOrderedGlobally` is conditional for a one-shard topic and unsupported
otherwise.

A stream is a total order and a consumer group hands out entries in it, so one
consumer of a subscription sees a shard in publication order. What breaks it,
stated rather than omitted:

- **two consumers of one subscription** interleave across the shard — that is
  what a consumer group is for;
- **two of the four nack arms re-add** the entry under a new identifier, so a
  retried message is delivered after everything published since it failed. The
  capability note names the bound arm, so a program can ask rather than assume.

`Config.Partitions > 1` removes the global order: four shards are four
independent total orders and nothing sequences them against each other. A key
chooses its shard by FNV-1a over the key bytes, which is stable across
processes and across restarts — a key that hashed differently in two
publishers would put one key's messages in two shards and break per-key
ordering with nothing failing.

A message with **no key** goes to shard 0 rather than to a random or
round-robin shard. The specification makes a keyless message unordered so any shard is
correct, and shard 0 is the one that needs no hash; the cost is that a keyless
workload on a partitioned topic does not spread.

## Delays

| `Config.Nack` | Redelivery | Delay | Keeps position | Pending list |
|---|---|---|---|---|
| `leavepending` (default) | the reclaim sweep, after one acknowledgement deadline | only up to the deadline; a longer one is **refused** | yes | one entry per in-flight message |
| `zsetmover` | a mover releases the parked entry when it is due | **arbitrary**, to within `MoverInterval` | no | empty |
| `readd` | immediate | **refused** | no | empty |
| `stop` | the reclaim sweep, and the shard is held until the entry is settled | only up to the deadline | yes, strictly | one entry per in-flight message |

**A delay an arm cannot honour is refused with `ErrUnsupported` and settles
nothing**, which is the rule. `leavepending` and `stop` accept a delay
*shorter* than the deadline — the redelivery then arrives later than asked,
which is within "held back for at least this long" — and refuse a longer one,
because that would arrive **early**.

A delayed nack under either arm restarts the entry's idle clock with one
guarded `XCLAIM … JUSTID` script, which leaves the delivery counter alone: the
reclaim sweep counts idle time from the last delivery, so without the restart a
nack made late in the lease came back before its delay. A nack with no delay
sends nothing.

The zset arm's park is `ZADD` plus `XACK` in one Lua script and its release is
`ZREM` plus `XADD` in another, so a crash cannot leave the message both
pending and parked (a duplicate) or neither (a loss), and two movers racing
over one due entry produce exactly one redelivery — the `ZREM` decides. It is
advertised as `EmulatedDurable` **with the condition stated**: a parked entry
lives in a sorted set, so it survives exactly what the append-only file or the
snapshot preserves.

A parked member carries the entry's source identifier, and that is not
decoration: a sorted set is a **set**, so two identical messages parked for the
same instant would otherwise be one member and one of them would be silently
discarded.

`stop` is the only arm with head-of-line blocking, and it is the point of the
arm: nothing behind the failed message is delivered until it is settled, so a
strictly ordered stream survives a retry. One poisonous message stops its
shard until `MaxAttempts` dead-letters it.

The hold covers more than the next read, because entries behind the failed
message can already be on their way:

- **Every failed message of a shard holds it.** When two fail before either
  is settled, the shard reads nothing new until both are, and the second is
  retried only after the first.
- **A fetch already waiting at the server** when the nack lands is answered
  with whatever was published during its block. The source keeps those
  entries pending and hands them out after the retry, in order. Each sweep
  restarts their idle clocks, so the wait is not counted as an attempt and no
  other consumer claims them. A source under this arm sweeps at least every
  third of its own deadline for that reason, even when the backend's interval
  is longer. The read that hands them out restarts the clocks once more, so
  their leases start with that read.
- **Entries the source read but has not handed out yet** (a read takes up to
  `Prefetch`, and a fetch hands out only as many as its caller has room for)
  are nacked with the failed message. They stay pending and come back through
  the sweep after it, counted as a second attempt. The same goes for what a
  read or a sweep brings back while the nack lands, before the source has
  buffered it. An entry among them that cannot be decoded is dealt with at
  once instead, as `Fetch` would deal with it: it never reaches a handler, so
  it has no order to keep, and a hold on it would never be lifted.
- **With every shard held, `Fetch` waits** for a settlement, a sweep round,
  its context or `Close`, at most one `Block` at a time, rather than going
  straight round its loop.
- **A failed message trimmed out of the stream** (by `MaxLen`, `XTRIM` or
  `XDEL`) before its retry can never be delivered again, so its hold lifts at
  the next sweep and the source acknowledges its pending row. On Redis 6.2 the
  hold used to stop its shard for good: `XAUTOCLAIM` there claims a trimmed
  entry like any other and answers a null, so its row stays this consumer's.
  A retry already with its handler keeps the hold until it is settled.

## Dead-letter queue

**There is no dead-letter destination unless the configuration names one**, and
a `Reject` without one reports `ErrUnsupported` and settles nothing.
That is worth stating first, because a driver that derived a destination from
the topic's name — which this one did until the suite ran against a real
server — makes a `Reject` succeed on a subscription nobody gave a destination
to, and the message is moved to a key the operator has no consumer for.

| Setting | Destination | Move |
|---|---|---|
| `Config.DeadLetterSuffix` | `{topic}` + suffix | **one Lua script**: copy and acknowledge in one step |
| `Config.DeadLetterTopic` | that topic's own key, with its **own** hash tag | two commands, and a documented duplicate window |
| neither | none | `Reject` is `ErrUnsupported` |

The script does **not** `XDEL` the entry from the source stream: a dead-letter
is a decision about *this subscription*, and deleting the entry would remove it
for every other consumer group on the topic.

The copy leaves the source's bookkeeping behind: no `x:a` attempt count and no
group stamp, so the dead-letter subscription's first delivery of it is attempt
1 and not a redelivery.

**The two-command form's window is the cost of a destination on another
slot.** A cluster refuses a script whose keys are on two slots (CROSSSLOT), so
the move becomes `XADD` then the guarded `XACK` — and a crash between them
leaves the message in the dead-letter stream **and** pending in the source, so
it is dead-lettered twice. A deployment that cannot tolerate that uses
`DeadLetterSuffix`, whose destination shares the topic's tag by construction.

**An entry the source cannot decode is dead-lettered as soon as it is read**,
when a destination is configured, by the same move a `Reject` makes. It never
reaches a handler, so `MaxAttempts` never counts it, and a later attempt cannot
decode it either. With no destination it stays pending, the reclaim sweep brings
it back every `AckDeadline`, and `Health` reports it as `Degraded`.

## Headers & limits

`FeatureNativeHeaders`, `FeatureBinaryHeaders` and `FeatureDuplicateHeaders`
are all **native**, which is not the usual answer for a broker with no header
field. A stream entry is an ordered field-value list of binary-safe strings, so
the entry layout carries the whole promise in the entry itself: positional
`h:<n>` fields keep the order and allow a repeated key, and a RESP bulk string
carries any byte.

The consequence is the opposite of most drivers': **the portable envelope is
never needed here.** `MetadataAdaptive` behaves exactly like
`MetadataNativeStrict`, and `MetadataLenientDrop` never drops anything. Only
`MetadataPortableEnvelope` differs, and it differs because the **caller** asked
for a wire format rather than because the driver needed one.

**That mode is also the driver's one real limit, and it is checked before any
I/O in every mode.** `driverkit.Envelope` encodes anything and decodes under
`wire.Limits`, so a message admitted above those bounds is published
successfully and then breaks every `Fetch` on that subscription for ever — an
unsettleable record the driver made out of a publication it accepted.
The bound is `wire.DefaultLimits`, field for
field, applied in every mode so that a message accepted in one is accepted in
all:

| Bound | Value |
|---|---|
| key | 64 KiB |
| body | 64 MiB |
| identifier | 256 B |
| headers | 1024 |
| header key | 256 B |
| header value | 64 KiB |
| string property | 4 KiB |
| logical topic | 512 B |

Plus one that is not a length: the identifier, every header **key** and every
string property must be **valid UTF-8**, because the envelope's decoder
validates them and its encoder does not. `FuzzRedisFields` found that in a
tenth of a second. Header *values*, the key and the body are not checked: the
envelope carries those as bytes.

`Capabilities.Limits` advertises the message size and the header count, which
are the two has fields for. Redis's own `proto-max-bulk-len` (512 MiB by
default) is **not** advertised: it is a server setting this driver does not
read, and the specification forbids advertising a default as a fact.

A **topic name** may not contain `{`, `}`, a carriage return or a newline: the
first two would break the hash tag and put a topic's shards on different slots,
and the last two break every operator tool that reads a key list.

## Reconnect

What the driver does do is recover from the one thing a reconnection breaks:
**an empty script cache.** A server that restarted, failed over or ran `SCRIPT
FLUSH` has forgotten every script, and every settlement route here is an
`EVALSHA` — the guarded acknowledgement, the guarded extension, the
dead-letter move, the delayed park, the delayed release, the re-add. A
`NOSCRIPT` is answered by sending the command again as `EVAL` with the
script's body, **on every route**, because a driver that recovered on only some of them would stop
being able to acknowledge anything after a failover while still being able to
publish. `EVAL` carries the command's own keys, so on a cluster it reaches
the node whose cache was empty, and that node caches the script as it runs it.
A `SCRIPT LOAD` names no key, so a cluster client sends it to any node, and the
node that answered `NOSCRIPT` could stay empty. In a pipeline only the
commands that met `NOSCRIPT` are retried: the others succeeded, and re-sending
an `XADD` is a second copy of the message.

`Health` (the specification) reports `Connecting`, then `Ready`, and goes `Degraded` on
three background failures, each of which is invisible from the caller's side:

- **a reclaim sweep that failed.** The sweep is the only thing that
  redelivers, so a sweep that keeps failing is a subscription that has
  silently stopped being at-least-once.
- **an entry that cannot be decoded.** It is a record in the stream that
  nothing can deliver; it is left pending, which hands it to `MaxAttempts` and
  the dead-letter route rather than being delivered as an empty message.
- **a delayed mover that failed.** It is a parked redelivery that is not
  happening.

## At-least-once window

A delivery is at-least-once between `XREADGROUP` and the guarded `XACK`.
Inside that window:

- **the message is duplicated** if the handler completes and the
  acknowledgement's round trip fails, or if the handler outlives
  `Config.AckDeadline` and the sweep redelivers while it is still running;
- **the message is NOT lost** if the consumer dies: the entry stays in the
  pending-entries list and the sweep — this consumer's successor, or another
  consumer — claims it once it has been idle for the deadline;
- **the message is NOT acknowledged by somebody else's mistake**: that is what
  the guard is for, and `AckGuard: none` is the setting that gives it up.

The worst-case redelivery latency is `AckDeadline + ClaimInterval` (by default
30 s + 10 s), because the entry becomes claimable at the deadline and the
sweep notices at its next pass.

**`Close` abandons rather than requeues**, and that is the honest answer rather
than the convenient one. There is no "unclaim" command: an unsettled entry
stays in the pending list and becomes claimable once it has been idle for the
deadline, so another consumer's sweep picks it up. The one command that would
look like a prompt hand-back — `XGROUP DELCONSUMER` — deletes the entries along
with the consumer, so it runs only when the pending list is empty.

**Durability is the server's.** `ConfirmAccepted` means the entry is in memory;
a server that has not fsynced and then loses power loses it. `ConfirmDurable`
is the boundary that says otherwise, and it needs an append-only file.

## Config & DSN

    redis://user:password@host:6379,host2:6379/0?partitions=4&nack=zsetmover

The path is the logical database number, which is Redis's own convention. A
database beside several addresses is **refused**: a cluster has only database
0, and ignoring the setting would talk to a database the operator did not
choose.

| Parameter | `Config` field | Default |
|---|---|---|
| `keyprefix` | `KeyPrefix` | none; goes **inside** the hash tag |
| `partitions` | `Partitions` | 1 |
| `maxlen` | `MaxLen` | 0 — **streams are not trimmed** |
| `autocreate` | `AutoCreate` | false |
| `ackdeadline` | `AckDeadline` | 30 s |
| `claiminterval` | `ClaimInterval` | `AckDeadline`/3 |
| `claimcount` | `ClaimCount` | 128 |
| `block` | `Block` | 5 s; never 0 |
| `nack` | `Nack` | the bake-off winner (`leavepending`); `auto` is refused as a DSN value |
| `ackguard` | `AckGuard` | `owner` |
| `consumerprefix` | `ConsumerPrefix` | host name and process id; a nonce is always appended |
| `moverinterval`, `movercount` | `MoverInterval`, `MoverCount` | 250 ms, 128 |
| `durablelocal`, `durablereplicas` | `DurableLocal`, `DurableReplicas` | 1, 0 |
| `durabletimeout` | `DurableTimeout` | 5 s |
| `dialtimeout` | `DialTimeout` | 5 s |
| `deadlettertopic` | `DeadLetterTopic` | **none** |
| `deadlettersuffix` | `DeadLetterSuffix` | **none** (`:dlq` is the value a deployment usually gives it) |
| `tls`, `insecureskipverify` | `TLS`, `InsecureSkipVerify` | off |

A parameter this driver does not have is a **construction error naming it**,
not a setting nobody notices was ignored. `insecureskipverify=true` without
`tls=true` is refused: the alternative is a connection that dialled in the
clear after the operator asked to skip verification.

`Config.String()`, `Config.LogValue()` and `Config.DSN()` all redact the
credential. Anything a typed `Config` cannot carry — a Sentinel
configuration, a custom dialler, client-side caching, an authentication
callback — is reached through `OpenWithClient`, where the caller builds the
`rueidis.Client` and says who closes it.

**Trimming is explicit.** `MaxLen` is zero by default, because a stream is a
log and a driver that trimmed by default would delete a subscription's unread
messages to save memory nobody said was short. When it is set it is the
approximate form (`MAXLEN ~ n`): exact trimming walks the whole radix tree,
which is a latency spike on the publish path.

## Known gotchas

- **A topic is one cluster slot.** Partitions do not spread a topic across
  nodes. See *Topic / subscription mapping*.
- **`Config.Partitions` has no server-side record.** Changing it re-hashes
  every key.
- **`StartEarliest` is not "every message ever published"** — it is the
  beginning of whatever the stream still holds, and a trimmed stream has
  forgotten the rest.
- **A consumer row per source instance.** The consumer name carries a nonce,
  so `XINFO CONSUMERS` grows with the number of sources a process has opened.
  `Close` removes the row when the source holds nothing pending; a source that
  closed with entries in flight leaves one, and it goes when a sweep has taken
  those entries away and something closes cleanly. An operator who sees a long
  list is seeing crashed processes, which is information rather than a leak.
- **`Admin.DeleteSubscription` is destructive in a way `DeleteTopic` is not.**
  `XGROUP DESTROY` removes the group's pending-entries list with the group, so
  every message that subscription had been handed and not settled is
  forgotten. The messages stay in the stream for other subscriptions.
- **`Admin.Purge` does not touch the pending list**, deliberately: a purge is
  about what has not been delivered, and a pending entry has been delivered
  and is somebody's in-flight work.
- **`Admin.Stats`'s lag can be unknown.** Redis reports a nil lag after an
  `XDEL` or an `XTRIM` made the count unreliable, and the specification's `Limit[T]` is how
  that is reported rather than as a zero.
- **An entry trimmed while it was pending.** Redis 7.0's `XAUTOCLAIM` reports
  the identifiers it dropped for that reason and the driver forgets them; on
  6.2 there is no such reply element, so such an entry stays in the pending
  list for ever. That is the pending-list growth the specification asks about, and
  it is a reason to prefer 7.x. The exception is a failed message the `stop`
  arm holds its shard for, which the source looks up in the stream and
  acknowledges once it is gone, on either version.
- **Request/reply is unsupported.** Redis Streams has no reply channel, and
  building one out of a second stream per request would be a feature this
  driver invented rather than a broker capability.
- **Wildcard subscriptions are unsupported.** Redis's pattern subscription is
  pub/sub, which has no consumer groups, no pending list and no
  acknowledgement.

## Maturity & tier

See the pinned client version and the module tests when evaluating this driver.

## Driver-specific critical tests

Beyond the suite, the tests that exist because of *this* broker:

- **`FuzzRedisReclaimer`** (`internal/streams`) — two consumers of one group
  against one server, interleaving reads, sweeps, acknowledgements, nacks,
  extensions and clock jumps. The invariant is that an entry whose pending row
  names consumer X is never removed by consumer Y's acknowledgement. It found
  the unguarded extension in 1.3 seconds.
- **`FuzzRedisFields`** (`internal/streams`) — a message the limits accept
  round-trips through the entry layout **and** through the portable envelope.
  It found the UTF-8 requirement in a tenth of a second.
- **`FuzzRedisKeyNaming`** — every key of one topic is on one cluster slot, and
  a key maps back to its shard.
- **`FuzzRedisDSN`** — the parser never panics and whatever it accepts renders
  back to something safe to log.
- **`FuzzRedisShardRouting`** — a key always reaches the same shard.
- **`TestReclaimDoesNotSettleTheSupersededDelivery`** — the three shapes of the
  disqualifier: another consumer claimed it, our own sweep superseded it, and a
  settlement in flight is not redelivered.
- **`TestGuardNoneLeavesTheWindowOpen`** — the negative direction of the guard,
  so that the positive tests cannot pass because something else refused.
- **`TestExtendKeepsTheAttemptCount`** — twenty extensions do not move the
  delivery counter, and the entry is not reclaimed.
- **`TestExtendLongerThanTheDeadlineIsRefused`** — an extension longer than
  the deadline sends nothing, and the amount the lease scheduler asks for is
  accepted.
- **`TestCloseLeavesPendingEntriesForTheSweep`** — the shutdown path is one
  command away from deleting messages.
- **`TestRepeatedSweepsDoNotStrandABufferedDelivery`** — a redelivery that
  waited in the buffer through four sweeps is still settleable.
- **`TestScriptCacheRecoveryKeepsSettlementWorking`** — a failover empties the
  script cache and settlement keeps working.
- **`TestDeadLetterMoveIsAtomic`** and
  **`TestDeadLetterWithNoDestinationIsRefused`** — the copy and the
  acknowledgement are one command, and a subscription with no destination
  refuses rather than inventing one.
- **`TestEmptyMessageIsPublishable`** — a message with nothing in it is still a
  message.
- **`TestModel/<kind>`** (`internal/streams`) — the four arms against
  one contract.
- **`conformance/matrix/redis_test.go`** — the capability table really does
  differ between Redis 6.2 and 7.x, which is what says it is reading the
  version rather than guessing.
