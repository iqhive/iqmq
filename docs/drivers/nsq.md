# nsq

<!-- Fixed sections per the driver documentation. Every section must be present. -->

**Maturity:** Experimental · **Tier:** 3 · **Owner:** @qoke

## Client & version policy

The native baselines for this driver live in `benchmarks/native-baselines/` and measure the clients below against the harness's `nsq` image (`make bench-native CLIENTS="nsq"`). A driver number is only comparable with a baseline taken from the same client version, broker version and confirmation boundary, so the pins move together; each baseline's package comment records the settlement boundary it measures as well.

| Baseline | Client | Pinned version | Confirmation boundary |
|---|---|---|---|
| `nsq` | github.com/nsqio/go-nsq | `github.com/nsqio/go-nsq@v1.1.0` | nsqd OK response to PUB/MPUB (memory-backed until mem-queue-size) |

### Verified facts (the API contract)

Checked while building the native baselines; re-verify when a pin moves.

- **size limits.** nsqd's defaults cap a single message at 1 MiB (--max-msg-size) and a whole command body at 5 MiB (--max-body-size), so an MPUB of 100 64 KiB messages (6.4 MiB) must be split into smaller chunks.
  _Verified against: nsqd v1.3.0 flag defaults; verified by the MPUB chunking in the nsq baseline._
- **deferred publish and discovery.** go-nsq exposes per-message delays through Producer.DeferredPublish (DPUB) and discovers nsqd instances through nsqlookupd, which requires nsqd to broadcast an address the client can reach.
  _Verified against: go-nsq v1.1.0 producer.go:233; conformance/harness/brokers.go NSQ._
- **output buffering.** nsqd buffers writes to each client and flushes them on a timeout the client sets in IDENTIFY: go-nsq defaults to output_buffer_size 16 KiB and output_buffer_timeout 250 ms, so a single small message can take a quarter of a second to arrive on an otherwise idle connection while a 64 KiB message fills the buffer and leaves at once. A latency-sensitive consumer must lower the timeout, and the client warns that going below 25 ms costs nsqd noticeable CPU.
  _Verified against: go-nsq v1.1.0 config.go:165-172; visible in BenchmarkNative/nsq/EndToEnd, where 64 B and 1 KiB round trips take ~250 ms and 64 KiB takes ~380 us._
- **channels.** A topic with no channel discards what is published to it, so a channel must exist (SUB, or the /channel/create HTTP endpoint) before the first publish or messages are lost.
  _Verified against: nsqd v1.3.0 behaviour; verified by the nsq baseline's provisioning step._

## Topic / subscription mapping

| iqmq | NSQ |
|---|---|
| topic | an nsqd **topic**, `Config.TopicPrefix` prepended verbatim |
| subscription | an nsqd **channel** |
| `Message.Key` | nothing native — see *Headers & limits* |
| `Message.Headers` | nothing native — see *Headers & limits* |
| `DeliveryInfo.Attempt` | nsqd's `Attempts`, 1 on the first delivery |
| `DeliveryInfo.LeaseExpires` | the receipt time plus the negotiated `msg_timeout` |
| `DeliveryInfo.Partition`, `Offset`, `Sequence` | never set: nsqd has none of them |

A channel is both the fan-out and the consumer group: every channel of a
topic receives a copy of every message, and every consumer of one channel
competes for its messages. A subscription therefore cannot be empty — there is
no such thing as an unnamed channel — which is the opposite of the SQS profile,
where the subscription is empty because a queue has no fan-out.

The mapping is a **pure prepend and never an escape**. nsqd's grammar is
`^[.a-zA-Z0-9_-]+(#ephemeral)?$`, one to sixty-four bytes, and a portable topic
outside it is *refused* rather than transformed: a mapper that replaced the
illegal bytes would map `a/b` and `a-b` onto one NSQ topic, and a caller would
then find one subscription receiving another's messages.
`nsq.CheckTopic` is checked against go-nsq's own validator
by `FuzzNSQTopicName`, because this driver validates by hand — one
regular-expression match per publication is not free — and two spellings of one
grammar drift the day somebody edits either.

A topic carrying a pattern metacharacter (`*`, `?`, `#`, `>`) is refused with
`ErrUnsupported` and not with "invalid topic": NSQ has no pattern
subscription at all, and the feature being missing is the more useful thing to
tell a caller who wrote a pattern on purpose. A trailing `#ephemeral` is not a
pattern.

### Topology

nsqd creates a topic on the first publication to it whatever any client asks,
which is a property of the broker rather than a choice this driver makes. The
**channel** is not created that way, and that matters: a topic holds its
messages until it has at least one channel and then hands the backlog to the
channel that appears — a *second* channel created afterwards starts empty. So a
consumer that subscribes after the publications it wants has missed them.

`Config.EnsureTopology` is the deliberate creation, through nsqd's HTTP API, and
it is **off by default**. The specification's "no hidden topology
creation" is about a driver that could provision and chooses not to; the
conformance harness turns it on, because there the kit is the operator.

## Publish path & confirmation boundary

`Publish` is `PUB`, `PublishBatch` is `MPUB`, and a publication carrying a delay
is `DPUB`. All three are answered by nsqd with `OK`, and this driver waits for
that answer.

**The boundary is: nsqd has taken the message into the topic it names.** That
queue is in memory up to nsqd's `--mem-queue-size` and on disk beyond it, so
`OK` survives a consumer crash and does *not* survive an nsqd crash with an
in-memory queue.

| mode | Honoured as |
|---|---|
| `ConfirmNone` | the same round trip: go-nsq has no publish this driver could use without giving up the answer |
| `ConfirmDefault`, `ConfirmAccepted` | nsqd's `OK` |
| `ConfirmDurable` | **refused when the sink is opened**, with `ErrUnsupported` |

A caller's context bounds the wait and not the work: the frame is written by
go-nsq's asynchronous form and the driver selects on the answer and on the
context. **A context that expires after the frame is on the wire makes the
outcome ambiguous** — `IsAmbiguous` is true and `Retryable` is false — because
nsqd may well have taken the publication, and reporting "did not publish" would
make a duplicate look like a loss.

There is no `AsyncSink` and no `FlushSink`: a publication is a completed round
trip by the time `Publish` returns, so there is nothing admitted to flush.

With several nsqd addresses configured, publications go round-robin and **there
is no failover**: the specification gives retry policy to the core, and a driver that tried
the next endpoint would turn one publication into two attempts with the core's
own count blind to the second. The core's next attempt lands on the next
endpoint anyway, because the cursor has moved.

### Batch splitting

`MPUB` is one command carrying many bodies and one answer, so a partial outcome
is not something nsqd reports. A batch larger than `Config.MaxBodyBytes`
(nsqd's `--max-body-size`) is **split** into as many `MPUB`s as it needs,
counting the command's own framing, and each piece is its own command with its
own outcome. A batch can therefore half succeed, and the `*BatchError`'s `Errs`
are index-aligned with the messages — every index a failed command carried gets
that command's error, and no index gets another message's outcome.

A batch carrying a delay cannot use `MPUB` at all, because `DPUB` takes one
body; such a batch is published one message at a time.

`FeatureAtomicBatchPublish` is **not** claimed.

## Consume path

`nsq.Consumer.AddConcurrentHandlers` delivers by calling the driver and the SPI
asks the driver to be called, so `driverkit.PushAdapter` sits between them:

- `MaxInFlight` is the core's resolved **prefetch**, so nsqd's own RDY
  accounting is the primary bound and the ring is sized to hold everything it
  can send;
- the ring's capacity is that prefetch, and `Push` **blocks** when it is full,
  which is the backpressure a broker that delivers by callback understands;
- the handler pool is `min(prefetch, 8)`. It does not need to be the prefetch:
  a handler goroutine only moves a delivery into the ring, and what the pool
  size bounds is how many deliveries sit in a *blocked* push — each of them a
  message whose nsqd timeout is running down while nothing works on it, so
  fewer is safer rather than slower.

`DisableAutoResponse` is set on **every** delivery, before anything else can
return, and the handler always returns `nil`. Both halves are load-bearing:
without the first, go-nsq's handler loop would `FIN` the message the moment the
callback returned — before the application had seen it — and with an error
return it would requeue the message with a backoff of its own.

A delivery whose body this driver cannot decode is **requeued with a delay and
counted**, because nsqd's verbs are finish, requeue and touch and none of them
says "this is not my message". `Backend.Undecodable()` is the count. The
health state does not change: the connection that carried the record is
healthy, and a degraded state would make the next successful `Ping` report a
reconnection.

`Fetch` **drops** a delivery that went dead while it sat in the ring — one
whose lease expired, or which a later delivery of the same message superseded.
Handing it out would make the handler's work certainly wasted and its
acknowledgement certainly refused. `Backend.Stale()` counts them.

### Discovery

`Config.LookupdAddrs` is preferred over `Config.Addrs` whenever it is set, and
the preference is not a tie-break: a consumer connected straight to the nsqd
addresses never learns about an nsqd that joins the cluster later, so on a
cluster a direct connection silently consumes a *subset* of the topic.
With one nsqd the two are equivalent and the direct form is
one fewer moving part. Publication always goes to `Config.Addrs`: nsqlookupd is
a discovery service for consumers and cannot be published through.

## Ack / nack / extend mapping

| iqmq | NSQ | Notes |
|---|---|---|
| `Ack` | `FIN` | |
| `Nack` (requeue) | `REQ(delay)` | nsqd's own delayed redelivery |
| `Nack` (drop) | `FIN` | the specification calls it destructive and it is: finishing is nsqd's only way to make a message go away |
| `Nack` (dead letter) | — | `ErrUnsupported`; see *Dead-letter queue* |
| `Extend` | `TOUCH` | not a settlement |
| batch settlement | — | nsqd has no batched verb, so no `BatchSettler` |

**nsqd answers none of the three.** It replies to `PUB`, `MPUB` and `DPUB` and
to nothing else, so the settlement boundary here is *the frame was handed to
the connection*. Every correctness question a broker would otherwise answer is
therefore answered locally, before the frame is written, and this is the part of
the driver worth reading twice.

### The local lease check

nsqd will accept a `FIN` for a message whose timeout has already expired, and
after a redelivery that `FIN` finishes **another consumer's work**. There is no
verb that asks "do I still hold this?". The specification makes "lets a stale ack settle a
later redelivery" a disqualifier rather than a rough edge, so the driver keeps:

- a local view of each delivery's deadline, moved by `TOUCH` and read before
  every settlement, with `Config.LeaseGuard` of slack;
- a map from nsqd message identifier to the delivery it currently vouches for,
  so a **redelivery supersedes** its predecessor the moment it arrives;
- a delivery **generation**, so a settlement carrying a stale one is refused.

A settlement for a delivery that fails any of these is refused with
`ErrLeaseLost` and **nothing is written** at that moment. A delivery that
failed only the deadline is given back with `REQ(0)` shortly afterwards, for
the reason under *The lease sweeper*. A second settlement of a delivery
that is still ours is `ErrAlreadySettled`, which is a different answer for a
different reason: one says the work was accounted for once, the other says it
was never accounted for and the message is coming back.

### The lease sweeper

The driver **gives a delivery back one `LeaseGuard` before its deadline**, with
`REQ(0)`, if nothing has settled or extended it. It costs nothing that expiry
would not have cost — nsqd would have requeued the message a moment later — and
it turns the local check from a diagnosis into a prevention: with it, a delivery
is superseded only by a redelivery the driver did not see coming.

`Config.LeaseGuard` is therefore two things at once, and a caller using the
core's lease extension has to keep them in step: it is how much slack the
settlement check demands, and it is how early the delivery is given back. A
caller that opts into `mq.WithLeaseExtension(lead)` must choose a `lead`
LONGER than `LeaseGuard`, or the driver requeues the delivery before the core
gets round to extending it and a long handler never keeps its lease at all.
The default lead is zero — extension is opt-in — so the default configuration
has nothing to trip over.

The guard also bounds the deadline. A consumer's own `AckDeadline` has to be
longer than `LeaseGuard`, as the backend's does: at or below it, every delivery
is inside its guard the moment it arrives, so `Fetch` drops it and the sweeper
gives it back, and nothing is ever handed out. Opening such a consumer is
refused with `CodeInvalidConfig`. `MinAckDeadline` is below the default
`LeaseGuard`, so the shortest deadline nsqd accepts is one of them unless the
guard is lowered too.

Once a deadline has passed, nsqd may already have redelivered the message, so
the driver never writes a `FIN` or `TOUCH` for that delivery. It still owes
nsqd an answer, and that matters more than it sounds: go-nsq counts one
response per *delivery* on each connection, does not finish closing a
connection until that count is zero, and does not reconnect to that nsqd until
the close has finished. An unanswered delivery on a connection that drops
therefore stops the subscription receiving from that nsqd at all. go-nsq does
not tell the driver when a connection is closing, so the driver cannot wait for
the moment a frame is harmless.

When a redelivery arrives and supersedes the old delivery, the old one's debt
is **chained onto the delivery that superseded it** and paid once that one has
been answered, when nsqd no longer holds the identifier in flight and the
frame is refused harmlessly. When none arrives, the lease sweeper answers the
old delivery with **`REQ(0)` on its next pass but one**, one to two sweep
intervals (half a `LeaseGuard` each) after a settlement or a `Fetch` found the
lease gone. The wait gives a redelivery that is already on its way time to
arrive and take the debt over, which is the common case shortly after a deadline.

nsqd accepts a `REQ` only from the client holding the message, so the sweeper's
frame never touches another consumer's delivery, and on a dropped connection
it goes nowhere but still settles go-nsq's count, so the consumer reconnects.
The one case where it changes anything is a redelivery to this same connection
that nsqd sent and the driver has still not received when the sweep comes
round. nsqd requeues that redelivery, the driver hands it out as live, and its
`Ack` later finds nothing in flight at nsqd, so the message is delivered once
more. That is a **duplicate and never a loss**, and it is why the frame is a
`REQ`: a `FIN` in the same place would finish a message whether or not anyone
had done its work.

### `TOUCH` is not an addition

nsqd's `TOUCH` **resets** a message's timeout to the channel's negotiated
`msg_timeout`, measured from now, and caps a message's total in-flight time at
`--max-msg-timeout` from its first delivery. So:

- what a granted `Extend(by)` **buys** is the channel's timeout from now,
  whatever `by` asked for, so a `by` longer than that timeout is refused with
  `CodeInvalidArgument` and nothing is written. The SPI promises at least
  `by`, and nsqd would redeliver the message one timeout later underneath a
  handler that was told it had longer. The timeout is the consumer's
  `AckDeadline`, which is the `msg_timeout` its connection negotiates, and it
  is exactly `DeliveryInfo.LeaseExpires` minus `ReceivedAt`. That difference is
  what the core's lease scheduler asks for, so its extensions are never
  refused;
- `Extend(by)` is also refused when it would end past `Config.MaxAckDeadline`
  from the delivery, which mirrors that cap.
  `Capabilities().Limit(LimitMaxLease)` is that cap, the longest a delivery
  can be held across many extensions, and not what one extension buys;
- `Extend(0)` or less is `CodeInvalidArgument`: nsqd would reset the timeout to
  the channel's default, which for a short remaining lease is an extension
  nobody asked for and for a long one is a shortening.

An extension leaves the delivery settleable. The settler has a *peek* beside its
*claim* for exactly that: a driver whose `Extend` took the delivery's state
would make the acknowledgement that follows it fail, and the message would be
processed twice for no reason but the extension meant to prevent it.

## Ordering

**No ordering is claimed, in any scope.** the specification's "ordering never claimed" is
literal here: nsqd makes no promise about the order in which a channel's
messages are delivered, not even for one producer, one consumer and no
failures — an in-memory queue and a disk-backed queue are drained
independently, and a requeue puts a message back wherever there is room.

A `Conditional` claim with a note would be the wrong answer, because there is no
condition under which it holds. A caller that needs ordering needs the core's
hash lanes over a broker that has it, or another broker.

## Delays

Native, in both directions:

- a **publication** delay is `DPUB`, bounded by `Config.MaxRequeueDelay`
  (nsqd's `--max-req-timeout`, default one hour) and advertised as the specification's
  `MaxDelay`. `PublishOptions.DeliverAt` is converted to a delay against the
  injected clock, and an instant already past is "deliver now" rather than an
  error;
- a **redelivery** delay is `REQ(delay)`, bounded the same way.

`RequeueWithoutBackoff` and not `Requeue`: go-nsq's `Requeue` sets the
consumer's RDY to zero for the whole subscription while a backoff of its own
runs, so one message's failure would stop every other message. The delay is the
core's and the client library does not get a second one.

Scheduled publication is nsqd's own and needs no emulation, so nothing here is
`EmulatedVolatile` and no `Emulation` policy is consulted.

## Dead-letter queue

**No native route.** `FeatureDeadLetter` means the driver knows where a dead
letter goes and can get it there in one native settlement, and nsqd has no verb
for it — so `Nack(NackDeadLetter())` is refused with `ErrUnsupported` rather
than emulated inside the driver.

the specification's "ED" for this broker is the **core's** emulation: `mq.WithDeadLetter`
publishes the message to a topic and then acknowledges the original, which 
classes as semantics-preserving and which needs nothing from this driver. It
carries the duplicate risk the specification requires to be disclosed: the publication and
the acknowledgement are two operations and a failure between them means the
message exists in both places.

`mq.WithNativeDeadLetter()` is refused by the core against this driver, which is
the rule that a capability the driver does not have is refused rather than
emulated in silence.

## Headers & limits

**NSQ has no headers at all** — not a limited set, not string-only values. The
protocol carries a body and nothing else. So the promise of ordered,
duplicate-preserving, binary-capable headers is kept here by the portable
envelope of `github.com/iqhive/iqmq/wire` and **by nothing else**, and the same
goes for the message key, the message identifier and the typed properties.

the four modes therefore reduce to three behaviours:

| Mode | Behaviour |
|---|---|
| `MetadataNativeStrict` (default) | a message carrying **any** header, identifier, typed property or key — including an *empty* key, because the specification makes nil and empty different — is refused with `ErrUnsupported` **before any I/O**, naming the field |
| `MetadataPortableEnvelope` | every message is enveloped; a non-iqmq consumer reads the envelope rather than the body |
| `MetadataAdaptive` | the messages that carry metadata are enveloped and a bare body is left alone |
| `MetadataLenientDrop` | the metadata is discarded and the body is published; `Backend.DroppedMetadata()` counts what went |

`Config.Metadata` is the deployment's **one-time** "yes, envelope on this
broker". The specification makes the header-less envelope an interop-affecting emulation,
available under `EmulationAll` or an explicit metadata mode, and the SPI carries
no emulation policy at all — so there is nowhere else to say it once rather than
on every publisher and consumer. A handle that names a mode
keeps it: the field replaces the zero value and nothing else, so an explicit
`MetadataPortableEnvelope` is never downgraded and an explicit
`MetadataLenientDrop` is never upgraded. A handle that names
`MetadataNativeStrict` *explicitly* is indistinguishable from one that named
nothing, which is why the conformance run documents
`metadata/native-strict-rejection` as inapplicable.

With the envelope configured, the three header features are advertised as
`EmulatedDurable` with the emulation stated, because the headers really do
travel and are as durable as the message. Without it they
are `Unsupported`.

Two of this driver's reasons to envelope are invisible to
`driverkit.Envelope.Needed`, which reads the capability table
:

- a **message key**. The inventory has no native-key feature, so a keyed
  message with no other metadata answers "nothing to do" and would go out with
  its key discarded, silently.
- an **empty body**. nsqd answers a zero-length `PUB` with `E_BAD_MESSAGE`, so
  the nil-versus-empty distinction survives here only inside an envelope,
  which is never empty. In the two non-enveloping modes an empty message
  **cannot be published at all**, and that is refused before the round trip.

Under `MetadataAdaptive` a body that *begins with the envelope's magic* is
enveloped too, even though nsqd could carry it natively. With no headers there
is nowhere to record which records are enveloped, so the body is the only
evidence there is — and a driver that published such a body natively would
decode a payload as metadata on the way back, or leave a record it could neither
deliver nor settle. `wire.Detector.DetectFull` is the read side of the same rule: it
claims a body only when the body is, exactly and entirely, a canonical envelope.
A producer that deliberately publishes a valid envelope as a payload is still
indistinguishable from one that enveloped it; that is a property of a broker
with nothing out of band, and `drivers/sqs` avoids it with a message attribute
because it has one.

### Limits

Every limit this driver advertises **mirrors an nsqd flag** rather than a value
nsqd reports: nsqd publishes none of its limits over either the TCP protocol or
its HTTP API, so an operator who moves a flag must move the configuration with
it.

| limit | Config field | nsqd flag | Default |
|---|---|---|---|
| `MaxMessageBytes` | `MaxMessageBytes` | `--max-msg-size` | 1 MiB |
| `MaxBatchBytes` | `MaxBodyBytes` | `--max-body-size` | 5 MiB |
| `MaxDelay` | `MaxRequeueDelay` | `--max-req-timeout` | 1 h |
| `MaxLease` | `MaxAckDeadline` | `--max-msg-timeout` | 15 min |
| `MaxBatchMessages`, `MaxHeaderCount`, `MaxHeaderBytes` | — | — | **not advertised**: nsqd bounds no such thing and the specification forbids guessing |

Two more flags are mirrored as *refusals* rather than as limits, because nsqd
enforces them in the `IDENTIFY` handshake and a client that asks for less has
its whole connection refused: `Config.OutputBufferTimeout` must be at least
`--min-output-buffer-timeout` (25 ms) and an ack deadline at least
`--min-msg-timeout` (1 s). Both are checked by `Config.Validate`, which is the specification's
"validate config before starting background work" — the alternative is every
operation on the client failing with an error about a handshake.

The size check is applied to **what will be written**, which for an enveloped
message is the envelope: a 1 MiB body plus its headers is over nsqd's default
even though the body alone is not, and finding that out from nsqd costs a round
trip and an `E_BAD_MESSAGE`.

## Reconnect

go-nsq owns the reconnection, in both halves: a producer redials on the next
publication, and a consumer either re-polls nsqlookupd or retries the direct
address every `Config.LookupdPollInterval`. This driver adds no loop of its own,
which is why there is no `driverkit.Reconnector` here.

The consequence is stated rather than hidden: **go-nsq offers no reconnection
callback**, so the specification's `OnReconnect` has only one source on this driver — a
`Ping` that succeeds after a failure. It is coarse, and a caller that never
calls `Ping` never sees one. `Backend.Health()` is what
carries the state, and `Backend.Reconnects()` and
`Backend.DegradedTransitions()` are the numbers.

A lost connection makes nsqd requeue everything that connection was holding, at
once. Every delivery from it is therefore superseded when its message comes back
and every settlement for it is refused with `ErrLeaseLost` — which is the same
machinery as an expired lease and is tested the same way.

`Config.HeartbeatInterval` must be less than `Config.ReadTimeout` or go-nsq
reads with a deadline nsqd's heartbeat cannot beat, and every idle connection
looks dead. `Validate` refuses it.

## At-least-once window

The window is between nsqd accepting a message and the driver's `FIN` reaching
it, and a message is delivered again whenever that window is interrupted:

- the per-message timeout expires — or the lease sweeper hands the message back
  one guard earlier, deliberately;
- the connection holding it goes away, in which case nsqd makes every message
  on it available at once;
- the source is closed, which requeues everything it has handed out and never
  settled;
- the lease sweeper gives back a delivery whose lease has passed, and its
  `REQ` lands on a redelivery still on its way to the same connection (see
  *The lease sweeper*);
- a `REQ` for any reason, including the core's retry schedule.

**A duplicate is normal and an ordering change is normal with it.** There is no
de-duplication and no idempotent publish: `FeatureDeduplication`,
`FeatureIdempotentPublish` and `FeatureExactlyOnceDelivery` are all
unsupported, and a handler has to be idempotent.

Two narrower windows are worth naming. A publication whose context expires while
the frame is in flight is **ambiguous**, so a core retry of it may produce a
second message. And a `FIN` is a frame nsqd does not answer, so a `FIN` lost
with the connection produces a redelivery of a message the application had
finished.

## Config & DSN

```go
client, err := nsq.Open(ctx, nsq.Config{
    Addrs:        []string{"nsqd-a:4150", "nsqd-b:4150"},
    LookupdAddrs: []string{"http://lookupd-a:4161", "http://lookupd-b:4161"},
    AdminAddrs:   []string{"nsqd-a:4151", "nsqd-b:4151"},
    Metadata:     mq.MetadataAdaptive,
    AckDeadline:  30 * time.Second,
})
```

The DSN form is the same configuration as a connection string
(`nsq.ParseDSN`). The authority is the nsqd **TCP** endpoints, a host with no
port takes 4150, and the path is `Config.TopicPrefix`:

```
nsq://nsqd-a:4150,nsqd-b:4150/acme.?lookupd=http://lookupd:4161&metadata=adaptive&ackdeadline=30s
```

The parameters are listed on `ParseDSN`, and an unknown one is a construction
error naming it. Three decisions in the DSN are not obvious:

- `lookupd=` and `admin=` are **comma-separated lists**, because the shared
  parser refuses a repeated key outright and that refusal is right for every
  scalar it was written for ;
- `max_in_flight` is **not a parameter**, though the example DSN shows one.
  Prefetch is the core's: it is resolved from the consumer configuration and
  handed to the driver, and a DSN that set it would be a second owner of one
  number that the driver would then have to choose between, silently
  ;
- `Config.AdminAddrs`, when empty, is derived from the TCP addresses by adding
  one to each port — nsqd's own default layout, and a **guess** rather than a
  discovery, which is why the field is explicit.

`Config.AuthSecret` is a `dsn.Secret`: neither `Config.String()`, nor
`Config.LogValue()`, nor `Config.DSN()`, nor any error this driver builds can
carry it.

### Administration

`Client.Admin()` is nsqd's **HTTP** API — `/topic/create`, `/topic/delete`,
`/channel/create`, `/channel/delete`, `/channel/empty`, `/stats` — because that
is where nsqd puts topology; the TCP protocol has none.

Every one of those endpoints is answered by *one* nsqd about *itself*.
nsqlookupd knows which nsqd holds a topic but cannot create, delete or purge
anything, so an administrative call has to reach every nsqd the deployment has:
this driver sends each one to **every** address in `Config.AdminAddrs` and
reports the first failure, and `Stats` **sums** what they answer. A deployment
whose `AdminAddrs` name a subset of its nsqd instances gets a partial answer.

`QueueStats.Lag` is never known: a channel holds what has not been finished and
keeps nothing else, so there is no position in a log to be behind. The specification makes
that an unknown rather than a zero.

The HTTP requests go over a transport of the backend's own, cloned from
`http.DefaultTransport` when the backend opens: a proxy or dialer an application
set there still applies, nothing else in the process can close its connections
under a request in flight, and `Close` releases them. An
admin call after `Close` returns `ErrClosed`. With `Config.TLS` set the requests
are HTTPS and verify each nsqd with `Config.TLS` against that nsqd's own host
name, exactly as the protocol connection does -- a `ServerName` in `Config.TLS`
is ignored by both.

**nsqlookupd discovery is the third HTTP surface, and its scheme is the
operator's to choose.** `Config.TLS` is nsqd's TLS, on the protocol connection;
what discovery does is decided by the scheme in each `LookupdAddrs` entry, and
go-nsq prepends `http://` when there is none. So a scheme-less or `http://`
lookupd address is **cleartext discovery even with `Config.TLS` set**, and that
is not refused: nsqlookupd is a different service from nsqd, a TLS nsqd behind a
plain-HTTP nsqlookupd on a private network is a real layout, and the traffic
carries no credential, though only because this driver turns go-nsq's
`LookupdAuthorization` off: that field defaults to **true**, and with an
`AuthSecret` configured the discovery poll carried
`Authorization: Bearer <secret>` in the clear. Measured, and the opposite of what
was first recorded. The secret now goes to nsqd's AUTH
handshake and nowhere else. What it discloses is the topic name in the query and
the nsqd host names in the answer. Write `https://` and `Config.TLS` is what the
poller uses — its roots, its client certificate and its `InsecureSkipVerify`.
That last part was not true before: go-nsq builds its own lookupd transport with
`TLSClientConfig` unset, so an `https://` lookupd verified against the system
roots and a private CA failed as an unknown authority. Both halves are measured
on the wire.

## Known gotchas

1. **A channel must exist before the publication you want it to see.** A topic
   hands its backlog to the first channel that appears; a channel created later
   starts empty. Use `Config.EnsureTopology` or `Admin.EnsureSubscription`.
2. **`OK` is not durable.** nsqd's queue is in memory up to
   `--mem-queue-size`, so a publication acknowledged by an nsqd that then dies
   uncleanly is gone. `ConfirmDurable` is refused rather than silently weakened.
3. **An empty message cannot be published** in a mode that does not envelope:
   nsqd answers a zero-length body with `E_BAD_MESSAGE`.
4. **go-nsq's defaults would break the core's retry accounting**, and this
   driver turns two of them off. `MaxAttempts` defaults to five and what go-nsq
   does when a message passes it is call `FINISH` — a message discarded by the
   client library while the core was still retrying it, which the specification makes a
   disqualifier. `MaxBackoffDuration` defaults to two minutes and its backoff
   sets RDY to zero for the whole subscription. Both are zero here, and
   `TestCriticalGoNSQRetriesAreOff` is what keeps them there.
5. **`output_buffer_timeout` has a floor of 25 ms and a default of 250 ms.**
   go-nsq's default adds a quarter of a second to the latency of every
   otherwise-idle delivery; this driver's default is nsqd's floor, and nsqd
   refuses anything below it in the `IDENTIFY` handshake.
6. **A `#ephemeral` topic or channel is deleted when its last client goes
   away**, taking whatever it held. The name is legal here and the consequence
   is nsqd's.
7. **Two nsqd instances are two independent brokers.** There is no
   replication, no partitioning and no cross-instance ordering; a topic exists
   separately on each nsqd that has seen a publication for it, and a consumer
   without nsqlookupd sees only the ones it was told about.
8. **`Attempts` is nsqd's and counts deliveries**, including the ones this
   driver refused to hand out because their lease had gone. A retry budget
   keyed on it is therefore slightly conservative, which is the safe direction.
9. **`Config.LeaseGuard` must be shorter than the core's lease-extension
   lead.** It is how early the driver gives a delivery back as well as how
   much slack a settlement demands, so a `mq.WithLeaseExtension(lead)` with
   `lead` below it means the driver requeues the delivery first and the
   extension never happens. Extension is opt-in and the default lead is zero,
   so a default configuration has nothing to trip over.
10. **The conformance run lowers the fidelity figure from 100,000 to
   10,000.** Every publication here is a `PUB` answered by nsqd, and the
   scenario publishes them one at a time; the specification asks a driver that lowers the
   figure to say so, and this is saying so.

## Maturity & tier

See the pinned client version and the module tests when evaluating this driver.

## Driver-specific critical tests

The specification names "NSQ deferred requeue, backoff, lookupd changes" for this driver.
They are `TestCritical*` in `drivers/nsq`, run by `make critical DRIVER=nsq`
and by `make ci`, against `nsqfake` — an in-process model of a channel's
message lifecycle, because every one of them is a decision the **broker** makes
and a real nsqd cannot be asked to make any of them on demand.

| Test | What it holds |
|---|---|
| `TestCriticalDeferredRequeue` | three requeues with rising delays: each comes back only after its own delay, with its attempt count one higher, and never through a client-library backoff |
| `TestCriticalGoNSQRetriesAreOff` | twelve negative settlements, past go-nsq's default `MaxAttempts` twice over, and the broker finishes nothing: a message the client library gave up on would have been finished and never delivered again |
| `TestCriticalLookupdIsPreferredOverDirectAddresses` | a configuration with a lookupd discovers through it, and one without connects directly |
| `TestCriticalARedeliveryAfterAConnectionLossIsNotSettledTwice` | eight messages made available again at once: every settlement from the first round is refused and none of them reaches the broker |

The model reproduces nsqd's exact laxity — a `FIN`, `REQ` or `TOUCH` names a
message *identifier* and lands on whatever is under it — and **counts** every
settlement that landed on the wrong delivery attempt. A model that refused a
stale settlement would make the driver's defence untestable, because the test
would pass with the defence removed.

Beside them, `TestCloseRacesTheHandlerForThousandsOfRounds` is this
repository's recurring defect class in the shape it takes here: a delivery
placed in the ring while the source is closing. Three thousand rounds, every
other delivery deliberately left unsettled, and the conservation law it asserts
is that after `Close` every message the broker took is either finished or back
in the channel and **none** is still in flight.
