# rabbitmq (AMQP 1.0 profile)

<!-- Fixed sections per the driver documentation. Every section must be present. -->

**Maturity:** Experimental · **Tier:** 2 · **Owner:** iqmq maintainers

This page is the AMQP 1.0 profile for RabbitMQ 4.0 and newer
(`drivers/rabbitmq/amqp10`, the second arm). The AMQP 0-9-1
driver has its own page, `rabbitmq.md`, and the two are **never silently
substituted** for one another — the specification says so, and the list under "Known
gotchas" is why.

## Client & version policy

The profile is built on `drivers/amqp10`, the shared AMQP 1.0 core, which wraps
`github.com/Azure/go-amqp`. It has no native baseline of its own yet: the
`amqp091` baseline in `benchmarks/native-baselines/` measures the other
protocol's client against the harness's `rabbitmq` image, and a number taken
from it is not comparable with this profile's, because the confirmation
boundary is reached by a different mechanism.

| Baseline | Client | Pinned version | Confirmation boundary |
|---|---|---|---|
| (none yet) | github.com/Azure/go-amqp | `github.com/Azure/go-amqp@v1.7.0` | would be: the peer's `accepted` disposition on a durable quorum queue |

**The version gate is this driver's headline behaviour.** `Open` refuses a
broker that reports anything below **RabbitMQ 4.0** with `mq.ErrUnsupported`
naming both versions, and nothing falls back to AMQP 0-9-1. RabbitMQ 4.0 is the
first release that speaks AMQP 1.0 natively; on 3.13 it is a plugin whose
feature set, address syntax and settlement behaviour differ, so a driver that
accepted either would be claiming this profile's behaviour for something else.
A peer that reports **no** version satisfies the gate no more than an old one
does: the specification makes "the peer did not say" distinct from any claim, and a gate
that read an unknown version as new enough would gate nothing.

A peer whose `product` connection property names something that is not RabbitMQ
is refused **before** the version is read. That is the same rule pointed the
other way: Apache Artemis reports version 2.44, which satisfies no RabbitMQ
gate, and it also auto-creates an address from an attach — so a client pointed
at the wrong endpoint would not fail, it would create an address literally
called `/exchanges/orders/orders` and publish into it for ever. A peer that
names no product at all is accepted, because an AMQP 1.0 proxy in front of
RabbitMQ need not forward the broker's properties; it still has to satisfy the
version gate.

### Verified facts (the API contract)

Checked while building this profile; re-verify when a pin moves. The
**unverified** list is under "Known gotchas" and is longer than this one,
because nothing in this increment ran against a broker.

- **native AMQP 1.0 from 4.0.** RabbitMQ 4.0 is the first release with a native
  AMQP 1.0 endpoint; earlier releases need `rabbitmq_amqp1_0`, a plugin with
  different addressing and different settlement behaviour.
  _Verified against: RabbitMQ 4 release notes; `internal/rabbit/version.go`'s
  `MinBrokerAMQP10`._
- **the address form.** A publication addresses an exchange and a routing key
  together as `/exchanges/<exchange>/<routing-key>`, and a consumer addresses a
  queue as `/queues/<queue>`, because an AMQP 1.0 link carries one address and
  has no second field for a routing key. Each segment is percent-encoded.
  _Verified against: RabbitMQ AMQP 1.0 address-format (v2) documentation;
  `internal/rabbit/protocol/amqp10.go`._
- **no topology from an address.** The endpoint creates no exchange, queue or
  binding: an attach to a queue that does not exist closes the link with
  `amqp:not-found`.
  _Verified against: RabbitMQ AMQP 1.0 documentation ("queues and exchanges
  must be declared out of band"); asserted by `TestProfileShape`'s
  `AutoCreates` row._
- **quorum queues.** A quorum queue replicates to a majority and fsyncs before
  the broker answers a publication, so an `accepted` disposition from one is a
  much stronger boundary than one from a classic queue and must not be compared
  with it.
  _Verified against: RabbitMQ quorum-queue documentation; the same fact
  `docs/drivers/rabbitmq.md` records for the confirm on the other protocol._

## Topic / subscription mapping

The same topology as the AMQP 0-9-1 driver, derived by the same code
(`internal/rabbit`), so that a caller who moves between the profiles finds one
topology rather than two wearing one name. What differs is how a name becomes
an **address**.

| Portable | RabbitMQ | AMQP 1.0 address |
|---|---|---|
| topic `orders` | exchange `<prefix>orders`, routing key `orders` | sink `/exchanges/orders/orders` |
| topic `orders` + subscription `workers` | durable quorum queue `<prefix>orders.workers` | source `/queues/orders.workers` |
| a rejected or expired message | exchange `<queue>.dlx`, queue `<queue>.dlq`, fixed routing key `dlq` | (the queue's own policy; this driver publishes no copy) |
| a wildcard pattern | a binding on a topic exchange | **not expressible** — `ErrUnsupported` |

Three things about it are load-bearing.

**Every segment is percent-encoded, and that is not tidiness.** The address
format is path-shaped and a routing key is arbitrary caller-supplied text: the
topic `orders/eu` is a legal RabbitMQ name, and an unescaped `/` in it would
end a segment, so the link would attach to the exchange `orders` with the
routing key `eu` — somebody else's traffic, with the publisher none the wiser.
The escaping is `internal/rabbit/protocol`'s, shared by both arms, and
`FuzzRabbitAMQP10Address` holds it to being reversible: decoding each segment
returns the name that went in, byte for byte, which is what makes two different
names unable to reach one address.

**Nothing is declared, and nothing can be.** `Config.Declare` exists and is
**refused** with a message naming the three things that can declare topology
(`drivers/rabbitmq`, the management API, `rabbitmqctl`). The field is kept
rather than omitted so that a caller who moves from the other driver finds it
and is told why, instead of not finding it and assuming the topology appears.
`Config.Names` is how a provisioner asks for the names it has to create.

**A wildcard subscription is refused with `ErrUnsupported`,** not with an
invalid-name error. A wildcard is a binding pattern and a receiving link
addresses a queue, so the pattern is not expressible in the portable `Topic` at
all; the refusal has to be `ErrUnsupported` because that is what the specification's
negative probe for `FeatureWildcardSubscribe` reads, and a refusal carrying
`CodeInvalidArgument` would leave the claim unverified in both directions.

## Publish path & confirmation boundary

A publication is one transfer on a sending link attached to the topic's
exchange path.

| Mode | Boundary |
|---|---|
| `ConfirmNone` | the transfer is sent **pre-settled**: the frame leaves, no disposition is coming, the call costs no round trip. Local admission, which the specification says may be lossy |
| `ConfirmAccepted` | the peer's `accepted` disposition **for this transfer**. It is the AMQP 1.0 equivalent of `basic.ack` on a confirm channel, per transfer and with no mode to enable |
| `ConfirmDurable` | the same `accepted` disposition, on a durable **quorum** queue, where RabbitMQ returns it once the message has been written to a majority of the queue's replicas. A classic-queue configuration defines no durable boundary, so a publisher that asks for `ConfirmDurable` against one **fails when its sink is opened** |

**There is no confirm mode and therefore no.** Every transfer
on an unsettled link gets its own disposition, so the per-message,
windowed and asynchronous strategies have no equivalent here: there is nothing
to select. What a caller sizes instead is the link's **credit**
(`Config.Credit`), which is also what `basic.qos` became. It is simpler and it
is not free — there is no cumulative form to amortise a batch across, which is
why this driver's conformance fidelity figure is lowered to 5,000 messages from
the 100,000.

Two publication outcomes deserve naming because a caller acts on them
differently from an ordinary failure.

- **Unroutable.** RabbitMQ **releases** a transfer that an exchange routes
  nowhere. The shared core's generic table reads `released` as the peer
  declining to act — retryable, because another node might take it — and on
  RabbitMQ that is wrong in the way that costs the most: the message is gone,
  the topology is what is wrong, and retrying throws away another copy. So this
  profile adds one rule to the table and it is the only one it adds: on
  `OpPublish`, `released` is **`CodeNotFound`, not retryable, not ambiguous**.
  That is the same answer the specification's "Mandatory + NotifyReturn = Rejected" gives on
  the other protocol, reached by a different mechanism — and it is a rule the
  core could not have written, because only the profile knows that this
  broker's `released` means unroutable rather than "try another node".
- **Ambiguous.** A transfer that has left the socket and whose disposition
  never arrived may have been accepted. The specification makes reporting that as a
  definite failure a disqualifier, so it is `CodeAmbiguous` with `Ambiguous`
  set. A peer's own `rejected` or `released` disposition stays definite, which
  is the half a driver that marked everything ambiguous would destroy.

A target address whose **exchange** does not exist is a third thing again: the
broker closes the link with `amqp:not-found`, which maps to `CodeNotFound`.

## Consume path

A source is a receiving link attached to `/queues/<topic>.<subscription>` with
a credit window. No link is attached when the source is opened: the first fetch
attaches one, so a broker that is down at that instant is a retryable fetch
failure rather than a consumer that could not be constructed.

Credit is the AMQP 1.0 answer to `basic.qos`, and it is the only window this
protocol has, because **it grants no lease at all**. An unsettled delivery is
this receiver's for as long as the link lives, so `DeliveryInfo.LeaseExpires`
is the zero time, `FeatureExtendLease` is unsupported, and there is no
`DeadlineExtender` anywhere in the module. `Config.Credit` (default 100) caps
what one source may hold whatever prefetch the core resolved.

`DeliveryInfo.Attempt` is the AMQP header's `delivery-count` **plus one** — the
header counts redeliveries, so Attempt is 1 on a first delivery. That is a
**protocol field of the header section** rather than RabbitMQ's
`x-delivery-count` header extension, which means the count needs no queue
argument and no header a publisher must be careful not to overwrite. Whether a
**classic** queue's redeliveries reach it is not verified here; the capability
note says so and the claim withdraws on one.

## Ack / nack / extend mapping

Settlement is one disposition each, and the mapping is more direct than
`basic.ack`/`basic.nack`/`basic.reject`: `modified` carries `delivery-failed`
and `undeliverable-here` as separate flags, so a nack says what it means rather
than leaving it to a broker's reading of a requeue.

| action | AMQP 1.0 outcome | Notes |
|---|---|---|
| `Ack` | `accepted` | final when the frame is written under the default `receiversettle=first`; confirmed by the broker under `second`, at a round trip each |
| `Nack` (requeue) | `released` | does **not** count the attempt, which is the protocol's own distinction and not an emulation |
| `Nack` (requeue, delay) | — | **`ErrUnsupported`, and settles nothing.** RabbitMQ has no AMQP 1.0 spelling for "hold this back" |
| `Nack` (drop) | `rejected` | AMQP 1.0 has no destroy outcome, so on a queue with a dead-letter exchange a dropped message reaches `<queue>.dlq` rather than being destroyed. The safe direction, stated rather than hidden |
| `Nack` (dead-letter) | `rejected` | needs `Config.DeadLetter`; without it the action is refused with `ErrUnsupported` and settles nothing, rather than rejecting the message into nowhere |
| `Extend` | — | there is no lease and no frame; `FeatureExtendLease` is unsupported |
| a delivery this driver could not decode | `modified{undeliverable-here}` | so that it goes back to the queue without this link being handed it again |

A settlement the driver cannot perform is refused **before** the delivery's
slot moves, which is the specification's "returns `ErrUnsupported` and settles nothing": a
driver that took the slot first and then discovered it could not honour the
action would leave the delivery unsettleable for ever.

## Ordering

A queue is FIFO, so one queue's messages are delivered in the order the
exchange routed them, and `FeatureOrderedByPartition` is claimed at group
scope. Nothing above one queue is ordered, and `FeatureOrderedGlobally` is not
claimed.

**Ordering by key is not claimed on this profile, and the other one claims it
conditionally.** On AMQP 0-9-1 the routing key is a field of every
`basic.publish`, so `WithKeyAsRoutingKey` makes one key's messages reach one
queue. Here the routing key is part of the link's **address**, so it is fixed
when the link attaches and cannot vary per message. Honouring the option would
need the anonymous-terminus extension — a sender attached to a null target with
each message's `to` property naming the address — which the shared core does
not implement: a sink is bound to one address for its lifetime.
`Config.KeyAsRoutingKey` is therefore **refused** rather than accepted and
ignored.

## Delays

**None, in either direction, and neither absence is a limitation of this
module's code.**

A delayed **redelivery** has nothing to express it: a `modified` disposition
makes the message available again at once, and RabbitMQ has no annotation that
holds one back. The other profile builds one from a TTL queue that dead-letters
back or from the delayed-message plugin; the first needs
topology this profile cannot create, and the second is unverified — see below.
`NackOptions.Delay` is refused with `ErrUnsupported` before any I/O and settles
nothing.

A **scheduled publication** has nothing either. The
`rabbitmq_delayed_message_exchange` plugin reads an `x-delay` **header** on
AMQP 0-9-1, and which AMQP 1.0 section and key that becomes — a message
annotation, an application property, neither — is **not verified here**. 
forbids claiming a capability that cannot be proved, so the profile carries no
delay rule at all and `PublishOptions.Delay` and `DeliverAt` are refused before
any I/O. The alternative would be worse than a refusal: a message sent without
its delay is delivered immediately to a publisher who asked for it in an hour.

`Limits.MaxDelay` is therefore left **unknown** rather than advertised as zero,
which the specification makes a different answer.

## Dead-letter queue

`<queue>.dlx` and `<queue>.dlq`, derived from the queue's own name by
`internal/rabbit`, with a **fixed** dead-letter routing key: RabbitMQ
dead-letters a message with its original routing key unless the queue names a
replacement, so a fan-out-shaped DLX with one binding would route a message
keyed `orders.eu` nowhere at all.

The route is the **queue's own `x-dead-letter-exchange` policy** and not
something this driver publishes to. A `rejected` disposition is what reaches
it. `Config.DeadLetter` records that whoever declared the queues set the
policy, which is what makes `FeatureDeadLetter` claimable; without it the claim
is withdrawn and a dead-letter `Nack` is refused.

The profile supplies no `DeadLetterAddress` to the shared core, deliberately.
Supplying one would make the core publish a copy itself and then reject the
original — one more copy, one more window, and a second dead-letter mechanism
for a broker that already has one.

## Headers & limits

A portable header is an **AMQP application property**, and that choice is
interoperability rather than taste. RabbitMQ converts between its two protocols
by mapping application properties to and from the AMQP 0-9-1 field table, so a
header this driver put in the **message-annotation** section would be in a
different place on the other protocol — and a message published through one
RabbitMQ driver would read back without that header through the other. The
profile's `AnnotationPrefix` is therefore **empty**, even though RabbitMQ's own
controls live under `x-` annotations: a caller who needs an annotation reaches
past the portable API.

An application-property value is typed, so a header value that is not valid
UTF-8 travels as an AMQP binary and comes back byte for byte —
`FeatureBinaryHeaders` at native level, as on the other profile, by a better
mechanism. A **repeated key** cannot travel at all, because
application-properties is a map: `MetadataPortableEnvelope` and
`MetadataAdaptive` carry one in the envelope and `MetadataNativeStrict` refuses
it.

Under `MetadataAdaptive` a message is also enveloped when its **body begins with
the envelope's own magic** (`IQMQ` and a version byte), even if the native
fields could carry everything else: the adaptive read side decides from the
body, so a native body that looks like an envelope would be claimed as one and
refused. A non-iqmq consumer therefore reads such a body wrapped.

**No limit is advertised**, and that is the rule rather than an omission:

| Limit | Advertised | Why |
|---|---|---|
| `MaxMessageBytes` | **unknown**, unless the peer declares a maximum on the sender link | RabbitMQ's `max_message_size` is a server setting rather than a connection property, so a number here would be a guess |
| `MaxHeaderBytes` | **unknown** | see below |
| `MaxDelay`, `MaxLease` | **unknown** | there is no delay and no lease; advertising zero would read as "every lease expires immediately" |

The header ceiling is the one worth explaining. An AMQP 1.0
application-property key is a string with no 255-byte limit, while an AMQP
0-9-1 field-table key is a shortstr — so a key longer than 255 bytes published
through this profile **cannot be read through the other one**. That is an
interoperability ceiling rather than a limit this driver enforces, and 
makes an advertised limit something a caller may rely on, so it is stated here
instead of advertised.

## Reconnect

The driver owns its dial loop, which the specification requires: go-amqp does not reconnect,
so the shared core's supervisor notices the connection's death, bumps the
connection generation **before anything else happens**, invalidates every
delivery token, and re-dials with jittered backoff (`Config.Backoff`, full
jitter by default). `Config.ReconnectAttempts` of zero means "until the client
is closed"; a positive value makes the backend go Degraded and stay there with
every operation answering `CodeUnavailable`.

There is **no topology to restore**, which is the one way a reconnection here is
simpler than on the other profile: this driver declared nothing, so a
reconnection re-attaches links and nothing else. `Config.Addrs` is a failover
set tried in order and re-tried from the first — AMQP 1.0 is one long-lived
connection, and a client that spread its links over two nodes would have half
its deliveries die with each.

## At-least-once window

From the transfer that delivered the message until the disposition the broker
acted on. A process that stops inside it — a crash, a link that detached, a
connection that died — leaves the delivery unsettled, and **detaching an AMQP
1.0 link returns everything unsettled on it to the queue**, where RabbitMQ
hands it to whichever consumer of that queue is next. So a handler may run
twice for one message and the application has to absorb the duplicate;
`DeliveryInfo.Attempt` is `delivery-count` plus one, so the second delivery
says so.

Under the default `receiversettle=first` a settlement is final when the
disposition frame is written, so an `Ack` costs no round trip and a link that
died immediately afterwards leaves the message redeliverable — at-least-once
behaving as advertised. Under `second` the peer confirms the settlement, at a
round trip each, and a failed settlement changes class with it: `first` means
the broker never got it (a lost lease, so the message is coming back), `second`
means it may have (an unknown outcome, sticky for ever).

A settlement whose link belongs to an **older connection** is refused with
`ErrLeaseLost` and never written to the wire. That is the disqualifier in
AMQP 1.0's clothing: a peer that reuses delivery ids across an attach would
otherwise apply the disposition to whatever took the delivery's place. The
guard is the shared core's; this profile adds nothing that could defeat it, and
"Driver-specific critical tests" below is what holds that.

## Config & DSN

`Config` is RabbitMQ's vocabulary; the protocol's own knobs are the shared
core's types rather than copies of them, because a translation table between
two identical enumerations is a table that can drift.
`Config.CoreConfig` and `Config.Profile` are the conversion and are this
module's real API.

| Field | What it is |
|---|---|
| `Addrs` | `amqp://host:port`, `amqps://host:port` or bare `host:port`; a failover set |
| `Username`, `Password` | SASL credentials; `Password` is a `dsn.Secret`, so no rendering can carry it |
| `Vhost` | the virtual host, which reaches RabbitMQ as the OPEN frame's `hostname` under `vhost:` |
| `HostName` | that field outright, for a deployment that needs a particular one. Refused together with `Vhost` |
| `Prefix` | prepended to every exchange and queue name; `internal/rabbit`'s, so both profiles agree |
| `ExchangeType`, `QueueType`, `DeadLetter`, `MaxPriority` | **assertions** about a topology this driver did not create. Each moves a capability claim |
| `Declare` | **refused**; see "Topic / subscription mapping" |
| `KeyAsRoutingKey` | **refused**; see "Ordering" |
| `Credit` | the link credit ceiling of one source: this protocol's `basic.qos` and its only window |
| `ReceiverSettle` | `first` (default) or `second`; see "At-least-once window" |
| `SenderSettle` | `mixed` (default) is the only mode under which one sink serves both `ConfirmNone` and `ConfirmAccepted` |
| `NonDurable` | publish with the AMQP `durable` flag clear. Inverted so that the zero Config publishes durable |
| `Backoff`, `Reconnect*`, `NoReconnect` | the dial loop |
| `Publisher`, `Consumer` | the AMQP 1.0 link controls — link name, properties, capabilities, source filters — which is this profile's escape hatch where the other has `ConsumeArgs` |
| `Clock`, `Hooks` | the injected clock and the callbacks |

The DSN scheme is **`rabbitmq+amqp10`**, which is neither `rabbitmq` (the AMQP
0-9-1 driver's) nor `amqp10` (the core's generic profile's). A registry in which
one scheme could open either profile would make a connection string ambiguous
about which capability table the caller gets, and the tables really differ.
Registering both RabbitMQ modules with one registry is the supported
arrangement.

    rabbitmq+amqp10://user:password@host:5672,host2:5672?vhost=acme&queuetype=quorum&deadletter=true

This profile's parameters are `vhost`, `prefix`, `exchangetype`, `queuetype`,
`deadletter`, `maxpriority` and `credit`; the shared core's — `sasl`, `tls`,
`insecureskipverify`, `insecureplainsasl`, `containerid`, `hostname`,
`sendersettle`, `receiversettle`, `idletimeout`, `maxframesize`, `maxsessions`,
`maxcredit`, `maxpending`, `nondurable`, `dialtimeout`, `operationtimeout`,
`writetimeout`, `backoff`, `reconnectbase`, `reconnectmax`, `reconnectgrowth`,
`reconnectattempts`, `noreconnect` — pass through unchanged. Anything else is a
construction error naming it, which is how a copied `declare=true` or
`confirm=windowed` is caught rather than ignored.

**A DSN has no path.** On AMQP 0-9-1 the URL's path is the virtual host;
AMQP 1.0 has no virtual-host field, so there is nothing for a path to mean and
one is refused rather than ignored.

`Config.DSN()` renders the configuration with the credential replaced by the
redaction marker and everything else intact, and the result parses back — which
is what makes it usable for the thing it exists for, a log line a person then
pastes.

## Known gotchas

**This section is the one that earns its place on this page**, because moving
between the two RabbitMQ profiles is a change of behaviour and the specification forbids
doing it silently.

### What a caller who moves from `drivers/rabbitmq` loses

1. **Native request/reply.** `amq.rabbitmq.reply-to` is an AMQP 0-9-1
   extension with no AMQP 1.0 form. A reply address still travels in the
   `reply-to` property, which is the portable emulation, not a native
   mechanism.
2. **A delayed redelivery and a scheduled publication.** Both are refused with
   `ErrUnsupported` before any I/O. A delayed `Nack` settles **nothing**, so
   the message stays the broker's — it is not dropped, and it is not requeued
   without the delay either.
3. **Ordering by key.** `Config.KeyAsRoutingKey` is refused: the routing key
   lives in the link's address.
4. **A wildcard subscription.** Refused with `ErrUnsupported`: the pattern is a
   binding, and a link addresses a queue.
5. **The confirm strategy.** There is no confirm mode, so the specification's
   three strategies have no equivalent. What a caller tunes is `Config.Credit`.
6. **`mandatory` and `basic.return`.** An unroutable message is **released** by
   the broker. The outcome a caller sees is the same as the other profile's —
   `CodeNotFound`, not retryable — but the mechanism is a disposition rather
   than a returned message, so there is no returned message to inspect.
7. **Declaring topology, and `Client.Admin`.** Both are refused. The exchange,
   the quorum queue and the dead-letter route have to exist before a link
   attaches, or the attach is answered with `amqp:not-found`. RabbitMQ 4 does
   have an AMQP 1.0 management extension over a `/management` node; this
   profile does not implement it and does not claim `FeatureAdmin`.
8. **At-most-once delivery.** Not claimed: every receiving link is attached
   unsettled and every delivery is settled explicitly. (The AMQP 0-9-1 arm's
   shared protocol table claims it natively; whether that driver really offers
   a `no-ack` consume path is its own page's question.)
9. **The virtual host moves.** It is a `vhost` parameter, not the DSN's path,
   and it reaches the broker as the connection's `hostname`. A copied
   `rabbitmq://host/acme` string is refused rather than silently landing in the
   default virtual host.
10. **A header key over 255 bytes stops being portable between the two
    profiles.** It is legal here and cannot become an AMQP 0-9-1 field-table
    key.

The specification marks this profile itself **(verify)** and marks `x-delivery-count →
Attempt (quorum queues)` **(verify)**. Nothing in this increment ran against a
RabbitMQ broker — every test runs against a scripted AMQP 1.0 peer — so the
honest split is:

Every unverified item above is either **Unsupported** in the capability table
or a **Conditional** claim whose condition is stated. None of them is a
capability this driver advertises.

### Smaller ones

- **Stream queues are not addressed.** RabbitMQ 4 exposes streams over AMQP
  1.0, and their semantics — offsets, filters, a consumer that reads a log
  rather than competing for messages — are not this profile's. `Config.QueueType`
  has quorum and classic only.
- **SASL `none` is in the enumeration and RabbitMQ does not want it.** It is
  the shared core's value for a peer that offers no SASL layer, which is what
  this module's own scripted test peer is.
- **`Config.MaxPriority` forces a classic queue.** RabbitMQ refuses
  `x-max-priority` on a quorum queue, so a prioritised subscription gives up
  the quorum queue's durable confirmation boundary and its attempt count. The
  conformance harness makes that trade explicitly for the priority scenario.
- **The conformance suite needs the topology created first.** This profile
  declares nothing, so a run against an empty broker fails at the first attach.
  `ConformanceHarnessWithProvisioner` takes the step that creates each
  scenario's Need; `ConformanceHarness` is that with no provisioner, for an
  endpoint an operator has already prepared.

## Maturity & tier

See the pinned client version and the module tests when evaluating this driver.

## Driver-specific critical tests

All of them run against the scripted peer and none needs a container.
