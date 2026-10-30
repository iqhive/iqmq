# activemq (AMQP 1.0, Artemis and Classic)

<!-- Fixed sections per the driver documentation. Every section must be present. -->

**Maturity:** Experimental · **Tier:** 3 · **Driver:** `github.com/iqhive/iqmq/drivers/activemq`

This page is the Apache ActiveMQ driver over **AMQP 1.0**, and it is one driver
with **two profiles**: Apache ActiveMQ **Artemis** and Apache ActiveMQ
**Classic**. Wherever the two products disagree this page says so in two
columns rather than in prose that would be true of neither.

The same two brokers over STOMP 1.2 are a separate module with a page of its
own, [activemq-stomp.md](activemq-stomp.md); the pair is variant family ** **
("ActiveMQ protocol") and the specification makes the protocol choice **explicit** —
neither is ever silently substituted for the other, because their capability
tables really differ. The protocol itself belongs to
[the shared AMQP 1.0 core](amqp10.md), which owns the session per link, the
credit window, the settlement token table, the reconnect loop and the condition
table, and knows about no broker at all. What this module adds is the two
things AMQP 1.0 does not settle and the two products disagree about: how a
topic and a subscription are addressed, and which of the features the
combination really has.

## Client & version policy

The native baselines for this driver live in `benchmarks/native-baselines/` and
measure the clients below against the harness's `artemis` image
(`make bench-native CLIENTS="goamqp"`). A driver number is only comparable with
a baseline taken from the same client version, broker version and confirmation
boundary, so the pins move together; each baseline's package comment records
the settlement boundary it measures as well.

| Baseline | Client | Pinned version | Confirmation boundary |
|---|---|---|---|
| `goamqp` | github.com/Azure/go-amqp | `github.com/Azure/go-amqp@v1.7.0` | AMQP 1.0 settled disposition from the broker (Sender.Send) |

The configured harness images are
**`apache/activemq-artemis:2.44.0-alpine`** and
**`apache/activemq-classic:6.2.0`**, as listed in
`conformance/harness/images.go`. Run both suites to generate
reports for the source revision and broker versions being assessed.

**The version floors are per product, because the two products gained AMQP 1.0
at different releases.** `Open` refuses a broker older than the floor with
`mq.ErrUnsupported` naming both versions:

| | Artemis | Classic |
|---|---|---|
| floor this driver enforces | **2.7** | **5.9** |
| why | a shared durable subscription is addressed by fully qualified queue name (`address::queue`), and 2.7.0 is the release whose acceptors accept one. An older Artemis speaks AMQP 1.0 perfectly well and would detach the receiver with `amqp:not-found` | the release in which the AMQP transport stopped being experimental |
| what the broker says about itself over AMQP 1.0 | `product: apache-activemq-artemis` and a bare `version` | **nothing at all**: no product and no version in the open performative |

Because Classic says nothing, **a broker that identifies itself as nothing is
accepted** — the specification makes "the peer did not say" distinct from any claim, and
refusing silence would refuse one of the four combinations the specification names. That is
the opposite of [the RabbitMQ AMQP 1.0 profile's](rabbitmq-amqp10.md) rule, and
deliberately so. What guards the wrong-broker case instead is the **product**
check, which is the half that can be answered from what the brokers do send: a
client configured for one product and pointed at the other is refused before it
creates anything.

### Verified facts (the API contract)

Checked while building the native baselines and then re-checked against the
pinned containers; re-verify when a pin moves. Three of the rows below changed
when the measurements came in — see "What the measurements changed" underneath.

- **Clients.** github.com/Azure/go-amqp is an actively maintained MIT-licensed AMQP 1.0 client and is what this driver uses for Artemis and for ActiveMQ Classic's AMQP transport; github.com/go-stomp/stomp/v3 is not a fallback for it but the client of a separate driver module (drivers/activemq/stomp) which a caller selects deliberately, and the one thing that module has and this one does not is a server-side selector, measured to filter on Classic and to be ignored on Artemis.
  _Verified against: go-amqp v1.7.0 README.md and LICENSE; internal/activemq/protocol/stomp.go's measured FeatureServerFilter row._
- **Routing type.** Artemis decides the routing type of an auto-created address from the link's capabilities: a sender or receiver that declares the "queue" capability gets an ANYCAST address and queue, and without it the broker falls back to its default-address-routing-type. That is how the goamqp baseline gets a queue to measure and it is not how this driver works: this driver creates no topology at all, expects auto-creation to be off, and states the routing type explicitly, because a multicast address with a durable queue per subscription is what fan-out needs and an anycast address is not.
  _Verified against: Artemis AMQP documentation; verified by the goamqp baseline's link options; drivers/activemq/admin.go's queueConfiguration._
- **Delays.** Scheduled delivery is broker-specific and section-specific, and each product ignores the other's spelling: over AMQP 1.0 Artemis reads the x-opt-delivery-delay message annotation and ActiveMQ Classic reads the AMQ_SCHEDULED_DELAY application property, while over STOMP both brokers read AMQ_SCHEDULED_DELAY as a frame header; Classic additionally needs schedulerSupport="true", which the pinned image does not set. An FQQN (address::queue) is how a consumer names an existing queue on an Artemis address.
  _Verified against: apache/activemq-artemis:2.44.0-alpine and apache/activemq-classic:6.2.0, both directions; Artemis and ActiveMQ Classic scheduled-message documentation._

#### What the measurements changed

All three corrections were written back into
`benchmarks/native-baselines/nativebench/facts.go`, which is where the
The specification ledger lives, so the sentences above are that ledger's own and
`TestFactsRecorded` holds the two copies together. A page that paraphrased a
disproved claim would have passed nothing and drifted quietly.

## Topic / subscription mapping

**`Config.Broker` is required and has no default.** That is the single most
important thing on this page, and it is not a convenience that was forgotten.
"ActiveMQ" is two products with one name and their topic mappings share no name:

| | Artemis | Classic |
|---|---|---|
| model | an **address** with a routing type; a durable queue per subscription | no address model at all; a **virtual topic**, which is a naming convention |
| topic → publish to | `<prefix><topic>` (a MULTICAST address) | `topic://VirtualTopic.<prefix><topic>` |
| subscription → consume from | `<address>::<address>.<subscription>` (a fully qualified queue name) | `queue://Consumer.<subscription>.VirtualTopic.<prefix><topic>` |
| the queue a subscription is | `<prefix><topic>.<subscription>` | `Consumer.<subscription>.VirtualTopic.<prefix><topic>` |
| who creates it | `Client.Admin()`, over the broker's management address | the broker, on attach; there is no management over AMQP 1.0 |
| de-duplication | `_AMQ_DUPL_ID` | none |

Both mappings satisfy the rule and that is the only thing they have in
common: two consumers of **one** subscription attach to the same queue and
compete for its messages, and two consumers of **different** subscriptions
attach to different queues and each receives the message.

**Nothing on the wire says which product a caller meant, and getting it wrong
is not an error on either broker.** A client configured for Classic and pointed
at Artemis would not fail: Artemis auto-creates an address literally called
`topic://VirtualTopic.orders`, publishes into it for ever, and delivers
nothing, while the consumers attach to `queue://Consumer.workers.VirtualTopic.orders`,
which is a different address again. Nothing reports an error. So the field is
required — the **zero value of `activemq.Broker` names no broker at all**
(`BrokerUnset`), so a deployment that omitted the line is refused with
`ErrNoBroker` at Open rather than silently mapped the Artemis way — and
`CheckProduct` refuses a broker that identifies itself as the other one
**before** the version is read and before anything is created. Both halves
were needed: the field was documented as required while `Artemis` was still
the zero value, and ten rows of this project's own test tables were silently
exercising Artemis by omission.

    iqmq.VirtualTopic.p   NOT DELIVERED
    VirtualTopic.iqmq.q   delivered
    VirtualTopic.r        delivered

The first ordering was `internal/activemq`'s first draft, and the conformance
suite is what found it: every scenario timed out waiting for a delivery.

Three more rules about names, each refused **before any I/O** rather than at the
broker — on Artemis a rejected attach detaches the link and takes the session
with it, and on Classic a bad destination is *accepted and created*, so the
caller's typo becomes a queue nothing consumes:

- **A subscription cannot contain `.`** on either product. On Classic
  `Consumer.a.b.VirtualTopic.t` and `Consumer.a.VirtualTopic.b.t` are two
  spellings of one queue name and the second is a *different topic*, so
  refusing the separator is what keeps `ParseQueue` a function — and `Admin`'s
  `ListTopics` needs it to be one.
- **A wildcard is not a topic.** Both products have a wildcard syntax over
  destination names (`#`, `*`, `>`) and a topic maps to its own address, so a
  pattern over topic names would have to span addresses while a receiving link
  attaches to one. It is refused with **`ErrUnsupported`**, not with an
  invalid-argument error, because that is what the negative probe for
  `FeatureWildcardSubscribe` reads — the conformance harness supplies
  `<topic>.*` deliberately so the negative claim is *proved* rather than taken
  on trust.
- **`::`, `/`, `,`, `:`, whitespace and control characters are refused in any
  name**, and 255 bytes is the ceiling. Each is a character that means
  something to a destination parser: `::` is Artemis's FQQN separator, `://`
  and a leading `/` are Classic's scheme and path forms, `,` separates the parts
  of a Classic composite destination, and `:` separates a STOMP header's name
  from its value.

### Reading a JMS queue by name

The mapping above names its own queues, so it cannot reach a queue that some
other application created and named, such as an `ORDERS.IN` a JMS producer
writes to. Set `Config.Addressing` to `activemq.AddressQueue` (or
`addressing=queue` in the DSN) and the topic becomes the queue's name, for
publishing and for consuming:

```go
cl, err := activemq.Open(ctx, activemq.Config{
	Broker:     activemq.Artemis,
	Addrs:      []string{"broker:5672"},
	Addressing: activemq.AddressQueue,
})
// ...
con, err := cl.Consumer(ctx, mq.ConsumerConfig{Topic: "ORDERS.IN", Subscription: "billing"})
```

| | Artemis | Classic |
|---|---|---|
| publish to and consume from | `<prefix><topic>`, with the AMQP `queue` capability on both links so that an address carrying both routing types resolves to its ANYCAST queue | `queue://<prefix><topic>` |
| what `Admin` creates | an ANYCAST address and a durable ANYCAST queue, both called `<prefix><topic>` | nothing: Classic has no `Admin` |
| where `DeadLetterTopic` sends a dead letter | another queue, named the same way, and the dead-letter link asks for the `queue` capability too | another queue, named the same way |

In this mode:

- `ConsumerConfig` still requires a subscription, and the driver ignores it.
  Every consumer of the queue competes for its messages, so `FeatureFanOut` is
  withdrawn from the capability table. A second client in the default mode is
  how one program gets fan-out and a JMS queue at once.
- `Prefix` goes in front of the queue's name. Leave it empty to read a queue
  that another application named.
- The name rules above apply to the queue's name, so a queue whose name
  contains `/`, `:` or `::` cannot be addressed this way.
- `Admin.DeleteSubscription` removes nothing and succeeds, because the queue it
  would resolve to is shared by every consumer and producer. `DeleteTopic`
  removes the queue and its messages.
- `Admin.ListTopics` reports every queue whose name starts with the prefix. With
  no prefix that includes the broker's own `DLQ` and `ExpiryQueue`, because
  they are JMS queues too.

## Publish path & confirmation boundary

A publication is one transfer on a sending link attached to the topic's
destination. The boundary is the shared core's, narrowed by what these brokers
do:

| Mode | Boundary |
|---|---|
| `ConfirmNone` | the transfer is sent **pre-settled**: the frame leaves, no disposition is coming, the call costs no round trip. Local admission, which the specification says may be lossy |
| `ConfirmAccepted` | the peer's `accepted` disposition **for this transfer** |
| `ConfirmDurable` | the same `accepted` disposition for a message published with AMQP's `durable` flag set. **Conditional**, and the condition is the deployment's: Artemis writes its journal entry before it accepts and Classic writes its store before it acknowledges a persistent send, so the acceptance *is* the durability — provided the destination is one the broker's address settings persist. It is withdrawn for a client configured `NonDurable`, which clears the flag the whole argument rests on |

Two publication outcomes deserve naming because a caller acts on them
differently from an ordinary failure.

- **Unroutable.** A `released` disposition on a **publication** means the broker
  took the transfer and routed it nowhere: on Artemis a multicast address with
  no queues bound to it, and on Classic a virtual topic nobody has a consumer
  queue for. That is the specification's "the topology is not there" rather than a retryable
  failure, so this profile adds one rule to the table and it is the only
  one it adds: on `OpPublish`, `released` is **`CodeNotFound`**. Reporting it as
  `Unavailable` would have a publisher retry for ever into a destination that
  will never take the message. The rule is registered for `OpPublish` **only**:
  a `released` on the consume side is this driver's own release path and never
  reaches the mapper as a failure.
- **Ambiguous.** A transfer that has left the socket and whose disposition never
  arrived **may** have been accepted. The specification makes reporting that as a definite
  failure a disqualifier, so it is ambiguous — `mq.IsAmbiguous(err)` is true —
  and `TestCriticalSenderLinkDetachIsAmbiguousAndRecovers` is what holds it. A
  peer's own `rejected` or `released` disposition stays **definite**, which is
  the half a driver that marked everything ambiguous would destroy.

A target whose **address does not exist** is a third thing again, and only on a
broker configured to say so: Artemis with auto-creation off refuses the attach
with `amqp:not-found` ("AMQ119002: target address … does not exist" for a
sender, "AMQ119010: source address does not exist" for a receiver), which the
core's condition table turns into `mq.CodeNotFound`.

**A batch is N transfers on one link.** There is no batch frame in AMQP 1.0 at
all, so a batch is never atomic: the link's credit makes it cheap and does not
make it one operation.

## Consume path

A source is a receiving link attached to the subscription's destination — an
FQQN on Artemis, the consumer queue on Classic — with a credit window. **No
link is attached when the source is opened**: the first fetch attaches one, so a
broker that is down at that instant is a retryable fetch failure rather than a
consumer that could not be constructed.

`Config.Credit` is the ceiling on the link credit one source may hold, and it
**is** the prefetch. It is the only window this protocol has, because AMQP 1.0
**grants no lease at all**: an unsettled delivery is this receiver's for as long
as the link lives, so `DeliveryInfo.LeaseExpires` is the zero time,
`FeatureExtendLease` is unsupported, and there is no extender anywhere in the
module.

`DeliveryInfo.Attempt` comes from the AMQP **header section's**
`delivery-count`, which counts *previous* attempts — so Attempt is 1 on a first
delivery and rises across a nack. That it is a protocol field of the header
section rather than a broker header extension is one of the four rows this
protocol wins over STOMP, where there is no count at all.

**On Classic the consumer queue must exist BEFORE the first publication.** A
virtual topic copies a publication into the consumer queues that exist *at that
moment*, so a queue created afterwards gets nothing — measured against the
pinned image, a scenario that published first and subscribed second received
nothing at all. `FeatureFanOut` is therefore **conditional** on Classic and
native on Artemis, and the condition is stated: it is the broker's *default*
virtual-destination configuration, and a deployment that changed the prefix
would take the fan-out away with no error anywhere.

A delivery this driver cannot decode is **not** handed over and **not**
acknowledged: acknowledging it would destroy it, so it stays unsettled and the
broker gets it back.

## Ack / nack / extend mapping

The four AMQP dispositions carry the verbs, and the mapping is the shared
core's:

| action | AMQP 1.0 outcome | Notes |
|---|---|---|
| `Ack` | `accepted` | final when the disposition frame is written under the default `receiversettle=first` |
| `Nack(NackRequeue())` | `modified{delivery-failed}` | the delivery comes back **and the attempt count rises** |
| `Nack(NackRequeue(), NackDelay(d))` | — | **`ErrUnsupported`, and settles nothing.** Measured; see below |
| `Nack(NackDrop())` | `rejected` | AMQP 1.0 has no destroy outcome, so on a destination with a dead-letter address a dropped message reaches it rather than being destroyed. The safe direction, stated rather than hidden |
| `Nack(NackDeadLetter())` | `rejected`, which reaches the broker's dead-letter address | or a publication to `Config.DeadLetterTopic`, when one is set |
| `Reject` | `rejected` | |
| `Extend` | — | there is no lease and no frame; `FeatureExtendLease` is unsupported |
| a closing consumer's unsettled deliveries | `released`, with `delivery-failed` **clear** | the driver is giving back what it never handed out, not reporting a failure, so the attempt must not count |

**`released` is deliberately NOT the requeue, and the reason is measured rather
than argued.** On both brokers a `released` delivery comes back with
`delivery-count` **unchanged** — three rounds at **0, 0, 0** — while
`modified{delivery-failed}` comes back at **0, 1, 2**. Attempt has to
rise across a nack or the specification's `MaxAttempts` is compared against a number that
never moves and a poison message is retried for ever.

**`modified`'s `undeliverable-here` flag is NOT set, and that too is measured.**
On Artemis a delivery modified with `undeliverable-here` is never redelivered to
the consumer that modified it — so a subscription with **one** consumer loses
the message entirely. A requeue that lost the message on a single-consumer
subscription would be a silent data defect, so the flag stays clear.

**There is no delayed redelivery, and the absence is measured rather than
assumed.** A publication's delay works (see "Delays"); the *same* annotation on
a `modified` **disposition** does not: on Artemis a `modified` carrying
`x-opt-delivery-delay: 3000` was redelivered **immediately**, with
`delivery-count` 1. The annotation is honoured on a publication and ignored on a
disposition, and each product's redelivery delay is an **address setting**
(Artemis's `redelivery-delay`) rather than a per-nack value. So there is no
protocol spelling for the delayed requeue and it is refused **before any
I/O**, settling nothing — the delivery stays the broker's, and a caller told
"unsupported" can still do something else with it.

A settlement the driver cannot perform is refused **before** the delivery's slot
moves, which is the specification's "returns `ErrUnsupported` and settles nothing": a driver
that took the slot first and then discovered it could not honour the action
would leave the delivery unsettleable for ever.

## Ordering

**Ordering by key is native, at key scope, and the mechanism was measured.**
the specification's `JMSXGroupID` is AMQP 1.0's **`group-id` message property**, and both
products read it as the JMS message group and dispatch one group exclusively to
one consumer of a subscription. Measured on Artemis with two competing consumers
on one queue: every `k0` message went to the first consumer and every `k1`
message to the second, in publication order within each group.

That is the **only** ordering claim this driver makes beyond one queue's FIFO
order. `FeatureOrderedByPartition` is claimed at group scope — a queue is FIFO,
so one subscription's messages arrive in the order the broker routed them — and
`FeatureOrderedGlobally` is **not** claimed: a multicast address fans a
publication into several queues and each is consumed independently. A nack
returns a message to its queue and the core makes no promise about where in it.

## Delays

A **scheduled publication** is supported on both products, spelled differently
on each, and this is the one capability a *deployment* can take away:

**Classic's scheduler is off by default and its absence is silent.** With
`schedulerSupport` unset the broker *accepts* the property, delivers the message
**immediately**, and passes the property through to the consumer untouched. A
driver that wrote the key anyway would report success for a delay that did not
happen — so `Config.Scheduler` is how a deployment says the scheduler is on, and
without it the profile's delay rule is the **zero** rule, which the core reads
as "this broker has no way to express that" and answers with `ErrUnsupported`
**before any I/O**. `FeatureScheduledPublish` is withdrawn to match.
On Artemis the field is ignored.

Both products get the **relative** form (`x-opt-delivery-delay` on Artemis,
`AMQ_SCHEDULED_DELAY` on Classic), so a `PublishOptions.DeliverAt` goes out as
the delay left until it, measured on `Config.Clock`. A `DeliverAt` that has
already passed goes out with no delay at all, because an explicit zero would
make an ordinary publication a scheduled one.

`Limits.MaxDelay` is left **unknown** rather than advertised: neither product
documents a ceiling on a scheduled delivery and neither refused one in testing,
and the rule is that an unknown limit is never an overloaded zero.

A **delayed redelivery** does not exist on either product over either protocol;
the measurement is under "Ack / nack / extend mapping" and
`FeatureDelayedRedelivery` is withdrawn.

## Dead-letter queue

There are two routes and a caller chooses which by configuration, because the
two have different failure modes and the specification will not let one stand for the other.

`DeadLetter` and `NoDeadLetter` both set is a construction error, because the
driver cannot ask the broker which is true.

What reaches either route is a **`rejected`** disposition. Measured on Artemis:
one rejection raised `DLQ`'s `countMessages` from 0 to 1 and the message was not
redelivered.

With `DeadLetterTopic` set, the copy leaves out the original's `_AMQ_DUPL_ID`
and its scheduled-delivery key (`x-opt-delivery-delay` on Artemis,
`AMQ_SCHEDULED_DELAY` on Classic). Artemis delivers both as they were published
and keeps its duplicate cache per address, so two subscriptions dead-lettering
one publication had the second copy accepted and dropped as a duplicate, and a
delayed message waited out its delay again at the dead-letter address. The
copy's header keeps the original's `durable`, `priority` and `ttl` and starts
its `delivery-count` at zero, so the copy's first delivery from
`DeadLetterTopic` is attempt 1. Artemis sends a message's header as it was
published until a delivery of it fails, so a copy that kept the original's
count arrived already counted as redelivered, and a consumer of the dead-letter
queue with `WithMaxAttempts` at or below that count treated every copy as
exhausted on arrival. The attempt the original failed on travels in the copy's
`x-iqmq-dead-letter-attempt` annotation.

With no `DeadLetterTopic` the profile supplies **no** `DeadLetterAddress` to the
shared core, deliberately: supplying one would make the core publish a copy
itself and then reject the original — one more copy, one more window, and a
second dead-letter mechanism for a broker that already has one. The conformance
run records the consequence honestly, skipping `dlq/failed-publish-never-acks`
with the note _"this configuration publishes no dead letter: a rejected delivery
reaches the broker's own dead-letter address … and there is no publication of the
driver's to fail. Set Config.DeadLetterTopic for the route this scenario is
about"_.

## Headers & limits

A portable header is an **AMQP application property**, and the values are typed:
a header value that is not valid UTF-8 travels as an AMQP binary and comes back
byte for byte, so **`FeatureBinaryHeaders` is native**. That is the second row
this protocol wins over STOMP, where every header is text.

A **repeated key** cannot travel at all, because application-properties is a
map and a second value under one key replaces the first;
`MetadataPortableEnvelope` and `MetadataAdaptive` carry one in the envelope and
`MetadataNativeStrict` refuses it.

Under `MetadataAdaptive` a message is also enveloped when its **body begins with
the envelope's own magic** (`IQMQ` and a version byte), even if the native
fields could carry everything else: the adaptive read side decides from the
body, so a native body that looks like an envelope would be claimed as one and
refused. A non-iqmq consumer therefore reads such a body wrapped.

`Config`'s annotation prefix is **`x-opt-`**, which is the prefix of Artemis's
own message annotations: a caller's header under it belongs in the
**message-annotations** section rather than in application-properties. The
reserved `x-iqmq-` prefix is the driver's own, and a caller's header under it is
refused rather than shadowed.

**The limits, and why only one is advertised:**

| Limit | Advertised | Why |
|---|---|---|
| `MaxHeaderBytes` | **255** | both products really impose a name ceiling, and the mapping enforces it **before any I/O**: an oversized header name is refused by the mapper rather than by the broker with a link detach. 255 is the smallest ceiling any broker in this repository's harness imposes, so it is the one a portable driver can advertise without claiming a limit it cannot check |
| `MaxMessageBytes` | **unknown** | the only place AMQP 1.0 carries a message-size ceiling is a link's attach performative, and the facts are read at **open**, before any link exists. A number here would be this driver's assertion rather than the broker's answer. (The pinned images' acceptors carry 100 MiB frames; that is the deployment's, not the protocol's) |
| `MaxDelay`, `MaxLease` | **unknown** | there is no lease at all, and neither product documents a delay ceiling. Advertising zero would read as "every lease expires immediately", which the specification makes a different answer |

**A delayed publication costs one more allocation on Artemis than on Classic,
and no amount of work in this driver can remove it.** An AMQP 1.0
message-annotations map is keyed by `any` (`amqp.Annotations` is `map[any]any`),
so an annotation boxes its **key** as well as its value, while
application-properties is `map[string]any` and boxes only the value. Artemis
reads the delay from the annotation section and Classic from the properties
section, so the exact counts this profile adds over an undelayed publication
are:

| Control | Artemis | Classic |
|---|---|---|
| a bare payload with a TTL and a priority | **0** | **0** (both are AMQP header fields; this profile's rules touch neither) |
| `PublishOptions.Delay` | **2** (key + value, boxed into a `map[any]any`) | **1** (value only, into a `map[string]any`) |
| `PublishOptions.DedupKey` | **1** (the key is already a string; boxing it is the one allocation) | — (refused: Classic has no de-duplication) |

The asymmetry does **not** compound: the annotation section's extra boxing is
paid once per annotation and the scheduled delivery is the only annotation
either product's rules write, so the same whole publication differs by exactly
one allocation between the products. `allocs_test.go` pins every one of these
counts exactly, so a rule whose value function built a string would be a diff in
that file rather than a number drifting upwards.

## Reconnect

The **shared core** owns the dial loop, which the specification requires of this driver
family: go-amqp does not reconnect, so the core's supervisor notices the
connection's death, **bumps the connection generation before anything else
happens**, invalidates every delivery token, and re-dials with jittered backoff.
`Config.Backoff` names the policy explicitly rather than leaving it to the
variant family's `Auto`, whose provisional winner is `none` — meaning no delay
at all, which the specification forbids.

`Config.Addrs` is a failover set tried round-robin **across reconnect
attempts**: Artemis's failover pair, Classic's network of brokers. What it is
**not** is a cluster the driver follows — AMQP 1.0's `connection-redirect` is
classified as a retryable transport failure by the core and is **not** followed,
so a redirect costs one reconnect interval rather than being honoured.
`Config.ReconnectAttempts` bounds the reconnector's whole
life rather than one outage, which is how the helper defines it; `NoReconnect`
makes a lost connection terminal, and a **borrowed** connection
(`OpenWithClient` with `ClientBorrowed`) is never redialled at all — dialling
would replace a connection the caller still holds — so such a client goes
Degraded and stays there.

**There is no topology to restore**, which is the one way a reconnection here is
simpler than on a driver that declares: this driver declared nothing, so a
reconnection re-dials and re-attaches links and nothing else.

A **link** detach is a different event from a connection loss, and the
difference is load-bearing. A broker detaches a link without closing the
connection when the destination is deleted, when a link is stolen by another
client of the same name, or when an address's settings change under it — Artemis
does all three. The connection is alive afterwards, so the reconnect loop never
runs: the link is dropped and re-attached by the **next fetch**, the connection
generation does **not** move (bumping it would invalidate every other link's
deliveries as well), and one fetch surfaces the failure once rather than every
fetch failing for ever. Two of the six critical tests are about exactly this.
When every credit slot is held, the fetch waits for a settlement and never
reaches the link, so the settlement that finds the link gone drops it instead.
Whatever condition the broker detached with, that settlement is a lost lease
(`receiversettle=first`) or an unknown outcome (`second`), and the waiting
fetch attaches a new link.

## At-least-once window

From the transfer that delivered the message until the disposition the broker
acted on. A process that stops inside it — a crash, a link that detached, a
connection that died — leaves the delivery unsettled, and **detaching an AMQP
1.0 link returns everything unsettled on it to the queue**, where the broker
hands it to whichever consumer of that queue is next. So a handler may run twice
for one message and the application has to absorb the duplicate;
`DeliveryInfo.Attempt` is the header's `delivery-count` plus one, so the second
delivery says so.

Under the default `receiversettle=first` a settlement is final when the
disposition frame is written, so an `Ack` costs no round trip and a link that
died immediately afterwards leaves the message redeliverable — at-least-once
behaving as advertised.

**`ReceiverSettleSecond` is refused on Artemis, and that is measured rather than
assumed.** Artemis answers an attach requesting receiver-settle-mode `second`
with `first`, and go-amqp refuses the negotiation outright (`amqp: receiver
settlement mode "second" requested, received "first" from server`). So
`Config.Validate` refuses the combination at construction rather than letting
**every** source fail at attach with a message about settlement modes instead of
about configuration.

A settlement whose link belongs to an **older link or an older connection** is
refused with **`ErrLeaseLost`** and never written to the wire. That is the specification's
disqualifier in AMQP 1.0's clothing: delivery ids start again from zero on a new
link, so a disposition built for the old link's delivery 0 would name the *new*
link's delivery 0 — settling somebody else's message, redelivering the caller's
own, and reporting nothing.
`TestCriticalStaleDispositionIsRefusedOnAReattachedLink` holds it, and asserts
that the reattached link received **no disposition at all**.

## Config & DSN

`Config` is ActiveMQ's vocabulary, translated into the shared core's
configuration by `Config.CoreConfig` rather than embedding it: a caller
configuring an ActiveMQ driver should be reading about addresses, queues and
virtual topics, and the protocol's own fields are passed through because they
are the protocol's rather than because they are interesting here.

| Field | What it is |
|---|---|
| `Broker` | **required, no default**: `activemq.Artemis` or `activemq.Classic` |
| `Addrs` | `amqp://host:port`, `amqps://host:port` or bare `host[:port]`; a failover set tried round-robin across reconnect attempts |
| `Username`, `Password` | SASL credentials. `Password` is a `dsn.Secret`, so no rendering can carry it. Artemis requires a user by default; Classic's pinned image allows anonymous connection |
| `HostName`, `ContainerID`, `Properties` | the OPEN frame's `hostname`, this client's container id and extra connection properties. The container id is worth setting on Artemis: it is what the broker shows in its connection list, and a fleet whose clients all report the same generated id is a fleet nobody can debug |
| `SASL`, `InsecurePlainSASL`, `TLS`, `InsecureSkipVerify` | the security surface; the default **refuses** PLAIN over a plaintext connection |
| `Prefix` | prepended to every address and queue name. **Inside** `VirtualTopic.` on Classic — see "Topic / subscription mapping" |
| `Addressing` | `activemq.AddressFanOut` (the default) or `activemq.AddressQueue`, which reads from and publishes to a JMS queue by its own name; see "Reading a JMS queue by name" |
| `BrokerAutoCreates` | reports that the broker still creates a destination from an attach. It changes **no behaviour** — the driver creates nothing either way — it changes what `Profile.AutoCreates` and this page's capability output report, so that a deployment reading its own output is not told a refusal it will not get. Forced true on Classic by the profile |
| `Scheduler` | reports that the broker's message scheduler is enabled. Ignored on Artemis; **required** on Classic for `FeatureScheduledPublish` |
| `Prioritised` | reports that the broker's destination policy reorders by priority. Ignored on Artemis, whose queues are priority-ordered; **required** on Classic for `FeaturePriority`, for the same reason and with the same silence as `Scheduler` |
| `DeadLetter`, `NoDeadLetter`, `DeadLetterTopic` | the dead-letter route; see "Dead-letter queue" |
| `Credit` | the link credit ceiling of one source: this protocol's `basic.qos` and its only window |
| `MaxPending` | how many asynchronous publications one sink may have outstanding |
| `NonDurable` | publish with the AMQP `durable` flag clear. Inverted so that the zero `Config` publishes durable |
| `ReceiverSettle` | `first` (default) or `second`. **`second` is refused on Artemis**; see "At-least-once window" |
| `SenderSettle` | the sender's settlement mode |
| `IdleTimeout`, `MaxFrameSize`, `MaxSessions`, `DialTimeout`, `OperationTimeout`, `WriteTimeout` | the connection and operation bounds |
| `Backoff`, `Reconnect*`, `NoReconnect` | the dial loop |
| `Publisher`, `Consumer` | the core's per-link option sets — link name, properties, capabilities, source filters — which is this profile's escape hatch |
| `Clock`, `Hooks` | the injected clock and the callbacks |

The DSN scheme is **`activemq`**, and the **product is a required parameter**:

    activemq://artemis:artemis@localhost:5672?broker=artemis&prefix=iqmq.
    activemq://localhost:5672?broker=classic&scheduler=true

One scheme for two products is the opposite of the obvious design and it is the
same decision `Config.Broker` documents: a scheme per product would let a
deployment move from one to the other by editing four characters of a connection
string, and the two products' topic mappings name nothing in common. Making it a
parameter means a DSN that does not say is **refused** rather than guessed at.
The scheme is **not** `amqp10`, which the shared core's generic profile claims,
and it is not shared with `drivers/activemq/stomp`, which registers
`activemq+stomp` — a registry in which one scheme could open either protocol
would make a connection string ambiguous about which capability table the caller
gets, and the two tables really differ: no nack at all over STOMP, no selector
over AMQP 1.0. Registering both modules with one registry is
the supported arrangement.

This driver's parameters are `broker` (required), `addressing` (`fanout` or
`queue`), `prefix`, `scheduler`,
`prioritised`, `autocreate`, `deadletter`, `deadlettertopic` and `credit`; the
shared core's — `containerid`, `hostname`, `sasl`, `tls`, `insecureskipverify`,
`insecureplainsasl`, `sendersettle`, `receiversettle`, `idletimeout`,
`maxframesize`, `maxsessions`, `maxcredit`, `maxpending`, `nondurable`,
`dialtimeout`, `operationtimeout`, `writetimeout`, `backoff`, `reconnectbase`,
`reconnectmax`, `reconnectgrowth`, `reconnectattempts` and `noreconnect` — pass
through unchanged. Anything else is a construction error naming it, which is how
a copied parameter is caught rather than ignored.

**A DSN has no path.** Neither ActiveMQ has virtual hosts and AMQP 1.0 has no
virtual-host field, so a path could only be a caller's mistaken expectation and
it is refused rather than ignored. `Config.DSN()` renders the configuration with
the credential replaced by the redaction marker and everything else intact, and
the result parses back.

### Artemis's management protocol, as measured

`Client.Admin()` exists on **Artemis only**, and it is the broker's own
management address reached over the very connection the client already has. It
is **not** the AMQP management specification, and the encoding took three tries
against the container:

A request is a message sent to the **`activemq.management`** address with
`_AMQ_ResourceName` (`"broker"`, `"queue.<name>"`, `"address.<name>"`) and
`_AMQ_OperationName` (`"createAddress"`, `"createQueue"`, …) in its
**application properties** and an AMQP **value** body holding the operation's
arguments as a **JSON array string**. Two other encodings were tried first and
both fail with **"Error reading in simpleString"**, because the broker decodes
the body with its own SimpleString codec:

| Body encoding | Result |
|---|---|
| a **data** section carrying the JSON bytes | fails: "Error reading in simpleString" |
| an AMQP **value** holding a real list | fails: "Error reading in simpleString" |
| an AMQP **value** holding the JSON array **as a string** | works |

The reply arrives on a dynamic receiver named by the request's `reply-to` and
carries `_AMQ_OperationSucceeded` and a JSON array body of its own.
A **refused** operation arrives as a *successful delivery*
whose `_AMQ_OperationSucceeded` is `false` and whose body is the reason, which is
why reading the reply is a separate step from receiving it: a caller that only
checked for a transport error would read "Queue X already exists" as success.
`AMQ229019` (a queue) and `AMQ229204` (an address) both say the thing already
exists, and both become `CodeConflict`;
`AMQ229017`/`AMQ229203` ("does not exist") are what make a `Delete` idempotent,
and the already-exists codes are what make an `Ensure` idempotent, which 
requires.

Every request opens its own session, sender and dynamic receiver and closes them
again — three frames per operation, and the right price: a long-lived management
link would be a fourth handle the reconnect supervisor has to invalidate, and
the operations are rare by construction.

Each request is bounded by the caller's deadline or, when it has none, by
`OperationTimeout`. Closing the session gets a fresh `OperationTimeout` of its
own, because the request's deadline may be what ended it. A broker that never
answers the close costs the caller that much more time, not a call that never
returns.

`EnsureSubscription` states **every** field of the queue rather than leaving any
to the broker's default, because Artemis's default for an auto-created topic
subscriber's queue is the opposite of what the specification asks for: `routing-type:
MULTICAST`, `durable: true`, `max-consumers: -1` (the "two consumers of one
subscription compete" half), `purge-on-no-consumers: false` (`true` would make it
a *non*-durable subscription, so the queue and everything in it would go when the
last consumer left), `auto-create-address: false` (a create that made its
own address would be the implicit topology this driver refuses) and `auto-delete:
false`.

`DeleteTopic` and `DeleteSubscription` destroy a queue only after asking it for
its address (`getAddress` on `queue.<name>`) and finding the topic's. A name is
not enough: with no `Prefix`, every queue called `orders.<something>` reads like
a subscription of `orders`, and `DeleteTopic("orders")` used to destroy a
dead-letter queue `orders.dlq` and a JMS durable subscription `orders.audit` on
other addresses, with their messages. A queue on another address is left alone,
and `DeleteSubscription` for a name that resolves to one succeeds, because that
subscription does not exist.

`Stats` reports `countMessages` as the depth and `getDeliveringCount` as the
in-flight count, and leaves **lag unknown** rather than computed: a broker with
no log has no position to be behind, and reporting the depth twice would make two
different questions look answered. `ListTopics` reports the topics with at least
one subscription rather than every address on the broker — a broker's address
list holds `activemq.management`, `DLQ`, `ExpiryQueue`, every MQTT session store
and every temporary reply address, and a driver that reported those as topics
would be reporting the broker's furniture.

## Known gotchas

### This driver creates no topology, and only one deployment can prove it

The specification makes implicit topology creation a defect, and on Artemis it is a defect
with teeth: a typo in a topic name becomes an address, the publications go into
it, and nothing anywhere reports a problem. So a sink or a source **attaches**
and never creates, and topology is `Client.Admin()`'s.

The consequence a deployment has to know is that this driver's guarantee is only
as good as the broker's configuration:

| | Artemis | Classic |
|---|---|---|
| does the broker create a destination from an attach? | **by default, yes** — and it can be told not to: `artemis create --no-autocreate`, or `<auto-create-queues>false` and `<auto-create-addresses>false` in the address settings | **yes, and it cannot be told not to** without an authorization plugin |
| with creation off, an attach to a destination that does not exist | is refused with `amqp:not-found` — "AMQ119010: source address does not exist" for a receiver, "AMQ119002: target address … does not exist" for a sender — which the core turns into `CodeNotFound`. **This is what makes the claim provable** | not applicable |
| so the "creates no topology" claim is | **observable**, and the shipped conformance evidence is taken with auto-creation off | the driver's own code rather than an observable refusal  |

**Run Artemis with auto-creation disabled.** With it on — which is the image's
default — the same attach silently creates the address and this driver cannot
tell.

### What a caller who chooses this driver over the STOMP one loses

Nothing, on Artemis, except a selector. The AMQP 1.0 arm supports **19** of
the features natively on Artemis against STOMP's **15**, and the four rows it
wins are the ones that matter: a real negative settlement, a real attempt count,
a binary header value, and a receive window that is the protocol's own credit
rather than a broker-specific subscribe header. What it gives up is
`FeatureServerFilter`: a JMS selector is an AMQP 1.0 link **source filter** and
go-amqp exposes no way to set one, so a selector is unavailable over this
protocol whatever the broker supports. See
[Choosing between the four combinations](#choosing-between-the-four-combinations).

The specification marks **two** things about this driver `(verify)`, and both are the delay
mechanisms. Both were verified against containers, and one of them turned out to
have a condition the specification does not mention:

| the claim | Status | What was found |
|---|---|---|
| delay via `x-opt-delivery-delay` (Artemis **(verify)**) | **verified** against `apache/activemq-artemis:2.44.0-alpine` | a 2000 ms annotation delayed delivery by 2.0 s, with a plain string key and with an `amqp.Symbol` key alike. `x-opt-delivery-time` works as the absolute form. `AMQ_SCHEDULED_DELAY` sent as an annotation delayed **nothing** |
| delay via `AMQ_SCHEDULED_DELAY` (Classic **STOMP** **(verify)**) | **verified, and understates it** | the header is **not** STOMP-only: over AMQP 1.0 Classic reads it as an **application property** and delayed delivery by 2.2 s. The `x-opt-delivery-delay` annotation Artemis reads delayed nothing here. **And the scheduler is off in the pinned image**: with `schedulerSupport` unset the broker delivers immediately and passes the property through, so the capability is withdrawn unless `Config.Scheduler` says the broker has it |

Everything else on this page that says "measured" was measured against one of
those two images; where a fact is documented rather than measured the page says
so. One fact is **documented rather than measured** and is called out here so
that nobody reads it as container evidence: **Classic's "creates no topology"
claim**, above, which is the driver's own code rather than an observable refusal
because Classic cannot be told not to auto-create.

### Smaller ones

- **Classic does not reorder by priority unless its policy says so, and the
  absence is silent — exactly like the scheduler's.** Classic carries the AMQP
  header's priority and dispatches in FIFO order unless the destination's policy
  sets `prioritizedMessages="true"`, which the pinned image does not do for
  queues at all. Measured with it off, a high-priority publication arrives
  **after** a low-priority one — so `FeaturePriority` is **withdrawn** on Classic
  unless `Config.Prioritised` says the policy is there, and the driver cannot
  read the policy to check it. Artemis's queues are priority-ordered and the
  field is ignored there.
- **`Config.DeadLetter` is true by default**, because both pinned images
  configure a dead-letter address for every address. It is an **assertion about
  the broker** that the driver cannot check: neither product reports a queue's
  dead-letter address over AMQP 1.0.
- **A de-duplication key is refused on Classic** rather than sent and ignored:
  Classic has no broker-side duplicate detection at all.
  `FeatureIdempotentPublish` is withdrawn on **both** products, and that
  withdrawal matters more than it looks — neither has a producer epoch or a
  sequence number, so a *retried* publication is a **second message** unless the
  caller supplied a de-duplication key for it. A caller who read an idempotent-
  publish claim would retry an ambiguous publication and get two messages.
- **The conformance suite needs the topology created first.** This driver
  declares nothing, so a run against an empty broker fails at the first attach.
  `ConformanceHarness` provisions it — on Artemis through this driver's own
  `Admin`, and on Classic by attaching a receiver to each consumer queue and
  letting go again, which is not a workaround but the whole of Classic's
  topology API over AMQP 1.0.
- **Two scenarios must not share a topic on Artemis**, because the derived queue
  name is `<topic>.<subscription>` and an Artemis queue name is unique across the
  whole broker — the second scenario would consume the first's messages. The
  harness's topic names carry a nonce for that reason, and a `.` is mapped out of
  a generated name because `ParseQueue` takes the queue apart at the last one.
- **`Client.Admin()` on Classic reports `ErrUnsupported`** rather than returning
  a surface that refuses every call. The profile supplies no `Admin`, so the
  backend does not satisfy `mq.AdminBackend` at all — which is what the specification asks of
  a driver that does not have the feature.
- **`FeatureAdmin` on Artemis is Conditional, not Native.** The management
  address is a destination like any other, and it needs the connecting user to
  hold the broker's management permissions — which the driver cannot check before
  the surface is used. A deployment that secured or disabled it would leave the
  surface present and every call refused.

## Maturity & tier

See the pinned client version and the module tests when evaluating this driver.

## Driver-specific critical tests

the list for this driver is "ActiveMQ link detach, failover,
protocol-specific redelivery metadata". All six tests below run against the
shared core's **scripted peer** and none needs a container, which is the point:
three of the behaviours this driver has to get right cannot be provoked from a
broker at all — a disposition sent on a link that has since detached, a consumer
cancelled while a fetch is parked, and a publication whose disposition never
arrives.

- **`TestCriticalStaleDispositionIsRefusedOnAReattachedLink`** — the specification's
  disqualifier. A receiving link that detaches and re-attaches starts its
  delivery ids again from zero, so a disposition built for the old link's
  delivery 0 would settle the **new** link's delivery 0: somebody else's message
  acknowledged, the caller's own redelivered, and nothing reporting an error. It
  would catch a driver that sent the settlement anyway — and it checks the other
  half too, that refusing the stale one did not poison the live one.
- **`TestCriticalLinkDetachIsRecovered`** — a broker detaches a link without
  closing the connection (a deleted destination, a stolen link, changed address
  settings; Artemis does all three). It would catch a source that keys
  re-attachment on the **connection** generation alone and therefore holds a dead
  link for ever, failing every fetch with a retryable error the engine then
  retries for ever. It also asserts the generation does **not** move, which would
  catch a fix that invalidated every other link's deliveries as well.
- **`TestCriticalSenderLinkDetachIsAmbiguousAndRecovers`** — the publish side. It
  would catch a publication whose disposition never arrived being reported as a
  definite failure (the ambiguous window), which makes a caller republish and
  the broker hold two copies — and a sink that never recovers afterwards.
- **`TestCriticalConsumerCancelUnblocksAParkedFetch`** —. A fetch parked on
  a link with no messages has nothing to interrupt it: the peer will not send and
  the context is not done, so only the close can wake it. It would catch a driver
  that waited for the context, which hangs for as long as the caller's deadline —
  or for ever, for a caller with none. It also asserts a second `Close` is a
  no-op.
- **`TestCriticalConsumerCancelReleasesUnsettledDeliveries`** — a consumer that
  goes away holding unsettled deliveries must give them back, or they sit in the
  broker's delivering set until the link times out and the next consumer waits
  for them. It would catch a close that dropped them — and, specifically, one
  that released them with `delivery-failed` **set**, which would make
  `MaxAttempts` fire on a message nobody tried.
- **`TestCriticalLinkDetachWhileAFetchIsParkedIsRecovered`** — the state in which
  the link-loss guard on the **read** is the only thing that can recover. A
  source whose whole credit is already with the peer issues no more, so a
  top-up cannot fail and cannot discover the death. It would catch a driver that
  dropped a dead link only when a flow frame failed, which holds this one for
  ever with every fetch failing on a link the window thinks is fine.

Two fuzz targets guard the parts a stranger's input reaches:
**`FuzzActiveMQAnnotations`** (the annotation and application-property mapping)
and **`FuzzActiveMQDestinations`** (the destination derivation for all four
combinations). `allocs_test.go` pins this profile's own hot-path allocation
counts exactly, so a change to the delay or de-duplication rules is a diff in a
test file rather than a number that drifted.

## Choosing between the four combinations

the expected answer is **"feature completeness on Artemis, then
perf → AMQP 1.0 default; STOMP kept for Classic"**, and the measurements bear it
out — with one qualification the row does not mention.

The coverage score is how many of the 39 features each combination supports
**natively**, computed from the protocol's own support table
(`internal/activemq/protocol`) rather than guessed:

So: **AMQP 1.0 is the default**, on both products. **STOMP is kept for
Classic**, and specifically for a caller who needs a server-side filter, because
that is the only thing in the family that the default cannot do.

The qualification is the **nack**, and it is sharper than a coverage score
suggests. A STOMP `NACK` frame **returns no message on either broker** —
measured, on Artemis it is treated as an acknowledgement — so the STOMP arm's
requeue is a subscription reset whose cost falls on *every* delivery the consumer
was holding. A caller who nacks often, or who holds a deep prefetch, wants this
driver and not that one. [activemq-stomp.md](activemq-stomp.md) states the whole
finding first, because it is the one a reader most needs.

What criterion does **not** decide from a benchmark is the second
term, perf. The benchmark measures the cost of the **property mapping**
each protocol's message model demands, which is on the hot path of every
publication and every delivery; the two protocols' throughput against the real
brokers needs two brokers and two client libraries, and 
records that as the criterion's gap. A warning about reading the mapping's
ns/op: the two arms do not do the same amount of work, because the STOMP arm
validates every header value as UTF-8 and checks every header name against the
frame's own while this arm's typed sections need neither — so a faster STOMP row
would be evidence that it checked less, which is exactly why the row puts feature
completeness first.
