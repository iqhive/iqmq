# rabbitmq

<!-- Fixed sections per the driver documentation. Every section must be present. -->

**Maturity:** Experimental · **Tier:** 2 · **Driver:** `github.com/iqhive/iqmq/drivers/rabbitmq`

## Client & version policy

The native baselines for this driver live in `benchmarks/native-baselines/` and measure the clients below against the harness's `rabbitmq` image (`make bench-native CLIENTS="amqp091"`). A driver number is only comparable with a baseline taken from the same client version, broker version and confirmation boundary, so the pins move together; each baseline's package comment records the settlement boundary it measures as well.

| Baseline | Client | Pinned version | Confirmation boundary |
|---|---|---|---|
| `amqp091` | github.com/rabbitmq/amqp091-go | `github.com/rabbitmq/amqp091-go@v1.15.0` | publisher confirm (basic.ack) on a durable quorum queue |

### Verified facts (the API contract)

Checked while building the native baselines; re-verify when a pin moves.

- **Client.** github.com/rabbitmq/amqp091-go is the AMQP 0-9-1 client maintained by the RabbitMQ core team, and it is the client the baseline measures; RabbitMQ 4 also speaks AMQP 1.0 natively, for which github.com/Azure/go-amqp is the Go client.
  _Verified against: amqp091-go v1.14.0 README.md; RabbitMQ 4 release notes._
- **Quorum queues.** Quorum queues count redeliveries in the x-delivery-count header and fsync before returning a publisher confirm, so a confirmed publish to a quorum queue is a much stronger boundary than a memory-durability publish and must not be compared with one.
  _Verified against: RabbitMQ quorum-queue documentation; verified by the amqp091 baseline's declaration._
- **Delayed messages.** Delayed publishing needs the rabbitmq_delayed_message_exchange plugin, which is not part of the official image: the harness builds hack/images/rabbitmq-delayed for it, and a driver must detect the exchange type rather than assume it.
  _Verified against: hack/images/rabbitmq-delayed/Dockerfile; conformance/harness/brokers.go RabbitMQDelayed._

## Topic / subscription mapping

| iqmq | RabbitMQ |
|---|---|
| topic | a durable exchange, `topic`-typed by default (`Config.ExchangeType`), named `<Prefix><topic>` |
| subscription | a durable quorum queue named `<Prefix><topic>.<subscription>`, bound to the exchange with the topic as the routing key |
| dead-letter destination | the exchange `<queue>.dlx` and the queue `<queue>.dlq`, bound with the fixed routing key `dlq` — or, with `Config.DeadLetterTopic`, **another topic's exchange**, so that the destination is consumable through `mq.Client` |
| delay bucket (`ttldlx`) | the queue `<queue>.delay.<milliseconds>` |
| delayed exchange (`plugin`) | the exchange `<exchange>.delayed`, type `x-delayed-message` |
| reply address | `amq.rabbitmq.reply-to`, which is not a topic at all — see **Known gotchas** |

The naming is derived once, in `internal/rabbit`, and is shared with `drivers/rabbitmq/amqp10`: a caller who moves between the two protocol profiles finds the same exchange, the same queue and the same dead-letter route, because two independent derivations would be two topologies wearing one name.

Three naming rules are narrower than the broker's, and each one exists because the alternative is a silent failure.

- **A topic may not contain `*` or `#`, and a wildcard SUBSCRIPTION is not supported either.** The topic is both the exchange's name and the routing key a subscription's queue is bound with, and on a topic exchange those two characters are the binding pattern's wildcards — so a topic literally called `orders.#` would bind a subscription's queue to every routing key under `orders`, and one subscription would receive another topic's messages. A wildcard subscription is refused for a deeper reason: a topic maps to its OWN exchange, so `orders.*` would have to bind one queue to every exchange whose name matches, including exchanges created later, and a binding names one exchange. `FeatureWildcardSubscribe` is therefore **unsupported** and a pattern topic is answered with `ErrUnsupported` rather than with an invalid argument, which is what makes the negative claim provable. Mapping every topic onto one shared exchange WOULD make it work and was rejected: one exchange is one authorisation boundary, so every publisher would need write on every topic. The pattern power is still reachable inside one topic through `Config.KeyAsRoutingKey` and a binding of the operator's own.
- **A subscription may not contain `.`**, because the queue is `<topic>.<subscription>` and a dotted subscription makes that name ambiguous to read and to parse back.
- **`dlq`, `dlx` and `delay` are reserved subscription names.** `<topic>.dlq` is the dead-letter queue of every other subscription of that topic, so a consumer of the `dlq` subscription would be consuming somebody else's dead-letter queue with every name valid and nothing refused.

Nothing is declared unless `Config.Declare` says so, which is the rule. With declaration off, a publisher's open performs a passive `exchange.declare` and a consumer's a passive `queue.declare`, so a topology nobody created is an error at open rather than a client that succeeds and moves no messages.

## Publish path & confirmation boundary

A publication is a `basic.publish` on a channel in confirm mode. There is one channel per publisher, which is what makes each publisher's confirm window, its `basic.return` correlation and its channel exceptions its own.

| `ConfirmationMode` | what this driver waits for |
|---|---|
| `ConfirmNone` | the frame has been written to the socket. Nothing has confirmed anything. |
| `ConfirmAccepted` (the default) | `basic.ack` for this publication's delivery tag. On a quorum queue that means the message was written to a majority of the queue's replicas; on a classic non-durable queue it means the broker holds it in memory. |
| `ConfirmDurable` | the same `basic.ack`, and the sink REFUSES TO OPEN unless the queues are durable quorum queues, so that the boundary really is a durable one. |

**How** the publisher waits is and is chosen by `Config.ConfirmKind`:

| kind | outstanding | blocks | `basic.return` needs a token | a channel close leaves ambiguous |
|---|---|---|---|---|
| `permessage` (the default) | 1 | yes | no | 1 publication |
| `windowed` | `ConfirmWindow` (64) | yes | yes | the whole window |
| `async` | `ConfirmWindow` (64) | no | yes | the whole window |

`FeatureBatchPublish` is therefore claimed as an **emulation** and not as an absence: there is no batch frame and a batch still works, because the core loops and this driver confirms each publication. The level follows the confirm's durability — emulated-durable on a quorum queue, emulated-volatile on a classic one — and `FeatureAtomicBatchPublish` stays unsupported, because there is no frame that could be all-or-nothing.

Two publication outcomes deserve naming because a caller acts on them differently from an ordinary failure.

**Rejected.** With `Config.Mandatory` — **off by default**; see **Known gotchas** — a publication no queue is bound for comes back as `basic.return` and is then acked. The broker took the message and threw it away, which the specification makes a rejection: `CodeNotFound`, not retryable, because retrying will throw it away again. It almost always means the topology is wrong.

The default is the interesting decision and it is worth stating plainly. With `mandatory` ON, a publication to a topic nobody has subscribed to FAILS — a stricter contract than the portable one, and one on which a publisher has to know the subscription topology, which no other driver in this repository asks of a caller. iqmq's own conformance kit publishes to unsubscribed topics in four scenarios and requires every one to succeed. It is still worth turning on in a deployment that provisions its own topology, because the broker's behaviour without it — discard an unroutable message and ack it — is the most common way a RabbitMQ deployment loses messages.

**Ambiguous.** A channel that closes with confirms outstanding leaves every one of those publications unresolved: the broker may have taken them all and may have taken none. The driver reports `CodeAmbiguous` with `Ambiguous` set, which the specification requires and names a settlement disqualifier to get wrong.

## Consume path

A source is a `basic.consume` on its subscription's queue, on a channel of its own, with `basic.qos(prefetch)` sent BEFORE the consume — the broker applies a prefetch to the consumers started after it, so the other order would be an unbounded consumer for the channel's whole life.

**The topology is declared when the source is opened and the `basic.consume` happens on the first `Receive`.** RabbitMQ pushes: a consume makes the broker start sending, and every message it sends counts against the prefetch and starts its acknowledgement deadline whether or not anybody has asked for a delivery — a lease ticking against a caller who has not called `Receive`. Two broker behaviours fall out of the lateness and neither is recoverable inside an eagerly-attached consumer, because both belong to the broker's QUEUE rather than to the client's buffer:

- a message expires by its own TTL while it is QUEUED, and not after it has been pushed to a consumer's buffer;
- a priority queue serves the most urgent message it HOLDS, and no priority can overtake a message that has already left the queue.

Both were failing their conformance probes against the pinned container with the message already sitting in this driver's ring. The declaration stays eager, because a publication made between opening a consumer and its first `Receive` has to reach a queue that exists. The cost is one round trip on the first `Receive`, which a caller was going to spend waiting for a delivery anyway.

RabbitMQ pushes. The SPI asks for a pull `Fetch`, and `driverkit.PushAdapter` is what makes the two meet: a bounded ring whose capacity is the prefetch, so a broker delivering faster than the application consumes is answered by the library's delivery goroutine blocking rather than by unbounded memory.

The prefetch is the ring's capacity, the token table's size and the per-delivery storage's length, all three. The broker will not hold more than `prefetch` unsettled deliveries at one consumer, so anything larger would be storage nothing can fill and anything smaller a ring the broker can overrun. That bound is the BROKER's count, not the driver's, and the two differ for an instant: a settlement writes its frame before it frees its token slot, and the broker answers the frame by sending the next delivery at once, so a delivery can arrive while the table is still full. The driver waits for the slot rather than leaving that delivery unsettled — which, on a live channel, RabbitMQ would never give back.

A consumer the BROKER cancels — which is what a `queue.delete` under a live consumer produces — is reported as `CodeNotFound` and not `ErrClosed`: the caller closed nothing, and the queue is gone.

## Ack / nack / extend mapping

| | AMQP 0-9-1 |
|---|---|
| `Ack` | `basic.ack{multiple=false}` |
| `Nack`, requeue, no delay | `basic.nack{requeue=true}` — back to the HEAD of the queue, marked redelivered |
| `Nack`, requeue, with a delay | The message is re-published and the original acknowledged. See **Delays**. |
| `Nack`, dead-letter | `basic.nack{requeue=false}` onto the queue's `x-dead-letter-exchange`; `ErrUnsupported` when `Config.DeadLetter` is off, and NOTHING is settled |
| `Nack`, drop | `basic.ack`. See **Known gotchas** — this is the mapping that looks wrong. |
| `Extend` | not offered. A delivery's lease here is the channel's lifetime, so there is nothing to extend and nothing to expire. |

Settlement is serialised per channel and INDIVIDUAL. `basic.ack{multiple=true}` would settle every delivery up to a tag, which is safe only when deliveries are settled in the order they arrived; the core makes no such promise, and the specification asks for "individual acks unless cumulative proven safe". It is not proven, so `FeatureBatchSettle` is Unsupported.

Two independent guards stand between a settlement and the wrong delivery, and both are necessary because a delivery tag is per CHANNEL and restarts at 1 on a new one.

1. `driverkit.TokenTable`'s generation, bumped at the moment a channel is lost, refuses every settlement for a delivery handed out before then with `ErrLeaseLost`.
2. The delivery's own channel identity is compared with the source's current one.

The second is **latent**: every route by which this driver replaces a channel invalidates the token table first, so the generation catches every currently reachable case, and the guard is kept for the change that adds a fourth route and forgets the invalidation. `FuzzRabbitTagSettlement` and `TestCriticalStaleTagNeverSettlesOnANewChannel` are the evidence, and both check that a refused settlement sends NO FRAME: a driver that sent the ack and mapped the broker's refusal would, on a broker that did not refuse it, have acknowledged a delivery the caller never saw.

## Ordering

A queue is FIFO, and that is the whole of what this broker guarantees.

- `FeatureOrderedByPartition` is **native**: one queue's messages are delivered in the order they were enqueued, and one consumer settling them in order sees them in order.
- `FeatureOrderedByKey` is **Unsupported by default** and Conditional with `Config.KeyAsRoutingKey`. With the key in a header the broker cannot read it, so two messages under one key can be interleaved by two publishers or spread across a consistent-hash exchange's queues. With the key AS the routing key one key's messages reach one queue, and the subscription's queue then binds with `#`.
- `FeatureOrderedGlobally` is **Unsupported**. Nothing above one queue is ordered.

A nack with `requeue=true` puts the message back at the HEAD, so ordering survives a retry; a DELAYED redelivery does not, because the message is re-published and loses its position.

## Delays

The specification. AMQP 0-9-1 has no delayed redelivery, so the specification's `NackDelay(d)` is built out of what the broker has, and there are two ways:

**`ttldlx` (the default).** The message is re-published into a queue whose `x-message-ttl` is the delay and whose `x-dead-letter-exchange` routes back, and then the original is acknowledged. It works on every RabbitMQ and needs no plugin. It costs:

- **one queue per (target, bucket) pair.** A queue's TTL is fixed at declaration, so one queue serves one delay. The delay is rounded UP to the first bucket of `Config.DelayLadder`, which defaults to twenty-one buckets doubling from 100 ms to a little over a day.
- **a rounding error.** The specification makes a delay a MINIMUM, so rounding up is allowed and rounding down is not; with the doubling ladder the worst case is one tick short of twice the requested delay. A deployment whose delays are known exactly should name them in `Config.DelayLadder` and gets both an exact delay and one queue per delay it really uses.
- **two broker hops** instead of one.

**`plugin`.** The message is re-published to `<exchange>.delayed` with an `x-delay` header and the broker holds it for exactly that long. One exchange serves every delay, the delay is exact and there is one hop. It needs `rabbitmq_delayed_message_exchange`, which is **not part of the official image**: `hack/images/rabbitmq-delayed/` builds one that has it, and the plugin only supports the RabbitMQ minor it was built for.

Two properties of the mechanism a caller has to know:

- **The order is publish, wait for the confirm, then ack.** A crash between the publish and the ack leaves both copies, which is a duplicate and inside the at-least-once; a crash between an ack and a publish would lose the message entirely. Waiting for the confirm is what makes the first option safe, and it is why a CONSUME channel is in confirm mode in this driver.
- **A delayed redelivery restarts the BROKER's attempt count, and the driver carries its own across the hop.** The redelivery counter belongs to the queue entry and a re-publication is a new entry, so without help every delayed redelivery would report attempt 1 for ever and a "dead-letter after three attempts" policy would retry a poison message without end. The driver writes the attempt it knows into the reserved header `x-iqmq-attempt` when it parks the copy and reads it back on the redelivery, taking the LARGER of what the broker counted and what it carried — a max and not a sum, because a requeue of the parked copy raises the broker's count too.

A delay queue dead-letters to the **default exchange** with the subscription's queue name as the routing key, and not to the topic's exchange. A delayed redelivery belongs to the one subscription that nacked the message; the topic exchange would fan it out as a duplicate to every other subscriber.

Scheduled PUBLICATION (`mq.Delay` on a publish) needs the plugin and is `Unsupported` without it. The TTL-and-dead-letter mechanism can delay a redelivery to one queue; a scheduled publication has to reach every subscription of a topic, which would need a delay queue per subscription and would deliver at a different moment to each.

## Dead-letter queue

`Config.DeadLetter` declares the exchange `<queue>.dlx` and the queue `<queue>.dlq` and points each subscription's queue at them through `x-dead-letter-exchange` and `x-dead-letter-routing-key`. It needs `Config.Declare`.

**`Config.DeadLetterTopic` sends the dead letters to another TOPIC instead**, and it exists because the derived destination is a QUEUE: an operator finds `<queue>.dlq` by name from the queue it belongs to, which is the right default, and nothing can consume it through `mq.Client`. The dead-letter destination is a place a caller reads from, so a deployment that wants to read its own failures names a topic and every subscription of that topic receives them. The derived queue is then not declared at all: the queues behind the named topic belong to that topic's own subscriptions, and declaring both would give a rejected message two destinations. The named topic has to exist — RabbitMQ drops a dead-lettered message whose dead-letter exchange is missing, silently — so the driver declares its exchange.

Three details, each of which is a way to lose a dead-lettered message:

- **The dead-letter exchange is declared BEFORE the queue that names it.** RabbitMQ accepts a queue whose `x-dead-letter-exchange` does not exist and then drops every dead-lettered message silently.
- **`x-dead-letter-routing-key` is a fixed word (`dlq`) and not the message's own key.** RabbitMQ dead-letters a message with its original routing key unless the queue names a replacement, so a dead-letter exchange with one binding would route a message keyed `orders.eu` nowhere at all.
- **The dead-letter queue has no dead-letter route of its own.** A dead-letter queue that dead-lettered would send an unprocessable message round a cycle; a message there is meant to sit until somebody looks at it.

Without a dead-letter route, a nack asking for one returns `ErrUnsupported` and settles NOTHING, so the delivery stays settleable and the core's own policy takes over.

## Headers & limits

Headers map to the `basic.properties` field table. It carries a byte array natively, so a binary header value survives byte for byte; what it cannot do is carry a REPEATED key, because it is a map. The specification promises duplicate-preserving headers, so:

| `MetadataMode` | a repeated header key |
|---|---|
| `MetadataNativeStrict` (the default) | refused with `ErrUnsupported` before any I/O |
| `MetadataPortableEnvelope` | carried, inside iqmq's envelope in the body |
| `MetadataAdaptive` | carried, and only the messages that need it are enveloped |
| `MetadataLenientDrop` | dropped, and counted |

the typed properties map to the fixed `basic.properties` fields — content type, content encoding, correlation id, reply-to, message id, timestamp — except `CausationID`, which has no slot and travels in the table under `x-iqmq-causation`.

Four table entries are this driver's own and are refused as caller headers. None of them ever reaches an application: a round trip through this driver adds no header the publisher did not set.

| entry | what it carries | why the protocol has no slot |
|---|---|---|
| `x-iqmq-key` | the message key | a key is bytes and a routing key is a shortstr, and using the key as the routing key changes the ROUTING (`Config.KeyAsRoutingKey`) |
| `x-iqmq-pub` | the publisher-confirm correlation token | `basic.return` carries the returned message and **not** its delivery tag, so with more than one publication outstanding there is nothing else to match on |
| `x-iqmq-nil` | that the published body was NIL rather than empty | a body is a sequence of content frames and a zero-length body is no frames, which is what a nil body is too; the specification makes the distinction observable |
| `x-iqmq-attempt` | the attempt count across a delayed redelivery | the broker's counter belongs to the queue ENTRY and a parked copy is a new entry  |

An empty body comes back as a non-nil zero-length slice and a nil body comes back nil, in both directions, which is what the carrier is for. The reading is guarded on the body's length so that a native publisher cannot forge a body away.

Limits:

| limit | advertised |
|---|---|
| `MaxHeaderBytes` | 255 — the protocol's own, because a field-table key is a shortstr |
| `MaxMessageBytes` | **not advertised.** RabbitMQ's `max_message_size` is a server configuration readable through the HTTP management API, and the specification makes an unadvertised limit distinct from one advertised as zero. A driver that guessed would be advertising a guess. |
| `MaxHeaderCount`, `MaxBatchMessages`, `MaxBatchBytes`, `MaxLease` | not advertised: the protocol bounds none of them and the lease is the channel's lifetime |
| `MaxDelay` | the delay strategy's ceiling: the ladder's largest bucket, or a little under twenty-five days for the plugin |

## Reconnect

This driver owns its dial loop, which the specification requires and which the client library can also do itself since v1.14 (see **Client & version policy**). A reconnection is four things in this order, and a failure in any of them is answered by another attempt rather than by a half-restored client:

1. **Dial.** `Config.Addrs` in order; the first that answers wins. The backoff is `driverkit.Reconnector`'s under `Config.BackoffPolicy`, which defaults to **full jitter** and NOT to the specification's `Auto` — whose current winner is `none`, i.e. no delay at all, which the specification's "reconnect with backoff (never busy-spin)" forbids. Since `driverkit.ReconnectConfig.Policy`'s own zero value is a jittered policy too, so this default agrees with the helper's rather than working around it. A caller who really wants no delay sets `backoff.None` and gets it; what they cannot get by accident is a busy dial loop.
2. **Watch.** The connection's own close and its `connection.blocked` notifications are registered before anything is published.
3. **Replay the topology.** Everything this client declared is declared again, in the order it was declared. The durable exchanges and quorum queues survive, and the DELAY queues may not — `x-expires` deletes an unused one — so a client that did not replay would come back unable to delay a redelivery.
4. **Re-attach the handles.** Every sink gets a new confirm-mode channel and every source a new consumer with its prefetch. It happens inside the dial step, so `Health.Ready` means the client can work again.

Before any of that, and at the moment the loss is noticed: every sink resolves the publications it was holding as **Ambiguous**, and every source bumps its token generation and discards the deliveries it had buffered. A settlement carrying a token from before then reports `ErrLeaseLost`.

A CHANNEL exception is narrower and is repaired by the HANDLE itself, once and without a loop, on both sides: a consumer attaches a new channel and a new `basic.consume`, and since a publisher does the same. Once, because a channel exception that repeats is a topology problem the caller has to fix and a driver that re-attached for ever would hide it behind an empty `Fetch` or a publisher that never publishes. Whichever route notices the loss first CLAIMS it, so a channel the supervision loop has already taken responsibility for — every channel on a lost connection — is repaired by the dial loop and not twice.

`connection.blocked` — RabbitMQ under a memory or disk alarm — is reported through health and the reconnect hook, because it is the only explanation a stalled publisher has: the broker stops READING from a publishing connection, so a publication blocks in the socket with no error and no timeout of its own. `Backend.Blocked` reads it directly.

## At-least-once window

The window is from the broker's `basic.deliver` to this driver's `basic.ack`. A process that stops inside it — a crash, a channel close, a connection loss — leaves the delivery unacknowledged, and RabbitMQ redelivers it to whichever consumer of that queue is next. So a handler may run twice for one message, and the redelivery accounting is the core's rather than this driver's.

For a PUBLICATION the window is from the `basic.publish` to its `basic.ack`. A publication whose confirm never arrives is Ambiguous, and a caller that retries it may duplicate the message; one whose channel closed is the same.

A delayed redelivery adds a second window of its own: the parked copy is published and confirmed before the original is acknowledged, so a crash between them leaves both. That is the duplicate-over-loss choice and it is deliberate.

**`FeatureAtMostOnce` is withdrawn.** `basic.consume` has a `no-ack` mode and this driver never uses it — every consumer settles explicitly, which is what a `Settler` is for — so a claim of at-most-once delivery would be a claim about a code path that does not exist. The AMQP 1.0 profile withdraws it for the same reason.

## Config & DSN

    rabbitmq://user:password@host:5672,host2:5672/vhost?declare=true&deadletter=true&deadlettertopic=parked&mandatory=true&confirm=windowed&confirmwindow=64

`Config` is the whole surface; the DSN is the same settings in a connection string, with strict unknown-parameter rejection — a parameter this driver does not have is a construction error naming it rather than a setting nobody notices was ignored. `Config.DSN()` renders a redacted round trip: everything but the password parses back.

The settings that change behaviour rather than tuning it:

| field | default | what it decides |
|---|---|---|
| `Declare` | **off** | whether this driver creates its own topology |
| `DeadLetter` | off | whether a dead-letter route exists at all |
| `DeadLetterTopic` | empty, so the destination is the derived `<queue>.dlq` | whether the dead letters land somewhere `mq.Client` can consume |
| `QueueType` | `quorum` | replication, and whether `x-delivery-count` exists |
| `ExchangeType` | `topic` | whether a wildcard subscription is expressible |
| `Mandatory` | **off** | whether an unroutable publication is Rejected or discarded and acked |
| `KeyAsRoutingKey` | off | whether ordering by key is expressible |
| `ConfirmKind` | `auto` → `permessage` | |
| `DelayKind` | `auto` → `ttldlx` | |
| `DetectDelayPlugin` | off | whether the driver probes for the plugin and upgrades |
| `MaxPriority` | 0, so priority is Unsupported | whether `mq.Priority` means anything, and it forces a CLASSIC queue |
| `BackoffPolicy` | `full` (jitter), explicitly and NOT the specification's `auto` | how the dial loop waits between attempts |

## Known gotchas

**`mandatory` is OFF by default, and it is worth turning on.** Without it the broker DISCARDS an unroutable message and acknowledges it, so a publisher with a misconfigured topology succeeds for ever while nothing arrives — the single most common way a RabbitMQ deployment loses messages. `Config.Mandatory` turns it on and it costs one returned frame on the failing path and nothing on the working one.

The default is off because ON is a stricter contract than the portable one: a publication to a topic nobody has subscribed to fails, so a publisher has to know the subscription topology, which no other driver in this repository asks of a caller. Set it in a deployment that provisions its own topology and publishes only to topics it created; leave it off in a library that does not know its caller's topology.

**`Nack(NackDrop())` is a `basic.ack`.** Drop destroys the message, and `basic.ack` is the frame that removes a message from a queue without routing it anywhere; `basic.nack{requeue=false}` would DEAD-LETTER it whenever the queue has a route, which is the opposite of destroying it.

**A reply address is not a topic.** `amq.rabbitmq.reply-to` is a routing key the broker minted, on the default exchange, under the reserved `amq.` prefix that the shared naming refuses to derive from. A responder publishes a reply to `Delivery.Properties.ReplyTo`, which reaches this driver as a topic — so the sink recognises a reply address (`rabbitmq.IsReplyAddress`) and routes around the naming layer entirely. It is the one special case in the publish path.

**Native request/reply overwrites `CorrelationID` and `ReplyTo`.** Direct reply-to means the driver owns the whole exchange, including the correlation, which is what the specification gives a native path. A caller who set either on a request would have it replaced.

**A priority needs the queue argument, or the broker ignores it.** `x-max-priority` is fixed at declaration, and a priority on a message to a queue that was not declared with one is accepted and ignored — the worst kind of unsupported. `FeaturePriority` is Unsupported without `Config.MaxPriority`, and the driver REFUSES the option rather than letting the broker swallow it. Setting it forces a classic queue, because RabbitMQ refuses `x-max-priority` on a quorum one — so a priority topic has no attempt count.

**A classic queue has no attempt count.** The redelivery counter is a quorum-queue field. On a classic queue Attempt is zero — which the specification makes "the broker does not count attempts" — unless the delivery is a delayed redelivery this driver parked, whose attempt the driver carries itself. MaxAttempts cannot be enforced from the broker's own number on a classic queue.

**A start position of a time or a sequence is REFUSED.** A queue has no history and no position: a message that has not been settled is at the head and one that has is gone, so `StartLatest` and `StartEarliest` are the same place. Refusing the other two is what makes the seek claims provable in the negative direction rather than leaving a caller planning on a replay that silently never happens.

**`Client.Admin().ListTopics` lists what THIS CLIENT declared.** AMQP 0-9-1 has no way to list exchanges at all — the protocol has no such method, and RabbitMQ exposes the list through its HTTP management API, a different service on a different port with its own credentials.

**`Stats` reports no lag, and no other client's in-flight count.** A queue has no position, so there is nothing to be behind; and the unacknowledged count this client can see is its own, because another consumer's is invisible from this connection. The rule that an unknown is never an overloaded zero is what makes saying so possible.

**One channel per publisher and per consumer.** A channel exception closes the CHANNEL, not just the operation, and on a confirm channel it takes every publication whose confirm had not arrived with it. Isolating the blast radius to one handle is what keeps one malformed publication from failing another publisher's in-flight messages; the cost is a channel per handle, which is a small integer to the broker and no socket at all.

**A publisher repairs an exception on its own channel ONCE — and if that repair fails, the publisher is finished.** A channel exception on a publisher's channel (a publish to an exchange that has been deleted is enough) detaches the publisher, and its own loop attaches a new channel, exactly as a consumer's does: no reconnection, and no loop, because an exception that repeats is a topology problem the caller has to fix. The publications the dead channel was holding are resolved AMBIGUOUS, which they are.

While a channel is being attached, a publication is refused with `CodeUnavailable` and **`Retryable`**, and `Health` answers Degraded — the connection is up, so nothing else would have said anything was wrong.

If the one repair attempt FAILS — the broker is at `channel_max`, the exchange really is gone, the declaration is refused — nothing will attach another channel until the connection is replaced, which on a channel exception may be never. That state has its own answer and it is not retryable: the refusal says so and tells you to **close this publisher and open a new one**, and it carries the broker's own reason; `Health` reports that reason rather than the generic one, so an operator can tell a client that is mending itself from one that is not. Retrying in place is the one thing that cannot work.

**A wildcard subscription is unsupported, not merely unimplemented.** See **Topic / subscription mapping**: a topic maps to its own exchange, so a pattern would have to bind one queue to every matching exchange. The refusal is `ErrUnsupported`.

**`(verify)` items.** The redelivery counter → Attempt on quorum queues is **verified** (`conformance/matrix/rabbitmq_test.go` nacks a delivery on 3.13, 4.1 and 4.3 and reads the count, and names the header it arrived in). The AMQP 1.0 profile's `>= 4` gate is **verified** in the same matrix, in both directions.

## Maturity & tier

See the pinned client version and the module tests when evaluating this driver.

## Driver-specific critical tests

In `drivers/rabbitmq/critical_test.go`, `reattach_test.go`, `sinkrepair_test.go` and `latechannelclose_test.go`, each against the scripted fake broker in `internal/rabbitfake` — every one of them is unreachable from the portable API against a healthy container, which is why the fake exists.

| test | what it proves |
|---|---|
| `TestCriticalChannelCloseDuringConfirm` | a channel that closes with four publications outstanding reports all four AMBIGUOUS, not failed. The library's own answer is a nack. |
| `TestCriticalMandatoryReturnIsAttributedToTheRightPublication` | with three publications outstanding on one channel and the binding removed for exactly one of them, the unroutable one is Rejected and the other two Confirmed. `basic.return` carries no delivery tag. |
| `TestCriticalConsumerCancel` | a broker-side cancel reports `CodeNotFound` and not `ErrClosed`, and refuses the settlements the consumer was holding without sending a frame. |
| `TestCriticalStaleTagNeverSettlesOnANewChannel` | a delivery tag from a replaced channel never settles on the new one, and NO frame reaches the broker — the guard refuses before the write. |
| `TestCriticalDelayedNackDoesNotLoseTheMessageWhenTheParkFails` | a delayed redelivery whose parked copy the broker refused leaves the original unacknowledged, so the message is redelivered rather than destroyed. |
| `TestCriticalReconnectRestoresTheTopology` | a connection loss against a broker that lost its topology restores the exchanges, the queues, the bindings, the dead-letter route AND the delay queues. |
| `TestCriticalReconnectIsNotRestoredMidReplay` | nothing reports the client restored while the replay is half done. The replay is stepped one declaration at a time through `rabbitfake.Broker.HoldDeclarations`, and at the instant the broker holds one queue again — which is where this test's predecessor used to run its assertions — the topology is incomplete, `RestoresForTest` has not risen and health is not Ready. |
| `TestCriticalConnectionLossResolvesWhatItWasHolding` | a lost CONNECTION, rather than a closed channel, resolves every outstanding publication AMBIGUOUS and refuses every delivery it had handed out with `ErrLeaseLost` — and does it through `driverkit.ReconnectConfig.Invalidate`, before the first dial attempt. |
| `TestCriticalASettlementOnAClosedChannelIsLeaseLost` | a settlement of a delivery whose channel has closed is refused with `ErrLeaseLost` and no frame even before the consumer has noticed the close, rather than reported SettlementUnknown . |
| `TestCriticalReconnectSurvivesALateChannelClose` | a publisher's close notification for its PREVIOUS channel, arriving after the reconnection re-attached it, does not detach the new channel . |
| `TestCriticalAReattachDoesNotOrphanALiveChannel` | the dial loop's re-attach of a consumer already attached on the current connection does nothing: one consumer on the broker, the held delivery still settleable, every message delivered . |
| `TestCriticalARedialThatFailsPartwayReattachesEveryHandle` | a dial attempt that fails after attaching some handles leaves them on a closed connection; the next attempt re-attaches every one of them, with their own close notifications held so nothing else can . |
| `TestCriticalAConsumerReplacedByARetriedDialWakesItsParkedNack` | a consumer channel retired that way wakes a delayed nack parked on it at once, rather than after the thirty-second publish timeout. |
| `TestCriticalAPublisherOpenedDuringAReplayIsAttachedOnce` | a publisher opened while the dial loop replays the topology is attached once, and a confirm on its channel resolves the publication it names . |
| `TestCriticalALateConfirmFromAReplacedChannelResolvesNothing` | a confirm from a publisher's replaced channel is dropped: the new channel's tag sequence restarted at one, so applying it would confirm a publication the broker nacked. |
| `TestCriticalAPublisherReplacedByARetriedDialResolvesWhatItHeld` | a publisher channel the dial loop replaces resolves what it held as AMBIGUOUS before its tags are reused; no callback is left waiting for ever. |
| `TestCriticalAFullTokenTableWaitsRatherThanStrands` | a delivery that arrives while every token slot is still held waits for one, instead of being left unsettled on a live channel where the broker never gives it back . |
| `TestCriticalEveryChannelExceptionIsRepaired` | a thousand channel exceptions in a row are each answered with a new channel and one consumer . |
| `TestCriticalAPoisonDeliveryDuringAChannelExceptionIsRepaired` | a channel exception with undecodable messages in flight is still answered: the dead-letter nack failing on the closed channel no longer stops the consumer's pump . |
| `TestCriticalAChannelExceptionDuringASlotWaitIsAnswered` | a channel exception while the pump waits for a token slot is answered at once, not when a caller next settles something . |
| `TestCriticalACloseDuringASlotWaitReturns`, `TestCriticalAConnectionLossDuringASlotWaitRecovers` | a close or a connection loss during that wait ends it, and the broker has the waiting message back. |
| `TestCriticalAPublisherRepairsItsOwnChannelException` | a channel exception on a publisher's channel is repaired by the publisher, once, with no reconnection and no second channel . |
| `TestCriticalAPublisherRepairDoesNotRaceARedial` | a publisher's close notification that arrives while the dial loop is mid-replay repairs nothing: the supervision loop had already claimed that loss . |
| `TestCriticalAPublisherRepairRacingCloseInstallsNothing` | a repair that is opening its channel when the publisher is closed installs nothing and leaves no channel open . |
| `TestCriticalAPublisherWithNoChannelIsNotReady` | `Health` answers Degraded, and the refusal stops blaming the dial loop, while a publisher has no channel . |
| `TestCriticalAForeignConsumerCancelDoesNotStrandThePump` | a `basic.cancel` naming another consumer does not end the pump, so the channel's close is still answered . |
| `TestCriticalHealthDoesNotWaitForAPublicationInFlight` | `Health` answers while a publication is parked in the socket write, because it reads no lock a write is held across . |
| `TestCriticalAPublicationThatMetADeadChannelStillLeavesTheRepairToTheLoop` | a publication that discovers the dead channel first detaches without claiming the loss, so the close notification still repairs it . |
| `TestCriticalAPublisherWhoseRepairFailedSaysSoAndStopsBlaming` | a repair that failed is refused as not-retryable, with the broker's reason and the advice to reopen . |
| `TestCriticalAFailedRepairDoesNotPinHealthDegraded` | closing the broken publisher and opening a working one returns the client to Ready . |
| `TestCriticalAConsumerWithNoChannelIsNotReady` | the consumer half of the health answer, in both directions . |
| `TestCriticalAConsumerThatNeverFetchedStaysReady`, `TestCriticalAConsumerTheBrokerCancelledStaysReady` | the two consumers that have no channel on purpose stay Ready . |

`FuzzRabbitTagSettlement` drives the whole fetch-and-settle path against channel closes, connection losses and broker-side cancels, and checks that every settlement names the delivery the caller settled, that a refused settlement sends nothing, and that a delivery whose channel was replaced can never settle successfully. `FuzzRabbitNaming` and `FuzzRabbitVersion` are in `internal/rabbit`; `FuzzRabbitConfirm` is in `internal/rabbit/confirm`; `FuzzRabbitDelay` in `internal/rabbit/delay`; `FuzzRabbitProtocol` in `internal/rabbit/protocol`.
