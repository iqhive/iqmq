# snssqs

<!-- Fixed sections per the driver documentation. Every section must be present. -->

**Maturity:** Experimental · **Tier:** 1 · **Owner:** iqmq maintainers

A **topic** is an SNS topic. A **subscription** is an SQS queue subscribed to it
with `RawMessageDelivery=true`. One publication reaches every subscribed queue,
and each queue's consumers compete for their own copy. Everything from the
queue inward is `drivers/sqs` — this driver opens a whole SQS backend over the
same credentials and wraps its sources.

## Client & version policy

Two clients, pinned together, because a fan-out speaks to two services.

| Client | Pinned version | What it does here |
|---|---|---|
| `github.com/aws/aws-sdk-go-v2/service/sns` | `v1.46.0` | the publish path, the topic and the subscription |
| `github.com/aws/aws-sdk-go-v2/service/sqs` | `v1.51.0` | everything from the queue inward, through `drivers/sqs` |

`DriverInfo.ClientName`/`ClientVersion` name the SNS client, because that is
what this module wraps; `Backend.SQS().Info()` names the other one.

### Verified facts (the API contract)

Checked while building the driver and the native baselines; re-verify when a
pin moves.

- **fan-out shape.** SNS-to-SQS fan-out is measured through the SQS side: the publish boundary is the SNS Publish response, the delivery boundary is the subscribing queue, and a subscription filter policy is evaluated by SNS on message attributes (or on the body with FilterPolicyScope MessageBody), so a driver that maps headers to attributes changes what the filter can see.
  _Verified against: AWS SNS message-filtering documentation; conformance/harness/brokers.go LocalStack._
- **message size.** An SNS message is at most 256 KiB (262,144 bytes, not characters) — a QUARTER of the mebibyte an SQS queue takes — and `PublishBatch` takes at most 10 entries with the same 256 KiB applying to the sum of a batch's payloads.
  _Verified against: aws-sdk-go-v2 service/sns v1.46.0 api_op_Publish.go (Message constraints), api_op_PublishBatch.go._
- **body character set.** An SNS `Message` must be a UTF-8 string under the same XML character set an SQS body is (#x9, #xA, #xD, #x20-#xD7FF, #xE000-#xFFFD, #x10000-#x10FFFF), and it may not be empty; so `Config.Body` is the same three-way policy `drivers/sqs` has, and a nil or empty payload travels as a placeholder with the `iqmq-enc` marker.
  _Verified against: aws-sdk-go-v2 service/sns v1.46.0 api_op_Publish.go (Message is required, UTF-8, 256 KB)._
- **no per-message timer.** `PublishInput` has no delay or delivery-time parameter of any kind. An SQS queue has `DelaySeconds`; SNS has nothing, so `mq.Delay` and `PublishOptions.DeliverAt` are refused before any I/O.
  _Verified against: aws-sdk-go-v2 service/sns v1.46.0 api_op_Publish.go (the whole input struct)._
- **raw message delivery.** `RawMessageDelivery=true` on an SQS subscription makes the queue's message body the published body and maps the SNS message attributes onto the SQS message attributes unchanged. Without it the body is a JSON notification (`{"Type":"Notification","MessageId":…,"Message":"<the body>","MessageAttributes":{…}}`) and the SQS message carries no attributes.
  _Verified against: aws-sdk-go-v2 service/sns v1.46.0 api_op_Subscribe.go (Attributes, RawMessageDelivery); AWS SNS raw-message-delivery documentation._
- **subscription identity.** An SNS subscription has no name: `Subscribe` returns an ARN and a subscription is identified by its protocol and endpoint. There is no "get the subscription for this topic and this queue" call, so the driver lists the topic's subscriptions and matches on the endpoint ARN.
  _Verified against: aws-sdk-go-v2 service/sns v1.46.0 api_op_Subscribe.go, api_op_ListSubscriptionsByTopic.go, types.Subscription._
- **queue access policy.** A real account needs the destination queue's `Policy` to allow `sns.amazonaws.com` to `sqs:SendMessage` on it, conditioned on the topic's ARN; without it SNS cannot deliver. Neither emulator enforces it, so no test in this repository proves it works.
  _Verified against: AWS SNS "Fanout to Amazon SQS queues" documentation; conformance/harness/brokers.go LocalStack (no IAM enforcement)._
- **emulator evidence.** LocalStack is a functional stand-in, not performance evidence, and it is the only one here: ElasticMQ implements SQS and no SNS at all. Numbers or claims taken against it describe the client and the container, never the managed service.
  _Verified against: conformance/harness/brokers.go LocalStack; the performance budgets layer 4._

## Topic / subscription mapping

A portable topic maps to an SNS topic **name** by a pure function, and the name
maps to an **ARN** by one `CreateTopic` (idempotent at the service) whose answer
is cached for the life of the backend. A `(topic, subscription)` pair maps to an
SQS queue **name**, and the queue's **ARN** — read from the queue, never built
from its URL — is what `Subscribe` takes as an endpoint.

| Portable | SNS / SQS |
|---|---|
| topic `orders/eu` | SNS topic `<TopicPrefix>orders-eu` (`/` → `-`) |
| topic named in `Config.TopicARNs` | that ARN, with no lookup at all |
| topic that is already an SNS ARN | used verbatim, **verified** with `GetTopicAttributes` |
| topic `orders/eu` + subscription `billing` | SQS queue `<QueuePrefix><TopicPrefix>orders-eu-billing` |
| `SourceConfig.Subscription` | the QUEUE — the opposite of `drivers/sqs`, where it is ignored |
| `DeliveryInfo.Topic` | the SNS topic (the inner driver's queue name is replaced) |
| `DeliveryInfo.Source` | the queue URL the copy came from |
| `Config.DeadLetterQueue` | a queue NAME, per subscription |

Five things about it are load-bearing.

**The mapping is not injective.** `orders/eu` and `orders-eu` are one SNS
topic, because an SNS topic name takes alphanumerics, hyphens and underscores
and nothing else. It is stated rather than hidden, and `Config.Resolver`,
`Config.TopicARNs` and `Config.QueueResolver` are the ways out
(for the queue half).

**A composed queue name that will not fit is hashed, not refused.** An SNS
topic name may be 256 characters and a queue name may be 80, so two entirely
legal inputs compose past the queue side's budget. The composed name then keeps
a readable head and ends in sixteen hex characters of SHA-256 over the whole
identity — prefix, topic and subscription. Truncating alone would give two
different pairs one queue, and one subscription's consumers would settle the
other's messages. A subscription whose *characters* a queue name cannot hold is
still refused on its own, so which answer a deployment gets does not depend on
how long its topic is.

**A topic ARN reaches this driver only through the SPI or `Config.TopicARNs`.**
`mq.ValidateTopic` refuses a name with colons in it, so `Client.Publisher` never
passes one through. The verbatim branch is for a caller composing the SPI
directly; a caller using the portable client names topics and maps them.

**A topic carrying a pattern metacharacter (`*`, `?`, `#`, `>`) is refused with
`ErrUnsupported`,** before the portable grammar is consulted. An SNS
subscription names one topic ARN and the service has no pattern subscription;
the refusal has to be `ErrUnsupported` because that is what the negative
probe for `FeatureWildcardSubscribe` reads.

**A `.fifo` topic name is refused.** SNS's FIFO topic is a different capability
table — ordering, deduplication, FIFO-queue subscribers only — and this driver
implements the standard topic, so the name is refused with `ErrUnsupported`
rather than accepted into a profile that does not exist. `DriverInfo.Profile` is
`"standard"` and says so.

`Config.AutoCreate` is off by default. A fan-out has THREE things to
create — a topic, a queue and a subscription — so a driver that provisioned by
default would turn a typo into three resources with a monthly bill.
`Admin().EnsureSubscription` is the explicit form, and on this broker it matters
more than on most: **a publication to a topic with no subscriptions is accepted
and delivered nowhere**, so a subscriber created after a publication never sees
it.

## Publish path & confirmation boundary

| Mode | Boundary |
|---|---|
| `ConfirmNone` | the SNS `Publish` response — there is no weaker boundary; the SDK has no fire-and-forget publication |
| `ConfirmAccepted` | the SNS `Publish` response |
| `ConfirmDurable` | the SNS `Publish` response |

All three are the same boundary and none costs an extra round trip: the service
has stored the message redundantly across availability zones before it answers.
`Receipt.Confirmation` reports the **resolved** mode, which is what the specification asks —
the receipt says what this publication waited for, not the strongest thing that
happened to be true.

**What the response does not mean.** It does not mean a copy is in any queue.
The fan-out happens after it, so a consumer may see the message a moment later,
and a subscription created after the publication never sees it at all. A caller
that read `ConfirmDurable` as "a consumer can see it now" would be reading a
promise nobody made; the capability note says so.

`Receipt.MessageID` is SNS's `MessageId`, which identifies the **publication**.
Each subscribing queue mints its own SQS `MessageId` for its own copy, so the
identifier a publisher holds and the identifier a consumer sees are different
numbers. Correlate on `Message.ID` (which travels in the `iqmq-id` attribute),
not on the receipt.

`PublishBatch` sends at most ten entries per call and splits on both the entry
count and the 256 KiB payload sum. Each entry's outcome is its own: the service
answers with a `Successful` list and a `Failed` list, and the result is a
`*mq.BatchError` whose `Errs` are **index-aligned with the caller's slice** —
not with the chunk, not with the failures. The entry identifier sent to the
service is the message's index, which is what makes the correlation exact
rather than positional. `FeatureAtomicBatchPublish` is not claimed, because a
partial batch is exactly what happens.

**Refused before any I/O** (and the specification makes a silent drop a
disqualifier):

| Option | Why |
|---|---|
| `mq.Delay`, `PublishOptions.DeliverAt` | SNS has no per-message timer at all  |
| `mq.TTL` | no per-message expiry; the queue's `MessageRetentionPeriod` is the only one |
| `mq.Priority` | no priority |
| `PublishOptions.Partition` | no partitions |
| `mq.DedupKey` | a standard topic has no deduplication; a FIFO topic does and is refused by name |
| a body over `Config.MaxMessageBytes` | `ErrMessageTooLarge` |
| a header key colliding with `iqmq-*` | the reserved prefix, refused in every metadata mode |

The delay refusal happens **before** the shared mapping runs, and that ordering
is the point: the mapping is `drivers/sqs`'s, an SQS queue *does* have
`DelaySeconds`, and it would happily return a publication with the delay in a
field SNS's `Publish` has no parameter for.

## Consume path

`Config.Pollers` concurrent `ReceiveMessage` calls per source, each a long poll
of `Config.WaitTime` (default and maximum twenty seconds) for at most
`Config.MaxMessages` (default and maximum ten) messages, feeding one bounded
ring (`driverkit.PushAdapter`) whose bound is what turns a slow consumer into
backpressure on the pollers. It is `drivers/sqs`'s consume path exactly, on the
subscription's own queue.

The one thing this driver adds is putting the **portable topic back**. The inner
driver is opened with the queue NAME as its topic — on that driver a topic *is*
a queue — so every delivery arrives naming the queue, and the specification wants "the topic
the message was published to". `DeliveryInfo.Source` is left as the queue URL,
because that is the field that should say which copy this is.

`ApproximateReceiveCount` → `DeliveryInfo.Attempt`, per copy. A count the
service did not send reads as zero, which the specification makes "the broker does not count
attempts" rather than "attempt zero".

`StartLatest` and `StartEarliest` are the same position and both are accepted; a
time or a sequence is refused with `ErrUnsupported` before any I/O. A queue has
no history: a settled message is deleted and an unsettled one is at the head.

## Ack / nack / extend mapping

Every one of these acts on **one copy in one queue**.

| Portable | SNS / SQS |
|---|---|
| `Ack` | `DeleteMessage`, or `DeleteMessageBatch` through `driverkit.BatchSettler` |
| `Nack` (requeue) | `ChangeMessageVisibility` with timeout 0 — immediately visible, attempt count incremented |
| `Nack` with a delay | `ChangeMessageVisibility` with the delay, bounded by twelve hours |
| `Nack` (drop) | `DeleteMessage` — the one action that destroys the copy |
| `Reject` / `Nack` (dead-letter) | `SendMessage` to `Config.DeadLetterQueue`, then delete the original **only once the send is confirmed** |
| `Extend` | `ChangeMessageVisibility` with the new timeout, measured from the call |
| a settlement whose window has passed | `ErrLeaseLost`, **before** the API call |
| a second settlement of one delivery | `ErrAlreadySettled` |
| a settlement racing another | `ErrSettlementInProgress` |
| an ambiguous settlement | `ErrSettlementUnknown`, sticky for ever |

**The local lease check is why a stale settlement is refused at all.** The
service accepts a `DeleteMessage` for a receipt handle whose message another
consumer now holds: the delete does not fail, and the message is gone. There is
no API that asks "do I still hold this?", so the refusal can only happen before
the call (inherited).

**Settlement is per queue.** This is the fan-out's one trap and it is worth
stating twice: one publication becomes N messages with N receipt handles, N
visibility windows and N attempt counts. A subscriber that acknowledges has
acknowledged its copy; the other N−1 are untouched. There is no operation in
this driver that settles "the publication", and `Backend.OpenSource` keeps no
per-topic cache that could make one. The specification's
`fanout/isolation` scenario checks exactly this against the container.

## Ordering

**Nothing is ordered.** A standard SNS topic delivering into standard SQS
queues orders neither a key's messages, nor a partition's, nor the topic's, and
the fan-out adds a second reason: a publication becomes N independent messages
in N queues, so there is no shared order to speak of even when each queue
happens to deliver in order.

`FeatureOrderedByKey`, `FeatureOrderedByPartition` and `FeatureOrderedGlobally`
are all absent. The guarantee "one key's messages are delivered in order"
does not hold here, and a caller that needs it wants an SNS **FIFO** topic
delivering into FIFO queues — which is a different capability table and a
profile this driver refuses by name rather than implements.

The conformance report **skips** the three negative ordering scenarios, and the
reason is the kit's rather than the driver's: the negative probe publishes
thirty-two messages under one key and fails the driver if they arrive in order.
That is the right test for a FEATURE ("the driver says it cannot do this; prove
it refuses") and the wrong one for a GUARANTEE ("the service does not promise
this"), and a single consumer against an emulator never reorders at all. The
skip reason is published in the report.

## Delays

**Publication delay: none.** SNS has no per-message timer, so `mq.Delay` and
`PublishOptions.DeliverAt` are refused with `ErrUnsupported` before any I/O and
`FeatureScheduledPublish` is absent. `Limits.MaxDelay` is left **unknown**
rather than zero: a zero with `Known` true would say "the longest delay is
nothing", which is a different claim.

This is the difference a caller porting a publisher from `drivers/sqs` meets
first — that driver claims `FeatureScheduledPublish` natively, because a queue
has `DelaySeconds`.

**Redelivery delay: native, per copy.** `Nack` with a delay is
`ChangeMessageVisibility` with it, bounded by the queue's twelve-hour visibility
timeout. The delay survives a process death, because it is the queue that holds
it. Whole seconds, rounded **up**, so a sub-second delay is never withdrawn.

A deployment that needs a delayed *publication* publishes to an SQS queue
directly with `drivers/sqs` and has SNS deliver into it, or uses a
`DelaySeconds` default on the queue (which is a queue attribute, not a
publication control).

## Dead-letter queue

Two routes, both **per subscription**, which is the only place a fan-out can put
one: a message N subscribers failed to handle failed N times independently, so
there is no dead-lettering "the publication".

1. **The queue's own redrive policy** (`Config.DeadLetterQueue` +
   `Config.MaxReceiveCount`, or `Backend.SQS()`'s `TypedAdmin.SetRedrivePolicy`)
   moves a copy the service has delivered `MaxReceiveCount` times, with no
   settlement from the consumer at all. It is what makes a message nobody ever
   settles safe.
2. **An explicit `Reject`** sends the copy to `Config.DeadLetterQueue` with its
   provenance (`iqmq-dlq-origin`, `iqmq-dlq-reason`) and deletes the original
   only once that send is confirmed. A failed or ambiguous send never
   acknowledges the original. A `Reject` with no `DeadLetterQueue`
   configured is refused with `ErrUnsupported` rather than destroying the
   message.

`FeatureDeadLetter` is claimed as `EmulatedDurable` with both routes in the
note, because the destination is an SQS queue and therefore durable.

**SNS's own subscription `RedrivePolicy` is not wired.** That is the
dead-letter queue for deliveries *SNS itself* could not make — a queue that has
been deleted, a permission that was revoked — and it is a different failure from
a consumer that could not process a message. It is the operator's, through the
console or Terraform; `TypedAdmin.Attributes` reads it.

## Headers & limits

| Limit | Value | Source |
|---|---|---|
| `MaxMessageBytes` | 262,144 (256 KiB) | SNS `Publish` |
| `MaxBatchMessages` | 10 | SNS `PublishBatch` |
| `MaxBatchBytes` | 262,144 | SNS `PublishBatch` |
| `MaxHeaderCount` | 9 | ten message attributes minus the body-encoding marker this driver reserves |
| `MaxHeaderBytes` | **unknown** | neither service states one separately from the message limit |
| `MaxDelay` | **unknown** | SNS has no per-message timer |
| `MaxLease` | 12 h | the subscribing queue's visibility timeout |

Headers become SNS message attributes, which raw message delivery maps onto SQS
message attributes unchanged. A value the character set cannot carry travels in
the `Binary` attribute type, which is what makes `FeatureBinaryHeaders` a native
claim; the attribute **name** still has to be a legal attribute name
(alphanumerics, hyphens, underscores, periods; no leading, trailing or doubled
period; not beginning with `AWS.` or `Amazon.`).

The reserved attributes are `drivers/sqs`'s, because the mapping is
: `iqmq-k` (key), `iqmq-id`, `iqmq-enc` (body encoding),
`iqmq-env` (envelope marker), `iqmq-ct`, `iqmq-ce`, `iqmq-cid`, `iqmq-cause`,
`iqmq-reply`, `iqmq-ts`. A caller's header colliding with one of them is refused
in **every** metadata mode.

the four modes, and what each does with metadata that will not fit:

| Mode | Behaviour |
|---|---|
| `MetadataNativeStrict` (default) | refuses, before any I/O |
| `MetadataAdaptive` | envelopes only the messages that need it |
| `MetadataPortableEnvelope` | always envelopes; every message on the queue has the same shape and a non-iqmq consumer reads base64 |
| `MetadataLenientDrop` | drops what will not fit, from the end of the header list, with a counter |

## Reconnect

**There is none, and there is nothing to reconnect.** Both services are
request/response over HTTPS: there is no link to lose, no session to
re-establish and no generation to invalidate. `Hooks.OnReconnect` is never
fired — firing it for a request that failed would report an event that did not
happen.

What a caller reads instead is `Client.Health()`, which is the **worse of the
two halves**. The publish half has no background work and nothing to fail in;
the consume half runs the pollers, so everything a health report can say about
this driver comes from the inner one. A poller's failed `ReceiveMessage` is
`HealthDegraded` with the failure, and it is not terminal: the next round
retries after 500 ms and a success clears it. A topic that has been deleted, a
queue that has been deleted, or a credential that has expired shows up there and
nowhere else.

`Client.Ping` reaches **both** services — SNS's `ListTopics` and the inner
driver's own `ListQueues` — because a client that can publish and not consume is
not a healthy fan-out client. Neither consumes a message nor creates topology.

## At-least-once window

Two windows, in series, and neither has an upper bound a caller can configure
away.

1. **SNS's.** The service retries a delivery it could not make, so a subscriber
   can see a duplicate of a publication SNS thought had failed.
2. **The queue's.** A copy whose visibility window passes before it is deleted
   is delivered again. `Config.AckDeadline` sets the window,
   `Config.LeaseGuard` is how long before it closes the driver stops trusting
   the delivery, and `Extend` moves it.

A publication whose `Publish` failed **after** the request left the socket is
reported as **ambiguous**: the service may have accepted it and lost the
reply, and on a fan-out an accepted message is already on its way to every
subscribed queue — so a driver that reported a definite failure would make N
duplicates look like a loss. A throttle on a publication is ambiguous for the
same reason; a throttle on an administrative call is not.

`FeatureExactlyOnceDelivery` is absent and there is nothing that makes a
delivery exactly-once. The answer applies unchanged: handlers are
idempotent, and `x/idempotency` is where the deduplication lives.

## Config & DSN

```go
client, err := snssqs.Open(ctx, snssqs.Config{
    Region:          "eu-west-1",
    AccessKeyID:     "AKIAEXAMPLE",
    SecretAccessKey: "…",
    AccountID:       "000000000000",
    AutoCreate:      false,   // three resources, opt-in
})
```

`Config.Validate` is pure — no I/O, no call to either service — and it ends by
validating the configuration the inner SQS driver will be built with, so every
refusal `drivers/sqs` wrote is this driver's refusal too, with no second copy to
keep in step.

The secret access key and the session token are `dsn.Secret`s, so neither
`Config.String()` nor a slog record nor an error built from one can carry them.
The access key ID is **not** a secret and is rendered, which is what
makes a redacted DSN still say which principal it was built for.

```
snssqs://AKIAEXAMPLE:secret@aws?region=eu-west-1&accountid=000000000000
snssqs://test:test@127.0.0.1:4566?region=localstack&insecuretransport=true&autocreate=true
```

The host `aws` means "the services' own regional endpoints" and takes no port;
any other host is an explicit endpoint for **both** services, which is how
LocalStack's edge endpoint is reached. It exists because `dsn.Parse` refuses an
authority with credentials and no host, so a production DSN cannot be written in
the hostless form (inherited). The path is
`Config.TopicPrefix`.

Parameters: `region`, `partition`, `accountid`, `queueprefix`, `autocreate`,
`noqueuepolicy`, `filterscope`, `pollers`, `maxmessages`, `waittime`,
`ackdeadline`, `leaseguard`, `settle`, `settlemaxitems`, `settleinterval`,
`body`, `maxmessagebytes`, `deadletterqueue`, `maxreceivecount`, `retention`,
`apitimeout`, `sessiontoken`, `insecuretransport`. An unknown parameter is a
construction error naming it.

**There is deliberately no `filterpolicy` parameter.** A policy is a JSON object
with quotes, braces and commas in it, and a DSN is a place people paste into a
shell: the percent-encoded form is unreadable and the raw form does not parse.
`Config.FilterPolicies` is a typed map keyed by subscription name, and
`TypedAdmin.SetFilterPolicy` is the runtime form.

### The filter policy, which is the conditional capability

`FeatureServerFilter` is **Conditional**, and the specification requires a conditional
support to state its condition — `driverkit`'s capability builder refuses one
with an empty note, so it cannot be forgotten. The condition:

- a policy in `Config.FilterPolicies` for that subscription, applied when this
  driver **creates** the subscription, or set afterwards with
  `TypedAdmin.SetFilterPolicy`. Without an entry there is no server-side filter
  at all and every message reaches its queue.
- `Config.FilterPolicyScope` decides what the policy is evaluated against.
  `attributes` (the service's default) filters on message attributes, which on
  this driver are the message's **headers**; `body` filters on the body parsed
  as JSON.

A policy is never written over an existing subscription's by opening a client:
changing which messages a live subscriber receives is an operator's decision,
not a restart's.

The claim's **positive** direction is unverified in the conformance report,
because the specification has no portable probe for a server-side filter — so the generated
matrix renders it as unsupported, which is the only honest answer for something
nobody checked. The driver's own
`TestCriticalFilterPolicyKeepsAMessageOutOfOneQueueOnly` proves it against the
in-process pair, in both directions: the filtered subscription receives the
matching publication and not the other, and the unfiltered one receives both.

## Known gotchas

**A publication to a topic with no subscriptions is accepted and delivered
nowhere.** Nothing fails, nothing warns, and a `Publish` that returns nil means
SNS stored the message — not that anybody will read it. Call
`Admin().EnsureSubscription` before you publish, or provision with Terraform.
`TypedAdmin.Subscriptions` lists how wide a fan-out really is.

**A subscription created after a publication never sees it.** There is no
history: SNS delivers to the subscriptions that exist at the moment of
publication. `FeatureReplay` is absent for this reason as well as for the
queue's.

**An attributes-scope filter policy cannot see an enveloped message's headers.**
Under `MetadataAdaptive` or `MetadataPortableEnvelope` the headers are inside the
body and the only attribute left is `iqmq-env`, so a policy written against
header names matches nothing and the subscription silently receives nothing. Use
`MetadataNativeStrict` on a filtered subscription, or filter on the body.

**Header order is not preserved.** Message attributes are a map, so the order a
publisher wrote its headers in is not on the wire. The decoder returns them in
NAME order so that two consumers of one message see the same order; a caller who
needs the published order uses the envelope (inherited).

**`DeleteTopic` leaves the queues, and `DeleteSubscription` leaves the queue.**
Both are deliberate: a queue may hold messages nobody has consumed, and a
portable operation that destroyed them would be the one thing that
loses data silently. `TypedAdmin.Queue` names the queue, and
`Backend.SQS()`'s admin is what deletes it.

**The queue access policy is written only for a queue that has none.** A queue
that already carries a `Policy` is a queue somebody configured, and a driver
that silently rewrote IAM would be a worse failure than a subscription that
needs one line of Terraform. The cost is that SNS may not be allowed to deliver,
which shows up as a subscription that receives nothing; `Config.NoQueuePolicy`
turns the whole step off. Neither emulator enforces the
policy, so **no test in this repository proves this step works.**

**The publisher's message identifier and the consumer's are different numbers.**
SNS's `MessageId` identifies the publication; each queue mints its own for its
copy. Correlate on `Message.ID`.

**the fidelity figure is lowered to 400 messages** (from the 100,000, and
from the SQS driver's 2,000). Every message is an SNS `Publish`, a fan-out and a
receive against an emulator that is slower per call than ElasticMQ; the figure
keeps one scenario inside the suite's per-scenario timeout. The specification asks a driver
that lowers it to say so, and this is that.

**One emulator, not two.** ElasticMQ has no SNS, so the cross-check that found
two of the defects — a claim one emulator proves and the other does not — is
not available here. What covers the consume half is `drivers/sqs`'s own report
against both.

## Maturity & tier

See the pinned client version and the module tests when evaluating this driver.

## Driver-specific critical tests

Four, one per state the SERVICES decide and that no amount of correct code can
produce on demand against a real endpoint. They run against
`internal/snsfake` — an in-process SNS that fans out into `drivers/sqs/sqsfake`
by calling its `SendMessage`, which is what raw message delivery *is* — and each
was watched failing under a deliberate mutation before it was made to pass.

| Test | What it holds |
|---|---|
| `TestCriticalNonRawSubscriptionIsRefused` | a subscription without raw delivery is refused at open, in **both** wrong states (the attribute absent, which is the service's default, and present-and-false) — and the same path with raw delivery on opens, delivers and round-trips, so the refusal is not "refuse everything" |
| `TestCriticalOneSubscriberSettlementLeavesTheOtherCopyAlone` | both copies are taken before either is settled; acking one leaves the other **settleable** (nil, not `ErrAlreadySettled` or `ErrLeaseLost`), the two deliveries name different sources, and a `Reject` by one leaves the other in flight |
| `TestCriticalPublishBatchPartialResultsAlignWithTheCallersSlice` | ten entries, three failed by position: every index carries its own answer, the seven that succeeded are in **every** subscribed queue and the three that failed are in none |
| `TestCriticalFilterPolicyKeepsAMessageOutOfOneQueueOnly` | the filtered subscription receives the matching publication and not the other, **and** the unfiltered one receives both — a policy that excluded everything would pass a weaker test |

Beside them: `TestADelayedPublicationIsRefusedAtTheSink` (checked at the SPI,
because the core already refuses a delay for a driver that does not advertise
the feature — a test through the client would pass with the driver's own
refusal deleted), `TestEnableRawDeliveryRepairsASubscriptionTheDriverRefuses`
(refused, repaired, consumed), `TestHealthReportsTheWorseOfTheTwoHalves`,
`TestAdminDeleteSubscriptionLeavesTheQueue`, and
`TestTheCompositionKeepsEveryOptionalSettlementSurface` — the one thing the
composition could lose silently, since a fan-out source that wrapped the inner
settler in a facade of its own would forward `mq.Settler` and retire the specification's
batched settlement and lease extension with nothing in the portable API to say
so.

Two fuzz targets. `FuzzSNSAttributes` is the publication mapping, the two
attribute translations and the round trip raw delivery closes, with the name
and body grammars **restated** in the target rather than shared with the
encoder that produced them. `FuzzFakeTopic` holds the in-process SNS server
itself to fan-out conservation, the raw-versus-non-raw difference,
subscription identity and isolation — because four critical tests and the other
fuzz target rest on that server, so a server that lies is a test suite that
lies.
