# sqs

<!-- Fixed sections per the driver documentation. Every section must be present. -->

**Maturity:** Experimental · **Tier:** 1 · **Owner:** iqmq maintainers

## Client & version policy

The native baselines for this driver live in `benchmarks/native-baselines/` and measure the clients below against the harness's `elasticmq` image (`make bench-native CLIENTS="sqs"`). A driver number is only comparable with a baseline taken from the same client version, broker version and confirmation boundary, so the pins move together; each baseline's package comment records the settlement boundary it measures as well.

| Baseline | Client | Pinned version | Confirmation boundary |
|---|---|---|---|
| `sqs` | github.com/aws/aws-sdk-go-v2/service/sqs | `github.com/aws/aws-sdk-go-v2/service/sqs@v1.52.7` | SendMessage/SendMessageBatch response (ElasticMQ, not evidence for managed SQS) |

### Verified facts (the API contract)

Checked while building the native baselines; re-verify when a pin moves.

- **message and batch size.** The maximum SQS message size is 1 MiB (1,048,576 bytes) after the 2025 increase from 256 KiB, SendMessageBatch takes at most 10 entries, and the same 1 MiB limit applies to the sum of a batch's payloads.
  _Verified against: aws-sdk-go-v2 service/sqs v1.51.0 api_op_SendMessageBatch.go:13-25; AWS SQS quotas documentation._
- **body character set.** An SQS body may contain only XML, JSON or unformatted text (#x9, #xA, #xD, #x20-#xD7FF, #xE000-#xFFFD, #x10000-#x10FFFF), so binary payloads must be encoded — the baseline base64-encodes them and pays the 4/3 inflation against the size limits.
  _Verified against: aws-sdk-go-v2 service/sqs v1.51.0 api_op_SendMessageBatch.go:27-34._
- **timers and visibility.** A message timer (per-message delay) is at most 15 minutes and is not supported on FIFO queues, which only take a queue-level delay; the visibility timeout defaults to 30 seconds and cannot exceed 12 hours.
  _Verified against: AWS SQS quotas documentation (message timer, visibility timeout); AWS FIFO-queue documentation._
- **emulator evidence.** ElasticMQ and LocalStack are functional stand-ins, not performance evidence: baseline numbers taken against them describe the client and the container, never the managed service.
  _Verified against: conformance/harness/brokers.go ElasticMQ, LocalStack; the performance budgets layer 4._

## Topic / subscription mapping

A topic maps to a queue **name** by a pure function, and the name maps to a
**URL** by one `GetQueueUrl` whose answer is cached for the life of the
backend. The indirection is the service's: every SQS operation takes a queue
URL, and a URL carries the region and the account, which is not something an
application knows.

| Portable | SQS |
|---|---|
| topic `orders/eu` | queue `<prefix>orders-eu` (`/` → `-`), plus `.fifo` on the FIFO profile |
| topic that is already a URL (`https://…`) | used verbatim |
| topic named in `Config.QueueURLs` | that URL, with no `GetQueueUrl` at all |
| `SourceConfig.Subscription` | **ignored** |
| `DeliveryInfo.Source` | the queue URL |

Three things about it are load-bearing.

**The mapping is not injective.** `orders/eu` and `orders-eu` are one queue,
because a queue name takes alphanumerics, hyphens and underscores and nothing
else. It is stated rather than hidden, and `Config.Resolver` and
`Config.QueueURLs` are the two ways out.

**The subscription is ignored, and that is the profile's shape rather than an
omission.** A queue has exactly one set of messages, so every consumer of one
topic competes for them whatever `Subscription` says: two consumers of
`"workers"` compete, and so do one consumer of `"workers"` and one of
`"audit"`. `FeatureConsumerGroups` is claimed with that as its note and
`FeatureFanOut` is not claimed at all. Two independent subscriptions need
`drivers/snssqs`, where the topic is an SNS topic and the subscription is a
queue.

**A topic carrying a pattern metacharacter (`*`, `?`, `#`, `>`) is refused
with `ErrUnsupported`,** before the portable grammar is consulted. The service
has no pattern subscription, and the refusal has to be `ErrUnsupported`
because that is what the negative probe for `FeatureWildcardSubscribe`
reads; letting `mq.ValidateTopic` answer first (with `CodeInvalidArgument`)
left the claim unproved in both directions.

`Config.AutoCreate` is off by default: a driver that provisioned a
queue for a topic it could not resolve would turn a typo into a queue with a
monthly bill. `Backend.QueueName` answers what a topic becomes without any
I/O, which is what an operator creating queues by hand needs.

## Publish path & confirmation boundary

| Mode | Boundary |
|---|---|
| `ConfirmNone` | the `SendMessage` response. There is nothing weaker: the SDK has no fire-and-forget send, and the receipt reports what the publication actually reached |
| `ConfirmAccepted` | the `SendMessage` / `SendMessageBatch` response |
| `ConfirmDurable` | **the same response.** The service has stored the message redundantly across availability zones before it answers, so a durable boundary costs no extra round trip and is not refused |

`PublishBatch` is `SendMessageBatch`, always, at **ten entries** and at most
one mebibyte of payload per call. A caller's batch of any size becomes a
sequence of calls, and the whole difficulty is what comes back:

- **each entry's success is its own durable confirmation.** The service
  answers with a `Successful` list and a `Failed` list; an entry in `Failed`
  was not stored.
- the result is a `*BatchError` whose `Errs` are **index-aligned with the
  messages handed in** — not with the chunk, not with the failure list, not
  with the messages that survived the driver's own encoding. The entry
  identifier sent to the service is the message's index in the caller's
  slice, which is what makes the correlation exact.
- a message the encoder refuses — an unsupported option, metadata that does
  not fit under the strict mode — never reaches the service and its error is
  at **its** index.
- on a FIFO queue, once an entry of a message group fails, the driver sends
  nothing more of that group in later calls of the same batch. Those entries
  get a retryable not-attempted error, so you can retry the group in order.
  A message the encoder refuses stops its group the same way; earlier
  versions sent the rest of the group, so the refused message, once fixed and
  sent again, landed behind them.
  Other groups, and standard queues, carry on as before. The core splits a
  `PublishBatch` of more than 10 into pieces of 10, because 10 is the
  advertised `MaxBatchMessages`, and holds a failed key back across its
  pieces in the same way.

The driver implements `Sink` and `BatchSink` and neither `AsyncSink` nor
`OwnedSink`: both would be a goroutine pool in front of the same synchronous
HTTP request, and the core supplies those loops for a driver
that does not offer its own.

A transport failure on the way back from a send is **ambiguous**: the message
may be on the queue. So is a throttle and an internal error, both of which the
service answers after it has read the request. The specification makes that distinction the
driver's, and reporting it as a definite failure would make a duplicate look
like a loss.

A throttled KMS key on an encrypted queue (`KmsThrottled`) is the exception.
The service needs the key before it can store the message, so it is a
retryable throttle and **definite** on every call, a send included, like the
rest of the KMS errors. Earlier versions matched no rule and reported it as a
non-retryable `CodeUnknown`.

## Consume path

`Config.Pollers` goroutines, each one `ReceiveMessage(MaxNumberOfMessages=10,
WaitTimeSeconds=20, VisibilityTimeout=AckDeadline)`, feeding one bounded ring
(`driverkit.PushAdapter`) that `Fetch` drains.

`WaitTimeSeconds` is `Config.WaitTime` in whole seconds, rounded **up**. A
`WaitTime` under a second used to truncate to zero, which the service reads as
a short poll: an empty queue answered at once and each poller asked again as
fast as the round trip allowed.

The bound is the backpressure. A poller blocked pushing into a full ring stops
asking the service for messages, which is the only flow control a long poll
has; a push adapter that buffered without bound would turn a slow consumer
into unbounded memory, which the specification forbids.

`Close` makes visible again what the ring holds and what a parked poller was
still admitting, in one `ChangeMessageVisibilityBatch` per 10. It leaves
alone a message whose window has closed, or is within `Config.LeaseGuard` of
closing: that message is visible already and may be another consumer's, and
on the reading of the service the driver plans for, a release naming its
stale receipt handle makes that consumer's copy visible. Earlier versions
released those handles as well.

## Ack / nack / extend mapping

| Portable | SQS |
|---|---|
| `Ack` | `DeleteMessageBatch` (ten entries), coalesced by `Config.Settle` — family ** ** |
| `Nack` | `ChangeMessageVisibilityBatch` with `0` |
| `Nack(NackDelay(d))` | `ChangeMessageVisibilityBatch` with `d` |
| `Nack(NackDrop)` | `DeleteMessageBatch` — the one action that destroys the message |
| `Reject` / `NackDeadLetter` | `SendMessage` to `Config.DeadLetterQueue`, then `DeleteMessage` — in that order |
| `Extend(by)` | `ChangeMessageVisibility(remaining + by)` — the service measures its timeout from the call and the specification measures from the deadline, so the remaining window is added |

**`Config.Settle` is the flush policy** and decides when a coalesced
batch leaves. All four kinds ship and none is selected silently:

| Kind | When it sends | Cost | What it holds |
|---|---|---|---|
| `immediate` (**the default**) | every settlement | one request each | nothing |
| `count` | at `SettleMaxItems` | one per ten | up to nine, **for ever** if nothing more arrives |
| `time` | every `SettleInterval` | falls with the rate | whatever arrives in an interval — **unbounded** |
| `hybrid` | whichever comes first | one per ten at rate | up to nine, until the next tick |

- `count`'s worst case holds a caller inside `Ack` until something unrelated
  flushes the queue, which the specification forbids;
- `time` bounds neither the queue nor the cost at low rates;
- `hybrid` needs settlements that arrive TOGETHER, and cannot require them. A
  caller that settles in a loop — which is what a single-threaded consumer
  does, and what the specification's own scenarios do — never fills a batch, so every
  settlement waits a whole `SettleInterval`. Bound as the default it made
  the specification's `publish/fidelity` fail at 592 of 2,000 messages with
  `ErrLeaseLost` on both profiles, because the deliveries already fetched sat
  unsettled while their visibility windows ran out.

**`Config.Settle = flush.Hybrid` batches settlements when handlers settle
concurrently.** Measure request counts and settlement latency with your
workload before choosing it; no retained scorecard establishes a speedup.

An `Ack` returns at the configured boundary: under `immediate` when its own
request does, and under the others when the batch's does. Under `time` and
`hybrid` a settlement waits for the next tick, which comes `SettleInterval`
(50 ms unless configured) after the previous tick's requests have finished,
so the wait can run past `SettleInterval` by those requests. Nothing is lost
by the coalescing (a settlement that has not been sent is one whose `Ack` has
not returned), and `Close` sends whatever is queued. `immediate` guarantees
that nothing is *held* and that no settlement waits behind another's
request, not that every settlement is its own request: two acknowledgements,
or two requeue or drop nacks with the same options, that arrive together can
travel together in one batch request. Dead-letter nacks are the exception:
the driver sends each one as `SendMessage` then `DeleteMessage`, one message
at a time, so two that travel together are two pairs of requests and the
second waits for the first.

**A partial `DeleteMessageBatch` is the case this mapping is written for.**
Some entries fail and some succeed; the ones that failed stay in the queue, get
their own error at their own index, and stay **settleable**, so a caller may
retry. An entry the service refuses with `ReceiptHandleIsInvalid` or
`MessageNotInflight` is `ErrLeaseLost` instead: the handle will never become
valid again.

A refused delete leaves a delivery settleable only while the driver still
vouches for it. When the message is received again while the delete is in
flight, the earlier delivery is `ErrLeaseLost` from then on, so a retry cannot
send its stale receipt handle and delete the copy the new receipt holds.
Earlier versions made it settleable again.

A settlement whose context is done before its request is made sends nothing,
because the SDK refuses such a request, so its failure is **definite** and the
delivery stays settleable. The same holds for an `Extend`, for a `Reject`
whose dead-letter queue URL could not be looked up, and for a `Reject` whose
context ends once the copy is sent and before the delete (the message is then
on both queues, a duplicate and not a loss). The consumer's settlement context
ends this way when a shutdown interrupts it. Earlier versions reported each of
these as ambiguous, so the delivery was stuck unknown and the message came
back after its window to be processed again.

A second settlement of a delivery whose settlement has finished keeps that
settlement's answer after the window closes: `ErrAlreadySettled` once settled,
`ErrSettlementUnknown` once ambiguous. A delete that did not land brings the
message back only once its window has closed, so this is the usual order.
Earlier versions answered `ErrLeaseLost` there and recorded the delivery as
lost, over the sticky unknown answer.

## Ordering

**Standard profile: no ordering is claimed, at any scope.** A standard queue
does not guarantee order, and this driver claims none —
`FeatureOrderedByKey`, `FeatureOrderedByPartition` and
`FeatureOrderedGlobally` are all unsupported. A standard queue with one
consumer at low volume very often *does* deliver in order, and relying on that
is how a program breaks the first time it scales out.

**FIFO profile: `FeatureOrderedByKey`, and only that.** `Key` becomes
`MessageGroupId`; a FIFO queue delivers one group in order and holds a group's
later messages while an earlier one is in flight. Nothing is ordered **across**
groups, so `FeatureOrderedGlobally` is not claimed on the FIFO profile either.

The key-to-group rendering is worth knowing. A key that is short printable
ASCII is used verbatim, so an ordinary key is readable in the console and
interoperable with a non-iqmq producer; anything else is `h-` followed by the
hex of its SHA-256. A collision can only **merge** groups, which adds an
ordering constraint and never removes one: the specification's "one key's messages are
delivered in order" still holds, and what a collision costs is FIFO
throughput, which is per group.

A keyless FIFO publication goes in one shared group, `iqmq-default`. The two
alternatives are worse: refusing would make an ordinary keyless publish fail on
the FIFO profile alone, and a unique group per message would silently turn the
profile a caller chose for its ordering into an unordered one. The cost is
throughput, because one group is serial.

## Delays

| | Standard | FIFO |
|---|---|---|
| `Delay(d)` / `DeliverAt(t)` | `DelaySeconds`, at most **15 minutes** | **refused** with `ErrUnsupported` |
| `Nack(NackDelay(d))` | `ChangeMessageVisibility(d)`, at most **12 hours** | same |

A delay is rounded **up** to whole seconds. Rounding down would turn a
500 ms delay into no delay at all, which is the guarantee the caller asked for
being quietly withdrawn.

A FIFO queue takes no per-message timer — only the queue-level `DelaySeconds`,
which is not a publication control — so `FeatureScheduledPublish` is claimed on
the standard profile and not on the FIFO one. The *redelivery* delay is
available on both, because it is a visibility change rather than a timer.

The redelivery delay is **native and durable**: the queue holds it, so it
survives this process dying. That is what makes `FeatureDelayedRedelivery` a
native claim here where it is an emulation on a log-shaped broker.

## Dead-letter queue

There are **two** routes, and a caller has to plan for both.

**The queue's own redrive policy** is the native one: the service moves a
message it has delivered `Config.MaxReceiveCount` times to
`Config.DeadLetterQueue`, with no settlement from the consumer at all. It is
what makes a message *nobody ever settles* — a handler that crashes the
process every time — safe. `TypedAdmin.SetRedrivePolicy` configures it and
`TypedAdmin.Attributes` reads it back.

**An explicit `Reject`** is the driver's: it sends the message to
`Config.DeadLetterQueue` with its provenance (`iqmq-dlq-origin`, and
`iqmq-dlq-reason` when the settlement named one) and deletes the original
**only once that send is confirmed**. The specification requires that order outright — "a
failed or ambiguous DLQ publish never acks the original" — and a driver that
deleted first would acknowledge a message it had written nowhere. When the
original's window closes while the send is answered, the original is not
deleted, because that delete could take the copy another consumer now holds:
`Reject` reports `ErrLeaseLost`, and the message is on the dead-letter queue
and comes back on its own queue as well, which is a duplicate and not a loss.

The send is a publication, so its failure is classified the way a failed
`Publish` is. A transport failure after the request left, a throttle, and a
server error (`InternalError`, `InternalFailure`, `ServiceUnavailable` or
`RequestTimeout`) can each follow a send the service stored, and the SDK's
default retryer repeats the send on all of them, so an earlier attempt may have
been stored too. In each case `Reject` reports an **ambiguous** error, and a
second settlement of the delivery is refused with `ErrSettlementUnknown`
instead of sending a second copy. The original stays on the queue and comes
back when its window passes. A refusal the service answered, such as a
credential that may not send to the dead-letter queue, is definite, and the
delivery stays settleable. Earlier versions called the throttle and the server
errors definite, so a retried `Reject` could send a second copy while the
first was reported as never sent. On a FIFO queue the `dl-<id>` deduplication
id hid that copy only inside the service's five-minute window.

The service holds a String attribute to the body's character set, so every
character of the reason outside it (a control character, invalid UTF-8,
`U+FFFE` or `U+FFFF`) is sent as `U+FFFD`. Sent as it was, such a reason had
the service refuse the whole send, and the message could not be dead-lettered
at all.

The provenance never displaces what the message was published with. When the
copy would carry more than 10 attributes, or be larger than
`Config.MaxMessageBytes` (the service counts every attribute's name, data type
and value as well as the body), `iqmq-dlq-reason` is left off, and then
`iqmq-dlq-origin`. Earlier versions held only the count, so a message published
at the size limit had its copy refused, and no `Reject` of it, nor the move of
an undecodable one that size, could succeed.

A `Reject` on a client with **no** `DeadLetterQueue` configured is refused with
`ErrUnsupported` and destroys nothing; the delivery stays settleable, so the
caller may requeue it instead. Only an explicit destination authorises a route
off the queue.

**A message the driver cannot decode is moved to `Config.DeadLetterQueue` as
soon as it arrives**, by the same send-then-delete a `Reject` makes. The copy is
the raw body with the message attributes it was published with, and its
`iqmq-dlq-reason` starts `undecodable:`. It never reaches a handler, and no
later receive could decode it either. With no dead-letter queue (or one that is
the consumer's own queue), or when that send fails, the driver leaves the
message invisible for the rest of the window
its receive opened, so it comes back once per `AckDeadline` and a redrive
policy set on the queue outside the driver still counts each receive. On a
FIFO queue it holds its message group until something moves it. Earlier
versions made it visible again at once, so the next poll received it again:
about 40,000 `ReceiveMessage` calls a second against the in-process fake, each
one a billed request on AWS, for as long as the message stayed on the queue.
Set a dead-letter queue if your producers can write messages this consumer
cannot read. `Health` is no signal for them: the poll that receives one reports
`Degraded` and its own success clears it straight away.

`TypedAdmin.Redrive` starts a `StartMessageMoveTask`, which drains a
dead-letter queue back to its source. It is asynchronous at the service: the
call returns a task handle and the messages move afterwards, so a caller that
wants to know the move finished watches the destination.

## Headers & limits

A message carries **at most ten message attributes**, and this driver spends
some of them. Each reserved name is present only when the field it carries is
set:

| Attribute | Carries |
|---|---|
| `iqmq-k` | `Message.Key` (Binary) |
| `iqmq-id` | `Message.ID` |
| `iqmq-enc` | the body encoding: `b64`, `nil` or `empty` |
| `iqmq-env` | set when the body is an iqmq envelope |
| `iqmq-ct`, `iqmq-ce`, `iqmq-cid`, `iqmq-cause`, `iqmq-reply`, `iqmq-ts` | the typed properties |

the question for this driver is what happens to the **eleventh** piece of
metadata, and `MetadataMode` is the answer:

| Mode | A message that does not fit |
|---|---|
| `MetadataNativeStrict` (default) | **refused** before any I/O, with an error naming why |
| `MetadataAdaptive` | the whole message is encoded into the body as an iqmq envelope |
| `MetadataPortableEnvelope` | every message is enveloped, so a non-iqmq consumer reads base64 |
| `MetadataLenientDrop` | headers are dropped from the end of the list, with a metric |

Four things make a message not fit: more than ten attributes, a header key that
is not a legal attribute name, a repeated header key (an attribute set is a
map), and — in **every** mode, refused rather than enveloped — a header key
colliding with one of this driver's reserved names.

**Binary header values are native.** The service's `Binary` attribute type
carries an arbitrary byte value, so `FeatureBinaryHeaders` is a native claim.
The attribute *name* still has to be a legal attribute name. A value the
service's character set cannot hold goes into a `Binary` attribute whatever its
type — including an identifier or a property, which is why the decoder reads
every string field through the value rather than through `StringValue`.

**An empty header value is carried, not refused.** The service refuses an
attribute with an empty value of either type, so the driver sends an empty
value as a `String.iqmq-empty` attribute (a `String` with a custom type label)
holding a placeholder, and the decoder hands back an empty value for it. A
consumer that is not this driver sees the placeholder `-`.

**Header ORDER is not preserved natively.** An attribute set is a map, so the
order a publisher wrote its headers in is not on the wire at all; the driver
decodes in attribute-name order, so two consumers of one message see the same
order and a test is reproducible. A caller that needs the published order
preserved uses `MetadataPortableEnvelope` or `MetadataAdaptive`.

**The body is TEXT.** The service accepts only the XML character set, so an
arbitrary byte string has no native representation, and `Config.Body` is the
policy with all three answers shipped: `auto` (the default) passes text through
and base64-encodes what it must; `text` refuses a non-text body before any
I/O; `base64` encodes everything and costs a third of the size limit. A nil
body and an empty body are both carried and are distinguished, because the
service refuses an empty `MessageBody` outright.

The advertised limits, every one of them read out of the pinned SDK (see
"Verified facts" above):

| Limit | Value |
|---|---|
| `MaxMessageBytes` | 1 MiB, or `Config.MaxMessageBytes` when smaller |
| `MaxBatchMessages` | 10 |
| `MaxBatchBytes` | 1 MiB |
| `MaxHeaderCount` | **9** — ten attributes less the encoding marker this driver always reserves |
| `MaxDelay` | 15 minutes (standard profile only) |
| `MaxLease` | 12 hours |
| `MaxHeaderBytes` | **unknown**, deliberately: the service bounds the whole message and states no separate header ceiling, and the specification forbids reading an unadvertised limit as zero |

## Reconnect

**There is none, and that is the honest answer rather than an omission.** SQS
is request/response over HTTPS: there is no connection to lose, no dial loop,
and no generation to bump. `driverkit.Reconnector` is not used, and
`Hooks.OnReconnect` is never fired — firing it for a request that failed would
report an event that did not happen.

A per-request failure is the SDK's to retry, and it does: the default
`aws.Retryer` handles a throttle and a 5xx. What this driver adds is the
classification — which of those failures is ambiguous.

## At-least-once window

**From the receive to the delete.** A message is invisible for the visibility
timeout and becomes visible again if nobody deletes it, so a handler that takes
longer than the window has its message delivered to somebody else.

The sharp edge is what happens next, and it is the reason this driver has a
**local lease check**. The service will accept a `DeleteMessage` for a receipt
handle whose message another consumer now holds: the delete does not fail, and
the message is gone. There is no API that asks "do I still hold this?", so the
only place the refusal can happen is in the driver, before the call. Every
settlement compares the delivery's recorded deadline against the injected
clock and answers `ErrLeaseLost` when the window has passed or is within
`Config.LeaseGuard` of passing.

The comparison is made again immediately before each request leaves, not only
when the settlement starts. A batched settlement sends one request per ten
deliveries, and a dead-letter route per delivery, one after another, and a
settlement waits for an `Extend` of the same delivery that is still on its way,
so a delivery can see its window close behind them. Such a delivery reports
`ErrLeaseLost` and nothing is sent for it. Earlier versions sent its delete
after the window had closed and reported a success.

It is a **local** view and the driver says so. It cannot see a window another
process shortened, and it deliberately refuses on a window this process
believes has passed even when the service would still have accepted the
delete. Both directions of that trade are the safe one: the cost is a
redelivery, and the alternative is deleting somebody else's work.

`Extend` is checked against the window itself, not the window less
`LeaseGuard`. The guard is a margin for the delete; an extension is the call
that keeps the window open, and refusing it inside the guard lost deliveries
whose window had not closed — every one of them, under a lease scheduler whose
lead was shorter than the guard. The deadline an extension records is measured
from before the call, so the settlement after it is still checked, guard and
all, against a window the service granted.

A second case the clock cannot catch: the same message received **twice** by
one source, because its window passed or because somebody made it visible
again early. Both handles are valid at the service. The source marks the
earlier delivery superseded the moment the later one arrives, so a settlement
for the earlier handle reports `ErrLeaseLost` rather than deleting the message
the later delivery owns. Earlier means the earlier receive, which the window
tells apart, and not the earlier admission: with more than one poller, a
poller parked on a full ring can admit its copy after another poller has
received the message again. Earlier versions let the copy admitted last win,
so that stale copy marked the fresh delivery lost: the message sat invisible
under the fresh receipt with nobody working on it, or the fresh delivery's
`Ack` was refused and the message was processed twice.

Across processes the lease check is the only defence, and a window another
consumer shortened is invisible to it. That is a property of the service and
not of the driver: a visibility timeout is advisory, and a deployment that
cannot tolerate a double delete uses the FIFO profile, whose group holds the
second delivery back until the first is settled.

`Extend` moves the local view with the service's, or the next settlement would
be refused against a deadline that is no longer the window. It also decides
WHAT to ask the service for. `Delivery.Extend` moves the deadline "at least
`by` further into the future" and `ChangeMessageVisibility` measures its
timeout from the call, so the driver sends the *remaining* window plus `by`:
sending `by` alone — which it did until the specification — set the window to
`by` from now and shortened the lease of any delivery with more than `by` left.
An extension whose sum passes the service's twelve hours is refused with
`CodeInvalidArgument` rather than clamped to the ceiling and reported as a
success, because a clamp moves the deadline by less than was promised.

An extension and a settlement of the same delivery reach the service one after
the other. Both set the visibility window from the moment the service reads
them, so the one that lands last decides when the message is visible again,
and the runner's lease scheduler can be extending a delivery at the moment its
handler nacks it. An `Extend` whose settlement is already in flight is refused
with `ErrSettlementInProgress`, and a settlement that starts while an `Extend`
is on its way waits for the service's answer and goes after it. Earlier
versions sent the two side by side, so a requeue `Nack` with a delay could have
its delay replaced by the rest of the window plus the extension. The wait ends
when the settlement's context does, with a definite failure that leaves the
delivery settleable; earlier versions waited for the extension's whole round
trip, up to `Config.APITimeout`, and every delivery after it in the same batch
waited too.

Nothing in the portable API reports what an extension bought: `DeliveryInfo` is
the snapshot the receive handed over, so `LeaseExpires` still names the original
deadline and the SPI's `Extend` returns only an error. A caller has to assume
the documented minimum, which on this driver is now true.

## Config & DSN

    sqs://AKIAEXAMPLE:secret@aws?region=eu-west-1&settle=hybrid
    sqs://x:x@127.0.0.1:9324/dev-?region=elasticmq&insecuretransport=true

The authority's user is the access key ID and its password the secret access
key, which makes this the first driver whose DSN carries a real credential —
and therefore the first whose `secrets/redaction` conformance scenario runs
rather than skips. `Config.String`, `Config.DSN` and `Config.LogValue` all
redact the secret and the session token and all keep the **access key ID**,
which is not a secret: a redacted DSN that did not say which principal it was
built for would be useless in a log.

The host is the endpoint. `aws` is the sentinel that means "the SDK's own
regional endpoint for `region`", and it exists because the shared parser
refuses an authority carrying credentials and no host — SQS is the first driver
whose endpoint is a *parameter* rather than part of the authority, so nothing
had hit it. An `http://` endpoint needs
`insecuretransport=true`, which is how a plaintext endpoint stays deliberate.

| Parameter | Setting |
|---|---|
| `region`, `accountid` | the region signing needs, and the account |
| `fifo`, `contentbaseddedup` | the profile, and its queue attribute |
| `autocreate` | let the driver create a queue (opt-in) |
| `pollers`, `maxmessages`, `waittime` | the consume shape; ten and twenty seconds are the service's maxima |
| `ackdeadline`, `leaseguard` | the visibility timeout, and how early a settlement is refused |
| `settle`, `settlemaxitems`, `settleinterval` | the flush policy |
| `body` | `auto`, `text` or `base64` — the binary-body policy |
| `maxmessagebytes` | override the advertised message limit for a smaller queue |
| `deadletterqueue`, `maxreceivecount` | the redrive policy |
| `retention`, `apitimeout` | a created queue's retention, and the bound on one call |
| `sessiontoken`, `insecuretransport` | a temporary credential, and plaintext |

`Config.Credentials` takes an `aws.CredentialsProvider` the caller built, which
is how a deployment reaches the SDK's default chain — environment, instance
role, SSO, web identity — without this module depending on the SDK's `config`,
`sts` and `sso` modules. `OpenWithClient` is the same escape hatch one level
up: a caller who needs a custom HTTP client, a middleware stack or that chain
builds the `*sqs.Client` and hands it over with explicit ownership.

There is **no silent fall-through to the environment**: a configuration with no
credentials and no provider signs anonymously, which an emulator accepts and a
real endpoint refuses at once. A driver that picked up a stray
`AWS_SECRET_ACCESS_KEY` would be talking to an account nobody named.

## Known gotchas

- **A stale receipt handle deletes the message anyway.** This is the one to
  read twice. The service does not refuse a delete for a superseded handle, so
  nothing but the driver's local lease check and its supersession bookkeeping
  stands between a slow handler and deleting another consumer's work. Across
  *processes* only the lease check applies, and it cannot see a window somebody
  else shortened.
- **`count` can hold a settlement for ever.** Nine settlements followed by
  silence are held until something else flushes the queue, and `Ack` does not
  return until the delete has happened. It is the cheapest arm and the reason
  `rules["A32"]` refuses it as a default.
- **`time` bounds nothing.** Between two ticks it queues whatever arrives:
  `flush.Policy.MaxHeld` reports −1 for it and for nothing else.
- **Header order is lost in native mode** (see "Headers & limits").
- **Two subscription names are one queue.** `SourceConfig.Subscription` is
  ignored, so two "independent" subscriptions compete. `drivers/snssqs` is the
  fan-out.
- **A queue has no history and no position.** `StartLatest` and
  `StartEarliest` are the same position, a consumer opened after a publication
  receives it, and a time or a sequence is refused with `ErrUnsupported`.
- **FIFO needs a deduplication identifier.** A publication with no `DedupKey`
  is given a unique one, so two identical bodies are two messages. Turning
  `contentbaseddedup` on makes the service discard the second publication of an
  identical body for five minutes, which is a message loss a caller has to ask
  for.
- **The FIFO dead-letter queue must also be a `.fifo` queue**, which
  `Config.Validate` enforces before any request.
- **A sub-second visibility timeout is rounded up to one second.** The service
  takes whole seconds and zero means "no lease at all".
- **`Purge` is once per minute.** The service answers
  `PurgeQueueInProgress` for a second one inside sixty seconds, which maps to
  `CodeConflict` rather than to a retryable failure: a caller that retried
  immediately would be refused for the rest of the minute.
- **`DeleteSubscription` is refused.** A subscription *is* the queue, so
  deleting one would delete the topic; answering "done" would be worse than
  refusing, and `DeleteTopic` is the operation that exists.
- **Emulator numbers are not evidence.** ElasticMQ and LocalStack describe the
  client and the container (layer 4).
- **Test each emulator separately.** Queue visibility attributes and
  per-receive overrides can behave differently. Run `TestConformance_sqs` and
  `TestConformance_sqs_localstack` against the intended environments before
  assuming their behaviour is equivalent.

## Maturity & tier

See the pinned client version and the module tests when evaluating this driver.

## Driver-specific critical tests

| Test | What it holds |
|---|---|
| `TestCriticalVisibilityExpiry` | the window passes: the message comes back, and the stale holder's delete is refused with `ErrLeaseLost` **before any API call** — because the call would succeed |
| `TestCriticalPartialDeleteBatch` | three entries of ten fail: the seven that succeeded are gone and report nil, the three that failed are still in the queue, report their own error at their own index, and are still settleable |
| `TestCriticalStaleReceiptHandle` | one message, two valid handles, the clock standing still: only the supersession bookkeeping can refuse the older one, and deleting the bookkeeping fails this test while deleting the lease check does not |
| `TestCriticalFIFOGrouping` | one group is ordered and a group's later message is held while an earlier one is in flight; different groups are not ordered together, which is what makes *not* claiming `FeatureOrderedGlobally` honest |

Beside them: `TestFailedDeadLetterSendNeverAcksTheOriginal` (the order),
`TestADeadLetterSendIsClassifiedAsAPublication` (a failed dead-letter send is
ambiguous exactly when a failed `Publish` is),
`TestAnUndecodableMessageWaitsOutItsWindow` and
`TestAnUndecodableMessageIsDeadLettered` (a message the driver cannot decode
costs one receive per window, or moves to the dead-letter queue raw),
`TestAmbiguousSettlementIsSticky` (the sticky unknown),
`TestExtendMovesTheWindow` (the local view moves with the service's), and three
fuzz targets — `FuzzSQSAttributes`, `FuzzSQSPartialResults` and
`FuzzSQSReceiptHandling`. The first found two defects in its first minute: an
envelope that encoded what it could not decode, and a reserved-name collision
accepted under `MetadataAdaptive`.
