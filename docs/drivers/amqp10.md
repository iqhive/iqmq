# amqp10 (shared AMQP 1.0 core)

<!-- Fixed sections per the driver documentation. Every section must be present. -->

**Maturity:** Experimental · **Tier:** 2 · **Module:** `github.com/iqhive/iqmq/drivers/amqp10`

The split is the point. AMQP 1.0 is a wire protocol with a settlement model,
and almost everything in this page is the protocol's rather than a broker's:
links, sessions, credit, dispositions, the message model, the error conditions
and the reconnection. What is *not* the protocol's — what an address means, how
a durable subscription is named, where a delay goes, which field counts
attempts, what is dead-lettered and how, and which broker versions work at all
— is a [`Profile`](#config-dsn), which is a struct of function fields a broker
module fills in. The core refuses to guess at any of it: every optional field's
zero value means "this broker cannot express that", and the answer a caller
gets is `ErrUnsupported` before any I/O rather than a guess that reaches the
wire.

## Client & version policy

| Client | Pinned version | What it gives |
|---|---|---|
| github.com/Azure/go-amqp | see `drivers/amqp10/go.mod` | connection, session, sender and receiver links, dispositions, manual credit |

The native baseline for AMQP 1.0 lives with the profile that has a broker to
measure against; this module measures the protocol mapping, which is variant
the subject.

### Verified facts (the API contract)

- **credit is the flow control, and the driver issues it.** go-amqp issues
  credit itself unless `ReceiverOptions.Credit` is `-1`, and this core always
  sets `-1`. The reason is the sticky unknown: a delivery whose settlement
  outcome is unknown never becomes settled, and under automatic credit the
  library re-issues credit when *it* considers a delivery settled — which for an
  unknown outcome is never. The window would shrink by one for every ambiguous
  settlement, permanently, and nothing could report it. With manual credit the
  drift is a counter a deployment reads (`Source.CreditStats`).
  _Verified against: go-amqp `ReceiverOptions.Credit`; `drivers/amqp10/credit.go`; the specification._
- **a session is the flow-control scope, so each link gets one.** The incoming
  and outgoing windows belong to the session and every link on it shares them,
  so a sink and a source on one session would make a stalled consumer throttle
  an unrelated publisher. A session costs one BEGIN frame and one channel
  number, of which there are 65,536.
  _Verified against: AMQP 1.0 §2.5.6; `drivers/amqp10/links.go`; the specification._
- **`released` does not count a delivery attempt and `modified{delivery-failed}`
  does.** This is the difference between a driver whose `MaxAttempts` policy
  works and one whose attempt count never rises. Nack means "I tried and
  failed", so it is `modified`; a delivery the *driver* gives back — a closing
  source's unhanded-out deliveries — is `released`.
  _Verified against: AMQP 1.0 §3.4.4–3.4.5; conformance `nack/immediate-redelivery` against RabbitMQ 4.3.5; the specification._
- **a message with no body section is legal and several brokers refuse it.** So
  a nil body is an explicit `amqp-value` of null rather than an omitted section,
  and an empty non-nil body is one `data` section of zero bytes. That is how
  the nil-versus-empty distinction survives the protocol.
  _Verified against: AMQP 1.0 §3.2; `drivers/amqp10/mapping.go`; the specification._
- **`group-id` is a string, and a key is bytes.** The specification makes
  `Message.Key` bytes and AMQP 1.0's group identity a string, so a key that is
  not valid UTF-8 travels as a binary application property instead. A textual
  key stays in `group-id`, where a broker that orders a message group can see
  it.
  _Verified against: AMQP 1.0 §3.2.4; `TestABinaryKeyTravelsAsABinaryProperty`; the specification._
- **go-amqp exposes no range disposition.** A disposition frame *can* cover a
  range of delivery ids and the library offers only the per-message wrappers, so
  `FeatureBatchSettle` is unsupported rather than a loop wearing a name that
  promises a saving.
  _Verified against: go-amqp `Receiver.AcceptMessage` and friends; `drivers/amqp10/capabilities.go`._

## Topic / subscription mapping

There is none in this module, and that is deliberate. An AMQP 1.0 address is
opaque to the protocol: `/exchanges/orders/orders` and `topic://orders` and
`queue:orders` are all legal and mean whatever the broker says. So
`Profile.SinkAddress` and `Profile.SourceAddress` are functions a broker module
supplies, and the core calls them with the topic and the subscription and
attaches to what comes back.

A profile that returns an error refuses the topology, and the core turns the
refusal into `ErrUnsupported` before any frame — which is how a wildcard topic,
a partition count a broker has no lanes for, and a retained topic on a broker
with no log are all answered without a socket.

`Profile.AutoCreates` says whether attaching to an address the broker does not
have creates it. It is a fact about the broker and not a choice: RabbitMQ 4's
AMQP 1.0 endpoint creates nothing and answers `amqp:not-found`, and Artemis
creates an address by default. The rule about creating production topology
behind a caller's back therefore falls to whoever runs the broker, and the
profile's job is to *say which it is* so the capability table is honest.

## Publish path & confirmation boundary

One sending link per sink, on a session of its own. A publication is one
transfer; the boundary is the **disposition**.

| `ConfirmationMode` | Boundary | Mechanism |
|---|---|---|
| `ConfirmNone` | the socket write | a pre-settled transfer: the sender settles as it sends and the peer answers nothing |
| `ConfirmAccepted` | the peer's `accepted` disposition | an unsettled transfer, awaited |
| `ConfirmDurable` | `accepted` **plus** the profile's own statement that acceptance means durable | `Profile.Durable`; refused at construction by a profile that does not claim one |

`ConfirmDurable` is the one a profile has to earn. AMQP 1.0's `accepted` says
the peer took responsibility and says nothing about disks, so the core refuses
a durable-confirmation publisher unless the profile states the boundary —
which is the rule that a driver either states the boundary or fails at
construction, applied to a protocol that cannot state it itself.

`PublishAsync` is bounded by `Config.MaxPending` and the message is borrowed
until the callback runs. A synchronous publication's body is not
copied; an asynchronous one's is, because the library holds the message until
its disposition.

## Consume path

One receiving link per source, on a session of its own, attached by the **first
Fetch** and not by the open.

The lateness is deliberate and is the same decision `drivers/rabbitmq` makes for
`basic.consume`: an attach tells the peer it may send, and everything it sends
is unsettled and counts against the prefetch whether or not anybody has asked
for a delivery. The generation bump and the token invalidation after a
reconnection are *eager*, because those are the correctness half — a settlement
must be refused from the instant the connection died — and the attach is lazy
because it cannot be done well eagerly: a supervisor re-attaching thirty links
has no caller's context and no good answer to "the fourth attach was refused".

The observable consequence is one failed fetch per reconnection, which is what a
caller of an at-least-once driver already handles. The observable consequence
for a *test* is that `Quiesce` has to warm the links first, which is what
`WarmForTest` is for.

Credit is issued manually, exactly the prefetch, and `Source.CreditStats`
reports the window: the prefetch, the credit the peer holds, the deliveries held
unsettled, and how many of those have an outcome nobody knows. The last number
is the one to watch: it never falls while the link lives, and a source whose
whole prefetch is unknown has to be reopened. A link that goes takes its unknown
deliveries back to the node with it, so the window starts again at zero.

## Ack / nack / extend mapping

| iqmq | AMQP 1.0 disposition |
|---|---|
| `Ack` | `accepted` |
| `Nack(requeue)` | `modified{delivery-failed=true}` |
| `Nack(requeue, delay)` | `modified` with the profile's delay annotation; `delivery-failed` is the profile's choice |
| `Nack(deadletter)` | the profile's dead-letter route, or `rejected` with a condition |
| `Nack(drop)` | `rejected` with `amqp:not-allowed` |
| `ExtendLease` | **unsupported**: a delivery is this receiver's for as long as the link lives, so there is no deadline to extend |

A message the mapper cannot represent is answered with `modified` — the attempt
counted and the link marked as one not to redeliver to — rather than released or
rejected: releasing it would have the broker hand it straight back for ever, and
rejecting it would destroy or dead-letter a message whose only fault is a body
shape iqmq has no field for. `Source.Undecodable` counts
them.

`NackOptions.Reason` is transported on a rejection only, in the condition's
description, because a dead-lettered message with no reason in it is a message
somebody has to guess about.

## Ordering

A link delivers in order. Beyond that the protocol promises nothing, and this
core claims nothing: `FeatureOrderedByPartition` is the link's own order and
`FeatureOrderedGlobally` is not claimed at all.

`FeatureOrderedByKey` is a **profile's** claim, and it rests on the message
GROUP: `Message.Key` is the `group-id` property, which is what Artemis orders as
`JMSXGroupID`. A profile whose broker does not order groups must not claim it,
and a key that is not valid UTF-8 is not a group at all (it travels as a
property), so the claim's note has to say that the ordering is for a textual
key. An enveloped message (every one under `MetadataPortableEnvelope`, and
those `MetadataAdaptive` envelopes) still carries its textual key in
`group-id`, because the broker groups by that property and cannot read the
envelope.

## Delays

The protocol has none. A delayed publication and a delayed redelivery are both
**annotations a broker reads**, and the key differs per broker — Artemis reads
`x-opt-delivery-delay`, ActiveMQ Classic reads `AMQ_SCHEDULED_DELAY`, and no
verified spelling exists for RabbitMQ 4.

So `Profile.Delay` and `Profile.NackDelay` are the seam, and their **zero value
refuses**: a core that guessed a key would publish a message the broker
delivers immediately, which is the worst kind of unsupported — the publication
succeeds and the delay does not happen. A profile that names no rule gets
`ErrUnsupported` before any I/O, and the capability table says so.

A rule reads either a relative delay or an absolute delivery time
(`DelayRule.Absolute`), and a caller may name either. The core restates the
one the rule does not read against `Config.Clock`: a `DeliverAt` reaches a
relative rule as the delay left until it, and a `Delay` reaches an absolute
rule as the instant it ends. A `DeliverAt` that has already passed is due now,
so a relative rule writes no delay for it at all; an absolute rule writes the
instant as given. Naming both on one publication is refused with
`CodeInvalidArgument` before any I/O.

## Dead-letter queue

Two shapes, and a profile picks one.

- **`Profile.DeadLetterAddress` is nil.** The broker's own policy routes a
  `rejected` delivery, which is what a queue argument or an address setting
  does. Nothing is published and nothing can fail.
- **`Profile.DeadLetterAddress` is set.** The core publishes a copy to that
  address and only then settles the original — publish, await the disposition,
  *then* reject — so a crash between them duplicates rather than loses, and a
  failed or ambiguous publish settles nothing. The copy waits for its
  disposition whatever `Config.SenderSettle` says: under `settled` the
  dead-letter link attaches `mixed`, because a pre-settled link has no
  disposition to wait for. The copy carries the original's sections except its
  delivery annotations and the profile's de-duplication and delay keys
  (`Profile.Dedup`, `Profile.Delay`). The broker acts on those two again when
  the copy is published: a kept de-duplication key made a second
  subscription's copy of the same publication a duplicate at the dead-letter
  address, and a kept delay held the copy back a second time. Its header is a
  fresh one that keeps the original's `durable`, `priority` and `ttl`, so its
  `delivery-count` starts at zero and its first delivery from the dead-letter
  address is attempt 1. A broker hands a header on as it was published until a
  delivery of it fails, so a copy that kept the original's count arrived
  already counted as redelivered, and a consumer there with `WithMaxAttempts`
  at or below that count treated every copy as exhausted on arrival. The
  attempt the original failed on is in `AnnotationDeadLetterAttempt`. A
  profile that reads the count from a broker annotation (`Profile.Attempt`
  with `AttemptFromAnnotation`) gets the same treatment: the copy drops that
  annotation or property, and `AnnotationDeadLetterAttempt` is read from it
  rather than from the header.

The first shape is the better one wherever the broker has a policy, because the
second costs one more copy and one more ambiguity window.

## Headers & limits

`Message.Headers` become **application properties**, typed: a value that is
valid UTF-8 travels as an AMQP string and one that is not travels as binary and
comes back byte for byte. A duplicate key cannot exist, because
application-properties is a map — `MetadataPortableEnvelope` and
`MetadataAdaptive` carry a repeated key in the envelope instead.

Under `MetadataAdaptive` a message is also enveloped when its **body begins with
the envelope's own magic** (`IQMQ` and a version byte), even if the native
fields could carry everything else: the adaptive read side decides from the
body, so a native body that looks like an envelope would be claimed as one and
refused. A non-iqmq consumer therefore reads such a body wrapped.

The AMQP properties iqmq has no field for are reachable under the reserved
`x-iqmq-` prefix: `x-iqmq-amqp-to`, `-amqp-subject`, `-amqp-user-id`,
`-amqp-absolute-expiry-time`, `-amqp-group-sequence`, `-amqp-reply-to-group-id`
and `-amqp-first-acquirer`, plus `x-iqmq-causation-id` and `x-iqmq-key`, which
the protocol has no home for. Every other name under the prefix is refused in
every metadata mode: a header the mapping does not consume would be published as
an application property whose name claims to be iqmq's.

`Profile.AnnotationPrefix` decides whether a header may become a **message
annotation** instead. It is empty for RabbitMQ, because that broker converts
between its two protocols through application properties and an annotated header
would be in a different place on the other profile.

Limits: `Limits.MaxMessageBytes` is the peer's own `max-message-size`, read off
the sender link at the first attach — a limit read from the broker rather than
configured. `Profile.Limits` may add what only the broker knows. Everything a
profile does not know stays *unknown* rather than zero, which is the rule.

## Reconnect

The core owns the dial loop (`driverkit.Reconnector`), with `Config.Backoff`
defaulting to **full jitter** and not to the specification's `Auto` — whose current
winner is `none`, i.e. no delay at all, which the specification forbids outright.
Since `ReconnectConfig.Policy`'s own zero value is
jittered too, so this default agrees with the helper's rather than working
around it.

1. the connection generation is bumped and every source's token table is
   invalidated, so a settlement for a delivery handed out before now is refused
   with `ErrLeaseLost` from that instant;
2. every sink's outstanding publication is resolved **ambiguous**, because the
   peer may have taken it;
3. the dial loop runs with its backoff;
4. links re-attach lazily, on the next operation.

`Config.NoReconnect` leaves supervision to a caller who brought their own
connection. A borrowed connection is never closed by the driver
(`driverkit.Ownership`).

## At-least-once window

A delivery is unsettled until the receiver disposes of it, and the window is the
**link's lifetime** rather than a lease: there is no deadline, so
`DeliveryInfo.LeaseExpires` is the zero time and `FeatureExtendLease` is
unsupported.

What that buys is a handler that may take as long as it likes. What it costs is
that a link which dies takes every unsettled delivery back to the broker, so the
redelivery count after a reconnection is the prefetch.

A settlement is often the first call to hear that the peer detached the link,
because a Fetch whose window is full waits for a settlement and never reaches
the link. A disposition that fails because its link went is a lost lease under
`ReceiverSettleFirst` and an unknown outcome under `ReceiverSettleSecond`,
whatever condition the peer detached with: `amqp:resource-deleted`,
`amqp:not-found` and `amqp:unauthorized-access` say why the link went, not
what became of the disposition, and `Condition` still reads them from the
error. The source drops the link there and then, so every other delivery from
it is refused with `ErrLeaseLost` and the waiting Fetch attaches a new link.
A Fetch already part way through on the dead link hands out nothing more from
it, since what the link had buffered went back to the node with it, and the new
link gets the whole prefetch.

`Profile.Attempt` says
where the count comes from — `delivery-count` plus one, by default — and a
profile whose broker does not maintain it must say so in the claim's note, or
the specification's `MaxAttempts` is compared against a number that never moves.

## Config & DSN

`Config` is the transport and the protocol: addresses, SASL, TLS, the settle
modes, the idle timeout, the reconnect bounds, the pending bound and the link
options. `Profile` is the broker. `Open(ctx, profile, cfg)` needs both.

The DSN scheme is `amqp10` for the generic profile a broker module
registers its own — `rabbitmq+amqp10`, `activemq` — with
`Register(registry, scheme, profile)`, which is one line rather than a second
copy of the DSN table. One scheme opening two profiles would make a DSN
ambiguous about which capability table the caller gets, decided by registration
order.

```
amqp10://user:password@host:5672,host2:5672?sasl=plain&tls=true&sendersettle=mixed
```

`Config.DSN()` renders a configuration back, with the credentials redacted and
the host percent-escaped. The escaping is not decoration: a rendering that its
own parser refuses makes the function useless for the one thing it exists for,
which the DSN fuzzer proved in fourteen seconds.

## Known gotchas

## Maturity & tier

See the pinned client version and the module tests when evaluating this driver.

## Driver-specific critical tests

In this module (`TestCritical*`, run by `make ci` against the scripted peer):

- **`TestCriticalStaleDispositionAfterDetach`** and
  **`TestCriticalStaleDeliveryGenerationIsRefused`** — the disqualifier,
  from both sides: a disposition for a delivery taken before a detach must never
  settle whatever holds that delivery id afterwards, and a settlement carrying a
  token from before a reconnection is refused with `ErrLeaseLost` rather than
  applied. They discriminate as well as assert — an ack for a delivery that was
  never handed out is *not* reported as a lost lease — so neither check is
  vacuously true.
- **`TestCriticalAmbiguousPublishWhenDispositionNeverArrives`** and
  **`TestCriticalDefinitePublishFailureIsNotAmbiguous`** — the two halves of
  The specification. A transfer whose disposition never comes is ambiguous, because the peer
  may have taken it; a transfer the peer REFUSED is definite, because it
  answered. Reporting either as the other is what the specification names a settlement
  disqualifier. A `rejected` disposition is an answer whatever condition it
  carries, `amqp:internal-error` included.
- **`TestCriticalDetachWithAConditionLeavesEveryWaitingPublicationAmbiguous`**
  — a link the peer detaches with a condition (a size or permission refusal)
  while transfers wait on it. The condition says why the link went and not
  which transfer it was about, so every waiting publication is ambiguous, and
  its code still names the condition.
- **`TestCriticalCloseWhileSendInFlight`** — the close against a publication
  in the socket: every outstanding transfer resolves exactly once, and the
  message a caller owned is released exactly once.
- **`TestCriticalCreditExhaustionDoesNotDeadlockFetch`** — the failure mode
  manual credit invites: a source whose every slot holds an unknown settlement
  can issue no credit, and it must say so (`ResourceExhausted`) rather than park
  a Fetch for ever.
- **`TestCriticalCloseUnblocksAParkedFetch`** — the specification's "Close unblocks a
  parked operation", with `ErrClosed` and not the link error the close caused.

The [RabbitMQ profile](rabbitmq-amqp10.md) adds three of its own. Each test
here was watched failing under a one-line mutation before it passed; the
campaign's log is in the increment's own record.
