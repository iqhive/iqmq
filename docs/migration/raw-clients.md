# From a raw client

This is the migration that costs the most and gains the most, and it is the one
this project's second design principle is about: **measured overhead budgets
against raw clients**. Before you read the mapping, read what the
measurement says, because it is not a flattering number.

## What the overhead actually is, measured

Driver overhead against the raw client is the
page, and its summary is:

* the **batched** budget is ≤ 5 % throughput loss. It is **breached on 4 of
  the 12 measured batched rows** -- all three `jetstream` rows and
  `redis/streams/PublishBatch/64B` -- and on the other eight this host's own
  spread is wider than the excess, so they are undecided rather than met. The
  worst row is `jetstream/PublishBatch/64KiB` at **+46 %**, an effect ten times
  its own measurement spread. It read as ten of twelve until each row's
  verdict was read on its confidence interval.
* the **single-message** budget is ≤ 10 %. It is breached on 1 of 12 rows,
  `jetstream/Publish/64KiB` at +31.9 %, which is the one of the three the point
  estimate flagged that is clear of its spread.
* Of the twenty-four gated ratio rows, **five are red, two are met and
  seventeen are undecided**: since a row's verdict is read on
  both medians' confidence intervals rather than on the two medians alone, and
  on this host seventeen rows' intervals straddle their ceiling. Nothing has
  been widened or waived to make any row green -- eight rows moved from breach
  to undecided and nine moved from met to undecided, and none moved from breach
  to met.

So: **if you are replacing a raw client with iqmq for a high-volume batched
publish path, today, expect to give up throughput, and measure it.** The
figures are per broker and per operation, so read the table rather than a
single number. The cause is unmeasured and is a profile away; the honest state
of it is on that page.

The competitor comparison in
the competitor benchmark puts the same question
differently: how do iqmq, Watermill and gocloud each compare with the raw
client at the *same* semantics.

## What you get for it

Every item here is something a raw-client codebase writes for itself, usually
once per broker, usually differently each time:

* **A capability table** (`client.Capabilities()`), generated from conformance
  runs, and a portable API that refuses what a transport cannot do instead of
  silently dropping it.
* **Settlement with a stated boundary.** `mq.ErrSettlementUnknown`,
  `mq.ErrLeaseLost` and `mq.ErrAlreadySettled` name three cases raw-client code
  usually conflates into "err != nil".
* **A runner** — bounded prefetch, an in-flight semaphore, lane routing by key,
  a fixed worker pool, one centralised lease-extension scheduler, retry with
  backoff, dead-lettering that never acks a failed DLQ publish, and a drain
  that stops receiving before it waits on workers.
* **A conformance kit.** `testkit.RunConformance` puts your own driver, or the
  one you chose, through 155 scenarios.
* **The memory driver and `synctest`**, so the tests of your message handlers
  need no container and no sleep.
* **One error model** across brokers, with `errors.As` reaching the native
  error underneath.

## What maps, per client

The mapping is mechanical; the boundaries are not. Confirmation is the field to
get right, because a raw-client call's boundary is whatever that client's
default is and iqmq makes you name it.

### nats.go

| nats.go | iqmq | Boundary |
|---|---|---|
| `nc.Publish(subj, b)` | `pub.Publish` with `mq.ConfirmNone` | in the client's write buffer |
| `nc.Publish` + `nc.Flush()` | `pub.Publish` with `mq.ConfirmAccepted` | the server has parsed it; nothing is stored |
| `js.Publish(subj, b)` (JetStream) | `jetstream` driver, `mq.ConfirmDurable` | a `PubAck` |
| `nc.SubscribeSync` + `NextMsg` | `con.Receive(ctx, dst)` | |
| `nc.QueueSubscribe` | `ConsumerConfig.Subscription` | a queue group |
| `msg.Ack()` (JetStream) | `d.Ack()` | on Core, `d.Ack()` is `mq.ErrNotSettleable` — correctly, there is no ack |
| `nats.Header` | `mq.Message.Headers` | NATS headers are a text line: a control byte, a leading space, a colon in a name and the reserved prefix are refused |

### franz-go / kafka-go

| Raw | iqmq | Boundary |
|---|---|---|
| `cl.Produce` with a callback | `pub.PublishAsync(ctx, m, done)` | `acks` maps to `ConfirmationMode` |
| `cl.ProduceSync` | `pub.Publish` | |
| `cl.PollFetches` | `con.Receive(ctx, dst)` | |
| `cl.CommitRecords` | `d.Ack()` | iqmq's offset tracker turns per-record acks into a watermark |
| manual partition assignment | `mq.Ordering`, `mq.WithOrderingByKey` | |
| `kgo.RecordPartitioner` | `mq.Message.Key` | the driver's partitioner |

### rueidis / go-redis

| Raw | iqmq | Boundary |
|---|---|---|
| `XADD` | `pub.Publish` with `mq.ConfirmAccepted` | the `XADD` reply |
| `XADD` + `WAITAOF` | `mq.ConfirmDurable` | needs an append-only file on the server |
| `XREADGROUP` | `con.Receive` | |
| `XACK` | `d.Ack()` | |
| `XAUTOCLAIM` | the driver's reclaim loop | |

### AWS SDK (SQS/SNS)

| Raw | iqmq | Boundary |
|---|---|---|
| `SendMessage` | `pub.Publish` | the API response |
| `SendMessageBatch` (10 max) | `pub.PublishBatch` | the driver splits and reports index-aligned results |
| `ReceiveMessage` (10 max, long poll) | `con.Receive(ctx, dst)` | |
| `DeleteMessage` | `d.Ack()` | |
| `ChangeMessageVisibility` | `d.Extend(by)` | **see the trap below** |
| message attributes (10 max) | `Headers` + `mq.MetadataAdaptive` | the envelope appears above ten attributes |

## What does not map at all

* **Every broker-specific knob.** Linger, prefetch, pipelining depth, poller
  count, partitioner, compression: these are typed options in the *driver*
  package (`kafka.WithLinger(2*time.Millisecond)`), not in the portable API,
  so they fail at compile time on the wrong driver rather than being ignored.
  Anything without a driver option is reachable through `client.Native()`,
  which hands you the raw client back.
* **Broker-specific administration.** `client.Admin()` covers the portable
  subset (`EnsureTopic`, `EnsureSubscription`); a Kafka partition reassignment
  or a JetStream stream update is `client.Native()`.
* **Anything that needs the raw client's own concurrency model.** franz-go's
  `PollFetches` is designed to be called from one goroutine and hands you a
  whole fetch; iqmq's runner owns that loop. Mixing the two on one client is
  possible through `OpenWithClient(ctx, cl, driverkit.ClientBorrowed, cfg)` and
  is your problem to keep coherent.
* **Exactly-once claims.** iqmq does not make one, and neither does the raw
  client. Kafka transactions are in `x/kafkatx`; the semantics you get are the
  broker's.

## Traps

**`ChangeMessageVisibility` on SQS sets the visibility window from the CALL, so
raw code that passes the extension it wants shortens the lease of any message
with more than that left.** Your raw code has this hazard and usually has a
comment about it; `iqmq`'s SQS driver does not, because `Delivery.Extend(by)`
moves the deadline at least `by` further and the driver sends the remaining
window plus `by` to achieve it. What the portable API
still cannot tell you is the new deadline: `DeliveryInfo` is the snapshot the
receive handed over, so assume the documented minimum.

**Publishing a `[]byte` you then mutate.** iqmq's `Raw` codec and the owned
publication path (`PublishOwned`) hand your slice to the driver without a copy,
which is the zero-copy path the specification is about. The contract is that you do not
touch it until the publication completes. A raw client generally copies, so
code that reused a buffer safely before may not now — or use `mq.Message.Body`
with the default safe mode, which clones caller-owned mutable data exactly
once.

**`d.Ack()` takes no context.** Settlement is not a cancellable operation in
this API; it is bounded by the driver's own timeouts. Raw code that passed a
per-ack context has nowhere to put it.

**A non-nil error from `Receive` can accompany valid deliveries.** Settle them
before you return the error, or you strand a lease per delivery on the last
fetch of every loop. Raw clients differ on this and most do not have the case.

**The portable `Prefetch` default is 128 and the driver may override it.** Raw
code tuned to a specific fetch size should set `ConsumerConfig.Prefetch` and
check the driver's note rather than assume.

## How to decide

1. **Read the overhead page for your broker and
   your operation.** If your hot path is a large batched publish on JetStream,
   the measured cost today is tens of per cent and you should know that before
   you port.
2. **Port one consumer first, not one publisher.** The runner is where most of
   the code you delete lives, and the receive path's measured overhead is not
   what the breached budgets are about.
3. **Keep the raw client reachable.** `OpenWithClient(ctx, native,
   driverkit.ClientBorrowed, cfg)` lets one connection serve both, so a hot
   path can stay raw while the rest moves.
4. **Run the conformance kit against your chosen driver** before you rely on a
   capability. `conformance/reports` has the stored runs; your broker version
   may differ from the one they were taken against.
