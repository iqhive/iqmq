# sqs-fifo — message groups, deduplication and the visibility timeout

Amazon SQS's **FIFO** profile through [`drivers/sqs`](../../drivers/sqs):
ordering inside a message group, deduplication by id, what a blocked group
looks like from the API, and all three states of a visibility window.

    go run .            # against an SQS-compatible endpoint on 127.0.0.1:9324
    go test ./...       # runs the same program against an in-process server

## What it shows

```
driver sqs profile "fifo": topic orders is queue orders.fifo
claims: ordering by key true, deduplication true
in flight: acct-1/created held unsettled, so the next receive produced acct-2/created instead of acct-1's second message
blocked: with acct-1 in flight and acct-2 empty, a receive produced nothing though the queue still holds 2 message(s)
order: acct-1 delivered created,paid,shipped, in publication order
dedup: 5 publications produced 4 messages; the second copy of dedup id acct-1/created was discarded by the service
lease: granted for about 2s, this consumer's own AckDeadline
lease: extended by 6s, and Info().LeaseExpires still reports the original deadline — a DeliveryInfo is the snapshot the receive handed over 
lease: a settlement after the window reports ErrLeaseLost, refused by the driver before the delete leaves the process
redelivery: acct-9/lease-demo came back marked as a redelivery, on a later attempt, and was settled
```

### The four things FIFO adds, and the one mechanism under all of them

| Portable API | SQS | What it buys |
|---|---|---|
| `Message.Key` | `MessageGroupId` | order **within a group**, none across groups |
| `mq.DedupKey(s)` | `MessageDeduplicationId` | the service discards a second publication of the same id for five minutes (the window is the service's and is not configurable) |
| — | one in flight per group | what makes the order hold under concurrency |
| `Config.AckDeadline` | the visibility timeout | the lease: unsettled, the window passes, the message comes back |

A publication with **no** `DedupKey` is given a unique identifier by the
driver, so it is never deduplicated by accident — which also means a retried
publication without one publishes a second message. A publication with **no
key** goes to `sqs.DefaultGroupID`, which puts every keyless message in one
group, in order, one in flight at a time: a throughput cliff rather than an
error, and almost always a mistake.

### A blocked group looks exactly like an empty queue

The third and fourth lines are the ones to take away. With
`acct-1/created` held unsettled:

- the next receive produces `acct-2/created` — a different group is a
  different lane;
- the receive **after** that produces **nothing at all**, though the queue
  still holds `acct-1`'s two remaining messages.

There is no error, no code and no signal for that state. A poll that comes
back empty from a FIFO queue may mean the queue is empty or it may mean every
group in it is blocked by a message somebody is still working on, and the
portable API cannot tell you which. Do not read an empty poll as an empty
queue. `Admin.Stats` is where a depth comes from.

### Two windows, two consumers, one queue

The example uses two consumers with different `AckDeadline`s, and shuts the
first down before opening the second. Both halves of that are load-bearing:

- an acknowledgement deadline is **per consumer** (the specification's `AckDeadline`,
  which this driver turns into the receive's `VisibilityTimeout`), and the two
  halves want opposite things from it. The blocked-group proof holds a message
  unsettled while it polls, so its window must outlast the polling; the lease
  phase has to **wait the window out**, so a long one is a long test. Sharing
  a two-second window made the third poll deliver the message the first was
  still holding — against a real broker, which is where it was found.
- `SourceConfig.Subscription` is **ignored** by this driver. A queue has
  exactly one set of messages, so two consumers of one topic are two
  *competitors*, not two subscriptions. Left open, the first would steal the
  lease phase's message about half the time. Two independent subscriptions
  need [`drivers/snssqs`](../../drivers/snssqs), where the topic is an SNS
  topic and the subscription is a queue.

### A run gets a queue of its own

`newTopic` appends a timestamp, and the queue is deleted at the end
best-effort. A FIFO queue makes a shared name expensive in two ways a standard
one does not, and both were met against a real broker while this was written:

- a message an earlier run left behind is delivered to this one, where it
  belongs to no group the program knows and breaks every assertion about what
  is in the queue;
- the **deduplication window is five minutes** and is not configurable, so a
  second run inside it has its first publication discarded as a duplicate of
  the earlier run's.

### A message nobody settles accumulates attempts on a timer

The final line does not print an attempt number, and that is a fact rather
than a simplification: the driver's pollers keep fetching while the consumer
is open, so during the nine-second wait the unsettled message was received,
expired and received again once per window. Its attempt count is a function of
wall time. That is also the mechanism a redrive policy's `maxReceiveCount`
counts — a message nobody settles reaches the dead-letter queue on a timer,
not on a decision.

### The lease, and two things the API will not tell you

1. **`Extend` does not report what it bought.** `DeliveryInfo` is the snapshot
   the receive handed over, so `LeaseExpires` still reports the *original*
   deadline after a successful extension, and the SPI's `Extend` returns only
   an error. A caller that needs the new deadline has to assume the documented
   minimum.
2. **On this driver `Extend(by)` used to make the lease shorter.** The specification and
   `Delivery.Extend` both say an extension moves the deadline "at least `by`
   further into the future". The driver implemented it as one
   `ChangeMessageVisibility` with `VisibilityTimeout=by`, and the service
   measures that **from now** — so `Extend(by)` set the window to `by` from
   now rather than `by` beyond the deadline it had, and a `by` shorter than
   what was left shortened the lease and made the message visible to another
   consumer early. The driver now sends the remaining window plus `by`, and
   refuses an extension whose sum passes the service's twelve hours rather
   than clamping to it and reporting success.
   `TestExtendDoesNotShortenThisDriversLease` is the evidence in this
   directory; `drivers/sqs/lease_test.go` brackets the new deadline from both
   sides.

The first is recorded and is still open — there is
nowhere in the portable API for an extension to report what it bought. The
second was and is closed by.

### Why the driver refuses a settlement it could have sent

The service will accept a `DeleteMessage` for a receipt handle whose message
another consumer now holds: there is no API that asks "do I still hold this?",
the delete does not fail, and the message is gone. So the refusal can only
happen in the driver, and it does — before the call, comparing the visibility
deadline it recorded at the receive against now on the injected clock, and
answering `mq.ErrLeaseLost`.

It is a **local** view and deliberately pessimistic in both directions: it
cannot see a window another process shortened, and it refuses on a window this
process believes has passed even where the service would still have taken the
delete. The cost is a redelivery. The alternative is deleting somebody else's
work.

## Running it against a broker

`go test ./...` always runs the example against
[`sqsfake`](../../drivers/sqs/sqsfake), this driver's own in-process server:
queues, per-message visibility windows on an injected clock, stale receipt
handles, `ApproximateReceiveCount`, and the FIFO rules above. It is **not** an
emulator and its numbers mean nothing; what it is, is enough of the service to
check every line above with no Docker at all.

One thing it used to get wrong, and the example worked around in the test
rather than in the program: `Server.Advance` expired a message's visibility
window but did **not** release its FIFO group, because the hold was a counter
decremented by a delete, by a visibility set to zero and by
`ExpireVisibility` — and not by the clock. So a FIFO message whose window
lapsed by the clock alone was visible and undeliverable for ever: the queue
reported depth 1 and invisible 0 and every receive returned nothing, which is
a test suite deadlocked against the fake rather than against SQS. The server
now derives a group's hold from what is in flight, so the clock releases the
group exactly as a real queue does, and the in-process arm just moves the
clock (closing).

To run against a container as well, start one and name it — the variable is
`conformance/harness`'s own convention, `IQMQ_<BROKER>_ADDR`, and ElasticMQ is
the emulator this project's SQS conformance evidence is taken against:

```sh
docker run -d --name sqs-example -p 9324:9324 \
  softwaremill/elasticmq-native@sha256:e4580ab9ad1bd5cd37b4ba04911bc5ccc8cd2d9ab4de56ece65acee71c24e05c
IQMQ_ELASTICMQ_ADDR=127.0.0.1:9324 go test -count=1 -v -run TestRunAgainstABroker ./...
docker rm -f sqs-example
```

(The digest is the one pinned in `conformance/harness/images.lock`. LocalStack
works too — `IQMQ_ELASTICMQ_ADDR=127.0.0.1:4566` — it just takes about a
minute to start.) The broker arm really waits out the visibility window, nine
seconds of it, because ElasticMQ's clock is the wall clock and there is no
other way to be on the far side of a timeout.

With `IQMQ_ELASTICMQ_ADDR` unset that test **skips** and says so; with
`IQMQ_REQUIRE_BROKERS=1` an unset variable is a **failure** instead.
`make examples` prints which examples reached a broker and which skipped, by
name, so a run in which nothing did says so rather than reading as green.

### For the managed service

Drop `Endpoint` and `InsecureTransport`, put a real region in `Config.Region`,
and use `sqs.OpenWithClient` with an `*sqs.Client` built from the AWS SDK's
default credential chain — the chain lives in a module this driver
deliberately does not depend on. Nothing else in `main.go` changes. Note that
numbers taken against either emulator describe the client and the container
and are never evidence for the service (layer 4).

## Files

| File | What is in it |
|---|---|
| `main.go` | the program: `Config`, `open`, `publish`, `blockedGroup`, `leasePhase` |
| `main_test.go` | both arms, the profile and dedup-id checks, and the `Extend` finding |
| `report_test.go` | the `IQMQ_EXAMPLE_REPORT` contract `make examples` reads |
