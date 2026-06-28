# bridge — moving messages between two brokers, and what the seam costs

A bridge from a JetStream stream (`drivers/jetstream`)
to the in-process engine (`memory`), which **produces** the
duplicate its own guarantee allows instead of describing it.

    go run .            # against a NATS server with JetStream on IQMQ_NATS_ADDR
    go test ./...       # runs the same program against an in-process server

## What it shows

Moving messages between two brokers looks like a loop and is not. The two
brokers know nothing about each other, so nothing can make the destination's
acceptance and the source's acknowledgement one atomic step, and the ORDER of
the three calls decides what happens when the run dies in the middle:

| Order | Guarantee |
|---|---|
| receive → publish → **confirm** → settle the source | at-least-once, duplicates possible on the destination |
| receive → settle the source → publish | at-most-once, and **lossy** |

The first is what this program does and what `iqmq bridge` implements. The
second is not offered: a duplicate is something a destination can be built to
tolerate — de-duplicate by a key, make the handler idempotent — and a lost
message is not.

Two details of the first order are easy to get wrong and both are load-bearing:

- **Confirm, do not merely publish.** `Publish` returning `nil` means the
  message was *admitted* to the driver, which for a buffering driver is not the
  same as accepted by the broker. Settling the
  source against an admission is the same lost message as settling before
  publishing, just harder to see. `Flush` is what turns one into the other.
- **Nack on every failure path.** A message the destination refused is a
  message the source still owes. A delivery simply dropped is held until its
  lease expires with nothing in the output saying so.

## The output

```
settlement: at-least-once across the seam -- publish, confirm, then settle the source
moved 3 of 3
destination holds: order-1 order-2 order-3
interrupted transfer: confirmed on the destination, source delivery NOT settled
after the source's ack deadline: redelivered attempt=2, and published again
destination now holds: order-4 order-4
duplicate on the destination: order-4 twice, from one publication and one redelivery
the seam cannot do better without a transaction spanning both brokers, which the specification does not offer
```

Line by line:

| Line | What it is |
|---|---|
| `settlement: …` | the sentence a bridge owes its reader. The specification forbids an unqualified guarantee, and a bridge is exactly where one would be read into silence |
| `moved 3 of 3` | the ordinary case: published, confirmed, then settled |
| `destination holds: …` | read back out of the destination, so "moved" is a fact and not a counter |
| `interrupted transfer: …` | **the line the example exists for.** One transfer deliberately skips the last step, which is what a crash between the confirmation and the acknowledgement looks like |
| `redelivered attempt=2` | the source's ack deadline expired and JetStream sent it again. Nothing sleeps for this: `Receive` blocks until the server does it |
| `destination now holds: order-4 order-4` | the duplicate, arriving |

`TestTheDuplicateIsReal` reads that last count directly rather than trusting
the printed line, because the golden block on its own would be satisfied by a
"fix" that settled the source before publishing — the duplicate would disappear
and so would the data loss's only visible symptom.

## Why these two brokers

The source is **JetStream** because a bridge's source needs real settlement:
the whole story is about the window between a confirmation and an
acknowledgement, and a broker with no acknowledgement has no window. NATS Core
is that broker — its `Ack` answers `mq.ErrNotSettleable` rather than pretending
— so a bridge whose source is NATS Core is at-most-once on the source side and
loses a message on any crash, whatever order its code uses.

The destination is the **memory engine** because the subject is the SEAM and not
the destination's behaviour.

## Neither end provisions itself

`Open` never creates production topology, so
this program creates both ends explicitly:

- the JetStream stream with `nq.Config.ManageStream`, which is the driver's own
  opt-in. Without it, `Open` binds to a stream that must already exist and
  fails with `code=404` — correctly;
- the destination's topic **and subscription** over the portable `Admin`
  surface. The subscription is created
  *before* anything is published: a queue retains nothing for a subscription
  that does not exist yet, so a bridge whose destination subscription is created
  afterwards delivers into a void and reports success.

`iqmq topology apply` is the command that does this for a deployment; see
the CLI page.

## Running it against a broker

`go test ./...` always runs the example against an **in-process**
`nats-server` with JetStream enabled, which is the real server compiled into
the test binary. To run it against a container as well, start one and name it —
the variable is `conformance/harness`'s own convention, `IQMQ_<BROKER>_ADDR`:

```sh
docker run -d --name nats-bridge-example -p 4222:4222 \
  nats@sha256:ad7a43eb7e3337c3c38ce5d784d1461791f95f730f252d2b25eee699752a0ca3 -js
IQMQ_NATS_ADDR=127.0.0.1:4222 go test -count=1 -v -run TestRunAgainstABroker ./...
docker rm -f nats-bridge-example
```

(The digest is the one pinned in `conformance/harness/images.lock`. The `-js`
matters: a plain NATS server answers the stream creation with "jetstream not
enabled", which this arm reports as a failure rather than a skip.) With
`IQMQ_NATS_ADDR` unset that test **skips** and says so; with
`IQMQ_REQUIRE_BROKERS=1` an unset variable is a **failure** instead, which is
what a run that meant to test against a broker sets. `make examples` prints
which examples reached a broker and which skipped, by name, so a run in which
nothing did says so rather than reading as green.

## Files

| File | What is in it |
|---|---|
| `main.go` | the program: `open`, `provision`, `transfer`, `awaitRedelivery` |
| `main_test.go` | both arms, the expected output compared byte for byte, and the duplicate counted directly |
| `report_test.go` | the `IQMQ_EXAMPLE_REPORT` contract `make examples` reads |
