# Architecture

![The layers: your code, the core, the SPI, driverkit, the drivers, the brokers](../assets/architecture.svg)

Five facts about that picture are worth stating in words, because they are the
ones that decide how the library behaves when something goes wrong.

## The core holds every policy, and the drivers hold none

Retries, backoff, attempt counting, dead-lettering, ordering mode, publisher
backpressure, lease extension, panic recovery and drain are all in the root
package. The specification puts it as a prohibition on drivers: *a driver MUST NOT implement
worker pools, retries or dead-letter logic.*

The reason is not tidiness. A retry loop in a driver is a second owner of
redelivery accounting, and two owners produce a message that is retried
25 times when you asked for 5, or dead-lettered by one layer and requeued by
the other. The specification requires **exactly one** owner, and the way to have exactly one
is for the layer that the caller configures to be the only layer that decides.

Where a broker has its own retry and dead-letter policy — SQS redrive, Pub/Sub
dead-letter topics, RabbitMQ DLX, Pulsar's `DLQPolicy` — the driver *maps* the
configuration onto it rather than adding a loop beside it, and the runner's own
policy stands down. `mq.WithNativeDeadLetter()` is how you ask for that
explicitly; the driver page says whether it exists.

## The SPI is three methods, and everything else is optional

A driver must implement `Backend` (open, ping, describe, close), `Sink`
(publish) and `Source` (fetch). That is the whole obligation.

Everything a broker does *differently* is an optional interface: `BatchSink`,
`AsyncSink`, `OwnedSink`, `FlushSink`, `Settler`, `BatchSettler`,
`DeadlineExtender`, `AdminBackend`, `HealthReporter`. The core type-asserts
each one **once**, when it opens the sink or the source, and compiles the
answer into a boolean. Nothing on a message's path performs a type assertion,
which is the rule 4 and the reason the abstraction can be thin enough to
have a measured overhead budget at all.

## The root module depends on the standard library only

So does the `memory` driver, which lives inside it. Every driver that needs a
third-party SDK is its own Go module under `drivers/`, `integrations/` or
`codec/`. Depending on this library therefore costs you one client library —
the one you chose — and not eleven.

`make stdlib-check` holds that property; it is not a convention.

## `memory` is a driver, not a mock

The in-process engine implements the same `Backend` as Kafka does, and it is
the reference target for the state-machine, linearizability and differential
tests. A test written against `memory.NewForTest` exercises the same runner,
the same settlement state machine and the same capability guards as
production. See [the memory driver](../testing/memory-driver.md).

It is not persistent and never advertises durable storage.

## Internal implementations

The variant families under `internal/` contain alternative implementations
for queues, wait strategies, timers, hashes, pools and dispatch.
Use the benchmark tools to compare candidates on your intended workload.
The public API does not expose these internal choices.

## What the diagram does not show

The picture has no arrows back from the driver to the core, and that is
accurate: a `Source` never consults the core, because every parameter it needs
is in the `SourceConfig` it was opened with. It also has no callback path — the
core pulls with a bounded `Fetch` and never registers a handler with a client
library, so a push-delivering client (NSQ, Pub/Sub streaming pull, Pulsar) is
adapted to a pull by `driverkit.PushAdapter` inside the driver rather than
turning the core inside out.
