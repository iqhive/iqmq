# Upgrades

Three different upgrades with three different risks: the library, the client
library underneath it, and the broker.

## Upgrading iqmq

`v0.x` while the API settles. Breaking changes are announced with migration
notes, and `make apidiff` holds the public surface against
`hack/api/root.export` so that a change to it is deliberate rather than
noticed by a user.

From `v1.0` the root package and the SPI are stable: growth happens through
**optional interfaces** and **concrete handles**, because adding a method to a
public interface is a breaking change for everyone who implemented it. The SPI
is `Backend` plus nine optional interfaces
rather than one large one.

Core minors ship as a **release train**: every official module is tagged
together, so `iqmq v1.4.0` and `iqmq/drivers/kafka v1.4.0` are a matched pair.
Patches are independent. A generated compatibility manifest lists which driver
ranges work with which core.

Deprecations are kept for at least two minor releases with `Deprecated:`
markers. Go version bumps happen only on minors, and are announced.

## Upgrading the client library under a driver

This is the one that bites, because it is a transitive dependency of *your*
module: `drivers/kafka` requires franz-go, and `go get -u` will move it.

Each driver's `go.mod` records its client dependency. Check that version and
the driver page's **Client & version policy** when upgrading.

This checkout contains no generated conformance reports or capability matrix
to establish tested compatibility. Run the relevant conformance suite against
your chosen source revision, client and broker versions before relying on a
compatibility claim.

## Upgrading the broker

The risk here is a capability changing under you, in either direction, and
three examples show the shapes:

* **a feature arrives.** JetStream's `Nats-TTL` needs 2.11 and scheduled
  messages need 2.12. A driver that advertises them unconditionally against a
  2.10 server would be lying, so the claim is conditional and the condition is
  the server version.
* **a feature becomes available a different way.** RabbitMQ ≥ 4 speaks AMQP
  1.0, which is why `drivers/rabbitmq/amqp10` exists as a separate profile —
  and it is **never silently substituted** for the 0-9-1 driver, because the
  two have different settlement and ordering behaviour.
* **a default changes.** This is the dangerous one, and the only defence is the
  broker-version matrix: `make matrix` runs the conformance suite against the
  broker versions `conformance/harness/images.lock` pins, nightly, and a claim
  that stops holding on a newer image is a failed scenario rather than a
  surprise in production.

## The order to do it in

1. Run `iqmq doctor` against the **new** broker with the **old** driver. It
   dials, reports capabilities and limits, and exits `3` when something a
   command needs is missing.
2. Compare that against the capability table for your profile. A level that
   dropped from `Native` to `Conditional`, or a limit that shrank, is the
   thing to find now rather than at peak.
3. Upgrade the driver, then the broker, on a topic you can afford to replay.
4. Watch `ConsumerStats.Redelivered`, `SettleFailures` and
   `DeadLetterFailures`. Those three move first when a settlement contract has
   changed.

## Preparing a release

Run the release checks, API compatibility checks and relevant test
suites against the intended source revision. Record fresh results
when making conformance or performance claims. Local release tags
are lightweight references and provide no signature verification.
