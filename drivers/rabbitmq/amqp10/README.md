# iqmq/drivers/rabbitmq/amqp10 — the AMQP 1.0 profile for RabbitMQ 4

This module is the **conditional AMQP 1.0 profile** of the driver documentation, the
second arm. It is not a second RabbitMQ driver and it is
not a driver at all: it is one value — an `amqp10.Profile` — handed to
`drivers/amqp10`, the shared AMQP 1.0 core that owns the links, the credit, the
dispositions, the message mapping and the dial loop.

    go get github.com/iqhive/iqmq/drivers/rabbitmq/amqp10

```go
client, err := rmq10.Open(ctx, rmq10.Config{
	Addrs:      []string{"amqp://rabbit-1:5672"},
	Username:   "iqmq",
	Password:   dsn.Secret("s3cret"),
	Vhost:      "acme",
	Prefix:     "acme.",
	QueueType:  rmq10.QueueQuorum,
	DeadLetter: true,
})
```

or `rabbitmq+amqp10://iqmq:s3cret@rabbit-1:5672?vhost=acme&deadletter=true`.

## It refuses RabbitMQ 3.13, on purpose

`Open` against a broker that reports 3.13 fails with `mq.ErrUnsupported`
naming both versions, and **nothing falls back to AMQP 0-9-1**. RabbitMQ 4.0 is
the first release that speaks AMQP 1.0 natively; on 3.13 it is a plugin whose
feature set, address syntax and settlement behaviour differ. The specification says this
profile is "never silently substituted", and a driver that quietly opened the
other protocol would be that substitution with a different spelling — so a
caller who wants AMQP 0-9-1 imports `drivers/rabbitmq` and says so.

A peer whose `product` property is not RabbitMQ's is refused before the version
is even read. Apache Artemis reports version 2.44, which passes no RabbitMQ
gate — but it also auto-creates an address from an attach, so a client pointed
at the wrong endpoint would not fail: it would create an address literally
called `/exchanges/orders/orders` and publish into it for ever.

## What differs from `drivers/rabbitmq`

The two profiles share one topology, derived by one piece of code
(`internal/rabbit`), so a caller who moves between them finds the same
exchange, the same quorum queue and the same dead-letter route. What they do
not share is this list, and it is the whole reason the specification forbids substituting
one for the other.

| | `drivers/rabbitmq` (0-9-1) | this module (1.0) |
|---|---|---|
| Unroutable message | `mandatory` + `basic.return`, then ack | the broker **releases** the transfer; this driver maps that to `CodeNotFound`, not retryable |
| Missing exchange | channel exception | the link closes with `amqp:not-found` |
| Publisher confirm | a confirm **mode**, with three strategies | one **disposition per transfer**; no mode, no cumulative form, no strategy to choose |
| The window a caller sizes | the confirm window **and** `basic.qos` | the link's **credit** (`Config.Credit`) |
| Request/reply | native, via `amq.rabbitmq.reply-to` | **none**; a reply address is the portable emulation |
| Attempt count | the `x-delivery-count` **header** | the header section's `delivery-count` **protocol field** |
| Settlement | `basic.ack` / `basic.nack` / `basic.reject` | a **disposition**: accepted / rejected / released / modified{delivery-failed, undeliverable-here} |
| Delayed redelivery | TTL queue or the plugin | **none** — see "unverified" below |
| Scheduled publication | the delayed-message plugin | **none** |
| Ordering by key | `WithKeyAsRoutingKey` | **none**: a link's address carries the routing key, so it cannot vary per message |
| Wildcard subscription | a binding pattern on a topic exchange | **none**: a receiving link addresses a queue |
| Topology | `Config.Declare` creates it | **cannot** — `Declare` is refused, and `Client.Admin` answers `ErrUnsupported` |
| Virtual host | the connection URL's path | the OPEN frame's `hostname`, as `vhost:<name>` |
| DSN scheme | `rabbitmq://` | `rabbitmq+amqp10://` |

## What it gains

Nothing in the capability table, which is worth saying plainly. What AMQP 1.0
gains here is better MECHANISMS for features both profiles claim: a typed
message model (a binary application property is a binary value rather than a
byte array in a field table), a disposition that maps onto the four actions
directly, and an attempt count that needs no queue argument. All three change
the notes and not the levels.

## The topology is somebody else's

RabbitMQ 4's AMQP 1.0 endpoint creates no exchange, queue or binding. An attach
to a queue that does not exist closes the link with `amqp:not-found`, which
this driver reports as `CodeNotFound`. So the specification's "no hidden creation" is a
property of the protocol here rather than a choice, and the exchange, the
quorum queue and the dead-letter route have to be created by `drivers/rabbitmq`,
the HTTP management API or `rabbitmqctl`. `Config.Names` is how a provisioner
asks for the names, and `ConformanceHarnessWithProvisioner` is how a
conformance suite creates them per scenario.

RabbitMQ 4 does have an AMQP 1.0 management extension over a `/management`
node. This profile does not implement it, and says so rather than claiming
`FeatureAdmin`.

## Four things are not verified, and none of them is claimed

Nothing in this increment ran against a RabbitMQ broker, so four claims 
marks **(verify)** are recorded as unverified rather than guessed at. Each is
Unsupported in the capability table or stated as a condition, never invented:

1. whether the delayed-message plugin is reachable at all over AMQP 1.0, and
   which section and key a delay would travel in → `FeatureScheduledPublish`
   and `FeatureDelayedRedelivery` are **Unsupported**;
2. whether a classic queue's redeliveries reach the header's `delivery-count`
   → the attempt claim states the condition and withdraws on a classic queue;
3. whether `x-death` survives a dead-letter hop in the AMQP 1.0 view → not
   claimed and not read;
4. that RabbitMQ selects a virtual host from `hostname: vhost:<name>` → it is
   documented RabbitMQ behaviour, implemented, and marked unverified on the
   driver page.

`docs/drivers/rabbitmq-amqp10.md` has the fifteen fixed sections,
including the whole list under "Known gotchas".

## Tests need no broker

Every test in this module runs against a scripted peer: go-amqp on the other
end of a loopback socket, whose `ConnOptions.Properties` are the `product` and
`version` a broker announces itself with. The AMQP 1.0 handshake is symmetric,
so that is a complete and correct peer for everything this module decides — the
version gate, the product gate, the capability table and the error mapping.
Nothing here starts a container.

    GOWORK=off go test -race ./...
    GOWORK=off go test -run='^$' -fuzz='^FuzzRabbitAMQP10Address$' -fuzztime=30s.

The conformance suite is env-gated on `IQMQ_RABBITMQ_ADDR` and needs the
topology to exist; `conformance/suites` is the shipped evidence.
