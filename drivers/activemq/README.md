# drivers/activemq

The Apache ActiveMQ driver over **AMQP 1.0**, for both products: Apache
ActiveMQ **Artemis** and Apache ActiveMQ **Classic**. It is a profile of
[`drivers/amqp10`](../amqp10/README.md), which owns the protocol and knows
about no broker at all.

`drivers/activemq/stomp` is the same two brokers over STOMP 1.2, and the pair
is variant family ** **.

```go
c, err := activemq.Open(ctx, activemq.Config{
    Broker:   activemq.Artemis,          // REQUIRED. See below.
    Addrs:    []string{"broker:5672"},
    Username: "app", Password: "…",
    Prefix:   "iqmq.",
})
```

## `Config.Broker` has no default, and that is the point

"ActiveMQ" is two products with one name and their topic mappings share no
name. Artemis routes a topic through a MULTICAST address and gives every
subscription a durable queue addressed by the fully qualified queue name
`address::queue`; Classic has virtual topics, where a publication to
`topic://VirtualTopic.orders` is copied into every queue called
`Consumer.<name>.VirtualTopic.orders`.

Nothing on the wire says which product a caller meant, and getting it wrong is
**not an error on either**: Artemis creates an address literally called
`topic://VirtualTopic.orders` and publishes into it for ever, and delivers
nothing. The specification's "never silently substituted" is why the field is required, and
`CheckProduct` is why a client pointed at the other product is refused before
it creates anything.

| | Artemis | Classic |
|---|---|---|
| topic | `<prefix><topic>` (multicast address) | `topic://VirtualTopic.<prefix><topic>` |
| subscription | `<address>::<address>.<sub>` (FQQN) | `queue://Consumer.<sub>.VirtualTopic.<prefix><topic>` |
| delay | message annotation `x-opt-delivery-delay` | application property `AMQ_SCHEDULED_DELAY` |
| de-duplication | `_AMQ_DUPL_ID` | none |
| topology | `Client.Admin()` (the broker's management address) | created by an attach; there is no management over AMQP 1.0 |

The prefix goes **inside** `VirtualTopic.` on Classic and the ordering was
settled by the broker rather than by reasoning: the default
virtual-destination interceptor matches `VirtualTopic.>`, so
`iqmq.VirtualTopic.orders` is an ordinary topic that accepts every publication
and writes to no consumer queue, silently. `internal/activemq`'s
`Config.Prefix` carries the three-line measurement.

## This driver creates no topology

The specification makes implicit topology creation a defect and on Artemis it is a defect
with teeth: a typo in a topic name becomes an address, the publications go
into it, and nothing anywhere reports a problem. So a sink or a source
**attaches** and never creates, and topology is `Client.Admin()`'s — which
only Artemis has.

Run Artemis with auto-creation **off** (`artemis create --no-autocreate`, or
`<auto-create-addresses>false`). With it off an attach to a destination that
does not exist is refused with `amqp:not-found` — "AMQ119010: source address
does not exist" for a receiver, "AMQ119002: target address … does not exist"
for a sender — which is what makes the claim provable rather than merely made.
Classic creates a destination on attach and cannot be told not to without an
authorization plugin.

## Settlement

| | AMQP 1.0 |
|---|---|
| `Ack` | `accepted` |
| `Nack(NackRequeue())` | `modified{delivery-failed}` |
| `Nack(NackRequeue(), NackDelay(d))` | refused: measured, the annotation is ignored on a disposition |
| `Nack(NackDrop())` | `rejected` |
| `Nack(NackDeadLetter())` | `rejected`, which reaches the broker's dead-letter address |
| a closing consumer's unsettled deliveries | `released`, which does not count the attempt |

`released` is deliberately **not** the requeue, and the reason is measured on
both products: a released delivery comes back with `delivery-count`
**unchanged** — 0, 0, 0 over three rounds — while `modified{delivery-failed}`
comes back at 0, 1, 2. The attempt has to rise across a nack or
`MaxAttempts` is compared against a number that never moves and a poison
message is retried for ever.

`undeliverable-here` is **not** set either: measured on Artemis, a delivery
modified with it is never redelivered to the consumer that modified it, so a
subscription with one consumer loses the message.

## Testing needs no broker

```sh
go test ./...                    # no container, no network
go test -race ./...
go test -run '^Fuzz' ./...       # FuzzActiveMQAnnotations, FuzzActiveMQDestinations
```

The scripted peer is the shared core's (`amqp10.NewPeer`,
`amqp10.BackendWithPeer`), which this increment added to the core because
three of the behaviours this driver has to get right cannot be provoked from a
container at all: a disposition sent on a link that has since detached, a
consumer cancelled while a fetch is parked, and a publication whose
disposition never arrives.
