# Getting started

Three pages, in order:

1. **[Quick start](quickstart.md)** — publish and consume one message against
   the in-process engine, with no broker and no container. About ten lines.
2. **[Installation](installation.md)** — which module you need for which
   broker, and why there is one per broker.
3. Then either [Concepts](../concepts/index.md), if you want to know what the
   words mean before you rely on them, or your
   [driver page](../drivers/index.md), if you already know your broker and want
   to know what it does about acks, ordering, delays and dead letters.

!!! warning "Pre-alpha"
    Nothing here is ready for production use. The repository is being built
    increment by increment against the specification, and
    Status is the honest list of what works, what is measured
    and what is open. No performance, delivery-guarantee or compatibility
    claim is made here without the evidence behind it.

## The shape of the API, in one paragraph

You open a `*mq.Client` — from a driver's `Open`, or from a DSN. You ask it
for a `*mq.PublisherHandle` bound to one topic, or publish ad hoc with
`client.Publish`. You ask it for a `*mq.ConsumerHandle` bound to a topic and a
subscription, and then either run a handler with `consumer.Run(ctx, handler)`
and let the runner settle for you, or call `consumer.Receive` and settle each
`*mq.Delivery` yourself. Everything else — batching, ordering, retries,
dead-lettering, lease extension, middleware, typed codecs, request/reply — is
an option or a wrapper over those five things.

## The four things to read before you ship

* **[Delivery guarantees](../concepts/delivery-guarantees.md)** — what an ack
  means, and why exactly-once *processing* is not on offer.
* **[Capability negotiation](../concepts/capabilities.md)** — how to find out
  what your broker can do, at construction, instead of at three in the
  morning.
* **[Confirmation boundaries](../concepts/confirmation.md)** — what `nil` from
  `Publish` does and does not tell you, per broker.
* **Your driver page's *Known gotchas* and *At-least-once window* sections.**
  Every driver has both, and they are where the surprises are written down.
