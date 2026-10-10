# Concepts

The semantic model of specification, one page per concept. Read them in this
order the first time: each one assumes the ones above it.

| Page | The question it answers |
|---|---|
| [Delivery guarantees](delivery-guarantees.md) | What does an ack mean, and what does it not mean? |
| [Duplicates and idempotency](idempotency.md) | Why will I see the same message twice, and what do I do about it? |
| [Acks and leases](acks-and-leases.md) | What happens between "received" and "settled"? |
| [Groups and subscriptions](subscriptions.md) | Who gets which message? |
| [Backpressure](backpressure.md) | What happens when a producer outruns a broker? |
| [Confirmation boundaries](confirmation.md) | When `Publish` returns `nil`, what has actually happened? |
| [Ambiguous publishes](ambiguous-publishes.md) | What if nobody knows whether it was accepted? |
| [Capability negotiation](capabilities.md) | How do I find out what this broker can do, before I depend on it? |

## The one idea underneath all of them

A portable messaging API has exactly two honest designs. It can offer the
**intersection** of what every broker does, which is small and useless; or it
can offer the **union** and lie about the brokers that cannot do it.

This library takes a third route and pays for it in surface area: the API is
the union, and every member of it is **guarded by a capability the driver
either advertises or refuses**. Asking for something a broker cannot do fails
at construction or before any I/O, with `ErrUnsupported` and a message that
names the feature — never silently, and never with an emulation you did not
ask for.

That choice is why these pages keep saying "which the driver page states".
A concept page here tells you what the word means; the
[driver page](../drivers/index.md) tells you what your broker does about it,
and the capability matrix tells you which of those
statements a probe has actually verified.

!!! warning "Pre-alpha, and the docs say where the gaps are"
    This is not a finished library, and these pages are not marketing. Where a
    guarantee is unproven, unprobed or breached, the page says so and links the
    evidence. Status is the list of what is open.
