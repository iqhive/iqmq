# Metadata modes

Specification. Some brokers have no headers at all (NSQ), some restrict
values to strings (SQS attributes, Pub/Sub attributes), some cap the count
(SQS: ten). Your `mq.Headers` do not change shape because of that, so
*something* has to give, and the answer is that **you choose which**.

`mq.PublisherConfig.Metadata` selects it:

| Mode | What it does when the broker cannot carry the metadata |
|---|---|
| `mq.MetadataNativeStrict` | refuses, with `ErrUnsupported` or `ErrMessageTooLarge`, **before any I/O**. The zero value. |
| `mq.MetadataPortableEnvelope` | always wraps: body and headers go into a self-describing envelope in the broker's payload field |
| `mq.MetadataAdaptive` | uses native fields where they fit, and wraps only the messages that do not |

## Why strict is the default

Because the alternative is a driver that silently changes your wire format.
An envelope is bytes in the body: a non-iqmq consumer reading that topic sees
a framed blob instead of your JSON. That may be exactly what you want, and it
must never be what you get by accident — so the mode that can do it is opt-in
and the mode that refuses is the zero value.

This is the same principle as `ConfirmDurable` failing at construction: a
refusal you can see beats an emulation you cannot.

## What the envelope is

A small, deterministic, versioned framing in `wire`: a magic prefix, a version,
the headers in their portable encoding, and the body. It round-trips headers
exactly — order, duplicates, binary values, the lot — through a broker that has
none.

Two things about it are worth knowing before you turn it on:

**It is visible to everyone else on that topic.** A consumer that is not this
library sees the framing. If the topic has other readers, the envelope is a
breaking change to them.

**Adaptive mode has to sniff.** A message arrives and the driver has to decide
whether its body is an envelope or a native payload that happens to begin with
the same bytes. The magic prefix makes that cheap and not free: a native body
that starts with the envelope's magic was once mis-decoded, which is a fixed
defect with a committed regression input and a fuzz target — see
[fuzzing](../testing/fuzzing.md). If your bodies are arbitrary binary and you
need certainty rather than a very good guess, `MetadataPortableEnvelope`
removes the question by never sniffing.

## Which drivers need it

`drivers/nsq` has no headers at all, so portable metadata means the envelope
there and the driver says so. The attribute-limited brokers — `drivers/sqs`,
`drivers/snssqs`, `drivers/gcppubsub` — need it for **binary** values, for
repeated keys, and for values over the broker's per-value cap.

## Headers themselves

`mq.Headers` is ordered, duplicate-preserving, binary-capable and
case-sensitive with a lowercase-kebab convention. `Set` replaces
(last-write-wins, which is the portable contract); `Add` and `Values` are the
multi-valued path and need `FeatureDuplicateHeaders` — a driver that cannot
represent duplicates rejects at publish rather than dropping one.

`iqmq-` is reserved (`mq.ReservedHeaderPrefix`) and a conflict is rejected. The
runner is the only writer of that prefix, and it writes exactly the five
dead-letter provenance headers.

Received binary values are surfaced **zero-copy**, because received headers are
read-only. That is the specification's ownership contract: the storage is borrowed until
the delivery is settled, and `Delivery.CloneMessage` is how you keep something
past that point.
