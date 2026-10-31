// Package nsq is the NSQ driver, over go-nsq.
//
// It wraps [github.com/nsqio/go-nsq]: one `*nsq.Producer` per nsqd endpoint for
// publication, one `*nsq.Consumer` per subscription for consumption, and
// nsqlookupd for discovery.
//
// # What NSQ is
//
// A topic is a topic and a subscription is a CHANNEL. Every channel of a topic
// receives a copy of every message (fan-out), and every consumer of one channel
// competes for its messages (a consumer group). Both are created on first use
// by nsqd, which is why [Config.EnsureTopology] exists and why this driver's
// Admin surface is the only place topology is created deliberately.
//
// NSQ is at-least-once and has no ordering of any kind. A message is redelivered
// when its per-message timeout expires or when it is requeued. A connection
// that goes away does not bring its messages back sooner: nsqd keeps each one
// in flight until its timeout. nsqd also makes no promise about the order in
// which a channel's messages are delivered, not even for one producer, one
// consumer and no failures. This driver therefore claims no ordering scope at
// all; see docs/drivers/nsq.md.
//
// # The confirmation boundary
//
// `PUB`, `MPUB` and `DPUB` are answered by nsqd with `OK` once the message is
// in the topic's queue. That queue is in memory up to nsqd's
// `--mem-queue-size` and on disk beyond it, so `OK` is an ACCEPTANCE and not a
// durable boundary: ConfirmAccepted is honoured and ConfirmDurable is refused
// when the sink is opened. The boundary for this driver is "nsqd has taken
// the message into the topic it names", which survives a consumer crash and
// does not survive an nsqd crash with an in-memory queue.
//
// # Metadata: the envelope is the only path
//
// NSQ has no headers. Not a limited header set, not string-only values — the
// protocol carries a body and nothing else. So the promise of ordered,
// duplicate-preserving, binary-capable headers is kept here by the portable
// envelope of [github.com/iqhive/iqmq/wire] and by nothing else, and the specification's
// four modes reduce to three real behaviours:
//
//   - MetadataNativeStrict, the default, refuses a message carrying any
//     metadata at all — a header, an identifier or a typed property — before
//     any I/O, with ErrUnsupported.
//   - MetadataPortableEnvelope envelopes every message. A non-iqmq consumer
//     then reads the envelope rather than the body, which is the documented
//     interoperability cost.
//   - MetadataAdaptive envelopes exactly the messages that carry metadata, and
//     leaves a bare body alone. It is the mode a mixed deployment wants, and
//     the mode whose discrimination is hardest: with nothing out of band, the
//     body is the only evidence there is (see [Config.Metadata] and
//     docs/drivers/nsq.md).
//   - MetadataLenientDrop drops the metadata and publishes the body.
//
// A message the envelope cannot carry cannot be published at all, so the
// refusal happens before any I/O and names the field and the bound it exceeded.
//
// # Settlement
//
// `FIN` is an acknowledgement, `REQ` a negative settlement (with a delay, which
// is nsqd's own delayed redelivery) and `TOUCH` a lease extension. None of the
// three is answered by nsqd: go-nsq writes the frame and the protocol has no
// reply. So a settlement's outcome is what this driver knows locally, and the
// lease is tracked locally for the same reason SQS tracks its visibility window
// locally — nsqd will accept a `FIN` for a message whose timeout has expired
// and which another consumer now holds. See [Config.LeaseGuard].
//
// A `TOUCH` is NOT a settlement. It moves the deadline and leaves the delivery
// settleable, and a `TOUCH` for a delivery this consumer no longer holds is
// refused with ErrLeaseLost rather than written and forgotten. The one case the
// driver cannot see is a connection that has dropped: go-nsq discards a
// `TOUCH` for a delivery on a closing connection and says nothing, so Extend
// reports success, nsqd keeps the original timeout, and the message is
// redelivered when it expires.
//
// # go-nsq's own retry policy is off
//
// `Config.MaxAttempts` is zero and `Config.MaxBackoffDuration` is zero, and
// every delivery is marked `DisableAutoResponse`. That is not tuning: go-nsq's
// defaults would `FIN` a message whose `Attempts` passed `MaxAttempts`
// (a silent discard of a message the core was still retrying), would requeue a
// message on a handler error with a backoff of its own, and would stop reading
// from the connection while that backoff ran. The specification gives redelivery accounting
// to exactly one layer and that layer is the core.
//
// # Opening
//
//	c, err := nsq.Open(ctx, nsq.Config{
//		Addrs:        []string{"127.0.0.1:4150"},
//		LookupdAddrs: []string{"http://127.0.0.1:4161"},
//	})
//
// or from a DSN, `nsq://127.0.0.1:4150?lookupd=http://127.0.0.1:4161`
// ([ParseDSN]).
//
// docs/drivers/nsq.md carries the fixed per-driver sections the specification requires,
// including the ordering statement delegates to each driver page and the
// full table of what maps to what.
//
// Maturity: experimental. Tier 3.
package nsq
