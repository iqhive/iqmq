// Package claimcheck moves a large payload to external storage and publishes
// a reference to it.
//
// It is the answer to [mq.ErrMessageTooLarge]: every broker has a maximum
// message size — 256 KiB or 1 MiB on SQS, 5 MiB on Pulsar, 1 MiB by default
// on Kafka — and a payload over it cannot be published at all. The pattern is
// to put the payload somewhere addressable, publish the address, and have the
// consumer fetch it. The refusal that sends a caller here is local and cheap:
// the core compares against the driver's advertised `MaxMessageBytes` before
// any I/O, so discovering the limit costs no round trip.
//
// # The reference
//
// A checked message's body is replaced by a reference:
//
//	magic     4 bytes  "IQK1"
//	flags     1 byte   reserved; a non-zero value is refused, not ignored
//	digest    1 byte   the digest algorithm (0 none, 1 SHA-256)
//	size      8 bytes  big-endian uint64, the payload's length
//	storeLen  2 bytes  big-endian uint16
//	keyLen    2 bytes  big-endian uint16
//	store     storeLen bytes  which store holds it
//	key       keyLen bytes    where in that store
//	digest    the algorithm's digest length, or nothing
//
// [ParseRef] reads it without allocating. As with every size field in this
// repository, `size` is a CLAIM and never an allocation: it is compared with
// [Config.MaxPayload] before the store is called, and the fetch is bounded
// again by a limited reader, which is the rule the two 64 GiB codecs
// established.
//
// The digest is not decoration. The payload arrives from an external store,
// so it is remote input: a store that returns the wrong object — a key
// collision, a stale replica, an operator restoring a backup over it, an
// attacker who can write to the bucket but not to the broker — hands the
// handler a body the publisher never sent. Verifying it against a digest
// recorded at publish time and carried in the reference is what makes that
// detectable, and it is on by default.
//
// # What happens when the store is unreachable
//
// The specification requires this to be stated rather than discovered, and the answers are
// deliberately asymmetric. The specification records them.
//
//   - **Unreachable while publishing: the publication fails.** Nothing is
//     published. The alternative — publish the reference anyway and hope the
//     upload lands later — produces a message that can never be handled, and
//     a consumer cannot tell it from a payload that was deleted. Fail closed.
//   - **Unreachable while consuming: the delivery fails and is NOT acked.**
//     [ErrStoreUnavailable] carries CodeUnavailable, so the runner retries
//     it. Acking a message whose payload could not be fetched is silent data
//     loss, and it is the one thing this package must never do.
//   - **The payload is GONE — the store answered, and said no such object.**
//     That is [ErrPayloadGone], CodePoisonMessage, which is PERMANENT, and
//     [CheckOut] returns it wrapped in [mq.DeadLetter] so the runner does
//     not spend its retries and their backoff on an answer that cannot
//     change: on the FIRST attempt the handler does not run and the
//     failure policy decides — the message is dead-lettered when a
//     dead-letter topic is configured, and the runner halts with a
//     *[mq.PoisonError] when none is (ADR-011). The dead-lettered message
//     carries the reference as its body and ErrPayloadGone as its failure
//     header, so which payload went missing is diagnosable. The specification records the choice and gone_test.go proves it through a real
//     runner.
//
// # Can a claim be settled while its payload is gone?
//
// Yes, and that is the sharp edge, so here is the rule: **the consumer never
// deletes the payload.** Deletion is the store's own lifecycle policy — an S3
// lifecycle rule, a GCS TTL — set longer than the broker's retention plus its
// longest redelivery window.
//
// The rejected alternative is delete-on-ack, which looks obviously right and
// breaks at-least-once delivery. The substrate redelivers: a lease that
// expires after the handler succeeded, a rebalance, a consumer restart, an
// operator replaying a day. Every one of those produces a second delivery of
// a message whose payload the first delivery has just deleted, and that
// second delivery is then dead-lettered as [ErrPayloadGone] — a message that
// was processed successfully, reported as poison. Delete-on-dead-letter is
// worse still: the dead-letter consumer is the reader that needs the payload
// most.
//
// So a settled claim may outlive its payload, and a redelivery after the
// store's TTL is [ErrPayloadGone]. That is a bounded, visible failure with a
// knob — the TTL — rather than a silent one, and it is the cost of the
// pattern rather than a defect in this implementation.
//
// # The stores
//
// [MemoryStore] is the test double. [FileStore] is a real store over a
// directory — atomic write-then-rename, a bound on total bytes that refuses
// rather than evicts, and a TTL sweep that is its lifecycle policy — for a
// single host or a shared POSIX filesystem. An S3-compatible store lives in a
// module of its own, kept out of this module
// so the root stays free of dependencies. Every one of them, and any store a
// caller writes, runs the contract in
// [bitbucket.org/iqhive/iqmq/x/claimcheck/storetest].
package claimcheck
