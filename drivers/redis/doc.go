// # What this broker is, in iqmq's terms
//
// A topic is one or more Redis streams, hash-tagged so that every shard of a
// topic lands on one cluster slot; a subscription is a consumer group; a
// delivery is a pending-entries-list row. Publication is XADD, consumption is
// XREADGROUP, and settlement is XACK.
//
// # The confirmation boundary
//
//	ConfirmNone      the XADD is written; its reply is not waited for
//	ConfirmAccepted  XADD's reply: the entry is in the server's memory and
//	                 will be served to a reader. Nothing about disk is implied
//	ConfirmDurable   WAITAOF: the write has reached the number of local and
//	                 replica append-only files the configuration names
//
// ConfirmDurable is CONDITIONAL and both conditions are stated in the
// capability table: WAITAOF needs Redis 7.0 or later, and it needs the server
// to keep an append-only file. A publisher asking for it on a server that
// cannot is refused at CONSTRUCTION, not at its first message — the rule,
// and the difference between an application that fails to start and one that
// has already told a user their data is safe.
//
// # The ack boundary and the at-least-once window
//
// A delivery is settled when XACK removes its pending-entries row. Between
// XREADGROUP and XACK the message is at-least-once: a consumer that dies
// leaves the row pending, and the reclaim loop hands it to another consumer
// once it has been idle for the acknowledgement deadline.
//
// Two things about that are unusual enough to state here rather than leave to
// the driver page.
//
// The lease is THIS DRIVER'S, not the broker's. Redis grants no lease at all:
// nothing expires, nothing redelivers, and a consumer that died holding a
// hundred entries holds them until something claims them away. The mandatory
// XAUTOCLAIM reclaim loop is that something, and a Redis Streams driver
// without it has an at-least-once claim that is false.
//
// XACK does not check who holds the entry. `XACK key group id` removes the
// pending row whoever owns it, so a driver that sends one for a delivery
// another consumer has reclaimed acknowledges that consumer's work — and if
// that consumer then dies, the message is lost. This driver closes the window
// in two places: a per-source generation table refuses a superseded
// settlement with no command sent, and a Lua-guarded XACK checks the pending
// row's consumer name atomically with the acknowledgement. The consumer name
// is unique per source instance, which is what makes the second check decide
// anything. See [github.com/iqhive/iqmq/internal/streams.Reclaimer].
//
// # Negative settlement
//
// Redis Streams has no nack: an entry is pending or acknowledged and nothing
// else. Four strategies build one, and which is bound is [Config.Nack] —
// leave-pending (the default), a Lua zset mover for arbitrary delays, an
// immediate re-add, and a shard-holding stop. What each costs and what each
// preserves is in [github.com/iqhive/iqmq/internal/streams] and in
// docs/drivers/redis.md.
//
// # Maturity and tier
//
// Maturity: Experimental. Tier 2 (the driver documentation): the driver is complete
// against the conformance suite and has a broker-version matrix, and it has
// not yet run in production.
package redis
