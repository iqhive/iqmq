// Package inbox records that a message was processed, in the same transaction
// as the side effect.
//
// It is the consumer half of the inbox/outbox pattern and the one way
// at-least-once DELIVERY becomes effectively-once PROCESSING on every broker.
// The whole idea is one sentence: the record that you handled a message and
// the effect of handling it are one database transaction, so they commit or
// roll back together and there is no window in which one exists without the
// other.
//
//	BEGIN
//	  INSERT INTO inbox (message_id) VALUES ($1)   -- fails on a duplicate
//	  …the side effect…
//	COMMIT
//
// # How this differs from x/idempotency, and why it matters
//
// The two packages look alike and are not interchangeable.
// [github.com/iqhive/iqmq/x/idempotency] takes a two-phase claim in a
// SEPARATE store — a Redis, or an in-process LRU — so the claim and the work
// are two systems with no transaction across them. It therefore REDUCES a
// duplicate rate that was never zero, and its own documentation says so.
// This package puts the record in the SAME transaction as the effect, so
// there is no window at all — for effects in that one database.
//
// That last clause is the whole condition, and this package cannot enforce
// it. [Store.Once] hands your function a context and runs it inside the
// store's transaction; if your function's side effect goes to a different
// database, or to an HTTP API, or to another broker, then the record and the
// effect are two systems again and you have bought a slower
// [github.com/iqhive/iqmq/x/idempotency]. The specification records that the
// interface admits the mistake and that the documentation is the only thing
// standing in its way.
//
// # What a duplicate does
//
// A message whose key is already recorded is ACKED without running the
// handler, and counted in [Counts.Duplicate]. That is the correct answer: the
// work has been done, the transaction that did it committed, and running the
// handler again is the duplicate this package exists to prevent. The
// middleware acks it itself, so a duplicate is settled under
// mq.SettlementManual too, where nothing else would settle a delivery the
// handler never saw.
//
// # A handler that settles its own delivery
//
// A handler that nacks or rejects its own delivery and returns nil has asked
// for the message again, so its transaction is rolled back -- the record and
// whatever the handler wrote with it -- and the redelivery runs the handler
// again. One that acks its own delivery, or leaves the settlement to the
// runner, commits.
//
// # The key
//
// [Config.Key] has no default, and the reason is the one failure in this
// package that can LOSE work: two distinct messages that derive one key make
// the second a false duplicate, which is acked without being handled. So the
// caller names the key, and the [KeyFunc] shapes come from
// [github.com/iqhive/iqmq/x/idempotency] rather than being declared again
// here — the key-derivation problem is identical, and two copies of
// KeyFromHeader that could drift apart would be worse than the coupling.
package inbox
