// Package outbox publishes messages your database transaction committed
//
// It is the publisher half of the inbox/outbox pattern. Write the message
// into your own database in the transaction that produced it; a relay reads
// the unsent rows, publishes them, and marks them sent.
//
//	BEGIN
//	  INSERT INTO orders …
//	  INSERT INTO outbox (topic, key, headers, body) VALUES …
//	COMMIT
//	-- then, separately: this package
//
// What it buys is that the order and the intent-to-publish commit or roll
// back together, so "the order exists and nobody was told" and "somebody was
// told about an order that was rolled back" both become impossible. What it
// costs is at-least-once publication, which you had anyway; a relay to run;
// and ordering that is the relay's rather than your application's.
//
// # There is no Append on the Store interface
//
// That is the design, not a gap. The append happens inside YOUR transaction,
// with your own handle — a `*sql.Tx`, a pgx transaction, a GORM session — and
// an interface that took one of those would bind this package to that one
// library. So the [Store] interface is the RELAY's half only: claim, mark
// sent, release. The append is an INSERT you write, and [Row] plus
// [EncodeHeaders] and [DecodeHeaders] define the column shape so that the
// round trip is specified rather than improvised.
//
// `examples/outbox-postgres` is the worked Postgres version, with
// `FOR UPDATE SKIP LOCKED` and a real container.
//
// # The two rules the relay must not break
//
// Both are about the same thing — a row must never be marked sent unless it
// was really published — and both have a test named after them.
//
//  1. **An ambiguous publication does not mark the row sent.** the specification's
//     ambiguous outcome is a publication whose acceptance is unknown.
//     Marking it sent turns "we do not know" into "we are done", which is
//     the only way this pattern can LOSE a message. Leaving it unsent
//     produces a duplicate, which is what at-least-once already promised.
//     [TestAnAmbiguousPublishLeavesTheRowUnsent] holds it.
//  2. **Only rows that were published are marked.** A batch in which the
//     third row fails marks the first two and releases the rest. Marking
//     the whole claim because the call returned is how a partial failure
//     loses everything after the failure point.
//     [TestAPartialBatchMarksOnlyWhatWasPublished] holds it.
//
// # Ordering
//
// One relay with [Config.Concurrency] of 1 — the default — publishes in row
// order. Anything above 1 reorders, and this package does not pretend
// otherwise: two workers claiming disjoint rows will finish in whatever order
// the broker acknowledges them. A deployment that needs per-key order and
// throughput partitions the claim by key hash, which is a Store concern
// because it is a WHERE clause.
package outbox
