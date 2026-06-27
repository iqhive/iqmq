// # What this package is, and what it deliberately is not
//
// It is one type: a [bitbucket.org/iqhive/iqmq/internal/streams.Commander] over
// go-redis's [github.com/redis/go-redis/v9.UniversalClient]. Everything that
// decides whether a message is lost or duplicated -- which key a topic's shard
// lives under, which fields an entry carries, which command a nack sends,
// whether an XACK is still the ack of the delivery it was handed, how a
// server's error reply becomes an [bitbucket.org/iqhive/iqmq.Error] -- is
// `internal/streams` and `drivers/redis`, shared with the rueidis driver
// character for character.
//
// That is the point rather than an economy. The specification asks for two Redis drivers
// "both implement[ing] a small internal command interface", and the value of
// two implementations of one contract is that they answer the same; two
// copies of four thousand lines would answer the same on the day they were
// written and then drift, and the copy that drifted would be the one nobody
// fuzzes. So this module's whole surface is the adapter, its configuration
// entry points, and the tests that hold its answers to the other client's.
//
// The cost is stated plainly: importing this package imports
// `drivers/redis`, which imports rueidis, so a program that wants go-redis
// alone still carries rueidis in its module graph.
//
// # Semantics
//
// Identical to `drivers/redis` by construction, including the confirmation
// boundary, the ack boundary and the at-least-once window,
// the driver-owned lease, the guarded acknowledgement, and negative
// settlement. Read
// [bitbucket.org/iqhive/iqmq/drivers/redis]'s package documentation for all of
// it; docs/drivers/goredis.md carries the specification's own sections and the
// client-library differences below.
//
// # What DOES differ, because the client library differs
//
//   - The connection model. rueidis multiplexes every command over one
//     auto-pipelined connection per node; go-redis takes a connection out of
//     a pool per command and pipelines only when asked. That is the whole
//     subject.
//   - Retries. go-redis retries a failed command by default, including one it
//     has already written, which on this broker turns an ambiguous XADD into
//     a duplicate. This driver turns that off.
//   - Error text. rueidis strips a leading "ERR " from every error reply and
//     go-redis does not, so the two clients hand the SAME server reply to the
//     error table under two spellings.
//
// # Maturity and tier
//
// Maturity: Experimental. Tier 2 (the driver documentation), inherited from the
// driver it is a client library for: complete against the conformance suite,
// with a report of its own, and not yet run in production.
package goredis
