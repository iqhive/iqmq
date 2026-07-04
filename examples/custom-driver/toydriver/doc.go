// Package toydriver is a complete iqmq driver written from outside the
// repository, against the published SPI and nothing else.
//
// It exists to answer one question that no test inside the repository can:
// **can a third party write a driver for iqmq using only its public API?**
// Every driver in the repository can reach into internal/engine,
// internal/errcode and internal/clock; this module has a go.mod of its own and
// imports exactly three iqmq packages — bitbucket.org/iqhive/iqmq for the SPI,
// bitbucket.org/iqhive/iqmq/driverkit for the toolbox, and (in its tests)
// bitbucket.org/iqhive/iqmq/testkit for the conformance suite. If something a
// driver needs is only reachable from inside, this package cannot compile, and
// that is the finding.
//
// # What the broker is
//
// A queue per subscription, in memory, with leases. It is deliberately NOT the
// memory driver written again: it is the shape most network brokers have —
// string-valued single-valued headers, no retention, no delayed redelivery, no
// admin API — because the interesting question is what the SPI asks of a driver
// that CANNOT do most of what iqmq's API can express. Everything it does not
// have is advertised as unsupported and refused, which the conformance kit
// checks in both directions: a driver that quietly does what it says it cannot
// fails the refusal.
//
//   - Topics are created by [New] and by nothing else, which is the rule.
//     Publishing to a topic nobody created is CodeNotFound.
//   - Subscriptions are created on demand by [Broker.OpenSource] and outlive
//     their consumers, which is what FeatureDurableSubscriptions means — here,
//     volatilely, because a process restart forgets everything.
//   - A publication is copied into every subscription of the topic (fan-out),
//     and the consumers of one subscription compete for it (groups).
//   - Headers are UTF-8 and single-valued, like SQS attributes or Pub/Sub
//     attributes. Binary and duplicate headers are refused under the strict
//     mode and carried by iqmq's own envelope under the portable mode, which is
//     what [driverkit.Envelope] is for.
//   - The clock is manual: [Broker.Advance] moves it and runs the sweeps the
//     move implies. A network driver would use the wall clock and a conformance
//     run against it would really wait.
//
// # What it is not
//
// It is not durable, not distributed and not fast. It holds every message in a
// slice under one mutex, and one mutex for a whole broker is exactly what
// the memory engine tells a real driver not to do. Nothing here is a model for
// an implementation that has to perform; it is a model for an implementation
// that has to be CORRECT about the contract, which is the part a driver author
// gets wrong first.
package toydriver
