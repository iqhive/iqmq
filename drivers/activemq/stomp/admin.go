package stomp

import (
	mq "github.com/iqhive/iqmq"
)

// Topology, and why this file is one refusal.
//
// STOMP has no topology frame. The protocol can SEND to a destination and
// SUBSCRIBE to one, and it cannot create one, delete one, purge one or report
// its depth: there is no frame for any of it, in any version of the
// specification. Both brokers do those things over interfaces of their own —
// Artemis's management address, Classic's JMX and its web console — and
// neither is reachable from a STOMP connection.
//
// So this backend deliberately does NOT satisfy [mq.AdminBackend], and
// `Client.Admin()` therefore reports ErrUnsupported rather than returning a
// surface that refuses every call. The core asserts the interface once when
// the client is built and records the answer, so a backend that satisfied it
// and then refused would make [mq.FeatureAdmin] look available to a caller who
// asked the client rather than the table.
//
// What creates topology for these brokers, in the order a deployment should
// prefer:
//
//  1. `drivers/activemq`'s Admin, against Artemis. It is the broker's own
//     management address over AMQP 1.0 and it creates the same names this
//     driver derives, because both derive them from `internal/activemq`.
//  2. On ActiveMQ Classic, a SUBSCRIBE. A consumer queue comes into existence
//     when a subscription attaches to it and the broker cannot be told not to
//     do that without an authorization plugin. It has to exist BEFORE the
//     first publication: a virtual topic copies a publication into the
//     consumer queues that exist AT THAT MOMENT, so a queue created later
//     gets nothing. `ConformanceHarness` does this explicitly, which is what
//     The specification asks of a driver that will not do it implicitly.
//  3. The broker's own configuration, which is what a production deployment
//     should be doing anyway.

// adminUnsupported is the answer, kept as a function so that the reason has
// somewhere to live and so that a test can assert the sentence rather than the
// absence.
//
// It is not wired to anything: the point is that nothing is.
func adminUnsupported() error { return errNoTopology }

// compile-time proof that this backend really does NOT carry the topology
// surface, which is the claim capabilities.go makes about FeatureAdmin. A
// build in which this compiles is a build in which `Client.Admin()` reports
// ErrUnsupported.
var _ = func() bool {
	var b any = (*Backend)(nil)
	_, carries := b.(mq.AdminBackend)
	return carries
}
