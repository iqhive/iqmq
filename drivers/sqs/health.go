package sqs

import (
	mq "bitbucket.org/iqhive/iqmq"
	"bitbucket.org/iqhive/iqmq/driverkit"
)

// Health is the backend's current state.
//
// It is answered without I/O, so a caller polling it never becomes load on the
// service — which matters more here than on a broker with a connection: every
// SQS operation is a priced request, so a Health that called the service would
// turn a liveness dashboard into a bill.
//
// The states this driver reaches, and what each one means:
//
//	Configured  the client exists and has made no request yet. It is where
//	            a backend starts and where it stays only briefly, because
//	            there is no connection to establish.
//	Ready       the last background poll succeeded, or none has failed.
//	Degraded    a poller's ReceiveMessage failed, with the failure. It is
//	            not terminal: the next round retries, and a success clears
//	            it. A queue that has been deleted, or a credential that has
//	            expired, shows up here and nowhere else.
//	Closed      terminal.
//
// There is no Connecting state and no reconnection, because SQS is
// request/response over HTTP: there is no link to lose. That is also why
// [bitbucket.org/iqhive/iqmq.Hooks.OnReconnect] is never fired — firing it for a
// poll that failed would report an event that did not happen — and it is why a
// caller who wants to know a poller is failing has to read Health. The specification asks
// for "hooks and health"; the SPI's hook set has no observation point for a
// health transition, which is recorded in the maintenance notes.
func (b *Backend) Health() mq.HealthSnapshot { return b.BaseBackend.Health() }

// degraded records that a background poll failed (the specification's "driver background
// failures surface via hooks and health, not only logs").
func (b *Backend) degraded(err error) {
	if b.health != nil {
		b.health.Degraded(err)
	}
}

// ready records that a background poll succeeded, clearing a previous
// failure.
//
// The state is checked first so that a healthy poller does not fire the
// health reporter's onChange on every round: a source with four pollers and a
// twenty-second long poll would otherwise call it twelve times a minute for
// nothing.
func (b *Backend) ready() {
	if b.health != nil && b.health.State() != mq.HealthReady {
		b.health.Ready()
	}
}

// compile-time proof that the health reporter is the kit's.
var _ = driverkit.NewHealth
