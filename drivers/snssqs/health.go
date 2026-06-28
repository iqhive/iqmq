package snssqs

import (
	"bitbucket.org/iqhive/iqmq/driverkit"
)

// The health state machine is `driverkit.Health`, and this driver's states are
// the SQS driver's for one reason: every piece of background work in a
// fan-out client is a poller, and the pollers are the inner driver's.
//
// The states reached, and what each one means:
//
//	Configured  the clients exist and have made no request yet. It is where
//	            a backend starts and where it stays only briefly, because
//	            there is no connection to establish.
//	Ready       nothing has failed.
//	Degraded    a poller's ReceiveMessage failed, or this driver's own
//	            construction did, with the failure. It is not terminal: the
//	            next round retries, and a success clears it.
//	Closed      terminal.
//
// There is no Connecting state and no reconnection, because both services are
// request/response over HTTP: there is no link to lose. That is also why
// [bitbucket.org/iqhive/iqmq.Hooks.OnReconnect] is never fired — firing it for a
// poll that failed would report an event that did not happen — and it is why a
// caller who wants to know a poller is failing has to read
// [Backend.Health]. The specification asks for "hooks and health"; the SPI's hook set has
// no observation point for a health transition, which is recorded in
// the maintenance notes and is not this increment's to fix.
//
// [Backend.Health] itself is in backend.go, because what it does is combine
// the two halves and that is a fact about the composition rather than about
// health.
var _ = driverkit.NewHealth
