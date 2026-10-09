package amqp10

import (
	mq "github.com/iqhive/iqmq"
)

// Health is the backend's current state.
//
// It is answered without I/O, so a caller polling it never becomes load on the
// broker — which matters more here than on a request/response driver: this
// driver holds one long-lived connection, and "is it up" is a question a
// deployment asks continuously.
//
// The state machine is
// [github.com/iqhive/iqmq/driverkit.Health]'s and it is driven by the dial loop
// and by nothing else:
//
//   - Configured is the instant between construction and the first dial, which
//     a caller of [Open] never observes because Open dials before it returns.
//   - Connecting is a dial in flight, which after the first one means a
//     reconnection: the supervisor reports it before every attempt.
//   - Ready is a connection that exists. It says nothing about a link: a source
//     whose attach the broker refuses reports that through its own Fetch, and a
//     health that went Degraded for it would make one bad subscription look
//     like a broken client.
//   - Degraded is a connection that died, for as long as the dial loop is
//     trying to replace it — and terminally when [Config.NoReconnect] is set or
//     [Config.ReconnectAttempts] has been reached, which is the state a caller
//     polling health is looking for.
//   - Closed is terminal. A driver whose loop reported Ready after the client
//     was closed would hand a caller a client it can poll for ever, which is
//     the one rule driverkit's Health enforces for every driver.
//
// The reporter is deliberately NOT given an onChange callback, and that is the
// gap recorded in the maintenance notes rather than a decision: [mq.Hooks] has no
// observation point for a health transition, so there is nowhere for one to go.
// What a caller gets instead is [mq.Hooks.OnReconnect] for every dial attempt,
// which covers the transitions this driver actually makes, and this snapshot for
// the state.
func (b *Backend) Health() mq.HealthSnapshot {
	if b.BaseBackend == nil {
		// A backend whose construction failed before the base existed. Its
		// reporter is the only thing that knows anything, and answering from
		// it is what makes a failed Open's health readable.
		return b.health.Snapshot()
	}
	return b.BaseBackend.Health()
}
