package driverkit

import (
	"sync"

	mq "github.com/iqhive/iqmq"
)

// Health is the health state machine, with the one rule that makes a
// health report worth reading: Closed is terminal.
//
// A driver whose background loop reports Ready after the client was closed hands
// a caller a client it can poll for ever. The transitions here are the five
// states the specification names, and every transition out of Closed is refused.
//
// Health is answered without I/O, so a caller polling it never becomes load on
// the broker. Every method is safe for concurrent use.
type Health struct {
	mu       sync.RWMutex
	state    mq.HealthState
	err      error
	onChange func(mq.HealthSnapshot)
}

// NewHealth returns a health reporter in the Configured state — the client
// exists and has not connected — calling onChange after every transition that
// changes the state. onChange runs outside the lock and must not block.
func NewHealth(onChange func(mq.HealthSnapshot)) *Health {
	return &Health{state: mq.HealthConfigured, onChange: onChange}
}

// Connecting records that a connection is being established.
func (h *Health) Connecting() { h.to(mq.HealthConnecting, nil) }

// Ready records that the client is usable. It clears the error: a client that
// is Ready has nothing wrong with it, and a stale error left behind would be
// reported for ever.
func (h *Health) Ready() { h.to(mq.HealthReady, nil) }

// Degraded records that the client is usable but something is wrong, or is not
// usable and may recover, with the reason. The specification forbids a payload, a credential
// or an address in the error, which is the driver's responsibility here.
func (h *Health) Degraded(err error) { h.to(mq.HealthDegraded, err) }

// Close records that the client is closed. It is terminal and idempotent.
func (h *Health) Close() { h.to(mq.HealthClosed, nil) }

// to applies a transition unless the reporter is closed.
func (h *Health) to(state mq.HealthState, err error) {
	h.mu.Lock()
	if h.state == mq.HealthClosed {
		h.mu.Unlock()
		return
	}
	changed := h.state != state || !sameErr(h.err, err)
	h.state, h.err = state, err
	snap := mq.HealthSnapshot{State: state, Err: err}
	fn := h.onChange
	h.mu.Unlock()
	if changed && fn != nil {
		fn(snap)
	}
}

// Snapshot is the current state.
func (h *Health) Snapshot() mq.HealthSnapshot {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return mq.HealthSnapshot{State: h.state, Err: h.err}
}

// State is the current state alone.
func (h *Health) State() mq.HealthState {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.state
}

// Closed reports whether the reporter has reached its terminal state.
func (h *Health) Closed() bool { return h.State() == mq.HealthClosed }

// sameErr compares two errors by identity, which is all a transition needs: a
// re-reported identical failure is not a change.
func sameErr(a, b error) bool { return a == b } //nolint:errorlint // identity is the comparison a transition wants
