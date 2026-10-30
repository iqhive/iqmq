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
	// queued holds the transitions not yet reported to onChange, in the
	// order they were made, and reporting says a goroutine is reporting
	// them.
	queued    []mq.HealthSnapshot
	reporting bool
}

// NewHealth returns a health reporter in the Configured state — the client
// exists and has not connected — calling onChange after every transition that
// changes the state. onChange runs outside the lock and must not block.
//
// onChange is called one transition at a time, in the order the transitions
// were made, so Closed is the last it hears. A transition made while another
// is being reported, by another goroutine or by onChange itself, is reported
// after it by the goroutine already reporting, and its own caller returns
// without waiting. Each caller used to call onChange for its own transition,
// so a Close made while a Ready was being reported could be reported first.
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
	if !changed || h.onChange == nil {
		h.mu.Unlock()
		return
	}
	h.queued = append(h.queued, mq.HealthSnapshot{State: state, Err: err})
	if h.reporting {
		h.mu.Unlock()
		return
	}
	h.reporting = true
	h.mu.Unlock()
	h.report()
}

// report calls onChange with each queued transition in turn until none is
// left. One goroutine at a time reports, which is what keeps the order.
//
// A panic in onChange still reports what is queued behind it before the panic
// goes on to the caller, each of those reports on the same terms. Dropping the
// queue lost a Close a hook made before it panicked, and Closed is terminal,
// so no later transition would have reported it.
func (h *Health) report() {
	done := false
	defer func() {
		if !done {
			h.report()
		}
	}()
	for {
		h.mu.Lock()
		if len(h.queued) == 0 {
			h.queued, h.reporting = nil, false
			h.mu.Unlock()
			done = true
			return
		}
		snap := h.queued[0]
		h.queued = h.queued[1:]
		h.mu.Unlock()
		h.onChange(snap)
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
