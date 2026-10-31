package kafka

import (
	"context"
	"io"
	"os"
	"syscall"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit"
)

// hooked is a backend with nothing but its health reporter, Ready, and the
// client hooks that drive it.
func hooked() (*Backend, *clientHooks) {
	b := &Backend{errs: newErrorMapper()}
	b.health = driverkit.NewHealth(b.onHealth)
	b.health.Ready()
	return b, &clientHooks{b: b}
}

// disconnect runs h as franz-go does when it closes a connection: every hook
// that takes OnBrokerDisconnect is given it.
func disconnect(h kgo.Hook, meta kgo.BrokerMetadata) {
	if d, ok := h.(kgo.HookBrokerDisconnect); ok {
		d.OnBrokerDisconnect(meta, nil)
	}
}

// TestAnIdleConnectionsReapIsNotALostLink: franz-go closes a connection that
// has been idle for its ConnIdleTimeout, thirty seconds by default, and runs
// OnBrokerDisconnect for it, as it does for every connection it closes. The
// hook read every disconnect as a lost link, and only a new connection set
// Ready back. The seed connection Open's Ping used goes idle once the client
// has learned the brokers, so a client that published successfully the whole
// time reported Degraded from its first reap on, and the next connection it
// dialled counted a reconnection that never happened.
func TestAnIdleConnectionsReapIsNotALostLink(t *testing.T) {
	b, h := hooked()
	seed := kgo.BrokerMetadata{NodeID: -1}
	broker := kgo.BrokerMetadata{NodeID: 0}
	h.OnBrokerConnect(seed, time.Millisecond, nil, nil)
	h.OnBrokerConnect(broker, time.Millisecond, nil, nil)
	disconnect(h, seed)
	if s := b.health.Snapshot(); s.State != mq.HealthReady {
		t.Errorf("Health after an idle connection's reap = %v (%v), want Ready", s.State, s.Err)
	}
	h.OnBrokerConnect(broker, time.Millisecond, nil, nil)
	if n := b.Reconnects(); n != 0 {
		t.Errorf("a connection dialled after a reap counted %d reconnections, want 0", n)
	}
	if n := b.DegradedTransitions(); n != 0 {
		t.Errorf("a reap counted %d degraded transitions, want 0", n)
	}
}

// TestAnOutageOfFailedDialsIsAReconnection: a client whose connections
// franz-go has reaped has nothing in flight when the broker goes away, so
// the outage reaches it only as dials that fail. The dial that succeeds after
// them is a reconnection, Ready and counted, with OnReconnect fired. A dial
// that fails before the client has ever connected, such as Open's first
// attempt, is not a lost link, and the dial after it is the first connection.
func TestAnOutageOfFailedDialsIsAReconnection(t *testing.T) {
	broker := kgo.BrokerMetadata{NodeID: 0}
	refused := syscall.ECONNREFUSED
	b, h := hooked()
	var fired []mq.ReconnectEvent
	b.cfg.Hooks.OnReconnect = func(_ context.Context, e mq.ReconnectEvent) { fired = append(fired, e) }
	h.OnBrokerConnect(broker, time.Millisecond, nil, nil)
	h.OnBrokerConnect(broker, time.Millisecond, nil, refused)
	if s := b.health.Snapshot(); s.State != mq.HealthDegraded {
		t.Errorf("Health after a dial that failed = %v, want Degraded", s.State)
	}
	h.OnBrokerConnect(broker, time.Millisecond, nil, refused)
	h.OnBrokerConnect(broker, time.Millisecond, nil, nil)
	if s := b.health.Snapshot(); s.State != mq.HealthReady {
		t.Errorf("Health after the broker came back = %v (%v), want Ready", s.State, s.Err)
	}
	if n := b.Reconnects(); n != 1 || len(fired) != 1 {
		t.Errorf("an outage of failed dials counted %d reconnections and fired OnReconnect %d times, want 1 and 1", n, len(fired))
	}

	b, h = hooked()
	h.OnBrokerConnect(broker, time.Millisecond, nil, refused)
	h.OnBrokerConnect(broker, time.Millisecond, nil, nil)
	if n := b.Reconnects(); n != 0 {
		t.Errorf("the first connection after a first dial that failed counted %d reconnections, want 0", n)
	}
}

// TestAFailedRequestIsALostLink is the other half: a request whose write or
// read fails is the link failing, so it is Degraded, and the next connection
// is Ready and a reconnection. A request that ends because its own context
// did, or because its client is closing, is neither.
func TestAFailedRequestIsALostLink(t *testing.T) {
	broker := kgo.BrokerMetadata{NodeID: 0}
	for _, err := range []error{context.Canceled, context.DeadlineExceeded, kgo.ErrClientClosed} {
		b, h := hooked()
		h.OnBrokerE2E(broker, int16(kmsg.Produce), kgo.BrokerE2E{ReadErr: err})
		if s := b.health.Snapshot(); s.State != mq.HealthReady {
			t.Errorf("Health after a request that ended with %v = %v (%v), want Ready", err, s.State, s.Err)
		}
	}
	for _, e2e := range []kgo.BrokerE2E{{ReadErr: io.EOF}, {WriteErr: syscall.ECONNRESET}, {ReadErr: os.ErrDeadlineExceeded}} {
		b, h := hooked()
		h.OnBrokerConnect(broker, time.Millisecond, nil, nil)
		h.OnBrokerE2E(broker, int16(kmsg.Fetch), e2e)
		if s := b.health.Snapshot(); s.State != mq.HealthDegraded {
			t.Errorf("Health after a request that failed with %v = %v, want Degraded", e2e.Err(), s.State)
		}
		h.OnBrokerConnect(broker, time.Millisecond, nil, nil)
		if s := b.health.Snapshot(); s.State != mq.HealthReady {
			t.Errorf("Health after the connection was dialled again = %v (%v), want Ready", s.State, s.Err)
		}
		if n := b.Reconnects(); n != 1 {
			t.Errorf("the dial after a request that failed with %v counted %d reconnections, want 1", e2e.Err(), n)
		}
	}
}
