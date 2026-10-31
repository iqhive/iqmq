package rabbitmq_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	mq "github.com/iqhive/iqmq"
	rq "github.com/iqhive/iqmq/drivers/rabbitmq"
	"github.com/iqhive/iqmq/drivers/rabbitmq/internal/rabbitfake"
)

// hookLog records what OnReconnect was told, in order.
type hookLog struct {
	mu     sync.Mutex
	events []mq.ReconnectEvent
}

func (l *hookLog) record(_ context.Context, ev mq.ReconnectEvent) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.events = append(l.events, ev)
}

// since is what the hook was told from index i on.
func (l *hookLog) since(i int) []mq.ReconnectEvent {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]mq.ReconnectEvent(nil), l.events[i:]...)
}

func (l *hookLog) len() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.events)
}

// TestEachDialReachesTheReconnectHookOnce is mq.Hooks.OnReconnect across an
// outage: one event per dial attempt, carrying that attempt's number and its
// error, nil only for the dial that restored the connection.
//
// The dial loop reports every attempt to the hook, and the health reporter
// reported every health transition to it as well, so each failed dial reached
// the hook three times: the Connecting before it with a nil error, which the
// event's contract reads as "the connection was re-established", the attempt
// itself, and the Degraded after it with the same error again. A hook that
// counted reconnections or cleared an alert on a nil error was wrong on every
// attempt.
func TestEachDialReachesTheReconnectHookOnce(t *testing.T) {
	broker := rabbitfake.New(rabbitfake.Config{})
	t.Cleanup(func() { _ = broker.Close() })
	var log hookLog
	cfg := base()
	cfg.ReconnectBase, cfg.ReconnectMax = 5*time.Millisecond, 5*time.Millisecond
	cfg.Hooks.OnReconnect = log.record
	cl, err := rq.OpenWithDialer(t.Context(), broker.Dialer(), cfg)
	if err != nil {
		t.Fatalf("opening a client over the fake broker: %v", err)
	}
	t.Cleanup(func() { _ = cl.Close() })
	b := backendOf(t, cl)
	opened := log.since(0)
	t.Logf("Open told the hook %+v", opened)
	if len(opened) != 1 || opened[0].Attempt != 1 || opened[0].Err != nil {
		t.Errorf("Open told the hook %+v, want one event for its one dial: attempt 1, no error", opened)
	}

	mark, restores, dials := log.len(), rq.RestoresForTest(b), len(broker.Dials())
	broker.SetDown(true)
	broker.LastConnection().CloseWith(&amqp.Error{Code: amqp.ConnectionForced, Reason: "restart", Server: true})
	waitFor(t, 10*time.Second, func() bool { return len(broker.Dials()) >= dials+3 })
	broker.SetDown(false)
	waitFor(t, 10*time.Second, func() bool { return rq.RestoresForTest(b) > restores })
	attempts := len(broker.Dials()) - dials

	events := log.since(mark)
	for _, ev := range events {
		t.Logf("attempt %d: %v", ev.Attempt, ev.Err)
	}
	if len(events) != attempts {
		t.Fatalf("the hook was told %d events for %d dial attempts, want one per attempt", len(events), attempts)
	}
	for i, ev := range events {
		last := i == len(events)-1
		switch {
		case ev.Attempt != i+1:
			t.Errorf("event %d names attempt %d, want %d", i, ev.Attempt, i+1)
		case last && ev.Err != nil:
			t.Errorf("the dial that restored the connection was reported with %v, want no error", ev.Err)
		case !last && ev.Err == nil:
			t.Errorf("attempt %d, which failed, was reported with no error: a hook reads that as the "+
				"connection re-established", ev.Attempt)
		}
	}
}

// TestConnectionBlockedStillReachesTheReconnectHook is the one health
// transition the hook is documented to hear apart from the dials:
// `connection.blocked`, with the broker's reason, and `connection.unblocked`.
func TestConnectionBlockedStillReachesTheReconnectHook(t *testing.T) {
	broker := rabbitfake.New(rabbitfake.Config{})
	t.Cleanup(func() { _ = broker.Close() })
	var log hookLog
	cfg := base()
	cfg.Hooks.OnReconnect = log.record
	cl, err := rq.OpenWithDialer(t.Context(), broker.Dialer(), cfg)
	if err != nil {
		t.Fatalf("opening a client over the fake broker: %v", err)
	}
	t.Cleanup(func() { _ = cl.Close() })
	b := backendOf(t, cl)
	mark := log.len()
	conn := broker.LastConnection()
	blockConnection(t, conn, b, "low on disk")
	conn.Unblock()
	waitFor(t, 5*time.Second, func() bool { return !b.Blocked() })
	waitFor(t, 5*time.Second, func() bool {
		events := log.since(mark)
		return len(events) > 0 && events[len(events)-1].Err == nil
	})
	// blockConnection may have had to say it more than once; each one the
	// client heard is an event.
	events := log.since(mark)
	for i, ev := range events {
		last := i == len(events)-1
		if last && ev.Err != nil || !last && !errors.Is(ev.Err, mq.ErrBackpressure) || ev.Attempt != 0 {
			t.Fatalf("connection.blocked and connection.unblocked told the hook %+v, want the alarm's "+
				"Backpressure and then no error, with no attempt number: neither is a dial", events)
		}
	}
	if len(events) < 2 {
		t.Fatalf("connection.blocked and connection.unblocked told the hook %+v, want both", events)
	}
}
