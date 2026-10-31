package pulsar_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
	pd "github.com/iqhive/iqmq/drivers/pulsar"
)

// brokerHealth is a Pulsar web service whose health endpoint a test steers: it
// answers status, or, while hang is set, holds the request until the caller
// gives up on it.
type brokerHealth struct {
	srv     *httptest.Server
	status  atomic.Int32
	hang    atomic.Bool
	arrived chan struct{}
}

func newBrokerHealth(t *testing.T) *brokerHealth {
	t.Helper()
	h := &brokerHealth{arrived: make(chan struct{}, 1)}
	h.status.Store(http.StatusOK)
	h.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/admin/v2/brokers/health" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		if h.hang.Load() {
			select {
			case h.arrived <- struct{}{}:
			default:
			}
			<-r.Context().Done()
			return
		}
		w.WriteHeader(int(h.status.Load()))
	}))
	t.Cleanup(h.srv.Close)
	return h
}

// reconnectHook counts the reconnections Config.Hooks.OnReconnect is told of.
func reconnectHook(n *atomic.Int32) func(*pd.Config) {
	return func(c *pd.Config) {
		c.Hooks.OnReconnect = func(context.Context, mq.ReconnectEvent) { n.Add(1) }
	}
}

// TestPingDegradesOnlyOnTheBrokersAnswer is Ping failing for a reason that says
// nothing about the broker, followed by a Ping that succeeds.
//
// Every probe failure moved health to Degraded and counted a degraded
// transition, and the next successful Ping was read as a reconnection:
// Reconnects counted one and Config.Hooks.OnReconnect fired, for a connection
// that never dropped. The same happened after any other degradation, such as
// an undecodable delivery. A reconnection is a Ping that succeeds after the
// broker or the link to it failed, which the last subtest holds.
func TestPingDegradesOnlyOnTheBrokersAnswer(t *testing.T) {
	t.Run("nothing to probe", func(t *testing.T) {
		var hooked atomic.Int32
		b, _ := newRig(t, reconnectHook(&hooked))
		if err := b.Ping(context.Background()); !errors.Is(err, mq.ErrUnsupported) {
			t.Fatalf("Ping with nothing to probe = %v, want ErrUnsupported", err)
		}
		state, degraded := b.Health().State, b.DegradedTransitions()
		_ = sinkOf(t, b, "ping-target", mq.MetadataNativeStrict)
		if err := b.Ping(context.Background()); err != nil {
			t.Fatalf("Ping with a handle: %v", err)
		}
		if state == mq.HealthDegraded || degraded != 0 || b.Reconnects() != 0 || hooked.Load() != 0 {
			t.Fatalf("a Ping that had nothing to probe left health %v and DegradedTransitions %d, and the next "+
				"Ping counted %d reconnection(s) and fired OnReconnect %d time(s); the connection never dropped",
				state, degraded, b.Reconnects(), hooked.Load())
		}
	})
	t.Run("the caller gives up", func(t *testing.T) {
		var hooked atomic.Int32
		h := newBrokerHealth(t)
		b, _ := newRig(t, func(c *pd.Config) {
			c.AdminURL = h.srv.URL
			reconnectHook(&hooked)(c)
		})
		h.hang.Store(true)
		ctx, cancel := context.WithCancel(context.Background())
		go func() {
			<-h.arrived
			cancel()
		}()
		if err := b.Ping(ctx); !errors.Is(err, context.Canceled) {
			t.Fatalf("a Ping its caller cancelled = %v, want context.Canceled", err)
		}
		state, degraded := b.Health().State, b.DegradedTransitions()
		h.hang.Store(false)
		if err := b.Ping(context.Background()); err != nil {
			t.Fatalf("Ping: %v", err)
		}
		if state == mq.HealthDegraded || degraded != 0 || b.Reconnects() != 0 || hooked.Load() != 0 {
			t.Fatalf("a Ping its caller cancelled left health %v and DegradedTransitions %d, and the next Ping "+
				"counted %d reconnection(s) and fired OnReconnect %d time(s)",
				state, degraded, b.Reconnects(), hooked.Load())
		}
	})
	t.Run("an undecodable delivery", func(t *testing.T) {
		var hooked atomic.Int32
		b, _ := newRig(t, reconnectHook(&hooked))
		src, err := b.OpenSource(context.Background(), mq.SourceConfig{
			Topic: "undecodable", Subscription: "workers", Prefetch: 4,
			Metadata: mq.MetadataPortableEnvelope, Start: mq.StartEarliest(),
		})
		if err != nil {
			t.Fatalf("OpenSource: %v", err)
		}
		t.Cleanup(func() { _ = src.Close() })
		snk := sinkOf(t, b, "undecodable", mq.MetadataNativeStrict)
		publish(t, snk, &mq.Message{Body: []byte("not an envelope")})
		fetchNothing(t, src, "an undecodable delivery was handed to a handler")
		waitFor(t, "the undecodable delivery to degrade health", func() bool {
			return b.Health().State == mq.HealthDegraded
		})
		if err := b.Ping(context.Background()); err != nil {
			t.Fatalf("Ping: %v", err)
		}
		if b.Health().State != mq.HealthReady || b.Reconnects() != 0 || hooked.Load() != 0 {
			t.Fatalf("a Ping after an undecodable delivery left health %v, counted %d reconnection(s) and fired "+
				"OnReconnect %d time(s); want Ready and no reconnection", b.Health().State, b.Reconnects(),
				hooked.Load())
		}
	})
	t.Run("a publication the broker could not be reached for", func(t *testing.T) {
		var hooked atomic.Int32
		b, cl := newClosingRigWith(t, reconnectHook(&hooked))
		snk := sinkOf(t, b, "unreachable", mq.MetadataNativeStrict)
		cl.newest(t).closeForGood(unreachable{})
		err := snk.Publish(context.Background(), &mq.Message{Body: []byte("x")}, nil)
		if mq.CodeOf(err) != mq.CodeUnavailable {
			t.Fatalf("a publication the broker could not be reached for = %v (code %v), want CodeUnavailable",
				err, mq.CodeOf(err))
		}
		if err := b.Ping(context.Background()); err != nil {
			t.Fatalf("Ping: %v", err)
		}
		if b.Health().State != mq.HealthReady || b.Reconnects() != 1 || hooked.Load() != 1 {
			t.Fatalf("a Ping that succeeded after a publication failed as unavailable left health %v, counted %d "+
				"reconnection(s) and fired OnReconnect %d time(s); want Ready, 1 and 1", b.Health().State,
				b.Reconnects(), hooked.Load())
		}
	})
	t.Run("the broker fails and comes back", func(t *testing.T) {
		var hooked atomic.Int32
		h := newBrokerHealth(t)
		b, _ := newRig(t, func(c *pd.Config) {
			c.AdminURL = h.srv.URL
			reconnectHook(&hooked)(c)
		})
		h.status.Store(http.StatusServiceUnavailable)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := b.Ping(ctx); mq.CodeOf(err) != mq.CodeUnavailable {
			t.Fatalf("a Ping the broker answered 503 = %v (code %v), want CodeUnavailable", err, mq.CodeOf(err))
		}
		if b.Health().State != mq.HealthDegraded || b.DegradedTransitions() != 1 {
			t.Fatalf("a Ping the broker failed left health %v and DegradedTransitions %d, want Degraded and 1",
				b.Health().State, b.DegradedTransitions())
		}
		h.status.Store(http.StatusOK)
		if err := b.Ping(ctx); err != nil {
			t.Fatalf("Ping: %v", err)
		}
		if b.Health().State != mq.HealthReady || b.Reconnects() != 1 || hooked.Load() != 1 {
			t.Fatalf("a Ping that succeeded after the broker failed left health %v, counted %d reconnection(s) "+
				"and fired OnReconnect %d time(s); want Ready, 1 and 1", b.Health().State, b.Reconnects(),
				hooked.Load())
		}
	})
}
