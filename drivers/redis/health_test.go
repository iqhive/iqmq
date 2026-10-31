package redis_test

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit"
	rd "github.com/iqhive/iqmq/drivers/redis"
	"github.com/iqhive/iqmq/internal/streams"
	"github.com/iqhive/iqmq/internal/streams/fakestreams"
)

// errLoading is the reply a server sends while it loads its dataset, which is
// what a sweep or a mover pass meets during a restart or a failover.
var errLoading = errors.New("LOADING Redis is loading the dataset in memory")

// failingVerb is the in-memory Redis with one verb failing, for the commands
// that name a key holding topic, for as long as on is set. A one-shot fault
// is not enough here: a sweep every few milliseconds clears it before a test
// can look.
type failingVerb struct {
	*fakestreams.Server
	verb  string
	topic string
	on    atomic.Bool
}

func (f *failingVerb) fails(c streams.Cmd) bool {
	if !f.on.Load() || len(c.Tokens) == 0 || !strings.EqualFold(c.Tokens[0], f.verb) {
		return false
	}
	for _, tok := range c.Tokens[1:] {
		if strings.Contains(tok, "{"+f.topic+"}") {
			return true
		}
	}
	return false
}

func (f *failingVerb) Do(ctx context.Context, c streams.Cmd) (streams.Value, error) {
	if f.fails(c) {
		return streams.Nil(), errLoading
	}
	return f.Server.Do(ctx, c)
}

func (f *failingVerb) DoMulti(ctx context.Context, cs []streams.Cmd) ([]streams.Result, error) {
	out := make([]streams.Result, len(cs))
	for i, c := range cs {
		out[i].Value, out[i].Err = f.Do(ctx, c)
	}
	return out, nil
}

// healthRig is a backend over a failingVerb, counting the OnReconnect calls a
// return to Ready makes.
type healthRig struct {
	srv        *failingVerb
	b          *rd.Backend
	reconnects atomic.Int32
}

func newHealthRig(t *testing.T, verb, topic string, opts ...func(*rd.Config)) *healthRig {
	t.Helper()
	h := &healthRig{srv: &failingVerb{
		Server: fakestreams.New(fakestreams.Options{AOF: true, Version: "7.4.2"}),
		verb:   verb,
		topic:  topic,
	}}
	cfg := rd.Config{
		AckDeadline:   time.Second,
		ClaimInterval: 10 * time.Millisecond,
		Block:         time.Millisecond,
		AutoCreate:    true,
		Hooks: mq.Hooks{OnReconnect: func(context.Context, mq.ReconnectEvent) {
			h.reconnects.Add(1)
		}},
	}
	for _, opt := range opts {
		opt(&cfg)
	}
	b, err := rd.BackendWithCommander(context.Background(), h.srv, driverkit.ClientOwned, cfg)
	if err != nil {
		t.Fatalf("BackendWithCommander: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })
	h.b = b
	return h
}

func (h *healthRig) open(t *testing.T, topic string) mq.Source {
	t.Helper()
	src, err := h.b.OpenSource(context.Background(), mq.SourceConfig{
		Topic: topic, Subscription: testSub, Prefetch: 1, Start: mq.StartEarliest(),
	})
	if err != nil {
		t.Fatalf("OpenSource(%s): %v", topic, err)
	}
	t.Cleanup(func() { _ = src.Close() })
	return src
}

// waitHealth waits up to two seconds for the backend to reach state.
func (h *healthRig) waitHealth(t *testing.T, state mq.HealthState, why string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for h.b.Health().State != state {
		if time.Now().After(deadline) {
			snap := h.b.Health()
			t.Fatalf("health is %v (%v) after 2s, want %v: %s", snap.State, snap.Err, state, why)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// TestHealthIsReadyAgainAfterABackgroundFailure is a background loop that
// fails for a while and then works again.
//
// The sweep, the delayed mover and an undecodable entry each report Degraded,
// and nothing ever reported Ready again after open. One LOADING reply during
// a restart left the backend Degraded for the rest of its life, a readiness
// probe failed for ever, and Hooks.OnReconnect, which fires on a return to
// Ready, never fired again.
func TestHealthIsReadyAgainAfterABackgroundFailure(t *testing.T) {
	t.Run("sweep", func(t *testing.T) {
		h := newHealthRig(t, "XAUTOCLAIM", testTopic)
		h.open(t, testTopic)
		base := h.reconnects.Load()
		h.srv.on.Store(true)
		h.waitHealth(t, mq.HealthDegraded, "the sweep fails every round")
		h.srv.on.Store(false)
		h.waitHealth(t, mq.HealthReady, "the sweep has worked again since its failure")
		if h.reconnects.Load() == base {
			t.Fatal("health came back to Ready and Hooks.OnReconnect was not called")
		}
	})
	t.Run("mover", func(t *testing.T) {
		h := newHealthRig(t, "ZRANGEBYSCORE", testTopic, func(c *rd.Config) {
			c.Nack = streams.ZSetMover
			c.MoverInterval = 10 * time.Millisecond
		})
		h.open(t, testTopic)
		h.srv.on.Store(true)
		h.waitHealth(t, mq.HealthDegraded, "the mover fails every pass")
		h.srv.on.Store(false)
		h.waitHealth(t, mq.HealthReady, "the mover has worked again since its failure")
	})
	t.Run("undecodable entry", func(t *testing.T) {
		h := newHealthRig(t, "", testTopic)
		src := h.open(t, testTopic)
		base := h.reconnects.Load()
		keys, err := streams.NewKeys(testTopic, 1)
		if err != nil {
			t.Fatal(err)
		}
		// A header index that does not run from zero, which no encoder writes.
		if _, err := h.srv.Server.Do(context.Background(), streams.XAdd(streams.XAddArgs{
			Key: keys.Stream(0), ID: streams.IDAuto, Fields: []string{"b", "body", "h:0", "1:kv", "h:2", "1:kv"},
		})); err != nil {
			t.Fatalf("XADD: %v", err)
		}
		fctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()
		if n, _ := src.Fetch(fctx, make([]mq.DriverDelivery, 1)); n != 0 {
			t.Fatalf("Fetch delivered %d undecodable entries", n)
		}
		h.waitHealth(t, mq.HealthReady, "the sweep has run since the undecodable entry")
		if h.reconnects.Load() == base {
			t.Fatal("the undecodable entry left health as it was, or it never came back to Ready")
		}
	})
}

// TestHealthStaysDegradedWhileAnySourceIsFailing is two sources on one
// backend, one of them failing.
//
// A good round of one source's sweep is not news about the other's. Ready
// comes back only once no source's background loop is failing, so a source
// that keeps failing beside a healthy one is Degraded throughout, rather than
// flapping to Ready and firing Hooks.OnReconnect at every round of the healthy
// one.
func TestHealthStaysDegradedWhileAnySourceIsFailing(t *testing.T) {
	h := newHealthRig(t, "XAUTOCLAIM", "failing.topic")
	h.open(t, testTopic)
	h.open(t, "failing.topic")
	h.srv.on.Store(true)
	h.waitHealth(t, mq.HealthDegraded, "one source's sweep fails every round")
	base := h.reconnects.Load()
	deadline := time.Now().Add(300 * time.Millisecond) // about 30 rounds of each sweep
	for time.Now().Before(deadline) {
		if st := h.b.Health().State; st != mq.HealthDegraded {
			t.Fatalf("health is %v while one of the two sources fails every sweep", st)
		}
		time.Sleep(2 * time.Millisecond)
	}
	if n := h.reconnects.Load() - base; n != 0 {
		t.Fatalf("Hooks.OnReconnect fired %d times while one source failed every sweep", n)
	}
	h.srv.on.Store(false)
	h.waitHealth(t, mq.HealthReady, "neither source is failing any more")
}

// TestHealthIsReadyOnceTheFailingSourceCloses is a source that was failing
// when it closed. It cannot report a good round again, so the backend is Ready
// as soon as no open source is failing.
func TestHealthIsReadyOnceTheFailingSourceCloses(t *testing.T) {
	h := newHealthRig(t, "XAUTOCLAIM", "failing.topic")
	h.open(t, testTopic)
	failing := h.open(t, "failing.topic")
	h.srv.on.Store(true)
	h.waitHealth(t, mq.HealthDegraded, "one source's sweep fails every round")
	if err := failing.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	h.waitHealth(t, mq.HealthReady, "the only failing source has closed")
}
