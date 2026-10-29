package stomp_test

import (
	"context"
	"strconv"
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
	stomp "github.com/iqhive/iqmq/drivers/activemq/stomp"
	"github.com/iqhive/iqmq/drivers/activemq/stomp/internal/stompsrv"
	"github.com/iqhive/iqmq/internal/activemq"
)

// A failover list, and who walks it.
//
// The client's dial loop and every source dial connections of their own from
// Config.Addrs. They used to take the next address from one counter between
// them, so each dialler's attempts were interleaved with everybody else's and
// none of them walked the list the way the documentation says: round-robin
// across its own attempts.

// deadNode is a failover node that is not taking clients: every CONNECT is
// answered with an ERROR frame and then the socket, which is what a dial to a
// node that is down costs a dialler.
func deadNode(t *testing.T) *stompsrv.Server {
	t.Helper()
	return newServer(t, stompsrv.Options{
		ServerHeader:  activemq.ServerArtemis + "/2.44.0",
		RefuseConnect: "AMQ229119: this node is not live",
	})
}

// TestEverySourceDialsTheNodeTheClientReached is a failover list of a live
// node and a dead one, with the client connected to the live one.
//
// The client's own dial took the first address, so the first source drew the
// second and failed, the next drew the first and opened, and so on: every
// second OpenSource was refused although the client was connected and the live
// node was taking connections. A source starts its walk at the node the
// client's connection reached.
func TestEverySourceDialsTheNodeTheClientReached(t *testing.T) {
	t.Parallel()
	live, dead := artemisServer(t), deadNode(t)
	cfg := artemisConfig(live)
	cfg.Addrs = []string{live.Addr(), dead.Addr()}
	b := openBackend(t, cfg)

	for i := range 4 {
		sub := "workers" + strconv.Itoa(i)
		ctx, cancel := context.WithTimeout(context.Background(), budget)
		s, err := b.OpenSource(ctx, mq.SourceConfig{Topic: "orders", Subscription: sub, Prefetch: 4})
		cancel()
		if err != nil {
			t.Fatalf("source %d of 4 failed to open on a client connected to a live node: %v (the dead node took "+
				"%d connection(s))", i+1, err, dead.Accepted())
		}
		t.Cleanup(func() { closeSource(t, s) })
	}
	if got := dead.Accepted(); got != 0 {
		t.Errorf("the dead node was dialled %d time(s) while four sources opened, want 0: each source starts at the "+
			"node the client reached", got)
	}
}

// TestSourcesRedialAcrossAFailoverListIndependently is the same list with two
// sources whose connections both died.
//
// Re-dialling through one counter, two sources fetching in turn draw
// alternate addresses, so one of them can draw the dead node on every attempt
// and never come back. Each source walks the list on its own, so each reaches
// the live node within one pass.
func TestSourcesRedialAcrossAFailoverListIndependently(t *testing.T) {
	t.Parallel()
	live, dead := artemisServer(t), deadNode(t)
	cfg := reconnecting(artemisConfig(live))
	cfg.Addrs = []string{live.Addr(), dead.Addr()}
	b := openBackend(t, cfg)
	first, _ := openSource(t, b, cfg, live, "orders", "workers", 4)
	second, _ := openSource(t, b, cfg, live, "orders", "billing", 4)

	// The newest connection goes first, so this is the second source's and
	// then the first's; the client's own connection stays up.
	dropConnection(t, live)
	dropConnection(t, live)

	deadline := time.Now().Add(budget)
	for time.Now().Before(deadline) && (first.Redials() < 1 || second.Redials() < 1) {
		fetchQuietly(first, 10*time.Millisecond)
		fetchQuietly(second, 10*time.Millisecond)
	}
	for name, src := range map[string]*stomp.Source{"first": first, "second": second} {
		if src.Redials() < 1 {
			t.Errorf("the %s source never re-dialled the live node within %v (the dead node took %d connection(s))",
				name, budget, dead.Accepted())
		}
	}
}
