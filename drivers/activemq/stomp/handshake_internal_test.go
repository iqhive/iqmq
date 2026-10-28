package stomp

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/iqhive/iqmq/driverkit"
	"github.com/iqhive/iqmq/internal/activemq"
)

// silentPeer accepts connections and never answers on them: a broker that
// took the socket and will not send CONNECTED.
func silentPeer(tb testing.TB) string {
	tb.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		tb.Fatalf("listening: %v", err)
	}
	var (
		mu    sync.Mutex
		conns []net.Conn
		wg    sync.WaitGroup
	)
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			mu.Lock()
			conns = append(conns, c)
			mu.Unlock()
		}
	}()
	tb.Cleanup(func() {
		_ = ln.Close()
		wg.Wait()
		mu.Lock()
		defer mu.Unlock()
		for _, c := range conns {
			_ = c.Close()
		}
	})
	return ln.Addr().String()
}

// TestDialOneBoundsTheHandshake is a peer that accepts the socket and never
// answers the CONNECT frame. The handshake is part of the dial, so the dial
// timeout bounds it, and so does the caller's own context: an unbounded one
// hung OpenBackend, a source's redial and the supervisor's reconnect for ever.
func TestDialOneBoundsTheHandshake(t *testing.T) {
	cases := []struct {
		name string
		dial time.Duration
		ctx  func() (context.Context, context.CancelFunc)
	}{
		{
			name: "dial-timeout",
			dial: 200 * time.Millisecond,
			ctx: func() (context.Context, context.CancelFunc) {
				return context.WithCancel(context.Background())
			},
		},
		{
			name: "caller-cancels",
			dial: time.Minute,
			ctx: func() (context.Context, context.CancelFunc) {
				ctx, cancel := context.WithCancel(context.Background())
				time.AfterFunc(200*time.Millisecond, cancel)
				return ctx, cancel
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := Config{
				Broker: activemq.Artemis, Addrs: []string{silentPeer(t)},
				ReceiptTimeout: time.Second, DialTimeout: tc.dial,
			}
			if err := cfg.Validate(); err != nil {
				t.Fatalf("Config.Validate: %v", err)
			}
			b, err := newBackend(cfg.resolve(), nil, driverkit.ClientOwned)
			if err != nil {
				t.Fatalf("newBackend: %v", err)
			}
			ctx, cancel := tc.ctx()
			defer cancel()
			done := make(chan error, 1)
			go func() {
				conn, _, err := b.dialOne(ctx, nil)
				if conn != nil {
					_ = conn.MustDisconnect()
				}
				done <- err
			}()
			select {
			case err := <-done:
				if err == nil {
					t.Fatal("a dial to a peer that never sent CONNECTED succeeded")
				}
			case <-time.After(10 * time.Second):
				t.Fatal("the dial is still waiting for a CONNECTED frame the peer will never send")
			}
		})
	}
}
