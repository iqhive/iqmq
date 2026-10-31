package stomp

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
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
//
// A dial DialTimeout ends while the caller's context is live is a retryable
// Unavailable that names DialTimeout ([dialTimedOut]). It came back as the
// context's DeadlineExceeded, which told a caller with time left that its own
// deadline was the problem.
func TestDialOneBoundsTheHandshake(t *testing.T) {
	cases := []struct {
		name     string
		dial     time.Duration
		ctx      func() (context.Context, context.CancelFunc)
		timedOut bool
	}{
		{
			name:     "dial-timeout",
			dial:     200 * time.Millisecond,
			timedOut: true,
			ctx: func() (context.Context, context.CancelFunc) {
				return context.WithCancel(context.Background())
			},
		},
		{
			// A caller's deadline later than DialTimeout used to replace it,
			// so a source re-dialling inside a fetch with a long deadline
			// waited out all of it. The sooner of the two ends the dial.
			name:     "dial-timeout-under-a-longer-deadline",
			dial:     200 * time.Millisecond,
			timedOut: true,
			ctx: func() (context.Context, context.CancelFunc) {
				return context.WithTimeout(context.Background(), time.Minute)
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
				conn, _, err := b.dialOne(ctx, cfg.Addrs[0], nil)
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
				if got := errors.Is(err, errDialTimedOut); got != tc.timedOut {
					t.Errorf("the dial returned %v; errors.Is(err, errDialTimedOut) is %v, want %v", err, got,
						tc.timedOut)
				}
				if mapped := b.errs.MapNet(mq.OpOpen, "", "", err, false); tc.timedOut &&
					(mq.CodeOf(mapped) != mq.CodeUnavailable || !mq.IsRetryable(mapped)) {
					t.Errorf("a dial DialTimeout ended maps to %v (code %v, retryable %v), want a retryable "+
						"Unavailable", mapped, mq.CodeOf(mapped), mq.IsRetryable(mapped))
				}
			case <-time.After(10 * time.Second):
				t.Fatal("the dial is still waiting for a CONNECTED frame the peer will never send")
			}
		})
	}
}
