package jetstream_test

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
	nq "github.com/iqhive/iqmq/drivers/jetstream"
)

// silentServer accepts connections and never answers them, which is what a
// wedged broker, or a load balancer with nothing behind it, looks like to a
// client waiting for INFO. It returns its address and a release that closes
// the connections it holds; both happen on cleanup too.
func silentServer(tb testing.TB) (string, func()) {
	tb.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		tb.Fatalf("listen: %v", err)
	}
	var (
		mu    sync.Mutex
		held  []net.Conn
		owner sync.WaitGroup
	)
	owner.Add(1)
	go func() {
		defer owner.Done()
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			mu.Lock()
			held = append(held, c)
			mu.Unlock()
		}
	}()
	release := func() {
		mu.Lock()
		defer mu.Unlock()
		for _, c := range held {
			_ = c.Close()
		}
	}
	tb.Cleanup(func() {
		_ = ln.Close()
		owner.Wait()
		release()
	})
	return ln.Addr().String(), release
}

// TestOpenEndsTheDialAtItsContextsDeadline is Open against a server that
// accepts the connection and never answers, under a context with a deadline.
//
// nats.go's connect takes no context, and once the server accepts, nats.go
// waits for its INFO for ConnectTimeout. Open checked its context before the
// dial and not during it, so a caller with a 200ms deadline waited the whole
// ConnectTimeout. The dial is held to the deadline now. A wss:// address is
// the case nats.go puts no deadline on at all: its TLS handshake runs before
// any, so it waited for as long as the server held the connection open.
// nats.go reports a handshake that failed as a TLS error, which the driver
// maps to Unavailable, so only the INFO wait is a DeadlineExceeded.
func TestOpenEndsTheDialAtItsContextsDeadline(t *testing.T) {
	for _, tc := range []struct {
		scheme string
		code   mq.ErrorCode
	}{
		{"nats://", mq.CodeDeadlineExceeded},
		{"wss://", mq.CodeUnavailable},
	} {
		t.Run(tc.scheme, func(t *testing.T) {
			addr, release := silentServer(t)
			ctx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
			defer cancel()
			type opened struct {
				b    *nq.Backend
				err  error
				took time.Duration
			}
			done := make(chan opened, 1)
			go func() {
				start := time.Now()
				b, err := nq.OpenBackend(ctx, nq.Config{Addrs: []string{tc.scheme + addr}, ConnectTimeout: 5 * time.Second})
				done <- opened{b, err, time.Since(start)}
			}()
			var o opened
			select {
			case o = <-done:
			case <-time.After(8 * time.Second):
				// Closing the server's end ends the wait, so the
				// goroutine returns.
				release()
				o = <-done
				t.Fatalf("Open with a 200ms deadline was still dialling 8s later; closing the server's end released it (err %v)", o.err)
			}
			b, err, took := o.b, o.err, o.took
			if err == nil {
				_ = b.Close()
				t.Fatal("Open succeeded against a server that never answers")
			}
			if took > 2*time.Second {
				t.Fatalf("Open with a 200ms deadline returned after %v (err %v): the dial ignored the context", took.Round(time.Millisecond), err)
			}
			if code := mq.CodeOf(err); code != tc.code {
				t.Errorf("Open past its deadline = %v (code %v), want %v", err, code, tc.code)
			}
			t.Logf("Open returned after %v: %v (retryable %v)", took.Round(time.Millisecond), err, mq.IsRetryable(err))
		})
	}
}

// TestOpensDeadlineDoesNotOutliveOpen holds the connection an Open with a
// deadline made to that deadline only while Open runs.
//
// The dial is held to the context's deadline through the dialer and the socket
// nats.go is handed. A write after the deadline, or a reconnection, held to it
// as well would fail for the life of the connection, so the connection has to
// answer a round trip after the deadline has passed, and again after it has
// reconnected.
func TestOpensDeadlineDoesNotOutliveOpen(t *testing.T) {
	cfg := baseConfig(t)
	cfg.ReconnectWait = 10 * time.Millisecond
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	b, err := nq.OpenBackend(ctx, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = b.Close() }()
	<-ctx.Done()

	if err := b.Ping(t.Context()); err != nil {
		t.Fatalf("a round trip after the deadline of the Open that made the connection: %v", err)
	}
	reconnects := b.Conn().Stats().Reconnects
	if err := b.Conn().ForceReconnect(); err != nil {
		t.Fatalf("ForceReconnect: %v", err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for b.Conn().Stats().Reconnects == reconnects || !b.Conn().IsConnected() {
		if time.Now().After(deadline) {
			t.Fatalf("the connection did not reconnect after the deadline of the Open that made it (status %v)", b.Conn().Status())
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err := b.Ping(t.Context()); err != nil {
		t.Fatalf("a round trip after reconnecting: %v", err)
	}
}
