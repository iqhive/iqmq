package rabbitmq_test

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
	rq "github.com/iqhive/iqmq/drivers/rabbitmq"
	"github.com/iqhive/iqmq/drivers/rabbitmq/internal/rabbitfake"
)

// silentListener accepts connections and never says a word, which is a broker
// whose container is paused as the kernel sees it: the TCP handshake completes
// and the AMQP one never starts. It signals each accepted connection's end on
// ended, and stops with the test.
func silentListener(t *testing.T) (addr string, ended <-chan struct{}) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listening: %v", err)
	}
	out := make(chan struct{}, 16)
	var wg sync.WaitGroup
	wg.Go(func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			wg.Go(func() {
				defer func() { _ = c.Close() }()
				_, _ = io.Copy(io.Discard, c)
				out <- struct{}{}
			})
		}
	})
	t.Cleanup(func() {
		_ = ln.Close()
		wg.Wait()
	})
	return ln.Addr().String(), out
}

// TestOpenEndsAtTheCallersDeadline is Open against a broker that accepts the
// connection and never answers the AMQP handshake, with Config.DialTimeout at
// its default of 15 s, and the caller's context ending first: by its deadline
// and by its cancellation.
//
// The handshake was bounded only by the dial timeout, a deadline on the socket,
// so Open returned 15 s later whatever the caller's context said; the CLI's
// --timeout was ignored for a paused broker. Open has to return when the
// context ends, with the context's error, and close the connection it was
// building, which the listener sees end.
func TestOpenEndsAtTheCallersDeadline(t *testing.T) {
	for _, tc := range []struct {
		name string
		ctx  func(context.Context) (context.Context, context.CancelFunc)
		want error
	}{
		{"deadline", func(p context.Context) (context.Context, context.CancelFunc) {
			return context.WithTimeout(p, 300*time.Millisecond)
		}, context.DeadlineExceeded},
		{"cancel", func(p context.Context) (context.Context, context.CancelFunc) {
			ctx, cancel := context.WithCancel(p)
			time.AfterFunc(300*time.Millisecond, cancel)
			return ctx, cancel
		}, context.Canceled},
	} {
		t.Run(tc.name, func(t *testing.T) {
			addr, ended := silentListener(t)
			cfg := base()
			cfg.Addrs = []string{addr}
			ctx, cancel := tc.ctx(t.Context())
			defer cancel()
			start := time.Now()
			cl, err := rq.Open(ctx, cfg)
			took := time.Since(start)
			if cl != nil {
				_ = cl.Close()
			}
			if took > 3*time.Second {
				t.Fatalf("Open against a broker that never answers took %v with a context of 300ms; it waited for "+
					"Config.DialTimeout (%v) instead of the caller", took, rq.DefaultDialTimeout)
			}
			if !errors.Is(err, tc.want) {
				t.Fatalf("Open = %v (code %v) after %v, want %v", err, mq.CodeOf(err), took, tc.want)
			}
			select {
			case <-ended:
			case <-time.After(2 * time.Second):
				t.Fatal("Open returned and left its half-built connection open")
			}
		})
	}
}

// TestOpenEndsAtTheCallersDeadlineInTheProbe is the same bound after the
// handshake: Config.DetectDelayPlugin probes with an `exchange.declare`, a
// round trip the client library gives no deadline, and the broker holds it
// (rabbitfake's declaration gate). Open has to return when the context ends,
// with the context's error, and close the connection it opened.
func TestOpenEndsAtTheCallersDeadlineInTheProbe(t *testing.T) {
	broker := rabbitfake.New(rabbitfake.Config{DelayedMessagePlugin: true})
	t.Cleanup(func() { _ = broker.Close() })
	gate := broker.HoldDeclarations()
	t.Cleanup(gate.Release)
	cfg := base()
	cfg.Declare, cfg.DetectDelayPlugin = true, true
	ctx, cancel := context.WithTimeout(t.Context(), 300*time.Millisecond)
	defer cancel()
	start := time.Now()
	opened := make(chan error, 1)
	go func() {
		cl, err := rq.OpenWithDialer(ctx, broker.Dialer(), cfg)
		if cl != nil {
			_ = cl.Close()
		}
		opened <- err
	}()
	select {
	case err := <-opened:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Open with its probe held = %v (code %v) after %v, want context.DeadlineExceeded",
				err, mq.CodeOf(err), time.Since(start))
		}
	case <-time.After(3 * time.Second):
		gate.Release()
		err := <-opened
		t.Fatalf("Open with its probe held was still open 3s after a 300ms context; it returned %v once the "+
			"broker answered", err)
	}
	if c := broker.LastConnection(); c == nil || !c.IsClosed() {
		t.Error("Open returned and left the connection it opened open")
	}
}
