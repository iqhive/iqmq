package kafka_test

import (
	"context"
	"errors"
	"net"
	"os"
	"testing"
	"time"

	"go.uber.org/goleak"

	mq "github.com/iqhive/iqmq"
	kq "github.com/iqhive/iqmq/drivers/kafka"
)

// silentBroker listens, accepts every connection and never answers, as a
// broker that is paused or hung behind a live socket does. It is closed on
// cleanup, its connections with it.
func silentBroker(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	var held []net.Conn
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			held = append(held, c)
		}
	}()
	t.Cleanup(func() {
		_ = ln.Close()
		<-done
		for _, c := range held {
			_ = c.Close()
		}
	})
	return ln.Addr().String()
}

// TestOpenOnASilentBrokerEndsAtTheCallersDeadline: franz-go sets each new
// connection up (ApiVersions, then SASL) under a deadline of its own, its
// request timeout overhead, which is Config.RequestTimeout, and does not look
// at the request's context until the connection is set up. So Open's Ping on
// a broker that accepted the connection and answered nothing ran for
// Config.RequestTimeout, 30 s by default, whatever the caller's deadline.
// Open closes the client it built when its context ends, which cuts the set-up
// short, and returns the context's error at the caller's deadline, leaving no
// goroutine behind.
func TestOpenOnASilentBrokerEndsAtTheCallersDeadline(t *testing.T) {
	for _, tc := range []struct {
		name string
		want error
		code mq.ErrorCode
	}{
		{"a deadline", context.DeadlineExceeded, mq.CodeDeadlineExceeded},
		{"a cancel", context.Canceled, mq.CodeCanceled},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Registered first, so it runs after the listener's own cleanup.
			before := goleak.IgnoreCurrent()
			t.Cleanup(func() { goleak.VerifyNone(t, before) })
			addr := silentBroker(t)
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			if errors.Is(tc.want, context.Canceled) {
				cancel()
				ctx, cancel = context.WithCancel(t.Context())
				stop := time.AfterFunc(time.Second, cancel)
				defer stop.Stop()
			}
			defer cancel()
			began := time.Now()
			c, err := kq.Open(ctx, kq.Config{Brokers: []string{addr}, RequestTimeout: 10 * time.Second})
			took := time.Since(began)
			if err == nil {
				_ = c.Close()
				t.Fatal("Open on a broker that answers nothing returned nil")
			}
			if took > 4*time.Second {
				t.Errorf("Open whose context ended after 1 s returned after %v, at Config.RequestTimeout, want when the context ended",
					took.Round(time.Millisecond))
			}
			if !errors.Is(err, tc.want) || mq.CodeOf(err) != tc.code {
				t.Errorf("Open whose context ended during its Ping = %v (code %v), want the context's %v", err, mq.CodeOf(err), tc.want)
			}
		})
	}
}

// TestBrokerOpenOnAPausedBrokerEndsAtTheCallersDeadline is the same against a
// real broker that holds its connections and answers nothing, named by
// IQMQ_KAFKA_SILENT_ADDR (a paused apache/kafka, say).
func TestBrokerOpenOnAPausedBrokerEndsAtTheCallersDeadline(t *testing.T) {
	addr := os.Getenv("IQMQ_KAFKA_SILENT_ADDR")
	if addr == "" {
		t.Skip("IQMQ_KAFKA_SILENT_ADDR is not set; this test asks a broker that answers nothing")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	began := time.Now()
	c, err := kq.Open(ctx, kq.Config{Brokers: []string{addr}})
	took := time.Since(began)
	if err == nil {
		_ = c.Close()
		t.Fatal("Open on a broker that answers nothing returned nil")
	}
	if took > 6*time.Second || !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("Open under a 3 s deadline = %v after %v, want the context's DeadlineExceeded at the deadline", err, took.Round(time.Millisecond))
	}
}
