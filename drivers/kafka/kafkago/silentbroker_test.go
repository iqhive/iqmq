package kafkago_test

import (
	"context"
	"errors"
	"net"
	"os"
	"testing"
	"time"

	"go.uber.org/goleak"

	mq "github.com/iqhive/iqmq"
	kq "github.com/iqhive/iqmq/drivers/kafka/kafkago"
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

// TestOpenOnASilentBrokerEndsAtTheCallersDeadline: Open on a broker that
// accepts the connection and answers nothing returns the context's error at
// the caller's deadline, not at Config.RequestTimeout, leaving no goroutine
// behind. kafka-go's metadata request looks at its context while it waits; the
// franz-go driver needed its Open to close its own client to get the same.
func TestOpenOnASilentBrokerEndsAtTheCallersDeadline(t *testing.T) {
	// Registered first, so it runs after the listener's own cleanup.
	before := goleak.IgnoreCurrent()
	t.Cleanup(func() { goleak.VerifyNone(t, before) })
	addr := silentBroker(t)
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	began := time.Now()
	c, err := kq.Open(ctx, kq.Config{Brokers: []string{addr}, RequestTimeout: 10 * time.Second})
	took := time.Since(began)
	if err == nil {
		_ = c.Close()
		t.Fatal("Open on a broker that answers nothing returned nil")
	}
	if took > 4*time.Second {
		t.Errorf("Open under a 1 s deadline returned after %v, want at the caller's deadline", took.Round(time.Millisecond))
	}
	if !errors.Is(err, context.DeadlineExceeded) || mq.CodeOf(err) != mq.CodeDeadlineExceeded {
		t.Errorf("Open whose deadline ended while it waited = %v (code %v), want the context's DeadlineExceeded", err, mq.CodeOf(err))
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
