package client

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"
)

// TestADialEndsWithItsContext is [Real.Dial] against a listener that accepts
// and never answers the AMQP handshake, with a 15 s dial timeout and a 300 ms
// context: the dial returns the context's own error at the deadline, and the
// listener sees the connection end. drivers/rabbitmq's
// TestOpenEndsAtTheCallersDeadline is the same through Open.
func TestADialEndsWithItsContext(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ended := make(chan struct{}, 1)
	var wg sync.WaitGroup
	wg.Go(func() {
		c, err := ln.Accept()
		if err != nil {
			return
		}
		_, _ = io.Copy(io.Discard, c)
		_ = c.Close()
		ended <- struct{}{}
	})
	t.Cleanup(func() {
		_ = ln.Close()
		wg.Wait()
	})
	ctx, cancel := context.WithTimeout(t.Context(), 300*time.Millisecond)
	defer cancel()
	start := time.Now()
	conn, err := Real{}.Dial(ctx, []string{"amqp://guest:guest@" + ln.Addr().String() + "/"},
		DialConfig{Timeout: 15 * time.Second})
	if conn != nil {
		_ = conn.Close()
	}
	if took := time.Since(start); took > 3*time.Second || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Dial = %v after %v, want the context's own error at its 300ms deadline", err, took)
	}
	select {
	case <-ended:
	case <-time.After(2 * time.Second):
		t.Fatal("Dial returned and left its connection open")
	}
}
