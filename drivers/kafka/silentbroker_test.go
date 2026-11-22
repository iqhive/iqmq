package kafka_test

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"os"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
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

// TestOpenOnASilentBrokerEndsAtTheCallersDeadline: Open's Ping on a broker
// that accepted the connection and answers nothing returns the context's error
// at the caller's deadline, and Open leaves no goroutine behind. Before
// franz-go v1.22.3 the Ping ran for Config.RequestTimeout, 30 s by default,
// whatever the caller's deadline, and Open closed its client when the context
// ended to cut it short (TestFranzGoPingOnASilentBrokerEndsAtTheContextsEnd).
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

// TestFranzGoPingOnASilentBrokerEndsAtTheContextsEnd holds the client library
// to what Open's deadline rests on: a request whose context ends while its
// connection is still being set up returns then. franz-go before v1.22.3
// (issue 1505) finished the set-up first, under its request timeout overhead,
// so a Ping on a broker that accepted the connection and answered nothing ran
// for the overhead, or the dial timeout, whatever its context said. The TLS
// case is a set-up that waits on the handshake rather than on ApiVersions; it
// ended at the deadline before v1.22.3 too.
func TestFranzGoPingOnASilentBrokerEndsAtTheContextsEnd(t *testing.T) {
	for _, tc := range []struct {
		name string
		opts []kgo.Opt
	}{
		{"plaintext", nil},
		{"TLS", []kgo.Opt{kgo.DialTLSConfig(&tls.Config{MinVersion: tls.VersionTLS12})}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Registered first, so it runs after the listener's own cleanup.
			before := goleak.IgnoreCurrent()
			t.Cleanup(func() { goleak.VerifyNone(t, before) })
			addr := silentBroker(t)
			opts := append([]kgo.Opt{kgo.SeedBrokers(addr), kgo.RequestTimeoutOverhead(30 * time.Second)}, tc.opts...)
			cl, err := kgo.NewClient(opts...)
			if err != nil {
				t.Fatalf("NewClient: %v", err)
			}
			defer cl.Close()
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()
			began := time.Now()
			err = cl.Ping(ctx)
			took := time.Since(began)
			if !errors.Is(err, context.DeadlineExceeded) || took > 4*time.Second {
				t.Errorf("Ping under a 1 s deadline on a broker that answers nothing = %v after %v, want the context's DeadlineExceeded at the deadline",
					err, took.Round(time.Millisecond))
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
