package jetstream_test

import (
	"context"
	"errors"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	natsgo "github.com/nats-io/nats.go"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit"
	nq "github.com/iqhive/iqmq/drivers/jetstream"
)

// deniedServer starts a JetStream server whose one user may publish anywhere
// but "perm.denied", which a stream under the prefix "perm" captures.
func deniedServer(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "iqmq-js-perm-*")
	if err != nil {
		t.Fatalf("store dir: %v", err)
	}
	srv, err := server.NewServer(&server.Options{
		Host: "127.0.0.1", Port: -1, NoLog: true, NoSigs: true,
		JetStream: true, StoreDir: dir,
		Users: []*server.User{{
			Username: "u", Password: "p",
			Permissions: &server.Permissions{
				Publish: &server.SubjectPermission{Allow: []string{">"}, Deny: []string{"perm.denied"}},
			},
		}},
	})
	if err != nil {
		t.Fatalf("server: %v", err)
	}
	go srv.Start()
	if !srv.ReadyForConnections(20 * time.Second) {
		t.Fatal("the server did not become ready")
	}
	t.Cleanup(func() {
		srv.Shutdown()
		srv.WaitForShutdown()
		_ = os.RemoveAll(dir)
	})
	return srv.ClientURL()
}

// TestAPublicationThePermissionsRefuseIsForbidden holds that a publication to
// a subject the bound stream captures but the user may not publish to is a
// definite CodeForbidden, reported when the server refuses it. The server
// discards it with an -ERR and sends no PubAck, and every path that waits for
// a PubAck used to wait out Config.PublishTimeout and report an ambiguous
// DeadlineExceeded: a message the server threw away reported as one the
// stream may hold, which a retry loop resends for ever.
func TestAPublicationThePermissionsRefuseIsForbidden(t *testing.T) {
	url := deniedServer(t)
	cfg := nq.Config{
		Addrs: []string{url}, Username: "u", Password: "p",
		Stream: "PERM", SubjectPrefix: "perm", ManageStream: true,
		PublishTimeout: 5 * time.Second,
	}
	open := func(t *testing.T) *nq.Backend {
		t.Helper()
		b, err := nq.OpenBackend(t.Context(), cfg)
		if err != nil {
			t.Fatalf("open: %v", err)
		}
		t.Cleanup(func() { _ = b.Close() })
		if err := mq.NewClient(b).Publish(t.Context(), "allowed", &mq.Message{Body: []byte("x")}); err != nil {
			t.Fatalf("a permitted publication: %v", err)
		}
		return b
	}
	forbidden := func(t *testing.T, what string, err error, took time.Duration) {
		t.Helper()
		var oe *mq.OpError
		if mq.CodeOf(err) != mq.CodeForbidden || mq.IsAmbiguous(err) || !errors.As(err, &oe) || oe.Operation != mq.OpPublish {
			t.Fatalf("%s to a subject the user may not publish to = %v (code %v, ambiguous %v) after %v, want a definite Forbidden on publish",
				what, err, mq.CodeOf(err), mq.IsAmbiguous(err), took.Round(time.Millisecond))
		}
		if took > 2500*time.Millisecond {
			t.Fatalf("%s the server refused took %v of the 5s PublishTimeout, want it ended by the refusal", what, took.Round(time.Millisecond))
		}
	}
	sinkOf := func(t *testing.T, b *nq.Backend) mq.Sink {
		t.Helper()
		s, err := b.OpenSink(t.Context(), mq.SinkConfig{Topic: "denied", Confirmation: mq.ConfirmDurable})
		if err != nil {
			t.Fatalf("OpenSink: %v", err)
		}
		t.Cleanup(func() { _ = s.Close() })
		return s
	}

	t.Run("Publish", func(t *testing.T) {
		s := sinkOf(t, open(t))
		start := time.Now()
		err := s.Publish(t.Context(), &mq.Message{Body: []byte("x")}, nil)
		forbidden(t, "Publish", err, time.Since(start))
	})
	t.Run("PublishBatch", func(t *testing.T) {
		s := sinkOf(t, open(t))
		start := time.Now()
		err := s.(mq.BatchSink).PublishBatch(t.Context(), []*mq.Message{{Body: []byte("a")}, {Body: []byte("b")}}, nil)
		forbidden(t, "PublishBatch", err, time.Since(start))
	})
	t.Run("PublishAsync", func(t *testing.T) {
		s := sinkOf(t, open(t))
		start := time.Now()
		done := make(chan error, 1)
		if err := s.(mq.AsyncSink).PublishAsync(t.Context(), &mq.Message{Body: []byte("x")}, nil, func(err error) { done <- err }); err != nil {
			t.Fatalf("PublishAsync: %v", err)
		}
		select {
		case err := <-done:
			forbidden(t, "PublishAsync", err, time.Since(start))
		case <-time.After(10 * time.Second):
			t.Fatal("the asynchronous publication was never resolved")
		}
	})
	// The callback of a refused asynchronous publication publishes again to
	// the refused subject. It ran on nats.go's callback goroutine, where the
	// second refusal queued behind it, so that publication waited out the
	// whole PublishTimeout.
	t.Run("PublishAsyncCallbackPublishesAgain", func(t *testing.T) {
		s := sinkOf(t, open(t))
		type again struct {
			err  error
			took time.Duration
		}
		done := make(chan again, 1)
		if err := s.(mq.AsyncSink).PublishAsync(t.Context(), &mq.Message{Body: []byte("x")}, nil, func(error) {
			start := time.Now()
			err := s.Publish(context.Background(), &mq.Message{Body: []byte("again")}, nil)
			done <- again{err, time.Since(start)}
		}); err != nil {
			t.Fatalf("PublishAsync: %v", err)
		}
		select {
		case a := <-done:
			forbidden(t, "Publish from a refused publication's callback", a.err, a.took)
		case <-time.After(10 * time.Second):
			t.Fatal("the asynchronous publication was never resolved")
		}
	})
	// The driver does not hear the refusal when the caller replaced its
	// error handler on a borrowed connection, and the publication then waits
	// out PublishTimeout. The connection's last error still says why.
	t.Run("PublishUnheard", func(t *testing.T) {
		nc, err := natsgo.Connect(url, natsgo.UserInfo("u", "p"))
		if err != nil {
			t.Fatalf("dialling: %v", err)
		}
		t.Cleanup(nc.Close)
		unheard := cfg
		unheard.PublishTimeout = 300 * time.Millisecond
		b, err := nq.BackendWithClient(t.Context(), nc, driverkit.ClientBorrowed, unheard)
		if err != nil {
			t.Fatalf("BackendWithClient: %v", err)
		}
		t.Cleanup(func() { _ = b.Close() })
		nc.SetErrorHandler(func(*natsgo.Conn, *natsgo.Subscription, error) {})
		s := sinkOf(t, b)
		ctx, cancel := context.WithTimeout(t.Context(), 300*time.Millisecond)
		defer cancel()
		start := time.Now()
		err = s.Publish(ctx, &mq.Message{Body: []byte("x")}, nil)
		forbidden(t, "Publish", err, time.Since(start))
	})
}

// TestARefusalOfTheTopicDoesNotFailAScheduledBatch holds that a scheduled
// PublishBatch is not answered by a refusal of its topic's subject. Its
// messages go to schedule subjects of their own, which the user may publish
// to, so the stream stores every one, while other publications to the topic
// are refused. The batch's wait was tracked under the topic's subject and any
// refusal answered a message, so in a test every one of 4,000 stored
// messages was reported a definite Forbidden, which a caller resends or gives
// up as lost.
func TestARefusalOfTheTopicDoesNotFailAScheduledBatch(t *testing.T) {
	url := deniedServer(t)
	b, err := nq.OpenBackend(t.Context(), nq.Config{
		Addrs: []string{url}, Username: "u", Password: "p",
		Stream: "PERM", SubjectPrefix: "perm", ManageStream: true,
		PublishTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })
	s, err := b.OpenSink(t.Context(), mq.SinkConfig{Topic: "denied", Confirmation: mq.ConfirmDurable})
	if err != nil {
		t.Fatalf("OpenSink: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	if err := s.Publish(t.Context(), &mq.Message{Body: []byte("x")}, &mq.PublishOptions{Delay: time.Hour}); err != nil {
		t.Fatalf("a scheduled publication to the topic = %v (code %v), want nil: its schedule subject is allowed", err, mq.CodeOf(err))
	}

	var stop atomic.Bool
	refusing := make(chan struct{})
	go func() {
		defer close(refusing)
		for !stop.Load() {
			_ = s.Publish(t.Context(), &mq.Message{Body: []byte("refused")}, nil)
		}
	}()
	defer func() { stop.Store(true); <-refusing }()

	accepted, refused := 0, 0
	var other []error
	for range 10 {
		ms := make([]*mq.Message, 200)
		for i := range ms {
			ms[i] = &mq.Message{Body: []byte("scheduled")}
		}
		err := s.(mq.BatchSink).PublishBatch(t.Context(), ms, &mq.PublishOptions{Delay: time.Hour})
		outcomes := make([]error, len(ms))
		var be *mq.BatchError
		switch {
		case err == nil:
		case errors.As(err, &be):
			outcomes = be.Errs
		default:
			for i := range outcomes {
				outcomes[i] = err
			}
		}
		for _, e := range outcomes {
			switch {
			case e == nil:
				accepted++
			case mq.CodeOf(e) == mq.CodeForbidden:
				refused++
			default:
				other = append(other, e)
			}
		}
	}
	if refused > 0 || len(other) > 0 {
		first := error(nil)
		if len(other) > 0 {
			first = other[0]
		}
		t.Fatalf("of 2,000 scheduled messages to an allowed schedule subject, %d were accepted, %d reported Forbidden and %d failed otherwise (the first: %v), while other publications to the topic were refused; want every one accepted",
			accepted, refused, len(other), first)
	}
}
