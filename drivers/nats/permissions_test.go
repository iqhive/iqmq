package nats_test

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	natsgo "github.com/nats-io/nats.go"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit"
	nq "github.com/iqhive/iqmq/drivers/nats"
)

// permissionedOptions is a server whose one user may publish only to the
// subjects in pub, and may subscribe to anything.
func permissionedOptions(pub ...string) *server.Options {
	return &server.Options{
		Host: "127.0.0.1", Port: -1, NoLog: true, NoSigs: true,
		Users: []*server.User{{
			Username: "u", Password: "p",
			Permissions: &server.Permissions{
				Publish:   &server.SubjectPermission{Allow: pub},
				Subscribe: &server.SubjectPermission{Allow: []string{">"}},
			},
		}},
	}
}

// permissionedServer starts a server whose one user may publish only under
// "allowed.>" and "_INBOX.>".
func permissionedServer(t *testing.T) *server.Server {
	t.Helper()
	srv, err := server.NewServer(permissionedOptions("allowed.>", "_INBOX.>"))
	if err != nil {
		t.Fatalf("server: %v", err)
	}
	go srv.Start()
	if !srv.ReadyForConnections(10 * time.Second) {
		t.Fatal("the server did not become ready")
	}
	t.Cleanup(func() { srv.Shutdown(); srv.WaitForShutdown() })
	return srv
}

// TestAPublicationThePermissionsRefuseIsForbidden holds that a publication the
// server refuses for permissions is reported as a definite CodeForbidden by
// every path that waits for the server. The server answers it with an -ERR
// written before the PONG of the confirmation flush, and the flush used to be
// all the driver waited for, so the message was discarded and Publish
// returned nil.
func TestAPublicationThePermissionsRefuseIsForbidden(t *testing.T) {
	srv := permissionedServer(t)
	url := srv.ClientURL()
	open := func(t *testing.T) (*nq.Backend, *mq.Client) {
		t.Helper()
		b, err := nq.OpenBackend(t.Context(), nq.Config{Addrs: []string{url}, Username: "u", Password: "p"})
		if err != nil {
			t.Fatalf("open: %v", err)
		}
		t.Cleanup(func() { _ = b.Close() })
		return b, mq.NewClient(b)
	}
	forbidden := func(t *testing.T, what string, err error) {
		t.Helper()
		var oe *mq.OpError
		if mq.CodeOf(err) != mq.CodeForbidden || mq.IsAmbiguous(err) || !errors.As(err, &oe) || oe.Operation != mq.OpPublish {
			t.Fatalf("%s to a subject the user may not publish to = %v (code %v, ambiguous %v), want a definite Forbidden on publish",
				what, err, mq.CodeOf(err), mq.IsAmbiguous(err))
		}
	}

	t.Run("Publish", func(t *testing.T) {
		_, c := open(t)
		cons := consumerOn(t, c, "denied.orders", "watch")
		forbidden(t, "Publish", c.Publish(t.Context(), "denied.orders", &mq.Message{Body: []byte("x")}))
		ctx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
		defer cancel()
		dst := make([]mq.Delivery, 1)
		if n, _ := cons.Receive(ctx, dst); n != 0 {
			t.Fatal("the server delivered a publication it refused, so the test proves nothing")
		}
		if err := c.Publish(t.Context(), "allowed.orders", &mq.Message{Body: []byte("x")}); err != nil {
			t.Fatalf("a permitted publication after a refused one = %v, want nil", err)
		}
	})
	t.Run("PublishBatch", func(t *testing.T) {
		_, c := open(t)
		p, err := c.Publisher(t.Context(), mq.PublisherConfig{Topic: "denied.batch"})
		if err != nil {
			t.Fatalf("publisher: %v", err)
		}
		t.Cleanup(func() { _ = p.Close() })
		err = p.PublishBatch(t.Context(), []*mq.Message{{Body: []byte("a")}, {Body: []byte("b")}})
		forbidden(t, "PublishBatch", err)
	})
	t.Run("Respond", func(t *testing.T) {
		b, _ := open(t)
		forbidden(t, "Respond", b.Respond(t.Context(), "denied.reply", &mq.Message{Body: []byte("x")}))
	})
	// The refusal stays the connection's last error after the operator
	// grants the subject, and a publication made then is not refused.
	t.Run("AfterAGrant", func(t *testing.T) {
		_, c := open(t)
		forbidden(t, "Publish before the grant", c.Publish(t.Context(), "granted.orders", &mq.Message{Body: []byte("x")}))
		opts := permissionedOptions("allowed.>", "_INBOX.>", "granted.>")
		opts.Port = srv.Addr().(*net.TCPAddr).Port
		if err := srv.ReloadOptions(opts); err != nil {
			t.Fatalf("granting the subject: %v", err)
		}
		if err := c.Publish(t.Context(), "granted.orders", &mq.Message{Body: []byte("x")}); err != nil {
			t.Fatalf("a publication after the grant = %v (code %v), want nil", err, mq.CodeOf(err))
		}
	})
	// A request is a publication too. It used to wait out its whole context
	// for a reply that could never come and was then reported an ambiguous
	// DeadlineExceeded, which a retry loop retries for ever.
	t.Run("Request", func(t *testing.T) {
		b, c := open(t)
		answers := consumerOn(t, c, "denied.svc", "responders")
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer cancel()
		start := time.Now()
		_, err := b.Request(ctx, "denied.svc", &mq.Message{Body: []byte("x")})
		forbidden(t, "Request", err)
		if took := time.Since(start); took > 2*time.Second {
			t.Fatalf("a Request the server refused took %v of its 5s, want it ended by the refusal", took.Round(time.Millisecond))
		}
		rctx, rcancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
		defer rcancel()
		if n, _ := answers.Receive(rctx, make([]mq.Delivery, 1)); n != 0 {
			t.Fatal("the server delivered a request it refused, so the test proves nothing")
		}
	})
	// The driver does not hear the refusal when the caller replaced its
	// error handler on a borrowed connection, and the request then waits out
	// its context. The connection's last error still says why.
	t.Run("RequestUnheard", func(t *testing.T) {
		nc, err := natsgo.Connect(url, natsgo.UserInfo("u", "p"))
		if err != nil {
			t.Fatalf("dialling: %v", err)
		}
		t.Cleanup(nc.Close)
		b, err := nq.BackendWithClient(t.Context(), nc, driverkit.ClientBorrowed, nq.Config{Addrs: []string{url}})
		if err != nil {
			t.Fatalf("BackendWithClient: %v", err)
		}
		t.Cleanup(func() { _ = b.Close() })
		nc.SetErrorHandler(func(*natsgo.Conn, *natsgo.Subscription, error) {})
		ctx, cancel := context.WithTimeout(t.Context(), 300*time.Millisecond)
		defer cancel()
		_, err = b.Request(ctx, "denied.svc", &mq.Message{Body: []byte("x")})
		forbidden(t, "Request", err)
	})
}

// subscribeOnlyOptions is a server whose one user may publish anywhere and
// subscribe only to the subjects in sub.
func subscribeOnlyOptions(sub ...string) *server.Options {
	return &server.Options{
		Host: "127.0.0.1", Port: -1, NoLog: true, NoSigs: true,
		Users: []*server.User{{
			Username: "u", Password: "p",
			Permissions: &server.Permissions{
				Publish:   &server.SubjectPermission{Allow: []string{">"}},
				Subscribe: &server.SubjectPermission{Allow: sub},
			},
		}},
	}
}

// subscribeOnlyServer starts a server whose one user may publish anywhere and
// subscribe only under "allowed.>" and "_INBOX.>".
func subscribeOnlyServer(t *testing.T) *server.Server {
	t.Helper()
	srv, err := server.NewServer(subscribeOnlyOptions("allowed.>", "_INBOX.>"))
	if err != nil {
		t.Fatalf("server: %v", err)
	}
	go srv.Start()
	if !srv.ReadyForConnections(10 * time.Second) {
		t.Fatal("the server did not become ready")
	}
	t.Cleanup(func() { srv.Shutdown(); srv.WaitForShutdown() })
	return srv
}

// TestASubscriptionThePermissionsRefuseIsForbidden holds that a consumer of a
// topic the user may not subscribe to is refused when it opens, with a
// definite CodeForbidden. The server refuses the subscription with an
// asynchronous -ERR, and the consumer used to open with a nil error and then
// receive nothing, while every publication to the topic went to the other
// subscribers and was lost here.
func TestASubscriptionThePermissionsRefuseIsForbidden(t *testing.T) {
	srv := subscribeOnlyServer(t)
	b, err := nq.OpenBackend(t.Context(), nq.Config{Addrs: []string{srv.ClientURL()}, Username: "u", Password: "p"})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })
	c := mq.NewClient(b)
	for range 2 {
		h, err := c.Consumer(t.Context(), mq.ConsumerConfig{Topic: "denied.orders", Subscription: "workers"})
		var oe *mq.OpError
		if mq.CodeOf(err) != mq.CodeForbidden || mq.IsAmbiguous(err) || !errors.As(err, &oe) || oe.Operation != mq.OpOpen {
			if err == nil {
				_ = h.Close()
			}
			t.Fatalf("a consumer of a topic the user may not subscribe to = %v (code %v), want a definite Forbidden on open",
				err, mq.CodeOf(err))
		}
	}
	allowed := consumerOn(t, c, "allowed.orders", "workers")
	if err := c.Publish(t.Context(), "allowed.orders", &mq.Message{Body: []byte("x")}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	receiveOne(t, allowed, 5*time.Second)

	// The refusal stays the connection's last error after the operator
	// grants the subject, and a consumer opened then is not refused.
	opts := subscribeOnlyOptions("allowed.>", "_INBOX.>", "denied.>")
	opts.Port = srv.Addr().(*net.TCPAddr).Port
	if err := srv.ReloadOptions(opts); err != nil {
		t.Fatalf("granting the subject: %v", err)
	}
	granted := consumerOn(t, c, "denied.orders", "workers")
	if err := c.Publish(t.Context(), "denied.orders", &mq.Message{Body: []byte("x")}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	receiveOne(t, granted, 5*time.Second)
}

// TestASubscriptionTheServerRefusesForItsLimitIsResourceExhausted holds that a
// consumer the server will not subscribe because the connection already holds
// as many subscriptions as the server allows fails to open with a retryable
// CodeResourceExhausted, at every attempt, on a connection the driver dialled
// and on one handed to it, and that one opened once a subscription has closed
// is not refused. The server refuses such a subscription with an -ERR and
// keeps the connection, as it does one it refuses for permissions, and the
// consumer opened with a nil error and received nothing: every publication to
// its topic for its queue group was lost.
func TestASubscriptionTheServerRefusesForItsLimitIsResourceExhausted(t *testing.T) {
	for _, handedOver := range []bool{false, true} {
		name := "dialled"
		if handedOver {
			name = "handed over"
		}
		t.Run(name, func(t *testing.T) {
			srv, err := server.NewServer(&server.Options{Host: "127.0.0.1", Port: -1, NoLog: true, NoSigs: true, MaxSubs: 1})
			if err != nil {
				t.Fatalf("server: %v", err)
			}
			go srv.Start()
			if !srv.ReadyForConnections(10 * time.Second) {
				t.Fatal("the server did not become ready")
			}
			t.Cleanup(func() { srv.Shutdown(); srv.WaitForShutdown() })
			cfg := nq.Config{Addrs: []string{srv.ClientURL()}}
			var b *nq.Backend
			if handedOver {
				nc, err := natsgo.Connect(srv.ClientURL())
				if err != nil {
					t.Fatalf("dialling: %v", err)
				}
				t.Cleanup(nc.Close)
				b, err = nq.BackendWithClient(t.Context(), nc, driverkit.ClientBorrowed, cfg)
				if err != nil {
					t.Fatalf("BackendWithClient: %v", err)
				}
			} else if b, err = nq.OpenBackend(t.Context(), cfg); err != nil {
				t.Fatalf("open: %v", err)
			}
			t.Cleanup(func() { _ = b.Close() })
			c := mq.NewClient(b)

			first, err := c.Consumer(t.Context(), mq.ConsumerConfig{Topic: "orders.one", Subscription: "workers"})
			if err != nil {
				t.Fatalf("the consumer within the limit: %v", err)
			}
			for i := range 3 {
				h, err := c.Consumer(t.Context(), mq.ConsumerConfig{Topic: "orders.two", Subscription: "workers"})
				var oe *mq.OpError
				if mq.CodeOf(err) != mq.CodeResourceExhausted || !mq.IsRetryable(err) || !errors.As(err, &oe) || oe.Operation != mq.OpOpen {
					if err == nil {
						_ = h.Close()
					}
					t.Fatalf("attempt %d at a consumer past the connection's subscription limit = %v (code %v, retryable %v), want a retryable ResourceExhausted on open",
						i+1, err, mq.CodeOf(err), mq.IsRetryable(err))
				}
			}

			// The refusal stays the connection's last error once the
			// first consumer has closed, and a consumer opened then is not
			// refused. The refusal does not name its subscription, so one
			// the driver heard late can be read as this one's, which a
			// retry gets past.
			if err := first.Close(); err != nil {
				t.Fatalf("closing the first consumer: %v", err)
			}
			var second *mq.ConsumerHandle
			for attempt := 1; second == nil; attempt++ {
				h, err := c.Consumer(t.Context(), mq.ConsumerConfig{Topic: "orders.two", Subscription: "workers"})
				switch {
				case err == nil:
					second = h
					t.Cleanup(func() { _ = h.Close() })
				case attempt == 3 || mq.CodeOf(err) != mq.CodeResourceExhausted:
					t.Fatalf("a consumer opened once the connection was under its subscription limit = %v (code %v) at attempt %d",
						err, mq.CodeOf(err), attempt)
				}
			}
			if err := c.Publish(t.Context(), "orders.two", &mq.Message{Body: []byte("x")}); err != nil {
				t.Fatalf("Publish: %v", err)
			}
			receiveOne(t, second, 5*time.Second)
		})
	}
}
