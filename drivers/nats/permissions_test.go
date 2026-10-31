package nats_test

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"

	mq "github.com/iqhive/iqmq"
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
}
