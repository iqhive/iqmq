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
	sjs "github.com/nats-io/nats.go/jetstream"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit"
	nq "github.com/iqhive/iqmq/drivers/jetstream"
)

// deniedServer starts a JetStream server whose user "u" may publish anywhere
// but "perm.denied", which a stream under the prefix "perm" captures.
func deniedServer(t *testing.T) string { return denyingServer(t, "perm.denied") }

// denyingServer starts a JetStream server whose user "u" may publish anywhere
// but the subjects deny names, and whose user "admin" may publish anywhere.
func denyingServer(t *testing.T, deny ...string) string { return denyingServerIn(t, "", deny...) }

// denyingServerIn is denyingServer for a server whose JetStream domain is
// domain, or that has none when it is empty.
func denyingServerIn(t *testing.T, domain string, deny ...string) string {
	return permittingServer(t, domain, &server.Permissions{
		Publish: &server.SubjectPermission{Allow: []string{">"}, Deny: deny},
	})
}

// permittingServer starts a JetStream server whose JetStream domain is
// domain, or that has none when it is empty, whose user "u" has perms, and
// whose user "admin" may do anything.
func permittingServer(t *testing.T, domain string, perms *server.Permissions) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "iqmq-js-perm-*")
	if err != nil {
		t.Fatalf("store dir: %v", err)
	}
	srv, err := server.NewServer(&server.Options{
		Host: "127.0.0.1", Port: -1, NoLog: true, NoSigs: true,
		JetStream: true, StoreDir: dir, JetStreamDomain: domain,
		Users: []*server.User{
			{Username: "u", Password: "p", Permissions: perms},
			{Username: "admin", Password: "p"},
		},
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

// TestARefusalOfTheLocalAPIDoesNotEndARequestToAnotherDomain holds that a
// JetStream API request made under Config.Domain, on a server that does not
// own that domain, is not ended by the server's refusal of a request to its
// own API for a stream of the same name. That server does not map the
// request, so "$JS.API.<rest>" is its local API and not this request's
// subject: the driver read such a refusal, made for another request on the
// connection, as this one's, a definite Forbidden for a request nobody
// refused. A responder on the domain's API subjects that never answers
// stands in for a slow hub, so the request waits out its bound.
func TestARefusalOfTheLocalAPIDoesNotEndARequestToAnotherDomain(t *testing.T) {
	url := denyingServerIn(t, "leaf", "$JS.API.STREAM.>")
	admin, err := natsgo.Connect(url, natsgo.UserInfo("admin", "p"))
	if err != nil {
		t.Fatalf("dialling as admin: %v", err)
	}
	t.Cleanup(admin.Close)
	arrived := make(chan struct{}, 8)
	if _, err := admin.Subscribe("$JS.hub.API.>", func(*natsgo.Msg) { arrived <- struct{}{} }); err != nil {
		t.Fatalf("the silent hub: %v", err)
	}
	if err := admin.Flush(); err != nil {
		t.Fatalf("the silent hub: %v", err)
	}
	nc, err := natsgo.Connect(url, natsgo.UserInfo("u", "p"))
	if err != nil {
		t.Fatalf("dialling: %v", err)
	}
	t.Cleanup(nc.Close)

	type opened struct {
		err  error
		took time.Duration
	}
	res := make(chan opened, 1)
	start := time.Now()
	go func() {
		b, err := nq.BackendWithClient(context.Background(), nc, driverkit.ClientBorrowed, nq.Config{
			Stream: "API", SubjectPrefix: "api", ManageStream: true, Domain: "hub", PublishTimeout: time.Second,
		})
		if err == nil {
			_ = b.Close()
		}
		res <- opened{err, time.Since(start)}
	}()
	select {
	case <-arrived:
	case <-time.After(5 * time.Second):
		t.Fatal("the request to the hub's API never arrived")
	}
	// Another request on the connection, to the server's own API for a
	// stream of the same name, which the server refuses.
	if _, err := nc.Request("$JS.API.STREAM.INFO.API", nil, 300*time.Millisecond); err == nil {
		t.Fatal("the server answered a request to its own stream API the user may not publish to")
	}
	o := <-res
	if mq.CodeOf(o.err) != mq.CodeDeadlineExceeded {
		t.Fatalf("Open under another domain while the server refused another request = %v (code %v) after %v, want the request's own DeadlineExceeded: the refusal was not its",
			o.err, mq.CodeOf(o.err), o.took.Round(time.Millisecond))
	}
}

// deniedDeadLetter opens a backend on deniedServer whose dead-letter topic is
// the one subject the user may not publish to.
func deniedDeadLetter(t *testing.T) *nq.Backend {
	t.Helper()
	b, err := nq.OpenBackend(t.Context(), nq.Config{
		Addrs: []string{deniedServer(t)}, Username: "u", Password: "p",
		Stream: "PERM", SubjectPrefix: "perm", ManageStream: true,
		DeadLetterTopic: "denied",
		PublishTimeout:  5 * time.Second,
		AckWait:         30 * time.Second,
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })
	return b
}

// TestADeadLetterCopyThePermissionsRefuseIsForbidden holds that Reject, whose
// dead-letter copy the server refuses for permissions, is a definite
// CodeForbidden reported when the refusal arrives, and leaves the delivery
// settleable. The server discards the copy and sends no PubAck, and the copy
// waited out Config.PublishTimeout and was reported an ambiguous
// DeadlineExceeded, so the delivery could not be settled again until its
// lease ran out, and the same happened at every redelivery.
func TestADeadLetterCopyThePermissionsRefuseIsForbidden(t *testing.T) {
	c := mq.NewClient(deniedDeadLetter(t))
	cons := consumerOn(t, c, "orders", "workers")
	publish(t, c, "orders", &mq.Message{Body: []byte("x")})
	d := receiveOne(t, cons, 10*time.Second)

	start := time.Now()
	err := d.Reject()
	took := time.Since(start)
	if mq.CodeOf(err) != mq.CodeForbidden || mq.IsAmbiguous(err) || took > 2500*time.Millisecond {
		t.Fatalf("Reject whose dead-letter copy the server refused = %v (code %v, ambiguous %v) after %v of the 5s PublishTimeout, want a definite Forbidden when the refusal arrives",
			err, mq.CodeOf(err), mq.IsAmbiguous(err), took.Round(time.Millisecond))
	}
	if err := d.Ack(); err != nil {
		t.Fatalf("Ack after a Reject the server refused = %v (code %v), want the delivery still settleable", err, mq.CodeOf(err))
	}
}

// TestADeadLetterCopyWhoseConsumerListIsRefusedIsForbidden holds that Reject
// on a stream with interest retention, which lists the stream's consumers
// before its copy goes out, is a definite CodeForbidden when the server
// refuses the list for permissions, reported when the refusal arrives, and
// leaves the delivery settleable: nothing was sent.
func TestADeadLetterCopyWhoseConsumerListIsRefusedIsForbidden(t *testing.T) {
	url := denyingServer(t, "$JS.API.CONSUMER.LIST.>")
	admin, err := natsgo.Connect(url, natsgo.UserInfo("admin", "p"))
	if err != nil {
		t.Fatalf("admin: %v", err)
	}
	t.Cleanup(admin.Close)
	jsc, err := sjs.New(admin)
	if err != nil {
		t.Fatalf("jetstream context: %v", err)
	}
	if _, err := jsc.CreateStream(t.Context(), sjs.StreamConfig{
		Name: "PERM", Subjects: []string{"perm.>"}, Retention: sjs.InterestPolicy,
	}); err != nil {
		t.Fatalf("creating the stream: %v", err)
	}
	b, err := nq.OpenBackend(t.Context(), nq.Config{
		Addrs: []string{url}, Username: "u", Password: "p",
		Stream: "PERM", SubjectPrefix: "perm", DeadLetterTopic: "dead",
		PublishTimeout: 5 * time.Second, AckWait: 30 * time.Second,
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })
	c := mq.NewClient(b)
	cons := consumerOn(t, c, "orders", "workers")
	publish(t, c, "orders", &mq.Message{Body: []byte("x")})
	d := receiveOne(t, cons, 10*time.Second)

	start := time.Now()
	err = d.Reject()
	took := time.Since(start)
	if mq.CodeOf(err) != mq.CodeForbidden || mq.IsAmbiguous(err) || took > 2500*time.Millisecond {
		t.Fatalf("Reject whose consumer list the server refused = %v (code %v, ambiguous %v) after %v of the 5s PublishTimeout, want a definite Forbidden when the refusal arrives",
			err, mq.CodeOf(err), mq.IsAmbiguous(err), took.Round(time.Millisecond))
	}
	if err := d.Ack(); err != nil {
		t.Fatalf("Ack after a Reject the server refused = %v (code %v), want the delivery still settleable", err, mq.CodeOf(err))
	}
}

// TestAnUndecodableMessageWhoseCopyIsRefusedDoesNotHoldUpTheSubscription holds
// that an undecodable message whose dead-letter copy the server refuses for
// permissions holds up the consume callback, and every message behind it, no
// longer than the refusal takes to arrive, and that health reports the
// refusal. The callback waited out Config.PublishTimeout for a PubAck the
// refusal had ruled out, at every redelivery of the message, and health then
// reported that wait's DeadlineExceeded.
func TestAnUndecodableMessageWhoseCopyIsRefusedDoesNotHoldUpTheSubscription(t *testing.T) {
	b := deniedDeadLetter(t)
	c := mq.NewClient(b)
	cons := consumerWith(t, c, mq.ConsumerConfig{
		Topic: "orders", Subscription: "workers", Prefetch: 2,
		Metadata: mq.MetadataPortableEnvelope,
	})
	publishPoison(t, c, "orders")
	start := time.Now()
	publishGood(t, c, "orders")
	d := receiveOne(t, cons, 15*time.Second)
	took := time.Since(start)
	_ = d.Ack()
	if took > 2500*time.Millisecond {
		t.Fatalf("the message behind an undecodable one whose dead-letter copy the server refused arrived after %v of the 5s PublishTimeout, want it held up only until the refusal arrived",
			took.Round(time.Millisecond))
	}
	if h := b.Health(); mq.CodeOf(h.Err) != mq.CodeForbidden {
		t.Fatalf("health after a refused dead-letter copy is %v %v (code %v), want the refusal (Forbidden)", h.State, h.Err, mq.CodeOf(h.Err))
	}
}

// TestAJetStreamAPIRequestThePermissionsRefuseIsForbidden holds that Open and
// OpenSource, whose JetStream API request the server refuses for permissions,
// fail with a definite CodeForbidden when the refusal arrives. The server
// discards the request and sends no reply, so each one waited out
// Config.PublishTimeout and failed with a retryable DeadlineExceeded, which a
// caller retried for ever at that cost per attempt. A refusal to create a
// durable still binds to the durable when it exists, as any failure to
// create one does.
func TestAJetStreamAPIRequestThePermissionsRefuseIsForbidden(t *testing.T) {
	cfg := nq.Config{
		Username: "u", Password: "p",
		Stream: "API", SubjectPrefix: "api", ManageStream: true,
		PublishTimeout: 5 * time.Second,
	}
	open := func(t *testing.T, url string, cfg nq.Config) (*nq.Backend, error) {
		t.Helper()
		cfg.Addrs = []string{url}
		b, err := nq.OpenBackend(t.Context(), cfg)
		if err == nil {
			t.Cleanup(func() { _ = b.Close() })
		}
		return b, err
	}
	forbidden := func(t *testing.T, what string, err error, took time.Duration) {
		t.Helper()
		if mq.CodeOf(err) != mq.CodeForbidden || mq.IsRetryable(err) || took > 2500*time.Millisecond {
			t.Fatalf("%s whose API request the server refused = %v (code %v, retryable %v) after %v of the 5s PublishTimeout, want a definite Forbidden when the refusal arrives",
				what, err, mq.CodeOf(err), mq.IsRetryable(err), took.Round(time.Millisecond))
		}
	}
	src := mq.SourceConfig{Topic: "orders", Subscription: "workers", Prefetch: 1}

	t.Run("Open", func(t *testing.T) {
		url := denyingServer(t, "$JS.API.STREAM.>")
		start := time.Now()
		_, err := open(t, url, cfg)
		forbidden(t, "Open", err, time.Since(start))
	})
	// Under Config.Domain the requests go to $JS.<domain>.API. subjects. The
	// server whose domain that is maps them to $JS.API. before it checks
	// permissions, so its refusal names the mapped subject; any other server,
	// a leaf node connected to that domain's hub say, refuses the subject the
	// client wrote. Only the second used to end the request.
	t.Run("Open under its server's domain", func(t *testing.T) {
		url := denyingServerIn(t, "hub", "$JS.API.STREAM.>")
		inDomain := cfg
		inDomain.Domain = "hub"
		start := time.Now()
		_, err := open(t, url, inDomain)
		forbidden(t, "Open under its server's domain", err, time.Since(start))
	})
	t.Run("Open under another server's domain", func(t *testing.T) {
		url := denyingServer(t, "$JS.hub.API.STREAM.>")
		inDomain := cfg
		inDomain.Domain = "hub"
		start := time.Now()
		_, err := open(t, url, inDomain)
		forbidden(t, "Open under another server's domain", err, time.Since(start))
	})
	t.Run("OpenSource", func(t *testing.T) {
		b, err := open(t, denyingServer(t, "$JS.API.CONSUMER.>"), cfg)
		if err != nil {
			t.Fatalf("open: %v", err)
		}
		start := time.Now()
		s, err := b.OpenSource(t.Context(), src)
		if err == nil {
			_ = s.Close()
		}
		forbidden(t, "OpenSource", err, time.Since(start))
	})
	t.Run("OpenSource of a durable it may not create", func(t *testing.T) {
		url := denyingServer(t, "$JS.API.CONSUMER.CREATE.>")
		admin := cfg
		admin.Username = "admin"
		a, err := open(t, url, admin)
		if err != nil {
			t.Fatalf("open as admin: %v", err)
		}
		s, err := a.OpenSource(t.Context(), src)
		if err != nil {
			t.Fatalf("creating the durable as admin: %v", err)
		}
		_ = s.Close()

		b, err := open(t, url, cfg)
		if err != nil {
			t.Fatalf("open: %v", err)
		}
		start := time.Now()
		s, err = b.OpenSource(t.Context(), src)
		took := time.Since(start)
		if err != nil || took > 2500*time.Millisecond {
			t.Fatalf("OpenSource of a durable that exists, whose creation the server refused = %v (code %v) after %v of the 5s PublishTimeout, want it bound to the durable when the refusal arrives",
				err, mq.CodeOf(err), took.Round(time.Millisecond))
		}
		_ = s.Close()
	})
	// The durable exists with other settings, so the create is refused
	// because it exists and the open has to read the durable to bind to it.
	// The lookup's refusal is the answer, not the create's Conflict.
	t.Run("OpenSource of a durable it may not read", func(t *testing.T) {
		url := denyingServer(t, "$JS.API.CONSUMER.INFO.>")
		admin := cfg
		admin.Username = "admin"
		a, err := open(t, url, admin)
		if err != nil {
			t.Fatalf("open as admin: %v", err)
		}
		other := src
		other.Prefetch = 2
		s, err := a.OpenSource(t.Context(), other)
		if err != nil {
			t.Fatalf("creating the durable as admin: %v", err)
		}
		_ = s.Close()

		b, err := open(t, url, cfg)
		if err != nil {
			t.Fatalf("open: %v", err)
		}
		start := time.Now()
		s, err = b.OpenSource(t.Context(), src)
		if err == nil {
			_ = s.Close()
		}
		forbidden(t, "OpenSource of a durable that exists", err, time.Since(start))
	})
}

// TestAConsumerWhosePullOrSubscriptionThePermissionsRefuseIsForbidden holds
// that a consumer whose first pull request, or whose subscription to the
// inbox the pulled messages come to, the server refuses for permissions fails
// to open with a definite CodeForbidden. The server refuses either with an
// -ERR and keeps the connection, and nats.go keeps it only as the
// connection's last error, so the consumer opened with a nil error and
// received nothing: every pull was refused, or the messages pulled went to a
// subscription the server never made.
func TestAConsumerWhosePullOrSubscriptionThePermissionsRefuseIsForbidden(t *testing.T) {
	for _, tc := range []struct {
		name, domain string
		perms        *server.Permissions
	}{
		{"pull", "", &server.Permissions{
			Publish: &server.SubjectPermission{Allow: []string{">"}, Deny: []string{"$JS.API.CONSUMER.MSG.NEXT.>"}},
		}},
		// The server whose domain the client names maps $JS.<domain>.API.
		// to $JS.API. before it checks permissions, so its refusal names
		// the mapped subject.
		{"pull under its server's domain", "hub", &server.Permissions{
			Publish: &server.SubjectPermission{Allow: []string{">"}, Deny: []string{"$JS.API.CONSUMER.MSG.NEXT.>"}},
		}},
		// The API requests' replies come to one subscription of two tokens
		// under the inbox prefix, and the consume inbox has one, so a
		// server that allows the first and refuses the second lets every
		// request through and the consumer receive nothing.
		{"subscription", "", &server.Permissions{
			Publish:   &server.SubjectPermission{Allow: []string{">"}},
			Subscribe: &server.SubjectPermission{Allow: []string{">"}, Deny: []string{"_INBOX.*"}},
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, err := nq.Open(t.Context(), nq.Config{
				Addrs: []string{permittingServer(t, tc.domain, tc.perms)}, Username: "u", Password: "p",
				Stream: "PULL", SubjectPrefix: "pull", ManageStream: true, Domain: tc.domain,
				PublishTimeout: 5 * time.Second,
			})
			if err != nil {
				t.Fatalf("open: %v", err)
			}
			t.Cleanup(func() { _ = c.Close() })
			start := time.Now()
			h, err := c.Consumer(t.Context(), mq.ConsumerConfig{Topic: "orders", Subscription: "workers"})
			took := time.Since(start)
			if !failedOpen(err, mq.CodeForbidden, false) || took > 2500*time.Millisecond {
				if err == nil {
					_ = h.Close()
				}
				t.Fatalf("a consumer whose %s the server refused = %v (code %v, retryable %v) after %v, want a definite Forbidden on open",
					tc.name, err, mq.CodeOf(err), mq.IsRetryable(err), took.Round(time.Millisecond))
			}
		})
	}
}

// TestAConsumerTheServerWillNotSubscribeForItsLimitIsResourceExhausted holds
// that a consumer whose subscription the server refuses because the
// connection already holds as many as it allows fails to open with a
// retryable CodeResourceExhausted, at every attempt, on a connection the
// driver dialled and on one handed to it, and that one opened once a
// consumer has closed is not refused. The server refuses such a subscription
// with an -ERR and keeps the connection, and the consumer opened with a nil
// error and received nothing, while the messages it pulled went nowhere.
func TestAConsumerTheServerWillNotSubscribeForItsLimitIsResourceExhausted(t *testing.T) {
	for _, handedOver := range []bool{false, true} {
		name := "dialled"
		if handedOver {
			name = "handed over"
		}
		t.Run(name, func(t *testing.T) {
			// Open's API requests take one subscription, nats.go's reply
			// subscription, and the first consumer a second.
			srv, err := server.NewServer(&server.Options{
				Host: "127.0.0.1", Port: -1, NoLog: true, NoSigs: true,
				JetStream: true, StoreDir: t.TempDir(), MaxSubs: 2,
			})
			if err != nil {
				t.Fatalf("server: %v", err)
			}
			go srv.Start()
			if !srv.ReadyForConnections(10 * time.Second) {
				t.Fatal("the server did not become ready")
			}
			t.Cleanup(func() { srv.Shutdown(); srv.WaitForShutdown() })
			cfg := nq.Config{
				Addrs: []string{srv.ClientURL()}, Stream: "LIMIT", SubjectPrefix: "limit",
				ManageStream: true, AckWait: 5 * time.Second,
			}
			c := dialledOrHandedOver(t, handedOver, cfg)

			first, err := c.Consumer(t.Context(), mq.ConsumerConfig{Topic: "one", Subscription: "first"})
			if err != nil {
				t.Fatalf("the consumer within the limit: %v", err)
			}
			for i := range 3 {
				h, err := c.Consumer(t.Context(), mq.ConsumerConfig{Topic: "two", Subscription: "second"})
				if !failedOpen(err, mq.CodeResourceExhausted, true) {
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
				h, err := c.Consumer(t.Context(), mq.ConsumerConfig{Topic: "two", Subscription: "second"})
				switch {
				case err == nil:
					second = h
					t.Cleanup(func() { _ = h.Close() })
				case attempt == 3 || mq.CodeOf(err) != mq.CodeResourceExhausted:
					t.Fatalf("a consumer opened once the connection was under its subscription limit = %v (code %v) at attempt %d",
						err, mq.CodeOf(err), attempt)
				}
			}
			publish(t, c, "two", &mq.Message{Body: []byte("x")})
			d := receiveOne(t, second, 10*time.Second)
			_ = d.Ack()
		})
	}
}

// failedOpen reports whether err is an open's failure with code, retryable
// as retryable says.
func failedOpen(err error, code mq.ErrorCode, retryable bool) bool {
	oe, ok := errors.AsType[*mq.OpError](err)
	return ok && oe.Operation == mq.OpOpen && mq.CodeOf(err) == code && mq.IsRetryable(err) == retryable
}

// dialledOrHandedOver opens a client on cfg, over a connection the driver
// dials or one the test dialled and lends it, closed on cleanup.
func dialledOrHandedOver(t *testing.T, handedOver bool, cfg nq.Config) *mq.Client {
	t.Helper()
	open := func() (*mq.Client, error) { return nq.Open(t.Context(), cfg) }
	if handedOver {
		nc, err := natsgo.Connect(cfg.Addrs[0])
		if err != nil {
			t.Fatalf("dialling: %v", err)
		}
		t.Cleanup(nc.Close)
		open = func() (*mq.Client, error) { return nq.OpenWithClient(t.Context(), nc, driverkit.ClientBorrowed, cfg) }
	}
	c, err := open()
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}
