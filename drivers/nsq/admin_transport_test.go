package nsq_test

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httptrace"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	mq "bitbucket.org/iqhive/iqmq"
	nq "bitbucket.org/iqhive/iqmq/drivers/nsq"
)

// connStub is a fake nsqd that counts the connections it accepts and reports
// each one it sees closed. It answers every request as nsqd answers
// /topic/create: 200 and no body, which is the response shape net/http
// returns to its idle pool before the caller has it.
type connStub struct {
	srv    *httptest.Server
	opened atomic.Int64
	closed chan struct{} // one value per connection the server saw close
}

func newConnStub(t *testing.T) *connStub {
	t.Helper()
	s := &connStub{closed: make(chan struct{}, 64)}
	s.srv = httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	// httptest chains its own connection tracking after this hook.
	s.srv.Config.ConnState = func(_ net.Conn, st http.ConnState) {
		switch st {
		case http.StateNew:
			s.opened.Add(1)
		case http.StateClosed, http.StateHijacked:
			select {
			case s.closed <- struct{}{}:
			default:
			}
		default:
		}
	}
	s.srv.Start()
	t.Cleanup(s.srv.Close)
	return s
}

func (s *connStub) addr() string { return strings.TrimPrefix(s.srv.URL, "http://") }

// TestClosingTheProcessWideTransportLeavesTheAdminConnectionAlone is the
// defect GitHub found as "EnsureTopic: nsq: admin: Unavailable: Post ...:
// net/http: HTTP/1.x transport connection broken: http: CloseIdleConnections
// called" in TestEveryWritingAdminCallReachesEveryNSQD.
//
// The admin client used to have no Transport of its own, so its connections
// sat in http.DefaultTransport's pool beside everyone else's, and a parallel
// test's httptest.Server.Close -- which closes DefaultTransport's idle
// connections -- could close one between net/http returning it to the pool
// and handing its bodiless response to the POST waiting on it. That window is
// a few instructions wide, so this test does not race for it: it asks the
// question the window depends on, which is whether a close of the
// process-wide pool reaches the admin surface's connection at all. Two calls
// either side of that close must travel on one connection.
//
// Deliberately NOT parallel: it closes the idle connections of the pool every
// other HTTP client in this binary shares, which is the very thing it proves
// the driver is immune to and the rest of the package's tests are not.
func TestClosingTheProcessWideTransportLeavesTheAdminConnectionAlone(t *testing.T) {
	d := newConnStub(t)
	admin := adminOver(t, d.addr())
	dt, ok := http.DefaultTransport.(*http.Transport)
	if !ok {
		t.Fatalf("http.DefaultTransport is a %T, not an *http.Transport; the test cannot close its pool", http.DefaultTransport)
	}

	if err := admin.EnsureTopic(t.Context(), mq.TopicSpec{Name: "orders"}); err != nil {
		t.Fatalf("first EnsureTopic: %v", err)
	}
	dt.CloseIdleConnections()
	if err := admin.EnsureTopic(t.Context(), mq.TopicSpec{Name: "orders"}); err != nil {
		t.Fatalf("EnsureTopic after the process-wide pool was closed: %v", err)
	}
	if n := d.opened.Load(); n != 1 {
		t.Errorf("two admin calls either side of http.DefaultTransport.CloseIdleConnections opened %d "+
			"connections, want 1: the admin surface's connection is in the process-wide pool, where anything "+
			"else in the process that tidies that pool (httptest.Server.Close does) can fail a POST in flight", n)
	}
}

// TestCloseReleasesTheAdminConnections: the admin transport is the backend's
// own, so Close is what gives its pooled connections back. Without it every
// closed backend would hold a socket to every nsqd it ever administered until
// the idle timeout.
func TestCloseReleasesTheAdminConnections(t *testing.T) {
	t.Parallel()
	d := newConnStub(t)
	b, _ := newBackendRig(t, func(c *nq.Config) { c.AdminAddrs = []string{d.addr()} })
	admin, err := b.Admin()
	if err != nil {
		t.Fatalf("Admin(): %v", err)
	}
	if err := admin.EnsureTopic(t.Context(), mq.TopicSpec{Name: "orders"}); err != nil {
		t.Fatalf("EnsureTopic: %v", err)
	}
	if err := b.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	// A bound on a failure, not a pause: the close is synchronous on the
	// client side and the server sees it as soon as its read returns.
	select {
	case <-d.closed:
	case <-time.After(10 * time.Second):
		t.Fatal("the nsqd never saw the admin connection close after Backend.Close; " +
			"the backend left its pooled connection open")
	}
}

// TestTheAdminSurfaceUsesTheConfiguredTLS: Config.TLS is what the protocol
// connection verifies nsqd with, and an https admin request has to verify the
// same nsqd the same way. The admin client used to travel on
// http.DefaultTransport, which knows nothing of Config.TLS: a deployment whose
// nsqd certificates come from a private CA -- the usual case -- had a protocol
// connection that worked and an admin surface that failed every call with
// "certificate signed by unknown authority", and a client certificate was
// never presented.
func TestTheAdminSurfaceUsesTheConfiguredTLS(t *testing.T) {
	t.Parallel()
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)
	roots := x509.NewCertPool()
	roots.AddCert(srv.Certificate())
	b, _ := newBackendRig(t, func(c *nq.Config) {
		c.AdminAddrs = []string{strings.TrimPrefix(srv.URL, "https://")}
		c.TLS = &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}
	})
	admin, err := b.Admin()
	if err != nil {
		t.Fatalf("Admin(): %v", err)
	}
	if err := admin.EnsureTopic(t.Context(), mq.TopicSpec{Name: "orders"}); err != nil {
		t.Fatalf("EnsureTopic over https with Config.TLS trusting the nsqd's CA: %v", err)
	}
}

// TestTheAdminSurfaceVerifiesEachNSQDByItsOwnName: go-nsq verifies each nsqd
// against the host it dials, whatever Config.TLS.ServerName says -- its
// upgradeTLS clones the configuration and overwrites ServerName -- and the
// admin surface talks to every nsqd in AdminAddrs, so it has to do the same.
// A clone that kept the caller's ServerName checked
// every nsqd's certificate against that one name, so a deployment whose
// protocol connections verified had admin calls that did not.
func TestTheAdminSurfaceVerifiesEachNSQDByItsOwnName(t *testing.T) {
	t.Parallel()
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)
	roots := x509.NewCertPool()
	roots.AddCert(srv.Certificate())
	b, _ := newBackendRig(t, func(c *nq.Config) {
		c.AdminAddrs = []string{strings.TrimPrefix(srv.URL, "https://")}
		// A name the test server's certificate does not cover: it is valid
		// for 127.0.0.1 and example.com.
		c.TLS = &tls.Config{RootCAs: roots, ServerName: "nsqd-a.example.net", MinVersion: tls.VersionTLS12}
	})
	admin, err := b.Admin()
	if err != nil {
		t.Fatalf("Admin(): %v", err)
	}
	if err := admin.EnsureTopic(t.Context(), mq.TopicSpec{Name: "orders"}); err != nil {
		t.Fatalf("EnsureTopic with Config.TLS.ServerName naming another host: %v; go-nsq verifies each nsqd by "+
			"its own name and so must the admin surface", err)
	}
}

// TestTheAdminTransportStartsFromTheProcessDefault: the admin transport is
// the backend's own, but it is a clone of http.DefaultTransport, so what an
// application configured there before Open still applies. The probe is a dialer: the process's DefaultTransport dials the
// fake nsqd whatever address it is asked for, and AdminAddrs names a port
// nothing listens on, so the call succeeds only through that dialer.
//
// Deliberately NOT parallel: it replaces http.DefaultTransport for its
// duration, which every other HTTP client in this binary reads.
func TestTheAdminTransportStartsFromTheProcessDefault(t *testing.T) {
	d := newConnStub(t)
	orig := http.DefaultTransport
	dt, ok := orig.(*http.Transport)
	if !ok {
		t.Fatalf("http.DefaultTransport is a %T, not an *http.Transport", orig)
	}
	custom := dt.Clone()
	var dialled atomic.Int64
	custom.DialContext = func(ctx context.Context, network, _ string) (net.Conn, error) {
		dialled.Add(1)
		var dialer net.Dialer
		return dialer.DialContext(ctx, network, d.addr())
	}
	http.DefaultTransport = custom
	t.Cleanup(func() { http.DefaultTransport = orig })

	b, _ := newBackendRig(t, func(c *nq.Config) { c.AdminAddrs = []string{"127.0.0.1:1"} })
	admin, err := b.Admin()
	if err != nil {
		t.Fatalf("Admin(): %v", err)
	}
	if err := admin.EnsureTopic(t.Context(), mq.TopicSpec{Name: "orders"}); err != nil {
		t.Fatalf("EnsureTopic through the process's DefaultTransport dialer: %v; the admin transport dropped "+
			"what the application set on http.DefaultTransport before Open", err)
	}
	if dialled.Load() == 0 {
		t.Error("the admin call never used the process's DefaultTransport dialer")
	}
}

// TestEachBackendHasItsOwnAdminTransport: one backend's Close must not touch
// another's pooled connection. A transport shared between backends -- a
// package-level one, say -- would pass every other test here and bring the
// original defect back through the driver's own Close.
func TestEachBackendHasItsOwnAdminTransport(t *testing.T) {
	t.Parallel()
	d := newConnStub(t)
	a, _ := newBackendRig(t, func(c *nq.Config) { c.AdminAddrs = []string{d.addr()} })
	b, _ := newBackendRig(t, func(c *nq.Config) { c.AdminAddrs = []string{d.addr()} })
	adminA, err := a.Admin()
	if err != nil {
		t.Fatalf("Admin(): %v", err)
	}
	adminB, err := b.Admin()
	if err != nil {
		t.Fatalf("Admin(): %v", err)
	}
	if err := adminA.EnsureTopic(t.Context(), mq.TopicSpec{Name: "orders"}); err != nil {
		t.Fatalf("EnsureTopic on the first backend: %v", err)
	}
	if err := adminB.EnsureTopic(t.Context(), mq.TopicSpec{Name: "orders"}); err != nil {
		t.Fatalf("EnsureTopic on the second backend: %v", err)
	}
	if err := b.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	select {
	case <-d.closed:
	case <-time.After(10 * time.Second):
		t.Fatal("the second backend's Close never released its admin connection")
	}
	before := d.opened.Load()
	if err := adminA.EnsureTopic(t.Context(), mq.TopicSpec{Name: "orders"}); err != nil {
		t.Fatalf("EnsureTopic on the first backend after the second closed: %v", err)
	}
	if after := d.opened.Load(); after != before {
		t.Errorf("the first backend opened a new admin connection (%d, then %d) after the SECOND backend closed: "+
			"the two share a transport, and one's Close empties the other's pool", before, after)
	}
}

// TestAnAdminCallAfterCloseIsRefused: an admin surface held past Close does
// not reach nsqd, and so cannot pool a connection nothing will release.
func TestAnAdminCallAfterCloseIsRefused(t *testing.T) {
	t.Parallel()
	d := newConnStub(t)
	b, _ := newBackendRig(t, func(c *nq.Config) { c.AdminAddrs = []string{d.addr()} })
	admin, err := b.Admin()
	if err != nil {
		t.Fatalf("Admin(): %v", err)
	}
	if err := b.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := admin.EnsureTopic(t.Context(), mq.TopicSpec{Name: "orders"}); !errors.Is(err, mq.ErrClosed) {
		t.Errorf("EnsureTopic after Close = %v, want ErrClosed", err)
	}
	if n := d.opened.Load(); n != 0 {
		t.Errorf("an admin call after Close opened %d connection(s) to nsqd", n)
	}
}

// TestAnAdminCallInFlightAtCloseReleasesItsConnection: a request that passed
// the closed check before Close, and asks the transport for a connection
// after Close emptied the pool, puts that connection back into the pool when
// it finishes -- where nothing will ever release it. net/http's own guard does
// not cover it: CloseIdleConnections makes the transport close connections
// that go idle LATER, but the next request that asks for one switches that
// off again. The request has to empty the pool again on its way out.
//
// The order is forced, not raced for: the request is held at the start of
// net/http's connection lookup (httptrace's GetConn) until Close has returned.
func TestAnAdminCallInFlightAtCloseReleasesItsConnection(t *testing.T) {
	t.Parallel()
	d := newConnStub(t)
	b, _ := newBackendRig(t, func(c *nq.Config) { c.AdminAddrs = []string{d.addr()} })
	admin, err := b.Admin()
	if err != nil {
		t.Fatalf("Admin(): %v", err)
	}
	atLookup, proceed := make(chan struct{}), make(chan struct{})
	ctx := httptrace.WithClientTrace(t.Context(), &httptrace.ClientTrace{
		GetConn: func(string) {
			close(atLookup)
			<-proceed
		},
	})
	done := make(chan error, 1)
	go func() { done <- admin.EnsureTopic(ctx, mq.TopicSpec{Name: "orders"}) }()
	select {
	case <-atLookup:
	case <-time.After(10 * time.Second):
		t.Fatal("the admin request never reached net/http's connection lookup")
	}
	if err := b.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	close(proceed)
	if err := <-done; err != nil {
		t.Fatalf("the EnsureTopic that was in flight at Close: %v", err)
	}
	select {
	case <-d.closed:
	case <-time.After(10 * time.Second):
		t.Fatal("the connection of an admin call in flight at Close was never released; it went back into a " +
			"pool Close had already emptied")
	}
}

// wrappedTransport is what an application puts in http.DefaultTransport when
// it wraps the default -- for tracing, say. It is not an *http.Transport.
type wrappedTransport struct {
	inner http.RoundTripper
	used  atomic.Int64
}

func (w *wrappedTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	w.used.Add(1)
	return w.inner.RoundTrip(r)
}

// TestAWrappedDefaultTransportGetsAnOwnedTransportAnyway: a DefaultTransport
// that is not an *http.Transport cannot be cloned, and using it as it is would
// share its pool again -- the defect the owned transport exists to prevent. So
// the backend gets DefaultTransport's documented settings in a transport of its
// own, and the wrapper is not used.
//
// Deliberately NOT parallel: it replaces http.DefaultTransport for its
// duration.
func TestAWrappedDefaultTransportGetsAnOwnedTransportAnyway(t *testing.T) {
	d := newConnStub(t)
	orig := http.DefaultTransport
	w := &wrappedTransport{inner: orig}
	http.DefaultTransport = w
	t.Cleanup(func() { http.DefaultTransport = orig })

	admin := adminOver(t, d.addr())
	if err := admin.EnsureTopic(t.Context(), mq.TopicSpec{Name: "orders"}); err != nil {
		t.Fatalf("EnsureTopic with a wrapped DefaultTransport: %v", err)
	}
	if n := w.used.Load(); n != 0 {
		t.Errorf("the wrapped DefaultTransport carried %d admin request(s); its pool is the process's", n)
	}
}
