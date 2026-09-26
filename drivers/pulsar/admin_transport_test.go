package pulsar_test

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

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit"
	pd "github.com/iqhive/iqmq/drivers/pulsar"
	"github.com/iqhive/iqmq/drivers/pulsar/pulsarfake"
)

// connAdmin is a fake Pulsar web service that counts the connections it
// accepts and reports each one it sees closed. It answers every request 204
// with no body, as the writing admin endpoints do -- the response shape
// net/http returns to its idle pool before the caller has it -- except the
// broker's runtime configuration, which it answers with runtime when that is
// set.
type connAdmin struct {
	srv     *httptest.Server
	opened  atomic.Int64
	closed  chan struct{} // one value per connection the server saw close
	runtime string
}

func newConnAdmin(t *testing.T) *connAdmin { return newConnAdminAnswering(t, "") }

func newConnAdminAnswering(t *testing.T, runtime string) *connAdmin {
	t.Helper()
	a := &connAdmin{closed: make(chan struct{}, 64), runtime: runtime}
	a.srv = httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if a.runtime != "" && strings.HasSuffix(r.URL.Path, "/brokers/configuration/runtime") {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(a.runtime))
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	// httptest chains its own connection tracking after this hook.
	a.srv.Config.ConnState = func(_ net.Conn, st http.ConnState) {
		switch st {
		case http.StateNew:
			a.opened.Add(1)
		case http.StateClosed, http.StateHijacked:
			select {
			case a.closed <- struct{}{}:
			default:
			}
		default:
		}
	}
	a.srv.Start()
	t.Cleanup(a.srv.Close)
	return a
}

// TestClosingTheProcessWideTransportLeavesTheAdminConnectionAlone is the
// defect drivers/nsq's admin surface failed a GitHub run with ("HTTP/1.x
// transport connection broken: http: CloseIdleConnections called"), which
// this driver had in the same shape and had not yet been caught by.
//
// The admin client had no Transport of its own, so its connections sat in
// http.DefaultTransport's pool, and anything else in the process closing that
// pool's idle connections -- a parallel test's httptest.Server.Close does --
// could close one between net/http returning it to the pool and handing its
// bodiless 204 to the request waiting on it. That window is a few
// instructions wide, so the test asks the question it depends on instead of
// racing for it: two admin calls either side of such a close must travel on
// one connection.
//
// Deliberately NOT parallel: it closes the idle connections of the pool every
// other HTTP client in this binary shares.
func TestClosingTheProcessWideTransportLeavesTheAdminConnectionAlone(t *testing.T) {
	a := newConnAdmin(t)
	b, _ := newRig(t, func(c *pd.Config) { c.AdminURL = a.srv.URL })
	admin, err := b.Admin()
	if err != nil {
		t.Fatalf("Admin(): %v", err)
	}
	dt, ok := http.DefaultTransport.(*http.Transport)
	if !ok {
		t.Fatalf("http.DefaultTransport is a %T, not an *http.Transport; the test cannot close its pool", http.DefaultTransport)
	}

	if err := admin.DeleteSubscription(t.Context(), "orders", "workers"); err != nil {
		t.Fatalf("first DeleteSubscription: %v", err)
	}
	before := a.opened.Load()
	dt.CloseIdleConnections()
	if err := admin.DeleteSubscription(t.Context(), "orders", "workers"); err != nil {
		t.Fatalf("DeleteSubscription after the process-wide pool was closed: %v", err)
	}
	if after := a.opened.Load(); after != before {
		t.Errorf("an admin call after http.DefaultTransport.CloseIdleConnections opened a new connection "+
			"(%d before, %d after): the admin surface's connection is in the process-wide pool, where anything "+
			"else in the process that tidies that pool (httptest.Server.Close does) can fail a request in flight",
			before, after)
	}
}

// TestCloseReleasesTheAdminConnections: the admin transport is the backend's
// own, so Close is what gives its pooled connections back.
func TestCloseReleasesTheAdminConnections(t *testing.T) {
	t.Parallel()
	a := newConnAdmin(t)
	b, _ := newRig(t, func(c *pd.Config) { c.AdminURL = a.srv.URL })
	admin, err := b.Admin()
	if err != nil {
		t.Fatalf("Admin(): %v", err)
	}
	if err := admin.DeleteSubscription(t.Context(), "orders", "workers"); err != nil {
		t.Fatalf("DeleteSubscription: %v", err)
	}
	if err := b.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	// A bound on a failure, not a pause: the close is synchronous on the
	// client side and the server sees it as soon as its read returns.
	select {
	case <-a.closed:
	case <-time.After(10 * time.Second):
		t.Fatal("the web service never saw an admin connection close after Backend.Close; " +
			"the backend left its pooled connection open")
	}
}

// awaitClose waits for the web service to see one connection close.
func (a *connAdmin) awaitClose(t *testing.T, what string) {
	t.Helper()
	// A bound on a failure, not a pause: the close is synchronous on the
	// client side and the server sees it as soon as its read returns.
	select {
	case <-a.closed:
	case <-time.After(10 * time.Second):
		t.Fatal(what)
	}
}

// TestARefusedOpenReleasesItsAdminConnection: an Open refused after it asked
// the broker for its facts has pooled the connection that answered, in a
// transport no caller can reach -- teardown only runs for a backend that was
// returned. Every refused Open would leave one for the idle timeout, where
// every refused Open used to share one in http.DefaultTransport. The refusal is the real one: EnableBatchIndexAck against a broker
// whose runtime configuration says it ignores an acknowledgement set.
func TestARefusedOpenReleasesItsAdminConnection(t *testing.T) {
	t.Parallel()
	a := newConnAdminAnswering(t, `{"acknowledgmentAtBatchIndexLevelEnabled":"false"}`)
	server := pulsarfake.New()
	cfg := rigConfig(server)
	cfg.AdminURL = a.srv.URL
	cfg.EnableBatchIndexAck = true
	b, err := pd.BackendWithClient(context.Background(), server.Client(), driverkit.ClientOwned, cfg)
	if err == nil {
		_ = b.Close()
		t.Fatal("the open was not refused, so this test did not reach the path it is about")
	}
	if n := a.opened.Load(); n != 1 {
		t.Fatalf("the refused open made %d admin connection(s), want the one its broker-facts lookup used", n)
	}
	a.awaitClose(t, "the admin connection of a refused Open was never released; it stays pooled for the "+
		"idle timeout in a transport nothing can reach")
}

// TestTheAdminTransportStartsFromTheProcessDefault: the admin transport is
// the backend's own, but it is a clone of http.DefaultTransport, so what an
// application configured there before Open still applies.
// Until the admin surface carries Config.TLS, DefaultTransport's
// TLSClientConfig is the in-process way to trust a private CA on an https
// AdminURL, and a transport built from defaults dropped it: every admin call,
// Ping and the broker-facts lookup failed as signed by an unknown authority.
//
// Deliberately NOT parallel: it replaces http.DefaultTransport for its
// duration, which every other HTTP client in this binary reads.
func TestTheAdminTransportStartsFromTheProcessDefault(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(srv.Close)
	orig := http.DefaultTransport
	dt, ok := orig.(*http.Transport)
	if !ok {
		t.Fatalf("http.DefaultTransport is a %T, not an *http.Transport", orig)
	}
	roots := x509.NewCertPool()
	roots.AddCert(srv.Certificate())
	custom := dt.Clone()
	custom.TLSClientConfig = &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}
	http.DefaultTransport = custom
	t.Cleanup(func() { http.DefaultTransport = orig })

	b, _ := newRig(t, func(c *pd.Config) { c.AdminURL = srv.URL })
	admin, err := b.Admin()
	if err != nil {
		t.Fatalf("Admin(): %v", err)
	}
	if err := admin.DeleteSubscription(t.Context(), "orders", "workers"); err != nil {
		t.Fatalf("DeleteSubscription over https with the process's DefaultTransport trusting the CA: %v; the "+
			"admin transport dropped what the application set on http.DefaultTransport before Open", err)
	}
}

// TestEachBackendHasItsOwnAdminTransport: one backend's Close must not touch
// another's pooled connection. A transport shared between backends would pass
// every other test here and bring the original defect back through the
// driver's own Close.
func TestEachBackendHasItsOwnAdminTransport(t *testing.T) {
	t.Parallel()
	a := newConnAdmin(t)
	first, _ := newRig(t, func(c *pd.Config) { c.AdminURL = a.srv.URL })
	second, _ := newRig(t, func(c *pd.Config) { c.AdminURL = a.srv.URL })
	adminFirst, err := first.Admin()
	if err != nil {
		t.Fatalf("Admin(): %v", err)
	}
	adminSecond, err := second.Admin()
	if err != nil {
		t.Fatalf("Admin(): %v", err)
	}
	if err := adminFirst.DeleteSubscription(t.Context(), "orders", "workers"); err != nil {
		t.Fatalf("DeleteSubscription on the first backend: %v", err)
	}
	if err := adminSecond.DeleteSubscription(t.Context(), "orders", "workers"); err != nil {
		t.Fatalf("DeleteSubscription on the second backend: %v", err)
	}
	if err := second.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	a.awaitClose(t, "the second backend's Close never released its admin connection")
	before := a.opened.Load()
	if err := adminFirst.DeleteSubscription(t.Context(), "orders", "workers"); err != nil {
		t.Fatalf("DeleteSubscription on the first backend after the second closed: %v", err)
	}
	if after := a.opened.Load(); after != before {
		t.Errorf("the first backend opened a new admin connection (%d, then %d) after the SECOND backend closed: "+
			"the two share a transport, and one's Close empties the other's pool", before, after)
	}
}

// adminCalls are the two routes an admin request takes: do, for the writing
// calls, and get, for the reading ones. Each carries its own closed check and
// its own release, so each is pinned on its own.
var adminCalls = []struct {
	route string
	call  func(context.Context, mq.Admin) error
}{
	{"do (DeleteSubscription)", func(ctx context.Context, a mq.Admin) error {
		return a.DeleteSubscription(ctx, "orders", "workers")
	}},
	{"get (Stats)", func(ctx context.Context, a mq.Admin) error {
		_, err := a.Stats(ctx, "orders", "workers")
		return err
	}},
}

// TestAnAdminCallAfterCloseIsRefused: an admin surface held past Close does
// not reach the web service, and so cannot pool a connection nothing will
// release.
func TestAnAdminCallAfterCloseIsRefused(t *testing.T) {
	t.Parallel()
	for _, route := range adminCalls {
		t.Run(route.route, func(t *testing.T) {
			t.Parallel()
			a := newConnAdmin(t)
			b, _ := newRig(t, func(c *pd.Config) { c.AdminURL = a.srv.URL })
			admin, err := b.Admin()
			if err != nil {
				t.Fatalf("Admin(): %v", err)
			}
			if err := b.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}
			before := a.opened.Load()
			if err := route.call(t.Context(), admin); !errors.Is(err, mq.ErrClosed) {
				t.Errorf("an admin call through %s after Close = %v, want ErrClosed", route.route, err)
			}
			if after := a.opened.Load(); after != before {
				t.Errorf("an admin call through %s after Close opened %d connection(s) to the web service",
					route.route, after-before)
			}
		})
	}
}

// TestAnAdminCallInFlightAtCloseReleasesItsConnection: a request that passed
// the closed check before Close, and asks the transport for a connection
// after Close emptied the pool, puts that connection back into the pool when
// it finishes. net/http's own guard does not cover it: CloseIdleConnections
// makes the transport close connections that go idle LATER, but the next
// request that asks for one switches that off again. The request has to empty
// the pool again on its way out. The order is forced: the
// request is held at the start of net/http's connection lookup (httptrace's
// GetConn) until Close has returned.
func TestAnAdminCallInFlightAtCloseReleasesItsConnection(t *testing.T) {
	t.Parallel()
	for _, route := range adminCalls {
		t.Run(route.route, func(t *testing.T) {
			t.Parallel()
			a := newConnAdmin(t)
			b, _ := newRig(t, func(c *pd.Config) { c.AdminURL = a.srv.URL })
			admin, err := b.Admin()
			if err != nil {
				t.Fatalf("Admin(): %v", err)
			}
			opened := a.opened.Load()
			atLookup, proceed := make(chan struct{}), make(chan struct{})
			ctx := httptrace.WithClientTrace(t.Context(), &httptrace.ClientTrace{
				GetConn: func(string) {
					close(atLookup)
					<-proceed
				},
			})
			done := make(chan struct{})
			// The call's answer is not the point -- the fake answers Stats with
			// an empty body, which does not decode -- its connection is.
			go func() {
				defer close(done)
				_ = route.call(ctx, admin)
			}()
			select {
			case <-atLookup:
			case <-time.After(10 * time.Second):
				t.Fatal("the admin request never reached net/http's connection lookup")
			}
			if err := b.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}
			// Close released whatever the broker-facts lookup at open had
			// pooled; the connection this test is about is the next one.
			for range opened {
				a.awaitClose(t, "Close never released the broker-facts lookup's connection")
			}
			close(proceed)
			<-done
			a.awaitClose(t, "the connection of an admin call in flight at Close was never released; it went "+
				"back into a pool Close had already emptied")
		})
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
	a := newConnAdmin(t)
	orig := http.DefaultTransport
	w := &wrappedTransport{inner: orig}
	http.DefaultTransport = w
	t.Cleanup(func() { http.DefaultTransport = orig })

	b, _ := newRig(t, func(c *pd.Config) { c.AdminURL = a.srv.URL })
	admin, err := b.Admin()
	if err != nil {
		t.Fatalf("Admin(): %v", err)
	}
	if err := admin.DeleteSubscription(t.Context(), "orders", "workers"); err != nil {
		t.Fatalf("DeleteSubscription with a wrapped DefaultTransport: %v", err)
	}
	if n := w.used.Load(); n != 0 {
		t.Errorf("the wrapped DefaultTransport carried %d admin request(s); its pool is the process's", n)
	}
}
