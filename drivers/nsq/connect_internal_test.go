package nsq

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"slices"
	"strings"
	"testing"
	"time"

	gonsq "github.com/iqhive/go-nsq"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/dsn"
)

// closeDuringConnectEnv names the scenario a child copy of the test binary
// runs for TestClosingWhileGoNSQConnectsDoesNotCrashTheProcess.
const closeDuringConnectEnv = "IQMQ_NSQ_CLOSE_DURING_CONNECT"

// TestClosingWhileGoNSQConnectsDoesNotCrashTheProcess closes a source while
// go-nsq is still making one of its connections, and the process has to
// survive it.
//
// go-nsq v1.1.0's Stop counted only the connections that had finished their
// handshake. With none finished it closed the channel its handler pool reads
// from at once, and a connection still waiting for nsqd's answer to IDENTIFY
// did not look at the stop again when the answer came: it subscribed, sent
// RDY, and the first message nsqd sent was put on the closed channel. That is
// a panic on a go-nsq goroutine, which no caller can recover, so each scenario
// runs in a child copy of this binary and the parent reads how it ended.
//
// There are two ways in. OpenSource connects outside the backend's lock, so a
// backend Close can reach the source while its connect is in progress. And
// with no API race at all, go-nsq redials a dropped connection on its own, so
// closing a source during an outage can land on a redial.
func TestClosingWhileGoNSQConnectsDoesNotCrashTheProcess(t *testing.T) {
	if os.Getenv(closeDuringConnectEnv) != "" {
		t.Skip("this process is the child; TestClosingWhileGoNSQConnects is the subject")
	}
	for _, scenario := range []string{"open", "redial", "redial beside another connection"} {
		t.Run(scenario, func(t *testing.T) {
			cmd := exec.CommandContext(t.Context(), os.Args[0],
				"-test.run=^TestClosingWhileGoNSQConnects$", "-test.v", "-test.count=1")
			cmd.Env = append(os.Environ(), closeDuringConnectEnv+"="+scenario)
			raw, err := cmd.CombinedOutput()
			out := string(raw)
			if err != nil || !strings.Contains(out, "--- PASS: TestClosingWhileGoNSQConnects") {
				t.Fatalf("the child process running %q did not pass (%v):\n%s", scenario, err, out)
			}
		})
	}
}

// TestClosingWhileGoNSQConnects is the child half of
// TestClosingWhileGoNSQConnectsDoesNotCrashTheProcess, and skips unless that
// test started it.
//
// A connection that finishes its handshake after the stop is closed by go-nsq
// itself, without asking nsqd for a message, so a Close that lands inside a
// redial leaves no idle connection behind and does not wait for one.
func TestClosingWhileGoNSQConnects(t *testing.T) {
	scenario := os.Getenv(closeDuringConnectEnv)
	if scenario == "" {
		t.Skip("run in a child process by TestClosingWhileGoNSQConnectsDoesNotCrashTheProcess")
	}
	w := newWireNSQD(t)
	addrs := []string{w.ln.Addr().String()}
	var other *wireNSQD
	if scenario == "redial beside another connection" {
		other = newWireNSQD(t)
		addrs = []string{other.ln.Addr().String(), w.ln.Addr().String()}
	}
	b, err := OpenBackend(context.Background(), Config{
		Addrs:               addrs,
		AckDeadline:         30 * time.Second,
		LeaseGuard:          time.Second,
		LookupdPollInterval: 20 * time.Millisecond,
		APITimeout:          2 * time.Second,
	})
	if err != nil {
		t.Fatalf("OpenBackend: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })
	cfg := mq.SourceConfig{
		Topic: "orders", Subscription: "workers", Prefetch: 1,
		Metadata: mq.MetadataNativeStrict, AckDeadline: 30 * time.Second,
	}
	release := make(chan struct{})

	switch scenario {
	case "open":
		// The backend closes while OpenSource waits for nsqd's answer to
		// IDENTIFY, and the answer comes after the Close.
		w.mu.Lock()
		w.hold[0] = release
		w.mu.Unlock()
		type opened struct {
			src mq.Source
			err error
		}
		done := make(chan opened, 1)
		go func() {
			src, err := b.OpenSource(context.Background(), cfg)
			done <- opened{src, err}
		}()
		<-w.identified
		if err := b.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
		close(release)
		got := <-done
		if got.src != nil {
			_ = got.src.Close()
			t.Fatalf("OpenSource returned a source for a backend that closed during its connect (err %v)", got.err)
		}
		if !errors.Is(got.err, mq.ErrClosed) {
			t.Fatalf("OpenSource on a backend that closed during its connect returned %v, want ErrClosed", got.err)
		}
		// And the connection go-nsq finished after the Close is closed
		// rather than left open: the stop came after it, so it was told.
		if cmds := w.commandsOf(0); !slices.Contains(cmds, "CLS") {
			t.Fatalf("the connection that finished after the backend closed was never closed: %q", cmds)
		}

	case "redial":
		// The first connection carries no message, so dropping it lets go-nsq
		// close it at once and redial. The source closes while the redial
		// waits for nsqd's answer to IDENTIFY.
		w.mu.Lock()
		w.quiet[0] = true
		w.hold[1] = release
		w.mu.Unlock()
		src, err := b.OpenSource(context.Background(), cfg)
		if err != nil {
			t.Fatalf("OpenSource: %v", err)
		}
		<-w.identified
		waitUntil(t, "the first connection to be ready", func() bool { return hasRDY(w.commandsOf(0)) })
		w.drop()
		select {
		case <-w.identified:
		case <-time.After(5 * time.Second):
			t.Fatal("go-nsq never redialled")
		}
		if err := src.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
		close(release)
		waitUntil(t, "the redial to subscribe", func() bool { return subscribed(w.commandsOf(1)) })
		// go-nsq closes the late connection itself rather than leaving it
		// open and idle with its read and write goroutines.
		waitUntil(t, "go-nsq to close the redial that finished after Close", func() bool { return w.hasEnded(1) })
		if hasRDY(w.commandsOf(1)) {
			t.Fatalf("the redial that finished after Close asked nsqd for messages: %q", w.commandsOf(1))
		}

	case "redial beside another connection":
		// The other nsqd stays connected while w's connection is redialled,
		// so go-nsq's stop has a connection to wait for, and its answer to
		// CLS is held until the redial has finished. A late connection go-nsq
		// kept would still be counted when that answer comes, and the stop
		// would wait for it until Config.APITimeout.
		holdCLS := make(chan struct{})
		other.mu.Lock()
		other.quiet[0] = true
		other.holdCLS[0] = holdCLS
		other.mu.Unlock()
		w.mu.Lock()
		w.quiet[0] = true
		w.hold[1] = release
		w.mu.Unlock()
		cfg.Prefetch = 2
		src, err := b.OpenSource(context.Background(), cfg)
		if err != nil {
			t.Fatalf("OpenSource: %v", err)
		}
		waitUntil(t, "both connections to subscribe", func() bool {
			return subscribed(other.commandsOf(0)) && subscribed(w.commandsOf(0))
		})
		<-w.identified
		w.drop()
		select {
		case <-w.identified:
		case <-time.After(5 * time.Second):
			t.Fatal("go-nsq never redialled")
		}
		closed := make(chan time.Duration, 1)
		go func() {
			start := time.Now()
			_ = src.Close()
			closed <- time.Since(start)
		}()
		waitUntil(t, "the stop to reach the other nsqd", func() bool { return slices.Contains(other.commandsOf(0), "CLS") })
		close(release)
		waitUntil(t, "the redial to subscribe", func() bool { return subscribed(w.commandsOf(1)) })
		// Long enough for go-nsq to count the late connection if it kept it.
		time.Sleep(100 * time.Millisecond)
		close(holdCLS)
		if took := <-closed; took > time.Second {
			t.Fatalf("Close took %v: go-nsq's stop waited for the redial that finished after it", took)
		}
		waitUntil(t, "go-nsq to close the redial that finished after the stop", func() bool { return w.hasEnded(1) })
		if hasRDY(w.commandsOf(1)) {
			t.Fatalf("the redial that finished after Close asked nsqd for messages: %q", w.commandsOf(1))
		}

	default:
		t.Fatalf("unknown scenario %q", scenario)
	}
}

// hasRDY reports whether a connection asked nsqd for at least one message.
func hasRDY(commands []string) bool {
	for _, c := range commands {
		if strings.HasPrefix(c, "RDY ") && c != "RDY 0" {
			return true
		}
	}
	return false
}

// subscribed reports whether a connection sent SUB.
func subscribed(commands []string) bool {
	for _, c := range commands {
		if strings.HasPrefix(c, "SUB ") {
			return true
		}
	}
	return false
}

// TestAHandshakeTheLinkCutIsUnavailableNotAConfigurationError holds the
// classification of a failed IDENTIFY to what failed.
//
// go-nsq wraps everything that goes wrong inside the handshake in one error
// type: nsqd's own refusal of the parameters, but also an EOF or a timeout
// reading its answer. Mapping the type to CodeInvalidConfig told a caller whose
// nsqd was restarting that the configuration was wrong, the core did not retry
// a publication that had never been sent, and health stayed ready. Only an nsqd
// error frame, or a TLS certificate the client refuses or is refused for, is
// about the configuration.
func TestAHandshakeTheLinkCutIsUnavailableNotAConfigurationError(t *testing.T) {
	hungUp := func(t *testing.T) *Backend {
		t.Helper()
		w := newWireNSQD(t)
		w.mu.Lock()
		w.hangUp[0] = true
		w.mu.Unlock()
		b, err := OpenBackend(context.Background(), Config{Addrs: []string{w.ln.Addr().String()}, APITimeout: 2 * time.Second})
		if err != nil {
			t.Fatalf("OpenBackend: %v", err)
		}
		t.Cleanup(func() { _ = b.Close() })
		return b
	}
	t.Run("a publication", func(t *testing.T) {
		b := hungUp(t)
		snk, err := b.OpenSink(context.Background(), mq.SinkConfig{Topic: "orders"})
		if err != nil {
			t.Fatalf("OpenSink: %v", err)
		}
		err = snk.Publish(context.Background(), &mq.Message{Body: []byte("x")}, nil)
		if err == nil {
			t.Fatal("Publish succeeded against an nsqd that hung up during the handshake")
		}
		if code := mq.CodeOf(err); code != mq.CodeUnavailable || !mq.IsRetryable(err) || mq.IsAmbiguous(err) {
			t.Fatalf("a link cut during IDENTIFY mapped to %v (retryable %v, ambiguous %v): %v; want a retryable, "+
				"unambiguous Unavailable, because no PUB was written", code, mq.IsRetryable(err), mq.IsAmbiguous(err), err)
		}
		if st := b.Health().State; st != mq.HealthDegraded {
			t.Fatalf("health is %v after the connection failed, want degraded", st)
		}
	})
	t.Run("a subscription", func(t *testing.T) {
		b := hungUp(t)
		src, err := b.OpenSource(context.Background(), mq.SourceConfig{Topic: "orders", Subscription: "workers", Prefetch: 1})
		if src != nil {
			_ = src.Close()
		}
		if code := mq.CodeOf(err); code != mq.CodeUnavailable || !mq.IsRetryable(err) {
			t.Fatalf("OpenSource against an nsqd that hung up during the handshake returned %v (code %v, retryable %v), "+
				"want a retryable Unavailable", err, code, mq.IsRetryable(err))
		}
	})
	t.Run("the reasons", func(t *testing.T) {
		m := newErrorMapper()
		for reason, want := range map[string]mq.ErrorCode{
			"E_BAD_BODY IDENTIFY msg timeout (100) is invalid":                                 mq.CodeInvalidConfig,
			"tls: failed to verify certificate: x509: certificate signed by unknown authority": mq.CodeInvalidConfig,
			"remote error: tls: bad certificate":                                               mq.CodeInvalidConfig,
			"EOF":                                                                              mq.CodeUnavailable,
			"read tcp 127.0.0.1:1->127.0.0.1:2: i/o timeout":                                   mq.CodeUnavailable,
			"write tcp 127.0.0.1:1->127.0.0.1:2: connection reset by peer":                     mq.CodeUnavailable,
			"invalid response from Snappy upgrade":                                             mq.CodeUnavailable,
		} {
			mapped := m.Map(mq.OpPublish, "orders", "", gonsq.ErrIdentify{Reason: reason})
			if got := mq.CodeOf(mapped); got != want {
				t.Errorf("IDENTIFY failing with %q mapped to %v, want %v", reason, got, want)
			}
			if retry := mq.IsRetryable(mapped); retry != (want == mq.CodeUnavailable) {
				t.Errorf("IDENTIFY failing with %q is retryable %v", reason, retry)
			}
		}
	})
}

// identifyRequiringAuth is nsqd v1.3.0's answer to IDENTIFY when it was started
// with an auth server: feature negotiation on, nothing upgraded, AUTH required.
const identifyRequiringAuth = `{"max_rdy_count":2500,"version":"1.3.0","max_msg_timeout":900000,"msg_timeout":60000,` +
	`"tls_v1":false,"deflate":false,"snappy":false,"auth_required":true}`

// TestAnAuthRefusalIsUnauthorized holds nsqd's answer to AUTH to what it says
// about the credentials.
//
// AUTH follows IDENTIFY, and go-nsq reports its failures as plain errors:
// "Error authenticating " and nsqd's frame, or "Auth Required" when nsqd asks
// for a secret the client was not given. The rules for E_AUTH_FAILED read an
// ErrProtocol or an ErrIdentify, which go-nsq never makes for AUTH, so a
// refused secret read as CodeUnknown and a caller that branches on
// CodeUnauthorized never saw one. A real go-nsq client makes the errors here,
// so they are the ones go-nsq returns.
func TestAnAuthRefusalIsUnauthorized(t *testing.T) {
	// answer is nsqd's answer to AUTH. With no secret go-nsq sends no AUTH at
	// all and returns "Auth Required" on nsqd's IDENTIFY answer alone, which
	// the case holds by checking that no AUTH reached the server.
	for name, c := range map[string]struct{ secret, answer string }{
		"a secret nsqd refuses":             {"wrong", "E_AUTH_FAILED AUTH failed"},
		"a secret with no authorisations":   {"no-grants", "E_UNAUTHORIZED AUTH no authorizations found"},
		"no secret where nsqd requires one": {"", ""},
	} {
		refusing := func(t *testing.T) (*Backend, *wireNSQD) {
			t.Helper()
			w := newWireNSQD(t)
			w.mu.Lock()
			w.identify = identifyRequiringAuth
			if c.answer != "" {
				w.auth = wireAnswer{frameType: gonsq.FrameTypeError, data: c.answer}
			}
			w.mu.Unlock()
			return openWireBackend(t, w, Config{AuthSecret: dsn.Secret(c.secret)}), w
		}
		authShape := func(t *testing.T, w *wireNSQD) {
			t.Helper()
			if sent := w.received("AUTH"); sent != (c.secret != "") {
				t.Fatalf("AUTH reached the server: %v, with secret %q", sent, c.secret)
			}
		}
		t.Run(name+", publishing", func(t *testing.T) {
			b, w := refusing(t)
			snk, err := b.OpenSink(context.Background(), mq.SinkConfig{Topic: "orders"})
			if err != nil {
				t.Fatalf("OpenSink: %v", err)
			}
			err = snk.Publish(context.Background(), &mq.Message{Body: []byte("x")}, nil)
			if code := mq.CodeOf(err); code != mq.CodeUnauthorized || mq.IsRetryable(err) || mq.IsAmbiguous(err) {
				t.Fatalf("Publish that nsqd's AUTH refused returned %v (code %v, retryable %v, ambiguous %v), "+
					"want a definite CodeUnauthorized that is not retryable", err, code, mq.IsRetryable(err), mq.IsAmbiguous(err))
			}
			authShape(t, w)
		})
		t.Run(name+", subscribing", func(t *testing.T) {
			b, w := refusing(t)
			src, err := b.OpenSource(context.Background(), mq.SourceConfig{Topic: "orders", Subscription: "workers", Prefetch: 1})
			if src != nil {
				_ = src.Close()
			}
			if code := mq.CodeOf(err); code != mq.CodeUnauthorized || mq.IsRetryable(err) {
				t.Fatalf("OpenSource that nsqd's AUTH refused returned %v (code %v, retryable %v), want CodeUnauthorized "+
					"that is not retryable", err, code, mq.IsRetryable(err))
			}
			authShape(t, w)
		})
	}
	t.Run("only an authentication code", func(t *testing.T) {
		native := errors.New("Error authenticating E_BAD_BODY AUTH body too big 2048 > 1024")
		if code := mq.CodeOf(newErrorMapper().Map(mq.OpPublish, "orders", "", native)); code == mq.CodeUnauthorized {
			t.Fatalf("%q mapped to CodeUnauthorized: nsqd refused the size of the AUTH body, not the credentials", native)
		}
	})
}

// identifyAgreeingToTLS is nsqd v1.3.0's answer to IDENTIFY when it has TLS
// configured and the client asked for it, with an auth server behind it.
const identifyAgreeingToTLS = `{"max_rdy_count":2500,"version":"1.3.0","max_msg_timeout":900000,"msg_timeout":60000,` +
	`"tls_v1":true,"deflate":false,"snappy":false,"auth_required":true}`

// tlsPair returns a server TLS configuration for 127.0.0.1 and a client one
// that trusts it.
func tlsPair(t *testing.T) (server, client *tls.Config) {
	t.Helper()
	srv := httptest.NewTLSServer(http.NotFoundHandler())
	t.Cleanup(srv.Close)
	roots := x509.NewCertPool()
	roots.AddCert(srv.Certificate())
	return srv.TLS.Clone(), &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}
}

// TestConfigTLSIsARequirement holds nj-22: with Config.TLS set, nothing goes to
// nsqd in the clear.
//
// go-nsq v1.1.0 asked for TLS in IDENTIFY and carried on in the clear when
// nsqd's answer declined, sending AUTH with the secret and every message. The
// fork fails the handshake there instead, before anything else is written, on
// every connection it makes: a publication, a subscription, a Ping, its own
// reconnections and the nsqds nsqlookupd names.
func TestConfigTLSIsARequirement(t *testing.T) {
	const secret = "s3cret-token"
	open := func(t *testing.T, cfg Config) *Backend {
		t.Helper()
		cfg.AuthSecret = dsn.Secret(secret)
		cfg.APITimeout = 2 * time.Second
		b, err := OpenBackend(context.Background(), cfg)
		if err != nil {
			t.Fatalf("OpenBackend: %v", err)
		}
		t.Cleanup(func() { _ = b.Close() })
		return b
	}
	refused := func(t *testing.T, what string, err error) {
		t.Helper()
		if mq.CodeOf(err) != mq.CodeInvalidConfig || mq.IsRetryable(err) || mq.IsAmbiguous(err) {
			t.Fatalf("%s = %v (code %v, retryable %v, ambiguous %v), want a definite CodeInvalidConfig that is not "+
				"retryable", what, err, mq.CodeOf(err), mq.IsRetryable(err), mq.IsAmbiguous(err))
		}
	}
	nothingInTheClear := func(t *testing.T, w *wireNSQD, n int) {
		t.Helper()
		if got := w.commandsOf(n); len(got) != 0 {
			t.Fatalf("connection %d sent %q after IDENTIFY to an nsqd that had declined TLS, in the clear", n, got)
		}
	}
	for name, identify := range map[string]string{
		"an nsqd that declines TLS":              identifyRequiringAuth,
		"an nsqd that does not negotiate at all": "",
	} {
		t.Run(name, func(t *testing.T) {
			_, client := tlsPair(t)
			w := newWireNSQD(t)
			w.mu.Lock()
			w.identify = identify
			w.mu.Unlock()
			b := open(t, Config{Addrs: []string{w.ln.Addr().String()}, TLS: client})
			snk, err := b.OpenSink(context.Background(), mq.SinkConfig{Topic: "orders"})
			if err != nil {
				t.Fatalf("OpenSink: %v", err)
			}
			refused(t, "Publish", snk.Publish(context.Background(), &mq.Message{Body: []byte("x")}, nil))
			src, err := b.OpenSource(context.Background(), mq.SourceConfig{Topic: "orders", Subscription: "workers", Prefetch: 1})
			if src != nil {
				_ = src.Close()
			}
			refused(t, "OpenSource", err)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			refused(t, "Ping", b.Ping(ctx))
			if st := b.Health().State; st != mq.HealthDegraded {
				t.Fatalf("health is %v after Ping found nsqd declining TLS, want degraded", st)
			}
			for n := range 3 {
				nothingInTheClear(t, w, n)
			}
		})
	}
	t.Run("a reconnection, once nsqd stops agreeing", func(t *testing.T) {
		server, client := tlsPair(t)
		w := newWireNSQD(t)
		w.mu.Lock()
		w.identify = identifyAgreeingToTLS
		w.upgrade = server
		w.auth = wireAnswer{data: `{"identity":"consumer","identity_url":"","permission_count":1}`}
		w.quiet[0] = true
		w.mu.Unlock()
		b := open(t, Config{Addrs: []string{w.ln.Addr().String()}, TLS: client, LookupdPollInterval: 20 * time.Millisecond})
		src, err := b.OpenSource(context.Background(), mq.SourceConfig{Topic: "orders", Subscription: "workers", Prefetch: 1})
		if err != nil {
			t.Fatalf("OpenSource over TLS: %v", err)
		}
		t.Cleanup(func() { _ = src.Close() })
		<-w.identified
		if !w.wasUpgraded(0) || !subscribed(w.commandsOf(0)) {
			t.Fatalf("the subscription did not go over TLS: upgraded %v, commands %q", w.wasUpgraded(0), w.commandsOf(0))
		}
		// nsqd restarts without its TLS configuration, and go-nsq redials it
		// on its own.
		w.mu.Lock()
		w.identify = identifyRequiringAuth
		w.upgrade = nil
		w.mu.Unlock()
		w.drop()
		redials := 0
		waitUntil(t, "go-nsq to redial twice, or to authenticate in the clear", func() bool {
			select {
			case <-w.identified:
				redials++
			default:
			}
			return redials == 2 || len(w.commandsOf(1)) > 0
		})
		nothingInTheClear(t, w, 1)
	})
	t.Run("an nsqd found through nsqlookupd", func(t *testing.T) {
		_, client := tlsPair(t)
		w := newWireNSQD(t)
		w.mu.Lock()
		w.identify = identifyRequiringAuth
		w.mu.Unlock()
		host, port, err := net.SplitHostPort(w.ln.Addr().String())
		if err != nil {
			t.Fatalf("SplitHostPort: %v", err)
		}
		lookupd := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, _ *http.Request) {
			rw.Header().Set("X-NSQ-Content-Type", "nsq; version=1.0")
			_, _ = fmt.Fprintf(rw, `{"channels":["workers"],"producers":[{"broadcast_address":%q,"tcp_port":%s}]}`, host, port)
		}))
		t.Cleanup(lookupd.Close)
		b := open(t, Config{Addrs: []string{w.ln.Addr().String()}, LookupdAddrs: []string{lookupd.URL}, TLS: client})
		src, err := b.OpenSource(context.Background(), mq.SourceConfig{Topic: "orders", Subscription: "workers", Prefetch: 1})
		if err != nil {
			t.Fatalf("OpenSource through nsqlookupd: %v", err)
		}
		t.Cleanup(func() { _ = src.Close() })
		select {
		case <-w.identified:
		case <-time.After(5 * time.Second):
			t.Fatal("go-nsq never connected to the nsqd nsqlookupd named")
		}
		// OpenSource's first discovery connects before it returns, so the
		// handshake has ended by now.
		nothingInTheClear(t, w, 0)
	})
	t.Run("an nsqd that agrees", func(t *testing.T) {
		server, client := tlsPair(t)
		w := newWireNSQD(t)
		w.mu.Lock()
		w.identify = identifyAgreeingToTLS
		w.upgrade = server
		w.auth = wireAnswer{data: `{"identity":"publisher","identity_url":"","permission_count":1}`}
		w.mu.Unlock()
		b := open(t, Config{Addrs: []string{w.ln.Addr().String()}, TLS: client})
		snk, err := b.OpenSink(context.Background(), mq.SinkConfig{Topic: "orders"})
		if err != nil {
			t.Fatalf("OpenSink: %v", err)
		}
		if err := snk.Publish(context.Background(), &mq.Message{Body: []byte("x")}, nil); err != nil {
			t.Fatalf("Publish over TLS: %v", err)
		}
		if got := w.commandsOf(0); !w.wasUpgraded(0) || !slices.Equal(got, []string{"AUTH", "PUB orders"}) {
			t.Fatalf("the connection sent %q (upgraded %v), want AUTH and the PUB, over TLS", got, w.wasUpgraded(0))
		}
	})
	t.Run("a certificate Config.TLS does not trust", func(t *testing.T) {
		server, _ := tlsPair(t)
		w := newWireNSQD(t)
		w.mu.Lock()
		w.identify = identifyAgreeingToTLS
		w.upgrade = server
		w.mu.Unlock()
		b := open(t, Config{Addrs: []string{w.ln.Addr().String()}, TLS: &tls.Config{MinVersion: tls.VersionTLS12}})
		snk, err := b.OpenSink(context.Background(), mq.SinkConfig{Topic: "orders"})
		if err != nil {
			t.Fatalf("OpenSink: %v", err)
		}
		refused(t, "Publish against an untrusted certificate", snk.Publish(context.Background(), &mq.Message{Body: []byte("x")}, nil))
		if w.received("AUTH") {
			t.Fatal("AUTH went to an nsqd whose certificate Config.TLS does not trust")
		}
	})
}

// TestAPingAsksOnlyTheNSQDItIsAboutToUse holds Ping to the one nsqd it reaches.
//
// Ping reaches one nsqd, the next in round-robin order. A check that asked every
// address in Config.Addrs first, one after another, let a single nsqd that
// accepted and never answered spend the caller's whole context, and a
// readiness check with any one nsqd down failed although the nsqd it pinged
// was healthy. Over N calls the round robin still asks each nsqd.
func TestAPingAsksOnlyTheNSQDItIsAboutToUse(t *testing.T) {
	// An nsqd that accepts and never answers: the kernel completes the
	// connection and nothing reads it.
	hung, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	t.Cleanup(func() { _ = hung.Close() })
	server, client := tlsPair(t)
	healthy := newWireNSQD(t)
	healthy.mu.Lock()
	healthy.identify = identifyAgreeingToTLS
	healthy.upgrade = server
	healthy.auth = wireAnswer{data: `{"identity":"probe","identity_url":"","permission_count":1}`}
	healthy.mu.Unlock()

	// Opening connects to neither.
	openCtx, cancelOpen := context.WithTimeout(context.Background(), time.Second)
	defer cancelOpen()
	b, err := OpenBackend(openCtx, Config{
		Addrs: []string{hung.Addr().String(), healthy.ln.Addr().String()},
		TLS:   client, AuthSecret: dsn.Secret("s3cret-token"), APITimeout: 2 * time.Second,
		// go-nsq's own connect to the hung nsqd, if anything made one, would
		// end here rather than hold Close for a minute.
		ReadTimeout: time.Second, HeartbeatInterval: 500 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("OpenBackend: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })

	ping := func() (time.Duration, error) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		start := time.Now()
		err := b.Ping(ctx)
		return time.Since(start), err
	}
	// The first Ping reaches the hung nsqd and fails, as a Ping of a dead
	// nsqd always has.
	if _, err := ping(); err == nil {
		t.Fatal("a Ping that reached the nsqd that never answers succeeded")
	}
	// The second reaches the healthy one, and the hung one costs it nothing.
	took, err := ping()
	if err != nil {
		t.Fatalf("Ping of the healthy nsqd = %v after %v, with the other nsqd in Addrs not answering; want "+
			"success: it must ask only the nsqd it pings", err, took)
	}
	if took > 500*time.Millisecond {
		t.Fatalf("Ping of the healthy nsqd took %v; the nsqd that never answers was asked too", took)
	}
	if st := b.Health().State; st != mq.HealthReady {
		t.Fatalf("health is %v after a Ping of the healthy nsqd succeeded, want ready", st)
	}
}

// TestARefusedSubscriptionFailsOpenSource holds nj-24: nsqd refusing SUB, which
// it does when its auth server does not let this identity subscribe to the
// channel, fails OpenSource with what nsqd said.
//
// go-nsq v1.1.0 wrote SUB without reading the answer. OpenSource succeeded, the
// source received nothing, health stayed ready, and go-nsq redialled every
// Config.LookupdPollInterval to be refused again.
func TestARefusedSubscriptionFailsOpenSource(t *testing.T) {
	const refusal = `E_UNAUTHORIZED AUTH failed for SUB on "orders" "workers"`
	w := newWireNSQD(t)
	w.mu.Lock()
	w.subscribe = wireAnswer{frameType: gonsq.FrameTypeError, data: refusal}
	w.mu.Unlock()
	b := openWireBackend(t, w, Config{})
	src, err := b.OpenSource(context.Background(), mq.SourceConfig{Topic: "orders", Subscription: "workers", Prefetch: 1})
	if src != nil {
		_ = src.Close()
	}
	if code := mq.CodeOf(err); code != mq.CodeUnauthorized || mq.IsRetryable(err) || !strings.Contains(err.Error(), refusal) {
		t.Fatalf("OpenSource that nsqd refused to subscribe returned %v (code %v, retryable %v), want nsqd's refusal "+
			"as a CodeUnauthorized that is not retryable", err, code, mq.IsRetryable(err))
	}
	if cmds := w.commandsOf(0); hasRDY(cmds) {
		t.Fatalf("the refused subscription asked nsqd for messages: %q", cmds)
	}
}

// TestTheCallersContextBoundsTheHandshake holds rn-18: a publication, a
// subscription and a Ping whose context ends while go-nsq is still waiting for
// nsqd's answer to IDENTIFY return then, with the context's error. The
// publication's failure is definite, because no PUB was written.
//
// go-nsq v1.1.0 took no context for its connect, so each of them waited for
// Config.DialTimeout plus Config.ReadTimeout, a minute and five seconds with
// the defaults, however soon its context ended.
func TestTheCallersContextBoundsTheHandshake(t *testing.T) {
	held := func(t *testing.T) (*Backend, *wireNSQD) {
		t.Helper()
		w := newWireNSQD(t)
		// The answers wait for the server's own close, at the end of the test.
		never := make(chan struct{})
		w.mu.Lock()
		for n := range 4 {
			w.hold[n] = never
		}
		w.mu.Unlock()
		// A read timeout well past the context's, so that only the context
		// can end the wait.
		return openWireBackend(t, w, Config{ReadTimeout: 3 * time.Second, HeartbeatInterval: time.Second}), w
	}
	bounded := func(t *testing.T, what string, call func(ctx context.Context) error) {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()
		start := time.Now()
		err := call(ctx)
		if took := time.Since(start); !errors.Is(err, context.DeadlineExceeded) || took > time.Second {
			t.Fatalf("%s under a 100ms context, against an nsqd that never answers IDENTIFY, returned %v after %v; "+
				"want context.DeadlineExceeded at 100ms", what, err, took)
		}
	}
	t.Run("a publication", func(t *testing.T) {
		b, w := held(t)
		snk, err := b.OpenSink(context.Background(), mq.SinkConfig{Topic: "orders"})
		if err != nil {
			t.Fatalf("OpenSink: %v", err)
		}
		bounded(t, "Publish", func(ctx context.Context) error {
			err = snk.Publish(ctx, &mq.Message{Body: []byte("x")}, nil)
			return err
		})
		if mq.IsAmbiguous(err) || w.received("PUB") {
			t.Fatalf("Publish cut short in the handshake is %v (ambiguous %v, PUB sent %v); nothing was written, so it is "+
				"definite", err, mq.IsAmbiguous(err), w.received("PUB"))
		}
	})
	t.Run("a subscription", func(t *testing.T) {
		b, w := held(t)
		bounded(t, "OpenSource", func(ctx context.Context) error {
			src, err := b.OpenSource(ctx, mq.SourceConfig{Topic: "orders", Subscription: "workers", Prefetch: 1})
			if src != nil {
				_ = src.Close()
			}
			return err
		})
		if w.received("SUB") {
			t.Fatal("go-nsq subscribed on the connection the context cut short")
		}
	})
	t.Run("a Ping", func(t *testing.T) {
		b, _ := held(t)
		bounded(t, "Ping", b.Ping)
	})
}

// TestALinkCutJustAfterTheHandshakeIsUnavailable holds the rest of what
// TestAHandshakeTheLinkCutIsUnavailableNotAConfigurationError holds.
//
// go-nsq reports two writes of its handshake as text and not as an
// ErrIdentify: the protocol magic, and a consumer's SUB, which it writes once
// IDENTIFY is answered. The network error inside is formatted into the message,
// so the network classification never saw it, and an nsqd that reset the
// connection as it went away made OpenSource fail as CodeUnknown, not
// retryable: the core gave up on a subscription a restart had cut.
func TestALinkCutJustAfterTheHandshakeIsUnavailable(t *testing.T) {
	t.Run("a SUB the reset refused", func(t *testing.T) {
		const attempts = 20
		w := newWireNSQD(t)
		w.mu.Lock()
		for n := range attempts {
			w.reset[n] = true
		}
		w.mu.Unlock()
		b := openWireBackend(t, w, Config{})
		failedSUB := 0
		for range attempts {
			src, err := b.OpenSource(context.Background(), mq.SourceConfig{Topic: "orders", Subscription: "workers", Prefetch: 1})
			if src != nil {
				// The SUB went out before the reset arrived.
				_ = src.Close()
				continue
			}
			if code := mq.CodeOf(err); code != mq.CodeUnavailable || !mq.IsRetryable(err) {
				t.Fatalf("OpenSource against an nsqd that reset the connection once it had answered IDENTIFY returned %v "+
					"(code %v, retryable %v), want a retryable Unavailable", err, code, mq.IsRetryable(err))
			}
			if strings.Contains(err.Error(), "failed to subscribe to") {
				failedSUB++
			}
		}
		if failedSUB == 0 {
			t.Fatalf("in %d attempts no SUB write met the reset, so the case was never set up", attempts)
		}
	})
	t.Run("the texts", func(t *testing.T) {
		m := newErrorMapper()
		reset := "write tcp 127.0.0.1:41000->127.0.0.1:4150: write: connection reset by peer"
		for op, native := range map[string]error{
			mq.OpOpen:    fmt.Errorf("[%s] failed to subscribe to %s:%s - %s", "127.0.0.1:4150", "orders", "workers", reset),
			mq.OpPublish: fmt.Errorf("[%s] failed to write magic - %s", "127.0.0.1:4150", reset),
		} {
			mapped := m.Map(op, "orders", "workers", native)
			if code := mq.CodeOf(mapped); code != mq.CodeUnavailable || !mq.IsRetryable(mapped) || mq.IsAmbiguous(mapped) {
				t.Errorf("%s failing with %q mapped to %v (retryable %v, ambiguous %v), want a retryable, definite "+
					"Unavailable", op, native, code, mq.IsRetryable(mapped), mq.IsAmbiguous(mapped))
			}
		}
	})
}
