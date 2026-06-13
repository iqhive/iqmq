package pulsar

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"errors"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"

	"bitbucket.org/iqhive/iqmq/dsn"
)

// tlsPort stands in for a broker's TLS port. It speaks no Pulsar at all: it
// completes the TLS handshake or fails it, reports which, and hangs up. That
// is the whole question a TLS configuration answers, so no broker is needed.
//
// Its certificate is httptest's, which is valid for 127.0.0.1 and trusted by
// nothing but the CA file written beside it.
type tlsPort struct {
	addr       string
	caFile     string
	handshakes chan handshake
}

type handshake struct {
	err         error
	clientCerts int
}

func newTLSPort(t *testing.T, requireClientCert bool) *tlsPort {
	t.Helper()
	src := httptest.NewUnstartedServer(http.NotFoundHandler())
	src.StartTLS()
	cert := src.TLS.Certificates[0]
	caPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: src.Certificate().Raw})
	src.Close()

	caFile := filepath.Join(t.TempDir(), "ca.pem")
	if err := os.WriteFile(caFile, caPEM, 0o600); err != nil {
		t.Fatalf("writing the CA file: %v", err)
	}
	cfg := &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12}
	if requireClientCert {
		cfg.ClientAuth = tls.RequireAnyClientCert
	}
	ln, err := tls.Listen("tcp", "127.0.0.1:0", cfg)
	if err != nil {
		t.Fatalf("tls.Listen: %v", err)
	}
	p := &tlsPort{addr: ln.Addr().String(), caFile: caFile, handshakes: make(chan handshake, 64)}
	// The port's goroutines are the test's own, so they are gone before the
	// test is: a handshake still waiting on a client that never finished it
	// would otherwise outlive the test and fail the package's goroutine
	// check with nothing pointing here.
	var wg sync.WaitGroup
	var mu sync.Mutex
	open := map[net.Conn]struct{}{}
	wg.Go(func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			mu.Lock()
			open[conn] = struct{}{}
			mu.Unlock()
			wg.Go(func() {
				defer func() {
					mu.Lock()
					delete(open, conn)
					mu.Unlock()
					_ = conn.Close()
				}()
				tc, ok := conn.(*tls.Conn)
				if !ok {
					return
				}
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				herr := tc.HandshakeContext(ctx)
				cancel()
				select {
				case p.handshakes <- handshake{err: herr, clientCerts: len(tc.ConnectionState().PeerCertificates)}:
				default:
				}
			})
		}
	})
	t.Cleanup(func() {
		_ = ln.Close()
		mu.Lock()
		for c := range open {
			_ = c.Close()
		}
		mu.Unlock()
		wg.Wait()
	})
	return p
}

// dial makes the client pulsar-client-go builds from cfg open a connection to
// the port, and returns what the port saw of the first handshake.
func (p *tlsPort) dial(t *testing.T, cfg Config) handshake {
	t.Helper()
	opts, err := clientOptions(cfg.resolve())
	if err != nil {
		t.Fatalf("clientOptions: %v", err)
	}
	cl, err := pulsar.NewClient(opts)
	if err != nil {
		t.Fatalf("pulsar.NewClient: %v", err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		// A lookup is the cheapest thing that opens a connection. It fails
		// -- nothing here speaks Pulsar -- within the operation timeout.
		_, _ = cl.TopicPartitions("persistent://public/default/t")
	}()
	defer func() {
		cl.Close()
		<-done
	}()
	select {
	case h := <-p.handshakes:
		return h
	case <-time.After(10 * time.Second):
		t.Fatal("the client never reached the TLS port")
		return handshake{}
	}
}

// dsnFor is a pulsar+ssl DSN for the port with the given parameters and
// short timeouts, so a client that cannot speak to it gives up quickly.
func (p *tlsPort) dsnFor(t *testing.T, params url.Values) Config {
	t.Helper()
	params.Set("connectiontimeout", "2s")
	params.Set("operationtimeout", "3s")
	cfg, err := ParseDSN("pulsar+ssl://" + p.addr + "/acme/billing?" + params.Encode())
	if err != nil {
		t.Fatalf("ParseDSN: %v", err)
	}
	return cfg
}

// TestAPulsarSSLDSNVerifiesWithItsTrustStore: trustcerts names the CA the
// broker is verified with. ParseDSN used to set Config.TLS
// for every pulsar+ssl DSN, and pulsar-client-go uses a Config.TLS as given
// and never reads the trust file beside it, so the broker was verified against
// the SYSTEM roots: a broker whose certificate only the named CA vouches for
// was refused, and one any public CA vouches for was accepted -- trust widened
// silently past what the operator pinned.
func TestAPulsarSSLDSNVerifiesWithItsTrustStore(t *testing.T) {
	t.Parallel()
	port := newTLSPort(t, false)
	h := port.dial(t, port.dsnFor(t, url.Values{"trustcerts": {port.caFile}}))
	if h.err != nil {
		t.Fatalf("the broker's handshake failed (%v) with trustcerts naming the CA that signed its certificate; "+
			"the DSN's trust store was not used", h.err)
	}
}

// TestAPulsarSSLDSNVerifiesTheBroker is the other side of the trust store: a
// DSN that names none verifies against the system roots, which do not trust
// the test CA, so the handshake is refused. A client that skipped
// verification would pass the test above and fail this one.
func TestAPulsarSSLDSNVerifiesTheBroker(t *testing.T) {
	t.Parallel()
	port := newTLSPort(t, false)
	h := port.dial(t, port.dsnFor(t, url.Values{}))
	if h.err == nil {
		t.Fatal("a pulsar+ssl DSN with no trust store completed a handshake with a broker only a private CA " +
			"vouches for; it is not verifying the broker")
	}
}

// TestAPulsarSSLDSNPresentsItsTLSClientCertificate: authname=tls is a client
// certificate on the connection. pulsar-client-go adds the
// provider's certificate only when it builds the TLS configuration itself, so
// the Config.TLS ParseDSN used to set meant it was never presented, and a
// broker that requires one refused every connection.
func TestAPulsarSSLDSNPresentsItsTLSClientCertificate(t *testing.T) {
	t.Parallel()
	port := newTLSPort(t, true)
	certFile, keyFile := clientCertificate(t)
	params, err := json.Marshal(map[string]string{"tlsCertFile": certFile, "tlsKeyFile": keyFile})
	if err != nil {
		t.Fatalf("json.Marshal: %v", err)
	}
	h := port.dial(t, port.dsnFor(t, url.Values{
		"trustcerts": {port.caFile},
		"authname":   {"tls"},
		"authparams": {string(params)},
	}))
	if h.err != nil || h.clientCerts != 1 {
		t.Fatalf("a broker requiring a client certificate saw handshake error %v and %d client certificate(s), "+
			"want none and 1: the DSN's authname=tls certificate was not presented", h.err, h.clientCerts)
	}
}

// TestATLSAuthProviderIsNeverPresentedOnAPlaintextScheme is the measurement
// that justifies refusing `AuthName: "tls"` beside a "pulsar://" service URL.
//
// `fix/dsn-dial` recorded the pairing as accepted, with "the client certificate
// is then simply never presented" as a claim about pulsar-client-go's source.
// This reads it off a socket instead, because the whole reason that branch found
// the TLS class at all is that every driver in it DID configure the library
// faithfully and the library ignored the configuration.
//
// The port REQUIRES a client certificate, so if one were presented it would be
// counted. What it sees instead is no TLS handshake at all -- the client wrote a
// plaintext Pulsar CONNECT frame, because pulsar-client-go picks the transport
// from the scheme alone -- and therefore zero certificates.
// `TestAPulsarSSLDSNPresentsItsTLSClientCertificate` is the positive control:
// the same authentication on "pulsar+ssl://" presents exactly one.
//
// It dials through `clientOptions` and NOT through `Validate`, deliberately.
// Validate refuses this pairing now, so a measurement that went through it could
// no longer be taken -- and a refusal whose premise nothing checks is a refusal
// that outlives its reason. The refusal itself is asserted at the end, so the
// two cannot drift apart.
func TestATLSAuthProviderIsNeverPresentedOnAPlaintextScheme(t *testing.T) {
	t.Parallel()
	port := newTLSPort(t, true)
	certFile, keyFile := clientCertificate(t)
	params, err := json.Marshal(map[string]string{"tlsCertFile": certFile, "tlsKeyFile": keyFile})
	if err != nil {
		t.Fatalf("json.Marshal: %v", err)
	}
	cfg := Config{
		ServiceURL:            "pulsar://" + port.addr,
		AuthName:              "tls",
		AuthParams:            dsn.Secret(params),
		TLSTrustCertsFilePath: port.caFile,
		ConnectionTimeout:     2 * time.Second,
		OperationTimeout:      3 * time.Second,
	}
	h := port.dial(t, cfg)
	if h.clientCerts != 0 {
		t.Fatalf("a client dialling pulsar:// presented %d client certificate(s); pulsar-client-go picks the "+
			"transport from the scheme, so there is no handshake to present one in and the refusal this "+
			"measurement justifies would be unnecessary", h.clientCerts)
	}
	if h.err == nil {
		t.Fatal("a TLS port that requires a client certificate completed a handshake with a client dialling " +
			"pulsar://; the measurement itself is wrong")
	}
	t.Logf("the port saw no TLS handshake (%v) and 0 client certificates, with authname=tls configured", h.err)
	if verr := cfg.Validate(); !errors.Is(verr, errTLSAuthWithoutTLS) {
		t.Errorf("Validate = %v, want %v: the measurement above is what this refusal rests on", verr,
			errTLSAuthWithoutTLS)
	}
}

// TestTLSAuthenticationNeedsATLSScheme is the refusal's boundary, in both
// directions.
//
// Widening a refusal is exactly when a working configuration starts failing, so
// the mechanisms that do NOT need a handshake are listed beside the one that
// does: Token, "oauth2", "basic" and "athenz" all travel in the CONNECT frame
// and are none of this rule's business on either scheme.
func TestTLSAuthenticationNeedsATLSScheme(t *testing.T) {
	t.Parallel()
	for name, cfg := range map[string]Config{
		"the short name":                {ServiceURL: "pulsar://broker:6650", AuthName: "tls"},
		"the long name":                 {ServiceURL: "pulsar://broker:6650", AuthName: "org.apache.pulsar.client.impl.auth.AuthenticationTls"},
		"the default plaintext service": {AuthName: "tls"},
	} {
		if err := cfg.Validate(); !errors.Is(err, errTLSAuthWithoutTLS) {
			t.Errorf("%s: Validate = %v, want %v", name, err, errTLSAuthWithoutTLS)
		}
	}
	for name, cfg := range map[string]Config{
		"TLS authentication on a TLS scheme": {ServiceURL: "pulsar+ssl://broker:6651", AuthName: "tls"},
		"the long name on a TLS scheme":      {ServiceURL: "pulsar+ssl://broker:6651", AuthName: "org.apache.pulsar.client.impl.auth.AuthenticationTls"},
		"oauth2 in the clear":                {ServiceURL: "pulsar://broker:6650", AuthName: "oauth2", AuthParams: "{}"},
		"basic in the clear":                 {ServiceURL: "pulsar://broker:6650", AuthName: "basic", AuthParams: "{}"},
		"a token in the clear":               {ServiceURL: "pulsar://broker:6650", Token: "t"},
		"no authentication at all":           {ServiceURL: "pulsar://broker:6650"},
	} {
		if err := cfg.Validate(); err != nil {
			t.Errorf("%s: Validate = %v, want it accepted", name, err)
		}
	}
}

// TestContradictoryTLSSettingsAreRefused: a Config.TLS is the whole TLS
// configuration to pulsar-client-go, so a setting beside it that it would
// ignore is refused at construction, as the driver documentation asks, rather than
// dropped.
func TestContradictoryTLSSettingsAreRefused(t *testing.T) {
	t.Parallel()
	base := func() Config {
		return Config{ServiceURL: "pulsar+ssl://broker:6651", TLS: &tls.Config{MinVersion: tls.VersionTLS12}}
	}
	for name, tc := range map[string]struct {
		mutate func(*Config)
		want   error
	}{
		"a trust file":             {func(c *Config) { c.TLSTrustCertsFilePath = "/etc/ssl/ca.pem" }, errTLSWithTrustFile},
		"an insecure flag":         {func(c *Config) { c.InsecureSkipVerify = true }, errTLSWithInsecure},
		"TLS authentication":       {func(c *Config) { c.AuthName = "tls" }, errTLSWithTLSAuth},
		"TLS authentication, long": {func(c *Config) { c.AuthName = "org.apache.pulsar.client.impl.auth.AuthenticationTls" }, errTLSWithTLSAuth},
	} {
		c := base()
		tc.mutate(&c)
		if err := c.Validate(); !errors.Is(err, tc.want) {
			t.Errorf("Config.TLS with %s: Validate = %v, want %v", name, err, tc.want)
		}
	}
	// What is not contradictory stays accepted.
	for name, mutate := range map[string]func(*Config){
		"Config.TLS alone":                    func(*Config) {},
		"an insecure flag Config.TLS carries": func(c *Config) { c.InsecureSkipVerify = true; c.TLS.InsecureSkipVerify = true },
		"a token":                             func(c *Config) { c.Token = "t" },
		"no Config.TLS, a trust file":         func(c *Config) { c.TLS = nil; c.TLSTrustCertsFilePath = "/etc/ssl/ca.pem" },
		"no Config.TLS, TLS authentication":   func(c *Config) { c.TLS = nil; c.AuthName = "tls" },
	} {
		c := base()
		mutate(&c)
		if err := c.Validate(); err != nil {
			t.Errorf("%s: Validate = %v, want nil", name, err)
		}
	}
}

// clientCertificate writes a self-signed client certificate and its key, and
// returns their paths.
func clientCertificate(t *testing.T) (string, string) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "iqmq-test-client"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("CreateCertificate: %v", err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatalf("MarshalECPrivateKey: %v", err)
	}
	dir := t.TempDir()
	certFile, keyFile := filepath.Join(dir, "client.pem"), filepath.Join(dir, "client.key")
	if err := os.WriteFile(certFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600); err != nil {
		t.Fatalf("writing the client certificate: %v", err)
	}
	if err := os.WriteFile(keyFile, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}), 0o600); err != nil {
		t.Fatalf("writing the client key: %v", err)
	}
	return certFile, keyFile
}
