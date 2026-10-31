package redis_test

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"io"
	"math/big"
	"net"
	"sync"
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
	rd "github.com/iqhive/iqmq/drivers/redis"
	"github.com/iqhive/iqmq/internal/streams/fakestreams"
	"github.com/iqhive/iqmq/internal/streams/respsrv"
)

// serveTLS is an in-memory Redis behind a TLS listener whose certificate no
// system root trusts. It returns the listener's address and a pool that
// trusts the certificate.
func serveTLS(t *testing.T) (string, *x509.CertPool) {
	t.Helper()
	srv, err := respsrv.Serve(fakestreams.New(fakestreams.Options{AOF: true, Version: "7.4.2"}), respsrv.Options{Version: "7.4.2"})
	if err != nil {
		t.Fatalf("respsrv.Serve: %v", err)
	}
	t.Cleanup(func() { _ = srv.Close() })
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "iqmq-test"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("certificate: %v", err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("certificate: %v", err)
	}
	roots := x509.NewCertPool()
	roots.AddCert(cert)
	ln, err := tls.Listen("tcp", "127.0.0.1:0", &tls.Config{
		MinVersion:   tls.VersionTLS12,
		Certificates: []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: key}},
	})
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	var (
		wg    sync.WaitGroup
		mu    sync.Mutex
		conns []net.Conn
	)
	wg.Go(func() {
		for {
			c, aerr := ln.Accept()
			if aerr != nil {
				return
			}
			up, derr := net.Dial("tcp", srv.Addr())
			if derr != nil {
				_ = c.Close()
				continue
			}
			mu.Lock()
			conns = append(conns, c, up)
			mu.Unlock()
			for _, pair := range [][2]net.Conn{{c, up}, {up, c}} {
				wg.Go(func() {
					_, _ = io.Copy(pair[0], pair[1])
					_ = pair[0].Close()
					_ = pair[1].Close()
				})
			}
		}
	})
	t.Cleanup(func() {
		_ = ln.Close()
		mu.Lock()
		for _, c := range conns {
			_ = c.Close()
		}
		mu.Unlock()
		wg.Wait()
	})
	return ln.Addr().String(), roots
}

// TestTypedInsecureSkipVerifyIsHonoured is Config.InsecureSkipVerify on a
// typed configuration.
//
// Its doc says it disables certificate verification, Validate accepts it
// beside TLS, and String() and DSN() render it. Only ParseDSN copied it into
// the *tls.Config, so a typed Config that set it still verified the
// certificate, and the configuration said the opposite of what the
// connection did. It is applied to a copy: the caller's own *tls.Config is
// never written to.
func TestTypedInsecureSkipVerifyIsHonoured(t *testing.T) {
	addr, roots := serveTLS(t)
	open := func(cfg rd.Config) error {
		cfg.Addrs, cfg.AutoCreate, cfg.ClaimInterval = []string{addr}, true, time.Hour
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		b, err := rd.OpenBackend(ctx, cfg)
		if err != nil {
			return err
		}
		defer func() { _ = b.Close() }()
		sink, err := b.OpenSink(ctx, mq.SinkConfig{Topic: testTopic})
		if err != nil {
			return err
		}
		defer func() { _ = sink.Close() }()
		return sink.Publish(ctx, &mq.Message{Topic: testTopic, Body: []byte("over TLS")}, nil)
	}

	// A refused handshake is not tried here: rueidis schedules a cluster
	// refresh after it and leaves the goroutine sleeping past Close, which
	// this package's goroutine check would report. The go-redis arm's test
	// holds the refusal.
	if err := open(rd.Config{TLS: &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots}}); err != nil {
		t.Fatalf("a Config that trusts the server's certificate: %v", err)
	}
	callers := &tls.Config{MinVersion: tls.VersionTLS12}
	typed := rd.Config{TLS: callers, InsecureSkipVerify: true}
	if got := rd.ClientOptions(typed).TLSConfig; got == nil || !got.InsecureSkipVerify {
		t.Errorf("ClientOptions hands rueidis a TLS configuration that verifies, for a Config with InsecureSkipVerify (%s)", typed)
	}
	if err := open(typed); err != nil {
		t.Fatalf("Config{TLS: non-nil, InsecureSkipVerify: true} still verified the certificate: %v", err)
	}
	if callers.InsecureSkipVerify {
		t.Fatal("the driver wrote InsecureSkipVerify into the caller's own tls.Config")
	}
}
