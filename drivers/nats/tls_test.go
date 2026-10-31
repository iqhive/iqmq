package nats_test

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"math/big"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"

	nq "github.com/iqhive/iqmq/drivers/nats"
)

// selfSignedTLS is a server TLS configuration with a certificate no root
// trusts, so a client that verifies it refuses the server.
func selfSignedTLS(t *testing.T) *tls.Config {
	t.Helper()
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
	return &tls.Config{
		MinVersion:   tls.VersionTLS12,
		Certificates: []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: key}},
	}
}

// TestTypedInsecureSkipVerifyIsHonoured holds that Config.InsecureSkipVerify
// does what it says on a typed Config as it does from a DSN. Only ParseDSN
// used to copy it into the tls.Config, so the same two settings connected
// from a DSN and failed certificate verification from a Config.
func TestTypedInsecureSkipVerifyIsHonoured(t *testing.T) {
	srv, err := server.NewServer(&server.Options{
		Host: "127.0.0.1", Port: -1, NoLog: true, NoSigs: true,
		TLS: true, TLSConfig: selfSignedTLS(t), TLSTimeout: 2,
	})
	if err != nil {
		t.Fatalf("server: %v", err)
	}
	go srv.Start()
	if !srv.ReadyForConnections(10 * time.Second) {
		t.Fatal("the server did not become ready")
	}
	t.Cleanup(func() { srv.Shutdown(); srv.WaitForShutdown() })
	addr := "nats://127.0.0.1:" + strconv.Itoa(srv.Addr().(*net.TCPAddr).Port)

	callers := &tls.Config{MinVersion: tls.VersionTLS12}
	typed := nq.Config{Addrs: []string{addr}, TLS: callers, InsecureSkipVerify: true}
	c, err := nq.Open(t.Context(), typed)
	if err != nil {
		t.Fatalf("Config{TLS: non-nil, InsecureSkipVerify: true} still verified the certificate: %v", err)
	}
	_ = c.Close()
	if callers.InsecureSkipVerify {
		t.Fatal("Open wrote InsecureSkipVerify into the caller's own tls.Config")
	}

	verified := nq.Config{Addrs: []string{addr}, TLS: &tls.Config{MinVersion: tls.VersionTLS12}}
	if c, err := nq.Open(t.Context(), verified); err == nil {
		_ = c.Close()
		t.Fatal("a Config without InsecureSkipVerify accepted a certificate no root trusts")
	}
}
