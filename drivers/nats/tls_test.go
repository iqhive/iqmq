package nats_test

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"math/big"
	"net"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"

	mq "github.com/iqhive/iqmq"
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

// tlsCutServer accepts connections, sends each an INFO that requires TLS and
// hands it to then, which ends it the way the test needs; done closes when
// the test ends.
func tlsCutServer(t *testing.T, then func(c net.Conn, done <-chan struct{})) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	done := make(chan struct{})
	var owner sync.WaitGroup
	owner.Add(1)
	go func() {
		defer owner.Done()
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			owner.Add(1)
			go func() {
				defer owner.Done()
				defer func() { _ = c.Close() }()
				_, _ = c.Write([]byte(`INFO {"server_id":"tls-cut","version":"2.10.0","proto":1,"max_payload":1048576,"tls_required":true}` + "\r\n"))
				then(c, done)
			}()
		}
	}()
	t.Cleanup(func() {
		close(done)
		_ = ln.Close()
		owner.Wait()
	})
	return ln.Addr().String()
}

// The ways tlsCutServer ends a connection: a reset once the client's
// handshake starts, as a server going away does; no answer to it at all; and
// a close once the handshake has completed, as a proxy refusing the client's
// certificate does.
var (
	resetHandshake = func(c net.Conn, _ <-chan struct{}) {
		if _, err := c.Read(make([]byte, 1)); err == nil {
			_ = c.(*net.TCPConn).SetLinger(0)
		}
	}
	holdHandshake = func(c net.Conn, done <-chan struct{}) {
		if _, err := c.Read(make([]byte, 1)); err == nil {
			<-done
		}
	}
	closeAfterHandshake = func(cfg *tls.Config) func(net.Conn, <-chan struct{}) {
		return func(c net.Conn, _ <-chan struct{}) {
			tc := tls.Server(c, cfg)
			if tc.Handshake() == nil {
				_ = tc.Close()
			}
		}
	}
)

// TestATLSHandshakeTheLinkCutIsRetryable holds that a TLS handshake the link
// ended, by a reset or by Open's deadline, is a failure a retry can get past,
// as the same cut before TLS is, and that a certificate refused, by either
// end, is not. nats.go reports every handshake failure as a TLS error, and the
// driver read each one as a certificate refusal: a non-retryable Unavailable,
// so a caller that retries only what is retryable gave up on a server
// restarting.
func TestATLSHandshakeTheLinkCutIsRetryable(t *testing.T) {
	open := func(t *testing.T, addr string, skipVerify bool) error {
		t.Helper()
		ctx, cancel := context.WithTimeout(t.Context(), 300*time.Millisecond)
		defer cancel()
		b, err := nq.OpenBackend(ctx, nq.Config{
			Addrs:              []string{addr},
			TLS:                &tls.Config{MinVersion: tls.VersionTLS12},
			InsecureSkipVerify: skipVerify,
			ConnectTimeout:     5 * time.Second,
		})
		if err == nil {
			_ = b.Close()
			t.Fatal("Open succeeded against a server whose TLS handshake cannot succeed")
		}
		return err
	}
	for _, tc := range []struct {
		name      string
		then      func(net.Conn, <-chan struct{})
		code      mq.ErrorCode
		retryable bool
	}{
		{"the server resets the connection", resetHandshake, mq.CodeUnavailable, true},
		{"Open's deadline ends the handshake", holdHandshake, mq.CodeDeadlineExceeded, true},
		{"the server closes the connection once the handshake is done", closeAfterHandshake(selfSignedTLS(t)), mq.CodeUnavailable, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := open(t, "nats://"+tlsCutServer(t, tc.then), true)
			if mq.CodeOf(err) != tc.code || mq.IsRetryable(err) != tc.retryable {
				t.Errorf("Open whose TLS handshake %s = %v (code %v, retryable %v), want %v and retryable %v",
					tc.name, err, mq.CodeOf(err), mq.IsRetryable(err), tc.code, tc.retryable)
			}
		})
	}
	t.Run("a certificate no root trusts", func(t *testing.T) {
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
		err = open(t, "nats://127.0.0.1:"+strconv.Itoa(srv.Addr().(*net.TCPAddr).Port), false)
		if mq.CodeOf(err) != mq.CodeUnavailable || mq.IsRetryable(err) {
			t.Errorf("Open that refused the server's certificate = %v (code %v, retryable %v), want Unavailable and not retryable",
				err, mq.CodeOf(err), mq.IsRetryable(err))
		}
	})
}
