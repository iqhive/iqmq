package stomp

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
	"testing"
	"time"
)

// TestATLSDialToAnIPv6LiteralVerifiesItsAddress holds dialNet to verifying an
// IPv6 literal against the address itself.
//
// With no ServerName configured, dialNet takes one from the endpoint, and
// splitHostPort leaves an IPv6 literal's brackets and zone on: "[::1%lo]".
// That is the name crypto/tls's own Dial derives, everything before the last
// colon, and it verifies: x509's VerifyHostname takes the brackets off and
// reads a zone, and the handshake sends no SNI for an IP literal. A review
// suspected a literal with a zone could never verify, which is what this pins.
// The listener is on ::1 with a certificate for ::1, dialled with and without
// a zone.
func TestATLSDialToAnIPv6LiteralVerifiesItsAddress(t *testing.T) {
	t.Parallel()
	lo := loopbackName()
	if lo == "" {
		t.Skip("this host has no loopback interface to name as a zone")
	}
	ln, err := net.Listen("tcp", "[::1]:0")
	if err != nil {
		t.Skipf("this host cannot listen on the IPv6 loopback: %v", err)
	}
	cert, roots := loopbackCertificate(t)
	tl := tls.NewListener(ln, &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12})
	// The server side hand-shakes each connection and closes it, until the
	// listener closes.
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			c, err := tl.Accept()
			if err != nil {
				return
			}
			_ = c.SetDeadline(time.Now().Add(10 * time.Second))
			_ = c.(*tls.Conn).Handshake()
			_ = c.Close()
		}
	}()
	t.Cleanup(func() {
		_ = tl.Close()
		<-done
	})
	port := strconv.Itoa(ln.Addr().(*net.TCPAddr).Port)
	b := &Backend{cfg: Config{TLS: &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}}}
	for _, addr := range []string{"[::1]:" + port, "[::1%" + lo + "]:" + port, "stomp+ssl://[::1%" + lo + "]:" + port} {
		ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
		conn, err := b.dialNet(ctx, addr)
		cancel()
		if err != nil {
			t.Errorf("a TLS dial to %s, whose certificate names ::1: %v", addr, err)
			continue
		}
		_ = conn.Close()
	}
}

// TestATLSSchemeDialsTLSInAnyCase holds the address's scheme to choosing TLS
// whatever its case.
//
// A scheme is case-insensitive, and stripScheme strips one in any case, but
// isTLSAddr compared it in lower case only, so "STOMP+SSL://h" was stripped
// to "h" and dialled in plain text: the CONNECT frame, its login and every
// message went in clear to an address that asked for TLS. Every case variant
// of the four TLS schemes dials a TLS listener here with no Config.TLS, so the
// scheme alone decides, and must come back as a finished TLS connection.
func TestATLSSchemeDialsTLSInAnyCase(t *testing.T) {
	t.Parallel()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listening on 127.0.0.1: %v", err)
	}
	cert, _ := loopbackCertificate(t)
	tl := tls.NewListener(ln, &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12})
	// The server side hand-shakes each connection and closes it, until the
	// listener closes. A plain dial never sends a ClientHello, so its
	// handshake ends at the deadline or when the client closes.
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			c, err := tl.Accept()
			if err != nil {
				return
			}
			_ = c.SetDeadline(time.Now().Add(10 * time.Second))
			_ = c.(*tls.Conn).Handshake()
			_ = c.Close()
		}
	}()
	t.Cleanup(func() {
		_ = tl.Close()
		<-done
	})
	target := "127.0.0.1:" + strconv.Itoa(ln.Addr().(*net.TCPAddr).Port)
	// The certificate names ::1, so verification is off: what is under test
	// is whether the dial speaks TLS at all, not what it trusts.
	b := &Backend{cfg: Config{InsecureSkipVerify: true}}
	dialled := 0
	for _, scheme := range []string{"stomp+ssl", "ssl", "stomps", "tls"} {
		for _, variant := range caseVariants(scheme) {
			addr := variant + "://" + target
			if !isTLSAddr(addr) {
				t.Errorf("isTLSAddr(%q) = false: the scheme asks for TLS", addr)
			}
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			conn, err := b.dialNet(ctx, addr)
			cancel()
			if err != nil {
				t.Errorf("dialling %q: %v", addr, err)
				continue
			}
			dialled++
			tc, ok := conn.(*tls.Conn)
			_ = conn.Close()
			if !ok || !tc.ConnectionState().HandshakeComplete {
				t.Errorf("dialling %q gave a %T, not a TLS connection: the login and every frame would go in clear",
					addr, conn)
			}
		}
	}
	if dialled != 256+8+64+8 {
		t.Errorf("dialled %d case variants, want %d", dialled, 256+8+64+8)
	}
	// The plain schemes stay plain in any case.
	for _, addr := range []string{"stomp://h", "STOMP://h", "tcp://h", "TCP://h", "h:61613"} {
		if isTLSAddr(addr) {
			t.Errorf("isTLSAddr(%q) = true: the scheme does not ask for TLS", addr)
		}
	}
}

// caseVariants is every way of casing the letters of s.
func caseVariants(s string) []string {
	var letters []int
	for i := range len(s) {
		if c := s[i]; c >= 'a' && c <= 'z' {
			letters = append(letters, i)
		}
	}
	out := make([]string, 0, 1<<len(letters))
	for mask := range 1 << len(letters) {
		b := []byte(s)
		for bit, i := range letters {
			if mask&(1<<bit) != 0 {
				b[i] -= 'a' - 'A'
			}
		}
		out = append(out, string(b))
	}
	return out
}

// loopbackName is the name of this host's loopback interface, or "" when it
// has none.
func loopbackName() string {
	ifs, err := net.Interfaces()
	if err != nil {
		return ""
	}
	for _, i := range ifs {
		if i.Flags&net.FlagLoopback != 0 {
			return i.Name
		}
	}
	return ""
}

// loopbackCertificate is a self-signed certificate for the IP ::1, and a pool
// that trusts it.
func loopbackCertificate(t *testing.T) (tls.Certificate, *x509.CertPool) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generating a key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "iqmq stomp test"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IPAddresses:           []net.IP{net.IPv6loopback},
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("creating the certificate: %v", err)
	}
	parsed, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("parsing the certificate: %v", err)
	}
	roots := x509.NewCertPool()
	roots.AddCert(parsed)
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}, roots
}
