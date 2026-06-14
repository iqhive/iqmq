package rabbitmq_test

import (
	"crypto/tls"
	"net"
	"strings"
	"testing"
	"time"

	mq "bitbucket.org/iqhive/iqmq"
	rq "bitbucket.org/iqhive/iqmq/drivers/rabbitmq"
)

// firstBytesOnTheWire dials addr through the real client library and returns
// the first bytes the peer receives, so a test can tell TLS from cleartext by
// what is actually sent rather than by what a Config says.
//
// A TLS ClientHello begins 0x16 0x03; the AMQP 0-9-1 protocol header is the
// ASCII "AMQP". Nothing here needs a broker: the listener reads the opening
// bytes and closes.
func firstBytesOnTheWire(t *testing.T, scheme string, cfg rq.Config) []byte {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer func() { _ = ln.Close() }()
	got := make(chan []byte, 1)
	go func() {
		c, err := ln.Accept()
		if err != nil {
			got <- nil
			return
		}
		defer func() { _ = c.Close() }()
		_ = c.SetReadDeadline(time.Now().Add(5 * time.Second))
		buf := make([]byte, 8)
		n, _ := c.Read(buf)
		got <- buf[:n]
	}()
	cfg.Addrs = []string{scheme + ln.Addr().String()}
	cfg.DialTimeout = 3 * time.Second
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate(%v): %v", cfg.Addrs, err)
	}
	go func() {
		c, err := rq.Open(t.Context(), cfg)
		if err == nil {
			_ = c.Close()
		}
	}()
	select {
	case b := <-got:
		return b
	case <-time.After(10 * time.Second):
		t.Fatalf("nothing reached the listener for %v", cfg.Addrs)
		return nil
	}
}

// TestTheTransportOnTheWireMatchesTheScheme reads the BYTES this driver puts
// on the connection, because the thing being asserted is a property and
// no amount of reasoning about Config fields establishes it.
//
// amqp091-go picks the transport from the address SCHEME alone, so a
// *tls.Config beside an "amqp://" address does NOT make the connection TLS --
// and this driver's dial URL carries the credential, so the password would go
// out in the clear. That pairing is refused; this test proves the two
// spellings that remain do what they claim.
func TestTheTransportOnTheWireMatchesTheScheme(t *testing.T) {
	plain := firstBytesOnTheWire(t, "amqp://", rq.Config{Username: "u", Password: "s3cret"})
	if !strings.HasPrefix(string(plain), "AMQP") {
		t.Errorf("an amqp:// address opened with % x, want the cleartext AMQP header", plain)
	}
	withTLS := firstBytesOnTheWire(t, "amqps://", rq.Config{
		Username: "u", Password: "s3cret",
		TLS: &tls.Config{InsecureSkipVerify: true},
	})
	if len(withTLS) < 2 || withTLS[0] != 0x16 || withTLS[1] != 0x03 {
		t.Errorf("an amqps:// address opened with % x, want a TLS ClientHello (16 03 ...) -- the credential "+
			"travels on this connection", withTLS)
	}
}

// TestATLSConfigCannotSitBesideAPlaintextScheme is the refusal the measurement
// above justifies.
func TestATLSConfigCannotSitBesideAPlaintextScheme(t *testing.T) {
	t.Parallel()
	lying := rq.Config{
		Addrs: []string{"amqp://h:5672"}, Username: "u", Password: "s3cret",
		TLS: &tls.Config{MinVersion: tls.VersionTLS12},
	}
	err := lying.Validate()
	if err == nil {
		t.Fatal("a TLS config beside an explicit amqp:// address validated; amqp091-go dials that in the clear " +
			"and this driver puts the password in the dial URL, so it would go out unprotected")
	}
	if mq.CodeOf(err) != mq.CodeInvalidConfig {
		t.Errorf("Validate = %v (code %v), want an InvalidConfig refusal", err, mq.CodeOf(err))
	}
	// The two coherent spellings still validate.
	for _, cfg := range []rq.Config{
		{Addrs: []string{"amqps://h:5671"}, TLS: &tls.Config{MinVersion: tls.VersionTLS12}},
		{Addrs: []string{"amqps://h:5671"}},
		{Addrs: []string{"h:5672"}, TLS: &tls.Config{MinVersion: tls.VersionTLS12}},
		{Addrs: []string{"amqp://h:5672"}},
	} {
		c := cfg
		if err := c.Validate(); err != nil {
			t.Errorf("Validate(%v, TLS=%v) = %v", c.Addrs, c.TLS != nil, err)
		}
	}
}
