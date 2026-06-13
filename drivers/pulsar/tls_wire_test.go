package pulsar_test

import (
	"crypto/tls"
	"net"
	"testing"
	"time"

	apulsar "github.com/apache/pulsar-client-go/pulsar"

	mq "bitbucket.org/iqhive/iqmq"
	pq "bitbucket.org/iqhive/iqmq/drivers/pulsar"
)

// firstBytesOnTheWire opens a connection through the real client library and
// returns the first bytes the peer receives, so a test can tell TLS from
// cleartext by what is actually sent rather than by what a Config says.
//
// A TLS ClientHello begins 0x16 0x03; a plaintext Pulsar connection opens with
// a length-prefixed binary CONNECT frame. Nothing here needs a broker.
func firstBytesOnTheWire(t *testing.T, scheme string) []byte {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	got := make(chan []byte, 1)
	accepted := make(chan struct{})
	// The first connection is the measurement. The client retries after it,
	// and a retry that is neither answered nor refused leaves producer
	// creation blocked for thirty seconds -- so every later connection is
	// accepted and dropped at once, which makes each retry fail immediately.
	go func() {
		defer close(accepted)
		first := true
		for {
			c, err := ln.Accept()
			if err != nil {
				if first {
					got <- nil
				}
				return
			}
			if !first {
				_ = c.Close()
				continue
			}
			first = false
			_ = c.SetReadDeadline(time.Now().Add(5 * time.Second))
			buf := make([]byte, 8)
			n, _ := c.Read(buf)
			_ = c.Close()
			got <- buf[:n]
		}
	}()
	url := scheme + ln.Addr().String()
	// The client is closed and waited for before this function returns: the
	// package's TestMain checks for leaked goroutines, and pulsar-client-go
	// starts a connection pool that only winds down on Close. An earlier
	// version dialled from a goroutine the test did not wait for, and the
	// leak check failed on the instrument rather than on the product.
	//
	// The connection itself is what is being measured, and it happens on the
	// first CreateProducer, which then spends the whole operation timeout
	// retrying against a listener that answers nothing -- so this returns as
	// soon as the bytes arrive and closes the client underneath it.
	cl, err := apulsar.NewClient(apulsar.ClientOptions{
		URL:                        url,
		TLSConfig:                  &tls.Config{InsecureSkipVerify: true},
		TLSAllowInsecureConnection: true,
		ConnectionTimeout:          200 * time.Millisecond,
		OperationTimeout:           200 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewClient(%q): %v", url, err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		if p, e := cl.CreateProducer(apulsar.ProducerOptions{Topic: "probe"}); e == nil {
			p.Close()
		}
	}()
	var out []byte
	select {
	case out = <-got:
	case <-time.After(15 * time.Second):
		t.Error("nothing reached the listener for " + url)
	}
	cl.Close()
	<-done
	_ = ln.Close()
	<-accepted
	return out
}

// TestTheTransportOnTheWireMatchesTheScheme reads the BYTES pulsar-client-go
// puts on the connection, with a *tls.Config set in BOTH cases.
//
// `PulsarServiceURI.UseTLS` is `ServiceName == https || ServiceInfos contains
// "ssl"` -- the scheme alone -- so `ClientOptions.TLSConfig` does not make a
// "pulsar://" connection TLS, and Token and AuthParams travel on it. That
// pairing is refused by Validate; this proves the premise rather than assuming
// it from the library's source.
func TestTheTransportOnTheWireMatchesTheScheme(t *testing.T) {
	plain := firstBytesOnTheWire(t, "pulsar://")
	if len(plain) >= 2 && plain[0] == 0x16 && plain[1] == 0x03 {
		t.Errorf("a pulsar:// URL opened with a TLS ClientHello (% x); the refusal this pins would be "+
			"unnecessary", plain)
	}
	withTLS := firstBytesOnTheWire(t, "pulsar+ssl://")
	if len(withTLS) < 2 || withTLS[0] != 0x16 || withTLS[1] != 0x03 {
		t.Errorf("a pulsar+ssl:// URL opened with % x, want a TLS ClientHello (16 03 ...) -- the token travels "+
			"on this connection", withTLS)
	}
}

// TestATLSConfigCannotSitBesideAPlaintextScheme is the refusal the measurement
// above justifies.
func TestATLSConfigCannotSitBesideAPlaintextScheme(t *testing.T) {
	t.Parallel()
	lying := pq.Config{
		ServiceURL: "pulsar://h:6650", Token: "s3cret",
		TLS: &tls.Config{MinVersion: tls.VersionTLS12},
	}
	err := lying.Validate()
	if err == nil {
		t.Fatal("a TLS config beside a pulsar:// service URL validated; pulsar-client-go connects in the clear " +
			"on that scheme, so the token would travel unprotected")
	}
	if mq.CodeOf(err) != mq.CodeInvalidConfig {
		t.Errorf("Validate = %v (code %v), want an InvalidConfig refusal", err, mq.CodeOf(err))
	}
	for _, cfg := range []pq.Config{
		{ServiceURL: "pulsar+ssl://h:6651", TLS: &tls.Config{MinVersion: tls.VersionTLS12}},
		{ServiceURL: "pulsar+ssl://h:6651"},
		{ServiceURL: "pulsar://h:6650"},
	} {
		c := cfg
		if err := c.Validate(); err != nil {
			t.Errorf("Validate(%q, TLS=%v) = %v", c.ServiceURL, c.TLS != nil, err)
		}
	}
}
