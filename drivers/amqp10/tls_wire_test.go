package amqp10_test

import (
	"context"
	"crypto/tls"
	"net"
	"strings"
	"testing"
	"time"

	amqp "github.com/Azure/go-amqp"

	mq "bitbucket.org/iqhive/iqmq"
	aq "bitbucket.org/iqhive/iqmq/drivers/amqp10"
)

// firstBytesOnTheWire dials through the real client library and returns the
// first bytes the peer receives, so a test can tell TLS from cleartext by what
// is actually sent rather than by what a Config says.
//
// A TLS ClientHello begins 0x16 0x03; the AMQP 1.0 protocol header is the
// ASCII "AMQP". Nothing here needs a broker: the listener reads the opening
// bytes and closes.
func firstBytesOnTheWire(t *testing.T, addr string) []byte {
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
	url := addr + ln.Addr().String()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	// A *tls.Config is passed on BOTH arms; the point of the measurement is
	// that it does not decide the transport. The dial is waited for so the
	// package's goroutine leak check fails on the product, not on this.
	done := make(chan struct{})
	go func() {
		defer close(done)
		c, err := amqp.Dial(ctx, url, &amqp.ConnOptions{
			TLSConfig: &tls.Config{InsecureSkipVerify: true},
		})
		if err == nil {
			_ = c.Close()
		}
	}()
	var out []byte
	select {
	case out = <-got:
	case <-time.After(10 * time.Second):
		t.Error("nothing reached the listener for " + url)
	}
	cancel()
	<-done
	return out
}

// TestTheTransportOnTheWireMatchesTheScheme reads the BYTES go-amqp puts on the
// connection, with a *tls.Config set in BOTH cases.
//
// go-amqp's dialConn switches on the URL scheme -- "amqps" and "amqp+ssl" wrap
// the connection in tls.Client, "amqp" does not -- so ConnOptions.TLSConfig
// does not make an "amqp://" connection TLS, and SASL PLAIN credentials travel
// on it in the clear. That pairing is refused by Validate; this proves the
// premise rather than assuming it from the library's source.
func TestTheTransportOnTheWireMatchesTheScheme(t *testing.T) {
	plain := firstBytesOnTheWire(t, "amqp://")
	if !strings.HasPrefix(string(plain), "AMQP") {
		t.Errorf("an amqp:// address opened with % x, want the cleartext AMQP header -- if this is now a "+
			"ClientHello the refusal this pins would be unnecessary", plain)
	}
	withTLS := firstBytesOnTheWire(t, "amqps://")
	if len(withTLS) < 2 || withTLS[0] != 0x16 || withTLS[1] != 0x03 {
		t.Errorf("an amqps:// address opened with % x, want a TLS ClientHello (16 03 ...) -- the SASL PLAIN "+
			"credential travels on this connection", withTLS)
	}
}

// TestATLSConfigCannotSitBesideAPlaintextScheme is the refusal the measurement
// above justifies.
func TestATLSConfigCannotSitBesideAPlaintextScheme(t *testing.T) {
	t.Parallel()
	// The credential-free row comes FIRST and is the one that matters. With a
	// Username and Password set, the SASL PLAIN guard refuses this pairing too,
	// so a credentialed row alone would still pass if this refusal were
	// deleted -- it was checked, and it does. Without credentials nothing else
	// looks at the scheme, so only this guard stands between an operator who
	// set a TLS config and a connection that is not TLS.
	for _, lying := range []aq.Config{
		{Addrs: []string{"amqp://h:5672"}, TLS: &tls.Config{MinVersion: tls.VersionTLS12}},
		{
			Addrs: []string{"amqp://h:5672"}, Username: "u", Password: "s3cret",
			TLS: &tls.Config{MinVersion: tls.VersionTLS12},
		},
	} {
		c := lying
		err := c.Validate()
		if err == nil {
			t.Fatalf("a TLS config beside an explicit amqp:// address validated (Username=%q); go-amqp dials "+
				"that in the clear, so anything on the connection goes out unprotected", c.Username)
		}
		if mq.CodeOf(err) != mq.CodeInvalidConfig {
			t.Errorf("Validate = %v (code %v), want an InvalidConfig refusal", err, mq.CodeOf(err))
		}
		// Name the diagnosis, not just the code: the SASL guard also returns
		// InvalidConfig here, and a refusal that no longer mentions the scheme
		// has stopped being the refusal this test is for.
		if !strings.Contains(err.Error(), `"amqp://"`) {
			t.Errorf("Validate(Username=%q) = %v, want the refusal that names the amqp:// scheme", c.Username, err)
		}
	}
	// The coherent spellings still validate.
	for _, cfg := range []aq.Config{
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
