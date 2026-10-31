package stomp

import (
	"context"
	"errors"
	"net"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
)

// TestADSNHostCannotRedirectTheDial holds the dial to the host a DSN names.
//
// stripScheme cut an address at the first "://" anywhere in it, and dialNet
// dials what is left, so a host that decodes to one sent the dial to whatever
// followed it. Measured against a listener on 127.0.0.1, both of these
// connected there, and the CONNECT frame that follows carries the login, and
// the passcode when TLS or InsecurePlainLogin lets it:
//
//	activemq+stomp://x%3A%2F%2F%5B127.0.0.1:P?broker=artemis
//	    Addrs ["[x://[127.0.0.1]:P"], dialled "[127.0.0.1]:P"
//	activemq+stomp://[fe80::1%25x%3A%2F%2F%5B127.0.0.1]:P?broker=artemis
//	    Addrs ["[fe80::1%x://[127.0.0.1]:P"], the same, once the zone was decoded
//
// The other rows are the bytes that move a dial in a URL. This driver builds
// no URL, so they name a host nobody can resolve, and the test holds that they
// connect nowhere. An address that IS a scheme URL, as Config.Addrs documents
// it, still dials its host.
func TestADSNHostCannotRedirectTheDial(t *testing.T) {
	t.Parallel()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listening on 127.0.0.1: %v", err)
	}
	var accepted atomic.Int32
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			accepted.Add(1)
			_ = c.Close()
		}
	}()
	t.Cleanup(func() {
		_ = ln.Close()
		<-done
	})
	port := strconv.Itoa(ln.Addr().(*net.TCPAddr).Port)
	b := &Backend{}
	for _, in := range []string{
		"activemq+stomp://x%3A%2F%2F%5B127.0.0.1:" + port + "?broker=artemis",
		"activemq+stomp://[fe80::1%25x%3A%2F%2F%5B127.0.0.1]:" + port + "?broker=artemis",
		"activemq+stomp://x%3A%2F%2F127.0.0.1:" + port + "?broker=artemis",
		"activemq+stomp://stomp%3A%2F%2F127.0.0.1:" + port + "?broker=artemis",
		"activemq+stomp://[fe80::1%25stomp%3A%2F%2F127.0.0.1]:" + port + "?broker=artemis",
		"activemq+stomp://tok%40127.0.0.1:" + port + "?broker=artemis",
		"activemq+stomp://[fe80::1%25x%40127.0.0.1]:" + port + "?broker=artemis",
		"activemq+stomp://x%2F127.0.0.1:" + port + "?broker=artemis",
		"activemq+stomp://x%3F127.0.0.1:" + port + "?broker=artemis",
		"activemq+stomp://x%23127.0.0.1:" + port + "?broker=artemis",
		"activemq+stomp://x%5C127.0.0.1:" + port + "?broker=artemis",
	} {
		cfg, err := ParseDSN(in)
		if err != nil {
			if mq.CodeOf(err) != mq.CodeInvalidConfig || errors.Is(err, mq.ErrDriverNotRegistered) {
				t.Errorf("ParseDSN(%q) = %v (code %v), want an InvalidConfig refusal", in, err, mq.CodeOf(err))
			}
			continue
		}
		for _, addr := range cfg.Addrs {
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			conn, err := b.dialNet(ctx, addr)
			cancel()
			if err == nil {
				_ = conn.Close()
				t.Errorf("ParseDSN(%q) gives the address %q, and the dial connected to 127.0.0.1, a host the DSN "+
					"does not name; the CONNECT frame would carry the login there", in, addr)
			}
		}
	}
	// What the dial is documented to take still reaches its host, and so does
	// "://host", which did before.
	for _, addr := range []string{
		"127.0.0.1:" + port, "stomp://127.0.0.1:" + port, "tcp://127.0.0.1:" + port, "://127.0.0.1:" + port,
	} {
		before := accepted.Load()
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		conn, err := b.dialNet(ctx, addr)
		cancel()
		if err != nil {
			t.Errorf("dialling %q: %v", addr, err)
			continue
		}
		_ = conn.Close()
		deadline := time.Now().Add(5 * time.Second)
		for accepted.Load() == before && time.Now().Before(deadline) {
			time.Sleep(10 * time.Millisecond)
		}
		if accepted.Load() == before {
			t.Errorf("dialling %q connected, but not to the listener on 127.0.0.1", addr)
		}
	}
}
