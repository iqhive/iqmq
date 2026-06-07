package jetstream_test

import (
	"errors"
	"net"
	"sync/atomic"
	"testing"
	"time"

	mq "bitbucket.org/iqhive/iqmq"
	nq "bitbucket.org/iqhive/iqmq/drivers/jetstream"
)

// TestADSNHostCannotRedirectTheDial holds ParseDSN to refusing a host that
// decodes to a byte the server URL it builds would give a meaning to.
//
// The shared parser percent-decodes a host, and ParseDSN writes the result
// into "nats://<host>:<port>", which nats.go reads with net/url. So
// "tok%40real-host" dialled real-host with the token "tok", and "a%2Fb" dialled
// "a". A DSN host is a host name or an IP literal, and none holds these bytes
// -- nor a comma, which nats.go splits a server string on.
func TestADSNHostCannotRedirectTheDial(t *testing.T) {
	for _, in := range []string{
		"natsjs://tok%40real-host:4222",
		"natsjs://u:p@tok%40real-host:4222",
		"natsjs://a%2Fb:4222",
		"natsjs://a%3Fb:4222",
		"natsjs://a%23b:4222",
		"natsjs://a%5Cb:4222",
		"natsjs://a%20b:4222",
		"natsjs://a%09b:4222",
		"natsjs://a%7Fb:4222",
		"natsjs://h:4222,tok%40real-host:4222",
		// A comma: nats.go reads the server string as a comma-separated
		// LIST before net/url sees it, so "nowhere.invalid,127.0.0.1" was two
		// servers and the client dialled the second -- measured, a TCP
		// connection reached the listener. The branch's own crasher
		// "nAtsjs://%2C0" is the same byte.
		"natsjs://a%2Cb:4222",
		"natsjs://nowhere%2C127.0.0.1:4222",
		"natsjs://%2C0",
	} {
		_, err := nq.ParseDSN(in)
		if err == nil {
			t.Errorf("ParseDSN(%q) was accepted: its host decodes to a byte the server URL splits on, so the "+
				"dial would go somewhere the DSN does not name", in)
			continue
		}
		if mq.CodeOf(err) != mq.CodeInvalidConfig || errors.Is(err, mq.ErrDriverNotRegistered) {
			t.Errorf("ParseDSN(%q) = %v (code %v), want an InvalidConfig refusal", in, err, mq.CodeOf(err))
		}
	}
	// What a host legitimately holds still parses: an IPv6 literal, one with
	// a zone, and a name.
	for in, want := range map[string]string{
		"natsjs://[::1]:4222":            "nats://[::1]:4222",
		"natsjs://[fe80::1%25eth0]:4222": "nats://[fe80::1%25eth0]:4222",
		"natsjs://broker-1.example:4222": "nats://broker-1.example:4222",
	} {
		cfg, err := nq.ParseDSN(in)
		if err != nil {
			t.Errorf("ParseDSN(%q): %v", in, err)
			continue
		}
		if len(cfg.Addrs) != 1 || cfg.Addrs[0] != want {
			t.Errorf("ParseDSN(%q) dials %q, want %q", in, cfg.Addrs, want)
		}
	}
}

// TestAnAddressIsDialledAsOneServer holds OpenBackend to handing nats.go its
// addresses as a LIST.
//
// It joined them with "," and passed one string, which nats.go splits on "," --
// so an address that held a comma became two servers, and the second was
// dialled. ParseDSN now refuses a host that decodes to one; this is the typed
// half, where a Config's own Addrs entry is the caller's and is not refused.
// The listener counts the connections that reach it: a single server named
// "nowhere.invalid,127.0.0.1:<port>" names no host that resolves, and nothing
// may arrive.
func TestAnAddressIsDialledAsOneServer(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	var reached atomic.Int32
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			reached.Add(1)
			_ = c.Close()
		}
	}()
	cfg := nq.Config{
		Addrs:          []string{"nats://nowhere.invalid," + ln.Addr().String()},
		ConnectTimeout: 2 * time.Second,
	}
	b, err := nq.OpenBackend(t.Context(), cfg)
	if err == nil {
		_ = b.Close()
	}
	_ = ln.Close()
	<-done
	if n := reached.Load(); n != 0 {
		t.Fatalf("the address %q reached %s %d times: it was split into two servers and the second was dialled",
			cfg.Addrs[0], ln.Addr(), n)
	}
	if err == nil {
		t.Fatalf("OpenBackend dialled %q and succeeded", cfg.Addrs[0])
	}
}
