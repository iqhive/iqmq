package nsq_test

import (
	"bytes"
	"context"
	"crypto/tls"
	"net"
	"strings"
	"testing"
	"time"

	mq "bitbucket.org/iqhive/iqmq"
	nq "bitbucket.org/iqhive/iqmq/drivers/nsq"
)

// The two tests below MEASURE what nsqlookupd discovery puts on the wire when
// `Config.TLS` is set, because the finding they answer was recorded as a claim
// and a claim about a transport is the one thing this repository will not take
// on trust: every driver in `fix/dsn-dial`'s TLS class DID populate the
// library's TLS field, and the library ignored it. Asserting that the driver
// configures something is what let that live.
//
// What they measure is the FIRST BYTES the discovery poller writes to a
// loopback listener. A cleartext HTTP request begins `GET `; a TLS ClientHello
// begins 0x16 0x03 (handshake, TLS 1.x record), and an ALPN name the caller put
// in `Config.TLS.NextProtos` appears in it literally -- which is how "the
// driver's own TLS configuration reached this socket" becomes a byte-level
// question with no certificate, no CA and no handshake completion in it.

// lookupdALPN is a protocol name no real client negotiates, used only so that
// its presence in a ClientHello says which *tls.Config built the hello.
const lookupdALPN = "iqmq-lookupd-probe"

// lookupdSecret is an AuthSecret whose bytes are distinctive enough to find in a
// wire capture, and short enough to fit inside a TLS record's plaintext had one
// been sent in the clear.
const lookupdSecret = "iqmq-lookupd-probe-secret"

// lookupdWire points a consumer's nsqlookupd discovery at a loopback listener
// and returns the first bytes the driver wrote to it.
//
// `addr` is whatever goes in `LookupdAddrs`, with `%s` where the listener's
// host and port belong, so a caller can measure the scheme-less form, the
// `http://` form and the `https://` form through one helper. The TLS config is
// NOT a parameter: every arm sets one, because "with a TLS config set" is the
// premise the finding is about.
func lookupdWire(t *testing.T, addr string) []byte {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listening for the discovery poller: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })

	// An ACCEPT LOOP, not one Accept, and the reason is a two-minute test
	// rather than a wrong one. go-nsq's `queryLookupd` retries twice on a
	// failed poll, and a listener that had stopped accepting left each retry's
	// connection in the kernel backlog with nobody reading it -- so the retries
	// waited out the transport's TLS handshake timeout (10s each, 20s for the
	// https arm) and the http.Client's own timeout, which go-nsq defaults to a
	// MINUTE (2m for the cleartext arm). Accepting and closing at once turns
	// each retry into an immediate EOF.
	first := make(chan []byte, 1)
	go func() {
		got := false
		for {
			c, aerr := ln.Accept()
			if aerr != nil {
				if !got {
					first <- nil
				}
				return
			}
			if !got {
				_ = c.SetReadDeadline(time.Now().Add(10 * time.Second))
				buf := make([]byte, 2048)
				n, _ := c.Read(buf)
				first <- buf[:n]
				got = true
			}
			_ = c.Close()
		}
	}()

	cfg := nq.Config{
		// A publishing address is required for a Config that can do both, and
		// nothing here publishes: the socket under test is the discovery one.
		Addrs:        []string{"127.0.0.1:4150"},
		LookupdAddrs: []string{strings.ReplaceAll(addr, "%s", ln.Addr().String())},
		// An AuthSecret on EVERY arm, because the question every arm answers is
		// what reaches the wire when one is configured. go-nsq's
		// `LookupdAuthorization` defaults to true, so this is the setting that
		// put a bearer token on a cleartext discovery poll.
		AuthSecret: lookupdSecret,
		// EVERY arm sets a TLS config, because that is the premise under test:
		// the recorded finding is about what discovery does WITH one set. The
		// ALPN name is what says which *tls.Config built a handshake, if one
		// happens at all.
		TLS: &tls.Config{
			MinVersion: tls.VersionTLS12,
			NextProtos: []string{lookupdALPN},
		},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	b, err := nq.OpenBackend(ctx, cfg)
	if err != nil {
		t.Fatalf("OpenBackend with lookupd %q: %v", cfg.LookupdAddrs[0], err)
	}
	t.Cleanup(func() { _ = b.Close() })
	// OpenSource connects eagerly (Backend.OpenSource calls source.connect),
	// and go-nsq queries the first nsqlookupd inline from
	// ConnectToNSQLookupds, so the request is on the wire by the time this
	// returns.
	src, err := b.OpenSource(ctx, mq.SourceConfig{Topic: "orders", Subscription: "workers", Prefetch: 1})
	if err != nil {
		t.Fatalf("OpenSource with lookupd %q: %v", cfg.LookupdAddrs[0], err)
	}
	t.Cleanup(func() { _ = src.Close() })

	select {
	case got := <-first:
		if len(got) == 0 {
			t.Fatal("the discovery poller connected and wrote nothing")
		}
		return got
	case <-time.After(20 * time.Second):
		t.Fatal("the discovery poller never reached the listener")
	}
	return nil
}

// TestLookupdDiscoveryIsCleartextOnAnHTTPAddressAndIsNotRefused is the
// documented behaviour, measured.
//
// `Config.TLS` is nsqd's TLS. It cannot make an `http://` URL into an `https://`
// one, and go-nsq prepends `http://` to an entry with no scheme at all -- so a
// LookupdAddrs entry that is not spelled `https://` is cleartext discovery
// whatever else the Config says. That is NOT refused, and the reason is in
// Config.LookupdAddrs' own doc: nsqlookupd is a different service from nsqd, and
// a TLS nsqd behind a plain-HTTP nsqlookupd on a private network is a real
// layout. What it discloses is the topic name and the returned nsqd host names.
//
// It discloses no CREDENTIAL only because `clientConfig` turns go-nsq's
// `LookupdAuthorization` off. That field defaults to TRUE, and with an
// AuthSecret configured this request carried `Authorization: Bearer <secret>` in
// the clear -- which is what the assertions below measure, and which
// `fix/dsn-dial` had recorded as impossible.
//
// Refusing it was considered and rejected on two counts: it would break a
// working deployment for a disclosure its operator may have accepted, and it
// could not be made complete without also refusing the scheme-less spelling
// this repository's own doc.go and examples use.
//
// This test exists so that the doc cannot drift from the wire. If a later
// change makes discovery TLS by default, this test fails and the doc paragraph
// it guards has to be rewritten with it.
func TestLookupdDiscoveryIsCleartextOnAnHTTPAddressAndIsNotRefused(t *testing.T) {
	t.Parallel()
	for _, spelling := range []string{"%s", "http://%s"} {
		t.Run(spelling, func(t *testing.T) {
			t.Parallel()
			got := lookupdWire(t, spelling)
			if bytes.HasPrefix(got, []byte{0x16, 0x03}) {
				t.Fatalf("discovery to %q began a TLS handshake. The documented behaviour is cleartext, "+
					"so either this is a fix and Config.LookupdAddrs' doc has to be rewritten, or "+
					"something is negotiating TLS that the operator did not ask for", spelling)
			}
			if !bytes.HasPrefix(got, []byte("GET ")) {
				t.Fatalf("discovery to %q wrote %q, which is neither a cleartext HTTP request nor a TLS "+
					"record; the measurement itself is wrong", spelling, firstLine(got))
			}
			// THE CREDENTIAL, which is the assertion this file exists for.
			// `fix/dsn-dial` recorded that a lookupd address carries none, and
			// the measurement said otherwise: go-nsq's `LookupdAuthorization`
			// defaults to TRUE, so `queryLookupd` put
			// `Authorization: Bearer <AuthSecret>` on this cleartext request.
			// `clientConfig` turns that field off, and this is what says so on
			// the wire rather than in a config assertion.
			if bytes.Contains(got, []byte(lookupdSecret)) {
				t.Fatalf("discovery to %q carried the AuthSecret in the clear: %q. That is a credential on an "+
					"unprotected wire, which is the class B20c.35 is about and NOT the topic-and-host-name "+
					"disclosure this test documents", spelling, firstLine(got))
			}
			if bytes.Contains(got, []byte("Authorization")) {
				t.Fatalf("discovery to %q carried an Authorization header: %q. Even with no secret in it, this "+
					"request is not the driver's to authorize -- nsqlookupd is a discovery service and "+
					"AuthSecret is nsqd's", spelling, firstLine(got))
			}
			if !bytes.Contains(got, []byte("topic=orders")) {
				t.Fatalf("discovery to %q wrote %q, which does not name the topic; the disclosure this "+
					"documents is the topic and host names, and the test has to see them to claim it",
					spelling, firstLine(got))
			}
		})
	}
}

// TestAnHTTPSLookupdAddressUsesTheDriversOwnTLSConfig is the half that WAS a
// defect.
//
// go-nsq builds its own `*http.Client` for nsqlookupd polling
// (`consumer.go`'s `ConnectToNSQLookupd`) with a transport whose
// `TLSClientConfig` it never sets, and it never looks at `Config.TlsConfig` for
// it -- that field reaches the nsqd TCP connection only. So `https://` worked
// with the system roots and nothing else: an nsqlookupd whose certificate came
// from a private CA in `Config.TLS.RootCAs` failed as signed by an unknown
// authority, `Config.TLS.InsecureSkipVerify` did nothing, and a client
// certificate was never presented. It is exactly the defect on
// a third HTTP surface, and go-nsq exposes the seam that fixes it
// (`Consumer.SetLookupdHttpClient`), so the driver now hands the poller the
// same client shape the admin surface uses.
//
// The ALPN name is the instrument. It proves WHICH *tls.Config built the
// ClientHello, which a handshake that merely succeeded would not: a system-root
// client and a driver-configured one both send a hello, and only one of them
// sends this one.
func TestAnHTTPSLookupdAddressUsesTheDriversOwnTLSConfig(t *testing.T) {
	t.Parallel()
	got := lookupdWire(t, "https://%s")
	if !bytes.HasPrefix(got, []byte{0x16, 0x03}) {
		t.Fatalf("discovery to an https:// lookupd wrote %q rather than a TLS record", firstLine(got))
	}
	if !bytes.Contains(got, []byte(lookupdALPN)) {
		t.Fatalf("the ClientHello discovery sent does not carry the ALPN name %q that Config.TLS asked "+
			"for, so the handshake was built from some other *tls.Config -- which is go-nsq's own "+
			"transport with TLSClientConfig unset. A private CA in Config.TLS.RootCAs then fails as "+
			"an unknown authority and InsecureSkipVerify does nothing", lookupdALPN)
	}
}

// firstLine is the first line of a wire capture, for a failure message that has
// to be readable whether the bytes are text or a TLS record.
func firstLine(b []byte) string {
	if i := bytes.IndexAny(b, "\r\n"); i >= 0 {
		b = b[:i]
	}
	if len(b) > 120 {
		b = b[:120]
	}
	return string(b)
}
