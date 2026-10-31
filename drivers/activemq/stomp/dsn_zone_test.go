package stomp_test

import (
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"

	mq "github.com/iqhive/iqmq"
	stomp "github.com/iqhive/iqmq/drivers/activemq/stomp"
	"github.com/iqhive/iqmq/internal/activemq"
)

// TestTheDialGetsTheZoneTheDSNNames holds ParseDSN to reading an IPv6 zone the
// way RFC 6874 writes one, "[fe80::1%25eth0]".
//
// The shared parser keeps a bracketed literal as written, and Addrs reach this
// driver's net.Dialer as they are (Backend.dialNet), so that DSN dialled the
// zone "25eth0": net.Dial took the address for a host name and asked DNS for
// it. A zone written with a bare "%" is not RFC 6874, but it parsed and
// dialled the zone it names, and it still does; decoding it would turn the
// interface index in "%12" into the byte 0x12.
func TestTheDialGetsTheZoneTheDSNNames(t *testing.T) {
	t.Parallel()
	for in, want := range map[string]string{
		"activemq+stomp://[fe80::1%25lo]:61613?broker=artemis":      "[fe80::1%lo]:61613",
		"activemq+stomp://[fe80::1%25eth%2D0]:61613?broker=artemis": "[fe80::1%eth-0]:61613",
		"activemq+stomp://[fe80::1%25lo]?broker=artemis":            "[fe80::1%lo]",
		"activemq+stomp://[fe80::1%2525x]:61613?broker=artemis":     "[fe80::1%25x]:61613",
		"activemq+stomp://[fe80::1%eth0]:61613?broker=artemis":      "[fe80::1%eth0]:61613",
		"activemq+stomp://[fe80::1%12]:61613?broker=artemis":        "[fe80::1%12]:61613",
		"activemq+stomp://[::1]:61613?broker=artemis":               "[::1]:61613",
		// A host with no colon is not an IPv6 literal, and the "%25" in it is a
		// "%" the shared parser has already decoded once.
		"activemq+stomp://a%2525b:61613?broker=artemis": "a%25b:61613",
	} {
		cfg, err := stomp.ParseDSN(in)
		if err != nil {
			t.Errorf("ParseDSN(%q): %v", in, err)
			continue
		}
		if !slices.Equal(cfg.Addrs, []string{want}) {
			t.Errorf("ParseDSN(%q) dials %q, want %q", in, cfg.Addrs, want)
		}
	}
	// The rendering writes a zone back the way ParseDSN reads it, so an
	// address whose zone itself holds "25" reads back as itself.
	for addr, want := range map[string]string{
		"[fe80::1%lo]:61613":  "[fe80::1%lo]:61613",
		"[fe80::1%25x]:61613": "[fe80::1%25x]:61613",
		"[fe80::1%a%25b]":     "[fe80::1%a%25b]",
		// A plaintext scheme is written bare, as it always was.
		"stomp://[fe80::1%25x]:61613": "[fe80::1%25x]:61613",
	} {
		rendered := stomp.Config{Broker: activemq.Artemis, Addrs: []string{addr}}.DSN()
		back, err := stomp.ParseDSN(rendered)
		if err != nil || !slices.Equal(back.Addrs, []string{want}) {
			t.Errorf("Addrs %q renders as %q, which parses back to %q, %v", addr, rendered, back.Addrs, err)
		}
	}
	// This driver's renderer writes an address as it is, and the shared parser
	// splits a DSN on these bytes before it sees the brackets. Kept as written,
	// "[fe80::1%25a%2Cb]" rendered as itself and parsed back; decoded, its zone
	// "a,b" has to be escaped again, or the rendering does not parse. A zone
	// holding "://" is here too: the renderer took it for a scheme and cut the
	// address there (TestADSNHostCannotRedirectTheDial).
	for _, z := range []string{",", "@", "/", "?", "#", "]", "://["} {
		var esc strings.Builder
		for i := range len(z) {
			fmt.Fprintf(&esc, "%%%02X", z[i])
		}
		in := "activemq+stomp://[fe80::1%25a" + esc.String() + "b]:61613?broker=artemis"
		want := "[fe80::1%a" + z + "b]:61613"
		cfg, err := stomp.ParseDSN(in)
		if err != nil || !slices.Equal(cfg.Addrs, []string{want}) {
			t.Errorf("ParseDSN(%q) dials %q, %v; want %q", in, cfg.Addrs, err, want)
			continue
		}
		rendered := cfg.DSN()
		back, err := stomp.ParseDSN(rendered)
		if err != nil || !slices.Equal(back.Addrs, cfg.Addrs) {
			t.Errorf("ParseDSN(%q) renders as %q, which parses back to %q, %v", in, rendered, back.Addrs, err)
		}
	}
	for _, in := range []string{
		"activemq+stomp://[fe80::1%25]:61613?broker=artemis",
		"activemq+stomp://[fe80::1%25e%zz]:61613?broker=artemis",
		"activemq+stomp://[fe80::1%25eth%]:61613?broker=artemis",
	} {
		_, err := stomp.ParseDSN(in)
		if err == nil {
			t.Errorf("ParseDSN(%q) was accepted: an empty zone, or one with a malformed escape, names no interface", in)
			continue
		}
		if mq.CodeOf(err) != mq.CodeInvalidConfig || errors.Is(err, mq.ErrDriverNotRegistered) {
			t.Errorf("ParseDSN(%q) = %v (code %v), want an InvalidConfig refusal", in, err, mq.CodeOf(err))
		}
	}
}
