package redis_test

import (
	"errors"
	"slices"
	"testing"

	mq "github.com/iqhive/iqmq"
	rd "github.com/iqhive/iqmq/drivers/redis"
)

// TestTheDialGetsTheZoneTheDSNNames holds ParseDSN to reading an IPv6 zone the
// way RFC 6874 writes one, "[fe80::1%25eth0]".
//
// The shared parser keeps a bracketed literal as written, and Addrs reach
// rueidis's dialler as they are (ClientOption.InitAddress), so that DSN
// dialled the zone "25eth0": net.Dial took the address for a host name and
// asked DNS for it. A zone written with a bare "%" is not RFC 6874, but it
// parsed and dialled the zone it names, and it still does; decoding it would
// turn the interface index in "%12" into the byte 0x12.
func TestTheDialGetsTheZoneTheDSNNames(t *testing.T) {
	t.Parallel()
	for in, want := range map[string]string{
		"redis://[fe80::1%25lo]:6379":      "[fe80::1%lo]:6379",
		"redis://[fe80::1%25eth%2D0]:6379": "[fe80::1%eth-0]:6379",
		"redis://[fe80::1%25lo]":           "[fe80::1%lo]:6379",
		"redis://[fe80::1%2525x]:6379":     "[fe80::1%25x]:6379",
		"redis://[fe80::1%eth0]:6379":      "[fe80::1%eth0]:6379",
		"redis://[fe80::1%12]:6379":        "[fe80::1%12]:6379",
		"redis://[::1]:6379":               "[::1]:6379",
		// A host with no colon is not an IPv6 literal, and the "%25" in it is a
		// "%" the shared parser has already decoded once.
		"redis://a%2525b:6379": "a%25b:6379",
	} {
		cfg, err := rd.ParseDSN(in)
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
	for _, addr := range []string{"[fe80::1%lo]:6379", "[fe80::1%25x]:6379", "[fe80::1%a%25b]:6379", "a%25b:6379"} {
		rendered := rd.Config{Addrs: []string{addr}}.DSN()
		back, err := rd.ParseDSN(rendered)
		if err != nil || !slices.Equal(back.Addrs, []string{addr}) {
			t.Errorf("Addrs %q renders as %q, which parses back to %q, %v", addr, rendered, back.Addrs, err)
		}
	}
	for _, in := range []string{
		"redis://[fe80::1%25]:6379",
		"redis://[fe80::1%25e%zz]:6379",
		"redis://[fe80::1%25eth%]:6379",
	} {
		_, err := rd.ParseDSN(in)
		if err == nil {
			t.Errorf("ParseDSN(%q) was accepted: an empty zone, or one with a malformed escape, names no interface", in)
			continue
		}
		if mq.CodeOf(err) != mq.CodeInvalidConfig || errors.Is(err, mq.ErrDriverNotRegistered) {
			t.Errorf("ParseDSN(%q) = %v (code %v), want an InvalidConfig refusal", in, err, mq.CodeOf(err))
		}
	}
}
