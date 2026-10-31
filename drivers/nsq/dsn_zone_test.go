package nsq_test

import (
	"errors"
	"slices"
	"testing"

	mq "github.com/iqhive/iqmq"
	nq "github.com/iqhive/iqmq/drivers/nsq"
)

// TestTheDialGetsTheZoneTheDSNNames holds ParseDSN to reading an IPv6 zone the
// way RFC 6874 writes one, "[fe80::1%25eth0]".
//
// The shared parser keeps a bracketed literal as written, and Addrs reach
// go-nsq's net.Dialer as they are (nsq.NewProducer, ConnectToNSQDs), so that
// DSN dialled the zone "25eth0": net.Dial took the address for a host name and
// asked DNS for it. A zone written with a bare "%" is not RFC 6874, but it
// parsed and dialled the zone it names, and it still does; decoding it would
// turn the interface index in "%12" into the byte 0x12.
func TestTheDialGetsTheZoneTheDSNNames(t *testing.T) {
	t.Parallel()
	for in, want := range map[string]string{
		"nsq://[fe80::1%25lo]:4150":      "[fe80::1%lo]:4150",
		"nsq://[fe80::1%25eth%2D0]:4150": "[fe80::1%eth-0]:4150",
		"nsq://[fe80::1%25lo]":           "[fe80::1%lo]:4150",
		"nsq://[fe80::1%2525x]:4150":     "[fe80::1%25x]:4150",
		"nsq://[fe80::1%eth0]:4150":      "[fe80::1%eth0]:4150",
		"nsq://[fe80::1%12]:4150":        "[fe80::1%12]:4150",
		"nsq://[::1]:4150":               "[::1]:4150",
	} {
		cfg, err := nq.ParseDSN(in)
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
	for _, addr := range []string{"[fe80::1%lo]:4150", "[fe80::1%25x]:4150", "[fe80::1%a%25b]:4150"} {
		rendered := nq.Config{Addrs: []string{addr}}.DSN()
		back, err := nq.ParseDSN(rendered)
		if err != nil || !slices.Equal(back.Addrs, []string{addr}) {
			t.Errorf("Addrs %q renders as %q, which parses back to %q, %v", addr, rendered, back.Addrs, err)
		}
	}
	for _, in := range []string{
		"nsq://[fe80::1%25]:4150",
		"nsq://[fe80::1%25e%zz]:4150",
		"nsq://[fe80::1%25eth%]:4150",
	} {
		_, err := nq.ParseDSN(in)
		if err == nil {
			t.Errorf("ParseDSN(%q) was accepted: an empty zone, or one with a malformed escape, names no interface", in)
			continue
		}
		if mq.CodeOf(err) != mq.CodeInvalidConfig || errors.Is(err, mq.ErrDriverNotRegistered) {
			t.Errorf("ParseDSN(%q) = %v (code %v), want an InvalidConfig refusal", in, err, mq.CodeOf(err))
		}
	}
}
