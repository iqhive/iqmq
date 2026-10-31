package kafka_test

import (
	"errors"
	"slices"
	"testing"

	mq "github.com/iqhive/iqmq"
	kq "github.com/iqhive/iqmq/drivers/kafka"
)

// TestTheDialGetsTheZoneTheDSNNames holds ParseDSN to reading an IPv6 zone the
// way RFC 6874 writes one, "[fe80::1%25eth0]".
//
// The shared parser keeps a bracketed literal as written, and Brokers reach
// franz-go's dialler as they are (kgo.SeedBrokers), so that DSN dialled the
// zone "25eth0": net.Dial took the address for a host name and asked DNS for
// it. A zone written with a bare "%" is not RFC 6874, but it parsed and
// dialled the zone it names, and it still does; decoding it would turn the
// interface index in "%12" into the byte 0x12.
func TestTheDialGetsTheZoneTheDSNNames(t *testing.T) {
	t.Parallel()
	for in, want := range map[string]string{
		"kafka://[fe80::1%25lo]:9092":      "[fe80::1%lo]:9092",
		"kafka://[fe80::1%25eth%2D0]:9092": "[fe80::1%eth-0]:9092",
		"kafka://[fe80::1%25lo]":           "[fe80::1%lo]:9092",
		"kafka://[fe80::1%2525x]:9092":     "[fe80::1%25x]:9092",
		"kafka://[fe80::1%eth0]:9092":      "[fe80::1%eth0]:9092",
		"kafka://[fe80::1%12]:9092":        "[fe80::1%12]:9092",
		"kafka://[::1]:9092":               "[::1]:9092",
		// A host with no colon is not an IPv6 literal, and the "%25" in it is a
		// "%" the shared parser has already decoded once.
		"kafka://a%2525b:9092": "a%25b:9092",
	} {
		cfg, err := kq.ParseDSN(in)
		if err != nil {
			t.Errorf("ParseDSN(%q): %v", in, err)
			continue
		}
		if !slices.Equal(cfg.Brokers, []string{want}) {
			t.Errorf("ParseDSN(%q) dials %q, want %q", in, cfg.Brokers, want)
		}
	}
	// The rendering writes a zone back the way ParseDSN reads it, so a broker
	// whose zone itself holds "25" reads back as itself.
	for _, broker := range []string{"[fe80::1%lo]:9092", "[fe80::1%25x]:9092", "[fe80::1%a%25b]:9092", "a%25b:9092"} {
		rendered := kq.Config{Brokers: []string{broker}}.DSN()
		back, err := kq.ParseDSN(rendered)
		if err != nil || !slices.Equal(back.Brokers, []string{broker}) {
			t.Errorf("Brokers %q renders as %q, which parses back to %q, %v", broker, rendered, back.Brokers, err)
		}
	}
	for _, in := range []string{
		"kafka://[fe80::1%25]:9092",
		"kafka://[fe80::1%25e%zz]:9092",
		"kafka://[fe80::1%25eth%]:9092",
	} {
		_, err := kq.ParseDSN(in)
		if err == nil {
			t.Errorf("ParseDSN(%q) was accepted: an empty zone, or one with a malformed escape, names no interface", in)
			continue
		}
		if mq.CodeOf(err) != mq.CodeInvalidConfig || errors.Is(err, mq.ErrDriverNotRegistered) {
			t.Errorf("ParseDSN(%q) = %v (code %v), want an InvalidConfig refusal", in, err, mq.CodeOf(err))
		}
	}
}
