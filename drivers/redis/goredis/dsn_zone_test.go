package goredis_test

import (
	"errors"
	"slices"
	"testing"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/drivers/redis/goredis"
)

// zoneRelease is the first release of drivers/redis whose ParseDSN decodes an
// IPv6 zone written as RFC 6874 has it.
const zoneRelease = "v1.2.0"

// TestTheDialGetsTheZoneTheDSNNames holds this module's DSN path, which is
// drivers/redis's ParseDSN, to handing go-redis the zone a DSN names when it
// writes one as RFC 6874 does, "[fe80::1%25eth0]".
//
// The shared parser keeps a bracketed literal as written, and Addrs reach
// go-redis's dialler as they are (UniversalOptions.Addrs), so that DSN dialled
// the zone "25eth0": net.Dial took the address for a host name and asked DNS
// for it. A zone written with a bare "%" is not RFC 6874, but it parsed and
// dialled the zone it names, and it still does.
func TestTheDialGetsTheZoneTheDSNNames(t *testing.T) {
	t.Parallel()
	if have, before := driversRedisBefore(t, zoneRelease); before {
		t.Skipf("drivers/redis %s keeps an RFC 6874 zone as written; %s decodes it", have, zoneRelease)
	}
	for in, want := range map[string]string{
		"redis://[fe80::1%25lo]:6379":      "[fe80::1%lo]:6379",
		"redis://[fe80::1%25eth%2D0]:6379": "[fe80::1%eth-0]:6379",
		"redis://[fe80::1%25lo]":           "[fe80::1%lo]:6379",
		// The one row a second decode changes: it would read "%x". This module
		// has no decode of its own, and this row is what says so.
		"redis://[fe80::1%2525x]:6379": "[fe80::1%25x]:6379",
		"redis://[fe80::1%eth0]:6379":  "[fe80::1%eth0]:6379",
		"redis://[fe80::1%12]:6379":    "[fe80::1%12]:6379",
	} {
		cfg, err := goredis.ParseDSN(in)
		if err != nil {
			t.Errorf("ParseDSN(%q): %v", in, err)
			continue
		}
		if got := goredis.UniversalOptions(cfg).Addrs; !slices.Equal(got, []string{want}) {
			t.Errorf("ParseDSN(%q) hands go-redis %q, want %q", in, got, want)
		}
	}
	for _, in := range []string{"redis://[fe80::1%25]:6379", "redis://[fe80::1%25e%zz]:6379"} {
		_, err := goredis.ParseDSN(in)
		if err == nil {
			t.Errorf("ParseDSN(%q) was accepted: an empty zone, or one with a malformed escape, names no interface", in)
			continue
		}
		if mq.CodeOf(err) != mq.CodeInvalidConfig || errors.Is(err, mq.ErrDriverNotRegistered) {
			t.Errorf("ParseDSN(%q) = %v (code %v), want an InvalidConfig refusal", in, err, mq.CodeOf(err))
		}
	}
}
