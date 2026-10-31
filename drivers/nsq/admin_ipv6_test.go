package nsq_test

import (
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync/atomic"
	"testing"

	mq "github.com/iqhive/iqmq"
	nq "github.com/iqhive/iqmq/drivers/nsq"
)

// TestTheHTTPAddressIsDerivedFromAnIPv6TCPAddress is the derivation of
// TestTheHTTPAddressIsDerivedFromTheTCPOneWhenItIsNotGiven for an IPv6 nsqd,
// with and without a zone.
//
// The derivation cut the address at its FIRST colon, which in "[::1]:4150" is
// inside the brackets, so the port it read was not a number and an IPv6
// address derived no HTTP address at all: Admin() reported the surface
// unsupported for a deployment that had named its nsqd. The derived address
// also goes into a URL, which writes a zone's "%" as "%25", so a zone has to
// survive that too.
func TestTheHTTPAddressIsDerivedFromAnIPv6TCPAddress(t *testing.T) {
	t.Parallel()
	ln, err := net.Listen("tcp", "[::1]:0")
	if err != nil {
		t.Skipf("this host cannot listen on the IPv6 loopback: %v", err)
	}
	var created atomic.Int32
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/topic/create" {
			created.Add(1)
		}
	}))
	_ = srv.Listener.Close()
	srv.Listener = ln
	srv.Start()
	t.Cleanup(srv.Close)
	// One BELOW the stub's port, so that the derivation has to add one to
	// land on it.
	below := strconv.Itoa(ln.Addr().(*net.TCPAddr).Port - 1)
	addrs := []string{net.JoinHostPort("::1", below)}
	if lo := loopbackName(); lo != "" {
		addrs = append(addrs, net.JoinHostPort("::1%"+lo, below))
	}
	for _, tcp := range addrs {
		before := created.Load()
		b, _ := newBackendRig(t, func(c *nq.Config) {
			c.Addrs = []string{tcp}
			c.AdminAddrs = nil
		})
		admin, err := b.Admin()
		if err != nil {
			t.Errorf("Admin() over the IPv6 address %s: %v", tcp, err)
			continue
		}
		if err := admin.EnsureTopic(t.Context(), mq.TopicSpec{Name: "orders"}); err != nil {
			t.Errorf("EnsureTopic over the HTTP address derived from %s: %v", tcp, err)
			continue
		}
		if created.Load() == before {
			t.Errorf("the request for %s never reached the nsqd one port above it", tcp)
		}
	}
}

// TestAnHTTPAddressTakesItsZoneAsAURLWritesIt holds Validate to the two
// address lists that become URLs.
//
// AdminAddrs and LookupdAddrs go into "http://" + addr (Backend.do, go-nsq's
// buildLookupAddr), and net/url reads only "%25" before an IPv6 zone: a bare
// "%" passed Validate and then failed at the request, `invalid URL escape
// "%et"`. Addrs are dialled, and net.Dial reads the bare "%", so it keeps it.
func TestAnHTTPAddressTakesItsZoneAsAURLWritesIt(t *testing.T) {
	t.Parallel()
	for _, cfg := range []nq.Config{
		{Addrs: []string{"[fe80::1%eth0]:4150"}},
		{Addrs: []string{"h:4150"}, AdminAddrs: []string{"[fe80::1%25eth0]:4151", "[::1]:4151"}},
		{LookupdAddrs: []string{"http://[fe80::1%25eth0]:4161", "[fe80::1%25eth%2D0]:4161/lookup"}},
	} {
		if err := cfg.Validate(); err != nil {
			t.Errorf("Validate(%q, %q, %q) = %v, want it accepted", cfg.Addrs, cfg.AdminAddrs, cfg.LookupdAddrs, err)
		}
	}
	for _, cfg := range []nq.Config{
		{Addrs: []string{"h:4150"}, AdminAddrs: []string{"[fe80::1%eth0]:4151"}},
		{LookupdAddrs: []string{"http://[fe80::1%eth0]:4161"}},
		{LookupdAddrs: []string{"[fe80::1%12]:4161"}},
	} {
		err := cfg.Validate()
		if err == nil {
			t.Errorf("Validate(%q, %q) accepted a zone with a bare %%, which fails at the HTTP request",
				cfg.AdminAddrs, cfg.LookupdAddrs)
			continue
		}
		if mq.CodeOf(err) != mq.CodeInvalidConfig {
			t.Errorf("Validate(%q, %q) = %v (code %v), want an InvalidConfig refusal",
				cfg.AdminAddrs, cfg.LookupdAddrs, err, mq.CodeOf(err))
		}
	}
	// A DSN parameter is decoded once before it is an address, so the DSN
	// writes such a zone "%2525".
	if _, err := nq.ParseDSN("nsq://h:4150?lookupd=http://[fe80::1%2525eth0]:4161&admin=[fe80::1%2525eth0]:4151"); err != nil {
		t.Errorf("a zone written %%2525 in lookupd= and admin= was refused: %v", err)
	}
	if _, err := nq.ParseDSN("nsq://h:4150?lookupd=http://[fe80::1%25eth0]:4161"); err == nil {
		t.Error("lookupd=http://[fe80::1%25eth0]:4161 was accepted: it decodes to a bare %, which fails at the request")
	}
}

// loopbackName is the name of this host's loopback interface, or "" when it
// has none.
func loopbackName() string {
	ifs, err := net.Interfaces()
	if err != nil {
		return ""
	}
	for _, i := range ifs {
		if i.Flags&net.FlagLoopback != 0 {
			return i.Name
		}
	}
	return ""
}
