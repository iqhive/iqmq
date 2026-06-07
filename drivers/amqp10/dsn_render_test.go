package amqp10_test

import (
	"crypto/tls"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"testing"

	aq "bitbucket.org/iqhive/iqmq/drivers/amqp10"
)

// settingsDiff names every field in which two configurations differ once both
// are resolved -- the form [aq.Config.DSN] renders from -- with the credential
// cleared, because the rendering redacts it on purpose.
//
// It compares FIELDS. `%+v` of a Config is its String, which is DSN, so a
// comparison written that way compares a rendering with a rendering and cannot
// fail when the rendering is what lost a setting. TLS is
// compared as "was TLS asked for", because *tls.Config carries a mutex and
// unexported state no round trip reproduces and the DSN carries only the
// request.
func settingsDiff(a, b aq.Config) []string {
	a, b = aq.ResolvedForTest(a), aq.ResolvedForTest(b)
	a.Password, b.Password = "", ""
	tlsA, tlsB := a.TLS != nil, b.TLS != nil
	a.TLS, b.TLS = nil, nil
	va, vb := reflect.ValueOf(a), reflect.ValueOf(b)
	var diffs []string
	if tlsA != tlsB {
		diffs = append(diffs, fmt.Sprintf("TLS set=%v, want set=%v", tlsB, tlsA))
	}
	for i := range va.NumField() {
		if !reflect.DeepEqual(va.Field(i).Interface(), vb.Field(i).Interface()) {
			diffs = append(diffs, fmt.Sprintf("%s %#v, want %#v", va.Type().Field(i).Name, vb.Field(i).Interface(), va.Field(i).Interface()))
		}
	}
	return diffs
}

// roundTrip parses in, renders it, parses the rendering and reports every way
// the trip changed it. It reports false when in itself does not parse.
func roundTrip(t *testing.T, in string) bool {
	t.Helper()
	cfg, err := aq.ParseDSN(in)
	if err != nil {
		return false
	}
	rendered := cfg.DSN()
	back, err := aq.ParseDSN(rendered)
	if err != nil {
		t.Errorf("ParseDSN(%q) renders as %q, which does not parse: %v", in, rendered, err)
		return true
	}
	if again := back.DSN(); again != rendered {
		t.Errorf("ParseDSN(%q) renders as %q and that renders as %q", in, rendered, again)
	}
	if diffs := settingsDiff(cfg, back); len(diffs) > 0 {
		t.Errorf("ParseDSN(%q) renders as %q, which parses back to a different configuration: %s",
			in, rendered, strings.Join(diffs, "; "))
	}
	return true
}

// renderPositions are the free-text positions of an AMQP 1.0 DSN, each with an
// X where the bytes under test go.
func renderPositions() []string {
	params := []string{"containerid", "hostname"}
	positions := make([]string, 0, 6+len(params))
	positions = append(positions,
		"amqp10://aXb:5672",
		"amqp10://Xb:5672",
		"amqp10://aXb",
		"amqp10://h:5672,aXb:5673",
		"amqp10://aXb@h:5672?tls=true",
		"amqp10://aXb:pw@h:5672?tls=true",
	)
	for _, k := range params {
		positions = append(positions, "amqp10://h:5672?"+k+"=aXb")
	}
	return positions
}

// TestTheRenderingParsesBack holds [aq.Config.DSN] to the one thing it exists
// for: a rendering an operator pastes back is the configuration it came from.
//
// Every printable ASCII byte that is not a letter or a digit, and one that is
// not UTF-8, goes into every free-text position -- a host, a second host, the
// user and each string parameter -- both percent-encoded and raw; then every
// PAIR of the bytes the grammar or the renderer treats specially, because a
// rule that depends on two kinds of byte only runs when a component holds
// both. Every DSN ParseDSN accepts has to render as one that parses back to
// the same configuration, field by field.
func TestTheRenderingParsesBack(t *testing.T) {
	t.Parallel()
	var specials []byte
	for c := byte(0x20); c < 0x7f; c++ {
		if c < '0' || c > '9' && c < 'A' || c > 'Z' && c < 'a' || c > 'z' {
			specials = append(specials, c)
		}
	}
	specials = append(specials, 0xff)
	const pairBytes = "%+ @:/?#&=[],;\\"
	positions := renderPositions()
	pairs := 0
	for _, pos := range positions {
		singles := 0
		for _, c := range specials {
			for _, x := range []string{fmt.Sprintf("%%%02X", c), string([]byte{c})} {
				if roundTrip(t, strings.Replace(pos, "X", x, 1)) {
					singles++
				}
			}
		}
		for i := range len(pairBytes) {
			for j := range len(pairBytes) {
				x := fmt.Sprintf("%%%02X%%%02X", pairBytes[i], pairBytes[j])
				if roundTrip(t, strings.Replace(pos, "X", x, 1)) {
					pairs++
				}
			}
		}
		// A position that accepted nothing would pass whatever the renderer
		// did with it.
		if singles == 0 {
			t.Errorf("%s: no single-byte input parsed, so this position does not test the renderer", pos)
		}
	}
	if pairs < len(pairBytes)*len(pairBytes) {
		t.Errorf("only %d two-byte inputs parsed across every position, so the pair rows test little", pairs)
	}
}

// TestEverySettingSurvivesTheRendering is the other axis: every parameter the
// parser reads, set to a value that is not its default, has to come back from
// the rendering. A setting the renderer does not write reads back as the
// default, so only a non-default value shows it was dropped.
func TestEverySettingSurvivesTheRendering(t *testing.T) {
	t.Parallel()
	for _, in := range []string{
		"amqp10://alice:pw@h1:5671,h2:5672?tls=true&insecureskipverify=true&insecureplainsasl=true" +
			"&sasl=plain&containerid=svc-1&hostname=vhost.example&sendersettle=mixed&receiversettle=second" +
			"&idletimeout=45s&maxframesize=32768&maxsessions=128&maxcredit=64&maxpending=256&nondurable=true" +
			"&dialtimeout=7s&operationtimeout=21s&writetimeout=11s&backoff=equaljitter&reconnectbase=300ms" +
			"&reconnectmax=25s&reconnectgrowth=1.5&reconnectattempts=9",
		"amqp10://h:5672?sasl=anonymous&backoff=decorrelated&noreconnect=true",
		"amqp10://h:5672?sasl=external&sendersettle=settled&receiversettle=first",
	} {
		if !roundTrip(t, in) {
			_, err := aq.ParseDSN(in)
			t.Fatalf("ParseDSN(%q): %v: the table's own input does not parse", in, err)
		}
	}
}

// TestEveryNumberSurvivesTheRendering is the edge of the same axis: every
// numeric parameter at zero, at -1 and at a large value. A value the parser
// and Validate ACCEPT has to come back from the rendering as itself; a
// renderer that writes a number only when it is positive drops a negative one,
// and a -1 that Validate lets through then reads back as 0. The float rows are here for a second reason: a large float renders
// in exponent form, and a "+" in a query VALUE decodes as a space. Values the
// parser refuses are skipped; the test counts the accepted ones so that it
// cannot pass by accepting nothing.
func TestEveryNumberSurvivesTheRendering(t *testing.T) {
	t.Parallel()
	accepted := 0
	for _, k := range []string{"maxframesize", "maxsessions", "maxcredit", "maxpending", "reconnectattempts"} {
		for _, v := range []string{"0", "-1", "32768"} {
			if roundTrip(t, "amqp10://h:5672?"+k+"="+v) {
				accepted++
			}
		}
	}
	for _, k := range []string{
		"idletimeout", "dialtimeout", "operationtimeout", "writetimeout", "reconnectbase",
		// reconnectmax is half of the base-under-max check, which read its
		// zero as "no cap" where the rendering reads it as the 30s default
		// ; a row for it is where that defect lived.
		"reconnectmax",
	} {
		for _, v := range []string{"0s", "-1s", "1h"} {
			if roundTrip(t, "amqp10://h:5672?"+k+"="+v) {
				accepted++
			}
		}
	}
	for _, v := range []string{"0", "-1", "1.5", "1e10", "1e-10", "100000000000000000000"} {
		if roundTrip(t, "amqp10://h:5672?reconnectgrowth="+v) {
			accepted++
		}
	}
	if accepted < 12 {
		t.Fatalf("only %d numeric inputs parsed, so this test tests little", accepted)
	}
}

// endpointsOf is the set of (host, port) pairs a list of addresses names, with
// the scheme and the default port normalised away.
//
// It is what makes a redirect fail. Asserting the endpoint COUNT, which is
// what the first version of this test did, passes a renderer that points every
// address at one attacker's host; asserting byte equality fails on
// normalisations that are not losses.
func endpointsOf(addrs []string, tlsDefault bool) map[string]bool {
	out := map[string]bool{}
	for _, a := range addrs {
		rest := a
		if i := strings.Index(rest, "://"); i >= 0 {
			rest = rest[i+3:]
		}
		if i := strings.IndexAny(rest, "/?#"); i >= 0 {
			rest = rest[:i]
		}
		if !strings.Contains(rest, ":") {
			port := aq.DefaultPort
			if tlsDefault {
				port = aq.DefaultTLSPort
			}
			rest += ":" + port
		}
		out[rest] = true
	}
	return out
}

// keysOf is a set's members, sorted, for a failure message.
func keysOf(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	slices.Sort(out)
	return out
}

// TestATypedConfigSurvivesTheRendering is the axis every row above is blind
// to: `roundTrip` always starts from a DSN string, so a field position a DSN
// cannot reach -- or reaches only in the one spelling ParseDSN constructs --
// is never measured. Three of review round 1's findings lived in exactly that
// blind spot.
//
// Here the TLS choice lives in the ADDRESS SCHEME with no *tls.Config, which
// is what this package's own Example_open does. Before the fix `renderAddr`
// stripped the scheme and nothing wrote `tls=`, so a TLS configuration
// rendered a PLAINTEXT DSN -- and for the SASL PLAIN example, one its own
// parser refused.
func TestATypedConfigSurvivesTheRendering(t *testing.T) {
	t.Parallel()
	for _, row := range []struct {
		name    string
		cfg     aq.Config
		tls     bool
		refused bool
	}{
		{name: "amqps in the address", cfg: aq.Config{Addrs: []string{"amqps://h:5671"}}, tls: true},
		{name: "amqp+ssl in the address", cfg: aq.Config{Addrs: []string{"amqp+ssl://h:5671"}}, tls: true},
		{name: "plain address", cfg: aq.Config{Addrs: []string{"amqp://h:5672"}}, tls: false},
		{name: "bare host and port", cfg: aq.Config{Addrs: []string{"h:5672"}}, tls: false},
		{name: "two amqps endpoints", cfg: aq.Config{Addrs: []string{"amqps://a:5671", "amqps://b:5671"}}, tls: true},
		{name: "the package's own example", cfg: aq.Config{
			Addrs: []string{"amqps://broker.example:5671"}, Username: "orders",
			Password: "s3cr3t", SASL: aq.SASLPlain, ReceiverSettle: aq.ReceiverSettleSecond,
		}, tls: true},
		// A bare address with a TLS config really is TLS: resolve writes
		// "amqps://" in, because go-amqp refuses an address with no scheme.
		{name: "bare address with a TLS config", cfg: aq.Config{
			Addrs: []string{"h:5671"}, TLS: &tls.Config{MinVersion: tls.VersionTLS12},
		}, tls: true},
		// A TLS config beside an explicit "amqp://" is refused: go-amqp picks
		// the transport from the scheme and would dial in the clear, so a
		// rendering of that pair INVENTS a transport.
		{name: "TLS config beside a plaintext scheme", cfg: aq.Config{
			Addrs: []string{"amqp://h:5672"}, TLS: &tls.Config{MinVersion: tls.VersionTLS12},
		}, refused: true},
		// A mixed list has no rendering: one `tls=` cannot describe both
		// endpoints, and the direction it got wrong was the unsafe one.
		{name: "mixed list has no rendering", cfg: aq.Config{
			Addrs: []string{"amqps://a:5671", "amqp://b:5672"},
		}, refused: true},
	} {
		err := row.cfg.Validate()
		if row.refused {
			if err == nil {
				t.Errorf("%s: Validate accepted it; it renders as %q, whose transport differs from what is "+
					"dialled", row.name, row.cfg.DSN())
			}
			continue
		}
		if err != nil {
			t.Errorf("%s: Validate: %v", row.name, err)
			continue
		}
		rendered := row.cfg.DSN()
		back, err := aq.ParseDSN(rendered)
		if err != nil {
			t.Errorf("%s: renders as %q, which does not parse: %v", row.name, rendered, err)
			continue
		}
		if got := back.TLS != nil; got != row.tls {
			t.Errorf("%s: renders as %q, which parses back with TLS=%v, want %v -- a rendering must not "+
				"lose or invent the transport", row.name, rendered, got, row.tls)
		}
		// The comparison is NOT field-by-field: ParseDSN normalises what a
		// typed Config may spell several ways (the scheme alias, a bare host
		// gaining one, the TLS choice arriving as a *tls.Config). What must
		// hold is that the rendering is STABLE and that the ENDPOINTS
		// themselves survive -- not merely how many there are, which is what
		// the first version of this test checked and what let a redirect
		// through.
		if again := back.DSN(); again != rendered {
			t.Errorf("%s: renders as %q and that renders as %q", row.name, rendered, again)
		}
		want := endpointsOf(row.cfg.Addrs, row.tls)
		got := endpointsOf(back.Addrs, row.tls)
		if !reflect.DeepEqual(want, got) {
			t.Errorf("%s: renders as %q, which parses back naming %v -- the configuration names %v",
				row.name, rendered, keysOf(got), keysOf(want))
		}
	}
}
