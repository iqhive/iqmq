package rabbitmq_test

import (
	"fmt"
	"reflect"
	"slices"
	"strings"
	"testing"

	rq "bitbucket.org/iqhive/iqmq/drivers/rabbitmq"
)

// settingsDiff names every field in which two configurations differ once both
// are resolved -- the form Config.DSN renders from -- with the credential
// cleared, because the rendering redacts it on purpose.
//
// It compares FIELDS. Comparing one rendering against another cannot fail when
// the rendering is what lost a setting, which is why this walks the struct.
// TLS is compared as "was TLS asked for", because
// *tls.Config carries a mutex and unexported state that no round trip
// reproduces and the DSN only carries the request.
func settingsDiff(a, b rq.Config) []string {
	a, b = rq.ResolvedForTest(a), rq.ResolvedForTest(b)
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
	cfg, err := rq.ParseDSN(in)
	if err != nil {
		return false
	}
	rendered := cfg.DSN()
	back, err := rq.ParseDSN(rendered)
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

// renderPositions are the free-text positions of a RabbitMQ DSN, each with an
// X where the bytes under test go.
func renderPositions() []string {
	// deadlettertopic needs the route turned on, or Validate refuses every
	// row and the position tests nothing. exchangetype and delayladder are
	// left out on purpose: one is a closed list of exchange kinds and the
	// other a ";"-separated list of durations, so neither has a spelling that
	// holds an arbitrary byte and still parses. Both are covered at a
	// non-default value by TestEverySettingSurvivesTheRendering.
	params := []string{"vhost", "prefix", "connectionname", "consumertagprefix"}
	positions := make([]string, 0, 8+len(params))
	positions = append(positions,
		"rabbitmq://aXb:5672/v",
		"rabbitmq://Xb:5672/v",
		"rabbitmq://aXb/v",
		"rabbitmq://h:5672,aXb:5673/v",
		"rabbitmq://aXb@h:5672/v",
		"rabbitmq://aXb:pw@h:5672/v",
		"rabbitmq://h:5672/aXb",
		"rabbitmq://h:5672/v?declare=true&deadletter=true&deadlettertopic=aXb",
	)
	for _, k := range params {
		positions = append(positions, "rabbitmq://h:5672/v?"+k+"=aXb")
	}
	return positions
}

// TestTheRenderingParsesBack holds Config.DSN to the one thing it exists for:
// a rendering an operator pastes back is the configuration it came from.
//
// Every printable ASCII byte that is not a letter or a digit, and one that is
// not UTF-8, goes into every free-text position -- a host, a second host, the
// user, the path and each string parameter -- both percent-encoded and raw;
// then every PAIR of the bytes the grammar or the renderer treats specially,
// because a rule such as "a host with a colon is bracketed unless it also
// holds a ']'" only runs when a component holds two kinds of byte. Every DSN
// ParseDSN accepts has to render as one that parses back to the same
// configuration, field by field.
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
		// did with it. How MUCH a position accepts is the validator's
		// business; that it accepts something is this test's.
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
		"rabbitmq://alice:pw@h1:5672,h2:5673/acme?prefix=app.&exchangetype=topic&queuetype=classic" +
			"&declare=true&deadletter=true&deadlettertopic=dlq&queuemaxlength=1000&maxpriority=9" +
			"&mandatory=true&keyasroutingkey=true&confirm=windowed&confirmwindow=64&delay=ttldlx" +
			"&delayladder=1s%3B5s%3B30s&delayqueuettl=2m&detectdelayplugin=true&heartbeat=20s" +
			"&dialtimeout=7s&publishtimeout=11s&channelmax=512&framesize=131072&connectionname=svc" +
			"&reconnectbase=1s&reconnectmax=45s&maxreconnects=7&backoff=equal&consumertagprefix=ctp-" +
			"&tls=true&insecureskipverify=true",
		"rabbitmq://h:5672/?vhost=other",
		"rabbitmq://h:5672/v?delay=plugin&confirm=permessage&backoff=full",
	} {
		if !roundTrip(t, in) {
			_, err := rq.ParseDSN(in)
			t.Fatalf("ParseDSN(%q): %v: the table's own input does not parse", in, err)
		}
	}
}

// TestEveryNumberSurvivesTheRendering is the edge of the same axis: every
// numeric parameter at zero, at -1 and at a large value. A value the parser
// and Validate ACCEPT has to come back from the rendering as itself; a
// renderer that writes a number only when it is positive drops a negative one,
// and a -1 that Validate lets through then reads back as 0. Values the parser refuses are skipped; the test counts the accepted
// ones so that it cannot pass by accepting nothing.
func TestEveryNumberSurvivesTheRendering(t *testing.T) {
	t.Parallel()
	accepted := 0
	for _, k := range []string{"queuemaxlength", "maxpriority", "confirmwindow", "channelmax", "framesize", "maxreconnects"} {
		for _, v := range []string{"0", "-1", "2147483647"} {
			if roundTrip(t, "rabbitmq://h:5672/v?"+k+"="+v) {
				accepted++
			}
		}
	}
	for _, k := range []string{"delayqueuettl", "heartbeat", "dialtimeout", "publishtimeout", "reconnectbase", "reconnectmax"} {
		for _, v := range []string{"0s", "-1s", "1h"} {
			if roundTrip(t, "rabbitmq://h:5672/v?"+k+"="+v) {
				accepted++
			}
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
// normalisations that are not losses. The identity of the endpoints is the
// thing in between, and it is the thing and this whole wave are about.
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
			port := rq.DefaultPort
			if tlsDefault {
				port = rq.DefaultTLSPort
			}
			rest += ":" + port
		}
		out[rest] = true
	}
	return out
}

// TestATypedConfigSurvivesTheRendering is the axis `roundTrip` is blind to:
// every row above starts from a DSN string, so a field position a DSN cannot
// reach is never measured.
//
// The TLS choice can live in the ADDRESS SCHEME here, which Config.url keeps
// and dials on, while renderAddr strips it -- so a TLS configuration rendered
// a DSN that parsed back to a CLEARTEXT dial, quietly.
func TestATypedConfigSurvivesTheRendering(t *testing.T) {
	t.Parallel()
	for _, row := range []struct {
		name    string
		cfg     rq.Config
		tls     bool
		refused bool
	}{
		{name: "amqps in the address", cfg: rq.Config{Addrs: []string{"amqps://h:5671"}}, tls: true},
		{name: "plain address", cfg: rq.Config{Addrs: []string{"amqp://h:5672"}}, tls: false},
		{name: "bare host and port", cfg: rq.Config{Addrs: []string{"h:5672"}}, tls: false},
		{name: "two amqps endpoints", cfg: rq.Config{Addrs: []string{"amqps://a:5671", "amqps://b:5671"}}, tls: true},
		{name: "two plain endpoints", cfg: rq.Config{Addrs: []string{"a:5672", "b:5672"}}, tls: false},
		// A mixed list is REFUSED, and the earlier version of this test
		// asserted its round trip was correct instead -- which pinned the
		// defect: the rendering carries one `tls=` for the whole list, wrote
		// "no TLS" for this one, and the parse-back turned the amqps endpoint
		// into a cleartext dial.
		{name: "mixed list has no rendering", cfg: rq.Config{Addrs: []string{"amqps://h:5671", "h:5672"}}, refused: true},
	} {
		err := row.cfg.Validate()
		if row.refused {
			if err == nil {
				t.Errorf("%s: Validate accepted it; it renders as %q, which parses back to a different "+
					"transport", row.name, row.cfg.DSN())
			}
			continue
		}
		if err != nil {
			t.Errorf("%s: Validate: %v", row.name, err)
			continue
		}
		rendered := row.cfg.DSN()
		back, err := rq.ParseDSN(rendered)
		if err != nil {
			t.Errorf("%s: renders as %q, which does not parse: %v", row.name, rendered, err)
			continue
		}
		if got := back.TLS != nil; got != row.tls {
			t.Errorf("%s: renders as %q, which parses back with TLS=%v, want %v -- a rendering must not "+
				"lose or invent the transport", row.name, rendered, got, row.tls)
		}
		// The endpoints themselves, not their number: a renderer that points
		// every address at one host keeps the count and changes everything
		// that matters.
		want := endpointsOf(row.cfg.Addrs, row.tls)
		got := endpointsOf(back.Addrs, row.tls)
		if !reflect.DeepEqual(want, got) {
			t.Errorf("%s: renders as %q, which parses back naming %v -- the configuration names %v",
				row.name, rendered, keysOf(got), keysOf(want))
		}
	}
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
