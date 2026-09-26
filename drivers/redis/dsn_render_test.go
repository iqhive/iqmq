package redis_test

import (
	"fmt"
	"reflect"
	"strings"
	"testing"

	rq "github.com/iqhive/iqmq/drivers/redis"
)

// settingsDiff names every field in which two configurations differ once both
// are resolved -- the form [rq.Config.DSN] renders from -- with the credential
// cleared, because the rendering redacts it on purpose.
//
// It compares FIELDS. `%+v` of a Config is its String, which is DSN, so a
// comparison written that way compares a rendering with a rendering and cannot
// fail when the rendering is what lost a setting. TLS is
// compared as "was TLS asked for", because *tls.Config carries a mutex and
// unexported state no round trip reproduces and the DSN carries only the
// request.
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

// renderPositions are the free-text positions of a Redis DSN, each with an X
// where the bytes under test go.
func renderPositions() []string {
	// The path is the database NUMBER, so there is no free-text path here.
	params := []string{"keyprefix", "consumerprefix", "deadlettertopic", "deadlettersuffix"}
	positions := make([]string, 0, 6+len(params))
	positions = append(positions,
		"redis://aXb:6379",
		"redis://Xb:6379",
		"redis://aXb",
		"redis://h:6379,aXb:6380",
		"redis://aXb@h:6379",
		"redis://aXb:pw@h:6379",
	)
	for _, k := range params {
		positions = append(positions, "redis://h:6379?"+k+"=aXb")
	}
	return positions
}

// TestTheRenderingParsesBack holds [rq.Config.DSN] to the one thing it exists
// for: a rendering an operator pastes back is the configuration it came from.
//
// Every printable ASCII byte that is not a letter or a digit, and one that is
// not UTF-8, goes into every free-text position -- a node, a second node,
// the user, the topic prefix and each string parameter -- both percent-encoded
// and raw; then every PAIR of the bytes the grammar or the renderer treats
// specially, because a rule such as "a host with a colon is bracketed unless
// it also holds a ']'" only runs when a component holds two kinds of byte.
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
		"redis://alice:pw@n1:6379,n2:6380?partitions=8&ackdeadline=45s&claiminterval=7s&claimcount=32" +
			"&block=3s&nack=zsetmover&ackguard=owner&moverinterval=11s&movercount=64&durablelocal=2" +
			"&durablereplicas=1&durabletimeout=4s&dialtimeout=6s&deadlettertopic=dlq&maxlen=100000" +
			"&autocreate=true&keyprefix=acme:&consumerprefix=svc-&tls=true&insecureskipverify=true",
		"redis://h:6379?nack=readd&deadlettersuffix=.dead",
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
// and a -1 that Validate lets through then reads back as 0. Values the parser refuses are skipped; the test counts the
// accepted ones so that it cannot pass by accepting nothing.
func TestEveryNumberSurvivesTheRendering(t *testing.T) {
	t.Parallel()
	accepted := 0
	for _, k := range []string{
		"partitions", "claimcount", "movercount", "durablelocal", "durablereplicas", "maxlen",
	} {
		for _, v := range []string{"0", "-1", "100000"} {
			if roundTrip(t, "redis://h:6379?"+k+"="+v) {
				accepted++
			}
		}
	}
	for _, k := range []string{
		"ackdeadline", "claiminterval", "block", "moverinterval",
		"durabletimeout", "dialtimeout",
	} {
		for _, v := range []string{"0s", "-1s", "1h"} {
			if roundTrip(t, "redis://h:6379?"+k+"="+v) {
				accepted++
			}
		}
	}
	if accepted < 10 {
		t.Fatalf("only %d numeric inputs parsed, so this test tests little", accepted)
	}
}
