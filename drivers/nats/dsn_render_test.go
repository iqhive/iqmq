package nats_test

import (
	"fmt"
	"reflect"
	"strings"
	"testing"

	nq "github.com/iqhive/iqmq/drivers/nats"
)

// settingsDiff names every field in which two configurations differ once both
// are resolved -- the form Config.DSN renders from -- with the credentials
// cleared, because the rendering redacts them on purpose.
//
// It compares FIELDS. `%+v` of a Config is its String, which is DSN, so a
// comparison written that way compares a rendering with a rendering and
// cannot fail when the rendering is what lost a setting.
func settingsDiff(a, b nq.Config) []string {
	a, b = nq.ResolvedForTest(a), nq.ResolvedForTest(b)
	a.Password, b.Password = "", ""
	a.Token, b.Token = "", ""
	va, vb := reflect.ValueOf(a), reflect.ValueOf(b)
	var diffs []string
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
	cfg, err := nq.ParseDSN(in)
	if err != nil {
		return false
	}
	rendered := cfg.DSN()
	back, err := nq.ParseDSN(rendered)
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

// renderPositions are the free-text positions of a NATS Core DSN, each with
// an X where the bytes under test go.
func renderPositions() []string {
	params := []string{"creds", "name"}
	positions := make([]string, 0, 8+len(params))
	positions = append(positions,
		"nats://aXb:4222",
		"nats://Xb:4222",
		"nats://aXb",
		"nats://h:4222,aXb:4223",
		"nats://aXb@h:4222",
		"nats://aXb:pw@h:4222",
		"nats://h:4222/aXb",
		"nats:///aXb",
	)
	for _, k := range params {
		positions = append(positions, "nats://h:4222?"+k+"=aXb")
	}
	return positions
}

// TestTheRenderingParsesBack holds Config.DSN to the one thing it exists for:
// a rendering an operator pastes back is the configuration it came from.
//
// Every printable ASCII byte that is not a letter or a digit, and one that is
// not UTF-8, goes into every free-text position -- a host, a second host, a
// user, the path and each string parameter -- both percent-encoded and raw;
// then every PAIR of the bytes the grammar or the renderer treats specially,
// because a rule such as "a host with a colon is bracketed unless it also holds
// a ']'" only runs when a component holds two kinds of byte. Every DSN ParseDSN
// accepts has to render as one that parses back to the same configuration,
// field by field. FuzzJetStreamDSN found the host row's "," first
// ("nAtsjs://%2C0" rendered its host as ",0") in the JetStream driver, whose
// renderer this one's shares its shape with.
func TestTheRenderingParsesBack(t *testing.T) {
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
		// A position that accepted nothing would pass whatever the
		// renderer did with it. Names -- a stream, a subject -- refuse most
		// punctuation, so how MUCH a position accepts is the validator's
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
	for _, in := range []string{
		"nats://alice:pw@h1:4222,h2:4223/acme?name=svc&pendingmessages=9&pendingbytes=99&connecttimeout=6s" +
			"&flushtimeout=7s&reconnectwait=1s&draintimeout=32s&maxreconnects=7&noecho=true" +
			"&pinginterval=2s&maxpingsout=4&tls=true&insecureskipverify=true",
		"nats://h:4222?token=t0k3n",
		"nats://h:4222?creds=/etc/nats/svc.creds&maxreconnects=-1",
	} {
		if !roundTrip(t, in) {
			_, err := nq.ParseDSN(in)
			t.Fatalf("ParseDSN(%q): %v: the table's own input does not parse", in, err)
		}
	}
}

// TestEveryNumberSurvivesTheRendering is the edge of the same axis: every
// numeric parameter at zero, at -1 and at a large value. A value the parser and
// Validate ACCEPT has to come back from the rendering as itself; a renderer that
// writes a number only when it is positive drops a negative one, and a -1 that
// Validate lets through then reads back as 0. FuzzV1CompatDSN's field-by-field
// check met exactly that on "gcppubsub://p?publishcount=-1". Values the parser refuses are skipped; the test counts the accepted
// ones so that it cannot pass by accepting nothing.
func TestEveryNumberSurvivesTheRendering(t *testing.T) {
	accepted := 0
	for _, k := range []string{"pendingmessages", "pendingbytes", "maxreconnects", "maxpingsout"} {
		for _, v := range []string{"0", "-1", "2147483647"} {
			if roundTrip(t, "nats://h:4222?"+k+"="+v) {
				accepted++
			}
		}
	}
	for _, k := range []string{"connecttimeout", "flushtimeout", "reconnectwait", "draintimeout", "pinginterval"} {
		for _, v := range []string{"0s", "-1s", "1h"} {
			if roundTrip(t, "nats://h:4222?"+k+"="+v) {
				accepted++
			}
		}
	}
	if accepted < 9 {
		t.Fatalf("only %d numeric inputs parsed, so this test tests little", accepted)
	}
}

// TestTheRenderingEscapesNoMoreThanItMust pins the other side of the escaping:
// a byte the parser gives no meaning to in that position is written as itself,
// because a log line is read by a person and an escape also triples a byte,
// which can push a rendering past dsn.MaxLen.
func TestTheRenderingEscapesNoMoreThanItMust(t *testing.T) {
	for in, want := range map[string]string{
		"nats://[::1]:4222":                       "nats://[::1]:4222",
		"nats://[fe80::1%25eth0]:4222":            "nats://[fe80::1%25eth0]:4222",
		"nats://[%%%]:4222":                       "nats://[%%%]:4222",
		"nats://a%3A%5Db:4222":                    "nats://a%3A]b:4222",
		"nats://alice%40example.com:pw@h:4222":    "alice@example.com:",
		"nats://h:4222?creds=/etc/nats/svc.creds": "creds=/etc/nats/svc.creds",
		"nats://h:4222?name=a%2Fb%3Dc%3Fd":        "name=a/b=c?d",
	} {
		cfg, err := nq.ParseDSN(in)
		if err != nil {
			t.Fatalf("ParseDSN(%q): %v", in, err)
		}
		if got := cfg.DSN(); !strings.Contains(got, want) {
			t.Errorf("ParseDSN(%q) renders as %q, which does not carry %q", in, got, want)
		}
	}
}
