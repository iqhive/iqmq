package gcppubsub_test

import (
	"fmt"
	"reflect"
	"strings"
	"testing"

	gps "github.com/iqhive/iqmq/drivers/gcppubsub"
)

// settingsDiff names every field in which two configurations differ.
//
// It compares FIELDS. `%+v` of a Config is its String, a summary that leaves
// out the filter, every duration and every count, so a comparison written
// that way could not see the rendering drop any of them.
// ParseDSN leaves no default behind and fills neither the Clock nor the
// credential JSON, so two parses of the same settings are equal as values.
func settingsDiff(a, b gps.Config) []string {
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
	cfg, err := gps.ParseDSN(in)
	if err != nil {
		return false
	}
	rendered := cfg.DSN()
	back, err := gps.ParseDSN(rendered)
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

// TestTheRenderingParsesBack holds Config.DSN to the one thing it exists for:
// a rendering an operator pastes back is the configuration it came from.
//
// Every printable ASCII byte that is not a letter or a digit, and one that is
// not UTF-8, goes into every free-text position a Pub/Sub DSN has -- the
// project in the authority and in the path, the endpoint, and each string
// parameter -- both percent-encoded and raw; then every PAIR of the bytes the
// grammar or the renderer treats specially, because a rule such as "an
// endpoint that is not a dial address goes in the parameter" only runs when a
// value holds two kinds of byte -- one byte alone keeps a value host-shaped.
// Every DSN ParseDSN accepts has to render as one that parses back to the same
// configuration, field by field. FuzzV1CompatDSN found the path row's ","
// first: "gcppubsub:///," is the project ",", and it rendered as
// "gcppubsub://,", an authority of two empty endpoints.
func TestTheRenderingParsesBack(t *testing.T) {
	var specials []byte
	for c := byte(0x20); c < 0x7f; c++ {
		if c < '0' || c > '9' && c < 'A' || c > 'Z' && c < 'a' || c > 'z' {
			specials = append(specials, c)
		}
	}
	specials = append(specials, 0xff)
	const pairBytes = "%+ @:/?#&=[],;\\"
	params := []string{
		"project", "endpoint", "credentialsfile", "deadlettertopic", "filter",
		"subscriptionprefix", "topicprefix",
	}
	positions := make([]string, 0, 5+len(params))
	positions = append(positions,
		"gcppubsub://aXb",
		"gcppubsub://Xb",
		"gcppubsub://aXb:8085/p",
		"gcppubsub://h:8085/aXb",
		"gcppubsub:///aXb",
	)
	for _, k := range params {
		pos := "gcppubsub://p?" + k + "=aXb"
		if k == "project" {
			// The project named in the authority AND the parameter is
			// refused, so the parameter row has no authority.
			pos = "gcppubsub://?" + k + "=aXb"
		}
		if k == "deadlettertopic" {
			// A dead-letter topic is refused without its delivery-attempt
			// count, and the count without the topic.
			pos += "&maxdeliveryattempts=5"
		}
		positions = append(positions, pos)
	}
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
		// renderer did with it.
		if singles == 0 {
			t.Errorf("%s: no single-byte input parsed, so this position does not test the renderer", pos)
		}
	}
	if pairs < len(pairBytes)*len(pairBytes) {
		t.Errorf("only %d two-byte inputs parsed across every position, so the pair rows test little", pairs)
	}
	// The shapes the review of this renderer named, each of which one byte
	// alone does not reach: an endpoint with a colon and an ampersand, a URL
	// endpoint with a fragment byte, an endpoint that is only a port, and a
	// project in the path with a byte its escaping has to handle.
	for _, in := range []string{
		"gcppubsub://p?endpoint=a%3A%26b",
		"gcppubsub://p?endpoint=https://x/y%23z",
		"gcppubsub://p?endpoint=%3A8085",
		"gcppubsub:///a:b%3Fc",
		"gcppubsub:///example.com:p%25x",
		"gcppubsub://[::1]/p",
		"gcppubsub://[::1]",
	} {
		if !roundTrip(t, in) {
			_, err := gps.ParseDSN(in)
			t.Errorf("ParseDSN(%q): %v: the table's own input does not parse", in, err)
		}
	}
}

// TestEverySettingSurvivesTheRendering is the other axis: every parameter the
// parser reads, set to a value that is not its zero, has to come back from the
// rendering, because one the renderer does not write reads back as zero.
func TestEverySettingSurvivesTheRendering(t *testing.T) {
	for _, in := range []string{
		"gcppubsub://127.0.0.1:8085/my-project?insecuretransport=true&ordering=true&retainacked=true" +
			"&autocreate=true&strictackdeadline=true&compression=true&ackdeadline=31s&maxextension=2m" +
			"&retention=11m&topicretention=15m&apitimeout=12s&publishdelay=13ms&publishtimeout=14s&prefetch=3" +
			"&maxoutstandingbytes=4096&publishcount=5&publishbytes=6000&completionworkers=7&completionqueue=8" +
			"&topicprefix=t-&subscriptionprefix=s-&deadlettertopic=dlq&maxdeliveryattempts=7" +
			"&filter=attributes.k%20%3D%20%22v%22&credentialsfile=/etc/gcp/key.json",
		"gcppubsub://my-project?exactlyonce=true",
	} {
		if !roundTrip(t, in) {
			_, err := gps.ParseDSN(in)
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
	for _, k := range []string{"prefetch", "maxoutstandingbytes", "publishcount", "publishbytes", "completionworkers", "completionqueue"} {
		for _, v := range []string{"0", "-1", "2147483647"} {
			if roundTrip(t, "gcppubsub://p?"+k+"="+v) {
				accepted++
			}
		}
	}
	for _, k := range []string{"ackdeadline", "maxextension", "retention", "topicretention", "apitimeout", "publishdelay", "publishtimeout"} {
		for _, v := range []string{"0s", "-1s", "1h"} {
			if roundTrip(t, "gcppubsub://p?"+k+"="+v) {
				accepted++
			}
		}
	}
	if accepted < 12 {
		t.Fatalf("only %d numeric inputs parsed, so this test tests little", accepted)
	}
}

// TestTheRenderingEscapesNoMoreThanItMust is the other half: a byte the parser
// gives no meaning to in that position is written as itself, and the project
// and the endpoint stay where a person expects them whenever the parser can
// read them back from there.
func TestTheRenderingEscapesNoMoreThanItMust(t *testing.T) {
	for in, want := range map[string]string{
		"gcppubsub://my-project":                                  "gcppubsub://my-project",
		"gcppubsub://127.0.0.1:8085/my-project":                   "gcppubsub://127.0.0.1:8085/my-project",
		"gcppubsub://[::1]:8085/my-project":                       "gcppubsub://[::1]:8085/my-project",
		"gcppubsub://[::1]/p":                                     "gcppubsub://[::1]/p",
		"gcppubsub://[%%%]:8085/p":                                "gcppubsub://[%%%]:8085/p",
		"gcppubsub://p?filter=attributes.x%20%3D%20%22y%22":       `filter=attributes.x = "y"`,
		"gcppubsub://p?credentialsfile=/etc/gcp/key.json":         "credentialsfile=/etc/gcp/key.json",
		"gcppubsub://p?endpoint=https://pubsub.example:443":       "gcppubsub://p?endpoint=https://pubsub.example:443",
		"gcppubsub:///example.com:my-project":                     "gcppubsub:///example.com:my-project",
		"gcppubsub://?project=example.com%3Amy-project":           "gcppubsub:///example.com:my-project",
		"gcppubsub://?endpoint=a%2Fb&project=q%3Ar&ordering=true": "gcppubsub:///q:r?endpoint=a/b&ordering=true",
	} {
		cfg, err := gps.ParseDSN(in)
		if err != nil {
			t.Fatalf("ParseDSN(%q): %v", in, err)
		}
		if got := cfg.DSN(); !strings.HasPrefix(got, want) && !strings.Contains(got, want) {
			t.Errorf("ParseDSN(%q) renders as %q, which does not carry %q", in, got, want)
		}
	}
}
