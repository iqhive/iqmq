package snssqs_test

import (
	"fmt"
	"reflect"
	"strings"
	"testing"

	sq "bitbucket.org/iqhive/iqmq/drivers/snssqs"
)

// settingsDiff names every field in which two configurations differ once both
// are resolved -- the form [sq.Config.DSN] renders from -- with the
// credentials cleared, because the rendering redacts them on purpose.
//
// It compares FIELDS. `%+v` of a Config is its String, which is DSN, so a
// comparison written that way compares a rendering with a rendering and cannot
// fail when the rendering is what lost a setting.
func settingsDiff(a, b sq.Config) []string {
	a, b = sq.ResolvedForTest(a), sq.ResolvedForTest(b)
	a.SecretAccessKey, b.SecretAccessKey = "", ""
	a.SessionToken, b.SessionToken = "", ""
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
	cfg, err := sq.ParseDSN(in)
	if err != nil {
		return false
	}
	rendered := cfg.DSN()
	back, err := sq.ParseDSN(rendered)
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

// renderPositions are the free-text positions of an snssqs DSN, each with an X
// where the bytes under test go.
func renderPositions() []string {
	// `deadletterqueue` needs `maxreceivecount`, or Validate refuses the row
	// for a reason that is not the renderer's.
	positions := []string{
		"snssqs://aXb:4566?region=r",
		"snssqs://Xb:4566?region=r",
		"snssqs://aXb?region=r",
		"snssqs://aXb@h:4566?region=r",
		"snssqs://aXb:sec@h:4566?region=r",
		"snssqs://h:4566/aXb?region=r",
		"snssqs://h:4566?region=aXb",
		"snssqs://h:4566?region=r&partition=aXb",
		"snssqs://h:4566?region=r&accountid=aXb",
		"snssqs://h:4566?region=r&queueprefix=aXb",
		"snssqs://h:4566?region=r&deadletterqueue=aXb&maxreceivecount=3",
	}
	return positions
}

// TestTheRenderingParsesBack holds [sq.Config.DSN] to the one thing it exists
// for: a rendering an operator pastes back is the configuration it came from.
//
// Every printable ASCII byte that is not a letter or a digit, and one that is
// not UTF-8, goes into every free-text position -- the host, the access key
// id, the path and each string parameter -- both percent-encoded and raw; then
// every PAIR of the bytes the grammar or the renderer treats specially,
// because a rule such as "a host with a colon is bracketed unless it also
// holds a ']'" only runs when a component holds two kinds of byte.
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
		"snssqs://AKIAEXAMPLE:secret@127.0.0.1:4566/acme?region=us-east-1&partition=aws-cn" +
			"&accountid=000000000000&queueprefix=q-&autocreate=true&noqueuepolicy=true&filterscope=body" +
			"&pollers=4&maxmessages=7&waittime=19s&ackdeadline=45s&leaseguard=3s&settle=hybrid" +
			"&settlemaxitems=9&settleinterval=250ms&body=base64&maxmessagebytes=131072" +
			"&deadletterqueue=parked&maxreceivecount=5&retention=4h&apitimeout=17s&sessiontoken=tok" +
			"&insecuretransport=true",
		// The production shape: a credential and the SDK's own regional
		// endpoints, which have no host of their own.
		"snssqs://AKIAEXAMPLE:secret@aws?region=eu-west-1",
		"snssqs://h:4566?region=r&settle=immediate&body=text&filterscope=attributes",
	} {
		if !roundTrip(t, in) {
			_, err := sq.ParseDSN(in)
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
	// Nine is the service's own ceiling for maxmessages and settlemaxitems, so
	// it is as large as those two go.
	for _, k := range []string{"pollers", "maxmessages", "settlemaxitems", "maxmessagebytes", "maxreceivecount"} {
		for _, v := range []string{"0", "-1", "9"} {
			if roundTrip(t, "snssqs://h:4566?region=r&"+k+"="+v) {
				accepted++
			}
		}
	}
	for _, k := range []string{"waittime", "ackdeadline", "leaseguard", "settleinterval", "retention", "apitimeout"} {
		for _, v := range []string{"0s", "-1s", "19s"} {
			if roundTrip(t, "snssqs://h:4566?region=r&"+k+"="+v) {
				accepted++
			}
		}
	}
	// The large-value axis, which nine cannot reach: the service's own
	// ceilings refuse most of these, and a refused row is skipped rather than
	// counted, so what this adds is the ones that ARE accepted -- a value the
	// parser takes and the renderer might drop is the shape this test exists
	// for.
	for _, k := range []string{"maxmessagebytes", "maxreceivecount", "pollers"} {
		for _, v := range []string{"1048576", "2147483647"} {
			if roundTrip(t, "snssqs://h:4566?region=r&"+k+"="+v) {
				accepted++
			}
		}
	}
	if accepted < 12 {
		t.Fatalf("only %d numeric inputs parsed, so this test tests little", accepted)
	}
}

// TestATypedConfigSurvivesTheRendering is the axis every `roundTrip` row is
// blind to: they all start from a DSN string, so an `Endpoint` spelling only a
// typed Config can hold is never measured. Round 1's BLOCKING finding lived in
// exactly that blind spot, and round 2 fixed it without adding this test, so
// all three of its behaviours were pinned by nothing.
//
// Reverting any one of them must fail here:
//   - the authority cut in renderEndpoint,
//   - errInsecureWithTLSEndpoint,
//   - errEndpointIsSentinel.
func TestATypedConfigSurvivesTheRendering(t *testing.T) {
	t.Parallel()
	for _, row := range []struct {
		name     string
		cfg      sq.Config
		refused  bool
		endpoint string
		prefix   string
	}{
		// The LocalStack shape: a base path the SDK honours. The path has no
		// DSN position (this one belongs to the prefix) and is dropped, which
		// is recorded in the maintenance notes -- but the host and the PORT must
		// survive. Round 1 rendered "[localhost:4566]" as a bracketed
		// pseudo-host and stole the path as the prefix.
		{
			name: "endpoint with a base path",
			cfg: sq.Config{
				Region: "r", Endpoint: "http://localhost:4566/000000000000",
				InsecureTransport: true,
			},
			endpoint: "http://localhost:4566", prefix: "",
		},
		{
			name:     "endpoint with a path and no port",
			cfg:      sq.Config{Region: "r", Endpoint: "https://h/sqs"},
			endpoint: "https://h", prefix: "",
		},
		{
			name:     "plain https endpoint",
			cfg:      sq.Config{Region: "r", Endpoint: "https://h:443"},
			endpoint: "https://h:443",
		},
		{
			name:     "no endpoint is the regional one",
			cfg:      sq.Config{Region: "r", AccessKeyID: "k", SecretAccessKey: "s"},
			endpoint: "",
		},
		// InsecureTransport MEANS plain HTTP, so an https:// endpoint beside
		// it contradicts itself -- and the rendering resolved it the UNSAFE
		// way, parsing back as http://.
		{
			name:    "https beside InsecureTransport",
			cfg:     sq.Config{Region: "r", Endpoint: "https://h", InsecureTransport: true},
			refused: true,
		},
		// "aws" is the authority's sentinel for the SDK's own regional
		// endpoint, so an endpoint whose host is literally that renders a DSN
		// parsing back to NO endpoint.
		{
			name:    "the regional sentinel as a host",
			cfg:     sq.Config{Region: "r", Endpoint: "https://aws"},
			refused: true,
		},
		{
			name:    "the sentinel with a trailing slash",
			cfg:     sq.Config{Region: "r", Endpoint: "https://aws/"},
			refused: true,
		},
		{
			name:    "the sentinel with a port",
			cfg:     sq.Config{Region: "r", Endpoint: "http://aws:4566", InsecureTransport: true},
			refused: true,
		},
	} {
		err := row.cfg.Validate()
		if row.refused {
			if err == nil {
				t.Errorf("%s: Validate accepted it; it renders as %q, which parses back to a different "+
					"endpoint", row.name, row.cfg.DSN())
			}
			continue
		}
		if err != nil {
			t.Errorf("%s: Validate: %v", row.name, err)
			continue
		}
		rendered := row.cfg.DSN()
		back, err := sq.ParseDSN(rendered)
		if err != nil {
			t.Errorf("%s: renders as %q, which does not parse: %v", row.name, rendered, err)
			continue
		}
		if back.Endpoint != row.endpoint {
			t.Errorf("%s: renders as %q, which parses back naming the endpoint %q, want %q",
				row.name, rendered, back.Endpoint, row.endpoint)
		}
		if back.TopicPrefix != row.prefix {
			t.Errorf("%s: renders as %q, whose TopicPrefix parses back as %q, want %q -- an endpoint's "+
				"path must not become the prefix", row.name, rendered, back.TopicPrefix, row.prefix)
		}
		if again := back.DSN(); again != rendered {
			t.Errorf("%s: renders as %q and that renders as %q", row.name, rendered, again)
		}
	}
}

// TestThePathEscaperEscapesWhatThePathDecodes pins an escaper the validator
// currently makes unreachable, the third copy of the one watched in sqs and pulsar (and missed here).
//
// Reverting escapePath to the identity fails no other test in this package,
// because every value that reaches the path position is validated to bytes
// that need no escaping. That is exactly the shape of a guard nobody has
// watched fail, so this watches it.
func TestThePathEscaperEscapesWhatThePathDecodes(t *testing.T) {
	t.Parallel()
	for in, want := range map[string]string{
		"plain":  "plain",
		"a%b":    "a%25b",
		"a?b":    "a%3Fb",
		"a#b":    "a%23b",
		"a%?#b":  "a%25%3F%23b",
		"a-b_c.": "a-b_c.",
	} {
		if got := sq.EscapePathForTest(in); got != want {
			t.Errorf("EscapePathForTest(%q) = %q, want %q", in, got, want)
		}
	}
}
