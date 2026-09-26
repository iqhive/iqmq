package kafkago_test

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"testing"

	kq "github.com/iqhive/iqmq/drivers/kafka/kafkago"
)

// settingsDiff names every field in which two configurations differ once both
// are resolved -- the form [kq.Config.DSN] renders from -- with the credential
// cleared, because the rendering redacts it on purpose.
//
// It compares FIELDS. `%+v` of a Config is its String, which is DSN, so a
// comparison written that way compares a rendering with a rendering and cannot
// fail when the rendering is what lost a setting. TLS is
// compared as "was TLS asked for", because *tls.Config carries a mutex and
// unexported state no round trip reproduces and the DSN carries only the
// request.
func settingsDiff(a, b kq.Config) []string {
	a, b = kq.ResolvedForTest(a), kq.ResolvedForTest(b)
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
	cfg, err := kq.ParseDSN(in)
	if err != nil {
		return false
	}
	rendered := cfg.DSN()
	back, err := kq.ParseDSN(rendered)
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

// renderPositions are the free-text positions of a kafka-go DSN, each with an X
// where the bytes under test go.
func renderPositions() []string {
	params := []string{"clientid", "deadlettertopic"}
	positions := make([]string, 0, 6+len(params))
	positions = append(positions,
		"kafkago://aXb:9092",
		"kafkago://Xb:9092",
		"kafkago://aXb",
		"kafkago://h:9092,aXb:9093",
		// A user name needs a SASL mechanism, or Validate refuses every row
		// for a reason that is not the renderer's.
		"kafkago://aXb@h:9092?sasl=plain",
		"kafkago://aXb:pw@h:9092?sasl=plain",
		"kafkago://h:9092/aXb",
	)
	for _, k := range params {
		positions = append(positions, "kafkago://h:9092?"+k+"=aXb")
	}
	return positions
}

// TestTheRenderingParsesBack holds [kq.Config.DSN] to the one thing it exists
// for: a rendering an operator pastes back is the configuration it came from.
//
// Every printable ASCII byte that is not a letter or a digit, and one that is
// not UTF-8, goes into every free-text position -- a broker, a second broker,
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
		"kafkago://alice:pw@b1:9092,b2:9093/acme.?clientid=svc&sasl=scram-sha-256&tls=true" +
			"&insecureskipverify=true&autocreatetopics=true&readcommitted=true&balancer=roundrobin" +
			"&batchmaxrecords=64&batchtimeout=7ms&batchmaxbytes=131072&maxattempts=4&writetimeout=11s" +
			"&metadatattl=31s&writebackoffmin=5ms&writebackoffmax=2s&fetchminbytes=2&fetchmaxbytes=1048576" +
			"&fetchmaxwait=333ms&queuecapacity=200&sessiontimeout=31s&rebalancetimeout=61s" +
			"&heartbeatinterval=2s&joinbackoff=6s&commitinterval=3s&revokewait=4s&requesttimeout=41s" +
			"&watchpartitionchanges=true&partitionwatchinterval=9s&partitions=6&replicationfactor=3" +
			"&tracker=bitset&nack=retrytopic&deadlettertopic=dlq&nackmaxattempts=5&retries=2&nackdelay=250ms",
		"kafkago://h:9092?tracker=segmented&nack=stop&balancer=range",
	} {
		if !roundTrip(t, in) {
			_, err := kq.ParseDSN(in)
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
// largeValue is the axis's "large" integer, named once because the guard
// below checks THIS value against the rendered defaults: two copies of the
// literal meant editing one of them silently disabled the guard that exists
// to stop the value coinciding with a default.
const largeValue = "4194307"

func TestEveryNumberSurvivesTheRendering(t *testing.T) {
	t.Parallel()
	accepted := 0
	// The value a baseline rendering writes for each parameter, so that the
	// axis can try "one more than the default" and so that a chosen "large"
	// value can be checked against them.
	//
	// The first round of this test used 1048576 as its large integer, which
	// is exactly DefaultFetchMaxBytes -- so `fetchminbytes=1048576` made
	// `FetchMinBytes > FetchMaxBytes` false by one byte and the third
	// "zero is unset vs zero is the default" defect
	// passed the very axis added to find it. A workload that happens to sit
	// on a boundary tests the boundary and nothing else.
	defaults := renderedDefaults(t, "kafkago://h:9092")
	for _, k := range []string{
		"batchmaxrecords", "batchmaxbytes", "maxattempts", "fetchminbytes", "fetchmaxbytes",
		"queuecapacity", "partitions", "replicationfactor", "nackmaxattempts", "retries",
	} {
		values := []string{"0", "-1", largeValue}
		if d, ok := defaults[k]; ok {
			if n, err := strconv.Atoi(d); err == nil {
				values = append(values, strconv.Itoa(n+1), strconv.Itoa(n*2+1))
			}
		}
		for _, v := range values {
			if roundTrip(t, "kafkago://h:9092?"+k+"="+v) {
				accepted++
			}
		}
	}
	// A large value that coincides with a rendered default cannot exercise a
	// cross-field bound, so the axis says so rather than quietly passing.
	for _, v := range []string{largeValue} {
		for k, d := range defaults {
			if d == v {
				t.Errorf("the large value %q is exactly the rendered default of %q, so a bound against it "+
					"cannot fail; choose a value no default equals", v, k)
			}
		}
	}
	for _, k := range []string{
		"batchtimeout", "writetimeout", "metadatattl", "writebackoffmin", "writebackoffmax",
		"fetchmaxwait", "sessiontimeout", "rebalancetimeout", "heartbeatinterval", "joinbackoff",
		"commitinterval", "revokewait", "requesttimeout", "partitionwatchinterval", "nackdelay",
	} {
		for _, v := range []string{"0s", "-1s", "1h"} {
			if roundTrip(t, "kafkago://h:9092?"+k+"="+v) {
				accepted++
			}
		}
	}
	if accepted < 14 {
		t.Fatalf("only %d numeric inputs parsed, so this test tests little", accepted)
	}
}

// renderedDefaults is every parameter a baseline rendering writes, mapped to
// the value it writes.
//
// It is how the numeric axis reaches a value that is one more than a default:
// a cross-field bound whose other side is defaulted can only be crossed by
// exceeding that default, and the test cannot know the defaults any other way
// without restating them.
func renderedDefaults(t *testing.T, baseline string) map[string]string {
	t.Helper()
	cfg, err := kq.ParseDSN(baseline)
	if err != nil {
		t.Fatalf("ParseDSN(%q): %v", baseline, err)
	}
	out := map[string]string{}
	rendered := cfg.DSN()
	_, query, ok := strings.Cut(rendered, "?")
	if !ok {
		t.Fatalf("the baseline rendering %q carries no parameters", rendered)
	}
	for _, pair := range strings.Split(query, "&") {
		if k, v, ok := strings.Cut(pair, "="); ok {
			out[k] = v
		}
	}
	if len(out) == 0 {
		t.Fatalf("the baseline rendering %q parsed to no parameters", rendered)
	}
	return out
}
