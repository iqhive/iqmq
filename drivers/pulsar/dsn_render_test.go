package pulsar_test

import (
	"fmt"
	"reflect"
	"slices"
	"strings"
	"testing"

	pq "github.com/iqhive/iqmq/drivers/pulsar"
)

// notCarriedByTheDSN names every [pq.Config] field a rendering does not write
// at all: 39 of the 46 parameters ParseDSN reads.
//
// It does NOT hold the two credentials. `Config.DSN` DOES write Token and
// AuthParams -- as the redaction, which is the whole point -- so they
// cannot round-trip for a reason every driver here shares, and lumping them
// in with the dropped settings made this list read 41 where its own comment
// said 39. They are skipped separately, below.
//
// [pq.Config.DSN]'s own doc comment says it is not a round trip, and this
// driver is the one place in the repository where that is true of more than a
// credential: `put` is called for seven parameters and every other setting --
// every timeout, every batching bound, the whole consumer shape -- is dropped.
// A rendering that drops a setting reads back as the default, so a DSN this
// driver rendered and a person pasted opens a DIFFERENT client, quietly.
//
// This list is the finding, written down rather than argued about: the test
// below holds the renderer to losing nothing OUTSIDE it, so a field that
// starts being dropped is a failure, and closing the gap is a matter of
// deleting entries. It is recorded in the maintenance notes with an owner, because
// rendering thirty-nine more parameters changes every Pulsar log line and is
// not the defect the maintenance notes asked this branch to close.
var notCarriedByTheDSN = []string{
	"AckGroupingMaxSize", "AckGroupingMaxTime", "AckTimeout", "AckWithResponse",
	"AllowOutOfOrderDelivery", "AutoDiscoveryPeriod", "BatchingMaxMessages",
	"BatchingMaxPublishDelay", "BatchingMaxSize", "BlockIfQueueFull", "Compression",
	"ConnectionTimeout", "DisableAckTimeout", "DisableBatching", "Domain",
	"EnableBatchIndexAck", "EnsureTopology", "Hashing", "InsecureSkipVerify",
	"KeepAliveInterval", "LeaseGuard", "ListenerName", "MaxAttempts",
	"MaxConnectionsPerBroker", "MaxMessageBytes", "MaxPendingMessages", "MemoryLimitBytes",
	"NackRedeliveryDelay", "OperationTimeout", "Partitions", "PingTopic",
	"ProducerName", "ReadCompacted", "ReceiverQueueSize", "RetryEnable",
	"RetryTopic", "SendTimeout", "TLSTrustCertsFilePath",
	"EnableTransactions",
}

// settingsDiff names every field in which two configurations differ, ignoring
// the ones the rendering is known not to carry.
//
// It compares FIELDS. Comparing one rendering against another cannot fail when
// the rendering is what lost a setting, which is why this walks the struct.
// redactedByTheDSN names the fields a rendering writes as a redaction rather
// than as their value, so a parse-back cannot recover them. That is the
// rendering doing its job, not a setting it drops.
var redactedByTheDSN = []string{"Token", "AuthParams"}

func settingsDiff(a, b pq.Config) []string {
	a, b = pq.ResolvedForTest(a), pq.ResolvedForTest(b)
	va, vb := reflect.ValueOf(a), reflect.ValueOf(b)
	var diffs []string
	for i := range va.NumField() {
		name := va.Type().Field(i).Name
		if slices.Contains(notCarriedByTheDSN, name) || slices.Contains(redactedByTheDSN, name) {
			continue
		}
		if !reflect.DeepEqual(va.Field(i).Interface(), vb.Field(i).Interface()) {
			diffs = append(diffs, fmt.Sprintf("%s %#v, want %#v", name, vb.Field(i).Interface(), va.Field(i).Interface()))
		}
	}
	return diffs
}

// TestTheDroppedSettingsListNamesRealFields keeps the list above honest: an
// entry that is not a field would silently stop excluding anything, and the
// instrument would look stricter than it is.
func TestTheDroppedSettingsListNamesRealFields(t *testing.T) {
	t.Parallel()
	typ := reflect.TypeOf(pq.Config{})
	for _, name := range append(append([]string{}, notCarriedByTheDSN...), redactedByTheDSN...) {
		if _, ok := typ.FieldByName(name); !ok {
			t.Errorf("the exclusion lists name %q, which is not a Config field", name)
		}
	}
	// The count is in the doc comment above and in the maintenance notes, so a
	// list that drifts from it makes both wrong. It read 41 while saying 39,
	// because two credentials the renderer DOES write had been folded in.
	if got := len(notCarriedByTheDSN); got != 39 {
		t.Errorf("notCarriedByTheDSN holds %d entries; its own comment and the maintenance notes say 39 -- "+
			"update both together or the gap is recorded wrongly", got)
	}
	for _, name := range redactedByTheDSN {
		if slices.Contains(notCarriedByTheDSN, name) {
			t.Errorf("%q is in both lists; it is written as a redaction, not dropped", name)
		}
	}
	if len(notCarriedByTheDSN) >= typ.NumField() {
		t.Fatalf("the list excludes %d of %d fields, so the comparison tests nothing",
			len(notCarriedByTheDSN), typ.NumField())
	}
}

// roundTrip parses in, renders it, parses the rendering and reports every way
// the trip changed it. It reports false when in itself does not parse.
func roundTrip(t *testing.T, in string) bool {
	t.Helper()
	cfg, err := pq.ParseDSN(in)
	if err != nil {
		return false
	}
	rendered := cfg.DSN()
	back, err := pq.ParseDSN(rendered)
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

// renderPositions are the free-text positions of a Pulsar DSN that the
// rendering carries, each with an X where the bytes under test go.
func renderPositions() []string {
	return []string{
		"pulsar://aXb:6650",
		"pulsar://Xb:6650",
		"pulsar://aXb",
		"pulsar://h:6650,aXb:6651",
		"pulsar://h:6650/aXb",
		"pulsar://h:6650/acme/aXb",
		"pulsar://h:6650/acme/ns/aXb",
		// AdminURL must be an http(s) URL or Validate refuses every row,
		// so the bytes go inside one.
		"pulsar://h:6650?admin=http%3A%2F%2FaXb%3A8080",
		"pulsar://h:6650?authname=aXb&authparams=p",
		"pulsar://h:6650?deadlettertopic=aXb",
	}
}

// TestTheRenderingParsesBack holds [pq.Config.DSN] to the one thing it exists
// for: a rendering an operator pastes back is the configuration it came from,
// as far as the rendering carries it.
//
// Every printable ASCII byte that is not a letter or a digit, and one that is
// not UTF-8, goes into every free-text position -- a host, a second host, the
// tenant, the namespace, the topic prefix and each string parameter the
// rendering writes -- both percent-encoded and raw; then every PAIR of the
// bytes the grammar or the renderer treats specially, because a rule such as
// "a host with a colon is bracketed unless it also holds a ']'" only runs when
// a component holds two kinds of byte.
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
// parser reads, set to a value that is not its default. A setting the renderer
// does not write reads back as the default, so only a non-default value shows
// it was dropped -- and here that is most of them, which is why
// notCarriedByTheDSN exists and why this test names it in its failure.
func TestEverySettingSurvivesTheRendering(t *testing.T) {
	t.Parallel()
	for _, in := range []string{
		"pulsar://h1:6650,h2:6651/acme/prod/svc-?admin=http%3A%2F%2Fh1%3A8080&subscriptiontype=keyshared" +
			"&metadata=portableenvelope&authname=token&authparams=file%3A%2F%2F%2Fsvc.jwt&deadlettertopic=dlq" +
			"&maxattempts=5",
		"pulsar+ssl://h:6651/acme/prod?subscriptiontype=failover&metadata=adaptive",
		"pulsar://h:6650?token=t0k3n",
	} {
		if !roundTrip(t, in) {
			_, err := pq.ParseDSN(in)
			t.Fatalf("ParseDSN(%q): %v: the table's own input does not parse", in, err)
		}
	}
}

// TestEveryNumberSurvivesTheRendering is the edge of the same axis: every
// numeric parameter at zero, at -1 and at a large value. A value the parser
// and Validate ACCEPT has to come back from the rendering as itself; a
// renderer that writes a number only when it is positive drops a negative one,
// and a -1 that Validate lets through then reads back as 0. Every numeric parameter this driver reads is in notCarriedByTheDSN,
// so what this measures today is that the parser's own acceptance is
// self-consistent; it becomes the real check the moment a number starts being
// rendered. Values the parser refuses are skipped; the test counts the
// accepted ones so that it cannot pass by accepting nothing.
func TestEveryNumberSurvivesTheRendering(t *testing.T) {
	t.Parallel()
	accepted := 0
	for _, k := range []string{
		"maxconnectionsperbroker", "memorylimitbytes", "batchingmaxmessages", "batchingmaxsize",
		"maxpendingmessages", "receiverqueuesize", "ackgroupingmaxsize", "maxattempts",
		"maxmessagebytes", "partitions",
	} {
		for _, v := range []string{"0", "-1", "2147483647"} {
			if roundTrip(t, "pulsar://h:6650?"+k+"="+v) {
				accepted++
			}
		}
	}
	for _, k := range []string{
		"connectiontimeout", "operationtimeout", "keepaliveinterval", "batchingmaxpublishdelay",
		"sendtimeout", "ackgroupingmaxtime", "nackredeliverydelay", "acktimeout", "leaseguard",
		"autodiscoveryperiod",
	} {
		for _, v := range []string{"0s", "-1s", "1h"} {
			if roundTrip(t, "pulsar://h:6650?"+k+"="+v) {
				accepted++
			}
		}
	}
	if accepted < 20 {
		t.Fatalf("only %d numeric inputs parsed, so this test tests little", accepted)
	}
}

// TestATypedServiceURLSurvivesTheRendering is the axis `roundTrip` is blind
// to: every row above starts from a DSN string, so a ServiceURL spelling only
// a typed Config can hold is never measured.
//
// pulsar-client-go splits a host list on "," AND ";" (splitURI in
// internal/service_uri.go), so "pulsar://a;b:6650" is two brokers and
// Validate accepts it. The first round's renderServiceURL split on "," alone,
// so the ";" fell INSIDE a host, was escaped to "%3B", and the rendering was
// then refused by this driver's own dialableHost -- a regression against main,
// which rendered it unchanged.
func TestATypedServiceURLSurvivesTheRendering(t *testing.T) {
	t.Parallel()
	for _, row := range []struct {
		url   string
		hosts []string
	}{
		{"pulsar://a;b:6650", []string{"a:6650", "b:6650"}},
		{"pulsar://a,b:6650", []string{"a:6650", "b:6650"}},
		{"pulsar://a:6650", []string{"a:6650"}},
		{"pulsar://a;b;c:6650", []string{"a:6650", "b:6650", "c:6650"}},
		{"pulsar+ssl://a;b:6651", []string{"a:6651", "b:6651"}},
	} {
		cfg := pq.Config{ServiceURL: row.url}
		if err := cfg.Validate(); err != nil {
			t.Errorf("Validate(%q): %v", row.url, err)
			continue
		}
		rendered := cfg.DSN()
		back, err := pq.ParseDSN(rendered)
		if err != nil {
			t.Errorf("ServiceURL %q renders as %q, which does not parse: %v", row.url, rendered, err)
			continue
		}
		// The BROKERS, not their number. The first version of this test
		// counted separators, which passes a renderer that points every
		// broker at one attacker's host -- the outcome this whole wave
		// exists to prevent. The separator is
		// normalised to the DSN's own "," and a host without a port gains
		// the scheme's default, and neither is a loss.
		want := map[string]bool{}
		for _, h := range row.hosts {
			want[h] = true
		}
		got := brokersOf(back.ServiceURL)
		if !reflect.DeepEqual(want, got) {
			t.Errorf("ServiceURL %q renders as %q, which parses back naming %v -- the configuration names %v",
				row.url, rendered, keysOfSet(got), row.hosts)
		}
		// Stability is asserted from the PARSED configuration onward, not
		// from the typed one: a typed "pulsar://a,b:6650" names a port for
		// "b" only, so the first rendering is a normalisation rather than a
		// fixed point.
		again, err := pq.ParseDSN(back.DSN())
		if err != nil {
			t.Errorf("ServiceURL %q: the second rendering %q does not parse: %v", row.url, back.DSN(), err)
			continue
		}
		if again.DSN() != back.DSN() {
			t.Errorf("ServiceURL %q: rendering is not stable: %q then %q", row.url, back.DSN(), again.DSN())
		}
	}
}

// brokersOf is the set of "host:port" a Pulsar service URL names, split the
// way pulsar-client-go's own splitURI splits it (on "," and on ";").
func brokersOf(serviceURL string) map[string]bool {
	rest := serviceURL
	if i := strings.Index(rest, "://"); i >= 0 {
		rest = rest[i+3:]
	}
	if i := strings.IndexAny(rest, "/?#"); i >= 0 {
		rest = rest[:i]
	}
	out := map[string]bool{}
	for _, h := range strings.FieldsFunc(rest, func(r rune) bool { return r == ',' || r == ';' }) {
		out[h] = true
	}
	return out
}

// keysOfSet is a set's members, sorted, for a failure message.
func keysOfSet(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	slices.Sort(out)
	return out
}

// TestThePathEscaperEscapesWhatThePathDecodes pins an escaper the validator
// currently makes unreachable.
//
// Reverting escapePath to the identity fails no other test in this package,
// because every value that reaches the path position is validated to bytes
// that need no escaping. That is exactly the shape of a guard that has never
// been watched fail, so this watches it: the set is the bytes the shared
// parser gives a meaning to in a DSN's path.
func TestThePathEscaperEscapesWhatThePathDecodes(t *testing.T) {
	t.Parallel()
	for in, want := range map[string]string{
		"plain": "plain",
		"a%b":   "a%25b",
		"a?b":   "a%3Fb",
		"a#b":   "a%23b",
		"a%?#b": "a%25%3F%23b",
		// pulsar splits its path into tenant/namespace/prefix, so "/" is
		// decoded there too and must be escaped.
		"a/b":    "a%2Fb",
		"a-b_c.": "a-b_c.",
	} {
		if got := pq.EscapePathForTest(in); got != want {
			t.Errorf("EscapePathForTest(%q) = %q, want %q", in, got, want)
		}
	}
}
