package nsq_test

import (
	"fmt"
	"reflect"
	"strings"
	"testing"

	nq "bitbucket.org/iqhive/iqmq/drivers/nsq"
)

// settingsDiff names every field in which two configurations differ once both
// are resolved -- the form [nq.Config.DSN] renders from -- with the credential
// cleared, because the rendering redacts it on purpose.
//
// It compares FIELDS. `%+v` of a Config is its String, which is DSN, so a
// comparison written that way compares a rendering with a rendering and cannot
// fail when the rendering is what lost a setting. TLS is
// compared as "was TLS asked for", because *tls.Config carries a mutex and
// unexported state no round trip reproduces and the DSN carries only the
// request.
func settingsDiff(a, b nq.Config) []string {
	a, b = nq.ResolvedForTest(a), nq.ResolvedForTest(b)
	a.AuthSecret, b.AuthSecret = "", ""
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

// renderPositions are the free-text positions of an NSQ DSN, each with an X
// where the bytes under test go.
func renderPositions() []string {
	// An NSQ DSN carries no credential in its authority -- the auth secret is
	// a parameter -- so there is no user position here.
	params := []string{"clientid", "hostname", "useragent", "lookupd", "admin"}
	positions := make([]string, 0, 5+len(params))
	positions = append(positions,
		"nsq://aXb:4150",
		"nsq://Xb:4150",
		"nsq://aXb",
		"nsq://h:4150,aXb:4151",
		"nsq://h:4150/aXb",
	)
	for _, k := range params {
		positions = append(positions, "nsq://h:4150?"+k+"=aXb")
	}
	return positions
}

// TestTheRenderingParsesBack holds [nq.Config.DSN] to the one thing it exists
// for: a rendering an operator pastes back is the configuration it came from.
//
// Every printable ASCII byte that is not a letter or a digit, and one that is
// not UTF-8, goes into every free-text position -- an nsqd, a second nsqd,
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
		"nsq://d1:4150,d2:4151/acme.?lookupd=http%3A%2F%2Fl1%3A4161,http%3A%2F%2Fl2%3A4161" +
			"&admin=d1%3A4151&maxmessagebytes=131072&maxbodybytes=1048576" +
			"&maxrequeuedelay=31m&ackdeadline=61s&maxackdeadline=13m&leaseguard=3s&dialtimeout=7s" +
			"&readtimeout=61s&writetimeout=2s&heartbeatinterval=29s&lookupdpollinterval=17s" +
			"&outputbuffertimeout=101ms&apitimeout=11s&clientid=svc&hostname=host-1&useragent=iqmq%2F1" +
			"&authsecret=s3cret&metadata=portableenvelope&ensuretopology=true&deflate=true&deflatelevel=5" +
			"&tls=true&insecureskipverify=true",
		"nsq://h:4150?snappy=true&metadata=adaptive",
	} {
		if !roundTrip(t, in) {
			_, err := nq.ParseDSN(in)
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
		"maxmessagebytes", "maxbodybytes", "deflatelevel",
	} {
		for _, v := range []string{"0", "-1", "9"} {
			if roundTrip(t, "nsq://h:4150?"+k+"="+v) {
				accepted++
			}
		}
	}
	for _, k := range []string{
		"maxrequeuedelay", "ackdeadline", "maxackdeadline", "leaseguard", "dialtimeout",
		"readtimeout", "writetimeout", "heartbeatinterval", "lookupdpollinterval",
		"outputbuffertimeout", "apitimeout",
	} {
		for _, v := range []string{"0s", "-1s", "1h"} {
			if roundTrip(t, "nsq://h:4150?"+k+"="+v) {
				accepted++
			}
		}
	}
	if accepted < 10 {
		t.Fatalf("only %d numeric inputs parsed, so this test tests little", accepted)
	}
}

// TestTheByteBoundsReconcileOneWay pins the two byte
// bounds mirror nsqd FLAGS, so the reconciliation between them is
// one-directional where the is symmetric.
//
// Round 2 made them symmetric, and that silently raised MaxBodyBytes past the
// `--max-body-size` default the constant mirrors:
// "nsq://h:4150?maxmessagebytes=8388608" resolved to body=8388608, so this
// driver considered MPUBs legal that nsqd answers with E_BAD_BODY. Pulling
// the message limit DOWN to an explicit body is safe -- it makes the driver
// stricter than the broker; pushing the body UP is a claim about a broker
// setting the driver cannot change, and is refused instead.
func TestTheByteBoundsReconcileOneWay(t *testing.T) {
	t.Parallel()
	// An explicit message limit above the EFFECTIVE body limit is refused,
	// whether the body was written or defaulted, and the error names the
	// field the operator also has to raise.
	for _, in := range []string{
		"nsq://h:4150?maxmessagebytes=8388608",
		"nsq://h:4150?maxmessagebytes=5242881",
		"nsq://h:4150?maxmessagebytes=8388608&maxbodybytes=1048576",
	} {
		_, err := nq.ParseDSN(in)
		if err == nil {
			t.Errorf("ParseDSN(%q) was accepted: it resolves to a body limit above nsqd's own --max-body-size, "+
				"so every MPUB this driver then considers legal comes back E_BAD_BODY", in)
			continue
		}
		if !strings.Contains(err.Error(), "MaxBodyBytes") {
			t.Errorf("ParseDSN(%q) = %v; the refusal should name MaxBodyBytes, which is what must be raised too", in, err)
		}
	}
	// The other direction clamps rather than refusing, and the resolved pair
	// is self-consistent so the rendering still parses back.
	for _, row := range []struct {
		in       string
		msg, bod int64
	}{
		{"nsq://h:4150?maxbodybytes=524288", 524288, 524288},
		{"nsq://h:4150?maxmessagebytes=8388608&maxbodybytes=16777216", 8388608, 16777216},
		{"nsq://h:4150?maxmessagebytes=1048576", 1048576, 5242880},
		{"nsq://h:4150", 1048576, 5242880},
	} {
		cfg, err := nq.ParseDSN(row.in)
		if err != nil {
			t.Errorf("ParseDSN(%q): %v", row.in, err)
			continue
		}
		r := nq.ResolvedForTest(cfg)
		if r.MaxMessageBytes != row.msg || r.MaxBodyBytes != row.bod {
			t.Errorf("ParseDSN(%q) resolves to msg=%d body=%d, want msg=%d body=%d",
				row.in, r.MaxMessageBytes, r.MaxBodyBytes, row.msg, row.bod)
		}
		if r.MaxBodyBytes < r.MaxMessageBytes {
			t.Errorf("ParseDSN(%q) resolves to a body smaller than one message (%d < %d)",
				row.in, r.MaxBodyBytes, r.MaxMessageBytes)
		}
		if !roundTrip(t, row.in) {
			t.Errorf("ParseDSN(%q): the row's own input does not parse", row.in)
		}
	}
}
