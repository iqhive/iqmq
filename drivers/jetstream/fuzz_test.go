package jetstream_test

import (
	"bytes"
	"strconv"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	natsgo "github.com/nats-io/nats.go"
	sjs "github.com/nats-io/nats.go/jetstream"

	mq "bitbucket.org/iqhive/iqmq"
	"bitbucket.org/iqhive/iqmq/driverkit"
	nq "bitbucket.org/iqhive/iqmq/drivers/jetstream"
	"bitbucket.org/iqhive/iqmq/dsn"
)

// FuzzJetStreamMetadata holds the metadata mapping to the properties that make
// it safe on a stream, which are the Core driver's two plus one this profile
// adds.
//
// The framing properties are the protocol's: a header block is "Name: value"
// per CRLF-terminated line, so a value carrying CRLF forges a second header and
// a value with a leading space comes back shorter than it went out. The third
// property is JetStream's own: the server OWNS the Nats- namespace —
// Nats-Msg-Id is de-duplication, Nats-TTL is expiry, Nats-Schedule is a
// schedule — so a caller header that reached it would change the broker's
// behaviour rather than travel with the message, and a server header decoded
// back as a caller header would be metadata the application never published.
func FuzzJetStreamMetadata(f *testing.F) {
	seeds := []struct {
		key         string
		val, dup    []byte
		id, ctype   string
		body        []byte
		dedup       string
		ttl         int64
		scheduleSec int64
	}{
		{key: "content-kind", val: []byte("order"), id: "id-1", ctype: "application/json", body: []byte("x")},
		{key: "trace-state", val: []byte{0x00, 0xff, 0xfe, 0x80}, dup: []byte{0x01}},
		{key: "Nats-Msg-Id", val: []byte("forged")},
		{key: "nats-ttl", val: []byte("1s")},
		{key: "NATS-Schedule", val: []byte("@at 2030-01-01T00:00:00Z")},
		{key: "a:b", val: []byte("colon-in-name")},
		{key: "k", val: []byte("a\r\nInjected: yes")},
		{key: "k", val: []byte(" leading-space")},
		{key: mq.ReservedHeaderPrefix + "key", val: []byte("reserved")},
		{key: "k", val: []byte("v"), dedup: "dedup-key", ttl: int64(time.Second), scheduleSec: 60},
		{key: "k", val: []byte("v"), dedup: "a\r\nInjected: yes"},
		{body: []byte{}},
		{},
	}
	for i := range seeds {
		seed := &seeds[i]
		f.Add(seed.key, seed.val, seed.dup, seed.id, seed.ctype, seed.body, seed.dedup, seed.ttl, seed.scheduleSec)
	}
	f.Fuzz(func(t *testing.T, key string, val, dup []byte, id, ctype string, body []byte, dedup string, ttl, scheduleSec int64) {
		m := &mq.Message{Body: body, ID: id}
		m.Properties.ContentType = ctype
		if key != "" {
			m.Headers.Add(key, val)
			if dup != nil {
				m.Headers.Add(key, dup)
			}
		}
		representable := nq.Representable(m) == nil

		var msg natsgo.Msg
		msg.Subject = "fuzz.subject"
		nq.Encode(&msg, m, nil)

		// Whatever was encoded has to survive the framing the server and the
		// client really use.
		if err := checkFraming(msg.Header); err != nil {
			t.Fatalf("Encode produced a header block that does not round-trip through nats.go: %v", err)
		}
		// Nothing a caller wrote may land in the server's namespace, whatever
		// the mode: a header there is read as a control.
		for k := range msg.Header {
			if nq.IsNATSHeader(k) {
				t.Fatalf("Encode put %q in the server's Nats- namespace from a caller header %q", k, key)
			}
		}

		// The publication controls travel as the server's own headers, and
		// they are added to the SAME block, so the framing has to hold with
		// them too.
		withControls := msg
		withControls.Header = cloneHeader(msg.Header)
		controls := applyControls(t, &withControls, dedup, ttl, scheduleSec)
		if controls {
			if err := checkFraming(withControls.Header); err != nil {
				t.Fatalf("the publication controls produced a block that does not round-trip: %v", err)
			}
		}

		var got mq.Message
		nq.Decode(&got, msg.Subject, msg.Reply, msg.Header, msg.Data, "")

		// A server header is never decoded back as a caller header, whatever
		// arrives on the wire.
		for k := range got.Headers.All() {
			if nq.IsNATSHeader(k) {
				t.Fatalf("Decode handed the caller the server's header %q", k)
			}
			// the five dead-letter provenance keys are the CORE's own and
			// are carried deliberately, so Decode hands them back on
			// purpose; nothing else under the prefix may reach a caller.
			if strings.HasPrefix(k, mq.ReservedHeaderPrefix) && !driverkit.ReservedHeaderAllowed(k) {
				t.Fatalf("Decode handed the caller the reserved header %q", k)
			}
		}

		if !representable {
			// Strict mode would have refused this message, so nothing is
			// promised about what a lenient encode produced — only that it
			// framed correctly and leaked nothing into the server's
			// namespace, both checked above.
			return
		}
		if (got.Body == nil) != (body == nil) || !bytes.Equal(got.Body, body) {
			t.Fatalf("body %q (nil %v) came back as %q (nil %v)", body, body == nil, got.Body, got.Body == nil)
		}
		if got.ID != id {
			t.Fatalf("id %q came back as %q", id, got.ID)
		}
		if got.Properties.ContentType != ctype {
			t.Fatalf("content type %q came back as %q", ctype, got.Properties.ContentType)
		}
		if key == "" {
			return
		}
		want := [][]byte{val}
		if dup != nil {
			want = append(want, dup)
		}
		vals := got.Headers.Values(key)
		if len(vals) != len(want) {
			t.Fatalf("header %q came back as %d values, want %d", key, len(vals), len(want))
		}
		for i := range want {
			if !bytes.Equal(vals[i], want[i]) {
				t.Fatalf("header %q value %d came back as %q, want %q", key, i, vals[i], want[i])
			}
		}
	})
}

// applyControls writes the per-call publication controls onto a message the way
// the sink does, and reports whether any were written.
//
// It duplicates three lines of sink.controls deliberately: the sink's own method
// is unexported and the property being fuzzed is about the HEADERS, which are
// the exported contract. A change to the header names the sink writes breaks
// TestScheduledPublicationFiresLater and TestDeduplicationDiscardsARepeat, which
// go through the real path against a real server.
func applyControls(t *testing.T, msg *natsgo.Msg, dedup string, ttl, scheduleSec int64) bool {
	t.Helper()
	wrote := false
	if dedup != "" && nq.HeaderValueOK([]byte(dedup)) {
		if msg.Header == nil {
			msg.Header = natsgo.Header{}
		}
		msg.Header.Set(sjs.MsgIDHeader, dedup)
		wrote = true
	}
	if ttl > 0 {
		if msg.Header == nil {
			msg.Header = natsgo.Header{}
		}
		msg.Header.Set(sjs.MsgTTLHeader, time.Duration(ttl).String())
		wrote = true
	}
	if scheduleSec > 0 && scheduleSec < 1<<40 {
		if msg.Header == nil {
			msg.Header = natsgo.Header{}
		}
		at := time.Unix(scheduleSec, 0).UTC()
		msg.Header.Set(sjs.ScheduleHeader, "@at "+at.Format(time.RFC3339))
		msg.Header.Set(sjs.ScheduleTargetHeader, msg.Subject)
		wrote = true
	}
	return wrote
}

// cloneHeader copies a header block, so that the control-header check does not
// change the block the round-trip check reads.
func cloneHeader(h natsgo.Header) natsgo.Header {
	if h == nil {
		return nil
	}
	out := make(natsgo.Header, len(h))
	for k, vals := range h {
		out[k] = append([]string(nil), vals...)
	}
	return out
}

// FuzzJetStreamSubjectMapping holds the topic-to-subject mapping and the durable
// naming to the properties that make them safe to put on a shared connection and
// into a JetStream API call.
//
// The mapping is the one place a caller's bytes decide what goes into the NATS
// protocol, and the protocol is line-oriented: a subject carrying a space, a
// tab, a CR or an LF does not produce a rejected publication, it produces a
// malformed frame on a connection every other publisher and subscriber in the
// process is sharing. The durable name is the same question one layer up: the
// server refuses a name carrying ".", "*", ">" or whitespace, and a driver that
// built one would fail at the API call rather than at the configuration.
func FuzzJetStreamSubjectMapping(f *testing.F) {
	for _, seed := range [3]string{"", "orders", "acme"} {
		f.Add(seed, "orders/created", "workers")
	}
	for _, seed := range []string{
		"orders.*", "orders.>", "", ".", "a..b", "a b", "a\nb", "a\x00b",
		"_INBOX.abc", "$KV.bucket", strings.Repeat("a/", 600),
	} {
		f.Add("", seed, "workers")
	}
	f.Add("", "orders", "a.b c/d>e*f")
	f.Add("", "orders", strings.Repeat("s", 300))
	f.Fuzz(func(t *testing.T, prefix, topic, subscription string) {
		subject, err := nq.Subject(prefix, topic)
		pattern, perr := nq.Pattern(prefix, topic)

		// Whatever comes out is a subject this driver would accept again: a
		// mapping whose own output it would refuse has produced something
		// nothing here has checked.
		if err == nil {
			if cerr := nq.CheckSubject(subject, false); cerr != nil {
				t.Fatalf("Subject(%q, %q) = %q, which CheckSubject refuses: %v", prefix, topic, subject, cerr)
			}
			if len(subject) > nq.MaxSubjectLen {
				t.Fatalf("Subject(%q, %q) is %d bytes, past the %d bound", prefix, topic, len(subject), nq.MaxSubjectLen)
			}
			for i := range len(subject) {
				if c := subject[i]; c <= ' ' || c >= 0x7f || c == '"' {
					t.Fatalf("Subject(%q, %q) = %q carries byte %#x, which the protocol cannot frame", prefix, topic, subject, c)
				}
			}
			// A publication's subject is never a pattern: a topic that
			// quietly became one would publish to everything under it.
			for tok := range strings.SplitSeq(subject, ".") {
				if tok == "*" || tok == ">" {
					t.Fatalf("Subject(%q, %q) = %q became a pattern", prefix, topic, subject)
				}
			}
			// The reverse names the same thing, stripped of the prefix.
			if back := nq.TopicOf(prefix, subject); prefix != "" && back == subject && strings.HasPrefix(subject, prefix+".") {
				t.Fatalf("TopicOf(%q, %q) did not strip the prefix", prefix, subject)
			}
		}
		if perr == nil {
			if cerr := nq.CheckSubject(pattern, true); cerr != nil {
				t.Fatalf("Pattern(%q, %q) = %q, which CheckSubject refuses: %v", prefix, topic, pattern, cerr)
			}
			// A ">" is only ever the last token: one in the middle matches
			// nothing while reading as though it matched everything.
			if i := strings.Index(pattern, ">"); i >= 0 && i != len(pattern)-1 {
				t.Fatalf("Pattern(%q, %q) = %q carries a \">\" that is not the last token", prefix, topic, pattern)
			}
		}
		// Anything Subject accepts, Pattern accepts: a subscription may name
		// everything a publication may, plus the wildcards.
		if err == nil && perr != nil {
			t.Fatalf("Subject accepted %q/%q and Pattern refused it: %v", prefix, topic, perr)
		}

		durable, derr := nq.Durable(nq.DefaultConsumerPrefix, subscription, pattern)
		if derr == nil {
			if cerr := nq.CheckName(durable); cerr != nil {
				t.Fatalf("Durable(%q, %q) = %q, which JetStream refuses: %v", subscription, pattern, durable, cerr)
			}
			if len(durable) > nq.MaxNameLen {
				t.Fatalf("Durable(%q, %q) is %d bytes, past the %d bound", subscription, pattern, len(durable), nq.MaxNameLen)
			}
			// The same subscription and filter always name the same durable,
			// or a durable subscription would lose its position on every
			// reopen.
			again, _ := nq.Durable(nq.DefaultConsumerPrefix, subscription, pattern)
			if again != durable {
				t.Fatalf("Durable is not a function: %q then %q", durable, again)
			}
		}
		// The schedule tree is reachable from a pattern or it is not, and the
		// answer never panics on any input.
		if perr == nil {
			_ = nq.PatternReachesSchedules(pattern, nq.DefaultScheduleSubject+".S")
		}
	})
}

// FuzzJetStreamDSN holds the parser's post-conditions: a DSN it accepted is a
// configuration Validate accepts and renders without its credential, and a DSN
// it refused names the parameter it refused.
func FuzzJetStreamDSN(f *testing.F) {
	for _, seed := range []string{
		"natsjs://host:4222",
		"natsjs://user:pass@host:4222/acme?stream=ORDERS&managestream=true",
		"natsjs://host:4222?storage=memory&replicas=3&doubleack=true",
		"natsjs://host:4222?streamsubjects=a.%3E%3Bb.%3E",
		"natsjs://host:4222?nonsense=1",
		"natsjs://host:4222?replicas=0",
		"nats://host:4222",
		"natsjs://\xc5",
		"natsjs://host:4222?creds=/p\x9ath",
		"://",
		"",
	} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, in string) {
		cfg, err := nq.ParseDSN(in)
		if err != nil {
			// A refusal must be renderable: an error carrying a raw byte
			// sequence is an error no log pipeline can carry.
			if !utf8.ValidString(err.Error()) {
				t.Fatalf("the refusal of %q is not valid UTF-8", in)
			}
			return
		}
		if verr := cfg.Validate(); verr != nil {
			t.Fatalf("ParseDSN accepted %q and Validate refuses the result: %v", in, verr)
		}
		rendered := cfg.DSN()
		if !utf8.ValidString(rendered) {
			t.Fatalf("the configuration from %q does not render as valid UTF-8", in)
		}
		// The credential POSITION is what is checked, not a substring
		// search: a one-character password matches a port number, which is
		// how the first version of this property failed on
		// "natsjs://00:0@0" while the rendering was perfectly correct. The specification
		// found the same trap on the Core driver's DSN. The second version
		// still searched, for ":<pw>@", and fails the same way on
		// "natsjs://0:4222@0?name=:4222@", whose NAME holds ":4222@" (the
		// seed password-shaped-name; FuzzUnifiedDSN found it). The userinfo
		// and the `token` parameter are read out of the rendering's own
		// structure now.
		if pw := cfg.Password.Reveal(); pw != "" {
			if got, ok := renderedPassword(rendered); !ok || got != dsn.Redacted {
				t.Fatalf("the rendering of %q carries %q in the credential position, not the redaction: %s", in, got, rendered)
			}
		}
		if tok := cfg.Token.Reveal(); tok != "" {
			if got, ok := renderedParam(rendered, "token"); !ok || got != dsn.Redacted {
				t.Fatalf("the rendering of %q carries %q as the token, not the redaction: %s", in, got, rendered)
			}
		}
		// The rendering parses, which is what makes it a usable diagnostic
		// rather than a string that merely looks like one -- and it parses
		// to the same settings, compared field by field, because a rendering
		// that dropped one still parses.
		//
		// A rendering past dsn.MaxLen is exempt, as FuzzDSNParse's and
		// FuzzUnifiedDSN's are: this driver writes every default out, about
		// three hundred bytes, so an input within that of the bound renders
		// past it, and that is the bound doing its job.
		if len(rendered) > dsn.MaxLen {
			return
		}
		back, rerr := nq.ParseDSN(rendered)
		if rerr != nil {
			t.Fatalf("the rendering of %q does not parse: %v (%s)", in, rerr, rendered)
		}
		if diffs := settingsDiff(cfg, back); len(diffs) > 0 {
			t.Fatalf("the rendering of %q parses back to different settings: %s (%s)", in, strings.Join(diffs, "; "), rendered)
		}
	})
}

// renderedPassword is what a rendering carries in the credential position --
// the authority's userinfo after its first colon -- read from the rendering's
// structure rather than searched for, and whether it carries one at all.
func renderedPassword(rendered string) (string, bool) {
	_, rest, ok := strings.Cut(rendered, "://")
	if !ok {
		return "", false
	}
	if i := strings.IndexAny(rest, "/?#"); i >= 0 {
		rest = rest[:i]
	}
	at := strings.LastIndexByte(rest, '@')
	if at < 0 {
		return "", false
	}
	_, pw, ok := strings.Cut(rest[:at], ":")
	return pw, ok
}

// renderedParam is the raw value a rendering carries for one query
// parameter, and whether it carries the parameter at all.
func renderedParam(rendered, key string) (string, bool) {
	_, query, ok := strings.Cut(rendered, "?")
	if !ok {
		return "", false
	}
	for _, pair := range strings.Split(query, "&") {
		if k, v, _ := strings.Cut(pair, "="); k == key {
			return v, true
		}
	}
	return "", false
}

// checkFraming serialises a header set the way nats.go does and decodes it with
// nats.go's own decoder, reporting any difference.
//
// This is the check that catches an injected header: a value carrying CRLF
// produces a block that decodes into MORE headers than went in, and one carrying
// a leading space produces a value shorter than it went out.
func checkFraming(h natsgo.Header) error {
	if len(h) == 0 {
		return nil
	}
	var b strings.Builder
	b.WriteString("NATS/1.0\r\n")
	for k, vals := range h {
		for _, v := range vals {
			b.WriteString(k)
			b.WriteString(": ")
			b.WriteString(v)
			b.WriteString("\r\n")
		}
	}
	b.WriteString("\r\n")
	back, err := natsgo.DecodeHeadersMsg([]byte(b.String()))
	if err != nil {
		return errFraming("the block does not decode: " + err.Error())
	}
	if len(back) != len(h) {
		return errFraming("the block decoded into " + strconv.Itoa(len(back)) + " names, want " + strconv.Itoa(len(h)))
	}
	for k, vals := range h {
		got := back[k]
		if len(got) != len(vals) {
			return errFraming("name " + k + " decoded into " + strconv.Itoa(len(got)) + " values, want " + strconv.Itoa(len(vals)))
		}
		for i := range vals {
			if got[i] != vals[i] {
				return errFraming("name " + k + " value " + strconv.Itoa(i) + " decoded as " + got[i] + ", want " + vals[i])
			}
		}
	}
	return nil
}

// errFraming is the framing failure, as a plain error.
type errFraming string

func (e errFraming) Error() string { return string(e) }
