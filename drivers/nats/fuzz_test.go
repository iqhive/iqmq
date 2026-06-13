package nats_test

import (
	"bytes"
	"strings"
	"testing"
	"unicode/utf8"

	natsgo "github.com/nats-io/nats.go"

	mq "bitbucket.org/iqhive/iqmq"
	nq "bitbucket.org/iqhive/iqmq/drivers/nats"
	"bitbucket.org/iqhive/iqmq/dsn"
)

// FuzzNATSSubjectMapping holds the topic-to-subject mapping to the properties
// that make it safe to put on a shared connection.
//
// The mapping is the one place a caller's bytes decide what goes into the NATS
// protocol, and the protocol is line-oriented: a subject carrying a space, a
// tab, a CR or an LF does not produce a rejected publication, it produces a
// malformed frame on a connection every other publisher and subscriber in the
// process is sharing. The second property is the one a reviewer would not think
// of: a mapping that let a wildcard through would turn a publication into a
// publication to a pattern and a subscription into a subscription to everything
// under it.
func FuzzNATSSubjectMapping(f *testing.F) {
	for _, seed := range [][2]string{
		{"", "orders"},
		{"", "orders/created"},
		{"", "orders.created"},
		{"acme", "orders/created"},
		{"", "orders.*"},
		{"", "orders.>"},
		{"", ""},
		{"", "."},
		{"", "a..b"},
		{"", "a b"},
		{"", "a\nb"},
		{"", "a\x00b"},
		{"", "_INBOX.abc"},
		{"$SYS", "a"},
		{"", strings.Repeat("a/", 600)},
	} {
		f.Add(seed[0], seed[1])
	}
	f.Fuzz(func(t *testing.T, prefix, topic string) {
		subject, err := nq.Subject(prefix, topic)
		pattern, perr := nq.Pattern(prefix, topic)

		// Whatever comes out is a subject this driver would accept again:
		// a mapping whose own output it would refuse is a mapping that has
		// produced something nothing here has checked.
		if err == nil {
			if cerr := nq.CheckSubject(subject, false); cerr != nil {
				t.Fatalf("Subject(%q, %q) = %q, which CheckSubject refuses: %v", prefix, topic, subject, cerr)
			}
			if len(subject) > nq.MaxSubjectLen {
				t.Fatalf("Subject(%q, %q) is %d bytes, past the %d bound", prefix, topic, len(subject), nq.MaxSubjectLen)
			}
			// The protocol is line-oriented and these four bytes are what
			// break a frame rather than a publication.
			if strings.ContainsAny(subject, " \t\r\n\x00") {
				t.Fatalf("Subject(%q, %q) = %q carries a byte that breaks the NATS protocol", prefix, topic, subject)
			}
			// A publication subject is never a pattern.
			for _, tok := range strings.Split(subject, ".") {
				if tok == "*" || tok == ">" {
					t.Fatalf("Subject(%q, %q) = %q is a pattern: a publication would reach a subject nobody named", prefix, topic, subject)
				}
			}
			// Every publishable subject is also subscribable.
			if perr != nil {
				t.Fatalf("Subject accepted (%q, %q) and Pattern refused it: %v", prefix, topic, perr)
			}
			if pattern != subject {
				t.Fatalf("Subject = %q and Pattern = %q for the same input", subject, pattern)
			}
		}
		if perr == nil {
			if cerr := nq.CheckSubject(pattern, true); cerr != nil {
				t.Fatalf("Pattern(%q, %q) = %q, which CheckSubject refuses: %v", prefix, topic, pattern, cerr)
			}
			if strings.ContainsAny(pattern, " \t\r\n\x00") {
				t.Fatalf("Pattern(%q, %q) = %q carries a byte that breaks the NATS protocol", prefix, topic, pattern)
			}
			// ">" matches the rest of a subject, so it is only ever last.
			if i := strings.Index(pattern, ">"); i >= 0 && i != len(pattern)-1 {
				t.Fatalf("Pattern(%q, %q) = %q puts > somewhere other than the end", prefix, topic, pattern)
			}
		}

		// The mapping is a function: the same input maps the same way every
		// time, which a reverse that strips a prefix depends on.
		again, aerr := nq.Subject(prefix, topic)
		if (err == nil) != (aerr == nil) || again != subject {
			t.Fatalf("Subject(%q, %q) is not deterministic: %q/%v then %q/%v", prefix, topic, subject, err, again, aerr)
		}
		if err == nil {
			// TopicOf undoes the prefix exactly and nothing else: the
			// separator swap is not reversed, because the forward mapping
			// is not injective. Stripping "more than
			// the prefix" is the failure this catches -- a prefix of "0"
			// in front of a topic that itself begins "0." had the second
			// token eaten by an earlier, sloppier form of this check.
			want := strings.ReplaceAll(topic, "/", ".")
			if back := nq.TopicOf(prefix, subject); back != want {
				t.Fatalf("TopicOf(%q, %q) = %q, want %q", prefix, subject, back, want)
			}
		}

		// A failure is always a portable refusal and never a bare error:
		// a caller decides what to do from mq.CodeOf.
		if err != nil && mq.CodeOf(err) != mq.CodeInvalidArgument {
			t.Fatalf("Subject(%q, %q) failed with code %v, want InvalidArgument", prefix, topic, mq.CodeOf(err))
		}
	})
}

// FuzzNATSHeaders holds the metadata mapping to the property that matters most
// on a text protocol: everything [nq.Representable] accepts survives the header
// framing byte for byte, and everything it refuses really would not.
//
// The framing check is done with the client library's own decoder over the
// bytes this driver would put on the wire, so the test agrees with nats.go
// rather than with a second opinion about what NATS headers are. A value that
// forged a second header, or a name truncated at a colon, shows up here as a
// round trip that changed the message rather than as a protocol error nobody
// sees until production.
func FuzzNATSHeaders(f *testing.F) {
	for _, seed := range []struct {
		key      string
		val, dup []byte
		id, ct   string
		body     []byte
	}{
		{key: "content-kind", val: []byte("order"), dup: []byte("two"), id: "msg-1", ct: "application/json", body: []byte("payload")},
		{key: "x", val: []byte(""), dup: nil, id: "", ct: "", body: nil},
		{key: "", val: []byte("v"), body: []byte{}},
		{key: "a:b", val: []byte("v"), body: []byte("x")},
		{key: "a b", val: []byte("v"), body: []byte("x")},
		{key: "k", val: []byte("a\r\nInjected: yes"), body: []byte("x")},
		{key: "k", val: []byte(" leading"), body: []byte("x")},
		{key: "k", val: []byte{0x00, 0xff}, body: []byte("x")},
		{key: "k", val: []byte("héllo"), dup: []byte("wörld"), body: []byte("x")},
		{key: "iqmq-key", val: []byte("stolen"), body: []byte("x")},
	} {
		f.Add(seed.key, seed.val, seed.dup, seed.id, seed.ct, seed.body)
	}
	f.Fuzz(func(t *testing.T, key string, val, dup []byte, id, contentType string, body []byte) {
		m := &mq.Message{Body: body, ID: id}
		m.Properties.ContentType = contentType
		if key != "" {
			m.Headers.Add(key, val)
			if dup != nil {
				m.Headers.Add(key, dup)
			}
		}
		representable := nq.Representable(m) == nil

		var msg natsgo.Msg
		msg.Subject = "fuzz"
		nq.Encode(&msg, m, nil)

		// Whatever was encoded has to survive the framing the server and
		// the client really use.
		if err := checkFraming(msg.Header); err != nil {
			t.Fatalf("Encode produced a header block that does not round-trip through nats.go: %v", err)
		}

		var got mq.Message
		nq.Decode(&got, &msg, "")

		if !representable {
			// Strict mode would have refused this message, so nothing is
			// promised about what a lenient encode produced — only that it
			// framed correctly, which is checked above.
			return
		}
		if (got.Body == nil) != (body == nil) || !bytes.Equal(got.Body, body) {
			t.Fatalf("body %q (nil %v) came back as %q (nil %v)", body, body == nil, got.Body, got.Body == nil)
		}
		if got.ID != id {
			t.Fatalf("id %q came back as %q", id, got.ID)
		}
		if got.Properties.ContentType != contentType {
			t.Fatalf("content type %q came back as %q", contentType, got.Properties.ContentType)
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

// checkFraming serialises a header set the way nats.go does and decodes it with
// nats.go's own decoder, reporting any difference.
//
// This is the check that catches an injected header: a value carrying CRLF
// produces a block that decodes into MORE headers than went in, and one
// carrying a leading space produces a value shorter than it went out.
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
		return errFraming("the block decoded into " + itoa(len(back)) + " names, want " + itoa(len(h)))
	}
	for k, vals := range h {
		got := back[k]
		if len(got) != len(vals) {
			return errFraming("name " + k + " decoded into " + itoa(len(got)) + " values, want " + itoa(len(vals)))
		}
		for i := range vals {
			if got[i] != vals[i] {
				return errFraming("name " + k + " value " + itoa(i) + " decoded as " + got[i] + ", want " + vals[i])
			}
		}
	}
	return nil
}

// errFraming is the framing failure, as a plain error.
type errFraming string

func (e errFraming) Error() string { return string(e) }

// itoa is strconv.Itoa without the import, so that the framing check reads as
// one block.
func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[i:])
}

// FuzzNATSDSN holds the DSN parser to the rules for a parser fed by bytes a
// stranger may have set: pure, bounded, total, and never a silently ignored
// parameter.
func FuzzNATSDSN(f *testing.F) {
	for _, seed := range []string{
		"nats://127.0.0.1:4222",
		"nats://user:pass@host:4222,host2:4222/acme?tls=true&noecho=true",
		"nats://host?creds=/etc/nats.creds&pendingmessages=1024",
		"nats://host?unknown=1",
		"nats://host?tls=false&insecureskipverify=true",
		"memory://",
		"nats://",
		"",
		"nats://host/a b",
		strings.Repeat("nats://h?", 2000),
	} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, s string) {
		cfg, err := nq.ParseDSN(s)
		if err != nil {
			if code := mq.CodeOf(err); code != mq.CodeInvalidConfig &&
				code != mq.CodeInvalidArgument && code != mq.CodeDriverNotRegistered {
				t.Fatalf("ParseDSN(%q) failed with code %v", s, code)
			}
			return
		}
		if verr := cfg.Validate(); verr != nil {
			t.Fatalf("ParseDSN(%q) produced a configuration Validate refuses: %v", s, verr)
		}
		// The rendering never carries the credential, whatever the DSN put
		// in it.
		rendered := cfg.DSN()
		if !utf8.ValidString(rendered) {
			t.Fatalf("ParseDSN(%q).DSN() is not valid UTF-8", s)
		}
		// The credential position is what is checked, not a substring
		// search: a one-character password matches a port number, which is
		// how the first version of this property failed on
		// "nats://00:0@00" while the rendering was perfectly correct. The
		// second version still searched, for ":<pw>@", and fails the same way
		// on "nats://0:4222@0?name=:4222@", whose NAME holds ":4222@" (the
		// seed password-shaped-name; FuzzUnifiedDSN found it). The userinfo
		// and the `token` parameter are read out of the rendering's own
		// structure now.
		if pw := cfg.Password.Reveal(); pw != "" {
			if got, ok := renderedPassword(rendered); !ok || got != dsn.Redacted {
				t.Fatalf("ParseDSN(%q).DSN() = %q carries %q in the credential position, not the redaction", s, rendered, got)
			}
		}
		if tok := cfg.Token.Reveal(); tok != "" {
			if got, ok := renderedParam(rendered, "token"); !ok || got != dsn.Redacted {
				t.Fatalf("ParseDSN(%q).DSN() = %q carries %q as the token, not the redaction", s, rendered, got)
			}
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
