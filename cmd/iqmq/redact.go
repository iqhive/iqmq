package main

import (
	"cmp"
	"encoding/json"
	"io"
	"net/url"
	"slices"
	"strings"

	"github.com/iqhive/iqmq/dsn"
)

// Redaction is two layers, and the reason there are two is that neither one is
// enough on its own (the security checks).
//
// The structural layer is that nothing in this package renders a DSN query
// parameter or a driver configuration field itself. A target is rendered by
// [redactTarget], which drops the password AND every parameter; a resolved
// configuration is rendered by the driver's own String, which the specification makes the
// configuration's obligation and which every shipped driver proves in its own
// tests. A CLI that reimplemented either rendering would be a second,
// unproved redaction of the same secret.
//
// The scrubbing layer is [scrubWriter], which holds every credential the CLI
// can identify and replaces it in the byte stream on its way to the terminal.
// It exists because the structural layer is a property of THIS code and the
// output also carries a driver's error strings, a native client's message and
// a broker's own reply — none of which this package wrote. A credential that
// reaches the writer through one of those is still not printed.
//
// Four places a credential can appear, and what catches each:
//
//  1. the DSN's userinfo      -- redactTarget, and the scrub of the password
//  2. a DSN query parameter   -- redactTarget drops parameters; the scrub
//     holds the value of every credential-named one
//  3. an environment variable -- the environment is reported by NAME and
//     set/unset only, never by value, and the scrub holds the values of the
//     credential-named variables
//  4. a configuration field   -- the driver's own String, behind the scrub

// isCredentialName reports whether a parameter or variable of this name
// carries a credential.
//
// The list itself lives in the dsn package, because the specification made
// [dsn.Config.String] redact the parameters it names and two copies of a list
// like this is the drift defect rather than a redundancy: the renderer and the
// scrubber have to agree about which name is a secret, and now they cannot
// disagree. It is still a substring match and not an exact one, so a driver
// that adds "apisecret" or "producerpassword" tomorrow is covered without any
// list changing.
func isCredentialName(name string) bool { return dsn.IsCredentialName(name) }

// redactTarget renders what a DSN names -- scheme, user, endpoints, path --
// and nothing else.
//
// Every query parameter is dropped rather than rendered redacted one by one,
// because the CLI cannot know which of a driver's parameters is a credential
// and a list it maintained would be wrong the moment a driver added one.
// [dsn.Config.String] round-trips the parameters, which is right for a
// configuration error naming a parameter and wrong for a report printed to a
// terminal: it renders "?token=s3cr3t" verbatim.
//
// A DSN that does not parse renders as its scheme alone, or as a fixed
// placeholder when even the scheme is unreadable. It is never echoed: an
// unparsable DSN is exactly the case where the bytes are least understood and
// most likely to be somebody's whole credential pasted into the wrong
// argument.
func redactTarget(raw string) string {
	cfg, err := dsn.Parse(raw)
	if err != nil {
		if scheme, serr := dsn.Scheme(raw); serr == nil {
			return scheme + "://<unparsable>"
		}
		return "<unparsable DSN>"
	}
	var b strings.Builder
	b.WriteString(cfg.Scheme)
	b.WriteString("://")
	if cfg.Username != "" || cfg.HasPassword {
		b.WriteString(cfg.Username)
		if cfg.HasPassword {
			b.WriteByte(':')
			b.WriteString(dsn.Redacted)
		}
		b.WriteByte('@')
	}
	for i := range cfg.Hosts {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(cfg.Hosts[i].String())
	}
	if cfg.Path != "" {
		b.WriteByte('/')
		b.WriteString(cfg.Path)
	}
	if n := cfg.Params.Len(); n > 0 {
		b.WriteString("?<")
		b.WriteString(itoa(n))
		switch n {
		case 1:
			b.WriteString(" parameter, not shown>")
		default:
			b.WriteString(" parameters, not shown>")
		}
	}
	return b.String()
}

// paramNames returns the names of a DSN's parameters, with no values, so a
// report can say which settings were given without saying what they were.
func paramNames(raw string) []string {
	cfg, err := dsn.Parse(raw)
	if err != nil {
		return nil
	}
	out := make([]string, 0, cfg.Params.Len())
	for k := range cfg.Params.All() {
		if isCredentialName(k) {
			out = append(out, k+"=<"+dsn.Redacted+">")
			continue
		}
		out = append(out, k)
	}
	return out
}

// secretsOf collects every credential a DSN carries, for the scrubbing layer.
//
// It reads the userinfo password and the value of every credential-named
// parameter. It does NOT collect every parameter value: scrubbing "us-east-1"
// out of the whole report because a parameter happened to carry it would
// mangle every line that mentions a region, and the structural layer is what
// keeps a non-credential parameter out of the output in the first place.
//
// Each credential is held decoded AND percent-encoded: as the DSN spells it,
// and as net/url would re-encode it. A native client that echoes its
// connection string prints the encoded form, and "p%40ss" is not "p@ss".
func secretsOf(raw string) []dsn.Secret {
	cfg, err := dsn.Parse(raw)
	if err != nil {
		return nil
	}
	out := make([]dsn.Secret, 0, cfg.Params.Len()+1)
	if cfg.Password != "" {
		out = append(out, cfg.Password)
		out = append(out, encodedForms(raw, cfg.Password)...)
	}
	for k, v := range cfg.Params.All() {
		if v != "" && isCredentialName(k) {
			out = append(out, dsn.Secret(v))
			out = append(out, encodedForms(raw, dsn.Secret(v))...)
		}
	}
	return out
}

// encodedForms returns the spellings of sec other than sec itself: every
// substring of raw that percent-decodes to it, in whatever hex case the DSN
// used, and the forms url.PathEscape and url.QueryEscape give.
func encodedForms(raw string, sec dsn.Secret) []dsn.Secret {
	plain := sec.Reveal()
	var out []dsn.Secret
	add := func(form string) {
		if form != plain && !slices.Contains(out, dsn.Secret(form)) {
			out = append(out, dsn.Secret(form))
		}
	}
	add(url.PathEscape(plain))
	add(url.QueryEscape(plain))
	for i := range len(raw) {
		if n := decodedPrefix(raw[i:], plain); n > 0 {
			add(raw[i : i+n])
		}
	}
	return out
}

// decodedPrefix returns the length of the prefix of raw that percent-decodes
// to plain, reading '+' as either itself or a space, or 0 when none does.
func decodedPrefix(raw, plain string) int {
	j := 0
	for k := range len(plain) {
		if j >= len(raw) {
			return 0
		}
		c, w := raw[j], 1
		if c == '%' && j+2 < len(raw) && isHex(raw[j+1]) && isHex(raw[j+2]) {
			c, w = unhex(raw[j+1])<<4|unhex(raw[j+2]), 3
		}
		if c == '+' && w == 1 && plain[k] == ' ' {
			c = ' '
		}
		if c != plain[k] {
			return 0
		}
		j += w
	}
	return j
}

func isHex(c byte) bool {
	return c >= '0' && c <= '9' || c >= 'a' && c <= 'f' || c >= 'A' && c <= 'F'
}

func unhex(c byte) byte {
	switch {
	case c >= 'a':
		return c - 'a' + 10
	case c >= 'A':
		return c - 'A' + 10
	}
	return c - '0'
}

// scrubWriter replaces every secret it holds with the redaction on the way to
// the underlying writer.
//
// It writes whole records: Write is called once per line or once per report by
// everything in this package, and a secret split across two Writes would
// otherwise pass through. That is a real constraint rather than a hope, and
// TestScrubWriterHoldsASplitSecret pins it: the writer buffers a tail as long
// as the longest secret it holds and emits it only when the next Write, or
// Flush, makes it unambiguous.
//
// Every byte of every occurrence is redacted, whichever secret it belongs to,
// and occurrences that overlap are one redaction. Replacing the secrets one at
// a time in the order they were added let a password that is part of a longer
// token go first, and the rest of the token reached the output. [dsn.Scrub]
// redacts the same way; the writer does not call it because it needs the
// places it redacted, to know which bytes it may emit and to count them.
type scrubWriter struct {
	w       io.Writer
	secrets []dsn.Secret
	// tail is the bytes held back, unredacted, because an occurrence the
	// next Write completes could still begin in them. It is maxLen-1 bytes
	// long, or longer while an occurrence that reaches into those bytes
	// could still be extended by an overlapping one.
	tail []byte
	// maxLen is the longest secret, which is how much has to be held back.
	maxLen int
	// replaced counts the occurrences this writer removed, so that a report
	// can say it was scrubbed.
	//
	// It exists because of Artemis. Its harness password is "artemis", the
	// broker parameter's value is "artemis" and the profile it reports is
	// "artemis", so a doctor run came out saying `profile=redacted` and
	// `broker=redacted` -- a reader could not tell which ActiveMQ had
	// answered. The scrub is right to replace every occurrence (a credential
	// that is also a common word is still a credential), so the fix is not to
	// scrub less but to SAY that the output was scrubbed, which turns a
	// confusing line into an explained one.
	replaced int
}

// newScrubWriter wraps w. A writer with no secrets still wraps, so that the
// output path is the same shape whether or not a credential was given: a code
// path that only exists when there is a secret to hide is a code path nobody
// exercises.
func newScrubWriter(w io.Writer, secrets ...dsn.Secret) *scrubWriter {
	s := &scrubWriter{w: w}
	s.add(secrets...)
	return s
}

// add records another secret, for a credential the CLI learns about after the
// writer exists -- a token read from a file, or a second DSN.
//
// Each secret is held in its JSON spellings too, because every -o json report
// reaches this writer already encoded: encoding/json writes "&" as \u0026 and
// '"' as \", so a report carrying a password with one of those in it matched
// nothing here and its reader decoded the password straight back out. A form
// that contains the secret itself ("\" becomes "\\") overlaps it, so it is
// redacted whole rather than left half escaped.
func (s *scrubWriter) add(secrets ...dsn.Secret) {
	for _, sec := range secrets {
		if sec == "" {
			continue
		}
		for _, form := range append(jsonForms(sec), sec) {
			s.secrets = append(s.secrets, form)
			s.maxLen = max(s.maxLen, len(form))
		}
	}
}

// jsonForms returns the spellings encoding/json gives sec inside a string,
// with HTML escaping and without it, other than sec itself.
func jsonForms(sec dsn.Secret) []dsn.Secret {
	plain := sec.Reveal()
	var out []dsn.Secret
	for _, escapeHTML := range []bool{true, false} {
		var b strings.Builder
		enc := json.NewEncoder(&b)
		enc.SetEscapeHTML(escapeHTML)
		if enc.Encode(plain) != nil {
			continue
		}
		// Encode writes the string quoted, then a newline.
		form := strings.TrimSuffix(b.String(), "\n")
		form = form[1 : len(form)-1]
		if form != plain && !slices.Contains(out, dsn.Secret(form)) {
			out = append(out, dsn.Secret(form))
		}
	}
	return out
}

// Write scrubs and forwards. It reports len(p) written whenever the underlying
// writer accepted everything it was given, because the caller counts the bytes
// it handed over and the redaction is shorter or longer than what it replaced.
func (s *scrubWriter) Write(p []byte) (int, error) {
	if len(s.secrets) == 0 {
		return s.w.Write(p)
	}
	// The occurrences are found in the WHOLE accumulated buffer, and only
	// then is a tail held back. Holding back first and scrubbing the rest is
	// the obvious implementation and it is wrong: a secret that straddles the
	// boundary is in neither half, so neither half matches it, and the two
	// halves reassemble it in the output. TestScrubWriterHoldsASplitSecret
	// caught exactly that, with the credential printed in full.
	//
	// An occurrence the next Write completes begins in the last maxLen-1
	// bytes, so those wait. A redaction that reaches into them waits whole
	// too: an occurrence still to come may overlap it, and emitting its
	// first part would leave the rest of that occurrence unmatched.
	in := string(append(s.tail, p...))
	runs := secretSpans(in, s.secrets)
	cut, done := max(len(in)-(s.maxLen-1), 0), len(runs)
	for i, r := range runs {
		if r.end > cut {
			cut, done = min(cut, r.start), i
			break
		}
	}
	emit := redactSpans(in[:cut], runs[:done])
	s.replaced += done
	s.tail = append(s.tail[:0], in[cut:]...)
	if len(emit) > 0 {
		if _, err := io.WriteString(s.w, emit); err != nil {
			return 0, err
		}
	}
	return len(p), nil
}

// Flush emits whatever is being held back. Every command calls it before it
// returns, so a report whose last bytes could have begun a secret is still
// printed.
func (s *scrubWriter) Flush() error {
	if len(s.tail) == 0 {
		return nil
	}
	in := string(s.tail)
	runs := secretSpans(in, s.secrets)
	s.replaced += len(runs)
	s.tail = s.tail[:0]
	_, err := io.WriteString(s.w, redactSpans(in, runs))
	return err
}

// span is the bytes [start, end) of a message that one redaction replaces.
type span struct{ start, end int }

// secretSpans returns the runs of msg's bytes that belong to an occurrence of
// any of the secrets, in order. Occurrences that overlap, of one secret or of
// two, are one run; occurrences that only touch stay two.
func secretSpans(msg string, secrets []dsn.Secret) []span {
	var all []span
	for _, sec := range secrets {
		if sec == "" {
			continue
		}
		plain := sec.Reveal()
		// Every occurrence, overlapping ones included: "aaa" holds "aa"
		// twice, and the second covers a byte the first does not.
		for i := 0; ; {
			j := strings.Index(msg[i:], plain)
			if j < 0 {
				break
			}
			all = append(all, span{start: i + j, end: i + j + len(plain)})
			i += j + 1
		}
	}
	slices.SortFunc(all, func(a, b span) int { return cmp.Compare(a.start, b.start) })
	runs := all[:0]
	for _, sp := range all {
		if n := len(runs); n > 0 && sp.start < runs[n-1].end {
			runs[n-1].end = max(runs[n-1].end, sp.end)
			continue
		}
		runs = append(runs, sp)
	}
	return runs
}

// redactSpans returns msg with each run replaced by the redaction.
func redactSpans(msg string, runs []span) string {
	if len(runs) == 0 {
		return msg
	}
	var b strings.Builder
	at := 0
	for _, r := range runs {
		b.WriteString(msg[at:r.start])
		b.WriteString(dsn.Redacted)
		at = r.end
	}
	b.WriteString(msg[at:])
	return b.String()
}

// scrubbed returns a copy of the environment whose BOTH streams hide every
// credential the CLI can identify, and the function that empties them.
//
// Both streams, because the first version of this scrubbed only stdout -- and
// every error path in this package writes to stderr. The specification asks a driver to
// sanitise a native error "without losing errors.As", which is the driver's
// obligation about the error; it is not a promise about a tool that prints
// one. A broker's reply, a native client's dial failure and a driver's own
// message all reach the terminal through stderr, and a credential in any of
// them is a credential on the terminal.
//
// The environment's credential-named variables are folded in here rather than
// at each call site, so a command cannot forget them.
func (e *cliEnv) scrubbed(targets ...string) (*cliEnv, func()) {
	secrets := make([]dsn.Secret, 0, 4*len(targets))
	for _, t := range targets {
		secrets = append(secrets, secretsOf(t)...)
	}
	out := newScrubWriter(e.stdout, secrets...)
	errw := newScrubWriter(e.stderr, secrets...)
	for _, name := range credentialEnvNames() {
		v, ok := e.getenv(name)
		if !ok || v == "" {
			continue
		}
		out.add(dsn.Secret(v))
		errw.add(dsn.Secret(v))
	}
	cp := *e
	cp.stdout, cp.stderr = out, errw
	return &cp, func() {
		_ = out.Flush()
		_ = errw.Flush()
		// The note goes to the UNDERLYING writer: it carries no credential,
		// and routing it through the scrub would let a short credential
		// mangle the very sentence that explains the mangling.
		if n := out.replaced + errw.replaced; n > 0 {
			_, _ = io.WriteString(e.stdout, "note: "+itoa(n)+" occurrence(s) of a credential "+
				"were replaced with "+quote(dsn.Redacted)+" above. A credential that is also "+
				"an\n  ordinary word -- a password of \"artemis\", say -- is replaced wherever "+
				"it appears, so a\n  field can read as redacted when it was not a secret.\n")
		}
	}
}

// credentialEnvNames are the environment variables whose values the scrub
// holds. It is [interestingEnv] filtered by [isCredentialName], so the two
// lists cannot disagree about which variable carries a secret.
func credentialEnvNames() []string {
	all := interestingEnv()
	out := make([]string, 0, len(all))
	for _, ev := range all {
		if isCredentialName(ev.Name) {
			out = append(out, ev.Name)
		}
	}
	return out
}

// ephemeralNote is the warning a memory target earns.
//
// memory.Open creates a NEW broker per call -- the specification forbids global state, so
// there is nowhere for another process's broker to live -- which means
// `iqmq pub memory:// t x` followed by `iqmq sub memory:// t s` sees nothing at
// all. That is correct behaviour and a trap a user falls into once; the CLI is
// the first thing to meet it from outside the library's own tests, so it says
// so rather than appearing to lose a message.
const ephemeralNote = "note: a memory:// target is created by THIS invocation and disappears with it, " +
	"so nothing another\n  iqmq command published is visible here. " +
	"Use a broker for two commands to share messages.\n"

// isEphemeral reports whether a DSN names the in-process memory broker.
func isEphemeral(raw string) bool {
	scheme, err := dsn.Scheme(raw)
	if err != nil {
		return false
	}
	return scheme == "memory" || scheme == "mem"
}
