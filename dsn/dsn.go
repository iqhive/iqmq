// Package dsn parses iqmq connection strings and maps a scheme to the driver
// that opens it (the API, ADR-012).
//
// A DSN is the convenience form of configuration; a typed Config is what a
// production program should write. What this package guarantees is that the
// convenience form cannot lie:
//
//   - Parsing is pure, bounded and total. It never panics, never allocates
//     unboundedly and reads at most [MaxLen] bytes, because the bytes may come
//     from an environment variable a stranger set.
//   - An unknown parameter is an error, never a warning. A DSN that quietly
//     ignored "capacty=100000" would hand a caller a broker they did not ask
//     for, and nobody would find it (ADR-010).
//   - Credentials never render. The password is a [Secret], which answers
//     fmt, slog and encoding/json with the redaction, and [Config.String]
//     round-trips the DSN with the credential removed.
//
// The registry half is [Registry]: a driver hands it the schemes it answers to
// and the function that opens a client from one. There is no package-level
// registry here on purpose — the specification forbids global mutable state in the root
// module, so a library that composes two brokers owns its own registry. The
// process-wide facade for a program that wants one lives in a module of its own.
package dsn

import (
	"log/slog"
	"strings"

	"bitbucket.org/iqhive/iqmq/internal/errcode"
)

// MaxLen bounds what [Parse] will look at. A DSN is a handful of parameters; a
// megabyte of them is an attack or a mistake, and the specification asks a parser to have
// an explicit limit rather than an implicit one.
const MaxLen = 8 << 10

// Host is one endpoint of a DSN's authority.
type Host struct {
	// Host is the host name or IP literal, with no brackets around an IPv6
	// address: the brackets are the DSN's syntax and not part of the name.
	Host string
	// Port is the port as written, empty when the DSN named none. It is a
	// string because a driver's default port is the driver's business and
	// because "the DSN did not say" must not be port zero.
	Port string
}

// String renders the endpoint as a dial address — the form a native client
// takes — re-bracketing an IPv6 literal. It is not escaped: a dial address is
// what it is, and percent-encoding one would hand the client a host it cannot
// resolve.
func (h Host) String() string { return h.render(false) }

// render is String with the option of escaping, which [Config.String] needs and
// a dial address must not have.
func (h Host) render(escaped bool) string {
	host := h.Host
	if escaped {
		host = escape(host)
	}
	if strings.ContainsRune(host, ':') {
		host = "[" + host + "]"
	}
	if h.Port == "" {
		return host
	}
	return host + ":" + h.Port
}

// Config is a parsed DSN.
//
// A Config is a value, and a copy shares the parameter storage with the
// original the way [bitbucket.org/iqhive/iqmq.Headers] does: [Params] records
// which parameters a driver has read, so the copy and the original agree about
// what is still unknown.
type Config struct {
	// Scheme is the lower-cased scheme, without the "://".
	Scheme string
	// Username is the user from the authority, percent-decoded.
	Username string
	// Password is the password from the authority, percent-decoded. It never
	// renders itself.
	Password Secret
	// HasPassword reports whether the authority carried a password at all,
	// so that an empty password and no password stay distinct.
	HasPassword bool
	// Hosts are the endpoints of the authority, in the order written. A DSN
	// with no authority has none.
	Hosts []Host
	// Path is the path, percent-decoded, with its leading slash removed. It
	// is empty when the DSN named none.
	Path string
	// Params are the query parameters, in the order written.
	Params Params
}

// Addr returns the first endpoint, or the empty string when the DSN named none.
func (c *Config) Addr() string {
	if len(c.Hosts) == 0 {
		return ""
	}
	return c.Hosts[0].String()
}

// Addrs returns every endpoint in the order written.
func (c *Config) Addrs() []string {
	out := make([]string, len(c.Hosts))
	for i := range c.Hosts {
		out[i] = c.Hosts[i].String()
	}
	return out
}

// String renders the DSN with every credential redacted, so a Config is safe
// to put in a log line or an error. It is not a round trip: neither
// [Config.Password] nor the value of any credential-named parameter is in the
// result, which is the whole reason the rendering exists.
//
// "Every credential" is the part had to fix. The password was redacted
// because it is a [Secret] and a Secret redacts itself; the query parameters
// were written out verbatim, and four shipped drivers read a credential from
// one -- `token` (nats, jetstream, pulsar), `authsecret` (nsq), `authparams`
// (pulsar), `sessiontoken` (sqs, snssqs). So the one function in this package
// documented as the safe rendering printed `?token=s3cr3t` in full.
// [IsCredentialName] decides which parameter is one, and it is the same
// predicate the CLI scrubs with, because two copies of that list is the drift
// defect rather than a redundancy.
//
// The receiver is a VALUE, which matters more than it looks. With a pointer
// receiver a Config value did not implement fmt.Stringer, so `%v` of one
// printed the struct -- and Params' fields are unexported, so fmt rendered
// them as three bare slices with the credential among them:
//
//	{natsjs user redacted true [127.0.0.1:4222]  {[token] [s3cr3t] [false]}}
//
// which is the exact hazard [Secret]'s own documentation names ("a Config
// printed with %v") reaching the output around the type that exists to stop
// it.
func (c Config) String() string {
	var b strings.Builder
	b.WriteString(c.Scheme)
	b.WriteString("://")
	if c.Username != "" || c.HasPassword {
		b.WriteString(escape(c.Username))
		if c.HasPassword {
			b.WriteByte(':')
			b.WriteString(Redacted)
		}
		b.WriteByte('@')
	}
	for i := range c.Hosts {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(c.Hosts[i].render(true))
	}
	if c.Path != "" {
		b.WriteByte('/')
		b.WriteString(escape(c.Path))
	}
	first := true
	for k, v := range c.Params.All() {
		if first {
			b.WriteByte('?')
			first = false
		} else {
			b.WriteByte('&')
		}
		b.WriteString(escape(k))
		b.WriteByte('=')
		if IsCredentialName(k) {
			b.WriteString(Redacted)
			continue
		}
		b.WriteString(escape(v))
	}
	return b.String()
}

// LogValue renders the DSN for slog with every credential redacted, so that
// `slog.Any("dsn", cfg)` cannot print one.
//
// slog does not consult fmt.Stringer: a value of a type that is not a
// LogValuer is formatted by the handler, and the built-in handlers reach
// fmt.Sprintf("%+v") for one, which prints the struct. String alone was
// therefore not enough.
func (c Config) LogValue() slog.Value { return slog.StringValue(c.String()) }

// Parse parses a DSN into a [Config].
//
// It is a pure function with an explicit bound and it never panics, which is the rule for every parser in this repository: the bytes may come from an
// environment variable a stranger set. Every failure carries
// [bitbucket.org/iqhive/iqmq.CodeInvalidConfig].
//
// The grammar is the specification:
//
//	scheme://[user[:password]@]host[:port][,host[:port]...][/path][?k=v&k=v]
//
// The scheme is required and lower-cased, because RFC 3986 makes it
// case-insensitive: "KAFKA://" and "kafka://" are one scheme, and every
// comparison after this one sees "kafka". Nothing else is case-folded. The
// authority is optional — a memory DSN has none — and may name several
// endpoints. A fragment is refused: a connection string has no fragment, and
// one in the position a fragment occupies is a mistake or another product's
// DSN. A repeated parameter is refused rather than resolved, because neither
// "first wins" nor "last wins" is a rule a reader of the DSN can see.
func Parse(s string) (Config, error) {
	if len(s) > MaxLen {
		return Config{}, errTooLong
	}
	scheme, rest, err := splitScheme(s)
	if err != nil {
		return Config{}, err
	}
	cfg := Config{Scheme: scheme}
	if i := strings.IndexByte(rest, '#'); i >= 0 {
		return Config{}, errFragment
	}
	query := ""
	if i := strings.IndexByte(rest, '?'); i >= 0 {
		rest, query = rest[:i], rest[i+1:]
	}
	authority := rest
	if i := strings.IndexByte(rest, '/'); i >= 0 {
		authority = rest[:i]
		path, err := unescape(rest[i+1:], false)
		if err != nil {
			return Config{}, err
		}
		cfg.Path = path
	}
	if err := parseAuthority(&cfg, authority); err != nil {
		return Config{}, err
	}
	if err := parseQuery(&cfg.Params, query); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

// Scheme returns the scheme of a DSN without parsing the rest of it, which is
// what a registry needs in order to choose a driver: a DSN whose parameters
// only the driver understands must still reach that driver.
func Scheme(s string) (string, error) {
	if len(s) > MaxLen {
		return "", errTooLong
	}
	scheme, _, err := splitScheme(s)
	return scheme, err
}

// splitScheme splits "scheme://rest" and canonicalises the scheme.
func splitScheme(s string) (scheme, rest string, err error) {
	i := strings.Index(s, "://")
	if i <= 0 {
		return "", "", errSyntax
	}
	scheme, err = canonicalScheme(s[:i])
	if err != nil {
		return "", "", err
	}
	return scheme, s[i+len("://"):], nil
}

// canonicalScheme is the one place a scheme is canonicalised. The grammar is
// RFC 3986's: a letter followed by letters, digits, "+", "-" and ".", and a
// scheme is case-insensitive, so it is lower-cased here and compared as that
// everywhere after.
//
// Every scheme this package compares goes through it -- [Parse], [Scheme] and
// the [Registry] -- because a scheme matched case-insensitively in one place
// and case-sensitively in the next is a DSN that parses and then reaches
// nothing. The registry stored a registration as it was spelled and looked a
// DSN up by its lower-cased scheme, so `Register("Kafka", …)` succeeded and no
// DSN could ever reach it.
func canonicalScheme(scheme string) (string, error) {
	if scheme == "" {
		return "", errScheme
	}
	for j := range len(scheme) {
		c := scheme[j]
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z':
		case j > 0 && (c >= '0' && c <= '9' || c == '+' || c == '-' || c == '.'):
		default:
			return "", errScheme
		}
	}
	return strings.ToLower(scheme), nil
}

// parseAuthority reads "[user[:password]@]host[:port][,host[:port]...]".
func parseAuthority(cfg *Config, authority string) error {
	if authority == "" {
		return nil
	}
	if i := strings.LastIndexByte(authority, '@'); i >= 0 {
		cred, rest := authority[:i], authority[i+1:]
		user, pass, hasPass := strings.Cut(cred, ":")
		if strings.ContainsRune(pass, ':') {
			// Two colons in a credential is ambiguous; a password with a
			// colon in it is written %3A.
			return errAuthority
		}
		u, err := unescape(user, false)
		if err != nil {
			return err
		}
		p, err := unescape(pass, false)
		if err != nil {
			return err
		}
		cfg.Username, cfg.Password, cfg.HasPassword = u, Secret(p), hasPass
		authority = rest
	}
	for _, endpoint := range strings.Split(authority, ",") {
		h, err := parseHost(endpoint)
		if err != nil {
			return err
		}
		cfg.Hosts = append(cfg.Hosts, h)
	}
	return nil
}

// parseHost reads one "host[:port]" endpoint, with an IPv6 literal in brackets.
func parseHost(s string) (Host, error) {
	if s == "" {
		return Host{}, errAuthority
	}
	if s[0] == '[' {
		end := strings.IndexByte(s, ']')
		if end < 0 {
			return Host{}, errAuthority
		}
		if end == 1 {
			// "[]" is an endpoint with no host, which is a malformed
			// authority rather than the default host.
			return Host{}, errAuthority
		}
		h := Host{Host: s[1:end]}
		switch tail := s[end+1:]; {
		case tail == "":
			// No port, exactly as "host" with no colon names none. The
			// port used to be checked here too, and an empty one is a
			// refusal, so "[::1]" was malformed while "localhost" was
			// not -- an IPv6 literal could not be written without a port,
			// and a rendering of one could not be read back.
		case tail[0] == ':':
			h.Port = tail[1:]
			if err := checkPort(h.Port); err != nil {
				return Host{}, err
			}
		default:
			return Host{}, errAuthority
		}
		return h, nil
	}
	host, port, hasPort := strings.Cut(s, ":")
	if strings.ContainsRune(port, ':') {
		return Host{}, errAuthority
	}
	h, err := unescape(host, false)
	if err != nil {
		return Host{}, err
	}
	if h == "" {
		return Host{}, errAuthority
	}
	if !hasPort {
		return Host{Host: h}, nil
	}
	if err := checkPort(port); err != nil {
		return Host{}, err
	}
	return Host{Host: h, Port: port}, nil
}

// checkPort refuses a port that is not decimal digits. An empty port is the
// endpoint having named none after a colon, which is a malformed authority
// rather than the default port.
func checkPort(port string) error {
	if port == "" {
		return errAuthority
	}
	for i := range len(port) {
		if port[i] < '0' || port[i] > '9' {
			return errAuthority
		}
	}
	return nil
}

// parseQuery reads "k=v&k=v", refusing a repeated or unnamed parameter.
func parseQuery(p *Params, query string) error {
	if query == "" {
		return nil
	}
	for _, pair := range strings.Split(query, "&") {
		if pair == "" {
			continue
		}
		rawKey, rawVal, _ := strings.Cut(pair, "=")
		key, err := unescape(rawKey, true)
		if err != nil {
			return err
		}
		val, err := unescape(rawVal, true)
		if err != nil {
			return err
		}
		if key == "" {
			return errEmptyKey
		}
		if p.find(key) >= 0 {
			return errRepeated
		}
		p.keys = append(p.keys, key)
		p.vals = append(p.vals, val)
		p.read = append(p.read, false)
	}
	return nil
}

// unescape decodes percent escapes, with no allocation for the common case of a
// string that carries none.
//
// plusSpace decodes "+" as a space, which is the form-encoding rule and applies
// to the query alone. A "+" in a host, a path or a credential is a literal
// plus: decoding one there turned the endpoint "0+" into "0 " and made
// [Config.String] render a DSN that no longer named the same broker, which is
// what FuzzDSNParse found.
func unescape(s string, plusSpace bool) (string, error) {
	if !strings.ContainsRune(s, '%') && (!plusSpace || !strings.ContainsRune(s, '+')) {
		return s, nil
	}
	var b strings.Builder
	b.Grow(len(s))
	for i := 0; i < len(s); i++ {
		switch c := s[i]; c {
		case '%':
			if i+2 >= len(s) {
				return "", errEscape
			}
			hi, ok1 := unhex(s[i+1])
			lo, ok2 := unhex(s[i+2])
			if !ok1 || !ok2 {
				return "", errEscape
			}
			b.WriteByte(hi<<4 | lo)
			i += 2
		case '+':
			if plusSpace {
				b.WriteByte(' ')
				continue
			}
			b.WriteByte('+')
		default:
			b.WriteByte(c)
		}
	}
	return b.String(), nil
}

// unhex reads one hexadecimal digit.
func unhex(c byte) (byte, bool) {
	switch {
	case c >= '0' && c <= '9':
		return c - '0', true
	case c >= 'a' && c <= 'f':
		return c - 'a' + 10, true
	case c >= 'A' && c <= 'F':
		return c - 'A' + 10, true
	}
	return 0, false
}

// escape percent-encodes the bytes a DSN cannot carry literally, so that
// [Config.String] renders something Parse reads back.
func escape(s string) string {
	if !strings.ContainsAny(s, "%+ @:/?#&=[],") {
		return s
	}
	const hex = "0123456789ABCDEF"
	var b strings.Builder
	b.Grow(len(s) + 8)
	for i := range len(s) {
		c := s[i]
		if strings.IndexByte("%+ @:/?#&=[],", c) >= 0 {
			b.WriteByte('%')
			b.WriteByte(hex[c>>4])
			b.WriteByte(hex[c&0xf])
			continue
		}
		b.WriteByte(c)
	}
	return b.String()
}

// The parse failures. The specification names no sentinel for a malformed DSN and errors.Is
// matches by code, so a caller compares mq.CodeOf(err) against
// mq.CodeInvalidConfig.
var (
	errTooLong   = errcode.New(errcode.InvalidConfig, "iqmq/dsn: the DSN is longer than 8 KiB")
	errSyntax    = errcode.New(errcode.InvalidConfig, "iqmq/dsn: the DSN is not a connection string")
	errScheme    = errcode.New(errcode.InvalidConfig, "iqmq/dsn: the DSN has no usable scheme")
	errEscape    = errcode.New(errcode.InvalidConfig, "iqmq/dsn: the DSN contains an invalid percent escape")
	errAuthority = errcode.New(errcode.InvalidConfig, "iqmq/dsn: the DSN's authority is malformed")
	errFragment  = errcode.New(errcode.InvalidConfig, "iqmq/dsn: a DSN has no fragment")
	errRepeated  = errcode.New(errcode.InvalidConfig, "iqmq/dsn: the DSN repeats a parameter")
	errEmptyKey  = errcode.New(errcode.InvalidConfig, "iqmq/dsn: the DSN has a parameter with no name")
)
