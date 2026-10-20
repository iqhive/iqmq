package sqs

import (
	"context"
	"strings"
	"time"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit/flush"
	"github.com/iqhive/iqmq/dsn"
)

// Scheme is the DSN scheme this driver answers:
//
//	sqs://AKIAEXAMPLE:secret@127.0.0.1:9324/prefix?region=elasticmq&insecuretransport=true
//
// The authority's user is the access key ID and its password the secret access
// key, which is what makes this the first driver whose DSN carries a real
// credential — and therefore the first whose `secrets/redaction` conformance
// scenario can run at all.
//
// A DSN with no host uses the SDK's own regional endpoint for `region`, and a
// DSN with a host names the endpoint explicitly, which is how an emulator is
// reached.
//
// [RegionalHost] is the third form and exists because of a limitation in the
// shared parser: [github.com/iqhive/iqmq/dsn.Parse] refuses an authority that
// carries credentials and no host ("sqs://key:secret@?region=…" is malformed),
// so a production DSN — credentials, and an endpoint the SDK derives from the
// region — cannot be written in the hostless form at all. SQS is the first
// driver whose endpoint is a PARAMETER rather than part of the authority, so
// nothing had hit it before. Naming the host "aws" is the documented way to
// say "credentials, and the regional endpoint"; recorded in
// the maintenance notes.
const Scheme = DriverName

// RegionalHost is the host that means "the SDK's own regional endpoint for
// Region" rather than an endpoint of its own.
//
//	sqs://AKIAEXAMPLE:secret@aws?region=eu-west-1
//
// It is not a real host name and is never dialled: [ParseDSN] turns it into an
// empty [Config.Endpoint].
const RegionalHost = "aws"

// Register registers this driver with a DSN registry under [Scheme]
// (ADR-012).
//
// A driver exposes this and nothing else: it never reaches for a global
// registry of its own, so a library that composes two brokers registers each
// of them with a registry it owns.
func Register(r dsn.Registrar) error {
	if r == nil {
		return errNilRegistrar
	}
	return r.Register(Scheme, OpenDSN)
}

// OpenDSN opens a client from a DSN.
func OpenDSN(ctx context.Context, s string) (*mq.Client, error) {
	cfg, err := ParseDSN(s)
	if err != nil {
		return nil, err
	}
	return Open(ctx, cfg)
}

// ParseDSN turns an SQS DSN into a [Config].
//
// It is [github.com/iqhive/iqmq/dsn.Parse] plus this driver's own parameter
// table, which is the point: the shared parser owns the grammar, the 8 KiB
// bound, the percent decoding and the credential redaction, and
// [github.com/iqhive/iqmq/dsn.Params.Strict] turns a parameter this driver
// does not have into a construction error naming it, rather than a setting
// nobody notices was ignored (ADR-010).
//
// The parameters, all optional:
//
//	region=<string>              the AWS region; signing needs one
//	accountid=<string>           the account the queues belong to
//	fifo=true|false              select the FIFO profile
//	contentbaseddedup=true|false the FIFO queue attribute of that name
//	autocreate=true|false let the driver create a queue (opt-in)
//	pollers=<n>                  concurrent ReceiveMessage loops per source
//	maxmessages=<n>              messages per ReceiveMessage, at most ten
//	waittime=<duration>          long poll, at most twenty seconds
//	ackdeadline=<duration>       visibility timeout, at most twelve hours
//	leaseguard=<duration>        refuse a settlement this long before expiry
//	settle=<kind> Auto|immediate|count|time|hybrid
//	settlemaxitems=<n>           batch size, at most ten
//	settleinterval=<duration>    time and hybrid flush period
//	body=auto|text|base64 the binary-body policy
//	maxmessagebytes=<n>          override the advertised message limit
//	deadletterqueue=<name>       redrive destination (a queue NAME)
//	maxreceivecount=<n>          receives before the redrive fires
//	retention=<duration>         MessageRetentionPeriod of a created queue
//	apitimeout=<duration>        bound on one API call with no deadline
//	sessiontoken=<secret>        session token of a temporary credential
//	insecuretransport=true|false dial the endpoint over plain HTTP
//
// The path names [Config.QueuePrefix], so sqs://host/acme- prefixes every
// queue name with "acme-". The authority's host is the endpoint, or
// [RegionalHost] for the SDK's own regional one.
func ParseDSN(s string) (Config, error) {
	c, err := dsn.Parse(s)
	if err != nil {
		return Config{}, err
	}
	if c.Scheme != Scheme {
		return Config{}, mq.ErrDriverNotRegistered
	}
	cfg := Config{
		AccessKeyID:     c.Username,
		SecretAccessKey: c.Password,
		QueuePrefix:     c.Path,
	}
	p := &c.Params
	var token string
	var settle, body string
	for _, step := range [...]func() error{
		func() error { return p.String("region", func(v string) { cfg.Region = v }) },
		func() error { return p.String("accountid", func(v string) { cfg.AccountID = v }) },
		func() error { return p.Bool("fifo", func(v bool) { cfg.FIFO = v }) },
		func() error { return p.Bool("contentbaseddedup", func(v bool) { cfg.ContentBasedDeduplication = v }) },
		func() error { return p.Bool("autocreate", func(v bool) { cfg.AutoCreate = v }) },
		func() error { return p.Int("pollers", func(v int) { cfg.Pollers = v }) },
		func() error { return p.Int("maxmessages", func(v int) { cfg.MaxMessages = v }) },
		func() error { return p.Duration("waittime", func(v time.Duration) { cfg.WaitTime = v }) },
		func() error { return p.Duration("ackdeadline", func(v time.Duration) { cfg.AckDeadline = v }) },
		func() error { return p.Duration("leaseguard", func(v time.Duration) { cfg.LeaseGuard = v }) },
		func() error { return p.String("settle", func(v string) { settle = v }) },
		func() error { return p.Int("settlemaxitems", func(v int) { cfg.SettleMaxItems = v }) },
		func() error { return p.Duration("settleinterval", func(v time.Duration) { cfg.SettleInterval = v }) },
		func() error { return p.String("body", func(v string) { body = v }) },
		func() error { return p.Int64("maxmessagebytes", func(v int64) { cfg.MaxMessageBytes = v }) },
		func() error { return p.String("deadletterqueue", func(v string) { cfg.DeadLetterQueue = v }) },
		func() error { return p.Int("maxreceivecount", func(v int) { cfg.MaxReceiveCount = v }) },
		func() error { return p.Duration("retention", func(v time.Duration) { cfg.Retention = v }) },
		func() error { return p.Duration("apitimeout", func(v time.Duration) { cfg.APITimeout = v }) },
		func() error { return p.String("sessiontoken", func(v string) { token = v }) },
		func() error { return p.Bool("insecuretransport", func(v bool) { cfg.InsecureTransport = v }) },
	} {
		if err := step(); err != nil {
			return Config{}, err
		}
	}
	if err := p.Strict(); err != nil {
		return Config{}, err
	}
	cfg.SessionToken = dsn.Secret(token)
	if settle != "" {
		k, ok := flush.ParseKind(settle)
		if !ok {
			return Config{}, errSettleKind
		}
		cfg.Settle = k
	}
	if body != "" {
		b, ok := parseBodyEncoding(body)
		if !ok {
			return Config{}, errBodyEncoding
		}
		cfg.Body = b
	}
	if len(c.Hosts) > 1 {
		// One endpoint, not a list: an SQS client dials one endpoint and the
		// SDK does the failover a region implies. A DSN naming two would be
		// a configuration nothing reads.
		return Config{}, errTwoEndpoints
	}
	if len(c.Hosts) == 1 && !dialableHost(c.Hosts[0].Host) {
		return Config{}, errDSNHost
	}
	if len(c.Hosts) == 1 && c.Hosts[0].Host != RegionalHost {
		scheme := "https://"
		if cfg.InsecureTransport {
			scheme = "http://"
		}
		cfg.Endpoint = scheme + c.Hosts[0].String()
	}
	if len(c.Hosts) == 1 && c.Hosts[0].Host == RegionalHost {
		if c.Hosts[0].Port != "" {
			// "aws:9324" is somebody expecting a host and getting a
			// sentinel, which would silently dial the region instead.
			return Config{}, errRegionalPort
		}
		if cfg.InsecureTransport {
			return Config{}, errInsecureWithoutEndpoint
		}
	}
	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

// parseBodyEncoding is [BodyEncoding]'s slug parser.
//
// It refuses an unknown name rather than defaulting, which matters more here
// than the enumeration's size suggests: the three policies differ in whether a
// binary body is refused, encoded or passed through, and a typo that silently
// selected `auto` would change what a publication does to the bytes.
func parseBodyEncoding(s string) (BodyEncoding, bool) {
	for i, name := range bodyEncodingNames {
		if name == s {
			return BodyEncoding(i), true
		}
	}
	return 0, false
}

// String renders the endpoint's host for a DSN.
func endpointHost(endpoint string) string {
	return strings.TrimPrefix(strings.TrimPrefix(endpoint, "https://"), "http://")
}

// dialableHost reports whether a host the shared parser DECODED can be written
// into the endpoint URL [ParseDSN] builds and still name only itself.
//
// The endpoint reaches the AWS SDK as BaseEndpoint and is parsed there as a
// URI, which gives "@" (user information), "/" (a path), "?" (a query) and "#"
// (a fragment) a meaning of their own, and a DSN writes any of them into a
// host as an escape. Measured against aws-sdk-go-v2/service/sqs by capturing
// the HTTP request a GetQueueUrl call produced:
// "sqs://k:s@tok%40real-host:9324" requested http://real-host:9324/;
// "a%2Fb", "a%3Fb" and "a%23b" each requested host "a" and lost the port with
// it; and "a%5Cb" made the SDK refuse with "Failed to parse uri" on the first
// API call, so the configuration failed later with an error about a string the
// operator never wrote.
//
// A host name or an IP literal holds none of them, so such a host is refused
// rather than escaped: there is no spelling of it that reaches what the DSN
// names.
//
// A comma is deliberately NOT here: the same measurement requested
// "http://a,b:9324/" with that host intact, because an endpoint is one URI and
// not a list. The separator that matters is the client library's, not a rule
// shared between drivers (the specification refused a comma for nats.go, which does split).
func dialableHost(host string) bool {
	for i := range len(host) {
		switch c := host[i]; {
		case c <= ' ', c == 0x7f, c == '@', c == '/', c == '?', c == '#', c == '\\':
			return false
		}
	}
	// A "%" in a DECODED host is refused too, unless the host also holds a
	// ":". The shared parser decodes once, so "%25" puts a literal "%" in
	// the host and "%2540" puts back the escape it came from -- and every
	// client library here reads its URL with net/url, which rejects any
	// "%XY" in a host except "%25". Measured: "tok%2540real-host" and
	// "a%25b" each produced "invalid URL escape" at the DIAL rather than at
	// construction, which is the outcome this set refuses a backslash for.
	// It is not a second redirect (net/url cannot be steered to another host
	// this way), it is the same late-failure class.
	//
	// The one legitimate "%" in a host is an IPv6 zone id, which always
	// arrives inside brackets and therefore always beside a ":"
	// ("[fe80::1%25eth0]"), so that exception is what the colon tests.
	if strings.ContainsRune(host, '%') && !strings.ContainsRune(host, ':') {
		return false
	}
	return true
}

// The escape sets, one per position of a DSN, each holding exactly the bytes
// the shared parser gives a meaning to THERE -- because it decodes every
// component, so each can hold any byte, and escaping more than a position
// needs makes a rendering unreadable and longer than what it parsed.
//
//   - a user ends at the first ":" (the secret) and the authority at "/", "?"
//     or "#"; an "@" in it is safe, because the credential ends at the LAST
//     one;
//   - a host is also split on "," (the next endpoint) and on ":" (the port),
//     and one that starts with "[" is read as a bracketed literal. "@", "/",
//     "?" and "#" are not in the set because [dialableHost] refuses them;
//   - the path runs from the first "/" to the "?";
//   - a query value runs from its "=" to the next "&", and a "+" in it decodes
//     as a space.
//
// "%" is in every set because it begins an escape, and "#" because a DSN with
// one anywhere is refused.
const (
	escUser  = "%:/?#"
	escHost  = "%,:"
	escPath  = "%?#"
	escQuery = "%+&#"
)

// escapeUser escapes the access key id for the DSN's credential position.
func escapeUser(s string) string { return escapeSet(s, escUser) }

// escapePath escapes the queue prefix, which the DSN carries as its path.
func escapePath(s string) string { return escapeSet(s, escPath) }

// escapeQuery escapes a query parameter's value.
func escapeQuery(s string) string { return escapeSet(s, escQuery) }

// escapeHost escapes an unbracketed host, including a leading "[", which would
// otherwise open a bracketed literal.
func escapeHost(s string) string {
	out := escapeSet(s, escHost)
	if strings.HasPrefix(out, "[") {
		out = "%5B" + out[1:]
	}
	return out
}

// escapeSet percent-encodes every byte of s that is in set.
func escapeSet(s, set string) string {
	if !strings.ContainsAny(s, set) {
		return s
	}
	const hex = "0123456789ABCDEF"
	var b strings.Builder
	b.Grow(len(s) + 8)
	for i := range len(s) {
		c := s[i]
		if strings.IndexByte(set, c) >= 0 {
			b.WriteByte('%')
			b.WriteByte(hex[c>>4])
			b.WriteByte(hex[c&0xF])
			continue
		}
		b.WriteByte(c)
	}
	return b.String()
}

// renderEndpoint renders [Config.Endpoint] as the DSN's authority: the scheme
// goes (the DSN carries it as `insecuretransport`) and the host is escaped for
// the position it lands in.
//
// A host with a colon or a "%" in it is written as a bracketed literal, raw,
// which is how an IPv6 address reads and how it came in: the parser takes a
// bracketed host verbatim, without decoding it, so neither needs an escape
// there -- and a "%" escaped outside the brackets would triple, which is the
// one way a parsed host could otherwise render longer than it was written.
func renderEndpoint(endpoint string) string {
	rest := endpointHost(endpoint)
	// A userinfo goes, rather than being escaped: [Config.Validate] refuses
	// an endpoint that carries one, so the only way to reach this is a
	// configuration that was already refused -- and the rule is about what
	// a rendering CARRIES, not about what a valid one carries. Escaping it
	// kept the bytes of the password in Config.DSN, and Config.String and
	// Config.LogValue are that rendering.
	if i := strings.LastIndexByte(rest, '@'); i >= 0 {
		rest = rest[i+1:]
	}
	// The authority ends at the first "/", "?" or "#". An endpoint may carry
	// a base path (LocalStack's "http://localhost:4566/<account>", or SQS
	// behind a proxy at "/sqs"), and this DSN's own path position is the
	// queue prefix, so the endpoint path has nowhere to go and is dropped --
	// as it was before this branch. What must NOT happen is what the first
	// round did: `splitAddr` saw no port in "localhost:4566/000000000000"
	// (the text after the last colon is not all digits), took the whole
	// string as the HOST, and escapeHost escaped the colon but not the
	// slash, so the rendering parsed back as the bracketed pseudo-host
	// "[localhost:4566]" with the account number as the queue prefix. The
	// host and the port survive now; the dropped path is recorded in
	// the maintenance notes with an owner.
	if i := strings.IndexAny(rest, "/?#"); i >= 0 {
		rest = rest[:i]
	}
	host, port := splitAddr(rest)
	out := escapeHost(host)
	if bracketable(host) {
		out = "[" + host + "]"
	}
	if port == "" {
		return out
	}
	return out + ":" + port
}

// bracketable reports whether host is better written as a raw bracketed
// literal: it has a byte an unbracketed host would have to escape and that a
// bracketed one keeps verbatim, and none that splits the authority first.
func bracketable(host string) bool {
	return strings.ContainsAny(host, ":%") && !strings.ContainsAny(host, "]/?#@,")
}

// splitAddr is the inverse of `dsn.Host.String`, which is how [ParseDSN] turns
// the DSN's endpoint into [Config.Endpoint]: a host with a colon in it is
// bracketed, and a port is the decimal digits after the last colon outside the
// brackets.
func splitAddr(a string) (host, port string) {
	if strings.HasPrefix(a, "[") {
		if i := strings.LastIndex(a, "]:"); i > 0 && allDigits(a[i+2:]) && strings.ContainsRune(a[1:i], ':') {
			return a[1:i], a[i+2:]
		}
		if len(a) >= 2 && strings.HasSuffix(a, "]") && strings.ContainsRune(a[1:len(a)-1], ':') {
			return a[1 : len(a)-1], ""
		}
	}
	if i := strings.LastIndexByte(a, ':'); i >= 0 && allDigits(a[i+1:]) && !strings.ContainsRune(a[:i], ':') {
		return a[:i], a[i+1:]
	}
	return a, ""
}

// allDigits reports whether s is one or more decimal digits.
func allDigits(s string) bool {
	if s == "" {
		return false
	}
	for i := range len(s) {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
}
