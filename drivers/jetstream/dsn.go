package jetstream

import (
	"context"
	"crypto/tls"
	"math"
	"strings"

	mq "bitbucket.org/iqhive/iqmq"
	"bitbucket.org/iqhive/iqmq/dsn"
	"bitbucket.org/iqhive/iqmq/internal/errcode"
)

// Scheme is the DSN scheme this driver answers:
//
//	natsjs://user:password@host:4222,host2:4222/prefix?stream=ORDERS&doubleack=true
//
// It is the JetStream profile alone. "nats://" is the Core driver's and is
// deliberately not claimed here: ADR-010's the specification chose separate modules
// precisely so that a caller cannot confuse one profile's capabilities with the
// other's, and a JetStream driver that answered the Core scheme would undo it.
const Scheme = "natsjs"

// DefaultPort is the port a DSN endpoint that names none is dialled on.
const DefaultPort = "4222"

// Register registers this driver with a DSN registry under [Scheme]
// (ADR-012).
//
// A driver exposes this and nothing else: it never reaches for a global registry
// of its own, so a library that composes two brokers registers each of them with
// a registry it owns.
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

// ParseDSN turns a JetStream DSN into a [Config].
//
// It is [bitbucket.org/iqhive/iqmq/dsn.Parse] plus this driver's own parameter
// table, which is the point: the shared parser owns the grammar, the 8 KiB
// bound, the percent decoding and the credential redaction, and
// [bitbucket.org/iqhive/iqmq/dsn.Params.Strict] turns a parameter this driver does
// not have into a construction error naming it, rather than a setting nobody
// notices was ignored (ADR-010).
//
// The parameters, all optional:
//
//	name=<string>                  connection name the server reports
//	creds=<path> NATS credentials file
//	token=<secret>                 token authentication
//	tls=true|false                 dial with TLS using the platform roots
//	insecureskipverify=true|false  with tls=true, do not verify the server
//	stream=<name>                  the stream every topic lives in
//	streamsubjects=<a;b;c>         the subjects the stream captures
//	managestream=true|false        let the driver create and update the stream
//	storage=file|memory            the stream's storage
//	replicas=<n>                   the stream's replica count
//	maxage=<duration>              how long the stream keeps a message
//	maxmsgs=<n>, maxbytes=<n>      the stream's own bounds
//	maxmsgsize=<n>                 the stream's own per-message ceiling
//	duplicatewindow=<duration>     the Nats-Msg-Id de-duplication window
//	consumerprefix=<name>          prepended to every durable consumer name
//	ackwait=<duration>             the lease a delivery gets
//	maxdeliver=<n>                 deliveries of one record before the server stops
//	inactivethreshold=<duration>   delete a durable this long after its last pull
//	deadlettertopic=<topic>        where Reject routes before it terminates
//	schedulesubject=<token>        the subject root schedule messages go to
//	disableschedules=true|false    refuse every scheduled publication
//	doubleack=true|false           make Ack a request/reply round trip
//	publishasyncmaxpending=<n>     bound on outstanding asynchronous publications
//	publishtimeout=<duration>      bound on a publication or settlement round trip
//	apiprefix=<prefix>             the JetStream API prefix
//	domain=<name>                  the JetStream domain
//	pendingmessages=<n>            per-subscription SetPendingLimits, messages
//	pendingbytes=<n>               per-subscription SetPendingLimits, bytes
//	connecttimeout=<duration>      bound on the initial dial
//	reconnectwait=<duration>       first delay between reconnection attempts
//	maxreconnects=<n>              attempts; negative for "for ever"
//	pinginterval=<duration>        connection liveness probe
//	maxpingsout=<n>                unanswered pings before the link is down
//	draintimeout=<duration>        bound on the drain a Close performs
//
// The path names [Config.SubjectPrefix], so natsjs://host/acme prefixes every
// subject with "acme.".
func ParseDSN(s string) (Config, error) {
	c, err := dsn.Parse(s)
	if err != nil {
		return Config{}, err
	}
	if c.Scheme != Scheme {
		return Config{}, mq.ErrDriverNotRegistered
	}
	cfg := Config{
		Username:      c.Username,
		Password:      c.Password,
		SubjectPrefix: c.Path,
	}
	for _, h := range c.Hosts {
		if !dialableHost(h.Host) {
			return Config{}, errDSNHost
		}
		if h.Port == "" {
			h.Port = DefaultPort
		}
		cfg.Addrs = append(cfg.Addrs, "nats://"+h.String())
	}
	p := &c.Params
	var token, subjects string
	var useTLS, insecure bool
	for _, step := range [...]func() error{
		func() error { return p.String("name", &cfg.Name) },
		func() error { return p.String("creds", &cfg.CredentialsFile) },
		func() error { return p.String("token", &token) },
		func() error { return p.Bool("tls", &useTLS) },
		func() error { return p.Bool("insecureskipverify", &insecure) },
		func() error { return p.String("stream", &cfg.Stream) },
		func() error { return p.String("streamsubjects", &subjects) },
		func() error { return p.Bool("managestream", &cfg.ManageStream) },
		func() error {
			return p.Enum("storage", storageKindNames[:], func(i int) { cfg.Storage = StorageKind(i) })
		},
		// Positive-only, because the struct's zero already means "the
		// server's default": a DSN that wrote replicas=0 would otherwise
		// read as a setting that was honoured.
		func() error { return p.PositiveInt("replicas", &cfg.Replicas) },
		func() error { return p.Duration("maxage", &cfg.MaxAge) },
		func() error { return p.Int64("maxmsgs", &cfg.MaxMsgs) },
		func() error { return p.Int64("maxbytes", &cfg.MaxBytes) },
		func() error { return positiveInt32(p, "maxmsgsize", &cfg.MaxMsgSize) },
		func() error { return p.Duration("duplicatewindow", &cfg.DuplicateWindow) },
		func() error { return p.String("consumerprefix", &cfg.ConsumerPrefix) },
		func() error { return p.Duration("ackwait", &cfg.AckWait) },
		func() error { return p.PositiveInt("maxdeliver", &cfg.MaxDeliver) },
		func() error { return p.Duration("inactivethreshold", &cfg.ConsumerInactiveThreshold) },
		func() error { return p.String("deadlettertopic", &cfg.DeadLetterTopic) },
		func() error { return p.String("schedulesubject", &cfg.ScheduleSubject) },
		func() error { return p.Bool("disableschedules", &cfg.DisableSchedules) },
		func() error { return p.Bool("doubleack", &cfg.DoubleAck) },
		func() error { return p.PositiveInt("publishasyncmaxpending", &cfg.PublishAsyncMaxPending) },
		func() error { return p.Duration("publishtimeout", &cfg.PublishTimeout) },
		func() error { return p.String("apiprefix", &cfg.APIPrefix) },
		func() error { return p.String("domain", &cfg.Domain) },
		func() error { return p.PositiveInt("pendingmessages", &cfg.PendingMessages) },
		func() error { return p.PositiveInt("pendingbytes", &cfg.PendingBytes) },
		func() error { return p.Duration("connecttimeout", &cfg.ConnectTimeout) },
		func() error { return p.Duration("reconnectwait", &cfg.ReconnectWait) },
		func() error { return p.Int("maxreconnects", &cfg.MaxReconnects) },
		func() error { return p.Duration("pinginterval", &cfg.PingInterval) },
		func() error { return p.PositiveInt("maxpingsout", &cfg.MaxPingsOut) },
		func() error { return p.Duration("draintimeout", &cfg.DrainTimeout) },
	} {
		if err := step(); err != nil {
			return Config{}, err
		}
	}
	if err := p.Strict(); err != nil {
		return Config{}, err
	}
	cfg.Token = dsn.Secret(token)
	if subjects != "" {
		// ";" and not ",": a comma separates the DSN's own endpoints, and a
		// subject list that used one would be ambiguous to read even where
		// the grammar allowed it.
		for part := range strings.SplitSeq(subjects, ";") {
			if part = strings.TrimSpace(part); part != "" {
				cfg.StreamSubjects = append(cfg.StreamSubjects, part)
			}
		}
	}
	if useTLS {
		// A DSN cannot carry a *tls.Config, so "tls=true" means the
		// platform roots and the server name taken from the address, which
		// is the only safe default a connection string can ask for.
		cfg.TLS = &tls.Config{MinVersion: tls.VersionTLS12, InsecureSkipVerify: insecure} //nolint:gosec // the field is the operator's explicit request and Validate refuses it without TLS
		cfg.InsecureSkipVerify = insecure
	} else if insecure {
		// Refusing is the only honest answer: the alternative is a DSN that
		// dialled in the clear after the operator asked to skip
		// verification, which reads as "less safe" and is "no TLS at all".
		return Config{}, errInsecureWithoutTLS
	}
	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

// positiveInt32 reads a positive parameter into an int32 field, refusing a value
// no int32 can hold as well as the zero the struct already means by "not set".
func positiveInt32(p *dsn.Params, key string, dst *int32) error {
	var n int
	if err := p.PositiveInt(key, &n); err != nil {
		return err
	}
	if n == 0 {
		return nil
	}
	if n > math.MaxInt32 {
		return errStreamMsgSize
	}
	*dst = int32(n)
	return nil
}

// The escapers a rendering needs, one per position, so that a rendered DSN
// parses back to the configuration it came from.
//
// The shared parser DECODES every component except a bracketed host, so a
// component can hold any byte and only an escaped one survives the trip.
// Each position escapes exactly the bytes the parser gives
// a meaning to THERE, and no more, for two reasons. A log line is read by a
// person: "deadlettertopic=orders/dlq", "alice@example.com@host" and
// "[::1]:4222" are what they should see. And an escape triples a byte, so a
// rendering that escaped more than its input had to would grow past
// `dsn.MaxLen` and stop parsing, which is a failure the escaping would have
// caused. With these sets a parsed endpoint, user, path or value never renders
// longer than it was written.
//
//   - a user ends at the first ":" (the password) and the authority at "/",
//     "?" or "#"; an "@" in it is safe, because the credential ends at the
//     LAST one;
//   - a host is also split on "," (the next endpoint), on "@" and on ":" (the
//     port), and one that starts with "[" is read as a bracketed literal;
//   - the path runs from the first "/" to the "?";
//   - a query value runs from its "=" to the next "&", and a "+" in it decodes
//     as a space.
//
// "%" is in every set because it begins an escape, and "#" because a DSN with
// one anywhere is refused.
const (
	escUser  = "%:/?#"
	escHost  = "%,@:/?#"
	escPath  = "%?#"
	escQuery = "%+&#"
)

// escapeUser escapes a user name.
func escapeUser(s string) string { return escapeSet(s, escUser) }

// escapePath escapes the path.
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

// renderAddr renders a dial address as a DSN endpoint that parses back to it.
//
// A host with a colon or a "%" in it is written as a bracketed literal, raw,
// which is how an IPv6 address reads and how it came in: the parser takes a
// bracketed host verbatim, without decoding it, so neither needs an escape
// there -- and a "%" escaped outside the brackets would triple, which is the
// one way a parsed host could otherwise render longer than it was written.
// That works unless the host holds a byte the authority is split on before
// the brackets are seen, and such a host is escaped instead, colons and all.
func renderAddr(a string) string {
	host, port := splitAddr(a)
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
// an endpoint into an address: a host with a colon in it is bracketed, and a
// port is the decimal digits after the last colon outside the brackets. It is
// exact for every address that function produces; anything else comes back
// whole as the host.
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

// dialableHost reports whether a host the shared parser DECODED can be written
// into the server URL "nats://<host>:<port>" and still name only itself.
//
// nats.go reads that URL with net/url, which gives "@" (user information), "/"
// (a path), "?" (a query) and "#" (a fragment) a meaning of their own, and a
// DSN writes any of them into a host as an escape: "natsjs://tok%40real-host"
// dialled real-host with the token "tok", and "a%2Fb" dialled "a". nats.go
// also read the server string as a comma-separated LIST before net/url saw it,
// so "nowhere.invalid%2C127.0.0.1" was two servers and the second was dialled.
// A host name or an IP literal holds none of them, nor a backslash, a space or
// a control byte, so such a host is refused rather than escaped: there is no
// spelling of it that dials what the DSN names.
func dialableHost(host string) bool {
	for i := range len(host) {
		switch c := host[i]; {
		case c <= ' ', c == 0x7f, c == '@', c == '/', c == '?', c == '#', c == '\\', c == ',':
			return false
		}
	}
	return true
}

// errDSNHost is the refusal of a host [dialableHost] rejects.
var errDSNHost = errcode.New(errcode.InvalidConfig,
	"iqmq/jetstream: a DSN host decodes to a byte a server URL gives a meaning to -- \"@\", \"/\", \"?\", \"#\", \",\", a backslash, a space or a control character -- so it would dial somewhere the DSN does not name")
