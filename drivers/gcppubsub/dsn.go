package gcppubsub

import (
	"context"
	"strconv"
	"strings"
	"time"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/dsn"
)

// Scheme is the DSN scheme this driver answers:
//
//	gcppubsub://my-project?ordering=true&ackdeadline=30s
//	gcppubsub://127.0.0.1:8085/my-project?insecuretransport=true
//
// The authority is the ENDPOINT when the DSN names one and the PROJECT when it
// does not, which is the one shape that makes both forms readable. A DSN with
// an endpoint puts the project in the path, which is how the emulator is
// reached; a DSN with no endpoint puts the project in the host, because a
// Pub/Sub client with no endpoint talks to the service's own and the only thing
// left to name is the project. The authority names ONE thing: a second host is
// refused, a project has no port, and a project is read as written, without
// the brackets a dial address puts round a colon. The `project` and `endpoint`
// parameters are the other spelling of the same two settings, and naming one
// in both places is refused.
//
// [Config.DSN] writes two more shapes when those two cannot carry a value:
//
//	gcppubsub:///example.com:my-project                       a project the authority would have to escape
//	gcppubsub://my-project?endpoint=https://pubsub.example:443 an endpoint that is not host[:port]
//
// Both parse back to the configuration they came from.
//
// The credential is NOT in the DSN. A Pub/Sub credential is a service-account
// key — a JSON document with a private key in it — and a DSN is a string that
// ends up in logs, environment listings and process tables. `credentialsfile`
// names a path, which is what a deployment has; [Config.CredentialsJSON] is
// reachable only from Go, and [OpenWithClient] is the way in for everything
// else.
const Scheme = DriverName

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

// ParseDSN turns a Pub/Sub DSN into a [Config].
//
// It is [github.com/iqhive/iqmq/dsn.Parse] plus this driver's own parameter
// table: the shared parser owns the grammar and the size bound, and this
// function owns what the parameters mean. Every parameter is read exactly once
// and [github.com/iqhive/iqmq/dsn.Params.Strict] then refuses anything left
// over, so a typo is an error rather than a setting that silently did nothing.
func ParseDSN(s string) (Config, error) {
	parsed, err := dsn.Parse(s)
	if err != nil {
		return Config{}, err
	}
	if parsed.Scheme != Scheme {
		return Config{}, errWrongScheme
	}
	cfg := Config{}
	// The authority names ONE thing: the endpoint when there is a path, the
	// project when there is not. A second host was silently dropped, which
	// is the quietly-ignored setting ADR-010 the specification forbids.
	if len(parsed.Hosts) > 1 {
		return Config{}, errDSNHosts
	}
	switch {
	case parsed.Path != "":
		if len(parsed.Hosts) == 1 {
			// A dial address. A host that decodes to a byte the gRPC
			// target grammar splits on dials somewhere else --
			// "a%3Fb:8085" reached "dns:///a" -- so it is refused rather
			// than passed on.
			if !dialableHost(parsed.Hosts[0].Host) {
				return Config{}, errDSNHost
			}
			cfg.Endpoint = parsed.Hosts[0].String()
		}
		cfg.ProjectID = parsed.Path
	case len(parsed.Hosts) == 1:
		// A NAME, not a dial address: the host as decoded, without the
		// brackets dsn.Host.String puts round one with a colon in it, so
		// the domain-scoped "example.com:my-project" is that project
		// however it was written. A port means nothing on a project, and
		// was kept as part of its name.
		if parsed.Hosts[0].Port != "" {
			return Config{}, errDSNProjectPort
		}
		cfg.ProjectID = parsed.Hosts[0].Host
	}
	p := &parsed.Params
	// The parameters are the other spelling of the two settings the
	// authority and the path name, for a value those cannot carry. Naming a
	// setting in both is refused, as dsn.Parse refuses a repeated parameter:
	// the parameter used to win silently, so "gcppubsub://emulator-host?project=p"
	// dropped the emulator and dialled the managed service.
	if v, ok := p.Lookup("project"); ok {
		if cfg.ProjectID != "" {
			return Config{}, errDSNProjectTwice
		}
		cfg.ProjectID = v
	}
	if v, ok := p.Lookup("endpoint"); ok {
		if cfg.Endpoint != "" {
			return Config{}, errDSNEndpointTwice
		}
		cfg.Endpoint = v
	}
	if v, ok := p.Lookup("credentialsfile"); ok {
		cfg.CredentialsFile = v
	}
	if v, ok := p.Lookup("topicprefix"); ok {
		cfg.TopicPrefix = v
	}
	if v, ok := p.Lookup("subscriptionprefix"); ok {
		cfg.SubscriptionPrefix = v
	}
	if v, ok := p.Lookup("filter"); ok {
		cfg.Filter = v
	}
	if v, ok := p.Lookup("deadlettertopic"); ok {
		cfg.DeadLetterTopic = v
	}
	for _, f := range []struct {
		key string
		dst *bool
	}{
		{"insecuretransport", &cfg.InsecureTransport},
		{"ordering", &cfg.Ordering},
		{"exactlyonce", &cfg.ExactlyOnce},
		{"retainacked", &cfg.RetainAcked},
		{"autocreate", &cfg.AutoCreate},
		{"strictackdeadline", &cfg.StrictAckDeadline},
		{"compression", &cfg.Compression},
	} {
		if v, ok := p.Lookup(f.key); ok {
			b, berr := parseBool(v)
			if berr != nil {
				return Config{}, berr
			}
			*f.dst = b
		}
	}
	for _, f := range []struct {
		key string
		dst *time.Duration
	}{
		{"ackdeadline", &cfg.AckDeadline},
		{"maxextension", &cfg.MaxExtension},
		{"retention", &cfg.Retention},
		{"apitimeout", &cfg.APITimeout},
		{"publishdelay", &cfg.PublishDelayThreshold},
		{"publishtimeout", &cfg.PublishTimeout},
	} {
		if v, ok := p.Lookup(f.key); ok {
			d, derr := time.ParseDuration(v)
			if derr != nil {
				return Config{}, errBadDuration
			}
			*f.dst = d
		}
	}
	for _, f := range []struct {
		key string
		dst *int
	}{
		{"prefetch", &cfg.Prefetch},
		{"maxoutstandingbytes", &cfg.MaxOutstandingBytes},
		{"publishcount", &cfg.PublishCountThreshold},
		{"publishbytes", &cfg.PublishByteThreshold},
		{"completionworkers", &cfg.CompletionWorkers},
		{"completionqueue", &cfg.CompletionQueue},
		{"maxdeliveryattempts", &cfg.MaxDeliveryAttempts},
	} {
		if v, ok := p.Lookup(f.key); ok {
			n, nerr := strconv.Atoi(v)
			if nerr != nil {
				return Config{}, errBadNumber
			}
			*f.dst = n
		}
	}
	if err := p.Strict(); err != nil {
		return Config{}, err
	}
	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

// DSN renders the configuration as a DSN, with every credential redacted
//
// Every non-secret setting the configuration carries is rendered, so the
// result re-parses to the same configuration: a DSN in a log line is something
// an operator can paste back. What it is NOT is a round trip for the
// CREDENTIAL, which is gone from the result on purpose — and
// [Config.CredentialsJSON] has no DSN parameter at all.
//
// A setting the rendering dropped is worse than it sounds and
// FuzzV1CompatDSN found one: `MaxDeliveryAttempts` was rendered only beside a
// dead-letter topic, so a configuration carrying the number alone re-parsed
// without it. The answer was to refuse that configuration rather than to
// render a number that names nothing.
//
// It found a second one in the other direction: a value the parser DECODED,
// written back raw. "gcppubsub:///," is the project "," in the path, and it
// rendered as "gcppubsub://,", an authority of two empty endpoints. Every
// free-text value is escaped for the position it lands in now, and the
// project and the endpoint go in the authority only when [ParseDSN] would read
// them back from there. A project that holds a byte the authority would have
// to escape -- the domain-scoped "example.com:my-project" -- goes in the path
// of an empty authority, where it reads as itself; an endpoint that is not a
// dial address the authority can carry -- "https://…" -- goes in the
// `endpoint` parameter.
func (c Config) DSN() string {
	var b strings.Builder
	b.WriteString(Scheme)
	b.WriteString("://")
	q := make([]string, 0, 8)
	endpoint, endpointOK := authorityFor(c.Endpoint)
	switch {
	case c.Endpoint != "" && endpointOK:
		b.WriteString(endpoint)
		b.WriteByte('/')
		b.WriteString(escapePath(c.ProjectID))
	default:
		// A project goes in the authority when it reads as itself there,
		// and in the path of an empty authority otherwise.
		if c.ProjectID != "" && escapeHost(c.ProjectID) == c.ProjectID {
			b.WriteString(c.ProjectID)
		} else {
			b.WriteByte('/')
			b.WriteString(escapePath(c.ProjectID))
		}
		if c.Endpoint != "" {
			q = append(q, "endpoint="+escapeQuery(c.Endpoint))
		}
	}
	if c.InsecureTransport {
		q = append(q, "insecuretransport=true")
	}
	if c.Ordering {
		q = append(q, "ordering=true")
	}
	if c.ExactlyOnce {
		q = append(q, "exactlyonce=true")
	}
	if c.RetainAcked {
		q = append(q, "retainacked=true")
	}
	if c.AutoCreate {
		q = append(q, "autocreate=true")
	}
	if c.StrictAckDeadline {
		q = append(q, "strictackdeadline=true")
	}
	if c.Compression {
		q = append(q, "compression=true")
	}
	for _, d := range [...]struct {
		key string
		val time.Duration
	}{
		{"ackdeadline", c.AckDeadline},
		{"maxextension", c.MaxExtension},
		{"retention", c.Retention},
		{"apitimeout", c.APITimeout},
		{"publishdelay", c.PublishDelayThreshold},
		{"publishtimeout", c.PublishTimeout},
	} {
		if d.val > 0 {
			q = append(q, d.key+"="+d.val.String())
		}
	}
	for _, n := range [...]struct {
		key string
		val int
	}{
		{"prefetch", c.Prefetch},
		{"maxoutstandingbytes", c.MaxOutstandingBytes},
		{"publishcount", c.PublishCountThreshold},
		{"publishbytes", c.PublishByteThreshold},
		{"completionworkers", c.CompletionWorkers},
		{"completionqueue", c.CompletionQueue},
	} {
		if n.val > 0 {
			q = append(q, n.key+"="+strconv.Itoa(n.val))
		}
	}
	if c.TopicPrefix != "" {
		q = append(q, "topicprefix="+escapeQuery(c.TopicPrefix))
	}
	if c.SubscriptionPrefix != "" {
		q = append(q, "subscriptionprefix="+escapeQuery(c.SubscriptionPrefix))
	}
	if c.DeadLetterTopic != "" {
		q = append(q,
			"deadlettertopic="+escapeQuery(c.DeadLetterTopic),
			"maxdeliveryattempts="+strconv.Itoa(c.MaxDeliveryAttempts),
		)
	}
	if c.Filter != "" {
		q = append(q, "filter="+escapeQuery(c.Filter))
	}
	if c.CredentialsFile != "" {
		q = append(q, "credentialsfile="+escapeQuery(c.CredentialsFile))
	}
	if c.CredentialsJSON != "" {
		q = append(q, "credentialsjson="+dsn.Redacted)
	}
	if len(q) > 0 {
		b.WriteByte('?')
		b.WriteString(strings.Join(q, "&"))
	}
	return b.String()
}

// The escapers the rendering needs, one per position, so that a rendered DSN
// parses back to the configuration it came from. They are the ones
// `drivers/jetstream` carries, with the same reasoning: the shared parser
// DECODES every component except a bracketed host, so each position escapes
// exactly the bytes the parser gives a meaning to THERE -- which keeps a
// filter or a file path readable in a log line, and keeps a parsed component
// from rendering longer than it was written.
//
//   - a host is split on ",", "@" and ":" and ends at "/", "?" or "#", and one
//     that starts with "[" is read as a bracketed literal;
//   - the path runs from the first "/" to the "?";
//   - a query value runs from its "=" to the next "&", and a "+" in it decodes
//     as a space.
const (
	escHost  = "%,@:/?#"
	escPath  = "%?#"
	escQuery = "%+&#"
)

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

// authorityFor renders a as the authority [ParseDSN] reads back as a, and
// reports false when there is none.
//
// ParseDSN takes an address from the authority as `dsn.Host.String` renders
// the first endpoint, so a is representable exactly when it IS such a
// rendering: a host, bracketed when it holds a colon, and an optional decimal
// port. That is checked by rendering the candidate rather than argued, so a
// value from a parameter -- which can be anything -- never gets an authority
// that reads back as something else. A host [ParseDSN] refuses in the
// authority, one [dialableHost] rejects, stays in the parameter.
func authorityFor(a string) (string, bool) {
	host, port := splitAddr(a)
	if host == "" || (dsn.Host{Host: host, Port: port}).String() != a || !dialableHost(host) {
		return "", false
	}
	out := escapeHost(host)
	if strings.ContainsAny(host, ":%") && !strings.ContainsAny(host, "]/?#@,") {
		// The parser takes a bracketed host verbatim, which is how an IPv6
		// literal reads and how it came in, and a "%" kept raw there does not
		// triple the way an escaped one would.
		out = "[" + host + "]"
	}
	if port == "" {
		return out, true
	}
	return out + ":" + port, true
}

// splitAddr is the inverse of `dsn.Host.String`: a host with a colon in it is
// bracketed, and a port is the decimal digits after the last colon outside the
// brackets. Anything else comes back whole as the host.
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

// parseBool accepts the spellings a DSN parameter uses for a flag.
func parseBool(v string) (bool, error) {
	switch strings.ToLower(v) {
	case "1", "true", "yes", "on":
		return true, nil
	case "0", "false", "no", "off":
		return false, nil
	default:
		return false, errBadBool
	}
}

// dialableHost reports whether a host the shared parser DECODED can be handed
// to the Pub/Sub client as an endpoint and still name only itself.
//
// The client passes the endpoint to gRPC as a dial target, which grpc-go reads
// as a URL: "?" and "#" end the host there -- "a%3Fb:8085" dialled
// "dns:///a", measured -- and "@", "/" and a backslash mean something in the
// same grammar. A host name or an IP literal holds none of them, nor a comma,
// a space or a control byte, so such a host is refused rather than escaped.
// An endpoint from the `endpoint` parameter is the
// caller's own dial target, written as the client reads it, and is not
// checked.
func dialableHost(host string) bool {
	for i := range len(host) {
		switch c := host[i]; {
		case c <= ' ', c == 0x7f, c == '@', c == '/', c == '?', c == '#', c == '\\', c == ',':
			return false
		}
	}
	return true
}
