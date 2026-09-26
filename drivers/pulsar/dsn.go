package pulsar

import (
	"context"
	"strings"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/dsn"
)

// Scheme is the DSN scheme this driver answers:
//
//	pulsar://broker:6650/tenant/namespace?admin=http://broker:8080&subscriptiontype=keyshared
const Scheme = DriverName

// SchemeTLS is the DSN scheme for a TLS endpoint, which is Pulsar's own
// spelling: `pulsar+ssl://`.
//
// It is a SECOND registration rather than a parameter, because it is what
// Pulsar's own tooling, documentation and service URLs use — a driver that
// accepted only `pulsar://?tls=true` would refuse the connection string an
// operator already has.
const SchemeTLS = DriverName + "+ssl"

// DefaultPort is the port a DSN endpoint that names none is dialled on: the
// binary protocol's, not the administrative API's.
const DefaultPort = "6650"

// DefaultTLSPort is the port a `pulsar+ssl://` endpoint that names none is
// dialled on.
const DefaultTLSPort = "6651"

// Register registers this driver with a DSN registry under both [Scheme] and
// [SchemeTLS] (ADR-012).
//
// A driver exposes this and nothing else: it never reaches for a global
// registry of its own, so a library that composes two brokers registers each of
// them with a registry it owns.
func Register(r dsn.Registrar) error {
	if r == nil {
		return errNilRegistrar
	}
	if err := r.Register(Scheme, OpenDSN); err != nil {
		return err
	}
	return r.Register(SchemeTLS, OpenDSN)
}

// OpenDSN opens a client from a DSN.
func OpenDSN(ctx context.Context, s string) (*mq.Client, error) {
	cfg, err := ParseDSN(s)
	if err != nil {
		return nil, err
	}
	return Open(ctx, cfg)
}

// ParseDSN turns a Pulsar DSN into a [Config].
//
// It is [github.com/iqhive/iqmq/dsn.Parse] plus this driver's own parameter
// table, which is the point: the shared parser owns the grammar, the 8 KiB
// bound, the percent decoding and the credential redaction, and
// [github.com/iqhive/iqmq/dsn.Params.Strict] turns a parameter this driver does
// not have into a construction error naming it, rather than a setting nobody
// notices was ignored (ADR-010).
//
// The authority is the broker's binary-protocol endpoint. The PATH is the
// tenant and namespace — `/tenant/namespace`, or `/tenant/namespace/prefix` —
// because that is how Pulsar's own service URLs read and because a topic
// carries no tenant of its own here (see [TopicOf]).
//
// The parameters, all optional:
//
//	admin=<url>                the HTTP administrative endpoint
//	domain=persistent|non-persistent
//	token=<secret>             a JWT bearer token
//	authname=<name>            an authentication provider ("tls", "oauth2", …)
//	authparams=<secret>        that provider's parameters
//	insecureskipverify=true|false   with pulsar+ssl, do not verify the broker
//	trustcerts=<path>          the CA file pulsar+ssl verifies the broker with,
//	                           in place of the system roots
//	listenername=<name>        an advertised listener
//	connectiontimeout=<duration>
//	operationtimeout=<duration>
//	keepaliveinterval=<duration>
//	maxconnectionsperbroker=<n>
//	memorylimitbytes=<n>
//	transactions=true|false    enable the transaction coordinator (x/pulsartx)
//	producername=<name>
//	disablebatching=true|false
//	batchingmaxpublishdelay=<duration>
//	batchingmaxmessages=<n>
//	batchingmaxsize=<n>
//	maxpendingmessages=<n>
//	blockifqueuefull=true|false
//	sendtimeout=<duration>
//	compression=none|lz4|zlib|zstd|snappy
//	hashing=javastring|murmur3
//	subscriptiontype=shared|keyshared|exclusive|failover
//	allowoutoforderdelivery=true|false   (Key_Shared only)
//	receiverqueuesize=<n>
//	ackgroupingmaxsize=<n>
//	ackgroupingmaxtime=<duration>
//	ackwithresponse=true|false
//	enablebatchindexack=true|false   (needs admin; see Config)
//	nackredeliverydelay=<duration>
//	retryenable=true|false
//	retrytopic=<topic>
//	deadlettertopic=<topic>
//	maxattempts=<n>
//	acktimeout=<duration>
//	disableacktimeout=true|false
//	leaseguard=<duration>
//	readcompacted=true|false
//	autodiscoveryperiod=<duration>
//	pingtopic=<topic>
//	metadata=nativestrict|portableenvelope|adaptive|lenientdrop
//	maxmessagebytes=<n>
//	ensuretopology=true|false
//	partitions=<n>
//
// Prefetch is deliberately NOT a parameter. It is the CORE's: it is resolved
// from the consumer configuration and handed to the driver in
// `SourceConfig.Prefetch`, and a DSN that set it would be a second owner of one
// number — the driver would then have to choose between them, silently.
// `receiverqueuesize` is a different number: it is the per-PARTITION buffer
// behind the prefetch, and it defaults to the prefetch when unset.
func ParseDSN(s string) (Config, error) {
	c, err := dsn.Parse(s)
	if err != nil {
		return Config{}, err
	}
	if c.Scheme != Scheme && c.Scheme != SchemeTLS {
		return Config{}, mq.ErrDriverNotRegistered
	}
	port := DefaultPort
	if c.Scheme == SchemeTLS {
		port = DefaultTLSPort
	}
	var cfg Config
	hosts := make([]string, 0, len(c.Hosts))
	for _, h := range c.Hosts {
		if !dialableHost(h.Host) {
			return Config{}, errDSNHost
		}
		if h.Port == "" {
			h.Port = port
		}
		hosts = append(hosts, h.String())
	}
	if len(hosts) == 0 {
		return Config{}, errNoEndpoint
	}
	cfg.ServiceURL = c.Scheme + "://" + strings.Join(hosts, ",")
	if err := applyPath(&cfg, c.Path); err != nil {
		return Config{}, err
	}
	p := &c.Params
	var token, authParams string
	var maxAttempts int
	for _, step := range [...]func() error{
		func() error { return p.String("admin", &cfg.AdminURL) },
		func() error { return p.String("domain", &cfg.Domain) },
		func() error { return p.String("token", &token) },
		func() error { return p.String("authname", &cfg.AuthName) },
		func() error { return p.String("authparams", &authParams) },
		func() error { return p.Bool("insecureskipverify", &cfg.InsecureSkipVerify) },
		func() error { return p.String("trustcerts", &cfg.TLSTrustCertsFilePath) },
		func() error { return p.String("listenername", &cfg.ListenerName) },
		func() error { return p.Duration("connectiontimeout", &cfg.ConnectionTimeout) },
		func() error { return p.Duration("operationtimeout", &cfg.OperationTimeout) },
		func() error { return p.Duration("keepaliveinterval", &cfg.KeepAliveInterval) },
		func() error { return p.Int("maxconnectionsperbroker", &cfg.MaxConnectionsPerBroker) },
		func() error { return p.Int64("memorylimitbytes", &cfg.MemoryLimitBytes) },
		func() error { return p.Bool("transactions", &cfg.EnableTransactions) },
		func() error { return p.String("producername", &cfg.ProducerName) },
		func() error { return p.Bool("disablebatching", &cfg.DisableBatching) },
		func() error { return p.Duration("batchingmaxpublishdelay", &cfg.BatchingMaxPublishDelay) },
		func() error { return p.Int("batchingmaxmessages", &cfg.BatchingMaxMessages) },
		func() error { return p.Int("batchingmaxsize", &cfg.BatchingMaxSize) },
		func() error { return p.Int("maxpendingmessages", &cfg.MaxPendingMessages) },
		func() error { return p.Bool("blockifqueuefull", &cfg.BlockIfQueueFull) },
		func() error { return p.Duration("sendtimeout", &cfg.SendTimeout) },
		func() error { return compressionParam(p, &cfg.Compression) },
		func() error { return hashingParam(p, &cfg.Hashing) },
		func() error { return subscriptionParam(p, &cfg.SubscriptionType) },
		func() error { return p.Bool("allowoutoforderdelivery", &cfg.AllowOutOfOrderDelivery) },
		func() error { return p.Int("receiverqueuesize", &cfg.ReceiverQueueSize) },
		func() error { return p.Int("ackgroupingmaxsize", &cfg.AckGroupingMaxSize) },
		func() error { return p.Duration("ackgroupingmaxtime", &cfg.AckGroupingMaxTime) },
		func() error { return p.Bool("ackwithresponse", &cfg.AckWithResponse) },
		func() error { return p.Bool("enablebatchindexack", &cfg.EnableBatchIndexAck) },
		func() error { return p.Duration("nackredeliverydelay", &cfg.NackRedeliveryDelay) },
		func() error { return p.Bool("retryenable", &cfg.RetryEnable) },
		func() error { return p.String("retrytopic", &cfg.RetryTopic) },
		func() error { return p.String("deadlettertopic", &cfg.DeadLetterTopic) },
		func() error { return p.Int("maxattempts", &maxAttempts) },
		func() error { return p.Duration("acktimeout", &cfg.AckTimeout) },
		func() error { return p.Bool("disableacktimeout", &cfg.DisableAckTimeout) },
		func() error { return p.Duration("leaseguard", &cfg.LeaseGuard) },
		func() error { return p.Bool("readcompacted", &cfg.ReadCompacted) },
		func() error { return p.Duration("autodiscoveryperiod", &cfg.AutoDiscoveryPeriod) },
		func() error { return p.String("pingtopic", &cfg.PingTopic) },
		func() error { return metadataParam(p, &cfg.Metadata) },
		func() error { return p.Int64("maxmessagebytes", &cfg.MaxMessageBytes) },
		func() error { return p.Bool("ensuretopology", &cfg.EnsureTopology) },
		func() error { return p.Int("partitions", &cfg.Partitions) },
	} {
		if err := step(); err != nil {
			return Config{}, err
		}
	}
	if err := p.Strict(); err != nil {
		return Config{}, err
	}
	cfg.Token, cfg.AuthParams = dsn.Secret(token), dsn.Secret(authParams)
	if maxAttempts < 0 {
		return Config{}, errNegativeBound
	}
	cfg.MaxAttempts = uint32(maxAttempts)
	// A pulsar+ssl DSN leaves Config.TLS nil, and that is what makes its TLS
	// parameters work. The scheme alone turns TLS on inside
	// pulsar-client-go, which then builds the configuration from
	// trustcerts (TLSTrustCertsFilePath), insecureskipverify
	// (TLSAllowInsecureConnection) and an authname=tls provider's client
	// certificate -- but only when no *tls.Config is given: v0.19.0's
	// connection.getTLSConfig returns a non-nil TLSConfig unchanged. This
	// used to set one here, so a DSN's trust store was never read (the
	// broker was verified against the system roots, which WIDENS what is
	// trusted) and its TLS client certificate was never presented.
	// The server name still comes from the address
	// and TLS 1.2 is still the floor: those are crypto/tls's own defaults
	// for a client.
	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

// applyPath reads the tenant, namespace and optional topic prefix out of a
// DSN's path.
//
// Pulsar's own service URLs put the namespace in the topic rather than in the
// connection string, so there is no established spelling to copy. The path is
// the natural place — `pulsar://broker:6650/acme/orders` reads as "the orders
// namespace of the acme tenant" — and the third segment is the topic prefix,
// which is what lets two iqmq deployments share one namespace.
func applyPath(cfg *Config, path string) error {
	path = strings.Trim(path, "/")
	if path == "" {
		return nil
	}
	parts := strings.Split(path, "/")
	if len(parts) > 3 {
		return errBadDSNPath
	}
	cfg.Tenant = parts[0]
	if len(parts) > 1 {
		cfg.Namespace = parts[1]
	}
	if len(parts) > 2 {
		cfg.TopicPrefix = parts[2]
	}
	return nil
}

// subscriptionParam reads the subscription type by its canonical lower-case
// name, which is the one SubscriptionKind.String() prints — so the DSN, the
// conformance report's profile and every log line that renders a type agree.
func subscriptionParam(p *dsn.Params, dst *SubscriptionKind) error {
	names := make([]string, 0, subscriptionKindCount)
	for k := SubscriptionKind(0); k.Valid(); k++ {
		names = append(names, k.String())
	}
	return p.Enum("subscriptiontype", names, func(i int) { *dst = SubscriptionKind(i) })
}

// compressionParam reads the compression kind by name.
func compressionParam(p *dsn.Params, dst *CompressionKind) error {
	names := make([]string, 0, compressionKindCount)
	for k := CompressionKind(0); k.Valid(); k++ {
		names = append(names, k.String())
	}
	return p.Enum("compression", names, func(i int) { *dst = CompressionKind(i) })
}

// hashingParam reads the hashing kind by name.
func hashingParam(p *dsn.Params, dst *HashingKind) error {
	names := make([]string, 0, hashingKindCount)
	for k := HashingKind(0); k.Valid(); k++ {
		names = append(names, k.String())
	}
	return p.Enum("hashing", names, func(i int) { *dst = HashingKind(i) })
}

// metadataParam reads the mode by its canonical lower-case name.
func metadataParam(p *dsn.Params, dst *mq.MetadataMode) error {
	names := make([]string, 0, 4)
	for m := mq.MetadataNativeStrict; m.Valid(); m++ {
		names = append(names, m.String())
	}
	return p.Enum("metadata", names, func(i int) { *dst = mq.MetadataMode(i) })
}

// DSN renders the configuration as a connection string with every credential
// redacted, which is what the specification requires of a round trip a person may paste into
// a log.
//
// It does NOT round-trip to the same Config: a redacted DSN cannot carry the
// token back, and a *tls.Config cannot be rendered at all. That is deliberate —
// a rendering that round-tripped would be a rendering that carried the
// credential.
func (c Config) DSN() string {
	scheme := Scheme
	url := c.serviceURL()
	if strings.HasPrefix(url, "pulsar+ssl://") {
		scheme = SchemeTLS
	}
	var b strings.Builder
	b.WriteString(renderServiceURL(url))
	b.WriteString("/")
	b.WriteString(escapePath(orDefault(c.Tenant, DefaultTenant)))
	b.WriteString("/")
	b.WriteString(escapePath(orDefault(c.Namespace, DefaultNamespace)))
	if c.TopicPrefix != "" {
		b.WriteString("/")
		b.WriteString(escapePath(c.TopicPrefix))
	}
	sep := "?"
	put := func(k, v string) {
		if v == "" {
			return
		}
		b.WriteString(sep)
		sep = "&"
		b.WriteString(k)
		b.WriteString("=")
		b.WriteString(escapeQuery(v))
	}
	put("admin", c.AdminURL)
	put("subscriptiontype", c.SubscriptionType.String())
	put("metadata", c.Metadata.String())
	if c.Token != "" {
		put("token", c.Token.String())
	}
	if c.AuthName != "" {
		put("authname", c.AuthName)
		put("authparams", c.AuthParams.String())
	}
	put("deadlettertopic", c.DeadLetterTopic)
	_ = scheme
	return b.String()
}

// dialableHost reports whether a host the shared parser DECODED can be written
// into the service URL [ParseDSN] builds and still name only itself.
//
// pulsar-client-go parses that URL with its own service-URI grammar
// (pulsar/internal/service_uri.go): it splits the host list on "," AND on ";"
// (splitURI), ends the authority at "/", "?" or "#", and takes everything
// before the LAST "@" as user information. Measured against v0.19.0 by reading
// the client's own "Connecting to broker" lines and counting the TCP
// connections that reached a local listener:
//
//	pulsar://tok%40real-host:6650  connected to pulsar://real-host:6650
//	pulsar://a%2Fb:6650            connected to pulsar://a:6650, port lost
//	pulsar://a%3Fb:6650            the same
//	pulsar://a%23b:6650            the same
//	pulsar://a%5Cb:6650            "Invalid service URL: InvalidConfiguration"
//	pulsar://nowhere.invalid%2C127.0.0.1:P  TWO brokers; the listener was
//	                                        connected to, twice
//	pulsar://nowhere.invalid%3B127.0.0.1:P  the same
//
// The last two are why this set is larger than the one drivers/rabbitmq,
// drivers/amqp10, drivers/sqs and drivers/snssqs use: those four client
// libraries read ONE address and kept "a,b" as the host, and none of them
// splits on ";" either. The separator that matters is the client library's.
//
// A host name or an IP literal holds none of these bytes, so such a host is
// refused rather than escaped: there is no spelling of it that dials what the
// DSN names.
func dialableHost(host string) bool {
	for i := range len(host) {
		switch c := host[i]; {
		case c <= ' ', c == 0x7f, c == '@', c == '/', c == '?', c == '#', c == '\\', c == ',', c == ';':
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
//   - a host is split on "," (the next endpoint), on ":" (the port) and, for
//     this driver, on ";" too, because the client library splits its host list
//     on both; one that starts with "[" is read as a bracketed literal. "@",
//     "/", "?" and "#" are in the set for a TYPED ServiceURL only -- a DSN can
//     no longer decode one into a host ([dialableHost]) -- so that such a host
//     never renders as a different host;
//   - the path is split on "/" into tenant, namespace and prefix (applyPath),
//     and ends at the "?";
//   - a query value runs from its "=" to the next "&", and a "+" in it decodes
//     as a space.
//
// "%" is in every set because it begins an escape, and "#" because a DSN with
// one anywhere is refused.
const (
	escHost  = "%,;:@/?#"
	escPath  = "%/?#"
	escQuery = "%+&#"
)

// escapePath escapes one segment of the DSN's path.
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

// renderServiceURL turns a Pulsar service URL into the DSN authority that
// parses back to it: the scheme is kept (a DSN's scheme names the protocol and
// "pulsar+ssl" is a scheme of its own) and each host is escaped for the
// position it lands in.
//
// A host with a colon or a "%" in it is written as a bracketed literal, raw,
// which is how an IPv6 address reads and how it came in: the parser takes a
// bracketed host verbatim, without decoding it, so neither needs an escape
// there -- and a "%" escaped outside the brackets would triple, which is the
// one way a parsed host could otherwise render longer than it was written.
//
// Anything a TYPED ServiceURL carries after the authority is dropped, as it
// was before: this DSN's own path is the tenant and namespace, which is
// written next.
func renderServiceURL(url string) string {
	scheme, rest, ok := strings.Cut(url, "://")
	if !ok {
		scheme, rest = Scheme, url
	}
	if i := strings.IndexAny(rest, "/?#"); i >= 0 {
		rest = rest[:i]
	}
	// Split on BOTH separators pulsar-client-go's splitURI recognises, and
	// emit the hosts joined by the DSN's own ",". A typed ServiceURL may use
	// ";" -- the library accepts it and `pulsar.NewClient` dials both brokers
	// -- and the first round split on "," alone, so the ";" fell inside a
	// host, was escaped to "%3B", and the rendering was then refused by this
	// driver's own dialableHost. That was a regression against main, which
	// rendered "pulsar://a;b:6650" unchanged and parsed it back.
	hosts := strings.FieldsFunc(rest, func(r rune) bool { return r == ',' || r == ';' })
	for i, h := range hosts {
		host, port := splitAddr(h)
		out := escapeHost(host)
		if bracketable(host) {
			out = "[" + host + "]"
		}
		if port != "" {
			out += ":" + port
		}
		hosts[i] = out
	}
	return scheme + "://" + strings.Join(hosts, ",")
}

// bracketable reports whether host is better written as a raw bracketed
// literal: it has a byte an unbracketed host would have to escape and that a
// bracketed one keeps verbatim, and none that splits the authority first.
func bracketable(host string) bool {
	return strings.ContainsAny(host, ":%") && !strings.ContainsAny(host, "]/?#@,;")
}

// splitAddr is the inverse of `dsn.Host.String`, which is how [ParseDSN] turns
// a DSN endpoint into one entry of the service URL: a host with a colon in it
// is bracketed, and a port is the decimal digits after the last colon outside
// the brackets.
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
