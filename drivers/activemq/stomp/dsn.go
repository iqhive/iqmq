package stomp

import (
	"context"
	"crypto/tls"
	"strconv"
	"strings"

	mq "bitbucket.org/iqhive/iqmq"
	"bitbucket.org/iqhive/iqmq/dsn"
	"bitbucket.org/iqhive/iqmq/internal/activemq"
	"bitbucket.org/iqhive/iqmq/internal/errcode"
)

// Scheme is the DSN scheme this driver answers:
//
//	activemq+stomp://user:pass@localhost:61613?broker=classic&selector=colour%3D%27red%27
//
// It is not "activemq", which `drivers/activemq` claims, and the reason is the specification's "never silently substituted": a registry in which one scheme could
// open either protocol would make a connection string ambiguous about which
// capability table the caller gets, and the two tables really differ -- there
// is no native nack over STOMP and no selector over AMQP 1.0. The "+" is
// RFC 3986's own sub-scheme separator and the shared parser accepts it, so
// `activemq+stomp` reads as what it is.
//
// The BROKER is a required parameter for the same reason it is a required
// field: the two products' destination forms name nothing in common, and on
// Classic a destination's own shape is the only thing that says whether a name
// is a queue or a topic.
const Scheme = "activemq+stomp"

// Register registers this driver with a DSN registry under [Scheme]
// (ADR-012).
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

// ParseDSN turns a DSN under [Scheme] into a [Config].
//
// The parameters, all optional but `broker`:
//
//	broker=artemis|classic     REQUIRED: which ActiveMQ
//	host=<name>                the CONNECT frame's `host` header
//	prefix=<string>            prepended to every destination
//	selector=<jms selector>    a message selector per subscription (Classic)
//	scheduler=true|false       the broker's message scheduler is enabled
//	prioritised=true|false     the broker's destination policy reorders by priority
//	deadlettertopic=<topic>    dead-letter to another of this driver's topics
//	receipts=true|false        ask for a RECEIPT on every publication (default true)
//	prefetch=<n>               deliveries one source buffers
//	prefetchbytes=<n>          the same buffer's byte budget
//	consumerwindow=<n>         Artemis's `consumer-window-size`, in bytes
//	receipttimeout=<duration>  how long to wait for a RECEIPT
//	heartbeatsend=<duration>   the CONNECT frame's outgoing heart-beat
//	heartbeatreceive=<duration> the incoming one
//	dialtimeout=<duration>     bounds one dial
//	reconnectbase, reconnectmax, reconnectgrowth, reconnectattempts, noreconnect
//	tls=true|false, insecureskipverify=true|false, insecureplainlogin=true|false
//	metadata=<mode> the mode
//
// A DSN with a path is refused rather than ignored: neither ActiveMQ has
// virtual hosts, and STOMP's `host` header is a parameter here because it is
// not a path either.
func ParseDSN(s string) (Config, error) {
	c, err := dsn.Parse(s)
	if err != nil {
		return Config{}, err
	}
	if c.Scheme != Scheme {
		return Config{}, mq.ErrDriverNotRegistered
	}
	if c.Path != "" {
		return Config{}, errPathUnsupported
	}
	cfg := Config{
		Addrs:    c.Addrs(),
		Login:    c.Username,
		Passcode: c.Password,
	}
	p := &c.Params
	broker, metadata := "", ""
	useTLS, noReceipts, hadReceipts := false, false, false
	for _, step := range [...]func() error{
		func() error { return p.String("broker", &broker) },
		func() error { return p.String("host", &cfg.Host) },
		func() error { return p.String("prefix", &cfg.Prefix) },
		func() error { return p.String("selector", &cfg.Selector) },
		func() error { return p.String("deadlettertopic", &cfg.DeadLetterTopic) },
		func() error { return p.String("metadata", &metadata) },
		func() error { return p.Bool("scheduler", &cfg.Scheduler) },
		func() error { return p.Bool("prioritised", &cfg.Prioritised) },
		func() error { return p.Bool("tls", &useTLS) },
		func() error { return p.Bool("insecureskipverify", &cfg.InsecureSkipVerify) },
		func() error { return p.Bool("insecureplainlogin", &cfg.InsecurePlainLogin) },
		func() error { return p.Bool("noreconnect", &cfg.NoReconnect) },
		func() error {
			// `receipts` is a tristate on the wire and two booleans in the
			// struct, because the struct's default is ON: a DSN that says
			// nothing must keep it, and `receipts=false` has to turn it off.
			v := true
			if err := p.Bool("receipts", &v); err != nil {
				return err
			}
			hadReceipts = p.Has("receipts")
			noReceipts = hadReceipts && !v
			return nil
		},
		func() error { return p.PositiveInt("prefetch", &cfg.Prefetch) },
		func() error { return int64Param(p, "prefetchbytes", &cfg.PrefetchBytes) },
		func() error { return p.PositiveInt("consumerwindow", &cfg.ConsumerWindow) },
		func() error { return p.PositiveInt("reconnectattempts", &cfg.ReconnectAttempts) },
		func() error { return p.Duration("receipttimeout", &cfg.ReceiptTimeout) },
		func() error { return p.Duration("heartbeatsend", &cfg.HeartbeatSend) },
		func() error { return p.Duration("heartbeatreceive", &cfg.HeartbeatReceive) },
		func() error { return p.Duration("dialtimeout", &cfg.DialTimeout) },
		func() error { return p.Duration("reconnectbase", &cfg.ReconnectBase) },
		func() error { return p.Duration("reconnectmax", &cfg.ReconnectMax) },
		func() error { return floatParam(p, "reconnectgrowth", &cfg.ReconnectGrowth) },
	} {
		if err := step(); err != nil {
			return Config{}, err
		}
	}
	if err := p.Strict(); err != nil {
		return Config{}, err
	}
	cfg.NoReceipts = noReceipts
	if useTLS {
		// A bare `tls=true` asks for the library's defaults, which is what
		// a connection string can express: a certificate pool or a client
		// certificate is a value no string carries, so a deployment that
		// needs one sets Config.TLS itself.
		cfg.TLS = &tls.Config{MinVersion: tls.VersionTLS12}
	}
	if metadata != "" {
		m, ok := parseMetadata(metadata)
		if !ok {
			return Config{}, errBadMetadataParam
		}
		cfg.Metadata = m
	}
	if broker == "" {
		return Config{}, errDSNNoBroker
	}
	b, ok := activemq.ParseBroker(strings.ToLower(broker))
	if !ok {
		return Config{}, errDSNBadBroker
	}
	cfg.Broker = b
	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

// DSN renders the configuration as a connection string with the passcode
// redacted, which is the round trip the specification asks for.
func (c Config) DSN() string {
	r := c.resolve()
	var b strings.Builder
	b.Grow(224)
	b.WriteString(Scheme)
	b.WriteString("://")
	if r.Login != "" {
		b.WriteString(escapeDSN(r.Login))
		if r.Passcode != "" {
			b.WriteByte(':')
			b.WriteString(dsn.Redacted)
		}
		b.WriteByte('@')
	}
	for i, addr := range r.Addrs {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(addr)
	}
	q := make([]string, 0, 24)
	add := func(key, value string) {
		if value != "" {
			q = append(q, key+"="+escapeDSN(value))
		}
	}
	addBool := func(key string, v bool) {
		if v {
			q = append(q, key+"=true")
		}
	}
	q = append(q, "broker="+r.Broker.String())
	add("host", r.Host)
	add("prefix", r.Prefix)
	add("selector", r.Selector)
	add("deadlettertopic", r.DeadLetterTopic)
	add("metadata", metadataName(r.Metadata))
	q = append(q,
		"prefetch="+itoa(r.Prefetch),
		"consumerwindow="+itoa(r.ConsumerWindow),
		"receipttimeout="+r.ReceiptTimeout.String(),
		"dialtimeout="+r.DialTimeout.String(),
	)
	addBool("scheduler", r.Scheduler)
	addBool("prioritised", r.Prioritised)
	// `receipts` is rendered only when it is FALSE, because true is the
	// default and rendering it would make the round trip depend on which of
	// the two struct fields the parser set.
	if !r.Receipts {
		q = append(q, "receipts=false")
	}
	addBool("tls", r.TLS != nil)
	addBool("insecureskipverify", r.InsecureSkipVerify)
	addBool("insecureplainlogin", r.InsecurePlainLogin)
	addBool("noreconnect", r.NoReconnect)
	if r.PrefetchBytes != 0 {
		q = append(q, "prefetchbytes="+strconv.FormatInt(r.PrefetchBytes, 10))
	}
	if r.HeartbeatSend != 0 {
		q = append(q, "heartbeatsend="+r.HeartbeatSend.String())
	}
	if r.HeartbeatReceive != 0 {
		q = append(q, "heartbeatreceive="+r.HeartbeatReceive.String())
	}
	if r.ReconnectBase != 0 {
		q = append(q, "reconnectbase="+r.ReconnectBase.String())
	}
	if r.ReconnectMax != 0 {
		q = append(q, "reconnectmax="+r.ReconnectMax.String())
	}
	if r.ReconnectGrowth != 0 {
		q = append(q, "reconnectgrowth="+strconv.FormatFloat(r.ReconnectGrowth, 'g', -1, 64))
	}
	if r.ReconnectAttempts != 0 {
		q = append(q, "reconnectattempts="+itoa(r.ReconnectAttempts))
	}
	if len(q) > 0 {
		b.WriteByte('?')
		b.WriteString(strings.Join(q, "&"))
	}
	return b.String()
}

// metadataName is the mode as a DSN value, or "" for the zero mode.
func metadataName(m mq.MetadataMode) string {
	switch m {
	case mq.MetadataNativeStrict:
		return ""
	case mq.MetadataPortableEnvelope:
		return "envelope"
	case mq.MetadataAdaptive:
		return "adaptive"
	case mq.MetadataLenientDrop:
		return "lenientdrop"
	}
	return ""
}

// parseMetadata reads the mode from a DSN value.
func parseMetadata(s string) (mq.MetadataMode, bool) {
	switch strings.ToLower(s) {
	case "nativestrict", "strict", "native":
		return mq.MetadataNativeStrict, true
	case "envelope", "portableenvelope":
		return mq.MetadataPortableEnvelope, true
	case "adaptive":
		return mq.MetadataAdaptive, true
	case "lenientdrop", "lenient", "drop":
		return mq.MetadataLenientDrop, true
	}
	return 0, false
}

// floatParam reads a non-negative floating-point parameter.
func floatParam(p *dsn.Params, key string, out *float64) error {
	v, ok := p.Lookup(key)
	if !ok {
		return nil
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil || f < 0 {
		return errBadFloatParam
	}
	*out = f
	return nil
}

// int64Param reads a non-negative 64-bit parameter.
func int64Param(p *dsn.Params, key string, out *int64) error {
	var n int
	if err := p.PositiveInt(key, &n); err != nil {
		return err
	}
	if n > 0 {
		*out = int64(n)
	}
	return nil
}

// escapeDSN percent-encodes the characters that mean something to the shared
// parser, so that a rendered DSN parses back. A JMS selector needs it: it
// carries "=" and "'".
func escapeDSN(s string) string {
	const set = "%+ @:/?#&=[],'"
	if !strings.ContainsAny(s, set) {
		return s
	}
	var b strings.Builder
	b.Grow(len(s) + 8)
	const hex = "0123456789ABCDEF"
	for i := range len(s) {
		ch := s[i]
		if strings.IndexByte(set, ch) >= 0 {
			b.WriteByte('%')
			b.WriteByte(hex[ch>>4])
			b.WriteByte(hex[ch&0xF])
			continue
		}
		b.WriteByte(ch)
	}
	return b.String()
}

// The DSN refusals.
var (
	errPathUnsupported = errcode.New(errcode.InvalidConfig,
		"iqmq/activemq/stomp: neither ActiveMQ has virtual hosts, so a DSN path names nothing; STOMP's own `host` header is the `host` parameter")
	errDSNNoBroker = errcode.New(errcode.InvalidConfig,
		"iqmq/activemq/stomp: the DSN does not say which ActiveMQ: add broker=artemis or broker=classic, because the two products' destination forms name nothing in common")
	errDSNBadBroker = errcode.New(errcode.InvalidConfig,
		"iqmq/activemq/stomp: the DSN's broker parameter is neither \"artemis\" nor \"classic\"")
	errBadFloatParam = errcode.New(errcode.InvalidConfig,
		"iqmq/activemq/stomp: a DSN parameter is not a non-negative number")
	errBadMetadataParam = errcode.New(errcode.InvalidConfig,
		"iqmq/activemq/stomp: the DSN's metadata parameter is not one of the message contract's four modes")
)
