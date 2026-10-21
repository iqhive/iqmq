package memory

import (
	"context"
	"net/url"
	"strconv"
	"strings"
	"time"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/dsn"
	"github.com/iqhive/iqmq/internal/errcode"
)

// The DSN schemes this driver answers to (the API contract):
//
//	memory://?partitions=8&capacity=100000&overflow=block&wildcards=true&autocreate=true
//
// mem:// is the alias. The specification names both, and a URL with no host and no
// path is the whole of a memory DSN: there is nothing to connect to.
const (
	// Scheme is the driver's DSN scheme.
	Scheme = DriverName
	// SchemeAlias is the short spelling the specification also names.
	SchemeAlias = "mem"
)

// Register registers this driver with a DSN registry, under [Scheme] and
// [SchemeAlias] (ADR-012).
//
// A driver exposes this and nothing else: it never reaches for a global
// registry of its own, so a library that composes two brokers registers each
// of them with a registry it owns. The parameter is
// [github.com/iqhive/iqmq/dsn.Registrar] — the registry half a driver needs —
// rather than an interface declared here, which is the specification's
// placeholder retired now that the specification has built the registry.
func Register(r dsn.Registrar) error {
	if r == nil {
		return errNilRegistrar
	}
	for _, scheme := range [...]string{Scheme, SchemeAlias} {
		if err := r.Register(scheme, OpenDSN); err != nil {
			return err
		}
	}
	return nil
}

// OpenDSN opens a client from a DSN.
func OpenDSN(ctx context.Context, dsn string) (*mq.Client, error) {
	cfg, err := ParseDSN(dsn)
	if err != nil {
		return nil, err
	}
	return Open(ctx, cfg)
}

// ParseDSN turns a memory DSN into a configuration.
//
// It is a pure function with explicit bounds and it never panics, which is the rule for every parser in this repository: the bytes may come from an
// environment variable a stranger set. Unknown parameters are an error rather
// than a warning — ADR-010's — because a DSN that silently ignored
// "capacity=100" would give a caller a broker they did not ask for.
//
// The parameters, all optional:
//
//	autocreate=true|false      create a topic the first time it is named
//	partitions=<n>             lanes per subscription; auto for GOMAXPROCS
//	capacity=<n>               records per lane; unbounded for no limit
//	capacitybytes=<n>          accounted bytes per lane
//	overflow=block|reject|dropnew|dropold
//	fanout=atomic|independent|besteffort|dropslow
//	ordering=relaxed|strict
//	kind=queue|topic|stream
//	ackdeadline=<duration>     lease duration; never for no expiry
//	maxattempts=<n>            deliveries before the dead-letter route
//	deadletter=<topic>
//	maxmessagesize=<n>
//	dedupwindow=<duration>
//	priority=true|false
//	wildcards=true|false
//	maxtotalmessages=<n>
//	maxtotalbytes=<n>
//	zeroonrelease=true|false
//	engine=safe                pin every variant family to its reference kind
func ParseDSN(dsn string) (Config, error) {
	if len(dsn) > maxDSNLen {
		return Config{}, errDSNTooLong
	}
	u, err := url.Parse(dsn)
	if err != nil {
		return Config{}, errDSNSyntax
	}
	if u.Scheme != Scheme && u.Scheme != SchemeAlias {
		return Config{}, mq.ErrDriverNotRegistered
	}
	if u.Opaque != "" || u.User != nil || u.Host != "" || strings.Trim(u.Path, "/") != "" || u.Fragment != "" {
		// There is nothing to connect to and no path to address, so
		// anything in those positions is a DSN for another driver or a
		// mistake. Either way it is not silently dropped.
		return Config{}, errDSNAddress
	}
	q, err := url.ParseQuery(u.RawQuery)
	if err != nil {
		return Config{}, errDSNSyntax
	}
	var cfg Config
	for key, vals := range q {
		if len(vals) != 1 {
			return Config{}, errDSNRepeated
		}
		if err := applyDSN(&cfg, key, vals[0]); err != nil {
			return Config{}, err
		}
	}
	// The configuration is validated by building nothing: a DSN that names
	// an impossible topology must fail where it was written, not at the
	// first publication.
	if _, err := cfg.DefaultTopology.resolve(); err != nil {
		return Config{}, err
	}
	if !cfg.Engine.Valid() {
		return Config{}, errEngineKinds
	}
	return cfg, nil
}

// maxDSNLen bounds what ParseDSN will look at. A DSN is a handful of
// parameters; a megabyte of them is an attack or a mistake, and the specification asks a
// parser to have an explicit limit rather than an implicit one.
const maxDSNLen = 8 << 10

// applyDSN applies one parameter.
func applyDSN(cfg *Config, key, val string) error {
	switch key {
	case "autocreate":
		return parseBool(val, func(v bool) { cfg.AutoCreate = v })
	case "partitions":
		if val == "auto" {
			cfg.DefaultTopology.Partitions = -1
			return nil
		}
		return parsePositive(val, func(v int) { cfg.DefaultTopology.Partitions = v })
	case "capacity":
		if val == "unbounded" {
			cfg.DefaultTopology.CapacityMessages = -1
			return nil
		}
		return parsePositive(val, func(v int) { cfg.DefaultTopology.CapacityMessages = v })
	case "capacitybytes":
		return parsePositive64(val, func(v int64) { cfg.DefaultTopology.CapacityBytes = v })
	case "overflow":
		return parseNamed(val, overflowPolicyNames[:], func(i int) {
			cfg.DefaultTopology.Overflow = OverflowPolicy(i)
		})
	case "fanout":
		return parseNamed(val, fanoutPolicyNames[:], func(i int) {
			cfg.DefaultTopology.Fanout = FanoutPolicy(i)
		})
	case "ordering":
		return parseNamed(val, orderingModeNames[:], func(i int) {
			cfg.DefaultTopology.Ordering = OrderingMode(i)
		})
	case "kind":
		return parseNamed(val, topologyKindNames[:], func(i int) {
			cfg.DefaultTopology.Kind = TopologyKind(i)
		})
	case "ackdeadline":
		if val == "never" {
			cfg.DefaultTopology.AckDeadline = -1
			return nil
		}
		return parseDuration(val, func(v time.Duration) { cfg.DefaultTopology.AckDeadline = v })
	case "maxattempts":
		// Parsed at the field's own width: a wider count would wrap, and
		// 1<<32 would come out as zero, which is unlimited attempts.
		n, err := strconv.ParseUint(val, 10, 32)
		if err != nil || n == 0 {
			return errDSNValue
		}
		cfg.DefaultTopology.MaxAttempts = uint32(n)
		return nil
	case "deadletter":
		if err := mq.ValidateTopic(val); err != nil {
			return err
		}
		cfg.DefaultTopology.DeadLetterTopic = val
		return nil
	case "maxmessagesize":
		return parsePositive(val, func(v int) { cfg.DefaultTopology.MaxMessageSize = v })
	case "dedupwindow":
		return parseDuration(val, func(v time.Duration) { cfg.DefaultTopology.DedupWindow = v })
	case "priority":
		return parseBool(val, func(v bool) { cfg.DefaultTopology.Priority = v })
	case "wildcards":
		return parseBool(val, func(v bool) { cfg.DefaultTopology.Wildcards = v })
	case "retentionmessages":
		return parsePositive(val, func(v int) { cfg.DefaultTopology.Retention.MaxMessages = v })
	case "retentionbytes":
		return parsePositive64(val, func(v int64) { cfg.DefaultTopology.Retention.MaxBytes = v })
	case "retentionage":
		return parseDuration(val, func(v time.Duration) { cfg.DefaultTopology.Retention.MaxAge = v })
	case "maxtotalmessages":
		return parsePositive(val, func(v int) { cfg.MaxTotalMessages = v })
	case "maxtotalbytes":
		return parsePositive64(val, func(v int64) { cfg.MaxTotalBytes = v })
	case "zeroonrelease":
		return parseBool(val, func(v bool) { cfg.ZeroOnRelease = v })
	case "engine":
		if val != "safe" {
			return errDSNValue
		}
		cfg.Engine.Safe = true
		return nil
	default:
		return errDSNUnknownParam
	}
}

// parseBool reads a boolean parameter. Only the two spellings a DSN is
// written with are accepted: "1" and "t" are strconv's, not a DSN's.
func parseBool(val string, set func(bool)) error {
	switch val {
	case "true":
		set(true)
	case "false":
		set(false)
	default:
		return errDSNValue
	}
	return nil
}

// parsePositive reads a strictly positive integer parameter. Zero is refused
// because zero is what the struct already means by "not set", and a DSN that
// said so would be indistinguishable from one that did not.
func parsePositive(val string, set func(int)) error {
	n, err := strconv.Atoi(val)
	if err != nil || n <= 0 {
		return errDSNValue
	}
	set(n)
	return nil
}

// parsePositive64 is parsePositive for a 64-bit field.
func parsePositive64(val string, set func(int64)) error {
	n, err := strconv.ParseInt(val, 10, 64)
	if err != nil || n <= 0 {
		return errDSNValue
	}
	set(n)
	return nil
}

// parseDuration reads a positive duration parameter in Go's own notation.
func parseDuration(val string, set func(time.Duration)) error {
	d, err := time.ParseDuration(val)
	if err != nil || d <= 0 {
		return errDSNValue
	}
	set(d)
	return nil
}

// parseNamed reads a parameter whose values are an enumeration's names. The
// zero name — every one of these enumerations calls it "default" or "auto" —
// is not accepted from a DSN: naming the default explicitly and omitting it
// are the same thing, and accepting the spelling invites a reader to think
// they differ.
func parseNamed(val string, names []string, set func(int)) error {
	for i, n := range names {
		if i == 0 || n != val {
			continue
		}
		set(i)
		return nil
	}
	return errDSNValue
}

// The DSN failures. The specification names no sentinel for a malformed DSN and
// errors.Is matches by code, so a caller compares mq.CodeOf(err) against
// mq.CodeInvalidConfig.
var (
	errNilRegistrar    = errcode.New(errcode.InvalidArgument, "iqmq/memory: Register was given a nil Registrar")
	errDSNSyntax       = errcode.New(errcode.InvalidConfig, "iqmq/memory: the DSN is not a URL")
	errDSNTooLong      = errcode.New(errcode.InvalidConfig, "iqmq/memory: the DSN is longer than 8 KiB")
	errDSNAddress      = errcode.New(errcode.InvalidConfig, "iqmq/memory: a memory DSN must name no host, user, path or fragment (it has nothing to connect to): write memory:// or memory://?autocreate=true")
	errDSNRepeated     = errcode.New(errcode.InvalidConfig, "iqmq/memory: the DSN repeats a parameter")
	errDSNUnknownParam = errcode.New(errcode.InvalidConfig, "iqmq/memory: the DSN names a parameter this driver does not have")
	errDSNValue        = errcode.New(errcode.InvalidConfig, "iqmq/memory: the DSN gives a parameter a value it cannot take")
)
