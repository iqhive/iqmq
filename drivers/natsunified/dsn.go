package natsunified

import (
	"context"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/drivers/jetstream"
	"github.com/iqhive/iqmq/drivers/nats"
	"github.com/iqhive/iqmq/dsn"
)

// The two schemes this facade answers to, which are the two profile drivers'
// own:
//
//	nats://user:password@host:4222,host2:4222/prefix?creds=/path.creds&tls=true
//	natsjs://user:password@host:4222/prefix?stream=ORDERS&doubleack=true
//
// The scheme IS the profile and there is no second spelling for it.
// A `profile=` parameter was considered and rejected: two
// ways to say which broker you are talking to is one way too many, and the
// parameter would also have to be refused on the scheme that contradicts it.
// The refusal a caller actually meets is better than that — `nats://` carries
// the Core driver's parameter table, so `nats://host?stream=ORDERS` is refused
// by [github.com/iqhive/iqmq/dsn.Params.Strict] naming `stream`, which says
// what is wrong and not merely that something is.
const (
	// SchemeCore selects [ProfileCore].
	SchemeCore = "nats"
	// SchemeJetStream selects [ProfileJetStream].
	SchemeJetStream = "natsjs"
)

// Schemes returns the schemes this facade answers to, canonical first.
func Schemes() []string { return []string{SchemeCore, SchemeJetStream} }

// Driver returns the facade as a [github.com/iqhive/iqmq/dsn.Driver], which is
// the route that registers both schemes ATOMICALLY:
// `(*dsn.Registry).RegisterDriver` refuses the whole driver when either scheme
// is taken, so a collision never leaves one profile reachable and the other not.
//
// It is the recommended route for this package and the reason it exists at all:
// a single-scheme driver cannot tell the difference, and a two-scheme one can
// (see the maintenance notes — `dsn.Registrar`, the interface every driver's
// `Register` takes, has no atomic form).
func Driver() dsn.Driver { return facadeDriver{} }

// facadeDriver is the [github.com/iqhive/iqmq/dsn.Driver] implementation.
type facadeDriver struct{}

// Name is the facade's registered name.
func (facadeDriver) Name() string { return DriverName }

// Schemes are the two profile schemes, canonical first.
func (facadeDriver) Schemes() []string { return Schemes() }

// Open opens a client from a DSN of either scheme.
func (facadeDriver) Open(ctx context.Context, s string) (*mq.Client, error) {
	return OpenDSN(ctx, s)
}

// driverRegistrar is a registry that can take a whole multi-scheme driver at
// once. [github.com/iqhive/iqmq/dsn.Registry] implements it; the
// [github.com/iqhive/iqmq/dsn.Registrar] interface every driver's `Register`
// takes does not, because it was written for a driver with one scheme.
type driverRegistrar interface {
	RegisterDriver(d dsn.Driver) error
}

// Register registers both profiles with a DSN registry (ADR-012).
//
// It prefers the atomic route: a registry that can take a whole driver gets one,
// so a scheme collision registers neither profile. A registry that can only take
// one scheme at a time gets two calls, and a collision on the second leaves the
// first registered — which is the half-driver state
// `(*dsn.Registry).RegisterDriver` exists to prevent and which this facade
// cannot prevent through an interface that has no atomic form. The fallback is
// here rather than omitted because the specification asks a driver to expose `Register` and
// nothing else, and a facade that refused every registrar but one concrete type
// would not compose.
func Register(r dsn.Registrar) error {
	if r == nil {
		return errNilRegistrar
	}
	if dr, ok := r.(driverRegistrar); ok {
		return dr.RegisterDriver(Driver())
	}
	for _, s := range Schemes() {
		if err := r.Register(s, OpenDSN); err != nil {
			return err
		}
	}
	return nil
}

// OpenDSN opens a client from a DSN of either scheme.
//
// The profile comes from the scheme and from nowhere else, so this is the whole
// of what a caller holding a connection string has to know. A requirement
// cannot be written in a DSN, so a program that needs one parses first:
//
//	cfg, err := natsunified.ParseDSN(s)
//	cfg.Require = []mq.Requirement{mq.RequireAtLeast(mq.FeatureExplicitAck, mq.Native)}
//	client, err := natsunified.Open(ctx, cfg)
func OpenDSN(ctx context.Context, s string) (*mq.Client, error) {
	cfg, err := ParseDSN(s)
	if err != nil {
		return nil, err
	}
	return Open(ctx, cfg)
}

// ParseDSN turns a `nats://` or `natsjs://` DSN into a [Config] with the profile
// the scheme names.
//
// Every parameter is the selected profile driver's own, and so is every refusal:
// this function reads the scheme and hands the whole string to
// `nats.ParseDSN` or `jetstream.ParseDSN`. That costs one extra pass over the
// string — the grammar is parsed twice, once here for the scheme and once in the
// driver — and it buys the thing that matters, which is that neither parameter
// table is duplicated here to go stale. A facade with its own copy of the
// twenty-nine JetStream parameters would be the second place a DSN parameter has
// to be added, and the one nobody remembers.
//
// See `nats.ParseDSN` and `jetstream.ParseDSN` for the two parameter tables.
func ParseDSN(s string) (Config, error) {
	p, err := ProfileForDSN(s)
	if err != nil {
		return Config{}, err
	}
	switch p {
	case ProfileCore:
		cfg, err := nats.ParseDSN(s)
		if err != nil {
			return Config{}, err
		}
		return Config{Profile: ProfileCore, Core: cfg}, nil
	case ProfileJetStream:
		cfg, err := jetstream.ParseDSN(s)
		if err != nil {
			return Config{}, err
		}
		return Config{Profile: ProfileJetStream, JetStream: cfg}, nil
	case ProfileUnset:
		return Config{}, errUnknownScheme
	default:
		return Config{}, errUnknownScheme
	}
}

// ProfileForDSN is the profile a DSN selects, without parsing the rest of it.
//
// It exists because a caller often needs the profile before it needs a client —
// to decide which half of its own configuration to read, to log which broker it
// is about to talk to, or to refuse a deployment whose DSN names the wrong one.
// The grammar is still the shared parser's, so a DSN this refuses for a bad
// scheme is a DSN [ParseDSN] would have refused too.
func ProfileForDSN(s string) (Profile, error) {
	c, err := dsn.Parse(s)
	if err != nil {
		return ProfileUnset, err
	}
	return ProfileForScheme(c.Scheme)
}

// compile-time proof that the facade is a registerable driver.
var _ dsn.Driver = facadeDriver{}
