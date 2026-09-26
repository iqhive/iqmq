package natsunified

import (
	"log/slog"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/drivers/jetstream"
	"github.com/iqhive/iqmq/drivers/nats"
	"github.com/iqhive/iqmq/internal/errcode"
)

// DriverName is the facade's own registered name. It is what the conformance
// report is keyed by and what `docs/drivers/natsunified.md` documents; it is
// NOT what `Client.Info().Name` answers, which is the profile driver's own name.
const DriverName = "natsunified"

// Version is the version of this facade module. It is empty until the module is
// tagged: the specification makes an empty string "the driver did not say" rather than a
// claim.
const Version = ""

// The names of the two drivers this facade delegates to, held as constants so
// that [Profile.Driver] does not depend on either package's spelling drifting.
// They are asserted against `nats.DriverName` and `jetstream.DriverName` by
// TestProfileDriverNamesMatchTheDrivers, because a constant copied from another
// package is a constant that can go stale.
const (
	coreDriverName      = "nats"
	jetStreamDriverName = "jetstream"
)

// Config is the facade's configuration: which profile, that profile's own
// typed configuration, and the capability requirements the client must meet.
//
// # Why it is two configurations and not one
//
// The obvious facade carries ONE field set and translates it into whichever
// driver configuration the profile selects. This one does not, and the reason
// is arithmetic rather than taste. The two driver
// configurations share nineteen connection fields and disagree about
// twenty-four more; a unified struct is therefore nineteen retyped fields, two
// profile-only groups, thirty-eight assignments and two validation paths that
// have to be kept in step with two other modules by hand — and the only thing
// it buys a caller is not writing `Core.` or `JetStream.` in front of a field
// name. TestSharedConfigurationFields keeps that count honest: it partitions
// both drivers' configurations by reflection and fails when either grows a
// field, so the number ADR-016 records is measured rather than counted once.
//
// Both profile configurations may be filled in at the same time and only the
// selected one is read. That is not an accident to be refused: an application
// deployed against Core in development and JetStream in production writes both
// and flips [Config.Profile], which is the one thing a facade is genuinely good
// for. A DSN has no such excuse — one scheme, one parameter list — and a
// `nats://` DSN carrying a JetStream parameter is refused by name.
type Config struct {
	// Profile is which profile to open. There is no default: [ProfileUnset]
	// is a construction error.
	Profile Profile
	// Core is the NATS Core configuration, read when Profile is
	// [ProfileCore] and ignored otherwise.
	Core nats.Config
	// JetStream is the JetStream configuration, read when Profile is
	// [ProfileJetStream] and ignored otherwise.
	JetStream jetstream.Config
	// Require are capability requirements, checked against the selected
	// profile's table inside [Open] — so a DSN that selects a profile which
	// cannot meet them fails at the Open rather than at the first publisher
	// or consumer that states the same requirement.
	//
	// It is the one refusal this facade adds that neither driver has, and it
	// exists because a run-time profile choice is exactly the case where a
	// caller's assumptions and the broker's capabilities can part company
	// silently. A DSN cannot carry a requirement, so the shape for a
	// DSN-configured program is ParseDSN, then set this, then Open.
	Require []mq.Requirement
}

// Validate reports whether the configuration can be used, without any I/O.
//
// It checks the one thing that is the facade's own — that a profile was named —
// and delegates the rest to the selected profile's own Validate, which is
// already pure ("validate config before starting background work"). The
// unselected profile's configuration is NOT validated: it is not going to be
// used, and refusing a program for a typo in the half of its configuration this
// deployment does not read would make the both-filled-in shape unusable.
func (c Config) Validate() error {
	if !c.Profile.Valid() {
		return errNoProfile
	}
	switch c.Profile {
	case ProfileCore:
		return c.Core.Validate()
	case ProfileJetStream:
		return c.JetStream.Validate()
	case ProfileUnset:
		return errNoProfile
	default:
		return errNoProfile
	}
}

// DSN renders the configuration as a connection string with every credential
// redacted, so a Config is safe to log or put in an error.
//
// It is the selected profile's own rendering, which is what makes the scheme in
// the result the scheme that would select this profile again. A configuration
// with no profile renders as the driver name and nothing else: there is no
// connection string for "the caller did not say which broker".
func (c Config) DSN() string {
	switch c.Profile {
	case ProfileCore:
		return c.Core.DSN()
	case ProfileJetStream:
		return c.JetStream.DSN()
	case ProfileUnset:
		return DriverName + "://?profile=unset"
	default:
		return DriverName + "://?profile=unset"
	}
}

// String renders the configuration with every credential redacted.
func (c Config) String() string { return c.DSN() }

// LogValue renders the configuration for slog with every credential redacted.
func (c Config) LogValue() slog.Value { return slog.StringValue(c.DSN()) }

// The facade's own refusals. The specification names no sentinel for a bad configuration
// and errors.Is matches by code, so a caller compares mq.CodeOf(err) against
// mq.CodeInvalidConfig.
//
// There are four of them, and that is the entire error surface this package
// adds: everything else a caller sees comes from the profile driver it selected,
// unwrapped and unwrapped-from, which is what makes a facade failure
// indistinguishable from the driver's own.
var (
	errNoProfile     = errcode.New(errcode.InvalidConfig, "iqmq/natsunified: Config.Profile is unset; name ProfileCore or ProfileJetStream, or open from a nats:// or natsjs:// DSN (there is no default profile)")
	errUnknownScheme = errcode.New(errcode.DriverNotRegistered, "iqmq/natsunified: the DSN's scheme is neither nats:// (NATS Core) nor natsjs:// (JetStream)")
	errNilRegistrar  = errcode.New(errcode.InvalidArgument, "iqmq/natsunified: Register was given a nil Registrar")
	errNilConn       = errcode.New(errcode.InvalidArgument, "iqmq/natsunified: OpenWithClient was given a nil connection")
)

// stringer and logValuer are the two rendering surfaces the specification asks a
// configuration to redact through; the third, encoding/json, is
// [github.com/iqhive/iqmq/dsn.Secret]'s own and is proved there.
type (
	stringer  interface{ String() string }
	logValuer interface{ LogValue() slog.Value }
)

// compile-time proof that Config renders through both of them.
var (
	_ stringer  = Config{}
	_ logValuer = Config{}
)
