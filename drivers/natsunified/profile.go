package natsunified

import "github.com/iqhive/iqmq/dsn"

// Profile is which NATS profile a client talks to.
//
// It is the facade's whole subject. The two profiles are separate modules
// (ADR-010) precisely so that one cannot be mistaken for the other, and
// a facade over them has to name the choice rather than blur it.
type Profile uint8

// The profiles. [ProfileUnset] is the zero value and is NOT a default: a
// [Config] carrying it is refused, and so is a DSN whose scheme names neither
// profile.
//
// The reason is what the two profiles disagree about. Core is at-most-once with
// no settlement at all and JetStream is at-least-once with a lease; a zero value
// that resolved to either would decide the most consequential property a queue
// has by omission, and the same reasoning gave `memory.Open` its
// `AutoCreate: false` (ADR-016) and its bounded-by-default lane.
const (
	// ProfileUnset is the zero value: the caller did not say.
	ProfileUnset Profile = iota
	// ProfileCore is NATS Core: at-most-once, no storage, no settlement.
	ProfileCore
	// ProfileJetStream is JetStream: at-least-once over a stream, with
	// explicit settlement and a lease.
	ProfileJetStream

	profileCount
)

// profileNames are the profiles' lower-case names, which are also the values
// [Profile.String] renders and the report profile the conformance harness
// carries.
var profileNames = [profileCount]string{"unset", "core", "jetstream"}

// profileSchemes are the DSN schemes the profiles answer to. ProfileUnset has
// none, which is what makes [ProfileForScheme] able to refuse a scheme rather
// than resolve it to a default.
var profileSchemes = [profileCount]string{"", SchemeCore, SchemeJetStream}

// String returns the profile's lower-case name.
func (p Profile) String() string {
	if p < profileCount {
		return profileNames[p]
	}
	return "Profile(?)"
}

// Valid reports whether p names a profile a client can be opened with.
// ProfileUnset is not valid, which is the point of it.
func (p Profile) Valid() bool { return p == ProfileCore || p == ProfileJetStream }

// Scheme is the DSN scheme this profile is selected by, or "" for
// [ProfileUnset].
func (p Profile) Scheme() string {
	if p < profileCount {
		return profileSchemes[p]
	}
	return ""
}

// Driver is the registered name of the driver this profile delegates to, which
// is also what `Client.Info().Name` answers: the facade never renames the
// driver that is really serving the caller.
func (p Profile) Driver() string {
	switch p {
	case ProfileCore:
		return coreDriverName
	case ProfileJetStream:
		return jetStreamDriverName
	case ProfileUnset:
		return ""
	default:
		return ""
	}
}

// Profiles returns the profiles a client can be opened with, in declaration
// order. ProfileUnset is not among them.
func Profiles() []Profile { return []Profile{ProfileCore, ProfileJetStream} }

// ProfileForScheme maps a DSN scheme to its profile.
//
// It is the facade's entire dispatch table and it is exported because a caller
// that holds a DSN often needs the profile before it needs a client — to decide
// which half of its own configuration to read, or to log which broker it is
// about to talk to. A scheme neither profile claims is
// [github.com/iqhive/iqmq.ErrDriverNotRegistered], which is what the registry
// answers for an unknown scheme and therefore the answer a caller already
// handles.
//
// The scheme is compared in the shared grammar's canonical spelling, because a
// scheme is case-insensitive (RFC 3986): "NATSJS" is the JetStream profile
// exactly as a DSN beginning "NATSJS://" is, which is also how the registry
// answers for it. A string that is more than a scheme -- one with "://" in it --
// comes back from the grammar shorter than it went in and names no profile.
func ProfileForScheme(scheme string) (Profile, error) {
	canon, err := dsn.Scheme(scheme + "://")
	if err != nil || len(canon) != len(scheme) {
		return ProfileUnset, errUnknownScheme
	}
	switch canon {
	case SchemeCore:
		return ProfileCore, nil
	case SchemeJetStream:
		return ProfileJetStream, nil
	default:
		return ProfileUnset, errUnknownScheme
	}
}
