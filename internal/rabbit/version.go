package rabbit

import (
	"strconv"
	"strings"

	"bitbucket.org/iqhive/iqmq/internal/errcode"
)

// Version is a RabbitMQ server version: the three numbers a capability gate is
// read against.
//
// It exists because four of this driver pair's claims move with the broker and
// nothing else in the repository can compare two version strings. RabbitMQ
// reports its version in the `server_properties` of the connection's
// `connection.start` frame (AMQP 0-9-1) or in the open frame's properties
// (AMQP 1.0), as a string such as "4.3.5", "3.13.7" or "4.1.8-rc.1", and the
// claims that depend on it are:
//
//   - AMQP 1.0 at all. RabbitMQ 4.0 is the first release that speaks AMQP 1.0
//     natively; on 3.13 it needs a plugin and behaves differently enough that
//     The specification forbids substituting one protocol for the other silently
//     `drivers/rabbitmq/amqp10` refuses a broker below 4.0.
//   - x-delivery-count on a quorum queue. It is the attempt counter the specification's
//     Attempt is read from, and a classic queue does not have it at all.
//   - Per-message TTL survival through a dead-letter hop, which the specification's
//     `ttldlx` kind rests on.
//   - The delayed-message exchange, which is a plugin rather than a version,
//     and is therefore DETECTED rather than inferred: see
//     `internal/rabbit/delay`.
//
// The zero Version is "the broker did not say", which the specification makes distinct from
// any claim: a driver that treated an unknown version as new enough would gate
// nothing.
type Version struct {
	// Major, Minor and Patch are the three numbers. A version string with
	// fewer parts fills the rest with zero, so "4" is 4.0.0.
	Major, Minor, Patch int
}

// ParseVersion reads a RabbitMQ version string.
//
// It is deliberately tolerant of what follows the numbers — "4.1.8-rc.1",
// "3.13.7+deb1", "4.2.9 (Erlang 27)" — because the string comes from a server
// and a driver that refused an unexpected suffix would refuse to connect to a
// release candidate. It is NOT tolerant of a missing major number: an empty or
// unparseable string is the zero Version and false, which is "the broker did
// not say".
//
// Nor does it accept a version whose three numbers are all zero — "0", "0.0",
// "0.0.0-rc.1" — and that is the zero Version's reservation rather than a
// judgement about the string. The zero Version IS "the broker did not say":
// [Version.IsZero], [Version.String], [CheckAtLeast] and [VersionError] all read
// it that way, so a parse that returned it with true would report a version it
// cannot render ("0" came back as "unknown") and a claim nothing downstream can
// tell apart from silence. No RabbitMQ release is 0.0.0 and every gate is 3.8
// or newer, so refusing it opens and closes exactly the gates accepting it did.
func ParseVersion(s string) (Version, bool) {
	s = strings.TrimSpace(s)
	if s == "" {
		return Version{}, false
	}
	var v Version
	out := [...]*int{&v.Major, &v.Minor, &v.Patch}
	for i := range out {
		if s == "" {
			break
		}
		n, rest, ok := leadingInt(s)
		if !ok {
			if i == 0 {
				return Version{}, false
			}
			break
		}
		*out[i] = n
		if rest == "" || rest[0] != '.' {
			break
		}
		s = rest[1:]
	}
	if v.IsZero() {
		return Version{}, false
	}
	return v, true
}

// leadingInt reads the digits at the front of s.
func leadingInt(s string) (n int, rest string, ok bool) {
	i := 0
	for i < len(s) && s[i] >= '0' && s[i] <= '9' {
		i++
	}
	if i == 0 {
		return 0, s, false
	}
	// A number longer than a machine word is not a version; refusing it is
	// better than wrapping, and no server sends one.
	if i > 9 {
		return 0, s, false
	}
	n, err := strconv.Atoi(s[:i])
	if err != nil {
		return 0, s, false
	}
	return n, s[i:], true
}

// Compare orders two versions: -1 when v is older, 0 when they are equal and
// +1 when v is newer.
func (v Version) Compare(o Version) int {
	for _, pair := range [...][2]int{{v.Major, o.Major}, {v.Minor, o.Minor}, {v.Patch, o.Patch}} {
		switch {
		case pair[0] < pair[1]:
			return -1
		case pair[0] > pair[1]:
			return 1
		}
	}
	return 0
}

// AtLeast reports whether v is o or newer. The zero Version is at least
// nothing but the zero version itself: an unknown broker version satisfies no
// gate, which is what keeps a gate from being open by omission.
func (v Version) AtLeast(o Version) bool { return v.Compare(o) >= 0 }

// IsZero reports whether the version is unknown.
func (v Version) IsZero() bool { return v == Version{} }

// String renders the three numbers, and "unknown" for the zero value.
func (v Version) String() string {
	if v.IsZero() {
		return "unknown"
	}
	return strconv.Itoa(v.Major) + "." + strconv.Itoa(v.Minor) + "." + strconv.Itoa(v.Patch)
}

// The version gates this driver pair reads. Each is a claim, so each is named.
var (
	// MinBrokerAMQP10 is the first RabbitMQ release that speaks AMQP 1.0
	// natively. `drivers/rabbitmq/amqp10` refuses anything
	// older rather than falling back to 0-9-1: the specification says the profile is
	// "never silently substituted".
	MinBrokerAMQP10 = Version{Major: 4}
	// MinBrokerQuorum is the first release with quorum queues, which are
	// where x-delivery-count comes from.
	MinBrokerQuorum = Version{Major: 3, Minor: 8}
	// MinBrokerSupported is the oldest broker `drivers/rabbitmq` claims to
	// work against, and the oldest image the version matrix runs.
	MinBrokerSupported = Version{Major: 3, Minor: 13}
)

// CheckAtLeast refuses a broker older than min, naming both versions.
func CheckAtLeast(got, min Version, what string) error {
	if got.IsZero() {
		return &VersionError{What: what, Min: min}
	}
	if got.AtLeast(min) {
		return nil
	}
	return &VersionError{What: what, Got: got, Min: min}
}

// VersionError is a broker too old for something, or one that did not say how
// old it is.
type VersionError struct {
	// What needs the version: "the AMQP 1.0 profile", "quorum queues".
	What string
	// Got is the broker's version, or the zero Version when it said nothing.
	Got Version
	// Min is the version What needs.
	Min Version
}

// Error implements error.
func (e *VersionError) Error() string {
	if e.Got.IsZero() {
		return "iqmq/rabbitmq: " + e.What + " needs RabbitMQ " + e.Min.String() +
			" or newer and the broker did not report its version"
	}
	return "iqmq/rabbitmq: " + e.What + " needs RabbitMQ " + e.Min.String() +
		" or newer; the broker is " + e.Got.String()
}

// Unwrap makes the refusal ErrUnsupported: the broker is real and reachable
// and does not have the feature, which is Unsupported rather than a
// configuration error.
func (e *VersionError) Unwrap() error { return errUnsupportedVersion }

var errUnsupportedVersion = errcode.New(errcode.Unsupported,
	"iqmq/rabbitmq: the broker is older than this profile needs")
