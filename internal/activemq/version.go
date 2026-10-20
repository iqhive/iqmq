package activemq

import (
	"strconv"
	"strings"

	"github.com/iqhive/iqmq/internal/errcode"
)

// Version is a broker version, as three numbers.
//
// It is read because first criterion term is
// supported-version compatibility, and because the two brokers gained the two
// protocols at different releases: a driver that connected anyway would fail
// at the first attach with a framing error rather than with a sentence naming
// the version it needs.
//
// The zero Version is "the broker did not say", which the specification makes distinct from
// any claim: Classic sends no product or version in its AMQP 1.0 open
// performative at all, so the zero value is the ORDINARY case for one of the
// four combinations and a driver that read it as "too old" would refuse every
// Classic connection.
type Version struct {
	// Major, Minor and Patch are the three numbers. A version string with
	// fewer parts fills the rest with zero, so "6" is 6.0.0.
	Major, Minor, Patch int
}

// ParseVersion reads a broker version out of whatever the broker sent.
//
// Both brokers put the version after a "/" in their STOMP `server` header
// ("ActiveMQ-Artemis/2.44.0", "ActiveMQ/6.2.0") and Artemis sends a bare
// version in its AMQP `version` property, so the leading product name is
// skipped when there is one. It is tolerant of what FOLLOWS the numbers —
// "2.45.0-SNAPSHOT", "6.2.0 (localhost)" — because a driver that refused a
// snapshot build would refuse to connect to a broker somebody is testing.
func ParseVersion(s string) (Version, bool) {
	s = strings.TrimSpace(s)
	if i := strings.LastIndex(s, "/"); i >= 0 {
		s = strings.TrimSpace(s[i+1:])
	}
	if s == "" {
		return Version{}, false
	}
	var parts [3]int
	for i := range parts {
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
		parts[i] = n
		s = rest
		if s == "" || s[0] != '.' {
			break
		}
		s = s[1:]
	}
	return Version{Major: parts[0], Minor: parts[1], Patch: parts[2]}, true
}

// leadingInt reads the decimal number at the front of s.
func leadingInt(s string) (n int, rest string, ok bool) {
	i := 0
	for i < len(s) && s[i] >= '0' && s[i] <= '9' {
		// Refuse a number long enough to overflow rather than wrapping it:
		// a version of 99999999999 is not older than anything.
		if n > (1<<31-1)/10 {
			return 0, s, false
		}
		n = n*10 + int(s[i]-'0')
		i++
	}
	if i == 0 {
		return 0, s, false
	}
	return n, s[i:], true
}

// Compare orders two versions.
func (v Version) Compare(o Version) int {
	for _, p := range [...][2]int{{v.Major, o.Major}, {v.Minor, o.Minor}, {v.Patch, o.Patch}} {
		switch {
		case p[0] < p[1]:
			return -1
		case p[0] > p[1]:
			return 1
		}
	}
	return 0
}

// AtLeast reports whether v is o or newer.
func (v Version) AtLeast(o Version) bool { return v.Compare(o) >= 0 }

// IsZero reports whether the broker said nothing.
func (v Version) IsZero() bool { return v == Version{} }

// String renders the version, or "unknown" for the zero value.
func (v Version) String() string {
	if v.IsZero() {
		return "unknown"
	}
	return strconv.Itoa(v.Major) + "." + strconv.Itoa(v.Minor) + "." + strconv.Itoa(v.Patch)
}

// The version floors these drivers read. Each is a claim, so each is named,
// and each is per BROKER as well as per protocol -- which is what makes
// the specification's "supported-version compatibility" two numbers rather than one.
var (
	// MinArtemis is the oldest Artemis these drivers claim. 2.0 is the
	// release that introduced the address model these drivers map a topic to
	// (addresses with a routing type, queues bound to them, and the fully
	// qualified queue name); Artemis 1.x has the HornetQ model, where a
	// topic is `jms.topic.<name>` and an FQQN does not exist, so the
	// addressing here would name nothing on it.
	MinArtemis = Version{Major: 2}
	// MinArtemisFQQN is the release whose AMQP and STOMP acceptors both
	// accept `address::queue`. FQQN arrived in 2.7.0.
	MinArtemisFQQN = Version{Major: 2, Minor: 7}
	// MinClassicAMQP is the oldest Classic whose AMQP 1.0 transport these
	// drivers claim. 5.9.0 is the release in which the AMQP transport
	// stopped being experimental.
	MinClassicAMQP = Version{Major: 5, Minor: 9}
	// MinClassicSTOMP is the oldest Classic that speaks STOMP 1.2, which is
	// the version `client-individual` acknowledgement and the `ack` header's
	// message identifier belong to. 5.6.0 added it.
	MinClassicSTOMP = Version{Major: 5, Minor: 6}
)

// CheckAtLeast refuses a broker older than min, naming both versions.
//
// A broker that said nothing is ACCEPTED, with the version left unknown, and
// that is the opposite of the RabbitMQ pair's rule for a deliberate reason:
// Classic sends no version over AMQP 1.0, so refusing silence would refuse the
// broker the specification names. What guards the wrong-broker case instead is the PRODUCT
// check (`CheckProduct`), which is the half that can be answered from what
// both brokers do send.
func CheckAtLeast(got, min Version, what string) error {
	if got.IsZero() || got.AtLeast(min) {
		return nil
	}
	return &VersionError{What: what, Got: got, Min: min}
}

// VersionError is a broker too old for something.
type VersionError struct {
	// What needs the version: "the AMQP 1.0 profile", "STOMP 1.2".
	What string
	// Got is the broker's version.
	Got Version
	// Min is the version What needs.
	Min Version
}

// Error implements error.
func (e *VersionError) Error() string {
	return "iqmq/activemq: " + e.What + " needs " + e.Min.String() +
		" or newer; the broker is " + e.Got.String()
}

// Unwrap makes the refusal ErrUnsupported: the broker is real and reachable
// and does not have the feature, which is Unsupported rather than a
// configuration error.
func (e *VersionError) Unwrap() error { return errUnsupportedVersion }

var errUnsupportedVersion = errcode.New(errcode.Unsupported,
	"iqmq/activemq: the broker is older than this driver needs")

// CheckProduct refuses a broker that is not the one the configuration names.
//
// It is the gate the version check cannot be, and the hazard it exists for is
// concrete: Artemis AUTO-CREATES an address from an attach unless the
// deployment turned that off, so a client configured for Classic and pointed
// at Artemis would not fail — it would create an address literally called
// `topic://VirtualTopic.orders`, publish into it for ever, and its consumers
// would attach to `queue://Consumer.workers.VirtualTopic.orders`, which is a
// different address again. Nothing would report an error and nothing would be
// delivered. This is the same reasoning `drivers/rabbitmq/amqp10` records for
// its own product gate, one broker along.
//
// A broker that identified itself as NOTHING is accepted: Classic sends no
// product over AMQP 1.0, and the specification makes silence distinct from a claim. Silence
// is decided AFTER trimming, because [productBroker] trims too and the two
// halves disagreeing about what "nothing" is was a real defect: a `server`
// header of one space was not silence and was not placeable either, so it
// refused the whole connection. FuzzActiveMQVersion found it.
func CheckProduct(want Broker, product string) error {
	if strings.TrimSpace(product) == "" {
		return nil
	}
	got, ok := productBroker(product)
	if !ok {
		return &ProductError{Product: product, Want: want}
	}
	if got != want {
		return &ProductError{Product: product, Want: want}
	}
	return nil
}

// productBroker reads a broker out of an AMQP `product` property or a STOMP
// `server` header.
//
// The order of the two tests is load-bearing: Artemis's STOMP server header is
// "ActiveMQ-Artemis/2.44.0" and Classic's is "ActiveMQ/6.2.0", so a Classic
// test written as a prefix match would match Artemis as well.
func productBroker(s string) (Broker, bool) {
	s = strings.TrimSpace(s)
	switch {
	case strings.EqualFold(s, ProductArtemis):
		return Artemis, true
	case hasPrefixFold(s, ServerArtemis):
		return Artemis, true
	case hasPrefixFold(s, ServerClassic):
		return Classic, true
	}
	return BrokerUnset, false
}

// hasPrefixFold is strings.HasPrefix, ASCII-case-insensitively.
func hasPrefixFold(s, prefix string) bool {
	return len(s) >= len(prefix) && strings.EqualFold(s[:len(prefix)], prefix)
}

// ProductError is a broker that is not the one the configuration names.
type ProductError struct {
	// Product is what the broker called itself.
	Product string
	// Want is the broker the configuration names.
	Want Broker
}

// Error implements error.
func (e *ProductError) Error() string {
	return "iqmq/activemq: this client is configured for " + e.Want.String() +
		" and the broker identifies itself as " + strconv.Quote(e.Product) +
		": the two products' topic mappings name nothing in common, and Artemis would auto-create the wrong one rather than refuse"
}

// Unwrap makes the refusal ErrUnsupported.
func (e *ProductError) Unwrap() error { return errUnsupportedVersion }
