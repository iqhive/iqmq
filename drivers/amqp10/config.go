package amqp10

import (
	"crypto/tls"
	"log/slog"
	"math"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	amqp "github.com/Azure/go-amqp"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/dsn"
	"github.com/iqhive/iqmq/internal/backoff"
	"github.com/iqhive/iqmq/internal/errcode"
)

// CoreName is the name of this shared core, which every broker-specific module
// built on it reports as its client library alongside go-amqp. It is not a
// driver name: a driver's name is [Profile.Driver].
const CoreName = "iqmq/amqp10"

// ClientModule and ClientVersion identify the native client library this core
// wraps, which the specification asks a driver to report.
const (
	// ClientModule is the go-amqp module path.
	ClientModule = "github.com/Azure/go-amqp"
	// ClientVersion is the version this core is built and tested against. It
	// is a constant rather than a build-info lookup because a driver's own
	// report must not depend on whether it was built with module
	// information.
	ClientVersion = "v1.7.0"
)

// DefaultPort is the port an address that names none is dialled on: 5672 for
// amqp and 5671 for amqps, which are the protocol's own.
const (
	// DefaultPort is the plaintext AMQP port.
	DefaultPort = "5672"
	// DefaultTLSPort is the AMQP-over-TLS port.
	DefaultTLSPort = "5671"
)

// The defaults a zero [Config] resolves to.
//
// Every one of them is a BOUND. The specification requires every internal buffer to be
// bounded by default, so none of these may mean "unlimited" when it is left
// unset, and the specification requires a dial loop that never busy-spins, so the reconnect
// bounds are real numbers rather than zero.
const (
	// DefaultAddr is the address a configuration that names none dials.
	DefaultAddr = "amqp://127.0.0.1:5672"
	// DefaultIdleTimeout is the longest the connection may be silent before
	// the peer is considered gone. It is go-amqp's own default, stated here
	// so that this driver's health does not depend on a library version.
	DefaultIdleTimeout = time.Minute
	// DefaultMaxFrameSize is the largest frame this connection accepts. It
	// is go-amqp's own default and the protocol's minimum is 512.
	DefaultMaxFrameSize = 64 << 10
	// DefaultDialTimeout bounds one dial attempt, so that a dial loop makes
	// progress against a host that accepts a TCP connection and then says
	// nothing.
	DefaultDialTimeout = 10 * time.Second
	// DefaultOperationTimeout bounds one link attach, detach or disposition
	// round trip whose caller gave no deadline. It is never a second
	// deadline on a caller that has one.
	DefaultOperationTimeout = 30 * time.Second
	// DefaultWriteTimeout bounds one frame write.
	DefaultWriteTimeout = 30 * time.Second
	// DefaultReconnectBase is the first delay between dial attempts.
	DefaultReconnectBase = 200 * time.Millisecond
	// DefaultReconnectMax caps every delay between dial attempts.
	DefaultReconnectMax = 30 * time.Second
	// DefaultReconnectGrowth is the factor between consecutive delays.
	DefaultReconnectGrowth = 2.0
	// DefaultMaxPending bounds the asynchronous publications outstanding on
	// one sink at once, which is what makes PublishAsync bounded rather than
	// a buffer that grows.
	DefaultMaxPending = 1000
)

// DefaultBackoff is the reconnect policy a configuration that names none gets.
//
// It is named explicitly rather than left to [backoff.Auto], and that is a
// decision rather than an oversight: the provisional Auto winner is
// `none`, which is no delay at all. A dial loop on `none` is answered by every
// client in a fleet at the same instant and again immediately after, which is
// the behaviour the specification's "reconnect with backoff (never busy-spin)" forbids. Full
// jitter is the policy that spreads a broker restart, so this core names it.
//
// Since it agrees with
// [github.com/iqhive/iqmq/driverkit.DefaultReconnectPolicy] rather than working
// around it: the helper's own zero value is jittered now, and this constant is
// kept because a driver's configuration should say what its dial loop does
// rather than defer the answer.
const DefaultBackoff = BackoffFullJitter

// BackoffPolicy is how long the dial loop waits between attempts.
//
// It is this module's own enumeration rather than
// [github.com/iqhive/iqmq/internal/backoff.Kind], because that type is in the
// root module's internal tree: a caller outside this repository cannot name a
// value of it, so a public field of that type would be a field nobody can set.
type BackoffPolicy uint8

// The backoff policies.
const (
	// BackoffDefault is the zero value and resolves to [DefaultBackoff].
	BackoffDefault BackoffPolicy = iota
	// BackoffNone retries immediately. It is here because the specification has the
	// kind and a test may want it; a deployment that chooses it has every
	// client in the fleet answer a broker restart at the same instant.
	BackoffNone
	// BackoffConstant waits ReconnectBase before every attempt.
	BackoffConstant
	// BackoffFullJitter is exponential backoff with full jitter: a delay
	// drawn uniformly from zero to the unjittered interval.
	BackoffFullJitter
	// BackoffEqualJitter is exponential backoff with equal jitter: half the
	// interval plus a uniform draw from the other half.
	BackoffEqualJitter
	// BackoffDecorrelated grows from the previous delay rather than from the
	// attempt number.
	BackoffDecorrelated

	backoffPolicyCount
)

var backoffPolicyNames = [backoffPolicyCount]string{
	"default", "none", "constant", "fulljitter", "equaljitter", "decorrelated",
}

// String returns the policy's lower-case name.
func (b BackoffPolicy) String() string {
	if b < backoffPolicyCount {
		return backoffPolicyNames[b]
	}
	return "BackoffPolicy(?)"
}

// Valid reports whether b names a policy.
func (b BackoffPolicy) Valid() bool { return b < backoffPolicyCount }

// Resolve maps BackoffDefault to [DefaultBackoff] and returns every other
// policy unchanged.
func (b BackoffPolicy) Resolve() BackoffPolicy {
	if b == BackoffDefault {
		return DefaultBackoff
	}
	return b
}

// kind is the kind this policy stands for.
func (b BackoffPolicy) kind() backoff.Kind {
	switch b.Resolve() {
	case BackoffNone:
		return backoff.None
	case BackoffConstant:
		return backoff.Constant
	case BackoffFullJitter:
		return backoff.Full
	case BackoffEqualJitter:
		return backoff.Equal
	case BackoffDecorrelated:
		return backoff.Decorrelated
	case BackoffDefault, backoffPolicyCount:
	}
	return backoff.Full
}

// SASLMechanism is how this client authenticates.
type SASLMechanism uint8

// The SASL mechanisms.
const (
	// SASLAuto is the zero value: PLAIN when a user name is configured and
	// ANONYMOUS when none is. It is what a connection string means by
	// carrying credentials or not.
	SASLAuto SASLMechanism = iota
	// SASLNone offers no SASL layer at all, for a peer that wants none.
	SASLNone
	// SASLAnonymous is the ANONYMOUS mechanism.
	SASLAnonymous
	// SASLPlain is the PLAIN mechanism, which sends the credential in the
	// clear and therefore belongs behind TLS. [Config.Validate] refuses it
	// without TLS unless [Config.InsecurePlainSASL] says so.
	SASLPlain
	// SASLExternal is the EXTERNAL mechanism, which authenticates with the
	// TLS client certificate.
	SASLExternal

	saslMechanismCount
)

var saslMechanismNames = [saslMechanismCount]string{"auto", "none", "anonymous", "plain", "external"}

// String returns the mechanism's lower-case name.
func (s SASLMechanism) String() string {
	if s < saslMechanismCount {
		return saslMechanismNames[s]
	}
	return "SASLMechanism(?)"
}

// Valid reports whether s names a mechanism.
func (s SASLMechanism) Valid() bool { return s < saslMechanismCount }

// SenderSettleMode is how a sending link settles its transfers.
//
// It is this module's own enumeration because go-amqp's is a POINTER in its
// option struct, where nil means "take the peer's choice": a nil-able field in
// a typed Config is a third state a DSN cannot express and a caller has to
// remember to dereference.
type SenderSettleMode uint8

// The sender settle modes.
const (
	// SenderSettleDefault is the zero value and resolves to
	// SenderSettleMixed, which is the mode that lets one link carry both a
	// confirmed publication and a fire-and-forget one — and therefore the
	// only mode under which ConfirmNone and ConfirmAccepted can share a
	// sink.
	SenderSettleDefault SenderSettleMode = iota
	// SenderSettleUnsettled sends every transfer unsettled, so every
	// publication waits for a disposition. A sink on this mode cannot honour
	// ConfirmNone and says so when it is opened.
	SenderSettleUnsettled
	// SenderSettleSettled sends every transfer pre-settled, so no
	// disposition ever arrives. A sink on this mode cannot honour
	// ConfirmAccepted or ConfirmDurable: there is nothing to confirm.
	SenderSettleSettled
	// SenderSettleMixed lets each transfer say, which is what this driver
	// uses.
	SenderSettleMixed

	senderSettleModeCount
)

var senderSettleModeNames = [senderSettleModeCount]string{"default", "unsettled", "settled", "mixed"}

// String returns the mode's lower-case name.
func (s SenderSettleMode) String() string {
	if s < senderSettleModeCount {
		return senderSettleModeNames[s]
	}
	return "SenderSettleMode(?)"
}

// Valid reports whether s names a mode.
func (s SenderSettleMode) Valid() bool { return s < senderSettleModeCount }

// Resolve maps the zero value to SenderSettleMixed.
func (s SenderSettleMode) Resolve() SenderSettleMode {
	if s == SenderSettleDefault {
		return SenderSettleMixed
	}
	return s
}

// amqpMode is the go-amqp value this mode stands for, or nil for "the peer
// decides".
func (s SenderSettleMode) amqpMode() *amqp.SenderSettleMode {
	switch s.Resolve() {
	case SenderSettleUnsettled:
		m := amqp.SenderSettleModeUnsettled
		return &m
	case SenderSettleSettled:
		m := amqp.SenderSettleModeSettled
		return &m
	case SenderSettleMixed:
		m := amqp.SenderSettleModeMixed
		return &m
	case SenderSettleDefault, senderSettleModeCount:
	}
	return nil
}

// ReceiverSettleMode is when a receiving link considers a disposition final.
type ReceiverSettleMode uint8

// The receiver settle modes.
const (
	// ReceiverSettleDefault is the zero value and resolves to
	// ReceiverSettleFirst.
	ReceiverSettleDefault ReceiverSettleMode = iota
	// ReceiverSettleFirst settles a delivery as soon as the disposition is
	// sent: one frame, no round trip, and a settlement whose write failed
	// means the broker did not get it — so the delivery is still the
	// broker's and will be redelivered. That is what makes a failed
	// settlement a LOST LEASE here rather than an unknown outcome.
	ReceiverSettleFirst
	// ReceiverSettleSecond waits for the sender to confirm the settlement,
	// which costs a round trip per settlement and makes a failed settlement
	// AMBIGUOUS: the disposition may have been taken and the confirmation
	// lost. A deployment that cannot tolerate a duplicate at that boundary
	// asks for it deliberately, and the driver's error table changes with
	// it.
	ReceiverSettleSecond

	receiverSettleModeCount
)

var receiverSettleModeNames = [receiverSettleModeCount]string{"default", "first", "second"}

// String returns the mode's lower-case name.
func (r ReceiverSettleMode) String() string {
	if r < receiverSettleModeCount {
		return receiverSettleModeNames[r]
	}
	return "ReceiverSettleMode(?)"
}

// Valid reports whether r names a mode.
func (r ReceiverSettleMode) Valid() bool { return r < receiverSettleModeCount }

// Resolve maps the zero value to ReceiverSettleFirst.
func (r ReceiverSettleMode) Resolve() ReceiverSettleMode {
	if r == ReceiverSettleDefault {
		return ReceiverSettleFirst
	}
	return r
}

// amqpMode is the go-amqp value this mode stands for.
func (r ReceiverSettleMode) amqpMode() *amqp.ReceiverSettleMode {
	switch r.Resolve() {
	case ReceiverSettleSecond:
		m := amqp.ReceiverSettleModeSecond
		return &m
	case ReceiverSettleFirst, ReceiverSettleDefault, receiverSettleModeCount:
	}
	m := amqp.ReceiverSettleModeFirst
	return &m
}

// Config is the typed configuration of the AMQP 1.0 core.
//
// A broker-specific module embeds or wraps one of these and adds whatever its
// broker needs; a DSN is the convenience form and parses into exactly this
// ([ParseDSN]). Every credential is a [github.com/iqhive/iqmq/dsn.Secret], so
// neither [Config.String] nor an slog record nor an error built from one can
// carry it.
//
// [Config.Validate] is pure: it performs no I/O and never dials, which is the specification's
// "validate config before starting background work".
type Config struct {
	// Addrs are the peers to dial, in the order they are tried. Empty means
	// DefaultAddr. Each is an AMQP URL: amqp://host:port or amqps://host:port.
	//
	// The dial loop tries them in order and starts again from the first, so
	// a list is a failover set rather than a load-balanced one: AMQP 1.0 is
	// one long-lived connection and a client that spread its links over two
	// peers would have half its deliveries die with each.
	Addrs []string

	// ContainerID is the container-id this client sends in its OPEN frame.
	// Empty means go-amqp generates one. It is what a broker's management
	// view identifies the connection by, so a deployment that wants to find
	// its own connections sets it.
	ContainerID string
	// HostName is the hostname sent in the OPEN frame and used as the TLS
	// server name. Empty means the host from the address, which is what a
	// virtual host on a shared endpoint needs overridden.
	HostName string
	// Properties are extra connection properties sent in the OPEN frame,
	// merged under this core's own. A profile that has to announce a
	// capability to its broker sets them.
	Properties map[string]any

	// SASL is the authentication mechanism. The zero value is SASLAuto:
	// PLAIN when Username is set and ANONYMOUS when it is not.
	SASL SASLMechanism
	// Username and Password are the SASL PLAIN credentials.
	Username string
	Password dsn.Secret
	// InsecurePlainSASL allows SASL PLAIN without TLS. It is named the way
	// The specification asks so that it is greppable and obviously unsafe: PLAIN sends
	// the password in the clear, so Validate refuses the combination unless
	// the operator has said this.
	InsecurePlainSASL bool

	// TLS is the TLS configuration. It supplies the PARAMETERS of a TLS
	// connection; it does not select one. go-amqp reads the transport from
	// the address's scheme, so a non-nil value turns TLS on for a bare
	// "host:port" address, is redundant beside "amqps://", and beside an
	// explicit "amqp://" is refused -- that connection would be in the clear
	// whatever this field holds. This comment said the opposite until the
	// bytes on the socket were measured.
	TLS *tls.Config
	// InsecureSkipVerify disables server certificate verification. It is
	// named the way the specification asks. Validate refuses it without TLS.
	InsecureSkipVerify bool

	// IdleTimeout is the longest the connection may be silent before the
	// peer is considered gone, which is what makes a dead TCP connection a
	// reconnection rather than a client that waits for ever. Zero means
	// DefaultIdleTimeout; a negative value disables it, which the specification's
	// bounded-by-default rule makes something an operator asks for.
	IdleTimeout time.Duration
	// MaxFrameSize is the largest frame this connection accepts. Zero means
	// DefaultMaxFrameSize, and the protocol's floor is 512.
	MaxFrameSize uint32
	// MaxSessions bounds the sessions on one connection. This driver opens
	// one session per link, so it also bounds the sinks and sources one
	// client may have open. Zero means go-amqp's default of 65536.
	MaxSessions uint16

	// SenderSettle is how a sending link settles its transfers. The zero
	// value is mixed, which is the only mode under which one sink can serve
	// both ConfirmNone and ConfirmAccepted.
	SenderSettle SenderSettleMode
	// ReceiverSettle is when a receiving link considers a disposition final.
	// The zero value is first, which costs no round trip per settlement; see
	// [ReceiverSettleMode] for what the choice does to a failed settlement's
	// classification.
	ReceiverSettle ReceiverSettleMode
	// MaxCredit caps the link credit any one source may hold, whatever
	// prefetch the core resolved. Zero means the prefetch stands.
	//
	// It exists because the prefetch is the CORE's number — resolves it
	// from the consumer's configuration — and a broker with a per-link
	// ceiling of its own would refuse the flow frame. A profile whose broker
	// has one sets it.
	MaxCredit int

	// NonDurable publishes messages with the AMQP header's `durable` flag
	// clear, so that a broker is free to lose them on a restart. It is
	// inverted so that the zero value publishes DURABLE messages: a driver
	// whose default lost messages on a broker restart would be surprising in
	// the one direction that cannot be recovered from.
	NonDurable bool

	// MaxPending bounds the asynchronous publications outstanding on one
	// sink. Zero means DefaultMaxPending; it is never unlimited.
	MaxPending int

	// DialTimeout bounds one dial attempt. Zero means DefaultDialTimeout.
	DialTimeout time.Duration
	// OperationTimeout bounds one attach, detach or disposition round trip
	// whose caller's context has no deadline. Zero means
	// DefaultOperationTimeout. It is never a second deadline on a caller
	// that has one.
	OperationTimeout time.Duration
	// WriteTimeout bounds one frame write. Zero means DefaultWriteTimeout;
	// a negative value defers to the underlying connection, which is
	// go-amqp's own escape and means no deadline at all.
	WriteTimeout time.Duration

	// Backoff is the dial loop's policy. The zero value is
	// [DefaultBackoff], which is full jitter — see that constant for why
	// this is not backoff.Auto.
	Backoff BackoffPolicy
	// ReconnectBase is the first delay between dial attempts. Zero means
	// DefaultReconnectBase.
	//
	// Zero here means "the default, UNLESS the partner below was written":
	// the two are reconciled so that the explicit one wins and the defaulted
	// one yields, which is what keeps a one-sided setting working and keeps
	// the resolved pair self-consistent. A pair whose
	// BOTH sides are written and inconsistent is refused.
	// So "ReconnectMax: 100ms" alone yields a ReconnectBase of 100ms rather
	// than the 200ms default, and "ReconnectBase: 1h" alone yields a
	// ReconnectMax of 1h rather than the 30s one.
	ReconnectBase time.Duration
	// ReconnectMax caps every delay between dial attempts. Zero means
	// DefaultReconnectMax, subject to the reconciliation described on
	// ReconnectBase.
	ReconnectMax time.Duration
	// ReconnectGrowth is the factor between consecutive delays. Below 1
	// means DefaultReconnectGrowth. NaN and the infinities are refused.
	ReconnectGrowth float64
	// ReconnectAttempts bounds the dial attempts of ONE reconnection. Zero
	// means the loop keeps trying until the client is closed, which is what
	// a long-lived client wants; a positive value makes the backend go
	// Degraded and stay there, with every operation answering
	// CodeUnavailable, which is what a short-lived job wants.
	ReconnectAttempts int
	// NoReconnect turns the dial loop off entirely, so a lost connection is
	// terminal and every operation afterwards reports CodeUnavailable. It is
	// for a caller that supervises its own connection through
	// [OpenWithClient] and does not want two supervisors.
	NoReconnect bool

	// Publisher and Consumer are the AMQP 1.0 link controls of every sink and
	// every source this client opens (see options.go). They are per-client
	// rather than per-handle because the SPI carries no driver-specific field
	// for a publisher or a consumer yet; a program that needs two consumers
	// with different selectors opens two clients.
	Publisher PublisherOptions
	Consumer  ConsumerOptions

	// Clock is the time source the driver reads. Nil means the wall clock.
	Clock mq.Clock
	// Hooks are the observability hooks. Only OnReconnect is fired
	// from here: a driver has no access to the client's own hooks, so the
	// reconnection events this core produces have nowhere else to go
	// (recorded in the maintenance notes as a gap in the SPI, and the reason
	// every driver takes Hooks in its own Config).
	Hooks mq.Hooks
}

// resolve returns a copy of cfg with every default filled in, so that nothing
// below this point has to ask what a zero means.
func (c Config) resolve() Config {
	if len(c.Addrs) == 0 {
		c.Addrs = []string{DefaultAddr}
	}
	// Every address gets an explicit scheme, because go-amqp picks the
	// transport from it and REFUSES an address that has none: dialling a bare
	// "127.0.0.1:5672" returns `parse "127.0.0.1:5672": first path segment in
	// URL cannot contain colon` before any connection is made, measured
	// against v1.7.0. A bare address was accepted by checkAddr and handed
	// through unchanged, so it failed at the dial with an error about a
	// string the operator never wrote.
	scheme := "amqp://"
	if c.TLS != nil {
		scheme = "amqps://"
	}
	addrs := make([]string, len(c.Addrs))
	for i, a := range c.Addrs {
		if hasScheme(a) {
			addrs[i] = a
			continue
		}
		addrs[i] = scheme + a
	}
	c.Addrs = addrs
	if c.IdleTimeout == 0 {
		c.IdleTimeout = DefaultIdleTimeout
	}
	if c.MaxFrameSize == 0 {
		c.MaxFrameSize = DefaultMaxFrameSize
	}
	if c.DialTimeout == 0 {
		c.DialTimeout = DefaultDialTimeout
	}
	if c.OperationTimeout == 0 {
		c.OperationTimeout = DefaultOperationTimeout
	}
	if c.WriteTimeout == 0 {
		c.WriteTimeout = DefaultWriteTimeout
	}
	c.ReconnectBase, c.ReconnectMax = reconcileBounds(
		c.ReconnectBase, c.ReconnectMax, DefaultReconnectBase, DefaultReconnectMax,
	)
	if c.ReconnectGrowth < 1 {
		c.ReconnectGrowth = DefaultReconnectGrowth
	}
	if c.MaxPending == 0 {
		c.MaxPending = DefaultMaxPending
	}
	c.Backoff = c.Backoff.Resolve()
	c.SenderSettle = c.SenderSettle.Resolve()
	c.ReceiverSettle = c.ReceiverSettle.Resolve()
	return c
}

// Validate reports whether the configuration can be used, without any I/O.
//
// It is pure on purpose: the specification requires a driver to refuse a configuration before
// it starts background work, and a validation that dialled would turn a typo
// into a ten-second timeout.
func (c Config) Validate() error {
	for _, a := range c.Addrs {
		if err := checkAddr(a); err != nil {
			return err
		}
	}
	switch {
	case !c.SASL.Valid():
		return errSASLMechanism
	case !c.SenderSettle.Valid():
		return errSenderSettle
	case !c.ReceiverSettle.Valid():
		return errReceiverSettle
	case !c.Backoff.Valid():
		return errBackoffPolicy
	case c.MaxFrameSize != 0 && c.MaxFrameSize < 512:
		// The protocol's own floor. A smaller frame cannot carry an OPEN
		// frame, so the connection would fail at negotiation.
		return errFrameSize
	case c.MaxCredit < 0 || c.MaxPending < 0 || c.ReconnectAttempts < 0:
		return errNegativeBound
	case c.DialTimeout < 0 || c.OperationTimeout < 0 ||
		c.ReconnectBase < 0 || c.ReconnectMax < 0:
		return errNegativeDuration
	case c.ReconnectGrowth < 0:
		return errNegativeBound
	case !finite(c.ReconnectGrowth):
		// NaN passes the sign check above and the default-below-one rule in
		// resolve, because it compares false with everything, and it made
		// every reconnect delay after the first zero. An infinite growth
		// has no delay to convert to.
		return errReconnectGrowth
	case c.ReconnectBase != 0 && c.ReconnectMax != 0 && c.ReconnectBase > c.ReconnectMax:
		// Both sides written and inconsistent: a contradiction the operator
		// wrote. A one-sided setting is NOT refused -- "reconnectmax=100ms"
		// alone means "cap it at 100ms", and [Config.resolve] yields the
		// defaulted partner to it rather than erroring.
		return errBackoffBounds
	}
	if !printableText(c.ContainerID) || !printableText(c.HostName) || !printableText(c.Username) {
		return errBadText
	}
	if c.Password != "" && c.Username == "" {
		return errPasswordWithoutUser
	}
	// A *tls.Config beside an address that explicitly says "amqp://" asks for
	// two different things, and the library grants the address's: it dials in
	// the clear. Refusing is the same shape as the InsecureSkipVerify check
	// below, and it is what closes both halves -- the SASL-PLAIN guard and the rendered transport.
	for _, a := range c.Addrs {
		if c.TLS != nil && strings.HasPrefix(a, "amqp://") {
			return errTLSWithPlaintextAddr
		}
	}
	// A list whose addresses disagree about TLS has no DSN spelling -- the
	// grammar carries one `tls=` for the whole list -- so a rendering of it
	// necessarily describes one of the endpoints wrongly, and the direction
	// it got wrong was the unsafe one: a list of "amqps://h:5671" and
	// "h:5672" rendered a plaintext DSN, turning the protected endpoint into
	// a cleartext dial on the way back.
	if len(c.Addrs) > 1 {
		first := addrUsesTLS(c.Addrs[0], c.TLS != nil)
		for _, a := range c.Addrs[1:] {
			if addrUsesTLS(a, c.TLS != nil) != first {
				return errMixedTransport
			}
		}
	}
	if c.InsecureSkipVerify && c.TLS == nil {
		// Refusing is the only honest answer: the alternative is a client
		// that dialled in the clear after the operator asked to skip
		// verification, which reads as "less safe" and is "no TLS at all".
		return errInsecureWithoutTLS
	}
	if c.plainSASL() && !c.usesTLS() && !c.InsecurePlainSASL {
		// SASL PLAIN sends the password in the clear. The specification forbids leaking a
		// credential, and a driver that did it by default would leak every
		// one.
		return errPlainWithoutTLS
	}
	if c.SASL == SASLPlain && c.Username == "" {
		return errPlainWithoutUser
	}
	// A configuration whose own rendering is longer than the shared parser
	// will look at has no DSN spelling either, for the same reason a list with
	// mixed transports has none: [Config.DSN] exists so that a configuration
	// can be logged and pasted back, and [github.com/iqhive/iqmq/dsn.Parse]
	// refuses anything over [github.com/iqhive/iqmq/dsn.MaxLen] before it
	// reads a byte of it.
	//
	// The rendering is LONGER than the DSN it came from, so a DSN that fits
	// can still render past the bound: the port the endpoint omitted is
	// written out, an escape in a host costs three bytes for one, and the
	// parameter table is rendered in full even where the DSN said nothing
	// (235 bytes of port and defaults for a single-endpoint DSN that named
	// none of them). So the check cannot live in the parser's own length test:
	// measured, `amqp10://` plus eight thousand ASCII bytes of host -- 8009
	// bytes, under the bound, and not one byte of it needing an escape -- was
	// accepted and rendered as 8244 bytes, which this driver's own parser then
	// refused with "the DSN is longer than 8 KiB". The crasher that found it
	// was the escaped shape of the same thing, where a 4028-byte DSN whose
	// host was "+" and "&" rendered three bytes for each (issue #52; recorded
	// in ADR-017 as this fix's assumption, whose own row cites this check).
	//
	// It is last because it is the only check here that builds a string, and
	// because a configuration that is wrong in a way with a name should be
	// told that name rather than its size.
	if len(c.DSN()) > dsn.MaxLen {
		return errDSNTooLong
	}
	return nil
}

// finite reports whether f is neither NaN nor an infinity.
func finite(f float64) bool { return !math.IsNaN(f) && !math.IsInf(f, 0) }

// plainSASL reports whether this configuration will offer SASL PLAIN.
func (c Config) plainSASL() bool {
	return c.SASL == SASLPlain || (c.SASL == SASLAuto && c.Username != "")
}

// usesTLS reports whether every address this configuration dials is protected.
//
// "Every" and not "any": a credential is sent to whichever peer answers, so a
// list with one plaintext address in it sends the password in the clear to that
// one. A check that asked "any" would pass a configuration whose second address
// leaks the credential.
func (c Config) usesTLS() bool {
	addrs := c.Addrs
	if len(addrs) == 0 {
		addrs = []string{DefaultAddr}
	}
	for _, a := range addrs {
		if !addrUsesTLS(a, c.TLS != nil) {
			return false
		}
	}
	return true
}

// addrUsesTLS reports whether go-amqp will dial this one address over TLS.
//
// It is the SCHEME that decides, not `Config.TLS`, because that is what the
// library reads: `dialConn` switches on `u.Scheme` alone -- "amqp" and "" take
// NetDialerDial, "amqps" and "amqp+ssl" take TLSDialWithDialer -- and
// `ConnOptions.TLSConfig` never enters the decision (go-amqp v1.7.0
// conn.go:238-246). Measured on a socket: "amqp://host" WITH a TLSConfig set
// writes the cleartext AMQP header 41 4d 51 50 ("AMQP") as its first bytes,
// while "amqps://host" writes a TLS ClientHello (16 03 01 ...).
//
// Reading `TLS != nil` as "TLS is on" was therefore false in both directions,
// and it was load-bearing twice: `Validate`'s the specification guard against SASL PLAIN in
// the clear was satisfiable by a field the library ignores, and promoted the same predicate to the authority for the rendered
// `tls=`, so a plaintext configuration rendered a DSN that parsed back to a
// TLS one. An address with NO scheme is TLS when a *tls.Config is set --
// [Config.resolve] writes that scheme in, because go-amqp refuses a bare
// "host:port" outright ("first path segment in URL cannot contain colon"),
// measured.
func addrUsesTLS(a string, haveTLSConfig bool) bool {
	if isTLSAddr(a) {
		return true
	}
	if strings.HasPrefix(a, "amqp://") {
		return false
	}
	return haveTLSConfig
}

// hasScheme reports whether an address states its own transport.
func hasScheme(a string) bool { return strings.Contains(a, "://") }

// isTLSAddr reports whether an address names a protected scheme.
func isTLSAddr(a string) bool {
	return strings.HasPrefix(a, "amqps://") || strings.HasPrefix(a, "amqp+ssl://")
}

// connOptions turns the configuration into go-amqp's connection options.
func (c Config) connOptions(profile Profile) *amqp.ConnOptions {
	opts := &amqp.ConnOptions{
		ContainerID:  c.ContainerID,
		HostName:     c.HostName,
		IdleTimeout:  c.IdleTimeout,
		MaxFrameSize: c.MaxFrameSize,
		MaxSessions:  c.MaxSessions,
		WriteTimeout: c.WriteTimeout,
		TLSConfig:    c.TLS,
		SASLType:     c.saslType(),
		Properties:   c.properties(profile),
	}
	return opts
}

// saslType is the go-amqp SASL layer this configuration asks for, or nil for
// none.
func (c Config) saslType() amqp.SASLType {
	switch c.SASL {
	case SASLNone:
		return nil
	case SASLAnonymous:
		return amqp.SASLTypeAnonymous()
	case SASLPlain:
		return amqp.SASLTypePlain(c.Username, c.Password.Reveal())
	case SASLExternal:
		return amqp.SASLTypeExternal("")
	case SASLAuto, saslMechanismCount:
	}
	if c.Username != "" {
		return amqp.SASLTypePlain(c.Username, c.Password.Reveal())
	}
	return amqp.SASLTypeAnonymous()
}

// properties are the connection properties this client announces: the
// profile's, the caller's, and this core's own identity last so that it cannot
// be overwritten by accident.
//
// The identity is worth sending. A broker's management view shows the
// connection properties, and an operator looking at fifty connections wants to
// know which are iqmq's — which is also how a support question about a driver
// gets to the right repository.
func (c Config) properties(profile Profile) map[string]any {
	out := make(map[string]any, len(c.Properties)+4)
	for k, v := range c.Properties {
		out[k] = v
	}
	out["product"] = CoreName
	out["version"] = ClientVersion
	if profile.Driver != "" {
		out["iqmq-driver"] = profile.Driver
	}
	if profile.Name != "" {
		out["iqmq-profile"] = profile.Name
	}
	return out
}

// checkAddr refuses an address that is not printable ASCII, or that names a
// scheme go-amqp cannot dial.
//
// The printability rule is not fussiness. An address goes into the dial and
// into every rendering of the configuration, so a byte that is not printable
// ASCII produces a log line and an error string that are not valid UTF-8. A
// non-ASCII host name reaches the resolver as punycode, which is ASCII, so
// nothing legitimate is refused here (the specification found the first instance of this from a fuzzed DSN; the rule is the same here).
func checkAddr(a string) error {
	if strings.TrimSpace(a) == "" {
		return errNoAddress
	}
	for i := range len(a) {
		if ch := a[i]; ch <= ' ' || ch >= 0x7f {
			return errBadAddress
		}
	}
	rest := a
	if i := strings.Index(a, "://"); i >= 0 {
		switch a[:i] {
		case "amqp", "amqps", "amqp+ssl":
		default:
			return errBadScheme
		}
		rest = a[i+3:]
	}
	// What is left is a host and a port and nothing else. go-amqp reads the
	// address with net/url, so an "@" in it is user information -- which it
	// turns into SASL PLAIN credentials, putting the bytes of a password into
	// every rendering of this Config, which is [Config.DSN] and therefore
	// String and LogValue -- and "/", "?" and "#" each end the host and
	// take the port with it. A backslash makes net/url refuse the address at
	// the dial. The documented forms are "amqp://host:port" and
	// "amqps://host:port"; the credential goes in Username and Password.
	if strings.ContainsAny(rest, "@/?#\\") {
		return errAddrNotHostPort
	}
	// The typed twin of dialableHost's "%" rule: net/url rejects every "%XY"
	// in a host but "%25", so a "%" in one makes go-amqp refuse the address
	// at the DIAL. The test applies to the HOST and not to the address,
	// because every address carries a port colon; the one legitimate "%" is
	// an IPv6 zone id, which lives inside brackets beside the literal's own
	// colons.
	if host, _ := splitHostPort(rest); strings.ContainsRune(host, '%') && !strings.ContainsRune(host, ':') {
		return errAddrNotHostPort
	}
	return nil
}

// printableText reports whether a configured string is safe to render.
//
// Everything Validate calls this on ends up in [Config.DSN], which ends up in a
// log line and in an error, so a control byte or an invalid UTF-8 sequence here
// produces output no log pipeline can carry.
func printableText(s string) bool {
	if !utf8.ValidString(s) {
		return false
	}
	for _, r := range s {
		if r < 0x20 || r == 0x7f {
			return false
		}
	}
	return true
}

// DSN renders the configuration as a connection string with the credential
// redacted, so a Config is safe to log or put in an error.
//
// It is not a round trip and is not meant to be one: the password is gone from
// the result on purpose, which is the whole reason the rendering exists. Every
// other parameter is rendered, so what comes back does name the same peer and
// the same behaviour — and [ParseDSN] accepts it, which is what the
// redacted-round-trip test holds.
func (c Config) DSN() string {
	cfg := c.resolve()
	var b strings.Builder
	b.Grow(256)
	b.WriteString(Scheme)
	b.WriteString("://")
	if cfg.Username != "" || cfg.Password != "" {
		b.WriteString(escapeDSN(cfg.Username))
		if cfg.Password != "" {
			b.WriteByte(':')
			b.WriteString(dsn.Redacted)
		}
		b.WriteByte('@')
	}
	for i, a := range cfg.Addrs {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(renderAddr(a))
	}
	q := make([]string, 0, 24)
	q = append(q,
		"sasl="+cfg.SASL.String(),
		"sendersettle="+cfg.SenderSettle.String(),
		"receiversettle="+cfg.ReceiverSettle.String(),
		"backoff="+cfg.Backoff.String(),
		"idletimeout="+cfg.IdleTimeout.String(),
		"maxframesize="+strconv.FormatUint(uint64(cfg.MaxFrameSize), 10),
		"dialtimeout="+cfg.DialTimeout.String(),
		"operationtimeout="+cfg.OperationTimeout.String(),
		"writetimeout="+cfg.WriteTimeout.String(),
		"reconnectbase="+cfg.ReconnectBase.String(),
		"reconnectmax="+cfg.ReconnectMax.String(),
		// The float is escaped for the same reason a string is: 'g' writes a
		// large or small magnitude in exponent form, and "1e+10" has a "+" in
		// it, which the parser decodes as a space.
		"reconnectgrowth="+escapeQuery(strconv.FormatFloat(cfg.ReconnectGrowth, 'g', -1, 64)),
		"maxpending="+strconv.Itoa(cfg.MaxPending),
	)
	if cfg.ContainerID != "" {
		q = append(q, "containerid="+escapeQuery(cfg.ContainerID))
	}
	if cfg.HostName != "" {
		q = append(q, "hostname="+escapeQuery(cfg.HostName))
	}
	if cfg.MaxSessions != 0 {
		q = append(q, "maxsessions="+strconv.FormatUint(uint64(cfg.MaxSessions), 10))
	}
	if cfg.MaxCredit != 0 {
		q = append(q, "maxcredit="+strconv.Itoa(cfg.MaxCredit))
	}
	if cfg.ReconnectAttempts != 0 {
		q = append(q, "reconnectattempts="+strconv.Itoa(cfg.ReconnectAttempts))
	}
	if cfg.NoReconnect {
		q = append(q, "noreconnect=true")
	}
	if cfg.NonDurable {
		q = append(q, "nondurable=true")
	}
	if cfg.usesTLS() {
		// usesTLS and not `cfg.TLS != nil`: this driver's TLS-ness can live
		// in the ADDRESS SCHEME ("amqps://", "amqp+ssl://"), which go-amqp
		// dials on, and renderAddr strips the scheme -- so a TLS
		// configuration rendered a PLAINTEXT DSN. This repository's own
		// example is that shape (Addrs ["amqps://broker.example:5671"] with
		// SASL PLAIN and no *tls.Config), and its rendering was refused by
		// this driver's own parser, because SASL PLAIN without TLS is
		// refused. A rendering must not lose the transport.
		q = append(q, "tls=true")
	}
	if cfg.InsecureSkipVerify {
		q = append(q, "insecureskipverify=true")
	}
	if cfg.InsecurePlainSASL {
		q = append(q, "insecureplainsasl=true")
	}
	b.WriteByte('?')
	b.WriteString(strings.Join(q, "&"))
	return b.String()
}

// stripScheme removes an AMQP URL's scheme, which [Config.DSN] re-adds as its
// own.
func stripScheme(a string) string {
	for _, p := range [...]string{"amqps://", "amqp+ssl://", "amqp://"} {
		if strings.HasPrefix(a, p) {
			return a[len(p):]
		}
	}
	return a
}

// renderAddr renders one dial address as a DSN endpoint [ParseDSN] reads back.
//
// The host is ESCAPED and the port is not, which is the whole of it. A dial
// address holds a host name as the resolver takes it, and a DSN's authority is
// percent-encoded — so a host that came out of a DSN's own decoding can hold any
// byte at all, including the "%" that begins an escape. Rendering it raw
// produced a connection string this driver's own parser refused, which FuzzDSN
// found in fourteen seconds with `Amqp10://[%]:00000000`: the rendering is what
// goes into a log line a person then pastes, so a rendering that cannot be
// parsed back makes [Config.DSN] useless for the one thing it exists for.
//
// An IPv6 literal loses its brackets and gains two escapes — "::1" renders as
// "%3A%3A1" — which is what the shared parser's own rendering does for the same
// reason: the brackets are the DSN's syntax rather than part of the name, and an
// escaped colon is unambiguous without them.
func renderAddr(a string) string {
	host, port := splitHostPort(stripScheme(a))
	out := escapeDSN(host)
	if port != "" {
		out += ":" + port
	}
	return out
}

// splitHostPort separates a dial address's host from its port, understanding the
// bracketed form of an IPv6 literal.
//
// A colon is only a port separator when what follows it is decimal digits, which
// is what keeps an unbracketed IPv6 literal from being split at its last colon.
//
// It is the exact inverse of `dsn.Host.String`, which is how [ParseInto] turns a
// DSN endpoint into an address: that function brackets any host holding a colon,
// so the closing bracket is the LAST "]:" and not the first "]". Reading the
// first one split `[a:]b]:5672` -- the address a host decoding to `a:]b` becomes
// -- into the host "a:" and nothing else, and the rendering silently dropped
// `b]:5672`.
func splitHostPort(s string) (host, port string) {
	if strings.HasPrefix(s, "[") {
		if i := strings.LastIndex(s, "]:"); i > 0 && allDigits(s[i+2:]) && strings.ContainsRune(s[1:i], ':') {
			return s[1:i], s[i+2:]
		}
		if len(s) >= 2 && strings.HasSuffix(s, "]") && strings.ContainsRune(s[1:len(s)-1], ':') {
			return s[1 : len(s)-1], ""
		}
		// Otherwise the leading "[" is not a bracket at all -- `dsn.Host.String`
		// brackets only a host that holds a colon -- so the address falls
		// through to the unbracketed reading. Treating it as a literal read
		// `[]b:5672` as the empty host and threw `b:5672` away.
	}
	i := strings.LastIndexByte(s, ':')
	if i < 0 || i+1 >= len(s) || !allDigits(s[i+1:]) {
		return s, ""
	}
	return s[:i], s[i+1:]
}

// allDigits reports whether s is one or more decimal digits, which is what the
// shared parser accepts as a port.
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

// escapeDSN percent-encodes the bytes a DSN's authority cannot carry literally.
//
// The set is the shared parser's own, which is what makes the two halves agree:
// anything this escapes, [github.com/iqhive/iqmq/dsn.Parse] decodes.
const escapeDSNSet = "%+ @:/?#&=[],"

// escapeQuerySet is the same thing for a query VALUE, which the shared parser
// reads from its "=" to the next "&" and decodes with "+" standing for a space.
// So the set is exactly "%+&#" -- "#" because a DSN with one ANYWHERE is
// refused, "%" because it begins an escape -- and nothing else: an "=", a "/"
// or an "@" in a value means nothing to the parser, and escaping a byte a
// reader could have read makes a connection string harder to read and longer
// than what it parsed.
//
// `containerid` and `hostname` were written raw, so `containerid=a&b` rendered
// a parameter this driver does not have, `a#b` rendered a DSN with a fragment,
// `a%b` an invalid escape, and `a+b` came back as "a b".
const escapeQuerySet = "%+&#"

// escapeDSN escapes one authority component.
func escapeDSN(s string) string { return escapeInto(s, escapeDSNSet) }

// escapeQuery escapes a query parameter's value.
func escapeQuery(s string) string { return escapeInto(s, escapeQuerySet) }

// escapeInto percent-encodes every byte of s that is in set.
func escapeInto(s, set string) string {
	if !strings.ContainsAny(s, set) {
		return s
	}
	const hexDigits = "0123456789ABCDEF"
	var b strings.Builder
	b.Grow(len(s) + 8)
	for i := range len(s) {
		c := s[i]
		if strings.IndexByte(set, c) >= 0 {
			b.WriteByte('%')
			b.WriteByte(hexDigits[c>>4])
			b.WriteByte(hexDigits[c&0xf])
			continue
		}
		b.WriteByte(c)
	}
	return b.String()
}

// String renders the configuration with the credential redacted.
func (c Config) String() string { return c.DSN() }

// LogValue renders the configuration for slog with the credential redacted.
func (c Config) LogValue() slog.Value { return slog.StringValue(c.DSN()) }

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

// The configuration refusals. The specification names no sentinel for a bad configuration
// and errors.Is matches by code, so a caller compares mq.CodeOf(err) against
// mq.CodeInvalidConfig.
var (
	errNoAddress = errcode.New(errcode.InvalidConfig,
		"iqmq/amqp10: an address in Addrs is empty")
	errBadAddress = errcode.New(errcode.InvalidConfig,
		"iqmq/amqp10: an address in Addrs is not printable ASCII; an international host name is dialled as punycode")
	errBadScheme = errcode.New(errcode.InvalidConfig,
		"iqmq/amqp10: an address in Addrs names a scheme that is not amqp, amqps or amqp+ssl")
	errTLSWithPlaintextAddr = errcode.New(errcode.InvalidConfig,
		"iqmq/amqp10: a TLS config is set beside an address whose scheme is \"amqp://\"; go-amqp picks the transport from the scheme and would dial in the clear, so write \"amqps://\" or drop the TLS config")
	errMixedTransport = errcode.New(errcode.InvalidConfig,
		"iqmq/amqp10: the addresses disagree about TLS; a DSN carries one \"tls=\" for the whole list, so such a list has no rendering and one endpoint would be described wrongly")
	errAddrNotHostPort = errcode.New(errcode.InvalidConfig,
		"iqmq/amqp10: an address in Addrs is more than a host and a port; it holds \"@\", \"/\", \"?\", \"#\" or a backslash, and the user name and password go in Username and Password")
	errDSNTooLong = errcode.New(errcode.InvalidConfig,
		"iqmq/amqp10: this configuration renders a DSN longer than the 8 KiB the shared parser reads, so its own rendering could not be parsed back; the addresses are too long or too many")
	errBadText = errcode.New(errcode.InvalidConfig,
		"iqmq/amqp10: a configured container id, host name or user carries a control character or is not valid UTF-8")
	errSASLMechanism = errcode.New(errcode.InvalidConfig,
		"iqmq/amqp10: SASL is not one of auto, none, anonymous, plain or external")
	errSenderSettle = errcode.New(errcode.InvalidConfig,
		"iqmq/amqp10: SenderSettle is not one of unsettled, settled or mixed")
	errReceiverSettle = errcode.New(errcode.InvalidConfig,
		"iqmq/amqp10: ReceiverSettle is not one of first or second")
	errBackoffPolicy = errcode.New(errcode.InvalidConfig,
		"iqmq/amqp10: Backoff is not one of none, constant, fulljitter, equaljitter or decorrelated")
	errBackoffBounds = errcode.New(errcode.InvalidConfig,
		"iqmq/amqp10: ReconnectBase is longer than ReconnectMax, so the cap would shorten the first delay")
	errFrameSize = errcode.New(errcode.InvalidConfig,
		"iqmq/amqp10: MaxFrameSize below 512 cannot carry an AMQP 1.0 OPEN frame")
	errNegativeBound = errcode.New(errcode.InvalidConfig,
		"iqmq/amqp10: a bound cannot be negative")
	errReconnectGrowth = errcode.New(errcode.InvalidConfig,
		"iqmq/amqp10: ReconnectGrowth must be a finite number")
	errNegativeDuration = errcode.New(errcode.InvalidConfig,
		"iqmq/amqp10: a timeout cannot be negative")
	errPasswordWithoutUser = errcode.New(errcode.InvalidConfig,
		"iqmq/amqp10: a password needs a user name")
	errPlainWithoutUser = errcode.New(errcode.InvalidConfig,
		"iqmq/amqp10: SASL PLAIN needs a user name")
	errInsecureWithoutTLS = errcode.New(errcode.InvalidConfig,
		"iqmq/amqp10: InsecureSkipVerify has no meaning without TLS")
	errPlainWithoutTLS = errcode.New(errcode.InvalidConfig,
		"iqmq/amqp10: SASL PLAIN sends the password in the clear and every address is unprotected; dial amqps:// or set a TLS config, or set InsecurePlainSASL to say so deliberately")
	errNilRegistrar = errcode.New(errcode.InvalidArgument,
		"iqmq/amqp10: Register was given a nil Registrar")
	errBadDSNScheme = errcode.New(errcode.InvalidArgument,
		"iqmq/amqp10: Register was given a string that is not a DSN scheme")
	errNilConn = errcode.New(errcode.InvalidArgument,
		"iqmq/amqp10: OpenWithClient was given a nil connection")
	errNilProfileOpen = errcode.New(errcode.InvalidConfig,
		"iqmq/amqp10: this core cannot be opened without a Profile; it is the AMQP 1.0 core of a broker-specific driver and not a driver of its own")
)

// reconcileBounds is the shared shape of the four "zero is unset vs zero is
// the default" defects the specification records.
//
// A cross-field check in Validate reads a zero as "not set", while resolve and
// [Config.DSN] read the same zero as "the default" -- so a configuration whose
// EFFECTIVE values are exactly what the check refuses passed it, and its own
// rendering then did not parse. Comparing the effective values in Validate
// closes that but over-refuses, turning a one-sided setting whose partner is
// merely defaulted into an error, and those are ordinary intents.
//
// So the EXPLICIT value wins and the DEFAULTED one yields: a defaulted low
// above an explicit high is pulled down to it, and a defaulted high below an
// explicit low is pushed up. Validate then refuses only a contradiction the
// operator actually wrote -- both sides non-zero and inconsistent, which is
// what it refused before this branch -- and the resolved pair is always
// self-consistent, which is what makes the rendering parse back.
func reconcileBounds[T ~int | ~int64](low, high, defLow, defHigh T) (T, T) {
	lowSet, highSet := low != 0, high != 0
	if !lowSet {
		low = defLow
	}
	if !highSet {
		high = defHigh
	}
	switch {
	case !lowSet && low > high:
		low = high
	case !highSet && high < low:
		high = low
	}
	return low, high
}
