package stomp

import (
	"crypto/tls"
	"log/slog"
	"strconv"
	"strings"
	"time"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/dsn"
	"github.com/iqhive/iqmq/internal/activemq"
	"github.com/iqhive/iqmq/internal/activemq/protocol"
	"github.com/iqhive/iqmq/internal/errcode"
)

// The driver's identity.
const (
	// DriverName is the name this driver reports. It is the same driver as
	// `drivers/activemq` -- one broker family, the specification's "protocol choice
	// explicit" -- and the PROFILE is which product over which protocol.
	DriverName = "activemq"
	// Version is this driver's own version, which separates from the
	// client library's.
	Version = "0.1.0"
	// ClientModule is the STOMP client library.
	ClientModule = "github.com/go-stomp/stomp/v3"
	// ClientVersion is the version this driver is built and tested against.
	// It is a constant rather than a build-info lookup because a driver's
	// own report must not depend on whether it was built with module
	// information.
	ClientVersion = "v3.2.1"
)

// The profile names, which are what a conformance report's `profile` field and
// `DriverInfo.Profile` carry. They name the PROTOCOL as well as the product,
// because `drivers/activemq` uses the bare product names and two reports
// cannot share a key.
const (
	// ProfileArtemis is Apache ActiveMQ Artemis over STOMP.
	ProfileArtemis = "artemis-stomp"
	// ProfileClassic is Apache ActiveMQ Classic over STOMP.
	ProfileClassic = "classic-stomp"
)

// The defaults a configuration that names nothing gets.
const (
	// DefaultPort is the port an address that names none is dialled on.
	DefaultPort = "61613"
	// DefaultTLSPort is the STOMP-over-TLS port.
	DefaultTLSPort = "61614"
	// DefaultPrefetch is how many deliveries one source buffers when the
	// caller asks for no prefetch.
	DefaultPrefetch = 32
	// DefaultConsumerWindow is the byte budget Artemis's STOMP flow control
	// is expressed in. It is 1 MiB, the default of Artemis's core protocol
	// (`ActiveMQClient.DEFAULT_CONSUMER_WINDOW_SIZE`), and not that of its
	// STOMP acceptor: a client-acknowledged SUBSCRIBE without
	// `consumer-window-size` gets the acceptor's `stompConsumerWindowSize`,
	// which defaults to 10 KiB (`STOMP_DEFAULT_CONSUMER_WINDOW_SIZE` in
	// Artemis 2.44.0's `TransportConstants`, and measured at about that on
	// the pinned image). This driver always sends the header, so the
	// acceptor's default never applies to it.
	//
	// It is bytes and not messages, which is the one place the portable
	// prefetch cannot be expressed: `SourceConfig.Prefetch` is a COUNT, and
	// Artemis's `consumer-window-size` is a byte budget, so a driver can
	// honour one or the other and not both. This driver bounds the count on
	// its own side, with a buffer of its own, and passes the byte budget to
	// the broker.
	DefaultConsumerWindow = 1 << 20
	// DefaultReceiptTimeout bounds the wait for a RECEIPT frame.
	DefaultReceiptTimeout = 30 * time.Second
	// DefaultDialTimeout bounds one dial.
	DefaultDialTimeout = 15 * time.Second
)

// Broker is which ActiveMQ a client is talking to. It is the shared
// `internal/activemq` type, named here so that a program outside this
// repository can set [Config.Broker] at all: an internal package's constants
// cannot be imported from another module.
type Broker = activemq.Broker

// The brokers [Config.Broker] accepts.
const (
	// Artemis is Apache ActiveMQ Artemis.
	Artemis = activemq.Artemis
	// Classic is Apache ActiveMQ Classic.
	Classic = activemq.Classic
)

// Addressing is how [Config] maps a topic and a subscription to the broker's
// destinations; see [Config.Addressing].
type Addressing = activemq.Addressing

// The addressing modes [Config.Addressing] accepts.
const (
	// AddressFanOut is the default: each subscription is a durable queue of
	// its own and every subscription receives every message.
	AddressFanOut = activemq.AddressFanOut
	// AddressQueue reads from and publishes to a JMS queue by its own name.
	AddressQueue = activemq.AddressQueue
)

// Config is this driver's configuration.
type Config struct {
	// Broker is which ActiveMQ this client is talking to. It is REQUIRED and
	// has no default: the two products' topic mappings name nothing in
	// common, and on Classic a destination's own shape is the only thing
	// that says whether a name is a queue or a topic — so a wrong guess
	// publishes into a queue every consumer is subscribed to and none of
	// them receives anything from, with no error anywhere.
	Broker activemq.Broker

	// Addrs are the broker endpoints, tried round-robin across reconnect
	// attempts. The client's dial loop and each source walk the list on
	// their own, and a source starts at the address the client's connection
	// reached.
	Addrs []string
	// Login and Passcode are the STOMP CONNECT frame's credentials. Both
	// empty connects anonymously, which is what Classic's pinned image
	// allows and Artemis's does not.
	Login    string
	Passcode dsn.Secret
	// Host is the CONNECT frame's `host` header, which STOMP 1.1 made
	// mandatory and which a broker behind a router uses for virtual hosting.
	// Empty sends the address's own host.
	Host string
	// TLS is the TLS configuration. Non-nil dials over TLS.
	TLS *tls.Config
	// InsecureSkipVerify disables certificate verification.
	InsecureSkipVerify bool
	// InsecurePlainLogin permits a passcode over a plaintext connection,
	// which sends it in clear. The name is the specification's `Insecure*` convention:
	// the default refuses.
	InsecurePlainLogin bool

	// Prefix is prepended to every destination so that two deployments can
	// share one broker. On Classic it goes INSIDE the `VirtualTopic.` prefix
	// (`VirtualTopic.<prefix><topic>`), and the ordering was settled by the
	// broker rather than by reasoning: Classic's default
	// virtual-destination interceptor matches `VirtualTopic.>`, so
	// `iqmq.VirtualTopic.orders` is an ordinary topic that accepts every
	// publication and writes to no consumer queue, silently.
	// `internal/activemq.Config.Prefix` has the measurement.
	//
	// Under [AddressQueue] it is prepended to the queue's name, so a caller
	// reading a queue some other application named leaves it empty.
	Prefix string

	// Addressing is how a topic and a subscription map to destinations.
	//
	// The zero value, [AddressFanOut], is the mapping described in the
	// package documentation, and it is unchanged by this field's existence.
	//
	// [AddressQueue] is for a JMS queue that already exists and that this
	// driver does not get to name. A topic is then the queue's name, on both
	// the publish and the consume side; the subscription
	// [github.com/iqhive/iqmq.ConsumerConfig] requires is accepted and
	// ignored; and every consumer of the queue competes for its messages, so
	// nothing fans out. On Artemis the destination is the queue's bare name
	// and the frames ask for the ANYCAST routing type (`destination-type` on
	// a SEND, `subscription-type` on a SUBSCRIBE); on Classic it is
	// `/queue/<name>`.
	Addressing Addressing

	// Selector is a JMS message selector applied to every subscription this
	// client opens (the specification's "selectors via typed config").
	//
	// It is refused on Artemis rather than accepted and dropped, and that is
	// measured: Artemis ignores a `selector` header when the subscription is
	// a fully qualified queue name, which is the only form this driver's
	// durable shared subscription takes, so a caller who set one would be
	// told nothing and filtered nothing.
	Selector string

	// Prioritised reports that the broker reorders a destination's messages
	// by priority.
	//
	// It exists for the same reason [Config.Scheduler] does and it is the
	// same silent absence: ActiveMQ Classic carries the `priority` frame
	// header and dispatches in FIFO order unless the destination's policy
	// sets `prioritizedMessages="true"`, which the pinned image does not do
	// for queues at all. Artemis's queues are priority-ordered and this
	// field is ignored there.
	Prioritised bool

	// Scheduler reports that the broker's message scheduler is enabled.
	// Classic's is off unless the broker is configured with
	// `schedulerSupport="true"`, and with it off a publication carrying
	// `AMQ_SCHEDULED_DELAY` is delivered immediately with the header passed
	// through to the consumer — so a driver that claimed
	// FeatureScheduledPublish regardless would promise a delay the broker
	// discards. Artemis's scheduler is always available and this field is
	// ignored there.
	Scheduler bool

	// DeadLetterTopic routes a dead-lettered message to another topic of
	// this driver's own: the driver PUBLISHES it there and acknowledges the
	// original only once that publication's receipt has arrived, which is the specification's "a failed or ambiguous dead-letter publish never acknowledges
	// the original".
	//
	// Empty leaves it to the broker, which differs between the two products
	// and is why the capability note differs: on Classic a NACK is a
	// poison-ack and reaches the broker's dead-letter strategy, and on
	// Artemis a NACK destroys the message, so `Nack(NackDeadLetter())` is
	// refused there unless this is set.
	//
	// It needs receipts, so Validate refuses it with NoReceipts.
	DeadLetterTopic string

	// Receipts asks for a RECEIPT frame on every publication and every
	// acknowledgement, which is what moves the confirmation boundary from
	// "the frame was written" to "the broker has it".
	//
	// It is ON by default, and that is the opposite of a performance
	// default on purpose: without it a STOMP publication is fire-and-forget
	// and ConfirmAccepted cannot be honoured at all, so a sink would
	// have to refuse the confirmation mode a caller gets by not choosing.
	Receipts bool
	// NoReceipts turns them off, for a publisher that has chosen
	// ConfirmNone for every publication and wants the round trip back. It is
	// a separate field rather than `Receipts` defaulting to false because
	// the zero Config should be the safe one. The capability table then
	// withdraws the publisher confirms along with the receipts, and a
	// DeadLetterTopic is refused.
	NoReceipts bool
	// ReceiptTimeout bounds the wait for a RECEIPT. Zero takes
	// DefaultReceiptTimeout.
	ReceiptTimeout time.Duration

	// Prefetch is how many deliveries one source buffers for Fetch. Zero
	// takes DefaultPrefetch. `SourceConfig.Prefetch` overrides it per
	// source. On Classic it is also the broker's own window,
	// `activemq.prefetchSize`, and so the most a source is sent before it
	// acknowledges; on Artemis that bound is ConsumerWindow. A source reads
	// everything the window lets the broker send, whether or not the buffer
	// has room for it yet (see [Source]).
	Prefetch int
	// PrefetchBytes is the byte budget the same buffer bounds. It bounds the
	// deliveries waiting for a fetch and not what a source holds, which is
	// the broker's window: on Classic a byte bound on a source comes from
	// Prefetch and the size of the largest message. Zero leaves the buffer
	// bounded by the count alone.
	PrefetchBytes int64
	// ConsumerWindow is the byte budget sent to Artemis as
	// `consumer-window-size`. Artemis charges it for each MESSAGE frame it
	// sends and refunds it on the ACK, so it is the most a source holds
	// there. Zero takes DefaultConsumerWindow.
	ConsumerWindow int

	// HeartbeatSend and HeartbeatReceive are the CONNECT frame's heart-beat
	// pair. Both zero disables heart-beats, which is what a test wants and
	// what a deployment should not have: without them a connection that
	// died silently is discovered by the next write.
	//
	// A source's own connection keeps sending heart-beats while its buffer
	// is full, however long the caller leaves it unfetched, because what
	// reads its subscription never waits on the caller (see [Source]).
	HeartbeatSend    time.Duration
	HeartbeatReceive time.Duration

	// DialTimeout bounds one dial. Zero takes DefaultDialTimeout. It holds
	// under a caller's deadline too: the sooner of the two ends the dial.
	DialTimeout time.Duration
	// ReconnectBase, ReconnectMax and ReconnectGrowth shape the backoff.
	ReconnectBase   time.Duration
	ReconnectMax    time.Duration
	ReconnectGrowth float64
	// ReconnectAttempts bounds the dials made for ONE outage, which is how
	// `driverkit.Reconnector` defines it: the count starts again after every
	// connection that succeeds. Zero is no limit. A source's own connection
	// is held to the same budget, and a connection that has spent it is
	// terminal: every operation that needs it afterwards reports a
	// non-retryable CodeUnavailable. A source re-dials inside a fetch, and a
	// dial that the fetch's own context ends is not counted.
	ReconnectAttempts int
	// NoReconnect makes a lost connection terminal, the backend's and each
	// source's own alike, and every operation that needs it afterwards
	// reports a non-retryable CodeUnavailable. Once the backend's connection
	// is gone, Health stays Degraded with that refusal as its error.
	NoReconnect bool

	// Metadata is the mode for metadata the protocol has no place for.
	Metadata mq.MetadataMode

	// Clock is the clock the driver reads. Nil uses the real one.
	//
	// It is read on the publish path and that is not incidental: STOMP's
	// `expires` header is an ABSOLUTE instant and TTL is a relative
	// duration, so a publication with a TTL is resolved against this clock
	// and the shared mapping refuses one that was not.
	Clock mq.Clock
	// Hooks are the driver's own events.
	Hooks mq.Hooks
}

// resolve fills the defaults, leaving the caller's value untouched.
func (c Config) resolve() Config {
	if len(c.Addrs) == 0 {
		c.Addrs = []string{"localhost:" + DefaultPort}
	}
	if c.Prefetch <= 0 {
		c.Prefetch = DefaultPrefetch
	}
	if c.ConsumerWindow <= 0 {
		c.ConsumerWindow = DefaultConsumerWindow
	}
	if c.ReceiptTimeout <= 0 {
		c.ReceiptTimeout = DefaultReceiptTimeout
	}
	if c.DialTimeout <= 0 {
		c.DialTimeout = DefaultDialTimeout
	}
	c.Receipts = !c.NoReceipts
	return c
}

// Validate refuses a configuration before any I/O.
func (c Config) Validate() error {
	if !c.Broker.Valid() {
		return activemq.ErrNoBroker
	}
	if !c.Metadata.Valid() {
		return protocol.ErrBadMetadataMode
	}
	if !c.Addressing.Valid() {
		return activemq.ErrBadAddressing
	}
	if c.Receipts && c.NoReceipts {
		return errReceiptsBoth
	}
	if c.Selector != "" && c.Broker == activemq.Artemis {
		return errArtemisSelector
	}
	if c.Prefix != "" {
		if err := activemq.CheckName(c.Prefix, "prefix"); err != nil {
			return err
		}
	}
	if c.DeadLetterTopic != "" {
		if err := activemq.CheckTopic(c.DeadLetterTopic); err != nil {
			return err
		}
		// The route acknowledges the original only once the copy's RECEIPT
		// arrives, and a client with receipts off has none to wait for: its
		// dead-letter sink was refused at open, so every Nack(DeadLetter)
		// failed and every undecodable delivery stayed where it was, behind
		// a capability table that advertised the route.
		if c.NoReceipts {
			return errDeadLetterNeedsReceipts
		}
	}
	// The resolved default is localhost, so an EMPTY slice is fine and an
	// empty STRING in the slice is not: a caller who built the list from
	// configuration and got a blank line should be told. What this must NOT
	// do is return here -- an early return skipped every check below,
	// including the plaintext-passcode refusal, so `Config{Broker: Artemis,
	// Passcode: "x"}` was accepted and then dialled plaintext localhost
	// with the credential on the wire.
	for _, a := range c.Addrs {
		if strings.TrimSpace(a) == "" {
			return errEmptyAddr
		}
		if strings.ContainsAny(a, " \t\r\n") {
			return errBadAddr
		}
	}
	if c.Passcode != "" && !c.usesTLS() && !c.InsecurePlainLogin {
		return errPlainPasscode
	}
	if c.Prefetch < 0 || c.PrefetchBytes < 0 || c.ConsumerWindow < 0 || c.ReconnectAttempts < 0 {
		return errNegative
	}
	if c.ReceiptTimeout < 0 || c.DialTimeout < 0 || c.ReconnectBase < 0 || c.ReconnectMax < 0 ||
		c.HeartbeatSend < 0 || c.HeartbeatReceive < 0 {
		return errNegative
	}
	return nil
}

// usesTLS reports whether every endpoint is protected.
//
// The credential check reads EVERY address rather than the first, because a
// failover list that mixes a TLS node and a plaintext one sends the passcode
// in clear as soon as it fails over -- which is the one moment nobody is
// watching.
//
// "Protected" is [isTLSAddr], the same test the dialler applies. This kept a
// list of its own that named two of the four schemes, so `stomps://` and
// `tls://` were dialled over TLS and their passcode refused as plaintext.
func (c Config) usesTLS() bool {
	if c.TLS != nil {
		return true
	}
	for _, a := range c.Addrs {
		if !isTLSAddr(a) {
			return false
		}
	}
	return len(c.Addrs) > 0
}

// Names derives the topology for one topic and one subscription.
func (c Config) Names(topic, subscription string) (activemq.Names, error) {
	return activemq.Derive(activemq.Config{
		Broker:          c.Broker,
		Prefix:          c.Prefix,
		Topic:           topic,
		Subscription:    subscription,
		DeadLetterTopic: c.DeadLetterTopic,
		Addressing:      c.Addressing,
		// STOMP says which namespace a Classic destination is in with
		// `/topic/` and `/queue/` rather than with a scheme, and the
		// shared mapping refuses the other protocol's form outright:
		// getting it wrong does not fail, it publishes into a queue
		// nothing is subscribed to.
		STOMP: true,
	})
}

// profileName is the profile the report and DriverInfo carry.
func (c Config) profileName() string {
	if c.Broker == activemq.Classic {
		return ProfileClassic
	}
	return ProfileArtemis
}

// protocolOptions is what the shared mapping needs beyond the message.
func (c Config) protocolOptions() protocol.Options {
	return protocol.Options{
		Broker:     c.Broker,
		Metadata:   c.Metadata,
		Persistent: true,
		KeyHeader:  protocol.KeyHeader,
		KeyAsGroup: true,
	}
}

// String renders the configuration as its DSN, with the passcode redacted.
func (c Config) String() string { return c.DSN() }

// LogValue is the redacting form `slog` uses.
func (c Config) LogValue() slog.Value { return slog.StringValue(c.DSN()) }

// itoa is strconv.Itoa, named so that the DSN renderer reads as a list.
func itoa(n int) string { return strconv.Itoa(n) }

// The configuration refusals.
var (
	errReceiptsBoth = errcode.New(errcode.InvalidConfig,
		"iqmq/activemq/stomp: Receipts and NoReceipts are both set")
	errArtemisSelector = errcode.New(errcode.Unsupported,
		"iqmq/activemq/stomp: Artemis ignores a STOMP `selector` header on a fully qualified queue name, which is the only form this driver's durable shared subscription takes, so a selector here would filter nothing and say nothing; use drivers/activemq with a queue whose filter Admin set, or ActiveMQ Classic")
	errEmptyAddr = errcode.New(errcode.InvalidConfig,
		"iqmq/activemq/stomp: one of the addresses is empty")
	errBadAddr = errcode.New(errcode.InvalidConfig,
		"iqmq/activemq/stomp: an address contains whitespace")
	errPlainPasscode = errcode.New(errcode.InvalidConfig,
		"iqmq/activemq/stomp: a STOMP passcode travels in the CONNECT frame in clear and not every address is protected; use TLS, or set InsecurePlainLogin to say so deliberately")
	errNegative = errcode.New(errcode.InvalidConfig,
		"iqmq/activemq/stomp: a count or a duration is negative")
	errDeadLetterNeedsReceipts = errcode.New(errcode.InvalidConfig,
		"iqmq/activemq/stomp: DeadLetterTopic acknowledges the original only once the dead-letter copy's RECEIPT arrives, and NoReceipts turns receipts off; turn receipts on or leave DeadLetterTopic empty")
	errNilRegistrar = errcode.New(errcode.InvalidArgument,
		"iqmq/activemq/stomp: Register was given no registry")
)
