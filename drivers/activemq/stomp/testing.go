package stomp

import (
	"context"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-stomp/stomp/v3/frame"

	mq "bitbucket.org/iqhive/iqmq"
	"bitbucket.org/iqhive/iqmq/driverkit"
	"bitbucket.org/iqhive/iqmq/dsn"
	"bitbucket.org/iqhive/iqmq/internal/activemq"
	"bitbucket.org/iqhive/iqmq/internal/activemq/protocol"
	"bitbucket.org/iqhive/iqmq/testkit"
)

// ConformanceHarness is what this driver hands the conformance kit.
//
// It is exported, and in a non-test file, for the reason every landed driver's
// is: two modules run it. This module's own env-gated test runs it against an
// endpoint named by an environment variable, and `conformance/suites` runs it
// against the pinned container, which is the evidence the generated capability
// matrix is built from.
//
// It provisions the topology every scenario needs, because this driver creates
// none. Over STOMP the only topology operation there is IS a subscription, so
// that is what the provisioner does: it subscribes to each subscription the
// Need names and lets go again. On Classic that creates the consumer queue,
// which has to exist before the first publication or the virtual topic copies
// nothing into it; on Artemis it fails against a broker with auto-creation
// disabled, and the suite there supplies a provisioner of its own that uses
// `drivers/activemq`'s Admin (admin.go says why).
func ConformanceHarness(cfg Config, ev testkit.Evidence, broker testkit.Broker) testkit.Harness {
	return ConformanceHarnessWithProvisioner(cfg, ev, broker, nil)
}

// ConformanceHarnessWithProvisioner is [ConformanceHarness] with the topology
// step replaced, which is what the Artemis suite needs: a STOMP client cannot
// create an address on a broker that will not auto-create one, so the Artemis
// run provisions over AMQP 1.0 and then consumes over STOMP.
//
// A nil provisioner takes this driver's own, which is a subscribe-and-release.
func ConformanceHarnessWithProvisioner(cfg Config, ev testkit.Evidence, broker testkit.Broker,
	provision func(ctx context.Context, cfg Config, n testkit.Need) error,
) testkit.Harness {
	nonce := strconv.FormatInt(time.Now().UnixNano(), 36)
	var seq counter
	backends := &backendRegistry{}
	if provision == nil {
		provision = Provision
	}
	return testkit.Harness{
		Driver:   DriverName,
		Profile:  cfg.profileName(),
		Evidence: ev,
		Broker:   broker,
		Client:   testkit.ClientInfo{Module: ClientModule, Version: ClientVersion},
		Timeout:  45 * time.Second,
		// The specification asks for 100,000 interleaved messages. Every publication
		// here is a SEND frame and a RECEIPT the broker commits before it
		// answers, and both brokers are a JVM writing a journal, so the
		// figure is lowered to keep one scenario inside the suite's
		// per-scenario timeout. docs/drivers/activemq-stomp.md says so,
		// which is what the specification asks of a driver that lowers it.
		Fidelity: 2_000,
		TopicName: func(purpose string) string {
			// The name has to survive `internal/activemq`'s validation AND
			// be a queue name on Artemis, where the derived queue is
			// `<topic>.<subscription>` and a queue name is unique across
			// the whole broker -- so two scenarios sharing a topic would
			// share a queue and the second would consume the first's
			// messages.
			return "iqmq-conf-" + nonce + "-" + strconv.FormatUint(seq.add(), 36) + "-" + sanitiseTopic(purpose)
		},
		Secrets:             secretsOf(cfg),
		OpenWrongCredential: openWrongCredential(cfg),
		Open: func(ctx context.Context, n testkit.Need) (*mq.Client, error) {
			shaped, err := shape(cfg, n)
			if err != nil {
				return nil, err
			}
			if err := provision(ctx, shaped, n); err != nil {
				return nil, err
			}
			b, err := OpenBackend(ctx, shaped)
			if err != nil {
				return nil, err
			}
			c := mq.NewClient(b, clientOptions(shaped)...)
			backends.remember(c, b)
			return c, nil
		},
		Quiesce: backends.quiesce,
		// The pattern is supplied even though this driver does NOT claim
		// FeatureWildcardSubscribe, which is the lesson: without it the
		// kit's negative probe skips and the claim stays unverified in both
		// directions. With it the probe subscribes on `<topic>.*`,
		// `internal/activemq.CheckTopic` answers ErrUnsupported, and the
		// claim is proved rather than taken on trust.
		Wildcard: func(topic string) string { return topic + ".*" },
		Skip:     conformanceSkips(cfg),
	}
}

// Provision brings each subscription's destination into existence by
// subscribing to it and letting go again.
//
// That is the whole of STOMP's topology API, and doing it here rather than in
// the driver's own consume path is the point: the specification makes implicit creation a
// defect, so the driver never does it and a deployment or a test does it
// deliberately.
func Provision(ctx context.Context, cfg Config, n testkit.Need) error {
	topics := make([]string, 0, 2)
	topics = append(topics, n.Topic)
	if n.DeadLetterTopic != "" {
		topics = append(topics, n.DeadLetterTopic)
	}
	b, err := OpenBackend(ctx, cfg)
	if err != nil {
		return err
	}
	defer func() { _ = b.Close() }()
	for _, t := range topics {
		for i := range max(1, len(n.Subscriptions)) {
			src, err := b.OpenSource(ctx, mq.SourceConfig{
				Topic:        t,
				Subscription: n.Sub(i),
				// One deep enough to attach and no deeper: this source
				// exists to create the destination and must not take
				// delivery of anything already in it.
				Prefetch: 1,
			})
			if err != nil {
				return err
			}
			// On ActiveMQ Classic the SUBSCRIBE IS the topology
			// operation -- a consumer queue comes into existence when a
			// subscription attaches to it and nothing else creates one
			// over this protocol -- which is why `OpenSource` above has
			// already sent it. `warm` covers the one
			// case that leaves: a source whose socket died between the
			// open and here, which it re-subscribes. It is a no-op
			// otherwise, and it CANNOT confirm the frame arrived -- a
			// SUBSCRIBE carries no receipt , so what
			// orders this against the publication that follows is the
			// socket and the `Close` below, not an acknowledgement.
			if werr := b.warm(ctx); werr != nil {
				_ = src.Close()
				return werr
			}
			if err := src.Close(); err != nil {
				return err
			}
		}
	}
	return nil
}

// shape applies the parts of a scenario's Need that are CONFIGURATION here,
// and refuses the parts this protocol cannot express at all.
//
// The rule for what to refuse is the narrow one the specification recorded: refuse a Need
// only when accepting it would make a scenario prove something FALSE.
// `Wildcards` in particular is accepted and provisioned
// as an ordinary topic, which is what makes the negative claim provable.
func shape(cfg Config, n testkit.Need) (Config, error) {
	out := cfg
	// ADAPTIVE mode, which is an ASK a deployment makes once and
	// which the kit cannot make because it configures every handle at the
	// zero value — so it has to be here, exactly as `drivers/nsq`'s harness
	// makes the same ask for the same reason.
	//
	// Without it three scenarios fail and all three are the same fact: a
	// STOMP frame header is TEXT. `publish/key-roundtrip` publishes a key of
	// "\x00\xff", `metadata/envelope-roundtrip` publishes a header value
	// that is not valid UTF-8, and `publish/roundtrip-bodies` publishes a
	// NIL body and requires it to come back distinguishable from an empty
	// one — which a frame body cannot express either. The specification promises all
	// three everywhere and the envelope is how the promise is kept on a
	// broker that cannot keep it; `MetadataAdaptive` envelopes only the
	// messages that need it, so the wire is unchanged for the ones that do
	// not.
	out.Metadata = mq.MetadataAdaptive
	if n.DeadLetterTopic != "" {
		out.DeadLetterTopic = n.DeadLetterTopic
	}
	if n.Partitions > 1 {
		return Config{}, unsupportedNeed("several partitions; a STOMP destination is the only lane there is, so everything would arrive in order for the wrong reason")
	}
	if n.Retain {
		return Config{}, unsupportedNeed("a retained topic; a SUBSCRIBE reads what a destination holds rather than a history, so a replay scenario would read an empty queue and call it a pass")
	}
	if n.Capacity > 0 && n.Capacity < out.Prefetch {
		out.Prefetch = n.Capacity
	}
	return out, nil
}

// unsupportedNeed is the refusal the kit turns into a skip with a reason.
func unsupportedNeed(what string) error { return &needError{what: what} }

// needError names the topology this driver cannot build. It wraps
// ErrUnsupported so that the kit's errors.Is finds the answer, and carries
// the reason so that the skip in the report says what was missing.
type needError struct{ what string }

// Error renders the refusal.
func (e *needError) Error() string { return "iqmq/activemq/stomp: this driver cannot build " + e.what }

// Unwrap reaches the sentinel.
func (e *needError) Unwrap() error { return mq.ErrUnsupported }

// conformanceSkips names the scenarios this combination documents as
// inapplicable, with the reason, which the report publishes.
//
// A skip is never a pass: the claim a skipped scenario would have proved stays
// unverified in the generated matrix, which is the only honest answer for
// something nobody checked. Every entry names something somebody watched fail.
func conformanceSkips(cfg Config) map[string]string {
	out := map[string]string{}
	if cfg.DeadLetterTopic == "" {
		// This configuration publishes no dead letter: a rejected delivery
		// reaches the broker's own strategy on Classic and is destroyed on
		// Artemis, and there is no publication of the driver's to fail. The
		// scenario is about a driver that publishes one and must not
		// acknowledge the original when that publication fails.
		out["dlq/failed-publish-never-acks"] = "this configuration publishes no dead letter: on ActiveMQ Classic a NACK frame is a poison-ack that reaches the broker's own dead-letter strategy and on Artemis it destroys the message, and neither is a publication of the driver's that can fail. Set Config.DeadLetterTopic for the route this scenario is about"
	}
	if cfg.Metadata == mq.MetadataAdaptive || cfg.Metadata == mq.MetadataPortableEnvelope {
		// This harness configures the envelope, so a STRICT publisher's
		// refusal cannot be exercised: the scenario opens one with
		// PublisherConfig.Metadata and this driver honours the client's
		// mode for the handles the kit opens with a zero config, which is
		// every other scenario. `drivers/nsq`'s harness documents the same
		// skip for the same reason.
		out["metadata/native-strict-rejection"] = "this harness configures the adaptive envelope, because the specification promises binary and duplicate headers everywhere and a STOMP frame header is text; a strict publisher's refusal is exercised by TestSinkRefusesWhatItCannotRepresent in the driver's own tests instead"
	}
	// The two things the broker does AT DISPATCH, which a push protocol
	// with a prefetch spends before a caller can see them. Both are
	// measured and both are the SAME fact: a STOMP subscription exists from
	// the moment the source opens, so the broker sends as the scenario
	// publishes and this driver buffers what arrives -- the ordering the
	// broker applied and the expiry it would have applied are both in the
	// past by the time Receive is called. Neither claim is false: a message
	// that sits in the queue IS reordered and IS swept (measured against
	// the pinned image with no consumer attached). What cannot be
	// demonstrated is the claim through THIS driver's consumer.
	out["capability/Priority/works"] = "a STOMP subscription is a push and this driver buffers: the broker orders a destination's messages at DISPATCH, and by the time a caller receives, the dispatch has already happened in publication order. Measured with no consumer attached, both brokers do reorder; measured through a consumer, the probe reads the low-priority message first. drivers/activemq over AMQP 1.0 proves the claim, because a receiving link's credit is issued on the first fetch"
	out["capability/MessageTTL/works"] = "the same fact: a message whose TTL expires AFTER it has been pushed to this driver's buffer is delivered, because the broker dropped nothing -- it had already handed the message over. Measured with no consumer attached, Classic does drop an expired message at dispatch; drivers/activemq over AMQP 1.0 proves the claim"
	out["start/positions"] = "a SUBSCRIBE reads what a destination holds. STOMP has no committed position and no retained history, so StartLatest and StartEarliest are the same place and a consumer opened after a publication still receives it; the driver refuses a time or a sequence rather than pretending to one"
	if cfg.Broker == activemq.Artemis {
		out["capability/ServerFilter/works"] = "measured: Artemis ignores a STOMP `selector` header when the subscription is a fully qualified queue name, which is the only form this driver's durable shared subscription takes, so Config.Validate refuses a selector here and there is nothing to exercise"
	}
	return out
}

// secretsOf is the credentials the kit must find in no rendered string.
// openWrongCredential dials with the credential mangled, for the one error a
// client library is most likely to render a connection string into.
//
// Nil when there is nothing to get wrong: a broker this harness reaches
// without authenticating has no authentication failure to produce, and
// `secrets/open-refusal` skips rather than passing on a refusal that never
// happened.
//
// The credential is CHANGED rather than cleared, because an empty one can
// take a different path in the client library -- a different mechanism
// offered, a different refusal, or an anonymous fallback -- and the error
// this is about is the one where the server rejected a credential it was
// given. The dial is bound to a single attempt: without that, the driver's
// reconnect loop retries the refusal until the scenario's context expires and
// the probe reads a deadline rather than the broker's answer.
func openWrongCredential(cfg Config) func(context.Context) error {
	if cfg.Passcode.Reveal() == "" {
		return nil
	}
	wrong := cfg
	wrong.Passcode = dsn.Secret(cfg.Passcode.Reveal() + "-wrong")
	wrong.ReconnectAttempts = 1
	wrong.DialTimeout = 5 * time.Second
	return func(ctx context.Context) error {
		c, err := Open(ctx, wrong)
		if err == nil {
			//nolint:contextcheck // Close is the prompt close and takes no context by design
			_ = c.Close()
			return nil
		}
		return err
	}
}

func secretsOf(cfg Config) []string {
	if s := cfg.Passcode.Reveal(); s != "" {
		return []string{s}
	}
	return nil
}

// sanitiseTopic turns a scenario's purpose into a name both brokers accept.
//
// A "." is mapped away as well as the obvious refusals, which is Artemis's
// constraint rather than a general one: the derived queue name is
// `<topic>.<subscription>` and `activemq.ParseQueue` takes it apart at the
// last ".", so a topic containing one would parse back as a different topic.
func sanitiseTopic(purpose string) string {
	return strings.Map(func(r rune) rune {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '-', r == '_':
			return r
		default:
			return '-'
		}
	}, purpose)
}

// backendRegistry remembers which backend belongs to which client, so that
// Quiesce can reach it.
//
// It exists because of a defect the specification recorded and this driver would have
// repeated: a harness that reached for `Backend.Native` gets whatever the
// driver put there, a type assertion fails silently, and the quiesce becomes a
// no-op.
type backendRegistry struct {
	mu sync.Mutex
	m  map[*mq.Client]*Backend
}

// remember records the pair.
func (r *backendRegistry) remember(c *mq.Client, b *Backend) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.m == nil {
		r.m = make(map[*mq.Client]*Backend, 4)
	}
	r.m[c] = b
}

// quiesce waits for everything published so far to have been acted on.
//
// Every publication in a receipted configuration has already been committed by
// the broker before Publish returned, so there is nothing to wait for and the
// honest answer is a liveness check. What it ALSO has to do is what the AMQP
// core's WarmForTest does: several of the probes publish, quiesce, then
// receive once, and a subscription that has not been established yet would
// read nothing -- so the quiesce ensures every source is subscribed.
func (r *backendRegistry) quiesce(ctx context.Context, c *mq.Client) error {
	r.mu.Lock()
	b := r.m[c]
	r.mu.Unlock()
	if b == nil {
		return errNoBackendForClient
	}
	if err := b.Ping(ctx); err != nil {
		return err
	}
	return b.warm(ctx)
}

// warm re-establishes any source whose subscription is not live.
//
// It was the STOMP equivalent of the AMQP core's WarmForTest and it existed
// for that helper's measured reason : a probe that
// publishes, waits and then receives once reads nothing at all unless the
// subscription was already in place, because a destination only copies a
// publication into the subscriptions that exist when it is published.
//
// **On this driver `OpenSource` already does that**, eagerly and by design
// , so for a source whose connection has not moved this is
// a NO-OP -- `ensureSubscribed` returns at once when the subscription is live
// and the generation still matches. What is left is the case the eager
// subscribe does not cover: a source whose socket died between the open and
// here, which this re-subscribes rather than leaving for its first fetch.
//
// What it does NOT do, and what nothing in this driver can do, is wait for
// the broker to have READ the frame: `Conn.Subscribe` returns when the frame
// is queued and a SUBSCRIBE carries no receipt. A test
// that needs the subscription to exist server-side waits for the frame at the
// server (`stompsrv.Server.WaitSubscribed`); a probe against a real broker
// has only the ordering the socket gives it.
func (b *Backend) warm(ctx context.Context) error {
	b.mu.Lock()
	srcs := make([]*Source, 0, len(b.srcs))
	for s := range b.srcs {
		srcs = append(srcs, s)
	}
	b.mu.Unlock()
	for _, s := range srcs {
		if err := s.ensureSubscribed(ctx); err != nil {
			return err
		}
	}
	return nil
}

// counter is the harness's topic sequence.
type counter struct {
	mu sync.Mutex
	n  uint64
}

// add increments and returns the counter.
func (c *counter) add() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.n++
	return c.n
}

// The exported test accessors. Each reports something the portable API cannot
// show, and each is exported because this module's critical tests live in
// another package.

// CapabilitiesForTest is the table this configuration would publish.
//
// The configuration is RESOLVED first, as [SendHeadersForTest] resolves it and
// as every real Open does. Without that an unresolved `Config` reads as
// `NoReceipts` -- the resolved default turns receipts ON -- and the table
// withdrew three rows a real client has, so a test asserting on the table was
// asserting on a client nobody can open.
func CapabilitiesForTest(cfg Config) (mq.Capabilities, error) { return cfg.resolve().capabilities() }

// DestinationsForTest is the two destinations a topic and a subscription map
// to, which is what a provisioner and an operator both need.
func DestinationsForTest(cfg Config, topic, subscription string) (publish, consume string, err error) {
	n, err := cfg.Names(topic, subscription)
	if err != nil {
		return "", "", err
	}
	return n.Publish, n.Consume, nil
}

// SubscribeHeadersForTest is the SUBSCRIBE frame's headers, so that the
// broker-specific ones are pinned by a test rather than only by a container.
func SubscribeHeadersForTest(cfg Config, prefetch int) map[string]string {
	b := &Backend{cfg: cfg.resolve()}
	s := &Source{b: b, cfg: mq.SourceConfig{Prefetch: prefetch}}
	return headersOf(s.subscribeOptions())
}

// headersOf renders a set of frame options as the headers they set, which is
// how a test reads what the driver would have sent.
func headersOf(opts []func(*frame.Frame) error) map[string]string {
	f := frame.New(frame.SUBSCRIBE)
	for _, o := range opts {
		if o == nil {
			continue
		}
		_ = o(f)
	}
	out := make(map[string]string, f.Header.Len())
	for i := range f.Header.Len() {
		k, v := f.Header.GetAt(i)
		out[k] = v
	}
	return out
}

// SendHeadersForTest is the SEND frame's headers for one message, which is the
// mapping's whole output and the thing a broker's behaviour depends on.
func SendHeadersForTest(cfg Config, m *mq.Message, o *mq.PublishOptions, clock mq.Clock) (map[string]string, error) {
	r := cfg.resolve()
	if err := r.Validate(); err != nil {
		return nil, err
	}
	b := &Backend{cfg: r, clock: clock}
	if b.clock == nil {
		b.clock = wallClock{}
	}
	caps, err := r.capabilities()
	if err != nil {
		return nil, err
	}
	base, err := driverkit.NewBase(driverkit.BaseConfig{
		Info: r.info(), Capabilities: caps, Ownership: driverkit.ClientBorrowed,
	}, nil)
	if err != nil {
		return nil, err
	}
	b.BaseBackend = base
	p, err := protocol.New(protocol.STOMP)
	if err != nil {
		return nil, err
	}
	env, err := newEnvelope(r, caps)
	if err != nil {
		return nil, err
	}
	s := &Sink{b: b, topic: "t", proto: p, envelope: env, receipts: r.Receipts, confirm: mq.ConfirmAccepted}
	s.mu.Lock()
	_, opts, err := s.frameFor(m, o)
	if err != nil {
		s.mu.Unlock()
		return nil, err
	}
	ct := s.fields.ContentType
	s.mu.Unlock()
	out := headersOfSend(opts)
	if ct != "" {
		out["content-type"] = ct
	}
	return out, nil
}

// headersOfSend renders SEND options as their headers.
func headersOfSend(opts []func(*frame.Frame) error) map[string]string {
	f := frame.New(frame.SEND)
	for _, o := range opts {
		if o == nil {
			continue
		}
		_ = o(f)
	}
	out := make(map[string]string, f.Header.Len())
	for i := range f.Header.Len() {
		k, v := f.Header.GetAt(i)
		out[k] = v
	}
	return out
}

// ClockForTest is the clock a backend resolved to.
//
// It exists for one thing a unit test of this package cannot otherwise see:
// every test here configures a clock, so the DEFAULT -- the wall clock a real
// deployment gets -- was built by nothing and its timers fired in no test. A
// timer that never fires is a driver whose receipts never time out.
func ClockForTest(b *Backend) mq.Clock { return b.clock }
