package activemq

import (
	"context"
	"encoding/json"
	"strconv"
	"strings"
	"sync"
	"time"

	mq "github.com/iqhive/iqmq"
	core "github.com/iqhive/iqmq/drivers/amqp10"
	"github.com/iqhive/iqmq/dsn"
	"github.com/iqhive/iqmq/internal/activemq"
	"github.com/iqhive/iqmq/testkit"
)

// ConformanceHarness is what this driver hands the conformance kit.
//
// It is exported, and in a non-test file, for the reason every landed driver's
// is: two different modules run it. This module's own env-gated test runs it
// against an endpoint named by an environment variable, and
// `conformance/suites` runs it against the pinned container, which is the
// evidence the generated capability matrix is built from. A harness duplicated
// in two places would drift, and the half that drifted would be the one nobody
// ran.
//
// It provisions the topology every scenario needs, because this driver creates
// none: on Artemis through this driver's own [mq.Admin], and on Classic by
// attaching a receiver to each subscription's consumer queue and letting go
// again. The Classic half is not a workaround — it is the broker's topology
// API. A virtual topic copies a publication into the consumer queues that
// EXIST when it is published, so a queue created after the publication gets
// nothing, and measured against the pinned image a scenario that published
// first and subscribed second received nothing at all.
func ConformanceHarness(cfg Config, ev testkit.Evidence, broker testkit.Broker) testkit.Harness {
	return ConformanceHarnessWithProvisioner(cfg, ev, broker, nil)
}

// ConformanceHarnessWithProvisioner is [ConformanceHarness] with the topology
// step replaced, for a suite that creates its topology another way — a
// deployment whose broker is administered by something else, or a matrix run
// that wants to see what a driver does against topology it did not make.
//
// A nil provisioner takes this driver's own, which is what the shipped
// evidence is taken with.
func ConformanceHarnessWithProvisioner(cfg Config, ev testkit.Evidence, broker testkit.Broker,
	provision func(ctx context.Context, cfg Config, n testkit.Need) error,
) testkit.Harness {
	nonce := strconv.FormatInt(time.Now().UnixNano(), 36)
	var seq atomic64
	backends := &backendRegistry{}
	if provision == nil {
		provision = Provision
	}
	return testkit.Harness{
		Driver:   DriverName,
		Profile:  cfg.profileName(),
		Evidence: ev,
		Broker:   broker,
		Client:   testkit.ClientInfo{Module: core.ClientModule, Version: core.ClientVersion},
		Timeout:  45 * time.Second,
		// The specification asks for 100,000 interleaved messages. Every publication here
		// is a transfer whose disposition the broker commits before it
		// answers, and both of these brokers are a JVM writing a journal, so
		// the figure is lowered to keep one scenario inside the suite's
		// per-scenario timeout. docs/drivers/activemq.md says so, which is
		// what the specification asks of a driver that lowers it.
		Fidelity: 5_000,
		TopicName: func(purpose string) string {
			// The nonce comes first so that a truncation cannot make two
			// names equal. The name has to survive `internal/activemq`'s
			// validation AND be a queue name on Artemis, where the derived
			// queue is `<topic>.<subscription>` and a queue name is unique
			// across the whole broker — so two scenarios sharing a topic
			// would share a queue and the second would consume the first's
			// messages.
			return "iqmq-conf-" + nonce + "-" + strconv.FormatUint(seq.add(1), 36) + "-" + sanitiseTopic(purpose)
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
			// The BACKEND is opened and the client built around it, rather
			// than [Open] being called, because Quiesce needs the backend
			// and cannot get it from the client: `Backend.Native` answers
			// the peer CONNECTION, which is the escape hatch and the right
			// answer for a caller reaching past the SPI. So the harness
			// remembers the pair it made (the shape of).
			b, err := OpenBackend(ctx, shaped)
			if err != nil {
				return nil, err
			}
			c, err := ClientForBackend(b, shaped)
			if err != nil {
				return nil, err
			}
			backends.remember(c, b)
			return c, nil
		},
		Quiesce: backends.quiesce,
		// The pattern is supplied even though this driver does NOT claim
		// FeatureWildcardSubscribe, which is the lesson: without it the
		// kit's negative probe skips and the claim stays unverified in both
		// directions. With it the probe opens a subscription on `<topic>.*`,
		// `internal/activemq.CheckTopic` answers ErrUnsupported, and the
		// "no wildcard subscription" claim is proved rather than taken on
		// trust. "*" is a wildcard on both products.
		Wildcard: func(topic string) string { return topic + ".*" },
		Skip:     conformanceSkips(cfg),
	}
}

// Provision creates the topology a scenario's Need names.
//
// It is exported because it is the answer to "what do I have to create before
// this driver will work", and that answer is not guessable: on Artemis the
// subscription's queue is `<prefix><topic>.<subscription>` on a MULTICAST
// address of the same name, durable, shared and not purged when its last
// consumer leaves; on Classic it is `<prefix>Consumer.<subscription>.
// VirtualTopic.<topic>` and it has to exist BEFORE the first publication.
func Provision(ctx context.Context, cfg Config, n testkit.Need) error {
	topics := make([]string, 0, 2)
	topics = append(topics, n.Topic)
	if n.DeadLetterTopic != "" {
		topics = append(topics, n.DeadLetterTopic)
	}
	switch cfg.Broker {
	case activemq.Artemis:
		return provisionArtemis(ctx, cfg, topics, n)
	case activemq.Classic:
		return provisionClassic(ctx, cfg, topics, n)
	case activemq.BrokerUnset:
	}
	return activemq.ErrNoBroker
}

// provisionArtemis creates the addresses and queues through this driver's own
// Admin, which is Artemis's management address over the same connection.
func provisionArtemis(ctx context.Context, cfg Config, topics []string, n testkit.Need) error {
	c, err := Open(ctx, cfg)
	if err != nil {
		return err
	}
	// Shutdown rather than Close, so the context the caller gave bounds the
	// teardown as well as the work: the specification gives Close no context and a
	// provisioner that ignored the caller's deadline would outlive the
	// scenario that asked for the topology.
	defer func() { _ = c.Shutdown(context.WithoutCancel(ctx)) }()
	adm, err := c.Admin()
	if err != nil {
		return err
	}
	for _, t := range topics {
		if err := adm.EnsureTopic(ctx, mq.TopicSpec{Name: t}); err != nil {
			return err
		}
		for i := range max(1, len(n.Subscriptions)) {
			sub := n.Sub(i)
			if err := adm.EnsureSubscription(ctx, mq.SubscriptionSpec{Topic: t, Name: sub}); err != nil {
				return err
			}
		}
	}
	return nil
}

// provisionClassic brings each consumer queue into existence by attaching a
// receiver to it and letting go again.
//
// That is the whole of Classic's topology API over AMQP 1.0, and doing it here
// rather than in the driver's own consume path is the point: the specification makes
// implicit creation a defect, so the driver never does it and a deployment or
// a test does it deliberately. The queue survives the detach — it is a durable
// queue with no consumer — which is what makes a virtual topic's fan-out
// durable at all.
func provisionClassic(ctx context.Context, cfg Config, topics []string, n testkit.Need) error {
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
				// One credit rather than the default prefetch: this source
				// exists to create the queue and must not take delivery of
				// anything that is already in it.
				Prefetch: 1,
			})
			if err != nil {
				return err
			}
			// Opening a source ATTACHES NOTHING. The core attaches a
			// receiving link lazily, on the first fetch (`links.go`: "a
			// reconnection does not re-attach the links; the next operation
			// on a sink or a source does"), so a provisioner that opened
			// and closed a source sent no frame at all and created no
			// queue. Warming is what sends the attach, and on Classic the
			// attach IS the topology operation.
			//
			// Watched failing before this line, with the addressing already
			// correct: every Classic scenario reported "no delivery
			// arrived: … DeadlineExceeded" because the consumer queue did
			// not exist when the publication was copied.
			if werr := core.WarmForTest(ctx, b); werr != nil {
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
// and refuses the parts these brokers cannot express at all.
//
// A refusal becomes a skip with a reason in the report rather than a failure,
// and the rule for what to refuse is the narrow one the specification recorded: refuse a
// Need only when accepting it would make a scenario prove something FALSE.
// Almost nothing is refused here.
//
// `Wildcards` in particular is ACCEPTED and provisioned as an ordinary topic,
// which looks wrong and is what makes the negative claim provable: the probe
// then opens a subscription on a pattern, this driver refuses the pattern as a
// topic, and `capability/WildcardSubscribe/refused` passes. Refusing the Need
// would skip it and leave the claim unverified in both directions.
func shape(cfg Config, n testkit.Need) (Config, error) {
	out := cfg
	if n.DeadLetterTopic != "" {
		out.DeadLetterTopic = n.DeadLetterTopic
	}
	if n.Partitions > 1 {
		// A scenario that asks for several partitions is asking for a
		// topology whose absence would make it pass for the wrong reason:
		// everything would arrive in order because there is only ever one
		// lane.
		return Config{}, unsupportedNeed("several partitions; an AMQP 1.0 address is the only lane there is")
	}
	if n.Retain {
		// A retained topic is what replay and the seek positions need, and
		// a receiving link reads what is AT an address rather than a
		// history. Accepting the Need would make `capability/Replay/refused`
		// pass for the wrong reason on a FAN-OUT broker -- the harness
		// provisions every subscription the Need names before the scenario
		// runs, so the publication is already in the second subscription's
		// queue and the probe sees it arrive without anything having been
		// replayed. Watched failing: "the driver advertises no Replay and
		// the feature worked anyway". It is the same gap the specification recorded for
		// `drivers/rabbitmq/amqp10` and `drivers/snssqs`.
		// See.
		return Config{}, unsupportedNeed("a retained topic; a receiving link attaches to an address and reads what is there, so there is no history to replay -- and on a fan-out broker the probe cannot tell a replay from a second subscription that was already bound")
	}
	if n.MaxMessageSize > 0 && int64(n.MaxMessageSize) > maxFrameBody {
		return Config{}, unsupportedNeed("a message larger than a frame body; AMQP 1.0 splits a transfer across frames and neither broker's acceptor is configured for it here")
	}
	return out, nil
}

// maxFrameBody is the largest message the pinned images' acceptors carry
// without large-message handling: Artemis's `amqpMinLargeMessageSize` is
// 102,400 bytes and Classic's `wireFormat.maxFrameSize` is 100 MiB, so the
// smaller of the two is what a portable Need can ask for.
const maxFrameBody int64 = 102_400

// unsupportedNeed is the refusal the kit turns into a skip with a reason.
func unsupportedNeed(what string) error { return &needError{what: what} }

// needError names the topology this driver cannot build. It wraps
// ErrUnsupported so that the kit's errors.Is finds the answer, and carries
// the reason so that the skip in the report says what was missing.
type needError struct{ what string }

// Error renders the refusal.
func (e *needError) Error() string { return "iqmq/activemq: this driver cannot build " + e.what }

// Unwrap reaches the sentinel.
func (e *needError) Unwrap() error { return mq.ErrUnsupported }

// conformanceSkips names the scenarios this combination documents as
// inapplicable, with the reason, which the report publishes.
//
// A skip is never a pass: the claim a skipped scenario would have proved stays
// unverified in the generated matrix, which is the only honest answer for
// something nobody checked. The list is deliberately short, and every entry
// is something somebody watched fail.
func conformanceSkips(cfg Config) map[string]string {
	out := map[string]string{
		// The core's own entry, restated here because the core's is
		// unexported and this driver builds its harness rather than
		// delegating to the core's.
		"start/positions": "a receiving link attaches to an address and reads what is there. AMQP 1.0 has no committed position and no retained history, so StartLatest and StartEarliest are the same place and a consumer opened after a publication still receives it; the driver refuses a time or a sequence rather than pretending to one",
	}
	if cfg.DeadLetterTopic == "" {
		// This configuration PUBLISHES no dead letter: Profile.DeadLetterAddress
		// is nil, so a rejected delivery is routed by the BROKER's own
		// dead-letter address and there is no publication of this driver's
		// to fail. The scenario is about a driver that publishes one and
		// must not acknowledge the original when that publication fails.
		//
		// Watched failing before this entry: "Reject succeeded on a topic
		// with no dead-letter destination: the message was destroyed where
		// The specification requires ErrUnsupported" -- which is the scenario reading
		// the broker's route as no route at all.
		out["dlq/failed-publish-never-acks"] = "this configuration publishes no dead letter: a rejected delivery reaches the broker's own dead-letter address (Artemis's address settings, Classic's dead-letter strategy) and there is no publication of the driver's to fail. Set Config.DeadLetterTopic for the route this scenario is about"
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
	if cfg.Password.Reveal() == "" {
		return nil
	}
	wrong := cfg
	wrong.Password = dsn.Secret(cfg.Password.Reveal() + "-wrong")
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
	if s := cfg.Password.Reveal(); s != "" {
		return []string{s}
	}
	return nil
}

// sanitiseTopic turns a scenario's purpose into a name both brokers accept.
//
// A "." is mapped away as well as the obvious refusals, and that is Artemis's
// constraint rather than a general one: the derived queue name is
// `<topic>.<subscription>` and [activemq.ParseQueue] takes it apart at the
// last ".", so a topic containing one would parse back as a different topic
// and Admin's ListTopics would report it wrongly.
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
// repeated: a harness that reached for `Backend.Native` gets an `*amqp.Conn`,
// a type assertion to the backend fails silently, and the quiesce becomes a
// no-op — which showed up there as `capability/BatchReceive/works` reading one
// delivery of eight with nothing saying why.
type backendRegistry struct {
	mu sync.Mutex
	m  map[*mq.Client]*core.Backend
}

// remember records the pair.
func (r *backendRegistry) remember(c *mq.Client, b *core.Backend) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.m == nil {
		r.m = make(map[*mq.Client]*core.Backend, 4)
	}
	r.m[c] = b
}

// lookup finds a client's backend.
func (r *backendRegistry) lookup(c *mq.Client) *core.Backend {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.m[c]
}

// quiesce waits for everything published so far to have been acted on.
//
// A connection liveness check is the strongest "everything before this has
// been processed" this protocol offers: a confirmed publication has already
// been acted on by the time its disposition arrives, so there is nothing else
// to wait for. What it ALSO does is warm the links, which four of the specification's
// probes need: they publish, quiesce, then receive once, and every one of them
// would read the first fetch's single transfer instead of the queue's contents
// unless a link were already attached.
func (r *backendRegistry) quiesce(ctx context.Context, c *mq.Client) error {
	b := r.lookup(c)
	if b == nil {
		return errNoBackend
	}
	if err := core.WarmForTest(ctx, b); err != nil {
		return err
	}
	if err := b.Ping(ctx); err != nil {
		return err
	}
	return settle(ctx)
}

// settle waits for the peer to have sent what the credit allows.
//
// It is a bounded WAIT and not a poll on an observable event, because there is
// no observable event: warming attaches the link and writes the flow frame,
// and the frame is all the driver can see. go-amqp holds the transfers that
// arrive next in its own buffer and exposes no count of them, and the only
// call that would reveal one CONSUMES it -- so a quiesce that polled would be
// taking delivery of the very messages the scenario is about.
//
// the specification's `capability/BatchReceive/works` is what needs it: it publishes eight
// messages, quiesces, and reads ONCE, and the specification defines a fetch as one blocking
// wait plus a non-blocking drain -- so a fetch that ran before the transfers
// arrived fills one slot of eight and the claim fails. Watched failing: "a
// driver advertising batch receive filled 1 of 8 slots in one Receive".
//
// `drivers/nsq` does the same thing for the same reason and says so; this is
// that precedent, on a broker that is a JVM writing a journal.
// See.
func settle(ctx context.Context) error {
	t := time.NewTimer(quiesceDelay)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// quiesceDelay is how long the quiesce waits for the peer's transfers.
const quiesceDelay = 300 * time.Millisecond

// atomic64 is a counter the harness uses for its topic sequence. It is a type
// of its own so that the harness struct literal stays readable.
type atomic64 struct {
	mu sync.Mutex
	n  uint64
}

// add increments and returns the counter.
func (a *atomic64) add(n uint64) uint64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.n += n
	return a.n
}

// The exported test accessors. Each reports something the portable API cannot
// show, and each is exported because this module's critical tests live in
// another package — a test that reached into unexported state would be testing
// the implementation rather than the contract.

// CapabilitiesForTest is the table this configuration would publish
// against a broker reporting product and version.
func CapabilitiesForTest(cfg Config, product, version string) (mq.Capabilities, error) {
	p, err := cfg.Profile()
	if err != nil {
		return mq.Capabilities{}, err
	}
	return core.CapabilitiesForTest(p, core.Facts{Product: product, Version: version})
}

// MissingFeaturesForTest is the features this configuration's table leaves
// unanswered, which must be none: `driverkit.CapabilityBuilder` refuses an
// incomplete table and this is how a test says WHICH row is missing.
func MissingFeaturesForTest(cfg Config) ([]mq.Feature, error) {
	p, err := cfg.Profile()
	if err != nil {
		return nil, err
	}
	return core.MissingFeaturesForTest(p, core.Facts{Product: activemq.ProductArtemis, Version: "2.44.0"})
}

// AddressForTest is the two destinations a topic and a subscription map to.
func AddressForTest(cfg Config, topic, subscription string) (sink, source string, err error) {
	p, err := cfg.Profile()
	if err != nil {
		return "", "", err
	}
	if sink, err = p.SinkAddress(topic); err != nil {
		return "", "", err
	}
	if subscription == "" {
		return sink, "", nil
	}
	if source, err = p.SourceAddress(topic, subscription); err != nil {
		return "", "", err
	}
	return sink, source, nil
}

// CheckBrokerForTest is the product-and-version gate, for a test that has no
// broker to be refused by.
func CheckBrokerForTest(cfg Config, product, version string) error {
	return cfg.checkBroker(core.Facts{Product: product, Version: version})
}

// QueueConfigurationForTest is the queue configuration this driver's Admin
// sends to Artemis when it creates a subscription.
//
// It is exported because the four fields in it are what make a subscription a
// DURABLE SHARED one rather than the other thing, and none of them is visible
// to a caller: Artemis auto-creates a JMS topic subscriber's queue with
// `purge-on-no-consumers: true`, which destroys the subscription when its last
// consumer leaves. A test that could not read the request would be reading the
// broker's defaults instead.
func QueueConfigurationForTest(cfg Config, topic, subscription string) (string, error) {
	n, err := cfg.Names(topic, subscription)
	if err != nil {
		return "", err
	}
	body, err := json.Marshal(queueConfiguration(n))
	if err != nil {
		return "", err
	}
	return string(body), nil
}

// ManagementRequestForTest is the management message this driver sends, so
// that the encoding the broker turned out to need is pinned by a test rather
// than only by a container.
func ManagementRequestForTest(resource, op, replyTo string, args ...any) (value string, props map[string]any, err error) {
	m, err := managementRequest(resource, op, replyTo, args...)
	if err != nil {
		return "", nil, err
	}
	s, _ := m.Value.(string)
	return s, m.ApplicationProperties, nil
}
