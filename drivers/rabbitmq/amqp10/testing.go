package amqp10

import (
	"context"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	mq "bitbucket.org/iqhive/iqmq"
	core "bitbucket.org/iqhive/iqmq/drivers/amqp10"
	"bitbucket.org/iqhive/iqmq/dsn"
	"bitbucket.org/iqhive/iqmq/internal/rabbit"
	"bitbucket.org/iqhive/iqmq/internal/rabbit/protocol"
	"bitbucket.org/iqhive/iqmq/testkit"
)

// ConformanceHarness is what this profile hands the conformance kit.
//
// It is exported, and in a non-test file, because three different modules run
// it: this module's own env-gated test against an endpoint named by
// IQMQ_RABBITMQ_ADDR, `conformance/suites` against the pinned container — which
// is the evidence the generated capability matrix is built from — and
// `conformance/matrix` against several broker versions, which for this profile
// is where the version gate is proved against real 3.13 and 4.x images. A
// harness duplicated in three places would drift, and the copy that drifted
// would be the one nobody ran.
//
// # It cannot provision the topology, and that is not a gap in the harness
//
// RabbitMQ 4's AMQP 1.0 endpoint creates nothing (see [Config.Declare]), so
// unlike the AMQP 0-9-1 driver's harness — which turns Declare on and lets the
// driver build each scenario's exchange, queue and binding — this one has
// nothing to turn on. Every scenario needs its exchange, its queues and its
// bindings to exist BEFORE Open, or the first attach is answered with
// `amqp:not-found`.
//
// [ConformanceHarnessWithProvisioner] is therefore the entry point a
// conformance suite really wants: it takes the step that creates the topology —
// the AMQP 0-9-1 driver's Admin surface, RabbitMQ's HTTP management API or
// `rabbitmqctl` inside the container — and runs it for each scenario's Need
// before the client is opened. This function is that one with no provisioner,
// which is the right shape for an endpoint whose topology an operator has
// already created, and it is why the signature is the one every other driver's
// harness has.
func ConformanceHarness(cfg Config, ev testkit.Evidence, broker testkit.Broker) testkit.Harness {
	return ConformanceHarnessWithProvisioner(cfg, ev, broker, nil)
}

// ConformanceHarnessWithProvisioner is [ConformanceHarness] with a step that
// creates each scenario's topology first.
//
// provision is called once per scenario, before the client is opened, with the
// Need the scenario asked for: its topic, its subscriptions and — when it wants
// one — its dead-letter route. It is the seam the specification's "no hidden creation" leaves
// for the operator, and on this profile the conformance kit IS the operator. A
// nil provisioner means the topology already exists.
//
// The names it has to create are not the harness's to invent: they are
// `internal/rabbit`'s, which both RabbitMQ drivers derive identically, and
// [Names] is how a provisioner asks for them.
func ConformanceHarnessWithProvisioner(cfg Config, ev testkit.Evidence, broker testkit.Broker, provision func(ctx context.Context, n testkit.Need) error) testkit.Harness {
	var seq atomic.Uint64
	nonce := strconv.FormatInt(time.Now().UnixNano(), 36)
	backends := &backendRegistry{}
	return testkit.Harness{
		Driver:   DriverName,
		Profile:  ProfileName,
		Evidence: ev,
		Broker:   broker,
		Client:   testkit.ClientInfo{Module: core.ClientModule, Version: core.ClientVersion},
		Timeout:  45 * time.Second,
		// The specification asks for 100,000 interleaved messages. Every publication here
		// is a transfer whose own disposition the broker commits before it
		// answers — there is no confirm window to amortise it across, because
		// there is no confirm mode — so the figure is lowered to keep one
		// scenario inside the suite's per-scenario timeout, and
		// docs/drivers/rabbitmq-amqp10.md says so, which is what the specification asks
		// of a driver that lowers it.
		Fidelity: 5_000,
		TopicName: func(purpose string) string {
			// The nonce comes first so that a truncation cannot make two
			// names equal, and the whole name has to survive
			// `internal/rabbit`'s own validation: an exchange name is 255
			// bytes of anything but the reserved "amq." prefix, and a topic
			// may not carry the topic exchange's wildcards.
			return "iqmq-conf-" + nonce + "-" + strconv.FormatUint(seq.Add(1), 36) + "-" + sanitiseTopic(purpose)
		},
		Secrets:             secretsOf(cfg),
		OpenWrongCredential: openWrongCredential(cfg),
		Open: func(ctx context.Context, n testkit.Need) (*mq.Client, error) {
			shaped, err := shape(cfg, n)
			if err != nil {
				return nil, err
			}
			if provision != nil {
				if err := provision(ctx, n); err != nil {
					return nil, err
				}
			}
			// The BACKEND is opened and the client built around it, rather
			// than `Open` being called, because [quiesce] needs the backend
			// and cannot get it from the client: `Backend.Native` answers the
			// peer CONNECTION, which is the escape hatch and the right
			// answer for a caller reaching past the SPI. So the harness
			// remembers the pair it made.
			b, err := OpenBackend(ctx, shaped)
			if err != nil {
				return nil, err
			}
			cc, cerr := shaped.CoreConfig()
			if cerr != nil {
				return nil, cerr
			}
			c := core.ClientForBackend(b, cc)
			backends.remember(c, b)
			return c, nil
		},
		Quiesce: backends.quiesce,
		// The wildcard pattern is supplied even though this profile does NOT
		// claim FeatureWildcardSubscribe, which is the lesson: without it
		// the kit's negative probe skips and the claim stays unverified in
		// both directions. With it the probe opens a subscription on
		// `<topic>.#`, this profile answers ErrUnsupported (see
		// WildcardError), and the "no wildcard subscription" claim is proved
		// rather than taken on trust.
		Wildcard: func(topic string) string { return topic + ".#" },
		Skip:     conformanceSkips(),
	}
}

// Names is the topology one topic and one subscription map to, for a
// conformance suite or an operator that has to create it.
//
// It is `internal/rabbit`'s own derivation behind an exported door, and the
// door exists because the names are not a convention a provisioner can guess:
// the queue is `<prefix><topic>.<subscription>`, its dead-letter exchange and
// queue are derived from the queue's name, and the dead-letter binding uses a
// FIXED routing key rather than the message's own. A provisioner that derived
// them slightly differently would build a topology whose parts do not point at
// each other, which is the failure `internal/rabbit` exists to prevent.
func (c Config) Names(topic, subscription string) (rabbit.Names, error) {
	return rabbit.Derive(rabbit.Config{
		Prefix:       c.Prefix,
		Topic:        topic,
		Subscription: subscription,
	})
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

// sanitiseTopic turns a scenario's purpose into a name `internal/rabbit`
// accepts.
//
// A "/" or a "*" or a "#" would be refused — the last two are the topic
// exchange's wildcards and a topic is its own routing key — and a "." would
// make the derived queue name ambiguous to parse back, so the purpose is mapped
// rather than passed through.
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

// shape applies the parts of a scenario's Need that are CONFIGURATION on this
// profile, and refuses the parts this protocol cannot express at all.
//
// A refusal here becomes a skip with a reason in the report rather than a
// failure, and the rule for what to refuse is the narrow one the specification recorded:
// refuse a Need only when accepting it would make a scenario prove something
// FALSE. A priority topology is accepted, because the AMQP header has a
// priority field and what the broker does with it is what the scenario is
// there to see; several partitions are refused, because everything would
// arrive in order for the wrong reason.
func shape(cfg Config, n testkit.Need) (Config, error) {
	out := cfg
	switch {
	case n.Partitions > 1:
		return Config{}, unsupportedNeed("several partitions; an AMQP 1.0 address is the only lane there is, so every message of a topic would arrive in order for the wrong reason")
	case n.Retain:
		return Config{}, unsupportedNeed("a retained topic; a receiving link reads what is in a queue rather than a history, so a replay scenario would read an empty queue and call it a pass")
	case n.DedupWindow > 0:
		return Config{}, unsupportedNeed("broker-side de-duplication; RabbitMQ has none and this profile writes no de-duplication key")
	case n.Wildcards:
		return Config{}, unsupportedNeed("a wildcard subscription; the pattern is a binding on a topic exchange and a receiving link addresses a queue")
	}
	if n.DeadLetterTopic != "" {
		// the dead-letter destination is the SUBSCRIPTION's and the SPI's
		// SourceConfig does not carry it, so on both RabbitMQ profiles it is
		// configuration. Here it is also an assertion about the queue, which
		// is why the provisioner has to have declared the queue with a
		// dead-letter exchange for the claim to be true.
		out.DeadLetter = true
	}
	if n.Priority {
		// A priority ceiling is what makes Priority mean anything, and
		// it forces a CLASSIC queue: RabbitMQ refuses `x-max-priority` on a
		// quorum queue. So a priority scenario runs against a classic queue,
		// and with it go the attempt count and the durable confirmation
		// boundary — a real trade the capability table states.
		out.MaxPriority = 9
		out.QueueType = QueueClassic
	}
	return out, nil
}

// unsupportedNeed is the refusal the kit turns into a skip with a reason.
func unsupportedNeed(what string) error { return &needError{what: what} }

// needError names the topology this profile cannot build. It wraps
// ErrUnsupported so that the kit's errors.Is finds the answer, and carries
// the reason so that the skip in the report says what was missing.
type needError struct{ what string }

// Error renders the refusal.
func (e *needError) Error() string {
	return "iqmq/rabbitmq: the AMQP 1.0 profile cannot provide " + e.what
}

// Unwrap reaches the sentinel.
func (e *needError) Unwrap() error { return mq.ErrUnsupported }

// conformanceSkips names the scenarios this profile documents as inapplicable,
// with the reason, which the report publishes.
//
// A skip is never a pass: the claim a skipped scenario would have proved stays
// unverified in the generated matrix, which is the only honest answer for
// something nobody checked. The list is deliberately SHORT — one entry, which
// the shared core documents for the protocol rather than for this broker — and
// it is expected to grow once the suite has run against a container: every
// entry a driver adds should be one somebody watched fail, and nothing in this
// increment ran against a broker.
func conformanceSkips() map[string]string {
	return map[string]string{
		"start/positions":               "a receiving link attaches to a queue and reads what is in it. AMQP 1.0 has no committed position and no retained history, so StartLatest and StartEarliest are the same place and the driver refuses a time or a sequence rather than pretending to one",
		"dlq/failed-publish-never-acks": deadLetterSkip,
	}
}

// deadLetterSkip is why the specification's "a failed dead-letter publish never
// acknowledges the original" is inapplicable to this profile.
//
// The scenario's premise is a driver that CLAIMS a dead-letter route and has
// nowhere to send one, and this profile cannot be in that state for a reason
// stronger than the other profile's: it PUBLISHES no dead letter at all.
// [Profile.DeadLetterAddress] is nil, so a `rejected` disposition is answered
// by the queue's own `x-dead-letter-exchange` and the routing is the broker's
// — there is no publish to fail. A subscription whose queue was declared with
// the route always has somewhere to send; one whose queue was not sends
// nowhere and the broker drops the message, which is the broker's behaviour
// and not a settlement this driver made.
//
// The rule itself is held where this profile can break it: a delayed requeue
// is refused outright (there is no verified delay annotation), so the one path
// that would publish-then-settle does not exist, and
// TestCriticalUnhonourableNackSettlesNothing is what says a refusal settles
// nothing.
const deadLetterSkip = "this profile PUBLISHES no dead letter: Profile.DeadLetterAddress is nil, so a rejected message is routed by the queue's own x-dead-letter-exchange and there is no publish that could fail. The scenario's premise -- a claim with no destination and a driver that must refuse rather than destroy -- is unreachable, and the rule is held instead by TestCriticalUnhonourableNackSettlesNothing on the paths this profile refuses"

// quiesce waits until the broker has pushed to this client's consumers
// whatever it was holding for them.
//
// It is `Harness.Quiesce`, and a connection liveness check is not enough for
// it. A receiving link is attached by the FIRST Fetch, so until then the peer
// has been told nothing and has sent nothing — and four probes are about what
// the broker does with the messages it still HOLDS: the priority order, a TTL
// expiry, a de-duplication window, and whether one Receive can return more
// than one delivery. Every one of them publishes, then quiesces, then receives
// once, and every one of them would read the first Fetch's single transfer
// instead of the queue's contents.
//
// So the links are attached and their credit issued ([core.WarmForTest]), the
// connection is pinged — which is a round trip and therefore a lower bound on
// "the peer has seen the credit" — and then a short fixed wait lets the
// transfers arrive. The wait is a number and not an observable because there is
// no observable: go-amqp buffers a receiver's transfers where nothing outside
// it can count them, and a harness's fixed 200 ms is the honest form of that.
func (r *backendRegistry) quiesce(ctx context.Context, c *mq.Client) error {
	b := r.lookup(c)
	if b == nil {
		// A client this harness did not open: nothing to warm, and a Quiesce
		// that failed here would replace a scenario's own message with this
		// one.
		return nil
	}
	if err := core.WarmForTest(ctx, b); err != nil {
		return err
	}
	if err := c.Ping(ctx); err != nil {
		return err
	}
	timer := time.NewTimer(quiesceSettle)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// quiesceSettle is how long [quiesce] waits for the transfers the credit it
// issued let the peer send. It is a TEST HARNESS's number: long enough for a
// container on a loopback interface and short enough that a scenario which
// never receives anything fails on its own budget rather than this one.
const quiesceSettle = 200 * time.Millisecond

// The exported test accessors.
//
// Every one of them reports something the portable API cannot show, and each is
// exported because the tests that need it live in another package or another
// module — `conformance/suites`, `conformance/matrix` and this module's own
// external test package. A test that reached into unexported state would be
// testing the implementation; a test that could not reach these at all could
// not tell a driver that never entered the case from one that handled it.

// CheckBrokerForTest puts a peer's own OPEN-frame properties through this
// profile's version and product gate, with no connection at all.
//
// It is the gate itself and not a copy of it: [Config.Profile] installs this
// very function as
// [bitbucket.org/iqhive/iqmq/drivers/amqp10.Profile.CheckBroker], and the shared
// core calls it with the facts it read off the peer. Exporting it is what lets
// a test drive the gate across a table of versions — 3.13.7, 4.0.0, 4.1.8-rc.1,
// the empty string — without one container per row.
func CheckBrokerForTest(product, version string) error {
	return checkBroker(core.Facts{Product: product, Version: version})
}

// CapabilitiesForTest builds this profile table from facts a test can
// name, without opening a backend.
//
// It is the only way to hold a conditional claim to its own condition: the
// table depends on the queue type, the dead-letter route, the priority ceiling
// and the durable flag, and varying those against a broker would mean four
// deployments. It is also what the golden test compares and what
// `conformance/matrix` reads in order to hold this profile's table against the
// AMQP 0-9-1 driver's — which is the whole subject.
func CapabilitiesForTest(cfg Config, product, version string) (mq.Capabilities, error) {
	profile, err := cfg.Profile()
	if err != nil {
		return mq.Capabilities{}, err
	}
	return core.CapabilitiesForTest(profile, core.Facts{Product: product, Version: version})
}

// MissingFeaturesForTest is the features this profile's table never mentions,
// in inventory order.
//
// It exists so that a test can assert the whole inventory was considered,
// which is the difference between a feature this profile decided it does not
// have and one nobody looked at. It fires when the inventory grows.
func MissingFeaturesForTest(cfg Config) ([]mq.Feature, error) {
	profile, err := cfg.Profile()
	if err != nil {
		return nil, err
	}
	return core.MissingFeaturesForTest(profile, core.Facts{})
}

// AddressForTest is the AMQP 1.0 address this profile derives for a topic and a
// subscription, without opening anything.
//
// Both are returned together because they have to agree: the publishing
// address names the exchange a subscription's queue is bound to, and a profile
// whose two halves derived from different names would build a topology whose
// parts do not point at each other. It is what the address fuzz target drives
// and what a cross-module test compares against the AMQP 0-9-1 driver's names.
func AddressForTest(cfg Config, topic, subscription string) (sink, source string, err error) {
	profile, err := cfg.Profile()
	if err != nil {
		return "", "", err
	}
	sink, err = profile.SinkAddress(topic)
	if err != nil {
		return "", "", err
	}
	source, err = profile.SourceAddress(topic, subscription)
	if err != nil {
		return "", "", err
	}
	return sink, source, nil
}

// ProtocolSupportForTest is `internal/rabbit/protocol`'s own answer for one
// feature on each of the two arms.
//
// It is exported so that a test outside the root module can hold this driver to
// the rule every driver in this repository is held to — narrow the protocol's
// table, never widen it — and so that the two arms' tables can be compared
// without importing the root module's internal tree.
func ProtocolSupportForTest(f mq.Feature) (amqp091, amqp10 mq.Support, err error) {
	older, err := protocol.New(protocol.AMQP091)
	if err != nil {
		return mq.Support{}, mq.Support{}, err
	}
	newer, err := protocol.New(protocol.AMQP10)
	if err != nil {
		return mq.Support{}, mq.Support{}, err
	}
	return older.Support(f), newer.Support(f), nil
}

// backendRegistry is the harness's map from a client to the backend it was
// built around.
//
// It exists because [quiesce] needs the backend and the client cannot give it
// one: `core.Backend.Native` answers the peer CONNECTION, which is the escape
// hatch and the right answer for a caller reaching past the SPI. A harness
// that reached for `Native` got an `*amqp.Conn`, the type assertion failed
// silently, and the quiesce became a no-op — which showed up as
// `capability/BatchReceive/works` reading one delivery of eight and nothing at
// all saying why.
//
// It is per HARNESS rather than a package-level map, so two harnesses in one
// process do not share one, and the entries live as long as the harness: a
// conformance run opens one client per scenario and the map is a few hundred
// pointers.
type backendRegistry struct {
	mu sync.Mutex
	m  map[*mq.Client]*core.Backend
}

// remember records the pair.
func (r *backendRegistry) remember(c *mq.Client, b *core.Backend) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.m == nil {
		r.m = make(map[*mq.Client]*core.Backend, 8)
	}
	r.m[c] = b
}

// lookup is the backend for a client, or nil.
func (r *backendRegistry) lookup(c *mq.Client) *core.Backend {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.m[c]
}
