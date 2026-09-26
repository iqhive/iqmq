package rabbitmq

import (
	"context"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/drivers/rabbitmq/internal/client"
	"github.com/iqhive/iqmq/dsn"
	"github.com/iqhive/iqmq/internal/errcode"
	"github.com/iqhive/iqmq/internal/rabbit"
	"github.com/iqhive/iqmq/internal/rabbit/delay"
	"github.com/iqhive/iqmq/internal/rabbit/protocol"
	"github.com/iqhive/iqmq/testkit"
)

// ConformanceHarness is what this driver hands the conformance kit.
//
// It is exported, and in a non-test file, because three different modules run
// it: this module's own env-gated test against an endpoint named by
// IQMQ_RABBITMQ_ADDR, `the conformance suite` against the
// pinned container — which is the evidence the generated capability matrix is
// built from — and `the capability matrix` against three broker
// versions. A harness duplicated in three places would drift, and the copy
// that drifted would be the one nobody ran.
func ConformanceHarness(cfg Config, ev testkit.Evidence, broker testkit.Broker) testkit.Harness {
	var seq atomic.Uint64
	nonce := strconv.FormatInt(time.Now().UnixNano(), 36)
	return testkit.Harness{
		Driver:   DriverName,
		Profile:  "amqp091",
		Evidence: ev,
		Broker:   broker,
		Client:   testkit.ClientInfo{Module: ClientModule, Version: ClientVersion},
		Timeout:  45 * time.Second,
		// The specification asks for 100,000 interleaved messages. Every one of them is a
		// `basic.publish` that waits for its own `basic.ack` under the
		// default per-message confirm strategy, so the figure is lowered to
		// keep one scenario inside the suite's per-scenario timeout.
		// docs/drivers/rabbitmq.md says so, which is what the specification asks of a
		// driver that lowers it.
		Fidelity: 5_000,
		TopicName: func(purpose string) string {
			// An exchange name takes 255 bytes of anything, so the only real
			// constraint is that two scenarios must not share a topic and
			// that a name has to be recognisable in `rabbitmqctl
			// list_exchanges`. The nonce comes first so that a truncation
			// cannot make two names equal.
			return "iqmq-conf-" + nonce + "-" + strconv.FormatUint(seq.Add(1), 36) + "-" + sanitiseTopic(purpose)
		},
		Secrets:             secretsOf(cfg),
		OpenWrongCredential: openWrongCredential(cfg),
		Open: func(ctx context.Context, n testkit.Need) (*mq.Client, error) {
			c, err := Open(ctx, shape(cfg, n))
			if err != nil {
				return nil, err
			}
			if perr := provision(ctx, c, n); perr != nil {
				// the prompt close, which takes no context: a client
				// whose provisioning failed is a client nobody will use, and
				// the caller's context is the one that failed. contextcheck
				// cannot know that Close is the context-free half of the
				// lifecycle.
				//nolint:contextcheck // Close is the prompt close and takes no context by design
				_ = c.Close()
				return nil, perr
			}
			return c, nil
		},
		// The wildcard is supplied even though the topic-exchange syntax is
		// this driver's own, which is the lesson: without it both wildcard
		// scenarios skip and the claim stays unverified in BOTH directions.
		Wildcard: func(topic string) string { return topic + ".#" },
		Quiesce:  quiesce,
		Skip:     conformanceSkips(),
	}
}

// provision creates the topology a scenario's Need names, BEFORE the scenario
// runs.
//
// It is what `Harness.Open`'s "opens a client with the topology the scenario
// needs" asks for, and leaving it out is a defect rather than a shortcut. This
// driver binds a queue per SUBSCRIPTION, and a publication to an exchange with
// no queue bound is discarded and acknowledged. So a scenario that
// publishes before it opens its consumer -- which several do, because a
// retained-log broker makes that the natural shape -- would lose the message,
// and the scenario would fail with a timeout that says nothing about the
// property it was testing.
//
// Every name in the Need is provisioned, not only the first: `Need.Sub(1)` is
// how the replay and consumer-group scenarios name a second subscription, and
// the dead-letter destination is a topic in its own right whose own
// subscriptions have to exist for anything to read from it.
func provision(ctx context.Context, c *mq.Client, n testkit.Need) error {
	adm, err := c.Admin()
	if err != nil {
		return err
	}
	topics := []string{n.Topic}
	if n.DeadLetterTopic != "" {
		topics = append(topics, n.DeadLetterTopic)
	}
	for _, topic := range topics {
		if topic == "" {
			continue
		}
		if err := adm.EnsureTopic(ctx, mq.TopicSpec{Name: topic}); err != nil {
			return err
		}
		for _, sub := range n.Subscriptions {
			if sub == "" {
				continue
			}
			if err := adm.EnsureSubscription(ctx, mq.SubscriptionSpec{Topic: topic, Name: sub}); err != nil {
				return err
			}
		}
	}
	return nil
}

// quiesce waits until the broker has pushed to this client's consumers
// whatever it was holding for them.
//
// It is `Harness.Quiesce`, and on a PUSH broker with a lazily-attached
// consumer (see [source.attach]) that sentence has two halves. The consumers
// are STARTED, because a broker cannot push to a consumer that does not exist
// and "the broker has finished what it was doing" is otherwise vacuously true;
// and then the wait is for the deliveries to arrive, because `basic.consume`
// returns before the first of them.
//
// The wait is in two parts, and the first is not a poll. A `basic.qos` on the
// consume channel is a synchronous round trip, and frames on one channel are
// ordered -- so its reply cannot overtake the deliveries the broker had
// already written. By the time it returns, every delivery the broker held at
// consume time is in the socket. The second part waits for this driver's own
// pump to move them out of the library's channel and into the ring, which is
// a goroutine and therefore a poll: the ring's depth is read until it stops
// changing.
//
// Four probes need it and every one of them is about something the broker
// decides over the messages it HOLDS rather than over the ones it has already
// pushed: the priority order, a TTL expiry, a de-duplication window and
// whether one Receive can return more than one delivery.
func quiesce(ctx context.Context, c *mq.Client) error {
	b, ok := BackendForTest(c)
	if !ok {
		return nil
	}
	_, srcs := b.children()
	for _, s := range srcs {
		// A source that cannot be started is not a quiesce failure: whatever
		// is wrong with it is the scenario's to report, and a Quiesce that
		// failed here would replace the scenario's own message with this
		// one.
		if err := s.ensureAttached(ctx); err != nil {
			continue
		}
		if ch, _ := s.current(); ch != nil {
			_ = ch.Qos(s.prefetch, 0, false)
		}
	}
	deadline := b.clock.Now().Add(quiesceBudget)
	last := -1
	for stable := 0; stable < 2 && b.clock.Now().Before(deadline); {
		depth := 0
		for _, s := range srcs {
			depth += s.adapter.Len()
		}
		if depth == last {
			stable++
		} else {
			stable, last = 0, depth
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(quiescePoll):
		}
	}
	return nil
}

// The bounds on [quiesce]'s second half. They are a TEST HARNESS's numbers and
// not the driver's: the poll is short enough that a scenario does not notice it
// and the budget is short enough that a broker which never delivers fails the
// scenario rather than the quiesce.
const (
	quiescePoll   = 5 * time.Millisecond
	quiesceBudget = 2 * time.Second
)

// secretsOf is the credentials the kit must find in no rendered string.
func secretsOf(cfg Config) []string {
	if s := cfg.Password.Reveal(); s != "" {
		return []string{s}
	}
	return nil
}

// openWrongCredential dials with the password mangled, for the one error a
// client library is most likely to render a connection string into.
//
// Nil when there is no credential to get wrong: a broker this harness reaches
// without authenticating has no authentication failure to produce, and the
// scenario skips rather than passing on a refusal that never happened.
//
// The password is CHANGED rather than cleared. An empty one can take a
// different path in the library -- no PLAIN mechanism offered, a different
// refusal, or in RabbitMQ's case the guest fallback -- and the error this is
// about is the one where the server rejected a credential it was given.
func openWrongCredential(cfg Config) func(context.Context) error {
	if cfg.Password.Reveal() == "" {
		return nil
	}
	wrong := cfg
	wrong.Password = dsn.Secret(cfg.Password.Reveal() + "-wrong")
	// ONE attempt, and a short one. Without this the driver's dial loop
	// retries the refusal with backoff and `Open` returns whatever the
	// scenario's context says when its budget runs out -- so the probe reads
	// `context deadline exceeded`, which carries no credential because it
	// carries nothing, and passes in exactly the harness timeout. It did,
	// against a real broker, at 45.000 s. The scenario now refuses a
	// deadline as well, so the two halves each catch it.
	wrong.MaxReconnects = 1
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

// sanitiseTopic turns a scenario's purpose into a routing-key-safe name.
//
// A "/" or a "*" or a "#" in a topic would be refused by `internal/rabbit` —
// the first two are not routing-key words and the last two are the topic
// exchange's wildcards — so the purpose is mapped rather than passed through.
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
// driver rather than topology.
//
// Declare is turned ON here and is OFF by default in production, which is not
// a contradiction: the rule is about a deployment, and the conformance
// kit's own provisioning is the operator. Leaving it off would make every
// scenario fail at open with "the exchange does not exist".
func shape(cfg Config, n testkit.Need) Config {
	out := cfg
	out.Declare = true
	if n.DeadLetterTopic != "" {
		// the dead-letter destination is the SUBSCRIPTION's and the SPI's
		// SourceConfig does not carry it, so on this driver it is
		// configuration and the harness is what turns a scenario's Need into
		// it.
		//
		// The scenario names a TOPIC and the driver's own default destination
		// is a derived QUEUE, which is the difference that matters: a queue
		// is not consumable through mq.Client, and the kit's dead-letter
		// probe opens a consumer of the named topic and waits for the
		// rejected message there. [Config.DeadLetterTopic] is what turns the
		// route into a topic's exchange, and turning it on only when the
		// scenario asked is what makes the negative direction reachable at
		// all.
		out.DeadLetter = true
		out.DeadLetterTopic = n.DeadLetterTopic
	}
	if n.Priority {
		// A priority ceiling is what makes Priority mean anything,
		// and it forces a CLASSIC queue: RabbitMQ refuses
		// `x-max-priority` on a quorum queue. So a priority scenario is run
		// against a classic queue and the attempt count is unavailable for
		// it, which is a real trade the capability table states.
		out.MaxPriority = 9
		out.QueueType = QueueClassic
	}
	if n.Capacity > 0 {
		out.QueueMaxLength = int64(n.Capacity)
	}
	if n.MaxMessageSize > 0 {
		// There is nothing to set: RabbitMQ's message-size ceiling is a
		// SERVER configuration (`max_message_size`), not a queue argument,
		// so a scenario that wants an oversized publication refused has to
		// be run against a broker configured for it. The capability table
		// advertises no limit, so the scenario skips rather than failing.
		_ = n.MaxMessageSize
	}
	return out
}

// conformanceSkips names the scenarios this driver documents as inapplicable,
// with the reason, which the report publishes.
//
// Every entry names something the BROKER does not have or something the kit
// cannot observe, and each was added after watching the scenario fail against
// the pinned container rather than in anticipation.
func conformanceSkips() map[string]string {
	return map[string]string{
		"dlq/failed-publish-never-acks":         deadLetterSkip,
		"capability/Replay/refused":             replaySkip,
		"start/positions":                       "a queue has no committed position and no retained history: a message that has not been settled is at the head and one that has is gone, so StartLatest and StartEarliest are the same place. The driver REFUSES a time or a sequence at open (which is what makes the two seek claims provable in the negative direction), and the two it accepts cannot be told apart by what arrives",
		"capability/OrderedByKey/refused":       orderingSkip,
		"capability/OrderedByPartition/refused": orderingSkip,
		"capability/OrderedGlobally/refused":    orderingSkip,
	}
}

// deadLetterSkip is why the specification's "a failed dead-letter publish never
// acknowledges the original" is inapplicable to this driver.
//
// The scenario's premise is a driver that CLAIMS a dead-letter route and has
// nowhere to send one, and this driver cannot be in that state: the
// destination is DERIVED from the subscription's own queue, so a subscription
// with the feature enabled always has one. What the scenario reads as "the
// message was destroyed" is a message sitting in `<queue>.dlq`, durable and
// inspectable, which the kit cannot see because a queue is not a topic.
//
// The property itself is not skipped, because this driver never PUBLISHES a
// dead letter: a dead-letter route on RabbitMQ is a queue argument and the
// broker does the routing, so the failure mode the scenario names does not
// exist here. The one place the driver does publish and then acknowledge is a
// delayed redelivery, and TestCriticalUnhonourableNackSettlesNothing and the
// `errParkRefused` path are what hold the same rule there: a park that did not
// land leaves the original unacknowledged.
const deadLetterSkip = "the dead-letter destination is DERIVED from the subscription's queue (`<queue>.dlq`), so a subscription that claims the feature always has somewhere to route and the scenario's premise -- a claim with no destination -- is unreachable. The rejected message is not destroyed, it is in `<queue>.dlq`; the \"a failed dead-letter publish never acknowledges the original\" is held by TestCriticalUnhonourableNackSettlesNothing instead, on the one path where this driver does publish a copy (a delayed redelivery)"

// replaySkip is why the negative Replay claim is unverifiable through this
// probe, which is a limitation of the KIT and not of this driver.
//
// The probe publishes, then reads the publication through one subscription and
// then through a second created afterwards. On a driver with a retained log
// that is a replay. On this one it is FAN-OUT: a subscription is a queue bound
// to the topic's exchange, both queues are bound before the publication
// (`ConformanceHarness`'s `provision` creates every subscription the Need
// names, which is what `Harness.Open` asks for), so the publication is already
// in both and the second subscription receives it without anything being
// replayed. The probe therefore passes for a driver that claims no Replay,
// which the refusal scenario reads as "the feature worked anyway".
//
// The driver cannot refuse its way out of it either: the probe opens with
// StartEarliest, and StartEarliest is the only position a queue HAS -- the
// head. Refusing it would also make the dead-letter destination unconsumable,
// because the kit's own dead-letter probe opens its DLQ consumer with
// StartEarliest. Recorded in the maintenance notes next to the ordering negatives.
const replaySkip = "the probe cannot tell a retained log from fan-out. A subscription here is a queue bound to the topic's exchange and the harness provisions every subscription the Need names before the scenario runs, so a publication is already in BOTH subscriptions' queues and the second one receives it -- which is fan-out, not replay. Refusing the probe is not open to the driver either: it opens with StartEarliest, which is the only position a queue has, and refusing that would make the kit's own dead-letter probe unable to consume the destination"

// orderingSkip is the reason the three ordering negatives share.
//
// It is a limitation of the CONFORMANCE KIT rather than of this driver, and it
// is recorded as one: the maintenance notes names it ("the kit cannot refute a
// negative claim about a guarantee"), and `fix/testkit` is repairing it.
const orderingSkip = "the negative probe fails the driver if thirty-two messages under one key arrive in order, and a single consumer of one queue at that volume always receives them in order -- a queue is FIFO. The claim is that the driver does not GUARANTEE ordering by key unless the key is the routing key, which one run cannot refute; the positive direction is proved by the OrderedByPartition claim, which a queue really does hold"

// ClientVersionForTest is the pinned client version, so that a test in another
// module can hold [ClientVersion] to what go.mod really requires.
func ClientVersionForTest() string { return ClientVersion }

// BackendForTest extracts the backend from a client, for a test that needs the
// driver's own surface through the portable API.
//
// It is exported because the driver's own critical tests and the conformance
// module both need it, and a driver whose internals can only be reached from
// its own package cannot be tested from another module — which
// `conformance/suites` and `conformance/critical` are (the second half).
func BackendForTest(c *mq.Client) (*Backend, bool) {
	b, ok := c.Native().(*Backend)
	return b, ok
}

// StaleForTest is how many superseded deliveries a source's pump has dropped.
//
// It is exported because the property is invisible from the portable API: a
// source that handed out a superseded delivery would fail a settlement the
// caller can do nothing about, and the only way to tell that apart from a
// source that never reached the case is to count.
func StaleForTest(b *Backend) uint64 {
	_, srcs := b.children()
	var n uint64
	for _, s := range srcs {
		n += s.Stale()
	}
	return n
}

// OutstandingForTest is how many publications each of a backend's sinks is
// holding unconfirmed.
//
// It is the number "ambiguity accuracy" is read from, and it is
// exported so that a test can assert the window is respected rather than
// inferring it from timing.
func OutstandingForTest(b *Backend) []int {
	sinks, _ := b.children()
	out := make([]int, 0, len(sinks))
	for _, s := range sinks {
		s.mu.Lock()
		n := 0
		if s.tr != nil {
			n = s.tr.Outstanding()
		}
		s.mu.Unlock()
		out = append(out, n)
	}
	return out
}

// TopologyForTest is every declaration this client made, in the order it made
// them.
//
// It is what a test asserts the topology restore against: the specification requires the
// driver to restore the topology after a reconnection, and the only way to
// check that without a broker's own management API is to compare what was
// declared before with what was declared after.
func TopologyForTest(b *Backend) []string {
	decls := b.topo.all()
	out := make([]string, 0, len(decls))
	for _, d := range decls {
		out = append(out, d.id())
	}
	return out
}

// RestoresForTest is how many times the dial loop has reported this client
// restored -- the first connection included, one per completed reconnection
// afterwards.
//
// It is exported because it is the only signal that says "the reconnection has
// FINISHED", and the property is invisible from the portable API. Waiting on
// the broker's own state instead is what made
// `TestCriticalReconnectRestoresTheTopology` rare-flaky: the replay declares
// one thing at a time, so "the broker holds a queue again" is true from the
// first declaration and says nothing about the other five. This counter rises
// when [driverkit.ReconnectConfig.Dial] returned -- after the replay and after
// every handle re-attached.
func RestoresForTest(b *Backend) uint64 {
	if b.rc == nil {
		return 0
	}
	return b.rc.Restores()
}

// StaleChannelClosesForTest is how many channel-close notifications a
// publisher's loop has read for a channel that had already been replaced.
//
// It is exported because the reordering it counts is invisible from the
// portable API and a test that cannot see it cannot tell a repaired sink from
// one whose stale notification simply has not arrived yet: the symptom of the
// defect is a publisher that never works again, and "it still works" is true
// of a test that asserted too early as well as of a fixed driver. Waiting for
// this to rise is what makes the assertion happen strictly after the event it
// is about.
//
// Every reconnection produces one per publisher; see the `gen` field on this
// package's sink for why.
func StaleChannelClosesForTest(b *Backend) uint64 {
	if b == nil {
		return 0
	}
	return b.staleCloses.Load()
}

// StaleConfirmsForTest is how many publisher confirms a sink's loop read for a
// channel that had already been replaced, and dropped.
func StaleConfirmsForTest(b *Backend) uint64 { return b.staleConfirms.Load() }

// ChannelRepairsForTest is how many channel exceptions a handle answered
// ITSELF: it read the close notification, found the loss unclaimed and
// attached a replacement, with no dial loop involved. See
// [Backend.channelRepairs] for why it is counted at all.
func ChannelRepairsForTest(b *Backend) uint64 { return b.channelRepairs.Load() }

// DeclinedClosesForTest is how many close notifications a handle's loop read
// for a channel whose loss somebody else had already claimed -- the
// supervision loop before a redial -- and therefore did not repair.
//
// It means the same thing on a sink as on a source: a generation that has
// MOVED is counted by [StaleChannelClosesForTest] on both, and only a channel
// somebody has already given up is a decline. The source used to count both
// as declines, which made this useless to a test holding one of each
// (round 1, correctness-declinedcloses-conflated).
//
// It is the counter a test waits on to assert that a repair did NOT happen:
// the absence of a second channel is also true of a test that looked before
// the notification arrived.
func DeclinedClosesForTest(b *Backend) uint64 { return b.declinedCloses.Load() }

// ClosingSinksForTest is how many of the client's publishers have begun
// closing: [sink.Close] has marked them closed and has not yet dropped them.
//
// It is the one point in `Close` a test can stand on. The interlock of the specification is about what an attach does when it resumes INSIDE that
// window, and a test that started its Close and then released the attach would
// be racing the two rather than ordering them.
func ClosingSinksForTest(b *Backend) int {
	sinks, _ := b.children()
	n := 0
	for _, s := range sinks {
		if s.isClosed() {
			n++
		}
	}
	return n
}

// MapForTest maps a native error through this driver rules.
//
// It is exported because the mapping is a table over the library's own error
// type and the AMQP specification's reply codes, and the only way to hold
// every row to its code and its retryability is to feed the mapper the error.
// Driving each one through a real request would need a fault per row and would
// still not reach the rows whose operation nothing in the suite performs.
func MapForTest(b *Backend, op string, err error) error {
	return b.errs.Map(op, "t", "s", err)
}

// MapNetForTest is [MapForTest] for a failure whose request may already have
// left the socket, which is the distinction the specification asks a driver to make and
// the one an ambiguity test is about.
func MapNetForTest(b *Backend, op string, err error, sentAlready bool) error {
	return b.errs.MapNet(op, "t", "s", err, sentAlready)
}

// DialerForTest is the dialler a backend was built over, so that a test can
// drive the same fake the driver is using.
func DialerForTest(b *Backend) client.Dialer { return b.dial }

// FactsForTest is the broker and configuration facts the capability table was
// built from.
func FactsForTest(b *Backend) string { return b.Facts() }

// CapabilitiesForTest is the table this client built, for a golden test in
// another module.
func CapabilitiesForTest(b *Backend) mq.Capabilities { return b.caps }

// OfflineBackendForTest builds a backend with no connection at all.
//
// It exists for the surfaces that are pure and are the ones a driver most
// often gets wrong: the error table, the capability table, the naming and the
// message mapping. Every one of them is decided before a socket exists — which
// is why [newBackend] and [Backend.start] are separate functions — and a test
// that had to dial in order to reach them would be a test nobody runs without
// Docker.
//
// The returned stop function releases the backend's own context. There is no
// goroutine to stop, because none is started until the first dial.
func OfflineBackendForTest(cfg Config) (b *Backend, stop func(), err error) {
	back, err := newBackend(cfg)
	if err != nil {
		return nil, nil, err
	}
	caps, err := capabilities(back.fx)
	if err != nil {
		back.cancel()
		return nil, nil, err
	}
	back.caps = caps
	return back, back.cancel, nil
}

// CapabilitiesForFactsForTest builds the capability table from a set of facts
// named in the portable terms a test can express.
//
// It is the only way to hold a conditional claim to its own condition without
// a broker for every combination: the table depends on the broker's version,
// the queue type, whether the delayed-message plugin is there and whether this
// client may declare, and four of those cannot be varied by configuration
// alone.
func CapabilitiesForFactsForTest(version string, quorum, declare, deadLetter, delayPlugin bool, maxPriority uint8, keyAsRoutingKey bool) (mq.Capabilities, error) {
	v, _ := rabbit.ParseVersion(version)
	return capabilities(facts{
		version:         v,
		quorum:          quorum,
		declare:         declare,
		deadLetter:      deadLetter,
		delayPlugin:     delayPlugin,
		delayKind:       delay.Auto,
		maxPriority:     maxPriority,
		keyAsRoutingKey: keyAsRoutingKey,
		mandatory:       true,
	})
}

// DecodeForTest maps one AMQP delivery into a message and its delivery
// metadata, which is the receive half of the protocol mapping.
//
// It is exported because the mapping's hardest cases cannot be reached through
// a broker at all: `x-delivery-count` arriving in a width RabbitMQ does not
// currently choose, a nested field table from a publisher that is not this
// driver, a delivery tag of zero. Each of them is a table the library could
// hand over and a driver has to survive, and driving them through a real
// broker would mean a broker that produced them on request.
func DecodeForTest(b *Backend, d *amqp.Delivery, quorum bool) (*mq.Message, mq.DeliveryInfo, error) {
	var (
		msg    mq.Message
		fields protocol.Fields
	)
	info, err := decode(b.proto, &msg, &fields, d, b.options(mq.MetadataNativeStrict), "workers", quorum, b.clock.Now())
	return &msg, info, err
}

// GenerationsForTest is each open source's channel generation.
//
// It is the one observable that says "the driver has NOTICED": a channel that
// the broker closed is not a channel the driver knows about until the
// notification reaches its pump, and everything that follows — the token
// invalidation, the ring drain, the re-attach — happens after that. A test
// that armed a fault and then asserted on the driver's behaviour without
// waiting for the generation to move would be racing the notification rather
// than testing the guard, and `FuzzRabbitTagSettlement` spent forty seconds
// proving that: it reported a duplicate delivery that was really the model's
// own epoch running ahead of the driver's.
func GenerationsForTest(b *Backend) []uint32 {
	_, srcs := b.children()
	out := make([]uint32, 0, len(srcs))
	for _, s := range srcs {
		_, gen := s.current()
		out = append(out, gen)
	}
	return out
}

// AttachedForTest is how many open sources currently have a channel.
//
// It is the companion to [GenerationsForTest] and answers the question that
// one cannot: a source whose consumer the BROKER cancelled does not re-attach,
// so its generation moves once and then stops, and a test waiting for the next
// move would wait for ever. Zero attached sources is the post-cancel steady
// state.
func AttachedForTest(b *Backend) int {
	_, srcs := b.children()
	n := 0
	for _, s := range srcs {
		if ch, _ := s.current(); ch != nil {
			n++
		}
	}
	return n
}

// errOneSourceForTest is what the seams below answer when the client does not
// have exactly the one consumer they need.
var errOneSourceForTest = errcode.New(errcode.InvalidArgument,
	"iqmq/rabbitmq: this test seam needs a client with exactly one open consumer")

// errTableFullForTest is [OccupySlotForTest]'s answer when the consumer has no
// free slot to occupy, which is a different mistake from having the wrong
// number of consumers and used to be reported as that one.
var errTableFullForTest = errcode.New(errcode.InvalidArgument,
	"iqmq/rabbitmq: OccupySlotForTest found every token slot of the consumer in use")

// LateAdmissionForTest reproduces the state an admission that raced a channel
// replacement leaves behind, which no test can otherwise reach without
// winning a race.
//
// The order is the one `source.claimLost` and `source.discard` run in and the
// one a late admission lands between. The delivery is admitted under the
// generation that IS current, so `source.admit`'s own staleness check passes,
// and the channel is then replaced without the ring being drained again --
// which is what the pump's own push does when it completes after
// `source.discard` has walked the ring. The ring is then holding a delivery
// from a channel the broker has already taken everything back on, and whether
// that delivery can reach a caller is.
func LateAdmissionForTest(b *Backend, body string) error {
	_, srcs := b.children()
	if len(srcs) != 1 {
		return errOneSourceForTest
	}
	s := srcs[0]
	s.mu.Lock()
	gen := s.gen
	s.mu.Unlock()
	d := &amqp.Delivery{DeliveryTag: 1, Body: []byte(body)}
	// A nil `dead` channel is the slot wait's channel-death exit removed, and
	// it is right here: this seam admits ONE delivery into a table that has
	// every slot free, so `claimSlot` never waits and never reaches the
	// select (round 1, rigour-6).
	if err := s.admit(s.b.ctx, gen, newStore(s.prefetch), nil, d); err != nil {
		return err
	}
	s.claimLost(0)
	return nil
}

// SharedSlotForTest is the check: it makes the replacement
// attachment claim the token slot the previous one is still using, and reports
// what the two deliveries say their bodies are.
//
// It is the shape the race detector found through `FuzzRabbitTagSettlement`,
// with the race taken out of it. `driverkit.TokenTable.InvalidateAll` frees
// every slot at once, so the pump of the channel that replaced a dead one
// claims slot zero while the delivery that had slot zero is still in a
// caller's hands -- and while per-slot storage belonged to the SOURCE, both
// deliveries were one `mq.Message`. Two pumps writing it is the race; a body
// The specification says stays borrowed until settlement being rewritten under a caller
// who is reading it is the damage.
//
// Both returned bodies are the ones that were admitted when the storage
// belongs to the attachment. They are both `second` when it does not.
func SharedSlotForTest(b *Backend, first, second string) (string, string, error) {
	_, srcs := b.children()
	if len(srcs) != 1 {
		return "", "", errOneSourceForTest
	}
	s := srcs[0]
	s.mu.Lock()
	gen := s.gen
	s.mu.Unlock()

	got := make([]mq.DriverDelivery, 1)
	held := make([]*mq.Message, 0, 2)
	for i, body := range []string{first, second} {
		if i == 1 {
			// The reconnection, as far as the token table is concerned:
			// every slot is free again and the next claim takes the one the
			// delivery above is still using.
			s.tokens.InvalidateAll()
		}
		d := &amqp.Delivery{DeliveryTag: uint64(i + 1), Body: []byte(body)}
		// nil `dead`, for [LateAdmissionForTest]'s reason: the table is
		// invalidated before the second admission, so neither waits.
		if err := s.admit(s.b.ctx, gen, newStore(s.prefetch), nil, d); err != nil {
			return "", "", err
		}
		n, err := s.adapter.Fetch(s.b.ctx, got)
		if err != nil {
			return "", "", err
		}
		if n != 1 {
			return "", "", errOneSourceForTest
		}
		held = append(held, got[0].Message)
	}
	return string(held[0].Body), string(held[1].Body), nil
}

// ReattachForTest runs the dial loop's re-attach step, `source.reattach`, on
// the client's one consumer, which is what `Backend.redial` does to every
// source once the new connection is installed.
//
// It reproduces, without winning a race, the order the specification is about:
// the caller's first Fetch after a reconnection attaches the source on the new
// connection, and the dial loop's own re-attach reaches the source AFTERWARDS.
// A test calls it on a source that is already attached and asserts that
// nothing was replaced.
func ReattachForTest(b *Backend) error {
	_, srcs := b.children()
	if len(srcs) != 1 {
		return errOneSourceForTest
	}
	return srcs[0].reattach(b.ctx)
}

// OccupySlotForTest claims one of the consumer's token slots for no delivery,
// and returns the function that frees it.
//
// It holds open, for as long as a test likes, the window the specification is
// about: a settlement writes its frame BEFORE it frees its slot, and the
// broker may send the next delivery in between, while the table is still full.
// A test that relied on the real window would be asserting who won a race.
func OccupySlotForTest(b *Backend) (release func(), err error) {
	_, srcs := b.children()
	if len(srcs) != 1 {
		return nil, errOneSourceForTest
	}
	s := srcs[0]
	token, ok := s.tokens.Claim(&deliveryRef{})
	if !ok {
		return nil, errTableFullForTest
	}
	return func() { _ = s.tokens.Release(token) }, nil
}

// SlotWaitsForTest is how many deliveries found the consumer's token table
// full and waited for a slot. See [source.slotWaits].
func SlotWaitsForTest(b *Backend) uint64 {
	_, srcs := b.children()
	var n uint64
	for _, s := range srcs {
		n += s.slotWaits.Load()
	}
	return n
}

// WaitingRequestsForTest is how many replies the request endpoint is still
// holding a waiter for.
//
// It exists because "the correlation table does not leak" is otherwise
// unobservable: a request that gave up returns its error either way, and an
// entry left behind costs nothing a test can see until a long-running client
// has accumulated one per timed-out request. Removing the cleanup passed
// every request/reply test until this could be asserted on.
func (b *Backend) WaitingRequestsForTest() int {
	if b.rq == nil {
		return 0
	}
	b.rq.mu.Lock()
	defer b.rq.mu.Unlock()
	return len(b.rq.waiting)
}

// RepairInFlightForTest runs fn while the client's one publisher is marked as
// having a repair in flight, which is the state [sink.detachedError] tells
// "a channel is coming" from "no channel is coming" by.
//
// It exists because that state is DERIVED rather than stored: there is no
// sticky record to corrupt, and therefore no late write for a test to drive.
// What a test can still ask is that the derivation answers correctly on each
// side of the flag, and this is how it stands on the near side of it.
func RepairInFlightForTest(b *Backend, fn func()) error {
	sinks, _ := b.children()
	if len(sinks) != 1 {
		return errOneSinkForTest
	}
	s := sinks[0]
	s.repairing.Store(true)
	defer s.repairing.Store(false)
	fn()
	return nil
}

// errOneSinkForTest is what the seam above answers when the client does not
// have exactly the one publisher it needs.
var errOneSinkForTest = errcode.New(errcode.InvalidArgument,
	"iqmq/rabbitmq: this test seam needs a client with exactly one open publisher")
