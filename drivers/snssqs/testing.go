package snssqs

import (
	"context"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	mq "bitbucket.org/iqhive/iqmq"
	"bitbucket.org/iqhive/iqmq/testkit"
)

// ConformanceHarness is what this driver hands the conformance kit.
//
// It is exported, and in a non-test file, because two different modules run
// it: this module's own env-gated test against an endpoint named by
// IQMQ_SNSSQS_ADDR, and the conformance suite against the pinned
// LocalStack container, which is the evidence the generated capability matrix
// is built from. A harness duplicated in two places would drift, and the half
// that drifted would be the one nobody ran.
//
// LocalStack and nothing else, and that is not a preference: ElasticMQ has no
// SNS at all. So this driver has ONE emulator where `drivers/sqs` has two, and
// the cross-check that found two of the defects — a claim one emulator
// proves and the other does not — is not available here. It is recorded in
// the maintenance notes.
func ConformanceHarness(cfg Config, ev testkit.Evidence, broker testkit.Broker) testkit.Harness {
	var seq atomic.Uint64
	nonce := strconv.FormatInt(time.Now().UnixNano(), 36)
	return testkit.Harness{
		Driver: DriverName,
		// The profile is EMPTY, so the report is snssqs.json and the
		// generated matrix column is "snssqs". There is one profile: the
		// FIFO topic is refused by CheckTopicName rather than implemented
		// , so a second column would describe nothing.
		Profile:  "",
		Evidence: ev,
		Broker:   broker,
		Client:   testkit.ClientInfo{Module: ClientModule, Version: ClientVersion},
		// Sixty seconds rather than the kit's thirty. Every message here
		// takes two hops — a signed HTTPS request to SNS, then SNS's own
		// delivery into a queue, then a long poll — against an emulator
		// that starts in about a minute and is slower per call than
		// ElasticMQ.
		Timeout: 60 * time.Second,
		// The specification asks for 100,000 interleaved messages. Every one of them
		// here is an SNS Publish followed by a fan-out and a receive, and
		// the fidelity scenario publishes them one at a time, so the figure
		// is lowered to keep one scenario inside the suite's per-scenario
		// timeout. It is lower than the SQS driver's 2,000 for the same
		// reason that driver's is lower than 100,000, one hop further on.
		// docs/drivers/snssqs.md says so, which is what the specification asks of a
		// driver that lowers it.
		Fidelity:  400,
		TopicName: topicNamer(nonce, &seq),
		// The credentials are what makes `secrets/redaction` run rather
		// than skip: the scenario needs a harness that declares a secret.
		Secrets: secretsOf(cfg),
		Open: func(ctx context.Context, n testkit.Need) (*mq.Client, error) {
			if err := checkNeed(n); err != nil {
				return nil, err
			}
			c, err := Open(ctx, shape(cfg, n))
			if err != nil {
				return nil, err
			}
			if err := provision(ctx, c, n); err != nil {
				// Client.Close takes no context, which contextcheck reads
				// as a lost one; the SPI's Close has none either, and what
				// bounds the release calls a closing client makes is
				// Config.APITimeout.
				_ = c.Close() //nolint:contextcheck // Client.Close takes no context; Config.APITimeout is the bound
				return nil, err
			}
			return c, nil
		},
		// The wildcard is supplied even though this driver does not claim
		// FeatureWildcardSubscribe, which is the lesson: without it both
		// wildcard scenarios skip and the claim stays unverified in BOTH
		// directions. With it, the driver's own ErrUnsupported proves the
		// negative.
		Wildcard: func(topic string) string { return topic + "-star" + "*" },
		Quiesce:  quiesce,
		Skip:     conformanceSkips(),
	}
}

// topicNamer builds the scenario topic names.
//
// The budget is the tightest thing about this harness and it is the fan-out's
// doing: an SNS topic name may be 256 characters, a QUEUE name may be 80, and
// this driver composes a queue name out of a topic AND a subscription. So the
// nonce and the counter come FIRST — two scenarios with the same truncated
// purpose would otherwise share a topic — and the purpose is what gets cut,
// to a length that leaves room for the longest subscription any scenario uses
// ("conformance"), the dead-letter scenario's "-dlq" suffix and the separator.
//
// [QueueName]'s hash would cover an overflow anyway ; the
// point of staying inside the budget is that the queue names in a failing
// run's logs are readable.
func topicNamer(nonce string, seq *atomic.Uint64) func(string) string {
	return func(purpose string) string {
		head := "iqmqsns-" + nonce + "-" + strconv.FormatUint(seq.Add(1), 36) + "-"
		const room = 60
		return head + truncate(sanitise(purpose), room-len(head))
	}
}

// secretsOf is the credentials the kit must find in no rendered string.
func secretsOf(cfg Config) []string {
	out := make([]string, 0, 2)
	for _, s := range [...]string{cfg.SecretAccessKey.Reveal(), cfg.SessionToken.Reveal()} {
		if s != "" {
			out = append(out, s)
		}
	}
	return out
}

// provision creates the topology every scenario needs BEFORE it runs, through
// the portable admin surface.
//
// It is the one thing this harness does that the SQS driver's does not, and it
// is load-bearing rather than tidy. SNS delivers to the subscriptions that
// exist WHEN A MESSAGE IS PUBLISHED: a publication to a topic nobody has
// subscribed to is accepted and delivered nowhere. Several scenarios
// publish before they open a consumer — `capability/Replay` is one — and on
// this broker every one of them would lose its message, which would look like
// a driver that drops publications rather than a topology that was not there
// yet.
//
// Provisioning here also matches what the plan asks of a driver suite
// ("Provision via client.Admin()") and what a real deployment does: an SNS
// fan-out's subscriptions are created by whoever creates the topic, not by the
// first publisher.
func provision(ctx context.Context, c *mq.Client, n testkit.Need) error {
	admin, err := c.Admin()
	if err != nil {
		return err
	}
	topics := []string{n.Topic}
	if n.DeadLetterTopic != "" {
		topics = append(topics, n.DeadLetterTopic)
	}
	for _, topic := range topics {
		if err := admin.EnsureTopic(ctx, mq.TopicSpec{Name: topic}); err != nil {
			return err
		}
		for _, sub := range n.Subscriptions {
			if err := admin.EnsureSubscription(ctx, mq.SubscriptionSpec{Topic: topic, Name: sub}); err != nil {
				return err
			}
		}
	}
	return nil
}

// quiesce waits for the fan-out to have happened.
//
// The service's own documentation calls its delivery eventually consistent and
// there is nothing to poll — no API reports "every message you published has
// reached every queue" — so this is a sleep, and it is twice the SQS driver's
// because there is an extra hop. It is what the scenarios that assert NOTHING
// arrived wait for; without it they would pass for the wrong reason.
func quiesce(ctx context.Context, _ *mq.Client) error {
	timer := time.NewTimer(quiesceDelay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// quiesceDelay is how long Quiesce waits.
const quiesceDelay = 500 * time.Millisecond

// shape applies the parts of a scenario's Need that are CONFIGURATION on this
// driver rather than topology.
//
// AutoCreate is turned ON here and is OFF by default in production, which is
// not a contradiction: the rule is about a deployment, and the conformance
// kit's own provisioning is the operator. NoQueuePolicy is turned on with it,
// because neither emulator enforces a queue policy and writing one would be
// two API calls per queue for no observable effect.
func shape(cfg Config, n testkit.Need) Config {
	out := cfg
	out.AutoCreate = true
	out.NoQueuePolicy = true
	switch {
	case n.AckDeadline > 0:
		out.AckDeadline = n.AckDeadline
	case n.AckDeadline < 0:
		// The kit's "leases that never expire". Every SQS lease expires, so
		// the honest answer is the longest one the service grants: twelve
		// hours is "never" inside a sixty-second scenario, and refusing the
		// Need would skip the scenario instead of running it (the rule).
		out.AckDeadline = MaxVisibility
	}
	if n.MaxMessageSize > 0 {
		// The subscribing queues really are created with this ceiling and
		// the driver advertises it, so the oversized-payload scenario
		// refuses a four-kilobyte message rather than a 256-kibibyte one.
		out.MaxMessageBytes = int64(max(n.MaxMessageSize, minQueueMessageSize))
	}
	if n.DeadLetterTopic != "" {
		// the destination is the SUBSCRIPTION's and the SPI's
		// SourceConfig does not carry it, so on this driver it is
		// configuration and the harness is what turns a scenario's Need
		// into it. The name is the queue the dead-letter TOPIC's own
		// subscription maps to, so the scenario can consume the destination
		// as a topic — which is what its probe does.
		if name, err := QueueName(n.DeadLetterTopic, n.Sub(0), cfg.TopicPrefix, cfg.QueuePrefix); err == nil {
			out.DeadLetterQueue = name
			out.MaxReceiveCount = int(max(n.MaxAttempts, 2))
		}
	}
	return out
}

// minQueueMessageSize is the smallest MaximumMessageSize SQS accepts for a
// queue, which is the floor a scenario's MaxMessageSize is raised to.
const minQueueMessageSize = 1024

// checkNeed answers whether this broker can build the topology a scenario asks
// for.
//
// It refuses NOTHING, and that is deliberate. The obvious reading of Need is
// "refuse what this broker cannot build", and a harness written that way skips
// the negative direction of every claim the driver makes — which is exactly
// the evidence the specification asks for. The rule that works is narrower: refuse a Need
// only when accepting it would make a scenario prove something FALSE
// , and on this broker there is no such Need. A priority
// topic, a deduplication window, a partition count and a retained topic are
// all things these services do not have, and in every case the refusal the
// scenario is looking for happens where put it.
func checkNeed(testkit.Need) error { return nil }

// conformanceSkips names the scenarios this driver documents as inapplicable,
// with the reason, which the report publishes.
//
// Every entry names something the SERVICES do not have or something the kit
// cannot observe, and each of them was added after watching the scenario fail
// against the pinned LocalStack container rather than in anticipation.
//
// The list is the SQS standard profile's minus one and plus one, and both
// movements are the fan-out:
//
//   - `capability/FanOut/refused` is GONE, because this driver claims the
//     feature: the positive probe and `fanout/isolation` run instead, and the
//     second of them is the per-queue settlement check.
//   - `capability/Replay/refused` is NEW, and it is the same shape as the specification's
//     ExactlyOnceDelivery skip — a probe two features share. The replay probe
//     publishes, consumes with one subscription, and expects a SECOND
//     subscription to be refused or to find nothing. On a fan-out the second
//     subscription has its own queue and its own copy, so it finds the message
//     and the probe passes — which would fail the refusal and force this
//     driver to claim a replay it does not have. A settled message is deleted
//     and a subscription created after a publication never sees it, so the
//     claim is right and the probe cannot refute it.
func conformanceSkips() map[string]string {
	return map[string]string{
		"start/positions":                       "a queue has no committed position and no retained history: a message that has not been deleted is at the head of its subscription's queue, so StartLatest and StartEarliest are the same position and a consumer opened after a publication receives what is still in the queue. The start positions have no meaning here, and the driver refuses a time or a sequence rather than pretending to one",
		"capability/Replay/refused":             replaySkip,
		"capability/OrderedByKey/refused":       orderingSkip,
		"capability/OrderedByPartition/refused": orderingSkip,
		"capability/OrderedGlobally/refused":    orderingSkip,
	}
}

// replaySkip is why the replay negative cannot run on a fan-out broker.
const replaySkip = "the negative probe publishes, consumes the publication with one subscription and expects a SECOND subscription to be refused or to find nothing. On a fan-out the second subscription is a DIFFERENT QUEUE with its own copy, so it finds the message and the probe reports that replay works — which is the fan-out being observed, not a history being replayed: a settled message is deleted, and a subscription created after a publication never receives it. The probe for FeatureFanOut and the probe for FeatureReplay are the same shape on this broker, which is the lesson B08 recorded about two features sharing one probe, one broker along "

// orderingSkip is the reason the three ordering negatives share.
//
// It is the finding restated: the kit cannot refute a negative claim about a
// GUARANTEE, only about a feature. The probe publishes thirty-two messages
// under one key and fails the driver if they arrive in order, which is the
// right test for "the driver says it cannot do this; prove it refuses" and the
// wrong one for "the service does not promise this".
const orderingSkip = "the negative probe fails the driver if thirty-two messages under one key arrive in order, and a standard SNS topic delivering into a standard queue, consumed by one consumer at that volume, almost always delivers them in order — an emulator with one consumer never reorders at all. The claim is that the services do not GUARANTEE the order, which one run cannot refute; an SNS FIFO topic is where ordering lives and is a profile this driver refuses rather than implements"

// sanitise turns a scenario name into topic-name characters.
func sanitise(purpose string) string {
	return strings.Map(func(r rune) rune {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '-', r == '_':
			return r
		default:
			return '-'
		}
	}, purpose)
}

// truncate cuts a name to n characters, and to nothing when n is not positive.
func truncate(s string, n int) string {
	if n <= 0 {
		return ""
	}
	if len(s) <= n {
		return s
	}
	return s[:n]
}

// SNSForTest is the SNS API slice a backend was built over, so that a test can
// drive the same client the driver uses.
//
// It is exported for the reason [BackendWithAPI] is : the
// scripted servers this driver's tests need are fixtures, and a surface a test
// needs is a surface.
func SNSForTest(b *Backend) any { return b.sns }

// SubscriptionForTest is the resolved topology of one (topic, subscription)
// pair, for a test that needs the queue a fan-out copy landed in.
func SubscriptionForTest(b *Backend, topic, sub string) (queueName, queueURL, queueARN, subARN string, ok bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	got, ok := b.subs[topic+"\x00"+sub]
	return got.QueueName, got.QueueURL, got.QueueARN, got.ARN, ok
}

// MapForTest maps a native error through this driver rules.
//
// It is exported because the mapping is a table over the SDK's generated error
// types and the service's own code strings, and the only way to hold every row
// to its code and its retryability is to feed the mapper the error.
func MapForTest(b *Backend, op string, err error) error {
	return b.errs.Map(op, "t", "s", err)
}

// MapNetForTest is [MapForTest] for a failure whose request may already have
// left the socket, which is the distinction the specification asks a driver to make and
// the one an ambiguity test is about.
func MapNetForTest(b *Backend, op string, err error, sentAlready bool) error {
	return b.errs.MapNet(op, "t", "s", err, sentAlready)
}

// BatchEntryErrorForTest maps one failed PublishBatch entry the way this
// driver does, for the test that holds every row of that translation.
//
// It is exported because a batch response reports a code STRING and a
// sender-fault flag rather than a typed error, so the translation cannot go
// through the rule table and cannot be reached by feeding the mapper an error.
// Producing every row through a real request would need a fault per row.
func BatchEntryErrorForTest(op, topic, code, message string, senderFault bool) error {
	return batchEntryError(op, topic, code, message, senderFault)
}

// CollapseForTest is the batch answer, for the test that holds it to index
// alignment without a service.
func CollapseForTest(errs []error) error { return collapse(errs) }
