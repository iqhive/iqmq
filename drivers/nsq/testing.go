package nsq

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
// IQMQ_NSQ_ADDR, and the conformance suite against the pinned
// nsqd/nsqlookupd pair, which is the evidence the generated capability matrix
// is built from. A harness duplicated in two places would drift, and the half
// that drifted would be the one nobody ran.
func ConformanceHarness(cfg Config, ev testkit.Evidence, broker testkit.Broker) testkit.Harness {
	var seq atomic.Uint64
	nonce := strconv.FormatInt(time.Now().UnixNano(), 36)
	return testkit.Harness{
		Driver:   DriverName,
		Evidence: ev,
		Broker:   broker,
		Client:   testkit.ClientInfo{Module: ClientModule, Version: ClientVersion},
		Timeout:  45 * time.Second,
		// The specification asks for 100,000 interleaved messages. Every publication
		// here is a PUB frame answered by nsqd — go-nsq has no
		// fire-and-forget publish this driver could use without giving up
		// the confirmation boundary — and the scenario publishes them one
		// at a time, so the figure is lowered to keep the scenario inside
		// the suite's timeout. docs/drivers/nsq.md says so, which is what
		// The specification asks of a driver that lowers it.
		Fidelity: 10_000,
		TopicName: func(purpose string) string {
			// An NSQ topic name is at most sixty-four bytes of
			// [.a-zA-Z0-9_-], so the nonce and the counter come FIRST and
			// the purpose is what gets truncated: two scenarios with the
			// same sixty-four-byte prefix would otherwise share a topic.
			head := "iqmqconf-" + nonce + "-" + strconv.FormatUint(seq.Add(1), 36) + "-"
			return head + truncate(sanitise(purpose), MaxNameLen-len(head))
		},
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
				// mq.Client.Close takes no context — the specification makes it
				// prompt and bounded rather than cancellable — so there
				// is no context to pass on here.
				_ = c.Close() //nolint:contextcheck // Client.Close has no context parameter to pass
				return nil, err
			}
			return c, nil
		},
		// The wildcard is supplied even though this driver does not claim
		// FeatureWildcardSubscribe, which is the lesson: without it both
		// wildcard scenarios skip and the claim stays unverified in BOTH
		// directions. With it, the driver's own ErrUnsupported proves the
		// negative.
		Wildcard: func(topic string) string { return topic + ".x*" },
		Quiesce:  quiesce,
		Skip:     conformanceSkips(cfg),
		// Advance is deliberately nil. There is no way to move a real
		// nsqd's clock, so a scenario that needs an expired lease waits for
		// one — which is why shape() gives every scenario a short ack
		// deadline rather than nsqd's default minute.
	}
}

// secretsOf is the credentials the kit must find in no rendered string.
func secretsOf(cfg Config) []string {
	if s := cfg.AuthSecret.Reveal(); s != "" {
		return []string{s}
	}
	return nil
}

// quiesce waits for nsqd to have handed over what it was given.
//
// nsqd holds a write to a client for up to its output-buffer timeout, so a
// scenario that expects NOTHING to arrive has to wait at least that long
// before concluding it. It is a sleep because there is nothing to poll: nsqd's
// /stats reports a channel's depth, and a depth of zero does not mean the
// message has reached this consumer's socket.
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
const quiesceDelay = 250 * time.Millisecond

// shape applies the parts of a scenario's Need that are CONFIGURATION on this
// driver rather than topology.
//
// Two of the settings deserve stating.
//
// EnsureTopology is turned ON here and is OFF by default in production, which
// is not a contradiction: the rule is about a deployment, and the
// conformance kit's own provisioning is the operator. Leaving it off would
// make a consumer opened before its channel existed miss everything published
// in between.
//
// AckDeadline is clamped into nsqd's own window rather than passed through,
// and Metadata is MetadataAdaptive. The specification says this broker needs the envelope for
// portable metadata and the specification makes that an emulation a deployment asks for
// once; the kit configures every handle at the zero value, so the ask has to
// be here. It is what lets `publish/key-roundtrip` and
// `publish/roundtrip-bodies` run at all — a key, a nil body and an empty body
// are all things nsqd cannot carry and the envelope can — and it is why
// `metadata/native-strict-rejection` is documented as inapplicable.
func shape(cfg Config, n testkit.Need) Config {
	out := cfg
	out.EnsureTopology = true
	if out.Metadata == mq.MetadataNativeStrict {
		out.Metadata = mq.MetadataAdaptive
	}
	switch {
	case n.AckDeadline > 0:
		// Clamped UP to nsqd's own floor. A scenario that asks for 500 ms
		// gets one second, because nsqd refuses less in the IDENTIFY
		// handshake — and a second is still short enough for a scenario
		// that waits for a lease to expire. Refusing the Need instead
		// would skip the scenario rather than run it (the rule).
		out.AckDeadline = max(n.AckDeadline, MinAckDeadline)
	case n.AckDeadline < 0:
		// The kit's "leases that never expire". Every NSQ lease expires, so
		// the honest answer is the longest one nsqd grants: fifteen minutes
		// is "never" inside a forty-five-second scenario, and refusing the
		// Need would skip the scenario instead of running it.
		out.AckDeadline = out.resolve().MaxAckDeadline
	default:
		// Shorter than nsqd's own minute, because `Advance` is nil here: a
		// scenario that waits for a lease to expire waits in real seconds.
		out.AckDeadline = 5 * time.Second
	}
	if out.LeaseGuard >= out.AckDeadline || out.LeaseGuard == 0 {
		out.LeaseGuard = out.AckDeadline / 5
	}
	if out.OutputBufferTimeout != 0 && out.OutputBufferTimeout < MinOutputBufferTimeout {
		out.OutputBufferTimeout = MinOutputBufferTimeout
	}
	if n.MaxMessageSize > 0 {
		// The limit really is enforced, and advertised, so the
		// oversized-payload scenario refuses a message just over it rather
		// than a one-mebibyte one.
		out.MaxMessageBytes = int64(n.MaxMessageSize)
		if out.MaxBodyBytes < out.MaxMessageBytes {
			out.MaxBodyBytes = out.MaxMessageBytes
		}
	}
	return out
}

// provision creates the topic and every channel a scenario named, before the
// scenario publishes anything.
//
// It matters on this broker in a way it does not on most. nsqd holds a topic's
// messages until the topic has at least one channel and then hands the backlog
// to the channel that appears — but a SECOND channel created afterwards starts
// empty. So a fan-out scenario that published before opening its second
// consumer would see the second receive nothing, through no fault of the
// driver. Creating them all up front is the operator's job, and here the kit
// is the operator.
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
		subs := n.Subscriptions
		if len(subs) == 0 {
			subs = []string{n.Sub(0)}
		}
		for _, sub := range subs {
			if err := admin.EnsureSubscription(ctx, mq.SubscriptionSpec{Topic: topic, Name: sub}); err != nil {
				return err
			}
		}
	}
	return nil
}

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
// all things nsqd does not have, and in every case the refusal the negative
// probe is looking for happens where the specification puts it — at the publication
// control or at the open — which is where the probe looks.
func checkNeed(testkit.Need) error { return nil }

// conformanceSkips names the scenarios this driver documents as inapplicable,
// with the reason, which the report publishes.
//
// Every entry names something nsqd does not have or something the kit cannot
// observe on it, and each was added after watching the scenario fail against
// the pinned container rather than in anticipation.
func conformanceSkips(cfg Config) map[string]string {
	skips := map[string]string{
		"metadata/native-strict-rejection": "this harness configures Config.Metadata, which is the deployment's one-time \"yes, envelope on this broker\" (the specification makes the header-less envelope an emulation available under EmulationAll, and the SPI carries no emulation policy). A handle that names MetadataNativeStrict explicitly is indistinguishable from one that named nothing, so no handle in this run is in strict mode. The refusal itself is covered by the driver's own TestTheEnvelopeIsTheOnlyMetadataPath, which watches every mode refuse or carry what it should",
		"start/positions":                  "an NSQ channel has no committed position and keeps nothing it has finished: a message that has not been finished is at the head. StartLatest and StartEarliest are therefore the same position, and the driver refuses a time or a sequence with ErrUnsupported rather than pretending to one",
	}
	if len(cfg.AuthSecret) == 0 {
		skips["secrets/redaction"] = "this harness declares no credential, because the pinned nsqd runs without authentication; the scenario needs a secret to look for and skips rather than passing vacuously"
	}
	return skips
}

// sanitise turns a scenario name into NSQ topic-name characters.
func sanitise(purpose string) string {
	return strings.Map(func(r rune) rune {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9',
			r == '.', r == '_', r == '-':
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
