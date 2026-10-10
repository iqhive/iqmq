package sqs

import (
	"context"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/testkit"
)

// ConformanceHarness is what this driver hands the conformance kit.
//
// It is exported, and in a non-test file, because two different modules run
// it: this module's own env-gated test against an endpoint named by
// IQMQ_SQS_ADDR, and conformance/suites/sqs_test.go against the pinned
// ElasticMQ and LocalStack containers, which is the evidence the generated
// capability matrix is built from. A harness duplicated in two places would
// drift, and the half that drifted would be the one nobody ran.
func ConformanceHarness(cfg Config, ev testkit.Evidence, broker testkit.Broker) testkit.Harness {
	var seq atomic.Uint64
	nonce := strconv.FormatInt(time.Now().UnixNano(), 36)
	profile := cfg.Profile()
	return testkit.Harness{
		Driver:   DriverName,
		Profile:  profile,
		Evidence: ev,
		Broker:   broker,
		Client:   testkit.ClientInfo{Module: ClientModule, Version: ClientVersion},
		Timeout:  45 * time.Second,
		// The specification asks for 100,000 interleaved messages. Every message here is
		// an HTTPS request to a queue, and the fidelity scenario publishes
		// them one at a time, so the figure is lowered to keep one scenario
		// inside the suite's per-scenario timeout. docs/drivers/sqs.md says
		// so, which is what the specification asks of a driver that lowers it.
		Fidelity: 2_000,
		TopicName: func(purpose string) string {
			// A queue name takes alphanumerics, hyphens and underscores and
			// is at most eighty characters, so the nonce and the counter come
			// FIRST and the purpose is what gets truncated: two scenarios
			// with the same eighty-character prefix would otherwise share a
			// queue.
			head := "iqmqconf-" + nonce + "-" + strconv.FormatUint(seq.Add(1), 36) + "-"
			return head + truncate(sanitise(purpose), MaxQueueNameLen-len(head)-len(fifoSuffix))
		},
		// The credentials are the point of this harness for the kit: SQS is
		// the first driver whose DSN carries a real secret, so
		// `secrets/redaction` can finally run rather than skip.
		Secrets: secretsOf(cfg),
		Open: func(ctx context.Context, n testkit.Need) (*mq.Client, error) {
			if err := checkNeed(n); err != nil {
				return nil, err
			}
			return Open(ctx, shape(cfg, n))
		},
		// The wildcard is supplied even though this driver does not claim
		// FeatureWildcardSubscribe, which is the lesson: without it both
		// wildcard scenarios skip and the claim stays unverified in BOTH
		// directions. With it, the driver's own ErrUnsupported proves the
		// negative.
		Wildcard: func(topic string) string { return topic + "-*" },
		Quiesce:  quiesce,
		Skip:     conformanceSkips(cfg.FIFO),
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

// quiesce waits for the service to have settled.
//
// The service's own documentation calls its counts and its delivery
// eventually consistent, and both emulators are immediate — so this is
// insurance for the managed service rather than something either container
// needs. It is a sleep because there is nothing to poll: no API reports "every
// message you sent is now receivable".
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
// AutoCreate is turned ON here and is OFF by default in production, which is
// not a contradiction: the rule is about a deployment, and the conformance
// kit's own provisioning is the operator. Leaving it off would make every
// scenario fail at open with "the queue does not exist".
func shape(cfg Config, n testkit.Need) Config {
	out := cfg
	out.AutoCreate = true
	switch {
	case n.AckDeadline > 0:
		out.AckDeadline = n.AckDeadline
	case n.AckDeadline < 0:
		// The kit's "leases that never expire". Every SQS lease expires, so
		// the honest answer is the longest one the service grants: twelve
		// hours is "never" inside a forty-five-second scenario, and
		// refusing the Need would skip the scenario instead of running it.
		out.AckDeadline = MaxVisibility
	}
	if n.MaxMessageSize > 0 {
		// The queue really is created with this ceiling, so the capability
		// table advertises it and the oversized-payload scenario refuses a
		// four-kilobyte message rather than a one-mebibyte one.
		out.MaxMessageBytes = int64(max(n.MaxMessageSize, minQueueMessageSize))
	}
	if n.DeadLetterTopic != "" {
		// the destination is the SUBSCRIPTION's and the SPI's
		// SourceConfig does not carry it, so on this driver it is
		// configuration and the harness is what turns a scenario's Need into
		// it. The name is derived through the same mapping a topic goes
		// through, so the scenario can consume the destination as a topic.
		if name, err := QueueName(n.DeadLetterTopic, cfg.QueuePrefix, cfg.FIFO); err == nil {
			out.DeadLetterQueue = name
			out.MaxReceiveCount = int(max(n.MaxAttempts, 2))
		}
	}
	if cfg.FIFO {
		// Every FIFO publication needs a deduplication identifier, and the
		// kit publishes without a DedupKey almost everywhere. The driver
		// generates a unique one per publication for exactly that, so
		// nothing is needed here — but a scenario that publishes the SAME
		// body twice and expects two deliveries would fail against a queue
		// created with content-based deduplication, which is why the
		// harness never turns it on.
		out.ContentBasedDeduplication = false
	}
	return out
}

// minQueueMessageSize is the smallest MaximumMessageSize the service accepts
// for a queue.
const minQueueMessageSize = 1024

// checkNeed answers whether this broker can build the topology a scenario
// asks for.
//
// It refuses NOTHING, and that is deliberate. The obvious reading of Need is
// "refuse what this broker cannot build", and a harness written that way skips
// the negative direction of every claim the driver makes — which is exactly
// the evidence the specification asks for. The rule that works is narrower: refuse a Need
// only when accepting it would make a scenario prove something FALSE
// , and on this broker there is no such Need. A priority
// topic, a deduplication window, a partition count and a retained topic are
// all things the service does not have, and in every case the refusal the
// scenario is looking for happens where put it — at the
// publication control or at the settlement — which is where the negative probe
// looks.
func checkNeed(testkit.Need) error { return nil }

// conformanceSkips names the scenarios this driver documents as inapplicable,
// with the reason, which the report publishes.
//
// Every entry names something the SERVICE does not have or something the kit
// cannot observe, and each of them was added after watching the scenario fail
// against the pinned ElasticMQ container rather than in anticipation.
//
// All of them are now the first shape: a queue has no HISTORY and no POSITION.
// A message that has not been deleted is at the head; one that has been
// deleted is gone. So "start at the latest" is not a position a queue can be
// opened at, and a second subscription cannot read what the first consumed —
// and a driver has nowhere to refuse either, because the SPI's StartPosition
// and Subscription are accepted at open and the difference only shows up in
// what arrives.
//
// The four this list USED to carry were a limitation of the kit, and they are
// gone because the kit was repaired rather than because anything about SQS
// changed. A negative claim about a GUARANTEE cannot be
// refuted by one run, and the kit now says so itself: `refusedGuarantee`
// reports a probe that saw no counter-example as a skip whose note explains
// that an absence is not observable in one run ,
// which is what the three ordering entries and the FIFO exactly-once entry
// were standing in for. And `publish/key-roundtrip` no longer correlates four
// keys by arrival order — each key is its own round trip  —
// which is what the FIFO entry for it was standing in for. A skip whose stated
// reason has been fixed is worse than no skip at all: the report goes on
// saying the scenario cannot run here, so nobody ever finds out that it can.
func conformanceSkips(fifo bool) map[string]string {
	skips := map[string]string{
		"start/positions":           "a queue has no committed position and no retained history: a message that has not been deleted is at the head, so StartLatest and StartEarliest are the same position and a consumer opened after a publication receives it. The start positions have no meaning on a queue, and the driver refuses a time or a sequence rather than pretending to one",
		"capability/FanOut/refused": "the probe opens two subscriptions of one topic and reads the refusal. A queue has exactly one set of messages, so the two compete and the second times out rather than being refused: the SPI accepts SourceConfig.Subscription at open and a driver has nowhere to say \"these two names are the same queue\". drivers/snssqs is where a real fan-out lives",
		"capability/Replay/refused": "the probe consumes a topic's history with one subscription and expects a second to be refused. A settled message is DELETED, so the second subscription finds nothing and times out rather than being refused; there is no open-time refusal a driver could make, because the start position the probe uses is one this driver accepts",
	}
	if !fifo {
		return skips
	}
	// One more on the FIFO profile: a probe whose condition is real and
	// which no kit repair has removed.
	skips["capability/BatchReceive/works"] = "the probe publishes eight messages with no key and asks for them in one Receive. On the FIFO profile they share one message group, and a FIFO queue holds a group's later messages while one is in flight — so the receive returns one, correctly. The claim is Conditional with that condition stated, and the standard profile's report proves the unconditional half"
	return skips
}

// sanitise turns a scenario name into queue-name characters.
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

// APIForTest is the API slice a backend was built over, so that a test can
// drive the same client the driver uses.
//
// It is exported for the reason [BackendWithAPI] is: the scripted server the
// four critical scenarios need is a test fixture, and a driver whose tests can
// only reach it through an unexported field cannot be tested from another
// module — which conformance/suites/sqs_test.go is.
func APIForTest(b *Backend) any { return b.api }

// StaleForTest is how many superseded deliveries a source's Fetch has dropped.
//
// It is exported because the property it reports is invisible from the
// portable API: a source that handed out a superseded delivery would fail a
// settlement the caller could do nothing about, and the only way to tell that
// apart from a driver that never reached the case is to count.
func StaleForTest(c any) (uint64, bool) {
	src, ok := c.(*source)
	if !ok {
		return 0, false
	}
	return src.Stale(), true
}

// ParkedPushesForTest is how many of a source's pollers are parked in the
// ring's Push, waiting for room.
//
// It is exported because it is the one observable state in which a poller is
// known to be holding received messages it has not admitted -- the rest of
// the ReceiveMessage answer it is part-way through -- which is the state
// Close has to release and, until the fix it tests, did not.
func ParkedPushesForTest(c any) (int, bool) {
	src, ok := c.(*source)
	if !ok {
		return 0, false
	}
	return src.ring.Blocked(), true
}

// SourcesForTest is every source a backend currently has open, so that a test
// can reach the counter above.
func SourcesForTest(b *Backend) []any {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make([]any, 0, len(b.srcs))
	for s := range b.srcs {
		out = append(out, s)
	}
	return out
}

// MapForTest maps a native error through this driver rules.
//
// It is exported because the mapping is a table over the SDK's generated error
// types and the service's own code strings, and the only way to hold every row
// to its code and its retryability is to feed the mapper the error. Driving
// each one through a real request would need a fault per row and would still
// not reach the rows whose operation nothing in the suite performs.
func MapForTest(b *Backend, op string, err error) error {
	return b.errs.Map(op, "t", "s", err)
}

// MapNetForTest is [MapForTest] for a failure whose request may already have
// left the socket, which is the distinction the specification asks a driver to make and
// the one an ambiguity test is about.
func MapNetForTest(b *Backend, op string, err error, sentAlready bool) error {
	return b.errs.MapNet(op, "t", "s", err, sentAlready)
}
