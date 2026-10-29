package redis

import (
	"context"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/internal/streams"
	"github.com/iqhive/iqmq/testkit"
)

// ConformanceHarness is what this driver hands the conformance kit.
//
// It is exported, and in a non-test file, because two different modules run
// it: this module's own env-gated test against a server named by
// IQMQ_REDIS_ADDR, and conformance/suites/redis_test.go against the pinned
// container, which is the evidence the generated capability matrix is built
// from. A harness duplicated in two places would drift, and the half that
// drifted would be the one nobody ran.
//
// The harness encodes the things only the driver knows: which of a scenario's
// topology Needs this broker can build, and which scenarios have nothing to
// observe here.
func ConformanceHarness(cfg Config, ev testkit.Evidence, broker testkit.Broker) testkit.Harness {
	var seq atomic.Uint64
	nonce := strconv.FormatInt(time.Now().UnixNano(), 36)
	return testkit.Harness{
		Driver:   DriverName,
		Profile:  "streams",
		Evidence: ev,
		Broker:   broker,
		Client:   testkit.ClientInfo{Module: "github.com/redis/rueidis", Version: rueidisVersion},
		Timeout:  45 * time.Second,
		// The specification asks for 100,000 interleaved messages. Every message here is
		// a round trip to a server, and the fidelity scenario publishes them
		// one at a time, so the figure is lowered to keep one scenario inside
		// the suite's per-scenario timeout. The driver page says so, which is
		// what the specification asks of a driver that lowers it.
		Fidelity: 20_000,
		TopicName: func(purpose string) string {
			return "iqmqconf." + nonce + "." + strconv.FormatUint(seq.Add(1), 36) + "." + sanitise(purpose)
		},
		Open: func(ctx context.Context, n testkit.Need) (*mq.Client, error) {
			if err := checkNeed(n); err != nil {
				return nil, err
			}
			return Open(ctx, shape(cfg, n))
		},
		Skip: conformanceSkips(),
	}
}

// shape applies the parts of a scenario's Need that are CONFIGURATION on this
// driver rather than topology.
//
// Three of them are, and none of the three has a broker-side equivalent: the
// shard count is [Config.Partitions], the acknowledgement deadline is
// [Config.AckDeadline] (Redis grants no lease, so the deadline is entirely
// the driver's), and a dead-letter destination is a key derived from the
// topic. A harness that ignored them would run the ordering scenarios on one
// shard and the lease scenarios on a thirty-second deadline, and both would
// pass for the wrong reason.
//
// AutoCreate is turned ON here and is OFF by default in production, which is
// not a contradiction: the rule is about a deployment, and the conformance
// kit's Provision step is the operator. Leaving it off would make every
// scenario fail at open with "the stream does not exist".
func shape(cfg Config, n testkit.Need) Config {
	out := cfg
	out.AutoCreate = true
	if n.Partitions > 1 {
		out.Partitions = n.Partitions
	}
	if n.AckDeadline > 0 {
		out.AckDeadline = n.AckDeadline
		// The sweep has to be faster than the deadline or a lease-expiry
		// scenario waits for the default interval rather than for the
		// deadline it asked for.
		out.ClaimInterval = n.AckDeadline / 4
		if out.ClaimInterval < 10*time.Millisecond {
			out.ClaimInterval = 10 * time.Millisecond
		}
	}
	if n.DeadLetterTopic != "" {
		// the destination is the SUBSCRIPTION, and the SPI's
		// SourceConfig does not carry it — so on this driver it is
		// configuration, and the harness is what turns a scenario's Need
		// into it. A harness that ignored it would make every dead-letter
		// scenario run against a destination the scenario cannot consume.
		out.DeadLetterTopic = n.DeadLetterTopic
	}
	if n.DedupWindow > 0 {
		// Nothing to set: de-duplication is unsupported and the scenario's
		// negative direction is the publication control's refusal. Accepting
		// the Need is what lets that scenario RUN, which is the rule.
		_ = n.DedupWindow
	}
	return out
}

// ConformanceConfigFor answers the two questions a harness's Open has to ask
// of a scenario's Need: whether this broker can build the topology at all,
// and what the parts of it that are CONFIGURATION on this driver become.
func ConformanceConfigFor(cfg Config, n testkit.Need) (Config, error) {
	if err := checkNeed(n); err != nil {
		return Config{}, err
	}
	return shape(cfg, n), nil
}

// checkNeed answers whether this broker can build the topology a scenario
// asks for.
//
// It refuses almost nothing, and that is deliberate. The obvious reading of
// Need is "refuse what this broker cannot build", and a harness written that
// way skips the NEGATIVE direction of every claim the driver makes — which is
// exactly the evidence the specification asks for. The rule that works is narrower: refuse
// a Need only when accepting it would make a scenario prove something FALSE.
//
// On this broker that leaves one refusal, and it is not a topology at all:
// see below.
func checkNeed(n testkit.Need) error {
	if n.Wildcards {
		// A Redis stream key is a key. There is no pattern subscription for
		// streams — Redis's pattern subscription is pub/sub, which has no
		// consumer groups, no pending list and no acknowledgement — so a
		// scenario that asked for a wildcard subscription and got a
		// single-topic one would pass while proving nothing about
		// wildcards.
		return unsupportedNeed("pattern subscription for streams: PSUBSCRIBE is pub/sub, which has no consumer groups and no acknowledgement")
	}
	return nil
}

// unsupportedNeed is the refusal the kit turns into a skip with a reason.
func unsupportedNeed(what string) error { return &needError{what: what} }

// needError names the topology this broker cannot build. It wraps
// ErrUnsupported so that the kit's errors.Is finds the answer, and carries
// the reason so that the skip in the report says what was missing.
type needError struct{ what string }

func (e *needError) Error() string { return "iqmq/redis: Redis Streams has no " + e.what }

func (e *needError) Unwrap() error { return mq.ErrUnsupported }

// sanitise turns a scenario name into a key token.
//
// The one character that MUST go is a brace: the key naming hash-tags the
// topic, and a brace inside the tag would leave a topic's shards on different
// cluster slots. [streams.NewKeys] refuses one outright, so a scenario name
// carrying it would fail at open with a confusing error rather than run.
func sanitise(purpose string) string {
	return strings.Map(func(r rune) rune {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '-':
			return r
		default:
			return '-'
		}
	}, purpose)
}

// conformanceSkips names the scenarios this driver documents as inapplicable,
// with the reason, which the report publishes.
//
// It is deliberately short, and every entry names something the broker does
// not have rather than something the driver has not got round to.
func conformanceSkips() map[string]string {
	return map[string]string{}
}

// ProvisionFor creates the topology a test needs, for a caller driving the
// SPI rather than the conformance kit.
//
// It exists because the conformance kit's Provision step goes through
// Client.Admin, and a driver's own integration test often wants the streams
// and the group without a client at all.
func ProvisionFor(ctx context.Context, cmder streams.Commander, cfg Config, topic, sub string) error {
	keys, err := cfg.resolve().keysFor(topic)
	if err != nil {
		return err
	}
	group, err := streams.Group(sub)
	if err != nil {
		return err
	}
	cmds := make([]streams.Cmd, 0, keys.Shards)
	for i := range keys.Shards {
		cmds = append(cmds, streams.XGroupCreate(keys.Stream(i), group, streams.IDZero, true))
	}
	results, err := cmder.DoMulti(ctx, cmds)
	if err != nil {
		return err
	}
	for _, res := range results {
		if res.Err != nil && !streams.IsBusyGroup(res.Err) {
			return res.Err
		}
	}
	return nil
}

// MapForTest maps a native error through this driver rules.
//
// It is exported for one reason: the mapping is a prefix table over Redis's
// error replies, and the only way to hold seventeen rows to their codes and
// their retryability is to feed the mapper the reply text. Driving each one
// through a real command would need seventeen fault injections and would
// still not reach the rows whose command nothing in the suite sends.
func MapForTest(b *Backend, op string, err error) error {
	return b.errs.Map(op, "t", "s", err)
}

// SweepForTest runs one round of the mandatory reclaim loop against a source,
// synchronously.
func SweepForTest(b *Backend, src any) (int, int, error) {
	s, ok := src.(*source)
	if !ok {
		return 0, 0, errNilClient
	}
	n, err := s.sweep(contextTODO())
	if err != nil {
		return 0, 0, err
	}
	s.nudge()
	return n, s.cons.Reclaimer().Live(), nil
}

// contextTODO is a background context for the test helper above. It is spelled
// out rather than taken as a parameter so that the helper's signature does not
// suggest the sweep is cancellable from outside: in production its context is
// the loop's own, bounded by the claim interval.
func contextTODO() context.Context { return context.Background() }
