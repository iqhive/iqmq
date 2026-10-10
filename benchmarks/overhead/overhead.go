// Package overhead measures the layer-3 driver row --
// `BenchmarkDriver/<driver>/{Publish,PublishBatch,Receive,EndToEnd}/{64B,1KiB,64KiB}`
// -- against the raw-client baselines of `BenchmarkNative/<client>/…`.
//
// It is the numerator of the first two budgets, which are the project's
// second design principle:
//
//	Core overhead vs raw client, batched workloads   <= 5% throughput loss (goal 3%)
//	Core overhead vs raw client, single-message      <= 10% throughput loss;
//	                                                 <= 10 us added p99 where measurable
//
// Three things make the comparison a difference rather than two experiments.
//
// The LOOP is shared. Both arms run github.com/iqhive/iqmq/benchmarks/opbench,
// so the payload generator, the 100-message batch, the 256-message prefetch,
// the untimed refill, the round-trip warm-up and the reported metrics are one
// implementation. The only thing that differs between a
// `BenchmarkNative` row and the `BenchmarkDriver` row beside it is which
// opbench.Session the leaf opens.
//
// The CONFIRMATION BOUNDARY is stated rather than inherited. The specification's
// native-comparison rules refuse to compare an unconfirmed publication with a
// broker-confirmed one, and every baseline in
// benchmarks/native-baselines/nativebench/clients.go documents its own
// boundary as "the strongest ordinary acknowledgement the client offers
// without extra round trips" -- a NATS flush, a JetStream PubAck, an XADD
// reply with no WAIT. That is exactly mq.ConfirmAccepted, so this arm asks for
// it explicitly instead of taking mq.ConfirmDefault and hoping.
//
// The TOPOLOGY is the driver's own. Each profile's client comes from the
// driver's exported ConformanceHarness, which is the same translation from a
// testkit.Need to a stream, group, subject subtree or queue that the suite
// runs through -- rather than a second provisioning path written for the
// benchmark, which is how a driver comes to be measured in a shape no test
// covers.
//
// The topic's NAME is the one exception, and benchTopicNames is where it is
// argued: a conformance nonce is a test artefact, and on a broker that charges
// per command for the key's bytes it was being measured as though it were the
// library.
//
// Brokers come from the environment (IQMQ_REDIS_ADDR, IQMQ_NATS_ADDR, …) and a
// profile with no address SKIPS. This module deliberately does not start
// containers: hack/bench/run-local.sh does, one broker at a time, because a
// measurement on this host competes with everything else running on it and
// several brokers at once make the numbers contended rather than comparable.
package overhead

import (
	"context"
	"errors"
	"os"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
	jq "github.com/iqhive/iqmq/drivers/jetstream"
	nq "github.com/iqhive/iqmq/drivers/nats"
	rd "github.com/iqhive/iqmq/drivers/redis"
	"github.com/iqhive/iqmq/drivers/redis/goredis"
	"github.com/iqhive/iqmq/memory"
	"github.com/iqhive/iqmq/testkit"

	"github.com/iqhive/iqmq/benchmarks/opbench"
)

// Profile is one measurable driver profile: the arm name it publishes under,
// the raw-client baseline its numbers are read against, and how to build a
// client for it.
type Profile struct {
	// Arm is the benchmark path segment after BenchmarkDriver. It is
	// `<driver>` or `<driver>/<profile>`, the same pair testkit.Harness
	// reports and conformance/reports names its files after.
	Arm string
	// Native is the slug of the BenchmarkNative baseline this arm is
	// compared with, or "" when the driver wraps no third-party client and
	// there is therefore nothing to compare it to. The memory driver is the
	// one such profile: its "raw client" is the engine itself.
	Native string
	// AddrEnv is the environment variable carrying the broker address, or ""
	// for an in-process profile.
	AddrEnv string
	// Harness builds the driver's own conformance harness for an address.
	Harness func(addr string) testkit.Harness
	// Note records anything a reader of the overhead figure has to know
	// before subtracting one arm from the other.
	Note string
}

// Profiles returns every driver profile this package can measure, in
// measurement order: cheapest broker first, because a run that has to be
// repeated when the guard calls it contended is repeated from the start.
//
// It is deliberately a subset of the sixteen driver profiles that exist. A
// profile is here when its broker starts in seconds on a shared host and its
// raw-client baseline exists to compare against; docs/performance/overhead.md
// names every profile that is NOT here and why.
func Profiles() []Profile {
	return []Profile{
		{
			Arm:     "memory",
			AddrEnv: "",
			Harness: memoryHarness,
			Note:    "in-process; no raw client, so no BenchmarkNative pair -- the comparison for this profile is A28's api-vs-engine ratio",
		},
		{
			Arm:     "redis/streams",
			Native:  "rueidis",
			AddrEnv: "IQMQ_REDIS_ADDR",
			Harness: func(addr string) testkit.Harness {
				return benchTopicNames(rd.ConformanceHarness(rd.Config{Addrs: []string{addr}}, testkit.EvidenceContainer, broker("redis")))
			},
		},
		{
			Arm:     "redis/goredis",
			Native:  "goredis",
			AddrEnv: "IQMQ_REDIS_ADDR",
			Harness: func(addr string) testkit.Harness {
				return benchTopicNames(goredis.ConformanceHarness(rd.Config{Addrs: []string{addr}}, testkit.EvidenceContainer, broker("redis")))
			},
		},
		{
			Arm:     "nats/core",
			Native:  "nats",
			AddrEnv: "IQMQ_NATS_ADDR",
			Harness: func(addr string) testkit.Harness {
				return nq.ConformanceHarness(nq.Config{Addrs: []string{"nats://" + addr}}, testkit.EvidenceContainer, broker("nats"))
			},
			Note: "NATS Core is at-most-once on both arms: nothing is stored and nothing is settled",
		},
		{
			Arm:     "jetstream",
			Native:  "jetstream",
			AddrEnv: "IQMQ_NATS_ADDR",
			Harness: func(addr string) testkit.Harness {
				return jq.ConformanceHarness(jq.Config{Addrs: []string{"nats://" + addr}}, testkit.EvidenceContainer, broker("nats"))
			},
		},
	}
}

// benchTopicNames replaces a driver's CONFORMANCE topic naming with the
// benchmark's own, for the profiles where the name's LENGTH is part of what the row measures.
//
// the native-comparison rules ask both arms for the same topology, and a
// Redis stream key is topology with a per-command CPU cost attached. Measured
// with `redis-cli MONITOR` on each arm of
// `BenchmarkDriver/redis/streams/PublishBatch/64B`:
//
//	driver  XADD {iqmqconf.dluwbjfjtbjk.1.bench-PublishBatch}  44 B key, 42 hashed, 150 RESP B
//	native  XADD iqmqbench-add7a70a-1                          20 B key, 20 hashed, 126 RESP B
//
// The stored entry is identical on both arms (one field pair, a 1-byte field
// name, the 64-byte value), so the difference was the KEY and nothing else.
// rueidis computes the cluster slot with a table-driven CRC16 once per command
// on both arms (internal/cmds.slot, which hashes the hash tag's contents when
// there is a tag and the whole key when there is not), and in the driver's
// focused CPU profile `cmds.crc16` was the largest self-time symbol on the
// publish path. So the driver arm was being charged for hashing, sending and
// parsing 22 bytes per XADD that no deployment has.
//
// WHICH SIDE MOVES is the whole question, and it is not symmetric.
//
// The BRACES stay. `{topic}` is real driver behaviour, not a benchmark
// artefact: internal/streams.Keys hash-tags every key so that a topic's shards
// and its dead-letter and delayed keys land on one cluster slot and the Lua
// move stays atomic (drivers/redis/config.go's DeadLetterSuffix). The native
// arm publishes to ONE stream and needs no tag, so giving IT braces would
// charge rueidis for a guarantee it was not asked for and would flatter this
// library's ratio. The driver keeps paying for its own tagging: 2 RESP bytes
// per XADD, which is the honest remainder.
//
// The NONCE goes. `iqmqconf.<nonce>.<n>.<purpose>` is the conformance
// harness's resource naming (drivers/redis/testing.go), and it is there so
// that the scenarios do not collide with each other or with an earlier run
// on the same server. The benchmark needs exactly that property and gets it
// from opbench.Name's run id and counter in 20 bytes instead of 42 — the same
// generator, with the same stem, that every BenchmarkNative baseline already
// uses. Nothing about the driver changes; the measurement stops including the
// harness.
//
// After this the two arms hash the SAME number of bytes per command and the
// driver's key is longer by exactly the two braces it earns, which is what
// `TestTheRedisArmsHashTheSameKeyBytes` pins.
//
// It is applied per profile, not to every harness, because a driver's harness
// naming can be load-bearing: the JetStream harness returns subjects under the
// run's stream root, and a name from somewhere else would be a subject that
// stream does not capture. nats/core has the same asymmetry as Redis and no
// measured cost from it, and is left alone until somebody measures it.
func benchTopicNames(h testkit.Harness) testkit.Harness {
	// purpose is deliberately unused: it would lengthen the key, the native
	// arm carries no equivalent, and opbench.Name is already unique per call.
	h.TopicName = func(string) string { return opbench.Name(opbench.ResourceStem) }
	return h
}

// broker records which pinned image the numbers were taken against. The
// version is left to the run's own record: this module does not start the
// container and must not claim to know what tag it came from.
func broker(name string) testkit.Broker {
	return testkit.Broker{Image: os.Getenv("IQMQ_BENCH_" + name + "_IMAGE")}
}

var memSeq atomic.Uint64

// memoryHarness is the in-process profile's harness. The memory driver's own
// conformance harness is in a _test.go file and is therefore unreachable from
// here, so this is the same shape written out: AutoCreate off, the topology
// created through the portable Admin surface before the client is used, which
// is what the specification requires of Open and what every network profile above does
// through its driver's own translation.
func memoryHarness(string) testkit.Harness {
	return testkit.Harness{
		Driver:   "memory",
		Evidence: testkit.EvidenceInProcess,
		Broker:   testkit.Broker{Image: "in-process"},
		Timeout:  20 * time.Second,
		TopicName: func(purpose string) string {
			return "bench." + strconv.FormatUint(memSeq.Add(1), 36) + "." + purpose
		},
		Open: func(ctx context.Context, n testkit.Need) (*mq.Client, error) {
			c, err := memory.Open(ctx, memory.Config{})
			if err != nil {
				return nil, err
			}
			if err := provision(ctx, c, n); err != nil {
				//nolint:contextcheck // a client whose topology could not be created was never used; a prompt Close is all it needs, as in client.go's own never-returned-handle paths
				_ = c.Close()
				return nil, err
			}
			return c, nil
		},
	}
}

// provision creates the topic and subscription a Need names through the
// portable Admin surface.
func provision(ctx context.Context, c *mq.Client, n testkit.Need) error {
	adm, err := c.Admin()
	if err != nil {
		return err
	}
	if err := adm.EnsureTopic(ctx, mq.TopicSpec{Name: n.Topic}); err != nil {
		return err
	}
	for _, sub := range n.Subscriptions {
		if err := adm.EnsureSubscription(ctx, mq.SubscriptionSpec{Topic: n.Topic, Name: sub}); err != nil {
			return err
		}
	}
	return nil
}

// errNoProgress is what a Receive that returned nothing and reported nothing
// becomes: a contract violation, reported rather than spun on.
var errNoProgress = errors.New("overhead: Receive returned no delivery and no error, which ConsumerHandle.Receive's contract forbids")

// Addr returns the profile's broker address and whether it is available.
func (p Profile) Addr() (string, bool) {
	if p.AddrEnv == "" {
		return "", true
	}
	addr := os.Getenv(p.AddrEnv)
	return addr, addr != ""
}

// session is opbench.Session over the portable API: one publisher, one
// consumer, one topic, one subscription.
//
// Everything that can be hoisted out of the timed body is hoisted, because
// this is the specification measured a +-15% spread on and named
// `BenchmarkDriver/*`'s inherited problem: the message
// value, the delivery destination and the batch slice are allocated once and
// reused, so what the loop times is the publication and the settlement rather
// than the fixture.
type session struct {
	client *mq.Client
	pub    *mq.PublisherHandle
	con    *mq.ConsumerHandle
	one    *mq.Message
	batch  []*mq.Message
	dst    []mq.Delivery
	// settle is whether this driver's deliveries can be acknowledged at all.
	// NATS Core is at-most-once: it advertises no FeatureExplicitAck and every
	// Ack answers mq.ErrNotSettleable, which is what the first version of this
	// session failed every leaf with. Its raw-client baseline does not
	// acknowledge either -- core NATS has no ack to send -- so NOT settling is
	// what makes the two arms the same work, and settling where the driver
	// supports it is what makes the other four profiles comparable with
	// baselines that do.
	settle bool
}

// Open builds a session for one benchmark leaf. It fails the benchmark rather
// than returning a broken session, per opbench.Open's contract.
func Open(tb testing.TB, h testkit.Harness, p opbench.Params) opbench.Session {
	tb.Helper()
	ctx := context.Background()
	purpose := "bench-" + p.Op
	topic := purpose
	if h.TopicName != nil {
		topic = h.TopicName(purpose)
	}
	need := testkit.Need{Topic: topic, Subscriptions: []string{"bench"}}
	client, err := h.Open(ctx, need)
	if err != nil {
		tb.Fatalf("overhead: opening a client for %s: %v", p.Op, err)
	}
	pub, err := client.Publisher(ctx, mq.PublisherConfig{
		Topic: need.Topic,
		// Stated, not inherited: this is the boundary every
		// BenchmarkNative baseline documents for itself.
		Confirmation: mq.ConfirmAccepted,
	})
	if err != nil {
		_ = client.Close()
		tb.Fatalf("overhead: opening a publisher for %s: %v", p.Op, err)
	}
	con, err := client.Consumer(ctx, mq.ConsumerConfig{
		Topic:        need.Topic,
		Subscription: need.Sub(0),
		// Start is left unset, which resolves to Latest, and the consumer is
		// opened BEFORE anything is published -- which is what every
		// BenchmarkNative baseline does (nats subscribes in its own `open`).
		// StartEarliest is not portable across these profiles: it needs a
		// retained log, so it is mq.ErrUnsupported on NATS Core and on the
		// memory driver's default queue topology, and a leaf that asked for
		// it would not open at all. `testkit.RunBenchmarks` does ask for it,
		// which is why its benchmarks cannot run on an unretained profile
		// (recorded in the maintenance notes).
	})
	if err != nil {
		_ = pub.Close()
		_ = client.Close()
		tb.Fatalf("overhead: opening a consumer for %s: %v", p.Op, err)
	}
	return &session{
		client: client,
		pub:    pub,
		con:    con,
		one:    &mq.Message{},
		dst:    make([]mq.Delivery, 64),
		settle: client.Capabilities().Has(mq.FeatureExplicitAck),
	}
}

func (s *session) Publish(ctx context.Context, payload []byte) error {
	s.one.Body = payload
	return s.pub.Publish(ctx, s.one)
}

func (s *session) PublishBatch(ctx context.Context, payloads [][]byte) error {
	for len(s.batch) < len(payloads) {
		s.batch = append(s.batch, &mq.Message{})
	}
	ms := s.batch[:len(payloads)]
	for i, p := range payloads {
		ms[i].Body = p
	}
	return s.pub.PublishBatch(ctx, ms)
}

func (s *session) Receive(ctx context.Context, n int) error {
	for n > 0 {
		dst := s.dst
		if n < len(dst) {
			dst = dst[:n]
		}
		// A non-nil error may accompany a non-zero count, and the deliveries
		// returned with it are valid (ConsumerHandle.Receive's contract), so
		// they are settled BEFORE the error is reported. Returning first
		// would strand a lease per delivery on the last fetch of every leaf.
		got, rerr := s.con.Receive(ctx, dst)
		if s.settle {
			for i := range got {
				if err := dst[i].Ack(); err != nil {
					return err
				}
			}
		}
		if rerr != nil {
			return rerr
		}
		// Receive blocks until at least one delivery is available, ctx is done
		// or the consumer is closed, so this cannot happen -- and if a driver
		// ever made it happen, the loop would spin for the whole 20-minute
		// test timeout instead of saying so.
		if got == 0 {
			return errNoProgress
		}
		n -= got
	}
	return nil
}

func (s *session) Close() error {
	if err := s.con.Close(); err != nil {
		_ = s.pub.Close()
		_ = s.client.Close()
		return err
	}
	if err := s.pub.Close(); err != nil {
		_ = s.client.Close()
		return err
	}
	return s.client.Close()
}
