package main

import (
	"sort"
	"strings"
)

// Every row of the benchmark configuration and §9.2 carries its own decision criterion
// in its last column -- "ns/op & p99 under MPMC contention", "adopt unsafe
// only if > 10% gain", "interface unless > 5%", "read-heavy vs churn
// workloads" -- and `bakeoff decide` used to apply one rule to all
// thirty-eight of them: fastest geomean, with a 3% simplicity tie-break. A
// row that says "only if it wins by more than 10%" was decided by a 3% rule,
// and a family whose arms are two BUILDS of one kind set was handed a winner
// that is a fact about another family.
//
// This table is where a family declares its rule. It lives beside the scorer
// rather than in hack/bakeoff/<family>.env because `make ci` can then hold
// the two to each other: TestEveryDeclaredFamilyHasADecisionRule fails on a
// family the campaign measures and this table cannot decide, and `score`
// refuses a family that has no row here rather than substituting a rule of
// its own.

// tieBand is §0.3(2)'s "within 3%": candidates the data cannot separate by
// more than this fraction are resolved by simplicity, not by their medians.
const tieBand = 0.03

// exclusionMargin is §0.3(3)'s "by more than 10%".
const exclusionMargin = 0.10

// decisionRule is one §9 row's criterion in the terms the scorer evaluates.
//
// The zero value is §0.3(1)+(2) alone: the lowest geomean of ns/op over the
// whole measured workload, with candidates the interval cannot separate by
// more than 3% resolved by complexity. Every field narrows or replaces one
// part of that with what the row itself says.
type decisionRule struct {
	// Criterion is the row's last column, quoted, so that the scorecard and
	// the ADR state the rule they were decided by in the specification's own
	// words rather than in the tool's.
	Criterion string

	// Metric is the primary metric the geomean is taken over; "" is ns/op.
	// A row whose criterion is a latency percentile (A3) is decided on that
	// percentile, and a run that did not record it is refused rather than
	// decided on throughput instead.
	Metric string

	// Workload selects the rows the decision is taken over. Each entry is one
	// selector, and a row must satisfy every one of them; a selector matches
	// when any of its "|"-separated alternatives equals one segment of the
	// row's name. A required selector that matches no measured row is an
	// error: a criterion that names a shape the run did not measure cannot be
	// applied to it.
	Workload []string
	// WorkloadOpt is the same, for a shape the default benchmark matrix does
	// not measure (IQMQ_BENCH_MATRIX widens several of them). A selector that
	// matches nothing is dropped with a note rather than refused.
	WorkloadOpt []string
	// WorkloadWhy explains the narrowing on the scorecard.
	WorkloadWhy string

	// Report names extra workload slices whose own best kind is reported as a
	// note, for a row whose criterion names more than one workload ("read-heavy
	// vs churn"). It changes no decision.
	Report []string

	// Baselines are kinds measured for comparison that can never be the
	// default: §9.1's "(baseline)" arms (A4's `time.Timer` per message, B6's
	// driver-direct call). They are excluded from candidacy and from the
	// coverage computation, and reported with their own disposition.
	Baselines []string

	// NotDefault names kinds that are full arms of the family -- measured,
	// scored, and covering the deciding workload -- which nevertheless cannot
	// be the default, with the reason. Unlike Baselines they stay in the
	// coverage computation, because they do cover the workload; what they
	// cannot do is win. §0.3 puts the correctness gate before the numbers, so
	// an arm whose cost is a capability the default must keep is reported with
	// its numbers and passed over. Family A2's `logcursor` is the case: it
	// shares one log per partition, and `Engine.CreateTopic` therefore refuses
	// the stream profile, wildcards, priority and per-subscription capacity
	// , so binding it would take those away from the engine
	// a caller gets from `New(Config{})`.
	NotDefault map[string]string

	// Incumbent holds the default unless a challenger beats it by Margin.
	// Margin 0 means any win the interval establishes is enough ("unless
	// beaten"); Margin 0.30 is A20's "stdlib unless > 30% gain".
	Incumbent string
	Margin    float64

	// Fixed marks a row whose default §9.1 states outright -- the others are
	// opt-in or all shipped -- so the measurement reports overhead and does
	// not choose. The winner is Incumbent.
	Fixed bool

	// ZeroAllocs marks a row that requires zero allocations of its default. A
	// candidate that allocates is excluded while any candidate does not; if
	// none reaches zero the criterion is recorded as unmet rather than
	// silently dropped.
	ZeroAllocs bool
	// AllocsFirst marks a row decided on allocations first and time second.
	AllocsFirst bool

	// MaxMetric excludes a candidate whose worst row exceeds a ceiling the
	// row states (A3's "≤ 1% idle CPU"), on the same terms as ZeroAllocs.
	MaxMetric map[string]float64

	// Unobservable lists the parts of the criterion this tool cannot
	// evaluate. They are recorded on the scorecard and printed in the ADR, so
	// that a record never implies a check that no data could support.
	Unobservable []string

	// Note is anything else a reader of the scorecard needs.
	Note string

	// Undecidable is non-empty for a row whose rule the scorer cannot express
	// at all. The family is still measured and still gets a scorecard -- the
	// numbers are what another record is computed from -- but no winner is
	// named and nothing is bound.
	Undecidable string
}

// rules is the §9.1/§9.2 table, one row per family the campaign measures.
var rules = map[string]decisionRule{
	"A1": {
		Criterion:   "ns/op & p99 under MPMC contention, 0 allocs → seq-MPMC expected; mutex ring retained as `Safe`",
		Workload:    []string{"MPMC"},
		WorkloadWhy: "§9.1 row A1 decides the memory engine's lane queue under MPMC contention. The SPSC and MPSC specialisations cannot run the multi-producer shapes at all, so they are not candidates for the default and are retained as separately selectable kinds.",
		ZeroAllocs:  true,
	},
	"A2": {
		Criterion:   "throughput at 16 subs *and* slow-subscriber isolation → per-sub envelopes expected",
		WorkloadOpt: []string{"16"},
		WorkloadWhy: "§9.1 row A2 names the 16-subscriber fan-out as its throughput criterion.",
		// §8.4, verbatim: "FullCopy and channel-based fan-out are required
		// baselines [→ A2]". They are measured for comparison, not to win.
		Baselines: []string{"fullcopy", "chanfanout"},
		NotDefault: map[string]string{
			"logcursor": "shares one sequence-numbered log per partition, so `Engine.CreateTopic` refuses the stream profile, wildcards, priority and per-subscription capacity (assumption A15.4); binding it would take those away from `New(Config{})`, and §0.3 puts the correctness gate before the numbers",
		},
		Unobservable: []string{"slow-subscriber isolation, which is a stress scenario (§9.4 step 3) and not a benchmark metric"},
	},
	"A3": {
		Criterion:    "p99 under load with ≤ 1% idle CPU → hybrid/eventcount; separate defaults for low/high contention allowed",
		Metric:       "p99-ns",
		MaxMetric:    map[string]float64{"cpu-pct": 1},
		Unobservable: []string{"§9.1 row A3 allows separate defaults for low and high contention; this tool names one"},
	},
	"A4": {
		Criterion:    "insert/fire ns & B/entry at 1e6 → hierarchical wheel; heap for sparse",
		Workload:     []string{"insert|fire"},
		WorkloadOpt:  []string{"n=1000000"},
		WorkloadWhy:  "§9.1 row A4 decides on insert and fire cost at a million entries; cancel is measured and reported.",
		Baselines:    []string{"stdtimer"},
		Unobservable: []string{"§9.1 row A4's second default (\"heap for sparse\") is a second workload's answer; this tool names one"},
	},
	"A5": {
		Criterion: "ns/op with acceptable distribution → xxh3/wyhash",
		// BenchmarkHash/Lane and BenchmarkHash/KafkaPartition sit at the same
		// depth as the kinds and are not kinds: they measure the range
		// reduction alone and the Kafka partitioner, each over one row. Left
		// as candidates they are two kinds covering one row of seven, which
		// makes the family undecidable; left in the coverage computation they
		// would narrow the decision to that one row.
		Baselines:    []string{"Lane", "KafkaPartition"},
		Unobservable: []string{"χ² lane distribution and collisions on real key corpora, which are correctness gates and not metrics"},
	},
	"A6": {
		Criterion:    "throughput + GC pause; adopt only if measurably better → free list for reuse mode",
		Incumbent:    "none",
		Margin:       0,
		Unobservable: []string{"GC pause and cycle counts"},
		Note:         "\"adopt only if measurably better\" is read as: the GC-only arm keeps the default unless the interval establishes a win, whatever its size.",
	},
	"A7": {
		Criterion: "correctness first; overhead ≤ 5% → Atomic default, others opt-in",
		Fixed:     true,
		Incumbent: "atomic",
		Note:      "§9.1 row A7 fixes the default on correctness and makes the others opt-in, so the measurement reports each policy's overhead and does not choose.",
	},
	"A8": {
		Criterion:    "throughput under Zipf with ordering preserved → hash lanes + stealing for keyless",
		Unobservable: []string{"ordering preservation, which is the correctness gate of §9.4 step 1", "§9.1 row A8's second default for keyless dispatch"},
		// One arm is measured, scored, covers the deciding workload, WINS the
		// criterion and cannot be the default. It is family A32's shape
		//  and A2's and A16's before it: the default is the
		// best arm that does not take a guarantee out of the configuration a
		// caller gets by not choosing.
		//
		NotDefault: map[string]string{
			"perpartition": "the arm wins §9.1 row A8's throughput criterion and cannot be the default, because the number of lanes it can use is the number of partitions the SOURCE reports rather than the number of workers the runner was configured with. It routes a delivery to a lane by its partition and does no cross-lane work at all, which is why it is the fastest arm here -- and the memory engine's default topology is ONE partition (`TopologyConfig{Partitions: 1}`), as is every driver profile whose source reports one, so bound as the default it makes `ConsumerHandle.Run` single-threaded whatever `MaxInFlight` says. Measured with the binding in place: `TestRunByKeyRunsDistinctKeysConcurrently` reports \"no two deliveries were ever handled at the same time with eight workers and thirty-two keys\", and `TestRunExtendsWithoutATimerPerDelivery` and `TestRunAgainstMemoryDrainsBeforeItReturns` time out with leases outstanding because the deliveries queue behind the one lane; all three pass with the arm unbound. That is §10.1's correctness precondition -- a benchmark may not select an implementation that violates a declared contract -- so the arm is measured, scored, reported and excluded as the DEFAULT, and remains the right choice for a consumer of a many-partition source that wants per-partition ordering without hashing.",
		},
		Note: "§9.1 row A8 expects \"hash lanes + stealing for keyless\" and the re-measurement expects neither: `perpartition` is the fastest arm and is excluded as a default because its lane count is the source's partition count, and `hashlanes` -- the expected winner and the reference -- is 16.1 % behind `perpartition` and 8.8 % behind `chanperlane`. The default is `chanperlane`, the fastest arm that gives every worker a lane of its own. The exclusion is §0.3's rule and not a thumb on the scale: the numbers are in the table, and a consumer of a partitioned source should set `perpartition`.",
	},
	"A9": {
		Criterion:   "ns/op at contention → striped",
		WorkloadOpt: []string{"P=8|P=32|P=64"},
		WorkloadWhy: "§9.1 row A9 decides on contended increment at 8/32/64 goroutines; the wider sweep needs IQMQ_BENCH_MATRIX=full.",
	},
	"A10": {
		Criterion: "adopt unsafe only if > 10% gain (ADR); default pure Go",
		// The same deciding workload as A1, so that the two scorecards carry
		// the same row keys and ADR-021's kind-by-kind join is a join on the
		// row name.
		Workload:    []string{"MPMC"},
		WorkloadWhy: "the same rows as family A1, whose kinds these are: ADR-021 computes the per-kind gain by joining this scorecard with A1's on the row name.",
		Undecidable: "§9.1 row A10 is \"A1 on both builds\": its arms are the same queue kinds compiled twice, " +
			"with and without -tags iqmq_unsafe, and the rule is a comparison BETWEEN two runs. " +
			"`bakeoff decide` compares kinds inside one results file, so what it could report here is which " +
			"queue kind is fastest when the unsafe slot is compiled in -- a fact about family A1. " +
			"ADR-021 joins benchmarks/reports/scorecards/A1.json and A10.json on the row name and applies " +
			"the > 10% rule there (assumption A24.30).",
	},
	"A11": {
		Criterion: "throughput & p99 within budget, rebalance correctness -> franz-go expected; loser demoted to community",
		// The arms are two client LIBRARIES, so there is no reference
		// implementation whose simplicity could break a tie and no `Auto`
		// constant to bind: `hack/bakeoff/A11.env` sets AUTO_FILE=- for the
		// same reason family A10 does. What the scorecard records is which
		// client costs less per message, over every operation and payload
		// size §9.1 row A11 names, and ADR-013 is where that becomes a
		// decision about a driver's TIER (§7.1).
		Incumbent: "franz",
		// "franz-go expected" is an expectation and not a fixed default, so
		// the margin is the §0.3(3) exclusion band: the incumbent keeps the
		// row unless the challenger wins by more than ten per cent. A Kafka
		// client is not a data structure -- swapping the canonical one moves
		// a capability table, a DSN scheme and a conformance report -- so a
		// three-per-cent tie-break would be the wrong size of evidence for
		// the size of the change.
		Margin: exclusionMargin,
		Report: []string{"publish", "batch", "consume", "endtoend"},
		Unobservable: []string{
			"rebalance correctness, which is a correctness gate (§9.4 step 1) and not a metric: the five critical scenarios of plan rows B04 and B05, which both drivers pass and whose FIRST scenario they answer DIFFERENTLY -- kafka-go has no cooperative assignor, so a rebalance invalidates the whole assignment where franz-go keeps the partitions that did not move",
			"the p99 against a container, the rebalance storm and the reconnect chaos, which this family measures against an in-process kfake cluster and therefore does not produce; hack/bakeoff/A11.env declares each of them in CRITERION_GAP with the command that does",
			"\"loser demoted to community\" is a statement about a driver's §7.1 tier and its page, which no measurement makes and ADR-013 records",
		},
		Note: "",
	},
	"A14": {
		Criterion:    "≤ 20 ns ack, bounded memory → bitset (segmented for wide gaps)",
		Unobservable: []string{"§9.1 row A14's second default (segmented, for wide gaps) is a second workload's answer"},
		Note:         "",
	},
	"A13": {
		Criterion: "on Artemis and Classic; feature coverage score (delay, groups, priority, TTL) -> feature completeness on Artemis, then perf -> AMQP 1.0 default; STOMP kept for Classic",
		// This row is decided by its FIRST criterion term and not by its
		// second. "Feature completeness on Artemis, then perf" says the
		// order outright, and the completeness half is settled before any
		// measurement: on Artemis the AMQP 1.0 arm covers 19 of §4.8's 39
		// features natively against STOMP's 15, and on Classic 16 against
		// 13. So the arm a caller gets by not choosing is the more complete
		// one, whatever the mapping costs.
		Fixed:     true,
		Incumbent: "amqp10",
		Margin:    0,
		// The decision is taken over the whole measured workload. Narrowing
		// it to the Artemis rows was the tempting reading of "completeness on
		// Artemis" and it would have been wrong twice over: `features` and
		// `min-broker` are reported per broker on every row, so the Artemis
		// half of the criterion is already IN the data rather than in the row
		// selection, and dropping the Classic rows would throw away the half
		// of the perf term where the AMQP 1.0 arm's mapping does different
		// work (the delay moves from the annotation section to the
		// application properties, and a de-duplication key is refused rather
		// than carried).
		WorkloadWhy: "§9.1 row A13's criterion is read on Artemis AND Classic, and both brokers are benchmark-name segments rather than separate runs, so the decision is taken over every row: two directions of the mapping at two header counts on each of the two products. The Artemis half of the criterion is carried by the per-row `features` and `min-broker` metrics.",
		Report:      []string{"artemis", "classic", "carry", "restore"},
		NotDefault: map[string]string{
			"stomp": "loses four rows of §4.8 on BOTH brokers, and the first of them is dangerous rather than merely absent. (1) A STOMP NACK frame returns NO message on either broker: measured on Artemis, one NACK and one ACK took `getMessagesAcknowledged` from 0 to 2, the queue emptied, the dead-letter queue did not grow and a re-subscribe saw nothing -- the NACK was treated exactly as an ACK -- so a driver that mapped `Nack` to it would SILENTLY DESTROY messages while reporting a requeue; on Classic a NACK is a POISON_ACK and cannot be transacted at all (\"Poison ack cannot be transacted\", which arrives as an ERROR frame and closes the connection). The AMQP 1.0 arm's `modified{delivery-failed}` disposition returns the delivery and raises `delivery-count`, measured as 0, 1, 2 across three rounds on both brokers. (2) No STOMP delivery carries an attempt COUNT on either broker -- measured: no `delivery-count`, no `JMSXDeliveryCount`, no `_AMQ_DELIVERY_COUNT`, and the boolean `redelivered` header is not a substitute because the redelivery a requeue produces arrives with `redelivered: false` on Artemis -- so §5.6's Attempt is ZERO, its documented value for \"the broker does not count attempts\", and a MaxAttempts policy above that driver has nothing to count with, against a real count on the other arm. (3) A frame header is TEXT, so a binary header value and a binary message key cannot be carried at all. (4) The receive window is a broker-specific SUBSCRIBE header rather than the link's own credit. It ships anyway and §7.2 asks for it (\"protocol choice explicit\"), for the one row it WINS: a `selector` header filters broker-side on Classic (measured: a `colour = 'red'` selector kept a blue message out), where no AMQP 1.0 subscription filter is reachable through go-amqp -- and that row is itself ARTEMIS-NEGATIVE, because the selector is measurably ignored there when the subscription is a fully qualified queue name, which is the only shape this driver uses on Artemis.",
		},
		Unobservable: []string{
			"throughput against the real brokers, which needs two brokers and two client libraries; conformance/suites runs the same §11.2 suite through both drivers against apache/activemq-artemis:2.44.0-alpine and apache/activemq-classic:6.2.0, and conformance/matrix exercises the four version floors",
			"the feature coverage score's own truth: `features` is computed from Protocol.Support rather than measured, and the four conformance suites are what prove each row in both directions",
			"the four broker behaviours the capability tables rest on -- a STOMP NACK returning nothing, an Artemis `modified` disposition ignoring `x-opt-delivery-delay`, a `selector` being ignored under an FQQN subscription, and no STOMP delivery carrying an attempt count. Each is a fact about a running broker that no microbenchmark can produce; the critical scenarios of drivers/activemq and drivers/activemq/stomp are where they are asserted against the pinned images",
			"the relative cost of the two arms' mappings as a comparison between PROTOCOLS: the STOMP arm validates every header value as UTF-8 and every header name against the frame's own, because its Carry IS its driver's mapping, and the AMQP 1.0 arm's typed sections need neither. A faster STOMP row is evidence that it checked less, which is why the row puts completeness first",
		},
		Note: "Both arms ship, so the \"winner\" is which protocol a caller gets by not choosing rather than which one is deleted: §7.2 lists `drivers/activemq` (AMQP 1.0 via go-amqp) and `drivers/activemq/stomp` (go-stomp/v3) with \"protocol choice explicit\", and `internal/activemq/protocol`'s AutoKind is what `New(Auto)` answers with. `features` and `min-broker` are reported on every row, per BROKER, so the scorecard carries the whole of the row's criterion -- which is four numbers and not one, because \"on Artemis and Classic\" makes the family a decision per product as well as per protocol. Both arms allocate nothing in either direction on either broker (TestAllocs), so allocs/op separates nothing here and the perf term is ns/op alone.",
	},
	"A12": {
		Criterion: "throughput/connection & allocs -> rueidis expected",
		// The whole measured workload is the deciding one: every row is a
		// command a Redis Streams driver sends, and "throughput per
		// connection" is not a property of one of them. Narrowing to a
		// single operation would decide the family on a corner of itself,
		// which is the mistake §0.3 and assumption A24.30 are about.
		WorkloadWhy: "§9.1 row A12's criterion is throughput per connection and allocations over the driver's own command mix, so the decision is taken over every row: one publication, three pipelining depths, two fetch sizes, the 10,000-entry reclaim sweep and the blocking read.",
		// Allocations are half the criterion, and they are read as a tie
		// break rather than as a gate: BOTH arms allocate per command by
		// construction -- go-redis takes a command as `...any` and boxes
		// every token, rueidis appends to a pooled builder -- so a
		// zero-allocation requirement would exclude the whole family and
		// decide nothing. AllocsFirst would make the family a count of
		// boxes; the row says "throughput/connection & allocs", in that
		// order.
		Report: []string{"publish", "pipeline", "consume", "reclaim", "block"},
		Note:   "Both arms ship: §7.2 asks for two Redis drivers over two client libraries, so this family's winner is which one docs/drivers/redis.md recommends and which one the broker-version matrix is run against, not which one is removed. `AutoKind` in benchmarks/redisclient/auto.go records it and binds nothing at run time, because a caller chooses a client library by which module it imports.",
		Unobservable: []string{
			"a real network's latency: the family is measured against an in-process server behind internal/streams/respsrv, so every ns/op is a loopback round trip and only the ratio between two arms measured the same way is read",
			"a cluster's routing cost: both adapters state their commands' key positions and the unit tests hold them to it, but a multi-node cluster is not in the measurement",
		},
	},
	"A15": {
		Criterion: "throughput vs failure rate 0.1/1/10%; ordering violations; lag → none silently chosen; default block-partition under `SafeOnly`",
		// ticks/record is the reference simulation's virtual time per record,
		// which is the throughput the row names. ns/op is what `Strategy.Plan`
		// costs to call and is not this row's subject: the interesting cost of
		// a nack strategy is what it does to the partition -- three of the
		// four kinds do something quite different with it -- and Plan is a
		// pure function either way (hack/bakeoff/A15.env).
		Metric:      "ticks/record",
		Workload:    []string{"failrate=0.1|failrate=1|failrate=10"},
		WorkloadWhy: "§9.1 row A15 states its own three failure rates, 0.1/1/10%, and is decided at them; the `plan` rows measure the decision alone and are reported beside them.",
		Report:      []string{"failrate=0.1", "failrate=1", "failrate=10"},
		// "none silently chosen; default block-partition under `SafeOnly`" is
		// the row's default column, and it fixes the default outright: a
		// campaign that finds retry topics three times cheaper does NOT move
		// `nack.AutoKind`, because the cheaper kind buys its throughput with
		// the partition's order. So the measurement reports what each kind
		// costs and what each kind gives up, and chooses nothing.
		Fixed:     true,
		Incumbent: "blockpartition",
		Unobservable: []string{
			"\"none silently chosen\", which is a property of the configuration surface and not a metric: `nack.New` refuses a kind it was not given, `Kind` is a typed field with no string parsing, and `AutoProvisional` keeps the generated binding marked provisional until ADR-013 is ratified",
			"ordering violations (reorder/record) and lag, which the benchmark does report at every failure rate: the scorer takes its geomean over one metric, and since this row's default is fixed they enter the record as the margin a caller pays for order rather than as a selection",
		},
		Note: "§9.1 row A15 fixes the default, so this scorecard is a cost record and not a choice: block-partition retry is the default under `SafeOnly` because it is the only kind that finalises a partition's records in offset order, which is the one thing a Kafka offset commit cannot express twice.",
	},
	"A16": {
		Criterion: "leave-pending default; zset for delays (redelivery latency accuracy; PEL growth; duplicates under crash)",
		// The row is decided on the REFERENCE arm unless a challenger beats
		// it, and the margin is zero because the criterion names
		// leave-pending outright. That is not a thumb on the scale: the three
		// terms §9.1 decides on are not benchmark metrics at all, and the arm
		// that wins each of them is already known from the implementation
		// rather than from a measurement.
		//
		//   redelivery-latency accuracy -- only the zset arm honours an
		//     arbitrary delay, and the other three refuse one. That is a
		//     correctness property (TestModel/<kind>/delay-honoured-or-
		//     refused), not a number.
		//   PEL growth -- leave-pending and stop hold one pending entry per
		//     in-flight message and the re-adding arms hold none. That is
		//     arithmetic from the arm's definition.
		//   duplicates under crash -- every arm's write is one Lua script, so
		//     none of them has a window; the arms that send NOTHING have no
		//     window by construction. FuzzRedisReclaimer is the evidence.
		//
		// What the benchmark measures is the remaining question: what an arm
		// costs when it is not deciding anything. `cmds/op` is the headline
		// there, because a round trip on this broker is tens of microseconds
		// against a few hundred nanoseconds of plan building, and
		// leave-pending sends zero commands where two of the others send one.
		Incumbent: "leavepending",
		Margin:    0,
		// Three of the four arms are full arms -- measured, scored, covering
		// the deciding workload -- and cannot be the DEFAULT, each for a
		// reason §0.3 puts before the numbers. It is family A2's shape
		// : the default is the best arm that does not take
		// a guarantee out of the configuration a caller gets by not choosing.
		//
		// Without this the family is decided on noise: at delay=0 every arm
		// builds the same one command or none, so the medians sit within a
		// few hundred nanoseconds of each other and whichever arm the run
		// happens to favour wins. The dry run picked `stop` on one sample per
		// row, and `stop` is the arm that stops a shard on one failing
		// message.
		NotDefault: map[string]string{
			"stop":      "the shard is held until the failed entry is settled, so one message whose handler always fails stops its whole shard until §4.5's MaxAttempts dead-letters it. That is the correct behaviour for a strictly ordered stream and a liveness failure for a work queue, so it is a configuration a caller chooses rather than the one they get by not choosing.",
			"readd":     "the entry is acknowledged and added back under a NEW identifier, so a retried message is delivered after everything published since it failed. §4.4's per-key ordering would then not survive a retry in the default configuration, which is the same refusal §8.2 and §8.3 make of family A2's shared log.",
			"zsetmover": "the entry is acknowledged and parked in a sorted set, which reorders it (as `readd` does) and additionally adds a key and a mover goroutine per shard. §9.1 names it the answer for DELAYS, which is a caller's requirement rather than the default's: it is the only arm that honours an arbitrary NackOptions.Delay, and the capability table advertises the delay only when it is bound.",
		},
		Workload:    []string{"requeue|drop|deadletter", "delay=0"},
		WorkloadWhy: "the three §4.3 actions are what a negative settlement can be, at delay=0 -- the only delay every arm accepts. The delay sweep is deliberately NOT part of the deciding set: three of the four arms refuse a delay outright, so the zset mover's delay=1m rows exist for it alone and including them would leave it spanning eight rows against the others' six, which is a family decided on a corner of itself.",
		Report:      []string{"requeue", "deadletter"},
		Unobservable: []string{
			"redelivery-latency accuracy, which is a correctness property of the arm (only the zset mover honours an arbitrary delay) rather than a measurement",
			"pending-entries-list growth, which follows from the arm's definition rather than from a benchmark",
			"duplicates under crash, which FuzzRedisReclaimer and the arms' Lua atomicity establish",
		},
		Note: "§9.1 row A16 names leave-pending as the default and the zset mover as the answer for delays, so the arms are not orderable on one metric: the default is the cheapest arm that preserves the entry's position and its delivery counter, and the delayed arm is the only one that can delay. The benchmark decides nothing on its own and reports what each arm costs in commands and allocations per settlement.",
	},
	"A17": {
		Criterion: "delay accuracy; broker CPU; queue count growth -> plugin when detected, TTL+DLX fallback",
		// §9.1 fixes the outcome independently of the measurement, and the
		// reason is what Auto MEANS: it is the configuration a caller gets by
		// not choosing. The delayed-message exchange is a community plugin
		// and is not part of the official RabbitMQ image, so a default that
		// assumed it would fail at open against a stock broker -- which is
		// worse than being slower. The plugin arm is therefore a full arm,
		// measured and scored and better on every metric here, that cannot
		// be the default; the driver's own detection is what selects it.
		Fixed:     true,
		Incumbent: "ttldlx",
		Margin:    0,
		NotDefault: map[string]string{
			"plugin": "needs rabbitmq_delayed_message_exchange, which is not part of the official RabbitMQ image (hack/images/rabbitmq-delayed builds one that has it) and which only supports the RabbitMQ minor it was built for. It is better on all three of §9.1's terms -- an exact delay, no delay queues at all and one broker hop instead of two -- and it cannot be what a caller gets by not choosing, because a client configured with it against a stock broker fails at open. drivers/rabbitmq detects the exchange type and upgrades to it.",
		},
		Unobservable: []string{
			"broker CPU, which needs a broker under load and a way to attribute its Erlang scheduler time to a mechanism; hops/plan is reported as the proxy",
			"the delayed-message plugin's behaviour across a broker restart, which its own documentation describes and no benchmark here reaches",
		},
		Note: "Both arms ship. What the campaign records is the margin a deployment WITHOUT the plugin pays: the rounding error of the bucket ladder, the queue count it needs, and the second broker hop. A deployment whose delays are known exactly removes the first two by naming them in delay.Config.Ladder.",
	},
	"A18": {
		Criterion:    "decode ns/op & 0 allocs with caller buffer → TLV (forward-compat)",
		Workload:     []string{"Decode"},
		WorkloadWhy:  "§9.1 row A18 decides on decode cost; Encode and Size are measured and reported.",
		ZeroAllocs:   true,
		Unobservable: []string{"forward compatibility, which is what the row's tentative default turns on"},
	},
	"A19": {
		Criterion:    "ns/op & sortability → UUIDv7",
		Unobservable: []string{"sortability and the 1e7 collision test, which are correctness properties"},
	},
	"A20": {
		Criterion:    "stdlib unless > 30% gain with zero-dep preserved",
		Incumbent:    "json",
		Margin:       0.30,
		Unobservable: []string{"preservation of the zero-dependency property"},
	},
	"A21": {
		Criterion:    "lowest peak, fast recovery → full jitter",
		Unobservable: []string{"peak concurrency and time-to-recover in the retry-storm simulation, which the benchmark does not report"},
	},
	"A22": {
		Criterion:  "0-alloc callback default; inline only if documented and never under locks",
		ZeroAllocs: true,
		// This row cannot be decided as its benchmark is named, and the
		// refusal is the right answer rather than a rule that papers over it.
		// §9.1's arms are "callback (inline vs deferred) · future/handle ·
		// per-call channel adapter": a {callback,future,channel} ×
		// {inline,deferred} grid. The benchmark expresses the callback arm's
		// two timings as two KINDS (`inline`, `deferred`, whose rows are
		// `BenchmarkPublish/Async/<kind>/inflight=<n>`) and the other two
		// arms' timings as a DIMENSION (`.../future/{inline,deferred}/
		// inflight=<n>`). With the kind segment removed the two groups have
		// no row name in common at all, so there is no workload two arms
		// share and nothing to take a geomean over.
		Note:         "§9.1 row A22's arms are a {callback,future,channel} × {inline,deferred} grid, and the benchmark spells the callback arm's two timings as kinds and the other arms' as a dimension, so the four kinds' rows are disjoint. The fix is in the benchmark's names, not here.",
		Unobservable: []string{"\"inline only if documented and never under locks\", a review condition on the winner and not a measurement"},
	},
	"A23": {
		Criterion:    "throughput vs linger; timer count; low-rate latency → shared ticker expected",
		Unobservable: []string{"timer count", "low-rate latency"},
	},
	"A25": {
		Criterion:    "allocs/op; timer count → Clock-based",
		AllocsFirst:  true,
		Unobservable: []string{"timer count"},
	},
	"A26": {
		Criterion: "bind default via build tag if > 5% cost",
		Incumbent: "interface",
		Margin:    0.05,
		// §10.5 names three arms and §9.1 row A26 two: `native` is this
		// package's own ring with no package boundary in the way, which is
		// the floor the two dispatch mechanisms are measured against and not
		// a mechanism anything could be bound to. Reading it as a candidate
		// reads the absence of the dispatch layer as a cheaper
		// implementation of it , and it wins by
		// construction.
		Baselines: []string{"native"},
		Note:      "§9.1 row A26 keeps the interface unless the indirection costs more than 5%; §10.5's third arm, `native`, is the no-boundary floor and not a candidate.",
	},
	"A27": {
		Criterion: "read-heavy vs churn workloads → sharded/COW expected",
		Report:    []string{"readheavy", "churn"},
		Note:      "",
	},
	"A28": {
		Criterion: "correctness for strict ordering, then latency",
		Note:      "a placement that cannot preserve strict ordering fails the family's model test and is disqualified by §9.4 step 1, so latency decides among the rest.",
	},
	"A29": {
		Criterion: "all shipped; safe default",
		Fixed:     true,
		Incumbent: "safe",
		Note:      "the copy mode is chosen per publication, not bound to an Auto constant; §9.1 row A29 ships all four and makes safe the default.",
	},
	"A30": {
		Criterion:    "≤ 30 ns/msg, 0 allocs → closure chain unless beaten",
		Workload:     []string{"enabled"},
		WorkloadWhy:  "the disabled arms measure the cost of a middleware nobody enabled and must be indistinguishable from a bare handler call; the enabled arms are what the row is decided on.",
		Incumbent:    "closure",
		Margin:       0,
		ZeroAllocs:   true,
		Unobservable: []string{"the ≤ 30 ns/msg ceiling is benchmarks/budgets.yaml's, enforced by `make gate`, not a selection rule"},
	},
	"A31": {
		Criterion: "ns/op under contention; simpler wins ties",
	},
	"A32": {
		Criterion: "cost, ack latency, shutdown correctness \u2192 hybrid (\u2264 100 ms flush) expected",
		// roundtrips/settlement is the cost the service charges for: one
		// DeleteMessageBatch call carries up to ten receipt handles, so the
		// arms span a factor of ten and nothing else about them does. ns/op is
		// reported beside it and is not the criterion -- `Submitted` is two
		// integer comparisons in every kind (hack/bakeoff/A32.env).
		Metric:      "roundtrips/settlement",
		Workload:    []string{"rate=1|rate=10|rate=100"},
		WorkloadWhy: "the arrival rate is settlements per ticker interval, and it is the only dimension that separates the four kinds: at one per interval every kind but immediate costs one call per settlement, and at a hundred the two count-bearing kinds cost one per ten. The `decide` rows measure the policy's own code and are reported beside them.",
		Report:      []string{"rate=1", "rate=10", "rate=100"},
		Incumbent:   "immediate",
		Margin:      0.10,
		// Two of the four arms are full arms -- measured, scored, covering the
		// deciding workload -- and cannot be the DEFAULT, each for a reason
		// §0.3 and §10.1 put before the numbers. It is family A2's and A16's
		// shape : the default is the best arm that does not
		// take a guarantee out of the configuration a caller gets by not
		// choosing.
		//
		// Without this the family decides on the cheapest arm, which is
		// `count`: it makes exactly one call per ten settlements at every rate,
		// because it never sends a partial batch at all. That is also what
		// makes it unusable as a default.
		NotDefault: map[string]string{
			"hybrid": "§9.1 expected this arm and the measurement agrees about its COST -- 0.2 requests per settlement against the reference's 1.0 -- and it cannot be the default, because a batching settler only coalesces settlements that ARRIVE TOGETHER. A caller that settles in a loop (the shape §11.2's own `collectAck` has, and the shape a single-threaded consumer has) never fills a batch, so every settlement waits a whole SettleInterval and the consumer settles at one message per interval. Bound as the default it made §11.2's `publish/fidelity` fail at 592 of 2,000 messages with ErrLeaseLost on both SQS profiles against the pinned container, and `settle/stale-rejection` fail too: the deliveries already fetched sat unsettled while their visibility windows ran out. That is §10.1's correctness precondition -- a benchmark may not select an implementation that violates declared delivery semantics -- so the arm is measured, scored, reported and excluded as the DEFAULT, and remains the right configuration for a consumer whose handlers settle concurrently (assumption B08.22).",
			"count":  "a settlement waits for a batch that may never fill: nine settlements followed by silence are held until something unrelated flushes the queue, and `Ack` does not return until the delete has happened. §4.11 requires every operation to be bounded, and this arm bounds a settlement's latency by the ARRIVAL RATE rather than by anything the configuration names. It is the cheapest arm at every rate and the only one whose worst case is unbounded.",
			"time":   "the arm bounds neither the queue nor the cost. Between two ticks every arrival is queued and nothing sends, so what it holds is the arrival rate times the interval -- `Policy.MaxHeld` reports -1 for it and for nothing else -- which is §10.1's \"unbounded memory under bounded configuration\" in the one place the policy could have bounded it and does not. At low rates it also costs exactly what `immediate` costs and pays a full interval of latency for it.",
		},
		Unobservable: []string{
			"shutdown correctness, which is a property of the MECHANISM rather than of any policy: driverkit.BatchSettler.Close takes the closed flag and the queue under one lock, so a settlement either lands before the drain and is sent or is refused with ErrClosed. What a policy decides is how much is in that queue when Close runs, which the benchmark does report as `held`",
			"\"none silently chosen\", which is a property of the configuration surface and not a metric: flush.New refuses a kind it was not given, Config.Settle is a typed field with no string parsing, and AutoProvisional keeps the generated binding marked provisional until ADR-013 is ratified",
		},
		Note: "§9.1 row A32 expects hybrid, and the measurement agrees with the expectation about COST and refuses it as a DEFAULT: 0.2 requests per settlement against the reference's 1.0, and a caller that settles in a LOOP rather than concurrently pays a whole interval per settlement, which failed two §11.2 scenarios on the default configuration. So all four arms are measured and three are excluded as defaults -- `count` and `time` because each leaves one of the cost and the latency unbounded, `hybrid` because coalescing needs concurrent arrivals it cannot require -- and the default is the reference: the one arm that holds no settlement a caller is waiting for. The three exclusions are §0.3's rule and not a thumb on the scale; the numbers are in the table and a caller whose handlers settle concurrently should set `hybrid`.",
	},
	"A33": {
		Criterion: "throughput and ambiguity accuracy -> windowed/async for batch, per-message for `Publish`",
		// §9.1 names TWO defaults for this row and `AutoKind` can only be
		// one, so the incumbent is the one that answers the call §4.2 makes
		// a promise about. `mq.Sink.Publish` has to RETURN an outcome, and
		// only a blocking strategy has one to return; with one publication
		// outstanding the `basic.ack` that resolves the call is
		// unambiguously that call's, the most a channel close can leave
		// ambiguous is one message, and a `mandatory` return needs no
		// correlation token because it has exactly one candidate. The other
		// half of §9.1's expectation -- "windowed/async for batch" -- is a
		// per-call choice rather than a default: an `mq.AsyncSink`
		// publication is where the window's throughput is worth its
		// ambiguity, and drivers/rabbitmq uses the window there and for
		// several goroutines publishing at once. A batch gets nothing from
		// it: the core runs a batch as a loop of synchronous Publish calls,
		// and each waits for its own confirm under every kind.
		Incumbent: "permessage",
		Margin:    0,
		NotDefault: map[string]string{
			"async": "reports every outcome to a callback and never blocks the publisher, which is `mq.AsyncSink`'s contract (§5.4) and not `mq.Sink.Publish`'s: a synchronous publish whose outcome arrives later on another goroutine has no outcome to return. It is the fastest arm measured here and cannot be the default, because the default has to serve the synchronous call.",
		},
		Unobservable: []string{
			"the broker's own service rate: the reference simulation's cost model is a fixed virtual round trip, so the absolute figures are virtual microseconds and only the ratio between the arms is read",
			"the correctness half of \"ambiguity accuracy\" -- that a cumulative confirm resolves exactly the right publications, that a `basic.return` makes an ack a rejection, and that a channel close is Ambiguous rather than failed -- which is TestModel and FuzzRabbitConfirm in the correctness gate",
		},
		Note: "All three arms ship and are selected per publisher. `ambiguous/close` is what the two pipelined arms pay for their throughput: zero for the incumbent, the whole window for the others.",
	},
	"A35": {
		Criterion: "supported-version compatibility, feature subset, perf -> 0-9-1 default; 1.0 conditional profile",
		// This row is decided by its FIRST criterion term and not by its
		// third. AMQP 1.0 is native to RabbitMQ only from 4.0; on 3.13 it is
		// a plugin whose feature set, address syntax and settlement
		// behaviour differ, and §7.2 says the profile is "never silently
		// substituted". So the arm a caller gets by not choosing is the one
		// every broker in the version matrix speaks, whatever the mapping
		// costs.
		Fixed:     true,
		Incumbent: "amqp091",
		Margin:    0,
		NotDefault: map[string]string{
			"amqp10": "needs RabbitMQ 4.0 or newer and is refused below it rather than falling back (the driver documentation, \"never silently substituted\"). It is a separate module a caller imports deliberately: `drivers/rabbitmq/amqp10`. Its property mapping is the cheaper of the two here -- AMQP 1.0's ttl is a duration where AMQP 0-9-1's `expiration` is a decimal string, so the older protocol allocates once per publication with a TTL -- and it loses direct reply-to, the `mandatory` return and the confirm mode that family A33 exists for.",
		},
		Unobservable: []string{
			"throughput against a real broker, which needs a broker and two client libraries; conformance/suites/rabbitmq_test.go and conformance/suites/rabbitmq_amqp10_test.go run the same §11.2 suite through both drivers and conformance/matrix/rabbitmq_test.go exercises the version floor",
			"the feature subset's own truth: `features` is computed from Protocol.Support rather than measured, and the conformance suites are what prove each row in both directions",
		},
		Note: "Both arms ship, so the \"winner\" is which protocol a caller gets by not choosing rather than which one is deleted. `min-broker` and `features` are reported on every row so that the scorecard carries the whole of §9.1's criterion and not the third of it that has a clock attached.",
	},
	"A34": {
		Criterion: "correctness, maintenance, native comparison \u2192 v2 default",
		// Fixed, because the row's own criterion is two things no benchmark
		// can report and one that is not a comparison between the arms.
		// "Correctness" is a gate and is declared unobservable below;
		// "maintenance" is a fact about the UPSTREAM module -- v1 is marked
		// deprecated by its own go.mod -- and is what decides the row
		// whatever the numbers say; "native comparison" is the driver against
		// the RAW client, which is benchmarks/native-baselines/pubsub against
		// a container and is not row-for-row comparable with this family's
		// in-process service. So the measurement reports what each generation
		// costs and does not choose (hack/bakeoff/A34.env).
		//
		// This is family B6's shape and not family A11's. A11 has a real
		// decision to take between two maintained libraries; here one arm is
		// deprecated upstream, so binding the faster one would be binding a
		// module that will stop building. B08's A32 story is the other half
		// of the same lesson: a winner measured and bound broke a §11.2
		// scenario, and correctness comes before the geomean.
		Fixed:     true,
		Incumbent: "v2",
		Unobservable: []string{
			"correctness, which is a gate rather than a metric: the arms share every line of the driver above drivers/gcppubsub/internal/psapi, and what can differ is the client library. TestModel in drivers/gcppubsub/v1compat runs one scripted sequence through both arms (at-least-once, an acknowledgement being terminal, a nack redelivering, order within an ordering key, and §4.3's answer to a second settlement); TestBothArmsAnswerOneCapabilityTable holds the two to one §4.8 table field for field; and TestTheArmsAreWireCompatible publishes through each generation and consumes through the other",
			"maintenance, which is a fact about the upstream module: cloud.google.com/go/pubsub is marked deprecated in favour of cloud.google.com/go/pubsub/v2, so the v1 arm is shipped as an experimental migration path and goes when it stops building",
			"the native comparison, which is the driver against the RAW client rather than one arm against the other: benchmarks/native-baselines/pubsub measures cloud.google.com/go/pubsub/v2 against the pinned emulator CONTAINER, and a driver number is only comparable with a baseline taken against the same service",
		},
		Note: "§9.1 row A34 ships v2 as the default and keeps the v1 adapter \"while buildable\", and the measurement reports what each generation costs. Both arms are measured against one in-process pstest service over one gRPC connection, in one binary, so the difference is the client library's and not the host's.",
	},
	"A36": {
		Criterion:    "third-party driver simplicity; core overhead",
		Unobservable: []string{"third-party driver simplicity, which the row actually turns on and which x/apibench/scoring measures as the lines a driver author writes (11 against 2 for a client that batches)"},
	},
	"A37": {
		Criterion: "stable vs high-cardinality destinations → all shipped; bound recommended",
		Fixed:     true,
		Incumbent: "bound",
		Note:      "§9.1 row A37 ships all three routing shapes and recommends the bound publisher; the measurement reports what each costs.",
	},
	"B1": {
		Criterion:    "end-to-end memory 1P/1C allocs/op & ns/op; misuse tests; usability study (lines to consume+ack)",
		AllocsFirst:  true,
		Unobservable: []string{"the usability study (lines to consume and ack), scored by x/apibench/scoring"},
	},
	"B2": {
		Criterion:    "allocs per delivery; Kafka offset-style and SQS receipt-style workloads; race stress",
		AllocsFirst:  true,
		Unobservable: []string{"race stress, which is a correctness gate", "developer-task line counts: the settlement representation is invisible in a caller's code (hack/bakeoff/B2.env)"},
	},
	"B3": {
		Criterion:    "conversion cost at each driver boundary; hashing; allocs; binary fidelity",
		AllocsFirst:  true,
		Unobservable: []string{"binary fidelity"},
	},
	"B4": {
		Criterion:    "Get/Set/Add/Delete/Clone at 0/2/8/32/128 headers; duplicates; encode determinism; driver mapping allocs",
		Unobservable: []string{"encode determinism", "§10.2's 15 ns / 0 allocs ceiling is waived for this row until it is ratified (assumption A23.14)"},
	},
	"B5": {
		Criterion:    "allocs at `Publish(ctx, m, Delay(d))`; construction cost; hot-loop overhead; DX review",
		AllocsFirst:  true,
		Unobservable: []string{"the break-even between the compiled arm's construction cost and its call site, which is a publish-time number its three siblings pay inside the timed body (assumption A23.19)", "DX review"},
	},
	"B6": {
		Criterion: "dispatch benchmark (negligible expected); API-evolution review",
		Baselines: []string{"direct"},
		// Fixed, because the row says in its own criterion that the benchmark
		// is expected to be negligible and that what it turns on is the
		// API-evolution review -- which this rule already declares
		// unobservable. The measurement bore the prediction out: `concrete`
		// 9051.9 ns and `interface` 8894.1 ns, 1.8% apart and inside §0.3(2)'s
		// three per cent band. Inside the band the rule prefers the simpler
		// arm, and `loc` is 0 for both of these packages, so the tie-break had
		// nothing to compare and crowned whichever the band happened to order
		// first -- freezing the most consequential shape in the public API on
		// a difference the row itself says is not evidence. ADR-003
		// (Accepted, structural) decided it: concrete handles implementing
		// narrow interfaces. The measurement reports what each costs.
		Fixed:        true,
		Incumbent:    "concrete",
		Unobservable: []string{"the API-evolution review, which is what the row turns on"},
		Note:         "the `direct` arm is §10.5's native baseline -- no handle, no delivery type, no settlement representation -- so reading it as a sibling would read the absence of a feature as a cheaper implementation of it (assumption A23.9).",
	},
	"B7": {
		Criterion:    "`Has()` ns; expressiveness matrix",
		Unobservable: []string{"the expressiveness matrix, which is what the row turns on: a 32-byte packed field cannot hold §4.8's note and a 1024-byte array can", "§4.8's Limits half is not prototyped by this row (hack/bakeoff/B7.env)"},
	},
	"B8": {
		Criterion:    "allocs per encode/decode; ergonomics with `typed`",
		AllocsFirst:  true,
		Unobservable: []string{"ergonomics with `typed`; §9.2's tentative default is both shapes"},
	},
	"B9": {
		Criterion:    "overhead vs `Receive`; GC pressure at batch 1/10/100/1000",
		AllocsFirst:  true,
		Unobservable: []string{"GC pressure beyond allocs/op and B/op"},
	},
	"B10": {
		Criterion:    "allocs on failure path; `errors.Is` cost",
		AllocsFirst:  true,
		Unobservable: []string{"the two matching disciplines are observationally equivalent under §4.10's own \"errors.Is matches by Code\" rule, so the row turns on a codeless sentinel and on who can discover a code"},
	},
	"B11": {
		Criterion:    "cancellation/chaos behaviour; usability",
		Unobservable: []string{"cancellation and chaos behaviour, and usability, which are what the row turns on; the benchmark measures only what each shape costs"},
	},
	"B12": {
		Criterion:    "DX review; option growth",
		Unobservable: []string{"the DX review and option growth, which are what the row turns on; the benchmark measures only what each shape costs"},
	},
}

// ruleFor returns the family's decision rule.
func ruleFor(family string) (decisionRule, bool) {
	r, ok := rules[family]
	return r, ok
}

// ruleFamilies lists the families this table can decide, for error messages
// and for the test that holds it against hack/bakeoff/*.env.
func ruleFamilies() []string {
	out := make([]string, 0, len(rules))
	for f := range rules {
		out = append(out, f)
	}
	sort.Slice(out, func(i, j int) bool { return rowLess(out[i], out[j]) })
	return out
}

// primaryMetric is the unit the geomean is taken over.
func (r decisionRule) primaryMetric() string {
	if r.Metric == "" {
		return "ns/op"
	}
	return r.Metric
}

// isBaseline reports whether slug is a comparison baseline rather than a
// candidate for the default.
// notDefault returns why a kind cannot be the default, or "" if it can.
func (r decisionRule) notDefault(slug string) string { return r.NotDefault[slug] }

func (r decisionRule) isBaseline(slug string) bool {
	for _, b := range r.Baselines {
		if b == slug {
			return true
		}
	}
	return false
}

// selects reports whether a row name satisfies one selector: any of the
// selector's "|"-separated alternatives equals one segment of the name.
// Matching on whole segments and not on substrings is deliberate -- "16"
// must select the 16-subscriber fan-out and not `batch=16`.
func selects(rowName, selector string) bool {
	segs := strings.Split(rowName, "/")
	for _, alt := range strings.Split(selector, "|") {
		for _, s := range segs {
			if s == alt {
				return true
			}
		}
	}
	return false
}

// selectsAll reports whether a row satisfies every selector.
func selectsAll(rowName string, selectors []string) bool {
	for _, sel := range selectors {
		if !selects(rowName, sel) {
			return false
		}
	}
	return true
}
