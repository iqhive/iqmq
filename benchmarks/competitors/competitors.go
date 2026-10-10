// Package competitors is the layer-5 row: iqmq measured
// against Watermill, gocloud.dev and a raw client, at pinned versions and at
// EQUIVALENT SEMANTICS.
//
//	BenchmarkCompare/{iqmq,watermill,gocloud,raw}/<family>/<op>/<size>
//
// # Equivalent semantics is the whole difficulty
//
// So this package declares the semantics of each FAMILY, configures every arm
// in it to those semantics, proves what can be proved, and excludes what
// cannot be made equivalent -- by name, with the reason, in Exclusions().
// A smaller honest table beats a complete misleading one.
//
// Three structural decisions carry most of the weight:
//
//   - **One loop.** Every arm runs github.com/iqhive/iqmq/benchmarks/opbench,
//     the same timing loop as the layer-3 rows: the three payload sizes,
//     the four operations, the 100-message batch, the 256-message prefetch,
//     the untimed refill and the round-trip warm-up. The only thing that
//     differs between two arms of a family is which opbench.Session the leaf
//     opens.
//   - **One native client, one version, one connection.** Every arm of the
//     `nats` family is handed the SAME *nats.Conn, built once per session by
//     this package, from the one nats.go version the whole repository pins.
//     Watermill and gocloud both accept an existing connection
//     (NewPublisherWithNatsConn, OpenTopicV2) and so does iqmq
//     (nats.OpenWithClient), so the comparison is not four libraries each
//     dialling with their own defaults -- it is four libraries over one
//     socket.
//   - **One confirmation boundary, stated and chosen because it is the
//     STRONGEST every arm can reach.** On NATS Core that is "the publication
//     is in the client's write buffer": gocloud's natspubsub SendBatch
//     deliberately does not flush, and watermill-nats' Publish does not
//     either, so neither can offer a flushed boundary at all. iqmq is
//     therefore configured to mq.ConfirmNone rather than its default
//     ConfirmAccepted, which would have made its arm pay a PING/PONG round
//     trip the others never pay. The native-comparison rule -- an
//     unconfirmed publication is never compared as equivalent to a
//     broker-confirmed one -- cuts both ways, and this is the direction it
//     cuts here.
//
// # What is deliberately NOT compared
//
// Exclusions() is the machine-readable list and the published page renders it.
// The short version: `Receive` and `EndToEnd` on every family, because four
// libraries have four prefetch and buffering models and the specification requires the
// same prefetch on both sides; `PublishBatch` for gocloud, because
// gocloud.dev/pubsub has no batch API at all and a loop of Send is not a
// client-level batch; and the whole `raw` arm of the `memory` family, because
// an in-process "raw client" is a Go channel with no acknowledgement, which is
// not the same work.
//
// Everything named in Exclusions() is still MEASURED. Its number appears in
// the table with "not compared" against it, exactly as
// docs/performance/overhead.md does, because a row that was measured and
// withheld is evidence and a row that was never measured is a gap.
//
// This package starts no container. hack/bench/run-compare.sh does, one broker
// at a time, from the digests conformance/harness/images.lock pins.
package competitors

import (
	"os"
	"sort"
	"strings"
	"testing"

	"github.com/iqhive/iqmq/benchmarks/opbench"
)

// Family is one broker (or the in-process engine) that every arm in it runs
// against. It is the `<family>` path segment after the arm name.
type Family struct {
	// Name is the path segment: "memory" or "nats".
	Name string
	// AddrEnv is the environment variable carrying the broker address, or ""
	// for an in-process family.
	AddrEnv string
	// Semantics states, in one sentence, the delivery and confirmation
	// semantics EVERY arm in this family is configured to. It is rendered
	// beside the family's rows on the published page: a comparison whose
	// semantics are not stated is not a comparison.
	Semantics string
	// Proof says how the claim in Semantics was established, and it is a
	// separate field because "we configured it" and "we checked it" are
	// different statements.
	Proof string
	// Denominator is the library every arm's figure is divided by, and it is
	// a declared field because the choice decides what the table MEANS.
	//
	// Where the family has a raw-client arm it is "raw", which makes the
	// column the specification's own question -- "core overhead vs raw client" -- asked of
	// three libraries instead of one, with no arm privileged and iqmq one row
	// among four. Where it has none it is "iqmq", because a ratio needs a
	// denominator and there is no floor to use instead; that is a choice of
	// units and not a claim, and the published page says so.
	Denominator string
}

// Families returns every family, in measurement order: the one that needs no
// container first, because a run the guard calls contended is repeated from
// the start.
func Families() []Family {
	return []Family{
		{
			Name:        "memory",
			AddrEnv:     "",
			Denominator: "iqmq",
			Semantics: "in-process, at-least-once with an explicit acknowledgement per message; a publication returns when the in-process broker has accepted it -- " +
				"BUT the arms do not agree on payload ownership, so this family is measured and NOT compared (see Exclusions)",
			Proof: "every arm's session acknowledges every delivery and the correctness gate round-trips one message and a full batch at every payload size, " +
				"so the arms agree on delivery and on settlement. They do NOT agree on ownership -- iqmq clones the body on admission, Watermill and gocloud alias it -- " +
				"which is what withdrew this family's ratio column; and their OVERFLOW policies differ too (iqmq and Watermill block on a bounded buffer, gocloud's is unbounded), " +
				"which the benchmark never reaches because opbench drains before its outstanding bound",
		},
		{
			Name:        "nats",
			AddrEnv:     "IQMQ_NATS_ADDR",
			Denominator: "raw",
			Semantics:   "NATS Core, at-most-once, nothing stored and nothing settled; a publication returns once it is in the client's write buffer (no flush, no acknowledgement)",
			Proof: "all four arms are handed the SAME *nats.Conn from the one pinned nats.go version, so the transport, the dial options and the write buffer are one object; " +
				"iqmq is configured to mq.ConfirmNone, which is the boundary gocloud's natspubsub and watermill-nats can offer (neither flushes) and the boundary the raw arm uses; " +
				"no arm acknowledges, because NATS Core has no acknowledgement to send",
		},
	}
}

// Arm is one library measured against one family.
type Arm struct {
	// Library is the first path segment after BenchmarkCompare: "iqmq",
	// "watermill", "gocloud" or "raw".
	Library string
	// Family is the family name.
	Family string
	// Version is the pinned module version the arm was measured at. It is a
	// literal, checked against go.mod by TestPinnedVersionsMatchGoMod, so the
	// published page cannot claim a version the run did not use.
	Version string
	// Config records what had to be configured to reach the family's
	// semantics, in the arm's own vocabulary. It is what a reader checks the
	// comparison with.
	Config string
	// Open opens a session for one benchmark leaf.
	Open func(tb testing.TB, addr string, p opbench.Params) opbench.Session
}

// Name returns the arm's benchmark path prefix, "<library>/<family>".
func (a Arm) Name() string { return a.Library + "/" + a.Family }

// Arms returns every arm, grouped by family in Families() order and by library
// in a fixed order inside a family, so that a run's rows appear in one order.
func Arms() []Arm {
	return []Arm{
		{
			Library: "iqmq", Family: "memory", Version: "(this tree)",
			Config: "memory.Open with the default queue topology; the topic and subscription created through the portable Admin surface before the client is used; every delivery acknowledged",
			Open:   openIqmqMemory,
		},
		{
			Library: "watermill", Family: "memory", Version: watermillVersion,
			Config: "gochannel.NewGoChannel with OutputChannelBuffer set to opbench's outstanding bound and BlockPublishUntilSubscriberAck off; every message acknowledged",
			Open:   openWatermillMemory,
		},
		{
			Library: "gocloud", Family: "memory", Version: gocloudVersion,
			Config: "mempubsub.NewTopic and NewSubscription with no ack deadline override; every message acknowledged",
			Open:   openGocloudMemory,
		},
		{
			Library: "iqmq", Family: "nats", Version: "(this tree)",
			Config: "nats.OpenWithClient over the shared *nats.Conn, ClientBorrowed, Confirmation: mq.ConfirmNone; nothing acknowledged (NATS Core advertises no explicit ack)",
			Open:   openIqmqNats,
		},
		{
			Library: "watermill", Family: "nats", Version: watermillNatsVersion,
			Config: "nats.NewPublisherWithNatsConn / NewSubscriberWithNatsConn over the shared *nats.Conn with JetStream.Disabled; the default GobMarshaler replaced by the native-header marshaler so the body on the wire is the payload",
			Open:   openWatermillNats,
		},
		{
			Library: "gocloud", Family: "nats", Version: gocloudNatsVersion,
			Config: "natspubsub.OpenTopicV2 / OpenSubscriptionV2 over the shared *nats.Conn (V2 uses native NATS headers rather than gob-encoding them into the body)",
			Open:   openGocloudNats,
		},
		{
			Library: "raw", Family: "nats", Version: natsGoVersion,
			Config: "nats.Conn.PublishMsg and a synchronous Subscription.NextMsgWithContext on the shared *nats.Conn; no flush, no acknowledgement",
			Open:   openRawNats,
		},
	}
}

// ArmsFor returns the arms of one family.
func ArmsFor(family string) []Arm {
	var out []Arm
	for _, a := range Arms() {
		if a.Family == family {
			out = append(out, a)
		}
	}
	return out
}

// Addr returns the family's broker address and whether it is available.
func (f Family) Addr() (string, bool) {
	if f.AddrEnv == "" {
		return "", true
	}
	addr := os.Getenv(f.AddrEnv)
	return addr, addr != ""
}

// Exclusion is one comparison this package measures and refuses to compare,
// with the reason.
//
// It is a declared list rather than a note in a document because the rendered
// page is generated from it, and because TestEveryExclusionNamesRealRows
// checks that each one names an arm and an operation that exist -- an
// exclusion for a row nobody measures is a reassurance rather than a
// disclosure.
type Exclusion struct {
	// Library is the arm's library, or "*" for every library.
	Library string
	// Family is the family, or "*" for every family.
	Family string
	// Op is the operation, or "*" for every operation.
	Op string
	// Reason says why the comparison cannot be made equivalent.
	Reason string
}

// Exclusions returns every comparison that is measured and not compared.
func Exclusions() []Exclusion {
	return []Exclusion{
		{
			Library: "*", Family: "*", Op: "Receive",
			Reason: "the native-comparison rules require the same prefetch on both sides and these four arms have four buffering models: " +
				"the raw arm reads nats.go's client-side pending buffer with no round trip, iqmq goes through the core's fetch buffer, a lane and a delivery, " +
				"gocloud's natspubsub deliberately disables read-ahead (MaxBatchSize and MaxHandlers of one, because NATS Core is at-most-once), " +
				"and watermill pushes into an output channel from its own subscriber goroutines. " +
				"Increment B11 measured that asymmetry in both directions and at two orders of magnitude on the same broker, so it is not a small correction ",
		},
		{
			Library: "*", Family: "*", Op: "EndToEnd",
			Reason: "it contains a Receive, so it inherits the prefetch asymmetry above. " +
				"This is B11's own generalisation, arrived at after four EndToEnd rows had been published as budget breaches and withdrawn: " +
				"an operation is comparable only if EVERY round trip in it is comparable, so the exclusion is decided per component and not per leaf name",
		},
		{
			Library: "gocloud", Family: "*", Op: "PublishBatch",
			Reason: "gocloud.dev/pubsub has no batch API. Topic.Send takes one message, and the batcher behind it merges CONCURRENT sends rather than a caller's batch, " +
				"so a sequential loop of Send is what a gocloud user writes and is not a client-level batch. The row is measured with that loop and is not compared with one",
		},
		{
			Library: "*", Family: "memory", Op: "*",
			Reason: "the three in-process arms do not agree on PAYLOAD OWNERSHIP, which is a different guarantee and not a different speed. " +
				"iqmq's memory engine is in ADR-009's safe copy mode: it CLONES the message on admission, so the caller may reuse its Body the instant Publish returns. " +
				"Watermill's gochannel calls message.Copy(), which is `NewMessage(m.UUID, m.Payload)` -- a new struct ALIASING the same payload slice -- and gocloud's mempubsub stores the driver.Message pointer itself; " +
				"in both, a subscriber reads the caller's backing array later, so the caller may not touch it. " +
				"Read in all three sources rather than inferred from the timings, which is how it was found: iqmq pays a 64 KiB memcpy per message at the largest payload and the other two pay nothing, " +
				"so `watermill/memory/Publish/64KiB` measured 2,374 ns against 6,544 ns at 64 B -- FASTER on a payload 1,024 times larger -- and the renderer turned that into a -889.8 % throughput loss. " +
				"A figure that absurd is the arms disagreeing, not one library beating another. " +
				"The medians are published as measured and none of them is compared; making the family comparable needs the arms configured to one ownership rule and a new campaign ",
		},
		{
			Library: "raw", Family: "memory", Op: "*",
			Reason: "there is no raw client for an in-process broker. The nearest thing is a Go channel, which has no acknowledgement, no subscription and no delivery, " +
				"so it is not the same work; the honest in-process baseline is iqmq's own engine, using an api-against-engine ratio and not a competitor comparison. No such arm exists",
		},
	}
}

// Excluded reports whether an arm's operation is compared, and the reason when
// it is not.
func Excluded(library, family, op string) (reason string, excluded bool) {
	for _, e := range Exclusions() {
		if match(e.Library, library) && match(e.Family, family) && match(e.Op, op) {
			return e.Reason, true
		}
	}
	return "", false
}

func match(pattern, s string) bool { return pattern == "*" || pattern == s }

// Libraries returns the library names in published order.
func Libraries() []string { return []string{"iqmq", "watermill", "gocloud", "raw"} }

// Names returns every benchmark name this package produces, in the order
// BenchmarkCompare produces them. It is what `bakeoff lint`-style checks and
// the renderer read, so that a name is declared in one place.
func Names() []string {
	var out []string
	for _, f := range Families() {
		for _, a := range ArmsFor(f.Name) {
			out = append(out, opbench.Names("BenchmarkCompare", a.Name())...)
		}
	}
	return out
}

// Pinned returns the pinned versions this package reports, keyed by module
// path, so that TestPinnedVersionsMatchGoMod can check them against go.mod
// and the published page cannot claim a version the run did not use.
func Pinned() map[string]string {
	return map[string]string{
		"github.com/ThreeDotsLabs/watermill":         watermillVersion,
		"github.com/ThreeDotsLabs/watermill-nats/v2": watermillNatsVersion,
		"gocloud.dev":                   gocloudVersion,
		"gocloud.dev/pubsub/natspubsub": gocloudNatsVersion,
		"github.com/nats-io/nats.go":    natsGoVersion,
	}
}

// PinnedList returns Pinned() as sorted "module version" lines, for a report.
func PinnedList() []string {
	p := Pinned()
	out := make([]string, 0, len(p))
	for mod, v := range p {
		out = append(out, mod+" "+v)
	}
	sort.Strings(out)
	return out
}

// The pinned versions. They are literals so that the published page and the
// module graph cannot disagree without a test failing.
const (
	watermillVersion     = "v1.5.3"
	watermillNatsVersion = "v2.2.0"
	gocloudVersion       = "v0.46.0"
	gocloudNatsVersion   = "v0.46.0"
	natsGoVersion        = "v1.54.0"
)

// topicName returns a broker-safe unique name for one leaf. Only [a-z0-9-] is
// used, which every broker in the harness accepts.
func topicName() string {
	return strings.ReplaceAll(opbench.Name("compare"), ".", "-")
}
