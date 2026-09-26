package streams_test

import (
	"strconv"
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/internal/streams"
)

// BenchmarkRedisNack/<kind>/<action>/delay=/batch= measures one negative
// settlement's cost for each kind.
//
// The headline metric is `rtts/op`, not ns/op, and that is the point of the family. Every arm's Go work is a few hundred nanoseconds of string
// building; what separates them is how many round trips a nack costs on a
// broker where a round trip is tens of microseconds —
//
//	leavepending  0 commands
//	stop          0 commands
//	readd         1 script call
//	zsetmover     1 script call to park, plus one per release
//
// — and what each arm then costs the broker in pending-entries-list growth
// and redelivery latency, which the model tests measure and the driver page
// states. ns/op and allocs/op are here because a settlement is on the hot
// path and a strategy that allocated per nack would be paying for
// something the caller cannot see.
//
// The plan is a pure function, so the benchmark measures the strategy and not
// a server: no I/O, no clock, nothing shared between iterations.
func BenchmarkRedisNack(b *testing.B) {
	keys, err := streams.NewKeys("acme.orders", 4)
	if err != nil {
		b.Fatalf("NewKeys: %v", err)
	}
	entry := benchEntry(benchHeaders())
	now := time.Unix(1767225600, 0)
	for _, k := range streams.Kinds() {
		b.Run(k.String(), func(b *testing.B) {
			strategy, serr := streams.NewNack(k, streams.NackConfig{
				Keys:             keys,
				Group:            "workers",
				Consumer:         "source-a-0f3c",
				AckDeadline:      30 * time.Second,
				DeadLetterStream: keys.Stream(0) + ":dlq",
			})
			if serr != nil {
				b.Fatalf("NewNack: %v", serr)
			}
			for _, action := range benchActions() {
				b.Run(action.name, func(b *testing.B) {
					for _, delay := range benchDelays(strategy, action.action) {
						b.Run("delay="+delay.name, func(b *testing.B) {
							for _, batch := range benchBatches() {
								b.Run("batch="+strconv.Itoa(batch), func(b *testing.B) {
									benchNack(b, strategy, entry, action.action, delay.d, batch, now)
								})
							}
						})
					}
				})
			}
		})
	}
}

// benchAction is one of the negative-settlement actions.
type benchAction struct {
	name   string
	action mq.NackAction
}

// benchActions are the three actions every kind implements. The dead-letter
// arm is measured because it is the one route that is a Lua script for every
// kind, so it is where the arms' costs converge — reporting only the requeue
// would make the family look more different than it is.
func benchActions() []benchAction {
	return []benchAction{
		{"requeue", mq.NackActionRequeue},
		{"drop", mq.NackActionDrop},
		{"deadletter", mq.NackActionDeadLetter},
	}
}

// benchDelay is one delay setting.
type benchDelay struct {
	name string
	d    time.Duration
}

// benchDelays is the delay sweep, narrowed to what the arm under test can
// actually honour.
//
// A delay an arm refuses is not measured: the refusal costs nothing and a row
// of "the plan was refused in 4 ns" would enter the family's comparable set
// and decide the bake-off on an arm's inability. Every arm therefore has the
// delay=0 row, which is what `bakeoff decide` reads, and only the arms that
// delay have the delay=1m row.
func benchDelays(s streams.Strategy, action mq.NackAction) []benchDelay {
	out := []benchDelay{{"0", 0}}
	if action == mq.NackActionRequeue && s.Delays() {
		out = append(out, benchDelay{"1m", time.Minute})
	}
	return out
}

// benchBatches is the batch ends: one settlement, and a hundred.
func benchBatches() []int {
	if testing.Short() {
		return []int{1}
	}
	return []int{1, 100}
}

// benchHeaders is a realistic header set: four small headers, which is what a
// traced message carries.
func benchHeaders() mq.Headers {
	var h mq.Headers
	h.Add("traceparent", []byte("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"))
	h.Add("content-type", []byte("application/json"))
	h.Add("x-tenant", []byte("acme"))
	h.Add("x-retry", []byte("0"))
	return h
}

func benchEntry(h mq.Headers) streams.Entry {
	m := &mq.Message{
		Topic:      "acme.orders",
		Key:        []byte("order-4711"),
		Body:       make([]byte, 256),
		ID:         "01JQZ8V3K5T2WQ9X7YB6C4D2E1",
		Headers:    h,
		Properties: mq.Properties{ContentType: "application/json"},
	}
	return streams.Entry{
		ID:     streams.ID{Ms: 1767225600000, Seq: 7},
		Shard:  2,
		Fields: streams.Encode(nil, m),
	}
}

// benchNack runs one arm.
//
// rtts/op is reported as the number of commands the plan produced, which on
// this broker is the number of round trips a settlement costs: a plan's
// commands are pipelined, so a two-command plan is one round trip for
// latency and two commands for the server's work. Both numbers matter and
// the benchmark reports the command count, because it is the one the arms
// differ in by construction.
func benchNack(b *testing.B, s streams.Strategy, entry streams.Entry, action mq.NackAction, delay time.Duration, batch int, now time.Time) {
	b.ReportAllocs()
	req := streams.NackRequest{
		Entry:   entry,
		Options: mq.NackOptions{Action: action, Delay: delay},
		Now:     now,
		Attempt: 2,
	}
	// One plan up front, so a refusal is a benchmark failure rather than a
	// measurement of the refusal path.
	probe, err := s.Plan(req)
	if err != nil {
		b.Fatalf("%v: Plan refused the workload: %v", s.Kind(), err)
	}
	cmds := 0
	for b.Loop() {
		for i := range batch {
			r := req
			r.Entry.ID = streams.ID{Ms: entry.ID.Ms, Seq: entry.ID.Seq + uint64(i)}
			plan, perr := s.Plan(r)
			if perr != nil {
				b.Fatalf("Plan: %v", perr)
			}
			cmds += len(plan.Cmds)
		}
	}
	_ = probe
	b.ReportMetric(float64(cmds)/float64(b.N*batch), "cmds/op")
	b.ReportMetric(float64(len(probe.Cmds)), "rtts/op")
}

// BenchmarkRedisFields/<direction>/headers= measures the entry field layout,
// which is on every publication's and every delivery's path.
func BenchmarkRedisFields(b *testing.B) {
	for _, n := range []int{0, 4, 16} {
		var h mq.Headers
		for i := range n {
			h.Add("h"+strconv.Itoa(i), []byte("value"+strconv.Itoa(i)))
		}
		m := &mq.Message{
			Topic:      "acme.orders",
			Key:        []byte("order-4711"),
			Body:       make([]byte, 256),
			ID:         "01JQZ8V3K5T2WQ9X7YB6C4D2E1",
			Headers:    h,
			Properties: mq.Properties{ContentType: "application/json"},
		}
		b.Run("Encode/headers="+strconv.Itoa(n), func(b *testing.B) {
			b.ReportAllocs()
			dst := make([]string, 0, 64)
			for b.Loop() {
				dst = streams.Encode(dst[:0], m)
			}
			if len(dst) == 0 {
				b.Fatal("Encode produced nothing")
			}
		})
		fields := streams.Encode(nil, m)
		b.Run("Decode/headers="+strconv.Itoa(n), func(b *testing.B) {
			b.ReportAllocs()
			var out mq.Message
			for b.Loop() {
				if err := streams.Decode(&out, fields); err != nil {
					b.Fatalf("Decode: %v", err)
				}
			}
		})
		b.Run("Check/headers="+strconv.Itoa(n), func(b *testing.B) {
			b.ReportAllocs()
			lim := streams.DefaultLimits
			for b.Loop() {
				if err := lim.Check(m); err != nil {
					b.Fatalf("Check: %v", err)
				}
			}
		})
	}
}

// BenchmarkRedisReclaimer measures the bookkeeping a settlement goes through
// before a command is built: the hand-out, the check and the release.
//
// It is the cost the guard adds in process, and it has to be small: it is
// paid per delivery and per settlement, on top of a round trip the driver
// cannot avoid.
func BenchmarkRedisReclaimer(b *testing.B) {
	b.Run("hand-settle", func(b *testing.B) {
		b.ReportAllocs()
		rec := streams.NewReclaimer()
		key := streams.EntryKey{Shard: 1, ID: streams.ID{Ms: 1767225600000, Seq: 1}}
		for b.Loop() {
			gen, _, ok := rec.Hand(key)
			if !ok {
				b.Fatal("Hand refused")
			}
			if err := rec.Begin(key, gen); err != nil {
				b.Fatalf("Begin: %v", err)
			}
			rec.Settled(key)
		}
	})
	b.Run("check", func(b *testing.B) {
		b.ReportAllocs()
		rec := streams.NewReclaimer()
		key := streams.EntryKey{Shard: 1, ID: streams.ID{Ms: 1767225600000, Seq: 1}}
		gen, _, _ := rec.Hand(key)
		for b.Loop() {
			if err := rec.Check(key, gen); err != nil {
				b.Fatalf("Check: %v", err)
			}
		}
	})
}

// BenchmarkXAddReplies measures the publish path's REPLY HANDLING, one
// pipelined batch per iteration: what a driver spends reading back what the
// server said about a hundred XADDs.
//
// One op is a whole batch, so ns/op is directly ns/batch and comparable with
// the budget the batched row is held to -- which is the only reason this
// benchmark exists. A per-call figure would be a true number nobody could
// place against the 88,516 ns/batch the `redis/streams` PublishBatch row has
// to shed.
//
// The two arms are the before and the after:
//
//	parse  [streams.ParseXAdd], which builds the identifier and is what the
//	       batch path called while discarding every one of them
//	check  [streams.CheckXAdd], which answers the same error and builds nothing
//
// There is no I/O, no clock and nothing shared between iterations: the replies
// are built once, so what is measured is the checking and not the making. The
// identifiers are shaped like a real batch's -- one millisecond spans several
// entries, so the sequence is one or two digits where the millisecond is
// thirteen -- because the cost being removed is per DIGIT and a corpus of
// "1-1" would understate it.
func BenchmarkXAddReplies(b *testing.B) {
	const batch = 100
	replies := benchXAddReplies(batch)
	b.Run("parse", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			for _, v := range replies {
				if _, err := streams.ParseXAdd(v); err != nil {
					b.Fatalf("ParseXAdd: %v", err)
				}
			}
		}
	})
	b.Run("check", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			for _, v := range replies {
				if err := streams.CheckXAdd(v); err != nil {
					b.Fatalf("CheckXAdd: %v", err)
				}
			}
		}
	})
}

// benchXAddReplies builds n XADD replies the way a server answers a pipeline:
// the millisecond advances every fiftieth entry and the sequence runs within
// it.
func benchXAddReplies(n int) []streams.Value {
	vs := make([]streams.Value, n)
	for i := range n {
		id := streams.ID{Ms: 1767225600123 + uint64(i/50), Seq: uint64(i % 50)}
		vs[i] = streams.Str(id.String())
	}
	return vs
}
