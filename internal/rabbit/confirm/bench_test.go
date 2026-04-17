package confirm

import "testing"

// The benchmarks.
//
// The decision column is "throughput and ambiguity accuracy", and neither of
// them is what a microbenchmark of the decision measures: `OnSubmit` is one
// integer comparison in all three kinds, so `ns/op` on it is the same number
// three times and says nothing. What the kinds differ about is what a STREAM
// of publications costs and what a channel close leaves unresolved, so each
// arm drives the reference simulation of sim_test.go at one batch width and
// reports the criterion's own numbers:
//
//	virt-us/publish    throughput: the publisher's elapsed virtual time per
//	                   publication. A per-message strategy pays a round trip
//	                   each; a pipelined one pays one to fill the pipeline.
//	stalls/publish     how often the publishing goroutine blocked, which is
//	                   the mechanism behind the number above.
//	frames/publish     what the broker had to send, which is the part of the
//	                   cost that is the BROKER's: a per-message strategy gives
//	                   it nothing to coalesce.
//	ambiguous/close    ambiguity accuracy: how many publications a channel
//	                   close leaves unresolved. It is measured, once, by a
//	                   separate run that closes instead of flushing -- see
//	                   benchAmbiguity -- because it is a property of the
//	                   strategy rather than a per-iteration cost.
//	ns/op, allocs/op   what this package's own code costs, reported beside the
//	                   criterion rather than as it.
//
//	BenchmarkRabbitConfirm/<kind>/decide
//	BenchmarkRabbitConfirm/<kind>/batch=<1|16|128>
//
// The batch width is the dimension that separates the three kinds, and it is
// the one the specification's own expectation is stated in terms of ("windowed/async for
// batch, per-message for `Publish`"): at batch=1 a windowed strategy IS a
// per-message strategy, because it owes its caller an outcome before the call
// returns, and at batch=128 it pays one round trip where the reference kind
// pays a hundred and twenty-eight.

// benchPublications is how many messages one iteration publishes. It is large
// enough that building the simulation is noise beside the workload and small
// enough that one iteration is tens of microseconds, so a full-profile arm
// stays far inside the harness's three-second ceiling per benchmark.
const benchPublications = 512

// benchRTT and benchPublishCost are the simulation's cost model in virtual
// microseconds: a millisecond round trip against a microsecond of local work,
// which is the ratio a real channel to a broker on the same network shows and
// the ratio that makes the three kinds separable at all.
const (
	benchRTT         = 1000
	benchPublishCost = 1
	// benchAckEvery is how many confirms the broker coalesces. Eight rather
	// than one, because a real broker under load coalesces and because it is
	// the only setting under which frames/publish can differ between the
	// kinds.
	benchAckEvery = 8
)

// BenchmarkRabbitConfirm measures every kind.
func BenchmarkRabbitConfirm(b *testing.B) {
	for _, k := range Kinds() {
		b.Run(k.String(), func(b *testing.B) {
			b.Run("decide", func(b *testing.B) { benchDecide(b, k) })
			b.Run("batch=1", func(b *testing.B) { benchStream(b, k, 1) })
			b.Run("batch=16", func(b *testing.B) { benchStream(b, k, 16) })
			b.Run("batch=128", func(b *testing.B) { benchStream(b, k, 128) })
		})
	}
}

// benchDecide measures the decision alone: what a strategy's own code costs to
// call, which is the part of the family that is not the criterion.
func benchDecide(b *testing.B, k Kind) {
	st, err := New(k, Config{})
	if err != nil {
		b.Fatal(err)
	}
	w := st.Window()
	var sink Action
	b.ReportAllocs()
	for b.Loop() {
		sink = st.OnSubmit(w)
	}
	_ = sink
}

// benchStream drives one kind at one batch width and reports the criterion's
// own metrics.
func benchStream(b *testing.B, k Kind, batch int) {
	cfg := simConfig{
		Publications: benchPublications,
		Batch:        batch,
		PublishCost:  benchPublishCost,
		RTT:          benchRTT,
		AckEvery:     benchAckEvery,
	}
	// The ambiguity is measured once, outside the loop: it is a property of
	// the strategy and the batch width, not a cost that accumulates per
	// iteration, and running it inside the loop would put a second workload
	// into ns/op.
	ambiguous := benchAmbiguity(b, k, batch)

	var elapsed, stalls, frames, runs int64
	b.ReportAllocs()
	for b.Loop() {
		s := newSim(b, k, cfg)
		s.run()
		elapsed += s.elapsed()
		stalls += int64(s.stalls)
		frames += int64(s.frames)
		runs++
	}
	if runs == 0 {
		return
	}
	per := float64(runs * benchPublications)
	b.ReportMetric(float64(elapsed)/per, "virt-us/publish")
	b.ReportMetric(float64(stalls)/per, "stalls/publish")
	b.ReportMetric(float64(frames)/per, "frames/publish")
	b.ReportMetric(float64(ambiguous), "ambiguous/close")
}

// benchAmbiguity is the specification's "ambiguity accuracy": a run of one batch that
// CLOSES instead of flushing, and the number of publications that leaves
// unresolved.
//
// It is the price the two pipelined kinds pay for the throughput measured
// beside it, and it is measured rather than computed from MaxAmbiguous so that
// the number on the scorecard is an observation of this code and not a
// restatement of its documentation.
func benchAmbiguity(b *testing.B, k Kind, batch int) int {
	s := newSim(b, k, simConfig{
		Publications: batch,
		// A batch wider than the run, so nothing is flushed before the
		// close.
		Batch:       batch + 1,
		PublishCost: benchPublishCost,
		RTT:         benchRTT,
		AckEvery:    benchAckEvery,
		DetachAtEnd: true,
	})
	s.run()
	return s.ambiguous
}
