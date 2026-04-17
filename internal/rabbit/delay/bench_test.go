package delay

import (
	"testing"
	"time"
)

// The benchmarks.
//
// The decision column is "delay accuracy; broker CPU; queue count growth", and
// two of those three are properties of the PLAN rather than of its cost, so
// they are reported as custom metrics beside ns/op and are what the family is
// decided on:
//
//	error-ms      delay accuracy: the mean milliseconds by which the granted
//	              delay exceeds the requested one. Zero for an exact
//	              strategy; a bucket ladder's own arithmetic for the other.
//	queues        queue count growth: how many distinct delay queues the
//	              workload's delays need. Zero for the plugin, one per
//	              (target, bucket) pair for the TTL arm.
//	hops/plan     the broker's own work, as the number of times it moves a
//	              message before it is deliverable again. It is a PROXY for
//	              "broker CPU", which needs a broker to measure; see
//	              CRITERION_GAP declares/plan how much topology a workload asks the driver to declare,
//	              which is the other half of the growth term: a bucket queue
//	              is declared once and then remembered, so this is the count
//	              of DISTINCT declarations per plan.
//	ns/op         what the planning code costs, reported beside the criterion
//	              rather than as it. It is a handful of comparisons and a
//	              string concatenation in both arms.
//
//	BenchmarkRabbitDelay/<kind>/decide
//	BenchmarkRabbitDelay/<kind>/dist=<fixed|ladder|uniform>
//
// The distribution is the dimension that separates the arms, and it is the one
// a deployment's own answer depends on:
//
//	fixed    one delay, repeated -- thirty seconds, which is what a retry
//	         schedule is actually configured with. It is the case the TTL arm
//	         needs only ONE queue for, and it is also the case that shows the
//	         rounding honestly: thirty seconds is not on the doubling ladder,
//	         so it is granted the bucket above (51.2 s) and the error term is
//	         twenty-one seconds for every message.
//	ladder   delays drawn from the ladder itself. No rounding error and the
//	         whole ladder as queues, which is the queue-count worst case
//	         without the accuracy cost.
//	uniform  delays spread uniformly over three decades, which is what a
//	         retry schedule with jitter produces and where the TTL arm pays
//	         both costs at once.

// benchPlans is how many plans one iteration builds.
const benchPlans = 256

// BenchmarkRabbitDelay measures every kind.
func BenchmarkRabbitDelay(b *testing.B) {
	for _, k := range Kinds() {
		b.Run(k.String(), func(b *testing.B) {
			b.Run("decide", func(b *testing.B) { benchDecide(b, k) })
			for _, dist := range []string{"fixed", "ladder", "uniform"} {
				b.Run("dist="+dist, func(b *testing.B) { benchDist(b, k, dist) })
			}
		})
	}
}

// benchDecide measures one plan, which is the part of the family that is not
// the criterion.
func benchDecide(b *testing.B, k Kind) {
	st, err := New(k, Config{})
	if err != nil {
		b.Fatal(err)
	}
	req := request(30 * time.Second)
	b.ReportAllocs()
	var sink Plan
	for b.Loop() {
		p, perr := st.Plan(req)
		if perr != nil {
			b.Fatal(perr)
		}
		sink = p
	}
	_ = sink
}

// benchDist builds a workload's plans and reports the criterion's own metrics.
func benchDist(b *testing.B, k Kind, dist string) {
	st, err := New(k, Config{})
	if err != nil {
		b.Fatal(err)
	}
	reqs := workload(st, dist, benchPlans)

	// The three plan properties are measured once, outside the loop: they
	// are facts about the strategy and the workload, not costs that
	// accumulate per iteration, and computing them inside the loop would put
	// a second workload into ns/op.
	var errSum, hops, declares int64
	seen := map[string]struct{}{}
	for _, req := range reqs {
		p, perr := st.Plan(req)
		if perr != nil {
			b.Fatalf("delay %v refused: %v", req.Delay, perr)
		}
		errSum += int64((p.Granted - req.Delay) / time.Millisecond)
		hops += int64(p.Hops)
		if p.Declare != nil {
			if _, dup := seen[p.Declare.Name]; !dup {
				seen[p.Declare.Name] = struct{}{}
				declares++
			}
		}
	}
	queues := st.Queues(reqs)

	b.ReportAllocs()
	for b.Loop() {
		for _, req := range reqs {
			p, perr := st.Plan(req)
			if perr != nil {
				b.Fatal(perr)
			}
			if p.Granted < req.Delay {
				b.Fatalf("delay %v granted %v", req.Delay, p.Granted)
			}
		}
	}
	n := float64(len(reqs))
	b.ReportMetric(float64(errSum)/n, "error-ms")
	b.ReportMetric(float64(queues), "queues")
	b.ReportMetric(float64(hops)/n, "hops/plan")
	b.ReportMetric(float64(declares)/n, "declares/plan")
}

// workload builds the requests of one distribution.
//
// The delays are derived arithmetically rather than from a random source, so
// that two runs of the campaign measure the same workload: a family decided on
// "delay accuracy" cannot have a different accuracy each time it is measured.
func workload(st Strategy, dist string, n int) []Request {
	out := make([]Request, 0, n)
	ladder := DefaultLadder()
	for i := range n {
		var d time.Duration
		switch dist {
		case "fixed":
			d = 30 * time.Second
		case "ladder":
			d = ladder[i%len(ladder)]
		default:
			// Three decades, stepped by a prime number of milliseconds so
			// that consecutive requests land in different buckets and the
			// sequence does not repeat inside one workload.
			d = time.Duration(1+(i*1013)%1_000_000) * time.Millisecond
		}
		if d > st.MaxDelay() {
			d = st.MaxDelay()
		}
		out = append(out, request(d))
	}
	return out
}
