// Command overhead reports what each driver costs over its native client, from
// a benchmark run's raw output.
package main

import (
	"flag"
	"fmt"
	"os"
	"sort"
	"strings"

	"github.com/iqhive/iqmq/benchmarks/internal/benchfmt"
	"github.com/iqhive/iqmq/benchmarks/internal/budgets"
	"github.com/iqhive/iqmq/benchmarks/internal/stat"
	"github.com/iqhive/iqmq/benchmarks/opbench"
	"github.com/iqhive/iqmq/benchmarks/overhead"
)

// budget is the ceiling one operation's shape is held to, in per cent of
// throughput lost against the raw client. A zero means the operation is
// measured and not compared.
func budget(op string) (pct float64, compared bool) {
	switch op {
	case "PublishBatch":
		return 5, true
	case "Publish":
		return 10, true
	default:
		return 0, false
	}
}

func main() {
	results := flag.String("results", "", "raw benchmark output to read (a run's raw.txt)")
	flag.Parse()
	if *results == "" {
		fmt.Fprintln(os.Stderr, "usage: overhead -results <raw.txt>")
		os.Exit(2)
	}
	data, err := os.ReadFile(*results)
	if err != nil {
		fmt.Fprintln(os.Stderr, "overhead:", err)
		os.Exit(2)
	}
	rs, err := benchfmt.ParseBytes(data)
	if err != nil {
		fmt.Fprintln(os.Stderr, "overhead:", err)
		os.Exit(2)
	}
	if len(rs) == 0 {
		fmt.Fprintf(os.Stderr, "!! overhead: %s holds no benchmark row\n", *results)
		os.Exit(2)
	}

	// samples[name][unit] is every value measured for that row.
	samples := map[string]map[string][]float64{}
	for _, r := range rs {
		base := r.Base()
		byUnit := samples[base]
		if byUnit == nil {
			byUnit = map[string][]float64{}
			samples[base] = byUnit
		}
		for unit, v := range r.Values {
			byUnit[unit] = append(byUnit[unit], v)
		}
	}

	type row struct {
		arm, native, op, size    string
		dMed, nMed, dHalf, nHalf float64
		dN, nN                   int
		compared                 bool
		budgetPct                float64
		breach                   bool
	}
	var rows []row
	missing := map[string]bool{}

	for _, p := range overhead.Profiles() {
		for _, op := range opbench.Ops() {
			for _, size := range opbench.Sizes() {
				dName := strings.Join([]string{"BenchmarkDriver", p.Arm, op, size.Name}, "/")
				d, ok := samples[dName]
				if !ok {
					continue
				}
				r := row{arm: p.Arm, native: p.Native, op: op, size: size.Name}
				r.dMed, r.dHalf, r.dN = summarise(d["ns/op"])
				r.budgetPct, r.compared = budget(op)
				switch {
				case p.Native != "":
					nName := strings.Join([]string{"BenchmarkNative", p.Native, op, size.Name}, "/")
					n, nok := samples[nName]
					switch {
					case !nok:
						missing[nName] = true
						r.compared = false
					default:
						r.nMed, r.nHalf, r.nN = summarise(n["ns/op"])
					}
				default:
					r.compared = false
				}
				if r.compared && r.nMed > 0 {
					r.breach = loss(r.dMed, r.nMed) > r.budgetPct
				}
				rows = append(rows, r)
			}
		}
	}
	if len(rows) == 0 {
		fmt.Fprintf(os.Stderr, "!! overhead: %s holds no BenchmarkDriver row for any declared profile\n", *results)
		os.Exit(2)
	}

	fmt.Println("| Arm | Op | Size | driver ns/op | native ns/op | throughput loss | budget | verdict |")
	fmt.Println("|---|---|---|---|---|---|---|---|")
	breaches := 0
	for i := range rows {
		r := &rows[i]
		native, lossPct, budg, verdict := "—", "—", "—", "not compared"
		if r.nN > 0 {
			native = fmt.Sprintf("%.0f ±%.1f%% (n=%d)", r.nMed, r.nHalf, r.nN)
		}
		if r.compared && r.nMed > 0 {
			lossPct = fmt.Sprintf("%+.1f%%", loss(r.dMed, r.nMed))
			budg = fmt.Sprintf("≤ %.0f%%", r.budgetPct)
			verdict = "met"
			if r.breach {
				verdict = "**BREACH**"
				breaches++
			}
		}
		fmt.Printf("| `%s` | %s | %s | %.0f ±%.1f%% (n=%d) | %s | %s | %s | %s |\n",
			r.arm, r.op, r.size, r.dMed, r.dHalf, r.dN, native, lossPct, budg, verdict)
	}

	fmt.Printf("\n%d row(s); %d breach(es) of the overhead budgets.\n", len(rows), breaches)
	if len(missing) > 0 {
		names := make([]string, 0, len(missing))
		for n := range missing {
			names = append(names, n)
		}
		sort.Strings(names)
		fmt.Printf("\nNot compared, because the run holds no baseline row for them: %s\n", strings.Join(names, ", "))
	}
	fmt.Println("\n`Receive` and `EndToEnd` are measured on both arms and deliberately not")
	fmt.Println("turned into a ratio: the native-comparison rules require the same")
	fmt.Println("prefetch on both sides and these arms do not have it, in both directions and")
	fmt.Println("at two orders of magnitude (nats/core Receive: the driver 9x slower;")
	fmt.Println("jetstream Receive: the driver 34x faster). EndToEnd contains a receive, so it")
	fmt.Println("carries the same asymmetry. Both medians are in the table above; what is")
	fmt.Println("compared is a batched publication and a single-message publication, which is")
	fmt.Println("what the two budgets are written about.")
}

// loss is the throughput a driver arm gives up against its baseline, in per
// cent: the rate lost, not the duration added. They are not the same figure
// and the specification budgets the first.
//
// The arithmetic itself lives in benchmarks/internal/budgets, which is the
// package the GATE reads too, so that this page's figures and the `kind: ratio`
// budget that enforces them cannot drift apart: a report and the gate that
// enforces it computing the ratio twice is two chances to get it wrong, and
// the first thing that drifts in a comparison is the arithmetic.
// `TestLossIsTheRateNotTheDuration` below and
// `TestThroughputLossIsTheRateNotTheDuration` beside the implementation pin
// the same two boundaries from both sides.
func loss(driverNs, nativeNs float64) float64 {
	return budgets.ThroughputLossPct(driverNs, nativeNs)
}

// summarise returns a row's median, the half-width of its median confidence
// interval as a percentage of that median, and the number of samples.
func summarise(xs []float64) (med, halfPct float64, n int) {
	if len(xs) == 0 {
		return 0, 0, 0
	}
	med = stat.Median(xs)
	lo, hi := stat.MedianCI(xs)
	if med > 0 {
		halfPct = (hi - lo) / 2 / med * 100
	}
	return med, halfPct, len(xs)
}
