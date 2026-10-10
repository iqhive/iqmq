// Command compare renders the layer-5 competitor table from
// a stored run: for every measured leaf, each arm's median beside iqmq's, the
// ratio where the comparison is equivalent, and the reason where it is not.
//
//	compare -results benchmarks/reports/results/<run>/raw.txt
//
// It exists for the reason `cmd/overhead` exists: the figures on a published
// page have to be REPRODUCIBLE from the stored rows rather than copied out of
// a terminal. A table a person typed is a table nobody can check.
//
// Three things it does that a naive renderer would not, each of them a lesson
// this repository has already paid for:
//
// A row an exclusion covers is printed with its median and "not compared", and
// the reasons are printed under the table. Nothing is silently dropped: a row
// measured and withheld is evidence, a row never measured is a gap, and the
// difference has to be visible.
package main

import (
	"compress/gzip"
	"flag"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"

	"github.com/iqhive/iqmq/benchmarks/competitors"
	"github.com/iqhive/iqmq/benchmarks/internal/benchfmt"
	"github.com/iqhive/iqmq/benchmarks/internal/budgets"
	"github.com/iqhive/iqmq/benchmarks/internal/stat"
	"github.com/iqhive/iqmq/benchmarks/opbench"
)

func main() {
	results := flag.String("results", "", "raw benchmark output to read (a run's raw.txt)")
	flag.Parse()
	if *results == "" {
		fmt.Fprintln(os.Stderr, "usage: compare -results <raw.txt>")
		os.Exit(2)
	}
	rs, err := readResults(*results)
	if err != nil {
		fmt.Fprintln(os.Stderr, "compare:", err)
		os.Exit(2)
	}
	if len(rs) == 0 {
		fmt.Fprintf(os.Stderr, "!! compare: %s holds no benchmark row\n", *results)
		os.Exit(2)
	}

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

	type cell struct {
		med, half float64
		n         int
		allocs    float64
		present   bool
	}
	read := func(name string) cell {
		byUnit, ok := samples[name]
		if !ok {
			return cell{}
		}
		med, half, n := summarise(byUnit["ns/op"])
		allocs := 0.0
		if len(byUnit["allocs/op"]) > 0 {
			allocs = stat.Median(byUnit["allocs/op"])
		}
		return cell{med: med, half: half, n: n, allocs: allocs, present: n > 0}
	}

	reasons := map[string]bool{}
	missing := map[string]bool{}
	compared, excluded := 0, 0

	fmt.Println("| Family | Op | Size | Arm | ns/op | allocs/op | throughput loss vs the family's denominator | verdict |")
	fmt.Println("|---|---|---|---|---|---|---|---|")
	for _, f := range competitors.Families() {
		for _, op := range opbench.Ops() {
			for _, size := range opbench.Sizes() {
				base := read(name(f.Denominator, f.Name, op, size.Name))
				for _, arm := range competitors.ArmsFor(f.Name) {
					n := name(arm.Library, f.Name, op, size.Name)
					c := read(n)
					if !c.present {
						missing[n] = true
						continue
					}
					reason, isExcluded := competitors.Excluded(arm.Library, f.Name, op)
					ratio, verdict := "—", "not compared"
					switch {
					case isExcluded:
						reasons[reason] = true
						excluded++
					case arm.Library == f.Denominator:
						ratio, verdict = "—", "the denominator"
						compared++
					case !base.present:
						missing[name(f.Denominator, f.Name, op, size.Name)] = true
					default:
						// The throughput this arm gives up against the family's denominator: positive means this arm
						// moves fewer messages a second. Same function, same
						// direction as docs/performance/overhead.md's
						// "throughput loss" column.
						loss := budgets.ThroughputLossPct(c.med, base.med)
						ratio = fmt.Sprintf("%+.1f%%", loss)
						verdict = "compared"
						if spread := base.half + c.half; spread > abs(loss) {
							verdict = fmt.Sprintf("compared; spread ±%.1f%% > effect", spread)
						}
						compared++
					}
					fmt.Printf("| `%s` | %s | %s | `%s` | %.0f ±%.1f%% (n=%d) | %.0f | %s | %s |\n",
						f.Name, op, size.Name, arm.Library, c.med, c.half, c.n, c.allocs, ratio, verdict)
				}
			}
		}
	}

	fmt.Printf("\n%d compared row(s), %d measured and not compared.\n", compared, excluded)

	fmt.Println("\n### Semantics each family was configured to")
	for _, f := range competitors.Families() {
		fmt.Printf("\n**`%s`** — %s\n\n*How that was established:* %s\n\n*Denominator:* `%s`.\n",
			f.Name, f.Semantics, f.Proof, f.Denominator)
		for _, a := range competitors.ArmsFor(f.Name) {
			fmt.Printf("\n* `%s` %s — %s\n", a.Library, a.Version, a.Config)
		}
	}

	fmt.Println("\n### Excluded from comparison, with the reason")
	for _, e := range competitors.Exclusions() {
		if !reasons[e.Reason] && !strings.Contains(e.Reason, "No such arm exists") {
			continue
		}
		fmt.Printf("\n* **%s / %s / %s** — %s\n", e.Library, e.Family, e.Op, e.Reason)
	}

	fmt.Println("\n### Pinned versions")
	fmt.Println()
	for _, line := range competitors.PinnedList() {
		fmt.Printf("* `%s`\n", strings.Replace(line, " ", "` `", 1))
	}

	if len(missing) > 0 {
		names := make([]string, 0, len(missing))
		for n := range missing {
			names = append(names, n)
		}
		sort.Strings(names)
		fmt.Printf("\n### Not in this run\n\nThe run holds no row for these, so nothing above them is a comparison: %s\n",
			strings.Join(names, ", "))
	}
}

// readResults reads a stored run's rows, gzipped or not.
//
// A stored run commits `raw.txt.gz` and .gitignore excludes `raw.txt`, so the
// only file a fresh checkout has is the compressed one -- which means a
// renderer that could not read it made this page's own "Reproduce" line
// unrunnable by anybody but the agent that took the measurement. The same
// shape as `benchgate`'s readResults, for the same reason stated there: the
// evidence a published figure is read off is a committed campaign file and
// not a fresh terminal capture.
func readResults(path string) ([]benchfmt.Result, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()
	var in io.Reader = f
	if strings.HasSuffix(path, ".gz") {
		zr, zerr := gzip.NewReader(f)
		if zerr != nil {
			return nil, fmt.Errorf("%s: %w", path, zerr)
		}
		defer func() { _ = zr.Close() }()
		in = zr
	}
	return benchfmt.Parse(in)
}

// name builds a canonical layer-5 benchmark name.
func name(library, family, op, size string) string {
	return strings.Join([]string{"BenchmarkCompare", library, family, op, size}, "/")
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

func abs(f float64) float64 {
	if f < 0 {
		return -f
	}
	return f
}
