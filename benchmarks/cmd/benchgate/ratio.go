package main

import (
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/iqhive/iqmq/benchmarks/internal/benchfmt"
	"github.com/iqhive/iqmq/benchmarks/internal/budgets"
	"github.com/iqhive/iqmq/benchmarks/internal/stat"
)

// ratioRow is one `kind: ratio` budget evaluated over a single run:
// the performance budgets's "core overhead vs raw client", which is a ratio
// between two DIFFERENT benchmarks and not a ceiling on either of them.
//
// A row whose two patterns are not BOTH present is INCONCLUSIVE and never
// passing. That is assumption A25.9's rule and this budget is the shape that
// invites breaking it: a ratio whose denominator was not measured has no
// value, and the natural implementation -- skip the row -- is how three
// budgets and all five §8.10 targets came to match nothing for four
// increments.
type ratioRow struct {
	numerator, denominator string
	spec                   string
	maxPct                 float64
	numName, denName       string
	numNs, denNs           float64
	numN, denN             int
	numCIPct, denCIPct     float64
	lossPct                float64
	// netPct and upperPct are the loss read at the two ends of the two
	// medians' own confidence intervals: netPct with both intervals in the
	// driver arm's favour, upperPct with both against it. The verdict is
	// decided on those rather than on lossPct alone.
	netPct, upperPct float64
	breach           bool
	// undecided is a row that WAS measured and whose interval straddles the
	// ceiling: neither a breach nor something this run may call met.
	undecided    bool
	inconclusive bool
	reason       string
	waived       string
}

// matchOneGroup finds the single measured row a ratio pattern names. A pattern
// that matches more than one row is reported rather than resolved: which of
// two rows the ratio meant is not a thing this tool may guess, and
// `benchgate -lint` refuses such a pattern against the stored names.
func matchOneGroup(pat string, groups []*benchfmt.Group) (*benchfmt.Group, int) {
	var found *benchfmt.Group
	n := 0
	for _, g := range groups {
		if budgets.Match(pat, g.Name) {
			n++
			found = g
		}
	}
	if n != 1 {
		return nil, n
	}
	return found, 1
}

// ratioRows evaluates every ratio budget the named gate set is responsible
// for, over one side's groups, and returns the rows plus how many of them are
// a breach, how many could not be decided, and how many ratio budgets belong
// to another gate set entirely.
//
// The scoping is deliberate and is a decision rather than a derivation. A
// ratio needs both arms in one run and no gate set measures them by accident:
// the `pr` and `nightly` sets are interleaved Go-benchmark runs with no broker
// at all, so a ratio row left in their scope could only ever be inconclusive,
// and a gate that is red at every run for a reason that is not about the code
// is the failure mode this repository has recorded six times. So a ratio
// budget names its gate set(s) in `gates:`, a set it does not name reports it
// as out of scope, and a set it DOES name is held to A25.9's rule in full.
//
// `undecided` and `inconclusive` are counted apart because they have
// different owners: an inconclusive row needs a run that measures both arms,
// an undecided one needs a quieter host or more samples. Both are "no
// verdict" and neither is a pass.
func ratioRows(cfg *budgets.Config, gate string, groups []*benchfmt.Group, now time.Time) (rows []ratioRow, breaches, undecided, inconclusive, outOfScope int) {
	for i := range cfg.Budgets {
		b := &cfg.Budgets[i]
		if !b.IsRatio() {
			continue
		}
		if !b.InGate(gate) {
			outOfScope++
			continue
		}
		r := ratioRow{numerator: b.Numerator, denominator: b.Denominator, spec: b.Spec, maxPct: b.MaxPct}
		ng, nn := matchOneGroup(b.Numerator, groups)
		dg, dn := matchOneGroup(b.Denominator, groups)
		switch {
		case nn == 0:
			r.inconclusive, r.reason = true, "the run holds no row matching the numerator"
		case nn > 1:
			r.inconclusive, r.reason = true, fmt.Sprintf("%d rows match the numerator; a ratio needs exactly one on each side", nn)
		case dn == 0:
			r.inconclusive, r.reason = true, "the run holds no row matching the denominator, so the ratio has no value"
		case dn > 1:
			r.inconclusive, r.reason = true, fmt.Sprintf("%d rows match the denominator; a ratio needs exactly one on each side", dn)
		}
		if !r.inconclusive {
			r.numName, r.denName = ng.Name, dg.Name
			r.numNs, r.numCIPct, r.numN = summariseNs(ng)
			r.denNs, r.denCIPct, r.denN = summariseNs(dg)
			switch {
			case r.numNs <= 0 || r.denNs <= 0:
				r.inconclusive, r.reason = true, "one of the two medians is zero, so the ratio is undefined"
			default:
				r.lossPct = budgets.ThroughputLossPct(r.numNs, r.denNs)
				// The verdict is read on the INTERVAL and not on the two
				// medians alone. A ratio row has two
				// medians and two confidence intervals, and until this it
				// compared the point estimate with the ceiling and stopped
				// there -- which is not what the gate does anywhere else:
				// the base-against-candidate path nets both intervals off
				// the effect before calling a regression
				// and marks a median nobody can bound undecided rather than
				// met.
				//
				// Additive, which is A25.16's arithmetic exactly, so that a
				// reader can recompute the verdict from the loss and the two
				// half-widths this table prints. Root-sum-square would keep
				// two more breaches on the stored run and choosing it for
				// that reason would be picking the arithmetic by its
				// outcome.
				r.netPct = r.lossPct - r.numCIPct - r.denCIPct
				r.upperPct = r.lossPct + r.numCIPct + r.denCIPct
				switch {
				case r.netPct > r.maxPct:
					r.breach = true
				case r.upperPct <= r.maxPct:
					// Met, and met at both ends of both intervals.
				default:
					r.undecided = true
					r.reason = fmt.Sprintf("the %.0f%% ceiling lies inside the combined interval: the loss is %+.1f%% and the two medians are ±%.1f%% and ±%.1f%%, so this run reaches neither verdict",
						r.maxPct, r.lossPct, r.numCIPct, r.denCIPct)
				}
			}
		}
		// A waiver is matched against the numerator, which is the row a waiver
		// would be written about. It clears the BREACH and leaves both
		// no-verdict states alone: "not measured" is not a thing a waiver may
		// turn into a pass , and neither is "this run could
		// not decide it".
		if w, ok := cfg.Waived(r.numName, now); ok && r.numName != "" {
			r.waived = w.Issue
			r.breach = false
		}
		if r.breach {
			breaches++
		}
		if r.undecided {
			undecided++
		}
		if r.inconclusive {
			inconclusive++
		}
		rows = append(rows, r)
	}
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].numerator != rows[j].numerator {
			return rows[i].numerator < rows[j].numerator
		}
		return rows[i].denominator < rows[j].denominator
	})
	return rows, breaches, undecided, inconclusive, outOfScope
}

func summariseNs(g *benchfmt.Group) (med, halfPct float64, n int) {
	xs := g.Samples["ns/op"]
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

// ratioMarkdown renders the ratio section of a verdict.
func ratioMarkdown(b *strings.Builder, rows []ratioRow, outOfScope int, gate string) {
	if len(rows) == 0 && outOfScope == 0 {
		return
	}
	fmt.Fprintf(b, "\n### §10.2 overhead ratios (`kind: ratio`, gate set `%s`)\n\n", gate)
	if len(rows) == 0 {
		fmt.Fprintf(b, "No ratio budget belongs to this gate set; %d belong to another one.\n", outOfScope)
		return
	}
	b.WriteString("| numerator | denominator | numerator ns/op | denominator ns/op | throughput loss | net of CI | §10.2 budget | verdict |\n|---|---|---|---|---|---|---|---|\n")
	for i := range rows {
		r := &rows[i]
		num, den, loss, net, verdict := r.numerator, r.denominator, "—", "—", "met"
		switch {
		case r.inconclusive:
			verdict = "**INCONCLUSIVE** — " + r.reason
		case r.undecided:
			verdict = "**UNDECIDED** — " + r.reason
		case r.breach:
			verdict = "**BREACH**"
		}
		if r.waived != "" {
			verdict = "waived (" + r.waived + ")"
		}
		numCell, denCell := "not measured", "not measured"
		if r.numN > 0 {
			numCell = fmt.Sprintf("%.0f ±%.1f%% (n=%d)", r.numNs, r.numCIPct, r.numN)
		}
		if r.denN > 0 {
			denCell = fmt.Sprintf("%.0f ±%.1f%% (n=%d)", r.denNs, r.denCIPct, r.denN)
		}
		if !r.inconclusive {
			loss = fmt.Sprintf("%+.1f%%", r.lossPct)
			net = fmt.Sprintf("%+.1f%% … %+.1f%%", r.netPct, r.upperPct)
		}
		fmt.Fprintf(b, "| `%s` | `%s` | %s | %s | %s | %s | ≤ %.0f%% | %s |\n", num, den, numCell, denCell, loss, net, r.maxPct, verdict)
	}
	if outOfScope > 0 {
		fmt.Fprintf(b, "\n%d ratio budget(s) belong to another gate set and were not evaluated here.\n", outOfScope)
	}
	b.WriteString("\nThe figure is the throughput LOST, not the duration added: an arm 10 % slower\n")
	b.WriteString("per operation moves 9.09 % fewer messages a second, so §10.2's 5 % batched\n")
	b.WriteString("budget permits an operation 5.26 % longer (assumption B11.9). A row whose two\n")
	b.WriteString("patterns are not both present is inconclusive, never passing (assumption A25.9).\n")
	b.WriteString("\nThe verdict is read on the `net of CI` column and not on the loss alone\n")
	b.WriteString("(assumption C01.2): a row is a BREACH when the loss clears the ceiling with both\n")
	b.WriteString("medians' confidence intervals read in the driver arm's favour, `met` when it is\n")
	b.WriteString("under the ceiling with both read against it, and UNDECIDED when the ceiling lies\n")
	b.WriteString("between the two -- which is neither a breach nor a pass. Both ends are the loss\n")
	b.WriteString("plus or minus the sum of the two half-widths printed beside the medians, so every\n")
	b.WriteString("verdict in this table can be recomputed from the table.\n")
}

// runRatios is the `-ratios` entry point: §10.2's ratio budgets over ONE
// results file, which is what `hack/bench/run-local.sh` produces.
func runRatios(budgetsPath, resultsPath, gate, out, sysinfo string) int {
	cfg, err := budgets.Load(budgetsPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, "benchgate -ratios:", err)
		return 2
	}
	if _, ok := cfg.GateSets[gate]; !ok {
		fmt.Fprintf(os.Stderr, "benchgate -ratios: %q is not a gate set in %s\n", gate, budgetsPath)
		return 2
	}
	rs, err := readResults(resultsPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, "benchgate -ratios:", err)
		return 2
	}
	groups := benchfmt.GroupByName(rs)
	rows, breaches, undecided, inconclusive, outOfScope := ratioRows(cfg, gate, groups, time.Now())
	if len(rows) == 0 {
		// A gate set with no ratio budget of its own is the same failure this
		// repository keeps finding: a verdict that says PASS over an empty
		// table.
		fmt.Fprintf(os.Stderr, "!! benchgate -ratios: no `kind: ratio` budget names gate set %q, so nothing was compared\n", gate)
		return 2
	}
	status := "PASS"
	code := 0
	switch {
	case breaches > 0:
		status, code = "FAIL", 1
	case undecided+inconclusive > 0:
		status, code = "INCONCLUSIVE", 3
	}
	var b strings.Builder
	fmt.Fprintf(&b, "## benchgate -ratios: %s (gate set `%s`)\n\n", status, gate)
	if sysinfo != "" {
		fmt.Fprintf(&b, "sysinfo: `%s` (provisional host)\n\n", sysinfo)
	}
	fmt.Fprintf(&b, "Results: `%s`\n\n", resultsPath)
	fmt.Fprintf(&b, "Ratio rows: %d · Breaches: %d · Undecided: %d · Inconclusive: %d · Out of this gate set: %d\n",
		len(rows), breaches, undecided, inconclusive, outOfScope)
	ratioMarkdown(&b, rows, outOfScope, gate)
	specs := map[string]bool{}
	for i := range rows {
		if rows[i].spec != "" {
			specs[rows[i].spec] = true
		}
	}
	if len(specs) > 0 {
		cited := make([]string, 0, len(specs))
		for sp := range specs {
			cited = append(cited, sp)
		}
		sort.Strings(cited)
		b.WriteString("\nSpecification rows these ceilings come from:\n")
		for _, sp := range cited {
			fmt.Fprintf(&b, "\n* %s\n", sp)
		}
	}
	md := b.String()
	if out != "" {
		if err := os.WriteFile(out, []byte(md), 0o644); err != nil {
			fmt.Fprintln(os.Stderr, "benchgate -ratios:", err)
			return 2
		}
	}
	fmt.Print(md)
	if code != 0 {
		// The `!!` marker for the same reason `-lint` carries it: "green" is a
		// log with no FAIL and no !! line in it, and `make` flattens a status.
		fmt.Fprintf(os.Stderr, "!! benchgate -ratios: %d breach(es), %d undecided and %d inconclusive row(s) of §10.2's overhead budgets\n", breaches, undecided, inconclusive)
	}
	return code
}
