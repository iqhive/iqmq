// Command benchgate compares two benchmark runs and enforces the regression
// policy of specification §10.6: a statistically significant slowdown above
// the threshold, any allocs/op increase, a geomean regression across the
// gate set, or a breached absolute budget fails the gate.
//
//	benchgate -base base.txt -head head.txt [-budgets budgets.yaml] [-gate pr] [-out verdict.md]
//	benchgate -ratios -run <run>/raw.txt.gz [-gate overhead] [-out verdict.md]
//	benchgate -self-test
//	benchgate -lint [-budgets budgets.yaml] [-results DIR] [-scorecards DIR]
//
// TWO PATHS, TWO FLAGS. `-run` is ONE run's raw benchmark output -- the
// `raw.txt` or `raw.txt.gz` of a single campaign run, which is the only shape a
// `kind: ratio` budget can be evaluated over -- and `-results` is the stored
// campaign DIRECTORY that `-lint` takes an inventory of. Both were spelled
// `-results` until assumption C17.1, and each is now REFUSED outside its own
// mode rather than read by the wrong consumer or silently ignored.
//
// -ratios is the performance budgets's first two budgets, which are the only ones
// in the file that are not about one benchmark: they are a ratio between two
// DIFFERENT benchmarks measured in the SAME run --
// `BenchmarkDriver/<driver>/<op>/<size>` over
// `BenchmarkNative/<client>/<op>/<size>` -- so they are evaluated inside one
// results file rather than across a base and a candidate.
// `hack/bench/run-local.sh` writes exactly that input: both arms into one
// raw.txt, against one container, inside one guarded window, with the arm
// order reversed every other round. Nothing else in the repository produces a
// file with two arms in it.
//
// Exit status: 0 pass, 1 regression or budget breach, 2 error -- which
// includes a gate set that selected no benchmark at all, because a comparison
// over nothing is not a pass  -- 3 inconclusive (no
// regression found but at least one gate benchmark had a confidence interval
// too wide to decide).
package main

import (
	"compress/gzip"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/iqhive/iqmq/benchmarks/internal/benchfmt"
	"github.com/iqhive/iqmq/benchmarks/internal/budgets"
	"github.com/iqhive/iqmq/benchmarks/internal/stat"
)

// The DEFAULTS of the two path flags, named because their SHAPES are the whole
// of the split. `make budgets-lint` (Makefile) invokes `-lint` with no
// `-results` at all, so the stored-campaign default has to stay a DIRECTORY
// that exists relative to `benchmarks/`; repointing it at a file would leave
// `-lint` reading nothing and reporting nothing. `-ratios` has no default,
// because a default run is not a thing: it names ONE run or it is refused.
const (
	defaultResultsDir = "reports/results"
	defaultRunFile    = ""
)

func main() { os.Exit(run(os.Args[0], os.Args[1:], os.Stderr)) }

// run is main with its arguments and its error stream handed in. The flag
// wiring is what assumption C17.1 changed, and an instrument nobody has watched
// fail is assumed not to work, so the dispatch has to be reachable from a test
// rather than only from a shell caller: `flags_test.go` drives exactly this
// function.
func run(prog string, args []string, stderr io.Writer) int {
	fs := flag.NewFlagSet(prog, flag.ContinueOnError)
	fs.SetOutput(stderr)
	base := fs.String("base", "", "benchmark output of the baseline")
	head := fs.String("head", "", "benchmark output of the candidate")
	budgetsPath := fs.String("budgets", "benchmarks/budgets.yaml", "budgets file")
	gate := fs.String("gate", "pr", "gate set name")
	out := fs.String("out", "", "write the markdown verdict to this file")
	sysinfo := fs.String("sysinfo", "", "sysinfo JSON path to reference in the verdict")
	selfTest := fs.Bool("self-test", false, "run the built-in fixtures and exit")
	lintOnly := fs.Bool("lint", false, "check the budgets document against the stored campaign and exit")
	ratiosOnly := fs.Bool("ratios", false, "evaluate the gate set's kind: ratio budgets over one run's results and exit")
	// A TIME ceiling is a figure measured on one machine. `budgets.yaml` records
	// the run each one came from and declares `hosts: {authoritative: []}`, so no
	// host is ratified to judge them -- and the file's own argument for which rows
	// may carry an absolute ceiling at all is about stability WITHIN a host: "an
	// absolute ceiling on one of those is a coin toss, and a gate that fails at
	// random gets switched off". Across hosts the coin is weighted differently
	// again. `BenchmarkAPI/Dispatch/interface/P=1` is budgeted at 11 ns/op from a
	// measured 10.66 that two campaign runs put 1.3% apart; two hosted-runner
	// draws of the SAME commit measured 10.1 and 31.9, and the 31.9 failed a pull
	// request whose base measured 32.4 on the same runner -- so the head was
	// faster and the gate called it a breach.
	//
	// This drops the TIME ceilings only. allocs/op and B/op are deterministic
	// counts that transfer between machines exactly, every head-against-base
	// comparison still applies because interleaving measures both sides on the
	// one host, and so do the kind: ratio budgets, which are ratios. It is the
	// same scope assumption A25.5 gives a waiver, for a reason about the host
	// rather than about ownership.
	provisionalHost := fs.Bool("provisional-host", false, "do not judge TIME ceilings (ns/op): this host is not ratified to measure them")
	results := fs.String("results", defaultResultsDir, "the stored campaign results DIRECTORY, for -lint")
	runFile := fs.String("run", defaultRunFile, "ONE run's raw.txt or raw.txt.gz, for -ratios")
	scorecards := fs.String("scorecards", "reports/scorecards", "stored per-family scorecards, for -lint")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}

	// Which of the two path flags the caller actually TYPED, as against which
	// ones carry a default. A wrong-way-round invocation is refused here, by
	// name, instead of being handed to a consumer that cannot use it: the two
	// forms that already failed on an `os` error keep failing, and the two that
	// would have been silently IGNORED -- a `-run` in a mode that does not read
	// it, a `-results` in a mode that does not read it -- now fail too, because
	// a path argument that goes nowhere is how a gate becomes a check that
	// passes by finding nothing.
	typed := map[string]bool{}
	fs.Visit(func(f *flag.Flag) { typed[f.Name] = true })
	if typed["run"] && !*ratiosOnly {
		_, _ = fmt.Fprintln(stderr, "benchgate: -run names ONE run's raw.txt or raw.txt.gz and is read only by -ratios; the stored campaign DIRECTORY that -lint takes an inventory of is -results")
		return 2
	}
	if typed["results"] && *ratiosOnly {
		_, _ = fmt.Fprintln(stderr, "benchgate -ratios: -results is the stored campaign DIRECTORY, read only by -lint; pass this run's raw.txt or raw.txt.gz as -run")
		return 2
	}

	if *selfTest {
		return runSelfTest()
	}
	if *lintOnly {
		return lint(*budgetsPath, *results, *scorecards)
	}
	if *ratiosOnly {
		if *runFile == "" {
			_, _ = fmt.Fprintln(stderr, "benchgate -ratios: -run is required: it names ONE run's raw.txt or raw.txt.gz, the only shape a kind: ratio budget can be evaluated over")
			return 2
		}
		return runRatios(*budgetsPath, *runFile, *gate, *out, *sysinfo)
	}
	if *base == "" || *head == "" {
		_, _ = fmt.Fprintln(stderr, "benchgate: -base and -head are required")
		return 2
	}
	cfg, err := budgets.Load(*budgetsPath)
	if err != nil {
		_, _ = fmt.Fprintln(stderr, "benchgate:", err)
		return 2
	}
	baseRes, err := readResults(*base)
	if err != nil {
		_, _ = fmt.Fprintln(stderr, "benchgate:", err)
		return 2
	}
	headRes, err := readResults(*head)
	if err != nil {
		_, _ = fmt.Fprintln(stderr, "benchgate:", err)
		return 2
	}
	v := compare(cfg, *gate, baseRes, headRes, time.Now(), *provisionalHost)
	md := v.markdown(*sysinfo)
	if *out != "" {
		if err := os.WriteFile(*out, []byte(md), 0o644); err != nil {
			_, _ = fmt.Fprintln(stderr, "benchgate:", err)
			return 2
		}
	}
	fmt.Print(md)
	return v.exitCode()
}

func readResults(path string) ([]benchfmt.Result, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()
	// A stored run's rows are gzipped (`raw.txt.gz`), and a stored run is
	// exactly what `-ratios` is pointed at: the evidence a §10.2 ratio is read
	// off is a committed campaign file, not a fresh terminal capture.
	var in io.Reader = f
	if strings.HasSuffix(path, ".gz") {
		zr, zerr := gzip.NewReader(f)
		if zerr != nil {
			return nil, fmt.Errorf("%s: %w", path, zerr)
		}
		defer func() { _ = zr.Close() }()
		in = zr
	}
	rs, err := benchfmt.Parse(in)
	if err != nil {
		return nil, err
	}
	if len(rs) == 0 {
		return nil, fmt.Errorf("%s: %w", path, benchfmt.ErrNoResults)
	}
	return rs, nil
}

// row is the comparison of one benchmark.
type row struct {
	name         string
	inGate       bool
	baseNs       float64
	headNs       float64
	deltaPct     float64
	p            float64
	ciPct        float64
	baseCIPct    float64
	baseAllocs   float64
	headAllocs   float64
	baseBytes    float64
	headBytes    float64
	regressions  []string
	inconclusive bool
	budgetBreach []string
	// timeCeilingSkipped: this row is over its ns/op ceiling and -provisional-host
	// said this machine may not judge that. Reported, never counted.
	timeCeilingSkipped bool
	waived             string
	waivedAll          bool
	waivedTime         bool
	budgeted           bool
	undecided          bool
}

// targetRow is one §8.10 performance target and what the candidate measured
// against it. Targets are reported, never gated.
type targetRow struct {
	name    string
	unit    string
	figure  float64
	got     float64
	lower   bool
	present bool
}

type verdict struct {
	gate                string
	rows                []row
	geomeanRatio        float64
	geomeanN            int
	thresholdPct        float64
	regressions         int
	breaches            int
	inconclusives       int
	onlyHead            []string
	onlyBase            []string
	missing             []string
	targets             []targetRow
	gateRows            int
	gateSetSize         int
	undecided           int
	geomeanRegressed    bool
	ratios              []ratioRow
	ratioOutOfScope     int
	timeCeilingsSkipped int
	provisionalHost     bool
}

func (v *verdict) exitCode() int {
	if v.gateSetSize > 0 && v.gateRows == 0 {
		return 2
	}
	if v.regressions > 0 || v.breaches > 0 || len(v.missing) > 0 {
		return 1
	}
	if v.inconclusives > 0 {
		return 3
	}
	return 0
}

func compare(cfg *budgets.Config, gate string, baseRes, headRes []benchfmt.Result, now time.Time, provisionalHost bool) *verdict {
	v := &verdict{gate: gate, thresholdPct: cfg.Defaults.ThresholdPct, gateSetSize: len(cfg.GateSets[gate]), provisionalHost: provisionalHost}
	baseGroups := index(benchfmt.GroupByName(baseRes))
	headGroups := benchfmt.GroupByName(headRes)
	var ratios []float64
	for _, hg := range headGroups {
		bg, ok := baseGroups[hg.Name]
		if !ok {
			v.onlyHead = append(v.onlyHead, hg.Name)
		}
		r := row{name: hg.Name, inGate: cfg.InGateSet(gate, hg.Name)}
		for i := range cfg.Budgets {
			if budgets.Match(cfg.Budgets[i].Match, hg.Name) {
				r.budgeted = true
				break
			}
		}
		if r.inGate {
			v.gateRows++
		}
		if w, ok := cfg.Waived(hg.Name, now); ok {
			r.waived = w.Issue
			r.waivedAll = w.WaivesRegressions()
			r.waivedTime = w.WaivesTime()
		}
		headNs := hg.Samples["ns/op"]
		r.headNs = stat.Median(headNs)
		r.headAllocs = stat.Median(hg.Samples["allocs/op"])
		r.headBytes = stat.Median(hg.Samples["B/op"])
		if ok {
			baseNs := bg.Samples["ns/op"]
			r.baseNs = stat.Median(baseNs)
			r.baseAllocs = stat.Median(bg.Samples["allocs/op"])
			r.baseBytes = stat.Median(bg.Samples["B/op"])
			if r.baseNs > 0 && len(headNs) > 0 {
				r.deltaPct = (r.headNs - r.baseNs) / r.baseNs * 100
				_, r.p = stat.MannWhitney(baseNs, headNs)
				// Same quantity as the B/op guard below computes for its own
				// metric, through the same helper, so the two cannot drift
				// apart in how they measure a median's precision.
				r.ciPct = medianCIPct(headNs, r.headNs)
				r.baseCIPct = medianCIPct(baseNs, r.baseNs)
				if r.inGate {
					// A row waived at scope: all is out of the comparison
					// altogether, geomean included -- otherwise waiving it
					// leaves it able to fail the gate through the geomean,
					// which is where the waiver looks like it works and does
					// not.
					if !r.waivedTime {
						ratios = append(ratios, r.headNs/r.baseNs)
					}
					// The effect has to clear the threshold after the two
					// medians' own confidence intervals are taken off it: a
					// +7.3 % "regression" on a package the branch does not
					// touch, with ±3.7 % and ±3.0 % intervals either side,
					// is the host and not the code.
					netPct := r.deltaPct - r.ciPct - r.baseCIPct
					switch {
					case netPct > cfg.Defaults.ThresholdPct && r.p < cfg.Defaults.Alpha:
						r.regressions = append(r.regressions, fmt.Sprintf("ns/op +%.1f%% (net of CI +%.1f%%, p=%.3f)", r.deltaPct, netPct, r.p))
					case r.ciPct > cfg.Defaults.CIHalfwidthRejectPct:
						// Too wide to decide. It fails the gate only on a row
						// that carries an absolute ceiling: there the gate is
						// asserting "this median is under N" and must not
						// claim it from a median nobody can bound. On a row
						// that is only compared, "we could not tell" is not a
						// regression, and §10.6 asks the gate to fail on a
						// significant regression rather than on the absence of
						// an answer -- the five retained `Run/Dispatch`
						// profiles came back at ±9.7-30.4 % on a host with a
						// sibling build running, while the bound default beside
						// them was decided.
						r.undecided = true
						r.inconclusive = r.budgeted
					}
				}
			}
			if _, has := hg.Samples["allocs/op"]; has && r.inGate && r.headAllocs > r.baseAllocs+cfg.Defaults.AllocsTolerance {
				r.regressions = append(r.regressions, fmt.Sprintf("allocs/op %.0f -> %.0f", r.baseAllocs, r.headAllocs))
			}
			if r.inGate && r.baseBytes > 0 {
				baseB, headB := bg.Samples["B/op"], hg.Samples["B/op"]
				bd := (r.headBytes - r.baseBytes) / r.baseBytes * 100
				_, pb := stat.MannWhitney(baseB, headB)
				// The same net-of-CI rule the ns/op guard above applies, and
				// for the same reason: a delta inside the two medians' own
				// intervals is the host and not the code. It was raw here
				// while ns/op subtracted both intervals, and the rows the
				// difference bit are waived with the words "median CI is
				// 3-28 % at twenty samples a side" -- so the gate was
				// comparing a noisy number against a 3 % threshold on one
				// metric and a quiet one on another (assumption C33.5
				// recorded the suspicion; this acts on it).
				//
				// These are B/op's OWN intervals. Reusing r.ciPct would be a
				// category error: that is ns/op's, and a row can be noisy in
				// time and exact in bytes or the other way about.
				//
				// It does NOT weaken the deterministic case, which is most of
				// them: identical samples give a zero-width interval, so the
				// net equals the raw delta and a +1-allocation-sized jump in
				// a row that always reports the same B/op is caught exactly
				// as before. The guard can only move a row whose bytes
				// actually vary between samples.
				netB := bd - medianCIPct(headB, r.headBytes) - medianCIPct(baseB, r.baseBytes)
				if netB > cfg.Defaults.BytesThresholdPct && pb < cfg.Defaults.Alpha {
					r.regressions = append(r.regressions, fmt.Sprintf("B/op +%.1f%% (net of CI +%.1f%%, p=%.3f)", bd, netB, pb))
				}
			}
			if bm, ok := hg.Samples["msgs/s"]; ok && r.inGate {
				bb := bg.Samples["msgs/s"]
				if mb, mh := stat.Median(bb), stat.Median(bm); mb > 0 {
					d := (mh - mb) / mb * 100
					_, pm := stat.MannWhitney(bb, bm)
					// Same noise floor as ns/op, from the same intervals.
					if d+r.ciPct+r.baseCIPct < -cfg.Defaults.ThresholdPct && pm < cfg.Defaults.Alpha {
						r.regressions = append(r.regressions, fmt.Sprintf("msgs/s %.1f%% (p=%.3f)", d, pm))
					}
				}
			}
		}
		for i := range cfg.Budgets {
			b := &cfg.Budgets[i]
			if !budgets.Match(b.Match, hg.Name) {
				continue
			}
			if b.NsOpSet && r.headNs > b.NsOpMax {
				switch {
				case provisionalHost:
					r.timeCeilingSkipped = true
				default:
					r.budgetBreach = append(r.budgetBreach, fmt.Sprintf("ns/op %.1f > %.0f", r.headNs, b.NsOpMax))
				}
			}
			if _, has := hg.Samples["allocs/op"]; has && b.AllocsSet && r.headAllocs > b.AllocsOpMax {
				r.budgetBreach = append(r.budgetBreach, fmt.Sprintf("allocs/op %.0f > %.0f", r.headAllocs, b.AllocsOpMax))
			}
			if _, has := hg.Samples["B/op"]; has && b.BOpSet && r.headBytes > b.BOpMax {
				r.budgetBreach = append(r.budgetBreach, fmt.Sprintf("B/op %.0f > %.0f", r.headBytes, b.BOpMax))
			}
		}
		if r.waived != "" {
			// A waiver's default scope clears the absolute ceiling only: the
			// row is still held to not getting worse than the base, because a
			// waiver that blinds the comparison as a side effect is how a
			// gate stops working without anybody editing it.
			r.budgetBreach = nil
			if r.waivedTime {
				// Drop the time half and keep the exact counts.
				kept := r.regressions[:0]
				for _, g := range r.regressions {
					if strings.HasPrefix(g, "ns/op") || strings.HasPrefix(g, "msgs/s") {
						continue
					}
					kept = append(kept, g)
				}
				r.regressions = kept
				r.inconclusive = false
			}
			if r.waivedAll {
				r.regressions = nil
			}
		}
		v.regressions += len(r.regressions)
		v.breaches += len(r.budgetBreach)
		if r.timeCeilingSkipped {
			v.timeCeilingsSkipped++
		}
		if r.undecided {
			v.undecided++
		}
		if r.inconclusive {
			v.inconclusives++
		}
		v.rows = append(v.rows, r)
	}
	for name := range baseGroups {
		found := false
		for _, hg := range headGroups {
			if hg.Name == name {
				found = true
				break
			}
		}
		if !found {
			v.onlyBase = append(v.onlyBase, name)
			// A gate benchmark the candidate no longer has is not a pass: the
			// comparison for it silently disappears.
			if cfg.InGateSet(gate, name) {
				v.missing = append(v.missing, name)
			}
		}
	}
	sort.Strings(v.onlyBase)
	sort.Strings(v.missing)
	v.targets = targetRows(cfg, headGroups)
	// §10.2's ratio budgets, over the CANDIDATE side alone: a ratio is two
	// rows of one run, not one row of two runs. Only the
	// budgets that name this gate set are evaluated; the rest are counted and
	// reported, because a budget silently out of scope is indistinguishable
	// from one that is met.
	var rBreach, rUndec, rInconc int
	v.ratios, rBreach, rUndec, rInconc, v.ratioOutOfScope = ratioRows(cfg, gate, headGroups, now)
	v.breaches += rBreach
	// Both no-verdict states count as inconclusive for the exit status: a
	// ratio row whose ceiling lies inside its combined interval is not a pass
	// , any more than one whose denominator was never
	// measured is.
	v.inconclusives += rUndec + rInconc
	if len(ratios) > 0 {
		v.geomeanRatio = stat.Geomean(ratios)
		v.geomeanN = len(ratios)
		if (v.geomeanRatio-1)*100 > cfg.Defaults.ThresholdPct {
			v.geomeanRegressed = true
			v.regressions++
		}
	}
	return v
}

// targetRows reads each §8.10 target off the candidate's own run. Nothing in
// the repository reported these before: the figures were parsed and dropped,
// which for a "reported, never gated" number is the same as not having it.
func targetRows(cfg *budgets.Config, headGroups []*benchfmt.Group) []targetRow {
	var out []targetRow
	for _, t := range cfg.Targets {
		for _, m := range t.Metrics() {
			row := targetRow{unit: m.Unit, figure: m.Figure, lower: m.LowerIsOK}
			for _, hg := range headGroups {
				if !budgets.Match(t.Match, hg.Name) {
					continue
				}
				samples, ok := hg.Samples[m.Unit]
				if !ok {
					continue
				}
				got := stat.Median(samples)
				// One target may cover several rows; report the worst.
				if !row.present || (m.LowerIsOK && got > row.got) || (!m.LowerIsOK && got < row.got) {
					row.name, row.got, row.present = hg.Name, got, true
				}
			}
			if row.name == "" {
				row.name = t.Match
			}
			out = append(out, row)
		}
	}
	return out
}

func index(gs []*benchfmt.Group) map[string]*benchfmt.Group {
	m := make(map[string]*benchfmt.Group, len(gs))
	for _, g := range gs {
		m[g.Name] = g
	}
	return m
}

func (v *verdict) markdown(sysinfo string) string {
	var b strings.Builder
	status := "PASS"
	switch v.exitCode() {
	case 1:
		status = "FAIL"
	case 2:
		status = "ERROR"
	case 3:
		status = "INCONCLUSIVE"
	}
	fmt.Fprintf(&b, "## benchgate: %s (gate set `%s`)\n\n", status, v.gate)
	if sysinfo != "" {
		fmt.Fprintf(&b, "sysinfo: `%s` (provisional host)\n\n", sysinfo)
	}
	if v.geomeanN > 0 {
		note := ""
		if v.geomeanRegressed {
			note = " — **above the threshold**"
		}
		fmt.Fprintf(&b, "Geomean of gate-set ns/op ratios (head/base) over %d benchmarks: **%.4f** (threshold +%.0f%%)%s\n\n",
			v.geomeanN, v.geomeanRatio, v.thresholdPct, note)
	}
	fmt.Fprintf(&b, "Regressions: %d · Budget breaches: %d · Inconclusive: %d (of %d undecided) · Gate rows: %d of %d pattern(s)\n\n",
		v.regressions, v.breaches, v.inconclusives, v.undecided, v.gateRows, v.gateSetSize)
	// Said in the verdict, not only in the job that passed the flag: a reader of
	// a green gate has to be able to see that a ceiling went unjudged, or
	// `-provisional-host` becomes a silent way to switch part of the gate off.
	if v.provisionalHost {
		fmt.Fprintf(&b, "`-provisional-host`: TIME ceilings (ns/op) were NOT judged on this machine, and %d row(s) are over one. "+
			"Absolute times do not transfer between hosts and `budgets.yaml` declares no authoritative host; "+
			"head-against-base comparisons, allocation and byte ceilings, and ratio budgets all still applied.\n\n",
			v.timeCeilingsSkipped)
	}
	if v.gateSetSize > 0 && v.gateRows == 0 {
		fmt.Fprintf(&b, "**Gate set `%s` selected no benchmark in the candidate run — nothing was compared.**\n\n", v.gate)
	}
	b.WriteString("| benchmark | gate | base ns/op | head ns/op | Δ | p | base CI± | head CI± | allocs | verdict |\n|---|---|---|---|---|---|---|---|---|---|\n")
	for i := range v.rows {
		r := &v.rows[i]
		verdictText := "ok"
		notes := make([]string, 0, len(r.regressions)+len(r.budgetBreach))
		notes = append(notes, r.regressions...)
		for _, br := range r.budgetBreach {
			notes = append(notes, "budget "+br)
		}
		switch {
		case len(notes) > 0:
			verdictText = "**" + strings.Join(notes, "; ") + "**"
		case r.timeCeilingSkipped:
			verdictText = "over its ns/op ceiling; NOT JUDGED (provisional host)"
		case r.undecided:
			verdictText = "inconclusive"
			if !r.budgeted {
				verdictText = "undecided (no ceiling)"
			}
		}
		// A waiver REPLACES the verdict text only when there is nothing left to
		// say. It used to replace it unconditionally, which hid a counted
		// regression behind the word "waived": a `scope: time` waiver drops the
		// ns/op and msgs/s halves and deliberately KEEPS the exact counts
		// , so a surviving `allocs/op` or `B/op` regression
		// was added to `v.regressions`, failed the job, and then had its note
		// thrown away here. #85 hit it -- `Regressions: 1` over a table in which
		// all 51 rows read `ok` or `waived`, and no column shows B/op at all --
		// and a gate that fails without naming the row is past crying wolf.
		if r.waived != "" {
			switch {
			case len(notes) > 0:
				verdictText = "waived (" + r.waived + ") BUT STILL **" + strings.Join(notes, "; ") + "**"
			default:
				verdictText = "waived (" + r.waived + ")"
			}
		}
		gate := ""
		if r.inGate {
			gate = "✓"
		}
		p := ""
		if r.p > 0 {
			p = fmt.Sprintf("%.3f", r.p)
		}
		delta := ""
		if r.baseNs > 0 && !math.IsNaN(r.deltaPct) {
			delta = fmt.Sprintf("%+.2f%%", r.deltaPct)
		}
		fmt.Fprintf(&b, "| %s | %s | %.1f | %.1f | %s | %s | %.1f%% | %.1f%% | %.0f→%.0f | %s |\n",
			r.name, gate, r.baseNs, r.headNs, delta, p, r.baseCIPct, r.ciPct, r.baseAllocs, r.headAllocs, verdictText)
	}
	if len(v.onlyHead) > 0 {
		fmt.Fprintf(&b, "\nNew benchmarks (no baseline): %s\n", strings.Join(v.onlyHead, ", "))
	}
	if len(v.onlyBase) > 0 {
		fmt.Fprintf(&b, "\nMissing from head: %s\n", strings.Join(v.onlyBase, ", "))
	}
	if len(v.missing) > 0 {
		fmt.Fprintf(&b, "\n**Gate benchmarks missing from head: %s**\n", strings.Join(v.missing, ", "))
	}
	ratioMarkdown(&b, v.ratios, v.ratioOutOfScope, v.gate)
	if len(v.targets) > 0 {
		b.WriteString("\n### §8.10 performance targets (reported, never gated)\n\n")
		b.WriteString("| benchmark | metric | target | measured | |\n|---|---|---|---|---|\n")
		for _, t := range v.targets {
			mark, got := "not measured", "—"
			if t.present {
				got = fmt.Sprintf("%.4g", t.got)
				switch {
				case (t.lower && t.got <= t.figure) || (!t.lower && t.got >= t.figure):
					mark = "met"
				default:
					mark = "missed"
				}
			}
			cmp := "≤"
			if !t.lower {
				cmp = "≥"
			}
			fmt.Fprintf(&b, "| %s | %s | %s %.4g | %s | %s |\n", t.name, t.unit, cmp, t.figure, got, mark)
		}
	}
	return b.String()
}

const selfTestBudgets = `
defaults: {threshold_pct: 3, alpha: 0.05, min_count: 10, allocs_tolerance: 0}
gate_sets:
  pr: ["BenchmarkMemory/PubSubAck/*", "BenchmarkHeaders/*"]
  ratio: ["BenchmarkDriver/*", "BenchmarkNative/*"]
budgets:
  - {match: "BenchmarkHeaders/*/Get/n=8", ns_op_max: 15, allocs_op_max: 0, kind: specification, spec: "fixture"}
  # §10.2's two ratio shapes, as fixtures. They name the ` + "`ratio`" + ` gate set, so
  # the ` + "`pr`" + ` cases above also exercise the out-of-scope path: a ratio budget
  # that belongs to another set must not change the exit status of a set that
  # cannot measure it, and must still be counted in the verdict.
  - kind: ratio
    numerator: "BenchmarkDriver/fixture/PublishBatch/64B"
    denominator: "BenchmarkNative/fixture/PublishBatch/64B"
    max_pct: 5
    gates: ["ratio"]
    spec: "fixture §10.2 batched <= 5 % throughput loss"
  - kind: ratio
    numerator: "BenchmarkDriver/fixture/Publish/64B"
    denominator: "BenchmarkNative/fixture/Publish/64B"
    max_pct: 10
    gates: ["ratio"]
    spec: "fixture §10.2 single-message <= 10 % throughput loss"
waivers:
  - {match: "BenchmarkHeaders/waived/Get/n=8", issue: "#0 fixture", expires: "2099-01-01", reason: "fixture"}
  - {match: "BenchmarkMemory/PubSubAck/waivedall/*", issue: "#0 fixture", expires: "2099-01-01", reason: "fixture", scope: all}
  - {match: "BenchmarkMemory/PubSubAck/waivedtime/*", issue: "#0 fixture", expires: "2099-01-01", reason: "fixture", scope: time}
`

// fixture is ten samples of one benchmark. The spread is proportional rather
// than absolute so that a 20 ns row and a 1000 ns row carry the same relative
// confidence interval: an absolute 0.5 ns step made the small rows
// "inconclusive" (a ±5 % median CI is the reject threshold) and that is a
// property of the fixture, not of anything the gate is being asked about.
func fixture(name string, ns, allocs float64) string {
	const n = 10
	var b strings.Builder
	for i := range n {
		fmt.Fprintf(&b, "%s\t1000\t%.4f ns/op\t0 B/op\t%.0f allocs/op\n", name, ns*(1+float64(i)*0.0005), allocs)
	}
	return b.String()
}

// spread is ten samples alternating ±pct around ns, which puts the median's
// confidence interval half-width at pct: it is how a case asks for a
// particular precision rather than hoping the fixture happens to have one.
func spread(name string, ns, pct float64) string {
	var b strings.Builder
	for i := range 10 {
		f := 1 - pct/100
		if i%2 == 1 {
			f = 1 + pct/100
		}
		fmt.Fprintf(&b, "%s\t1000\t%.4f ns/op\t0 B/op\t0 allocs/op\n", name, ns*f)
	}
	return b.String()
}

// noisy is ±20 % around ns, four times the ±5 % reject threshold, for the
// too-wide-to-decide cases.
func noisy(name string, ns float64) string { return spread(name, ns, 20) }

// exact is ten IDENTICAL samples, so the median's confidence interval has
// zero width. It is what a case uses to pin an arithmetic boundary rather
// than a verdict about precision: since assumption C01.2 a ratio row's
// verdict is read on the interval, so §10.2's rate-vs-duration boundary
//
//	can only be asserted as `met` by a measurement precise
//
// enough to separate 4.76 % from 5 %, and no spread this fixture can produce
// is.
func exact(name string, ns float64) string { return spread(name, ns, 0) }

// medianCIPct is the half-width of samples' median confidence interval as a
// percentage of median, which is the quantity both the ns/op and the B/op
// guards subtract from a delta before comparing it against the threshold.
//
// It returns 0 for a median of zero or samples with no spread, which is what
// makes the B/op guard a no-op on the deterministic rows: zero taken off a
// delta leaves the delta.
func medianCIPct(samples []float64, median float64) float64 {
	if median <= 0 || len(samples) == 0 {
		return 0
	}
	lo, hi := stat.MedianCI(samples)
	return (hi - lo) / 2 / median * 100
}

// selfTestGatedRow is the self-test's gated benchmark. It is package level
// because gatedBytesRamp needs it and runSelfTest's own `gated` is a local.
const selfTestGatedRow = "BenchmarkMemory/PubSubAck/1P1C/safe-8"

// gatedBytesRamp is a B/op fixture for the gated row: ten samples spread
// EVENLY across +/-pct around b bytes, at a fixed ns so nothing it produces
// can fail the TIME half.
// `fixture` and `spread` both write a constant `0 B/op`, so before this there
// was no way to ask a case for a row whose bytes vary at all.
//
// Evenly spread rather than alternating between two values, and that is the
// whole reason this helper exists rather than reusing [spread]'s shape. A
// two-valued sample cannot separate the two rules: MedianCI at n=10 is
// [x(2), x(9)], so two values put the interval exactly at them, and the pair
// of samples then overlaps so heavily that Mann-Whitney never reaches alpha --
// the case passes on the p-value and says nothing about the interval. The
// first version of these cases did exactly that, and reverting the rule left
// them all green, which is how it was caught. An even ramp gives a wide
// interval AND a significant p, which is the combination the guard is about.
//
// b is in the hundreds of thousands so that the `%.0f` rounding of each sample
// is a rounding of a five-figure number rather than a distortion of the spread.
// It takes no name because the gated row is the only one these cases need:
// `ceiling` carries an absolute ns ceiling of 10 that the fixed 1000 ns here
// would breach, which would make a B/op case fail for a reason that is not
// about B/op (golangci-lint's unparam asked the question).
func gatedBytesRamp(b, pct float64) string {
	var out strings.Builder
	const n = 10
	lo := b * (1 - pct/100)
	hi := b * (1 + pct/100)
	for i := range n {
		v := b
		if pct != 0 {
			v = lo + (hi-lo)*float64(i)/float64(n-1)
		}
		fmt.Fprintf(&out, "%s\t1000\t1000.0000 ns/op\t%.0f B/op\t0 allocs/op\n", selfTestGatedRow, v)
	}
	return out.String()
}

func runSelfTest() int {
	cfg, err := budgets.Parse([]byte(selfTestBudgets))
	if err != nil {
		fmt.Fprintln(os.Stderr, "self-test: budgets:", err)
		return 2
	}
	if problems := cfg.Validate(); len(problems) > 0 {
		for _, p := range problems {
			fmt.Fprintln(os.Stderr, "self-test: budgets:", p)
		}
		return 2
	}
	const (
		gated   = selfTestGatedRow
		ceiling = "BenchmarkHeaders/x/Get/n=8"
		waived  = "BenchmarkHeaders/waived/Get/n=8"
		allWaiv = "BenchmarkMemory/PubSubAck/waivedall/x-8"
		timWaiv = "BenchmarkMemory/PubSubAck/waivedtime/x-8"
		ungated = "BenchmarkOther/x-8"
		noCeil  = "BenchmarkHeaders/noceiling/Get/n=2-8"
		// The two arms of §10.2's batched ratio, and the numerator of its
		// single-message one.
		batchNum = "BenchmarkDriver/fixture/PublishBatch/64B-16"
		batchDen = "BenchmarkNative/fixture/PublishBatch/64B-16"
		pubNum   = "BenchmarkDriver/fixture/Publish/64B-16"
		pubDen   = "BenchmarkNative/fixture/Publish/64B-16"
	)
	// bothArms is the input a ratio budget is evaluated over: two arms in ONE
	// file. Nothing else in the repository produces that shape, which is why
	// the fixture builds it explicitly. The denominator is
	// the unit and only the numerator varies, because a ratio is scale-free:
	// what these cases are about is the ratio between the arms.
	const denNs = 1000.0
	bothArms := func(numNs float64) string {
		return fixture(batchNum, numNs, 0) + fixture(batchDen, denNs, 0) +
			fixture(pubNum, numNs, 0) + fixture(pubDen, denNs, 0)
	}
	// The same two arms measured exactly (zero-width intervals), for the
	// cases that pin an arithmetic boundary rather than a verdict about
	// precision.
	bothArmsExact := func(numNs float64) string {
		return exact(batchNum, numNs) + exact(batchDen, denNs) +
			exact(pubNum, numNs) + exact(pubDen, denNs)
	}
	// The numerator measured to a stated precision against an exact
	// denominator, for the net-of-CI cases. The numerator is fixed at a
	// 9.09 % point loss and only the PRECISION varies, because what that pair
	// of cases is about is the same excess read through two intervals.
	const netNumNs = 1100.0
	bothArmsSpread := func(pct float64) string {
		return spread(batchNum, netNumNs, pct) + exact(batchDen, denNs) +
			spread(pubNum, netNumNs, pct) + exact(pubDen, denNs)
	}
	cases := []struct {
		label           string
		base, head      string
		want            int
		gate            string
		provisionalHost bool
	}{
		{"identical", fixture(gated, 1000, 0), fixture(gated, 1000, 0), 0, "", false},
		{"+4% slower", fixture(gated, 1000, 0), fixture(gated, 1040, 0), 1, "", false},
		{"+1 alloc", fixture(gated, 1000, 0), fixture(gated, 1000, 1), 1, "", false},
		{"+1% faster", fixture(gated, 1000, 0), fixture(gated, 990, 0), 0, "", false},
		{
			"budget breach", fixture(gated, 1000, 0) + fixture(ceiling, 10, 0),
			fixture(gated, 1000, 0) + fixture(ceiling, 20, 0), 1, "", false,
		},
		// B/op's net-of-CI guard. The first two are the pair that makes the
		// change: a deterministic jump must still be caught, and a jump
		// inside the two medians' own intervals must not be. Without the
		// first, the guard could be a way to stop judging bytes at all.
		{
			// The deterministic rows are most of them, and the guard must not
			// touch one: identical samples have a zero-width interval, so the
			// net is the raw delta.
			"B/op: a deterministic +10% is still caught",
			gatedBytesRamp(100000, 0), gatedBytesRamp(110000, 0), 1, "", false,
		},
		{
			// THE case. +8% against +/-4% ramps either side is net +2.0%
			// after both intervals come off, with p=0.0002 -- so it is
			// significant and inside the noise at once, which is the only
			// shape that tells the two rules apart. Reverting the rule to the
			// raw delta makes this case, and only this case, fail.
			"B/op: a significant +8% inside both medians' intervals is the host",
			gatedBytesRamp(100000, 4), gatedBytesRamp(108000, 4), 0, "", false,
		},
		{
			// And the guard must not swallow a real jump through the same
			// noise: +15% on those ramps is net +9%.
			"B/op: a +15% jump is still caught through those same intervals",
			gatedBytesRamp(100000, 4), gatedBytesRamp(115000, 4), 1, "", false,
		},
		// -provisional-host, the four cases that matter. A machine that may not
		// judge a TIME ceiling must still catch everything else, or the flag
		// becomes a way to switch the gate off.
		{
			"provisional host: the time ceiling is not judged",
			fixture(gated, 1000, 0) + fixture(ceiling, 20, 0),
			fixture(gated, 1000, 0) + fixture(ceiling, 20, 0), 0, "", true,
		},
		{
			"provisional host: a REGRESSION is still caught",
			fixture(gated, 1000, 0) + fixture(ceiling, 20, 0),
			fixture(gated, 1040, 0) + fixture(ceiling, 20, 0), 1, "", true,
		},
		{
			"provisional host: an ALLOCATION is still caught",
			fixture(gated, 1000, 0) + fixture(ceiling, 20, 0),
			fixture(gated, 1000, 1) + fixture(ceiling, 20, 0), 1, "", true,
		},
		{
			"provisional host: a row inside its ceiling still passes",
			fixture(gated, 1000, 0) + fixture(ceiling, 5, 0),
			fixture(gated, 1000, 0) + fixture(ceiling, 5, 0), 0, "", true,
		},
		// The ceiling on its own, with nothing having changed: an absolute
		// budget is not a comparison.
		{
			"budget breach, no regression", fixture(gated, 1000, 0) + fixture(ceiling, 20, 0),
			fixture(gated, 1000, 0) + fixture(ceiling, 20, 0), 1, "", false,
		},
		// A waiver at the default scope clears the ceiling and leaves the
		// comparison alone: the breach goes, a regression on the row stays.
		{
			"waived breach", fixture(gated, 1000, 0) + fixture(waived, 20, 0),
			fixture(gated, 1000, 0) + fixture(waived, 20, 0), 0, "", false,
		},
		{
			"waived breach, also slower", fixture(gated, 1000, 0) + fixture(waived, 20, 0),
			fixture(gated, 1000, 0) + fixture(waived, 30, 0), 1, "", false,
		},
		// scope: all waives the comparison too, which is why it has to be
		// asked for by name.
		{"scope all, slower", fixture(allWaiv, 1000, 0), fixture(allWaiv, 1040, 0), 0, "", false},
		// scope: time drops the clock and keeps the exact counts.
		{"scope time, slower", fixture(timWaiv, 1000, 0), fixture(timWaiv, 1040, 0), 0, "", false},
		{"scope time, +1 alloc", fixture(timWaiv, 1000, 0), fixture(timWaiv, 1000, 1), 1, "", false},
		// A gate set that selected nothing is an error, not a pass.
		// A median nobody can bound fails the gate on a row that carries an
		// absolute ceiling, because there the gate is asserting the median is
		// under it; on a row that is only compared, "we could not tell" is
		// not a regression.
		{
			"undecided, no ceiling", fixture(gated, 1000, 0) + noisy(noCeil, 1000),
			fixture(gated, 1000, 0) + noisy(noCeil, 1000), 0, "", false,
		},
		{
			"undecided, with ceiling", fixture(gated, 1000, 0) + noisy(ceiling, 10),
			fixture(gated, 1000, 0) + noisy(ceiling, 10), 3, "", false,
		},
		{"no gate rows", fixture(ungated, 1000, 0), fixture(ungated, 1000, 0), 2, "", false},
		// A gate benchmark the candidate no longer reports is not a pass
		// either: its comparison silently disappears.
		{
			"gate row gone", fixture(gated, 1000, 0) + fixture(ceiling, 10, 0),
			fixture(ceiling, 10, 0), 1, "", false,
		},

		// --- kind: ratio, §10.2's first two budgets  ---
		//
		// Identical arms lose nothing, so both ceilings are met.
		{"ratio: identical arms", bothArms(1000), bothArms(1000), 0, "ratio", false},
		// The boundary §10.2's batched budget really permits. The figure is the
		// RATE lost, not the duration added: an operation 5 % LONGER gives up
		// 4.76 % of the throughput and MEETS a 5 % budget, and the duration
		// reading would have failed it. 6 % longer is a
		// 5.66 % loss and does not.
		{"ratio: 5 % longer meets the 5 % budget, measured exactly", bothArmsExact(1050), bothArmsExact(1050), 0, "ratio", false},
		{"ratio: 6 % longer breaches it", bothArms(1060), bothArms(1060), 1, "ratio", false},
		// The same 5 % boundary measured with this fixture's own ±0.1 %
		// spread is UNDECIDED and not met: 4.76 % and the 5 % ceiling are
		// 0.24 % apart and the combined interval is wider than that.
		// It was `met` before the interval was read, and
		// the pair of cases is what says which fact each one pins.
		{"ratio: 5 % longer at ±0.1 % cannot be separated from the ceiling", bothArms(1050), bothArms(1050), 3, "ratio", false},
		// The net-of-CI rule's own pair, and the reason it is not a
		// relaxation. Both cases are the SAME excess -- a 9.09 % point loss
		// over a 5 % ceiling -- measured to two precisions: at ±5 % the
		// ceiling lies inside the interval and the row is UNDECIDED, which
		// is exit 3 and not a pass; at ±2 % the same excess IS a breach.
		// Neither case alone says anything, because a rule that never
		// reports a breach satisfies the first and a rule that ignores the
		// interval satisfies the second.
		{"ratio: a breach its interval cannot support is undecided", bothArmsSpread(5), bothArmsSpread(5), 3, "ratio", false},
		{"ratio: the same excess measured precisely is a breach", bothArmsSpread(2), bothArmsSpread(2), 1, "ratio", false},
		// An unambiguous breach, at the shape B11 measured: +46 % on the
		// batched row.
		{"ratio: +46 % is a breach", bothArms(1460), bothArms(1460), 1, "ratio", false},
		// A row whose denominator was not measured is INCONCLUSIVE, never
		// passing. This is the trap the budget invites and the reason three
		// budgets and all five §8.10 targets matched nothing for four
		// increments.
		{
			"ratio: a missing denominator is inconclusive",
			fixture(batchNum, 1000, 0) + fixture(pubNum, 1000, 0) + fixture(pubDen, 1000, 0),
			fixture(batchNum, 1000, 0) + fixture(pubNum, 1000, 0) + fixture(pubDen, 1000, 0), 3, "ratio", false,
		},
		// And a missing numerator likewise: the gate set selected the row and
		// the run does not have it.
		{
			"ratio: a missing numerator is inconclusive",
			fixture(batchDen, 1000, 0) + fixture(pubNum, 1000, 0) + fixture(pubDen, 1000, 0),
			fixture(batchDen, 1000, 0) + fixture(pubNum, 1000, 0) + fixture(pubDen, 1000, 0), 3, "ratio", false,
		},
	}
	ok := true
	for _, c := range cases {
		br, err := benchfmt.ParseBytes([]byte(c.base))
		if err != nil {
			fmt.Fprintln(os.Stderr, "self-test: base:", err)
			return 2
		}
		hr, err := benchfmt.ParseBytes([]byte(c.head))
		if err != nil {
			fmt.Fprintln(os.Stderr, "self-test: head:", err)
			return 2
		}
		gate := c.gate
		if gate == "" {
			gate = "pr"
		}
		v := compare(cfg, gate, br, hr, time.Now(), c.provisionalHost)
		got := v.exitCode()
		mark := "ok"
		if got != c.want {
			mark = "MISMATCH"
			ok = false
		}
		fmt.Printf("self-test %-42s gate=%-6s exit=%d want=%d %s\n", c.label, gate, got, c.want, mark)
	}
	if !ok {
		return 1
	}
	fmt.Println("self-test passed")
	return 0
}
