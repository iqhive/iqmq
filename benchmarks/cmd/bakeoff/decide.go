package main

import (
	"compress/gzip"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"text/template"
	"time"

	"github.com/iqhive/iqmq/benchmarks/internal/benchfmt"
	"github.com/iqhive/iqmq/benchmarks/internal/stat"
)

// kindScore is the aggregated result of one kind.
type kindScore struct {
	Kind  string `json:"kind"`
	Ident string `json:"ident,omitempty"`
	// Benchmarks is how many of the family's deciding rows this kind
	// measured, out of DecisionRows. A kind that covers fewer than all of
	// them is not a candidate for the default.
	Benchmarks   int     `json:"benchmarks"`
	DecisionRows int     `json:"decision_rows"`
	GeomeanNs    float64 `json:"geomean_ns_op"`
	RatioVsRef   float64 `json:"ratio_vs_ref"`
	RatioVsBest  float64 `json:"ratio_vs_best"`
	// RatioLo/Mid/Hi are the 95% confidence interval of this kind's geomean
	// ratio against the best candidate (§9.4 step 2's benchstat interval).
	RatioLo   float64 `json:"ratio_ci_lo,omitempty"`
	RatioMid  float64 `json:"ratio_ci_mid,omitempty"`
	RatioHi   float64 `json:"ratio_ci_hi,omitempty"`
	AllocsMax float64 `json:"allocs_op_max"`
	// BytesMean is the arithmetic mean of the per-row median B/op. It is a
	// mean and not a geometric mean because a footprint adds and zero is a
	// real value: a kind that allocates nothing reports zero here.
	BytesMean  float64 `json:"b_op_mean"`
	P99Geomean float64 `json:"p99_ns_geomean"`
	Fairness   float64 `json:"fairness,omitempty"`
	LOC        int     `json:"loc"`
	// LOCAttributed reports whether LOC is a count of this kind's own source
	// or a failure to find any. They must not be the same answer: `order`
	// prefers the smaller LOC inside §0.3(2)'s simplicity band, and a kind
	// whose lines nobody could attribute used to arrive there as a zero and
	// so read as the simplest arm in the family.
	LOCAttributed bool   `json:"loc_attributed"`
	Unsafe        bool   `json:"unsafe"`
	Complexity    int    `json:"complexity"`
	Disqualified  bool   `json:"disqualified"`
	Reason        string `json:"reason,omitempty"`
	Disposition   string `json:"disposition"`
	// Medians is every row this kind measured, keyed as `go test` prints it
	// -- name and GOMAXPROCS rung -- so that no two rungs share a key.
	Medians map[string]float64 `json:"medians_ns_op"`
	// GeomeanByProcs is the geomean of the deciding rows at each rung.
	GeomeanByProcs map[string]float64 `json:"geomean_by_gomaxprocs,omitempty"`
	// MaxMetric is the worst per-row median of every other unit the rows
	// reported, which is what a row's own ceiling (A3's idle CPU) is read
	// from. It is a **ceiling** statistic and must not be read as a
	// comparison between kinds: "worst row" is the right question for "≤ 1%
	// idle CPU on every row", and the wrong one for a per-entry cost, because
	// for a fixed-size structure the worst row is the smallest population.
	// Family A4 is the trap: its winner reports 136.7 B/entry here against a
	// runner-up's 24.73, both from the thousand-entry rows where a
	// 4,096-slot table is mostly empty, while at the million entries §9.1's
	// criterion names the same two are 0.139 and 0.027 against the heap's
	// 9.085. Nothing decides on it -- §0.3(3)'s memory exclusion reads
	// B/op and allocs/op -- and it is not rendered in the ADR or the variant
	// report for that reason.
	MaxMetric map[string]float64 `json:"max_metric,omitempty"`
}

// guardCheck records one §0.3(3) exclusion metric and whether there was any
// data to evaluate it against.
type guardCheck struct {
	Metric string `json:"metric"`
	Unit   string `json:"unit"`
	Status string `json:"status"`
	Detail string `json:"detail,omitempty"`
}

const (
	guardEvaluated = "evaluated"
	guardNoData    = "no data"
)

// hostInfo is the machine a scorecard was measured on, copied out of the
// run's sysinfo.json so that the ADR rendered from the scorecard carries the
// machine the benchmark configuration step 5 requires without re-reading the run
// directory.
type hostInfo struct {
	Hostname   string  `json:"hostname,omitempty"`
	OS         string  `json:"os,omitempty"`
	Arch       string  `json:"arch,omitempty"`
	CPUModel   string  `json:"cpu_model,omitempty"`
	CPUs       int     `json:"cpus,omitempty"`
	GOMAXPROCS int     `json:"gomaxprocs,omitempty"`
	Kernel     string  `json:"kernel,omitempty"`
	Governor   string  `json:"governor,omitempty"`
	NUMANodes  int     `json:"numa_nodes,omitempty"`
	LoadAvg1   float64 `json:"loadavg1,omitempty"`
	GoVersion  string  `json:"go_version,omitempty"`
}

// scorecard is the machine-readable decision. It is written both into the run
// directory and, under the family's name, into the stable scorecard directory,
// from which `bakeoff adr` renders the ADRs: an ADR that collects nine
// families is then a pure function of their scorecards and needs no
// re-measurement.
type scorecard struct {
	Family    string `json:"family"`
	Package   string `json:"package"`
	BenchRoot string `json:"bench_root"`
	Reference string `json:"reference"`
	Winner    string `json:"winner"`
	Rule      string `json:"rule"`
	// Criterion is the row's own decision criterion from the benchmark configuration
	// or §9.2, quoted, and Metric the unit it is decided on.
	Criterion string `json:"criterion,omitempty"`
	Metric    string `json:"metric,omitempty"`
	// Workload says why the deciding row set is narrower than everything
	// measured, when the row's criterion narrows it.
	Workload string `json:"workload,omitempty"`
	// Rungs are the GOMAXPROCS values the deciding rows were measured at.
	Rungs []int `json:"gomaxprocs_rungs,omitempty"`
	// Guards records which of §0.3(3)'s three exclusion metrics could be
	// evaluated, so that no ADR claims a check that had no data.
	Guards []guardCheck `json:"exclusion_guards,omitempty"`
	// Unobservable lists the parts of the row's criterion this tool cannot
	// evaluate at all.
	Unobservable []string `json:"unobservable,omitempty"`
	// LOCTotal is every attributable line of the package's non-test source
	// and LOCUnattributed the part of it charged to no kind. They are here
	// because §0.3(2)'s simplicity comparison is only a comparison when the
	// remainder cannot reorder the band, and a reader of a LOC tie-break has
	// no way to see that from the per-kind counts alone.
	LOCTotal        int `json:"loc_total,omitempty"`
	LOCUnattributed int `json:"loc_unattributed,omitempty"`
	// Deciding reports whether the profile these numbers come from may decide
	// anything (only `full` may).
	Deciding     bool   `json:"deciding"`
	Provisional  bool   `json:"provisional"`
	ADR          int    `json:"adr,omitempty"`
	Title        string `json:"title,omitempty"`
	AutoKey      string `json:"auto_key,omitempty"`
	AutoFile     string `json:"auto_file,omitempty"`
	Sysinfo      string `json:"sysinfo,omitempty"`
	RawResults   string `json:"raw_results,omitempty"`
	RunID        string `json:"run_id,omitempty"`
	Profile      string `json:"profile,omitempty"`
	Methodology  string `json:"methodology,omitempty"`
	Commit       string `json:"commit,omitempty"`
	CommitStatus string `json:"commit_status,omitempty"`
	CommitDirty  bool   `json:"commit_dirty,omitempty"`
	// CommitDirtySource is CommitDirty restricted to the code under
	// measurement: a campaign writes a generated binding and a variant report
	// for every family it finishes, so from the second family onward the tree
	// is always dirty with the campaign's own product. Only this one says the
	// numbers cannot be tied to a commit.
	CommitDirtySource bool      `json:"commit_dirty_source,omitempty"`
	Host              *hostInfo `json:"host,omitempty"`
	DecidedAt         string    `json:"decided_at"`
	// Common is the row set the decision was taken over: the deciding
	// workload's rows that every CANDIDATE covers. It is not the rows every
	// kind covers -- a kind that cannot run the workload the row is decided
	// on is reported, not allowed to narrow the decision to its own corner.
	Common []string    `json:"common_benchmarks"`
	Kinds  []kindScore `json:"kinds"`
	Notes  []string    `json:"notes,omitempty"`
}

type decideOpts struct {
	family, results, pkg, benchRoot, ref, disqualified string
	adrNum                                             int
	title, autoKey, outReport, autoFile, sysinfo       string
	scorecards                                         string
	profile, methodology                               string
	idents                                             string
	rawPath, rawOut                                    string
	removedAt                                          string
	classB                                             bool
	dryRun                                             bool
	kindDim                                            string
	autoPrefix                                         string
}

func decide(args []string) error {
	fs := flag.NewFlagSet("decide", flag.ContinueOnError)
	var o decideOpts
	fs.StringVar(&o.family, "family", "", "variant family id (e.g. A1)")
	fs.StringVar(&o.results, "results", "", "comma-separated benchmark output files")
	fs.StringVar(&o.pkg, "pkg", "", "family package directory (for LOC and auto.go)")
	fs.StringVar(&o.benchRoot, "bench", "", "benchmark name prefix, e.g. BenchmarkMemory/Queue")
	fs.StringVar(&o.kindDim, "kind-dim", "", "name of the key=value dimension carrying the kind (default: segment after -bench)")
	fs.StringVar(&o.ref, "ref", "", "reference (Safe) kind slug")
	fs.StringVar(&o.disqualified, "disqualified", "", "comma-separated kinds that failed the correctness gate")
	fs.IntVar(&o.adrNum, "adr", 0, "ADR number this family belongs to (0 = none); `bakeoff adr` renders it")
	fs.StringVar(&o.title, "title", "", "the family's own title, used as its section heading in the ADR")
	fs.StringVar(&o.autoKey, "auto-key", "", "family key used in ADR auto: bindings (e.g. queue)")
	fs.StringVar(&o.outReport, "out-report", "docs/benchmarks/variants", "report directory")
	fs.StringVar(&o.scorecards, "scorecards", "benchmarks/reports/scorecards", "directory for the stable per-family scorecard (\"-\" disables)")
	fs.StringVar(&o.profile, "profile", "", "bake-off profile the results come from (full|quick)")
	fs.StringVar(&o.methodology, "methodology", "", "how the results were measured, recorded verbatim in the ADR")
	fs.StringVar(&o.autoFile, "auto-file", "", "auto.go to regenerate (default <pkg>/auto.go; \"-\" disables)")
	fs.StringVar(&o.autoPrefix, "auto-prefix", "", "prefix for the generated constants, for a package hosting more than one family (e.g. Arch -> ArchAutoKind/ArchAutoProvisional)")
	fs.StringVar(&o.sysinfo, "sysinfo", "", "sysinfo JSON path")
	fs.StringVar(&o.idents, "idents", "", "slug=Ident,... mapping (default: from TestKindsList)")
	fs.StringVar(&o.rawPath, "raw", "", "path of the raw results directory to reference")
	fs.StringVar(&o.rawOut, "raw-out", "", "directory for the run's own copy of the scorecard (default: -raw's directory; \"-\" writes none)")
	fs.StringVar(&o.removedAt, "removed", "", "commit of the last tree containing this family's arms, for a family whose kinds have been removed (§9.4(5), the ADR template's Disposition field)")
	fs.BoolVar(&o.classB, "class-b", false, "this row is one of the benchmark configuration's mutually exclusive API shapes, so a shape the API did not take is a loser and not a profile")
	fs.BoolVar(&o.dryRun, "dry-run", false, "print the scorecard only")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if o.family == "" || o.results == "" || o.benchRoot == "" {
		return errors.New("decide: -family, -results and -bench are required")
	}
	var results []benchfmt.Result
	for _, path := range strings.Split(o.results, ",") {
		rs, err := readResults(strings.TrimSpace(path))
		if err != nil {
			return err
		}
		results = append(results, rs...)
	}
	idents := map[string]string{}
	switch {
	case o.idents != "":
		for _, part := range strings.Split(o.idents, ",") {
			slug, ident, _ := strings.Cut(strings.TrimSpace(part), "=")
			idents[slug] = ident
		}
	case o.pkg != "" && !o.dryRun:
		if fam, ok, err := loadFamilyDir(o.pkg); err == nil && ok {
			idents = fam.idents
		}
	}
	sc, err := score(o, results, idents)
	if err != nil {
		return err
	}
	data, _ := json.MarshalIndent(sc, "", "  ")
	if o.dryRun {
		fmt.Println(string(data))
		return nil
	}
	// The run directory's copy of the scorecard goes beside the measurement
	// by default. `-raw-out -` suppresses it without changing the path the
	// scorecard RECORDS, which is what lets a re-decide be run against
	// committed evidence as an experiment  rather than as
	// a rewrite of it.
	rawOut := o.rawOut
	if rawOut == "" {
		rawOut = o.rawPath
	}
	if rawOut != "" && rawOut != "-" {
		if err := os.WriteFile(filepath.Join(rawOut, "scorecard.json"), append(data, '\n'), 0o644); err != nil {
			return err
		}
	}
	// Only a deciding profile writes the canonical artefacts. `quick` takes
	// three 200 ms samples with the load guard in smoke mode under a ceiling
	// of 64, and assumption A24.33 says its numbers must never reach an ADR;
	// it used to write the stable scorecard, the published variant page and
	// the generated auto.go all the same. The run's own scorecard above is
	// still written, and says which profile produced it.
	if !sc.Deciding {
		fmt.Printf("bakeoff decide %s: profile %q decides nothing (assumption A24.33): no scorecard, report or auto.go written; the run's own scorecard is in %s\n",
			o.family, or(o.profile, "unrecorded"), o.rawPath)
		return nil
	}
	// The run directory is named after the run, so a second run of the same
	// family writes a second copy and neither is "the" scorecard. This copy
	// is named after the family and is the one `bakeoff adr` reads.
	if o.scorecards != "" && o.scorecards != "-" {
		if err := os.MkdirAll(o.scorecards, 0o755); err != nil {
			return err
		}
		if err := os.WriteFile(filepath.Join(o.scorecards, o.family+".json"), append(data, '\n'), 0o644); err != nil {
			return err
		}
	}
	if err := os.MkdirAll(o.outReport, 0o755); err != nil {
		return err
	}
	if err := render(reportTmpl, filepath.Join(o.outReport, o.family+".md"), sc, o); err != nil {
		return err
	}
	autoFile := o.autoFile
	if autoFile == "" && o.pkg != "" {
		autoFile = filepath.Join(o.pkg, "auto.go")
	}
	switch {
	case autoFile == "" || autoFile == "-":
	case sc.Winner == "":
		// A family whose rule this tool cannot express (A10's two builds)
		// still lands a scorecard -- the numbers are what another record is
		// computed from -- but binds nothing.
		fmt.Printf("bakeoff decide %s: no winner named (%s); %s left alone\n", o.family, sc.Rule, autoFile)
	default:
		if err := writeAuto(autoFile, o, sc); err != nil {
			return err
		}
	}
	fmt.Printf("bakeoff decide %s: winner=%s (rule: %s)\n", o.family, or(sc.Winner, "none"), sc.Rule)
	return nil
}

// loadFamilyDir runs TestKindsList in a package directory.
func loadFamilyDir(dir string) (*family, bool, error) {
	abs, err := filepath.Abs(dir)
	if err != nil {
		return nil, false, err
	}
	return loadFamily(abs, ".")
}

// row is one measured configuration: a benchmark name with the kind segment
// removed, at one GOMAXPROCS rung. The rung is part of the identity and not a
// dimension to pool over: the full profile measures at -cpu=1,4,8,32, so a
// median taken over the four rungs together describes no configuration --
// and the families whose whole subject is how a kind responds to the ladder
// (A1's lock-free queues against a mutex ring, A2's fan-out architectures,
// A9's striped counters) are exactly the ones such a median cannot compare.
type row struct {
	name  string
	procs int
}

// String is the row as `go test -bench` prints it, so that a scorecard names
// rows a reader can find in raw.txt.
func (r row) String() string { return r.name + "-" + strconv.Itoa(r.procs) }

func rowLessThan(a, b row) bool {
	if a.name != b.name {
		return a.name < b.name
	}
	return a.procs < b.procs
}

// kindSamples holds one kind's samples: row -> unit -> values.
type kindSamples map[row]map[string][]float64

// decidingProfiles are the bake-off profiles whose numbers may decide
// anything. `quick` runs the load guard in smoke mode with a ceiling of 64
// and takes three 200 ms samples; `probe` measures nothing at all.
var decidingProfiles = map[string]bool{"full": true}

// score applies the row's own §9.1/§9.2 criterion, within §0.3.
func score(o decideOpts, results []benchfmt.Result, idents map[string]string) (*scorecard, error) {
	rule, ok := ruleFor(o.family)
	if !ok {
		return nil, fmt.Errorf("family %s has no decision rule: every §9.1 row carries its own criterion and "+
			"`bakeoff decide` refuses to substitute a rule of its own. Add the row to `rules` in "+
			"benchmarks/cmd/bakeoff/rules.go (declared: %s)", o.family, strings.Join(ruleFamilies(), ", "))
	}
	disq := map[string]string{}
	for _, d := range strings.Split(o.disqualified, ",") {
		if d = strings.TrimSpace(d); d != "" {
			disq[d] = "failed the correctness gate"
		}
	}
	samples := map[string]kindSamples{}
	// footprintSeen is whether -benchmem was in force at all: §0.3(3)'s
	// footprint guard is only evaluated where there is a footprint to read,
	// and "every kind reports 0 B/op" and "nothing reported B/op" are
	// different facts.
	footprintSeen := false
	for _, r := range results {
		base, procs := r.Row()
		// underRoot matches on segment boundaries, and `bakeoff lint` uses
		// the same predicate: what the campaign measures and what the
		// decision reads have to be the same set. A plain prefix test folded
		// BenchmarkMiddlewareCompose -- the once-per-Run composition cost,
		// not family A30's subject -- into A30's geomean, a third of its
		// rows.
		if !underRoot(base, o.benchRoot) {
			continue
		}
		kind, bench, ok := splitKind(base, o.benchRoot, o.kindDim)
		if !ok {
			continue
		}
		if samples[kind] == nil {
			samples[kind] = kindSamples{}
		}
		key := row{name: bench, procs: procs}
		if samples[kind][key] == nil {
			samples[kind][key] = map[string][]float64{}
		}
		for u, v := range r.Values {
			if u == "B/op" || u == "allocs/op" {
				footprintSeen = true
			}
			samples[kind][key][u] = append(samples[kind][key][u], v)
		}
	}
	if len(samples) == 0 {
		return nil, fmt.Errorf("no results under %s", o.benchRoot)
	}
	var kinds []string
	for k := range samples {
		kinds = append(kinds, k)
	}
	sort.Strings(kinds)

	sc := &scorecard{
		Family: o.family, Package: repoPath(o.pkg), BenchRoot: o.benchRoot, Reference: o.ref, Provisional: true,
		ADR: o.adrNum, Title: o.title, AutoKey: o.autoKey, AutoFile: repoPath(o.autoFile),
		Sysinfo: o.sysinfo, Profile: o.profile, Methodology: o.methodology,
		DecidedAt: time.Now().UTC().Format(time.RFC3339),
		Criterion: rule.Criterion, Metric: rule.primaryMetric(),
		Unobservable: rule.Unobservable, Deciding: decidingProfiles[o.profile],
	}
	if rule.Note != "" {
		sc.Notes = append(sc.Notes, rule.Note)
	}
	// §9.4 step 5 requires the ADR to carry the raw data and the machine. The
	// scorecard is what the ADR is rendered from, so the run id, the host and
	// the commit are recorded in it rather than left in the run directory.
	if o.rawPath != "" {
		sc.RunID = filepath.Base(o.rawPath)
		sc.RawResults = "benchmarks/reports/results/" + sc.RunID + "/raw.txt.gz"
		sc.Host, sc.Commit, sc.CommitStatus, sc.CommitDirty, sc.CommitDirtySource = readSysinfo(filepath.Join(o.rawPath, "sysinfo.json"))
	}
	if sc.Commit == "" && sc.CommitStatus == "" {
		sc.Commit = gitCommit(o.pkg)
	}

	// A kind the family announced that produced no samples has vanished from
	// the scorecard, and from the ADR, without a word.
	if err := checkAnnounced(sc, samples, idents, disq, rule); err != nil {
		return nil, err
	}

	metric := rule.primaryMetric()
	sel, err := activeSelectors(sc, rule, samples, metric)
	if err != nil {
		return nil, err
	}
	candidates, partial, decisionRows, err := coverage(o, samples, kinds, disq, rule, sel, metric)
	if err != nil {
		return nil, err
	}
	sc.Common = make([]string, 0, len(decisionRows))
	for _, r := range decisionRows {
		sc.Common = append(sc.Common, r.String())
	}
	sc.Workload = rule.WorkloadWhy

	// Aggregate every kind: the candidates over the deciding rows, the others
	// over what they measured, so that the published table is complete.
	byKind := map[string]*kindScore{}
	rungs := map[int]bool{}
	for _, r := range decisionRows {
		rungs[r.procs] = true
	}
	// The denominator §0.3(2)'s simplicity comparison is a share of, and the
	// remainder charged to no kind. The
	// attribution is computed for the whole family at once because "this
	// declaration is shared" is not a statement one kind can make.
	seeds := make([]kindSeed, 0, len(kinds))
	for _, k := range kinds {
		seeds = append(seeds, kindSeed{Slug: k, Ident: idents[k]})
	}
	loc := attributeLOC(o.pkg, seeds)
	for _, k := range kinds {
		ks := aggregate(k, samples[k], decisionRows, rungs, metric)
		ks.Ident = idents[k]
		if o.pkg != "" {
			ks.LOC, ks.LOCAttributed, ks.Unsafe = loc.Lines[k], loc.attributed(k), loc.Unsafe[k]
		}
		ks.Complexity = ks.LOC
		if ks.Unsafe {
			ks.Complexity += 1000
		}
		if reason, bad := disq[k]; bad {
			ks.Disqualified = true
			ks.Reason = reason
		}
		byKind[k] = ks
	}
	for _, r := range decisionRows {
		sc.Rungs = appendRung(sc.Rungs, r.procs)
	}
	sc.Guards = guardStatus(byKind, candidates, o.ref, footprintSeen)
	for _, g := range sc.Guards {
		if g.Status != guardEvaluated {
			sc.Notes = append(sc.Notes, fmt.Sprintf("§0.3(3) %s guard: not evaluated -- %s", g.Metric, g.Detail))
		}
	}

	if rule.Undecidable != "" {
		sc.Rule = "not decided by this tool"
		sc.Notes = append(sc.Notes, rule.Undecidable)
		finish(sc, byKind, kinds, candidates, partial, rule, o, decisionRows, samples, metric)
		return sc, nil
	}
	sc.LOCTotal = loc.Total
	sc.LOCUnattributed = loc.unattributed()
	if err := pick(sc, byKind, candidates, rule, o, decisionRows, samples, metric); err != nil {
		return nil, err
	}
	finish(sc, byKind, kinds, candidates, partial, rule, o, decisionRows, samples, metric)
	return sc, nil
}

// checkAnnounced holds the scorecard to the kind list the family announced
// through its TestKindsList: a kind with no samples is an error unless the
// correctness gate disqualified it, in which case it is recorded as such.
func checkAnnounced(sc *scorecard, samples map[string]kindSamples, idents, disq map[string]string, rule decisionRule) error {
	if len(idents) == 0 {
		return nil
	}
	var missing, gated []string
	announced := make([]string, 0, len(idents))
	for slug := range idents {
		announced = append(announced, slug)
	}
	sort.Strings(announced)
	for _, slug := range announced {
		if _, ok := samples[slug]; ok {
			continue
		}
		if _, bad := disq[slug]; bad {
			gated = append(gated, slug)
			continue
		}
		missing = append(missing, slug)
	}
	if len(missing) > 0 {
		return fmt.Errorf("family %s announced %d kind(s) that produced no benchmark result: %s. "+
			"A kind that vanishes here vanishes from the scorecard and from the ADR; check the per-kind "+
			"-bench filter and the benchmark's own kind loop", sc.Family, len(missing), strings.Join(missing, ", "))
	}
	if len(gated) > 0 {
		sc.Notes = append(sc.Notes, fmt.Sprintf("disqualified and not measured: %s", strings.Join(gated, ", ")))
	}
	var extra []string
	for slug := range samples {
		if _, ok := idents[slug]; !ok && !rule.isBaseline(slug) {
			extra = append(extra, slug)
		}
	}
	if len(extra) > 0 {
		sort.Strings(extra)
		sc.Notes = append(sc.Notes, fmt.Sprintf("measured but not announced by the family's kind list: %s", strings.Join(extra, ", ")))
	}
	return nil
}

// activeSelectors resolves the row's workload selectors against what was
// measured. A required selector that matches nothing is an error: a criterion
// naming a shape the run did not measure cannot be applied to it. An optional
// one -- a shape only IQMQ_BENCH_MATRIX widens to -- is dropped with a note.
func activeSelectors(sc *scorecard, rule decisionRule, samples map[string]kindSamples, metric string) ([]string, error) {
	all := map[row]bool{}
	for _, ks := range samples {
		for r, units := range ks {
			if len(units[metric]) > 0 {
				all[r] = true
			}
		}
	}
	if len(all) == 0 {
		return nil, fmt.Errorf("no row under %s reports %s, which is what §9.1 row %s is decided on",
			sc.BenchRoot, metric, sc.Family)
	}
	matches := func(sel string) int {
		n := 0
		for r := range all {
			if selects(r.name, sel) {
				n++
			}
		}
		return n
	}
	var active []string
	for _, sel := range rule.Workload {
		if matches(sel) == 0 {
			return nil, fmt.Errorf("family %s is decided on the rows matching %q (%s) and no measured row matches it; "+
				"the run measured %d row(s) under %s", sc.Family, sel, rule.Criterion, len(all), sc.BenchRoot)
		}
		active = append(active, sel)
	}
	for _, sel := range rule.WorkloadOpt {
		if matches(sel) == 0 {
			sc.Notes = append(sc.Notes, fmt.Sprintf("§9.1 row %s names the workload %q and this run did not measure it; "+
				"the decision is taken over the rows that were measured", sc.Family, sel))
			continue
		}
		active = append(active, sel)
	}
	return active, nil
}

// coverage partitions the kinds into the candidates that span the deciding
// workload and the ones that cannot run all of it, and returns the rows the
// decision is taken over.
//
// This is the defect that would have bound a single-producer-single-consumer
// queue as the memory engine's default MPMC lane queue: the kinds of family
// A1 do not all cover the same rows -- SPSC cannot run the multi-producer
// shapes -- so the set of rows every KIND measured was two SPSC rows out of a
// hundred and six, and the family was decided on the one shape its §9.1
// criterion is not about. §0.3(1)'s "representative workload geomean" is read
// here as: the geomean over the whole deciding workload, taken among the
// candidates that can run all of it. A kind that cannot is reported, never
// crowned.
func coverage(o decideOpts, samples map[string]kindSamples, kinds []string, disq map[string]string,
	rule decisionRule, sel []string, metric string,
) (candidates, partial []string, decisionRows []row, err error) {
	rowsOf := func(k string) map[row]bool {
		out := map[row]bool{}
		for r, units := range samples[k] {
			if len(units[metric]) == 0 || !selectsAll(r.name, sel) {
				continue
			}
			out[r] = true
		}
		return out
	}
	union := map[row]bool{}
	eligible := make([]string, 0, len(kinds))
	for _, k := range kinds {
		if _, bad := disq[k]; bad || rule.isBaseline(k) {
			continue
		}
		// An arm that cannot be the default still covers the workload, so its
		// rows belong in the union that defines the deciding set -- it is only
		// candidacy it is excluded from.
		for r := range rowsOf(k) {
			union[r] = true
		}
		if rule.notDefault(k) != "" {
			continue
		}
		eligible = append(eligible, k)
	}
	if len(union) == 0 {
		return nil, nil, nil, fmt.Errorf("no qualifying kind of family %s reports %s under the deciding workload %v",
			o.family, metric, sel)
	}
	for _, k := range eligible {
		if len(rowsOf(k)) == len(union) {
			candidates = append(candidates, k)
			continue
		}
		partial = append(partial, k)
	}
	// One eligible arm is a decision, not a degeneracy, when the others were
	// excluded by the row's own declaration rather than by what they can run.
	// Family A2 is the case: §8.4 makes `fullcopy` and channel fan-out
	// required baselines and assumption A15.4 makes `logcursor` ineligible as
	// a default, so per-subscription envelopes is the only architecture left
	// -- which is §8.4's "tentative default" arrived at from the rule, with
	// every arm's numbers still reported. The refusal below is for the other
	// case: several arms are eligible and the workload has narrowed to a
	// corner only one of them can run.
	soleEligible := len(candidates) == 1 && len(eligible) == 1
	if len(candidates) < 2 && !soleEligible {
		var cov []string
		for _, k := range eligible {
			cov = append(cov, fmt.Sprintf("%s covers %d/%d", k, len(rowsOf(k)), len(union)))
		}
		return nil, nil, nil, fmt.Errorf("family %s cannot be decided: %d of %d kinds span the deciding workload "+
			"(%s). Deciding on the rows they happen to share would decide the family on a corner of itself; "+
			"narrow the row's Workload in benchmarks/cmd/bakeoff/rules.go to a workload at least two kinds can "+
			"run, or declare the arms that are not candidates as Baselines.%s",
			o.family, len(candidates), len(eligible), strings.Join(cov, ", "), noteSuffix(rule.Note))
	}
	for r := range union {
		decisionRows = append(decisionRows, r)
	}
	sort.Slice(decisionRows, func(i, j int) bool { return rowLessThan(decisionRows[i], decisionRows[j]) })
	return candidates, partial, decisionRows, nil
}

// aggregate reduces one kind's samples to the numbers the decision and the
// report need. The geomean is taken over the deciding rows the kind covers;
// per-rung geomeans are kept beside it, because a family whose subject is the
// GOMAXPROCS ladder cannot be read off one number.
func aggregate(kind string, ks kindSamples, decisionRows []row, rungs map[int]bool, metric string) *kindScore {
	out := &kindScore{Kind: kind, Medians: map[string]float64{}, GeomeanByProcs: map[string]float64{}}
	// Every row the kind measured appears in the published medians, and the
	// worst per-row median of every other unit is kept beside them: a row's
	// own ceiling can live on a row of its own (§9.1 row A3's idle-CPU
	// share is measured by BenchmarkMemory/Wait/<kind>/IdleCPU, which
	// reports no percentile at all), so a ceiling read only from the
	// deciding rows would never fire.
	for r, units := range ks {
		if v := units[metric]; len(v) > 0 {
			out.Medians[r.String()] = stat.Median(v)
		}
		for u, vals := range units {
			if u == metric {
				continue
			}
			if m := stat.Median(vals); m > out.MaxMetric[u] {
				if out.MaxMetric == nil {
					out.MaxMetric = map[string]float64{}
				}
				out.MaxMetric[u] = m
			}
		}
	}
	var primary, bytes, p99s []float64
	perRung := map[int][]float64{}
	covered := 0
	for _, r := range decisionRows {
		units := ks[r]
		if units == nil {
			continue
		}
		v := units[metric]
		if len(v) == 0 {
			continue
		}
		covered++
		m := stat.Median(v)
		primary = append(primary, m)
		perRung[r.procs] = append(perRung[r.procs], m)
		if b := units["B/op"]; len(b) > 0 {
			bytes = append(bytes, stat.Median(b))
		}
		if a := units["allocs/op"]; len(a) > 0 {
			out.AllocsMax = math.Max(out.AllocsMax, stat.Median(a))
		}
		if q := units["p99-ns"]; len(q) > 0 {
			p99s = append(p99s, stat.Median(q))
		}
		if f := units["fairness"]; len(f) > 0 {
			out.Fairness = math.Max(out.Fairness, stat.Median(f))
		}
	}
	out.Benchmarks = covered
	out.DecisionRows = len(decisionRows)
	out.GeomeanNs = stat.Geomean(primary)
	// A footprint adds, and zero is a real value, so it is a mean and not a
	// geometric mean: Geomean drops the zeros and would report a kind that
	// allocates on one row in ten as if it allocated on all of them.
	out.BytesMean = stat.Mean(bytes)
	out.P99Geomean = stat.Geomean(p99s)
	for procs := range rungs {
		if v := perRung[procs]; len(v) > 0 {
			out.GeomeanByProcs[strconv.Itoa(procs)] = stat.Geomean(v)
		}
	}
	return out
}

// guardStatus records, per §0.3(3) metric, whether the exclusion could be
// evaluated at all. §0.3 writes the exclusion as a MUST, and in this tree two
// of its three metrics have no data behind them: no benchmark emits a
// fairness metric, and p99 is absent from most families because it is
// reported only where the benchmark builds a histogram. The ADR states which
// ran rather than claiming all three.
func guardStatus(byKind map[string]*kindScore, candidates []string, ref string, footprintSeen bool) []guardCheck {
	// The guard compares a candidate against the REFERENCE, so a metric only
	// one side reports cannot fire. Counting "any candidate has it" would
	// have made this record's own claim as vacuous as the guard it describes.
	nP99, nFair := 0, 0
	for _, k := range candidates {
		if byKind[k].P99Geomean > 0 {
			nP99++
		}
		if byKind[k].Fairness > 0 {
			nFair++
		}
	}
	refKS, haveRef := byKind[ref]
	refP99 := haveRef && refKS != nil && refKS.P99Geomean > 0
	refFair := haveRef && refKS != nil && refKS.Fairness > 0

	p99 := guardCheck{Metric: "p99 latency", Unit: "p99-ns", Status: guardEvaluated}
	switch {
	case nP99 == 0:
		p99.Status = guardNoData
		p99.Detail = "no candidate reports a p99-ns metric; the latency histogram is built only by the benchmarks that report percentiles themselves or honour -bench.latency"
	case !refP99:
		p99.Status = guardNoData
		p99.Detail = fmt.Sprintf("%d candidate(s) report p99-ns but the reference kind %q does not, and the guard is a comparison against the reference", nP99, ref)
	case nP99 < len(candidates):
		p99.Detail = fmt.Sprintf("evaluated for the %d of %d candidates that report p99-ns", nP99, len(candidates))
	}

	foot := guardCheck{Metric: "memory footprint", Unit: "B/op, allocs/op", Status: guardEvaluated}
	if !footprintSeen {
		foot.Status = guardNoData
		foot.Detail = "no row reports B/op or allocs/op; the run was made without -benchmem"
	}

	fair := guardCheck{Metric: "fairness", Unit: "fairness", Status: guardEvaluated}
	switch {
	case nFair == 0:
		fair.Status = guardNoData
		fair.Detail = "no benchmark in the repository emits a fairness metric; the scorer reads `fairness` as a cost (lower is better, e.g. a max/min service-share ratio) and nothing produces it"
	case !refFair:
		fair.Status = guardNoData
		fair.Detail = fmt.Sprintf("%d candidate(s) report a fairness metric but the reference kind %q does not", nFair, ref)
	}
	return []guardCheck{p99, foot, fair}
}

// pick names the winner under the row's own rule.
func pick(sc *scorecard, byKind map[string]*kindScore, candidates []string, rule decisionRule,
	o decideOpts, decisionRows []row, samples map[string]kindSamples, metric string,
) error {
	live := make([]string, 0, len(candidates))
	for _, k := range candidates {
		if !byKind[k].Disqualified && byKind[k].GeomeanNs > 0 {
			live = append(live, k)
		}
	}
	if len(live) == 0 {
		return fmt.Errorf("every candidate kind of family %s is disqualified or produced no %s", o.family, metric)
	}
	// A row that requires zero allocations, or caps a metric, excludes the
	// candidates that breach it -- but only while some candidate does not, so
	// that a criterion no kind meets is recorded rather than emptying the set.
	live = applyCeilings(sc, byKind, live, rule)

	// §9.1 rows whose default is fixed by the row itself (correctness first,
	// all shipped) are not chosen by the numbers.
	if rule.Fixed {
		if !contains(live, rule.Incumbent) {
			return fmt.Errorf("family %s's default is fixed by §9.1 to %q and that kind is not a qualifying candidate: %s",
				o.family, rule.Incumbent, rule.Criterion)
		}
		sc.Winner = rule.Incumbent
		sc.Rule = fmt.Sprintf("default fixed by §9.1 to %s (%s)", rule.Incumbent, rule.Criterion)
		return nil
	}

	ratio := func(a, b string) (lo, mid, hi float64) {
		return ratioCI(samples, decisionRows, metric, a, b)
	}
	// A row with an incumbent keeps it unless a challenger's interval
	// establishes a win larger than the row's own margin.
	if rule.Incumbent != "" && contains(live, rule.Incumbent) {
		var beat []string
		want := 1 - rule.Margin
		for _, k := range live {
			if k == rule.Incumbent {
				continue
			}
			_, _, hi := ratio(k, rule.Incumbent)
			if hi > 0 && hi < want {
				beat = append(beat, k)
			}
		}
		if len(beat) == 0 {
			sc.Winner = rule.Incumbent
			sc.Rule = fmt.Sprintf("%s retained: no candidate's 95%% interval establishes a gain over it of more than %.0f%% (%s)",
				rule.Incumbent, rule.Margin*100, rule.Criterion)
			return nil
		}
		live = beat
		sc.Notes = append(sc.Notes, fmt.Sprintf("%d candidate(s) beat %s by more than %.0f%% with 95%% confidence: %s",
			len(beat), rule.Incumbent, rule.Margin*100, strings.Join(beat, ", ")))
	}

	ordered, ruleText := order(byKind, live, rule, ratio, sc.LOCUnattributed)
	ref := byKind[o.ref]
	for _, k := range ordered {
		c := byKind[k]
		if ref != nil && c != ref && !ref.Disqualified {
			if reason := regresses(c, ref); reason != "" {
				sc.Notes = append(sc.Notes, fmt.Sprintf("%s excluded from default: %s", c.Kind, reason))
				continue
			}
		}
		sc.Winner = k
		sc.Rule = ruleText
		return nil
	}
	if ref != nil && !ref.Disqualified {
		sc.Winner = o.ref
		sc.Rule = "every faster candidate regressed p99 or footprint by >10% against the reference (§0.3(3)); reference retained"
		return nil
	}
	sc.Winner = ordered[0]
	sc.Rule = ruleText
	return nil
}

// order ranks the live candidates: fastest first, except that §0.3(2)'s
// simplicity band comes first among the candidates the data cannot separate
// from the best by more than 3%. The band is decided by the interval and not
// by the medians -- a 2% difference that is pure noise used to read as a tie
// and a 4% one as a winner, and neither statement was supported.
func order(byKind map[string]*kindScore, live []string, rule decisionRule,
	ratio func(a, b string) (lo, mid, hi float64), unattributed int,
) (ordered []string, ruleText string) {
	incomplete := false
	best := live[0]
	for _, k := range live {
		if lessOn(byKind[k], byKind[best], rule) {
			best = k
		}
	}
	var band, rest []string
	for _, k := range live {
		if k == best {
			continue
		}
		lo, _, _ := ratio(k, best)
		// Only data that establishes a candidate is more than 3% worse than
		// the best takes it out of the simplicity band.
		if lo > 0 && lo > 1+tieBand {
			rest = append(rest, k)
			continue
		}
		band = append(band, k)
	}
	band = append(band, best)
	// §0.3(2)'s simplicity preference needs a count of every band member's
	// lines to be a comparison at all. One member whose lines could not be
	// attributed takes the whole band out of it -- otherwise that member
	// arrives as a zero and wins on being unmeasured -- and the rule text
	// below says which rule was applied.
	//
	// Use of unsafe is not part of that: it is read from the file's imports
	// and build tag, not from a line count, so it still orders an unsafe arm
	// behind a safe one whatever the counts say.
	simplicity := true
	for _, k := range band {
		if !byKind[k].LOCAttributed {
			simplicity = false
			break
		}
	}
	// And the same guard for a band whose counts are attributed but not
	// COMPLETE. `unattributed` is the package's non-test
	// lines charged to no kind; in the worst case they all belong to one band
	// member, so LOC can only order the band when the remainder is smaller
	// than the smallest gap it would have to close. Family B9 is the case:
	// five kinds in one package where `seqSession` and `iterSession` carry
	// neither the slug `seq2`/`iternext` nor the ident `Seq2`/`IterNext`, so
	// each was charged nine lines of a fifty-seven-line implementation, and
	// 9 against `value`'s 64 flipped a §9.2 API shape on the strength of two
	// misnamed types.
	if simplicity && unattributed > 0 {
		gap := 0
		for i := range band {
			for j := range band {
				if d := byKind[band[i]].LOC - byKind[band[j]].LOC; d > 0 && (gap == 0 || d < gap) {
					gap = d
				}
			}
		}
		if gap == 0 || unattributed >= gap {
			simplicity = false
			incomplete = true
		}
	}
	// The name is the last tiebreak, so that two kinds the data and the
	// complexity count cannot separate are still ordered the same way on
	// every run.
	sort.Slice(band, func(i, j int) bool {
		a, b := byKind[band[i]], byKind[band[j]]
		switch {
		case a.Unsafe != b.Unsafe:
			return !a.Unsafe
		case simplicity && a.LOC != b.LOC:
			return a.LOC < b.LOC
		case lessOn(a, b, rule):
			return true
		case lessOn(b, a, rule):
			return false
		}
		return band[i] < band[j]
	})
	sort.Slice(rest, func(i, j int) bool {
		a, b := byKind[rest[i]], byKind[rest[j]]
		switch {
		case lessOn(a, b, rule):
			return true
		case lessOn(b, a, rule):
			return false
		}
		return rest[i] < rest[j]
	})
	ordered = make([]string, 0, len(band)+len(rest))
	ordered = append(ordered, band...)
	ordered = append(ordered, rest...)
	switch {
	case len(ordered) == 1:
		// Naming a geomean contest with one entrant would be a false claim,
		// and the ADR prints this string verbatim.
		ruleText = "the only arm the row leaves eligible; every other is a declared baseline or cannot be the default"
	case len(band) > 1 && incomplete:
		ruleText = fmt.Sprintf("%d candidate(s) within 3%% of the best on the 95%% interval; safe before unsafe, then the deciding metric -- not §0.3(2)'s simplicity, because %d of the package's lines are attributable to no kind, which is more than the counts differ by (assumption C01.7)", len(band), unattributed)
	case len(band) > 1 && !simplicity:
		ruleText = fmt.Sprintf("%d candidate(s) within 3%% of the best on the 95%% interval; safe before unsafe, then the deciding metric -- not §0.3(2)'s simplicity, because at least one of them has no source line attributable to it", len(band))
	case len(band) > 1:
		ruleText = fmt.Sprintf("%d candidate(s) within 3%% of the best on the 95%% interval; safe before unsafe, then simplest (LOC) preferred", len(band))
	case rule.AllocsFirst:
		ruleText = "fewest allocs/op, then fastest geomean, with a 95% interval"
	default:
		ruleText = "fastest geomean, established by a 95% interval"
	}
	return ordered, ruleText
}

// lessOn is the row's primary ordering: allocations first where the row says
// so (§9.1 row A25, §9.2's allocs-per-delivery rows), otherwise the geomean.
func lessOn(a, b *kindScore, rule decisionRule) bool {
	if rule.AllocsFirst && a.AllocsMax != b.AllocsMax {
		return a.AllocsMax < b.AllocsMax
	}
	return a.GeomeanNs < b.GeomeanNs
}

// applyCeilings drops the candidates that breach a ceiling the row states,
// unless that would drop them all -- in which case the criterion is recorded
// as met by no kind, which is a fact for the ADR rather than a reason to
// decide nothing.
func applyCeilings(sc *scorecard, byKind map[string]*kindScore, live []string, rule decisionRule) []string {
	if rule.ZeroAllocs {
		var zero, allocating []string
		for _, k := range live {
			if byKind[k].AllocsMax == 0 {
				zero = append(zero, k)
				continue
			}
			allocating = append(allocating, k)
		}
		switch {
		case len(zero) == 0:
			sc.Notes = append(sc.Notes, "§9.1 requires zero allocations of this row's default and no kind reached it")
		case len(allocating) > 0:
			sc.Notes = append(sc.Notes, fmt.Sprintf("excluded from the default by the row's zero-allocation criterion: %s",
				strings.Join(allocating, ", ")))
			live = zero
		}
	}
	// The ceilings are visited in a fixed order: a scorecard is what an ADR
	// is rendered from and must be a function of the data, notes included.
	units := make([]string, 0, len(rule.MaxMetric))
	for unit := range rule.MaxMetric {
		units = append(units, unit)
	}
	sort.Strings(units)
	for _, unit := range units {
		limit := rule.MaxMetric[unit]
		var under, over []string
		for _, k := range live {
			if v, ok := byKind[k].MaxMetric[unit]; ok && v > limit {
				over = append(over, k)
				continue
			}
			under = append(under, k)
		}
		switch {
		case len(under) == 0:
			sc.Notes = append(sc.Notes, fmt.Sprintf("§9.1 caps %s at %g for this row's default and no kind is under it", unit, limit))
		case len(over) > 0:
			sort.Strings(over)
			sc.Notes = append(sc.Notes, fmt.Sprintf("excluded from the default by the row's %s ≤ %g criterion: %s",
				unit, limit, strings.Join(over, ", ")))
			live = under
		}
	}
	return live
}

// finish fills in the ratios, the intervals, the dispositions and the notes a
// reader needs to see the decision was not taken on one number.
func finish(sc *scorecard, byKind map[string]*kindScore, kinds, candidates, partial []string,
	rule decisionRule, o decideOpts, decisionRows []row, samples map[string]kindSamples, metric string,
) {
	isCandidate := map[string]bool{}
	for _, k := range candidates {
		isCandidate[k] = true
	}
	best := ""
	for _, k := range candidates {
		if byKind[k].Disqualified || byKind[k].GeomeanNs <= 0 {
			continue
		}
		if best == "" || lessOn(byKind[k], byKind[best], rule) {
			best = k
		}
	}
	ref := byKind[o.ref]
	for _, k := range kinds {
		ks := byKind[k]
		if ref != nil && ref.GeomeanNs > 0 {
			ks.RatioVsRef = ks.GeomeanNs / ref.GeomeanNs
		}
		if best != "" && byKind[best].GeomeanNs > 0 {
			ks.RatioVsBest = ks.GeomeanNs / byKind[best].GeomeanNs
			switch {
			case isCandidate[k] && k != best:
				ks.RatioLo, ks.RatioMid, ks.RatioHi = ratioCI(samples, decisionRows, metric, k, best)
			case k == best:
				ks.RatioLo, ks.RatioMid, ks.RatioHi = 1, 1, 1
			}
		}
		switch {
		case ks.Disqualified:
			ks.Disposition = "disqualified"
		case rule.isBaseline(k):
			ks.Disposition = "baseline (never the default)"
		case rule.notDefault(k) != "":
			ks.Disposition = "measured, never the default: " + rule.notDefault(k)
		case !isCandidate[k]:
			ks.Disposition = fmt.Sprintf("not a candidate: covers %d of %d deciding rows", ks.Benchmarks, ks.DecisionRows)
		case k == sc.Winner:
			ks.Disposition = "default"
		case k == o.ref:
			ks.Disposition = "reference (Safe)"
		case o.classB:
			// the benchmark configuration is "mutually exclusive API shapes": only
			// one shape can BE the API, so a shape the API did not take is
			// a LOSER in §9.4(5)'s sense and there is no profile
			// disposition available to it. Saying otherwise would be the
			// one thing the M6 review must not do -- claim that a shape a
			// caller cannot select is retained for callers who want it.
			ks.Disposition = "loser: a §9.2 shape the API did not take, and mutually exclusive shapes cannot be retained as profiles (§9.4(5))"
		default:
			// the benchmark configuration's rows are "runtime-selectable internals":
			// every arm is reachable through its own `Kind` at the family's
			// `New`, so an arm that is not the default is exactly what
			// §9.4(5) means by "retained as a profile", and the M6
			// disposition review is what says so rather than leaving it
			// open.
			ks.Disposition = "profile: retained and runtime-selectable through its Kind (§9.4(5))"
		}
		// A removed family's arms are all gone -- the default, the
		// reference and the baselines with them -- so the removal is
		// APPENDED to whichever disposition the kind earned rather than
		// replacing it: which shape the bake-off chose is the part of the
		// record a reader needs most once the code is only in history.
		if o.removedAt != "" {
			ks.Disposition += "; removed (" + o.removedAt + ")"
		}
		sc.Kinds = append(sc.Kinds, *ks)
	}
	for _, k := range partial {
		sc.Notes = append(sc.Notes, fmt.Sprintf("%s cannot run the workload this row is decided on (%d of %d rows) and is not a candidate for the default",
			k, byKind[k].Benchmarks, byKind[k].DecisionRows))
	}
	noteRungCrossover(sc, byKind, candidates)
	for _, sel := range rule.Report {
		noteWorkloadBest(sc, samples, decisionRows, candidates, metric, sel)
	}
}

// noteRungCrossover records a winner that changes with the GOMAXPROCS rung.
// The geomean over the ladder is one number and §0.3(1) asks for one, but a
// kind that wins at one thread and loses at thirty-two is the answer to two
// questions and the ADR has to say so.
func noteRungCrossover(sc *scorecard, byKind map[string]*kindScore, candidates []string) {
	if sc.Winner == "" || len(sc.Rungs) < 2 {
		return
	}
	var disagree []string
	for _, procs := range sc.Rungs {
		key := strconv.Itoa(procs)
		best, bestV := "", math.Inf(1)
		for _, k := range candidates {
			v, ok := byKind[k].GeomeanByProcs[key]
			if !ok || v <= 0 {
				continue
			}
			if v < bestV {
				best, bestV = k, v
			}
		}
		if best != "" && best != sc.Winner {
			disagree = append(disagree, fmt.Sprintf("GOMAXPROCS=%s: %s", key, best))
		}
	}
	if len(disagree) > 0 {
		sc.Notes = append(sc.Notes, fmt.Sprintf("the fastest kind changes with the GOMAXPROCS rung -- %s -- while the geomean over the ladder names %s; the ladder's rungs are separate measurements and this row is not settled by one of them",
			strings.Join(disagree, ", "), sc.Winner))
	}
}

// noteWorkloadBest reports one named workload's own best kind, for a row
// whose criterion names more than one (§9.1 row A27's read-heavy against
// churn). It changes no decision.
func noteWorkloadBest(sc *scorecard, samples map[string]kindSamples, decisionRows []row,
	candidates []string, metric, sel string,
) {
	best, bestV := "", math.Inf(1)
	for _, k := range candidates {
		var v []float64
		for _, r := range decisionRows {
			if !selects(r.name, sel) {
				continue
			}
			if s := samples[k][r][metric]; len(s) > 0 {
				v = append(v, stat.Median(s))
			}
		}
		if g := stat.Geomean(v); g > 0 && g < bestV {
			best, bestV = k, g
		}
	}
	if best != "" {
		sc.Notes = append(sc.Notes, fmt.Sprintf("best on the %q rows alone: %s (%.1f %s geomean)", sel, best, bestV, metric))
	}
}

// ratioCI is the 95% interval of the geomean ratio of a to b over the
// deciding rows: the benchmark configuration step 2's benchstat interval, lifted to
// the geomean §0.3(1) decides on. The seed is derived from the two kinds and
// the row count, so the same raw.txt always yields the same interval.
func ratioCI(samples map[string]kindSamples, decisionRows []row, metric, a, b string) (lo, mid, hi float64) {
	as := make([][]float64, 0, len(decisionRows))
	bs := make([][]float64, 0, len(decisionRows))
	for _, r := range decisionRows {
		av, bv := samples[a][r][metric], samples[b][r][metric]
		if len(av) == 0 || len(bv) == 0 {
			continue
		}
		as = append(as, av)
		bs = append(bs, bv)
	}
	if len(as) == 0 {
		return 0, 0, 0
	}
	return stat.GeomeanRatioCI(as, bs, ciIters, seedFor(a, b, len(as)))
}

// ciIters is the bootstrap resample count. A thousand is enough for a 2.5%
// tail to be stable to about a tenth of a per cent, which is finer than any
// margin §9.1 states.
const ciIters = 1000

func seedFor(a, b string, rows int) uint64 {
	h := uint64(1469598103934665603)
	for _, s := range []string{a, "\x00", b} {
		for i := range len(s) {
			h ^= uint64(s[i])
			h *= 1099511628211
		}
	}
	return h ^ uint64(rows)
}

// noteSuffix appends the row's own note to a refusal, so that a family this
// tool cannot decide says why in the same breath.
func noteSuffix(note string) string {
	if note == "" {
		return ""
	}
	return " Note: " + note
}

func contains(xs []string, x string) bool {
	for _, s := range xs {
		if s == x {
			return true
		}
	}
	return false
}

func appendRung(rungs []int, procs int) []int {
	for _, p := range rungs {
		if p == procs {
			return rungs
		}
	}
	rungs = append(rungs, procs)
	sort.Ints(rungs)
	return rungs
}

// regresses reports why candidate c must not become the default relative to
// the reference under §0.3(3): more than 10% worse p99, footprint or fairness
// while improving throughput. Each metric is tested only where there is data
// for it; which ones ran is on the scorecard (scorecard.Guards), so that no
// record claims a check that could not fire.
func regresses(c, ref *kindScore) string {
	if c.GeomeanNs >= ref.GeomeanNs {
		return ""
	}
	if ref.P99Geomean > 0 && c.P99Geomean > ref.P99Geomean*(1+exclusionMargin) {
		return fmt.Sprintf("p99 %.0f ns vs reference %.0f ns (>10%% worse)", c.P99Geomean, ref.P99Geomean)
	}
	switch {
	case ref.AllocsMax == 0 && c.AllocsMax > 0:
		return fmt.Sprintf("allocates %.0f/op where the reference allocates nothing", c.AllocsMax)
	case ref.BytesMean == 0 && c.BytesMean > 0:
		return fmt.Sprintf("%.0f B/op where the reference allocates nothing", c.BytesMean)
	case ref.BytesMean > 0 && c.BytesMean > ref.BytesMean*(1+exclusionMargin):
		return fmt.Sprintf("B/op %.0f vs reference %.0f (>10%% worse)", c.BytesMean, ref.BytesMean)
	}
	if ref.Fairness > 0 && c.Fairness > ref.Fairness*(1+exclusionMargin) {
		return fmt.Sprintf("fairness %.2f vs reference %.2f (>10%% worse)", c.Fairness, ref.Fairness)
	}
	return ""
}

// splitKind extracts the kind from a benchmark name and returns the name
// with the kind segment removed.
func splitKind(base, root, dim string) (kind, bench string, ok bool) {
	segs := strings.Split(base, "/")
	rootSegs := strings.Split(root, "/")
	if len(segs) <= len(rootSegs) {
		return "", "", false
	}
	if dim != "" {
		for i, s := range segs {
			if k, v, found := strings.Cut(s, "="); found && k == dim {
				rest := append(append([]string{}, segs[:i]...), segs[i+1:]...)
				return v, strings.Join(rest, "/"), true
			}
		}
		return "", "", false
	}
	i := len(rootSegs)
	kind = segs[i]
	if _, v, found := strings.Cut(kind, "="); found {
		kind = v
	}
	rest := append(append([]string{}, segs[:i]...), segs[i+1:]...)
	return kind, strings.Join(rest, "/"), true
}

// packageLOC is every attributable line of a package's non-test Go source:
// the denominator `complexity`'s per-kind counts are a share of.
//
// It exists because `LOCAttributed` answered the wrong question. Assumption
// A26.14 set that flag because a kind whose lines nobody could attribute
// arrived in §0.3(2)'s simplicity band as a ZERO and so read as the simplest
// arm in the family. The flag says "some lines were found", and the rule needs
// "this kind's lines were found": `complexityByDecl` finding nine lines of a
// kind whose implementation is fifty-seven defeats the guard exactly as a zero
// did, one level along.
func packageLOC(pkg string) int {
	entries, err := os.ReadDir(pkg)
	if err != nil {
		return 0
	}
	total := 0
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		data, err := os.ReadFile(filepath.Join(pkg, name))
		if err != nil {
			continue
		}
		for _, line := range strings.Split(string(data), "\n") {
			if t := strings.TrimSpace(line); t != "" && !strings.HasPrefix(t, "//") {
				total++
			}
		}
	}
	return total
}

// declBelongsTo reports whether a top-level declaration is part of a kind's
// implementation.
func declBelongsTo(d ast.Decl, slug, ident string) bool {
	names := []string{}
	switch d := d.(type) {
	case *ast.FuncDecl:
		names = append(names, d.Name.Name)
		if d.Recv != nil && len(d.Recv.List) > 0 {
			names = append(names, receiverName(d.Recv.List[0].Type))
		}
	case *ast.GenDecl:
		for _, sp := range d.Specs {
			switch sp := sp.(type) {
			case *ast.TypeSpec:
				names = append(names, sp.Name.Name)
			case *ast.ValueSpec:
				for _, n := range sp.Names {
					names = append(names, n.Name)
				}
			}
		}
	}
	low := strings.ToLower(slug)
	for _, n := range names {
		if n == "" {
			continue
		}
		if ident != "" && strings.Contains(n, ident) {
			return true
		}
		ln := strings.ToLower(n)
		if low != "" && (strings.HasPrefix(ln, low) || strings.HasSuffix(ln, low)) {
			return true
		}
	}
	return false
}

// receiverName is the type name of a method's receiver, pointer or not.
func receiverName(e ast.Expr) string {
	switch e := e.(type) {
	case *ast.StarExpr:
		return receiverName(e.X)
	case *ast.Ident:
		return e.Name
	case *ast.IndexExpr: // a generic receiver, Type[P]
		return receiverName(e.X)
	case *ast.IndexListExpr:
		return receiverName(e.X)
	}
	return ""
}

// LOCText renders the implementation size for a report: the count, or `n/a`
// when nothing could be attributed to the kind. Printing a bare 0 there is
// what made "simplest preferred" look like a statement about the code.
func (k kindScore) LOCText() string {
	// A positive count is self-evidently attributed. The test is written this
	// way so that a scorecard stored before `loc_attributed` existed still
	// renders the number it measured, while the ones that stored a zero --
	// which is every arm of three families and some arms of three more --
	// render what that zero actually meant.
	if k.LOC > 0 || k.LOCAttributed {
		return strconv.Itoa(k.LOC)
	}
	return "n/a"
}

// readResults parses one file of `go test -bench` output, compressed or not.
//
// A run's `raw.txt` is gitignored and its `raw.txt.gz` is what gets committed
// beside the scorecard, so a path whose file is absent is retried with a `.gz`
// suffix. Without this, re-deciding a family from the evidence in the
// repository is impossible on any tree that did not measure it -- which is
// the reason the simplicity metric's defect stood recorded and unrepaired
// while the repair itself was small.
func readResults(path string) ([]benchfmt.Result, error) {
	name := path
	if _, err := os.Stat(name); err != nil && !strings.HasSuffix(name, ".gz") {
		if _, gzErr := os.Stat(name + ".gz"); gzErr == nil {
			name += ".gz"
		}
	}
	f, err := os.Open(name)
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()
	var r io.Reader = f
	if strings.HasSuffix(name, ".gz") {
		zr, err := gzip.NewReader(f)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", name, err)
		}
		defer func() { _ = zr.Close() }()
		r = zr
	}
	return benchfmt.Parse(r)
}

func slugify(s string) string {
	var b strings.Builder
	for _, r := range strings.ToLower(s) {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9':
			b.WriteRune(r)
		default:
			if b.Len() > 0 && !strings.HasSuffix(b.String(), "-") {
				b.WriteByte('-')
			}
		}
	}
	return strings.Trim(b.String(), "-")
}

func packageName(dir string) string {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return ""
	}
	fset := token.NewFileSet()
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".go") || strings.HasSuffix(e.Name(), "_test.go") {
			continue
		}
		if f, err := parser.ParseFile(fset, filepath.Join(dir, e.Name()), nil, parser.PackageClauseOnly); err == nil {
			return f.Name.Name
		}
	}
	return ""
}

func writeAuto(path string, o decideOpts, sc *scorecard) error {
	dir := filepath.Dir(path)
	pkg := packageName(dir)
	if pkg == "" {
		return fmt.Errorf("cannot determine package name of %s", dir)
	}
	if sc.Winner == "" {
		return fmt.Errorf("%s: no winner was named (%s); nothing to bind", o.family, sc.Rule)
	}
	ident := ""
	for i := range sc.Kinds {
		if sc.Kinds[i].Kind == sc.Winner {
			ident = sc.Kinds[i].Ident
		}
	}
	if ident == "" {
		return fmt.Errorf("no Go identifier known for winner %q; pass -idents", sc.Winner)
	}
	adrRef := "pending"
	if o.adrNum > 0 {
		adrRef = fmt.Sprintf("ADR-%03d", o.adrNum)
	}
	// A package that hosts one family names its binding AutoKind; one that
	// hosts several (internal/engine has the architecture and the fan-out
	// policy) prefixes each with the family's own name.
	kindConst, provConst := "AutoKind", "AutoProvisional"
	auto := "Auto"
	if o.autoPrefix != "" {
		kindConst = o.autoPrefix + "AutoKind"
		provConst = o.autoPrefix + "AutoProvisional"
		auto = o.autoPrefix + "Auto"
	}
	var b strings.Builder
	fmt.Fprintf(&b, "// Code generated by bakeoff decide (%s, %s); DO NOT EDIT.\n\n", o.family, adrRef)
	fmt.Fprintf(&b, "package %s\n\n", pkg)
	fmt.Fprintf(&b, "// %s is the benchmark-selected default bound to %s (variant %s, %s).\n",
		kindConst, auto, o.family, adrRef)
	fmt.Fprintf(&b, "const %s = %s\n\n", kindConst, ident)
	fmt.Fprintf(&b, "// %s reports that %s was chosen on a non-authoritative host\n// and must be re-ratified before the v1 defaults freeze.\n",
		provConst, kindConst)
	fmt.Fprintf(&b, "const %s = %v\n", provConst, sc.Provisional)
	return os.WriteFile(path, []byte(b.String()), 0o644)
}

// repoPath rewrites a path the caller gave relative to its own directory
// (hack/bakeoff.sh runs `decide` from benchmarks/ and passes `../<PKG>`) as a
// repository-relative one, so that the ADR and the scorecard name a path a
// reader can open from the repository root. "-" (no generated binding) and an
// absolute path are left alone.
func repoPath(p string) string {
	if p == "" || p == "-" || filepath.IsAbs(p) {
		return p
	}
	p = filepath.ToSlash(filepath.Clean(p))
	for {
		rest, ok := strings.CutPrefix(p, "../")
		if !ok {
			return p
		}
		p = rest
	}
}

// readSysinfo copies the machine identity out of a run's sysinfo.json. A
// missing or malformed file is not an error: a dry run and a unit test have
// no run directory, and the ADR then says the host is unrecorded rather than
// refusing to render.
func readSysinfo(path string) (*hostInfo, string, string, bool, bool) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, "", "", false, false
	}
	var si struct {
		hostInfo
		GitCommit       string `json:"git_commit"`
		GitCommitStatus string `json:"git_commit_status"`
		GitDirty        bool   `json:"git_dirty"`
		GitDirtySource  bool   `json:"git_dirty_source"`
	}
	if err := json.Unmarshal(data, &si); err != nil {
		return nil, "", "", false, false
	}
	h := si.hostInfo
	return &h, si.GitCommit, si.GitCommitStatus, si.GitDirty, si.GitDirtySource
}

// gitCommit is the fallback when no sysinfo.json was written (a `quick` run
// started by hand, say). An empty result is reported as "unrecorded".
func gitCommit(dir string) string {
	if dir == "" {
		dir = "."
	}
	cmd := exec.Command("git", "-C", dir, "rev-parse", "HEAD")
	out, err := cmd.Output()
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(out))
}

func render(tmpl *template.Template, path string, sc *scorecard, o decideOpts) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	return tmpl.Execute(f, map[string]any{
		"S":        sc,
		"AutoKey":  o.autoKey,
		"RawPath":  repoRelative(o.rawPath),
		"AutoFile": o.autoFile,
		"Date":     time.Now().UTC().Format("2006-01-02"),
		"ADR":      fmt.Sprintf("ADR-%03d", o.adrNum),
		"Title":    o.title,
	})
}

var funcs = template.FuncMap{
	"pct": func(r float64) string {
		if r == 0 {
			return "n/a"
		}
		return fmt.Sprintf("%+.1f%%", (r-1)*100)
	},
	"f1": func(v float64) string { return fmt.Sprintf("%.1f", v) },
	"f0": func(v float64) string { return fmt.Sprintf("%.0f", v) },
	// ci renders a 95% confidence interval of a geomean ratio as the two
	// percentages it brackets, which is what tells a real difference from
	// noise: an interval that spans 0% establishes nothing.
	"ci": func(lo, hi float64) string {
		if lo == 0 && hi == 0 {
			return "n/a"
		}
		return fmt.Sprintf("%+.1f%% … %+.1f%%", (lo-1)*100, (hi-1)*100)
	},
	"rungs": func(rs []int) string {
		out := make([]string, 0, len(rs))
		for _, r := range rs {
			out = append(out, strconv.Itoa(r))
		}
		return strings.Join(out, ", ")
	},
	"rungOf": func(k kindScore, procs int) string {
		v, ok := k.GeomeanByProcs[strconv.Itoa(procs)]
		if !ok {
			return "—"
		}
		return fmt.Sprintf("%.1f", v)
	},
	"or1": func(s, alt string) string {
		if s == "" {
			return alt
		}
		return s
	},
	"gstatus": func(status string) string {
		if status == guardEvaluated {
			return "evaluated"
		}
		return "**not evaluated**"
	},
}

var reportTmpl = template.Must(template.New("report").Funcs(funcs).Parse(`# Bake-off {{.S.Family}} — ` + "`{{.S.BenchRoot}}`" + `

- **Status:** provisional (shared host){{if not .S.Deciding}} — **profile ` + "`{{or1 .S.Profile \"unrecorded\"}}`" + ` decides nothing (assumption A24.33)**{{end}}
- **Decision:** {{or1 .S.Winner "not decided"}} — {{.S.Rule}}
- **§9.1 criterion:** {{or1 .S.Criterion "—"}}
- **Decided on:** {{len .S.Common}} row(s) of ` + "`{{.S.Metric}}`" + `{{if .S.Rungs}} at GOMAXPROCS {{rungs .S.Rungs}}{{end}}
{{if .S.Workload}}- **Deciding workload:** {{.S.Workload}}
{{end}}- **ADR:** {{.ADR}}
- **Reference kind:** {{.S.Reference}}
- **Decided:** {{.S.DecidedAt}}
- **sysinfo:** ` + "`{{.S.Sysinfo}}`" + `
- **Raw results:** ` + "`{{.RawPath}}`" + `

## Results

| kind | geomean {{.S.Metric}} | 95% CI vs best | vs ref | vs best | max allocs/op | B/op mean | p99 geomean | LOC | unsafe | rows | disposition |
|---|---|---|---|---|---|---|---|---|---|---|---|
{{range .S.Kinds}}| {{.Kind}} | {{f1 .GeomeanNs}} | {{ci .RatioLo .RatioHi}} | {{pct .RatioVsRef}} | {{pct .RatioVsBest}} | {{f0 .AllocsMax}} | {{f1 .BytesMean}} | {{f1 .P99Geomean}} | {{.LOCText}} | {{.Unsafe}} | {{.Benchmarks}}/{{.DecisionRows}} | {{.Disposition}} |
{{end}}
## §0.3(3) exclusion guards

| metric | unit | status | detail |
|---|---|---|---|
{{range .S.Guards}}| {{.Metric}} | ` + "`{{.Unit}}`" + ` | {{gstatus .Status}} | {{or1 .Detail "—"}} |
{{end}}{{if .S.Unobservable}}
Parts of this row's criterion that no benchmark here can evaluate:
{{range .S.Unobservable}}
- {{.}}{{end}}
{{end}}
## Per-rung geomeans ({{.S.Metric}})

| kind |{{range .S.Rungs}} GOMAXPROCS={{.}} |{{end}}
|---|{{range .S.Rungs}}---|{{end}}
{{$r := .S.Rungs}}{{range .S.Kinds}}| {{.Kind}} |{{$k := .}}{{range $r}} {{rungOf $k .}} |{{end}}
{{end}}
## Per-row medians ({{.S.Metric}})

| row |{{range .S.Kinds}} {{.Kind}} |{{end}}
|---|{{range .S.Kinds}}---|{{end}}
{{$k := .S.Kinds}}{{range .S.Common}}| {{.}} |{{$b := .}}{{range $k}} {{f1 (index .Medians $b)}} |{{end}}
{{end}}
## Caveats

Non-authoritative until re-run on pinned hardware. {{range .S.Notes}}
- {{.}}{{end}}
`))

// repoRelative renders a filesystem path the way a committed page has to carry
// it: relative to the repository root, with forward slashes.
//
// `hack/bakeoff.sh` passes `-raw "$root/benchmarks/reports/results/<run>"`,
// which is absolute, and the variant page's "Raw results" line took it
// verbatim -- so 45 of the 47 committed pages under docs/benchmarks/variants
// name one agent's worktree: `docs/benchmarks/variants/A11.md` points at
// `/home/.../iqmq-wt/b05/benchmarks/reports/results/…`, A12 at `b07`, A16 at
// `b06`. A path that exists on one machine, in one worktree, for as long as
// that worktree does, is worse than no path at all. The scorecard's own
// `RawResults` field was always built relative and is the shape this matches.
//
// The repository root is found by walking up for a `.git` entry -- a directory
// in a clone, a file in a linked worktree, so os.Stat is the right test for
// both. A path with no `.git` above it (a temporary directory in a test) is
// returned cleaned and unchanged, because inventing a relative path for it
// would be a guess.
func repoRelative(p string) string {
	if p == "" {
		return ""
	}
	abs, err := filepath.Abs(p)
	if err != nil {
		return filepath.ToSlash(filepath.Clean(p))
	}
	for dir := abs; ; {
		if _, err := os.Stat(filepath.Join(dir, ".git")); err == nil {
			rel, rerr := filepath.Rel(dir, abs)
			if rerr != nil {
				break
			}
			return filepath.ToSlash(rel)
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			break
		}
		dir = parent
	}
	return filepath.ToSlash(filepath.Clean(p))
}
