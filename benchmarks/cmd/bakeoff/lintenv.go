package main

import (
	"bufio"
	"bytes"
	"fmt"
	"go/version"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
)

// The last group is the declaration this file exists to check. A campaign was
// launched and stopped two hours in because two families' declarations
// produced data their §9.1 decision could not be taken from, and nothing in
// the repository could say so ahead of the measurement:
//
//   - family A1's grid held P=1,4 / batch=1,16 / occupancy=50, against a
//     criterion written about MPMC contention and near-full/near-empty
//     occupancy — none of which it measured;
//   - family A22's four kinds had no benchmark name in common at all, so
//     `bakeoff decide` would have failed with "no benchmark is present for
//     every kind" after paying for the whole family's measurement;
//   - family A1's kinds *did* have two names in common, and both were SPSC
//     rows: `spsc` would have won the family on two single-producer
//     benchmarks and been bound as the engine's default MPMC lane queue.
//
// So a declaration now says which grid it is measured at (MATRIX), which
// dimensions the decision is read at (DECIDED_AT), which metrics it is read
// on (DECIDED_ON), which kinds cannot run the whole workload (PARTIAL_KINDS),
// which criterion metrics this family's package cannot produce at all
// (CRITERION_GAP), and which other family shares its benchmark run
// (SHARES_ROOT) — and every one of those is checked against a
// `-benchtime=1x` listing, which needs no measurement.
type bakeoffFamily struct {
	// requiresGo is REQUIRES_GO: the oldest toolchain on which every kind of
	// the family exists. On an older one the kinds test announces fewer kinds
	// than the family has, so the comparability checks would report a
	// defect in the declaration that is really a fact about the toolchain.
	requiresGo string

	id string
	//
	// A12's arms are two client LIBRARIES, so the package that constructs
	// both has to import both, and no root-module package can: the root
	// module is standard-library-only. hack/bakeoff.sh moves `go test` with
	// `-C` for such a family, and this file has to run its listings in the
	// same place or it would lint a package the campaign does not measure.
	moddir    string
	pkg       string
	benchRoot string
	kindsTest string
	kindDim   string
	extraArgs []string

	// matrix is IQMQ_BENCH_MATRIX for this family's whole bake-off run: the
	// correctness gate, the kind list, the filter probe and the measurement.
	matrix string
	// criterion is the benchmark configuration's decision column, verbatim. It is
	// documentation for a reader, not a checked field.
	criterion string
	// decidedAt names the benchmark-name segments the criterion is read at.
	decidedAt []string
	// decidedOn names the metrics it is read on.
	decidedOn []string
	// partial maps a kind that cannot run the family's whole workload to the
	// reason. Those kinds are excluded from the comparable set.
	partial map[string]string

	removed string
	// class is the benchmark configuration's class: "A" (runtime-selectable internals,
	// the default) or "B" (mutually exclusive API shapes).
	class string
	// gap maps a criterion metric this family's package cannot produce to
	// where the evidence for it lives instead.
	gap map[string]string
	// sharesRoot lists the other families measured from this benchmark root.
	sharesRoot []string
}

// root returns the directory a family's `go test` runs in: the repository root
// for a root-module family, and its MODDIR otherwise. It is the one place the
// two cases meet, so a check that forgot MODDIR is a compile error rather than
// a listing of the wrong package.
func (f bakeoffFamily) root(repoRoot string) string {
	if f.moddir == "" {
		return repoRoot
	}
	return filepath.Join(repoRoot, f.moddir)
}

// gatePkg is the family's package as a REPOSITORY-relative path, which is what
// the benchmark gate and hack/gate-if-touched.sh both need: the first compiles
// it, the second matches it against `git diff --name-only`, and neither works
// with a path that is relative to a nested module.
func (f bakeoffFamily) gatePkg() string {
	rel := strings.TrimPrefix(f.pkg, "./")
	if f.moddir == "" {
		return f.pkg
	}
	if rel == "." || rel == "" {
		return "./" + f.moddir
	}
	return "./" + path.Join(f.moddir, rel)
}

// benchRow is one line of a `go test -bench` listing: the name as printed,
// and the units it reported.
type benchRow struct {
	name  string
	units []string
}

// matrixValues are the IQMQ_BENCH_MATRIX grids a family may declare.
//
// `huge` is deliberately not among them. Two benchmark files offer it
// (internal/queue, internal/timer) for the sweeps that go beyond what any
// §9.1 criterion reads — the queue's 894-row cross is 9.7 hours per build at
// the harness's ten counts × four GOMAXPROCS rungs × one second — and a
// family that declared it would put the campaign into the region of a week.
var matrixValues = map[string]bool{"default": true, "full": true}

// comparableDivisor is the fraction of a family's measured rows that its
// decision must be able to read: |common| >= |union| / comparableDivisor.
//
// Three rather than two, because a family may deliberately measure a crossing
// on the arms that can vary it — family A22's two adapters are measured over
// both delivery positions while its two callback kinds pin one each, so the
// comparable core is half the rows and the other half is data the ADR reads
// even though `bakeoff decide` does not. What the threshold has to catch is a
// decision taken from a corner: family A1's declaration produced a comparable
// set of two rows out of 106 (1.9%), both of them SPSC, for a criterion about
// MPMC contention.
const comparableDivisor = 3

// readBakeoffEnvs loads every family declaration under hack/bakeoff, in id
// order.
func readBakeoffEnvs(root string) ([]bakeoffFamily, error) {
	paths, err := filepath.Glob(filepath.Join(root, "hack", "bakeoff", "*.env"))
	if err != nil {
		return nil, err
	}
	fams := make([]bakeoffFamily, 0, len(paths))
	for _, path := range paths {
		data, err := os.ReadFile(path)
		if err != nil {
			return nil, err
		}
		env := parseEnvAssignments(data)
		fam := bakeoffFamily{
			id:         strings.TrimSuffix(filepath.Base(path), ".env"),
			moddir:     strings.TrimSuffix(strings.TrimPrefix(env["MODDIR"], "./"), "/"),
			pkg:        env["PKG"],
			benchRoot:  env["BENCH"],
			kindsTest:  env["KINDS_TEST"],
			kindDim:    env["KIND_DIM"],
			extraArgs:  strings.Fields(env["EXTRA_ARGS"]),
			matrix:     env["MATRIX"],
			criterion:  env["CRITERION"],
			decidedAt:  strings.Fields(env["DECIDED_AT"]),
			decidedOn:  strings.Fields(env["DECIDED_ON"]),
			partial:    parseEnvList(env["PARTIAL_KINDS"]),
			gap:        parseEnvList(env["CRITERION_GAP"]),
			sharesRoot: strings.Fields(env["SHARES_ROOT"]),
			removed:    env["REMOVED"],
			class:      env["CLASS"],
			requiresGo: env["REQUIRES_GO"],
		}
		if fam.class == "" {
			fam.class = "A"
		}
		if fam.kindsTest == "" {
			fam.kindsTest = "TestKindsList"
		}
		if fam.moddir == "." {
			fam.moddir = ""
		}
		fams = append(fams, fam)
	}
	sort.Slice(fams, func(i, j int) bool { return fams[i].id < fams[j].id })
	return fams, nil
}

// parseEnvAssignments reads the KEY=VALUE lines of a bake-off env file. The
// files are sourced by hack/bakeoff.sh, so only the plain assignment form is
// recognised here: a declaration that needed more than that would be linted
// differently from the way it is measured, which is the whole failure this
// check exists to stop.
func parseEnvAssignments(data []byte) map[string]string {
	out := map[string]string{}
	sc := bufio.NewScanner(bytes.NewReader(data))
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		key, value, ok := strings.Cut(line, "=")
		if !ok || key == "" || strings.ContainsAny(key, " \t") {
			continue
		}
		out[key] = unquoteEnv(value)
	}
	return out
}

// unquoteEnv strips one layer of shell quoting from an assignment's value.
func unquoteEnv(v string) string {
	v = strings.TrimSpace(v)
	if len(v) >= 2 && (v[0] == '"' || v[0] == '\'') && v[len(v)-1] == v[0] {
		return v[1 : len(v)-1]
	}
	return v
}

// parseEnvList reads a "key=text; key=text" declaration — PARTIAL_KINDS and
// CRITERION_GAP, whose values are prose and so cannot be separated by spaces.
// An entry with no text is kept with an empty value, which the checks report
// as a missing reason rather than silently accepting.
func parseEnvList(v string) map[string]string {
	if strings.TrimSpace(v) == "" {
		return nil
	}
	out := map[string]string{}
	for _, entry := range strings.Split(v, ";") {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}
		key, text, _ := strings.Cut(entry, "=")
		out[strings.TrimSpace(key)] = strings.TrimSpace(text)
	}
	return out
}

// trimProcs strips the -<GOMAXPROCS> suffix `go test` appends to a benchmark
// name.
func trimProcs(name string) string {
	if i := strings.LastIndexByte(name, '-'); i > 0 {
		if _, err := strconv.Atoi(name[i+1:]); err == nil {
			return name[:i]
		}
	}
	return name
}

// underRoot reports whether a benchmark name sits under the family's root.
func underRoot(name, benchRoot string) bool {
	return name == benchRoot || strings.HasPrefix(name, benchRoot+"/")
}

// selectsKind counts the benchmarks the bake-off would measure one kind from.
// It is the Go-side equivalent of the two filters hack/bakeoff.sh runs
//
//	-bench '^<root>/<slug>(/|$)'   (the kind is a path segment)
//	-bench '^<root>(/|$)'          (the kind is a <dim>=<slug> dimension)
//
// and it splits the kind out of a name exactly as `bakeoff decide` does
// (splitKind), so a name this accepts is a name the campaign measures *and*
// the decision can attribute to a kind. Anything looser would let a family
// pass the lint and then produce a scorecard with a kind missing.
func selectsKind(names []string, benchRoot, kindDim, slug string) int {
	n := 0
	for _, name := range names {
		base := trimProcs(name)
		if !underRoot(base, benchRoot) {
			continue
		}
		if kind, _, ok := splitKind(base, benchRoot, kindDim); ok && kind == slug {
			n++
		}
	}
	return n
}

// rowsByKind groups a listing the way `bakeoff decide` does: the kind is
// split out of each name and what remains — the name the kinds are compared
// on — is the key, mapped to the units that row reported.
//
// It returns the rows attributable to no kind as well. Those are measured by
// the whole-root filter of a KIND_DIM family and by nothing at all for a
// family whose kind is a path segment, and either way `bakeoff decide` drops
// them: family A4's `time.Timer` baseline rows and family A5's aggregate
// `Lane`/`KafkaPartition` rows are the two live examples.
func rowsByKind(rows []benchRow, benchRoot, kindDim string) (byKind map[string]map[string][]string, unattributed []string) {
	byKind = map[string]map[string][]string{}
	for _, row := range rows {
		base := trimProcs(row.name)
		if !underRoot(base, benchRoot) {
			continue
		}
		kind, rest, ok := splitKind(base, benchRoot, kindDim)
		if !ok {
			unattributed = append(unattributed, base)
			continue
		}
		if byKind[kind] == nil {
			byKind[kind] = map[string][]string{}
		}
		byKind[kind][rest] = row.units
	}
	sort.Strings(unattributed)
	return byKind, unattributed
}

// comparableRows is the set of names present for every one of kinds, sorted:
// the set `bakeoff decide` intersects and computes its geomeans over, and so
// the only rows a family's decision can be taken from.
func comparableRows(byKind map[string]map[string][]string, kinds []string) []string {
	if len(kinds) == 0 {
		return nil
	}
	var common []string
	for rest := range byKind[kinds[0]] {
		ok := true
		for _, k := range kinds[1:] {
			if _, has := byKind[k][rest]; !has {
				ok = false
				break
			}
		}
		if ok {
			common = append(common, rest)
		}
	}
	sort.Strings(common)
	return common
}

// unionRows counts the distinct comparison names the family measures across
// all its kinds: what the campaign pays for, against which comparableRows is
// what the decision can read.
func unionRows(byKind map[string]map[string][]string, kinds []string) int {
	seen := map[string]bool{}
	for _, k := range kinds {
		for rest := range byKind[k] {
			seen[rest] = true
		}
	}
	return len(seen)
}

// hasSegment reports whether term is a whole path segment of any of names.
// DECIDED_AT terms are segments — "MPMC", "P=32", "occupancy=95", "16" — so a
// substring match would let "occupancy=9" stand for "occupancy=95".
func hasSegment(names []string, term string) bool {
	for _, name := range names {
		for _, seg := range strings.Split(name, "/") {
			if seg == term {
				return true
			}
		}
	}
	return false
}

// spanningKinds are the declared kinds that are not declared PARTIAL_KINDS:
// the ones the family is decided by comparing.
func spanningKinds(fam bakeoffFamily, slugs []string) []string {
	var out []string
	for _, slug := range slugs {
		if _, isPartial := fam.partial[slug]; !isPartial {
			out = append(out, slug)
		}
	}
	return out
}

// checkComparability holds a family to the two things `bakeoff decide` needs
// of its kinds and that neither the campaign nor the lint used to check ahead
// of the measurement: the kinds must have benchmark names in common, and
// those names must be most of what the family measures rather than a corner
// of it.
//
// A kind that legitimately cannot run the whole workload is declared in
// PARTIAL_KINDS with its reason and excluded from the comparison — family
// A1's `spsc` and `mpsc` are single-producer and single-consumer by
// construction (`queue.Kind.Modes`), so of 106 rows the intersection across
// all nine kinds was the two SPSC rows and `spsc` would have won the family.
// A stale PARTIAL_KINDS entry — a kind that does span the workload — fails
// too, because excluding a kind that could have been compared is how a
// declaration quietly narrows its own bake-off.
func checkComparability(fam bakeoffFamily, byKind map[string]map[string][]string, slugs []string) (common, problems []string) {
	for _, slug := range sortedKeys(fam.partial) {
		if _, declared := byKind[slug]; !declared {
			problems = append(problems, fmt.Sprintf(
				"%s: PARTIAL_KINDS names %q, which is not a kind of this family (%s announces %s)",
				fam.id, slug, fam.kindsTest, strings.Join(slugs, ","),
			))
			continue
		}
		if fam.partial[slug] == "" {
			problems = append(problems, fmt.Sprintf(
				"%s: PARTIAL_KINDS entry %q has no reason; write it as %q",
				fam.id, slug, slug+"=<what this kind cannot run>",
			))
		}
	}
	spanning := spanningKinds(fam, slugs)
	if len(spanning) < 2 {
		problems = append(problems, fmt.Sprintf(
			"%s: %d of %d kinds are declared PARTIAL_KINDS, leaving nothing to compare; a family needs at least two kinds that span its workload",
			fam.id, len(fam.partial), len(slugs),
		))
		return nil, problems
	}
	common = comparableRows(byKind, spanning)
	union := unionRows(byKind, slugs)
	if len(common) == 0 {
		problems = append(problems, fmt.Sprintf(
			"%s: no benchmark name is common to the kinds %s once the kind is stripped, so `bakeoff decide -family %s` fails with \"no benchmark is present for every kind\" after measuring all %d rows; %s",
			fam.id, strings.Join(spanning, ","), fam.id, union, narrowestKinds(byKind, spanning),
		))
		return nil, problems
	}
	if len(common)*comparableDivisor < union {
		problems = append(problems, fmt.Sprintf(
			"%s: only %d of the %d rows the campaign measures are common to the kinds %s (%.0f%%, at least %d%% required), so the family would be decided from a corner of its own workload; %s",
			fam.id, len(common), union, strings.Join(spanning, ","),
			100*float64(len(common))/float64(union), 100/comparableDivisor,
			narrowestKinds(byKind, spanning),
		))
	}
	for _, slug := range sortedKeys(fam.partial) {
		rows, declared := byKind[slug]
		if !declared {
			continue
		}
		missing := false
		for _, rest := range common {
			if _, has := rows[rest]; !has {
				missing = true
				break
			}
		}
		if !missing {
			problems = append(problems, fmt.Sprintf(
				"%s: kind %q is declared PARTIAL_KINDS but has every one of the %d comparable rows; drop it from the declaration so the bake-off compares it",
				fam.id, slug, len(common),
			))
		}
	}
	return common, problems
}

// narrowestKinds names the kinds with the fewest rows, which is what an empty
// or tiny comparable set is nearly always caused by.
func narrowestKinds(byKind map[string]map[string][]string, kinds []string) string {
	widest := 0
	for _, k := range kinds {
		widest = max(widest, len(byKind[k]))
	}
	var narrow []string
	for _, k := range kinds {
		if len(byKind[k]) < widest {
			narrow = append(narrow, fmt.Sprintf("%s has %d rows against %d", k, len(byKind[k]), widest))
		}
	}
	if len(narrow) == 0 {
		return "every kind has the same number of rows, so it is the names themselves that differ"
	}
	sort.Strings(narrow)
	return "narrowest: " + strings.Join(narrow, ", ") +
		" -- declare the narrow kinds in PARTIAL_KINDS with the reason, or give them the missing rows"
}

// checkDecidedAt requires every dimension the declaration says the decision
// is read at to appear in the comparable set the declared MATRIX produces.
//
// This is the check the stopped campaign needed. Family A1's criterion is
// read under MPMC contention at near-full and near-empty occupancy, and the
// grid `hack/bakeoff.sh` ran produced occupancy=50 and P=1,4 only; family
// A4's is read at 1e6 entries and the grid produced 1e3 and 1e5. Both were
// green in every gate in the repository.
func checkDecidedAt(fam bakeoffFamily, common []string) (problems []string) {
	if len(fam.decidedAt) == 0 {
		return []string{fmt.Sprintf(
			"%s: env file declares no DECIDED_AT; name the benchmark-name segments the benchmark configuration's criterion for this row is read at (\"MPMC\", \"P=32\", \"n=1000000\", ...), so that a grid which does not produce them fails here rather than after the measurement",
			fam.id,
		)}
	}
	for _, term := range fam.decidedAt {
		if hasSegment(common, term) {
			continue
		}
		problems = append(problems, fmt.Sprintf(
			"%s: DECIDED_AT names %q and MATRIX=%s produces no comparable row with that segment; the campaign would measure the family for hours and its criterion (%s) could not be read from the result",
			fam.id, term, fam.declaredMatrix(), fam.criterionOrUnknown(),
		))
	}
	return problems
}

// checkDecidedOn requires every metric the decision is read on to be reported
// by a comparable row of every kind, and every metric declared missing in
// CRITERION_GAP to be genuinely missing.
//
// Family A21 is why the gap half exists. Its §9.1 criterion is "lowest peak,
// fast recovery" and the retry-storm simulation that produces
// peak-concurrency and recover-s lives in `benchmarks/sim/retrystorm` — the
// nested benchmarks module, which a `go test` run in the family's PKG cannot
// reach, exactly as family A29's benchmark once did. The
// campaign measures three microbenchmark rows instead, and the ADR would be
// written from a number nothing in the campaign produced. That is now
// declared rather than discovered.
func checkDecidedOn(fam bakeoffFamily, byKind map[string]map[string][]string, spanning, common []string) (problems []string) {
	if len(fam.decidedOn) == 0 {
		problems = append(problems, fmt.Sprintf(
			"%s: env file declares no DECIDED_ON; name the metrics the decision reads (ns/op, allocs/op, p99-ns, msgs/s, ...)", fam.id,
		))
	}
	for _, unit := range fam.decidedOn {
		for _, kind := range spanning {
			if reportsUnit(byKind[kind], common, unit) {
				continue
			}
			problems = append(problems, fmt.Sprintf(
				"%s: DECIDED_ON names %q and no comparable row of kind %q reports it; the decision would be taken on a metric the benchmark does not produce",
				fam.id, unit, kind,
			))
			break
		}
	}
	for _, unit := range sortedKeys(fam.gap) {
		if fam.gap[unit] == "" {
			problems = append(problems, fmt.Sprintf(
				"%s: CRITERION_GAP entry %q says nothing about where the evidence for it lives; write it as %q",
				fam.id, unit, unit+"=<path or command that produces it>",
			))
		}
		for _, kind := range spanning {
			if !reportsUnit(byKind[kind], nil, unit) {
				continue
			}
			problems = append(problems, fmt.Sprintf(
				"%s: CRITERION_GAP claims %q is not measurable here, but rows of kind %q under %s report it; move it to DECIDED_ON",
				fam.id, unit, kind, fam.benchRoot,
			))
			break
		}
	}
	return problems
}

// reportsUnit reports whether one kind reports unit on at least one of the
// given rows. With rows nil every row of the kind is considered.
func reportsUnit(rows map[string][]string, only []string, unit string) bool {
	has := func(units []string) bool {
		for _, u := range units {
			if u == unit {
				return true
			}
		}
		return false
	}
	if only == nil {
		for _, units := range rows {
			if has(units) {
				return true
			}
		}
		return false
	}
	for _, rest := range only {
		if has(rows[rest]) {
			return true
		}
	}
	return false
}

// checkSharedRoots holds the families that share a benchmark root to what the
// campaign has to know about them.
//
// Two live cases. Families A2 (fan-out storage architecture) and A7 (fan-out
// admission policy) declare the same PKG and BENCH with different KIND_DIMs,
// so `hack/bakeoff.sh` measures the same 112 rows twice — an hour and a
// quarter of a shared host for one run's worth of data — and nothing said so.
// Families A1 and A10 declare the same PKG and BENCH with different build
// tags, and row A10's criterion ("adopt unsafe only if > 10% gain") is a join
// of the two scorecards on the row name, which is empty unless both are
// measured at the same MATRIX.
//
// Two families declaring the same root, dimension AND build tags are not two
// families at all: they would measure and decide the identical thing twice
// under two names, and `bakeoff adr` would render both.
func checkSharedRoots(fams []bakeoffFamily) (problems []string) {
	type key struct{ moddir, pkg, bench string }
	// The families are held by pointer: a declaration is a wide struct and
	// this walks every pair of every group.
	groups := map[key][]*bakeoffFamily{}
	var order []key
	for i := range fams {
		fam := &fams[i]
		if fam.pkg == "" || fam.benchRoot == "" {
			continue
		}
		k := key{fam.moddir, fam.pkg, fam.benchRoot}
		if _, seen := groups[k]; !seen {
			order = append(order, k)
		}
		groups[k] = append(groups[k], fam)
	}
	sort.Slice(order, func(i, j int) bool {
		if order[i].pkg != order[j].pkg {
			return order[i].pkg < order[j].pkg
		}
		return order[i].bench < order[j].bench
	})
	for _, k := range order {
		group := groups[k]
		if len(group) < 2 {
			continue
		}
		ids := make([]string, 0, len(group))
		for _, fam := range group {
			ids = append(ids, fam.id)
		}
		for i := range group {
			fam := group[i]
			for _, other := range group[i+1:] {
				if fam.kindDim == other.kindDim && strings.Join(fam.extraArgs, " ") == strings.Join(other.extraArgs, " ") {
					problems = append(problems, fmt.Sprintf(
						"%s and %s declare the same PKG, BENCH, KIND_DIM and EXTRA_ARGS (%s, %s), so the campaign would measure and decide one family twice under two names",
						fam.id, other.id, fam.pkg, fam.benchRoot,
					))
				}
				if fam.matrix != other.matrix {
					problems = append(problems, fmt.Sprintf(
						"%s and %s share the benchmark root %s but declare MATRIX=%s and MATRIX=%s; their scorecards are joined on the benchmark name and two grids do not produce the same names",
						fam.id, other.id, fam.benchRoot, fam.declaredMatrix(), other.declaredMatrix(),
					))
				}
			}
			missing := map[string]bool{}
			for _, id := range ids {
				if id != fam.id {
					missing[id] = true
				}
			}
			for _, id := range fam.sharesRoot {
				delete(missing, id)
			}
			if len(missing) > 0 {
				problems = append(problems, fmt.Sprintf(
					"%s: SHARES_ROOT does not name %s, which %s measured from the same %s in %s; a campaign that does not know that measures those rows once per family",
					fam.id, strings.Join(sortedSet(missing), " "), plural(len(missing), "is", "are"),
					fam.benchRoot, fam.pkg,
				))
			}
		}
	}
	return problems
}

// sortedKeys returns a map's keys in order, so a problem list is stable.
func sortedKeys(m map[string]string) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// sortedSet returns a set's members in order.
func sortedSet(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// plural picks the verb form for a count.
func plural(n int, one, many string) string {
	if n == 1 {
		return one
	}
	return many
}

// declaredMatrix renders the family's MATRIX for a message.
func (f bakeoffFamily) declaredMatrix() string {
	if f.matrix == "" {
		return "<undeclared>"
	}
	return f.matrix
}

// criterionOrUnknown renders the family's CRITERION for a message.
func (f bakeoffFamily) criterionOrUnknown() string {
	if f.criterion == "" {
		return "undeclared: see the benchmark configuration"
	}
	return f.criterion
}

// benchmarkRowsUnder lists the benchmarks the family's root filter selects,
// with the units each reports. The filter is character for character the one
// hack/bakeoff.sh measures with, at the family's own declared grid and with
// -benchmem, so a root that selects nothing here selects nothing there, a row
// missing here is missing from the campaign's results, and a metric absent
// here is absent from its scorecard.
//
// This is the check that used to live only in the campaign script. `go test
// -bench` splits its pattern on unbracketed slashes and matches each part
// against one element of the benchmark name, so a root that names no
// benchmark of this package selects nothing, exits 0 and prints PASS.
//
// Family A29 is the case that showed why the check has to run here. Its
// benchmark existed -- in the nested `benchmarks` module, and in §10.5's
// shape-first order -- but A29.env declared PKG=./internal/engine, where the
// family's kind list and model tests live, and a `go test` run in a
// root-module package cannot reach a nested module, so the family's root
// selected nothing. What kept that invisible was not the exit status: it was
// that `bakeoff lint` never read `hack/bakeoff/*.env` at all, so nothing in
// the repository ever related a family's BENCH root to its PKG until the
// campaign script's own probe did (adr/ADR-017-assumptions.md, assumption
// A24.7).
func benchmarkRowsUnder(root, pkg, benchRoot, matrix string, extra []string) ([]benchRow, error) {
	args := make([]string, 0, 11+len(extra))
	args = append(args,
		"test", "-run", "^$", "-bench", "^"+benchRoot+"(/|$)",
		"-benchtime", "1x", "-count=1", "-cpu=1", "-benchmem",
	)
	args = append(args, extra...)
	args = append(args, pkg)
	cmd := goCmd(root, args...)
	if matrix != "" {
		cmd.Env = append(cmd.Env, "IQMQ_BENCH_MATRIX="+matrix)
	}
	out, err := cmd.CombinedOutput()
	if err != nil {
		return nil, childFailure(pkg, fmt.Sprintf("-bench %q at IQMQ_BENCH_MATRIX=%s", benchRoot, matrix), out, err)
	}
	var rows []benchRow
	sc := bufio.NewScanner(bytes.NewReader(out))
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if !strings.HasPrefix(line, "Benchmark") {
			continue
		}
		rows = append(rows, parseBenchRow(line))
	}
	return rows, nil
}

// parseBenchRow reads one "<name> <iters> [<value> <unit>]..." line.
func parseBenchRow(line string) benchRow {
	fields := strings.Fields(line)
	row := benchRow{name: fields[0]}
	for i := 3; i < len(fields); i += 2 {
		row.units = append(row.units, fields[i])
	}
	return row
}

// rowNames projects a listing onto the benchmark names.
func rowNames(rows []benchRow) []string {
	out := make([]string, 0, len(rows))
	for _, row := range rows {
		out = append(out, row.name)
	}
	return out
}

// lintBakeoffFamilies holds every hack/bakeoff/*.env declaration to what the
// campaign needs of it: its BENCH root must select at least one benchmark in
// its PKG at the grid it declares, every kind its KINDS_TEST announces must
// have a benchmark the per-kind filter selects, the kinds must have rows in
// common to be compared on, those rows must carry the dimensions and the
// metrics the declaration says the decision is read at and on, and two
// families that share a benchmark root must say so.
//
// only, when non-nil, restricts the check to the families whose package is in
// it, so `make bench-lint PKG=./internal/engine` checks the four families that
// package hosts and no others.
func lintBakeoffFamilies(root string, only map[string]bool) (problems []string, checked int, err error) {
	removed := 0
	defer func() {
		if removed > 0 {
			fmt.Fprintf(os.Stderr, "bakeoff lint: %d removed §9.2 famil(y/ies) checked as declarations only (§9.4(5))\n", removed)
		}
	}()
	fams, err := readBakeoffEnvs(root)
	if err != nil {
		return nil, 0, err
	}
	// One module path per module a family lives in, not one for the
	// repository: a MODDIR family's package belongs to another module, so
	// `make bench-lint PKG=<a root package>` must not match it and
	// `PKG=<its own package>` must.
	modPaths := map[string]string{}
	if only != nil {
		for i := range fams {
			moddir := fams[i].moddir
			if _, ok := modPaths[moddir]; ok {
				continue
			}
			mp, merr := modulePath(fams[i].root(root))
			if merr != nil {
				return nil, 0, merr
			}
			modPaths[moddir] = mp
		}
	}
	// Families sharing a package, a benchmark root, build tags and a grid
	// (A2 and A7 do) are measured from one benchmark run, so they are listed
	// once here too.
	type rootKey struct{ moddir, pkg, bench, extra, matrix string }
	listed := map[rootKey][]benchRow{}
	type kindsKey struct{ moddir, pkg, test string }
	announced := map[kindsKey][]string{}
	var selected []bakeoffFamily
	for i := range fams {
		fam := fams[i]
		if fam.pkg == "" {
			problems = append(problems, fmt.Sprintf("%s: env file declares no PKG", fam.id))
			continue
		}
		if fam.benchRoot == "" {
			problems = append(problems, fmt.Sprintf("%s: env file declares no BENCH", fam.id))
			continue
		}
		// A removed family has no package to list benchmarks from, and the
		// checks below all end in `go test` in that package. What is checked
		// instead is that the removal is real and that the declaration is
		// consistent with it.
		if fam.removed != "" {
			if fam.class != "B" {
				problems = append(problems, fmt.Sprintf(
					"%s: declares REMOVED= and CLASS=%s; a §9.1 arm is runtime-selectable through its Kind and is retained as a profile, not removed (§9.4(5))",
					fam.id, fam.class,
				))
			}
			dir := filepath.Join(fam.root(root), filepath.FromSlash(strings.TrimPrefix(fam.pkg, "./")))
			if _, serr := os.Stat(dir); serr == nil {
				problems = append(problems, fmt.Sprintf(
					"%s: declares REMOVED=%s and %s is still in the tree; the removal has not happened",
					fam.id, short(fam.removed), fam.pkg,
				))
			}
			removed++
			continue
		}
		if only != nil && !only[importPath(modPaths[fam.moddir], fam.pkg)] {
			continue
		}
		checked++
		selected = append(selected, fam)
		if !matrixValues[fam.matrix] {
			problems = append(problems, fmt.Sprintf(
				"%s: MATRIX=%s is not a campaign grid (default|full); MATRIX is what hack/bakeoff.sh exports as IQMQ_BENCH_MATRIX for this family's correctness gate, kind list, filter probe and measurement",
				fam.id, fam.declaredMatrix(),
			))
			continue
		}
		nk := kindsKey{fam.moddir, fam.pkg, fam.kindsTest}
		slugs, seen := announced[nk]
		if !seen {
			f, ok, kerr := loadKinds(fam.root(root), fam.pkg, fam.kindsTest)
			if kerr != nil {
				return nil, 0, kerr
			}
			if ok {
				slugs = f.kinds
			}
			announced[nk] = slugs
		}
		if len(slugs) == 0 {
			problems = append(problems, fmt.Sprintf("%s: %s in %s announces no kinds",
				fam.id, fam.kindsTest, fam.pkg))
			continue
		}
		rk := rootKey{fam.moddir, fam.pkg, fam.benchRoot, strings.Join(fam.extraArgs, " "), fam.matrix}
		rows, seen := listed[rk]
		if !seen {
			if rows, err = benchmarkRowsUnder(fam.root(root), fam.pkg, fam.benchRoot, fam.matrix, fam.extraArgs); err != nil {
				return nil, 0, err
			}
			listed[rk] = rows
		}
		if len(rows) == 0 {
			problems = append(problems, fmt.Sprintf(
				"%s: -bench '^%s(/|$)' selects no benchmark in %s; hack/bakeoff.sh %s cannot measure the family",
				fam.id, fam.benchRoot, fam.pkg, fam.id,
			))
			continue
		}
		absent := false
		for _, slug := range slugs {
			if selectsKind(rowNames(rows), fam.benchRoot, fam.kindDim, slug) > 0 {
				continue
			}
			absent = true
			filter := fam.benchRoot + "/" + slug
			if fam.kindDim != "" {
				filter = fam.benchRoot + " with " + fam.kindDim + "=" + slug
			}
			problems = append(problems, fmt.Sprintf(
				"%s: kind %q has no benchmark under %s; hack/bakeoff.sh %s would measure %d kind(s) of %d",
				fam.id, slug, filter, fam.id, len(slugs)-1, len(slugs),
			))
		}
		if absent {
			// Every check below reads the per-kind row sets, and a family
			// with a kind missing entirely would report all of them too.
			continue
		}
		switch skip, problem := olderThanRequired(fam); {
		case problem != "":
			problems = append(problems, problem)
			continue
		case skip:
			// Printed rather than silent: a family checked as a declaration
			// only is a family whose comparison this run did NOT hold, and
			// the reader of a green lint must be able to see which.
			fmt.Fprintf(os.Stderr, "bakeoff lint: %s checked as a declaration only on %s: its kinds need REQUIRES_GO=%s, so %d of them exist on this toolchain\n",
				fam.id, runtime.Version(), fam.requiresGo, len(slugs))
			continue
		}
		byKind, _ := rowsByKind(rows, fam.benchRoot, fam.kindDim)
		common, cmpProblems := checkComparability(fam, byKind, slugs)
		problems = append(problems, cmpProblems...)
		if len(common) == 0 {
			continue
		}
		problems = append(problems, checkDecidedAt(fam, common)...)
		problems = append(problems, checkDecidedOn(fam, byKind, spanningKinds(fam, slugs), common)...)
	}
	problems = append(problems, checkSharedRoots(selected)...)
	return problems, checked, nil
}

// modulePath reports the root module's import path.
func modulePath(root string) (string, error) {
	out, err := goCmd(root, "list", "-m").Output()
	if err != nil {
		return "", fmt.Errorf("go list -m: %w", err)
	}
	return strings.TrimSpace(string(out)), nil
}

// importPath maps an env file's PKG (a directory relative to the root module)
// to the import path `go list` reports for it.
func importPath(modPath, pkg string) string {
	rel := strings.TrimPrefix(strings.TrimPrefix(pkg, "."), "/")
	if rel == "" {
		return modPath
	}
	return modPath + "/" + rel
}

// olderThanRequired reports whether the running toolchain is older than the
// family's REQUIRES_GO, and a problem when the declaration itself is not a
// Go version -- an unparsable REQUIRES_GO would otherwise skip the family's
// checks on every toolchain, which is a check that cannot fail (assumption
// A20b.1).
func olderThanRequired(fam bakeoffFamily) (bool, string) {
	if fam.requiresGo == "" {
		return false, ""
	}
	want := "go" + strings.TrimPrefix(fam.requiresGo, "go")
	if !version.IsValid(want) {
		return false, fmt.Sprintf("%s: REQUIRES_GO=%q is not a Go version (write it as 1.27)", fam.id, fam.requiresGo)
	}
	have := runtime.Version()
	if !version.IsValid(have) {
		// A development toolchain names no release; it is treated as new
		// enough, which is what building from tip means.
		return false, ""
	}
	return version.Compare(have, want) < 0, ""
}
