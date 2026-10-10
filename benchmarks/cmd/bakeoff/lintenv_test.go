package main

import (
	"reflect"
	"strings"
	"testing"
)

// TestParseEnvAssignments pins the reader of hack/bakeoff/*.env against the
// forms those files actually use: quoted and unquoted values, comment lines,
// and a comment that contains an assignment of its own (A1.env documents its
// EXTRA_ARGS that way, and reading it as a setting would lint the family with
// build tags the campaign never passes).
func TestParseEnvAssignments(t *testing.T) {
	got := parseEnvAssignments([]byte(`
# Family A29: payload copy modes.
#   EXTRA_ARGS="-tags iqmq_unsafe" hack/bakeoff.sh A1
PKG=./internal/engine
BENCH=BenchmarkMemory/PubSubAck
KIND_DIM=""
KINDS_TEST=TestCopyModeKindsList
TITLE="Payload ownership (A29)"
AUTO_FILE=-
not an assignment
`))
	want := map[string]string{
		"PKG":        "./internal/engine",
		"BENCH":      "BenchmarkMemory/PubSubAck",
		"KIND_DIM":   "",
		"KINDS_TEST": "TestCopyModeKindsList",
		"TITLE":      "Payload ownership (A29)",
		"AUTO_FILE":  "-",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("parseEnvAssignments = %#v, want %#v", got, want)
	}
}

// TestSelectsKind holds the lint's per-kind check to the two shapes a family
// carries its kind in, and — the point of the check — to failing when the
// kind is absent. A check that counted any benchmark under the root would
// pass a family whose kinds had no benchmarks at all.
func TestSelectsKind(t *testing.T) {
	segment := []string{
		"BenchmarkMemory/PubSubAck/safe/1P1C/bytes=0-1",
		"BenchmarkMemory/PubSubAck/safe/1P1C/bytes=64-1",
		"BenchmarkMemory/PubSubAck/owned/1P1C/bytes=0-1",
		"BenchmarkMemory/FanOut/4/arch=logcursor-1",
	}
	dimensioned := []string{
		"BenchmarkMemory/FanOut/4/arch=logcursor/policy=atomic-8",
		"BenchmarkMemory/FanOut/4/arch=fullcopy/policy=atomic-8",
	}
	for _, tc := range []struct {
		name  string
		names []string
		root  string
		dim   string
		slug  string
		want  int
	}{
		{"segment/present", segment, "BenchmarkMemory/PubSubAck", "", "safe", 2},
		{"segment/one", segment, "BenchmarkMemory/PubSubAck", "", "owned", 1},
		{"segment/absent", segment, "BenchmarkMemory/PubSubAck", "", "zerocopy", 0},
		{"segment/other root", segment, "BenchmarkMemory/FanOut", "", "safe", 0},
		{"dim/present", dimensioned, "BenchmarkMemory/FanOut", "arch", "logcursor", 1},
		{"dim/absent", dimensioned, "BenchmarkMemory/FanOut", "arch", "persubenvelopes", 0},
		{"dim/wrong dim", dimensioned, "BenchmarkMemory/FanOut", "queue", "logcursor", 0},
		{"root absent", nil, "BenchmarkMemory/PubSubAck", "", "safe", 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := selectsKind(tc.names, tc.root, tc.dim, tc.slug); got != tc.want {
				t.Fatalf("selectsKind(%q, %q, %q) = %d, want %d", tc.root, tc.dim, tc.slug, got, tc.want)
			}
		})
	}
}

func TestTrimProcsAndImportPath(t *testing.T) {
	for in, want := range map[string]string{
		"BenchmarkMemory/PubSubAck/safe/1P1C/bytes=64-1": "BenchmarkMemory/PubSubAck/safe/1P1C/bytes=64",
		"BenchmarkMemory/PubSubAck/safe-32":              "BenchmarkMemory/PubSubAck/safe",
		"BenchmarkQueue":                                 "BenchmarkQueue",
	} {
		if got := trimProcs(in); got != want {
			t.Fatalf("trimProcs(%q) = %q, want %q", in, got, want)
		}
	}
	for in, want := range map[string]string{
		"./internal/engine": "m/internal/engine",
		"internal/engine":   "m/internal/engine",
		"./x/apibench/b1":   "m/x/apibench/b1",
		".":                 "m",
	} {
		if got := importPath("m", in); got != want {
			t.Fatalf("importPath(%q) = %q, want %q", in, got, want)
		}
	}
}

// TestReadBakeoffEnvsCoversEveryFamily reads the repository's own
// declarations: every one must name a package, a benchmark root and a kind
// list, because a declaration missing any of them is a family the campaign
// silently skips.
func TestReadBakeoffEnvsCoversEveryFamily(t *testing.T) {
	fams, err := readBakeoffEnvs("../../..")
	if err != nil {
		t.Fatalf("readBakeoffEnvs: %v", err)
	}
	if len(fams) < 30 {
		t.Fatalf("read %d families from hack/bakeoff, want the whole campaign", len(fams))
	}
	seen := map[string]bool{}
	for _, fam := range fams {
		if fam.pkg == "" || fam.benchRoot == "" || fam.kindsTest == "" {
			t.Errorf("%s: pkg=%q bench=%q kindsTest=%q", fam.id, fam.pkg, fam.benchRoot, fam.kindsTest)
		}
		if seen[fam.id] {
			t.Errorf("%s declared twice", fam.id)
		}
		seen[fam.id] = true
	}
	if !seen["A29"] {
		t.Error("hack/bakeoff declares no A29; the copy-mode family is the one this lint was extended for")
	}
}

// TestModDirIsReadAndResolved pins the one declaration that moves a family out
// of the root module.
//
// Family A12's arms are two client LIBRARIES, so the package that constructs
// both imports both, and no root-module package can: the root module is
// standard-library-only. MODDIR names the module PKG is relative to, and
// every listing in this file has to run THERE — a check that silently ran in
// the repository root would report the family as selecting no benchmark,
// which is the answer the whole file exists to distinguish from a real one.
func TestModDirIsReadAndResolved(t *testing.T) {
	fams, err := readBakeoffEnvs("../../..")
	if err != nil {
		t.Fatalf("readBakeoffEnvs: %v", err)
	}
	byID := map[string]bakeoffFamily{}
	for _, fam := range fams {
		byID[fam.id] = fam
	}
	a12, ok := byID["A12"]
	if !ok {
		t.Fatal("hack/bakeoff declares no A12; the Redis client-library family is the one MODDIR was added for")
	}
	if a12.moddir == "" {
		t.Error("A12 declares no MODDIR, so its package would be looked for in the root module, which cannot import a client library")
	}
	if got, want := a12.root("/repo"), "/repo/"+a12.moddir; got != want {
		t.Errorf("A12.root = %q, want %q", got, want)
	}
	// And a family that declares none is unaffected: its listings run in the
	// repository root exactly as before.
	a16, ok := byID["A16"]
	if !ok {
		t.Fatal("hack/bakeoff declares no A16")
	}
	if a16.moddir != "" {
		t.Errorf("A16 declares MODDIR=%q; the Redis nack family is a root-module family", a16.moddir)
	}
	if got := a16.root("/repo"); got != "/repo" {
		t.Errorf("A16.root = %q, want the repository root unchanged", got)
	}
	// "." and "./" are the root module spelled out, and must not become a
	// directory named ".".
	for _, spelling := range []string{".", "./"} {
		fam := bakeoffFamily{moddir: strings.TrimSuffix(strings.TrimPrefix(spelling, "./"), "/")}
		if fam.moddir == "." {
			fam.moddir = ""
		}
		if got := fam.root("/repo"); got != "/repo" {
			t.Errorf("MODDIR=%q resolves to %q, want the repository root", spelling, got)
		}
	}
}

// TestParseEnvList pins the reader of the two prose-valued declarations,
// PARTIAL_KINDS and CRITERION_GAP. Their entries are separated by ";" because
// the reasons contain spaces, and an entry with no reason has to survive the
// parse so that the check can report it rather than accepting it.
func TestParseEnvList(t *testing.T) {
	got := parseEnvList("spsc=single producer, single consumer (Kind.Modes); mpsc=single consumer ; bare")
	want := map[string]string{
		"spsc": "single producer, single consumer (Kind.Modes)",
		"mpsc": "single consumer",
		"bare": "",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("parseEnvList = %#v, want %#v", got, want)
	}
	if got := parseEnvList("  "); got != nil {
		t.Fatalf("parseEnvList(blank) = %#v, want nil", got)
	}
}

// TestParseBenchRow reads the units off a `go test -bench` line, which is
// where the DECIDED_ON check learns what a row reports.
func TestParseBenchRow(t *testing.T) {
	row := parseBenchRow("BenchmarkMemory/Queue/seqmpmc/MPMC/P=32/batch=1/occupancy=0/latency-32 " +
		"2000000\t420.0 ns/op\t1940 p99-ns\t0 B/op\t0 allocs/op")
	if row.name != "BenchmarkMemory/Queue/seqmpmc/MPMC/P=32/batch=1/occupancy=0/latency-32" {
		t.Fatalf("name = %q", row.name)
	}
	if want := []string{"ns/op", "p99-ns", "B/op", "allocs/op"}; !reflect.DeepEqual(row.units, want) {
		t.Fatalf("units = %v, want %v", row.units, want)
	}
}

// queueRows is family A1's shape in miniature: seven kinds that run every
// mode, one that cannot consume from many goroutines and one that can neither
// produce nor consume from many. It is the case that made this check
// necessary — the intersection across all nine kinds is the SPSC rows, and
// the family's criterion is read under MPMC contention.
func queueRows() map[string]map[string][]string {
	full := map[string][]string{
		"SPSC/P=1/occupancy=0":      {"ns/op", "allocs/op"},
		"MPSC/P=32/occupancy=0":     {"ns/op", "allocs/op"},
		"SPMC/P=32/occupancy=0":     {"ns/op", "allocs/op"},
		"MPMC/P=32/occupancy=0":     {"ns/op", "allocs/op"},
		"MPMC/P=32/occupancy=0/lat": {"ns/op", "allocs/op", "p99-ns"},
	}
	byKind := map[string]map[string][]string{}
	for _, k := range []string{"mutexring", "chan", "seqmpmc", "disruptor", "sharded", "appendlog", "chunked"} {
		byKind[k] = full
	}
	byKind["mpsc"] = map[string][]string{
		"SPSC/P=1/occupancy=0":  {"ns/op", "allocs/op"},
		"MPSC/P=32/occupancy=0": {"ns/op", "allocs/op"},
	}
	byKind["spsc"] = map[string][]string{"SPSC/P=1/occupancy=0": {"ns/op", "allocs/op"}}
	return byKind
}

func queueSlugs() []string {
	return []string{"mutexring", "chan", "seqmpmc", "disruptor", "sharded", "appendlog", "chunked", "mpsc", "spsc"}
}

// TestComparableRowsAndUnion holds the two counts the comparability check is
// stated in: what every kind has in common, and what the campaign pays for.
func TestComparableRowsAndUnion(t *testing.T) {
	byKind := queueRows()
	if got := comparableRows(byKind, queueSlugs()); !reflect.DeepEqual(got, []string{"SPSC/P=1/occupancy=0"}) {
		t.Fatalf("comparableRows(all nine kinds) = %v, want the one SPSC row", got)
	}
	spanning := []string{"mutexring", "chan", "seqmpmc", "disruptor", "sharded", "appendlog", "chunked"}
	if got := comparableRows(byKind, spanning); len(got) != 5 {
		t.Fatalf("comparableRows(spanning) = %v, want all five rows", got)
	}
	if got := unionRows(byKind, queueSlugs()); got != 5 {
		t.Fatalf("unionRows = %d, want 5", got)
	}
	if got := comparableRows(byKind, nil); got != nil {
		t.Fatalf("comparableRows(no kinds) = %v, want nil", got)
	}
}

// TestCheckComparability is the check that would have caught family A1's
// declaration: undeclared, the family is decided on one row of five, all of
// them single-producer; declared, it is decided on all five.
func TestCheckComparability(t *testing.T) {
	byKind, slugs := queueRows(), queueSlugs()
	undeclared := bakeoffFamily{id: "A1", kindsTest: "TestKindsList"}
	common, problems := checkComparability(undeclared, byKind, slugs)
	if len(common) != 1 || len(problems) != 1 {
		t.Fatalf("undeclared: common=%v problems=%v, want one row and one problem", common, problems)
	}
	if !strings.Contains(problems[0], "corner of its own workload") ||
		!strings.Contains(problems[0], "spsc has 1 rows against 5") {
		t.Fatalf("undeclared problem does not name the cause: %q", problems[0])
	}
	declared := bakeoffFamily{id: "A1", partial: map[string]string{
		"spsc": "single producer and single consumer",
		"mpsc": "single consumer",
	}}
	common, problems = checkComparability(declared, byKind, slugs)
	if len(problems) != 0 {
		t.Fatalf("declared: problems=%v, want none", problems)
	}
	if len(common) != 5 {
		t.Fatalf("declared: common=%v, want all five rows", common)
	}
	// A stale entry — a kind that does span the workload — must fail too, or
	// a declaration could quietly drop a kind out of its own bake-off.
	stale := bakeoffFamily{id: "A1", partial: map[string]string{
		"spsc":      "single producer and single consumer",
		"mpsc":      "single consumer",
		"mutexring": "not in fact narrow",
	}}
	_, problems = checkComparability(stale, byKind, slugs)
	if len(problems) != 1 || !strings.Contains(problems[0], `kind "mutexring" is declared PARTIAL_KINDS but has every one of the 5 comparable rows`) {
		t.Fatalf("stale entry: problems=%v", problems)
	}
	// An entry without a reason, and one naming a kind that does not exist.
	bad := bakeoffFamily{id: "A1", kindsTest: "TestKindsList", partial: map[string]string{
		"spsc": "single producer and single consumer", "mpsc": "", "ringbuffer": "no such kind",
	}}
	_, problems = checkComparability(bad, byKind, slugs)
	if len(problems) != 2 {
		t.Fatalf("bad entries: problems=%v, want two", problems)
	}
	// Every kind declared partial leaves nothing to compare.
	all := bakeoffFamily{id: "A1", partial: map[string]string{}}
	for _, s := range slugs {
		all.partial[s] = "declared away"
	}
	if _, problems = checkComparability(all, byKind, slugs); len(problems) != 1 ||
		!strings.Contains(problems[0], "leaving nothing to compare") {
		t.Fatalf("all partial: problems=%v", problems)
	}
}

// TestCheckComparabilityEmptyCommon is family A22's shape: two kinds whose
// delivery position is pinned and two adapters that carry it as a segment, so
// that no name is shared by all four.
func TestCheckComparabilityEmptyCommon(t *testing.T) {
	byKind := map[string]map[string][]string{
		"inline":   {"inline/inflight=1": {"ns/op"}},
		"deferred": {"deferred/inflight=1": {"ns/op"}},
		"future":   {"inline/inflight=1": {"ns/op"}, "deferred/inflight=1": {"ns/op"}},
		"channel":  {"inline/inflight=1": {"ns/op"}, "deferred/inflight=1": {"ns/op"}},
	}
	slugs := []string{"inline", "deferred", "future", "channel"}
	common, problems := checkComparability(bakeoffFamily{id: "A22"}, byKind, slugs)
	if common != nil {
		t.Fatalf("common = %v, want none", common)
	}
	if len(problems) != 1 || !strings.Contains(problems[0], "no benchmark is present for every kind") {
		t.Fatalf("problems = %v", problems)
	}
}

// TestCheckDecidedAt is the grid check: a term the declared grid does not
// produce fails, and a term that is only a substring of a segment does not
// pass for it.
func TestCheckDecidedAt(t *testing.T) {
	common := []string{"MPMC/P=32/occupancy=0", "SPSC/P=1/occupancy=95"}
	ok := bakeoffFamily{id: "A1", matrix: "full", decidedAt: []string{"MPMC", "P=32", "occupancy=95"}}
	if problems := checkDecidedAt(ok, common); len(problems) != 0 {
		t.Fatalf("problems = %v, want none", problems)
	}
	missing := bakeoffFamily{id: "A1", matrix: "default", decidedAt: []string{"P=32", "occupancy=50"}}
	problems := checkDecidedAt(missing, []string{"MPMC/P=4/occupancy=50"})
	if len(problems) != 1 || !strings.Contains(problems[0], `DECIDED_AT names "P=32" and MATRIX=default`) {
		t.Fatalf("problems = %v", problems)
	}
	substring := bakeoffFamily{id: "A1", matrix: "full", decidedAt: []string{"occupancy=9"}}
	if problems := checkDecidedAt(substring, common); len(problems) != 1 {
		t.Fatalf("occupancy=9 matched occupancy=95: %v", problems)
	}
	if problems := checkDecidedAt(bakeoffFamily{id: "A1"}, common); len(problems) != 1 ||
		!strings.Contains(problems[0], "declares no DECIDED_AT") {
		t.Fatalf("problems = %v", problems)
	}
}

// TestCheckDecidedOn covers both halves: a metric the decision claims to read
// must be reported by a comparable row of every kind, and a metric declared
// unmeasurable must really be unmeasured — a stale CRITERION_GAP would leave
// a family reporting a criterion it can in fact evaluate.
func TestCheckDecidedOn(t *testing.T) {
	byKind := map[string]map[string][]string{
		"heap":  {"insert/n=1000": {"ns/entry", "B/entry"}, "fire/n=1000": {"ns/entry"}},
		"wheel": {"insert/n=1000": {"ns/entry"}, "fire/n=1000": {"ns/entry"}},
	}
	spanning := []string{"heap", "wheel"}
	common := []string{"insert/n=1000", "fire/n=1000"}
	fam := bakeoffFamily{id: "A4", decidedOn: []string{"ns/entry", "B/entry"}}
	problems := checkDecidedOn(fam, byKind, spanning, common)
	if len(problems) != 1 || !strings.Contains(problems[0], `DECIDED_ON names "B/entry" and no comparable row of kind "wheel"`) {
		t.Fatalf("problems = %v", problems)
	}
	none := bakeoffFamily{id: "A4"}
	if problems := checkDecidedOn(none, byKind, spanning, common); len(problems) != 1 ||
		!strings.Contains(problems[0], "declares no DECIDED_ON") {
		t.Fatalf("problems = %v", problems)
	}
	gap := bakeoffFamily{
		id: "A21", decidedOn: []string{"ns/entry"},
		gap: map[string]string{"peak-concurrency": "benchmarks/sim/retrystorm", "ns/entry": "nowhere"},
	}
	problems = checkDecidedOn(gap, byKind, spanning, common)
	if len(problems) != 1 || !strings.Contains(problems[0], `CRITERION_GAP claims "ns/entry" is not measurable here`) {
		t.Fatalf("stale gap not reported: %v", problems)
	}
	noReason := bakeoffFamily{
		id: "A21", decidedOn: []string{"ns/entry"},
		gap: map[string]string{"peak-concurrency": ""},
	}
	if problems := checkDecidedOn(noReason, byKind, spanning, common); len(problems) != 1 ||
		!strings.Contains(problems[0], "says nothing about where the evidence for it lives") {
		t.Fatalf("problems = %v", problems)
	}
}

// TestCheckSharedRoots covers the three ways two declarations of one
// benchmark root go wrong: the same dimension (one family measured twice
// under two names), two grids (A1 and A10's scorecards would have no row name
// to join on) and silence (the campaign measures the rows once per family).
func TestCheckSharedRoots(t *testing.T) {
	a2 := bakeoffFamily{
		id: "A2", pkg: "./internal/engine", benchRoot: "BenchmarkMemory/FanOut",
		kindDim: "arch", matrix: "default", sharesRoot: []string{"A7"},
	}
	a7 := bakeoffFamily{
		id: "A7", pkg: "./internal/engine", benchRoot: "BenchmarkMemory/FanOut",
		kindDim: "policy", matrix: "default", sharesRoot: []string{"A2"},
	}
	if problems := checkSharedRoots([]bakeoffFamily{a2, a7}); len(problems) != 0 {
		t.Fatalf("declared pair: %v", problems)
	}
	same := a7
	same.kindDim = "arch"
	problems := checkSharedRoots([]bakeoffFamily{a2, same})
	if len(problems) != 1 || !strings.Contains(problems[0], "would measure and decide one family twice under two names") {
		t.Fatalf("same dimension: %v", problems)
	}
	silent := a7
	silent.sharesRoot = nil
	problems = checkSharedRoots([]bakeoffFamily{a2, silent})
	if len(problems) != 1 || !strings.Contains(problems[0], "A7: SHARES_ROOT does not name A2") {
		t.Fatalf("silent: %v", problems)
	}
	a1 := bakeoffFamily{
		id: "A1", pkg: "./internal/queue", benchRoot: "BenchmarkMemory/Queue",
		matrix: "full", sharesRoot: []string{"A10"},
	}
	a10 := bakeoffFamily{
		id: "A10", pkg: "./internal/queue", benchRoot: "BenchmarkMemory/Queue",
		extraArgs: []string{"-tags", "iqmq_unsafe"}, matrix: "default", sharesRoot: []string{"A1"},
	}
	problems = checkSharedRoots([]bakeoffFamily{a1, a10})
	if len(problems) != 1 || !strings.Contains(problems[0], "declare MATRIX=full and MATRIX=default") {
		t.Fatalf("grid mismatch: %v", problems)
	}
	// One family per root is the normal case and reports nothing.
	if problems := checkSharedRoots([]bakeoffFamily{a2}); len(problems) != 0 {
		t.Fatalf("single family: %v", problems)
	}
}

// TestRowsByKindAndSegments pins the two name operations the checks stand on:
// splitting the kind out the way `bakeoff decide` does, and matching a
// DECIDED_AT term against whole segments.
func TestRowsByKindAndSegments(t *testing.T) {
	rows := []benchRow{
		{name: "BenchmarkMemory/Scheduler/heap/insert/n=1000-8", units: []string{"ns/entry"}},
		{name: "BenchmarkMemory/Scheduler/wheel/insert/n=1000-8", units: []string{"ns/entry"}},
		{name: "BenchmarkMemory/Scheduler-8", units: []string{"ns/op"}},
		{name: "BenchmarkOther/Scheduler/heap-8", units: []string{"ns/op"}},
	}
	byKind, unattributed := rowsByKind(rows, "BenchmarkMemory/Scheduler", "")
	if len(byKind) != 2 || byKind["heap"]["BenchmarkMemory/Scheduler/insert/n=1000"] == nil {
		t.Fatalf("byKind = %#v", byKind)
	}
	if !reflect.DeepEqual(unattributed, []string{"BenchmarkMemory/Scheduler"}) {
		t.Fatalf("unattributed = %v, want the root row", unattributed)
	}
	if !hasSegment([]string{"a/n=1000/b"}, "n=1000") {
		t.Fatal("hasSegment missed a whole segment")
	}
	if hasSegment([]string{"a/n=1000/b"}, "n=100") {
		t.Fatal("hasSegment matched a prefix of a segment")
	}
}

// TestEveryFamilyDeclaresItsDecision reads the repository's own declarations.
// The fields are what `make bench-lint` checks against a listing, and a
// family that declares none of them would pass every other check in this file
// while producing data its §9.1 decision cannot be taken from — which is what
// the stopped campaign did.
func TestEveryFamilyDeclaresItsDecision(t *testing.T) {
	fams, err := readBakeoffEnvs("../../..")
	if err != nil {
		t.Fatalf("readBakeoffEnvs: %v", err)
	}
	for _, fam := range fams {
		if !matrixValues[fam.matrix] {
			t.Errorf("%s: MATRIX=%q is not a campaign grid", fam.id, fam.matrix)
		}
		if fam.criterion == "" {
			t.Errorf("%s: no CRITERION (the benchmark configuration's decision column)", fam.id)
		}
		if len(fam.decidedAt) == 0 {
			t.Errorf("%s: no DECIDED_AT", fam.id)
		}
		if len(fam.decidedOn) == 0 {
			t.Errorf("%s: no DECIDED_ON", fam.id)
		}
	}
}
