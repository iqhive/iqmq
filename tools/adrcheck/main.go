// Command adrcheck validates the architecture decision records under adr/.
//
// Every ADR must carry a front-matter block with status, kind and date. In
// -strict mode every variant row of specification §9 must be owned by
// exactly one ADR, measured ADRs must carry raw data and dispositions, and
// every ADR's auto binding must be well formed.
//
// ADR-017's assumption ledger is checked further: every identifier is well
// formed, agrees with its row's increment and is unique; every Alternatives
// entry names a row that exists, at most once; and every citation in the
// repository names a row that exists, written as an identifier rather than
// as a position in a shared sequence.
//
// The bake-off's family -> ADR map (`ADR=` in hack/bakeoff/*.env) is checked
// against the ADRs themselves, because an ADR number is shared by many
// families and nothing used to notice when they disagreed: no two files may
// claim one ADR number, an `auto:` key must name a family that has an env
// file and belongs to that ADR, and a family whose ADR already has a file
// must be listed in its `variants`.
package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"

	"github.com/iqhive/iqmq/internal/yamlmini"
	"github.com/iqhive/iqmq/tools/internal/mdcheck"
)

var (
	validStatus = map[string]bool{"Proposed": true, "Accepted (provisional)": true, "Accepted": true, "Superseded": true, "Rejected": true}
	validKind   = map[string]bool{"structural": true, "policy": true, "measured": true}
	dateRE      = regexp.MustCompile(`^\d{4}-\d{2}-\d{2}$`)
	// driverRows are §9 rows that need a broker and are decided in M3–M5.
	driverRows = []string{"A11", "A12", "A13", "A15", "A16", "A17", "A32", "A33", "A34", "A35"}
)

// specRows lists every variant id in specification §9.
func specRows() []string {
	var rows []string
	for i := 1; i <= 37; i++ {
		rows = append(rows, fmt.Sprintf("A%d", i))
	}
	for i := 1; i <= 12; i++ {
		rows = append(rows, fmt.Sprintf("B%d", i))
	}
	for i := 1; i <= 15; i++ {
		rows = append(rows, fmt.Sprintf("C%d", i))
	}
	return rows
}

type adr struct {
	path   string
	num    int // the number in the file name, 0 when it has none
	front  *yamlmini.Map
	body   string
	status string
	kind   string
	rows   []string
	auto   []string // auto: keys, in the order the front matter lists them
}

// fileNumRE picks the ADR number out of a file name.
var fileNumRE = regexp.MustCompile(`^ADR-(\d+)-`)

// bakeoffFamily is one hack/bakeoff/<family>.env file, reduced to the fields
// that tie a variant family to an ADR.
type bakeoffFamily struct {
	family  string
	adr     int
	autoKey string
}

// loadFamilies reads the bake-off's family -> ADR map. A missing directory is
// not an error: the checker runs against an ADR directory alone in its own
// tests. The count is reported so that a silently skipped map is visible.
func loadFamilies(dir string) ([]bakeoffFamily, []string) {
	paths, err := filepath.Glob(filepath.Join(dir, "*.env"))
	if err != nil {
		return nil, []string{"bake-off families: " + err.Error()}
	}
	sort.Strings(paths)
	var fams []bakeoffFamily
	var problems []string
	for _, path := range paths {
		data, err := os.ReadFile(path)
		if err != nil {
			problems = append(problems, path+": "+err.Error())
			continue
		}
		f := bakeoffFamily{family: strings.TrimSuffix(filepath.Base(path), ".env")}
		for _, line := range strings.Split(string(data), "\n") {
			line = strings.TrimSpace(line)
			if line == "" || strings.HasPrefix(line, "#") {
				continue
			}
			key, val, ok := strings.Cut(line, "=")
			if !ok {
				continue
			}
			val = strings.Trim(strings.TrimSpace(val), `"'`)
			switch key {
			case "ADR":
				if val == "" {
					continue
				}
				n, err := strconv.Atoi(val)
				if err != nil {
					problems = append(problems, fmt.Sprintf("%s: ADR=%q is not an ADR number", path, val))
					continue
				}
				f.adr = n
			case "AUTO_KEY":
				f.autoKey = val
			}
		}
		fams = append(fams, f)
	}
	return fams, problems
}

// checkBakeoffMap holds the ADRs and the bake-off map to each other.
func checkBakeoffMap(adrs []*adr, fams []bakeoffFamily) []string {
	var problems []string
	// (1) One number, one file. ADR numbers are shared by many families and
	// a per-family render used to be able to add a second file for a number
	// that already had one, under a name the family's title slugified to.
	byNum := map[int][]*adr{}
	for _, a := range adrs {
		if a.num > 0 {
			byNum[a.num] = append(byNum[a.num], a)
		}
	}
	nums := make([]int, 0, len(byNum))
	for n := range byNum {
		nums = append(nums, n)
	}
	sort.Ints(nums)
	for _, n := range nums {
		if len(byNum[n]) < 2 {
			continue
		}
		var names []string
		for _, a := range byNum[n] {
			names = append(names, filepath.Base(a.path))
		}
		sort.Strings(names)
		problems = append(problems, fmt.Sprintf("ADR-%03d is claimed by %d files: %s; one number is one record, and the material of a second belongs inside the first", n, len(names), strings.Join(names, ", ")))
	}
	if len(fams) == 0 {
		return problems
	}
	// (2) An auto: key names a family that exists and belongs to this ADR.
	keyOwners := map[string][]bakeoffFamily{}
	for _, f := range fams {
		if f.autoKey != "" {
			keyOwners[f.autoKey] = append(keyOwners[f.autoKey], f)
		}
	}
	for _, a := range adrs {
		for _, key := range a.auto {
			owners, ok := keyOwners[key]
			if !ok {
				problems = append(problems, fmt.Sprintf("%s: auto.%s names no variant family: no hack/bakeoff/*.env declares AUTO_KEY=%s", a.path, key, key))
				continue
			}
			if a.num == 0 {
				continue
			}
			var owned bool
			var elsewhere []string
			for _, f := range owners {
				if f.adr == a.num {
					owned = true
					break
				}
				elsewhere = append(elsewhere, fmt.Sprintf("%s (ADR-%03d)", f.family, f.adr))
			}
			if !owned {
				problems = append(problems, fmt.Sprintf("%s: auto.%s binds a family this ADR does not collect: %s", a.path, key, strings.Join(elsewhere, ", ")))
			}
		}
	}
	// (3) A family whose ADR has a file is listed in that file's variants.
	// Scoped to numbers that already have a file: the ADRs of the families
	// still to be measured are rendered by `bakeoff adr` after the campaign.
	for _, f := range fams {
		if f.adr == 0 {
			problems = append(problems, fmt.Sprintf("hack/bakeoff/%s.env: ADR= is empty; every variant family belongs to an ADR (plan row A24)", f.family))
			continue
		}
		owners := byNum[f.adr]
		if len(owners) != 1 {
			continue
		}
		a := owners[0]
		if !slices.Contains(a.rows, f.family) {
			problems = append(problems, fmt.Sprintf("%s: hack/bakeoff/%s.env says ADR=%d but the ADR's variants are [%s]: add %s", a.path, f.family, f.adr, strings.Join(a.rows, ", "), f.family))
		}
	}
	return problems
}

func main() {
	dir := flag.String("dir", "adr", "directory holding ADRs")
	strict := flag.Bool("strict", false, "require every §9 row to be owned by exactly one ADR")
	skipDriver := flag.Bool("skip-driver", false, "in strict mode, ignore driver-bound rows")
	skip := flag.String("skip", "", "comma-separated rows to ignore in strict mode")
	printAuto := flag.Bool("print-auto", false, "print the merged auto bindings")
	envs := flag.String("envs", "hack/bakeoff", "directory of the bake-off's per-family .env files (the family -> ADR map)")
	repo := flag.String("repo", "", "repository root to scan for assumption citations (default: the parent of -dir)")
	idents := flag.Bool("identifiers", false, "resolve what each ledger row's evidence NAMES: Test/Fuzz names via `go test -list`, back-ticked Go identifiers against the package's symbols (slow: it builds every module's test binaries)")
	flag.Parse()
	if err := run(*dir, *strict, *skipDriver, *skip, *printAuto, *repo, *envs, *idents); err != nil {
		fmt.Fprintln(os.Stderr, "adrcheck:", err)
		os.Exit(1)
	}
}

func run(dir string, strict, skipDriver bool, skip string, printAuto bool, repo, envs string, idents bool) error {
	if repo == "" {
		repo = filepath.Dir(dir)
	}
	files, err := filepath.Glob(filepath.Join(dir, "*.md"))
	if err != nil {
		return err
	}
	var adrs []*adr
	var problems []string
	for _, f := range files {
		base := filepath.Base(f)
		if base == "README.md" || strings.HasPrefix(base, "0000-") || strings.HasPrefix(base, "TEMPLATE") {
			continue
		}
		a, errs := load(f)
		problems = append(problems, errs...)
		if a != nil {
			adrs = append(adrs, a)
		}
	}
	owners := map[string][]string{}
	auto := map[string]string{}
	for _, a := range adrs {
		for _, r := range a.rows {
			owners[r] = append(owners[r], filepath.Base(a.path))
		}
		if v, ok := a.front.Get("auto"); ok {
			m, ok := v.(*yamlmini.Map)
			if !ok {
				problems = append(problems, a.path+": auto must be a mapping of family: kind")
				continue
			}
			for _, k := range m.Keys {
				slug := yamlmini.String(m.Values[k])
				if slug == "" {
					problems = append(problems, fmt.Sprintf("%s: auto.%s is empty", a.path, k))
				}
				auto[k] = slug
			}
		}
	}
	if strict {
		skipped := map[string]bool{}
		if skipDriver {
			for _, r := range driverRows {
				skipped[r] = true
			}
		}
		for _, r := range strings.Split(skip, ",") {
			if r = strings.TrimSpace(r); r != "" {
				skipped[r] = true
			}
		}
		for _, r := range specRows() {
			if skipped[r] {
				continue
			}
			switch n := len(owners[r]); {
			case n == 0:
				problems = append(problems, fmt.Sprintf("row %s is not owned by any ADR", r))
			case n > 1:
				problems = append(problems, fmt.Sprintf("row %s is owned by %d ADRs: %s", r, n, strings.Join(owners[r], ", ")))
			}
		}
	}
	if printAuto {
		keys := make([]string, 0, len(auto))
		for k := range auto {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			fmt.Printf("%s=%s\n", k, auto[k])
		}
	}
	fams, famProblems := loadFamilies(envs)
	problems = append(problems, famProblems...)
	problems = append(problems, checkBakeoffMap(adrs, fams)...)
	rows, ledgerProblems := loadLedger(filepath.Join(dir, ledgerFile))
	problems = append(problems, ledgerProblems...)
	cites := 0
	if len(rows) > 0 {
		n, ps := scanRepo(repo, rows)
		cites = n
		problems = append(problems, ps...)
	}
	var idv identVerdict
	if idents && len(rows) > 0 {
		v, ps := checkIdentifiers(repo, filepath.Join(dir, ledgerFile), rows, repoModules(repo))
		idv = v
		problems = append(problems, ps...)
	}
	if len(problems) > 0 {
		sort.Strings(problems)
		for _, p := range problems {
			fmt.Fprintln(os.Stderr, " -", p)
		}
		if idents {
			reportIdentifiers(idv)
		}
		return fmt.Errorf("%d problem(s) in %d ADR(s)", len(problems), len(adrs))
	}
	fmt.Fprintf(os.Stderr, "adrcheck: %d ADR(s), %d assumption(s), %d citation(s), %d bake-off famil(y/ies) ok (strict=%v)\n", len(adrs), len(rows), cites, len(fams), strict)
	if idents {
		reportIdentifiers(idv)
	}
	return nil
}

// reportIdentifiers prints how every name was resolved. Each exemption is
// named and counted rather than folded into a pass: an instrument nobody has
// watched fail is assumed not to work, and "398 exact matches, 15 absent" is
// what made the backlog this mode exists for legible in the first place.
func reportIdentifiers(v identVerdict) {
	fmt.Fprintf(os.Stderr, "adrcheck -identifiers: tests exact=%d family-prefix=%d subtest=%d  identifiers=%d  declared-absent=%d  entries skipped for a path not in the tree=%d\n",
		v.exact, v.prefix, v.subtest, v.idents, v.absent, v.skipped)
}

func load(path string) (*adr, []string) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, []string{err.Error()}
	}
	text := strings.ReplaceAll(string(data), "\r\n", "\n")
	if !strings.HasPrefix(text, "---\n") {
		return nil, []string{path + ": missing front matter"}
	}
	end := strings.Index(text[4:], "\n---\n")
	if end < 0 {
		return nil, []string{path + ": unterminated front matter"}
	}
	frontText := text[4 : 4+end]
	body := text[4+end+5:]
	v, err := yamlmini.Parse([]byte(frontText))
	if err != nil {
		return nil, []string{path + ": " + err.Error()}
	}
	front, ok := v.(*yamlmini.Map)
	if !ok {
		return nil, []string{path + ": front matter is not a mapping"}
	}
	a := &adr{path: path, front: front, body: body}
	var problems []string
	if m := fileNumRE.FindStringSubmatch(filepath.Base(path)); m != nil {
		a.num, _ = strconv.Atoi(m[1])
	} else {
		problems = append(problems, path+": file name must be ADR-<number>-<slug>.md")
	}
	a.status = yamlmini.String(front.Values["status"])
	if !validStatus[a.status] {
		problems = append(problems, fmt.Sprintf("%s: invalid status %q", path, a.status))
	}
	a.kind = yamlmini.String(front.Values["kind"])
	if !validKind[a.kind] {
		problems = append(problems, fmt.Sprintf("%s: invalid kind %q", path, a.kind))
	}
	if d := yamlmini.String(front.Values["date"]); !dateRE.MatchString(d) {
		problems = append(problems, fmt.Sprintf("%s: invalid date %q", path, d))
	}
	if v, ok := front.Get("auto"); ok {
		if m, ok := v.(*yamlmini.Map); ok {
			a.auto = append(a.auto, m.Keys...)
		}
	}
	for _, r := range yamlmini.Strings(front.Values["variants"]) {
		for _, part := range strings.Split(r, ",") {
			if part = strings.TrimSpace(part); part != "" {
				a.rows = append(a.rows, part)
			}
		}
	}
	if !strings.HasPrefix(strings.TrimSpace(body), "# ") {
		problems = append(problems, path+": body must start with a title heading")
	}
	problems = append(problems, mdcheck.Check(path, strings.Count(text[:len(text)-len(body)], "\n"), body)...)
	// `authoritative` describes the record's measured CONTENT, not its
	// status, and those are two different facts about two different halves
	// of a record that can hold both. The key has three
	// meanings and each is checked against whether the record actually
	// contains a measurement: `yes` -- every measurement in it was taken on
	// a pinned host; `no` -- it contains or rests on one that was not; `n/a`
	// -- it contains none. A generated bake-off block is the machine-visible
	// form of "contains a measurement".
	hasBakeoff := strings.Contains(body, "<!-- BEGIN bakeoff-generated")
	authVal, hasAuth := front.Get("authoritative")
	// YAML 1.1 reads a plain `no` as the boolean false and a plain `yes` as
	// true, which is how the first version of C01.6's own comparison came to
	// be a check that could not fail. Both spellings of both are normalised
	// here, and the test table carries all four.
	authStr := yamlmini.String(authVal)
	switch authStr {
	case "false":
		authStr = "no"
	case "true":
		authStr = "yes"
	}
	switch {
	case hasBakeoff && !hasAuth:
		problems = append(problems, path+": carries a generated bake-off block and declares no authoritative: key; "+
			"a record that contains a measurement has to say whether the host was pinned")
	case hasBakeoff && authStr == "n/a":
		problems = append(problems, path+": declares authoritative: n/a and carries a generated bake-off block, "+
			"which is a measurement; n/a is for a record that contains none")
	case !hasBakeoff && authStr == "yes":
		problems = append(problems, path+": declares authoritative: yes and carries no measurement to qualify")
	}

	// The split C01 left open, resolved. A record whose
	// PRIMARY decision is structural or policy may be plainly `Accepted`
	// about that decision and still carry a provisional binding chosen on an
	// unpinned host -- ADR-003, ADR-007, ADR-009, ADR-011 and ADR-017 all
	// do. So a record can be Accepted about one thing and provisional about
	// another, and what was missing is that nothing said WHICH: the reader
	// had a status about one half and a front-matter key about the other with
	// no statement of where the line falls. `provisional_scope` is that
	// statement, it is required of exactly the records that need it, and it
	// is refused on a `kind: measured` record -- there the whole decision is
	// provisional and the status already says so, and a scope key would be a
	// second, narrower claim about the same thing.
	scope := strings.TrimSpace(yamlmini.String(front.Values["provisional_scope"]))
	switch {
	case hasBakeoff && a.kind != "measured" && scope == "":
		problems = append(problems, path+": is "+a.kind+" and carries a generated bake-off block, so part of it is "+
			"provisional and part of it is not; declare provisional_scope: naming which part (assumption C02.5)")
	case a.kind == "measured" && scope != "":
		problems = append(problems, path+": is measured and declares provisional_scope:; the whole record is "+
			"provisional and its status says so")
	case !hasBakeoff && scope != "":
		problems = append(problems, path+": declares provisional_scope: and carries no measurement to scope")
	}

	if a.kind == "measured" {
		if len(a.rows) == 0 {
			problems = append(problems, path+": measured ADR must list variants")
		}
		for _, section := range []string{"## Raw data", "## Alternatives", "## Decision"} {
			if !strings.Contains(body, "\n"+section) && !strings.HasPrefix(body, section) {
				problems = append(problems, fmt.Sprintf("%s: measured ADR lacks section %q", path, section))
			}
		}
		if !strings.Contains(body, "sysinfo:") {
			problems = append(problems, path+": measured ADR must reference its sysinfo file")
		}
		auth, ok := front.Get("authoritative")
		if !ok {
			problems = append(problems, path+": measured ADR must declare authoritative: yes|no")
		}
		// A measured decision taken on an unpinned host may not be recorded
		// as plainly Accepted: that is the plan's C01 rule -- "ADRs stay
		// 'Accepted (provisional, single host)' with the v1 caveat unless a
		// pinned host exists" -- and nothing enforced it.
		//
		// The status vocabulary is closed (`validStatus` above), so "single
		// host" is carried by `authoritative: no` rather than by a fourth
		// spelling of Accepted, and this is the check that makes the two
		// agree. `bakeoff adr` writes both keys itself, so the way in is a
		// hand edit to a generated record or a hand-written measured one --
		// which is exactly what a reader would then have no way to
		// distinguish from a ratified decision.
		//
		// `yamlmini.String(auth) == "no"` is what this was first written as
		// and it is a check that cannot fail: YAML 1.1 reads a plain `no` as
		// the BOOLEAN false, so `String` renders it "false" and the
		// comparison never matched any file in the repository. Both
		// spellings are accepted here, and
		// TestMeasuredADRMayNotBeAcceptedOnAnUnpinnedHost carries the `no`
		// form for exactly that reason.
		notAuthoritative := false
		switch yamlmini.String(auth) {
		case "no", "false":
			notAuthoritative = true
		}
		//
		// The rule is scoped to `kind: measured` deliberately, and four
		// records sit just outside it: ADR-003 and ADR-007 (structural) and
		// ADR-009 and ADR-011 (policy) each declare `authoritative: no`,
		// carry a generated bake-off block, say plainly `Accepted`, and
		// three of them bind a family (`b6: concrete`, `b7: array`,
		// `b10: prealloccodes`). Their PRIMARY decision is structural or
		// policy and does not depend on a host, so their status is about a
		// thing no measurement can unsettle; the measured half inside them
		// is disclosed by its own "Authoritative: no ... Re-ratify"
		// paragraph and by `AutoProvisional` in the generated binding.
		// Widening the rule to every record with a generated block would
		// re-open four decisions other increments took, which C01 is not the
		// row to do -- it is recorded as a finding instead.
		if notAuthoritative && a.status == "Accepted" {
			problems = append(problems, path+": measured ADR declares authoritative: no "+
				"and status: Accepted; a decision taken on an unpinned host is "+
				"\"Accepted (provisional)\" until it is re-ratified (the contribution policy, plan row C01)")
		}
		if strings.HasPrefix(a.status, "Accepted") && !strings.Contains(body, "## Disposition") {
			problems = append(problems, path+": accepted measured ADR must record dispositions")
		}
	}
	return a, problems
}

// --- ADR-017's assumption ledger -----------------------------------------
//
// The ledger identifies each row by the increment that took it plus an
// ordinal local to that increment (A12.1, A19.7, A27b.1 for a second branch
// off one plan row): two branches can never take the same identifier, no row
// is ever renumbered, and a citation stays valid for the life of the
// repository. Four things are checked, because
// adr-check was green through every failure the old shared sequence
// produced -- sixteen rows deleted by a merge, duplicate Alternatives
// entries, a non-ascending list and a stale cross-reference:
//
//  1. every identifier is well formed, agrees with its row's Increment
//     column, and is unique;
//  2. every Alternatives entry names a row that exists, at most once;
//  3. every citation anywhere in the repository names a row that exists;
//  4. the old bare-number form is rejected, so the sequence cannot come back.

// ledgerFile is the assumptions ledger inside the ADR directory, and
// tableHeader is the header row of its table.
const (
	ledgerFile  = "ADR-017-assumptions.md"
	tableHeader = "| ID | Assumption | Where | Increment |"
)

var (
	// An increment may carry one trailing lower-case letter (A27b), which
	// is what a second branch off one plan row takes: the row that landed
	// owns A27.1..A27.13 and the next plan row's number belongs to another
	// agent, so a suffixed increment is the only identifier a follow-on
	// branch can take without renumbering anyone.
	//
	// The leading letter is the plan PART, not a fixed "A". Parts A to D are
	// the plan's four, and the first increment of part B could not take a
	// row at all until this accepted one: every regexp here was written when
	// only part A existed, so `B01.1` was rejected as "not
	// <increment>.<ordinal>" and the first driver increment had nowhere to
	// record a decision.
	idRE  = regexp.MustCompile(`^([A-D])(\d+)([a-z]?)\.(\d+)$`)
	incRE = regexp.MustCompile(`^([A-D])(\d+)([a-z]?)$`)
	altRE = regexp.MustCompile(`^-\s+\*\*Assumption\s+([^*]+?)\s*\*\*`)
	// citeRE matches a citation and the identifiers it names, "X and Y"
	// and "X, Y" forms included; idTokenRE picks them out of the match.
	citeRE    = regexp.MustCompile(`(?i)\bassumptions?(?:(?:\s+|\s*,\s*|\s+and\s+)[A-D]\d+[a-z]?\.\d+)+`)
	idTokenRE = regexp.MustCompile(`[A-D]\d+[a-z]?\.\d+`)
	// bareRE matches the sequence form the ledger no longer uses.
	bareRE = regexp.MustCompile(`(?i)\bassumptions?\s+#?\d`)
)

// citeExt lists the file extensions a citation can be written in; citeName
// lists extension-free files that carry prose. Everything else (fuzz
// corpora, checksums, images) is skipped.
var (
	citeExt = map[string]bool{
		".go": true, ".md": true, ".env": true, ".yml": true, ".yaml": true,
		".sh": true, ".txt": true, ".json": true, ".tla": true, ".cfg": true,
		".mod": true, ".c": true,
	}
	citeName = map[string]bool{"Makefile": true}
	// citeSkip names documents that are not scanned for citations.
	citeSkip = map[string]bool{}
)

// assumption is one row of the ledger.
type assumption struct {
	id   string // as written, e.g. A15.16
	line int    // 1-based line in the ledger
	// claim and where are the row's Assumption and Where columns, kept for
	// the -identifiers mode: the Where column is the row's evidence, and a
	// Test name in the prose cites evidence exactly as much as one written
	// beside a file path.
	claim string
	where string
}

// prefixRegistryRE finds the registry's opening line, and registryLineRE one
// of its `<prefix>  <branch>  <subject>` lines.
var (
	prefixRegistryRE = regexp.MustCompile(`prefix registry:`)
	registryLineRE   = regexp.MustCompile(`^\s*([A-D]\d+[a-z]?)\s+(\S+)\s+(\S.*?)\s*$`)
	historicalRE     = regexp.MustCompile(`^\s*historical:\s*(.*?)\s*$`)
)

// normalisePrefix strips a zero-padded increment number, so that `B01` and
// `B1` are one prefix.
//
// They have to be: [normalise] already reads B01.4 and B1.4 as the same row —
// the ledger spells nine of its prefixes padded because that is how the
// increment's own commit spells them — so comparing the registry to the table
// by raw string would refuse a branch for writing `B1.5` under a `B01` entry,
// which is a rule about typography rather than about ownership.
func normalisePrefix(p string) string {
	m := incRE.FindStringSubmatch(p)
	if m == nil {
		return p
	}
	n, err := strconv.Atoi(m[2])
	if err != nil {
		return p
	}
	return m[1] + strconv.Itoa(n) + m[3]
}

// prefixRegistry is the ledger's own record of who owns each increment prefix.
type prefixRegistry struct {
	found      bool
	registered map[string]string // prefix -> branch
	historical map[string]bool
	line       int
}

// loadPrefixRegistry reads the HTML comment above the table.
//
// It is read from the ledger rather than from a file of its own because
// `adrcheck` already reads the ledger, a second source could disagree with it,
// and a branch taking a prefix has to touch this file anyway — so the
// registration lands in the same commit as the rows and cannot be forgotten
// separately.
func loadPrefixRegistry(lines []string) (prefixRegistry, []string) {
	reg := prefixRegistry{registered: map[string]string{}, historical: map[string]bool{}}
	var problems []string
	start := -1
	for i, l := range lines {
		if prefixRegistryRE.MatchString(l) && strings.Contains(l, "<!--") {
			start, reg.found, reg.line = i, true, i+1
			break
		}
	}
	if !reg.found {
		return reg, nil
	}
	for i := start; i < len(lines); i++ {
		if m := historicalRE.FindStringSubmatch(lines[i]); m != nil {
			for _, p := range strings.Fields(m[1]) {
				reg.historical[normalisePrefix(p)] = true
			}
		} else if m := registryLineRE.FindStringSubmatch(lines[i]); m != nil {
			// All three parts are required: a bare prefix is a claim with
			// nobody behind it, and registryLineRE not matching one is how
			// that is refused.
			key := normalisePrefix(m[1])
			if prev, ok := reg.registered[key]; ok {
				problems = append(problems, fmt.Sprintf("%d: the prefix registry lists %s twice (%s and %s); "+
					"a prefix has one owner", i+1, m[1], prev, m[2]))
				continue
			}
			reg.registered[key] = m[2]
		}
		if strings.Contains(lines[i], "-->") {
			break
		}
	}
	return reg, problems
}

// checkPrefixes refuses an increment prefix the registry does not account for.
//
// the contributor notes forbids taking an identifier's ORDINAL from the length of
// the table and was silent on taking its PREFIX from the highest one present.
// `main` topped out at C10, so C11 looked derived rather than guessed, and
// three branches each followed that reasonable-looking local rule to the same
// answer: seventeen rows about to collide into seven.
//
// The rule is "a prefix a BRANCH adds must be registered", not "every prefix
// must be documented". The 82 that predate the registry are grandfathered on
// its `historical:` line, because a rule that fails on arrival is a rule that
// gets deleted rather than obeyed — the same argument assumption C14.2 makes
// about the path check.
func checkPrefixes(path string, lines []string, rows map[string]assumption) []string {
	reg, problems := loadPrefixRegistry(lines)
	for i, p := range problems {
		problems[i] = path + ":" + p
	}
	if !reg.found {
		return append(problems, fmt.Sprintf("%s: no prefix registry above the table; "+
			"a comment opening with \"prefix registry:\" is what says who owns an increment prefix (assumption C14.4)", path))
	}
	// The first row of each prefix, so the message names a line a reader can
	// open rather than only the prefix.
	first := map[string]assumption{}
	for _, row := range rows {
		m := idRE.FindStringSubmatch(row.id)
		if m == nil {
			continue
		}
		p := normalisePrefix(m[1] + m[2] + m[3])
		if prev, ok := first[p]; !ok || row.line < prev.line {
			first[p] = row
		}
	}
	unregistered := make([]string, 0, len(first))
	for p := range first {
		if reg.registered[p] != "" || reg.historical[p] {
			continue
		}
		unregistered = append(unregistered, p)
	}
	sort.Strings(unregistered)
	for _, p := range unregistered {
		problems = append(problems, fmt.Sprintf("%s:%d: assumption %s uses the increment prefix %s, which the prefix "+
			"registry at line %d lists neither as owned nor as historical. Never take a prefix because it is the next "+
			"number after the highest one in the table -- that is how three branches took C11 at once. Ask the "+
			"integrator for a prefix and add it to the registry in this commit (assumption C14.4)",
			path, first[p].line, first[p].id, p, reg.line))
	}
	return problems
}

// normalise maps an identifier to a lookup key, so that a zero-padded
// increment (A09.1, which is how one row's own column and commit spell it)
// and its unpadded form name the same row. A branch suffix is part of the
// key: A27b.1 and A27.1 are two different rows, owned by two different
// branches of one plan row. The plan part is part of the key too: A1.1 and
// B1.1 are two rows owned by two different increments.
func normalise(id string) (string, bool) {
	m := idRE.FindStringSubmatch(id)
	if m == nil {
		return "", false
	}
	inc, err := strconv.Atoi(m[2])
	if err != nil {
		return "", false
	}
	ord, err := strconv.Atoi(m[4])
	if err != nil || ord < 1 {
		return "", false
	}
	return m[1] + strconv.Itoa(inc) + m[3] + "." + strconv.Itoa(ord), true
}

// loadLedger parses the assumption table and its Alternatives entries.
func loadLedger(path string) (map[string]assumption, []string) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, []string{path + ": " + err.Error()}
	}
	lines := strings.Split(strings.ReplaceAll(string(data), "\r\n", "\n"), "\n")
	head := -1
	for i, l := range lines {
		if strings.TrimSpace(l) == tableHeader {
			head = i
			break
		}
	}
	if head < 0 {
		return nil, []string{fmt.Sprintf("%s: no assumption table (want the header row %q)", path, tableHeader)}
	}
	var problems []string
	rows := map[string]assumption{}
	for i := head + 2; i < len(lines) && strings.HasPrefix(lines[i], "|"); i++ {
		cells := strings.Split(lines[i], "|")
		if len(cells) != 6 {
			// Name the ROW, not only its line. A row's cell may not hold a
			// literal pipe -- there is no escaping anywhere in this file --
			// so writing `success() or failure()` with the operator instead of
			// the word makes an eight-column row, which is skipped here and
			// therefore UNDEFINES the assumption. Everything that cites it
			// then fails, in every file, naming the citing files: the
			// diagnosis pointed everywhere except at the broken row. The
			// identifier of the row is what a reader needs first, and its
			// first cell still holds it whatever the split did to the rest.
			problems = append(problems, fmt.Sprintf("%s:%d: the row whose first cell is %q has %d columns, want ID | Assumption | Where | Increment; "+
				"a cell may not contain a literal pipe (write an operator in words) and this row defines no assumption, so every citation of it will be reported instead",
				path, i+1, strings.TrimSpace(cells[1]), len(cells)-2))
			continue
		}
		// A row ends AT its final pipe. Anything after it is text that a
		// merge glued onto the row, and every check here is blind to it: the
		// split still yields six cells, the fourth still reads the
		// increment, and the rendered table shows the text as a fifth column
		// rather than as whatever it was. `## Alternatives` spent weeks
		// living on the end of a row that way, so the file had no
		// Alternatives heading at all and nothing said so (assumption
		// C02b.6). Reported, not skipped: the row itself is still a row.
		if tail := strings.TrimSpace(cells[5]); tail != "" {
			problems = append(problems, fmt.Sprintf("%s:%d: %q follows the row's final |; a heading or a paragraph "+
				"glued onto a row is invisible to every check here and renders as a fifth column", path, i+1, tail))
		}
		id, inc := strings.TrimSpace(cells[1]), strings.TrimSpace(cells[4])
		key, ok := normalise(id)
		if !ok {
			problems = append(problems, fmt.Sprintf("%s:%d: identifier %q is not <increment>.<ordinal>: take <your increment>.<next ordinal you own>, never a number from a shared sequence", path, i+1, id))
			continue
		}
		mi := incRE.FindStringSubmatch(inc)
		if mi == nil {
			problems = append(problems, fmt.Sprintf("%s:%d: assumption %s records its increment as %q, which is not <part letter><number>", path, i+1, id, inc))
			continue
		}
		incN, err := strconv.Atoi(mi[2])
		if err != nil || !strings.HasPrefix(key, mi[1]+strconv.Itoa(incN)+mi[3]+".") {
			problems = append(problems, fmt.Sprintf("%s:%d: identifier %s does not match the row's increment %s", path, i+1, id, inc))
			continue
		}
		if prev, ok := rows[key]; ok {
			problems = append(problems, fmt.Sprintf("%s:%d: identifier %s is already taken by the row at line %d; an identifier is never reused", path, i+1, id, prev.line))
			continue
		}
		rows[key] = assumption{id: id, line: i + 1, claim: cells[2], where: cells[3]}
	}
	if len(rows) == 0 {
		problems = append(problems, path+": the assumption table has no rows")
	}
	named := map[string]int{}
	for i, l := range lines {
		m := altRE.FindStringSubmatch(l)
		if m == nil {
			continue
		}
		name := strings.TrimSpace(m[1])
		key, ok := normalise(name)
		switch {
		case !ok:
			problems = append(problems, fmt.Sprintf("%s:%d: Alternatives entry names %q, which is not an assumption identifier", path, i+1, name))
		case rows[key].id == "":
			problems = append(problems, fmt.Sprintf("%s:%d: Alternatives names assumption %s, which no row defines", path, i+1, name))
		case named[key] != 0:
			problems = append(problems, fmt.Sprintf("%s:%d: Alternatives names assumption %s twice (already at line %d)", path, i+1, name, named[key]))
		default:
			named[key] = i + 1
		}
	}
	problems = append(problems, checkPrefixes(path, lines, rows)...)
	return rows, problems
}

// unwrap strips a continuation line's indent and comment marker, so that a
// citation split across two lines is still read as one.
func unwrap(s string) string {
	s = strings.TrimLeft(s, " \t")
	for _, marker := range []string{"//", "#", "*"} {
		if rest, ok := strings.CutPrefix(s, marker); ok {
			return strings.TrimLeft(rest, " \t")
		}
	}
	return s
}

// scanCitations checks every citation in one file against the ledger.
func scanCitations(path, label string, rows map[string]assumption) (int, []string) {
	data, err := os.ReadFile(path)
	if err != nil {
		return 0, []string{path + ": " + err.Error()}
	}
	lines := strings.Split(strings.ReplaceAll(string(data), "\r\n", "\n"), "\n")
	found := 0
	var problems []string
	for i, line := range lines {
		window := line
		if i+1 < len(lines) {
			window = line + " " + unwrap(lines[i+1])
		}
		for _, at := range bareRE.FindAllStringIndex(window, -1) {
			if at[0] >= len(line) {
				continue // the citation starts on the next line; reported there
			}
			problems = append(problems, fmt.Sprintf("%s:%d: %q cites an assumption by number; cite the identifier the ledger's ID column gives (<increment>.<ordinal>)", label, i+1, strings.TrimSpace(window[at[0]:min(at[1]+3, len(window))])))
		}
		for _, at := range citeRE.FindAllStringIndex(window, -1) {
			if at[0] >= len(line) {
				continue
			}
			for _, id := range idTokenRE.FindAllString(window[at[0]:at[1]], -1) {
				found++
				key, ok := normalise(id)
				if !ok || rows[key].id == "" {
					problems = append(problems, fmt.Sprintf("%s:%d: cites assumption %s, which no row of %s defines", label, i+1, id, ledgerFile))
				}
			}
		}
	}
	return found, problems
}

// scanSkipDir names directories whose contents .gitignore covers and which
// hold generated prose that CITES assumptions, so that the citation count is
// comparable between two checkouts of the same commit.
//
// A `make gate` run writes benchmarks/reports/gate/<run>-verdict.md, and each
// verdict cites 36 assumptions. A worktree that has run the gate three times
// therefore reports 108 citations more than a fresh checkout of the same
// commit, and the instruction every increment is given -- "check the counts
// went UP after merging main" -- is unusable when the difference between two
// trees is which of them happened to run a benchmark. C04 measured it: 3435
// citations against another worktree's 3465 while ADDING 40 rows. Owner C07
// per the maintenance notes.
//
// It is a path list rather than a `git check-ignore` call on purpose: adrcheck
// runs in `make ci` and in a release workflow, and shelling out to git makes
// the result depend on a git binary and on whether the tree is a repository at
// all. These four directories are the ones .gitignore covers that contain
// citing prose; conformance/reports/out and dist hold JSON and binaries and
// cite nothing, but are listed so the set is "the ignored output trees" rather
// than "the ones that bit us".
var scanSkipDir = map[string]bool{
	"benchmarks/reports/gate":      true,
	"benchmarks/reports/quick":     true,
	"benchmarks/reports/native":    true,
	"benchmarks/reports/fuzz":      true,
	"conformance/reports/out":      true,
	"conformance/reports/incoming": true,
	"dist":                         true,
	"reports":                      true,
	"site":                         true,
}

// scanRepo checks the citations in every prose and source file under root.
func scanRepo(root string, rows map[string]assumption) (int, []string) {
	found := 0
	var problems []string
	err := filepath.WalkDir(root, func(p string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, relErr := filepath.Rel(root, p)
		if relErr != nil {
			rel = p
		}
		rel = filepath.ToSlash(rel)
		if d.IsDir() {
			switch d.Name() {
			case ".git", "node_modules", ".claude":
				return filepath.SkipDir
			}
			if scanSkipDir[rel] {
				return filepath.SkipDir
			}
			return nil
		}
		if citeSkip[rel] || (!citeExt[filepath.Ext(p)] && !citeName[d.Name()]) {
			return nil
		}
		n, ps := scanCitations(p, rel, rows)
		found += n
		problems = append(problems, ps...)
		return nil
	})
	if err != nil {
		problems = append(problems, "citation scan: "+err.Error())
	}
	return found, problems
}
