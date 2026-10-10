package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"text/template"
)

// `bakeoff adr` renders the bake-off's architecture decision records.
//
// An ADR number is shared by many variant families -- nine share ADR-014 and
// seven share ADR-017 -- so an ADR cannot be rendered by the command that
// decides one family: each family would overwrite the previous family's file
// and leave the last one's view of a decision that belongs to all of them.
// This command renders one file per ADR NUMBER instead, from every scorecard
// belonging to it:
//
//   - the family -> ADR map comes from `ADR=` in hack/bakeoff/<family>.env,
//     so `variants:` lists every family the ADR owns whether or not it has
//     been measured yet;
//   - the measured material comes from the stable per-family scorecards
//     `bakeoff decide` writes, so a render is repeatable without measuring;
//   - hack/bakeoff/adrs.tsv gives the ADR its number, file name and title,
//     which belong to the ADR rather than to any one of its families.
//
// When the number already has a file -- adr/ADR-009-... and ADR-017 are
// hand-written -- the material is written INTO that file, between markers, so
// that a re-render replaces only the generated part and never disturbs prose
// a human wrote, and no second file claims the same number.

// Markers delimiting the generated section. A re-render replaces exactly the
// text between them; everything outside is preserved byte for byte.
const (
	beginMarker = "<!-- BEGIN bakeoff-generated (bakeoff adr) -->"
	endMarker   = "<!-- END bakeoff-generated -->"
)

// generatedKey marks an ADR file this command created and therefore owns
// whole: its front matter and its body outside the markers are regenerated
// too. A file WITHOUT the key is a hand-written record and the material is
// attached to it, nested under one wrapper heading, with its status, kind,
// date and prose left exactly as the human wrote them. Delete the key from a
// generated file to take it over by hand; the next render will attach to it
// instead of rewriting it.
const (
	generatedKey   = "generated"
	generatedValue = "bakeoff adr"
)

// famEnv is one hack/bakeoff/<family>.env file.
type famEnv struct {
	Family   string
	Pkg      string
	Bench    string
	Ref      string
	AutoKey  string
	AutoFile string
	Title    string
	ADR      int
}

// Binds reports whether the family generates an Auto constant, and therefore
// whether it contributes an `auto:` key to its ADR. AUTO_FILE=- is a family
// whose kind is chosen per call or bound by a build tag (A26, A29): it has
// nothing to bind and claims no key.
func (e *famEnv) Binds() bool { return e.AutoKey != "" && e.AutoFile != "-" }

// adrEntry is one row of hack/bakeoff/adrs.tsv.
type adrEntry struct {
	Num   int
	Slug  string
	Title string
	Extra []string // §9 rows with no env file of their own
}

func adrCmd(args []string) error {
	fs := flag.NewFlagSet("adr", flag.ContinueOnError)
	envs := fs.String("envs", "../hack/bakeoff", "directory of per-family .env files")
	registry := fs.String("registry", "../hack/bakeoff/adrs.tsv", "ADR registry (number, slug, extra variants, title)")
	scorecards := fs.String("scorecards", "reports/scorecards", "directory of per-family scorecards")
	outADR := fs.String("out-adr", "../adr", "ADR directory")
	only := fs.Int("adr", 0, "render only this ADR number (0 = all)")
	root := fs.String("root", "..", "repository root, for the staleness cross-check")
	allowStale := fs.Bool("allow-stale", false, "downgrade the profile and staleness refusals to notes in the record")
	dryRun := fs.Bool("dry-run", false, "print what would be written and to where")
	if err := fs.Parse(args); err != nil {
		return err
	}
	fams, err := loadEnvs(*envs)
	if err != nil {
		return err
	}
	reg, err := loadRegistry(*registry)
	if err != nil {
		return err
	}
	docs, err := plan(fams, reg, *scorecards)
	if err != nil {
		return err
	}
	// A scorecard is a file on disk and says which run, which profile and
	// which commit produced it. Nothing used to read any of that, so a family
	// whose run failed mid-campaign contributed its PREVIOUS run's numbers to
	// the ADR without a word, and a `quick` scorecard -- three 200 ms samples
	// under a load ceiling of 64, which assumption A24.33 says decides
	// nothing -- rendered as a decision.
	if stale := checkProvenance(*root, docs, *only, *allowStale); len(stale) > 0 {
		return fmt.Errorf("%d scorecard(s) refused; nothing written:\n - %s\nRe-measure them, or pass -allow-stale to record the ADR with the staleness stated in it",
			len(stale), strings.Join(stale, "\n - "))
	}
	// Render everything first, then write. A file whose markers a human or a
	// merge has left half present is refused here, before any file is
	// touched, so a refusal costs the run its output rather than costing one
	// record its prose.
	var writes []*adrWrite
	var problems []string
	skipped := 0
	for _, d := range docs {
		if *only != 0 && d.Num != *only {
			continue
		}
		path, existed, err := adrPath(*outADR, d)
		if err != nil {
			problems = append(problems, err.Error())
			continue
		}
		if len(d.Measured) == 0 {
			// An ADR with nothing measured would be a file full of "not yet
			// measured", so it is not created; an existing file keeps its
			// prose and gains its data on the render after the campaign.
			fmt.Printf("bakeoff adr: ADR-%03d skipped: none of %s has a scorecard yet\n", d.Num, d.Rows)
			skipped++
			continue
		}
		w, err := prepareADR(path, existed, d)
		if err != nil {
			problems = append(problems, err.Error())
			continue
		}
		// A render refreshes a tool-owned record's title but cannot rename
		// its file: two files claiming one number is the thing adrPath and
		// `make adr-check` both refuse. So a slug corrected in the registry
		// is reported with the move that applies it, rather than silently
		// ignored the way the title used to be.
		if !d.Attach {
			if want := fmt.Sprintf("ADR-%03d-%s.md", d.Num, d.Slug); filepath.Base(path) != want {
				fmt.Printf("bakeoff adr: note: %s keeps an older file name; the registry now spells the slug %q. A render must not create a second file for one number, so apply it by hand: git mv %s %s\n",
					filepath.Base(path), d.Slug, path, filepath.Join(filepath.Dir(path), want))
			}
		}
		if *dryRun {
			verb := "write"
			switch {
			case d.Attach:
				verb = "attach to"
			case !existed:
				verb = "create"
			}
			fmt.Printf("bakeoff adr: would %s %s (variants %s; measured %s)\n", verb, path, d.Rows, d.measuredRows())
			continue
		}
		writes = append(writes, w)
	}
	if len(problems) > 0 {
		// The reasons travel in the error rather than being printed here, so
		// that one refusal produces one message and a caller that only reads
		// the error still learns which file and why.
		return fmt.Errorf("%d ADR(s) refused; nothing written:\n - %s", len(problems), strings.Join(problems, "\n - "))
	}
	for _, w := range writes {
		if err := w.commit(); err != nil {
			return err
		}
		fmt.Printf("bakeoff adr: %s %s (variants %s; measured %s)\n", w.verb, w.path, w.doc.Rows, w.doc.measuredRows())
	}
	fmt.Fprintf(os.Stderr, "bakeoff adr: %d ADR(s) rendered, %d without data\n", len(writes), skipped)
	return nil
}

// loadEnvs reads every hack/bakeoff/*.env file.
func loadEnvs(dir string) (map[string]*famEnv, error) {
	paths, err := filepath.Glob(filepath.Join(dir, "*.env"))
	if err != nil {
		return nil, err
	}
	if len(paths) == 0 {
		return nil, fmt.Errorf("no *.env files in %s", dir)
	}
	fams := map[string]*famEnv{}
	for _, p := range paths {
		e, err := loadEnv(p)
		if err != nil {
			return nil, err
		}
		fams[e.Family] = e
	}
	return fams, nil
}

// loadEnv parses one KEY=VALUE env file. Only the keys the ADR render needs
// are read; the file's comments and its measurement keys are ignored.
func loadEnv(path string) (*famEnv, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	e := &famEnv{Family: strings.TrimSuffix(filepath.Base(path), ".env")}
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
		case "PKG":
			e.Pkg = val
		case "BENCH":
			e.Bench = val
		case "REF":
			e.Ref = val
		case "AUTO_KEY":
			e.AutoKey = val
		case "AUTO_FILE":
			e.AutoFile = val
		case "TITLE":
			e.Title = val
		case "ADR":
			if val == "" {
				continue
			}
			n, err := strconv.Atoi(val)
			if err != nil {
				return nil, fmt.Errorf("%s: ADR=%q is not a number", path, val)
			}
			e.ADR = n
		}
	}
	return e, nil
}

// loadRegistry reads hack/bakeoff/adrs.tsv.
func loadRegistry(path string) (map[int]*adrEntry, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	reg := map[int]*adrEntry{}
	for i, line := range strings.Split(string(data), "\n") {
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		cells := strings.Split(line, "\t")
		if len(cells) != 4 {
			return nil, fmt.Errorf("%s:%d: %d column(s), want number, slug, extra-variants, title", path, i+1, len(cells))
		}
		num, err := strconv.Atoi(strings.TrimSpace(cells[0]))
		if err != nil {
			return nil, fmt.Errorf("%s:%d: %q is not an ADR number", path, i+1, cells[0])
		}
		if _, dup := reg[num]; dup {
			return nil, fmt.Errorf("%s:%d: ADR-%03d is listed twice", path, i+1, num)
		}
		e := &adrEntry{Num: num, Slug: strings.TrimSpace(cells[1]), Title: strings.TrimSpace(cells[3])}
		if extra := strings.TrimSpace(cells[2]); extra != "" && extra != "-" {
			for _, r := range strings.Split(extra, ",") {
				if r = strings.TrimSpace(r); r != "" {
					e.Extra = append(e.Extra, r)
				}
			}
		}
		if e.Slug == "" || e.Title == "" {
			return nil, fmt.Errorf("%s:%d: ADR-%03d needs a slug and a title", path, i+1, num)
		}
		reg[num] = e
	}
	if len(reg) == 0 {
		return nil, fmt.Errorf("%s: no ADR rows", path)
	}
	return reg, nil
}

// adrFamily is one family's contribution to an ADR.
type adrFamily struct {
	Row     string
	Title   string
	Pkg     string
	Bench   string
	Ref     string
	AutoKey string
	Binds   bool
	S       *scorecard // nil when the family has no scorecard yet
	// Provenance is what the staleness cross-check concluded about this
	// family's numbers, printed in the record so a reader is never left to
	// assume they describe the tree in front of them.
	Provenance string
}

// autoBinding is one `auto:` front-matter key.
type autoBinding struct {
	Key  string
	Slug string
	Row  string
	File string
}

// adrDoc is everything one ADR file is rendered from.
type adrDoc struct {
	Num      int
	Slug     string
	Title    string
	Rows     string
	Date     string
	Families []adrFamily
	Measured []adrFamily
	Pending  []adrFamily
	Auto     []autoBinding
	Attach   bool
	// CommitSpread is set when the families of one ADR were measured at
	// different commits, which is what a campaign interrupted and restarted
	// looks like from here.
	CommitSpread string
	// Arches are the architectures the scorecards were measured on. §9.4
	// step 2 asks for two (one x86-64, one arm64) and the Protocol coverage
	// section names what was actually used.
	Arches string
	H0     string // wrapper heading level (attach mode only)
	H1     string // the generated section headings
	H2     string // per-family headings
}

func (d *adrDoc) measuredRows() string {
	rows := make([]string, 0, len(d.Measured))
	for i := range d.Measured {
		rows = append(rows, d.Measured[i].Row)
	}
	if len(rows) == 0 {
		return "none"
	}
	return strings.Join(rows, ", ")
}

// variants is the front-matter row list, in §9 order: every family this ADR
// collects, plus every row the file already listed. ADR-011 owns the policy
// rows C5 and C6 and ADR-017 the assumptions ledger; no env file names those,
// and attaching a measured row must not take them away.
func (d *adrDoc) variants(front string) []string {
	seen := map[string]bool{}
	rows := make([]string, 0, len(d.Families))
	for i := range d.Families {
		if row := d.Families[i].Row; !seen[row] {
			seen[row] = true
			rows = append(rows, row)
		}
	}
	for _, r := range frontVariants(front) {
		if !seen[r] {
			seen[r] = true
			rows = append(rows, r)
		}
	}
	sort.Slice(rows, func(i, j int) bool { return rowLess(rows[i], rows[j]) })
	return rows
}

// plan groups the families by ADR number and loads their scorecards.
func plan(fams map[string]*famEnv, reg map[int]*adrEntry, scorecardDir string) ([]*adrDoc, error) {
	byNum := map[int][]*famEnv{}
	for _, e := range fams {
		if e.ADR == 0 {
			return nil, fmt.Errorf("family %s has no ADR number: set ADR= in hack/bakeoff/%s.env", e.Family, e.Family)
		}
		byNum[e.ADR] = append(byNum[e.ADR], e)
	}
	var nums []int
	for n := range byNum {
		if _, ok := reg[n]; !ok {
			return nil, fmt.Errorf("ADR-%03d collects %d famil(y/ies) but has no row in the ADR registry", n, len(byNum[n]))
		}
		nums = append(nums, n)
	}
	sort.Ints(nums)
	var docs []*adrDoc
	for _, n := range nums {
		entry := reg[n]
		d := &adrDoc{Num: n, Slug: entry.Slug, Title: entry.Title}
		members := byNum[n]
		sort.Slice(members, func(i, j int) bool { return rowLess(members[i].Family, members[j].Family) })
		for _, e := range members {
			f := adrFamily{
				Row: e.Family, Title: e.Title, Pkg: strings.TrimPrefix(e.Pkg, "./"), Bench: e.Bench,
				Ref: e.Ref, AutoKey: e.AutoKey, Binds: e.Binds(),
			}
			sc, measured, err := loadScorecard(filepath.Join(scorecardDir, e.Family+".json"))
			if err != nil {
				return nil, err
			}
			if f.S = sc; measured {
				// What was measured wins over what the env file says now.
				if sc.Package != "" {
					f.Pkg = sc.Package
				}
				if sc.BenchRoot != "" {
					f.Bench = sc.BenchRoot
				}
				if sc.Reference != "" {
					f.Ref = sc.Reference
				}
			}
			d.Families = append(d.Families, f)
		}
		for _, row := range entry.Extra {
			d.Families = append(d.Families, adrFamily{Row: row})
		}
		sort.SliceStable(d.Families, func(i, j int) bool { return rowLess(d.Families[i].Row, d.Families[j].Row) })
		for i := range d.Families {
			f := &d.Families[i]
			if f.S == nil {
				d.Pending = append(d.Pending, *f)
				continue
			}
			d.Measured = append(d.Measured, *f)
			if f.Binds && f.S.Winner != "" {
				d.Auto = append(d.Auto, autoBinding{Key: f.AutoKey, Slug: f.S.Winner, Row: f.Row, File: f.S.AutoFile})
			}
			if date, _, _ := strings.Cut(f.S.DecidedAt, "T"); date > d.Date {
				d.Date = date
			}
		}
		sort.Slice(d.Auto, func(i, j int) bool { return d.Auto[i].Key < d.Auto[j].Key })
		// One `auto:` key is one family. A8 and A26 both declare
		// AUTO_KEY=dispatch and only A8 binds anything; if both bound, the
		// block would carry the key twice and the parser would keep whichever
		// came last, binding one family's constant to the other's winner.
		for i := 1; i < len(d.Auto); i++ {
			if d.Auto[i].Key == d.Auto[i-1].Key {
				return nil, fmt.Errorf("ADR-%03d: families %s and %s both bind auto.%s; one key is one family",
					n, d.Auto[i-1].Row, d.Auto[i].Row, d.Auto[i].Key)
			}
		}
		d.Rows = strings.Join(d.variants(""), ", ")
		docs = append(docs, d)
	}
	return docs, nil
}

// checkProvenance holds every measured scorecard to what it says about
// itself before any ADR is rendered from it, and records the verdict on the
// family so that the ADR states it. Four things are checked:
//
//   - the profile: only `full` decides anything ;
//   - the tree: a scorecard measured on a dirty worktree, or with no commit
//     recorded at all, cannot be tied to any code;
//   - the history: a commit this repository does not have, or one that is not
//     an ancestor of HEAD, was measured on a tree that is not this one;
//   - the code: if the family's own package changed between the measured
//     commit and HEAD, the numbers describe code that is no longer there.
//
// Outside a git worktree the last two cannot be checked, and the verdict says
// so rather than passing silently. -allow-stale turns every refusal into a
// note in the record, which is what a deliberate re-render of old data needs.
func checkProvenance(root string, docs []*adrDoc, only int, allowStale bool) []string {
	var problems []string
	head, gitOK := "", false
	if out, err := gitAt(root, "rev-parse", "HEAD"); err == nil {
		head, gitOK = out, true
	}
	for _, d := range docs {
		if only != 0 && d.Num != only {
			continue
		}
		commits := map[string][]string{}
		for i := range d.Measured {
			f := &d.Measured[i]
			var bad, notes []string
			if !decidingProfiles[f.S.Profile] {
				bad = append(bad, fmt.Sprintf("was measured under profile %q, which decides nothing (assumption A24.33); re-run `hack/bakeoff.sh %s full`",
					or(f.S.Profile, "unrecorded"), f.Row))
			}
			if f.S.CommitDirtySource {
				bad = append(bad, "was measured on a dirty worktree, so its numbers cannot be tied to any commit")
			}
			if f.S.Commit != "" {
				// The commit spread is a fact from the scorecards and is
				// recorded whether or not git is here to check it against.
				commits[f.S.Commit] = append(commits[f.S.Commit], f.Row)
			}
			switch {
			case f.S.CommitStatus == "unavailable":
				bad = append(bad, "source revision unavailable; correspondence with the current code has not been verified")
			case f.S.Commit == "":
				bad = append(bad, "records no commit, so nothing can say what code it measured")
			case !gitOK:
				notes = append(notes, "staleness not verified: "+root+" is not a git worktree")
			default:
				if _, err := gitAt(root, "cat-file", "-e", f.S.Commit+"^{commit}"); err != nil {
					bad = append(bad, fmt.Sprintf("was measured at commit %s, which this repository does not have", short(f.S.Commit)))
					break
				}
				if _, err := gitAt(root, "merge-base", "--is-ancestor", f.S.Commit, head); err != nil {
					bad = append(bad, fmt.Sprintf("was measured at commit %s, which is not an ancestor of HEAD (%s): it measured a tree that is not this one",
						short(f.S.Commit), short(head)))
					break
				}
				pkg := strings.TrimPrefix(f.Pkg, "./")
				if pkg == "" {
					notes = append(notes, fmt.Sprintf("measured at %s; the family names no package, so nothing checked whether its code changed since", short(f.S.Commit)))
					break
				}
				if _, err := gitAt(root, "diff", "--quiet", f.S.Commit, head, "--", pkg); err != nil {
					n, _ := gitAt(root, "rev-list", "--count", f.S.Commit+".."+head, "--", pkg)
					bad = append(bad, fmt.Sprintf("was measured at commit %s and %s has changed in %s commit(s) since: the numbers describe code that is no longer there",
						short(f.S.Commit), pkg, or(n, "some")))
					break
				}
				notes = append(notes, fmt.Sprintf("measured at %s; %s is unchanged since", short(f.S.Commit), pkg))
			}
			if f.S.Winner == "" {
				notes = append(notes, "names no winner: "+or(f.S.Rule, "no rule recorded"))
			}
			for _, b := range bad {
				msg := fmt.Sprintf("ADR-%03d: %s %s", d.Num, f.Row, b)
				if allowStale {
					notes = append(notes, b)
					continue
				}
				problems = append(problems, msg)
			}
			f.Provenance = strings.Join(notes, "; ")
			if f.Provenance == "" {
				f.Provenance = "current"
			}
		}
		arches := map[string]bool{}
		for i := range d.Measured {
			if h := d.Measured[i].S.Host; h != nil && h.Arch != "" {
				arches[h.Arch] = true
			}
		}
		if len(arches) > 0 {
			names := make([]string, 0, len(arches))
			for a := range arches {
				names = append(names, a)
			}
			sort.Strings(names)
			d.Arches = strings.Join(names, ", ")
		}
		if len(commits) > 1 {
			parts := make([]string, 0, len(commits))
			for c, rows := range commits {
				parts = append(parts, fmt.Sprintf("%s (%s)", short(c), strings.Join(rows, ", ")))
			}
			sort.Strings(parts)
			d.CommitSpread = strings.Join(parts, ", ")
		}
	}
	return problems
}

// gitAt runs one git command in root and returns its trimmed output.
func gitAt(root string, args ...string) (string, error) {
	cmd := exec.Command("git", append([]string{"-C", root}, args...)...)
	out, err := cmd.Output()
	return strings.TrimSpace(string(out)), err
}

func short(commit string) string {
	if len(commit) > 12 {
		return commit[:12]
	}
	return commit
}

// loadScorecard reads one per-family scorecard. A missing file means the
// family has not been measured yet, which is reported as measured=false
// rather than as an error: an ADR must list a family it collects whether or
// not the campaign has reached it.
func loadScorecard(path string) (sc *scorecard, measured bool, err error) {
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	sc = &scorecard{}
	if err := json.Unmarshal(data, sc); err != nil {
		return nil, false, fmt.Errorf("%s: %w", path, err)
	}
	return sc, true, nil
}

// rowLess orders §9 rows by class then number, so that A2 precedes A10 and
// every render lists them in the same order.
func rowLess(a, b string) bool {
	ca, na := splitRow(a)
	cb, nb := splitRow(b)
	if ca != cb {
		return ca < cb
	}
	if na != nb {
		return na < nb
	}
	return a < b
}

func splitRow(row string) (class string, num int) {
	i := 0
	for i < len(row) && (row[i] < '0' || row[i] > '9') {
		i++
	}
	n, err := strconv.Atoi(row[i:])
	if err != nil {
		return row, 0
	}
	return row[:i], n
}

// adrPath finds the file for an ADR number: the existing one when the number
// already has a file, otherwise the name the registry's slug gives. A number
// claimed by two files is refused rather than guessed at.
func adrPath(dir string, d *adrDoc) (path string, existed bool, err error) {
	matches, err := filepath.Glob(filepath.Join(dir, fmt.Sprintf("ADR-%03d-*.md", d.Num)))
	if err != nil {
		return "", false, err
	}
	sort.Strings(matches)
	switch len(matches) {
	case 0:
		return filepath.Join(dir, fmt.Sprintf("ADR-%03d-%s.md", d.Num, d.Slug)), false, nil
	case 1:
		return matches[0], true, nil
	default:
		return "", false, fmt.Errorf("ADR-%03d is claimed by %d files: %s", d.Num, len(matches), strings.Join(matches, ", "))
	}
}

// adrWrite is one rendered ADR, held until every ADR of the run has rendered
// without complaint. `bakeoff adr` writes about a dozen files in a loop at the
// end of a multi-hour campaign, so a file it refuses must not leave the set
// half written: everything is prepared first, and a single refusal writes
// nothing at all.
type adrWrite struct {
	path    string
	content []byte
	verb    string // "wrote" or "attached to"
	doc     *adrDoc
}

// prepareADR renders d for path without touching the file system.
func prepareADR(path string, existed bool, d *adrDoc) (*adrWrite, error) {
	var front, body string
	if existed {
		data, err := os.ReadFile(path)
		if err != nil {
			return nil, err
		}
		front, body, err = splitFront(string(data))
		if err != nil {
			return nil, fmt.Errorf("%s: %w", path, err)
		}
		// A file this command wrote it owns; one a human wrote it attaches
		// to. Without this distinction the second render of a generated file
		// would treat it as hand-written and nest the whole document one
		// heading deeper, so a re-render would not be idempotent.
		d.Attach = !hasKey(front, generatedKey, generatedValue)
	} else {
		body = fmt.Sprintf("\n# ADR-%03d: %s\n", d.Num, d.Title)
	}
	d.H0, d.H1, d.H2 = "##", "##", "###"
	if d.Attach {
		d.H1, d.H2 = "###", "####"
	}
	block, err := renderBlock(d)
	if err != nil {
		return nil, err
	}
	if !d.Attach {
		// The generator owns the whole record, so the fields the plan fixes
		// for it are (re)asserted rather than preserved.
		front = setScalar(front, "status", "Accepted (provisional)")
		front = setScalar(front, "kind", "measured")
		front = setScalar(front, "date", d.Date)
		front = setScalar(front, generatedKey, generatedValue)
	}
	// A record the generator owns lists exactly the rows the map gives it, so
	// that a row moved to another ADR leaves. A hand-written one keeps rows
	// no env file names: ADR-011 owns the policy rows C5 and C6, and
	// attaching a measured row must not take them away.
	keep := ""
	if d.Attach {
		keep = front
	}
	front = setScalar(front, "variants", "["+strings.Join(d.variants(keep), ", ")+"]")
	front = setScalar(front, "authoritative", "no")
	if len(d.Auto) > 0 {
		entries := make([][2]string, 0, len(d.Auto))
		for _, a := range d.Auto {
			entries = append(entries, [2]string{a.Key, a.Slug})
		}
		front = setBlock(front, "auto", entries)
	}
	if !d.Attach {
		// The title heading is the tool's line, so a title corrected in
		// hack/bakeoff/adrs.tsv reaches the file on the next render. Without
		// this, the heading sat outside the markers and no render could ever
		// refresh it: a campaign renders a dozen ADRs and someone fixes a
		// title afterwards, and the correction would never land while the
		// front matter around it was being rewritten every time.
		body = setTitleHeading(body, d)
	}
	body, err = spliceBlock(body, block)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	// The verb reports what the render did to the record, which is a fact
	// about who owns it and not about whether the file happened to exist: a
	// tool-owned record is written whether this is its first render or its
	// tenth, and only a hand-written one is attached to.
	verb := "wrote"
	if d.Attach {
		verb = "attached to"
	}
	return &adrWrite{path: path, content: []byte("---\n" + front + "\n---\n" + body), verb: verb, doc: d}, nil
}

// titleHeading is the `# ADR-<number>: <title>` line of a generated record.
func titleHeading(d *adrDoc) string {
	return fmt.Sprintf("# ADR-%03d: %s", d.Num, d.Title)
}

// setTitleHeading refreshes the title line of a record the tool owns, leaving
// every other line of the body -- including prose a human added outside the
// markers -- exactly where it was. The first `# ` heading is the title; a body
// that has lost it gains one at the top, because `make adr-check` requires the
// body to start with a title heading.
func setTitleHeading(body string, d *adrDoc) string {
	want := titleHeading(d)
	lines := strings.Split(body, "\n")
	for i, line := range lines {
		if !strings.HasPrefix(line, "# ") {
			continue
		}
		if line == want {
			return body
		}
		lines[i] = want
		return strings.Join(lines, "\n")
	}
	return "\n" + want + "\n" + strings.TrimLeft(body, "\n")
}

// commit replaces the file atomically: the content is written to a temporary
// file in the same directory and renamed over the target, so an interrupted
// or failed render leaves the previous file exactly as it was and never a
// truncated one. `os.WriteFile` truncates in place, which put a hand-written
// record at risk of being destroyed by a crash between two of the dozen files
// one run writes. The temporary name begins with a dot and ends in .tmp, so
// adrPath's ADR-<number>-*.md glob can never see it.
func (w *adrWrite) commit() error {
	dir := filepath.Dir(w.path)
	f, err := os.CreateTemp(dir, ".bakeoff-adr-*.tmp")
	if err != nil {
		return err
	}
	tmp := f.Name()
	// Removing after a successful rename is a no-op; removing after any
	// failure is what keeps the directory clean.
	defer func() { _ = os.Remove(tmp) }()
	if _, err := f.Write(w.content); err != nil {
		_ = f.Close()
		return err
	}
	// Durability as well as atomicity: a rename that outlives the data it
	// points at would leave an empty file after a power failure.
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	// CreateTemp makes the file 0600; an ADR is world-readable like its
	// neighbours.
	if err := os.Chmod(tmp, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, w.path)
}

// hasKey reports whether front matter carries `key: value`.
func hasKey(front, key, value string) bool {
	for _, line := range strings.Split(front, "\n") {
		if rest, ok := strings.CutPrefix(line, key+":"); ok {
			return strings.TrimSpace(rest) == value
		}
	}
	return false
}

// frontVariants reads the rows a file's front matter already lists.
func frontVariants(front string) []string {
	for _, line := range strings.Split(front, "\n") {
		rest, ok := strings.CutPrefix(line, "variants:")
		if !ok {
			continue
		}
		var rows []string
		for _, r := range strings.Split(strings.Trim(strings.TrimSpace(rest), "[]"), ",") {
			if r = strings.TrimSpace(r); r != "" {
				rows = append(rows, r)
			}
		}
		return rows
	}
	return nil
}

// splitFront separates a document's front matter from its body.
func splitFront(doc string) (front, body string, err error) {
	doc = strings.ReplaceAll(doc, "\r\n", "\n")
	if !strings.HasPrefix(doc, "---\n") {
		return "", "", errors.New("missing front matter")
	}
	end := strings.Index(doc[4:], "\n---\n")
	if end < 0 {
		return "", "", errors.New("unterminated front matter")
	}
	return doc[4 : 4+end], doc[4+end+5:], nil
}

// setScalar replaces or appends `key: value` in front matter, keeping every
// other line as it was written. A key held in a block form is replaced whole.
func setScalar(front, key, value string) string {
	lines, at := dropKey(frontLines(front), key)
	entry := key + ": " + value
	if at < 0 {
		return strings.Join(append(lines, entry), "\n")
	}
	return strings.Join(append(lines[:at:at], append([]string{entry}, lines[at:]...)...), "\n")
}

// setBlock replaces or appends a key holding a mapping, written in the block
// form ParseADRAutoBinding reads.
func setBlock(front, key string, entries [][2]string) string {
	lines, at := dropKey(frontLines(front), key)
	block := []string{key + ":"}
	for _, e := range entries {
		block = append(block, "  "+e[0]+": "+e[1])
	}
	if at < 0 {
		return strings.Join(append(lines, block...), "\n")
	}
	return strings.Join(append(lines[:at:at], append(block, lines[at:]...)...), "\n")
}

// frontLines splits front matter into lines. Empty front matter has no
// lines at all, so that the first key written into a new file does not land
// after a blank one.
func frontLines(front string) []string {
	if front == "" {
		return nil
	}
	return strings.Split(front, "\n")
}

// dropKey removes a front-matter key and, when it holds a block, every
// indented line under it. It returns the remaining lines and the index the
// key was found at (-1 when absent), so the replacement lands where the key
// was rather than at the end.
func dropKey(lines []string, key string) ([]string, int) {
	at := -1
	out := make([]string, 0, len(lines))
	for i := 0; i < len(lines); i++ {
		if !strings.HasPrefix(lines[i], key+":") {
			out = append(out, lines[i])
			continue
		}
		at = len(out)
		for i+1 < len(lines) && (strings.HasPrefix(lines[i+1], " ") || strings.HasPrefix(lines[i+1], "\t")) {
			i++
		}
	}
	return out, at
}

// spliceBlock replaces the generated section of a body, or appends one when
// the body has none. Rendering the same document twice therefore produces the
// same bytes.
//
// The markers are a contract, not a convention: a body whose markers are not
// a well-formed single pair is refused by markerSpan and nothing is written.
// The version that took the first BEGIN and the first END and appended
// whenever it could not find both silently DELETED prose. A hand-written ADR
// holding a lone BEGIN -- a merge that dropped the other line, an interrupted
// render, a marker someone copied as a placeholder -- gained a second BEGIN on
// the first render (append) and then, on the second, spliced from the first
// BEGIN to the only END and threw away everything the human had written
// between them: measured 242 -> 3896 -> 3789 bytes, with "## Decision" and
// "## Consequences" gone and exit 0 throughout. A stray END above the BEGIN
// appended a whole fresh copy on every render instead, 3803 -> 7407 -> 11011
// bytes, and nothing said so.
func spliceBlock(body, block string) (string, error) {
	section := beginMarker + "\n" + block + endMarker + "\n"
	start, end, present, err := markerSpan(body)
	if err != nil {
		return "", err
	}
	if present {
		return body[:start] + section + body[end:], nil
	}
	return strings.TrimRight(body, "\n") + "\n\n" + section, nil
}

// markerSpan locates the generated section of a body. present is false, with
// no error, for a body that has no markers at all: one is appended. Anything
// that is not zero or one well-formed pair is an error naming the lines,
// because the alternative is a tool that edits a human's file on a guess.
//
// A marker counts only on a line of its own. ADR-017's own prose quotes both
// markers while explaining them, and a substring search would have spliced
// the generated section into the middle of a table cell.
func markerSpan(body string) (start, end int, present bool, err error) {
	begins := markerLines(body, beginMarker)
	ends := markerLines(body, endMarker)
	switch {
	case len(begins) > 1:
		return 0, 0, false, fmt.Errorf("%d BEGIN markers (%s); a file holds at most one generated section%s",
			len(begins), lineList(begins), markerAdvice)
	case len(ends) > 1:
		return 0, 0, false, fmt.Errorf("%d END markers (%s); a file holds at most one generated section%s",
			len(ends), lineList(ends), markerAdvice)
	case len(begins) == 1 && len(ends) == 0:
		return 0, 0, false, fmt.Errorf("a BEGIN marker on %s with no END marker; the generated section has no end, so nothing can say where the prose after it starts%s",
			lineList(begins), markerAdvice)
	case len(begins) == 0 && len(ends) == 1:
		return 0, 0, false, fmt.Errorf("an END marker on %s with no BEGIN marker; the generated section has no start%s",
			lineList(ends), markerAdvice)
	case len(begins) == 1 && ends[0].off < begins[0].off:
		return 0, 0, false, fmt.Errorf("the END marker (%s) comes before the BEGIN marker (%s)%s",
			lineList(ends), lineList(begins), markerAdvice)
	case len(begins) == 0:
		return 0, 0, false, nil
	}
	// The section runs from the start of the BEGIN line to just past the
	// newline that ends the END line.
	return begins[0].off, min(ends[0].off+len(endMarker)+1, len(body)), true, nil
}

// markerAdvice tells the reader what to do about a malformed pair. Both ways
// out are safe: a restored pair is spliced, and no markers at all means a
// fresh section is appended below the prose.
const markerAdvice = ". Restore the pair by hand, or delete both markers together with the generated text between them, and render again"

// markerLine is one whole-line occurrence of a marker.
type markerLine struct {
	off  int // byte offset the line starts at
	line int // 1-based line number
}

// markerLines lists every line of body that is exactly marker.
func markerLines(body, marker string) []markerLine {
	var found []markerLine
	num := 1
	for off := 0; off <= len(body); num++ {
		line := body[off:]
		if n := strings.IndexByte(line, '\n'); n >= 0 {
			line = line[:n]
		}
		if line == marker {
			found = append(found, markerLine{off: off, line: num})
		}
		next := off + len(line) + 1
		if next <= off {
			break
		}
		off = next
	}
	return found
}

// lineList renders marker positions as "line 7" or "lines 7, 42".
func lineList(ms []markerLine) string {
	nums := make([]string, 0, len(ms))
	for _, m := range ms {
		nums = append(nums, strconv.Itoa(m.line))
	}
	if len(nums) == 1 {
		return "line " + nums[0]
	}
	return "lines " + strings.Join(nums, ", ")
}

// renderBlock renders the generated section.
func renderBlock(d *adrDoc) (string, error) {
	var b strings.Builder
	if err := blockTmpl.Execute(&b, d); err != nil {
		return "", err
	}
	// One trailing newline, so that the closing marker sits on its own line
	// whatever the template's last action left behind.
	return strings.TrimRight(b.String(), "\n") + "\n", nil
}

var blockFuncs = template.FuncMap{
	"pct": funcs["pct"],
	"f1":  funcs["f1"],
	"f0":  funcs["f0"],
	"code": func(s string) string {
		if s == "" {
			return "—"
		}
		return "`" + s + "`"
	},
	"or1": funcs["or1"],
	// gstatus spells a guard's machine status for a reader: the point of the
	// column is that a record must not imply a check that had no data.
	"gstatus": func(status string) string {
		if status == guardEvaluated {
			return "evaluated"
		}
		return "**not evaluated**"
	},
	"ci":     funcs["ci"],
	"rungs":  funcs["rungs"],
	"rungOf": funcs["rungOf"],
	"host": func(h *hostInfo) string {
		if h == nil {
			return "unrecorded"
		}
		parts := []string{h.Hostname, h.OS + "/" + h.Arch, h.CPUModel}
		if h.CPUs > 0 {
			parts = append(parts, fmt.Sprintf("%d CPUs", h.CPUs))
		}
		if h.Kernel != "" {
			// sysinfo records the whole kernel version banner; the ADR wants
			// the release, not the compiler that built it.
			if f := strings.Fields(h.Kernel); len(f) > 3 {
				parts = append(parts, "kernel "+strings.Join(f[:3], " "))
			} else {
				parts = append(parts, "kernel "+h.Kernel)
			}
		}
		if h.Governor != "" {
			parts = append(parts, "governor "+h.Governor)
		}
		if h.GoVersion != "" {
			parts = append(parts, h.GoVersion)
		}
		var kept []string
		for _, p := range parts {
			if p != "" && p != "/" {
				kept = append(kept, p)
			}
		}
		return strings.Join(kept, ", ")
	},
	"disq": func(sc *scorecard) string {
		var bad []string
		for _, k := range sc.Kinds {
			if k.Disqualified {
				bad = append(bad, k.Kind+" ("+or(k.Reason, "no reason recorded")+")")
			}
		}
		if len(bad) == 0 {
			return "none"
		}
		return strings.Join(bad, "; ")
	},
}

func or(s, alt string) string {
	if s == "" {
		return alt
	}
	return s
}

// blockTmpl is the one and only ADR renderer. Its heading levels shift by one
// when the material is attached to a hand-written ADR, so that the generated
// sections nest under a single wrapper heading instead of duplicating the
// headings the human file already has.
var blockTmpl = template.Must(template.New("block").Funcs(blockFuncs).Parse(
	`{{- if .Attach}}{{.H0}} Bake-off data ({{.Rows}})

Generated by ` + "`bakeoff adr`" + ` from the per-family scorecards under
` + "`benchmarks/reports/scorecards/`" + `. The decision this file records is not
changed by it; everything between the two markers is rewritten on every
render, so edit the prose outside them.

{{end -}}
{{.H1}} Context

This record collects the bake-off (the benchmark configuration) for §9 rows
**{{.Rows}}**. Every kind of every row is implemented -- no kind is skipped
for looking like a loser -- and each passed the correctness gate of §9.4
step 1 before it was measured under step 2 by ` + "`hack/bakeoff.sh <row>`" + `.

| row | subject | package | benchmark root | reference | winner | rule |
|---|---|---|---|---|---|---|
{{range .Families}}| {{.Row}} | {{or1 .Title "—"}} | {{code .Pkg}} | {{code .Bench}} | {{or1 .Ref "—"}} | {{if .S}}**{{or1 .S.Winner "not decided"}}**{{else}}not measured{{end}} | {{if .S}}{{.S.Rule}}{{else}}—{{end}} |
{{end}}{{if .CommitSpread}}
The rows collected here were **not all measured on one tree**: {{.CommitSpread}}.
{{end}}
{{.H1}} Decision

Each row's winner is the one its section below records, chosen by **that
row's own criterion** (the benchmark configuration, quoted in the row's section)
within §0.3: the lowest geomean of the row's primary metric over the deciding
rows every candidate covers, each GOMAXPROCS rung counted as the separate
measurement it is; candidates whose 95% confidence interval cannot separate
them from the best by more than 3% resolved by complexity (LOC +
1000×unsafe); and §0.3(3)'s exclusion applied to whichever of p99 latency,
memory footprint and fairness the run produced data for. **Each row's section
states which of the three was evaluated and which had none**, so nothing here
claims a check that could not fire.
{{if .Auto}}
` + "`Auto`" + ` is bound accordingly:

{{range .Auto}}- ` + "`{{.Key}}: {{.Slug}}`" + ` ({{.Row}}){{if .File}} -- generated into {{code .File}}{{end}}
{{end}}{{else}}
No family collected here binds a generated ` + "`Auto`" + ` constant: their kinds are
chosen per call or by a build tag.
{{end}}{{if .Pending}}
Not yet measured, and therefore not yet decided:

{{range .Pending}}- **{{.Row}}**{{if .Title}} ({{.Title}}){{end}} -- run ` + "`hack/bakeoff.sh {{.Row}} full`" + ` and re-render.
{{end}}{{end}}
{{.H1}} Alternatives

{{range .Measured}}{{$.H2}} {{.Row}} -- {{or1 .Title "—"}}

Winner: **{{.S.Winner}}** ({{.S.Rule}}). Reference: {{code .S.Reference}}.
{{if .Binds}}Binds ` + "`{{.AutoKey}}`" + `.{{else}}Binds no ` + "`Auto`" + ` constant.{{end}}

§9.1 criterion: {{or1 .S.Criterion "—"}}
{{if .S.Workload}}
Deciding workload: {{.S.Workload}}
{{end}}
| kind | geomean {{or1 .S.Metric "ns/op"}} | 95% CI vs best | vs ref | vs best | max allocs/op | B/op mean | p99 geomean | LOC | unsafe | rows | disposition | note |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
{{range .S.Kinds}}| {{.Kind}} | {{f1 .GeomeanNs}} | {{ci .RatioLo .RatioHi}} | {{pct .RatioVsRef}} | {{pct .RatioVsBest}} | {{f0 .AllocsMax}} | {{f1 .BytesMean}} | {{f1 .P99Geomean}} | {{.LOCText}} | {{.Unsafe}} | {{.Benchmarks}}/{{.DecisionRows}} | {{.Disposition}} | {{.Reason}} |
{{end}}{{if .S.Rungs}}
Per-rung geomeans, because a rung is a measurement and not a dimension to
pool over:

| kind |{{range .S.Rungs}} GOMAXPROCS={{.}} |{{end}}
|---|{{range .S.Rungs}}---|{{end}}
{{$r := .S.Rungs}}{{range .S.Kinds}}| {{.Kind}} |{{$k := .}}{{range $r}} {{rungOf $k .}} |{{end}}
{{end}}{{end}}
§0.3(3) exclusion guards:

| metric | unit | status | detail |
|---|---|---|---|
{{range .S.Guards}}| {{.Metric}} | ` + "`{{.Unit}}`" + ` | {{gstatus .Status}} | {{or1 .Detail "—"}} |
{{end}}{{if .S.Unobservable}}
Parts of this row's criterion no benchmark here can evaluate, and which this
record therefore does not claim to have applied:
{{range .S.Unobservable}}
- {{.}}{{end}}
{{end}}
Evidence: {{len .S.Common}} deciding row(s) every candidate covers, run
` + "`{{or1 .S.RunID \"unrecorded\"}}`" + ` (profile {{or1 .S.Profile "unrecorded"}}), decided {{.S.DecidedAt}}.
Provenance: {{or1 .Provenance "unchecked"}}.
Disqualified kinds: {{disq .S}}.{{range .S.Notes}}
- {{.}}{{end}}

{{end}}{{.H1}} Protocol coverage

This record cites the benchmark configuration and the campaign implements part of it.
Done: step 1's correctness gate per kind (the family's model tests, its
differential fuzz corpus and the race detector); step 2's repeated
` + "`b.Loop`" + ` measurement at a pinned ` + "`GOMAXPROCS`" + ` with the kinds
interleaved, the warm-up discarded, ` + "`-benchmem`" + ` metrics and 95% confidence
intervals; step 4's §0.3 scoring under **each row's own §9.1/§9.2 criterion**;
and step 5's record with the raw data and the machine. Not done, and therefore
not covered by any decision here:

- **one architecture, not two.** Step 2 asks for an x86-64 and an arm64
  runner; every number here was measured on {{or1 .Arches "an unrecorded architecture"}}.
- **closed-loop only.** Step 2 asks for open-loop,
  coordinated-omission-resistant measurement at roughly 50/80/100/120% of
  measured capacity. Nothing in the campaign measures capacity, so no
  percentage of it is measured, and no default here is chosen on behaviour
  above saturation -- which is where an admission policy and a wait strategy
  differ most.
- **no stress stage.** Step 3's registry scenarios -- contention sweeps,
  bursts, Zipf skew, slow and stalled consumers, failure injection, memory
  plateau -- are not a stage of the campaign. Some families sweep some of
  those dimensions inside their own benchmark, which is not the same thing.
- **one ` + "`GOMAXPROCS`" + ` rung, and a bounded benchtime.** §10.4 asks for a
  ` + "`GOMAXPROCS`" + ` ladder of 1, 2, 4, 8 and max; the campaign measures one
  rung, at the pinned width. No single benchmark may occupy the machine for
  more than three seconds, and no campaign may run for hours, so the count,
  the rungs and the benchtime are chosen to fit inside those ceilings -- see
  Raw data below for the exact values this decision was measured at. What the
  ladder would have added is mostly a second copy of an axis the benchmarks
  sweep themselves: family A1's contention point is ` + "`P=32`" + ` inside the
  benchmark name rather than a scheduler rung, and the allocation counts the
  API-shape rows turn on do not vary with ` + "`GOMAXPROCS`" + ` at all. What it
  would genuinely have added, and what is therefore not covered here, is how a
  kind behaves when the scheduler is narrower or wider than the machine it was
  measured on.
- **a shared, unpinned host.** Step 2 asks for dedicated runners with no
  colocated noise; see Raw data below for what was enforced instead.

{{.H1}} Correctness gate

Every kind ran the family's shared model test, its differential fuzz corpus
and the race detector before measurement; ` + "`hack/bakeoff.sh`" + ` disqualifies a
kind whose ` + "`TestModel/<kind>`" + ` or fuzz target fails and records it above,
and a run in which a non-kind test fails produces no numbers at all.

{{.H1}} Raw data

{{range .Measured}}- **{{.Row}}**: sysinfo: {{code .S.Sysinfo}}; raw results:
  {{code .S.RawResults}}; run id ` + "`{{or1 .S.RunID \"unrecorded\"}}`" + `; commit
  ` + "`{{or1 .S.Commit (or1 .S.CommitStatus \"unrecorded\")}}`" + `{{if .S.CommitDirtySource}} (tree dirty){{end}}{{if and .S.CommitDirty (not .S.CommitDirtySource)}} (the campaign's own generated bindings uncommitted){{end}}; machine: {{host .S.Host}};
  methodology: {{or1 .S.Methodology "unrecorded"}}; provenance:
  {{or1 .Provenance "unchecked"}}
{{end}}
Every run holds the host-wide benchmark lock, waits for the load average to
fall below the threshold and is discarded and repeated when a foreign
benchmark runs before or during it (` + "`hack/benchguard.sh`" + `).

**Authoritative: no** -- measured on a shared, unpinned host. Re-ratify with
` + "`make bakeoff PROFILE=full FAMILY=<row>`" + ` on pinned hardware, then
` + "`bakeoff adr`" + `, before the v1 defaults freeze.

{{.H1}} Disposition

{{range .Measured}}- **{{.Row}}**:{{range .S.Kinds}} {{.Kind}} = {{.Disposition}};{{end}}
{{end}}
{{.H1}} Auto binding

{{if .Auto}}{{range .Auto}}- ` + "`{{.Key}}: {{.Slug}}`" + ` ({{.Row}}) -- {{code .File}}, ` + "`AutoProvisional = true`" + `
{{end}}{{else}}None: no family collected here generates an ` + "`Auto`" + ` constant.
{{end}}
{{.H1}} Consequences

The defaults may change when the bake-off is re-run on authoritative
hardware, which is what ` + "`AutoProvisional`" + ` announces. Losing kinds stay
selectable until the M6 disposition review; a kind removed before v1.0 is
recorded above with the commit of the last tree that contained it.
`,
))
