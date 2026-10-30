package main

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
)

// absentMarker opens an entry of the Where column that names evidence which
// must NOT resolve. absentReason separates the names from the reason.
const (
	absentMarker = "absent:"
	absentReason = " -- "
	// absentReasonMin is the shortest reason accepted. A marker is a claim
	// about why something is missing, and three words is not one.
	absentReasonMin = 24
)

var (
	// testNameRE finds a Test or Fuzz function name anywhere in a row.
	testNameRE = regexp.MustCompile(`\b(?:Test|Fuzz)[A-Z][A-Za-z0-9_]*`)
	// tickRE finds one back-ticked token and where it sits.
	tickRE = regexp.MustCompile("`([^`]+)`")
	// identRE is the shape of a back-ticked token this checker undertakes to
	// resolve as a Go identifier: a bare name, a qualified one (`sink.Close`),
	// or a method on a pointer receiver (`(*Sink).Close`).
	identRE = regexp.MustCompile(`^\(?\*?[A-Za-z_][A-Za-z0-9_]*\)?(?:\.[A-Za-z_][A-Za-z0-9_]*)*$`)
	// subtestRE finds a t.Run subtest name, which go test -list cannot see.
	subtestRE = regexp.MustCompile(`\.Run\(\s*"([^"]+)"`)
	// filePathRE is a path or file name rather than an identifier.
	filePathRE = regexp.MustCompile(`\.(?:go|md|ya?ml|sh|env|tsv|json|txt|tmpl|toml|proto|sql|py|mod|sum|gz|cfg|tla|html)$`)
	// wordySepRE is the gap allowed BETWEEN two citations of one entry: the
	// column's convention is a path, a colon and a comma-separated list, so a
	// gap holding a word is prose and what follows it is not a citation. This
	// is what keeps "the `FeatureDeadLetter` exercise", "`cliEnv.bridge`'s
	// `abort`" and "the `incomplete` branch of `order`" out, and lets
	// "`a.go`: `X`, `Y`" and "`a.go` (`X`)" in.
	wordySepRE = regexp.MustCompile("^[\\s,:;/()\\[\\]\"'’—–+&|-]*$")
	// buildTagRE picks the tag identifiers out of one //go:build line.
	buildTagRE = regexp.MustCompile(`\b([A-Za-z_][A-Za-z0-9_.]*)\b`)
)

// platformTags are build tags this check does not vary, because they are the
// toolchain's rather than this repository's: a test behind `//go:build race` is
// the same test, and a `go1.27` file is selected by the toolchain in use.
var platformTags = map[string]bool{
	"race": true, "unix": true, "linux": true, "darwin": true, "windows": true,
	"cgo": true, "gc": true, "gccgo": true, "amd64": true, "arm64": true,
	"js": true, "wasm": true, "purego": true, "ignore": true,
}

// identCite is one name a row's evidence column asks about.
type identCite struct {
	name string
	dirs []string // package directories the same entry named
}

// rowEvidence is what one row's Where column names, after the marker is split
// out of it.
type rowEvidence struct {
	idents  []identCite
	absent  []string
	skipped int // entries whose path names nothing in the tree
}

// tree is the parsed repository: which directories hold a file of each base
// name, every package's declarations, and every declaration anywhere.
type tree struct {
	repo    string
	byBase  map[string][]string // "consumer.go" -> the directories holding one
	pkgSyms map[string]map[string]bool
	allSyms map[string]bool
}

// expandBraces expands one `{a,b}` group, which is how the ledger writes a path
// shared by several drivers (`drivers/{kafka,nats}/mapping.go`).
func expandBraces(s string) []string {
	open := strings.IndexByte(s, '{')
	if open < 0 {
		return []string{s}
	}
	shut := strings.IndexByte(s[open:], '}')
	if shut < 0 {
		return []string{s}
	}
	shut += open
	var out []string
	for _, alt := range strings.Split(s[open+1:shut], ",") {
		out = append(out, expandBraces(s[:open]+strings.TrimSpace(alt)+s[shut+1:])...)
	}
	return out
}

// isPath reports whether a back-ticked token names a file or directory rather
// than an identifier. `Makefile` is a path: it is a file at the repository
// root, and reading it as a Go name would be a finding about nothing.
func (t *tree) isPath(tok string) bool {
	if strings.Contains(tok, "/") || filePathRE.MatchString(tok) {
		return true
	}
	if _, err := os.Stat(filepath.Join(t.repo, tok)); err == nil {
		return true
	}
	return false
}

// isGoPath reports whether a path token names Go source: a `.go` file, or a
// directory that holds one.
func (t *tree) isGoPath(tok string) bool {
	for _, p := range expandBraces(tok) {
		if strings.HasSuffix(p, ".go") {
			continue
		}
		if filePathRE.MatchString(p) {
			return false // some other kind of file
		}
		if _, ok := t.pkgSyms[filepath.Join(t.repo, p)]; !ok {
			return false // a directory with no Go files in it, or not a directory
		}
	}
	return true
}

// dirsFor resolves the package directories one path token names. A glob is
// expanded against the tree; a bare file name is resolved by base name,
// because the ledger writes the row's own package that way (`consumer.go`).
func (t *tree) dirsFor(tok string) (dirs []string, ok bool) {
	for _, p := range expandBraces(tok) {
		switch {
		case strings.ContainsAny(p, "*?"):
			matches, err := filepath.Glob(filepath.Join(t.repo, p))
			if err != nil {
				continue
			}
			for _, m := range matches {
				dirs = append(dirs, filepath.Dir(m))
			}
		case !strings.Contains(p, "/") && filePathRE.MatchString(p):
			dirs = append(dirs, t.byBase[p]...)
		default:
			full := filepath.Join(t.repo, p)
			st, err := os.Stat(full)
			if err != nil {
				continue
			}
			if st.IsDir() {
				dirs = append(dirs, full)
				continue
			}
			dirs = append(dirs, filepath.Dir(full))
		}
	}
	sort.Strings(dirs)
	return slicesCompact(dirs), len(dirs) > 0
}

func slicesCompact(s []string) []string {
	out := s[:0]
	var prev string
	for i, v := range s {
		if i > 0 && v == prev {
			continue
		}
		out = append(out, v)
		prev = v
	}
	return out
}

// citedToken is one back-ticked token of an evidence entry, in the order the
// entry writes them.
type citedToken struct {
	text   string
	isPath bool
}

// citedTokens reads one evidence entry into its back-ticked tokens.
//
// The Where column's convention is a path, a colon and a comma-separated list
// of names, repeated: each path governs the names that FOLLOW it until the next
// path. A back-tick reached across a WORD is prose about the code rather than a
// citation of it, and reading it as a citation is how "the `FeatureDeadLetter`
// exercise" becomes a finding about a constant that exists.
func (t *tree) citedTokens(entry string) []citedToken {
	var out []citedToken
	prevEnd := 0
	for _, m := range tickRE.FindAllStringSubmatchIndex(entry, -1) {
		tok := strings.TrimSpace(entry[m[2]:m[3]])
		gap := entry[prevEnd:m[0]]
		if t.isPath(tok) {
			out = append(out, citedToken{text: tok, isPath: true})
			prevEnd = m[1]
			continue
		}
		// prevEnd advances only on an ACCEPTED token, so that the second
		// back-tick of a prose run is judged from where the prose started
		// rather than from the token the prose already disqualified: in "the
		// `authoritative`/`provisional_scope` block" the gap before the second
		// name is "/", which would read as a list separator, and the whole
		// phrase is prose about two YAML keys.
		if !wordySepRE.MatchString(gap) {
			continue
		}
		if tok == "" || strings.ContainsAny(tok, " \t") || testNameRE.MatchString(tok) {
			continue
		}
		if identRE.MatchString(tok) {
			out = append(out, citedToken{text: tok})
			prevEnd = m[1]
		}
	}
	return out
}

// parseEvidence splits one row's Where column into the names it cites and the
// names it declares must not resolve.
func (t *tree) parseEvidence(where string) (rowEvidence, []string) {
	var ev rowEvidence
	var problems []string
	for _, entry := range strings.Split(where, ";") {
		trimmed := strings.TrimSpace(entry)
		if strings.HasPrefix(trimmed, absentMarker) {
			problems = append(problems, t.parseMarker(trimmed, &ev)...)
			continue
		}
		// A path governs the names that follow it, and it has to be a GO path
		// or the names beside it are not Go names. That is what the residue
		// turned out to be almost entirely made of: `hack/benchguard.sh`'s
		// `decide_swamped` is a shell function, `hack/bakeoff/*.env`'s
		// `CRITERION_GAP` a shell variable, `models/tla/*.tla`'s
		// `DrainBounded` a TLA+ operator, `.github/workflows/*.yml`'s
		// `linearizability` a job and `Makefile`'s `apidiff` a target. Every
		// one is a real name of a real thing and none of them is a Go
		// identifier, so a Go symbol table has nothing to say about them --
		// and said it forty times.
		var dirs []string
		governed := false
		for _, tok := range t.citedTokens(entry) {
			if tok.isPath {
				governed = true
				dirs = nil
				if !t.isGoPath(tok.text) {
					continue
				}
				d, ok := t.dirsFor(tok.text)
				if !ok {
					continue
				}
				dirs = d
				continue
			}
			if !governed {
				continue // a bare name with no package named before it
			}
			if len(dirs) == 0 {
				ev.skipped++
				continue
			}
			ev.idents = append(ev.idents, identCite{name: tok.text, dirs: dirs})
		}
	}
	return ev, problems
}

// withoutMarkers drops the `absent:` entries from a Where column.
func withoutMarkers(where string) string {
	var kept []string
	for _, entry := range strings.Split(where, ";") {
		if strings.HasPrefix(strings.TrimSpace(entry), absentMarker) {
			continue
		}
		kept = append(kept, entry)
	}
	return strings.Join(kept, ";")
}

// parseMarker reads one `absent:` entry.
func (t *tree) parseMarker(entry string, ev *rowEvidence) []string {
	var problems []string
	rest := strings.TrimSpace(strings.TrimPrefix(entry, absentMarker))
	names, reason, ok := strings.Cut(rest, absentReason)
	if !ok {
		return []string{fmt.Sprintf("its %s entry has no %q and therefore no reason; "+
			"a marker with no reason is a silent exemption", absentMarker, strings.TrimSpace(absentReason))}
	}
	if n := len(strings.TrimSpace(reason)); n < absentReasonMin {
		problems = append(problems, fmt.Sprintf("its %s entry gives a reason of %d characters; "+
			"say why the name must not resolve, in at least %d", absentMarker, n, absentReasonMin))
	}
	before := len(ev.absent)
	for _, m := range tickRE.FindAllStringSubmatch(names, -1) {
		ev.absent = append(ev.absent, strings.TrimSpace(m[1]))
	}
	if len(ev.absent) == before {
		problems = append(problems, fmt.Sprintf("its %s entry names nothing in back-ticks before %q",
			absentMarker, strings.TrimSpace(absentReason)))
	}
	return problems
}

// skipWalk reports whether a walk of the tree leaves the directory named name
// unread. .claude is on the list because an agent's worktree under
// .claude/worktrees is a whole second checkout: its copy of a deleted test or
// identifier would satisfy a citation of it.
func skipWalk(name string) bool {
	switch name {
	case ".git", ".claude", "node_modules":
		return true
	}
	return false
}

// loadTree parses every Go file in the repository once.
//
// It is a parse and not a type-check on purpose: the question is "is this name
// declared", nothing needs to resolve to a type, and a type-check would need
// every dependency of fifty modules to build.
func loadTree(repo string) *tree {
	t := &tree{repo: repo, byBase: map[string][]string{}, pkgSyms: map[string]map[string]bool{}, allSyms: map[string]bool{}}
	var dirs []string
	_ = filepath.WalkDir(repo, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return nil //nolint:nilerr // an unreadable directory is not this check's business
		}
		if d.IsDir() {
			if skipWalk(d.Name()) {
				return filepath.SkipDir
			}
			return nil
		}
		dir := filepath.Dir(path)
		t.byBase[d.Name()] = append(t.byBase[d.Name()], dir)
		if strings.HasSuffix(d.Name(), ".go") && (len(dirs) == 0 || dirs[len(dirs)-1] != dir) {
			dirs = append(dirs, dir)
		}
		return nil
	})
	for base := range t.byBase {
		sort.Strings(t.byBase[base])
		t.byBase[base] = slicesCompact(t.byBase[base])
	}
	for _, dir := range dirs {
		syms := packageSymbols(dir)
		t.pkgSyms[dir] = syms
		for n := range syms {
			t.allSyms[n] = true
		}
	}
	return t
}

// packageSymbols returns every name one directory's Go files declare.
//
// Declarations, and also composite-literal keys, short variable declarations
// and labels — because that is how the code the ledger cites is written: a
// driver's capability table is `CheckBroker: checkBroker` inside a literal, and
// `wrongProtocol := []driverkit.Rule{…}` is a rule set a row names by its
// variable. Reading only top-level declarations reported both as deleted.
// Every .go file in the directory is parsed, whatever its build tags and
// whatever package clause it carries: the question is "is this name declared
// here", and a name behind `//go:build iqmq_debug` is declared. That is also
// why this reads the directory itself rather than calling the deprecated
// [parser.ParseDir] — whose stated shortcoming is that it ignores build tags,
// and whose recommended replacement (golang.org/x/tools/go/packages) this
// module may not import, being held to the standard library.
func packageSymbols(dir string) map[string]bool {
	names := map[string]bool{}
	ents, err := os.ReadDir(dir)
	if err != nil {
		return names
	}
	add := func(id *ast.Ident) {
		if id != nil && id.Name != "" && id.Name != "_" {
			names[id.Name] = true
		}
	}
	fset := token.NewFileSet()
	for _, e := range ents {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".go") {
			continue
		}
		f, perr := parser.ParseFile(fset, filepath.Join(dir, e.Name()), nil, parser.SkipObjectResolution)
		if perr != nil || f == nil {
			continue // a file that does not parse declares nothing this can read
		}
		ast.Inspect(f, func(n ast.Node) bool {
			switch d := n.(type) {
			case *ast.FuncDecl:
				add(d.Name)
			case *ast.TypeSpec:
				add(d.Name)
			case *ast.ValueSpec:
				for _, id := range d.Names {
					add(id)
				}
			case *ast.Field:
				for _, id := range d.Names {
					add(id)
				}
			case *ast.KeyValueExpr:
				if id, ok := d.Key.(*ast.Ident); ok {
					add(id)
				}
			case *ast.AssignStmt:
				if d.Tok == token.DEFINE {
					for _, lhs := range d.Lhs {
						if id, ok := lhs.(*ast.Ident); ok {
							add(id)
						}
					}
				}
			case *ast.LabeledStmt:
				add(d.Label)
			}
			return true
		})
	}
	return names
}

// gatingTags reports, per module, every build tag that gates a _test.go file.
//
// This is not a nicety. Nineteen test files in the `conformance` module sit
// behind `//go:build integration`, so a plain `go test -list` selects none of
// them and six perfectly good citations read as absent — and the repair a
// reader would reach for is the escape marker, which would then record "this
// name does not exist" about a test that does. The tags are discovered from the
// tree rather than listed here, so a new one cannot hide a test by being
// unknown to this file.
func gatingTags(repo string, modules []string) map[string][]string {
	byMod := map[string]map[string]bool{}
	for _, mod := range modules {
		root := filepath.Join(repo, mod)
		_ = filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
			if err != nil {
				return nil //nolint:nilerr // an unreadable directory is not this check's business
			}
			if d.IsDir() {
				if skipWalk(d.Name()) {
					return filepath.SkipDir
				}
				if path != root {
					if _, serr := os.Stat(filepath.Join(path, "go.mod")); serr == nil {
						return filepath.SkipDir // a nested module lists itself
					}
				}
				return nil
			}
			if !strings.HasSuffix(d.Name(), "_test.go") {
				return nil
			}
			data, rerr := os.ReadFile(path)
			if rerr != nil {
				return nil
			}
			for _, line := range strings.Split(string(data), "\n") {
				line = strings.TrimSpace(line)
				if !strings.HasPrefix(line, "//go:build") {
					if line == "" || strings.HasPrefix(line, "//") {
						continue
					}
					return nil // past the build constraints
				}
				for _, m := range buildTagRE.FindAllStringSubmatch(line[len("//go:build"):], -1) {
					if platformTags[m[1]] || strings.HasPrefix(m[1], "go1.") {
						continue
					}
					if byMod[mod] == nil {
						byMod[mod] = map[string]bool{}
					}
					byMod[mod][m[1]] = true
				}
				return nil
			}
			return nil
		})
	}
	out := map[string][]string{}
	for mod, tags := range byMod {
		for t := range tags {
			out[mod] = append(out[mod], t)
		}
		sort.Strings(out[mod])
	}
	return out
}

// listedTests runs `go test -list` over every module and returns the names the
// toolchain would select.
//
// Each module is listed once with no tags and once per build tag that gates a
// test file in it, because tags are not always compatible with one another
// (`iqmq_queue_mpmc` and `iqmq_queue_native` are exclusive) and a union would
// build neither arm.
//
// A module with no packages at all (hack/tools, which exists for its go.mod)
// exits non-zero saying so, and that is not a failure of this check.
func listedTests(repo string, modules []string) (map[string]bool, []string) {
	all := map[string]bool{}
	var problems []string
	tags := gatingTags(repo, modules)
	for _, mod := range modules {
		for _, tagSet := range append([]string{""}, tags[mod]...) {
			problems = append(problems, listWithTags(repo, mod, tagSet, all)...)
		}
	}
	return all, problems
}

// listWithTags is one `go test -list` invocation, folded into all.
func listWithTags(repo, mod, tags string, all map[string]bool) []string {
	args := []string{"test", "-list", ".*"}
	if tags != "" {
		args = append(args, "-tags", tags)
	}
	args = append(args, "./...")
	cmd := exec.Command("go", args...)
	cmd.Dir = filepath.Join(repo, mod)
	cmd.Env = append(os.Environ(), "GOWORK=off")
	out, err := cmd.CombinedOutput()
	if err != nil && !strings.Contains(string(out), "matched no packages") {
		where := mod
		if tags != "" {
			where += " -tags " + tags
		}
		return []string{fmt.Sprintf("go test -list in %s: %v: %s", where, err, firstLines(string(out), 3))}
	}
	for _, line := range strings.Split(string(out), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.ContainsAny(line, " \t") || !testNameRE.MatchString(line) {
			continue
		}
		all[line] = true
	}
	return nil
}

// subtestNames collects every t.Run name in the tree's test files, because `go
// test -list` cannot see a subtest and ten rows cite one.
//
// It is an EXACT match, not a substring one. An earlier checker asked whether
// any t.Run name was a substring of the citation or the other way about, which
// meant a `t.Run("Rank")` anywhere in the repository excused
// `TestRankIsNotTheDeclarationOrder` — and it hid nine dangling citations that
// way, in rows whose evidence had gone with `x/apibench`.
func subtestNames(repo string) map[string]bool {
	names := map[string]bool{}
	_ = filepath.WalkDir(repo, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return nil //nolint:nilerr // an unreadable directory is not this check's business
		}
		if d.IsDir() {
			if skipWalk(d.Name()) {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, "_test.go") {
			return nil
		}
		data, rerr := os.ReadFile(path)
		if rerr != nil {
			return nil
		}
		for _, m := range subtestRE.FindAllStringSubmatch(string(data), -1) {
			names[m[1]] = true
		}
		return nil
	})
	return names
}

// identVerdict counts how each cited name was resolved, so that no exemption is
// folded into a pass. The instrument is part of the code under test.
type identVerdict struct {
	exact, prefix, subtest, absent, idents, skipped int
}

// checkIdentifiers resolves every name the ledger's rows cite.
func checkIdentifiers(repo, ledger string, rows map[string]assumption, modules []string) (identVerdict, []string) {
	var v identVerdict
	listed, problems := listedTests(repo, modules)
	subtests := subtestNames(repo)
	t := loadTree(repo)

	ids := make([]string, 0, len(rows))
	for k := range rows {
		ids = append(ids, k)
	}
	sort.Strings(ids)

	for _, key := range ids {
		row := rows[key]
		at := fmt.Sprintf("%s:%d: assumption %s", ledger, row.line, row.id)
		ev, evProblems := t.parseEvidence(row.where)
		for _, p := range evProblems {
			problems = append(problems, at+" "+p)
		}
		v.skipped += ev.skipped

		marked := map[string]bool{}
		for _, n := range ev.absent {
			marked[n] = true
		}

		// Test and Fuzz names, from the whole row EXCEPT its markers: a
		// marker names the thing it exempts, so scanning it would make every
		// marker self-justifying and "a marker for a name nobody names" a
		// check that cannot fail.
		cited := map[string]bool{}
		for _, n := range testNameRE.FindAllString(row.claim+" "+withoutMarkers(row.where), -1) {
			cited[n] = true
		}
		names := make([]string, 0, len(cited))
		for n := range cited {
			names = append(names, n)
		}
		sort.Strings(names)
		for _, n := range names {
			if marked[n] {
				continue // handled below, in both directions
			}
			switch {
			case listed[n]:
				v.exact++
			case prefixOfListed(n, listed):
				v.prefix++
			case subtests[n]:
				v.subtest++
			default:
				problems = append(problems, fmt.Sprintf("%s cites %s, which `go test -list` selects in no module: "+
					"rename the citation, name replacement evidence, withdraw the claim, or say why it must not resolve with "+
					"`%s `%s`%s<reason>`", at, n, absentMarker, n, absentReason))
			}
		}

		// Back-ticked Go identifiers, from the Where column.
		for _, c := range ev.idents {
			if marked[c.name] {
				continue
			}
			v.idents++
			if t.resolves(c) {
				continue
			}
			problems = append(problems, fmt.Sprintf("%s names `%s`, which no package in the tree declares "+
				"(the entry names %s): update the citation, or say why it must not resolve with `%s `%s`%s<reason>`",
				at, c.name, strings.Join(t.rel(c.dirs), ", "), absentMarker, c.name, absentReason))
		}

		// The marker, in both directions.
		for _, n := range ev.absent {
			v.absent++
			c, isIdent := identFor(n, ev.idents)
			if !cited[n] && !isIdent {
				problems = append(problems, fmt.Sprintf("%s marks `%s` absent and does not cite it; "+
					"a marker for a name nobody names outlives its reason", at, n))
			}
			switch {
			case listed[n]:
				problems = append(problems, fmt.Sprintf("%s marks `%s` absent, and `go test -list` now selects it: "+
					"the row's claim has become false, or a deleted package is back under its old name", at, n))
			case isIdent && t.resolves(c):
				problems = append(problems, fmt.Sprintf("%s marks `%s` absent, and the tree declares it now: "+
					"the row's claim has become false", at, n))
			}
		}
	}
	return v, problems
}

// resolves reports whether the citation's last component is declared by any
// package the entry named, or failing that anywhere in the repository.
//
// The last component is what is checked: `settler.Extend` is a method on a type
// the package holds and `TokenTable.Freed` is a field of one, and this checker
// does not undertake to know which receiver a method is on — it undertakes to
// know that the name is declared at all, which is exactly what catches a
// citation whose subject a commit deleted.
func (t *tree) resolves(c identCite) bool {
	last := lastComponent(c.name)
	for _, dir := range c.dirs {
		if t.pkgSyms[dir][last] {
			return true
		}
	}
	return t.allSyms[last]
}

func lastComponent(name string) string {
	parts := strings.Split(strings.Trim(name, "()*"), ".")
	return parts[len(parts)-1]
}

// prefixOfListed reports whether a citation names a FAMILY of tests rather than
// one: `FuzzDifferential` for FuzzDifferential_jetstream and its siblings.
func prefixOfListed(name string, listed map[string]bool) bool {
	for n := range listed {
		if n != name && strings.HasPrefix(n, name) {
			return true
		}
	}
	return false
}

func identFor(name string, cites []identCite) (identCite, bool) {
	for _, c := range cites {
		if c.name == name {
			return c, true
		}
	}
	return identCite{}, false
}

func (t *tree) rel(dirs []string) []string {
	out := make([]string, 0, len(dirs))
	for _, d := range dirs {
		if r, err := filepath.Rel(t.repo, d); err == nil {
			out = append(out, r)
			continue
		}
		out = append(out, d)
	}
	if len(out) == 0 {
		return []string{"no package"}
	}
	return out
}

func firstLines(s string, n int) string {
	lines := strings.Split(strings.TrimSpace(s), "\n")
	if len(lines) > n {
		lines = lines[:n]
	}
	return strings.Join(lines, "; ")
}

// repoModules lists every module directory, root first, the way
// hack/modules.sh does. It reads the tree rather than shelling out, so that
// this check works against a fixture directory in its own tests.
func repoModules(repo string) []string {
	mods := []string{"."}
	_ = filepath.WalkDir(repo, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return nil //nolint:nilerr // an unreadable directory is not this check's business
		}
		if d.IsDir() {
			if skipWalk(d.Name()) || d.Name() == "testdata" || d.Name() == "states" {
				return filepath.SkipDir
			}
			return nil
		}
		if d.Name() != "go.mod" {
			return nil
		}
		rel, rerr := filepath.Rel(repo, filepath.Dir(path))
		if rerr != nil || rel == "." {
			return nil
		}
		mods = append(mods, rel)
		return nil
	})
	sort.Strings(mods[1:])
	return mods
}
