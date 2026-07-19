// Command fuzzratchet fails when a package that parses external input or
// holds concurrent state lacks a fuzz target, and when the total number of
// fuzz targets drops below the recorded baseline.
//
// A package is considered *parsing* when it exports a function whose name
// starts with Parse, Decode, Unmarshal, Validate or Open and that takes a
// string, []byte or *url.URL parameter. It is considered *stateful* when it
// declares a struct with a sync.Mutex, sync.RWMutex or atomic field that has
// at least one exported method. Packages listed under required: in the
// configuration are always checked; packages listed under allow: are exempt
// with a reason and an issue reference.
//
// the test suite makes the exemption conditional -- "without allow-listed
// justification" -- so an allow entry is itself checked: it needs a package, a
// reason, and an issue that looks like a reference a reader can follow (#123,
// owner/repo#123, a URL, an ADR, or an ADR-017 assumption). Every entry is
// checked whether or not its package needs a target, because the check used to
// sit behind the "this package has no Fuzz target" arm and an entry naming a
// package that has one was accepted with no issue at all.
package main

import (
	"flag"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"

	"bitbucket.org/iqhive/iqmq/internal/yamlmini"
)

type config struct {
	baseline   int
	required   map[string]bool
	allow      map[string]allowEntry
	reductions []reduction
	raw        string
}

// reduction is a declared, justified DROP in the baseline.
//
// A ratchet that can only rise cannot express a deliberate deletion, so a
// repository that deletes code has two options and both are bad: keep the dead
// code for ever, or edit the number down with nothing to say why. Increment
// C02 is the live case -- `x/apibench` is 183 files of prototypes of a frozen
// API and holds twenty fuzz targets over shapes that no longer exist -- and
// this is the third option.
//
// A reduction is an audit trail, and it is CHECKED rather than believed: it
// needs a from and a to with from > to, a reason, an issue a reader can
// follow, and the paths whose removal justifies it -- every one of those paths
// must be ABSENT from the tree, because a reduction whose subject is still
// there is either wrong or has been reverted, and the count it lands on may
// not be higher than the targets the tree actually holds.
type reduction struct {
	from, to       int
	hasFrom, hasTo bool
	removed        []string
	reason         string
	issue          string
}

type allowEntry struct {
	reason string
	issue  string
}

// issueRef reports whether s names something a reader can follow: a bare or
// qualified GitHub issue, a URL, an ADR, or an ADR-017 assumption.
func issueRef(s string) bool {
	s = strings.TrimSpace(s)
	if s == "" {
		return false
	}
	if strings.HasPrefix(s, "http://") || strings.HasPrefix(s, "https://") {
		return len(s) > len("https://")
	}
	if i := strings.IndexByte(s, '#'); i >= 0 && digits(s[i+1:]) {
		return true
	}
	if adrRef.MatchString(s) || assumptionRef.MatchString(s) {
		return true
	}
	return false
}

var (
	adrRef        = regexp.MustCompile(`\bADR-[0-9]{3}\b`)
	assumptionRef = regexp.MustCompile(`\bassumption [A-D][0-9]+[a-z]?\.[0-9]+\b`)
)

// digits reports whether s is one or more decimal digits, possibly followed by
// prose ("#42 the parser is being replaced").
func digits(s string) bool {
	n := 0
	for n < len(s) && s[n] >= '0' && s[n] <= '9' {
		n++
	}
	return n > 0
}

type pkgInfo struct {
	rel       string
	name      string
	parsing   []string
	stateful  []string
	fuzzCount int
}

func main() {
	root := flag.String("root", ".", "repository root")
	cfgPath := flag.String("config", "hack/fuzz-ratchet.yaml", "configuration file")
	update := flag.Bool("update", false, "rewrite baseline_count with the current total")
	flag.Parse()
	if err := run(*root, *cfgPath, *update); err != nil {
		fmt.Fprintln(os.Stderr, "fuzzratchet:", err)
		os.Exit(1)
	}
}

func run(root, cfgPath string, update bool) error {
	cfg, err := loadConfig(filepath.Join(root, cfgPath))
	if err != nil {
		return err
	}
	pkgs, err := scan(root)
	if err != nil {
		return err
	}
	total := 0
	var problems []string
	// Every allow entry is checked before anything is scanned: an entry is a
	// standing exemption from a §11.3 MUST and has to carry its justification
	// whatever the package it names turns out to look like.
	for _, rel := range sortedKeys(cfg.allow) {
		a := cfg.allow[rel]
		switch {
		case rel == "":
			problems = append(problems, "allow entry without a package")
		case a.reason == "" && a.issue == "":
			problems = append(problems, fmt.Sprintf("allow entry %s needs a reason and an issue reference", rel))
		case a.reason == "":
			problems = append(problems, fmt.Sprintf("allow entry %s needs a reason", rel))
		case !issueRef(a.issue):
			problems = append(problems, fmt.Sprintf("allow entry %s needs an issue reference, not %q "+
				"(#123, owner/repo#123, a URL, ADR-0NN or \"assumption A19.4\")", rel, a.issue))
		}
	}
	seen := map[string]bool{}
	exempted := map[string]bool{}
	for _, p := range pkgs {
		seen[p.rel] = true
		total += p.fuzzCount
		reasons := append([]string{}, p.parsing...)
		for _, s := range p.stateful {
			reasons = append(reasons, "stateful "+s)
		}
		if cfg.required[p.rel] {
			reasons = append(reasons, "required by configuration")
		}
		if len(reasons) == 0 || p.fuzzCount > 0 {
			continue
		}
		if _, ok := cfg.allow[p.rel]; ok {
			exempted[p.rel] = true
			continue
		}
		problems = append(problems, fmt.Sprintf("%s: no Fuzz target (%s)", p.rel, strings.Join(reasons, "; ")))
	}
	for _, rel := range sortedKeys(cfg.allow) {
		switch {
		case rel == "":
			// Already reported above; without this arm it is reported twice.
		case !seen[rel]:
			problems = append(problems, fmt.Sprintf("allow entry %s does not match any package", rel))
		case !exempted[rel]:
			// The entry is no longer doing anything: the package either grew a
			// fuzz target or stopped being one the ratchet asks about. Saying
			// so is what keeps the allow list from accumulating standing
			// exemptions nobody has re-read.
			problems = append(problems, fmt.Sprintf("allow entry %s is stale: the package needs no exemption", rel))
		}
	}
	for rel := range cfg.required {
		if !seen[rel] {
			problems = append(problems, fmt.Sprintf("required entry %s does not match any package", rel))
		}
	}
	// Every declared reduction is checked before the baseline is compared,
	// so that a malformed one is a refusal rather than a silent licence.
	lastTo, haveFloor := 0, false
	for i, r := range cfg.reductions {
		wellFormed := false
		switch {
		case !r.hasFrom || !r.hasTo || r.from < 0 || r.to < 0:
			problems = append(problems, fmt.Sprintf("reduction %d needs both a from and a to count", i+1))
		case r.from <= r.to:
			problems = append(problems, fmt.Sprintf("reduction %d goes from %d to %d, which is not a reduction; "+
				"the baseline rises by `make ratchet-update`", i+1, r.from, r.to))
		case len(r.removed) == 0:
			problems = append(problems, fmt.Sprintf("reduction %d (%d -> %d) names no removed path; "+
				"a baseline only falls because something specific left the tree", i+1, r.from, r.to))
		case r.reason == "":
			problems = append(problems, fmt.Sprintf("reduction %d (%d -> %d) needs a reason", i+1, r.from, r.to))
		case !issueRef(r.issue):
			problems = append(problems, fmt.Sprintf("reduction %d (%d -> %d) needs an issue reference, not %q "+
				"(#123, owner/repo#123, a URL, ADR-0NN or \"assumption A19.4\")", i+1, r.from, r.to, r.issue))
		default:
			wellFormed = true
		}
		for _, rel := range r.removed {
			if rel == "" {
				continue
			}
			if _, err := os.Stat(filepath.Join(root, filepath.FromSlash(rel))); err == nil {
				problems = append(problems, fmt.Sprintf("reduction %d (%d -> %d) is justified by the removal of %s, "+
					"which is still in the tree", i+1, r.from, r.to, rel))
			}
		}
		// Only a well-formed reduction sets a floor: a malformed one is
		// already a refusal, and letting it also move the floor buries the
		// message that says what is wrong with it.
		if wellFormed && (!haveFloor || r.to > lastTo) {
			lastTo, haveFloor = r.to, true
		}
	}
	// A reduction records a count, so it may not overstate one: a `to` above
	// the targets that actually exist is a record of a deletion that left
	// more behind than the tree has.
	if haveFloor && lastTo > total {
		problems = append(problems, fmt.Sprintf("the highest declared reduction lands at %d target(s) and the tree has %d",
			lastTo, total))
	}
	if total < cfg.baseline {
		problems = append(problems, fmt.Sprintf("fuzz target count %d is below baseline %d", total, cfg.baseline))
	}
	if update && total != cfg.baseline {
		if err := writeBaseline(filepath.Join(root, cfgPath), cfg.raw, total); err != nil {
			return err
		}
		fmt.Fprintf(os.Stderr, "fuzzratchet: baseline_count updated %d -> %d\n", cfg.baseline, total)
	}
	if len(problems) > 0 {
		sort.Strings(problems)
		for _, p := range problems {
			fmt.Fprintln(os.Stderr, " -", p)
		}
		return fmt.Errorf("%d problem(s); %d fuzz target(s) in %d package(s)", len(problems), total, len(pkgs))
	}
	fmt.Fprintf(os.Stderr, "fuzzratchet: ok, %d fuzz target(s) in %d package(s) (baseline %d)\n", total, len(pkgs), cfg.baseline)
	return nil
}

func loadConfig(path string) (*config, error) {
	cfg := &config{required: map[string]bool{}, allow: map[string]allowEntry{}}
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return cfg, nil
		}
		return nil, err
	}
	cfg.raw = string(data)
	v, err := yamlmini.Parse(data)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	m, _ := v.(*yamlmini.Map)
	if m == nil {
		return cfg, nil
	}
	cfg.baseline, _ = yamlmini.Int(m.Values["baseline_count"])
	for _, r := range yamlmini.Maps(m.Values["reductions"]) {
		// A `to` of zero is a legitimate floor -- a whole tree can leave --
		// so presence is read from the parse and not from the value.
		from, hasFrom := yamlmini.Int(r.Values["from"])
		to, hasTo := yamlmini.Int(r.Values["to"])
		red := reduction{
			from:    from,
			to:      to,
			hasFrom: hasFrom,
			hasTo:   hasTo,
			reason:  yamlmini.String(r.Values["reason"]),
			issue:   yamlmini.String(r.Values["issue"]),
			removed: yamlmini.Strings(r.Values["removed"]),
		}
		cfg.reductions = append(cfg.reductions, red)
	}
	for _, r := range yamlmini.Strings(m.Values["required"]) {
		cfg.required[filepath.ToSlash(r)] = true
	}
	for _, a := range yamlmini.Maps(m.Values["allow"]) {
		cfg.allow[yamlmini.String(a.Values["package"])] = allowEntry{
			reason: yamlmini.String(a.Values["reason"]),
			issue:  yamlmini.String(a.Values["issue"]),
		}
	}
	return cfg, nil
}

func writeBaseline(path, raw string, total int) error {
	lines := strings.Split(raw, "\n")
	found := false
	for i, l := range lines {
		if strings.HasPrefix(strings.TrimSpace(l), "baseline_count:") {
			lines[i] = fmt.Sprintf("baseline_count: %d", total)
			found = true
		}
	}
	if !found {
		lines = append([]string{fmt.Sprintf("baseline_count: %d", total)}, lines...)
	}
	return os.WriteFile(path, []byte(strings.Join(lines, "\n")), 0o644)
}

// sortedKeys returns the map's keys in order, so a refusal reads the same way
// on every run.
func sortedKeys[V any](m map[string]V) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

var skipDirs = map[string]bool{".git": true, "testdata": true, "node_modules": true, "vendor": true, "states": true}

func scan(root string) ([]pkgInfo, error) {
	var pkgs []pkgInfo
	fset := token.NewFileSet()
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() {
			return nil
		}
		name := d.Name()
		if path != root && (skipDirs[name] || strings.HasPrefix(name, ".") || strings.HasPrefix(name, "_")) {
			return filepath.SkipDir
		}
		info, ok, err := scanDir(fset, path)
		if err != nil {
			return err
		}
		if ok {
			rel, _ := filepath.Rel(root, path)
			info.rel = filepath.ToSlash(rel)
			pkgs = append(pkgs, info)
		}
		return nil
	})
	sort.Slice(pkgs, func(i, j int) bool { return pkgs[i].rel < pkgs[j].rel })
	return pkgs, err
}

func scanDir(fset *token.FileSet, dir string) (pkgInfo, bool, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return pkgInfo{}, false, err
	}
	var info pkgInfo
	hasGo := false
	methods := map[string]bool{} // receiver type -> has exported method
	stateTypes := map[string]bool{}
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".go") {
			continue
		}
		hasGo = true
		f, err := parser.ParseFile(fset, filepath.Join(dir, e.Name()), nil, parser.SkipObjectResolution)
		if err != nil {
			return pkgInfo{}, false, err
		}
		isTest := strings.HasSuffix(e.Name(), "_test.go")
		if !isTest {
			info.name = f.Name.Name
		}
		for _, decl := range f.Decls {
			switch d := decl.(type) {
			case *ast.FuncDecl:
				if isTest {
					if d.Recv == nil && strings.HasPrefix(d.Name.Name, "Fuzz") && d.Name.Name != "Fuzz" {
						info.fuzzCount++
					}
					continue
				}
				if d.Recv != nil {
					if d.Name.IsExported() {
						methods[receiverName(d.Recv)] = true
					}
					continue
				}
				if d.Name.IsExported() && isParsingName(d.Name.Name) && takesInput(d.Type) {
					info.parsing = append(info.parsing, "exported "+d.Name.Name)
				}
			case *ast.GenDecl:
				if isTest {
					continue
				}
				for _, spec := range d.Specs {
					ts, ok := spec.(*ast.TypeSpec)
					if !ok {
						continue
					}
					st, ok := ts.Type.(*ast.StructType)
					if !ok {
						continue
					}
					if hasStateField(st) {
						stateTypes[ts.Name.Name] = true
					}
				}
			}
		}
	}
	if !hasGo {
		return pkgInfo{}, false, nil
	}
	for t := range stateTypes {
		if methods[t] {
			info.stateful = append(info.stateful, t)
		}
	}
	sort.Strings(info.stateful)
	sort.Strings(info.parsing)
	return info, true, nil
}

func receiverName(recv *ast.FieldList) string {
	if recv == nil || len(recv.List) == 0 {
		return ""
	}
	t := recv.List[0].Type
	if star, ok := t.(*ast.StarExpr); ok {
		t = star.X
	}
	if idx, ok := t.(*ast.IndexExpr); ok {
		t = idx.X
	}
	if idx, ok := t.(*ast.IndexListExpr); ok {
		t = idx.X
	}
	if id, ok := t.(*ast.Ident); ok {
		return id.Name
	}
	return ""
}

func isParsingName(name string) bool {
	for _, p := range []string{"Parse", "Decode", "Unmarshal", "Validate", "Open"} {
		if strings.HasPrefix(name, p) {
			return true
		}
	}
	return false
}

func takesInput(ft *ast.FuncType) bool {
	if ft.Params == nil {
		return false
	}
	for _, p := range ft.Params.List {
		switch t := p.Type.(type) {
		case *ast.Ident:
			if t.Name == "string" {
				return true
			}
		case *ast.ArrayType:
			if id, ok := t.Elt.(*ast.Ident); ok && id.Name == "byte" && t.Len == nil {
				return true
			}
		case *ast.StarExpr:
			if sel, ok := t.X.(*ast.SelectorExpr); ok && sel.Sel.Name == "URL" {
				return true
			}
		case *ast.SelectorExpr:
			if t.Sel.Name == "URL" {
				return true
			}
		}
	}
	return false
}

func hasStateField(st *ast.StructType) bool {
	if st.Fields == nil {
		return false
	}
	for _, f := range st.Fields.List {
		t := f.Type
		if star, ok := t.(*ast.StarExpr); ok {
			t = star.X
		}
		sel, ok := t.(*ast.SelectorExpr)
		if !ok {
			continue
		}
		pkg, ok := sel.X.(*ast.Ident)
		if !ok {
			continue
		}
		switch {
		case pkg.Name == "sync" && (sel.Sel.Name == "Mutex" || sel.Sel.Name == "RWMutex"):
			return true
		case pkg.Name == "atomic":
			return true
		}
	}
	return false
}
