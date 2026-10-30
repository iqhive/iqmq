// Command modelcheck cross-checks the TLA+ specifications against the Go
// reference models.
//
// models/tla/TRACE.md carries a table mapping each specification to the Go
// model package that mirrors it. For every spec with a .cfg file, each
// invariant and property named in the .cfg must
//
//  1. be defined by the .tla module, as a top-level operator; and
//  2. be documented, by name, in the doc comment of the Go model package's
//     Invariants method,
//
// and the model must not document an invariant that its own module defines
// but the configuration does not check. So each machine-checked property is
// claimed by name in the Go model, each name the Go model claims of that
// module is machine-checked, and neither list can drift from the other
// without this failing. A documented name the module does not define is a
// Go-side check with no TLA+ counterpart, which is allowed.
//
// What it does not check is that the Go model's Invariants method actually
// evaluates what its doc comment claims: a bullet is a name, and deleting
// the evaluation while keeping the bullet passes here. That is the Go side's
// own business, and internal/model/shutdown answers it with
// TestInvariantsHaveTeeth, which forces a fault for each documented
// invariant and asserts the named one fires. A model whose Invariants has no
// such test is documented-but-possibly-inert as far as this tool can tell.
//
// The doc comment is parsed rather than the package text searched: a name
// that happens to appear in an unrelated comment or identifier elsewhere in
// the package is not a counterpart, and treating it as one is how a check of
// this shape stops being able to fail.
//
// Since C04 it checks four more things, which together are what makes
// models/tla/TRACE.md a cross-reference rather than a claim:
//
//  1. Every .cfg in the directory is checked, not only the five the trace
//     table names. A sibling configuration (FanoutReserveBestEffort,
//     MemoryLeaseRelaxed, ...) adds nothing to its module and is resolved to
//     the base module it EXTENDS, whose Go model it therefore shares. Six of
//     the eleven configurations were cross-checked by nothing before.
//  2. Every configured invariant has a mutation in models/tla/mutations.txt
//     that targets it by name, so an invariant cannot be added to a
//     configuration without a deliberate defect that proves it can fail.
//     `make tla-mutate` is what runs them.
//  3. Every invariant the Go model documents has a case in that package's
//     TestInvariantsHaveTeeth, which is what says the documented bullet is
//     evaluated rather than decoration. This tool can only see that the name
//     appears in the test; the test itself asserts that the invariant of
//     that name fires.
//  4. Every configured invariant has a row in its specification's section of
//     TRACE.md naming a Go model check and an implementation test, and every
//     Test or Fuzz name the trace cites exists in the repository. A trace
//     table that names a test nobody has run since it was renamed is the
//     same defect as an invariant that cannot fail.
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
	"strconv"
	"strings"
)

func main() {
	trace := flag.String("trace", "models/tla/TRACE.md", "trace table")
	mutations := flag.String("mutations", "models/tla/mutations.txt", "mutation manifest")
	root := flag.String("root", ".", "repository root, for the test names the trace cites")
	flag.Parse()
	if err := run(*trace, *mutations, *root); err != nil {
		fmt.Fprintln(os.Stderr, "modelcheck:", err)
		os.Exit(1)
	}
}

var rowRE = regexp.MustCompile(`^\|\s*` + "`?" + `([A-Za-z0-9_]+)` + "`?" + `\s*\|\s*` + "`?" + `([^|` + "`" + `]+)` + "`?" + `\s*\|`)

func run(tracePath, mutationsPath, root string) error {
	data, err := os.ReadFile(tracePath)
	if err != nil {
		return err
	}
	trace := string(data)
	dir := filepath.Dir(tracePath)
	// models[Module] is the Go model package the trace table pairs with the
	// module; every configuration of that module shares it.
	models := map[string]string{}
	for _, line := range strings.Split(trace, "\n") {
		m := rowRE.FindStringSubmatch(strings.TrimSpace(line))
		if m == nil {
			continue
		}
		spec, pkg := m[1], strings.TrimSpace(m[2])
		if spec == "Spec" || strings.HasPrefix(spec, "-") || !strings.Contains(pkg, "/") {
			continue
		}
		models[spec] = pkg
	}
	if len(models) == 0 {
		return fmt.Errorf("%s: the first table pairs no specification with a Go model", tracePath)
	}
	targets, err := mutationTargets(mutationsPath)
	if err != nil {
		return err
	}
	tests, err := testNames(root)
	if err != nil {
		return err
	}
	rows := traceRows(trace)

	cfgs, err := filepath.Glob(filepath.Join(dir, "*.cfg"))
	if err != nil {
		return err
	}
	if len(cfgs) == 0 {
		return fmt.Errorf("%s: no configurations", dir)
	}
	sort.Strings(cfgs)
	var problems []string
	teethChecked := map[string]bool{}
	for _, cfgPath := range cfgs {
		spec := strings.TrimSuffix(filepath.Base(cfgPath), ".cfg")
		cfgData, err := os.ReadFile(cfgPath)
		if err != nil {
			return err
		}
		module, err := baseModule(dir, spec)
		if err != nil {
			problems = append(problems, fmt.Sprintf("%s: %v", spec, err))
			continue
		}
		pkg, ok := models[module]
		if !ok {
			problems = append(problems, fmt.Sprintf(
				"%s: %s.cfg checks module %s, which the trace table in %s pairs with no Go model",
				spec, spec, module, tracePath,
			))
			continue
		}
		problems = append(problems, checkSpec(dir, spec, module, pkg, string(cfgData))...)
		for _, n := range invariants(string(cfgData)) {
			if !targets[n] {
				problems = append(problems, fmt.Sprintf(
					"%s: invariant %s has no mutation in %s, so nothing shows it can fail",
					spec, n, mutationsPath,
				))
			}
			switch r, ok := rows[module][n]; {
			case !ok:
				problems = append(problems, fmt.Sprintf(
					"%s: invariant %s has no row in the %s section of %s", spec, n, module, tracePath,
				))
			case r.model == "" || r.test == "":
				problems = append(problems, fmt.Sprintf(
					"%s: the %s row of %s names no %s", spec, n,
					tracePath, map[bool]string{true: "Go model check", false: "implementation test"}[r.model == ""],
				))
			}
		}
		if !teethChecked[pkg] {
			teethChecked[pkg] = true
			problems = append(problems, checkTeeth(pkg)...)
		}
	}
	for _, name := range citedTests(trace) {
		if !tests[name] {
			problems = append(problems, fmt.Sprintf(
				"%s cites %s, which is not a Test or Fuzz function under %s", tracePath, name, root,
			))
		}
	}
	if len(problems) > 0 {
		sort.Strings(problems)
		for _, p := range problems {
			fmt.Fprintln(os.Stderr, " -", p)
		}
		return fmt.Errorf("%d problem(s)", len(problems))
	}
	fmt.Fprintf(os.Stderr, "modelcheck: %d configuration(s) over %d module(s) cross-checked, %d mutation target(s), %d cited test(s)\n",
		len(cfgs), len(models), len(targets), len(citedTests(trace)))
	return nil
}

// baseModule returns the module a configuration's invariants are defined in:
// the module itself, or the one it EXTENDS when it adds nothing of its own.
// The sibling configurations exist because a .cfg binds one value per
// constant, so four policies need four modules that differ in nothing else.
func baseModule(dir, spec string) (string, error) {
	data, err := os.ReadFile(filepath.Join(dir, spec+".tla"))
	if err != nil {
		return "", err
	}
	text := string(data)
	if len(operators(text)) > 0 {
		return spec, nil
	}
	for _, line := range strings.Split(text, "\n") {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "EXTENDS ") {
			continue
		}
		for _, name := range strings.Split(strings.TrimPrefix(line, "EXTENDS "), ",") {
			name = strings.TrimSpace(name)
			if _, err := os.Stat(filepath.Join(dir, name+".tla")); err == nil {
				return name, nil
			}
		}
	}
	return "", fmt.Errorf("%s.tla defines no operator and extends no module of this directory", spec)
}

// mutationTargets returns the invariant names models/tla/mutations.txt has a
// deliberate defect for.
func mutationTargets(path string) (map[string]bool, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	out := map[string]bool{}
	for _, line := range strings.Split(string(data), "\n") {
		if !strings.HasPrefix(line, "## ") {
			continue
		}
		parts := strings.Split(strings.TrimSpace(strings.TrimPrefix(line, "## ")), "/")
		if len(parts) < 2 {
			return nil, fmt.Errorf("%s: bad mutation header %q", path, line)
		}
		out[parts[1]] = true
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("%s: no mutations", path)
	}
	return out, nil
}

// row is one line of a specification's invariant table.
type row struct{ model, test string }

// sectionRE matches the per-specification headings of the trace table.
var sectionRE = regexp.MustCompile(`^###\s+([A-Za-z0-9_]+)\s*$`)

// invRowRE matches a three-column row whose first cell is a bare name.
var invRowRE = regexp.MustCompile("^\\|\\s*`?([A-Za-z_][A-Za-z0-9_]*)`?[^|]*\\|([^|]*)\\|([^|]*)\\|")

// traceRows returns, per specification section, the invariant rows it holds.
func traceRows(trace string) map[string]map[string]row {
	out := map[string]map[string]row{}
	section := ""
	for _, line := range strings.Split(trace, "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), "#") {
			// Any other heading ends the section, so a table somewhere else
			// in the file cannot pass for a specification's invariant rows
			// and satisfy a row this spec is missing.
			section = ""
			if m := sectionRE.FindStringSubmatch(strings.TrimSpace(line)); m != nil {
				section = m[1]
			}
			continue
		}
		if section == "" {
			continue
		}
		m := invRowRE.FindStringSubmatch(strings.TrimSpace(line))
		if m == nil {
			continue
		}
		if out[section] == nil {
			out[section] = map[string]row{}
		}
		out[section][m[1]] = row{model: strings.TrimSpace(m[2]), test: strings.TrimSpace(m[3])}
	}
	return out
}

// citedTestRE matches a Test or Fuzz function name cited in backticks,
// with any subtest path after it.
var citedTestRE = regexp.MustCompile("`((?:Test|Fuzz)[A-Za-z0-9_]+)[^`]*`")

// citedTests returns the test function names the trace table cites.
func citedTests(trace string) []string {
	seen := map[string]bool{}
	var out []string
	for _, m := range citedTestRE.FindAllStringSubmatch(trace, -1) {
		if !seen[m[1]] {
			seen[m[1]] = true
			out = append(out, m[1])
		}
	}
	sort.Strings(out)
	return out
}

// testNames returns every Test and Fuzz function defined under root, across
// every module: the trace cites the engine's tests, the drivers' and the
// models' own. It does not read .claude, where an agent's worktree is a whole
// second checkout whose copy of a deleted test would satisfy its citation.
func testNames(root string) (map[string]bool, error) {
	out := map[string]bool{}
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			switch d.Name() {
			case ".git", ".claude", "node_modules", "testdata", "states":
				return fs.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(d.Name(), "_test.go") {
			return nil
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		for _, m := range funcRE.FindAllStringSubmatch(string(data), -1) {
			out[m[1]] = true
		}
		return nil
	})
	return out, err
}

// funcRE matches a test or fuzz function definition.
var funcRE = regexp.MustCompile(`(?m)^func ((?:Test|Fuzz)[A-Za-z0-9_]*)\(`)

// checkTeeth reports every invariant the model documents that its
// TestInvariantsHaveTeeth does not name.
func checkTeeth(pkg string) []string {
	doc, err := invariantsDoc(pkg)
	if err != nil {
		return []string{fmt.Sprintf("%s: %v", pkg, err)}
	}
	lits, err := teethLiterals(pkg)
	if err != nil {
		return []string{fmt.Sprintf("%s: %v", pkg, err)}
	}
	var out []string
	names := make([]string, 0, len(doc))
	for n := range bullets(doc) {
		names = append(names, n)
	}
	sort.Strings(names)
	for _, n := range names {
		if !lits[n] {
			out = append(out, fmt.Sprintf(
				"%s: TestInvariantsHaveTeeth has no case for %s, so nothing shows the invariant is evaluated", pkg, n,
			))
		}
	}
	return out
}

// teethLiterals returns the string literals of the package's
// TestInvariantsHaveTeeth function.
func teethLiterals(pkg string) (map[string]bool, error) {
	entries, err := os.ReadDir(pkg)
	if err != nil {
		return nil, err
	}
	fset := token.NewFileSet()
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), "_test.go") {
			continue
		}
		f, err := parser.ParseFile(fset, filepath.Join(pkg, e.Name()), nil, 0)
		if err != nil {
			return nil, err
		}
		for _, d := range f.Decls {
			fn, ok := d.(*ast.FuncDecl)
			if !ok || fn.Name.Name != "TestInvariantsHaveTeeth" || fn.Recv != nil {
				continue
			}
			out := map[string]bool{}
			ast.Inspect(fn, func(n ast.Node) bool {
				if lit, ok := n.(*ast.BasicLit); ok && lit.Kind == token.STRING {
					if s, err := strconv.Unquote(lit.Value); err == nil {
						out[s] = true
					}
				}
				return true
			})
			return out, nil
		}
	}
	return nil, fmt.Errorf("no TestInvariantsHaveTeeth in %s: nothing shows its invariants are evaluated", pkg)
}

// checkSpec reports every mismatch between one specification's configured
// invariants, the operators its module defines and the invariants its Go
// model documents.
func checkSpec(dir, spec, module, pkg, cfg string) []string {
	var problems []string
	names := invariants(cfg)
	if len(names) == 0 {
		return []string{fmt.Sprintf("%s: %s.cfg names no invariant or property", spec, spec)}
	}

	defined := map[string]bool{}
	tla, err := os.ReadFile(filepath.Join(dir, module+".tla"))
	switch {
	case err != nil:
		problems = append(problems, fmt.Sprintf("%s: %v", spec, err))
	default:
		defined = operators(string(tla))
		for _, n := range names {
			if !defined[n] {
				problems = append(problems,
					fmt.Sprintf("%s: %s.cfg names %s, which %s.tla does not define", spec, spec, n, module))
			}
		}
	}

	doc, err := invariantsDoc(pkg)
	if err != nil {
		return append(problems, fmt.Sprintf("%s: %v", spec, err))
	}
	documented := bullets(doc)
	for _, n := range names {
		if !documented[n] {
			problems = append(problems,
				fmt.Sprintf("%s: invariant %s is not documented by %s.(*Model).Invariants", spec, n, pkg))
		}
	}
	// The other direction, scoped to what the module actually defines: a
	// model that documents an invariant its own specification declares but
	// the configuration forgets to check is the drift worth catching. A
	// documented name the module does not define is a Go-side check with no
	// TLA+ counterpart — internal/model/memq documents Isolation, which is
	// models/tla/FanoutReserve's — and is not drift.
	configured := make(map[string]bool, len(names))
	for _, n := range names {
		configured[n] = true
	}
	extra := make([]string, 0, len(documented))
	for n := range documented {
		if !configured[n] && defined[n] {
			extra = append(extra, n)
		}
	}
	sort.Strings(extra)
	for _, n := range extra {
		problems = append(problems,
			fmt.Sprintf("%s: %s.tla defines %s and %s documents it, but %s.cfg does not check it",
				spec, module, n, pkg, spec))
	}
	return problems
}

// invariants extracts the names listed under INVARIANT(S) and PROPERTY(IES)
// in a TLC configuration.
func invariants(cfg string) []string {
	var names []string
	collecting := false
	for _, line := range strings.Split(cfg, "\n") {
		line = strings.TrimSpace(line)
		if i := strings.Index(line, "\\*"); i >= 0 {
			line = strings.TrimSpace(line[:i])
		}
		if line == "" {
			continue
		}
		fields := strings.Fields(line)
		switch strings.ToUpper(fields[0]) {
		case "INVARIANT", "INVARIANTS", "PROPERTY", "PROPERTIES":
			collecting = true
			names = append(names, fields[1:]...)
			continue
		case "SPECIFICATION", "INIT", "NEXT", "CONSTANT", "CONSTANTS", "CONSTRAINT", "CONSTRAINTS", "ACTION_CONSTRAINT", "SYMMETRY", "VIEW", "CHECK_DEADLOCK", "ALIAS", "POSTCONDITION":
			collecting = false
			continue
		}
		if collecting {
			names = append(names, fields...)
		}
	}
	return names
}

// opRE matches a top-level TLA+ operator definition: a name in the first
// column followed by == , with or without parameters.
var opRE = regexp.MustCompile(`(?m)^([A-Za-z_][A-Za-z0-9_]*)\s*(?:\([^)]*\))?\s*==`)

// operators returns the operators a TLA+ module defines at the top level.
func operators(tla string) map[string]bool {
	out := map[string]bool{}
	for _, m := range opRE.FindAllStringSubmatch(tla, -1) {
		out[m[1]] = true
	}
	return out
}

// invariantsDoc returns the doc comment of the Invariants method of the
// package's Model type.
//
// The files are read and parsed one at a time rather than through
// parser.ParseDir, which is deprecated because it ignores build tags. The
// model packages have none, but reading the directory here keeps the tool off
// an API that is going away and needs no more than a per-file parse.
func invariantsDoc(pkg string) (string, error) {
	entries, err := os.ReadDir(pkg)
	if err != nil {
		return "", err
	}
	fset := token.NewFileSet()
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".go") || strings.HasSuffix(e.Name(), "_test.go") {
			continue
		}
		f, err := parser.ParseFile(fset, filepath.Join(pkg, e.Name()), nil, parser.ParseComments)
		if err != nil {
			return "", err
		}
		for _, d := range f.Decls {
			fn, ok := d.(*ast.FuncDecl)
			if !ok || fn.Name.Name != "Invariants" || fn.Recv == nil || fn.Doc == nil {
				continue
			}
			return fn.Doc.Text(), nil
		}
	}
	return "", fmt.Errorf("no documented Invariants method in %s", pkg)
}

// bulletRE matches the "  - Name: prose" bullets the models list their
// invariants as.
var bulletRE = regexp.MustCompile(`(?m)^\s*-\s+([A-Za-z_][A-Za-z0-9_]*):`)

// bullets returns the invariant names a doc comment lists.
func bullets(doc string) map[string]bool {
	out := map[string]bool{}
	for _, m := range bulletRE.FindAllStringSubmatch(doc, -1) {
		out[m[1]] = true
	}
	return out
}
