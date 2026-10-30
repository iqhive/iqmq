// Command exportdoc checks exported declarations against
// hack/api/exportdoc.policy and examples against hack/api/workflows.txt.
// It checks comment coverage, restatement limits and runnable examples.
package main

import (
	"flag"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"unicode"
)

// mode is what the policy asks of one module.
type mode int

const (
	modeRequire mode = iota // every exported declaration must have a doc comment
	modeReport              // counted and printed, never a failure
	modeSkip                // not walked at all; the reason is printed
)

// String names the mode the way the policy file spells it.
func (m mode) String() string {
	switch m {
	case modeRequire:
		return "require"
	case modeReport:
		return "report"
	default:
		return "skip"
	}
}

// policyEntry is one module line of the policy.
type policyEntry struct {
	module string
	mode   mode
	reason string
	// restateMax is the ceiling, in per-cent of documented declarations, for
	// comments that only restate the identifier's name. -1 means unset.
	restateMax int
	// fieldsMin is the floor, in per-cent, for exported struct fields and
	// interface methods that carry a comment. -1 means unset. revive's
	// `exported` rule does not look at fields at all, so this is the half of
	// the coverage question `make lint` cannot answer.
	fieldsMin int
}

// exampleEntry is one §13 workflow line of the policy: the workflow's own
// words, the examples that cover it, and a note.
type exampleEntry struct {
	workflow string
	// refs are "package:ExampleName" pairs, or the single token "perdriver",
	// which asks for one example in every package that exports Open.
	refs []string
	note string
}

// finding is one exported declaration the walk has something to say about.
type finding struct {
	pos   string
	pkg   string
	kind  string
	name  string
	why   string // "undocumented" or "restates"
	first string // the comment's first sentence, for a restatement
}

// counts is one module's tally.
type counts struct {
	decls      int
	documented int
	restates   int
	fields     int
	fieldsDoc  int
}

func main() {
	var (
		root     = flag.String("root", ".", "repository root")
		policy   = flag.String("policy", "hack/api/exportdoc.policy", "policy file, relative to -root")
		listAll  = flag.Bool("list-restatements", false, "print every comment counted as a restatement")
		listFlds = flag.Bool("list-fields", false, "print every exported struct field and interface method with no comment")
		maxPrint = flag.Int("max-print", 40, "how many findings of each kind to print")
	)
	flag.Parse()

	abs, err := filepath.Abs(*root)
	if err != nil {
		fatal(err)
	}
	mods, exs, err := readPolicy(policyPath(abs, *policy))
	if err != nil {
		fatal(err)
	}
	if err := run(os.Stdout, os.Stderr, abs, mods, exs, *listAll, *listFlds, *maxPrint); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, "exportdoc:", err)
	os.Exit(2)
}

// readPolicy parses the policy file: "module<TAB>mode[:max%]<TAB>reason" lines
// and "example<TAB>workflow<TAB>refs<TAB>note" lines.
func readPolicy(path string) ([]policyEntry, []exampleEntry, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, nil, err
	}
	var (
		mods []policyEntry
		exs  []exampleEntry
	)
	seen := map[string]bool{}
	seenWF := map[string]bool{}
	for i, line := range strings.Split(string(b), "\n") {
		line = strings.TrimRight(line, " \t\r")
		if strings.TrimSpace(line) == "" || strings.HasPrefix(strings.TrimSpace(line), "#") {
			continue
		}
		if fields := strings.Split(line, "\t"); fields[0] == "example" {
			if len(fields) != 4 || strings.TrimSpace(fields[1]) == "" || strings.TrimSpace(fields[3]) == "" {
				return nil, nil, fmt.Errorf("%s:%d: want four tab-separated fields (example, workflow, refs, note)", path, i+1)
			}
			e := exampleEntry{workflow: normalizeWorkflow(fields[1]), note: strings.TrimSpace(fields[3])}
			for _, r := range strings.Split(strings.TrimSpace(fields[2]), ",") {
				if r = strings.TrimSpace(r); r != "" && r != "-" {
					e.refs = append(e.refs, r)
				}
			}
			if seenWF[e.workflow] {
				return nil, nil, fmt.Errorf("%s:%d: workflow %q listed twice", path, i+1, e.workflow)
			}
			seenWF[e.workflow] = true
			exs = append(exs, e)
			continue
		}
		fields := strings.SplitN(line, "\t", 3)
		for j := range fields {
			fields[j] = strings.TrimSpace(fields[j])
		}
		if len(fields) < 3 || fields[0] == "" || fields[2] == "" {
			return nil, nil, fmt.Errorf("%s:%d: want three tab-separated fields (module, mode[:max%%], reason)", path, i+1)
		}
		e := policyEntry{module: fields[0], reason: fields[2], restateMax: -1, fieldsMin: -1}
		parts := strings.Split(fields[1], ",")
		switch parts[0] {
		case "require":
			e.mode = modeRequire
		case "report":
			e.mode = modeReport
		case "skip":
			e.mode = modeSkip
		default:
			return nil, nil, fmt.Errorf("%s:%d: unknown mode %q (want require, report or skip)", path, i+1, parts[0])
		}
		for _, opt := range parts[1:] {
			k, v, ok := strings.Cut(strings.TrimSpace(opt), "=")
			n, err := strconv.Atoi(strings.TrimSuffix(v, "%"))
			if !ok || err != nil || n < 0 || n > 100 {
				return nil, nil, fmt.Errorf("%s:%d: %q is not key=<0-100> (want restate-max= or fields-min=)", path, i+1, opt)
			}
			switch k {
			case "restate-max":
				e.restateMax = n
			case "fields-min":
				e.fieldsMin = n
			default:
				return nil, nil, fmt.Errorf("%s:%d: unknown option %q (want restate-max= or fields-min=)", path, i+1, k)
			}
		}
		if seen[e.module] {
			return nil, nil, fmt.Errorf("%s:%d: module %q listed twice", path, i+1, e.module)
		}
		seen[e.module] = true
		mods = append(mods, e)
	}
	if len(mods) == 0 {
		return nil, nil, fmt.Errorf("%s: no module entries", path)
	}
	return mods, exs, nil
}

// report is a writer with the error dropped, deliberately: a broken pipe on a
// progress line is not a reason to fail an audit, and errcheck is right to ask
// the question rather than let 30 unchecked Fprintf calls stand.
type report struct{ w io.Writer }

func (r report) printf(format string, a ...any) { _, _ = fmt.Fprintf(r.w, format, a...) }

func (r report) println(a ...any) { _, _ = fmt.Fprintln(r.w, a...) }

func run(w, errw io.Writer, root string, entries []policyEntry, exs []exampleEntry, listAll, listFields bool, maxPrint int) error {
	out, errs := report{w}, report{errw}
	// Every module the repository has must be in the policy, and every entry
	// must name a module that exists: a module nobody listed would be a
	// module this check silently never reads.
	have, err := modules(root)
	if err != nil {
		return err
	}
	byModule := map[string]policyEntry{}
	for _, e := range entries {
		byModule[e.module] = e
	}
	var problems []string
	for _, m := range have {
		if _, ok := byModule[m]; !ok {
			problems = append(problems, fmt.Sprintf("module %s is not in the policy: add it with a mode and a reason", m))
		}
	}
	for _, e := range entries {
		if !contains(have, e.module) {
			problems = append(problems, fmt.Sprintf("the policy names %s, which is not a module of this repository", e.module))
		}
	}

	total := counts{}
	var undoc, restated, undocFields []finding
	// floorFields are the undocumented fields of the modules whose
	// fields-min floor is BREACHED, kept apart from undocFields so that the
	// failure can name them. The percentage on its own sends a reader to
	// `make exported-doc STRICT=1` to find out which field it meant, and a
	// gate that does not say what to fix is a gate people learn to re-run
	// rather than read.
	var floorFields []finding
	var skipped []policyEntry
	out.println("==> exportdoc: exported declarations per module")
	for _, e := range entries {
		if e.mode == modeSkip {
			skipped = append(skipped, e)
			continue
		}
		c, fs, err := walkModule(root, e.module)
		if err != nil {
			return err
		}
		pct, rpct, fpct := coverage(c.documented, c.decls), share(c.restates, c.documented), coverage(c.fieldsDoc, c.fields)
		out.printf("    %-30s %-8s %4d decls  %6.1f%% documented  %4d restate-name (%4.1f%%)  %4d/%-4d fields (%5.1f%%)\n",
			e.module, e.mode, c.decls, pct, c.restates, rpct, c.fieldsDoc, c.fields, fpct)
		total.decls += c.decls
		total.documented += c.documented
		total.restates += c.restates
		total.fields += c.fields
		total.fieldsDoc += c.fieldsDoc
		var bare []finding
		for _, f := range fs {
			switch f.why {
			case "undocumented":
				if e.mode == modeRequire {
					undoc = append(undoc, f)
				}
			case "restates":
				restated = append(restated, f)
			case "undocumented-field":
				undocFields = append(undocFields, f)
				bare = append(bare, f)
			}
		}
		if e.restateMax >= 0 && c.restates > 0 && rpct > float64(e.restateMax) {
			problems = append(problems, fmt.Sprintf("%s: %.1f%% of documented declarations only restate the identifier's name, over the %d%% the policy allows",
				e.module, rpct, e.restateMax))
		}
		// The floor is checked whatever the mode, `report` included: a
		// report-mode module is one whose DECLARATION coverage is not a
		// failure, and that says nothing about a floor the policy went
		// on to name. `conformance` is the module that depends on this.
		if e.fieldsMin >= 0 && fpct < float64(e.fieldsMin) {
			problems = append(problems, fmt.Sprintf("%s: %d of %d exported fields and interface methods carry a comment (%.1f%%), under the %d%% the policy requires",
				e.module, c.fieldsDoc, c.fields, fpct, e.fieldsMin))
			floorFields = append(floorFields, bare...)
		}
	}

	if len(skipped) > 0 {
		out.println("==> exportdoc: NOT CHECKED, and why:")
		for _, e := range skipped {
			out.printf("      %-30s %s\n", e.module, e.reason)
		}
	}
	out.printf("==> exportdoc: %d exported declarations, %d documented (%.1f%%), %d of those (%.1f%%) only restate the identifier's name\n",
		total.decls, total.documented, coverage(total.documented, total.decls), total.restates, share(total.restates, total.documented))
	out.printf("==> exportdoc: %d of %d exported struct fields and interface methods carry a comment (%.1f%%)\n",
		total.fieldsDoc, total.fields, coverage(total.fieldsDoc, total.fields))

	if listAll {
		sort.Slice(restated, func(i, j int) bool { return restated[i].pos < restated[j].pos })
		for _, f := range restated {
			out.printf("    restates: %s %s %s: %s\n", f.pos, f.kind, f.name, f.first)
		}
	}

	if listFields {
		sort.Slice(undocFields, func(i, j int) bool { return undocFields[i].pos < undocFields[j].pos })
		for _, f := range undocFields {
			out.printf("    undocumented field: %s %s\n", f.pos, f.name)
		}
	}

	problems = append(problems, checkExamples(out, root, exs)...)

	if len(floorFields) > 0 {
		sort.Slice(floorFields, func(i, j int) bool { return floorFields[i].pos < floorFields[j].pos })
		errs.println("!! exportdoc: exported fields and interface methods with no comment, in a module under its fields-min floor:")
		for i, f := range floorFields {
			if i >= maxPrint {
				errs.printf("   ... and %d more\n", len(floorFields)-maxPrint)
				break
			}
			errs.printf("   %s: field %s\n", f.pos, f.name)
		}
	}
	if len(undoc) > 0 {
		sort.Slice(undoc, func(i, j int) bool { return undoc[i].pos < undoc[j].pos })
		errs.println("!! exportdoc: exported declarations with no doc comment:")
		for i, f := range undoc {
			if i >= maxPrint {
				errs.printf("   ... and %d more\n", len(undoc)-maxPrint)
				break
			}
			errs.printf("   %s: %s %s\n", f.pos, f.kind, f.name)
		}
		problems = append(problems, fmt.Sprintf("%d exported declarations have no doc comment", len(undoc)))
	}
	if len(problems) > 0 {
		for _, p := range problems {
			errs.println("!! exportdoc:", p)
		}
		return fmt.Errorf("exportdoc: %d problem(s)", len(problems))
	}
	return nil
}

// share is n as a per-cent of of, and 0 for an empty denominator: 0 of 0
// restatements is 0%, not 100%.
func share(n, of int) float64 {
	if of == 0 {
		return 0
	}
	return float64(n) * 100 / float64(of)
}

// coverage is share with the other empty-denominator convention: a module
// with no exported declaration at all is fully covered.
func coverage(n, of int) float64 {
	if of == 0 {
		return 100
	}
	return share(n, of)
}

func contains(ss []string, s string) bool {
	for _, x := range ss {
		if x == s {
			return true
		}
	}
	return false
}

// modules lists every module of the repository, the way hack/modules.sh does:
// every directory holding a go.mod, relative to the root, "." for the root.
// Like hack/modules.sh it skips .claude, where an agent's worktree is a whole
// second checkout and not a module of this one.
func modules(root string) ([]string, error) {
	var out []string
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			switch d.Name() {
			case ".git", ".claude", "testdata", "node_modules", "states":
				return fs.SkipDir
			}
			return nil
		}
		if d.Name() != "go.mod" {
			return nil
		}
		rel, err := filepath.Rel(root, filepath.Dir(path))
		if err != nil {
			return err
		}
		out = append(out, filepath.ToSlash(rel))
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Strings(out)
	return out, nil
}

// walkModule reads every package of one module that is part of its exported
// surface: no internal package (not importable from outside), no test file,
// no testdata, and no package main (its identifiers are not importable at
// all, so a doc comment there is for the reader of the command and not for a
// caller).
func walkModule(root, module string) (counts, []finding, error) {
	dir := root
	if module != "." {
		dir = filepath.Join(root, module)
	}
	var (
		c   counts
		out []finding
	)
	err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() {
			return nil
		}
		name := d.Name()
		if path != dir {
			if name == "testdata" || name == "internal" || strings.HasPrefix(name, ".") {
				return fs.SkipDir
			}
			// A nested module is its own policy entry.
			if _, err := os.Stat(filepath.Join(path, "go.mod")); err == nil {
				return fs.SkipDir
			}
		}
		pc, pf, err := readPackage(root, path)
		if err != nil {
			return err
		}
		out = append(out, pf...)
		c.decls += pc.decls
		c.documented += pc.documented
		c.restates += pc.restates
		c.fields += pc.fields
		c.fieldsDoc += pc.fieldsDoc
		return nil
	})
	return c, out, err
}

func readPackage(root, dir string) (counts, []finding, error) {
	var c counts
	fset := token.NewFileSet()
	ents, err := os.ReadDir(dir)
	if err != nil {
		return c, nil, err
	}
	var files []*ast.File
	pkgName := ""
	for _, e := range ents {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".go") || strings.HasSuffix(e.Name(), "_test.go") {
			continue
		}
		f, err := parser.ParseFile(fset, filepath.Join(dir, e.Name()), nil, parser.ParseComments|parser.SkipObjectResolution)
		if err != nil {
			return c, nil, fmt.Errorf("exportdoc: %w", err)
		}
		files = append(files, f)
		pkgName = f.Name.Name
	}
	if len(files) == 0 || pkgName == "main" {
		return c, nil, nil
	}
	rel, err := filepath.Rel(root, dir)
	if err != nil {
		return c, nil, err
	}
	rel = filepath.ToSlash(rel)

	var out []finding
	record := func(node ast.Node, kind, name string, doc *ast.CommentGroup) {
		c.decls++
		pos := fset.Position(node.Pos())
		where := fmt.Sprintf("%s/%s:%d", rel, filepath.Base(pos.Filename), pos.Line)
		if doc == nil || strings.TrimSpace(doc.Text()) == "" {
			out = append(out, finding{pos: where, pkg: rel, kind: kind, name: name, why: "undocumented"})
			return
		}
		c.documented++
		if first, ok := restatesName(name, doc.Text()); ok {
			c.restates++
			out = append(out, finding{pos: where, pkg: rel, kind: kind, name: name, why: "restates", first: first})
		}
	}
	countField := func(owner string, n *ast.Ident, documented bool) {
		c.fields++
		if documented {
			c.fieldsDoc++
			return
		}
		pos := fset.Position(n.Pos())
		out = append(out, finding{
			pos:  fmt.Sprintf("%s/%s:%d", rel, filepath.Base(pos.Filename), pos.Line),
			pkg:  rel,
			kind: "field",
			name: owner + "." + n.Name,
			why:  "undocumented-field",
		})
	}

	for _, f := range files {
		for _, decl := range f.Decls {
			switch d := decl.(type) {
			case *ast.FuncDecl:
				if !d.Name.IsExported() {
					continue
				}
				if d.Recv != nil {
					recv := receiverName(d.Recv)
					if !ast.IsExported(recv) {
						continue
					}
					record(d, "method", recv+"."+d.Name.Name, d.Doc)
					continue
				}
				record(d, "func", d.Name.Name, d.Doc)
			case *ast.GenDecl:
				for _, spec := range d.Specs {
					switch s := spec.(type) {
					case *ast.TypeSpec:
						if !s.Name.IsExported() {
							continue
						}
						doc := s.Doc
						if doc == nil {
							doc = d.Doc
						}
						record(s, "type", s.Name.Name, doc)
						fieldsOf(s, doc, countField)
					case *ast.ValueSpec:
						kind := "var"
						if d.Tok == token.CONST {
							kind = "const"
						}
						for _, n := range s.Names {
							if !n.IsExported() {
								continue
							}
							doc := s.Doc
							if doc == nil {
								doc = d.Doc
							}
							if doc == nil {
								doc = s.Comment
							}
							record(n, kind, n.Name, doc)
						}
					}
				}
			}
		}
	}
	return c, out, nil
}

// fieldsOf reports every exported struct field and interface method of one
// type, and says for each whether it is documented.
//
// A field with no comment of its own is documented if the nearest preceding
// comment in the same type, or the type's own doc comment, NAMES it. That is
// the Go convention for a group -- "Start and End bracket the operation on the
// logical clock" documents both, and "RecordHeader is ... a string key and a
// []byte value" documents Key and Value -- and reading each field in isolation
// called six documented fields of this repository's own root module
// undocumented before the rule was added.
func fieldsOf(s *ast.TypeSpec, typeDoc *ast.CommentGroup, count func(owner string, n *ast.Ident, documented bool)) {
	typeText := commentText(typeDoc)
	carried := typeText
	step := func(doc, comment *ast.CommentGroup, names []*ast.Ident) {
		own := commentText(doc) + " " + commentText(comment)
		if strings.TrimSpace(own) != "" {
			carried = own
		}
		for _, n := range names {
			if !n.IsExported() {
				continue
			}
			// The type's own doc comment counts wherever the field sits: a
			// type that documents its fields in a table at the top (see
			// nsqfake.Stats) documents the field that comes after another
			// field's comment too.
			documented := strings.TrimSpace(own) != "" || namesIdent(carried, n.Name) || namesIdent(typeText, n.Name)
			count(s.Name.Name, n, documented)
		}
	}
	switch t := s.Type.(type) {
	case *ast.StructType:
		for _, f := range t.Fields.List {
			if len(f.Names) == 0 { // embedded
				continue
			}
			step(f.Doc, f.Comment, f.Names)
		}
	case *ast.InterfaceType:
		for _, m := range t.Methods.List {
			step(m.Doc, m.Comment, m.Names)
		}
	}
}

func commentText(g *ast.CommentGroup) string {
	if g == nil {
		return ""
	}
	return g.Text()
}

// namesIdent says whether a comment names an identifier: one of the comment's
// own tokens is the identifier, compared case-insensitively and WITHOUT
// camelCase splitting. "Start and End bracket ..." names End, and
// "RecordHeader is ... a string key and a []byte value" names Key and Value,
// which is how prose documents a pair of fields. Matching the split word
// sequence instead let "MaxBatchBytes bounds one batch" count as documenting a
// sibling field called MaxBatch, so the comparison is whole-token.
func namesIdent(comment, ident string) bool {
	if ident == "" {
		return false
	}
	want := strings.ToLower(ident)
	for _, tok := range strings.FieldsFunc(comment, func(r rune) bool {
		return !unicode.IsLetter(r) && !unicode.IsDigit(r)
	}) {
		if strings.EqualFold(tok, want) {
			return true
		}
	}
	return false
}

func receiverName(recv *ast.FieldList) string {
	if recv == nil || len(recv.List) == 0 {
		return ""
	}
	t := recv.List[0].Type
	for {
		switch x := t.(type) {
		case *ast.StarExpr:
			t = x.X
		case *ast.IndexExpr: // generic receiver
			t = x.X
		case *ast.IndexListExpr:
			t = x.X
		case *ast.Ident:
			return x.Name
		default:
			return ""
		}
	}
}

// policyPath resolves the policy flag against the root, leaving an absolute
// path alone so a scratch policy outside the tree can be measured with.
func policyPath(root, p string) string {
	if filepath.IsAbs(p) {
		return p
	}
	return filepath.Join(root, p)
}
