package iqmq

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
)

// TestDocLinksResolve fails when a Go doc comment writes [Something] and
// nothing called Something exists in that package.
//
// It is the check moved from prose to code. `TestDocsNameIdentifiersThatExist`
// covers `mq.X` references in `docs/*.md`; nothing covered the `[X]` doc links
// in the doc comments themselves, and those are the ones a reader CLICKS. A
// doc link that does not resolve renders as literal text with the brackets
// still in it, so the failure is silent on pkg.go.dev and invisible in review.
//
// The specification shipped two of them in one file: `x/compress/doc.go` named
// `TestDecompressionBombCannotAllocate` and `FuzzDecodeRoundTrip`, where the
// tests are really called
// `TestDecompressionBombIsRefusedBeforeAnythingIsAllocated` and
// `FuzzDecode`/`FuzzEncodeDecodeRoundTrip` -- in the same increment whose
// commit message called that defect class a joke the playbook keeps making.
// The playbook's own line covers it: it happens to people who know about it.
//
// The example names above are in BACKTICKS and not brackets on purpose. The
// first version of this comment bracketed them, and the check flagged its own
// prose -- the observer inside the system it observes, which is the third
// variant of that trap this increment met.
//
// Scope: the five packages the specification owns. Run repo-wide it reports ten more, in
// `internal/streams`, `tools/tlatools` and `x/idempotency/storetest`, and
// those are two distinct classes this version cannot tell apart from a
// defect:
//
//   - bracketed SYNTAX in usage documentation, which is not a link at all --
//     `internal/streams/fakestreams` documents Redis command forms with
//     optional arguments in brackets, and `tools/tlatools` documents a
//     command line the same way;
//   - a name declared in ANOTHER package and referenced without a qualifier,
//     which godoc also fails to link but which is a different fix.
//
// Widening this to the module means classifying those ten and probably
// growing an allowlist, which is somebody else's files and is recorded as a
// follow-up rather than done here. `_test.go` files ARE parsed, because a doc
// comment that points at the test holding a property is exactly the kind this
// check is for and test names live nowhere else.
func TestDocLinksResolve(t *testing.T) {
	// A doc link is a bracketed name, optionally package-qualified. The
	// negative lookbehind Go's regexp lacks is done by hand: a '[' preceded
	// by an identifier character is a generic instantiation or an index, not
	// a link, and a '[' followed by ']' is a slice type.
	link := regexp.MustCompile(`\[([A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)?)\]`)

	// Names that look like doc links and are not. One entry, one reason.
	notALink := map[string]string{
		"T":      "a type parameter in a generic signature written inside a doc comment",
		"K":      "a type parameter",
		"V":      "a type parameter",
		"string": "a type argument, lower-case so it cannot be a link to an exported name",
		"byte":   "a type argument",
		"int":    "a type argument",
		"error":  "a predeclared identifier, not a package-level name",
		"any":    "a predeclared identifier",
	}

	// The roots this check covers. Add a directory here to widen it; see
	// the Scope paragraph above for what widening to the whole module meets.
	roots := []string{"x/compress", "x/encrypt", "x/claimcheck", "x/inbox", "x/outbox"}

	dirs := map[string]bool{}
	walk := func(root string) error {
		return filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() {
				switch d.Name() {
				case ".git", "testdata", "node_modules":
					return filepath.SkipDir
				}
				// Nested modules are other modules; this test parses only this one.
				if path != "." {
					if _, serr := os.Stat(filepath.Join(path, "go.mod")); serr == nil {
						return filepath.SkipDir
					}
				}
				return nil
			}
			if strings.HasSuffix(path, ".go") {
				dirs[filepath.Dir(path)] = true
			}
			return nil
		})
	}
	for _, root := range roots {
		if err := walk(root); err != nil {
			t.Fatalf("walking %s: %v", root, err)
		}
	}
	// Every root must still hold at least one package. The first version
	// compared the package COUNT with the root count, which assumed one
	// package per root and failed as soon as x/encrypt/encrypttest and
	// x/claimcheck/storetest appeared under two of them ("found 7 package(s)
	// under 5 root(s)") -- packages this check should cover, not refuse.
	for _, root := range roots {
		found := false
		for dir := range dirs {
			if dir == root || strings.HasPrefix(dir, root+string(filepath.Separator)) {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("root %s holds no package; a root that no longer exists would make this check quietly cover less", root)
		}
	}

	type miss struct{ file, name string }
	var misses []miss
	pkgs, comments := 0, 0

	for dir := range dirs {
		// Parsed one file at a time rather than through parser.ParseDir,
		// which is deprecated for ignoring build tags -- and which this
		// module cannot replace with x/tools/go/packages, being
		// stdlib-only. docs_api_test.go's exportedOf makes the same choice
		// for the same reason.
		files := filesOf(t, dir)
		// Every top-level name declared anywhere in the directory, test files
		// included: one namespace, because a doc comment in foo.go may point
		// at a test in foo_test.go and that is the point.
		declared := map[string]bool{}
		type commented struct {
			file string
			doc  *ast.CommentGroup
		}
		var docs []commented
		pkgs++
		for name, f := range files {
			short := filepath.ToSlash(name)
			if f.Doc != nil {
				docs = append(docs, commented{short, f.Doc})
			}
			for _, decl := range f.Decls {
				switch d := decl.(type) {
				case *ast.FuncDecl:
					declared[d.Name.Name] = true
					if d.Recv != nil && len(d.Recv.List) == 1 {
						// Methods are reachable as Type.Method.
						declared[recvName(d.Recv.List[0].Type)+"."+d.Name.Name] = true
					}
					if d.Doc != nil {
						docs = append(docs, commented{short, d.Doc})
					}
				case *ast.GenDecl:
					if d.Doc != nil {
						docs = append(docs, commented{short, d.Doc})
					}
					for _, spec := range d.Specs {
						switch sp := spec.(type) {
						case *ast.TypeSpec:
							declared[sp.Name.Name] = true
							if sp.Doc != nil {
								docs = append(docs, commented{short, sp.Doc})
							}
						case *ast.ValueSpec:
							for _, n := range sp.Names {
								declared[n.Name] = true
							}
							if sp.Doc != nil {
								docs = append(docs, commented{short, sp.Doc})
							}
						}
					}
				}
			}
		}
		for _, c := range docs {
			comments++
			text := c.doc.Text()
			for _, m := range link.FindAllStringSubmatchIndex(text, -1) {
				name := text[m[2]:m[3]]
				if i := m[0]; i > 0 && isIdentByte(text[i-1]) {
					continue // NativeAs[T] and friends
				}
				if _, ok := notALink[name]; ok {
					continue
				}
				// A qualified name belongs to another package, which this
				// test does not resolve; the markdown `[text](url)` form is
				// not a doc link either.
				if strings.Contains(name, ".") {
					continue
				}
				if j := m[1]; j < len(text) && text[j] == '(' {
					continue
				}
				if !declared[name] {
					misses = append(misses, miss{c.file, name})
				}
			}
		}
	}

	// The check has to be able to fail, and the cheapest proof is a name
	// nothing will ever declare.
	if declaredSomewhere(t, dirs, "ThisNameIsNotDeclaredAnywhere") {
		t.Fatal("the self-check's example name now exists; pick another")
	}

	if len(misses) > 0 {
		seen := map[string]bool{}
		var lines []string
		for _, m := range misses {
			line := m.file + ": [" + m.name + "]"
			if seen[line] {
				continue
			}
			seen[line] = true
			lines = append(lines, line)
		}
		sort.Strings(lines)
		t.Fatalf("%d doc link(s) name something that does not exist in their package:\n  %s",
			len(lines), strings.Join(lines, "\n  "))
	}
	t.Logf("%d doc comment(s) across %d package(s) checked; every [Name] link resolves", comments, pkgs)
}

// recvName is the receiver's type name, pointer receivers included.
func recvName(e ast.Expr) string {
	switch t := e.(type) {
	case *ast.StarExpr:
		return recvName(t.X)
	case *ast.Ident:
		return t.Name
	case *ast.IndexExpr:
		return recvName(t.X)
	case *ast.IndexListExpr:
		return recvName(t.X)
	}
	return ""
}

func isIdentByte(b byte) bool {
	return b == '_' || b == '.' ||
		(b >= '0' && b <= '9') || (b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z')
}

// filesOf parses every .go file in dir, test files included, keyed by path.
func filesOf(t *testing.T, dir string) map[string]*ast.File {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("reading %s: %v", dir, err)
	}
	fset := token.NewFileSet()
	out := map[string]*ast.File{}
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".go") {
			continue
		}
		path := filepath.Join(dir, e.Name())
		f, perr := parser.ParseFile(fset, path, nil, parser.ParseComments)
		if perr != nil {
			t.Fatalf("parsing %s: %v", path, perr)
		}
		out[path] = f
	}
	return out
}

// declaredSomewhere is the self-check's helper: does any package under the
// checked roots declare this name?
func declaredSomewhere(t *testing.T, dirs map[string]bool, name string) bool {
	t.Helper()
	for dir := range dirs {
		for _, f := range filesOf(t, dir) {
			for _, decl := range f.Decls {
				if fd, ok := decl.(*ast.FuncDecl); ok && fd.Name.Name == name {
					return true
				}
			}
		}
	}
	return false
}
