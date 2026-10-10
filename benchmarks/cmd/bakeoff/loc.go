package main

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
)

// kindSeed identifies one kind of a family for line attribution: its
// benchmark slug and its Go identifier, as the family's TestKindsList
// announces them.
type kindSeed struct{ Slug, Ident string }

// locReport is one package's line attribution, computed for every kind of
// the family at once.
//
// Computing it for the whole family rather than one kind at a time is what
// makes the answer honest: a declaration the family SHARES -- the Kind
// vocabulary, the interface, `New`, a helper two arms both call -- can only
// be recognised as shared by looking at every arm, and charging it to each
// arm separately counts the same lines several times. That is how
// x/apibench/b9's four-line const block came to be nine lines of `seq2` AND
// nine lines of `iternext`.
type locReport struct {
	// Total is every attributable line of the package's non-test source:
	// the denominator the per-kind counts are a share of.
	Total int
	// Lines is the count charged to each kind's slug.
	Lines map[string]int
	// Unsafe records a kind whose implementation reaches unsafe.
	Unsafe map[string]bool
}

// attributed reports whether this KIND's own source was located, which is
// the question §0.3(2)'s simplicity band needs and the question assumption
// A26.14's `LOCAttributed` flag did not answer.
func (r locReport) attributed(slug string) bool { return r.Lines[slug] > 0 }

// unattributed is the part of the package charged to no kind: its imports and
// package doc, the family's own vocabulary and dispatch, and every
// declaration more than one arm reaches. `order` may only apply §0.3(2)'s
// simplicity preference when this is smaller than the gap the ordering turns
// on.
func (r locReport) unattributed() int {
	n := r.Total
	for _, v := range r.Lines {
		n -= v
	}
	if n < 0 {
		return 0
	}
	return n
}

// locDecl is one top-level declaration of a file no kind owns.
type locDecl struct {
	file     string
	decl     ast.Decl
	names    []string
	refs     []string
	from, to int
}

// attributeLOC counts the non-test source lines implementing each kind.
//
// Attribution has three sources, in decreasing strength:
//
//  1. FILE NAME -- `<slug>.go`, `<slug>_*.go` and `*_<slug>.go`. Exact for
//     the families that give every arm its own file, and most do, so the
//     whole file is charged including its imports. The suffix form is here
//     because family A16 writes `nack_readd.go`, `nack_stop.go` and
//     `nack_leavepending.go`, which the prefix forms miss; `nack_zset.go`
//     it still misses, because that arm's slug is `zsetmover`.
//  2. DECLARATION NAME -- a declaration whose name carries the kind's ident,
//     or has the slug as a prefix or a suffix once case is dropped
//     (`heap4Timer` and `newHeap4` belong to `heap4`, `secondChance` does not
//     belong to `cond`).
//  3. THE FAMILY'S OWN DISPATCH -- what a `switch` arm on the kind's ident
//     reaches, and the arm's own lines. `case Seq2: return newSeqSession(c)`
//     is the family saying, in code, which implementation is that kind's, and
//     it is the only one of the three that does not depend on somebody having
//     named a type after a slug. An arm that IS the implementation -- family
//     A21's five backoff policies are five `case` bodies of one `Next`
//     method, eleven lines for `decorrelated` against one for `none` -- is
//     charged its own lines, which is the only count of it there is.
//
// The three are SEEDS; the count is the transitive closure from them over
// the package's own declarations, charged to a kind only when that kind is
// the only one that reaches it. A declaration two arms reach is shared
// infrastructure and belongs to neither, which is the same answer the file
// rule gives for a file no arm is named after.
//
// This is assumption C01.7's repair and assumption C02.1's row. What it
// replaces attributed by NAME alone, so a kind whose implementation is named
// after the CONCEPT was charged only the lines that happened to mention its
// slug: `x/apibench/b9`'s `seqSession`/`newSeqSession`/`Seq` carry neither the
// slug `seq2` nor the ident `Seq2`, and that kind was charged nine lines of a
// fifty-seven-line implementation while `value` was charged sixty-four --
// which ordered §0.3(2)'s simplicity band by which arms had been named
// consistently.
//
// The closure is SYNTACTIC: package-level names are unique within a package,
// so `go/ast` resolves the edges that matter without a build. Rejected:
// `go/types`, which C01's follow-up named. It needs the package to LOAD, and
// `bakeoff decide` attributes packages in other modules than the one it runs
// in -- `drivers/kafka/kafkago` and `drivers/gcppubsub/v1compat` are
// attributed from inside `benchmarks/` -- so a type-checked attribution would
// need every driver's third-party dependency graph resolvable from the
// benchmark module, and would answer "not attributed" for exactly the
// families whose arms are hardest to name.
func attributeLOC(pkg string, kinds []kindSeed) locReport {
	rep := locReport{Lines: map[string]int{}, Unsafe: map[string]bool{}}
	if pkg == "" {
		return rep
	}
	entries, err := os.ReadDir(pkg)
	if err != nil {
		return rep
	}
	rep.Total = packageLOC(pkg)

	// 1. File ownership. The longest matching slug wins, so that `heap4.go`
	// belongs to `heap4` and not to `heap` in a family that has both.
	type parsed struct {
		name    string
		lines   []string
		file    *ast.File
		unsafe  bool
		ownedBy string
	}
	fset := token.NewFileSet()
	files := make([]*parsed, 0, len(entries))
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		data, err := os.ReadFile(filepath.Join(pkg, name))
		if err != nil {
			continue
		}
		f, err := parser.ParseFile(fset, name, data, parser.SkipObjectResolution)
		if err != nil {
			continue
		}
		p := &parsed{name: name, lines: strings.Split(string(data), "\n"), file: f}
		p.unsafe = strings.Contains(string(data), "iqmq_unsafe")
		for _, imp := range f.Imports {
			if imp.Path.Value == `"unsafe"` {
				p.unsafe = true
			}
		}
		stem := strings.TrimSuffix(name, ".go")
		for _, k := range kinds {
			if k.Slug == "" {
				continue
			}
			if !ownsFile(stem, k.Slug) {
				continue
			}
			if len(k.Slug) > len(p.ownedBy) {
				p.ownedBy = k.Slug
			}
		}
		files = append(files, p)
	}

	// Attribution is recorded per LINE, so that a line charged by two rules
	// -- a `case` arm inside a declaration one kind reaches -- is counted
	// once and by the more specific of the two.
	owner := map[string][]string{}
	for _, p := range files {
		owner[p.name] = make([]string, len(p.lines)+1)
	}

	// The whole of an owned file is the owner's, as it always was.
	for _, p := range files {
		if p.ownedBy == "" {
			continue
		}
		claim(owner[p.name], 1, len(p.lines), p.ownedBy)
		if p.unsafe {
			rep.Unsafe[p.ownedBy] = true
		}
	}

	// 2. The declaration table of the files no kind owns.
	var decls []locDecl
	byName := map[string][]int{}
	for _, p := range files {
		if p.ownedBy != "" {
			continue
		}
		for _, d := range p.file.Decls {
			from := fset.Position(d.Pos()).Line
			to := fset.Position(d.End()).Line
			dl := locDecl{
				file:  p.name,
				decl:  d,
				names: declNames(d),
				refs:  declRefs(d),
				from:  from,
				to:    to,
			}
			decls = append(decls, dl)
			for _, n := range indexNames(d) {
				if n != "" {
					byName[n] = append(byName[n], len(decls)-1)
				}
			}
		}
	}

	// 3. Seeds: by declaration name, and by the family's own dispatch.
	seeds := make([]map[int]bool, len(kinds))
	for i := range seeds {
		seeds[i] = map[int]bool{}
	}
	for j := range decls {
		for i, k := range kinds {
			if declBelongsTo(decls[j].decl, k.Slug, k.Ident) {
				seeds[i][j] = true
			}
		}
	}
	for _, p := range files {
		for i, k := range kinds {
			if k.Ident == "" {
				continue
			}
			for _, n := range dispatchRefs(p.file, k.Ident) {
				for _, idx := range byName[n] {
					seeds[i][idx] = true
				}
			}
		}
	}

	// 4. The transitive closure of each kind's seeds, then charge every
	// declaration exactly one kind reaches.
	reach := make([]map[int]bool, len(kinds))
	for i := range kinds {
		reach[i] = closeOver(seeds[i], decls, byName)
	}
	for j := range decls {
		at, n := -1, 0
		for i := range kinds {
			if reach[i][j] {
				at, n = i, n+1
			}
		}
		if n == 1 {
			claim(owner[decls[j].file], decls[j].from, decls[j].to, kinds[at].Slug)
		}
	}

	// 5. A `switch` arm naming exactly one kind is that kind's, and takes
	// precedence over the declaration around it; an arm naming several is
	// shared and takes the lines back off whichever kind reached the
	// enclosing declaration.
	for _, p := range files {
		if p.ownedBy != "" {
			continue
		}
		for _, arm := range switchArms(p.file, fset, kinds) {
			claim(owner[p.name], arm.from, arm.to, arm.slug)
		}
	}

	for _, p := range files {
		for line, slug := range owner[p.name] {
			if slug == "" || line == 0 || line > len(p.lines) {
				continue
			}
			if t := strings.TrimSpace(p.lines[line-1]); t != "" && !strings.HasPrefix(t, "//") {
				rep.Lines[slug]++
			}
		}
	}
	for i, k := range kinds {
		for j := range reach[i] {
			for _, p := range files {
				if p.name == decls[j].file && p.unsafe {
					rep.Unsafe[k.Slug] = true
				}
			}
		}
	}
	return rep
}

// ownsFile reports whether a file stem names a kind's own file. The suffix
// form needs three characters to be a name rather than a coincidence: a
// two-letter slug would capture files by their extension-like tails.
func ownsFile(stem, slug string) bool {
	switch {
	case slug == "":
		return false
	case stem == slug, strings.HasPrefix(stem, slug+"_"):
		return true
	case len(slug) >= 3 && strings.HasSuffix(stem, "_"+slug):
		return true
	}
	return false
}

// claim records lines [from, to] as belonging to slug, or to nobody when slug
// is empty.
func claim(owner []string, from, to int, slug string) {
	for i := from; i <= to && i < len(owner); i++ {
		if i >= 1 {
			owner[i] = slug
		}
	}
}

// switchArm is one `case` clause of a dispatch on a kind, and the kind it
// belongs to -- empty when it names more than one, which makes it shared.
type switchArm struct {
	from, to int
	slug     string
}

// switchArms finds every `case` clause whose expressions name kinds of this
// family. A clause naming one kind is that kind's implementation where the
// family has no separate declaration for it; a clause naming several belongs
// to none of them.
func switchArms(f *ast.File, fset *token.FileSet, kinds []kindSeed) []switchArm {
	var out []switchArm
	ast.Inspect(f, func(n ast.Node) bool {
		sw, ok := n.(*ast.SwitchStmt)
		if !ok || sw.Body == nil {
			return true
		}
		for _, stmt := range sw.Body.List {
			cc, ok := stmt.(*ast.CaseClause)
			if !ok || len(cc.List) == 0 || len(cc.Body) == 0 {
				continue
			}
			named := map[string]bool{}
			for _, e := range cc.List {
				id, ok := e.(*ast.Ident)
				if !ok {
					continue
				}
				for _, k := range kinds {
					if k.Ident != "" && id.Name == k.Ident {
						named[k.Slug] = true
					}
				}
			}
			if len(named) == 0 {
				continue
			}
			arm := switchArm{
				from: fset.Position(cc.Pos()).Line,
				to:   fset.Position(cc.End()).Line,
			}
			if len(named) == 1 {
				for slug := range named {
					arm.slug = slug
				}
			}
			out = append(out, arm)
		}
		return true
	})
	return out
}

// indexNames is every name a top-level declaration can be REACHED by. A
// method is reached through its receiver's type and not through its own name:
// a method call is always a selector, whose selected half `declRefs` does not
// follow, so indexing `wait` would make two kinds' identically named methods
// one node and every declaration of the package shared with every other.
func indexNames(d ast.Decl) []string {
	if f, ok := d.(*ast.FuncDecl); ok && f.Recv != nil && len(f.Recv.List) > 0 {
		return []string{receiverName(f.Recv.List[0].Type)}
	}
	return declNames(d)
}

// declNames is every name a top-level declaration declares: its own, and for
// a method the receiver's type.
func declNames(d ast.Decl) []string {
	var names []string
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
	return names
}

// declRefs is every identifier a declaration mentions. The selected half of a
// selector is skipped -- `x.Foo` refers to a field or to another package's
// export, never to a package-level declaration of this one -- and the rest is
// deliberately over-inclusive: a local variable that happens to share a
// package-level name adds an edge, which can only make a declaration look
// SHARED, and a shared declaration is charged to nobody. The error therefore
// runs towards not applying §0.3(2) rather than towards applying it on a
// count that is not one.
func declRefs(d ast.Decl) []string {
	seen := map[string]bool{}
	// A declaration does not reach itself, and a method does not reach the
	// type it is declared on: the edge runs the other way, from the type to
	// its methods, so that a kind reached through its type collects them.
	for _, n := range declNames(d) {
		seen[n] = true
	}
	var refs []string
	ast.Inspect(d, func(n ast.Node) bool {
		switch n := n.(type) {
		case *ast.SelectorExpr:
			ast.Inspect(n.X, func(m ast.Node) bool {
				if id, ok := m.(*ast.Ident); ok && !seen[id.Name] {
					seen[id.Name] = true
					refs = append(refs, id.Name)
				}
				return true
			})
			return false
		case *ast.Ident:
			if !seen[n.Name] {
				seen[n.Name] = true
				refs = append(refs, n.Name)
			}
		}
		return true
	})
	return refs
}

// dispatchRefs is every identifier mentioned by a `switch` arm whose case
// names ident: the family telling the attribution, in code, which
// declarations implement that kind.
//
// A bare identifier only. `case b2.Closure:` is another family's kind
// dispatched in this package's file, and reading it as this family's arm
// would charge x/apibench/b9's storage shapes to row B2's settlement shapes.
func dispatchRefs(f *ast.File, ident string) []string {
	var out []string
	ast.Inspect(f, func(n ast.Node) bool {
		sw, ok := n.(*ast.SwitchStmt)
		if !ok || sw.Body == nil {
			return true
		}
		for _, stmt := range sw.Body.List {
			cc, ok := stmt.(*ast.CaseClause)
			if !ok {
				continue
			}
			named := false
			for _, e := range cc.List {
				if id, ok := e.(*ast.Ident); ok && id.Name == ident {
					named = true
				}
			}
			if !named {
				continue
			}
			for _, s := range cc.Body {
				ast.Inspect(s, func(m ast.Node) bool {
					switch m := m.(type) {
					case *ast.SelectorExpr:
						return false
					case *ast.Ident:
						out = append(out, m.Name)
					}
					return true
				})
			}
		}
		return true
	})
	return out
}

// closeOver grows a seed set to every declaration reachable from it.
func closeOver(seed map[int]bool, decls []locDecl, byName map[string][]int) map[int]bool {
	got := map[int]bool{}
	queue := make([]int, 0, len(seed))
	for j := range seed {
		got[j] = true
		queue = append(queue, j)
	}
	for len(queue) > 0 {
		j := queue[0]
		queue = queue[1:]
		for _, r := range decls[j].refs {
			for _, k := range byName[r] {
				if !got[k] {
					got[k] = true
					queue = append(queue, k)
				}
			}
		}
	}
	return got
}
