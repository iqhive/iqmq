package main

import (
	"bufio"
	"bytes"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"sort"
	"strings"
)

// family describes one variant family package as announced by TestKindsList.
type family struct {
	pkg    string
	kinds  []string          // slugs in declaration order
	idents map[string]string // slug -> Go identifier
}

func lint(args []string) error {
	fs := flag.NewFlagSet("lint", flag.ContinueOnError)
	root := fs.String("root", "..", "root module directory")
	if err := fs.Parse(args); err != nil {
		return err
	}
	patterns := fs.Args()
	// Without an explicit package list the whole repository's bake-off
	// declarations are checked; with one, only the families whose package it
	// names. `make ci` runs the former, so the campaign's structural gate is
	// never narrower than the campaign.
	explicit := len(patterns) > 0
	if !explicit {
		patterns = []string{"./internal/..."}
	}
	pkgs, err := listPackages(*root, patterns)
	if err != nil {
		return err
	}
	var problems []string
	families := 0
	for _, pkg := range pkgs {
		fam, ok, err := loadFamily(*root, pkg)
		if err != nil {
			return err
		}
		if !ok {
			continue
		}
		families++
		modelKinds, err := modelSubtests(*root, pkg)
		if err != nil {
			return err
		}
		benchNames, err := benchmarkNames(*root, pkg)
		if err != nil {
			return err
		}
		for _, slug := range fam.kinds {
			if !modelKinds[slug] {
				problems = append(problems, fmt.Sprintf("%s: kind %q has no TestModel/%s subtest", pkg, slug, slug))
			}
			if !mentions(benchNames, slug) {
				problems = append(problems, fmt.Sprintf("%s: kind %q appears in no benchmark name", pkg, slug))
			}
		}
		for slug := range modelKinds {
			if _, known := fam.idents[slug]; !known {
				problems = append(problems, fmt.Sprintf("%s: TestModel/%s is not a declared kind", pkg, slug))
			}
		}
	}
	// The declarations in hack/bakeoff/*.env are the other half: a family
	// whose BENCH root selects no benchmark in its PKG, or whose declared
	// kinds have no benchmark of their own, is one the campaign cannot
	// measure. That used to be discovered by hack/bakeoff.sh on the night the
	// campaign started -- family A29's root named a benchmark that lived in
	// another module, which a `go test` run in its PKG cannot reach -- because
	// the walk above only sees packages with a TestKindsList and never reads a
	// family's benchmark root at all (adr/ADR-017-assumptions.md, assumption
	// A24.7).
	var only map[string]bool
	if explicit {
		only = make(map[string]bool, len(pkgs))
		for _, pkg := range pkgs {
			only[pkg] = true
		}
	}
	envProblems, declared, err := lintBakeoffFamilies(*root, only)
	if err != nil {
		return err
	}
	problems = append(problems, envProblems...)
	if len(problems) > 0 {
		sort.Strings(problems)
		for _, p := range problems {
			fmt.Fprintln(os.Stderr, " -", p)
		}
		return fmt.Errorf("%d problem(s) in %d famil(y/ies) and %d bake-off declaration(s)",
			len(problems), families, declared)
	}
	fmt.Fprintf(os.Stderr, "bakeoff lint: %d famil(y/ies) ok, %d bake-off declaration(s) measurable\n",
		families, declared)
	return nil
}

func goCmd(root string, args ...string) *exec.Cmd {
	cmd := exec.Command("go", args...)
	cmd.Dir = root
	cmd.Env = append(os.Environ(), "GOWORK=off")
	return cmd
}

func listPackages(root string, patterns []string) ([]string, error) {
	out, err := goCmd(root, append([]string{"list"}, patterns...)...).Output()
	if err != nil {
		return nil, fmt.Errorf("go list: %w", err)
	}
	var pkgs []string
	for _, l := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		if l != "" {
			pkgs = append(pkgs, l)
		}
	}
	return pkgs, nil
}

// loadFamily runs TestKindsList and parses its "kinds:" log line.
func loadFamily(root, pkg string) (*family, bool, error) {
	return loadKinds(root, pkg, "TestKindsList")
}

// loadKinds runs a package's kind-list test and parses its "kinds:" log line.
// A package that hosts more than one variant family gives each of them its
// own list test, which hack/bakeoff/<id>.env names in KINDS_TEST.
func loadKinds(root, pkg, test string) (*family, bool, error) {
	out, err := goCmd(root, "test", "-run", "^"+test+"$", "-v", "-count=1", pkg).CombinedOutput()
	if err != nil {
		// A signalled child is asked about FIRST: its partial output can
		// already carry the "no tests to run" line, and reading that as "this
		// package is not a family" removed the package from the check in
		// silence (child.go).
		if signalled(err) {
			return nil, false, childFailure(pkg, test, out, err)
		}
		if noSuchTest(out) {
			return nil, false, nil
		}
		return nil, false, childFailure(pkg, test, out, err)
	}
	if noSuchTest(out) {
		return nil, false, nil
	}
	fam := &family{pkg: pkg, idents: map[string]string{}}
	sc := bufio.NewScanner(bytes.NewReader(out))
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if i := strings.Index(line, "kinds:"); i >= 0 {
			for _, part := range strings.Split(strings.TrimSpace(line[i+len("kinds:"):]), ",") {
				part = strings.TrimSpace(part)
				if part == "" {
					continue
				}
				slug, ident, _ := strings.Cut(part, "=")
				fam.kinds = append(fam.kinds, slug)
				fam.idents[slug] = ident
			}
		}
	}
	if len(fam.kinds) == 0 {
		return nil, false, nil
	}
	return fam, true, nil
}

func modelSubtests(root, pkg string) (map[string]bool, error) {
	out, err := goCmd(root, "test", "-run", "^TestModel$", "-v", "-count=1", pkg).CombinedOutput()
	if err != nil {
		return nil, childFailure(pkg, "TestModel", out, err)
	}
	kinds := map[string]bool{}
	sc := bufio.NewScanner(bytes.NewReader(out))
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if strings.HasPrefix(line, "=== RUN   TestModel/") {
			name := strings.TrimPrefix(line, "=== RUN   TestModel/")
			slug, _, _ := strings.Cut(name, "/")
			kinds[slug] = true
		}
	}
	return kinds, nil
}

func benchmarkNames(root, pkg string) ([]string, error) {
	out, err := goCmd(root, "test", "-run", "^$", "-bench", ".", "-benchtime", "1x", "-count=1", pkg).CombinedOutput()
	if err != nil {
		return nil, childFailure(pkg, "benchmarks", out, err)
	}
	var names []string
	sc := bufio.NewScanner(bytes.NewReader(out))
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if strings.HasPrefix(line, "Benchmark") {
			names = append(names, strings.Fields(line)[0])
		}
	}
	return names, nil
}

// mentions reports whether slug appears as a path segment or as a "=slug"
// dimension value in any benchmark name.
func mentions(names []string, slug string) bool {
	for _, n := range names {
		for _, seg := range strings.Split(n, "/") {
			if i := strings.LastIndexByte(seg, '-'); i > 0 {
				if _, err := fmt.Sscanf(seg[i+1:], "%d", new(int)); err == nil && !strings.Contains(seg[i+1:], "=") {
					seg = seg[:i]
				}
			}
			if seg == slug || strings.HasSuffix(seg, "="+slug) {
				return true
			}
		}
	}
	return false
}
