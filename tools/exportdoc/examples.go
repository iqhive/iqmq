package main

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// specWorkflows reads the required example workflows from the API policy files.
func specWorkflows(root string) ([]string, error) {
	b, err := os.ReadFile(filepath.Join(root, "hack/api/workflows.txt"))
	if err != nil {
		return nil, err
	}
	var out []string
	for _, line := range strings.Split(string(b), "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), "#") {
			continue
		}
		if w := normalizeWorkflow(line); w != "" {
			out = append(out, w)
		}
	}
	if len(out) < 2 {
		return nil, fmt.Errorf("exportdoc: workflow requirements list must name at least two workflows")
	}
	return out, nil
}

// normalizeWorkflow reduces a workflow name to a comparable key: lower case,
// single-spaced, no backticks and no leading "and".
func normalizeWorkflow(s string) string {
	s = strings.ReplaceAll(s, "`", "")
	s = strings.ToLower(strings.Join(strings.Fields(s), " "))
	s = strings.TrimPrefix(s, "and ")
	return strings.TrimSpace(s)
}

// checkExamples holds the policy's example table to §13 and to the tree:
// every workflow §13 names is in the policy, every policy workflow is §13's,
// and every example a policy line names exists as a function in the package it
// names. "perdriver" asks for one example in every package that exports Open.
func checkExamples(out report, root string, exs []exampleEntry) []string {
	var problems []string
	want, err := specWorkflows(root)
	if err != nil {
		return []string{err.Error()}
	}
	have := map[string]exampleEntry{}
	for _, e := range exs {
		have[e.workflow] = e
	}
	out.printf("==> exportdoc: the workflow list names %d example_test.go workflows\n", len(want))
	covered, gaps := 0, 0
	for _, wf := range want {
		e, ok := have[wf]
		if !ok {
			problems = append(problems, fmt.Sprintf("the workflow list names the workflow %q and the policy does not: add it with the examples that cover it, or with '-' and the reason it has none", wf))
			continue
		}
		if len(e.refs) == 0 {
			gaps++
			out.printf("    %-38s NO EXAMPLE: %s\n", wf, e.note)
			continue
		}
		var missing []string
		for _, ref := range e.refs {
			if ref == "perdriver" {
				missing = append(missing, perDriverGaps(root)...)
				continue
			}
			pkg, fn, ok := strings.Cut(ref, ":")
			if !ok {
				problems = append(problems, fmt.Sprintf("workflow %q: %q is not a package:ExampleName reference", wf, ref))
				continue
			}
			names, err := exampleNames(filepath.Join(root, pkg))
			if err != nil {
				problems = append(problems, fmt.Sprintf("workflow %q: %v", wf, err))
				continue
			}
			if !names[fn] {
				missing = append(missing, ref)
			}
		}
		if len(missing) > 0 {
			problems = append(problems, fmt.Sprintf("workflow %q names %d example(s) that do not exist: %s", wf, len(missing), strings.Join(missing, " ")))
			continue
		}
		covered++
		out.printf("    %-38s %s\n", wf, strings.Join(e.refs, " "))
	}
	for wf := range have {
		if !contains(want, wf) {
			problems = append(problems, fmt.Sprintf("the policy names the workflow %q, which the workflow list does not contain", wf))
		}
	}
	out.printf("==> exportdoc: %d of %d §13 workflows have a runnable example; %d declared with no example and a reason\n", covered, len(want), gaps)
	return problems
}

// perDriverGaps returns every package that exports an Open and has no Example
// function at all, as "pkg:Example" references, so the caller can report them
// the same way it reports a named example that does not exist.
func perDriverGaps(root string) []string {
	var out []string
	for _, base := range []string{"drivers", "memory"} {
		dir := filepath.Join(root, base)
		_ = filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
			if err != nil || !d.IsDir() {
				return err
			}
			switch d.Name() {
			case "testdata", "internal":
				return fs.SkipDir
			}
			if !exportsOpen(path) {
				return nil
			}
			names, err := exampleNames(path)
			if err != nil || len(names) > 0 {
				return nil //nolint:nilerr // a package that will not parse is the compiler's finding, not this check's
			}
			rel, _ := filepath.Rel(root, path)
			out = append(out, filepath.ToSlash(rel)+":Example")
			return nil
		})
	}
	sort.Strings(out)
	return out
}

// exportsOpen says whether the package in dir declares a top-level Open.
func exportsOpen(dir string) bool {
	ents, err := os.ReadDir(dir)
	if err != nil {
		return false
	}
	fset := token.NewFileSet()
	for _, e := range ents {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".go") || strings.HasSuffix(e.Name(), "_test.go") {
			continue
		}
		f, err := parser.ParseFile(fset, filepath.Join(dir, e.Name()), nil, parser.SkipObjectResolution)
		if err != nil {
			continue
		}
		for _, d := range f.Decls {
			fd, ok := d.(*ast.FuncDecl)
			if ok && fd.Recv == nil && fd.Name.Name == "Open" {
				return true
			}
		}
	}
	return false
}

// exampleNames lists the Example functions declared in dir's test files.
func exampleNames(dir string) (map[string]bool, error) {
	ents, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", dir, err)
	}
	out := map[string]bool{}
	fset := token.NewFileSet()
	for _, e := range ents {
		if e.IsDir() || !strings.HasSuffix(e.Name(), "_test.go") {
			continue
		}
		f, err := parser.ParseFile(fset, filepath.Join(dir, e.Name()), nil, parser.SkipObjectResolution)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", filepath.Join(dir, e.Name()), err)
		}
		for _, d := range f.Decls {
			fd, ok := d.(*ast.FuncDecl)
			if ok && fd.Recv == nil && strings.HasPrefix(fd.Name.Name, "Example") {
				out[fd.Name.Name] = true
			}
		}
	}
	return out, nil
}
