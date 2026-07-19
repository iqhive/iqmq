// Command fuzzmanifest produces the fuzz manifest the test suite attaches
// to a release: targets, durations, corpus hash, result, arch and Go version.
//
// It enumerates every Fuzz target in every module, hashes its committed
// corpus, and merges any run results found under -results (JSON lines with
// fields target, package, duration_s, result, arch, go). The manifest is
// written as JSON to -o.
//
//	fuzzmanifest [-root .] [-results DIR] [-require-runs] [-o FILE]
//
// A run result names its package the way the `go` command was invoked with it
// -- an import path from `the fuzz runner all`, a ./-relative directory from
// `the fuzz runner smoke` -- and a target names its package by repository-relative
// directory, so the two are reconciled before the merge and a result that
// matches no target is an error rather than a silently dropped line.
// Without that, every manifest this repository had ever
// written carried the target list and the corpus hashes and no duration or
// result at all.
package main

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"go/ast"
	"go/build"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"time"
)

type runResult struct {
	Target    string  `json:"target"`
	Package   string  `json:"package"`
	DurationS float64 `json:"duration_s"`
	Result    string  `json:"result"`
	Arch      string  `json:"arch,omitempty"`
	Go        string  `json:"go,omitempty"`
	Date      string  `json:"date,omitempty"`
}

type targetEntry struct {
	Package      string `json:"package"`
	Name         string `json:"name"`
	Seeds        int    `json:"seeds"`
	CorpusSHA256 string `json:"corpus_sha256"`
	// Constraint is the file's `//go:build` expression when the default build
	// does NOT satisfy it -- `integration` for the three broker-backed
	// differential targets, which need a live broker and are built only with
	// `-tags integration`. The target is still in the manifest, with its
	// corpus hash, and `-require-runs` does not ask it for a run result: no
	// campaign this repository has can produce one, so requiring it would fail
	// every night for ever.
	Constraint string      `json:"build_constraint,omitempty"`
	Runs       []runResult `json:"runs,omitempty"`
}

type manifest struct {
	Generated string        `json:"generated"`
	Go        string        `json:"go"`
	Arch      string        `json:"arch"`
	Result    string        `json:"result"`
	Runs      int           `json:"total_runs"`
	DurationS float64       `json:"total_duration_s"`
	Targets   []targetEntry `json:"targets"`
	Total     int           `json:"total_targets"`
}

func main() {
	root := flag.String("root", ".", "repository root")
	results := flag.String("results", "", "directory of JSON-lines run results")
	out := flag.String("o", "reports/fuzz-manifest.json", "output file")
	requireRuns := flag.Bool("require-runs", false, "fail when a target has no run result")
	flag.Parse()
	if err := run(*root, *results, *out, *requireRuns); err != nil {
		fmt.Fprintln(os.Stderr, "fuzzmanifest:", err)
		os.Exit(1)
	}
}

var skipDirs = map[string]bool{".git": true, "testdata": true, "node_modules": true, "vendor": true, "states": true}

func run(root, results, out string, requireRuns bool) error {
	fset := token.NewFileSet()
	var entries []targetEntry
	modules := map[string]string{}
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			name := d.Name()
			if path != root && (skipDirs[name] || strings.HasPrefix(name, ".") || strings.HasPrefix(name, "_")) {
				return filepath.SkipDir
			}
			if mod, ok := modulePath(filepath.Join(path, "go.mod")); ok {
				rel, _ := filepath.Rel(root, path)
				modules[mod] = filepath.ToSlash(rel)
			}
			return nil
		}
		if !strings.HasSuffix(d.Name(), "_test.go") {
			return nil
		}
		f, err := parser.ParseFile(fset, path, nil, parser.SkipObjectResolution)
		if err != nil {
			return err
		}
		dir := filepath.Dir(path)
		rel, _ := filepath.Rel(root, dir)
		constraint, err := unmetConstraint(dir, d.Name())
		if err != nil {
			return err
		}
		for _, decl := range f.Decls {
			fd, ok := decl.(*ast.FuncDecl)
			if !ok || fd.Recv != nil || !strings.HasPrefix(fd.Name.Name, "Fuzz") || fd.Name.Name == "Fuzz" {
				continue
			}
			seeds, sum, err := corpusHash(filepath.Join(dir, "testdata", "fuzz", fd.Name.Name))
			if err != nil {
				return err
			}
			entries = append(entries, targetEntry{
				Package: filepath.ToSlash(rel), Name: fd.Name.Name,
				Seeds: seeds, CorpusSHA256: sum, Constraint: constraint,
			})
		}
		return nil
	})
	if err != nil {
		return err
	}
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].Package != entries[j].Package {
			return entries[i].Package < entries[j].Package
		}
		return entries[i].Name < entries[j].Name
	})
	m := manifest{
		Generated: time.Now().UTC().Format(time.RFC3339),
		Go:        runtime.Version(),
		Arch:      runtime.GOARCH,
		Result:    "not run",
		Targets:   entries,
		Total:     len(entries),
	}
	if results != "" {
		runs, err := loadResults(results, modules)
		if err != nil {
			return err
		}
		used := map[string]bool{}
		crashes, withRuns, runnable := 0, 0, 0
		for i := range entries {
			if entries[i].Constraint == "" {
				runnable++
			}
			key := entries[i].Package + "." + entries[i].Name
			entries[i].Runs = runs[key]
			if len(entries[i].Runs) == 0 {
				continue
			}
			used[key] = true
			withRuns++
			for _, r := range entries[i].Runs {
				m.Runs++
				m.DurationS += r.DurationS
				if r.Result != "ok" {
					crashes++
				}
			}
		}
		// A result that reaches no target is the defect assumption A25.2
		// records, and it is invisible in the manifest itself: the file looks
		// complete, every corpus hash is right, and the durations are simply
		// absent. So it fails here instead.
		var orphans []string
		for key := range runs {
			if !used[key] {
				orphans = append(orphans, key)
			}
		}
		if len(orphans) > 0 {
			sort.Strings(orphans)
			return fmt.Errorf("%d run result key(s) match no fuzz target: %s", len(orphans), strings.Join(orphans, " "))
		}
		switch {
		case crashes > 0:
			m.Result = fmt.Sprintf("%d crash(es) in %d run(s)", crashes, m.Runs)
		case m.Runs > 0:
			m.Result = fmt.Sprintf("ok, %d run(s) over %d target(s)", m.Runs, withRuns)
		}
		// -require-runs asks for a run of every target the DEFAULT build
		// includes. A build-constrained target is named in the refusal only if
		// it somehow did run, never as a hole: `conformance/differential`'s
		// three broker-backed targets are `//go:build integration` and no
		// runner without a broker can exercise them, so counting them made the
		// check fail on a campaign that had run everything it could.
		if requireRuns {
			var missing []string
			for _, e := range entries {
				if e.Constraint == "" && len(e.Runs) == 0 {
					missing = append(missing, e.Package+"."+e.Name)
				}
			}
			if len(missing) > 0 {
				sort.Strings(missing)
				m.Total = len(entries)
				if err := write(out, m); err != nil {
					return err
				}
				return fmt.Errorf("%d of %d runnable target(s) have no run result in %s: %s",
					len(missing), runnable, results, strings.Join(missing, " "))
			}
		}
	}
	if err := write(out, m); err != nil {
		return err
	}
	constrained := 0
	for _, e := range entries {
		if e.Constraint != "" {
			constrained++
		}
	}
	fmt.Fprintf(os.Stderr, "fuzzmanifest: %d target(s) (%d outside the default build), %d run(s), %s -> %s\n",
		len(entries), constrained, m.Runs, m.Result, out)
	return nil
}

func write(out string, m manifest) error {
	data, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return err
	}
	if dir := filepath.Dir(out); dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return err
		}
	}
	return os.WriteFile(out, append(data, '\n'), 0o644)
}

// unmetConstraint returns the file's `//go:build` expression when the DEFAULT
// build does not select the file, and "" when it does. go/build decides, so
// GOOS/GOARCH suffixes and every operator of the constraint grammar are
// handled the way `go test` handles them; the expression itself is read back
// only so the manifest can say what the target is waiting for.
func unmetConstraint(dir, name string) (string, error) {
	ctx := build.Default
	ctx.UseAllFiles = false
	ok, err := ctx.MatchFile(dir, name)
	if err != nil {
		// A file go/build cannot read at all is not a constraint question.
		return "", fmt.Errorf("%s: %w", filepath.Join(dir, name), err)
	}
	if ok {
		return "", nil
	}
	data, err := os.ReadFile(filepath.Join(dir, name))
	if err != nil {
		return "", err
	}
	for line := range strings.SplitSeq(string(data), "\n") {
		line = strings.TrimSpace(line)
		if expr, found := strings.CutPrefix(line, "//go:build "); found {
			return strings.TrimSpace(expr), nil
		}
		if strings.HasPrefix(line, "package ") {
			break
		}
	}
	// Excluded by a name suffix (`_windows_test.go`) rather than by a
	// constraint line; name the file so the reason is still legible.
	return "file " + name + " is not in the default build", nil
}

// modulePath reads the module path declared by a go.mod file.
func modulePath(path string) (string, bool) {
	data, err := os.ReadFile(path)
	if err != nil {
		return "", false
	}
	for _, line := range strings.Split(string(data), "\n") {
		if rest, ok := strings.CutPrefix(strings.TrimSpace(line), "module "); ok {
			return strings.TrimSpace(rest), true
		}
	}
	return "", false
}

// normalizePkg turns the package a run result names into the
// repository-relative directory a target entry is keyed by. `the fuzz runner all`
// and `shard` pass import paths and `smoke` passes a ./-relative directory, so
// both forms arrive.
func normalizePkg(pkg string, modules map[string]string) string {
	pkg = strings.TrimSuffix(filepath.ToSlash(strings.TrimSpace(pkg)), "/")
	pkg = strings.TrimPrefix(pkg, "./")
	// The longest matching module path wins, and the choice is made against
	// the path as it arrived. Rewriting pkg inside the loop and then testing
	// the next module against the rewritten value makes the answer depend on
	// the map's iteration order, which Go randomises per range: with
	// `.../benchmarks` visited before `.../benchmarks/nativebase`, pkg became
	// `benchmarks/nativebase/kafka`, the longer module no longer prefixed it,
	// and the nested module's packages were filed under the parent's
	// directory -- about half the time.
	best, bestDir := "", ""
	for mod, dir := range modules {
		if pkg != mod && !strings.HasPrefix(pkg, mod+"/") {
			continue
		}
		if len(mod) <= len(best) {
			continue
		}
		best, bestDir = mod, dir
	}
	if best == "" {
		if pkg == "" {
			return "."
		}
		return pkg
	}
	rest := strings.TrimPrefix(strings.TrimPrefix(pkg, best), "/")
	switch {
	case rest == "":
		return bestDir
	case bestDir == ".":
		return rest
	default:
		return bestDir + "/" + rest
	}
}

func corpusHash(dir string) (int, string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, "", nil
		}
		return 0, "", err
	}
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		if !e.IsDir() {
			names = append(names, e.Name())
		}
	}
	sort.Strings(names)
	h := sha256.New()
	for _, n := range names {
		data, err := os.ReadFile(filepath.Join(dir, n))
		if err != nil {
			return 0, "", err
		}
		h.Write([]byte(n))
		h.Write([]byte{0})
		h.Write(data)
		h.Write([]byte{0})
	}
	if len(names) == 0 {
		return 0, "", nil
	}
	return len(names), hex.EncodeToString(h.Sum(nil)), nil
}

func loadResults(dir string, modules map[string]string) (map[string][]runResult, error) {
	out := map[string][]runResult{}
	matches, err := filepath.Glob(filepath.Join(dir, "*.jsonl"))
	if err != nil {
		return nil, err
	}
	for _, m := range matches {
		f, err := os.Open(m)
		if err != nil {
			return nil, err
		}
		sc := bufio.NewScanner(f)
		sc.Buffer(make([]byte, 1<<20), 1<<20)
		for sc.Scan() {
			line := strings.TrimSpace(sc.Text())
			if line == "" {
				continue
			}
			var r runResult
			if err := json.Unmarshal([]byte(line), &r); err != nil {
				_ = f.Close()
				return nil, fmt.Errorf("%s: %w", m, err)
			}
			r.Package = normalizePkg(r.Package, modules)
			key := r.Package + "." + r.Target
			out[key] = append(out[key], r)
		}
		if err := f.Close(); err != nil {
			return nil, err
		}
		if err := sc.Err(); err != nil {
			return nil, err
		}
	}
	return out, nil
}
