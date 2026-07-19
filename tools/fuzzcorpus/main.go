// Command fuzzcorpus manages committed fuzz corpora.
//
//	fuzzcorpus export [-max 200] [-prune] [-root.]
//	    copy interesting inputs from the Go fuzz cache into testdata/fuzz
//	fuzzcorpus count [-root.]
//	    print seed counts per target
//
// Inputs found by go test -fuzz are kept in $GOCACHE/fuzz/<import path>/<Target>/.
// export copies files that are not already present into
// <package>/testdata/fuzz/<Target>/, smallest first, until the directory holds
// -max files. It never removes anything: the committed corpus is the
// regression corpus the test suite requires a fix to commit its seed to,
// and the cache is an unbounded record of every interesting input this machine
// has ever produced, so a trim that ran over both deleted committed crashers.
// -prune opts back in to trimming, and even then refuses
// to delete a file git tracks.
package main

import (
	"flag"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
)

type target struct {
	dir        string // package directory
	importPath string
	name       string
}

func main() {
	if len(os.Args) < 2 {
		usage()
	}
	fs := flag.NewFlagSet(os.Args[1], flag.ExitOnError)
	root := fs.String("root", ".", "repository root")
	max := fs.Int("max", 200, "maximum files per target after export")
	prune := fs.Bool("prune", false, "also delete untracked inputs above -max (never deletes a tracked file)")
	_ = fs.Parse(os.Args[2:])
	var err error
	switch os.Args[1] {
	case "export":
		err = export(*root, *max, *prune)
	case "count":
		err = count(*root)
	default:
		usage()
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "fuzzcorpus:", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: fuzzcorpus (export|count) [-root DIR] [-max N] [-prune]")
	os.Exit(2)
}

var skipDirs = map[string]bool{".git": true, "testdata": true, "node_modules": true, "vendor": true, "states": true}

// targets enumerates every Fuzz function in every module under root.
func targets(root string) ([]target, error) {
	fset := token.NewFileSet()
	var out []target
	modulePaths := map[string]string{} // dir -> module path
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			name := d.Name()
			if path != root && (skipDirs[name] || strings.HasPrefix(name, ".") || strings.HasPrefix(name, "_")) {
				return filepath.SkipDir
			}
			if data, err := os.ReadFile(filepath.Join(path, "go.mod")); err == nil {
				for _, line := range strings.Split(string(data), "\n") {
					if strings.HasPrefix(line, "module ") {
						modulePaths[path] = strings.TrimSpace(strings.TrimPrefix(line, "module "))
					}
				}
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
		for _, decl := range f.Decls {
			if fd, ok := decl.(*ast.FuncDecl); ok && fd.Recv == nil && strings.HasPrefix(fd.Name.Name, "Fuzz") && fd.Name.Name != "Fuzz" {
				out = append(out, target{dir: dir, name: fd.Name.Name})
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	for i := range out {
		out[i].importPath = importPath(out[i].dir, modulePaths)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].importPath != out[j].importPath {
			return out[i].importPath < out[j].importPath
		}
		return out[i].name < out[j].name
	})
	return out, nil
}

func importPath(dir string, modules map[string]string) string {
	best, bestPath := "", ""
	for mdir, mpath := range modules {
		if rel, err := filepath.Rel(mdir, dir); err == nil && !strings.HasPrefix(rel, "..") && len(mdir) > len(best) {
			best = mdir
			if rel == "." {
				bestPath = mpath
			} else {
				bestPath = mpath + "/" + filepath.ToSlash(rel)
			}
		}
	}
	return bestPath
}

func export(root string, max int, prune bool) error {
	out, err := exec.Command("go", "env", "GOCACHE").Output()
	if err != nil {
		return err
	}
	cache := filepath.Join(strings.TrimSpace(string(out)), "fuzz")
	ts, err := targets(root)
	if err != nil {
		return err
	}
	copied, trimmed, full := 0, 0, 0
	for _, t := range ts {
		dst := filepath.Join(t.dir, "testdata", "fuzz", t.name)
		have, err := existing(dst)
		if err != nil {
			return err
		}
		room := max - len(have)
		if room <= 0 {
			full++
		}
		src := filepath.Join(cache, filepath.FromSlash(t.importPath), t.name)
		cand, err := candidates(src, have)
		if err != nil {
			return err
		}
		// Smallest first: a small input reproduces the same edge with less to
		// re-execute on every `make fuzz-seeds`, and the committed corpus is
		// run as a test on every increment.
		sort.Slice(cand, func(i, j int) bool {
			if cand[i].size != cand[j].size {
				return cand[i].size < cand[j].size
			}
			return cand[i].name < cand[j].name
		})
		for _, c := range cand {
			if room <= 0 {
				break
			}
			data, err := os.ReadFile(filepath.Join(src, c.name))
			if err != nil {
				return err
			}
			if err := os.MkdirAll(dst, 0o755); err != nil {
				return err
			}
			if err := os.WriteFile(filepath.Join(dst, c.name), data, 0o644); err != nil {
				return err
			}
			copied++
			room--
		}
		if prune {
			n, err := trim(dst, max)
			if err != nil {
				return err
			}
			trimmed += n
		}
	}
	fmt.Fprintf(os.Stderr, "fuzzcorpus: %d target(s), %d file(s) exported, %d trimmed, %d target(s) already at the -max %d\n",
		len(ts), copied, trimmed, full, max)
	return nil
}

// file is one corpus entry.
type file struct {
	name string
	size int64
}

// existing lists the files already committed for a target.
func existing(dir string) ([]file, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	return infos(entries)
}

// candidates lists the cache files for a target that the corpus does not hold.
func candidates(dir string, have []file) ([]file, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	present := make(map[string]bool, len(have))
	for _, h := range have {
		present[h.name] = true
	}
	all, err := infos(entries)
	if err != nil {
		return nil, err
	}
	out := all[:0]
	for _, f := range all {
		if !present[f.name] {
			out = append(out, f)
		}
	}
	return out, nil
}

func infos(entries []os.DirEntry) ([]file, error) {
	out := make([]file, 0, len(entries))
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		info, err := e.Info()
		if err != nil {
			return nil, err
		}
		out = append(out, file{e.Name(), info.Size()})
	}
	return out, nil
}

// tracked reports the files in dir that git has under version control. An
// error means git could not answer, and then nothing in dir may be deleted.
func tracked(dir string) (map[string]bool, error) {
	out, err := exec.Command("git", "ls-files", "-z", "--", dir).Output()
	if err != nil {
		return nil, err
	}
	m := map[string]bool{}
	for _, p := range strings.Split(string(out), "\x00") {
		if p == "" {
			continue
		}
		m[filepath.Base(p)] = true
	}
	return m, nil
}

// trim keeps at most max files in dir, preferring the smallest inputs. A file
// git tracks is never deleted: those are the committed regression corpus, and
// a trim that could reach them cost 36 of them once.
func trim(dir string, max int) (int, error) {
	files, err := existing(dir)
	if err != nil {
		return 0, err
	}
	if len(files) <= max {
		return 0, nil
	}
	keep, err := tracked(dir)
	if err != nil {
		return 0, fmt.Errorf("%s: git cannot say which inputs are committed, so none may be pruned: %w", dir, err)
	}
	sort.Slice(files, func(i, j int) bool {
		if files[i].size != files[j].size {
			return files[i].size < files[j].size
		}
		return files[i].name < files[j].name
	})
	removed := 0
	for i, f := range files {
		if i < max || keep[f.name] {
			continue
		}
		if err := os.Remove(filepath.Join(dir, f.name)); err != nil {
			return removed, err
		}
		removed++
	}
	return removed, nil
}

func count(root string) error {
	ts, err := targets(root)
	if err != nil {
		return err
	}
	total := 0
	for _, t := range ts {
		n := 0
		if entries, err := os.ReadDir(filepath.Join(t.dir, "testdata", "fuzz", t.name)); err == nil {
			for _, e := range entries {
				if !e.IsDir() {
					n++
				}
			}
		}
		total += n
		fmt.Printf("%-60s %s %4d\n", t.importPath, t.name, n)
	}
	fmt.Printf("%d target(s), %d committed input(s)\n", len(ts), total)
	return nil
}
