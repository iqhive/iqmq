package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"

	"github.com/iqhive/iqmq/benchmarks/internal/budgets"
)

func gatePattern(args []string) error {
	fs := flag.NewFlagSet("gate-pattern", flag.ContinueOnError)
	path := fs.String("budgets", "budgets.yaml", "budgets file")
	packages := fs.Bool("packages", false, "print the packages that hold the gate set's benchmarks instead of the regular expression")
	root := fs.String("root", "..", "repository root, for -packages")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 1 {
		return errors.New("gate-pattern: NAME required")
	}
	cfg, err := budgets.Load(*path)
	if err != nil {
		return err
	}
	set, ok := cfg.GateSets[fs.Arg(0)]
	if !ok {
		return fmt.Errorf("gate-pattern: unknown gate set %q", fs.Arg(0))
	}
	if !*packages {
		fmt.Println(GlobsToRegexp(set))
		return nil
	}
	pkgs, err := gatePackages(*root, set, cfg.GatePackages[fs.Arg(0)])
	if err != nil {
		return err
	}
	if len(pkgs) == 0 {
		return fmt.Errorf("gate-pattern -packages %s: no hack/bakeoff/*.env declaration has a benchmark root this gate set selects, and budgets.yaml names no gate_packages", fs.Arg(0))
	}
	for _, p := range pkgs {
		fmt.Println(p)
	}
	return nil
}

// gatePackages names the packages `make gate` has to build to measure a gate
// set, read from the family declarations in hack/bakeoff. Deriving it beats
// listing it: `go test ./...` ran every package in the module ten times per
// side for the handful that hold a gated benchmark, and a hand-written list
// goes stale the first time a family moves.
func gatePackages(root string, set, extra []string) ([]string, error) {
	fams, err := readBakeoffEnvs(root)
	if err != nil {
		return nil, err
	}
	seen := map[string]bool{}
	var out []string
	// Packages that hold a gated benchmark without being a variant family.
	// The root package is the case: it holds the benchmarks §10.2's budgets
	// are written about -- `BenchmarkHeaders`, the option call sites -- and it
	// has no variant kinds, so it has no declaration to derive from. Without
	// this the gate would measure every family that holds a gated benchmark
	// and not the frozen API those budgets are for.
	//
	// `extra` is now THIS gate set's list rather than every set's: it was a
	// flat list in budgets.yaml unioned into all of them, so `./benchmarks/hooks`
	// -- added for the `pr` set's observer row -- was compiled and run by the
	// `selftest` set, whose pattern selects nothing in it.
	for _, p := range extra {
		if !seen[p] {
			seen[p] = true
			out = append(out, p)
		}
	}
	for i := range fams {
		fam := &fams[i]
		if fam.pkg == "" || fam.benchRoot == "" {
			continue
		}
		// A family whose arms were removed at the M6 disposition review has
		// no package to compile, and its benchmark root still overlaps a
		// gate-set pattern -- rows A37's `BenchmarkPublish` and B4's
		// `BenchmarkHeaders` both do. Deriving it made `bakeoff gate-pattern
		// -packages pr` a setup error, which is the failure
		// `checkGatePackages` exists to catch and which `make ci` cannot see
		// because `make gate` is not in it: the breakage would have surfaced
		// in `make gate-if-touched` at landing time or in the uninstalled
		// nightly job.
		if fam.removed != "" {
			continue
		}
		p := fam.gatePkg()
		if seen[p] {
			continue
		}
		for _, g := range set {
			if !overlaps(g, fam.benchRoot) {
				continue
			}
			seen[p] = true
			out = append(out, p)
			break
		}
	}
	sort.Strings(out)
	if err := checkGatePackages(root, out); err != nil {
		return nil, err
	}
	return out, nil
}

// checkGatePackages refuses a package list the gate cannot measure.
//
// This is what the `nightly` gate set needed and did not have. Every package
// here is printed for `hack/benchgate.sh`, which compiles it and then runs the
// binary in the package's own directory -- so a path that names no directory
// is a gate that fails at its compile step, hours after the change that broke
// it and in a job nobody is watching. Watched, from the derived `nightly`
// list, which contained `./redisclient` because family A12 declares
// `MODDIR=benchmarks`:
//
//	# ./redisclient
//	stat .../redisclient: directory not found
//	FAIL	./redisclient [setup failed]
//	!! gate: head does not build ./redisclient
//
// `make gate GATE=nightly` -- the documented safety net for everything the
// `pr` set leaves out -- had therefore been a setup error since that
// declaration landed, and nothing in `make ci` could notice. The paths are
// repository-relative now (`./benchmarks/redisclient`), which is also what
// hack/gate-if-touched.sh matches a `git diff --name-only` against, and this
// check is the thing that fails when one of them stops naming a directory.
func checkGatePackages(root string, pkgs []string) error {
	var problems []string
	for _, p := range pkgs {
		dir := filepath.Join(root, filepath.FromSlash(strings.TrimPrefix(p, "./")))
		info, err := os.Stat(dir)
		if err != nil || !info.IsDir() {
			problems = append(problems, fmt.Sprintf("%s names no directory under %s, so the gate's compile step fails on it", p, root))
			continue
		}
		if _, ok := moduleOf(root, p); !ok {
			problems = append(problems, fmt.Sprintf("%s is in no Go module inside the repository, so nothing can build it", p))
		}
	}
	if len(problems) > 0 {
		return fmt.Errorf("gate package list: %s", strings.Join(problems, "; "))
	}
	return nil
}

// moduleOf answers the question hack/benchgate.sh has to ask before it can
// compile a gated package: which module is this package in? It is the nearest
// ancestor directory with a go.mod, as a repository-relative path ("." for the
// root module).
//
// The gate used to assume every gated package was in the root module, which is
// the same assumption `hack/bakeoff.sh` made until MODDIR fixed it for the
// campaign. The campaign's fix never reached
// the gate.
func moduleOf(root, pkg string) (string, bool) {
	rel := strings.TrimPrefix(pkg, "./")
	if rel == "." {
		rel = ""
	}
	for {
		if _, err := os.Stat(filepath.Join(root, filepath.FromSlash(rel), "go.mod")); err == nil {
			if rel == "" {
				return ".", true
			}
			return rel, true
		}
		if rel == "" {
			return "", false
		}
		i := strings.LastIndexByte(rel, '/')
		if i < 0 {
			rel = ""
			continue
		}
		rel = rel[:i]
	}
}

// overlaps reports whether a gate-set glob and a benchmark root can name the
// same benchmark: they agree on every segment they both have, one of them being
// a prefix of the other. `BenchmarkMemory/PubSubAck/*/1P1C/…` overlaps the root
// `BenchmarkMemory/PubSubAck`, and the pattern `*` overlaps everything.
func overlaps(glob, root string) bool {
	if glob == "*" {
		return true
	}
	gs, rs := strings.Split(glob, "/"), strings.Split(root, "/")
	n := min(len(gs), len(rs))
	for i := range n {
		if gs[i] == "*" || rs[i] == "*" {
			continue
		}
		if !budgets.Match(gs[i], rs[i]) {
			return false
		}
	}
	return true
}

// GlobsToRegexp converts budgets glob patterns into one anchored -bench
// regular expression.
func GlobsToRegexp(globs []string) string {
	var alts []string
	for _, g := range globs {
		if g == "*" {
			return "."
		}
		var b strings.Builder
		b.WriteString("^")
		segs := strings.Split(g, "/")
		for i, seg := range segs {
			if i > 0 {
				b.WriteString("/")
			}
			if seg == "*" && i == len(segs)-1 {
				b.WriteString(".*")
				continue
			}
			for _, part := range strings.SplitAfter(seg, "*") {
				if strings.HasSuffix(part, "*") {
					b.WriteString(regexp.QuoteMeta(strings.TrimSuffix(part, "*")))
					b.WriteString("[^/]*")
				} else {
					b.WriteString(regexp.QuoteMeta(part))
				}
			}
		}
		if !strings.HasSuffix(g, "*") {
			b.WriteString("(/|$)")
		}
		alts = append(alts, b.String())
	}
	return strings.Join(alts, "|")
}

// modPkg answers the one question every script that compiles a gated package
// has to ask: which module is this package in, and what is its path inside
// that module?
//
// It exists as a subcommand so that there is ONE implementation of the rule.
// `hack/benchgate.sh` and `hack/bench-api.sh` both need it -- the first to
// compile a gate set's packages, the second to store a run of a package that
// is not a bake-off family -- and both used to assume the root module, which
// is how `make gate GATE=nightly` came to be a setup error and why a stored
// run of a nested-module package could not be made at all.
// Two copies of the rule in shell would be two chances to get it wrong.
//
// It prints "<moddir>\t<package inside the module>", both in the form a `cd`
// and a `go test` take:
//
//	./internal/engine        ->  .           ./internal/engine
//	./benchmarks/redisclient ->  benchmarks  ./redisclient
//	./drivers/kafka/kafkago  ->  drivers/kafka/kafkago  .
func modPkg(args []string) error {
	fs := flag.NewFlagSet("modpkg", flag.ContinueOnError)
	root := fs.String("root", "..", "repository root")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 1 {
		return errors.New("modpkg: one repository-relative package path required")
	}
	pkg := fs.Arg(0)
	mod, ok := moduleOf(*root, pkg)
	if !ok {
		return fmt.Errorf("modpkg: %s is in no Go module under %s", pkg, *root)
	}
	rel := strings.TrimPrefix(pkg, "./")
	if mod != "." {
		rel = strings.TrimPrefix(strings.TrimPrefix(rel, mod), "/")
	}
	if rel == "" || rel == "." {
		rel = "."
	} else {
		rel = "./" + rel
	}
	fmt.Printf("%s\t%s\n", mod, rel)
	return nil
}
