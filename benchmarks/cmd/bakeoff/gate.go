package main

import (
	"errors"
	"flag"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"regexp"
	"slices"
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
	for _, g := range expandGlob(glob) {
		if overlapsOne(g, root) {
			return true
		}
	}
	return false
}

// overlapsOne is overlaps for a glob with no {a,b} alternation in it.
func overlapsOne(glob, root string) bool {
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

// expandGlob is the globs a pattern's {a,b} alternations stand for, the
// expansion budgets.Match applies. A pattern whose brace is not closed stands
// for itself: Match selects nothing with it, and `make budgets-lint` refuses
// it for that.
func expandGlob(glob string) []string {
	alts, err := budgets.ExpandBraces(glob)
	if err != nil {
		return []string{glob}
	}
	return alts
}

// GlobsToRegexp converts budgets glob patterns into one anchored -bench
// regular expression.
//
// A {a,b} alternation becomes one alternative of the expression per pattern
// it expands to, so the gate measures what budgets.Match, and with it
// `make budgets-lint` and benchgate's InGateSet, says the pattern names.
// Quoted as literal braces it selected nothing. `go test` splits a
// -bench expression at its top-level `|` before it splits each alternative
// at `/`, so an alternative may differ from another in any segment.
func GlobsToRegexp(globs []string) string {
	expanded := make([]string, 0, len(globs))
	for _, g := range globs {
		expanded = append(expanded, expandGlob(g)...)
	}
	var alts []string
	for _, g := range expanded {
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
					continue
				}
				b.WriteString(regexp.QuoteMeta(part))
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
	fmt.Printf("%s\t%s\n", mod, packageInModule(mod, pkg))
	return nil
}

// packageInModule is a repository-relative package path as a `go test` run
// in its module's directory names it.
func packageInModule(mod, pkg string) string {
	rel := strings.TrimPrefix(pkg, "./")
	if mod != "." {
		rel = strings.TrimPrefix(strings.TrimPrefix(rel, mod), "/")
	}
	switch rel {
	case "", ".":
		return "."
	default:
		return "./" + rel
	}
}

// gateNames prints, one to a line, every benchmark name a -benchtime=1x pass
// over the in-process gate sets' packages reports, which are the names the
// gate can measure, then as `campaign <name>` the rows a §8.10 target needs
// from a family's own run. `make budgets-lint` hands the list to `benchgate
// -lint -names`. The public tree stores no campaign, so without a listing a
// mistyped `match:` in budgets.yaml passed the lint and then matched nothing
// at the gate, which reads the same as a budget that is met.
//
// Each name is followed by the gate sets whose packages report it, comma
// separated: `BenchmarkHeaders/Get/keys=registry/n=8 nightly,pr`. The gate
// builds and runs a set's own packages only (`gate-pattern -packages`), so a
// set's patterns are checked against its own names. One list for every set
// let a `pr` pattern that only a `nightly` package can measure pass the lint,
// and the gate never measured it.
//
// The gate sets a kind: ratio budget names are left out. Their benchmarks
// measure a driver against its raw client on a live broker, and a listing
// must not start one.
//
// Each module's packages go to one `go test`, which builds them in parallel
// and runs the benchmark binaries one at a time. The listing measures nothing:
// one iteration of each benchmark is what makes `go test` print its name.
func gateNames(args []string) error {
	fs := flag.NewFlagSet("gate-names", flag.ContinueOnError)
	path := fs.String("budgets", "budgets.yaml", "budgets file")
	root := fs.String("root", "..", "repository root")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		return errors.New("gate-names takes no arguments")
	}
	cfg, err := budgets.Load(*path)
	if err != nil {
		return err
	}
	ratio := cfg.RatioGateSets()
	byModule := map[string][]string{}
	// setsOf is each package's gate sets, the sets whose gate builds it.
	setsOf := map[string][]string{}
	for _, set := range slices.Sorted(maps.Keys(cfg.GateSets)) {
		if ratio[set] {
			continue
		}
		pkgs, err := gatePackages(*root, cfg.GateSets[set], cfg.GatePackages[set])
		if err != nil {
			return err
		}
		for _, p := range pkgs {
			if _, ok := setsOf[p]; !ok {
				// gatePackages has already refused a package in no module.
				mod, _ := moduleOf(*root, p)
				byModule[mod] = append(byModule[mod], p)
			}
			setsOf[p] = append(setsOf[p], set)
		}
	}
	// names is each listed benchmark's gate sets.
	names := map[string]map[string]bool{}
	for _, mod := range slices.Sorted(maps.Keys(byModule)) {
		dir := filepath.Join(*root, filepath.FromSlash(mod))
		modPath, err := modulePath(dir)
		if err != nil {
			return err
		}
		// `go test` names a package by its import path in the `pkg:` line it
		// prints before the package's benchmarks.
		pkgOf := map[string]string{}
		args := []string{"-bench", "."}
		for _, p := range byModule[mod] {
			inMod := packageInModule(mod, p)
			pkgOf[importPath(modPath, inMod)] = p
			args = append(args, inMod)
		}
		listed, err := listNames(dir, "", args...)
		if err != nil {
			return err
		}
		for imp, ns := range listed {
			p, ok := pkgOf[imp]
			if !ok {
				return fmt.Errorf("gate-names: the listing in %s printed %d benchmark(s) under package %q, which is not one of the gate sets' packages %v, so it cannot say which gate set measures them",
					dir, len(ns), imp, byModule[mod])
			}
			for _, n := range ns {
				if names[n] == nil {
					names[n] = map[string]bool{}
				}
				for _, set := range setsOf[p] {
					names[n][set] = true
				}
			}
		}
	}
	if len(names) == 0 {
		return errors.New("gate-names: the -benchtime=1x listing reported no benchmark, so there is nothing to check a pattern against")
	}
	// A §8.10 target is a campaign figure, read off a family's own run at its
	// declared grid and arguments, and the gate never sets either: the
	// scheduler target is read at n=1000000, which only IQMQ_BENCH_MATRIX=full
	// measures. So a target the default pass leaves unmatched is listed again
	// at the grid and arguments of each family whose root it overlaps,
	// selecting its own rows only (the whole of the scheduler's full grid takes
	// 40 s to set up, and the target needs three rows of it). Those rows are
	// printed as `campaign <name>`, and `benchgate -lint` matches only targets
	// against them, so a budget or gate-set pattern that names them still
	// fails: the gate could never measure it.
	campaign := map[string]bool{}
	if targets := unmatchedTargets(cfg, slices.Collect(maps.Keys(names))); len(targets) > 0 {
		fams, err := readBakeoffEnvs(*root)
		if err != nil {
			return err
		}
		for i := range fams {
			fam := &fams[i]
			if _, gated := setsOf[fam.gatePkg()]; !gated || fam.removed != "" || fam.benchRoot == "" || listedByDefault(fam) {
				continue
			}
			for _, p := range targets {
				if !overlaps(p, fam.benchRoot) {
					continue
				}
				args := append([]string{"-bench", GlobsToRegexp([]string{p})}, fam.extraArgs...)
				listed, err := listNames(fam.root(*root), fam.matrix, append(args, fam.pkg)...)
				if err != nil {
					return err
				}
				for _, ns := range listed {
					for _, n := range ns {
						campaign[n] = true
					}
				}
			}
		}
	}
	for _, n := range slices.Sorted(maps.Keys(names)) {
		fmt.Println(n + " " + strings.Join(slices.Sorted(maps.Keys(names[n])), ","))
	}
	for _, n := range slices.Sorted(maps.Keys(campaign)) {
		if names[n] == nil {
			fmt.Println("campaign " + n)
		}
	}
	return nil
}

// listedByDefault reports whether the default pass already lists every row a
// family's own run produces: it declares no grid but the default and no extra
// `go test` arguments.
func listedByDefault(fam *bakeoffFamily) bool {
	return (fam.matrix == "" || fam.matrix == "default") && len(fam.extraArgs) == 0
}

// listNames runs one -benchtime=1x `go test` in dir and returns every
// benchmark name it prints, by the import path of the package that printed it
// (the `pkg:` line `go test` writes before a package's benchmarks). matrix,
// when set, is the IQMQ_BENCH_MATRIX grid.
//
// Every IQMQ_ variable of the caller's environment is dropped first, so the
// listing is the same on every host: an exported IQMQ_BENCH_MATRIX=full
// widened the default pass to 53 s, and an exported IQMQ_KAFKA_ADDR or
// IQMQ_KAFKAGO_BENCH made drivers/kafka/kafkago's benchmark dial a broker or
// start one in process.
func listNames(dir, matrix string, args ...string) (map[string][]string, error) {
	cmd := goCmd(dir, append([]string{"test", "-run", "^$", "-benchtime", "1x", "-count=1"}, args...)...)
	cmd.Env = slices.DeleteFunc(cmd.Env, func(kv string) bool { return strings.HasPrefix(kv, "IQMQ_") })
	if matrix != "" {
		cmd.Env = append(cmd.Env, "IQMQ_BENCH_MATRIX="+matrix)
	}
	out, err := cmd.CombinedOutput()
	if err != nil {
		return nil, childFailure(dir, "the -benchtime=1x listing", out, err)
	}
	byPkg := map[string][]string{}
	pkg := ""
	for _, line := range strings.Split(string(out), "\n") {
		if p, ok := strings.CutPrefix(line, "pkg: "); ok {
			pkg = strings.TrimSpace(p)
			continue
		}
		if strings.HasPrefix(line, "Benchmark") {
			byPkg[pkg] = append(byPkg[pkg], trimProcs(strings.Fields(line)[0]))
		}
	}
	return byPkg, nil
}

// unmatchedTargets returns the budgets.yaml targets no listed name matches.
func unmatchedTargets(cfg *budgets.Config, names []string) []string {
	var out []string
	for _, t := range cfg.Targets {
		if !anyMatch(t.Match, names) {
			out = append(out, t.Match)
		}
	}
	return out
}

// anyMatch reports whether any listed name matches pattern.
func anyMatch(pattern string, names []string) bool {
	for _, n := range names {
		if budgets.Match(pattern, n) {
			return true
		}
	}
	return false
}
