package main

import (
	"compress/gzip"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"

	"github.com/iqhive/iqmq/benchmarks/internal/budgets"
)

// lint holds benchmarks/budgets.yaml to the two things a reader of a gate
// verdict depends on and that nothing used to check:
//
//   - every budget says where its number comes from, and a measured ceiling
//     agrees with the arithmetic it records (budgets.Config.Validate);
//   - every pattern in the file matches a benchmark the campaign actually
//     measured, and no absolute ns/op ceiling names an arm its family did not
//     choose.
//
// The second rule is the one with history. Three of the eight seeded budgets
// and all five §8.10 targets matched no benchmark name in the tree -- the two
// `wire` ceilings were written as four-segment patterns against five-segment
// names, the PubSubAck allocation ceiling spelled its arms
// `{owned,reuse}` where the benchmark carries `owned` as a kind and `reuse=on`
// as a dimension, and the scheduler target spelled n=1e6 where the benchmark
// says n=1000000. A budget that matches nothing reports nothing, passes
// forever, and is indistinguishable from a budget that is met.
//
// The names come from a stored campaign under -results, or from -names: every
// benchmark name a -benchtime=1x pass over the in-process gate sets' packages
// reports, which are the names the gate can measure, plus the `campaign` rows
// a §8.10 target needs from a family's own run (`bakeoff gate-names`, which
// `make budgets-lint` runs). The public tree stores no campaign, so before
// -names every pattern went unchecked on a fresh clone and a mistyped
// `match:` passed. A target is checked against both kinds of row, every other
// pattern against the gate's alone. A listed name carries the gate sets whose
// packages report it, because the gate builds and runs a set's own packages
// only: a gate set's patterns are checked against that set's names, and the
// `unevaluated` count against the `pr` set's. One list for every set let a
// `pr` pattern that only a `nightly` package can measure pass, with every
// budget over its rows counted as evaluated. A listing answers for every
// pattern but a ratio's: a kind: ratio budget, a waiver of one, and the gate
// sets ratio budgets name measure a driver against its raw client on a live
// broker, which a listing does not start, so those are checked against a
// stored campaign alone and counted as unchecked without one.
func lint(cfgPath, resultsDir, scorecardDir, namesPath string) int {
	cfg, err := budgets.Load(cfgPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, "benchgate -lint:", err)
		return 2
	}
	problems := cfg.Validate()

	var listed, campaignRows []string
	var setNames map[string][]string
	if namesPath != "" {
		listed, setNames, campaignRows, err = readNames(namesPath)
		if err != nil {
			fmt.Fprintln(os.Stderr, "benchgate -lint:", err)
			return 2
		}
		if len(listed) == 0 {
			fmt.Fprintf(os.Stderr, "benchgate -lint: %s holds no benchmark names to check the patterns against\n", namesPath)
			return 2
		}
	}

	stored, runs, err := inventory(resultsDir)
	switch {
	case errors.Is(err, fs.ErrNotExist) && listed == nil:
		// The stored campaign was not brought over from the Bitbucket
		// repository, because it held internal, project-specific information,
		// and a fresh clone has no local runs. The file is still checked on its
		// own; the patterns are checked against measured names once a campaign
		// is stored here again, or against a listing given with -names.
		if len(problems) > 0 {
			sort.Strings(problems)
			for _, p := range problems {
				fmt.Fprintln(os.Stderr, " -", p)
			}
			fmt.Fprintf(os.Stderr, "!! benchgate -lint: %d problem(s) in %s\n", len(problems), cfgPath)
			return 1
		}
		fmt.Fprintf(os.Stderr, "benchgate -lint: ok, %d budget(s) valid; %s does not exist and no -names listing was given, so no pattern was checked against a benchmark name\n", len(cfg.Budgets), resultsDir)
		return 0
	case errors.Is(err, fs.ErrNotExist):
		stored, runs = nil, nil
	case err != nil:
		fmt.Fprintln(os.Stderr, "benchgate -lint:", err)
		return 2
	case len(stored) == 0:
		fmt.Fprintf(os.Stderr, "benchgate -lint: %s holds no measured benchmark names to check the patterns against\n", resultsDir)
		return 2
	}

	// names is what every pattern a listing can answer for is checked
	// against: the listing when there is one, because it is the tree as it is
	// now, and the stored campaign otherwise.
	// targetNames adds the listing's campaign rows, which a target alone may
	// match.
	names, targetNames, namesFrom := stored, stored, resultsDir
	if listed != nil {
		names, targetNames, namesFrom = listed, append(slices.Clip(listed), campaignRows...), "the -benchtime=1x listing"
	}
	// gateNames is what an in-process gate set's gate can measure: the names
	// the listing has from that set's own packages, which are all the gate
	// builds, and a stored campaign's names when there is no listing.
	gateNames := func(set string) ([]string, string) {
		if listed == nil {
			return names, namesFrom
		}
		return setNames[set], namesFrom + " from the " + set + " gate set's packages"
	}
	ratioSets := cfg.RatioGateSets()
	unchecked := 0

	check := func(what, pattern string, in []string, from string) {
		for _, n := range in {
			if budgets.Match(pattern, n) {
				return
			}
		}
		problems = append(problems, fmt.Sprintf("%s %s matches none of the %d benchmark names in %s", what, pattern, len(in), from))
	}
	// checkStored is check for a pattern only a stored campaign can answer.
	checkStored := func(what, pattern string) {
		if stored == nil {
			unchecked++
			return
		}
		check(what, pattern, stored, resultsDir)
	}
	// A ratio pattern has to match EXACTLY ONE measured name. Zero is the
	// A25.9 failure -- a budget that reports nothing and passes for ever -- and
	// more than one leaves the ratio ambiguous, which is not a thing the
	// verdict may resolve by picking: whichever of two rows it chose would be
	// a silent decision about what §10.2's figure is being read on.
	checkOne := func(what, pattern string) {
		if stored == nil {
			unchecked++
			return
		}
		n := 0
		for _, name := range stored {
			if budgets.Match(pattern, name) {
				n++
			}
		}
		switch {
		case n == 0:
			problems = append(problems, fmt.Sprintf("%s %s matches none of the %d benchmark names in %s", what, pattern, len(stored), resultsDir))
		case n > 1:
			problems = append(problems, fmt.Sprintf("%s %s matches %d measured benchmark names; a ratio needs exactly one on each side", what, pattern, n))
		}
	}
	for i := range cfg.Budgets {
		b := &cfg.Budgets[i]
		switch {
		case b.IsRatio():
			checkOne("budget "+b.ID()+" numerator", b.Numerator)
			checkOne("budget "+b.ID()+" denominator", b.Denominator)
		default:
			check("budget", b.Match, names, namesFrom)
		}
		if b.Run != "" && stored != nil && !runs[b.Run] {
			problems = append(problems, fmt.Sprintf("budget %s: run %s is not a stored campaign run", b.ID(), b.Run))
		}
	}
	for _, t := range cfg.Targets {
		check("target", t.Match, targetNames, namesFrom)
	}
	for _, w := range cfg.Waivers {
		if cfg.WaivesARatio(w.Match) {
			checkStored("waiver", w.Match)
			continue
		}
		check("waiver", w.Match, names, namesFrom)
	}
	for _, set := range sortedKeys(cfg.GateSets) {
		for _, p := range cfg.GateSets[set] {
			if ratioSets[set] {
				checkStored("gate set "+set+" pattern", p)
				continue
			}
			in, from := gateNames(set)
			check("gate set "+set+" pattern", p, in, from)
		}
	}

	// A `kind: specification` ceiling declared over rows the `pr` gate set
	// does not select is enforced when somebody measures it and asserted in
	// prose when nobody does, which is how §10.2's runner-dispatch budget
	// came to carry the note "met: worst median 240.95 ns over two full
	// runs" while the arm it names was 3.2x over the ceiling on three of its
	// nine rows.
	//
	// `pr` and not "every gate set": `nightly: ["*"]` selects every
	// benchmark in the tree, so "outside every gate set" is never true and a
	// check keyed on it could not fail. `pr` is the set `make
	// gate-if-touched` runs before landing and the only one any job runs, so
	// it is the set that decides whether a ceiling is evaluated at all.
	//
	// A row is evaluated when a `pr` pattern selects it AND it is in a package
	// the `pr` gate builds: a pattern alone selects nothing in a package the
	// gate never compiles.
	pr := cfg.GateSets["pr"]
	prNames, _ := gateNames("pr")
	inPR := make(map[string]bool, len(prNames))
	for _, n := range prNames {
		inPR[n] = true
	}
	for i := range cfg.Budgets {
		b := &cfg.Budgets[i]
		if b.Kind != budgets.SpecificationKind || b.IsRatio() {
			continue
		}
		matched, unevaluated := 0, 0
		for _, n := range names {
			if !budgets.Match(b.Match, n) {
				continue
			}
			matched++
			seen := false
			for _, p := range pr {
				if inPR[n] && budgets.Match(p, n) {
					seen = true
					break
				}
			}
			if !seen {
				unevaluated++
			}
		}
		switch {
		case unevaluated == 0 && b.UnevaluatedSet:
			problems = append(problems, fmt.Sprintf("budget %s declares unevaluated: %d and the `pr` gate set selects all %d of its rows",
				b.ID(), b.Unevaluated, matched))
		case unevaluated == 0:
		case !b.UnevaluatedSet:
			problems = append(problems, fmt.Sprintf("budget %s is a §10.2 ceiling over %d row(s) and the `pr` gate set selects only %d of them; "+
				"declare `unevaluated: %d` so the file says which part of it no job evaluates (assumption C02.10)",
				b.ID(), matched, matched-unevaluated, unevaluated))
		case b.Unevaluated != unevaluated:
			problems = append(problems, fmt.Sprintf("budget %s declares unevaluated: %d and the `pr` gate set leaves %d of its %d row(s) unevaluated",
				b.ID(), b.Unevaluated, unevaluated, matched))
		}
	}

	losers, err := nonDefaultArms(scorecardDir)
	if errors.Is(err, fs.ErrNotExist) {
		// The per-family scorecards were not brought over from the Bitbucket
		// repository either, so no arm is known to have lost its bake-off.
		fmt.Fprintf(os.Stderr, "benchgate -lint: %s does not exist, so no budget was checked for naming a non-default arm\n", scorecardDir)
		losers, err = map[string]string{}, nil
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "benchgate -lint:", err)
		return 2
	}
	for i := range cfg.Budgets {
		b := &cfg.Budgets[i]
		// Only ns/op ceilings: §10.2's per-operation figures are written
		// against the implementation that ships, while its allocation figures
		// are written against a memory *mode* ("0 with reuse") and legitimately
		// name the owned and borrowed arms whichever one is the default.
		if !b.NsOpSet {
			continue
		}
		for _, seg := range strings.Split(b.Match, "/") {
			fam, ok := losers[seg]
			if !ok {
				continue
			}
			problems = append(problems, fmt.Sprintf("budget %s: %q is a retained profile of family %s, not its bound default -- an absolute ns/op ceiling binds the arm that ships (assumption A25.6)",
				b.Match, seg, fam))
		}
	}

	if len(problems) > 0 {
		sort.Strings(problems)
		for _, p := range problems {
			fmt.Fprintln(os.Stderr, " -", p)
		}
		// The `!!` marker, for the same reason bakeoff's report carries it:
		// "green" is a `make ci` log with no FAIL and no !! line in it, and
		// `make budgets-lint` runs this through `go run`, which flattens the
		// status.
		fmt.Fprintf(os.Stderr, "!! benchgate -lint: %d problem(s) in %s\n", len(problems), cfgPath)
		return 1
	}
	ratios := 0
	for i := range cfg.Budgets {
		if cfg.Budgets[i].IsRatio() {
			ratios++
		}
	}
	fmt.Fprintf(os.Stderr, "benchgate -lint: ok, %d budget(s) (%d of them ratios), %d target(s), %d waiver(s) and %d gate set(s) against %d benchmark name(s) in %s\n",
		len(cfg.Budgets), ratios, len(cfg.Targets), len(cfg.Waivers), len(cfg.GateSets), len(names), namesFrom)
	if len(campaignRows) > 0 {
		fmt.Fprintf(os.Stderr, "benchgate -lint: %d campaign row(s) from a family's own grid, for the targets only\n", len(campaignRows))
	}
	if unchecked > 0 {
		fmt.Fprintf(os.Stderr, "benchgate -lint: %d ratio pattern(s) were not checked: their benchmarks need a live broker, which a listing does not start, and %s does not exist\n",
			unchecked, resultsDir)
	}
	return 0
}

// readNames reads a listing as `bakeoff gate-names` prints it: one benchmark
// name the gate can measure to a line, followed by the gate sets whose
// packages report it (`<name> nightly,pr`), and a `campaign <name>` line for
// each row only a family's own run produces. It returns every gate name, the
// names of each gate set, and the campaign rows. A name with no gate set is
// refused: the lint cannot tell which set's gate measures it, and reading it
// as every set's is the defect the sets are there to close.
func readNames(path string) (gate []string, sets map[string][]string, campaign []string, err error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, nil, nil, err
	}
	sets = map[string][]string{}
	for i, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if name, ok := strings.CutPrefix(line, "campaign "); ok {
			campaign = append(campaign, strings.TrimSpace(name))
			continue
		}
		if line == "" {
			continue
		}
		name, in, ok := strings.Cut(line, " ")
		in = strings.TrimSpace(in)
		if !ok || in == "" {
			return nil, nil, nil, fmt.Errorf("%s:%d: %s names no gate set; list the names with `bakeoff gate-names`, which says which gate sets' packages report each one", path, i+1, name)
		}
		gate = append(gate, name)
		for _, set := range strings.Split(in, ",") {
			sets[set] = append(sets[set], name)
		}
	}
	return gate, sets, campaign, nil
}

// inventory collects every benchmark name the stored campaign measured, taking
// the newest run of each family, and the set of run ids it read.
func inventory(dir string) ([]string, map[string]bool, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, nil, err
	}
	// Run ids are <timestamp>-<family> and sort by timestamp, so the last one
	// seen for a family is its newest.
	newest := map[string]string{}
	var ids []string
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		id := e.Name()
		ids = append(ids, id)
		i := strings.LastIndexByte(id, '-')
		if i < 0 {
			continue
		}
		newest[id[i+1:]] = id
	}
	sort.Strings(ids)
	runs := make(map[string]bool, len(ids))
	for _, id := range ids {
		runs[id] = true
	}
	seen := map[string]bool{}
	for _, fam := range sortedKeys(newest) {
		path := filepath.Join(dir, newest[fam], "raw.txt.gz")
		f, err := os.Open(path)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return nil, nil, err
		}
		zr, err := gzip.NewReader(f)
		if err != nil {
			_ = f.Close()
			return nil, nil, fmt.Errorf("%s: %w", path, err)
		}
		data, err := io.ReadAll(zr)
		_ = zr.Close()
		_ = f.Close()
		if err != nil {
			return nil, nil, fmt.Errorf("%s: %w", path, err)
		}
		for _, line := range strings.Split(string(data), "\n") {
			if !strings.HasPrefix(line, "Benchmark") {
				continue
			}
			name := line
			if i := strings.IndexAny(line, " \t"); i >= 0 {
				name = line[:i]
			}
			seen[name] = true
		}
	}
	return sortedKeys(seen), runs, nil
}

// nonDefaultArms maps every kind slug a family measured and did not choose to
// that family's id, read from the stored per-family scorecards.
func nonDefaultArms(dir string) (map[string]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	out := map[string]string{}
	winners := map[string]bool{}
	type card struct {
		Family string `json:"family"`
		Winner string `json:"winner"`
		Kinds  []struct {
			Kind string `json:"kind"`
		} `json:"kinds"`
	}
	var cards []card
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".json") {
			continue
		}
		data, err := os.ReadFile(filepath.Join(dir, e.Name()))
		if err != nil {
			return nil, err
		}
		var c card
		if err := json.Unmarshal(data, &c); err != nil {
			return nil, fmt.Errorf("%s: %w", e.Name(), err)
		}
		// A family with no winner decided nothing, so it says nothing about
		// which of its arms ships. Family A10 is one: it shares A1's kind list
		// and is measured under a build tag.
		if c.Winner == "" {
			continue
		}
		cards = append(cards, c)
		winners[c.Winner] = true
	}
	for _, c := range cards {
		for _, k := range c.Kinds {
			// A slug another family chose is somebody's default, so it is not
			// reported: family A7's `atomic` and family A9's `atomic` are
			// different arms that happen to share a name.
			if k.Kind == c.Winner || winners[k.Kind] {
				continue
			}
			out[k.Kind] = c.Family
		}
	}
	return out, nil
}

func sortedKeys[V any](m map[string]V) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
