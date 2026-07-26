package main

// Mutation testing for the TLA+ specifications.
//
// A specification TLC has never reported a violation on is not evidence: an
// invariant that cannot fail passes for the same reason a correct one does.
// So every invariant and property a configuration checks has a recorded
// mutation — a deliberate defect in the module's next-state relation, or a
// weakened guard — under which TLC must report *that* name violated.
//
// Each mutation runs against a configuration reduced to the one name it
// targets, which is what makes the result attributable: with the whole
// invariant list configured, TLC reports whichever invariant it trips first,
// and an invariant implied by an earlier one in the list would never be seen
// to fail on its own (models/tla/TRACE.md records the two that are).
//
// The mutations live in models/tla/mutations.txt; tools/modelcheck fails when
// a configured invariant has none, so a new invariant cannot be added without
// one.

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

// mutation is one deliberate defect and the invariant it must trip.
type mutation struct {
	Config  string // the .cfg (and module) TLC is run on
	Target  string // the invariant or property that must be reported
	Label   string // distinguishes two mutations of one target
	Module  string // the module the text belongs to; defaults to Config
	Why     string // what the defect is, in one line
	Old     string // the text to replace, which must occur exactly once
	New     string // what to replace it with
	Line    int    // where the block starts, for error messages
	dirName string
}

func (m mutation) name() string {
	if m.Label == "" {
		return m.Config + "/" + m.Target
	}
	return m.Config + "/" + m.Target + "/" + m.Label
}

// result is what one mutation run produced.
type result struct {
	mutation
	reported string        // the invariant name TLC reported
	temporal bool          // TLC reported a temporal-property violation
	trace    []string      // the counterexample's states, first two kept
	took     time.Duration // wall clock of the TLC run
	err      error         // why the mutation did not count as evidence
}

// parseMutations reads the mutation manifest.
//
// The format is deliberately line-oriented rather than a serialisation
// format: the payload is TLA+ source with its own indentation and quoting,
// and every escape a JSON or YAML encoding would need is an opportunity to
// commit a mutation that does not apply.
func parseMutations(path string) ([]mutation, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var out []mutation
	var cur *mutation
	var block *string
	flush := func() error {
		if cur == nil {
			return nil
		}
		if cur.Old == "" {
			return fmt.Errorf("%s:%d: mutation %s has no old: block", path, cur.Line, cur.name())
		}
		if cur.Why == "" {
			return fmt.Errorf("%s:%d: mutation %s has no why: line", path, cur.Line, cur.name())
		}
		if cur.Module == "" {
			cur.Module = cur.Config
		}
		out = append(out, *cur)
		cur = nil
		return nil
	}
	for i, line := range strings.Split(string(data), "\n") {
		switch {
		case strings.HasPrefix(line, "## "):
			if err := flush(); err != nil {
				return nil, err
			}
			block = nil
			head := strings.TrimSpace(strings.TrimPrefix(line, "## "))
			parts := strings.Split(head, "/")
			if len(parts) < 2 || len(parts) > 3 {
				return nil, fmt.Errorf("%s:%d: bad header %q, want Config/Invariant[/label]", path, i+1, head)
			}
			cur = &mutation{Config: parts[0], Target: parts[1], Line: i + 1}
			if len(parts) == 3 {
				cur.Label = parts[2]
			}
		case cur == nil:
			// Preamble and comments between blocks.
		case strings.HasPrefix(line, "  | "), line == "  |":
			if block == nil {
				return nil, fmt.Errorf("%s:%d: text outside an old:/new: block", path, i+1)
			}
			if *block != "" {
				*block += "\n"
			}
			*block += strings.TrimPrefix(strings.TrimPrefix(line, "  | "), "  |")
		case strings.TrimSpace(line) == "old:":
			block = &cur.Old
		case strings.TrimSpace(line) == "new:":
			block = &cur.New
		case strings.HasPrefix(line, "module: "):
			block, cur.Module = nil, strings.TrimSpace(strings.TrimPrefix(line, "module: "))
		case strings.HasPrefix(line, "why: "):
			block, cur.Why = nil, strings.TrimSpace(strings.TrimPrefix(line, "why: "))
		case strings.TrimSpace(line) == "":
			block = nil
		case strings.HasPrefix(line, "#"):
			block = nil
		default:
			return nil, fmt.Errorf("%s:%d: unexpected line %q", path, i+1, line)
		}
	}
	if err := flush(); err != nil {
		return nil, err
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("%s: no mutations", path)
	}
	seen := map[string]bool{}
	for i := range out {
		m := &out[i]
		if seen[m.name()] {
			return nil, fmt.Errorf("%s:%d: duplicate mutation %s", path, m.Line, m.name())
		}
		seen[m.name()] = true
	}
	return out, nil
}

// classify reads a configuration and returns its invariants and properties.
func classify(cfg string) (inv, prop map[string]bool) {
	inv, prop = map[string]bool{}, map[string]bool{}
	target := map[string]bool(nil)
	for _, line := range strings.Split(cfg, "\n") {
		if i := strings.Index(line, `\*`); i >= 0 {
			line = line[:i]
		}
		fields := strings.Fields(line)
		if len(fields) == 0 {
			continue
		}
		switch strings.ToUpper(fields[0]) {
		case "INVARIANT", "INVARIANTS":
			target = inv
			fields = fields[1:]
		case "PROPERTY", "PROPERTIES":
			target = prop
			fields = fields[1:]
		case "SPECIFICATION", "INIT", "NEXT", "CONSTANT", "CONSTANTS", "CONSTRAINT",
			"CONSTRAINTS", "ACTION_CONSTRAINT", "SYMMETRY", "VIEW", "CHECK_DEADLOCK",
			"ALIAS", "POSTCONDITION":
			target = nil
			continue
		}
		if target == nil {
			continue
		}
		for _, f := range fields {
			target[f] = true
		}
	}
	return inv, prop
}

// reduceConfig returns cfg with its invariant and property lists replaced by
// the single name under test, so that a reported violation can only be the
// one the mutation aimed at.
func reduceConfig(cfg, target string, property bool) string {
	var out []string
	skipping := false
	for _, line := range strings.Split(cfg, "\n") {
		bare := line
		if i := strings.Index(bare, `\*`); i >= 0 {
			bare = bare[:i]
		}
		fields := strings.Fields(bare)
		if len(fields) > 0 {
			switch strings.ToUpper(fields[0]) {
			case "INVARIANT", "INVARIANTS", "PROPERTY", "PROPERTIES":
				skipping = true
				continue
			case "SPECIFICATION", "INIT", "NEXT", "CONSTANT", "CONSTANTS", "CONSTRAINT",
				"CONSTRAINTS", "ACTION_CONSTRAINT", "SYMMETRY", "VIEW", "CHECK_DEADLOCK",
				"ALIAS", "POSTCONDITION":
				skipping = false
			}
		}
		if skipping {
			continue
		}
		out = append(out, line)
	}
	kw := "INVARIANT"
	if property {
		kw = "PROPERTY"
	}
	out = append(out, kw+" "+target, "")
	return strings.Join(out, "\n")
}

var (
	invViolatedRE = regexp.MustCompile(`(?:Invariant|Action property|property) ([A-Za-z_][A-Za-z0-9_]*) is violated`)
	temporalRE    = regexp.MustCompile(`Temporal properties were violated`)
	stateRE       = regexp.MustCompile(`^State (\d+): `)
)

// runMutation applies one mutation to a copy of the specification directory
// and runs TLC on it.
func runMutation(jar, dir, work string, m mutation, xmx string) result {
	res := result{mutation: m}
	md := filepath.Join(work, m.dirName)
	if err := os.MkdirAll(md, 0o755); err != nil {
		res.err = err
		return res
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		res.err = err
		return res
	}
	var cfgData string
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".tla") {
			continue
		}
		b, err := os.ReadFile(filepath.Join(dir, e.Name()))
		if err != nil {
			res.err = err
			return res
		}
		text := string(b)
		if e.Name() == m.Module+".tla" {
			if n := strings.Count(text, m.Old); n != 1 {
				res.err = fmt.Errorf("the old: text occurs %d times in %s.tla, want exactly once", n, m.Module)
				return res
			}
			text = strings.Replace(text, m.Old, m.New, 1)
		}
		if err := os.WriteFile(filepath.Join(md, e.Name()), []byte(text), 0o644); err != nil {
			res.err = err
			return res
		}
	}
	b, err := os.ReadFile(filepath.Join(dir, m.Config+".cfg"))
	if err != nil {
		res.err = err
		return res
	}
	inv, prop := classify(string(b))
	if !inv[m.Target] && !prop[m.Target] {
		res.err = fmt.Errorf("%s.cfg checks neither an invariant nor a property called %s", m.Config, m.Target)
		return res
	}
	cfgData = reduceConfig(string(b), m.Target, prop[m.Target])
	if err := os.WriteFile(filepath.Join(md, m.Config+".cfg"), []byte(cfgData), 0o644); err != nil {
		res.err = err
		return res
	}

	// -deadlock switches TLC's deadlock check off: a mutation is a
	// deliberately broken specification and may well strand itself, and a
	// deadlock report would be an error that is not the evidence asked for.
	cmd := exec.Command("java",
		"-XX:+UseParallelGC", activeProcessorCount(), "-Xmx"+xmx, "-cp", jar, "tlc2.TLC",
		"-workers", "1", "-metadir", "states", "-deadlock", "-cleanup",
		"-config", m.Config+".cfg", m.Config+".tla")
	cmd.Dir = md
	start := time.Now()
	out, runErr := cmd.CombinedOutput()
	res.took = time.Since(start).Round(time.Millisecond)
	if err := os.WriteFile(filepath.Join(md, "tlc.log"), out, 0o644); err != nil {
		res.err = err
		return res
	}
	text := string(out)
	if mm := invViolatedRE.FindStringSubmatch(text); mm != nil {
		res.reported = mm[1]
	}
	res.temporal = temporalRE.MatchString(text)
	res.trace = firstStates(text, 2)
	switch {
	case runErr == nil:
		res.err = fmt.Errorf("TLC found no violation: the mutation is not caught by %s", m.Target)
	case res.reported != "" && res.reported != m.Target:
		res.err = fmt.Errorf("TLC reported %s violated, want %s", res.reported, m.Target)
	case res.reported == "" && !res.temporal:
		res.err = fmt.Errorf("TLC failed without reporting a violation (see %s)", filepath.Join(md, "tlc.log"))
	case len(res.trace) == 0:
		res.err = errors.New("TLC reported a violation with no counterexample trace")
	}
	return res
}

// firstStates returns the first n states of a TLC counterexample, each as one
// block of text.
func firstStates(out string, n int) []string {
	var states []string
	var cur []string
	for _, line := range strings.Split(out, "\n") {
		if m := stateRE.FindStringSubmatch(line); m != nil {
			if len(cur) > 0 {
				states = append(states, strings.TrimRight(strings.Join(cur, "\n"), "\n"))
				cur = nil
			}
			num, _ := strconv.Atoi(m[1])
			if num > n {
				break
			}
			cur = append(cur, line)
			continue
		}
		if cur == nil {
			continue
		}
		if strings.HasPrefix(line, "Error: ") || strings.HasPrefix(line, "The number of states") {
			break
		}
		cur = append(cur, line)
	}
	if len(cur) > 0 {
		states = append(states, strings.TrimRight(strings.Join(cur, "\n"), "\n"))
	}
	return states
}

// mutate runs the manifest's mutations and reports the ones that produced no
// evidence.
func mutate(jar string, args []string) error {
	fs := flag.NewFlagSet("mutate", flag.ContinueOnError)
	dir := fs.String("dir", "models/tla", "directory holding the specs")
	manifest := fs.String("manifest", "", "mutation manifest (default <dir>/mutations.txt)")
	only := fs.String("only", "", "run only mutations whose name contains this substring")
	xmx := fs.String("xmx", "1g", "JVM heap per mutation")
	record := fs.String("record", "", "write the counterexamples to this file")
	list := fs.Bool("list", false, "list the mutations and exit")
	if err := fs.Parse(args); err != nil {
		return err
	}
	path := *manifest
	if path == "" {
		path = filepath.Join(*dir, "mutations.txt")
	}
	muts, err := parseMutations(path)
	if err != nil {
		return err
	}
	sort.SliceStable(muts, func(i, j int) bool { return muts[i].Config < muts[j].Config })
	if *list {
		for i := range muts {
			fmt.Printf("%-44s %s\n", muts[i].name(), muts[i].Why)
		}
		fmt.Fprintf(os.Stderr, "tlamutate: %d mutation(s)\n", len(muts))
		return nil
	}
	// One work directory per run, not a fixed path: two mutate runs at once
	// used to share `states/mutants`, and the second one's RemoveAll deleted
	// the first one's mutants from under it -- which showed up as two runs
	// "failing" for want of a log file rather than as what it was.
	if err := os.MkdirAll(filepath.Join(*dir, "states"), 0o755); err != nil {
		return err
	}
	work, err := os.MkdirTemp(filepath.Join(*dir, "states"), "mutants-")
	if err != nil {
		return err
	}
	var results []result
	failed := 0
	start := time.Now()
	for i := range muts {
		m := muts[i]
		if *only != "" && !strings.Contains(m.name(), *only) {
			continue
		}
		m.dirName = fmt.Sprintf("%03d-%s-%s", i, m.Config, m.Target)
		if m.Label != "" {
			m.dirName += "-" + m.Label
		}
		res := runMutation(jar, *dir, work, m, *xmx)
		results = append(results, res)
		if res.err != nil {
			failed++
			fmt.Fprintf(os.Stderr, "!! %s: %v\n", m.name(), res.err)
			continue
		}
		what := res.reported
		if what == "" {
			what = m.Target + " (temporal)"
		}
		fmt.Fprintf(os.Stderr, "   %-44s %s violated in %s\n", m.name(), what, res.took)
	}
	if len(results) == 0 {
		return fmt.Errorf("no mutation matched -only %q", *only)
	}
	if *record != "" {
		if err := writeRecord(*record, results); err != nil {
			return err
		}
		fmt.Fprintf(os.Stderr, "tlamutate: wrote %s\n", *record)
	}
	fmt.Fprintf(os.Stderr, "tlamutate: %d mutation(s), %d without evidence, %s\n",
		len(results), failed, time.Since(start).Round(time.Millisecond))
	if failed > 0 {
		fmt.Fprintf(os.Stderr, "tlamutate: the mutated specifications are under %s\n", work)
		return fmt.Errorf("%d mutation(s) produced no counterexample", failed)
	}
	return os.RemoveAll(work)
}

// writeRecord writes the counterexamples as markdown: what was broken, and
// the first two states TLC reported.
func writeRecord(path string, results []result) error {
	var b strings.Builder
	b.WriteString(`# What TLC reports when a specification is wrong

Generated by ` + "`go run ./tools/tlatools mutate -record models/tla/MUTATIONS.md`" + `.

Every invariant and property ` + "`models/tla/*.cfg`" + ` checks has a mutation in
` + "`models/tla/mutations.txt`" + `: a deliberate defect in the module, under a
configuration reduced to the one name it targets, so that the violation TLC
reports is attributable to that name and not to whichever invariant the list
happens to trip first. ` + "`make tla-mutate`" + ` runs them and fails when a
mutation is *not* caught -- an invariant no defect can falsify is decoration,
and this file is the evidence that none of ours is.

The traces below are one run's counterexamples, kept to the first two states
because that is what a reader needs to see the defect. They are a record, not
a golden file: TLC may reach an equally valid counterexample by another route.

`)
	for i := range results {
		r := &results[i]
		fmt.Fprintf(&b, "## %s\n\n%s\n\n", r.name(), r.Why)
		reported := r.reported
		if reported == "" {
			reported = r.Target + " (temporal property)"
		}
		fmt.Fprintf(&b, "Mutation of `%s.tla`:\n\n```diff\n", r.Module)
		for _, line := range strings.Split(r.Old, "\n") {
			fmt.Fprintf(&b, "-%s\n", line)
		}
		for _, line := range strings.Split(r.New, "\n") {
			fmt.Fprintf(&b, "+%s\n", line)
		}
		fmt.Fprintf(&b, "```\n\nTLC reports **%s** violated (%s):\n\n```\n", reported, r.took)
		for _, s := range r.trace {
			b.WriteString(s)
			b.WriteString("\n\n")
		}
		b.WriteString("```\n\n")
	}
	return os.WriteFile(path, []byte(b.String()), 0o644)
}
