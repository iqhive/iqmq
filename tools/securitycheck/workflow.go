package main

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
)

// workflowStep is one step of a GitHub workflow as these checks read it: the
// shell lines its `run:` key runs, and why the step might not hold the
// workflow to them.
type workflowStep struct {
	// commands are the shell lines of the step's run: key, inline or block,
	// comments dropped.
	commands []string
	// lines are the 1-based file lines of commands, index for index.
	lines []int
	// gated names the `if:` or `continue-on-error:` of the step or of its
	// job, or is empty for a step that runs on every event and whose failure
	// fails the job.
	gated string
	// script is the whole run: value as GitHub's expression engine reads
	// it: the text after `run:` and every line under it, comment lines and
	// blank lines included, for the injection check.
	script []scriptLine
}

// scriptLine is one line of a run: value: its 1-based file line and its text
// with the indentation removed.
type scriptLine struct {
	line int
	text string
}

// workflowSteps reads the steps of a workflow, or of a fragment of one that
// starts inside a job's steps.
//
// It is a reader of the layout every workflow here uses, not a YAML parser: a
// job is a key two columns in under `jobs:`, its own keys sit one level
// deeper, a step is a `- ` item of the list under `steps:`, and the step's
// keys line up with the text after its dash.
func workflowSteps(body string) []workflowStep {
	lines := strings.Split(body, "\n")
	jobGate := jobGates(lines)
	hasSteps := false
	for _, l := range lines {
		if yamlKey(strings.TrimSpace(l)) == "steps" {
			hasSteps = true
			break
		}
	}
	var steps []workflowStep
	cur := -1      // index in steps of the step being read, or -1
	listCol := -1  // column of the steps list's dashes, or -1 outside a list
	stepsKey := -1 // column of the `steps:` key whose list comes next, or -1
	keyCol := -1   // column of the current step's keys
	block := -1    // column of an open run: block's key, or -1
	script := -1   // column of the run: key whose value is still being read, or -1
	for i, line := range lines {
		t := strings.TrimSpace(line)
		col := indentOf(line)
		// A run: value goes on for every line indented past its key: a
		// block's lines, comments too, and a plain scalar's continuation.
		// The expression engine substitutes into all of it.
		if script >= 0 {
			if t == "" || col > script {
				steps[cur].script = append(steps[cur].script, scriptLine{line: i + 1, text: t})
			} else {
				script = -1
			}
		}
		if block >= 0 {
			if t == "" {
				continue
			}
			if col > block {
				if !strings.HasPrefix(t, "#") {
					steps[cur].commands = append(steps[cur].commands, t)
					steps[cur].lines = append(steps[cur].lines, i+1)
				}
				continue
			}
			block = -1
		}
		if t == "" || strings.HasPrefix(t, "#") {
			continue
		}
		rest, dash := strings.CutPrefix(t, "- ")
		if listCol >= 0 && (col < listCol || col == listCol && !dash) {
			listCol, cur = -1, -1
		}
		if listCol < 0 {
			// A steps list starts at the first dash after a `steps:` key,
			// or, in a fragment with no `steps:` key, at the first dash.
			switch {
			case yamlKey(t) == "steps":
				stepsKey = col
			case dash && (stepsKey >= 0 && col >= stepsKey || !hasSteps):
				listCol, stepsKey = col, -1
			default:
				stepsKey = -1
			}
			if listCol < 0 {
				continue
			}
		}
		key := t
		if dash && col == listCol {
			key = strings.TrimSpace(rest)
			keyCol = col + len(t) - len(key)
			steps = append(steps, workflowStep{gated: jobGate[i]})
			cur = len(steps) - 1
		}
		if col+len(t)-len(key) != keyCol {
			continue
		}
		_, raw, _ := strings.Cut(key, ":")
		raw = strings.TrimSpace(raw)
		k, v := yamlKey(key), yamlValue(key)
		if k == "run" {
			steps[cur].script = append(steps[cur].script, scriptLine{line: i + 1, text: raw})
			script = keyCol
		}
		switch {
		case gates(k, v):
			if steps[cur].gated == "" {
				steps[cur].gated = "the step's " + k + ": " + v
			}
		case k != "run":
		case raw == "" || strings.HasPrefix(raw, "|") || strings.HasPrefix(raw, ">"):
			block = keyCol
		case !strings.HasPrefix(raw, "#"):
			steps[cur].commands = append(steps[cur].commands, strings.Trim(raw, `"'`))
			steps[cur].lines = append(steps[cur].lines, i+1)
		}
	}
	return steps
}

// jobGates returns, for each line of a workflow, the `if:` or
// `continue-on-error:` of the job holding it, or "".
//
// It is a pass of its own because a job's keys may come after its steps, and
// a job-level `if:` written there gates every step above it.
func jobGates(lines []string) []string {
	byLine := make([]string, len(lines))
	inJobs := false
	start, gate, keys := -1, "", -1
	flush := func(end int) {
		for j := start; j >= 0 && j < end; j++ {
			byLine[j] = gate
		}
	}
	for i, line := range lines {
		t := strings.TrimSpace(line)
		if t == "" || strings.HasPrefix(t, "#") {
			continue
		}
		col := indentOf(line)
		switch {
		case col == 0:
			flush(i)
			start, inJobs = -1, yamlKey(t) == "jobs"
		case inJobs && col == 2:
			flush(i)
			start, gate, keys = i, "", -1
		case start >= 0 && keys < 0:
			keys = col
			fallthrough
		case start >= 0 && col == keys:
			if k, v := yamlKey(t), yamlValue(t); gate == "" && gates(k, v) {
				gate = "the job's " + k + ": " + v
			}
		}
	}
	flush(len(lines))
	return byLine
}

// heldConditions are the `if:` conditions a step or job may carry and still
// count as running on every event, each with the reason it holds here. A
// condition is compared with its `${{ }}` and spaces removed.
var heldConditions = map[string]string{
	// codeql.yml's analyze job. Code scanning needs a public repository (or
	// a licence this one does not have), so a private fork skips the job
	// rather than failing it; this repository is public, so on every event
	// its workflows receive the condition is true.
	"!github.event.repository.private": "the repository is public",
}

// gates reports whether a step or job key holds it to fewer events, or lets
// its failure through: an `if:` other than a held condition, or a
// `continue-on-error:` other than false.
func gates(key, value string) bool {
	switch key {
	case "if":
		c := strings.NewReplacer("${{", "", "}}", "", " ", "").Replace(value)
		_, held := heldConditions[c]
		return !held
	case "continue-on-error":
		return value != "false"
	}
	return false
}

func indentOf(line string) int { return len(line) - len(strings.TrimLeft(line, " ")) }

// yamlKey is the key of a `key: value` line, or "" when t is not one. A
// quoted key (`"run": ...`) is the same key.
func yamlKey(t string) string {
	k, _, ok := strings.Cut(t, ":")
	if !ok {
		return ""
	}
	if len(k) >= 2 && (k[0] == '"' || k[0] == '\'') && k[len(k)-1] == k[0] {
		k = k[1 : len(k)-1]
	}
	if strings.ContainsAny(k, " \"'{[$") {
		return ""
	}
	return k
}

// yamlValue is the value of a `key: value` line, a trailing comment dropped.
func yamlValue(t string) string {
	_, v, _ := strings.Cut(t, ":")
	v = strings.TrimSpace(v)
	if i := strings.Index(v, " #"); i >= 0 {
		v = strings.TrimSpace(v[:i])
	}
	return v
}

// runCommands returns the shell lines of every `run:` step in a workflow that
// runs on every event and whose failure fails its job: the value of an inline
// `run: cmd`, or each line of a `run: |` block. A comment, a YAML one or a
// shell one inside the block, is not a command, and neither is any other key
// that names one, such as a step's `name:`.
//
// A step under an `if:`, or under a `continue-on-error:` that is not false,
// on itself or on its job, does not count: the first may never run, and the
// second runs with a failure the workflow cannot see. Both used to count, so
// vuln.yml passed with `make vuln` gated on an event the workflow never
// receives, and with its failure swallowed.
func runCommands(body string) []string {
	var out []string
	for _, s := range workflowSteps(body) {
		if s.gated == "" {
			out = append(out, errexitCommands(s.commands)...)
		}
	}
	return out
}

// whyNotHeld says, for each step of body with a command that `is` accepts in
// command position, why that step does not hold the workflow to it. It is
// empty when no step has one at all.
func whyNotHeld(body string, is func(fields []string) bool) []string {
	var out []string
	for _, s := range workflowSteps(body) {
		if !namesCommand(s.commands, is) {
			continue
		}
		switch {
		case s.gated != "":
			out = append(out, s.gated)
		case !runsCommand(s.commands, is):
			out = append(out, "an || after it that does not re-raise")
		case !runsCommand(errexitCommands(s.commands), is):
			out = append(out, "a `set +e` before it")
		}
	}
	return out
}

// namesCommand reports whether any of cmds has a command is accepts in
// command position, whatever follows it.
func namesCommand(cmds []string, is func(fields []string) bool) bool {
	for _, line := range cmds {
		segs, _ := shellSegments(line)
		for _, seg := range segs {
			if f := commandFields(seg); len(f) > 0 && is(f) {
				return true
			}
		}
	}
	return false
}

// errexitCommands drops the lines a `set +e` leaves unchecked: those after it,
// up to a `set -e`, other than the step's last line, whose status is the
// step's own.
func errexitCommands(cmds []string) []string {
	out := make([]string, 0, len(cmds))
	off := false
	for i, c := range cmds {
		switch f := strings.Fields(c); {
		case len(f) >= 2 && f[0] == "set" && (f[1] == "+e" || f[1] == "+o" && len(f) > 2 && f[2] == "errexit"):
			off = true
		case len(f) >= 2 && f[0] == "set" && (f[1] == "-e" || f[1] == "-o" && len(f) > 2 && f[2] == "errexit"):
			off = false
		case !off || i == len(cmds)-1:
			out = append(out, c)
		}
	}
	return out
}

// runsCommand reports whether any of cmds runs a command for which is
// reports true, and lets its failure through. A shell line is split at its
// separators (`&&`, `||`, `;`, `|`) and each command is passed as its fields,
// leading VAR=value assignments dropped, so that only a word in command
// position counts.
//
// A command followed by `||` counts only when what follows re-raises (`exit`
// with a status other than 0, or `false`). It used to be split off and counted
// like any other, so `make vuln || true` satisfied the check with the scan's
// failure discarded.
func runsCommand(cmds []string, is func(fields []string) bool) bool {
	for _, line := range cmds {
		segs, seps := shellSegments(line)
		for i, seg := range segs {
			f := commandFields(seg)
			if len(f) == 0 || !is(f) {
				continue
			}
			if i < len(seps) && seps[i] == "||" && !reraises(commandFields(segs[i+1])) {
				continue
			}
			return true
		}
	}
	return false
}

// shellSegments splits a shell line at `&&`, `||`, `;` and `|`, returning the
// commands and the separator after each.
func shellSegments(line string) (segs, seps []string) {
	start := 0
	for i := 0; i < len(line); i++ {
		var sep string
		switch {
		case strings.HasPrefix(line[i:], "&&"), strings.HasPrefix(line[i:], "||"):
			sep = line[i : i+2]
		case line[i] == ';' || line[i] == '|':
			sep = line[i : i+1]
		default:
			continue
		}
		segs, seps = append(segs, line[start:i]), append(seps, sep)
		i += len(sep) - 1
		start = i + 1
	}
	return append(segs, line[start:]), seps
}

func commandFields(seg string) []string {
	f := strings.Fields(seg)
	for len(f) > 0 && strings.Contains(f[0], "=") {
		f = f[1:]
	}
	return f
}

// reraises reports whether a command after `||` fails the step.
func reraises(f []string) bool {
	switch {
	case len(f) == 1 && f[0] == "false":
		return true
	case len(f) >= 1 && f[0] == "exit":
		return len(f) == 1 || f[1] != "0"
	}
	return false
}

var (
	// expressionRe finds a `${{ }}` expression in a run: value read whole, so
	// one that opens on one line and closes on a later one is still found.
	expressionRe = regexp.MustCompile(`(?s)\$\{\{(.*?)\}\}`)
	// stringLiteralRe finds a single-quoted expression string ('' escapes a
	// quote), the only string form GitHub's expression language has.
	stringLiteralRe = regexp.MustCompile(`'(?:[^']|'')*'`)
	// prControlledRe finds a github or inputs context in an expression whose
	// string literals are blanked: bare (`toJSON(github)`), dotted
	// (`github.head_ref`) or indexed (`github['head_ref']`).
	prControlledRe = regexp.MustCompile(`(?:^|[^\w.])((?:github|inputs)\b(?:\s*\.\s*[\w*-]+|\s*\[\s*'(?:[^']|'')*'\s*\])*)`)
)

// injection is one github or inputs context written into a run: value.
type injection struct {
	line int
	ctx  string
}

// runInjections returns every github or inputs context the `${{ }}`
// expressions of a workflow's run: values name, with the line each
// expression opens on.
//
// It reads each value the way the expression engine does, whole: a shell
// comment line in a block is substituted like any other (and a multi-line
// value breaks out of the comment), an expression may span lines, and a plain
// scalar continues on every line indented past its key.
func runInjections(body string) []injection {
	var out []injection
	for _, s := range workflowSteps(body) {
		var text strings.Builder
		starts := make([]int, len(s.script))
		for i, l := range s.script {
			starts[i] = text.Len()
			text.WriteString(l.text + "\n")
		}
		for _, m := range expressionRe.FindAllStringSubmatchIndex(text.String(), -1) {
			at := 0
			for i, st := range starts {
				if st <= m[0] {
					at = s.script[i].line
				}
			}
			for _, ctx := range prControlledContexts(text.String()[m[2]:m[3]]) {
				out = append(out, injection{line: at, ctx: ctx})
			}
		}
	}
	return out
}

// prControlledContexts returns the github and inputs contexts one expression
// names, spaces removed. A string literal is not a context (`'github'`), unless
// it is an index key (`github['head_ref']`).
func prControlledContexts(expr string) []string {
	var b strings.Builder
	last := 0
	for _, m := range stringLiteralRe.FindAllStringIndex(expr, -1) {
		b.WriteString(expr[last:m[0]])
		if strings.HasSuffix(strings.TrimRight(expr[:m[0]], " \t\n"), "[") {
			b.WriteString(expr[m[0]:m[1]])
		} else {
			b.WriteString("''")
		}
		last = m[1]
	}
	b.WriteString(expr[last:])
	matches := prControlledRe.FindAllStringSubmatch(b.String(), -1)
	out := make([]string, 0, len(matches))
	for _, m := range matches {
		out = append(out, strings.Join(strings.Fields(m[1]), ""))
	}
	return out
}

// checkWorkflowExpressions refuses a `github` or `inputs` context written into
// a `run:` value in any workflow.
//
// GitHub substitutes the expression into the script's TEXT before the shell
// reads it, so a value with shell syntax in it runs as shell. A branch name
// may hold `$`, `(`, `)` and `;`. The house rule is to pass such a value
// through `env:` and read it as a variable, and nothing checked it: after
// fuzz.yml's `${{ github.base_ref }}` moved to `env:`, bench.yml kept the same
// expression on the same trigger, and actionlint, which knows a list of
// untrusted fields, does not count base_ref among them.
//
// Every github field is refused, the ones no pull request chooses
// (github.sha, github.repository, github.run_id) included. Each has a
// GITHUB_* variable the runner already sets, or an `env:` line, so the safe
// spelling costs nothing, and a list of safe fields is a judgement to keep
// right as GitHub adds fields: github.ref_name, which looks as harmless as
// github.sha, is a branch name.
//
// Not read: `steps.*`, `needs.*`, `env.*`, `matrix.*`, `vars.*` and
// `secrets.*` (a value the workflow or the repository set, though a step's
// output can carry text a pull request wrote), a value in `env:` or `with:`,
// and actions/github-script's `script:` input, where the same substitution
// makes JavaScript. No workflow here uses github-script.
func (c *checker) checkWorkflowExpressions() {
	dir := filepath.Join(c.root, ".github", "workflows")
	var files []string
	for _, pattern := range []string{"*.yml", "*.yaml"} {
		m, err := filepath.Glob(filepath.Join(dir, pattern))
		if err != nil {
			c.problems = append(c.problems, err.Error())
			return
		}
		files = append(files, m...)
	}
	sort.Strings(files)
	found := 0
	for _, p := range files {
		b, err := os.ReadFile(p)
		if err != nil {
			c.problems = append(c.problems, err.Error())
			return
		}
		name := filepath.Base(p)
		for _, in := range runInjections(string(b)) {
			found++
			c.fail("injection", fmt.Sprintf("%s:%d %s", name, in.line, in.ctx),
				"%s:%d writes ${{ %s }} into a run: script; GitHub pastes the value into the script text before "+
					"the shell runs it, so pass it through the step's env: and read the variable", name, in.line, in.ctx)
		}
	}
	if found == 0 {
		c.okf("no run: value in %d workflow(s) names a github or inputs context in a ${{ }} expression", len(files))
	}
}
