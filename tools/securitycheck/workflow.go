package main

import (
	"cmp"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"slices"
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
	// gated names the key of the step, its job or the workflow that keeps
	// the step from holding the workflow to its commands (see gates), or is
	// empty for a step that runs on every event, under the default shell and
	// directory, and whose failure fails the job.
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
	workflowGate := ""
	for i, l := range lines {
		t := strings.TrimSpace(l)
		switch k := yamlKey(t); {
		case k == "steps":
			hasSteps = true
		case k == "defaults" && indentOf(l) == 0:
			workflowGate = cmp.Or(workflowGate, "the workflow's defaults:")
		case !strings.HasPrefix(t, "#") && makeEnvRe.MatchString(t):
			workflowGate = cmp.Or(workflowGate, fmt.Sprintf("the workflow's %s at line %d", makeEnvRe.FindStringSubmatch(t)[1], i+1))
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
			steps = append(steps, workflowStep{gated: cmp.Or(jobGate[i], workflowGate)})
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
				steps[cur].gated = strings.TrimSpace("the step's " + k + ": " + v)
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

// jobGates returns, for each line of a workflow, the first key of the job
// holding it that gates its steps (an `if:`, a `continue-on-error:`, a
// `needs:` or a `defaults:`), or "".
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
				gate = strings.TrimSpace("the job's " + k + ": " + v)
			}
		}
	}
	flush(len(lines))
	return byLine
}

// makeEnvRe finds a key naming one of the environment variables GNU make
// reads its own flags from. `MAKEFLAGS: n` in an env: is `make -n`: every recipe printed and none run, so
// a `make vuln` under it scans nothing and exits 0. Any value is refused, not
// only the dry-run letters, because a flags string is make's to parse
// (`n`, `-n`, `--just-print`, `--dry-run`, `-q`, `--question`, `-i` ...), and
// no workflow here sets one. It is found as a key in any spelling: block
// (`MAKEFLAGS: n`), quoted, or in a flow mapping (`env: {MAKEFLAGS: n}`).
var makeEnvRe = regexp.MustCompile(`(?:^|[\s{,"'])(MAKEFLAGS|GNUMAKEFLAGS|MFLAGS)["']?\s*:`)

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

// gates reports whether a step, job or workflow key keeps a step from holding
// the workflow to what its script says: an `if:` other than a held condition
// or a `needs:` (the step may not run), a `continue-on-error:` other than
// false (its failure cannot fail the job), or a `shell:`, a
// `working-directory:` or a `defaults:` (the script runs under another shell,
// which may not stop on a failure, or against another Makefile).
//
// A `needs:` is refused whatever it names. A job that needs a job skipped on
// every event is skipped too, and telling that job from one that always runs
// is a reading of conditions this check does not attempt.
//
// A key makeEnvRe finds anywhere in the workflow gates every step of it (see
// workflowSteps). What a text check cannot see stays out of reach: a step
// that writes MAKEFLAGS to $GITHUB_ENV, or a directory holding another `make`
// to $GITHUB_PATH, for the steps after it, and a Makefile recipe or
// hack/vuln.sh changed so the target scans nothing. Review owns those.
func gates(key, value string) bool {
	switch key {
	case "if":
		c := strings.NewReplacer("${{", "", "}}", "", " ", "").Replace(value)
		_, held := heldConditions[c]
		return !held
	case "continue-on-error":
		return value != "false"
	case "needs", "shell", "working-directory", "defaults":
		return true
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

// runCommands returns the script of every `run:` step in a workflow whose
// script is one shell line and that holds the workflow to it (see gates): the
// value of an inline `run: cmd`, or the one line of a `run: |` block. A
// comment, a YAML one or a shell one inside the block, is not a line of the
// script, and neither is any other key that names a command, such as a step's
// `name:`.
//
// A script of more than one line is not read at all. A line above the command
// can stop the shell before it (`exit 0`), turn its text into data (a
// heredoc), or keep its failure from failing the step (`set +e`), and a line
// below it can do the last; each of those passed this check once, and
// reading shell is not a thing it can finish. One line is a shape it can.
func runCommands(body string) []string {
	var out []string
	for _, s := range workflowSteps(body) {
		if s.gated == "" && len(s.commands) == 1 {
			out = append(out, s.commands[0])
		}
	}
	return out
}

// whyNotHeld says, for each step of body whose script names a command
// `names` accepts in command position, why that step does not hold the
// workflow to it. It is empty when no step names one at all.
func whyNotHeld(body string, names, is func(fields []string) bool) []string {
	var out []string
	for _, s := range workflowSteps(body) {
		if !namesCommand(s.commands, names) {
			continue
		}
		switch {
		case s.gated != "":
			out = append(out, s.gated)
		case len(s.commands) != 1:
			out = append(out, fmt.Sprintf("a script of %d lines, where only a script that is the one command counts", len(s.commands)))
		case !runsCommand(s.commands, is):
			out = append(out, fmt.Sprintf("%#q, which is more than the command (an `|| exit 1` after it is the one addition read)", s.commands[0]))
		}
	}
	return out
}

// namesCommand reports whether any of cmds has a command is accepts in
// command position, whatever surrounds it.
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

// runsCommand reports whether any of cmds is a command for which is reports
// true and nothing else, or that command followed by an `||` that re-raises
// (`exit` with a status other than 0, or `false`). The command's fields are
// passed as written, so an assignment before it (`MAKEFLAGS=n make vuln`) is
// not the command.
//
// It used to accept the command anywhere in the line: after `cd x &&`, before
// `| tee`, after `true ||`, and with any flag, `make -n vuln` included, so a
// scan that never ran and a scan whose failure a pipe swallowed both passed.
//
// A trailing comment (` # ...`) is dropped first: YAML strips it from a plain
// value and the shell ignores it in a block. Cutting there only shortens the
// line, and what is left still has to be the command alone.
func runsCommand(cmds []string, is func(fields []string) bool) bool {
	for _, line := range cmds {
		if i := strings.Index(line, " #"); i >= 0 {
			line = line[:i]
		}
		segs, seps := shellSegments(line)
		if f := strings.Fields(segs[0]); len(f) == 0 || !is(f) {
			continue
		}
		switch {
		case len(segs) == 1:
			return true
		case len(segs) == 2 && seps[0] == "||" && reraises(strings.Fields(segs[1])):
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
	// (`github.head_ref`) or indexed (`github['head_ref']`), in any case,
	// because the expression engine looks a context's name up without regard
	// to case and `GitHub.head_ref` is `github.head_ref`.
	prControlledRe = regexp.MustCompile(`(?i)(?:^|[^\w.])((?:github|inputs)\b(?:\s*\.\s*[\w*-]+|\s*\[\s*'(?:[^']|'')*'\s*\])*)`)
	// aliasRe finds a YAML alias standing as a value: `key: *name`,
	// `- *name` or `<<: *name`, on a line outside a block scalar.
	aliasRe = regexp.MustCompile(`^(?:-\s+)*(?:[^\s:#]+:\s+)?(\*[^\s,\[\]{}]+)\s*(?:#.*)?$`)
	// blockScalarRe finds a value that opens a block scalar, after any
	// anchor or tag on it: `|`, `>-`, `&name |`, `!!str >`.
	blockScalarRe = regexp.MustCompile(`^(?:[&!]\S*\s+)*[|>][-+0-9]*\s*(?:#.*)?$`)
)

// privilegedTriggers are the events that run a workflow with the base
// repository's token and secrets on text a pull request or a comment author
// wrote. In a workflow any of them starts, no `${{ }}` may stand in a run:
// value at all: an env.* or steps.* value is as much that author's text as
// github.event.* is, and the house rule for these triggers is that every such
// value reaches the script through env:.
var privilegedTriggers = []string{"pull_request_target", "workflow_run", "issue_comment"}

// injection is one value written into a run: script that the injection check
// refuses: a github or inputs context, any expression in a workflow a
// privileged trigger starts, or a YAML alias.
type injection struct {
	line int
	// ctx is the github or inputs context the expression names, the whole
	// expression (spaces removed) in a workflow a privileged trigger starts,
	// or the alias (`*name`).
	ctx string
}

// runInjections returns every github or inputs context the `${{ }}`
// expressions of a workflow's run: values name, with the line each
// expression opens on; in a workflow one of privilegedTriggers starts, every
// expression in a run: value; and every YAML alias in the workflow.
//
// It reads each value the way the expression engine does, whole: a shell
// comment line in a block is substituted like any other (and a multi-line
// value breaks out of the comment), an expression may span lines, and a plain
// scalar continues on every line indented past its key.
//
// An alias is refused wherever it stands. GitHub resolves anchors, so a run:
// value, a step or a whole job can be an alias of text this reader read
// somewhere else, as an env: value say, and never as a script.
func runInjections(body string) []injection {
	privileged := privilegedTrigger(body)
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
			expr := text.String()[m[2]:m[3]]
			if privileged != "" {
				out = append(out, injection{line: at, ctx: strings.Join(strings.Fields(expr), "")})
				continue
			}
			for _, ctx := range prControlledContexts(expr) {
				out = append(out, injection{line: at, ctx: ctx})
			}
		}
	}
	return append(out, workflowAliases(body)...)
}

// workflowAliases returns every YAML alias that stands as a value in body,
// outside a block scalar, with its line.
func workflowAliases(body string) []injection {
	var out []injection
	block := -1 // indentation of the key whose block scalar is open, or -1
	for i, line := range strings.Split(body, "\n") {
		t := strings.TrimSpace(line)
		col := indentOf(line)
		if block >= 0 {
			if t == "" || col > block {
				continue
			}
			block = -1
		}
		if t == "" || strings.HasPrefix(t, "#") {
			continue
		}
		if m := aliasRe.FindStringSubmatch(t); m != nil {
			out = append(out, injection{line: i + 1, ctx: m[1]})
			continue
		}
		// The block's lines are those indented past its key, or past the
		// dash of a list item that is the block itself.
		v, at := t, col
		for {
			rest, ok := strings.CutPrefix(v, "- ")
			if !ok {
				break
			}
			v = strings.TrimSpace(rest)
		}
		if _, after, ok := strings.Cut(v, ": "); ok && yamlKey(v) != "" {
			at = col + len(t) - len(v)
			v = strings.TrimSpace(after)
		}
		if blockScalarRe.MatchString(v) {
			block = at
		}
	}
	return out
}

// privilegedTrigger returns the first of privilegedTriggers that starts the
// workflow, or "".
func privilegedTrigger(body string) string {
	triggers := workflowTriggers(body)
	for _, t := range privilegedTriggers {
		if slices.Contains(triggers, t) {
			return t
		}
	}
	return ""
}

// workflowTriggers returns the events a workflow's `on:` names, in any of
// its three spellings: `on: push`, `on: [push, pull_request]`, or a mapping
// or list under the key.
func workflowTriggers(body string) []string {
	lines := strings.Split(body, "\n")
	for i, line := range lines {
		t := strings.TrimSpace(line)
		if indentOf(line) != 0 || yamlKey(t) != "on" {
			continue
		}
		if v := yamlValue(t); v != "" {
			var out []string
			for e := range strings.SplitSeq(strings.Trim(v, "[]"), ",") {
				if e = strings.Trim(strings.TrimSpace(e), `"'`); e != "" {
					out = append(out, e)
				}
			}
			return out
		}
		var out []string
		child := -1
		for _, l := range lines[i+1:] {
			lt := strings.TrimSpace(l)
			if lt == "" || strings.HasPrefix(lt, "#") {
				continue
			}
			c := indentOf(l)
			if c == 0 {
				break
			}
			if child < 0 {
				child = c
			}
			if c != child {
				continue
			}
			if item, ok := strings.CutPrefix(lt, "- "); ok {
				out = append(out, strings.Trim(strings.TrimSpace(item), `"'`))
				continue
			}
			if k := yamlKey(lt); k != "" {
				out = append(out, k)
			}
		}
		return out
	}
	return nil
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
// In a workflow that pull_request_target, workflow_run or issue_comment
// starts, every expression in a run: value is refused, whatever it names
// (privilegedTriggers says why). And a YAML alias is refused in any workflow,
// because what it stands for is text this check read somewhere else.
//
// Not read elsewhere: `steps.*`, `needs.*`, `env.*`, `matrix.*`, `vars.*` and
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
		privileged := privilegedTrigger(string(b))
		for _, in := range runInjections(string(b)) {
			found++
			subject := fmt.Sprintf("%s:%d %s", name, in.line, in.ctx)
			switch {
			case strings.HasPrefix(in.ctx, "*"):
				c.fail("injection", subject,
					"%s:%d is the YAML alias %s; GitHub runs the anchored text in its place, which this check never "+
						"reads as a script, so write the value where it is used", name, in.line, in.ctx)
			case privileged != "":
				c.fail("injection", subject,
					"%s:%d writes ${{ %s }} into a run: script of a workflow %s starts, where no expression may stand "+
						"in a script; pass the value through the step's env: and read the variable", name, in.line, in.ctx, privileged)
			default:
				c.fail("injection", subject,
					"%s:%d writes ${{ %s }} into a run: script; GitHub pastes the value into the script text before "+
						"the shell runs it, so pass it through the step's env: and read the variable", name, in.line, in.ctx)
			}
		}
	}
	if found == 0 {
		c.okf("no run: value in %d workflow(s) names a github or inputs context in a ${{ }} expression, none "+
			"started by %s has an expression in a run: value, and none holds a YAML alias",
			len(files), strings.Join(privilegedTriggers, ", "))
	}
}
