"""Hold `.github/workflows/ci.yml`'s `gate` job to the jobs it must aggregate,
and hold its skip waiver to the condition that justifies it.

`gate` is the job a branch-protection rule keys on and its verdict is drawn
from `needs`. A job that is not in `needs` can go red while `gate` reports
success, and the pull request merges. That is what happened: `needs` named
nine of the eleven jobs, and the two it omitted were `generated`
(generate-check, suites-check, docs-check, security-check,
security-check-selftest) and
`test-arm64`.

`hack/ci-parity.sh`'s first two rules ask whether the workflow RUNS a check.
This asks whether a FAILURE of that check can fail the workflow. Both have to
hold and neither implies the other -- the omitted `generated` job ran every one
of its seven targets on every pull request, which is what made the gap
invisible (assumption C08.9).

Five things decide that, and all five are held here (assumption C08.11):

1. **Reachability**, but only through edges that PROPAGATE a failure. A
   `needs` edge stops a consumer only when the consumer is skipped by that
   failure. GitHub drops a job's implicit `success()` prerequisite the moment
   its `if:` names any status check function, so a consumer carrying
   `always()`, `!cancelled()`, `failure()` or the long-hand
   `success() || failure()` RUNS on a failed prerequisite and reports its own
   success -- and the prerequisite's failure stops there. A consumer whose
   `if:` names no status function (`github.event_name == 'push'`, or plain
   `success()`) still requires its `needs` to have succeeded, so it does
   propagate. The one exception is an AGGREGATOR: a job that runs
   `hack/ci-gate-verify.sh` over `${{ toJSON(needs) }}` reads the results it
   was handed and goes red for them, which is precisely what `gate` does, so a
   second such job is a legitimate shape and not a hole.
2. **The gate's own `if:`**, which has to be `always()`. It is the line that
   makes the required check REPORT at all: without it GitHub skips `gate` as
   soon as a needed job fails, and a skipped required check does not block a
   merge. That is the same end state as the nine-of-eleven `needs` list, one
   line wide.
3. **The verdict**: the gate has to have a STEP whose `run:` is
   `hack/ci-gate-verify.sh`, which has a self-test, rather than an expression
   nobody has watched fail. Bound to a `run:` value and not to a substring of
   the job's text, because a commented-out `#  run: ./hack/ci-gate-verify.sh`
   beside an inline `jq` satisfied the substring and gated nothing.
4. **`NEEDS`** has to be `${{ toJSON(needs) }}`. A hardcoded map passes every
   other rule here and every row of the verdict's own self-test.
5. **`MAY_SKIP`**, which is what decides whether a SKIPPED job counts as
   success. A name in that list is a waiver, and this repository's standard for
   a waiver is that it is exact. So the list must be exactly the set recorded
   below; each name must be a job of the file; each must carry the `if:`
   expression recorded beside it, so that changing the reason forces the waiver
   to be re-justified; and each must have NO `needs:`, because a job skipped by
   a failed prerequisite is marked `skipped` too and would be accepted as the
   declared skip. Without this, widening `MAY_SKIP` to every job -- one word --
   turns the required gate into a rubber stamp with every check still green.

A sixth rule is about the gate's jobs rather than the gate's wiring:

6. **No job the gate covers may carry job-level `continue-on-error: true`.**
   Whether `needs.<job>.result` reports `success` or `failure` for such a job is
   NOT established here -- nothing in this repository has a `needs` edge onto one,
   so there is no observation to read, and `fix/nightly-plumbing` was right to
   decline the rule on those grounds. What settles it is that both answers are
   wrong in the same place: if `result` is `success` the gate reports green over a
   red job, which is C08.9 restored one word wide; if it is `failure` then
   `continue-on-error` does nothing on that job and is text telling a reader the
   opposite of what the gate does. A rule that holds on the disjunction needs
   neither measurement. What IS measured: soak.yml's `upcoming-compat` carries
   job-level `continue-on-error: true` and the jobs API reports
   `conclusion: failure` for it, so the flag does not rewrite the job's own
   conclusion (assumption C11.6). STEP-level `continue-on-error` is untouched by
   this rule -- bench.yml pairs it with a re-raising step on purpose, and one of
   the CLEAN shapes below holds that open.

TWO MORE rules apply to EVERY workflow rather than to ci.yml's gate, and
`hygiene()` is where they live:

- **Every job declares `timeout-minutes`.** Without it a job inherits GitHub's
  default of SIX HOURS, so a hung `go test` holds a runner for a working day and
  the run reports nothing a reader can act on. ci.yml had thirteen jobs and not
  one bound.
- **Every workflow declares `concurrency:`**, with a group that is not a constant
  (or every run of the workflow shares one group regardless of branch) and not
  keyed on `github.run_id` (which makes the group unique per run, so the
  declaration is a no-op and satisfies this rule while doing nothing), and with
  `cancel-in-progress` spelled out. The DIRECTION is deliberately not a rule:
  whether a superseded run may be cancelled depends on whether the run records
  something no later run reproduces, which no lint can read. Its PRESENCE is a
  rule, so the direction is always a decision somebody made rather than the
  default nobody chose.

`NEEDS` and `MAY_SKIP` are read from the STEP that runs the verdict, and a key
set in more than one place inside `gate` is a problem in itself. GitHub resolves
a step's `env:` over the job's, so while this file returned the FIRST matching
line in the job, a decoy job-level `MAY_SKIP: test-arm64` satisfied the rule
while the step handed the verdict all eleven jobs -- the round-1 rubber stamp,
restored, two lines wide and green.

Read by hack/ci-parity.sh, which `make ci` runs. Prints one line per problem
and nothing when there is none, and ALWAYS exits 0 -- the caller owns the
verdict, as it does for hack/aggregate-parity.py.

That is not a style choice. `ci-parity.sh` runs under `set -e` and reads this
through `problems="$(python3 hack/gate-parity.py "$wf")"`; a non-zero exit
there fails the assignment, `set -e` ends the script before the lines are
printed, and the check reports nothing while looking like it ran. It did
exactly that on the first draft.

`--selftest` mutates a copy of the real workflow in memory, once per rule, and
fails when a mutation does not produce the problem it is supposed to. It needs
no network, no runner and no Go.

The workflow is parsed here rather than with PyYAML, which this repository
depends on nowhere else and which would be a new undeclared dependency on the
`make ci` path. The parser understands the shapes `actionlint` allows in this
file and REPORTS rather than shrugs when it cannot find what it expects, so a
parser that stops understanding the file is a failure and not a silent pass.
"""

import json
import os
import re
import sys

GATE = "gate"

# The expression the gate must pass to the verdict. A literal map here would
# satisfy every other rule in this file and every row of ci-gate-verify.sh's
# own self-test, while gating nothing.
NEEDS_EXPR = "${{ toJSON(needs) }}"

# The script that IS the verdict, matched against a step's `run:` value.
VERDICT = "ci-gate-verify.sh"

# The gate's own condition. `always()` is what makes the gate run -- and so
# REPORT -- when a job it needs has failed. Anything else leaves GitHub skipping
# the gate on the first failure, and a skipped required check does not block a
# merge: the whole mechanism goes inert without the required check ever going
# red, which is the failure mode this file exists to make impossible.
GATE_IF = "always()"

# The skip waiver, as a PAIR: the job, and the condition that is the whole
# reason it may be skipped. `test-arm64` runs on a public fork and never here,
# so on this repository the waiver is the only thing keeping the gate green --
# and the condition is the only thing that makes the waiver honest.
MAY_SKIP_CONDITIONS = {
    "test-arm64": "${{ !github.event.repository.private }}",
}

# Jobs that deliberately sit outside the gate, and why. A waiver is exact: an
# unlisted job and a listed job that no longer exists both fail.
WAIVERS = {
    GATE: "the aggregate itself; a job cannot be its own prerequisite",
}

# An `if:` naming any of these runs its job even when a prerequisite failed, so
# an edge into it does not carry a failure to the gate. `failure(` covers both
# `${{ failure() }}` and `${{ success() || failure() }}`, the documented
# long-hand for `!cancelled()`; a report job written that way runs on BOTH
# outcomes and reports success in both, which is the quietest shape of this
# defect. `success(` is deliberately NOT here: `if: ${{ success() }}` on its own
# still requires every `needs` to have succeeded, so it propagates, and listing
# it would reject a job that gates correctly.
NON_PROPAGATING = ("always(", "cancelled(", "failure(")

# A job key line may carry a trailing comment: `  probejob: # split out`. It is
# legal YAML, actionlint accepts it, and while this regex ended at the colon the
# job was never recorded and never asked about -- one un-parsed job out of
# thirteen is exactly the shape that made C08.9 invisible for as long as it
# lasted. JOB_KEY_LINE_RE is the backstop that does not depend on this regex
# being right: it counts job key lines however they are spelled, so a job this
# parser cannot read is reported rather than silently dropped.
JOB_RE = re.compile(r"^  ([A-Za-z0-9_][A-Za-z0-9_-]*):[ \t]*(#.*)?$")
JOB_KEY_LINE_RE = re.compile(r"^  [^\s#][^:]*:")
KEY_RE = re.compile(r"^    ([A-Za-z0-9_-]+):[ \t]*(.*)$")
# The workflow-level `concurrency:` key, anchored at column 0 so a JOB-level
# `concurrency:` (four spaces) cannot satisfy the workflow-level rule, and its
# two-space children.
CONCURRENCY_RE = re.compile(r"^concurrency:[ \t]*(.*)$")
TOP_SUB_RE = re.compile(r"^  ([A-Za-z0-9_-]+):[ \t]*(.*)$")
STEPS_RE = re.compile(r"^    steps:[ \t]*(#.*)?$")
STEP_RE = re.compile(r"^      - ")
# `uses: actions/setup-go@<sha>` at any step indent, and that step's own
# `cache:` input. The second deliberately does NOT match
# `cache-dependency-path:`, which is a different input and is ignored
# outright once `cache` is false.
SETUP_GO_RE = re.compile(r"uses:\s*actions/setup-go@")
SETUP_GO_CACHE_RE = re.compile(r"^\s+cache:\s*(.*)$")
RUN_RE = re.compile(r"^[ \t]+(?:-[ \t]+)?run:[ \t]*(.*)$")
COMMENT_RE = re.compile(r"^[ \t]*#")
ITEM_RE = re.compile(r"^      - (.+?)[ \t]*$")
ENV_RE = re.compile(r"^[ \t]+([A-Z_][A-Z0-9_]*):[ \t]*(.*)$")
EXPR_RE = re.compile(r"^\$\{\{(.*)\}\}$")


def parse(text):
    """Return ({job: {...}}, job-key-line count).

    Each job carries `if`, `needs`, `lines` (every line of the job), `job_lines`
    (the job's own keys, i.e. everything outside `steps:`) and `steps` (one list
    of lines per step). The split matters because GitHub resolves a step's
    `env:` over the job's, so a rule about the value a STEP receives cannot be
    answered by the first matching line in the job.

    The second return value is how many job key lines the file has, counted
    without the job regex. A parser that loses a job has to say so, and
    comparing the two counts is the one check here that does not assume the
    regex above understood the file.
    """
    lines = text.splitlines()
    try:
        start = lines.index("jobs:") + 1
    except ValueError:
        return {}, 0
    jobs, name = {}, None
    key_lines = 0
    in_steps = False
    i = start
    while i < len(lines):
        line = lines[i]
        if line and not line.startswith(" ") and not line.startswith("#"):
            break  # a new top-level key; `jobs:` has ended
        m = JOB_RE.match(line)
        if m:
            name = m.group(1)
            jobs[name] = {
                "if": None,
                "needs": [],
                "lines": [],
                "job_lines": [],
                "steps": [],
                # `timeout` and `continue_on_error` are read at the JOB's own
                # indent (KEY_RE is four spaces), which is the whole distinction
                # a step-level `continue-on-error:` turns on: bench.yml's
                # deferred-failure pair is legitimate and sits two levels deeper.
                "timeout": None,
                "continue_on_error": None,
                "runs_on": None,
            }
            key_lines += 1
            in_steps = False
            i += 1
            continue
        if JOB_KEY_LINE_RE.match(line):
            key_lines += 1  # a job key line this parser could not read
        if name is None:
            i += 1
            continue
        jobs[name]["lines"].append(line)
        km = KEY_RE.match(line)
        if km is None and in_steps:
            if STEP_RE.match(line):
                jobs[name]["steps"].append([])
            if jobs[name]["steps"]:
                jobs[name]["steps"][-1].append(line)
        elif km is None:
            jobs[name]["job_lines"].append(line)
        if km:
            in_steps = STEPS_RE.match(line) is not None
            if not in_steps:
                jobs[name]["job_lines"].append(line)
            key, rest = km.group(1), km.group(2).strip()
            if key == "if":
                jobs[name]["if"] = rest
            elif key == "timeout-minutes":
                jobs[name]["timeout"] = rest
            elif key == "continue-on-error":
                jobs[name]["continue_on_error"] = rest
            elif key == "runs-on":
                jobs[name]["runs_on"] = rest
            elif key == "needs":
                if rest.startswith("["):
                    inner = rest[1: rest.rindex("]")] if "]" in rest else rest[1:]
                    jobs[name]["needs"] = [p.strip() for p in inner.split(",") if p.strip()]
                elif rest:
                    jobs[name]["needs"] = [rest]
                else:
                    j = i + 1
                    while j < len(lines):
                        im = ITEM_RE.match(lines[j])
                        if not im:
                            break
                        jobs[name]["needs"].append(im.group(1).strip())
                        jobs[name]["lines"].append(lines[j])
                        jobs[name]["job_lines"].append(lines[j])
                        j += 1
                    i = j - 1
        i += 1
    return jobs, key_lines


def unwrap(expr):
    """`${{ x }}` and `x` are the same expression to the runner."""
    e = (expr or "").strip()
    m = EXPR_RE.match(e)
    return m.group(1).strip() if m else e


def strip_comment(value):
    """A YAML scalar with its trailing `#` comment removed.

    Quote-aware, because a `run:` line may legitimately contain a `#`. Without
    this, `run: jq -e . <<< "$NEEDS"  # was: ./hack/ci-gate-verify.sh` reads as
    a step that runs the verdict.
    """
    out, quote = [], None
    for c in value:
        if quote:
            out.append(c)
            if c == quote:
                quote = None
        elif c in "'\"":
            quote = c
            out.append(c)
        elif c == "#" and (not out or out[-1] in " \t"):
            break
        else:
            out.append(c)
    return "".join(out).strip()


def verdict_step(job):
    """The job's step that RUNS the verdict script, or None.

    A step and not the job, because the env the runner hands the verdict is
    that step's; a `run:` value and not a substring of the job's text, because
    a comment naming the script is not a step that runs it.
    """
    for step in job.get("steps", []):
        for line in step:
            if COMMENT_RE.match(line):
                continue
            m = RUN_RE.match(line)
            if m and VERDICT in strip_comment(m.group(1)):
                return step
    return None


def propagates(jobs, name):
    """Does a failure of a prerequisite of `name` stop `name` succeeding?

    Only when `name` is skipped by that failure, or examines it itself. A job
    whose `if:` names a status check function this file calls NON_PROPAGATING
    runs anyway and reports its own success -- unless it is an aggregator that
    reads `needs` through the verdict script, which `gate` is and a second job
    legitimately may be. Without that exception the rule is wrong in the strict
    direction: it rejects the very shape the gate itself has.

    There is deliberately no special case for a MAY_SKIP job here. A waived
    job's skip IS absorbed by the verdict -- but check() already refuses a
    waived job that has any `needs:` at all, precisely so that no edge out of
    one exists to absorb anything, and a branch that the file's other rule makes
    unreachable is a branch nobody can watch fail.
    """
    job = jobs.get(name, {})
    cond = job.get("if") or ""
    if not any(tok in cond for tok in NON_PROPAGATING):
        return True
    return verdict_step(job) is not None


def reachable(jobs, direct):
    """Jobs whose failure can reach the gate, and the job each was reached by.

    The gate's DIRECT needs are read by the verdict itself, so each is
    covered. An edge from a covered job Y into X covers X only when a failed X
    really stops Y.
    """
    seen, stack, why = set(), list(direct), {}
    for d in direct:
        why[d] = None
    while stack:
        name = stack.pop()
        if name in seen or name not in jobs:
            continue
        seen.add(name)
        prop = propagates(jobs, name)
        for dep in jobs[name]["needs"]:
            # `why` is recorded whether or not the edge propagates, because a
            # job reached ONLY through a non-propagating consumer has to be
            # reported with that consumer named -- "not reachable at all" and
            # "reached through a job that runs anyway" are different defects
            # and the second is the one a reader will not otherwise see.
            why.setdefault(dep, name)
            if prop and dep not in seen:
                stack.append(dep)
    return seen, why


def env_of(lines, key):
    """The value of an env key in these lines, or None."""
    for line in lines:
        if COMMENT_RE.match(line):
            continue
        m = ENV_RE.match(line)
        if m and m.group(1) == key:
            return m.group(2).strip()
    return None


def env_count(lines, key):
    """How many places set this env key."""
    n = 0
    for line in lines:
        if COMMENT_RE.match(line):
            continue
        m = ENV_RE.match(line)
        if m and m.group(1) == key:
            n += 1
    return n


def resolve_env(gate, vstep, key):
    """(value the runner gives the verdict, number of places that set it).

    Step over job, which is how GitHub resolves it.
    """
    value = env_of(vstep, key) if vstep is not None else None
    if value is None:
        value = env_of(gate["job_lines"], key)
    return value, env_count(gate["lines"], key)


# Tokens that make a concurrency group unique per RUN, which satisfies "has a
# concurrency group" while deduplicating nothing -- the same shape as a hardcoded
# NEEDS map passing every other rule and gating nothing.
RUN_UNIQUE = ("github.run_id", "github.run_number", "github.run_attempt")

# GitHub kills a job on a hosted runner at 360 minutes. A larger
# `timeout-minutes` therefore never takes effect: the platform ends the job with
# no timeout annotation, so a reader cannot tell a hang from an infrastructure
# fault. Not applied to a self-hosted runner, which has no such ceiling, nor to
# a `runs-on` this file cannot resolve.
HOSTED_TIMEOUT_CEILING = 360


def concurrency_block(text):
    """(present, group, cancel-in-progress) for the WORKFLOW-level key.

    `concurrency: <scalar>` is the documented short form and counts as a group.
    Returned values are comment-stripped; `None` means the key is absent, which
    is a different answer from the empty string.
    """
    lines = text.splitlines()
    for i, line in enumerate(lines):
        m = CONCURRENCY_RE.match(line)
        if not m:
            continue
        group = strip_comment(m.group(1)) or None
        cancel = None
        for nxt in lines[i + 1:]:
            if not nxt.strip() or COMMENT_RE.match(nxt):
                continue
            if not nxt.startswith("  "):
                break  # a new top-level key; the block has ended
            sm = TOP_SUB_RE.match(nxt)
            if sm is None:
                break
            if sm.group(1) == "group":
                group = strip_comment(sm.group(2))
            elif sm.group(1) == "cancel-in-progress":
                cancel = strip_comment(sm.group(2))
        return True, group, cancel
    return False, None, None


def hygiene(text, path):
    """The two rules that hold for EVERY workflow, not only for ci.yml's gate.

    A job with no `timeout-minutes` inherits six hours; a workflow with no
    `concurrency:` lets superseded runs keep going. Both are named per job and
    per file, because "some workflow is missing something" is not a report
    anybody acts on.
    """
    problems = []
    jobs, key_lines = parse(text)
    if os.path.basename(path) == "ci.yml":
        for name, job in jobs.items():
            runner = (job["runs_on"] or "").strip()
            if runner and runner not in (PR_LINUX_RUNNER, PR_CROSS_RUNNER) and not runner.startswith(HOSTED_PREFIXES):
                problems.append(
                    f"{path}: job `{name}` can send pull requests to persistent runners; "
                    "use the PR-safe runner routing expression"
                )
            if runner == PR_CROSS_RUNNER:
                systems = re.findall(r"^\s+- os:\s*(\S+)\s*$", "\n".join(job["lines"]), re.M)
                if not systems or any(not system.startswith(HOSTED_PREFIXES) for system in systems):
                    problems.append(f"{path}: job `{name}` must use hosted OS labels for pull requests")
    if not jobs:
        problems.append(
            f"{path}: parsed no jobs at all; this check could not read the workflow, "
            f"and a job it cannot read is a job it cannot ask about"
        )
        return problems
    if key_lines != len(jobs):
        problems.append(
            f"{path}: parsed {len(jobs)} job(s) from {key_lines} job key line(s); this check "
            f"could not read the workflow, and a job it cannot read is a job it cannot ask about"
        )

    present, group, cancel = concurrency_block(text)
    if not present:
        problems.append(
            f"{path} has no `concurrency:`, so a superseded run of it keeps going: a new push "
            f"to a pull request leaves the old run burning a runner, and nothing serialises two "
            f"runs that write the same artefact"
        )
    else:
        if not group:
            problems.append(
                f"{path}'s `concurrency:` declares no group, so there is nothing for GitHub to "
                f"group runs by"
            )
        elif "${{" not in group:
            problems.append(
                f"{path}'s concurrency group is the constant {group!r}, so every run of the "
                f"workflow shares one group whatever its branch: a pull request would queue "
                f"behind, or cancel, main's run"
            )
        elif any(tok in group for tok in RUN_UNIQUE):
            problems.append(
                f"{path}'s concurrency group {group!r} is unique per run, so the declaration is "
                f"a no-op: it satisfies this rule and deduplicates nothing"
            )
        if cancel is None:
            problems.append(
                f"{path}'s `concurrency:` does not declare `cancel-in-progress`, so the direction "
                f"is GitHub's default of false rather than a decision: an omitted direction is "
                f"indistinguishable from one nobody chose, and the two are opposite for a "
                f"pull-request workflow and a workflow that records a measurement"
            )

    for name in sorted(jobs):
        raw = jobs[name]["timeout"]
        if raw is None:
            problems.append(
                f"{path}: job `{name}` has no `timeout-minutes`, so it inherits GitHub's default "
                f"of six hours: a job that stops making progress holds a runner for a working day "
                f"and the run reports nothing a reader can act on"
            )
            continue
        value = strip_comment(raw)
        if not value:
            problems.append(
                f"{path}: job `{name}` has `timeout-minutes:` with no value, which bounds nothing"
            )
            continue
        if "${{" in value:
            continue  # an expression; nothing static can judge what it resolves to
        try:
            minutes = int(value)
        except ValueError:
            problems.append(
                f"{path}: job `{name}`'s `timeout-minutes` is {value!r}, which is not a number "
                f"of minutes"
            )
            continue
        if minutes <= 0:
            problems.append(
                f"{path}: job `{name}`'s `timeout-minutes` is {minutes}, which is not a positive "
                f"number of minutes"
            )
            continue
        runs_on = jobs[name]["runs_on"] or ""
        if (
            minutes > HOSTED_TIMEOUT_CEILING
            and "self-hosted" not in runs_on
            and "${{" not in runs_on
        ):
            problems.append(
                f"{path}: job `{name}`'s `timeout-minutes` is {minutes}, above the "
                f"{HOSTED_TIMEOUT_CEILING}-minute ceiling GitHub enforces on a hosted runner, so "
                f"it never takes effect: the platform ends the job with no timeout annotation and "
                f"a reader cannot tell a hang from an infrastructure fault"
            )
    # Rule: inside a workflow that has moved to the self-hosted pool, a job on
    # a GitHub-hosted runner has to say why. The cost of getting this wrong is
    # asymmetric and that is the reason it is a rule: a job whose `runs-on`
    # names a pool that does not exist QUEUES rather than fails -- for the 24
    # hours GitHub allows -- while a job quietly left on `ubuntu-latest` just
    # costs money and nothing reports it. `.github/actionlint.yaml` catches the
    # first by declaring the custom labels; this catches the second.
    if os.path.basename(path) in MOVED_WORKFLOWS:
        for name in jobs:
            runs_on = (jobs[name]["runs_on"] or "").strip()
            if not runs_on:
                continue  # the `timeout-minutes` rules above already report a job this cannot read
            if "${{" in runs_on:
                continue  # an expression; nothing static can judge what it resolves to
            if not runs_on.startswith(HOSTED_PREFIXES):
                continue  # self-hosted, or a label list; this rule has nothing to ask
            key = f"{os.path.basename(path)}:{name}"
            if key not in HOSTED_REASONS:
                problems.append(
                    f"{path}: job `{name}` runs on the GitHub-hosted `{runs_on}` in a workflow "
                    f"whose compute is on {SELF_HOSTED_LINUX}, and HOSTED_REASONS does not say "
                    f"why: either move it, or name it there with the reason. An unexplained "
                    f"hosted job is how a bill that was cut grows back one job at a time"
                )
    # Rule: `actions/setup-go`'s cache has to match the pool the job runs on.
    # Both halves cost minutes and NEITHER IS EVER WRONG, so no test, no gate
    # and no bill reports them -- which is why this is a rule and not a review
    # note. A GitHub-hosted runner is ephemeral and sits beside the cache
    # service, so there the restore is necessary and takes ~20 s. A runner in
    # this pool is not ephemeral and is across the public internet from it: the
    # first self-hosted run of ci.yml spent 92 of 184 job-minutes in `setup-go`
    # restoring an 8.2 GB archive INTO A MACHINE THAT ALREADY HELD THE FILES,
    # and killed `generated` on its own ten-minute timeout while tarring 5.4 GB
    # back up (assumption C45.1).
    if os.path.basename(path) in MOVED_WORKFLOWS:
        for name in jobs:
            runs_on = (jobs[name]["runs_on"] or "").strip()
            if not runs_on:
                continue
            for step in jobs[name]["steps"]:
                if not any(SETUP_GO_RE.search(line) for line in step):
                    continue
                cache = None
                for line in step:
                    cm = SETUP_GO_CACHE_RE.match(line)
                    if cm:
                        cache = strip_comment(cm.group(1)).strip()
                where = f"{path}: job `{name}`'s `setup-go`"
                if "${{" in runs_on:
                    # compile-cross is one matrix straddling both pools, so a
                    # single static value would be wrong for one of its legs.
                    if cache is None or "${{" not in cache:
                        problems.append(
                            f"{where} sets `cache: {cache}` while the job's `runs-on` is the "
                            f"expression `{runs_on}`: one matrix spanning both pools needs the "
                            f"cache decided per leg, because the right answer differs by leg"
                        )
                elif "self-hosted" in runs_on:
                    if cache != "false":
                        problems.append(
                            f"{where} does not say `cache: false` on {SELF_HOSTED_LINUX}. The "
                            f"pool is not ephemeral, so $GOMODCACHE and $GOCACHE already persist "
                            f"on the runner's own disk; restoring them over the internet is pure "
                            f"cost and it is what made the first self-hosted run SLOWER than the "
                            f"hosted one it replaced"
                        )
                elif runs_on.startswith(HOSTED_PREFIXES):
                    if cache == "false":
                        problems.append(
                            f"{where} says `cache: false` on the GitHub-hosted `{runs_on}`, where "
                            f"the runner is ephemeral and keeps nothing between runs: every job "
                            f"would rebuild the module cache from zero"
                        )
    return problems


# The self-hosted pool this repository's compute runs on: the org's Default
# runner group, the `public` label, because the repository is public and the
# org's private runner group does not serve public repositories.
SELF_HOSTED_LINUX = "[self-hosted, Linux, X64, public]"

# Exact routing expressions keep every PR, including same-repository PRs, off
# persistent runners. Trusted events retain the existing compute pools.
PR_LINUX_RUNNER = '${{ fromJSON(github.event_name == \'pull_request\' && \'["ubuntu-latest"]\' || \'["self-hosted", "Linux", "X64", "public"]\') }}'
PR_CROSS_RUNNER = '${{ fromJSON(github.event_name == \'pull_request\' && format(\'["{0}"]\', matrix.os) || toJSON(matrix.runner)) }}'

# Workflows whose Linux jobs HAVE been moved to that pool. The rule below asks
# its question only of these, so it is a statement about what has been done
# rather than a wish: a workflow is added here by the change that moves it, and
# until then its hosted jobs are tracked in the maintenance notes, not excused one
# by one in a table that would be 25 rows of "not yet".
MOVED_WORKFLOWS = ("ci.yml",)

# `<workflow>:<job>` -> why that job is on a GITHUB-HOSTED runner inside a
# workflow that has otherwise moved. Named one by one and never as a class,
# because `runs-on: ubuntu-latest` is what anybody adding a job will type, and
# an unexplained hosted job is how a bill that was cut grows back one job at a
# time. The reason is printed in the report, so a reader can check the claim.
HOSTED_REASONS = {
    "ci.yml:modules": "coordination and not compute, 8s, and on the critical path: it hands out both matrices",
    "ci.yml:gate": "the verdict a branch rule keys on, 4s; a queued gate is a pull request that cannot merge, and the pool has been observed saturated",
    "ci.yml:workflows": "needs shellcheck, which the self-hosted image does not have and GitHub's does; moving it would make the Makefile's `CI will` warning false silently",
    "ci.yml:test-arm64": "ubuntu-24.04-arm: there is no self-hosted ARM runner, and this job is skipped on a private repository",
    "ci.yml:compile-cross": "PRs use hosted runners; trusted events retain self-hosted Windows and hosted macOS",
}

# A `runs-on` this rule reads as GitHub-hosted. Anything else -- a self-hosted
# label list, or an expression it cannot resolve -- is not asked for a reason.
HOSTED_PREFIXES = ("ubuntu-", "windows-", "macos-")


# Dependabot's `package-ecosystem` names mapped to the Renovate managers that
# read the same files. Renovate's `config:recommended` enables EVERY manager it
# detects, so an unrestricted Renovate covers every ecosystem Dependabot names
# whatever this table says; the table is only what makes a RESTRICTED Renovate --
# a genuine split by ecosystem, which is a legitimate shape -- answerable.
ECOSYSTEM_MANAGERS = {
    "gomod": ("gomod",),
    "github-actions": ("github-actions",),
    "pip": ("pip_requirements", "pip-compile", "pip_setup", "pipenv", "poetry"),
    "docker": ("dockerfile", "docker-compose"),
    "npm": ("npm",),
}

ECOSYSTEM_RE = re.compile(r"^[ \t]*-?[ \t]*package-ecosystem:[ \t]*[\"']?([A-Za-z0-9_-]+)")


def bots(dependabot_text, renovate_text, dependabot_path, renovate_path):
    """Two dependency bots over one dependency is a defect, not a configuration.

    Both files existing is fine when they cover DISJOINT ecosystems, and fine when
    one is disabled. Both enabled over the same ecosystem produces duplicate and
    conflicting pull requests, and the duplication is invisible until the second
    bot is installed -- which is the state this repository was in.
    """
    problems = []
    ecos = sorted(
        {
            m.group(1)
            for m in (ECOSYSTEM_RE.match(line) for line in dependabot_text.splitlines())
            if m
        }
    )
    if not ecos:
        problems.append(
            f"{dependabot_path} declares no `package-ecosystem`; this check could not read it, "
            f"and a bot config it cannot read is one it cannot ask about"
        )
        return problems
    try:
        cfg = json.loads(renovate_text)
    except ValueError as err:
        problems.append(
            f"{renovate_path} is not readable JSON ({err}); this check could not read it"
        )
        return problems
    if not isinstance(cfg, dict):
        problems.append(f"{renovate_path} is not a JSON object; this check could not read it")
        return problems
    if cfg.get("enabled") is False:
        return problems
    managers = cfg.get("enabledManagers")
    if managers is None:
        overlap, why = ecos, "it declares no `enabledManagers`, so it covers every manager it detects"
    else:
        overlap = [e for e in ecos if set(ECOSYSTEM_MANAGERS.get(e, (e,))) & set(managers)]
        why = f"its `enabledManagers` is {sorted(managers)}"
    if overlap:
        problems.append(
            f"{dependabot_path} and {renovate_path} are BOTH enabled over {overlap}: {why}. Two "
            f"bots updating one dependency produce duplicate and conflicting pull requests; "
            f"disable one, or scope them to disjoint ecosystems"
        )
    return problems


def check(text, path=".github/workflows/ci.yml"):
    problems = []
    jobs, key_lines = parse(text)
    # A parser that stops understanding the file has to say so; "no problems"
    # from a parse that found nothing is the failure this file is about.
    if len(jobs) < 5:
        problems.append(
            f"{path}: parsed {len(jobs)} job(s); this check could not read the workflow"
        )
        return problems
    # And the same rule one job at a time. The check above fires on a wholesale
    # parse failure; losing ONE job out of thirteen is the shape that matters,
    # because `check()` only asks about jobs `parse()` recorded.
    if key_lines != len(jobs):
        problems.append(
            f"{path}: parsed {len(jobs)} job(s) from {key_lines} job key line(s); this check "
            f"could not read the workflow, and a job it cannot read is a job it cannot ask about"
        )
    if GATE not in jobs:
        problems.append(f"{path} has no `{GATE}` job, so nothing aggregates its jobs")
        return problems

    gate = jobs[GATE]
    covered, why = reachable(jobs, gate["needs"])
    for name in jobs:
        if name in covered or name in WAIVERS:
            continue
        via = why.get(name)
        if via is not None and via in jobs:
            cond = jobs[via].get("if") or "(none)"
            problems.append(
                f"job `{name}` is reached only through `{via}`, which runs `if: {cond}` and "
                f"therefore runs even when `{name}` fails, so its failure cannot fail the gate"
            )
        else:
            problems.append(
                f"job `{name}` is not reachable from `{GATE}`'s needs, so its failure "
                f"cannot fail the gate a branch rule keys on"
            )
    for name in WAIVERS:
        if name not in jobs:
            problems.append(f"`{name}` is waived out of `{GATE}` but is not a job of {path}")
        elif name in covered and name != GATE:
            problems.append(f"`{name}` is waived out of `{GATE}` but IS reachable from it")

    # Rule 6. A job the gate covers may not carry JOB-level `continue-on-error`.
    # Read the module docstring for why this holds without settling what
    # `needs.<job>.result` reports for such a job: both answers are wrong, one by
    # gating nothing and one by saying the opposite of what it does.
    for name in sorted(set(covered) | {GATE}):
        if name not in jobs:
            continue
        coe = strip_comment(jobs[name]["continue_on_error"] or "")
        if coe and coe.lower() != "false":
            problems.append(
                f"job `{name}` is covered by `{GATE}` and carries job-level "
                f"`continue-on-error: {coe}`: either GitHub reports its `needs` result as success, "
                f"in which case the gate is green over a red job, or it reports failure, in which "
                f"case the flag does nothing and tells a reader the opposite -- neither is a shape "
                f"the required check may have. A step-level continue-on-error with a re-raising "
                f"step is the legitimate form and is not this"
            )

    if unwrap(gate.get("if")) != GATE_IF:
        problems.append(
            f"`{GATE}`'s `if:` is {gate.get('if')!r}, want `{GATE_IF}`: GitHub skips `{GATE}` "
            f"as soon as a job it needs fails, and a required check that was SKIPPED does not "
            f"block a merge -- so without this line the gate reports nothing and the pull "
            f"request merges over a red job"
        )

    vstep = verdict_step(gate)
    if vstep is None:
        problems.append(
            f"no step of `{GATE}` has `run:` hack/ci-gate-verify.sh, so its verdict is not the "
            f"one with a self-test"
        )

    needs_env, needs_n = resolve_env(gate, vstep, "NEEDS")
    if needs_env != NEEDS_EXPR:
        problems.append(
            f"`{GATE}`'s NEEDS is {needs_env!r}, want {NEEDS_EXPR!r}: a literal map passes "
            f"every other rule here and gates nothing"
        )
    if needs_n > 1:
        problems.append(
            f"`{GATE}` sets NEEDS in {needs_n} places: GitHub resolves a step's `env:` over the "
            f"job's, so a decoy is set more than once and the value this check reads need not be "
            f"the value the verdict receives"
        )

    raw, may_skip_n = resolve_env(gate, vstep, "MAY_SKIP")
    if may_skip_n > 1:
        problems.append(
            f"`{GATE}` sets MAY_SKIP in {may_skip_n} places: GitHub resolves a step's `env:` over "
            f"the job's, so a decoy is set more than once and the value this check reads need not "
            f"be the value the verdict receives"
        )
    listed = [p for p in re.split(r"[,\s]+", raw or "") if p]
    if set(listed) != set(MAY_SKIP_CONDITIONS):
        problems.append(
            f"`{GATE}`'s MAY_SKIP is {sorted(listed)}, want {sorted(MAY_SKIP_CONDITIONS)}: a "
            f"skip is accepted for every job it names, so widening it makes the required gate "
            f"a rubber stamp"
        )
    for name in listed:
        if name not in jobs:
            problems.append(f"MAY_SKIP names `{name}`, which is not a job of {path}")
            continue
        want = MAY_SKIP_CONDITIONS.get(name)
        got = jobs[name].get("if")
        if not got:
            problems.append(
                f"MAY_SKIP waives `{name}`, which carries no job-level `if:`; a job with no "
                f"skip condition has no declared reason to be skipped"
            )
        elif want is not None and got != want:
            problems.append(
                f"MAY_SKIP waives `{name}` on `if: {want}`, but the job now reads `if: {got}`; "
                f"the waiver IS the condition, so a changed condition has to be re-justified"
            )
        if jobs[name]["needs"]:
            problems.append(
                f"MAY_SKIP waives `{name}`, which has `needs: {jobs[name]['needs']}`; a job "
                f"skipped by a failed prerequisite is marked skipped too and would be accepted "
                f"as the declared skip"
            )
    return problems


def _probe_consumer(cond):
    """A `probeonly` job whose ONLY consumer is `workflows`, carrying `cond`.

    Anchored on the job key line alone, so an edit to the `workflows` job's
    name or description cannot quietly turn the mutation into a no-op.
    """

    def mutate(text):
        return text.replace(
            "  workflows:\n",
            "  probeonly:\n    name: probe\n    runs-on: ${{ fromJSON(github.event_name == 'pull_request' && '[\"ubuntu-latest\"]' || '[\"self-hosted\", \"Linux\", \"X64\", \"public\"]') }}\n"
            "    timeout-minutes: 10\n"
            "    steps:\n      - run: 'true'\n\n"
            f"  workflows:\n    needs: probeonly\n    if: {cond}\n",
            1,
        )

    return mutate


# A second aggregator: `if: always()`, so it RUNS when its prerequisite failed,
# but it reads the results through the verdict script and goes red for them --
# which is exactly what `gate` is. Rule 1 has to accept it, or it is wrong in
# the strict direction about the file's own central shape.
_SUBGATE = (
    "  probeonly:\n    name: probe\n    runs-on: ${{ fromJSON(github.event_name == 'pull_request' && '[\"ubuntu-latest\"]' || '[\"self-hosted\", \"Linux\", \"X64\", \"public\"]') }}\n"
    "    timeout-minutes: 10\n"
    "    steps:\n      - run: 'true'\n\n"
    "  subgate:\n    name: subgate\n    if: always()\n    needs: [probeonly]\n"
    "    runs-on: ${{ fromJSON(github.event_name == 'pull_request' && '[\"ubuntu-latest\"]' || '[\"self-hosted\", \"Linux\", \"X64\", \"public\"]') }}\n    timeout-minutes: 10\n    steps:\n"
    "      - name: verify\n        env:\n          NEEDS: ${{ toJSON(needs) }}\n"
    "        run: ./hack/ci-gate-verify.sh\n\n"
    "  workflows:\n"
)


# One row per rule: a mutation of the real workflow that MUST produce a
# problem, named by words the problem has to contain. A rule whose mutation
# still passes is a rule that cannot fail. Every one of these was first
# demonstrated by a reviewer against the unmutated check.
MUTATIONS = [
    (
        "a compute job sends pull requests directly to self-hosted runners",
        lambda t: t.replace(PR_LINUX_RUNNER, SELF_HOSTED_LINUX, 1),
        "PR-safe runner routing",
    ),
    (
        "cross compilation bypasses the pull-request runner selection",
        lambda t: t.replace(PR_CROSS_RUNNER, "${{ matrix.runner }}", 1),
        "PR-safe runner routing",
    ),
    (
        "a cross compilation PR leg names a persistent runner",
        lambda t: t.replace("- os: windows-latest", "- os: self-hosted", 1),
        "hosted OS labels",
    ),
    (
        # The regression this rule exists for, and the cheapest one to make:
        # `runs-on: ubuntu-latest` is what anybody adding or editing a job
        # types, and a job quietly back on a hosted runner costs money while
        # nothing reports it. The sibling failure -- a label naming a pool no
        # runner carries -- is NOT here because it does not fail at all, it
        # QUEUES for the 24 hours GitHub allows; `.github/actionlint.yaml`
        # declares the custom labels so that `make workflows-lint` catches it
        # instead, watched failing on a `privat` typo.
        "a moved job reverted to a GitHub-hosted runner",
        lambda t: t.replace(
            "    runs-on: " + PR_LINUX_RUNNER,
            "    runs-on: ubuntu-latest",
            1,
        ),
        "grows back",
    ),
    (
        # The regression that cost 92 job-minutes and killed `generated`: the
        # cache input is the default, so DELETING the line is the mutation, and
        # it is exactly what someone copying another job's `setup-go` block
        # would produce.
        "a self-hosted job restored the module cache again",
        lambda t: t.replace(
            "          go-version-file: go.mod\n          cache: ${{ github.event_name == 'pull_request' }}\n", "          go-version-file: go.mod\n", 1
        ),
        "decided per leg",
    ),
    (
        # And the other half, which is just as silent: `cache: false` carried
        # onto a hosted runner, where nothing survives the job.
        "cache: false carried onto a GitHub-hosted runner",
        lambda t: t.replace(
            "          cache-dependency-path: hack/tools/go.sum",
            "          cache: false",
            1,
        ),
        "ephemeral",
    ),
    (
        # The third branch of the same rule, and the one a reader is most
        # likely to "simplify": compile-cross is a single matrix whose legs sit
        # in DIFFERENT pools, so any static value is wrong for one of them.
        # Note what the mutation does NOT trip -- the runs-on rule skips an
        # expression it cannot resolve, so without this branch nothing asks.
        "a cache decided per leg collapsed to one static value",
        lambda t: t.replace(
            "          cache: ${{ github.event_name == 'pull_request' || matrix.os == 'macos-latest' }}",
            "          cache: false",
            1,
        ),
        "decided per leg",
    ),
    (
        "MAY_SKIP widened to every job",
        lambda t: t.replace(
            "MAY_SKIP: test-arm64",
            "MAY_SKIP: fmt, lint, stdlib-only, test, test-arm64, compile-cross, "
            "build-modules, harness, checks, generated, workflows",
        ),
        "rubber stamp",
    ),
    (
        "the waived job's if: replaced with false",
        lambda t: t.replace(
            "    if: ${{ !github.event.repository.private }}", "    if: false"
        ),
        "re-justified",
    ),
    (
        "the waived job gains a needs:",
        lambda t: t.replace(
            "  test-arm64:\n    name: test arm64 (core)",
            "  test-arm64:\n    needs: modules\n    name: test arm64 (core)",
        ),
        "failed prerequisite",
    ),
    (
        "a job dropped from the gate's needs",
        lambda t: t.replace(", generated, workflows]", ", workflows]"),
        "not reachable",
    ),
    (
        "NEEDS hardcoded instead of toJSON(needs)",
        lambda t: t.replace(
            "NEEDS: ${{ toJSON(needs) }}", "NEEDS: '{\"fmt\":{\"result\":\"success\"}}'"
        ),
        "gates nothing",
    ),
    (
        "the verdict replaced with an inline expression",
        lambda t: t.replace(
            "run: ./hack/ci-gate-verify.sh", "run: jq -e . <<< \"$NEEDS\""
        ),
        "self-test",
    ),
    (
        "the verdict replaced but still named in a comment",
        lambda t: t.replace(
            "run: ./hack/ci-gate-verify.sh",
            'run: jq -e . <<< "$NEEDS"  # was: ./hack/ci-gate-verify.sh',
        ),
        "self-test",
    ),
    (
        "a decoy job-level MAY_SKIP over a widened step-level one",
        lambda t: t.replace(
            "  gate:\n    name: gate\n    if: always()\n",
            "  gate:\n    name: gate\n    if: always()\n    env:\n      MAY_SKIP: test-arm64\n",
        ).replace(
            "MAY_SKIP: test-arm64\n        run:",
            "MAY_SKIP: fmt, lint, stdlib-only, test, test-arm64, compile-cross, "
            "build-modules, harness, checks, generated, workflows\n        run:",
        ),
        "rubber stamp",
    ),
    (
        "a decoy job-level NEEDS over a hardcoded step-level one",
        lambda t: t.replace(
            "  gate:\n    name: gate\n    if: always()\n",
            "  gate:\n    name: gate\n    if: always()\n    env:\n"
            "      NEEDS: ${{ toJSON(needs) }}\n",
        ).replace(
            "          NEEDS: ${{ toJSON(needs) }}\n",
            "          NEEDS: '{\"fmt\":{\"result\":\"success\"}}'\n",
        ),
        "gates nothing",
    ),
    (
        "MAY_SKIP set at job level as well as on the verdict step",
        lambda t: t.replace(
            "  gate:\n    name: gate\n    if: always()\n",
            "  gate:\n    name: gate\n    if: always()\n    env:\n      MAY_SKIP: test-arm64\n",
        ),
        "sets MAY_SKIP in 2 places",
    ),
    (
        "NEEDS set at job level as well as on the verdict step",
        lambda t: t.replace(
            "  gate:\n    name: gate\n    if: always()\n",
            "  gate:\n    name: gate\n    if: always()\n    env:\n"
            "      NEEDS: ${{ toJSON(needs) }}\n",
        ),
        "sets NEEDS in 2 places",
    ),
    (
        "the gate's own if: always() removed",
        lambda t: t.replace(
            "  gate:\n    name: gate\n    if: always()\n", "  gate:\n    name: gate\n"
        ),
        "does not block a merge",
    ),
    (
        "the gate's if: weakened to success()",
        lambda t: t.replace("  gate:\n    name: gate\n    if: always()\n",
                            "  gate:\n    name: gate\n    if: success()\n"),
        "does not block a merge",
    ),
    (
        "a job reached only through an always() consumer",
        _probe_consumer("always()"),
        "runs even when",
    ),
    (
        "a job reached only through a failure() consumer",
        _probe_consumer("${{ failure() }}"),
        "runs even when",
    ),
    (
        "a job reached only through a success() || failure() consumer",
        _probe_consumer("${{ success() || failure() }}"),
        "runs even when",
    ),
    (
        "a job whose key line carries a trailing comment",
        lambda t: t.replace(
            "  workflows:\n",
            "  probejob: # split out of checks\n    name: probe\n    runs-on: ${{ fromJSON(github.event_name == 'pull_request' && '[\"ubuntu-latest\"]' || '[\"self-hosted\", \"Linux\", \"X64\", \"public\"]') }}\n"
            "    timeout-minutes: 10\n    steps:\n      - run: 'true'\n\n  workflows:\n",
            1,
        ),
        "not reachable",
    ),
    (
        "a job key this parser cannot read at all",
        lambda t: t.replace(
            "  workflows:\n",
            "  \"probejob\":\n    name: probe\n    runs-on: ${{ fromJSON(github.event_name == 'pull_request' && '[\"ubuntu-latest\"]' || '[\"self-hosted\", \"Linux\", \"X64\", \"public\"]') }}\n"
            "    timeout-minutes: 10\n    steps:\n      - run: 'true'\n\n  workflows:\n",
            1,
        ),
        "could not read the workflow",
    ),
    (
        "the jobs: block renamed so the parser finds nothing",
        lambda t: t.replace("\njobs:\n", "\njobz:\n"),
        "could not read the workflow",
    ),
    # The timeout rule, four ways. The first is the defect as it stood in this
    # file -- thirteen jobs, no bound -- reduced to one job.
    (
        "a job's timeout-minutes deleted",
        lambda t: t.replace("    timeout-minutes: 60\n", "", 1),
        "no `timeout-minutes`",
    ),
    (
        "a timeout-minutes key with no value",
        lambda t: t.replace(
            "    timeout-minutes: 60\n", "    timeout-minutes:\n", 1
        ),
        "bounds nothing",
    ),
    (
        "a timeout of zero minutes",
        lambda t: t.replace(
            "    timeout-minutes: 60\n", "    timeout-minutes: 0\n", 1
        ),
        "not a positive number",
    ),
    (
        # Anchored on `gate`, which is still GitHub-hosted, and NOT on the
        # `test` job's 130 as it was: `test` moved to the self-hosted pool,
        # where this rule correctly does NOT apply, so the old anchor left
        # this mutation passing over a rule it no longer reached. A guard
        # whose own test stops testing it as the tree moves is the shape
        # the maintenance notes calls bounds decay, and this is an instance of
        # it caught by the self-test rather than in review.
        "a timeout above the ceiling a hosted runner enforces",
        lambda t: t.replace(
            "    timeout-minutes: 10\n",
            "    timeout-minutes: 1300\n",
            1,
        ),
        "never takes effect",
    ),
    # The concurrency rule, four ways: absent, a constant group, a group that is
    # unique per run, and an unstated direction.
    (
        "the workflow-level concurrency: key renamed away",
        lambda t: t.replace("\nconcurrency:\n", "\nconcurrencz:\n", 1),
        "has no `concurrency:`",
    ),
    (
        "a concurrency group that is a constant",
        lambda t: t.replace(
            "  group: ci-${{ github.workflow }}-${{ github.ref }}\n", "  group: ci\n", 1
        ),
        "shares one group",
    ),
    (
        "a concurrency group keyed on the run id, so it groups nothing",
        lambda t: t.replace(
            "  group: ci-${{ github.workflow }}-${{ github.ref }}\n",
            "  group: ci-${{ github.run_id }}\n",
            1,
        ),
        "no-op",
    ),
    (
        "cancel-in-progress left unstated",
        lambda t: t.replace(
            "  cancel-in-progress: ${{ github.event_name == 'pull_request' }}\n", "", 1
        ),
        "does not declare `cancel-in-progress`",
    ),
    # And rule 6: a gated job that cannot fail the gate because it is allowed to
    # fail by itself.
    (
        "a gated job given job-level continue-on-error",
        lambda t: t.replace(
            "  checks:\n    name: ratchet, ADRs, models, generated files, workspace\n",
            "  checks:\n    continue-on-error: true\n"
            "    name: ratchet, ADRs, models, generated files, workspace\n",
            1,
        ),
        "continue-on-error",
    ),
]

# The other direction. A rule that is wrong in the strict direction rejects a
# workflow that gates correctly, and the cost of that is a check somebody turns
# off. These shapes MUST stay clean; the unmutated file is the first of them.
CLEAN = [
    (
        "a second aggregator that re-reads the needs it was handed",
        lambda t: t.replace("  workflows:\n", _SUBGATE, 1).replace(
            ", generated, workflows]", ", generated, workflows, subgate]"
        ),
    ),
    (
        "a consumer whose if: names no status function",
        _probe_consumer("${{ github.event_name == 'push' }}"),
    ),
    (
        "a consumer whose if: is plain success()",
        _probe_consumer("${{ success() }}"),
    ),
    # The three shapes the new rules must NOT reject. Each is a legitimate thing
    # somebody will write, and rejecting it is how a check gets turned off.
    (
        "cancel-in-progress: false, which is right for a workflow that records",
        lambda t: t.replace(
            "  cancel-in-progress: ${{ github.event_name == 'pull_request' }}\n",
            "  cancel-in-progress: false\n",
            1,
        ),
    ),
    (
        "a timeout-minutes given as an expression",
        lambda t: t.replace(
            "    timeout-minutes: 20\n",
            "    timeout-minutes: ${{ fromJSON(vars.BUILD_TIMEOUT) }}\n",
            1,
        ),
    ),
    (
        "a STEP-level continue-on-error, which rule 6 does not reach",
        lambda t: t.replace(
            "      - run: make workflows-lint\n",
            "      - run: make workflows-lint\n        continue-on-error: true\n",
            1,
        ),
    ),
]


# The bot rule's own table. It needs no files: each row is the two configurations
# as text, so the rule is watched against shapes the repository has never had as
# well as the one it had.
_DB_REAL = "version: 2\nupdates:\n  - package-ecosystem: gomod\n  - package-ecosystem: github-actions\n"
_RN_ON = '{"extends": ["config:recommended"]}'
_RN_OFF = '{"enabled": false, "extends": ["config:recommended"]}'
_RN_SPLIT = '{"enabled": true, "enabledManagers": ["npm"]}'
_RN_GOMOD = '{"enabled": true, "enabledManagers": ["gomod"]}'

BOT_MUTATIONS = [
    ("both bots enabled, Renovate unrestricted", _DB_REAL, _RN_ON, "BOTH enabled"),
    ("both bots enabled, Renovate restricted to gomod", _DB_REAL, _RN_GOMOD, "BOTH enabled"),
    ("a dependabot.yml with no package-ecosystem", "version: 2\nupdates: []\n", _RN_OFF,
     "could not read it"),
    ("a renovate.json that is not JSON", _DB_REAL, "{not json", "could not read it"),
]

BOT_CLEAN = [
    ("Renovate disabled, which is this repository's shape", _DB_REAL, _RN_OFF),
    ("a genuine split: Renovate restricted to an ecosystem dependabot does not name",
     _DB_REAL, _RN_SPLIT),
]


def check_all(text, path):
    """The gate's five wiring rules, rule 6, and the two every-workflow rules.

    This is what the command line and the self-test both drive, so a rule that is
    watched failing here is a rule `make ci-parity` runs, and the reverse.
    """
    return check(text, path) + hygiene(text, path)


def selftest(path):
    with open(path, encoding="utf-8") as f:
        text = f.read()
    ok = fails = 0
    clean = check_all(text, path)
    if clean:
        fails += 1
        print(f"  FAIL  the unmutated {path} reports a problem", file=sys.stderr)
        for p in clean:
            print(f"        {p}", file=sys.stderr)
    else:
        ok += 1
        print(f"  ok    the unmutated {path} is clean")
    for name, mutate, want in MUTATIONS:
        mutated = mutate(text)
        if mutated == text:
            fails += 1
            print(
                f"  FAIL  {name}: the mutation changed nothing, so the row proves nothing",
                file=sys.stderr,
            )
            continue
        found = check_all(mutated, path)
        if any(want in p for p in found):
            ok += 1
            print(f"  ok    {name}")
        else:
            fails += 1
            print(f"  FAIL  {name}: no problem containing {want!r}; got {found}", file=sys.stderr)
    for name, mutate in CLEAN:
        mutated = mutate(text)
        if mutated == text:
            fails += 1
            print(
                f"  FAIL  {name}: the mutation changed nothing, so the row proves nothing",
                file=sys.stderr,
            )
            continue
        found = check_all(mutated, path)
        if found:
            fails += 1
            print(f"  FAIL  {name}: must stay clean, got {found}", file=sys.stderr)
        else:
            ok += 1
            print(f"  ok    {name} (stays clean)")
    for name, db, rn, want in BOT_MUTATIONS:
        found = bots(db, rn, ".github/dependabot.yml", "renovate.json")
        if any(want in p for p in found):
            ok += 1
            print(f"  ok    {name}")
        else:
            fails += 1
            print(f"  FAIL  {name}: no problem containing {want!r}; got {found}", file=sys.stderr)
    for name, db, rn in BOT_CLEAN:
        found = bots(db, rn, ".github/dependabot.yml", "renovate.json")
        if found:
            fails += 1
            print(f"  FAIL  {name}: must stay clean, got {found}", file=sys.stderr)
        else:
            ok += 1
            print(f"  ok    {name} (stays clean)")
    print(
        f"gate-parity: {ok} passed, {fails} failed "
        f"({len(MUTATIONS)} mutations that must be caught, "
        f"{len(CLEAN)} shapes that must stay clean, "
        f"{len(BOT_MUTATIONS)} bot-config mutations, "
        f"{len(BOT_CLEAN)} bot-config shapes that must stay clean)"
    )
    return fails == 0


def main(argv):
    if argv and argv[0] == "--selftest":
        path = argv[1] if len(argv) > 1 else ".github/workflows/ci.yml"
        sys.exit(0 if selftest(path) else 1)
    # `--hygiene <path>...` is the every-workflow half: the two rules that do not
    # depend on a `gate` job, so they can be asked of all fourteen files. Exits 0
    # like every other mode here -- the caller owns the verdict, for the reason
    # the module docstring gives.
    if argv and argv[0] == "--hygiene":
        for path in argv[1:]:
            with open(path, encoding="utf-8") as f:
                for p in hygiene(f.read(), path):
                    print(p)
        return
    # `--bots <dependabot.yml> <renovate.json>`: two dependency bots over one
    # dependency. Same exit-0 contract as every other mode.
    if argv and argv[0] == "--bots":
        db_path = argv[1] if len(argv) > 1 else ".github/dependabot.yml"
        rn_path = argv[2] if len(argv) > 2 else "renovate.json"
        with open(db_path, encoding="utf-8") as f:
            db = f.read()
        with open(rn_path, encoding="utf-8") as f:
            rn = f.read()
        for p in bots(db, rn, db_path, rn_path):
            print(p)
        return
    path = argv[0] if argv else ".github/workflows/ci.yml"
    with open(path, encoding="utf-8") as f:
        text = f.read()
    for p in check_all(text, path):
        print(p)


main(sys.argv[1:])
