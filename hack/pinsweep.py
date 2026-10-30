#!/usr/bin/env python3
"""Which AMBIGUOUS rules have their Retryable flag pinned by a test?

the message contract gives ambiguity precedence over retryability, and
`mq.IsRetryable` applies it: `e.Retryable && !IsAmbiguous(err)` (assumption
A31c.2).  A consequence is easy to miss and impossible to see by running the
suites: once a rule is ambiguous, its Retryable flag is INVISIBLE through the
predicate, so a test that reads the flag only that way stops pinning it -- in
either direction -- while still passing.  A driver could then flip an
ambiguous rule's Retryable flag and no test in its module would notice.

This is the instrument for that class.  For ONE construction it flips the
Retryable flag, runs the tests that could see it, and reports whether anything
failed:

    PINNED      a test noticed the flip: one that passes on the unmutated tree
                failed with the flag flipped; the flag is asserted somewhere
    UNPINNED    every test passed with the flag flipped; nothing pins it
    NO-MUTATION the pattern matched nothing, or the bytes did not change
    BUILD-ERR   the mutation did not compile; the site needs a hand-written one
    TEST-ERR    the run failed, but no test failed that passes unmutated (a
                test already broken, a TestMain that exits, a module that does
                not build); the run can say neither PINNED nor UNPINNED

Each module's tests run once on the unmutated tree first, the CONTROL run, and
only a failure the control run does not have counts as noticing the flip.

WHY IT IS BUILT PER CONSTRUCTION, AND WHY THAT IS THE WHOLE POINT
-----------------------------------------------------------------
Two earlier versions of this sweep were wrong in two different ways, and both
mistakes are the reason this file exists rather than a shell one-liner.

The first flipped every construction in a file at once by searching backwards
from each hit for a `Retryable:` to flip.  With several rules in one slice the
searches overlapped, the same two lines were flipped twice, and the file came
back BYTE IDENTICAL while the script reported "4 flags flipped".  Every
UNPINNED verdict it produced meant "no mutation was applied".  So this version
mutates exactly ONE construction per run -- overlap is structurally impossible
-- and refuses to report a verdict unless the file's md5 actually changed.

The second scored one verdict per MODULE: it flipped a module's constructions
together, so a single failing test anywhere in the module marked every
ambiguous rule in it PINNED.  That is blind to exactly the defect it was built
to find:

    had drivers/amqp10 held a second, ALREADY-PINNED ambiguous rule, flipping
    both at once would have returned PINNED, and the blocking finding that
    started this work would have survived undetected.

Hence: the verdict unit is the construction, never the module.

WHAT IT DOES NOT COVER
----------------------
  * It runs only the modules named for each construction, not the repository.
    A rule pinned from somewhere unlisted reads UNPINNED.  `drivers/kafka`'s
    rules are the known case -- their pin lives in `drivers/kafka/kafkago`'s
    cross-driver table -- and the inventory names both modules for them.
  * A composed-ambiguity entry (`alsoAmbiguous`, `ambiguous`) flips the whole
    copied set at once, because the flag is added by a transformer rather than
    written per rule.  That verdict IS an aggregate, and it is marked as one.
  * It says nothing about whether a rule's flag is CORRECT -- only whether a
    test would notice it changing.

Usage:
    hack/pinsweep.py one <file> <pattern> <mod>[,<mod>...]
    hack/pinsweep.py all [--inventory hack/pinsweep.tsv] [--filter SUBSTR]
                         [--out RESULTS.tsv] [--baseline BASELINE.tsv]
    hack/pinsweep.py compare <baseline.tsv> <results.tsv>
    hack/pinsweep.py --help

THE STORED BASELINE
-------------------
The full sweep is minutes of module test runs, so it belongs in a nightly or
weekly job rather than in `make ci` -- and a job whose output a person has to
read line by line is a job nobody reads.  `--out` writes the verdicts in a
stable, sorted, machine-readable form; `hack/pinsweep-baseline.tsv` is that file
COMMITTED; and `compare` says what changed with the exit status doing the
talking:

    0   every construction's verdict is the one the baseline records
    1   a REGRESSION: a construction the baseline records as PINNED is now
        UNPINNED, or one it records with a verdict now refuses to produce one
    3   DRIFT that is not a regression: an improvement to PINNED, a
        construction the baseline does not name, or one it names that the
        sweep no longer covers; or no stored baseline at all, so nothing
        could be compared

Drift is red rather than a warning, and the reason is the point of storing a
baseline at all: an improvement nobody records leaves the baseline saying
UNPINNED, so the SAME construction regressing back is then invisible.  A gate
that warns is a gate people learn to ignore (assumption C10.17), so the fix is
to regenerate the file and commit it, which is what the message says.  The two
are separate exit statuses because they are different answers: 1 is "somebody
removed a test", 3 is "this file is out of date".

Every `go test` runs under the host's core cap, read from `hack/cpucap.sh`;
the sweep is many module test runs and must not saturate a shared machine.
`hack/pinsweep_test.sh` is the self-test: it pins this tool's behaviour on one
construction known to be pinned and one known not to be, so a version that
reports verdicts it cannot support fails loudly.
"""

import argparse
import hashlib
import os
import re
import subprocess
import sys

RETRY_FIELD = re.compile(r"(Retryable:\s*)([^,}\n]+)")
RETRY_ASSIGN = re.compile(r"(\.Retryable\s*=\s*)([^\n]+?)(\s*)$")
# `x.Ambiguous = true`, or the parallel form `x.Code, x.Ambiguous = c, true`
# when the flag and its value are the last operands. Go requires as many
# values as operands, so the last pair is the flag and true.
AMB_ASSIGN = re.compile(r"^(\s*)(?:[^\s=,]+,\s*)*([^\s=,]+)\.Ambiguous\s*=\s*(?:[^=,\n]+,\s*)*true\s*$")
AMB_FIELD = re.compile(r"Ambiguous:\s*\S")

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

_cap_env = None


def cap_env():
    """The environment every `go test` below runs in: the host CPU ceiling.

    GOMAXPROCS and -p used to be the LITERALS 4 and 4 here, which is a second
    implementation of the ceiling and exactly the class assumption C10.11 is
    about -- and one `hack/cpucap-check.sh` could not see, because it scans the
    Makefile and `hack/*.sh` and this is a .py.  Reading them from the one script
    that owns the arithmetic means IQMQ_MAX_CPUS and IQMQ_TEST_WIDTH reach this
    sweep too, and a 2-core runner gets 2 rather than being told it has 4
    (assumption C13.7).

    It REFUSES rather than falling back to the old literals.  A silent fallback
    is how assumption C10.12's four clamps came to fail open together; and the
    whole sweep is many module test runs, so a version that guessed the ceiling
    on a host where the script is missing is the one place a guess costs most.

    Computed lazily and cached: `compare` and `audit` run no tests and must not
    need the script at all.
    """
    global _cap_env
    if _cap_env is None:
        script = os.path.join(ROOT, "hack/cpucap.sh")
        r = subprocess.run([script, "pair"], capture_output=True, text=True)
        pair = r.stdout.split()
        if r.returncode != 0 or len(pair) != 2:
            sys.exit(
                f"pinsweep: {script} pair did not answer a GOMAXPROCS and -p pair "
                f"(exit {r.returncode}, stdout {r.stdout!r}, stderr {r.stderr.strip()!r}); "
                "the sweep is many module test runs and will not guess the host's ceiling"
            )
        _cap_env = dict(os.environ, GOMAXPROCS=pair[0], GOFLAGS=f"-p={pair[1]}", GOWORK="off")
    return _cap_env


def md5(s):
    return hashlib.md5(s.encode()).hexdigest()


def flip_value(v):
    v = v.strip().rstrip(",")
    if v == "true":
        return "false"
    if v == "false":
        return "true"
    # An expression such as `!ambiguous && code == mq.CodeUnavailable`, or a
    # copied field such as `oe.Retryable`.  Forcing it true is a flip for
    # every input that made it false.
    return "true"


def _match_forward(text, open_at):
    """Offset of the `}` closing the `{` at open_at."""
    depth = 0
    for k in range(open_at + 1, len(text)):
        if text[k] == "{":
            depth += 1
        elif text[k] == "}":
            if depth == 0:
                return k
            depth -= 1
    return len(text)


def literal_span(text, ls, le):
    """The braces of the construction owning the line [ls, le).

    If the line OPENS a literal that it does not close -- `foo := Rule{` -- that
    literal is the construction.  Otherwise the construction is the innermost
    literal enclosing the line.  Getting this right is what keeps the flip off
    a NEIGHBOURING rule: `serverPublish := driverkit.Rule{Match: matchCode(`
    sits one line below `serverRetry`'s closing `), Code: ..., Retryable: true}`,
    and a nearest-line search flips serverRetry instead -- the non-ambiguous
    rule, which is not the thing under test.
    """
    line = text[ls:le]
    if "{" in line and line.count("{") == line.count("}"):
        # A rule written entirely on one line is its OWN construction, and the
        # search must not leave it.  Without this, a single-line rule carrying
        # no Retryable field -- `{Match: …, Code: …, Ambiguous: true},` -- sent
        # the search into the NEXT rule in the slice and flipped that one
        # instead, silently scoring a verdict about the wrong rule.  Six of
        # the thirty-three constructions do that, including the amqp10 row whose
        # repair this whole sweep exists to confirm.
        return ls, le
    if line.count("{") > line.count("}"):
        open_at = ls + line.rindex("{")
        return open_at, _match_forward(text, open_at)
    depth = 0
    for k in range(ls - 1, -1, -1):
        if text[k] == "}":
            depth += 1
        elif text[k] == "{":
            if depth == 0:
                return k, _match_forward(text, k)
            depth -= 1
    return 0, len(text)


def mutate(text, pattern):
    """Flip the Retryable flag of the ONE construction matching pattern."""
    lines = text.split("\n")
    hits = [i for i, ln in enumerate(lines) if pattern in ln]
    if len(hits) != 1:
        return None, f"pattern matched {len(hits)} lines, want exactly 1"
    i = hits[0]

    # Composed ambiguity: `out[i].Ambiguous = true` inside a transformer that
    # copies a rule set, or `oe.Ambiguous = true` on an error a mapper built.
    # Flip the copy's Retryable beside it, which leaves the non-ambiguous uses
    # of the same source rules untouched.
    m = AMB_ASSIGN.match(lines[i])
    if m:
        indent, recv = m.group(1), m.group(2)
        lines.insert(i + 1, f"{indent}{recv}.Retryable = !{recv}.Retryable")
        return "\n".join(lines), None

    ls = sum(len(l) + 1 for l in lines[:i])
    le = ls + len(lines[i])
    start, end = literal_span(text, ls, le)
    # Forward from the matched line first, then backward but never out of the
    # construction: a multi-line *OpError may carry `Retryable:` above its
    # `Ambiguous: true,` line.
    for lo, hi in ((ls, end), (start, ls)):
        window = text[lo:hi]
        for rx in (RETRY_FIELD, RETRY_ASSIGN):
            m = rx.search(window)
            if m:
                at = lo + m.start(2)
                return text[:at] + flip_value(m.group(2)) + text[lo + m.end(2) :], None

    # No Retryable in the construction: it is false by omission, so add one
    # beside the Ambiguous field that defines the construction.
    m = AMB_FIELD.search(text, ls if start >= ls else start, end)
    if not m:
        return None, "no Retryable field and no Ambiguous field in the construction"
    return text[: m.start()] + "Retryable: true, " + text[m.start() :], None


BUILD_ERR = re.compile(r"build failed|duplicate field|syntax error|undefined:")
FAIL_LINE = re.compile(r"^\s*--- FAIL: (\S+)", re.M)
TIMED_OUT = re.compile(r"^panic: test timed out", re.M)


def go_test(tree, mod):
    p = subprocess.run(
        ["go", "test", "-count=1", "./..."],
        cwd=os.path.join(tree, mod),
        env=cap_env(),
        capture_output=True,
        text=True,
    )
    return p.returncode, p.stdout + p.stderr


def failed_tests(out):
    """The tests a `go test` run names as failed, a timed-out binary as one."""
    names = set(FAIL_LINE.findall(out))
    if TIMED_OUT.search(out):
        names.add("(a test binary timed out)")
    return names


# The control run: each module's failing tests on the UNMUTATED tree, once per
# sweep. PINNED means a test noticed the flip, and a test that fails without
# the flip noticed nothing. Without this, any failing run that showed no build
# error was PINNED: a test already broken, a TestMain that exits, or a module
# that does not build under GOWORK=off (every driver whose go.mod requires an
# unpublished root, which reads `missing go.sum entry` and names no test) each
# pinned every construction they touched, and the sweep could not report one
# going UNPINNED.
_controls = {}


def control(tree, mod):
    key = (tree, mod)
    if key not in _controls:
        code, out = go_test(tree, mod)
        _controls[key] = failed_tests(out) if code != 0 else set()
    return _controls[key]


def run_tests(tree, mods, controls):
    """Run each module's tests over the mutant; PINNED needs a NEW failure.

    A module whose run failed with no test failing that the control run
    passes cannot say UNPINNED, because the tests it lost may be the ones that
    pin: TEST-ERR, unless another module pins the flag.
    """
    undecided = ""
    for mod in mods:
        code, out = go_test(tree, mod)
        if BUILD_ERR.search(out):
            return "BUILD-ERR", out
        if code == 0:
            continue
        new = sorted(failed_tests(out) - controls[mod])
        if new:
            return "PINNED", f"{mod}: --- FAIL: {new[0]}"
        if not undecided:
            last = next((l.strip() for l in reversed(out.split("\n")) if l.strip()), "")
            undecided = (
                f"{mod}: the run failed, and no test failed that passes on the unmutated tree "
                f"(unmutated failures: {', '.join(sorted(controls[mod])) or 'none named'}; last line: {last})"
            )
    if undecided:
        return "TEST-ERR", undecided
    return "UNPINNED", ""


def one(tree, path, pattern, mods, verbose=False):
    full = os.path.join(tree, path)
    with open(full) as f:
        original = f.read()
    before = md5(original)
    mutated, err = mutate(original, pattern)
    if mutated is None:
        return "NO-MUTATION", err
    if md5(mutated) == before:
        return "NO-MUTATION", "the bytes did not change"
    # Before the file is touched: the control runs read the unmutated tree.
    controls = {mod: control(tree, mod) for mod in mods}
    try:
        with open(full, "w") as f:
            f.write(mutated)
        if verbose:
            for a, b in zip(original.split("\n"), mutated.split("\n")):
                if a != b:
                    print(f"      - {a.strip()}\n      + {b.strip()}")
        verdict, note = run_tests(tree, mods, controls)
    finally:
        # Restore unconditionally: an interrupted sweep must never leave a
        # mutant in the tree.  The check below is not belt-and-braces -- a
        # half-restored file is worse than any verdict.
        with open(full, "w") as f:
            f.write(original)
    with open(full) as f:
        if md5(f.read()) != before:
            return "RESTORE-FAILED", full
    return verdict, note


def read_inventory(path):
    rows, excluded = [], []
    with open(path) as f:
        for ln in f:
            ln = ln.rstrip("\n")
            if ln.startswith("#exclude\t"):
                parts = ln.split("\t")
                excluded.append((parts[1], parts[2]))
                continue
            if not ln.strip() or ln.lstrip().startswith("#"):
                continue
            parts = ln.split("\t")
            if len(parts) != 3:
                sys.exit(f"pinsweep: malformed inventory line: {ln!r}")
            rows.append((parts[0], parts[1], parts[2].split(",")))
    return rows, excluded


# What makes an error ambiguous, and therefore what the audit must find.
#
# The FLAG is the obvious half. The CODES are the half it is easy to forget:
# mq.IsAmbiguous is true for CodeAmbiguous and CodeSettlementUnknown whatever
# the flag says (assumption A31b.4), so a rule carrying either code is
# ambiguous with no `Ambiguous:` field anywhere in it -- and its Retryable flag
# is exactly as unobservable. An audit that looked only for the flag would not
# notice such a rule being ADDED, which is the same one-directional blindness
# this instrument exists to correct, one axis along. Every such construction in
# the tree today also sets the flag nearby, so this widens what the audit can
# SEE rather than what it currently reports.
CONSTRUCTS = re.compile(
    r"Ambiguous:\s*(?:true|ambiguous)\b"
    r"|\.Ambiguous\s*=\s*true\b"
    r"|Code:\s*(?:mq\.)?Code(?:Ambiguous|SettlementUnknown)\b"
)


def audit(tree, inventory, baseline=""):
    """Does the inventory still name every ambiguous construction in the tree?

    This is what turns "the sweep enumerated the class" from a claim into a
    check.  An inventory is a hand-written list, and hand-written lists rot: a
    driver that adds an ambiguous rule tomorrow is a construction nothing
    sweeps, and without this nothing in the repository would say so.
    """
    rows, excluded = read_inventory(inventory)
    problems = []

    # Ownership is by the construction's SPAN, not by the line the pattern
    # happens to match: an inventory entry keyed on `serverPublish :=
    # driverkit.Rule{Match: matchCode(` owns the `Ambiguous: true` two lines
    # below it, inside the same literal.
    owned = set()
    for path, pattern, _ in rows:
        full = os.path.join(tree, path)
        if not os.path.exists(full):
            problems.append(f"inventory names {path}, which does not exist")
            continue
        with open(full) as f:
            text = f.read()
        lines = text.split("\n")
        hits = [i for i, ln in enumerate(lines) if pattern in ln]
        if len(hits) != 1:
            problems.append(f"{path}: pattern {pattern!r} selects {len(hits)} lines, want exactly 1")
            continue
        i = hits[0]
        if AMB_ASSIGN.match(lines[i]):
            owned.add((path, i))
            continue
        ls = sum(len(l) + 1 for l in lines[:i])
        start, end = literal_span(text, ls, ls + len(lines[i]))
        first = text.count("\n", 0, start)
        last = text.count("\n", 0, end)
        for j in range(first, last + 1):
            owned.add((path, j))

    # .claude holds agent worktrees, each a full checkout: walking them read
    # every other worktree's constructions as this tree's.
    for dirpath, dirnames, filenames in os.walk(tree):
        dirnames[:] = [
            d for d in dirnames if d not in (".git", ".claude", "vendor", "benchmarks", "__pycache__")
        ]
        for name in filenames:
            if not name.endswith(".go") or name.endswith("_test.go"):
                continue
            full = os.path.join(dirpath, name)
            rel = os.path.relpath(full, tree)
            with open(full, errors="replace") as f:
                for i, ln in enumerate(f):
                    if not CONSTRUCTS.search(ln):
                        continue
                    if (rel, i) in owned:
                        continue
                    if any(rel == f2 and p in ln for f2, p in excluded):
                        continue
                    problems.append(
                        f"{rel}:{i + 1} is an ambiguous construction the inventory does not name"
                    )
    # And the third list: the STORED BASELINE. The inventory audit catches a
    # construction being ADDED to the tree and says nothing about whether
    # anything pins it -- that is what the nightly sweep is for -- but a
    # construction the baseline does not name has no verdict for the nightly to
    # compare against, so the nightly would report it as "new" and exit 3 a week
    # later rather than here, for free, on the change that added it. This costs
    # no test run (assumption C13.7).
    if baseline and os.path.exists(baseline):
        stored = read_results(baseline)
        named = {(r[0], r[1]) for r in rows}
        for path, pattern in sorted(named - set(stored)):
            problems.append(
                f"{path}: the inventory names {pattern!r} and the stored baseline has no verdict "
                "for it; run `make pinsweep-baseline` and commit the result"
            )
        for path, pattern in sorted(set(stored) - named):
            problems.append(
                f"{path}: the stored baseline records a verdict for {pattern!r}, which the "
                "inventory no longer names; run `make pinsweep-baseline` and commit the result"
            )
    return rows, problems


# A verdict line as `hack/pinsweep-baseline.tsv` and `--out` spell it:
# VERDICT <TAB> file <TAB> pattern. The note is deliberately NOT in it -- it
# carries a test name and a duration, so a file that held it would differ on
# every run and nothing could be compared.
def write_results(path, results):
    with open(path, "w") as f:
        f.write(
            "# hack/pinsweep.py verdicts, one per ambiguous construction.\n"
            "# VERDICT <TAB> file <TAB> pattern. Regenerate with:\n"
            "#     make pinsweep-baseline\n"
            "# A PINNED -> UNPINNED move is a test that stopped pinning an\n"
            "# ambiguous rule's Retryable flag, which no suite can report\n"
            "# because the flag is invisible through mq.IsRetryable once the\n"
            "# rule is ambiguous (assumption C13.7).\n"
        )
        for verdict, path_, pattern in sorted(results, key=lambda r: (r[1], r[2])):
            f.write(f"{verdict}\t{path_}\t{pattern}\n")


def read_results(path):
    out = {}
    with open(path) as f:
        for ln in f:
            ln = ln.rstrip("\n")
            if not ln.strip() or ln.lstrip().startswith("#"):
                continue
            parts = ln.split("\t")
            if len(parts) != 3:
                sys.exit(f"pinsweep: malformed verdict line in {path}: {ln!r}")
            out[(parts[1], parts[2])] = parts[0]
    return out


# no_baseline is what a sweep says when there is no stored baseline to compare
# against, and it exits 3: a missing baseline is the most out-of-date one there
# is. It used to exit 0 while `hack/pinsweep-baseline.tsv` was gitignored, so
# the monthly soak job never had one and went green whatever the sweep found.
# A comparison that died on the missing file would throw away a ninety-minute
# sweep, so the verdicts are still written: they are the baseline to commit.
def no_baseline(baseline_path, results_path):
    return (
        f"pinsweep compare: no stored baseline at {baseline_path}, so nothing was "
        f"compared and no regression can be reported; the verdicts in {results_path} "
        "are this run's. Generate the baseline with `make pinsweep-baseline` and commit it."
    )


# compare reports what moved between a stored baseline and a fresh sweep.
#
# The ORDER of the checks is the message's usefulness: a regression is printed
# first and decides the exit status, because a run that is both a regression and
# stale is a regression. Everything is printed either way -- a comparison that
# stopped at the first difference would need one run per difference to clear.
def compare(baseline_path, results_path):
    base = read_results(baseline_path)
    got = read_results(results_path)
    regressions, improvements, added, removed, refusals = [], [], [], [], []
    for key, verdict in sorted(got.items()):
        was = base.get(key)
        if was is None:
            added.append((key, verdict))
        elif verdict == was:
            continue
        elif verdict not in ("PINNED", "UNPINNED"):
            refusals.append((key, was, verdict))
        elif was == "PINNED" and verdict == "UNPINNED":
            regressions.append(key)
        else:
            improvements.append((key, was, verdict))
    for key in sorted(base):
        if key not in got:
            removed.append((key, base[key]))

    for path_, pattern in regressions:
        print(f"  REGRESSION   PINNED -> UNPINNED  {path_}  {pattern}")
    for (path_, pattern), was, now in refusals:
        print(f"  REGRESSION   {was} -> {now}  {path_}  {pattern}")
    for (path_, pattern), was, now in improvements:
        print(f"  improved     {was} -> {now}  {path_}  {pattern}")
    for (path_, pattern), verdict in added:
        print(f"  new          {verdict:<9}  {path_}  {pattern}")
    for (path_, pattern), verdict in removed:
        print(f"  gone         was {verdict:<9}  {path_}  {pattern}")

    if regressions or refusals:
        print(
            f"pinsweep compare: {len(regressions) + len(refusals)} REGRESSION(S) "
            f"against {baseline_path}. A construction that was PINNED is not any more: "
            "an ambiguous rule's Retryable flag is invisible through mq.IsRetryable, so "
            "no suite can report this and nothing else will. Find the test that used to "
            "read the flag off the *mq.OpError and restore it; do NOT update the baseline "
            "to match."
        )
        return 1
    if improvements or added or removed:
        print(
            f"pinsweep compare: {len(improvements)} improvement(s), {len(added)} new and "
            f"{len(removed)} gone against {baseline_path}, and no regression. The stored "
            "baseline is out of date: regenerate it with `make pinsweep-baseline` and commit "
            "it, or the next regression on one of these is compared against a stale verdict "
            "and reads as no change."
        )
        return 3
    print(f"pinsweep compare: {len(got)} construction(s) match {baseline_path}")
    return 0


def main():
    here = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("mode", choices=["one", "all", "audit", "compare"])
    ap.add_argument("args", nargs="*")
    ap.add_argument("--tree", default=here)
    ap.add_argument("--inventory", default=os.path.join(here, "hack/pinsweep.tsv"))
    ap.add_argument("--filter", default="")
    ap.add_argument("--out", default="", help="write the verdicts here, for a stored baseline")
    ap.add_argument(
        "--baseline",
        default="",
        help="compare the sweep against this stored baseline and exit 1 on a regression, 3 on drift",
    )
    ap.add_argument("-v", "--verbose", action="store_true")
    a = ap.parse_args()

    if a.mode == "one":
        if len(a.args) != 3:
            sys.exit("usage: pinsweep.py one <file> <pattern> <mod>[,<mod>...]")
        v, note = one(a.tree, a.args[0], a.args[1], a.args[2].split(","), a.verbose)
        print(f"{v}\t{a.args[0]}\t{a.args[1]}\t{note}")
        return 0 if v in ("PINNED", "UNPINNED") else 1

    if a.mode == "audit":
        baseline = a.baseline or os.path.join(a.tree, "hack/pinsweep-baseline.tsv")
        rows, problems = audit(a.tree, a.inventory, baseline)
        for p in problems:
            print(f"  {p}")
        print(f"pinsweep audit: {len(rows)} construction(s) named, {len(problems)} problem(s)")
        return 1 if problems else 0

    if a.mode == "compare":
        if len(a.args) != 2:
            sys.exit("usage: pinsweep.py compare <baseline.tsv> <results.tsv>")
        if not os.path.exists(a.args[0]):
            print(no_baseline(a.args[0], a.args[1]))
            return 3
        return compare(a.args[0], a.args[1])

    rows = [r for r in read_inventory(a.inventory)[0] if a.filter in r[0] + r[1]]
    bad = 0
    results = []
    print(f"pinsweep: {len(rows)} construction(s), tree {a.tree}")
    for path, pattern, mods in rows:
        v, note = one(a.tree, path, pattern, mods, a.verbose)
        print(f"  {v:<13} {path:<38} {pattern[:44]:<44} {note[:60]}")
        results.append((v, path, pattern))
        if v not in ("PINNED", "UNPINNED"):
            bad += 1
    # The results are written even when a construction refused a verdict, and
    # the refusal is written WITH them: a baseline comparison has to be able to
    # see BUILD-ERR where it expected PINNED, which is exactly the move a
    # rename makes and is a regression rather than a gap.
    out = a.out
    if a.baseline and not out:
        out = os.path.join(a.tree, "reports", "pinsweep", "verdicts.tsv")
    if out:
        os.makedirs(os.path.dirname(os.path.abspath(out)), exist_ok=True)
        write_results(out, results)
        print(f"pinsweep: verdicts written to {out}")
    if a.baseline and not os.path.exists(a.baseline):
        print(no_baseline(a.baseline, out))
        return 1 if bad else 3
    if a.baseline:
        # The comparison decides, and a refusal is already visible to it as a
        # verdict that is not PINNED or UNPINNED -- so `bad` does not also get
        # to fail the run, or a refused construction the baseline already
        # records would be reported twice with two different reasons.
        return compare(a.baseline, out)
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main())
