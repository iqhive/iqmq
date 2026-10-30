#!/usr/bin/env bash
# Every target `make ci` runs has to run in the CI workflow too.
#
# `make ci` is the local gate every increment is held to, and
# `.github/workflows/ci.yml` is what a pull request is held to. They drifted
# once already: `fuzz-seeds`, `scorecard-check`, `bench-lint`,
# `benchguard-test`, `bakeoff-test`, `build` and `vet` were in the local gate
# and in no job, so seven of the twenty-four checks an increment passed here
# were never run on a branch. Nothing noticed, because a
# workflow that does not run a check is indistinguishable from one whose check
# passes.
#
# The rule: for each prerequisite of the `ci` target, either the workflow runs
# `make <target>` literally, or WAIVERS names it with the reason the workflow
# runs it some other way. A waiver has to be exact -- an unlisted target and a
# target with no reason both fail -- so the answer to "is this checked on a
# branch?" is always yes or a written explanation.
set -euo pipefail
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$root"
wf=.github/workflows/ci.yml

# Targets the workflow runs by another route, and the route. The value is
# printed in the report, so a reader can check the claim.
declare -A WAIVERS=(
  [lint]="the lint job installs golangci-lint with the action then runs ./hack/ci-group.sh <group> hack/lint.sh, which is the SAME script make lint runs per module locally, sharded over the six groups of hack/module-groups.json"
  [test-race]="the test job runs ./hack/ci-group.sh <group> go test -race, sharded over runners, which make test-race runs in one pass -- over every group on a pull request, a tag or a dispatch, and over every group but core on a push to a branch"
  [apidiff]="the checks job runs ./hack/apidiff.sh, which is the script the target runs"
)

# The `ci:` line is read with a tolerance for leading whitespace and WITHOUT
# one for a target name in its help comment, because both happened at once.
#
# The merge that landed C05 rewrote the line as
#
#         ci: fmt-check ... bakeoff-test ## Everything the CI workflow runs docs-check \
# work-sync: ## Regenerate go.work from the module list and fail on drift
#
# and three things followed. `docs-check` sat after the `##`, so make never ran
# it -- the help text claimed it did. The trailing backslash continued the
# COMMENT (a backslash-newline inside a make comment continues the comment), so
# `work-sync`'s rule line vanished and its recipe attached to `ci`: `make
# work-sync` printed "Nothing to be done" and exited 0, which is a check in
# this repository's own gate that could not fail, and `make ci` silently ran
# `go work use -r .`. And the leading whitespace made THIS script's `^ci:`
# match nothing, so it exited 2 rather than reporting the drift it exists to
# report.
# The prerequisites moved to `ci-inner:` when `ci` was wrapped in cpu-policy, so
# that the whole run holds the heavy-job lock once. `ci:` now carries only the
# help text and the re-entry, and `ci-inner:` carries the list. Both guards
# above still apply to it, and the backslash one matters MORE there: with no
# `##` on the line a trailing backslash is an ordinary make continuation, which
# this sed would not follow, so the prerequisites after it would be checked by
# nobody. `^[[:space:]]*ci:` cannot match `ci-inner:`, so there is no ambiguity.
ci_line="$(sed -n 's/^[[:space:]]*ci-inner:[[:space:]]*\(.*\)$/\1/p' Makefile)"
[ -n "${ci_line// /}" ] || { echo "ci-parity: could not read the ci-inner target's prerequisites from Makefile" >&2; exit 2; }
case "$ci_line" in
  *'\') echo "ci-parity: the ci-inner: line ends with a backslash; this script reads only its first line, so the prerequisites after it would go unchecked" >&2; exit 2 ;;
esac
deps="${ci_line%%##*}"
help="${ci_line#*##}"
[ "$help" = "$ci_line" ] && help=""
[ -f "$wf" ] || { echo "ci-parity: $wf is missing" >&2; exit 2; }

# A target counts as run only by a STEP whose `run:` is `make <target>` and
# that runs on every event: neither the step nor its job carries an `if:`.
# This used to be a grep for the text `make <target>` anywhere in the file,
# which a comment satisfied, and which could not see a step that runs only
# sometimes: test-tags, allocs and cover-threshold were steps of the `test`
# matrix gated on `matrix.group == 'core'`, a push to main drops the core leg,
# and so main never ran them while this check passed. The first run of all
# three after a merge was release validation, once the module tags were
# public.
#
# The awk reads the workflow's layout: a job at two spaces, its keys at four,
# a step's `- ` at six and the step's keys at eight.
unconditional="$(awk '
  function flush() {
    if (target != "" && !stepif && !jobif) print target
    target = ""; stepif = 0
  }
  /^  [^ #][^:]*:[ \t]*$/ { flush(); jobif = 0; next }
  /^    if:/ { jobif = 1; next }
  /^      - / { flush() }
  /^      - if:/ || /^        if:/ { stepif = 1 }
  /^      (- |  )run:[ \t]*make [a-z0-9][a-z0-9-]*[ \t]*$/ {
    t = $0; sub(/.*run:[ \t]*make[ \t]+/, "", t); sub(/[ \t]*$/, "", t); target = t
  }
  END { flush() }
' "$wf" | sort -u)"

problems=0
checked=0
for t in $deps; do
  checked=$((checked + 1))
  if printf '%s\n' "$unconditional" | grep -qx -- "$t"; then
    continue
  fi
  if [ -n "${WAIVERS[$t]:-}" ]; then
    printf '  %-16s another route: %s\n' "$t" "${WAIVERS[$t]}"
    continue
  fi
  if grep -qE "^[[:space:]]*(- )?run:[[:space:]]*make $t[[:space:]]*\$" "$wf"; then
    printf '  %-16s run by %s only under an if: (of its step or its job), so an event that does not meet it never runs the target\n' "$t" "$wf" >&2
    problems=$((problems + 1))
    continue
  fi
  printf '  %-16s NOT RUN by %s, and no waiver says why\n' "$t" "$wf" >&2
  problems=$((problems + 1))
done

# And the other direction, which nothing checked: a target the workflow runs
# has to be a prerequisite of `make ci`.
#
# The forward half above only asks "is every local check also run on a
# branch?". It cannot see a check that is run on a branch and NOT locally, and
# that is how `docs-check` spent two increments outside `make ci`: the merge of
# inc/C05 resolved the one-line `ci:` hunk with `docs-check` on the far side of
# the `##` comment, ci.yml kept running it, and an increment's local gate
# stopped running it with nothing to say so. `make ci` is the gate an increment
# is held to a dozen times and ci.yml is the gate a pull request meets once, so
# a check that lives only in the workflow is a check every increment ships
# without.
declare -A REVERSE_WAIVERS=(
  [ci]="the workflow aggregate step; ci cannot be a prerequisite of itself"
)
wfdeps="$(sed -nE 's/^[[:space:]]*(- )?run:[[:space:]]*make ([a-z0-9][a-z0-9-]*)[[:space:]]*$/\2/p' "$wf" | sort -u)"
[ -n "${wfdeps// /}" ] || { echo "ci-parity: found no 'run: make <target>' step in $wf" >&2; exit 2; }
wfchecked=0
for t in $wfdeps; do
  wfchecked=$((wfchecked + 1))
  case " $deps " in
    *" $t "*) continue ;;
  esac
  if [ -n "${REVERSE_WAIVERS[$t]:-}" ]; then
    printf '  %-16s workflow-only: %s\n' "$t" "${REVERSE_WAIVERS[$t]}"
    continue
  fi
  printf '  %-16s run by %s but NOT a prerequisite of make ci\n' "$t" "$wf" >&2
  problems=$((problems + 1))
done

# A waiver for a target the ci list no longer has is dead text that would
# excuse the next target to take that name.
for t in "${!WAIVERS[@]}"; do
  case " $deps " in
    *" $t "*) ;;
    *) printf '  %-16s waived but not a prerequisite of make ci\n' "$t" >&2; problems=$((problems + 1)) ;;
  esac
done

# A target name in the ci: line's HELP COMMENT is invisible to make, and reads
# to a human as if it ran. That is how docs-check came to be absent from the
# gate while the line said it was there.
for t in $help; do
  case "$t" in
    *[!a-z0-9-]*) continue ;;
  esac
  if grep -qE "^[[:space:]]*$t:" Makefile; then
    printf '  %-16s named in the ci: line AFTER the ## comment, so make does not run it\n' "$t" >&2
    problems=$((problems + 1))
  fi
done

# And every Go module has to be in exactly one CI test group.
#
# hack/module-groups.json is a hand-written map from group to module list and
# hack/ci-group.sh SKIPS a module that is not there, so a module nobody added
# is a module the CI `test` job never compiles. The `examples` group was `[]`
# for as long as it existed and the shard printed "group examples has no
# modules yet" and exited 0 -- the same shape as the drift above: a job that
# runs nothing is indistinguishable from a job whose checks pass. A module in
# two groups is checked twice and is a mistake of the other kind, so both
# directions are held.
groups=hack/module-groups.json
[ -f "$groups" ] || { echo "ci-parity: $groups is missing" >&2; exit 2; }
mapfile -t grouped < <(python3 -c '
import json, sys
for g, mods in json.load(open(sys.argv[1])).items():
    for m in mods:
        print(m)
' "$groups")
modules=0
for m in $(./hack/modules.sh); do
  modules=$((modules + 1))
  n=0
  for g in "${grouped[@]}"; do [ "$g" = "$m" ] && n=$((n + 1)); done
  case "$n" in
    1) ;;
    0) printf '  %-32s in NO CI test group; hack/ci-group.sh would skip it\n' "$m" >&2; problems=$((problems + 1)) ;;
    *) printf '  %-32s in %d CI test groups; it would be tested %d times\n' "$m" "$n" "$n" >&2; problems=$((problems + 1)) ;;
  esac
done
# A group entry naming a module that does not exist yet is RESERVED and not a
# problem: the map is written ahead of the plan -- ci-group.sh skips a module
# that is not there -- so this half reports and does not fail. It is still
# worth printing, because a reservation for a module that never arrives is the
# only way a name in this file can be wrong without anything noticing.
reserved=0
for g in "${grouped[@]}"; do
  [ "$g" = "." ] && continue
  [ -f "$g/go.mod" ] && continue
  reserved=$((reserved + 1))
  printf '  %-32s reserved in %s; no go.mod yet\n' "$g" "$groups"
done

# And the nightly capability-refresh path may not swallow a failure.
#
# `.github/workflows/integration.yml`'s aggregate job ran
#
#     cp -f conformance/reports/incoming/*.json conformance/reports/latest/ || true
#     make generate
#
# which cannot fail on stale input: a download that produced nothing was
# swallowed by the `||`, generation regenerated the COMMITTED reports, the diff
# was empty and the job reported success having refreshed nothing. A nightly
# that silently republishes last month's capabilities is worse than one that
# fails, so the step is `make aggregate` -- which attests the delivery before
# it merges it -- and the job may not contain an error-swallowing `||` at all.
nightly=.github/workflows/integration.yml
[ -f "$nightly" ] || { echo "ci-parity: $nightly is missing" >&2; exit 2; }
aggregate_problems="$(python3 hack/aggregate-parity.py "$nightly")"
if [ -n "$aggregate_problems" ]; then
  while IFS= read -r line; do
    printf '  %-16s %s\n' "aggregate" "$line" >&2
    problems=$((problems + 1))
  done <<< "$aggregate_problems"
fi

# And the third question, which the two above cannot ask.
#
# Both halves so far grep for the text `make <target>` -- the CLAIM that the
# workflow runs a check. Whether a FAILURE of that check can fail the workflow
# is a different question, and the answer lived in one line nobody checked:
# `gate`'s `needs`. It named nine of eleven jobs, and the two it omitted were
# `generated` -- generate-check, suites-check, docs-check, security-check,
# security-check-selftest, seven targets
# of `make ci` -- and `test-arm64`. Every one of those seven RAN on every pull
# request, which is exactly why the first two rules were satisfied and the gate
# a branch rule keys on was not gating them: a red `generated` job would have
# merged.
gate_problems="$(python3 hack/gate-parity.py "$wf")"
if [ -n "$gate_problems" ]; then
  while IFS= read -r line; do
    printf '  %-16s %s\n' "gate" "$line" >&2
    problems=$((problems + 1))
  done <<< "$gate_problems"
fi

# And the fourth question, which is about every workflow and not only this one.
#
# The three rules above are all about ci.yml. The other thirteen files had no
# operational bound at all: ONE of the fourteen declared `concurrency:`, so a
# superseded run of the other thirteen kept going, and thirty of the
# forty-one jobs had no `timeout-minutes` -- including all thirteen of ci.yml's,
# the biggest workflow in the repository, where a hung `go test` held a shared
# runner for GitHub's default SIX HOURS and the run reported nothing a reader
# could act on. `hack/gate-parity.py --hygiene` asks both of every file; it is the
# same parser and the same code path the self-test drives, so a rule watched
# failing there is a rule enforced here.
#
# ci.yml is skipped in this loop and not in the check: the call above already runs
# `check_all`, which includes these two rules, so asking again would count one
# problem twice.
hygiene_files=()
for f in .github/workflows/*.yml; do
  [ "$f" = "$wf" ] && continue
  hygiene_files+=("$f")
done
[ "${#hygiene_files[@]}" -gt 0 ] || { echo "ci-parity: found no workflow beside $wf; this check would watch nothing" >&2; exit 2; }
hygiene_problems="$(python3 hack/gate-parity.py --hygiene "${hygiene_files[@]}")"
if [ -n "$hygiene_problems" ]; then
  while IFS= read -r line; do
    printf '  %-16s %s\n' "hygiene" "$line" >&2
    problems=$((problems + 1))
  done <<< "$hygiene_problems"
fi

# And the fifth: two dependency bots over one dependency.
#
# .github/dependabot.yml and renovate.json both exist and both covered gomod,
# github-actions and pip -- the same ecosystems, both weekly on Monday, with the
# same three package groups. Only Dependabot is installed, so the duplication was
# invisible: all 26 dependency pull requests came from app/dependabot and there is
# no renovate/* branch and no Renovate Dependency Dashboard issue. Installing the
# Renovate app would have duplicated every one of them on the next Monday, with
# nothing in the repository able to notice beforehand. renovate.json now carries
# `enabled: false` and a `description` recording why, and this is what stops the
# pair drifting back.
bots_db=.github/dependabot.yml
bots_rn=renovate.json
if [ -f "$bots_db" ] && [ -f "$bots_rn" ]; then
  bot_problems="$(python3 hack/gate-parity.py --bots "$bots_db" "$bots_rn")"
  if [ -n "$bot_problems" ]; then
    while IFS= read -r line; do
      printf '  %-16s %s\n' "bots" "$line" >&2
      problems=$((problems + 1))
    done <<< "$bot_problems"
  fi
  bots_state="both present, one enabled"
else
  bots_state="only one dependency bot is configured"
fi

# And that rule is itself watched failing against a mutated copy of the real
# workflow -- one row per rule in `MUTATIONS` -- and watched STAYING CLEAN
# against shapes that gate correctly, in `CLEAN`, because a reachability rule
# that rejects a legitimate aggregator is a rule somebody eventually turns off.
# A review reproduced MAY_SKIP widened to every job and the waived job's `if:`
# replaced with `false` against the first version, which reported neither: the
# rule walked `needs` and read neither MAY_SKIP nor any job's `if:`. A second
# review then got both past it again behind a decoy `env:` block, because the
# value the rule read was the job's and the value the verdict receives is the
# step's.
#
# How many rows of each there are is NOT written here. It is read back below
# from the line the self-test itself prints, so it cannot go stale the way
# "nine ways" did on the day a tenth row was added.
if ! gate_selftest="$(python3 hack/gate-parity.py --selftest "$wf" 2>&1)"; then
  printf '  %-16s %s\n' "gate" "hack/gate-parity.py --selftest failed:" >&2
  printf '%s\n' "$gate_selftest" >&2
  problems=$((problems + 1))
fi
gate_rows="$(printf '%s\n' "$gate_selftest" |
  sed -n 's/^gate-parity: [0-9]* passed, [0-9]* failed (\(.*\))$/\1/p')"

# The gate's verdict is a script so that it can be watched failing, and this is
# where it is watched: a failed job, a cancelled job, an empty `needs` map
# (which satisfies jq's `all` vacuously), a needs entry that is not an object,
# and a SKIPPED job that MAY_SKIP does not name. A gate nobody has watched fail
# is assumed not to work, and this one had never been watched at all.
verifier=hack/ci-gate-verify.sh
[ -x "$verifier" ] || { echo "ci-parity: $verifier is missing or not executable" >&2; exit 2; }
if ! verify_out="$("$verifier" --selftest 2>&1)"; then
  printf '  %-16s %s\n' "gate" "$verifier --selftest failed:" >&2
  printf '%s\n' "$verify_out" >&2
  problems=$((problems + 1))
fi
verify_rows="$(printf '%s\n' "$verify_out" |
  sed -n 's/^ci-gate-verify: \([0-9]*\) passed, [0-9]* failed$/\1/p')"

# Both counts in the closing line come from the self-tests' own summaries. If
# either summary stops being readable, say so rather than printing a number
# that was true once: a count in prose is the thing this branch corrected six
# times, and a count nobody recomputes is indistinguishable from a wrong one.
if [ -z "$gate_rows" ] || [ -z "$verify_rows" ]; then
  printf '  %-16s %s\n' "gate" \
    "could not read the self-test row counts from their own summary lines" >&2
  problems=$((problems + 1))
fi

if [ "$problems" -ne 0 ]; then
  echo "ci-parity: $problems problem(s): a target of make ci that $wf does not run, a target $wf runs that make ci does not, a module no CI test group holds, a nightly capability refresh that cannot fail, a job whose failure cannot fail the gate, a job with no timeout-minutes, or a workflow with no concurrency group" >&2
  exit 1
fi
echo "ci-parity: ok, $checked target(s) of make ci run in $wf (${#WAIVERS[@]} by another route)"
echo "ci-parity: ok, $wfchecked target(s) $wf runs are prerequisites of make ci (${#REVERSE_WAIVERS[@]} waived as workflow-only)"
echo "ci-parity: ok, $modules module(s) each in exactly one group of $groups ($reserved reserved for a module that does not exist yet)"
echo "ci-parity: ok, $nightly's aggregate job runs make aggregate and swallows no failure"
echo "ci-parity: ok, every job of $wf is reachable from the gate's needs ($gate_rows), and the gate's verdict passed its own $verify_rows-row self-test"
echo "ci-parity: ok, every job of all $(( ${#hygiene_files[@]} + 1 )) workflow(s) declares timeout-minutes and every workflow declares a concurrency group"
echo "ci-parity: ok, no two dependency bots cover one ecosystem ($bots_state)"
