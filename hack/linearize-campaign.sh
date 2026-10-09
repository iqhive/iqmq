#!/usr/bin/env bash
# the test suite's memory linearizability campaign.
#
#   hack/linearize-campaign.sh [TIME]     TIME is the fuzz budget (default 30m)
#   hack/linearize-campaign.sh --selftest prove the campaign can fail
#
# `soak.yml` has called `make linearize-campaign TIME=3h` since the job was
# written and the target did not exist, so the weekly job failed at once, every
# week, with "No rule to make target" -- and the campaign has therefore never
# run.
#
# Three stages, each of which has to be green:
#
#   1. the checker itself (testkit/linear) under -race -- the instrument is
#      part of the code under test, and a checker that accepts everything would
#      make stages 2 and 3 vacuous;
#   2. the engine's recorded histories, LIN_COUNT times under -race --
#      TestEngineLinearizable and TestEngineLinearizableKeys record concurrent
#      publish/fetch/ack histories from competing consumers over both
#      partitionings §8.5 defines and check them with the Wing and Gong search;
#   3. FuzzEngineLinearizable for TIME under -race, where the operation mix,
#      the kind set and the shape come from the fuzzer.
#
# A per-stage report is written to $IQMQ_LINEARIZE_REPORTS (default
# reports/linearize), so the weekly job's artefact says which stage failed and
# for how long each ran rather than only that the job is red.
#
# Every package and pattern is overridable, which is what lets --selftest point
# the whole campaign at a package whose checks are known to fail.
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$root"

# The host CPU ceiling. Every `go test` below was unbounded, and stage 3 is the
# one that runs away: a `-fuzz` run's `-parallel` workers are separate
# PROCESSES, so LIN_PARALLEL MULTIPLIES with GOMAXPROCS instead of dividing it,
# and on a 96-core host nothing here bounded either factor.
#
# The PRODUCT is bounded too, and it did not used to be. `cpucap.sh env` sets
# GOMAXPROCS=4 on this host and LIN_PARALLEL defaulted to 4 INDEPENDENTLY, so
# stage 3 ran four worker processes at four each -- 16, exactly the ceiling, by
# arithmetic coincidence rather than by derivation. `LIN_PARALLEL=8` was 32 and
# nothing here stopped it: the ceiling held for the DEFAULTS only, and
# `cpu-cap-check` could not see it because `-parallel "$parallel"` is a variable
# rather than a literal. The derivation and the clamp are
# below, where `parallel` is chosen.
eval "$("$root/hack/cpucap.sh" env)"

# The fixture's own corpus directory, and nothing else. A `-fuzz` run that
# fails on its seed writes no crasher, but a mutation that got as far as one
# would, and `make linearize-selftest` runs inside `make ci` -- so the removal
# names one fixed path under the repository root, refuses to run when that root
# did not resolve, and never takes a pattern.
FIXTURE=hack/testdata/linearize-selftest
clean_fixture() {
  [ -n "$root" ] || { echo "linearize-campaign: no repository root" >&2; exit 2; }
  [ -d "$root/$FIXTURE" ] || { echo "linearize-campaign: $FIXTURE is missing" >&2; exit 2; }
  rm -rf -- "$root/$FIXTURE/testdata"
}

if [ "${1:-}" = "--selftest" ]; then
  # refused_pkg runs the campaign with one selection deliberately wrong and
  # asserts it exits 2, says why, and starts no stage.
  refused_pkg() { # description checker-pkg engine-pkg test-pat fuzz-pat want-text
    local desc="$1" cpkg="$2" epkg="$3" tpat="$4" fpat="$5" want="$6" o s
    o="$(
      LIN_CHECKER_PKG="$cpkg" LIN_ENGINE_PKG="$epkg" \
      LIN_TEST="$tpat" LIN_FUZZ="$fpat" LIN_COUNT=1 \
      IQMQ_LINEARIZE_REPORTS="$(mktemp -d)" \
      "$0" 10s 2>&1
    )" && s=0 || s=$?
    clean_fixture
    if [ "$s" -ne 2 ]; then
      echo "linearize-campaign: SELF-TEST FAILED: $desc exited $s, want 2" >&2
      printf '%s\n' "$o" >&2
      fails=1
    elif ! printf '%s' "$o" | grep -q "$want"; then
      echo "linearize-campaign: SELF-TEST FAILED: $desc did not say $want" >&2
      printf '%s\n' "$o" >&2
      fails=1
    elif printf '%s' "$o" | grep -q '^linearize-campaign: stage '; then
      echo "linearize-campaign: SELF-TEST FAILED: $desc started a stage anyway" >&2
      fails=1
    fi
  }

  # The negative control. hack/testdata/linearize-selftest holds a genuinely
  # non-linearizable history and a check that fails on it; `testdata` keeps it
  # out of every `./...` walk in the repository, so it is compiled by this
  # command and by nothing else.
  #
  # `make linearize-campaign` used to be a target that did not exist, which is
  # the loudest possible failure. Its replacement is a target that runs three
  # `go test` invocations, and a green one of those is indistinguishable from
  # one that selected no tests -- `go test -run '^NoSuchTest$'` exits 0 and
  # prints ok. So the campaign is watched failing here, on all three stages.
  echo "linearize-campaign: self-test (every stage must FAIL)"
  out="$(
    LIN_CHECKER_PKG=./hack/testdata/linearize-selftest \
    LIN_ENGINE_PKG=./hack/testdata/linearize-selftest \
    LIN_TEST='^TestLinearizeSelfTest$' \
    LIN_FUZZ='^FuzzLinearizeSelfTest$' \
    LIN_COUNT=1 \
    IQMQ_LINEARIZE_REPORTS="$(mktemp -d)" \
    "$0" 10s 2>&1
  )" && st=0 || st=$?
  clean_fixture
  fails=0
  if [ "$st" -eq 0 ]; then
    echo "linearize-campaign: SELF-TEST FAILED: the campaign passed over a package whose every check fails" >&2
    fails=1
  fi
  # It is not enough that it went red: it has to have gone red for the reason
  # the campaign exists for, and the two reasons have to be told apart. The
  # fixture prints NEGATIVE-CONTROL-REJECTED when the checker rejects the
  # history and NEGATIVE-CONTROL-ACCEPTED when it does not, and BOTH messages
  # end "... is not linearizable" -- which is what this used to grep for, so a
  # `linear.Check` mutated to accept every history satisfied it. The marker
  # has to be present AND its opposite absent.
  if ! printf '%s' "$out" | grep -q 'NEGATIVE-CONTROL-REJECTED'; then
    echo "linearize-campaign: SELF-TEST FAILED: the checker never rejected the control history" >&2
    printf '%s\n' "$out" >&2
    fails=1
  fi
  if printf '%s' "$out" | grep -q 'NEGATIVE-CONTROL-ACCEPTED'; then
    echo "linearize-campaign: SELF-TEST FAILED: the checker ACCEPTED a history that is not linearizable;" \
         "every linearizability check in this repository is vacuous" >&2
    printf '%s\n' "$out" >&2
    fails=1
  fi
  for stage in checker engine fuzz; do
    if ! printf '%s' "$out" | grep -q "stage $stage: FAILED"; then
      echo "linearize-campaign: SELF-TEST FAILED: stage $stage did not report a failure" >&2
      fails=1
    fi
  done
  # And the other way a stage can stop checking: a pattern that selects
  # nothing. `go test -run '^NoSuchTest$'` exits 0 and prints ok, so a renamed
  # test would make a stage green without running anything -- the failure mode
  # that looks exactly like a pass. The campaign refuses to start on it, and
  # this is what says so.
  vac="$(
    LIN_CHECKER_PKG=./hack/testdata/linearize-selftest \
    LIN_ENGINE_PKG=./hack/testdata/linearize-selftest \
    LIN_TEST='^TestThisWasRenamedInTheLastRefactor$' \
    LIN_FUZZ='^FuzzLinearizeSelfTest$' \
    LIN_COUNT=1 \
    IQMQ_LINEARIZE_REPORTS="$(mktemp -d)" \
    "$0" 10s 2>&1
  )" && vst=0 || vst=$?
  clean_fixture
  if [ "$vst" -ne 2 ]; then
    echo "linearize-campaign: SELF-TEST FAILED: a pattern that selects nothing exited $vst, want 2" >&2
    printf '%s\n' "$vac" >&2
    fails=1
  fi
  if ! printf '%s' "$vac" | grep -q 'selects no Test function'; then
    echo "linearize-campaign: SELF-TEST FAILED: nothing said the pattern selects no Test function" >&2
    fails=1
  fi
  if printf '%s' "$vac" | grep -q 'stage engine'; then
    echo "linearize-campaign: SELF-TEST FAILED: it ran a stage over a pattern that selects nothing" >&2
    fails=1
  fi

  # Stage 1's own pre-flight, which did not exist: a bare `go test` over a
  # package with no test files prints `?  pkg [no test files]` and exits 0, so
  # the stage whose job is to stop the other two being vacuous was the one that
  # could itself be vacuous.
  refused_pkg "checker package with no test files" \
    ./hack/testdata/linearize-selftest/notests ./hack/testdata/linearize-selftest \
    '^TestLinearizeSelfTest$' '^FuzzLinearizeSelfTest$' 'selects no Test function'

  # And the KIND of function a pattern selects. `go test -list` matches tests
  # and fuzz targets alike, so a `-fuzz` pattern naming a Test selected
  # something, passed the old guard and then fuzzed nothing.
  refused_pkg "a fuzz pattern that names a Test function" \
    ./hack/testdata/linearize-selftest ./hack/testdata/linearize-selftest \
    '^TestLinearizeSelfTest$' '^TestLinearizeSelfTest$' 'selects no Fuzz function'

  # And the CPU ceiling, which is the one thing here that is not about a stage
  # being vacuous. Stage 3's `-parallel` workers are PROCESSES that multiply
  # with GOMAXPROCS, so an over-large LIN_PARALLEL has to CLAMP rather than
  # multiply -- and the only way to watch that is to ask for an absurd one and
  # read the command the stage actually ran, which `stage` prints.
  want_par="$("$root/hack/cpucap.sh" p)"
  clamp="$(
    LIN_CHECKER_PKG=./hack/testdata/linearize-selftest \
    LIN_ENGINE_PKG=./hack/testdata/linearize-selftest \
    LIN_TEST='^TestLinearizeSelfTest$' \
    LIN_FUZZ='^FuzzLinearizeSelfTest$' \
    LIN_COUNT=1 LIN_PARALLEL=64 \
    IQMQ_LINEARIZE_REPORTS="$(mktemp -d)" \
    "$0" 10s 2>&1
  )" || true
  clean_fixture
  if ! printf '%s' "$clamp" | grep -q 'LIN_PARALLEL=64 clamped to '"$want_par"; then
    echo "linearize-campaign: SELF-TEST FAILED: LIN_PARALLEL=64 was not clamped to $want_par, or said nothing" >&2
    printf '%s\n' "$clamp" >&2
    fails=1
  fi
  # The message is not the thing that bounds the run; the flag is. So the
  # STAGE LINE is read too, which is the only place the number that reached
  # `go test` appears.
  if ! printf '%s' "$clamp" | grep -qE 'stage fuzz: .* -parallel '"$want_par"'( |$)'; then
    echo "linearize-campaign: SELF-TEST FAILED: stage fuzz did not run at -parallel $want_par" >&2
    printf '%s\n' "$clamp" >&2
    fails=1
  fi
  if printf '%s' "$clamp" | grep -q -- '-parallel 64'; then
    echo "linearize-campaign: SELF-TEST FAILED: stage fuzz ran at -parallel 64, which at GOMAXPROCS" \
         "${GOMAXPROCS:-?} is $(( 64 * ${GOMAXPROCS:-1} )) runnable threads against a ceiling of" \
         "$("$root/hack/cpucap.sh" ceiling)" >&2
    fails=1
  fi
  # A value that is not a number must be REFUSED rather than clamped, because
  # every clamp here is the condition of an `if` and a "[: integer expected"
  # failure reads as FALSE -- the guard failing open on the typo it exists for.
  bad="$(LIN_PARALLEL=4x IQMQ_LINEARIZE_REPORTS="$(mktemp -d)" "$0" 10s 2>&1)" && bst=0 || bst=$?
  clean_fixture
  if [ "$bst" -ne 2 ] || ! printf '%s' "$bad" | grep -q 'LIN_PARALLEL must be a whole number'; then
    echo "linearize-campaign: SELF-TEST FAILED: LIN_PARALLEL=4x exited $bst without refusing it" >&2
    printf '%s\n' "$bad" >&2
    fails=1
  fi

  if [ "$fails" -ne 0 ]; then exit 1; fi
  echo "linearize-campaign: self-test ok -- all three stages fail on a non-linearizable history and say" \
       "so distinguishably, a selection that would run nothing is refused before any stage starts, and an" \
       "over-large LIN_PARALLEL clamps to the ceiling rather than multiplying past it"
  exit 0
fi

time="${1:-${TIME:-30m}}"
checker_pkg="${LIN_CHECKER_PKG:-./testkit/linear/...}"
engine_pkg="${LIN_ENGINE_PKG:-./internal/engine}"
lin_test="${LIN_TEST:-^TestEngineLinearizable}"
lin_fuzz="${LIN_FUZZ:-^FuzzEngineLinearizable$}"
count="${LIN_COUNT:-20}"

# Stage 3's `-parallel`, DERIVED from the ceiling rather than defaulted beside
# it. A `-fuzz` run's `-parallel` workers are separate PROCESSES, each with its
# own GOMAXPROCS, so the count multiplies with the width exactly as `-p` does --
# and the `eval` at the top of this file has already set the width.
#
# `hack/cpucap.sh p` is the bound, and it is the same verb `hack/fuzz-changed.sh`
# uses for its own `-parallel`: min(CEILING / GOMAXPROCS, AVAILABLE) with a
# floor of one, which is exactly the number whose product with the exported
# GOMAXPROCS cannot exceed IQMQ_MAX_CPUS. `cpucap.sh fuzz` is the WRONG verb
# here even though this is a fuzz run: that triple carries its own GOMAXPROCS of
# 2 for a campaign that also fans out in the shell, and this campaign is one
# `go test` at the width `env` exported.
#
# LIN_PARALLEL is still the override and it still LOWERS. What it no longer does
# is raise: an over-large value clamps and says so, because a knob that
# multiplies past the ceiling is not an operator's choice about this script, it
# is an operator's choice about the whole host -- and that knob is
# IQMQ_MAX_CPUS, which is one number rather than a factor hidden in one script.
lin_cap="$("$root/hack/cpucap.sh" p)"
parallel="${LIN_PARALLEL:-$("$root/hack/cpucap.sh" p)}"
# Validated by SHAPE before any arithmetic. `[ "$parallel" -gt "$lin_cap" ]` on a
# non-numeric value is a "[: integer expected" failure, and under `set -e` a
# failing `if` condition is simply FALSE -- so the clamp would fail OPEN on
# exactly the typo it exists to catch. That is assumption C10.12's lesson, one
# file over: every clamp in `hack/cpucap.sh` failed open together and printed a
# GOMAXPROCS of twenty digits with exit status 0.
case "$parallel" in
  ''|*[!0-9]*)
    echo "linearize-campaign: LIN_PARALLEL must be a whole number (got '$parallel')" >&2
    exit 2 ;;
esac
if [ "$parallel" -lt 1 ]; then parallel=1; fi
if [ "$parallel" -gt "$lin_cap" ]; then
  echo "linearize-campaign: LIN_PARALLEL=$parallel clamped to $lin_cap -- a -fuzz worker is a PROCESS, so" \
       "$parallel of them at GOMAXPROCS ${GOMAXPROCS:-?} would be $(( parallel * ${GOMAXPROCS:-1} )) runnable" \
       "threads against the $("$root/hack/cpucap.sh" ceiling)-CPU ceiling. Raise IQMQ_MAX_CPUS to raise it." >&2
  parallel="$lin_cap"
fi
reports="${IQMQ_LINEARIZE_REPORTS:-$root/reports/linearize}"
mkdir -p "$reports"
stamp="$(date -u +%Y%m%dT%H%M%SZ)"
report="$reports/$stamp.json"

status=0
rows=()

stage() { # name command...
  local name="$1"; shift
  local start end rc
  start=$(date +%s)
  echo "linearize-campaign: stage $name: $*"
  if "$@"; then rc=0; else rc=$?; fi
  end=$(date +%s)
  if [ "$rc" -eq 0 ]; then
    echo "linearize-campaign: stage $name: ok in $((end-start))s"
    rows+=("{\"stage\":\"$name\",\"result\":\"ok\",\"duration_s\":$((end-start))}")
  else
    # Printed, not swallowed, and printed in a form the self-test can assert
    # on: a stage that fails silently is the defect this file is fixing.
    echo "linearize-campaign: stage $name: FAILED (exit $rc) after $((end-start))s" >&2
    rows+=("{\"stage\":\"$name\",\"result\":\"failed\",\"exit\":$rc,\"duration_s\":$((end-start))}")
    status=1
  fi
}

export GOWORK=off

# EVERY stage's selection is checked before anything runs. `go test -run
# '^NoSuchTest$'` exits 0 printing ok, `-fuzz` with a pattern that matches
# nothing does the same, and `go test` over a package with no test files prints
# `?  pkg [no test files]` and exits 0 -- so a rename or a drifted package path
# turns a stage into a check that cannot fail, silently and in the direction
# that looks like a pass. the contributor notes's A29 lesson, applied to a `-run`
# filter and to a bare package.
#
# The PREFIX matters as well as the count. `go test -list` matches its pattern
# against tests and fuzz targets alike, so `-fuzz '^TestSomething$'` selects a
# name and then fuzzes nothing; a stage therefore has to select a function of
# its own kind.
#
# awk rather than `grep -c` because grep exits 1 on a count of zero and this
# must not need an or-true.
selects() { # pattern package prefix -- number of matching functions of that kind
  go test -list "$1" "$2" | awk -v p="^$3" '$0 ~ p {c++} END {print c+0}'
}
refuse() { # kind pattern package
  echo "linearize-campaign: $1 pattern $2 selects no $1 function in $3;" \
       "a stage over it would exit 0 having run nothing" >&2
  exit 2
}
# Stage 1 first, and it is the one that was missed: it exists to stop stages 2
# and 3 being vacuous, and it was the one stage that could itself go green
# having compiled nothing under test.
n="$(selects '.' "$checker_pkg" Test)"
if [ "$n" -eq 0 ]; then refuse Test '(every test)' "$checker_pkg"; fi
echo "linearize-campaign: checker package $checker_pkg has $n Test function(s)"
for spec in "Test:$lin_test" "Fuzz:$lin_fuzz"; do
  prefix="${spec%%:*}"
  pat="${spec#*:}"
  n="$(selects "$pat" "$engine_pkg" "$prefix")"
  if [ "$n" -eq 0 ]; then refuse "$prefix" "$pat" "$engine_pkg"; fi
  echo "linearize-campaign: pattern $pat selects $n $prefix function(s) in $engine_pkg"
done

# 1. The checker, under -race. It is stdlib-only and fast.
stage checker go test -race -count=1 "$checker_pkg"

# 2. The engine's recorded histories, count times. §8.5's linearization points
#    are what the histories are checked against, and the interesting failures
#    are interleavings, so one pass proves much less than twenty.
stage engine go test -race -count="$count" -run "$lin_test" "$engine_pkg"

# 3. The fuzzer's own shapes, for the campaign's whole budget.
stage fuzz go test -race -run='^$' -fuzz="$lin_fuzz" -fuzztime="$time" -parallel "$parallel" "$engine_pkg"

verdict=failed
if [ "$status" -eq 0 ]; then verdict=ok; fi
{
  printf '{\n  "generated": "%s",\n  "fuzztime": "%s",\n  "count": %s,\n' "$stamp" "$time" "$count"
  printf '  "result": "%s",\n  "stages": [\n' "$verdict"
  first=1
  for row in "${rows[@]}"; do
    if [ "$first" -eq 0 ]; then printf ',\n'; fi
    printf '    %s' "$row"
    first=0
  done
  printf '\n  ]\n}\n'
} > "$report"

echo "linearize-campaign: $verdict, report in $report"
exit $status
