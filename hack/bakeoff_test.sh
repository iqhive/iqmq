#
#   hack/bakeoff_test.sh
#
# Nothing here measures anything. The cases that need a particular measurement
# outcome -- three contended attempts in a row, a guard that refused, a run
# that failed, a guard that printed no summary -- use a stub guard through
# hack/bakeoff.sh's BENCHGUARD hook, because the real outcomes cannot be
# arranged on demand: the host would have to be made busy at exactly the right
# moment. The stub emits the same summary line and the same benchmark rows the
# real one would, and returns the status the case is about.
# The throwaway families live in this script's temp directory and are passed
# through BAKEOFF_ENV, never written into hack/bakeoff/, where the campaign
# would pick them up.
set -uo pipefail
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# The host CPU ceiling. This script invokes `go` itself, so it is capped when
# run by hand and not only when a make recipe exports the cap into it.
# hack/cpucap.sh carries the arithmetic; the eval is a
# no-op beyond re-deriving the same GOMAXPROCS and -p when make already set
# them, and it never RAISES either.
eval "$("$root/hack/cpucap.sh" env)"
bakeoff="${BAKEOFF:-$root/hack/bakeoff.sh}"
tmp="$(mktemp -d)"
# Every throwaway family is named HT*, which no real family is (they are
# A<n>/B<n>), so the run directories hack/bakeoff.sh writes under the
# repository's own reports tree can be removed by name -- including those of a
# revision that does not announce its run directory at all, which is one of
# the defects under test.
cleanup() {
  rm -rf "$root"/benchmarks/reports/results/*-HT* \
         "$root"/benchmarks/reports/scorecards/HT*.json \
         "$root"/docs/benchmarks/variants/HT*.md
  rmdir "$root/benchmarks/reports/results" "$root/benchmarks/reports/scorecards" 2>/dev/null
  rm -rf "$tmp"
}
trap cleanup EXIT
export IQMQ_BENCH_LOCK="$tmp/lock"

pass=0; fail=0
skipped=0
ok()  { printf 'ok   %s\n' "$1"; pass=$((pass+1)); }
bad() { printf 'FAIL %s\n     %s\n' "$1" "$2"; fail=$((fail+1)); }
# skip is for a check this tree cannot answer -- not one that passed. It is
# counted separately and printed with its reason, so a run that skipped
# something says so in its own summary line rather than reading as 24/24.
skip() { printf 'SKIP %s\n     %s\n' "$1" "$2"; skipped=$((skipped+1)); }

# ---------------------------------------------------------------------------
# The stub guard. Reads one verdict per invocation from $STUB_PLAN (clean,
# contended, refused, fail:<rc>, nosummary; `clean` once the plan runs out),
# logs the command line it was given to $STUB_LOG, and emits the benchmark
# rows a real run of that filter would produce so that raw.txt fills.
cat > "$tmp/stubguard.sh" <<'STUB'
#!/usr/bin/env bash
set -uo pipefail
plan="${STUB_PLAN:?}"; log="${STUB_LOG:?}"; nfile="${STUB_N:?}"
n=$(( $(cat "$nfile" 2>/dev/null || echo 0) + 1 )); echo "$n" > "$nfile"
verdict="$(sed -n "${n}p" "$plan")"; [ -n "$verdict" ] || verdict=clean
report=""; bench=""
args=("$@")
for i in "${!args[@]}"; do
  case "${args[$i]}" in
    -o) report="${args[$((i+1))]}" ;;
    -bench=*) bench="${args[$i]#-bench=}" ;;
  esac
done
printf 'invocation=%s verdict=%s matrix=%s bench=%s args=%s\n' \
  "$n" "$verdict" "${IQMQ_BENCH_MATRIX:-<unset>}" "$bench" "$*" >> "$log"
kind="${bench##*/}"; kind="${kind#^}"; kind="${kind%$}"
emit_rows() {
  [ -n "$kind" ] || return 0
  local b
  for b in insert fire; do
    printf 'BenchmarkMemory/Scheduler/%s/%s/n=1000000-1\t1000\t200.0 ns/op\t0 B/op\t0 allocs/op\n' "$kind" "$b"
  done
}
summary() {
  printf 'benchguard: verdict=%s reason=%s load_before=1.00 load_after=1.00 load_max=1.00 load_ceiling=9.00 running_before=1 foreign_runnable_before=0 foreign_runnable_max=0 runnable_ceiling=6 foreign=0 foreign_pids=[] loadmax=1 own_load=8 duration_s=0 status=%s\n' "$1" "$2" "$3"
}
case "$verdict" in
  clean)     emit_rows; summary clean clean 0; exit 0 ;;
  nosummary) emit_rows; exit 0 ;;
  contended) summary contended foreign 0
             [ -n "$report" ] && printf '{"verdict":"contended","reason":"foreign"}\n' > "$report"
             exit 3 ;;
  refused)   echo "benchguard: host still busy after 0s (load1=99 foreign=[4242]); refused to measure" >&2; exit 5 ;;
  fail:*)    summary clean clean "${verdict#fail:}"
             seq 1 100 | sed 's/^/junk line /'
             exit "${verdict#fail:}" ;;
esac
STUB
chmod +x "$tmp/stubguard.sh"

# A throwaway family. $1 is the file to write, the rest are lines to append.
mkenv() {
  local f="$1"; shift
  printf '%s\n' "$@" > "$f"
}

# Run hack/bakeoff.sh with its output captured, and report its status.
run_bakeoff() {
  local outfile="$1"; shift
  "$bakeoff" "$@" > "$outfile" 2>&1
}
run_dir_of() { sed -n 's/^== [A-Za-z0-9]*: run -> //p' "$1" | tail -n1; }

b10env=(
  'PKG=./internal/timer'
  'BENCH=BenchmarkMemory/Scheduler'
  'REF=heap'
  'AUTO_KEY=t_b10'
  'AUTO_FILE=-'
)

# ---------------------------------------------------------------------------
# 1. A family that declares no MATRIX is refused, and the message says what to
#    declare. Silently defaulting is what measured all thirty-eight families
#    on the smoke grid.
mkenv "$tmp/HT1.env" "${b10env[@]}"
out="$(BAKEOFF_ENV="$tmp/HT1.env" "$bakeoff" HT1 probe 2>&1)"; rc=$?
if [ "$rc" -ne 2 ]; then
  bad "an undeclared MATRIX is refused, not defaulted" "expected exit 2, got $rc: $out"
elif ! printf '%s' "$out" | grep -q 'declares no MATRIX'; then
  bad "an undeclared MATRIX is refused, not defaulted" "the message does not name the omission: $out"
elif ! printf '%s' "$out" | grep -q 'MATRIX=full' || ! printf '%s' "$out" | grep -q 'MATRIX=default'; then
  bad "an undeclared MATRIX is refused, not defaulted" "the message does not say what to declare: $out"
else
  ok "an undeclared MATRIX is refused, not defaulted"
fi

# 2. And a value the Go side does not recognise is refused too: the benchmark
#    files compare IQMQ_BENCH_MATRIX with the literal "full", so MATRIX=Full
#    would measure the smoke grid and say nothing about it.
mkenv "$tmp/HT2.env" "${b10env[@]}" 'MATRIX=Full'
out="$(BAKEOFF_ENV="$tmp/HT2.env" "$bakeoff" HT2 probe 2>&1)"; rc=$?
if [ "$rc" -ne 2 ]; then
  bad "an unrecognised MATRIX is refused" "expected exit 2, got $rc: $out"
elif ! printf '%s' "$out" | grep -q "MATRIX='Full' is not one of"; then
  bad "an unrecognised MATRIX is refused" "the message does not name the value: $out"
else
  ok "an unrecognised MATRIX is refused"
fi

# 3. The grid reaches the family's Go code. internal/mwchain's benchLengths()
#    sweeps 4 chain lengths by default and 6 at MATRIX=full, so the filter
#    probe's own count is the evidence that IQMQ_BENCH_MATRIX was exported --
#    nothing in any executable line of the harness used to set it at all.
mwenv=('PKG=./internal/mwchain' 'BENCH=BenchmarkMiddleware' 'REF=closure' 'AUTO_KEY=t_mw' 'AUTO_FILE=-')
mkenv "$tmp/HT3D.env" "${mwenv[@]}" 'MATRIX=default'
mkenv "$tmp/HT3F.env" "${mwenv[@]}" 'MATRIX=full'
BAKEOFF_ENV="$tmp/HT3D.env" run_bakeoff "$tmp/t3d.log" HT3D probe >/dev/null 2>&1; rc3d=$?
BAKEOFF_ENV="$tmp/HT3F.env" run_bakeoff "$tmp/t3f.log" HT3F probe >/dev/null 2>&1; rc3f=$?
nd="$(sed -n 's/^   closure: \([0-9]*\) benchmark(s).*/\1/p' "$tmp/t3d.log")"
nf="$(sed -n 's/^   closure: \([0-9]*\) benchmark(s).*/\1/p' "$tmp/t3f.log")"
if [ "$rc3d" -ne 0 ] || [ "$rc3f" -ne 0 ]; then
  bad "the dimension grid reaches the family's benchmarks" "probe failed: default rc=$rc3d full rc=$rc3f; $(tail -5 "$tmp/t3d.log") / $(tail -5 "$tmp/t3f.log")"
elif [ -z "$nd" ] || [ -z "$nf" ]; then
  bad "the dimension grid reaches the family's benchmarks" "no filter count in the logs: $(cat "$tmp/t3d.log")"
elif [ "$nf" -le "$nd" ]; then
  bad "the dimension grid reaches the family's benchmarks" \
    "MATRIX=full selected $nf benchmark(s) and MATRIX=default $nd; the sweep never widened, so IQMQ_BENCH_MATRIX did not reach the benchmark"
elif ! grep -q 'matrix=default (IQMQ_BENCH_MATRIX, from the family)' "$tmp/t3d.log"; then
  bad "the dimension grid reaches the family's benchmarks" "the grid is not announced: $(cat "$tmp/t3d.log")"
else
  ok "the dimension grid reaches the family's benchmarks (default=$nd, full=$nf)"
fi

# 4. The per-kind filter is anchored at the slug. Family A4's `heap` is a
#    PREFIX of its sibling `heap4`; `-bench "^R/heap(/|$)"` matched the slug
#    element UNANCHORED and selected all of heap4's rows too, so two kinds
#    contributed the same rows and the family's decision was taken over data
#    belonging to one arm.
#
mkenv "$tmp/HT4.env" "${b10env[@]}" 'MATRIX=default'
BAKEOFF_ENV="$tmp/HT4.env" run_bakeoff "$tmp/t4.log" HT4 probe; rc=$?
na="$(sed -n 's/^   heap: \([0-9]*\) benchmark(s).*/\1/p' "$tmp/t4.log")"
np="$(sed -n 's/^   heap4: \([0-9]*\) benchmark(s).*/\1/p' "$tmp/t4.log")"
pat="$(sed -n "s/^   heap: .*\[-bench '\(.*\)'\]/\1/p" "$tmp/t4.log")"
if [ "$rc" -ne 0 ]; then
  bad "a kind's filter never selects its sibling's rows" "probe failed ($rc): $(tail -5 "$tmp/t4.log")"
elif [ -z "$na" ] || [ -z "$np" ]; then
  bad "a kind's filter never selects its sibling's rows" "no filter counts: $(cat "$tmp/t4.log")"
elif [ "$na" -ne "$np" ]; then
  bad "a kind's filter never selects its sibling's rows" \
    "heap selected $na benchmark(s) and heap4 $np; the substring kind is picking up its sibling's rows [-bench '$pat']"
elif [ "$pat" != '^BenchmarkMemory$/^Scheduler$/^heap$' ]; then
  bad "a kind's filter never selects its sibling's rows" "the filter is not anchored element by element: '$pat'"
else
  ok "a kind's filter never selects its sibling's rows ($na each)"
fi

# The remaining cases drive the measurement loop through the stub guard.
#
# These cases measure nothing -- `go` and the guard are both stubs -- so nothing
# here may depend on how busy the host is, and `make ci` runs it. It used to need
# IQMQ_CPUCAP_IDLE_MAX=1 to switch off an idleness requirement that
# `hack/cpucap.sh idle-cpus` put on a real measurement; six cases had failed
# exactly that way while this agent's own `make lint` saturated the cores the
# selector sampled. `idle-cpus` is gone -- the bake-off no
# longer chooses CPUs at all -- so there is no requirement left to switch off.
stub_env() {
  STUB_PLAN="$tmp/plan" STUB_LOG="$tmp/stublog" STUB_N="$tmp/n" \
  BENCHGUARD="$tmp/stubguard.sh" BAKEOFF_ENV="$1" IQMQ_BENCH_CPUS=1 \
  IQMQ_BENCH_MAXWAIT=5 "${@:2}"
}
mkenv "$tmp/HTM.env" "${b10env[@]}" 'MATRIX=default'
reset_stub() { : > "$tmp/stublog"; echo 0 > "$tmp/n"; printf '%s\n' "$@" > "$tmp/plan"; }

# 5. Three contended attempts and the round contributes nothing. A round
#    measures every kind of the family in one invocation, so a loss is a
#    round's rather than a kind's.
#    It used to `break` in silence: the log said "contended; repeating" and
#    then nothing, `contended_runs` counted attempts rather than losses,
#    nothing recorded WHICH kind lost, and the family exited zero with data
#    the decision treated as complete.
#    Plan: the pre-run probe is clean, then the first pair is contended three
#    times over.
reset_stub clean contended contended contended
stub_env "$tmp/HTM.env" run_bakeoff "$tmp/t5.log" HTM quick; rc=$?
d="$(run_dir_of "$tmp/t5.log")"
c="$( [ -n "$d" ] && cat "$d/contention.txt" 2>/dev/null )"
if [ "$rc" -ne 6 ]; then
  bad "a lost sample fails the family and names its round" "expected exit 6, got $rc: $(tail -20 "$tmp/t5.log")"
elif ! grep -q "LOST SAMPLE -- round 1 of 3 (every kind)" "$tmp/t5.log"; then
  bad "a lost sample fails the family and names its round" "the loss is not attributed to a round: $(tail -20 "$tmp/t5.log")"
elif ! printf '%s' "$c" | grep -qx 'lost_samples=1'; then
  bad "a lost sample fails the family and names its round" "contention.txt does not count the loss: $c"
elif ! printf '%s' "$c" | grep -qx 'lost_rounds=1'; then
  bad "a lost sample fails the family and names its round" "contention.txt does not attribute the loss: $c"
elif ! printf '%s' "$c" | grep -qx 'contended_runs=3'; then
  bad "a lost sample fails the family and names its round" "the three attempts are not counted: $c"
elif ! grep -q 'rounds lost to contention' "$tmp/t5.log"; then
  bad "a lost sample fails the family and names its round" "no summary of the loss: $(tail -20 "$tmp/t5.log")"
elif ! grep -q 'IQMQ_BENCH_ALLOW_LOST=1' "$tmp/t5.log"; then
  bad "a lost sample fails the family and names its round" "the override is not named: $(tail -20 "$tmp/t5.log")"
elif grep -q ': deciding' "$tmp/t5.log"; then
  bad "a lost sample fails the family and names its round" "it decided anyway: $(tail -20 "$tmp/t5.log")"
else
  ok "a lost sample fails the family and names its round"
fi

# 6. A guard that REFUSED to measure is not a contended sample. Both used to
#    be exit 3, so the harness retried a refusal three times -- three more
#    maxwait waits for the same answer -- and then dropped the pair as though
#    it had measured and lost it.
reset_stub clean refused
stub_env "$tmp/HTM.env" run_bakeoff "$tmp/t6.log" HTM quick; rc=$?
d="$(run_dir_of "$tmp/t6.log")"
c="$( [ -n "$d" ] && cat "$d/contention.txt" 2>/dev/null )"
if [ "$rc" -ne 4 ]; then
  bad "a refusal to measure is not a contended sample" "expected exit 4, got $rc: $(tail -20 "$tmp/t6.log")"
elif ! grep -q 'the host never went quiet' "$tmp/t6.log"; then
  bad "a refusal to measure is not a contended sample" "the refusal is not reported: $(tail -20 "$tmp/t6.log")"
elif ! printf '%s' "$c" | grep -qx 'refused_runs=1'; then
  bad "a refusal to measure is not a contended sample" "contention.txt does not count it: $c"
elif ! printf '%s' "$c" | grep -qx 'contended_runs=0'; then
  bad "a refusal to measure is not a contended sample" "it was counted as contention: $c"
elif [ "$(cat "$tmp/n")" -ne 2 ]; then
  bad "a refusal to measure is not a contended sample" "the refusal was retried: $(cat "$tmp/n") guard invocations, want 2"
else
  ok "a refusal to measure is not a contended sample"
fi

# 7. A guarded run that printed no summary line does not end the script in
#    silence. `grep '^benchguard:' "$tmp" | sed …` failed the pipeline whenever
#    there was no summary, and under `set -e` with `pipefail` that ended the
#    script with no diagnostic at all. The plan ends with a
#    failing run so the family stops before `bakeoff decide`, and that failure
#    also checks the failed-run log: `sed … >&2 | tail -40 >&2` sent the whole
#    log to stderr and left tail reading nothing, so the truncation was dead.
reset_stub clean nosummary fail:2
stub_env "$tmp/HTM.env" run_bakeoff "$tmp/t7.log" HTM quick; rc=$?
d="$(run_dir_of "$tmp/t7.log")"
junk="$(grep -c '^     junk line ' "$tmp/t7.log" || true)"
if [ "$rc" -ne 1 ]; then
  bad "a run with no summary line is reported, not swallowed" "expected exit 1, got $rc: $(tail -20 "$tmp/t7.log")"
elif ! grep -q 'printed no summary line' "$tmp/t7.log"; then
  bad "a run with no summary line is reported, not swallowed" "the missing summary is not reported: $(cat "$tmp/t7.log")"
elif ! ls "$d"/failed-*.log >/dev/null 2>&1; then
  bad "a run with no summary line is reported, not swallowed" "the failed run's log was not kept in $d"
elif [ "$junk" -ne 40 ]; then
  bad "a run with no summary line is reported, not swallowed" \
    "the failed run's log was truncated to 40 lines on stderr, got $junk of 100"
else
  ok "a run with no summary line is reported, not swallowed"
fi

# 8. The measurement's -timeout is passed and recorded. `go test`'s default is
#    ten minutes, which families A2 and A7 exceed at 112-144 benchmarks x 4
#    GOMAXPROCS rungs x 1 s, and nothing ever raised it.
reset_stub clean clean clean clean clean clean clean clean clean clean clean clean clean fail:2
stub_env "$tmp/HTM.env" run_bakeoff "$tmp/t8.log" HTM quick >/dev/null 2>&1
d="$(run_dir_of "$tmp/t8.log")"
m="$( [ -n "$d" ] && cat "$d/method.json" 2>/dev/null )"
if ! printf '%s' "$m" | grep -q '"timeout_s":[0-9]'; then
  bad "the measurement is given a timeout and records it" "method.json has no timeout: $m"
elif ! printf '%s' "$m" | grep -q '"matrix":"default"'; then
  bad "the measurement is given a timeout and records it" "method.json does not record the grid: $m"
elif ! grep -q -- '-timeout 1200s' "$tmp/stublog"; then
  bad "the measurement is given a timeout and records it" "no -timeout reached go test: $(head -3 "$tmp/stublog")"
elif ! grep -q 'matrix=default' "$tmp/stublog"; then
  bad "the measurement is given a timeout and records it" "IQMQ_BENCH_MATRIX did not reach the measurement: $(head -3 "$tmp/stublog")"
else
  ok "the measurement is given a timeout and records it"
fi

# 9. A resume continues from the pairs already in raw.txt rather than
#    remeasuring a family that takes hours. The run above
#    stopped at its thirteenth guard invocation, so twelve pairs are done...
#    except the last, which failed. Resume it and the completed pairs are
#    skipped and their rows are not duplicated.
before_rows="$( [ -n "$d" ] && grep -c '^Benchmark' "$d/raw.txt" 2>/dev/null || echo 0 )"
before_pairs="$( [ -n "$d" ] && wc -l < "$d/progress.tsv" 2>/dev/null || echo 0 )"
reset_stub clean
RESUME="$(basename "$d")" stub_env "$tmp/HTM.env" run_bakeoff "$tmp/t9.log" HTM quick >/dev/null 2>&1
after_rows="$(grep -c '^Benchmark' "$d/raw.txt" 2>/dev/null || echo 0)"
if [ "$before_pairs" -eq 0 ]; then
  bad "a resumed family keeps what it measured" "the interrupted run recorded no progress"
elif ! grep -q "$before_pairs (count, kind) pair(s) already measured" "$tmp/t9.log"; then
  bad "a resumed family keeps what it measured" "it did not skip the $before_pairs completed pair(s): $(head -8 "$tmp/t9.log")"
elif [ "$after_rows" -lt "$before_rows" ]; then
  bad "a resumed family keeps what it measured" "raw.txt shrank from $before_rows to $after_rows rows"
elif [ "$(sort "$d/progress.tsv" | uniq -d | wc -l)" -ne 0 ]; then
  bad "a resumed family keeps what it measured" "a (count, kind) pair was measured twice: $(sort "$d/progress.tsv" | uniq -d)"
else
  ok "a resumed family keeps what it measured ($before_pairs pair(s) skipped)"
fi

# 10. And a resume onto a run measured on another grid is refused, or one
#     raw.txt would hold two different benchmark sets.
mkenv "$tmp/HTMFULL.env" "${b10env[@]}" 'MATRIX=full'
reset_stub clean
out="$(RESUME="$(basename "$d")" stub_env "$tmp/HTMFULL.env" "$bakeoff" HTM quick 2>&1)"; rc=$?
if [ "$rc" -ne 2 ]; then
  bad "a resume onto a different method is refused" "expected exit 2, got $rc: $out"
elif ! printf '%s' "$out" | grep -q 'measured under a different method'; then
  bad "a resume onto a different method is refused" "no diagnostic: $out"
else
  ok "a resume onto a different method is refused"
fi

# 11. A correctness gate that fails with no kind test to blame says so. The
#     extraction that finds the disqualified kinds is a pipeline, and a grep
#     that matches nothing fails it: under `set -e` with `pipefail` the script
#     died THERE, before the message written for exactly this case.
mkenv "$tmp/HT11.env" 'PKG=./internal/nosuchpackage' 'BENCH=BenchmarkMemory/Scheduler' \
  'REF=x' 'AUTO_KEY=t_11' 'AUTO_FILE=-' 'MATRIX=default'
BAKEOFF_ENV="$tmp/HT11.env" run_bakeoff "$tmp/t11.log" HT11 probe; rc=$?
if [ "$rc" -eq 0 ]; then
  bad "a gate failure with no kind to blame is diagnosed" "the family passed its gate: $(cat "$tmp/t11.log")"
elif ! grep -q 'no kind test is responsible' "$tmp/t11.log"; then
  bad "a gate failure with no kind to blame is diagnosed" "the script ended without the diagnostic: $(cat "$tmp/t11.log")"
else
  ok "a gate failure with no kind to blame is diagnosed"
fi

# 12. And a kind list that produces no kinds is named rather than inferred.
#     The same pipeline shape hid `go test`'s own status: a build failure under
#     EXTRA_ARGS was reported as "printed no kinds", and `head -n1` on a pipe
#     SIGPIPEs its producer, which pipefail turns into exit 141.
mkenv "$tmp/HT12.env" "${b10env[@]}" 'MATRIX=default' 'KINDS_TEST=TestNoSuchListTest'
BAKEOFF_ENV="$tmp/HT12.env" run_bakeoff "$tmp/t12.log" HT12 probe; rc=$?
if [ "$rc" -eq 0 ]; then
  bad "an empty kind list is named" "the family found kinds it does not have: $(cat "$tmp/t12.log")"
elif ! grep -q "TestNoSuchListTest printed no 'kinds:' line" "$tmp/t12.log"; then
  bad "an empty kind list is named" "no diagnostic naming the list test: $(cat "$tmp/t12.log")"
else
  ok "an empty kind list is named"
fi

mkenv "$tmp/HT13.env" 'MODDIR=benchmarks' 'PKG=./redisclient' \
  'BENCH=BenchmarkRedisClient' 'REF=rueidis' 'AUTO_KEY=t_13' 'AUTO_FILE=-' 'MATRIX=default'
BAKEOFF_ENV="$tmp/HT13.env" run_bakeoff "$tmp/t13.log" HT13 probe; rc=$?
if [ "$rc" -ne 0 ]; then
  bad "a family in a nested module is measured there" "probe exited $rc: $(tail -n 20 "$tmp/t13.log")"
elif ! grep -q '^   rueidis: [1-9]' "$tmp/t13.log" || ! grep -q '^   goredis: [1-9]' "$tmp/t13.log"; then
  bad "a family in a nested module is measured there" "the filter probe found no rows per kind: $(cat "$tmp/t13.log")"
else
  ok "a family in a nested module is measured there"
fi

# 14. And a MODDIR that is not a module is refused before anything runs, with
#     the omission named. The failure to avoid is the silent one: `go test -C`
#     into a directory with no go.mod resolves against whatever module
#     encloses it, so the family would be measured somewhere nobody declared.
mkenv "$tmp/HT14.env" 'MODDIR=hack' 'PKG=./bakeoff' 'BENCH=BenchmarkNothing' \
  'REF=x' 'AUTO_KEY=t_14' 'AUTO_FILE=-' 'MATRIX=default'
out="$(BAKEOFF_ENV="$tmp/HT14.env" "$bakeoff" HT14 probe 2>&1)"; rc=$?
if [ "$rc" -ne 2 ]; then
  bad "a MODDIR that is not a module is refused" "expected exit 2, got $rc: $out"
elif ! printf '%s' "$out" | grep -q 'is not a module'; then
  bad "a MODDIR that is not a module is refused" "the message does not name the omission: $out"
else
  ok "a MODDIR that is not a module is refused"
fi

# 15. A MODDIR that escapes the repository is refused too.
mkenv "$tmp/HT15.env" 'MODDIR=../elsewhere' 'PKG=./x' 'BENCH=BenchmarkNothing' \
  'REF=x' 'AUTO_KEY=t_15' 'AUTO_FILE=-' 'MATRIX=default'
out="$(BAKEOFF_ENV="$tmp/HT15.env" "$bakeoff" HT15 probe 2>&1)"; rc=$?
if [ "$rc" -ne 2 ]; then
  bad "a MODDIR outside the repository is refused" "expected exit 2, got $rc: $out"
elif ! printf '%s' "$out" | grep -q 'inside the repository'; then
  bad "a MODDIR outside the repository is refused" "the message does not say why: $out"
else
  ok "a MODDIR outside the repository is refused"
fi

#
# B07 pins the POSITIVE path (a family in a nested module is measured there)
# and two of the refusals. The cases below are the ones that adds to: the
# NEGATIVE of the positive -- a nested package with no MODDIR at all -- an
# explicit `MODDIR=.`, and the other two spellings of a path that leaves the
# repository. Each of them would otherwise surface as a `go test` pattern
# error that sends a reader looking in the wrong place.

# 16. The negative of B07's case 13: a nested module's package is unreachable
#     WITHOUT a MODDIR, and the diagnostic has to name the package rather than
#     leave a bare `go test` pattern error.
mkenv "$tmp/HT16.env" 'PKG=./drivers/kafka/kafkago' 'BENCH=BenchmarkKafkaClient' \
  'REF=franz' 'AUTO_KEY=t_ht16' 'AUTO_FILE=-' 'MATRIX=default' \
  'KINDS_TEST=TestKafkaClientKindsList'
BAKEOFF_ENV="$tmp/HT16.env" run_bakeoff "$tmp/t16.log" HT16 probe; rc=$?
if [ "$rc" -eq 0 ]; then
  bad "a nested module's package is unreachable without MODDIR" "the family passed: $(cat "$tmp/t16.log")"
elif ! grep -q 'drivers/kafka/kafkago' "$tmp/t16.log"; then
  bad "a nested module's package is unreachable without MODDIR" "no diagnostic naming the package: $(cat "$tmp/t16.log")"
else
  ok "a nested module's package is unreachable without MODDIR"
fi

# 17. A MODDIR with no go.mod, in the one spelling B07's case 14 does not
#     use: a directory of the ROOT module rather than of no module at all.
#     PKG would be resolved twice over and the error would be a pattern error.
mkenv "$tmp/HT17.env" "${b10env[@]}" 'MATRIX=default' 'MODDIR=internal'
BAKEOFF_ENV="$tmp/HT17.env" run_bakeoff "$tmp/t17.log" HT17 probe; rc=$?
if [ "$rc" -ne 2 ]; then
  bad "a MODDIR with no go.mod is refused" "expected exit 2, got $rc: $(cat "$tmp/t17.log")"
elif ! grep -q 'is not a module' "$tmp/t17.log"; then
  bad "a MODDIR with no go.mod is refused" "the message does not say why: $(cat "$tmp/t17.log")"
else
  ok "a MODDIR with no go.mod is refused"
fi

# 18. And the other two spellings of a MODDIR that leaves the repository:
#     bare `..` and an absolute path. B07's case 15 covers `../elsewhere`.
for bad_module in '..' '/etc'; do
  mkenv "$tmp/HT18.env" "${b10env[@]}" 'MATRIX=default' "MODDIR=$bad_module"
  BAKEOFF_ENV="$tmp/HT18.env" run_bakeoff "$tmp/t18.log" HT18 probe; rc=$?
  if [ "$rc" -ne 2 ]; then
    bad "MODDIR='$bad_module' is refused" "expected exit 2, got $rc: $(cat "$tmp/t18.log")"
  elif ! grep -q 'inside the repository' "$tmp/t18.log"; then
    bad "MODDIR='$bad_module' is refused" "no diagnostic: $(cat "$tmp/t18.log")"
  else
    ok "MODDIR='$bad_module' is refused"
  fi
done

# 19. An explicitly declared MODDIR=. is ACCEPTED and is the repository root.
#     What this pins is the acceptance, not the normalisation: removing the
#     `.|./` case leaves the behaviour identical, because `go test -C $root/.`
#     is `go test -C $root`, so a case written against that line could not
#     fail. It fails against a refusal -- tightening the path validation to
#     reject `.` along with `..` and an absolute path is the plausible way to
#     break it, and then this case reports "MODDIR='.' must be a path inside
#     the repository". A family that spells the root out must stay measurable.
mkenv "$tmp/HT19.env" "${b10env[@]}" 'MATRIX=default' 'MODDIR=.'
BAKEOFF_ENV="$tmp/HT19.env" run_bakeoff "$tmp/t19.log" HT19 probe; rc=$?
if [ "$rc" -ne 0 ]; then
  bad "MODDIR=. is the repository root" "the family failed its probe: $(cat "$tmp/t19.log")"
elif ! grep -q 'probe ok' "$tmp/t19.log"; then
  bad "MODDIR=. is the repository root" "the probe did not finish: $(cat "$tmp/t19.log")"
else
  ok "MODDIR=. is the repository root"
fi

# ---------------------------------------------------------------------------
# The scorecard's declaration fields, stored with each measurement,
# as well as `hack/bakeoff.sh` -- and a scorecard is REPLACED rather than
# merged, so a flag either script forgets is a field the stored evidence loses.
#
# Three of these had already been lost by the time this check was written, and
# all three were silent. A whole-tree re-decide dropped `adr` and `title` from
# forty-six of the forty-eight scorecards, because the re-decide passed neither
# flag -- which also degraded twenty-seven generated `auto.go` headers from
# "(B9, ADR-006)" to "(B9, pending)". And the re-decide ignored MODDIR, so the
# two families whose PKG is `.` inside a nested module (A11, A34) recorded
# `"package": ".."` over drivers/kafka/kafkago and
# drivers/gcppubsub/v1compat, at exit 0, with an empty kind list behind it.
#
# It is a data check over committed files: no measurement, and it is what says
# the two writers of a scorecard agree with the one declaration of a family.
scorecard_problems() { # SCORECARD_DIR
  python3 - "$root" "${1:?scorecard dir}" <<'PY'
import json, os, re, sys, shlex
root, scdir = sys.argv[1], sys.argv[2]
envdir = os.path.join(root, "hack", "bakeoff")
for name in sorted(os.listdir(envdir)):
    if not name.endswith(".env"):
        continue
    fam = name[:-4]
    path = os.path.join(scdir, fam + ".json")
    if not os.path.exists(path):
        continue
    decl = {}
    for line in open(os.path.join(envdir, name)):
        line = line.strip()
        m = re.match(r'^([A-Z_]+)=(.*)$', line)
        if not m:
            continue
        try:
            val = " ".join(shlex.split(m.group(2)))
        except ValueError:
            val = m.group(2)
        decl[m.group(1)] = val
    sc = json.load(open(path))
    moddir = decl.get("MODDIR", "").rstrip("/")
    if moddir in (".", "./"):
        moddir = ""
    pkg = decl.get("PKG", "")
    pkg_rel = pkg[2:] if pkg.startswith("./") else pkg
    # PKG=. inside a MODDIR is the module's own root package, so the joined
    # path is the module directory and not "<moddir>/." -- which is what the
    # first draft of this check compared against, and it reported A11 and
    # A34 as wrong when they were right.
    if pkg_rel in (".", ""):
        want_pkg = moddir
    elif moddir:
        want_pkg = moddir + "/" + pkg_rel
    else:
        want_pkg = pkg_rel
    checks = [("family", fam), ("package", want_pkg), ("bench_root", decl.get("BENCH", "")),
              ("reference", decl.get("REF", "")), ("auto_key", decl.get("AUTO_KEY", ""))]
    if decl.get("ADR"):
        checks.append(("adr", int(decl["ADR"])))
    if decl.get("TITLE"):
        checks.append(("title", decl["TITLE"]))
    for field, want in checks:
        got = sc.get(field)
        if got != want:
            print("%s: scorecard %s=%r, hack/bakeoff/%s declares %r" % (fam, field, got, name, want))
PY
}

# 20. Every committed scorecard agrees with its family's declaration.
probs="$(scorecard_problems "$root/benchmarks/reports/scorecards")"
if [ -n "$probs" ]; then
  bad "every stored scorecard agrees with its family's declaration" "$(printf '%s' "$probs" | head -n 8)"
else
  ok "every stored scorecard agrees with its family's declaration"
fi

# 21. And the check itself fails on the three fields that were actually lost.
#     Without this the case above is a check nobody has watched fail, which
#     this repository assumes does not work: the six
#     comparisons are one dictionary lookup each and a typo in a field name
#     compares None with None for every family.
# The scorecards were not brought over from the Bitbucket repository, because
# they held internal, project-specific information, so on a fresh clone there
# is nothing to doctor and the case says so instead of failing.
if [ ! -f "$root/benchmarks/reports/scorecards/A11.json" ]; then
  skip "a scorecard that lost adr, title or package is reported" "benchmarks/reports/scorecards/A11.json is not in this repository; commit a scorecard for the case to run"
else
mkdir -p "$tmp/sc-doctored"
cp "$root/benchmarks/reports/scorecards/A11.json" "$tmp/sc-doctored/A11.json"
python3 - "$tmp/sc-doctored/A11.json" <<'PY'
import json, sys
p = sys.argv[1]
d = json.load(open(p))
d.pop("adr", None)          # what a re-decide with no -adr writes
d.pop("title", None)        # what a re-decide with no -title writes
d["package"] = ".."         # what a re-decide that ignores MODDIR writes
json.dump(d, open(p, "w"), indent=2)
PY
probs="$(scorecard_problems "$tmp/sc-doctored")"
missing=""
for field in adr title package; do
  printf '%s' "$probs" | grep -q "A11: scorecard $field=" || missing="$missing $field"
done
if [ -n "$missing" ]; then
  bad "a scorecard that lost adr, title or package is reported" "not reported:$missing (got: $(printf '%s' "$probs" | tr '\n' '; '))"
else
  ok "a scorecard that lost adr, title or package is reported"
fi
fi

#
# A removed family declares REMOVED=<commit> in its env file, and the commit
# has to be a real ancestor of HEAD: "the commit hash of the last tree
# containing a removed kind" is the one thing that makes the removal
# recoverable, so a typo in it is the whole record.
disposition_problems() { # SCORECARD_DIR
  python3 - "$root" "${1:?scorecard dir}" <<'PY'
import json, os, re, sys
root, scdir = sys.argv[1], sys.argv[2]
envdir = os.path.join(root, "hack", "bakeoff")
resolved = ("default", "profile:", "loser:", "reference (", "baseline (",
            "measured, never the default", "disqualified", "not a candidate")
for name in sorted(os.listdir(envdir)):
    if not name.endswith(".env"):
        continue
    fam = name[:-4]
    path = os.path.join(scdir, fam + ".json")
    if not os.path.exists(path):
        continue
    removed = ""
    klass = "A"
    for line in open(os.path.join(envdir, name)):
        m = re.match(r'^REMOVED=(\S+)', line.strip())
        if m:
            removed = m.group(1)
        m = re.match(r'^CLASS=(\S+)', line.strip())
        if m:
            klass = m.group(1)
    if removed and klass != "B":
        print("%s: declares REMOVED= and CLASS=%s; a §9.1 arm is runtime-selectable and is not removed"
              % (fam, klass))
    sc = json.load(open(path))
    for k in sc.get("kinds", []):
        d = k.get("disposition", "")
        if not d.startswith(resolved):
            print("%s: kind %s has no §9.4(5) disposition: %r" % (fam, k.get("kind"), d[:60]))
        # A §9.2 shape a caller cannot select may not be recorded as retained
        # for callers who want it, which is the one claim the M6 review must
        # not make.
        if klass == "B" and "runtime-selectable" in d:
            print("%s: kind %s is a §9.2 shape and claims to be runtime-selectable: %r"
                  % (fam, k.get("kind"), d[:60]))
        if removed and ("removed (%s)" % removed) not in d:
            print("%s: kind %s does not record the removal commit %s: %r"
                  % (fam, k.get("kind"), removed[:12], d[-60:]))
        if not removed and "removed (" in d:
            print("%s: kind %s records a removal that hack/bakeoff/%s does not declare"
                  % (fam, k.get("kind"), name))
PY
}

# 22. Every committed scorecard's every kind carries a resolved disposition.
probs="$(disposition_problems "$root/benchmarks/reports/scorecards")"
if [ -n "$probs" ]; then
  bad "every stored kind has a §9.4(5) disposition" "$(printf '%s' "$probs" | head -n 8)"
else
  ok "every stored kind has a §9.4(5) disposition"
fi

# 23. And the check fails on an unresolved one, and on a removal whose commit
#     the family does not declare. Without this it is a startswith() over a
#     tuple, which passes for every string if the tuple is wrong.
if [ ! -f "$root/benchmarks/reports/scorecards/A9.json" ]; then
  skip "an unresolved or undeclared disposition is reported" "benchmarks/reports/scorecards/A9.json is not in this repository; commit a scorecard for the case to run"
else
mkdir -p "$tmp/disp-doctored"
python3 - "$root/benchmarks/reports/scorecards/A9.json" "$tmp/disp-doctored/A9.json" <<'PY'
import json, sys
d = json.load(open(sys.argv[1]))
d["kinds"][0]["disposition"] = "retained pending profile review"
if len(d["kinds"]) > 1:
    d["kinds"][1]["disposition"] = "default; removed (deadbeefdeadbeef)"
json.dump(d, open(sys.argv[2], "w"), indent=2)
PY
probs="$(disposition_problems "$tmp/disp-doctored")"
missing=""
printf '%s' "$probs" | grep -q "has no §9.4(5) disposition" || missing="$missing unresolved"
printf '%s' "$probs" | grep -q "records a removal that hack/bakeoff" || missing="$missing undeclared-removal"
if [ -n "$missing" ]; then
  bad "an unresolved or undeclared disposition is reported" "not reported:$missing (got: $(printf '%s' "$probs" | tr '\n' '; '))"
else
  ok "an unresolved or undeclared disposition is reported"
fi
fi

# 24. A declared REMOVED= commit is a commit this repository has, and an
#     ancestor of HEAD. The recorded hash is the whole of the record once the
#     code is gone, so a hash nobody can check out is not one.
probs=""
# A shallow clone holds one commit, so it can answer neither question: the
# recorded hash is simply absent from it. GitHub's `actions/checkout` fetches
# depth 1 by default, and this check failed the first CI run on GitHub for
# exactly that reason -- a defect in the check rather than in the record. The
# harness job now fetches the full history so the check RUNS there; a shallow
# clone anywhere else skips it and says so, because a check that cannot see
# history must not report the record as wrong.
if [ "$(git -C "$root" rev-parse --is-shallow-repository 2>/dev/null)" = "true" ]; then
  skip "every declared removal commit is an ancestor of HEAD" "this is a shallow clone: run \`git fetch --unshallow\` for the check to mean anything"
else
for envf in "$root"/hack/bakeoff/*.env; do
  rev="$(sed -n 's/^REMOVED=\([^ ]*\).*/\1/p' "$envf" | head -n1)"
  [ -n "$rev" ] || continue
  fam="$(basename "$envf" .env)"
  if ! git -C "$root" cat-file -e "$rev^{commit}" 2>/dev/null; then
    probs="$probs$fam: REMOVED=$rev is not a commit of this repository"$'\n'
  elif ! git -C "$root" merge-base --is-ancestor "$rev" HEAD 2>/dev/null; then
    probs="$probs$fam: REMOVED=$rev is not an ancestor of HEAD"$'\n'
  fi
done
if [ -n "$probs" ]; then
  bad "every declared removal commit is an ancestor of HEAD" "$(printf '%s' "$probs" | head -n 8)"
else
  ok "every declared removal commit is an ancestor of HEAD"
fi
fi

printf '\nbakeoff_test: %d passed, %d failed, %d skipped\n' "$pass" "$fail" "$skipped"
[ "$fail" -eq 0 ]
