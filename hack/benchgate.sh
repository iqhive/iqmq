#!/usr/bin/env bash
# The §10.6 benchmark gate: measure a gate set on the candidate tree and on a
# base ref, interleaved, and hand both runs to benchmarks/cmd/benchgate.
#
#   hack/benchgate.sh [BASE_REF] [GATE]        # make gate BASE=… GATE=…
#   hack/benchgate.sh __measure …              # internal: the guarded half
#
# Exit status: 0 pass · 1 regression, budget breach or a gate benchmark missing
# from the candidate · 2 setup error (a side that does not build, a gate set
# that selected nothing) · 3 inconclusive (benchgate: a gate benchmark's median
# confidence interval was too wide to decide) · 4 the measurement RAN and was
# contended, so discard it and repeat · 5 the load guard refused to start and
# nothing ran, or the run was swamped by foreign runnable threads even though
# no foreign benchmark ran. 4 and 5 are separate from 3 because they are not
# answers about the code: conflating a busy host with a verdict is how a gate
# starts crying wolf and then gets ignored.
#
# the benchmark configuration step 2 asks for "candidate and baseline interleaved", and
# Appendix B of the plan spells the shape: `git worktree` base -> `go test -c`
# both -> interleaved runs, pinned (the plan said `taskset`; the pin now comes
# from `cpu-policy run --bench`, which picks quiet whole cores rather than loose
# threads and names them in CPU_POLICY_CPUS), load guard, warm-up discarded ->
# fail on +3 % sec/op, -3 % msgs/s, ANY allocs/op increase, +3 % B/op, or a
# breached budget.
#
# What this script does that its first version did not :
#
#   * It compiles each side once (`go test -c`) and interleaves the BINARIES.
#     Before, each of the five rounds re-entered `go test ./...`, so every
#     measurement carried a build-cache lookup for every package in the module
#     and the base side's first round carried the build itself.
#   * It measures only the packages that hold a benchmark the gate set selects
#     (`bakeoff gate-pattern -packages`), not `./...` -- which started every
#     package's test binary in the module, five times, per side.
#   * It alternates which side goes first between rounds, so a host that drifts
#     monotonically during the measurement does not drift into one side's
#     favour.
#   * It runs the whole interleaved measurement under `hack/benchguard.sh` at
#     strict settings, once, and REFUSES to produce a verdict from a contended
#     run. A gate is a decision (the contributor notes, host etiquette) and a
#     regression read off a contended run is the failure mode the guard exists
#     for. Before, nothing guarded it at all.
#   * It pins GOMAXPROCS and the CPU set to the same rung the A24 campaign
#     measured at (16), because the absolute ceilings in benchmarks/budgets.yaml
#     are that rung's medians and are not comparable across rungs.
#   * It does not swallow errors. `go test … 2>/dev/null … || true` on every
#     measurement meant a side that failed to build produced an empty file and
#     the gate went on to compare it.
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# The host CPU ceiling. This script invokes `go` itself, so it is capped when
# run by hand and not only when a make recipe exports the cap into it.
# hack/cpucap.sh carries the arithmetic; the eval never
# RAISES either half.
eval "$("$root/hack/cpucap.sh" env)"

# ---------------------------------------------------------------- the guarded half
if [ "${1:-}" = __measure ]; then
  shift
  out="$1" stamp="$2" pattern="$3" bindir="$4" wt="$5" rounds="$6" count="$7" benchtime="$8" cpus="$9" pkglist="${10}" headonly="${11}"
  started=$(date +%s)
  for r in $(seq 1 "$rounds"); do
    # Alternate the order: base first on odd rounds, candidate first on even.
    order="base head"
    [ $((r % 2)) -eq 0 ] && order="head base"
    for side in $order; do
      dir="$root"; [ "$side" = base ] && dir="$wt"
      while IFS= read -r pkg; do
        [ -n "$pkg" ] || continue
        # A package the BASE tree does not have is measured on the candidate
        # only: there is nothing to compare it against, and the absolute
        # budget is what binds it.
        if [ "$side" = base ] && grep -qxF -- "$pkg" "$headonly"; then continue; fi
        slug="$(echo "$pkg" | tr -c 'A-Za-z0-9' '_')"
        bin="$bindir/$side$slug.test"
        [ -x "$bin" ] || { echo "!! $side: $pkg has no test binary" >&2; exit 2; }
        ( cd "$dir/${pkg#./}" && env GOWORK=off "$bin" \
            -test.run='^$' -test.bench="$pattern" -test.benchmem \
            -test.benchtime="$benchtime" -test.count="$count" -test.cpu="$cpus" \
            -test.timeout=20m ) >> "$out/$stamp-$side.txt"
      done < "$pkglist"
    done
    echo "== gate: round $r/$rounds done ($(( $(date +%s) - started ))s elapsed)"
  done
  exit 0
fi

# ---------------------------------------------------------------- the driver
base_ref="${1:-main}"; gate="${2:-pr}"
# Twenty samples a side, at 200 ms per benchmark against the 3000 ms ceiling.
# Ten at 50 ms left 213 of 360 rows with a median interval wider than the ±5 %
# reject threshold, which is the gate declining to answer; the same rows at
# twenty sit at ±0.5-1.3 %.
rounds="${IQMQ_GATE_ROUNDS:-10}"
count="${IQMQ_GATE_COUNT:-2}"
benchtime="${IQMQ_GATE_BENCHTIME:-100ms}"
cpus="${IQMQ_GATE_CPUS:-${IQMQ_MAX_CPUS:-16}}"
max_cpus="${IQMQ_BENCH_MAX_CPUS:-${IQMQ_MAX_CPUS:-16}}"
per_bench_max_ms="${IQMQ_BENCH_PER_BENCH_MAX_MS:-3000}"
total_max_s="${IQMQ_BENCH_TOTAL_MAX_S:-3600}"

# The harness ceilings the contributor notes names apply to the gate too: two sides
# at ten samples each is the budget, and the gate has to fit inside it rather
# than reach for a bigger one.
case "$benchtime" in
  *ms) bt_ms="${benchtime%ms}" ;;
  *s)  bt_ms=$(( ${benchtime%s} * 1000 )) ;;
  *x)  bt_ms=0 ;;
  *)   echo "benchgate: IQMQ_GATE_BENCHTIME='$benchtime' must be in ms, s or x" >&2; exit 2 ;;
esac
per_bench_ms=$(( bt_ms * count ))
if [ "$per_bench_ms" -gt "$per_bench_max_ms" ]; then
  echo "benchgate: one benchmark would run for ${per_bench_ms}ms, over the ${per_bench_max_ms}ms ceiling" >&2
  echo "  lower IQMQ_GATE_BENCHTIME or IQMQ_GATE_COUNT; do not raise IQMQ_BENCH_PER_BENCH_MAX_MS for a gate" >&2
  exit 2
fi
if [ "$cpus" -gt "$max_cpus" ]; then
  echo "benchgate: GOMAXPROCS $cpus exceeds the ${max_cpus}-CPU ceiling a measurement may take" >&2
  exit 2
fi
samples=$(( rounds * count ))
echo "== gate: $samples sample(s) per side at -benchtime=$benchtime -cpu=$cpus, ${rounds} interleaved round(s)"

out="$root/benchmarks/reports/gate"; mkdir -p "$out"
stamp="$(date -u +%Y%m%dT%H%M%SZ)"
wt="$(mktemp -d)"
bindir="$(mktemp -d)"
trap 'git -C "$root" worktree remove --force "$wt" >/dev/null 2>&1 || true; rm -rf "$bindir"' EXIT

base_sha="$(git -C "$root" rev-parse --verify "$base_ref")"
head_sha="$(git -C "$root" rev-parse --verify HEAD)"
git -C "$root" worktree add --detach "$wt" "$base_sha" >/dev/null
echo "== gate: base $base_ref ($(git -C "$root" rev-parse --short "$base_sha")) vs candidate $(git -C "$root" rev-parse --short "$head_sha")"

# The gate set decides both what to measure and where: a hand-written package
# list goes stale the first time a family moves, and `./...` measured a hundred
# packages to reach five.
pattern="$(cd "$root/benchmarks" && GOWORK=off go run ./cmd/bakeoff gate-pattern -budgets budgets.yaml "$gate")"
pkglist="$bindir/packages.txt"
(cd "$root/benchmarks" && GOWORK=off go run ./cmd/bakeoff gate-pattern -budgets budgets.yaml -packages "$gate") > "$pkglist"
npkg="$(grep -c . "$pkglist" || true)"
echo "== gate: $npkg package(s), pattern $pattern"

# Compile both sides before anything is measured, so the interleaving compares
# two binaries rather than two build-cache states, and a side that does not
# build fails here instead of contributing an empty result file.
# The module a gated package is in, and its path inside that module, from the
# one implementation of the rule (`bakeoff modpkg`). The gate used to assume
# every gated package was in the ROOT module, which is the assumption
# `hack/bakeoff.sh` shed when MODDIR landed  and the
# gate never did -- so `make gate GATE=nightly` had been a setup error for as
# long as a nested-module family was declared. `gate-pattern -packages` now
# prints repository-relative paths and this resolves each one (assumption
# B11.2).
modpkg_of() {
  (cd "$root/benchmarks" && GOWORK=off go run ./cmd/bakeoff modpkg -root .. "$1")
}

# A gate set that gains a package could not be landed at all until this
# existed: the compile step demanded both sides build every gated package, and
# the base ref by definition does not have a package the candidate is adding.
# So the increment that closes the loop -- the three edits a budget needs, of
# which "the gate measures the package" is one -- made its own pre-landing gate
# a setup error. Watched, on a base worktree at main:
#
#     # ./overhead
#     stat /tmp/.../benchmarks/overhead: directory not found
#     FAIL	./overhead [setup failed]
#     !! gate: base does not build ./benchmarks/overhead
#
# That is a fair part of why three budgets and all five §8.10 targets went
# ungated for four increments. A package the base tree does not HAVE is now
# measured on the candidate only and named in the log; a package that exists
# and does not compile is still a setup error.
headonly="$bindir/headonly.txt"
: > "$headonly"
while IFS= read -r pkg; do
  [ -n "$pkg" ] || continue
  slug="$(echo "$pkg" | tr -c 'A-Za-z0-9' '_')"
  mod=""; rel=""
  IFS=$'\t' read -r mod rel < <(modpkg_of "$pkg") || true
  [ -n "${mod:-}" ] && [ -n "${rel:-}" ] || { echo "!! gate: $pkg is in no Go module in this repository" >&2; exit 2; }
  for side in base head; do
    dir="$root"; [ "$side" = base ] && dir="$wt"
    if [ "$side" = base ] && [ ! -d "$dir/${pkg#./}" ]; then
      printf '%s\n' "$pkg" >> "$headonly"
      continue
    fi
    if ! (cd "$dir/${mod#.}" && GOWORK=off go test -c -o "$bindir/$side$slug.test" "$rel"); then
      echo "!! gate: $side does not build $pkg (module $mod, package $rel)" >&2; exit 2
    fi
    [ -x "$bindir/$side$slug.test" ] || { echo "!! gate: $side built no test binary for $pkg (no test files?)" >&2; exit 2; }
  done
done < "$pkglist"
nho="$(grep -c . "$headonly" || true)"
if [ "${nho:-0}" -gt 0 ]; then
  echo "== gate: ${nho} package(s) are NEW on the candidate and measured head-only (no comparison, absolute budgets only):"
  sed 's/^/     /' "$headonly"
  if [ "${nho:-0}" -ge "${npkg:-0}" ]; then
    echo "!! gate: every gated package is new on the candidate, so there is nothing to compare against" >&2
    echo "   The absolute budgets can still be checked with \`make bench-api\`; a comparison needs a base that has the packages." >&2
    exit 2
  fi
fi
echo "== gate: both sides compiled"

# cpu-policy pins a benchmark itself: `run --bench` picks the sixteen quietest
# whole cores, one thread per core, never the reserved cache group, and exports
# the set as CPU_POLICY_CPUS. Nothing here pins any more. A second pin chosen
# from a second sample can only narrow or contradict the first, and the project's
# rule has always been that the ceiling is a CAPACITY cap, not an affinity
#  -- the one harness that picked its own cores is gone with
# `cpucap.sh idle-cpus`. The set is still RECORDED, because a measurement's
# conditions belong with its numbers  -- recorded FROM THAT SET.
#
# There used to be a `pin` variable here holding a `taskset` prefix, and the first
# version of this change deleted the taskset and LEFT THE VARIABLE, permanently
# empty, with `"pinned"` still derived from it: a boolean that could not be true,
# printed beside the `cpuset` that contradicted it. That is the same defect shape
# as the ceiling this increment was written to fix, which is why it is spelled out
# -- derive the record from `cpuset`, and do not reintroduce `pin`.
cpuset="${CPU_POLICY_CPUS:-}"
# The note still goes into the run's own metadata either way, and still not only
# to stderr: a gate's verdict assumes the conditions the measurement had, so a
# reader has to be able to see which ones those were.
if [ -n "$cpuset" ]; then
  pinned_note="pinned by cpu-policy to $cpuset"
  echo "== gate: $pinned_note"
else
  pinned_note="NOT PINNED: cpu-policy is not driving this run"
  echo "== gate: $pinned_note; the numbers below competed with whatever else is running" >&2
fi

# One guarded measurement for the whole comparison: the lock is taken once, the
# host is waited on once, and one verdict covers both sides. Strict, because a
# gate is a decision.
status=0
"$root/hack/benchguard.sh" run -o "$out/$stamp-guard.json" -- \
  "${BASH_SOURCE[0]}" __measure "$out" "$stamp" "$pattern" "$bindir" "$wt" \
  "$rounds" "$count" "$benchtime" "$cpus" "$pkglist" "$headonly" || status=$?
case "$status" in
  0) ;;
  3) echo "!! gate: the run was CONTENDED — a foreign benchmark ran before or during it." >&2
     echo "   Discard it and repeat; never read a regression off a contended run." >&2
     exit 4 ;;
  5) echo "!! gate: the load guard REFUSED to start — the host never went quiet." >&2
     echo "   The measurement never ran; there is nothing to discard. Try later." >&2
     exit 5 ;;
  *) echo "!! gate: the measurement failed with status $status" >&2; exit "$status" ;;
esac

# The guard's verdict answers "did a foreign BENCHMARK run", which is what a
# bake-off needs. A gate needs one more thing: the run must also not have been
# swamped. A run that started on a quiet host and then competed with 56 foreign
# runnable threads against a ceiling of 48 came back `clean` and reported three
# regressions on packages this branch does not touch. So
# the gate applies the same ceiling to the whole run.
#
# WHICH NUMBER the ceiling is applied to is assumption B11.13, and it used to
# be the maximum over every five-second sample. On this host that was a check
# that could not PASS: about thirty of other people's containers idle at 9-15
# foreign runnable threads and spike above 48, a `pr` window is 365 s and
# therefore about 72 samples, and the max of 72 samples of a tailed quantity
# reaches the ceiling reliably -- measured at 35, 62 and 50 on three
# consecutive attempts, the last on a box with every other agent stopped.
# It is now the p95 of the samples, computed by
# `hack/benchguard.sh swamped`, which is the ONE implementation of the rule:
# this script and hack/bench/run-local.sh each used to carry their own copy of
# it, keyed on the maximum.
guard="$out/$stamp-guard.json"
if [ -s "$guard" ]; then
  sw=0
  swout="$("$root/hack/benchguard.sh" swamped "$guard" "${IQMQ_GATE_RUNNABLE_MAX:-}" 2>&1)" || sw=$?
  echo "$swout" | sed 's/^/== gate: /'
  case "$sw" in
    0) ;;
    3) echo "!! gate: the run was SWAMPED — the p95 of its foreign-runnable samples is over the" >&2
       echo "   ceiling the guard starts on, so it competed for more than five per cent of its" >&2
       echo "   length. The guard called it clean because no foreign *benchmark* ran, which is its" >&2
       echo "   question; a gate's question is stricter." >&2
       echo "   Discard and repeat. (IQMQ_GATE_RUNNABLE_MAX overrides the ceiling deliberately.)" >&2
       exit 4 ;;
    *) echo "!! gate: the guard's report does not carry a decidable foreign-runnable series," >&2
       echo "   so the swamped rule could not be applied to this run: $swout" >&2
       exit 2 ;;
  esac
fi

note=""
for side in base head; do
  [ -s "$out/$stamp-$side.txt" ] || { echo "!! gate: the $side run produced no output" >&2; exit 2; }
  rows="$(grep -c '^Benchmark' "$out/$stamp-$side.txt" || true)"
  [ "${rows:-0}" -gt 0 ] || { echo "!! gate: the $side run measured no benchmark — the gate set selected nothing" >&2; exit 2; }
  note="$note $side=$rows"
done
echo "== gate: benchmark result lines:$note (total budget ${total_max_s}s)"

# The pin state is written beside the run, not only echoed, so a reader of the
# verdict can tell whether these numbers were measured on quiet cores.
printf '%s\n' "$pinned_note" > "$out/$stamp-pin.txt"
echo "== gate: pin state recorded in $out/$stamp-pin.txt"
(cd "$root/benchmarks" && GOWORK=off go run ./cmd/sysinfo -o "$out/$stamp-sysinfo.json")

# BUILT and then run, NOT `go run`. `go run` does not propagate the program's
# exit status: it prints "exit status N" on stderr and then exits 1 itself,
# whatever N was. So every status this script's header documents collapsed into
# 1 before any caller could see it, and 1 is the one that means "regression".
# An inconclusive gate (3) and a setup error (2) have both been arriving as a
# regression verdict for as long as this line has been a `go run`.
#
# It cost #85: that gate measured cleanly, decided two of its rows were
# inconclusive -- `BenchmarkHeaders/Set/keys=registry/n=8` and
# `BenchmarkMiddleware/chain/n=5`, both with a confidence interval wider than
# their delta -- wrote `benchgate: INCONCLUSIVE` into the verdict, exited 3, and
# failed the pull request as a regression with `Regressions: 0` printed in its
# own table. The same flattening is why `make gate` could never tell the two
# apart either: `make` reports its own 2 for any recipe failure.
#
# hack/gate-selftest.sh did not catch it because it asserts 0 on a clean tree
# and "not 0" on an injected allocation, so 2 and 3 sit between its two cases
# and were never checked. The case added to hack/benchgate_test.sh pins the
# propagation itself.
(cd "$root/benchmarks" && GOWORK=off go build -o "$bindir/benchgate" ./cmd/benchgate)

# IQMQ_GATE_PROVISIONAL_HOST=1 says "this machine may not judge an absolute
# TIME". A hosted runner is such a machine and `.github/workflows/bench.yml`
# sets it: `BenchmarkAPI/Dispatch/interface/P=1` is budgeted at 11 ns/op from a
# measured 10.66 that two campaign runs put 1.3 % apart on THIS host, and two
# hosted-runner draws of the same commit measured 10.1 and 31.9. The 31.9 failed
# a pull request whose base measured 32.4 on the same runner -- so the head was
# the faster side and the gate called it a breach. Everything else still
# applies: the comparison, the allocation and byte ceilings, and the ratio
# budgets.
gate_extra=()
[ "${IQMQ_GATE_PROVISIONAL_HOST:-}" = 1 ] && gate_extra+=(-provisional-host)

(cd "$root/benchmarks" && "$bindir/benchgate" \
   -base "$out/$stamp-base.txt" -head "$out/$stamp-head.txt" \
   -budgets "$root/benchmarks/budgets.yaml" -gate "$gate" \
   ${gate_extra+"${gate_extra[@]}"} \
   -sysinfo "$out/$stamp-sysinfo.json" -out "$out/$stamp-verdict.md")
