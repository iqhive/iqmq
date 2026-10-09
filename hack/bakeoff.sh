#!/usr/bin/env bash
# Full-profile bake-off for one variant family (the benchmark configuration).
#   hack/bakeoff.sh FAMILY [full|quick|probe]
# Two ceilings bound every profile, checked before anything is measured:
# no single benchmark may take more than IQMQ_BENCH_PER_BENCH_MAX_MS (3000)
# -- as one invocation, as one iteration, and as one benchmark name across the
# family's counts and rungs -- and no family may measure for longer than
# IQMQ_BENCH_FAMILY_MAX_S (600). hack/bakeoff-campaign.sh adds a total budget.
# `probe` runs everything that needs no measurement -- the family's correctness
# gate, its kind list and that every benchmark filter selects something -- and
# stops before taking the host lock.
# This script renders no ADR: an ADR number is shared by many families (nine
# share ADR-014, seven share ADR-017), so the ADRs are rendered once, after the
# campaign, from the per-family scorecards written below --
#   (cd benchmarks && go run ./cmd/bakeoff adr)   # or: make bakeoff-adr
# Reads hack/bakeoff/FAMILY.env defining PKG, BENCH, REF, AUTO_KEY, MATRIX,
# ADR, TITLE and optionally KIND_DIM, IDENTS, EXTRA_ARGS, KINDS_TEST (the list
# test when a package hosts more than one family), AUTO_FILE and AUTO_PREFIX
# (where and under what name the binding is generated), and MODDIR (the module
# PKG is relative to, for a family that cannot live in the root module).
#
# Environment knobs: MATRIX (fallback for a family that declares none),
# RESUME=<run_id>|auto (continue an interrupted measurement),
# IQMQ_BENCH_ALLOW_LOST=1 (decide a family that lost samples),
# IQMQ_BENCH_CPUS, IQMQ_BENCH_LOADMAX, IQMQ_BENCH_RUNNABLE_MAX,
# IQMQ_BENCH_MAXWAIT, IQMQ_BENCH_TIMEOUT_FACTOR.
#
# IQMQ_BENCH_CPUSET is GONE, not merely unused: it named a fixed CPU list for the
# pin, and nothing here pins any more -- `cpu-policy run --bench` does, and names
# the set it chose in CPU_POLICY_CPUS.
set -euo pipefail
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# The host CPU ceiling. This script invokes `go` itself, so it is capped when
# run by hand and not only when a make recipe exports the cap into it.
# hack/cpucap.sh carries the arithmetic; the eval is a
# no-op beyond re-deriving the same GOMAXPROCS and -p when make already set
# them, and it never RAISES either.
eval "$("$root/hack/cpucap.sh" env)"
family="${1:?FAMILY required}"; profile="${2:-full}"
case "$profile" in
  full|quick|probe) ;;
  *) echo "unknown profile $profile (full|quick|probe)" >&2; exit 2 ;;
esac
# A family's declaration is hack/bakeoff/<family>.env. The two overrides below
# are test hooks (hack/bakeoff_test.sh), the same shape as the guard's
# `ownership` and `verdict` hooks: BAKEOFF_ENV points at a throwaway family's
# declaration, which must NOT be put in hack/bakeoff/ where the campaign would
# pick it up, and BENCHGUARD at a stub guard, which is the only way to
# construct "three contended attempts" or "the guard refused" on demand rather
# than by hoping the host produces them.
env_file="${BAKEOFF_ENV:-$root/hack/bakeoff/$family.env}"
[ -f "$env_file" ] || { echo "no $env_file" >&2; exit 2; }
benchguard="${BENCHGUARD:-$root/hack/benchguard.sh}"
[ -x "$benchguard" ] || { echo "no executable guard at $benchguard" >&2; exit 2; }
# A MATRIX in the environment is only a fallback: the family's own declaration
# wins, because it is the one thing that records which grid the family's §9
# criterion is read at.
# Unset before sourcing, or a MATRIX exported by the caller is indistinguishable
# from one the family declares and method.json would record the wrong source.
matrix_env="${MATRIX:-}"
unset MATRIX
# shellcheck disable=SC1090
source "$env_file"
: "${PKG:?}" "${BENCH:?}" "${REF:?}" "${AUTO_KEY:?}"

# ---------------------------------------------------------------------------
# Which module the family lives in.
#
# MODDIR names the module directory PKG is relative to. It is empty for every
# family that does not need it, and the code below is then exactly what it was.
# `go test -C` is what moves the working directory: it must be the FIRST flag
# after `test`, which is why $gotest_dir is interpolated where it is at every
# one of the four call sites (the correctness gate, the kind list, the filter
# probe and the measurement) -- all four have to run in the same module as the
# measurement, or the family's gate, kinds and rows describe different code.
moddir="${MODDIR:-}"
case "$moddir" in .|./) moddir="" ;; esac
moddir="${moddir%/}"
gotest_dir=""
pkg_rel="${PKG#./}"
if [ -n "$moddir" ]; then
  case "$moddir" in
    /*|*..*) echo "$family: MODDIR='$moddir' must be a path inside the repository" >&2; exit 2 ;;
  esac
  [ -f "$root/$moddir/go.mod" ] || {
    echo "$family: MODDIR='$moddir' is not a module ($root/$moddir/go.mod does not exist)." >&2
    echo "MODDIR names the module PKG is relative to; leave it unset for a root-module family." >&2
    exit 2
  }
  gotest_dir="-C $root/$moddir"
  pkg_rel="$moddir/${PKG#./}"
fi

# ---------------------------------------------------------------------------
# The dimension grid.
#
# Fourteen benchmark files gate their the benchmark configuration dimension sweep
# on IQMQ_BENCH_MATRIX and otherwise use a deliberately short smoke grid.
# Nothing in any executable line of this script, hack/bakeoff-campaign.sh or
# the Makefile ever set that variable, so the stopped thirty-eight-family
# campaign measured the smoke grid throughout: family A1's raw.txt held
# P={1,4} batch={1,16} occupancy={50} where internal/queue's full branch gives
# P={1,4,8,32} batch={1,16,256} occupancy={0,50,95} -- 14 benchmarks per kind
# instead of 117 -- and family A4, whose §9.1 criterion is read at 1e6
# entries, measured 1e3 and 1e5. That data is unrecoverable: the rows are not
# in raw.txt.
#
# So an unset MATRIX is an error and not a default. Silently defaulting to the
# smoke grid is exactly what cost the campaign its hours, and the harness
# cannot know which grid a family needs -- that is a statement about the
# family's §9 criterion, which belongs in the family's declaration. Erroring
# costs a message; defaulting costs the campaign.
matrix="${MATRIX:-}"; matrix_src=family
if [ -z "$matrix" ]; then matrix="$matrix_env"; matrix_src=environment; fi
if [ -z "$matrix" ]; then matrix_src=unset; fi
case "$matrix" in
  default|full|huge) ;;
  *)
    {
      if [ "$matrix_src" = unset ]; then
        echo "$family: $env_file declares no MATRIX, and none was passed in the environment."
      else
        echo "$family: MATRIX='$matrix' is not one of default|full|huge (from the $matrix_src)."
        echo "A value the Go side does not recognise is the smoke grid under another name:"
        echo "the benchmark files test IQMQ_BENCH_MATRIX against the literals 'full' and"
        echo "'huge', so 'Full' or 'FULL' would measure the short grid and say nothing."
      fi
      echo
      echo "Declare the grid the family's the benchmark configuration criterion is read at, in"
      echo "$env_file:"
      echo "    MATRIX=full        # the decision needs the §9.1/§10.4 sweep"
      echo "    MATRIX=default     # the short grid is the whole of what it measures"
      echo "    MATRIX=huge        # and the opt-in sizes on top (internal/timer's 1e7)"
      echo "or pass MATRIX=... in the environment for a one-off run."
    } >&2
    exit 2 ;;
esac
# Exported once, so every `go test` below -- the correctness gate, the kind
# list, the filter probe and the measurement -- runs on the same grid. A kind
# list or a filter probe taken on a different grid from the measurement
# describes a different benchmark set, and the timeout is computed from the
# probe's count.
export IQMQ_BENCH_MATRIX="$matrix"

# The profile's settings, resolved before anything runs so that a resumed run
# can be checked against the method.json it is resuming.
# No single benchmark may occupy this machine for more than three seconds, and
# no campaign may run for hours by default. Both are ceilings rather than
# targets, both are checked before anything is measured, and both are the
# reason the profiles below are what they are.
#
# "One benchmark" has two meanings and the ceiling covers both: `-benchtime` is
# what a single invocation may take, and `count x rungs x benchtime` is what a
# single benchmark NAME costs across a family's whole run. The third case -- a
# single *iteration* longer than the ceiling -- cannot be bounded by a flag at
# all, because `go test` always finishes the iteration it started, so the
# filter probe measures it at -benchtime=1x and refuses the family.
per_bench_max_ms="${IQMQ_BENCH_PER_BENCH_MAX_MS:-3000}"

# The GOMAXPROCS ladder was `1,4,8,32`, which multiplied every family's cost
# fourfold for an axis its benchmarks mostly sweep themselves: family A1's
# contention point is `P=32` *inside* the benchmark name, not a `-cpu` rung,
# and the allocation counts the B-rows turn on do not vary with GOMAXPROCS at
# all. One rung at the pinned width keeps the contention the criteria name and
# costs a quarter as much. A family that needs the ladder asks for it with
# IQMQ_BENCH_CPUS, and pays for it inside the same ceilings.
# A measurement never takes more than this many CPUs, so the rest of the
# machine stays available -- and the guard's ceilings are the size of that
# rest, `nproc - max_cpus`, which is both what says the host has no room left
# and far above anything the measurement itself can contribute. The guard
# computes the same number; it is passed explicitly so `method.json` records
# what this run was actually held to.
# The default falls through to IQMQ_MAX_CPUS rather than repeating a 16, so the
# benchmark ceiling and the test ceiling are ONE number: a pinned benchmark host
# that raises IQMQ_MAX_CPUS raises both, and neither can drift away from the
# other. Every `${IQMQ_BENCH_MAX_CPUS:-...}` in hack/ reads the same way, and
# `make cpu-cap-check` fails on one that goes back to a bare literal.
max_cpus="${IQMQ_BENCH_MAX_CPUS:-${IQMQ_MAX_CPUS:-16}}"
# Asked of the guard, not computed here: this expression used to be copied into
# four scripts, and `nproc` reports CPU AFFINITY rather than the size of the
# machine, so on a host whose mask had been narrowed to the measurement's own
# width every copy quietly evaluated to 1.
headroom="$("$root/hack/benchguard.sh" headroom)"
case "$profile" in
  full)  count=10; cpus="${IQMQ_BENCH_CPUS:-$max_cpus}";       benchtime="${IQMQ_BENCH_TIME:-50ms}"; smoke=""; loadmax="${IQMQ_BENCH_LOADMAX:-$headroom}" ;;
  quick) count=3;  cpus="${IQMQ_BENCH_CPUS:-$((max_cpus/2))}"; benchtime="${IQMQ_BENCH_TIME:-50ms}"; smoke="-s"; loadmax="${IQMQ_BENCH_LOADMAX:-$headroom}" ;;
  probe) count=0;  cpus="${IQMQ_BENCH_CPUS:-1}";               benchtime="1x";                       smoke="-s"; loadmax="${IQMQ_BENCH_LOADMAX:-$headroom}" ;;
esac
# What the strict wait actually waits for. `loadmax` above is the verdict's
# backstop ceiling and no longer gates the wait: the 1-minute average cannot
# tell this measurement's own decaying contribution from a neighbour's build,
# and at 4.0 with a per-run peak near 12.4 every run waited 40-60 s for its own
# predecessor. The guard waits instead for no foreign
# benchmark and fewer than this many FOREIGN runnable threads, which our own
# exited processes do not contribute to. Both ceilings are `hack/benchguard.sh
# headroom` -- the POLICY's bench load limit where there is one, 24 on this
# 48-core host -- the size of the machine a measurement deliberately leaves alone:
# what fits in that rest is not competing with us, and our own share cannot reach
# it. It used to be `nproc - max_cpus` computed here, and that is the expression
# which silently became 1 when the mask narrowed. Do not
# re-derive it locally.
# Recorded in method.json beside the load ceiling, because it is now the
# condition every number below was measured under.
runnable="${IQMQ_BENCH_RUNNABLE_MAX:-$headroom}"
# The load ceiling is passed to the guard ONLY when the CALLER set one.
#
# `loadmax` above is what `hack/benchguard.sh headroom` answers when nobody asked
# for anything else, which is the number the guard derives for itself -- it was passed explicitly
# so that the record would name what the run was held to. Nothing is lost by
# not passing it: the guard writes `loadmax=` into its own summary line and
# into every guard-*.json beside the run, and what it writes there is the
# ceiling it ACTUALLY used, which is the stronger record. But the guard now
# treats an explicit -t as an
# instruction that must be obeyed however small, and derives its own only when
# it is left to: on a host where the derived ceiling does not exceed `max_cpus`
# -- every CI runner, though NOT this 48-core host, where 24 exceeds 16 and the
# load half therefore applies -- a DERIVED ceiling drops the
# load half of the pre-run gate, because there the average cannot exceed it
# without the measurement's own threads. Passing the same
# number explicitly would have kept this script, the guard's main caller, on
# the old behaviour: waiting out `-w` for load that was its own.
tflag=()
[ -n "${IQMQ_BENCH_LOADMAX:-}" ] && tflag=(-t "$loadmax")
# The gate and the probe get an explicit ceiling too. `go test`'s default is
# ten minutes for every invocation this script makes, and none of them ever
# raised it.
gate_timeout=1800
probe_timeout=1800

# ---------------------------------------------------------------------------
# `go test -bench` splits its pattern on unbracketed slashes and matches each
# part as an UNANCHORED regexp against one element of the benchmark name. So
#     -bench "^${BENCH}/${slug}(/|$)"
# anchored only the first element: the slug matched anywhere inside its own
# path element, and the root matched any function name starting with it.
# Both misfire in this tree today:
#   - family B10's reference kind is `prealloccodes` and its sibling is
#     `alloccodes`; the filter for alloccodes selected all 12 prealloccodes
#     rows as well, so two kinds contributed the same rows and B10's decision
#     was taken over data belonging to one arm;
#   - family A18 (BENCH=BenchmarkWire, ./internal/wire) also selected
#     BenchmarkWireZeroCopy/<kind>/*, and family A30
#     (BENCH=BenchmarkMiddleware, ./internal/mwchain) also selected
#     BenchmarkMiddlewareCompose/<kind>/* -- different benchmarks, folded into
#     the family's rows.
# Anchoring every element is also what the Go side already does:
# benchmarks/cmd/bakeoff/lintenv.go's underRoot/splitKind compare the root and
# the kind element exactly, so `make bench-lint` was checking a stricter set
# than the harness measured.
anchor_bench() { printf '%s\n' "$1" | awk -F/ '{s="";for(i=1;i<=NF;i++){s=s (i>1?"/":"") "^" $i "$"}; print s}'; }

# ---------------------------------------------------------------------------
# The run directory. Announced immediately, and before anything can fail, so
# that hack/bakeoff-campaign.sh can find the evidence of a family that FAILED
# -- it used to parse the directory out of the "done ->" line, which only a
# successful family prints, so exactly the families whose contention counts
# matter contributed none.
resumed=0
if [ -n "${RESUME:-}" ] && [ "$profile" != probe ]; then
  case "$RESUME" in
    auto) resume_dir="$(ls -1d "$root"/benchmarks/reports/results/*-"$family" 2>/dev/null | sort | tail -n1)" || resume_dir="" ;;
    *)    resume_dir="$root/benchmarks/reports/results/$RESUME" ;;
  esac
  [ -n "$resume_dir" ] && [ -d "$resume_dir" ] || { echo "$family: RESUME=$RESUME names no run directory under benchmarks/reports/results" >&2; exit 2; }
  [ -f "$resume_dir/method.json" ] || { echo "$family: $resume_dir has no method.json; it cannot be resumed" >&2; exit 2; }
  # A resume onto a run measured differently would mix two grids in one
  # raw.txt, which is the defect this script is being fixed for.
  want="\"profile\":\"$profile\",\"matrix\":\"$matrix\",\"count\":$count,\"gomaxprocs\":\"$cpus\",\"benchtime\":\"$benchtime\""
  grep -q "$want" "$resume_dir/method.json" || {
    { echo "$family: $resume_dir was measured under a different method; refusing to resume onto it"
      echo "  want: $want"
      echo "  have: $(cat "$resume_dir/method.json")"; } >&2
    exit 2
  }
  out="$resume_dir"; run_id="$(basename "$out")"; resumed=1
else
  run_id="$(date -u +%Y%m%dT%H%M%SZ)-$family"
  out="$root/benchmarks/reports/results/$run_id"
fi
mkdir -p "$out"
# Only a deciding profile's results are evidence; `quick` and `probe` leave
# scratch, and a `git add -A` has twice committed some. The run directory says
# so itself, so nobody has to remember which profile wrote it.
if [ "$profile" != full ]; then printf '*\n' > "$out/.gitignore"; fi
cd "$root"
echo "== $family: run -> $out"
echo "== $family: matrix=$matrix (IQMQ_BENCH_MATRIX, from the $matrix_src)"
[ "$resumed" = 1 ] && echo "== $family: resuming $run_id"

# Nothing this script starts outlives it: a day-long campaign gets signalled,
# and a benchmark left running -- or a host-wide lock left held on a
# descriptor a child inherited -- blocks the machine for every other agent.
guard_pid=""; tmp=""
kill_tree() {
  local p="$1" sig="${2:-TERM}" kid
  for kid in $(ps -o pid= --ppid "$p" 2>/dev/null); do kill_tree "$kid" "$sig"; done
  kill -"$sig" "$p" 2>/dev/null || true
}
cleanup() {
  trap - EXIT INT TERM HUP
  if [ -n "$guard_pid" ]; then kill_tree "$guard_pid" TERM; guard_pid=""; fi
  if [ -n "$tmp" ]; then rm -f "$tmp"; tmp=""; fi
  exec 9>&- 2>/dev/null || true
  :
}
trap 'rc=$?; cleanup; exit "$rc"' EXIT
trap 'echo "!! '"$family"': signalled; stopping the measurement and releasing the host lock" >&2; cleanup; exit 130' INT TERM HUP

echo "== $family: correctness gate ($PKG)"
disq=""
# EXTRA_ARGS carries the build tags a family is measured under (A10's
# -tags iqmq_unsafe), so the correctness gate has to run under them too: a
# family gated on a build it is not measured on is gated on other code.
# shellcheck disable=SC2086
if ! GOWORK=off go test $gotest_dir -race -count=1 -timeout "${gate_timeout}s" -json ${EXTRA_ARGS:-} "$PKG" > "$out/gate.jsonl"; then
  # `|| true`: with `set -o pipefail` a grep that matches nothing fails the
  # pipeline, and `set -e` then ended the script HERE -- before the message
  # written for exactly that case, and with no diagnostic at all. It is the
  # same shape as the guard-summary pipeline below, and this repository keeps
  # finding it.
  disq="$(grep -o '"Action":"fail","Package":"[^"]*","Test":"\(TestModel\|Fuzz[A-Za-z0-9_]*\)/[^"/]*' "$out/gate.jsonl" \
          | sed -e 's#.*/##' | sort -u | paste -sd, - || true)"
  if [ -z "$disq" ]; then
    {
      echo "   the correctness gate failed and no kind test is responsible; aborting."
      echo "   failing tests (full log: $out/gate.jsonl):"
      grep -o '"Action":"fail"[^}]*"Test":"[^"]*"' "$out/gate.jsonl" \
        | sed -e 's/.*"Test":"//' -e 's/"$//' | sort -u | sed 's/^/     /' || true
      grep -o '"Action":"output"[^}]*"Output":"[^"]*\(build failed\|cannot \|undefined\)[^"]*"' "$out/gate.jsonl" \
        | sed -e 's/.*"Output":"//' -e 's/"$//' | sed 's/^/     /' || true
    } >&2
    exit 1
  fi
  echo "   disqualified kinds: $disq"
fi

echo "== $family: kinds"
# A package that hosts more than one variant family gives each of them its own
# list test; KINDS_TEST names the one this family is measured from.
# Written to a file rather than read through a pipeline: a pipeline hid
# `go test`'s own status (a build failure under EXTRA_ARGS was reported as
# "printed no kinds", or ended the script silently under pipefail), and
# `head -n1` on a pipe SIGPIPEs its producer, which pipefail turns into 141.
kinds_test="${KINDS_TEST:-TestKindsList}"
# EXTRA_ARGS here too: A10 is measured under -tags iqmq_unsafe, and its kind
# list and generated identifiers were read from a different build from the one
# measured.
# shellcheck disable=SC2086
if ! GOWORK=off go test $gotest_dir -run "^${kinds_test}\$" -v -count=1 -timeout "${gate_timeout}s" ${EXTRA_ARGS:-} "$PKG" > "$out/kinds.txt" 2>&1; then
  { echo "   $kinds_test failed in $PKG (full log: $out/kinds.txt):"; tail -n 40 "$out/kinds.txt" | sed 's/^/     /'; } >&2
  exit 1
fi
kinds="$(sed -n 's/.*kinds: *//p' "$out/kinds.txt" | sed -n 1p)"
[ -n "$kinds" ] || { echo "   $kinds_test printed no 'kinds:' line in $PKG (full log: $out/kinds.txt)" >&2; exit 1; }
IDENTS="${IDENTS:-$kinds}"
slugs="$(echo "$kinds" | tr ',' '\n' | cut -d= -f1)"
# When the kind is a key=value dimension inside the benchmark name rather than
# a path segment, one run measures every kind at once and `bakeoff decide`
# splits the results by that dimension.
# One `go test` per round, covering every kind of the family, rather than one
# per kind per round. It is both the stronger interleaving and much the
# cheaper: §9.4 step 2 wants the candidate and the baseline interleaved, and
# inside a single invocation Go runs the matched benchmarks one after another,
# so every kind is measured adjacently and the whole set is repeated `count`
# times. Per-kind invocations interleaved just as often but paid `go test`'s
# start-up for each kind at each round -- measured at 6.5 s per run of which
# 1.25 s was measurement, 1,430 invocations for the campaign against 380, and
# 77 minutes of wall clock against 32. A disqualified kind is excluded by
# `decide` from results it is in, rather than by not measuring it.
runs="__all__"
# The filter probe stays per kind, whatever the measurement does: it is what
# counts a family's rows kind by kind and what catches a kind with no
# benchmark of its own, and it is where the per-kind filter's element-by-element
# anchoring is checked. Only the measurement batches.
probe_runs="$slugs"
# ...except where the kind is not a path segment at all. A KIND_DIM family
# carries its kind as `<dim>=<value>` inside the name, so a per-kind path
# filter selects nothing and the probe has to take the whole root, exactly as
# it did before the measurement was batched.
[ -n "${KIND_DIM:-}" ] && probe_runs="__all__"

# Prove every filter selects something before spending hours on the
# measurement, on the same grid the measurement will use -- a probe taken on a
# different grid counts a different benchmark set. The count is also what the
# measurement's -timeout is derived from.
echo "== $family: filters"
nmax=0; ntotal=0
for slug in $probe_runs; do
  if [ "$slug" = "__all__" ]; then f="$(anchor_bench "$BENCH")"; else f="$(anchor_bench "$BENCH/$slug")"; fi
  plog="$out/probe-$slug.txt"
  # No `|| true` swallowing the status this time: a build failure used to be
  # reported as "selects no benchmark", which sends a reader looking at the
  # family's declaration for a compiler error.
  # shellcheck disable=SC2086
  if ! GOWORK=off go test $gotest_dir -run='^$' -bench="$f" -benchtime=1x -count=1 -cpu=1 -timeout "${probe_timeout}s" ${EXTRA_ARGS:-} "$PKG" > "$plog" 2>&1; then
    { echo "   -bench '$f' did not build or run in $PKG (full log: $plog):"; tail -n 40 "$plog" | sed 's/^/     /'; } >&2
    exit 1
  fi
  n="$(grep -c '^Benchmark' "$plog" || true)"
  [ "$n" -gt 0 ] || { echo "   -bench '$f' selects no benchmark in $PKG (full log: $plog)" >&2; exit 1; }
  [ "$n" -gt "$nmax" ] && nmax="$n"
  ntotal=$(( ntotal + n ))
  # The probe runs at -benchtime=1x, so each row's ns/op IS the cost of one
  # iteration. A single iteration longer than the ceiling cannot be bounded by
  # -benchtime -- `go test` always finishes the iteration it started -- so it
  # has to be found here and fixed in the benchmark, not tuned around.
  slow="$(awk -v lim="$per_bench_max_ms" '/^Benchmark/ {
      for (i = 2; i <= NF; i++) if ($i == "ns/op") { ms = $(i-1) / 1000000
        if (ms > lim) printf "     %s: one iteration takes %.1fs\n", $1, ms/1000 }
    }' "$plog")"
  if [ -n "$slow" ]; then
    { echo "   -bench '$f': $(printf '%s\n' "$slow" | grep -c .) benchmark(s) exceed the ${per_bench_max_ms}ms per-benchmark ceiling on a single iteration:"
      printf '%s\n' "$slow"
      echo "     A single iteration cannot be shortened by -benchtime; the benchmark's own work has to be." ; } >&2
    exit 2
  fi
  echo "   $slug: $n benchmark(s) [-bench '$f']"
done
echo "   $ntotal row(s) in the family"

# `probe` is everything that can be checked without measuring: the family's
# correctness gate, its kind list and that every benchmark filter selects
# something. Running it over every family costs minutes and is what tells a
# structural breakage apart from a slow campaign.
if [ "$profile" = probe ]; then echo "== $family: probe ok"; exit 0; fi

# `quick` is a smoke profile -- the Makefile calls its sibling `bench-quick`
# "never used for decisions" -- so it records contention instead of waiting for
# it to clear. Only `full` feeds an ADR, and only `full` is strict: it waits
# for a quiet host, discards a contended run and repeats it. Making both
# profiles strict meant an agent checking that a family's bake-off runs at all
# sat for three quarters of an hour behind another agent's build.

# Pin the measurement to a fixed set of CPUs, taken from the set this
# container is actually allowed to use. The previous `taskset -c 8-15` was
# hardcoded: it happens to be allowed here, but the allowed set is sparse
# (`/proc/self/status` Cpus_allowed_list), so a different allocation would have
# had every run measure on CPUs the harness never chose -- or fail outright.
# The width matters as much as the identity: GOMAXPROCS=32 on eight CPUs
# measures four-way oversubscription of the Go scheduler, not the variant under
# test, which is the opposite of what the performance budgets's ladder is for. So
# the widest GOMAXPROCS value must not exceed the pinned width.
widest="$(echo "$cpus" | tr ',' '\n' | sort -n | tail -n1)"
if [ "$widest" -gt "$max_cpus" ]; then
  echo "$family: GOMAXPROCS $widest exceeds the ${max_cpus}-CPU ceiling a measurement may take" >&2
  echo "  lower IQMQ_BENCH_CPUS, or raise IQMQ_BENCH_MAX_CPUS deliberately -- pinning fewer CPUs than" >&2
  echo "  GOMAXPROCS is told it has measures the scheduler oversubscribed, not the variant" >&2
  exit 2
fi
# The verdict's own-load allowance (IQMQ_BENCH_OWN_LOAD), which assumption
# A24.56 set to the pinned width: a run pinned to w CPUs cannot make more than
# w of its own threads runnable. That was written when the strict wait held
# every run until the 1-minute average had decayed to 4.0, so a run's peak was
# its own contribution on top of an empty host. Since assumption A24.80 a run
# starts as soon as nothing FOREIGN is running, which for a campaign is
# straight after its own predecessor -- so the average a run inherits is its
# predecessor's, which is ours too, and is bounded by the same width. Measured
# on three back-to-back 80 s runs of 32 pinned threads: load_before 9.9 ->
# 27.5 -> 30.0 and peaks of 27.5, 32.4, 33.7 against a ceiling of 4.0 + 32 =
# 36 -- 2.3 of headroom, so a neighbour adding two to the average would have
# marked the campaign's own steady state `contended reason=load`, lost the
# sample three attempts later and failed the family. At 2w the ceiling is 68,
# the campaign cannot trip the load half with its own load at all, and gross
# foreign load -- what that half is for -- still does.
own_load=$((widest * 2))
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
# The refusal that used to live here -- "only N CPU(s) available for a GOMAXPROCS
# of $widest" -- is exactly the admission check a shared host must not have: it
# counted free CPUs inside this process's OWN affinity set, so it refused
# whenever a neighbour touched those cores even with the machine otherwise idle,
# and the only way past it was to force it and record numbers taken under
# contention. cpu-policy judges the whole machine instead.
if [ -n "$cpuset" ]; then
  echo "== $family: pinned by cpu-policy to $cpuset"
fi

# The measurement's -timeout, from the profile and the probe's own count.
# `go test`'s default is ten minutes and nothing ever raised it, which the
# widest families already exceed: A2 and A7 select 112 benchmarks in a single
# invocation at the smoke grid and 144 at the full one (they use KIND_DIM, so
# one run measures every kind at once), and 144 x 4 GOMAXPROCS rungs x 1 s is
# 576 s of measured time before any warm-up -- the run would be killed
# mid-family. The floor is twenty minutes; the factor covers `go test`'s
# warm-up ramp, the per-benchmark fixtures (family A4 builds a 1e6-entry
# scheduler per row) and GC, and the additive slack covers the build and the
# binary's start-up. Loose enough never to fire on a slow family, and still a
# bound on a hung one, which the ten-minute default was neither of.
case "$benchtime" in
  *ms) bt_ms="${benchtime%ms}" ;;
  *s)  bt_ms=$(( ${benchtime%s} * 1000 )) ;;
  *)   bt_ms=1000 ;;
esac
rungs="$(echo "$cpus" | tr ',' '\n' | grep -c . || true)"
[ "${rungs:-0}" -gt 0 ] || { echo "$family: IQMQ_BENCH_CPUS is empty; there is no GOMAXPROCS ladder to measure" >&2; exit 2; }
# The two ceilings, enforced before anything is measured. The arithmetic is
# printed either way, because a refusal a reader cannot check is not much
# better than the run it prevented.
if [ "$bt_ms" -gt "$per_bench_max_ms" ]; then
  echo "$family: -benchtime $benchtime is ${bt_ms}ms, above the ${per_bench_max_ms}ms a single benchmark invocation may take" >&2
  echo "  raise IQMQ_BENCH_PER_BENCH_MAX_MS deliberately, or lower IQMQ_BENCH_TIME" >&2
  exit 2
fi
per_bench_ms=$(( count * rungs * bt_ms ))
if [ "$per_bench_ms" -gt "$per_bench_max_ms" ]; then
  echo "$family: one benchmark would take ${per_bench_ms}ms ($count count x $rungs GOMAXPROCS rung(s) x ${bt_ms}ms), above the ${per_bench_max_ms}ms ceiling" >&2
  echo "  lower the count, the rungs (IQMQ_BENCH_CPUS) or IQMQ_BENCH_TIME; or raise IQMQ_BENCH_PER_BENCH_MAX_MS deliberately" >&2
  exit 2
fi
# And the family's own share of the campaign's budget. `ntotal` is every row
# the family will measure, across all its kinds, which is what the campaign
# sums, and since a round measures every kind in one invocation it is also
# what that invocation has to finish inside its timeout.
# Milliseconds, and rounded up: a family of three rows at 150 ms each costs
# 450 ms, and integer division by a thousand reports that as zero -- which
# reads as "free" in the budget line and compares as zero against a ceiling.
family_est_ms=$(( ntotal * per_bench_ms ))
family_est=$(( (family_est_ms + 999) / 1000 ))
family_max="${IQMQ_BENCH_FAMILY_MAX_S:-600}"
if [ "$family_est_ms" -gt $(( family_max * 1000 )) ]; then
  echo "$family: would measure for ${family_est_ms}ms ($ntotal rows x ${per_bench_ms}ms), above the ${family_max}s a single family may take" >&2
  echo "  raise IQMQ_BENCH_FAMILY_MAX_S deliberately, or narrow the family's MATRIX" >&2
  exit 2
fi
# The estimate counts measured time only, and measured time is not the cost.
# `-benchtime` bounds the timed loop; it does not bound the fixture a benchmark
# builds before the loop starts, and Go rebuilds that for every `-count`.
# Family A4 builds a million-entry timer per row and takes 1130 s against this
# estimate's 85 s -- thirteenfold. So the family ceiling below is a bound on
# what a family may ASK to measure, and the campaign's wall-clock timeout is
# what bounds what it actually costs.
echo "== $family: budget ${family_est_ms}ms measured ($ntotal rows x ${per_bench_ms}ms/benchmark; ceilings ${per_bench_max_ms}ms/benchmark, ${family_max}s/family asked; fixture setup is not counted)"

bench_est="$(awk -v n="$ntotal" -v r="$rungs" -v ms="$bt_ms" 'BEGIN{printf "%d", (n*r*ms)/1000}')"
bench_timeout="$(awk -v e="$bench_est" -v f="${IQMQ_BENCH_TIMEOUT_FACTOR:-4}" 'BEGIN{t=f*e+600; if (t<1200) t=1200; printf "%d", t}')"
echo "== $family: -timeout ${bench_timeout}s per round ($ntotal benchmark(s) x $rungs GOMAXPROCS rung(s) x $benchtime = ${bench_est}s measured, x${IQMQ_BENCH_TIMEOUT_FACTOR:-4} + 600s)"

# The method is part of the evidence: §9.4 requires the ADR to say how the
# numbers were taken, and a shared host makes that more important, not less.
# The matrix belongs here above all: it is what says which grid of §10.4's
# dimensions the family's decision was read at.
printf '{"profile":"%s","matrix":"%s","count":%s,"gomaxprocs":"%s","benchtime":"%s","max_cpus":%s,"headroom":%s,"per_benchmark_ms":%s,"per_benchmark_max_ms":%s,"family_budget_s":%s,"cpuset":"%s","loadmax":"%s","runnable_max":"%s","own_load":"%s","pinned":%s,"matrix_source":"%s","benchmarks_per_run":%s,"gomaxprocs_rungs":%s,"measured_est_s":%s,"timeout_s":%s,"gate_timeout_s":%s}\n' \
  "$profile" "$matrix" "$count" "$cpus" "$benchtime" "$max_cpus" "$headroom" "$per_bench_ms" "$per_bench_max_ms" "$family_est" "${cpuset:-}" "$loadmax" "$runnable" "$own_load" \
  "$([ -n "$cpuset" ] && echo true || echo false)" "$matrix_src" "$ntotal" "$rungs" "$bench_est" "$bench_timeout" "$gate_timeout" > "$out/method.json"
echo "== $family: host"
(cd benchmarks && GOWORK=off go run ./cmd/sysinfo -o "$out/sysinfo.json")
# Hold the host-wide benchmark lock for the whole measurement and refuse to
# start while other workloads keep the host busy.
exec 9>"${IQMQ_BENCH_LOCK:-/tmp/iqmq-benchguard.lock}"; flock -w "${IQMQ_BENCH_MAXWAIT:-900}" 9 || { echo "benchmark lock busy" >&2; exit 1; }
# Wait for the host to go quiet rather than refusing outright: another agent
# may be finishing a build, and every measurement below waits on the same
# condition anyway. It no longer waits for this script's own correctness gate
# and filter probe to fade out of the 1-minute average -- those processes have
# exited, so they are not on the run queue and the wait does not see them.
IQMQ_BENCH_LOCK_HELD=1 "$benchguard" run $smoke "${tflag[@]}" -r "$runnable" -w "${IQMQ_BENCH_MAXWAIT:-900}" -- true 9>&- \
  || { echo "host did not become quiet within ${IQMQ_BENCH_MAXWAIT:-900}s; refusing to measure" >&2; exit 1; }

echo "== $family: measuring (profile=$profile matrix=$matrix count=$count cpu=$cpus, kinds interleaved)"
# A family can take hours, and a campaign that runs for a day gets
# interrupted. progress.tsv records every (count, kind) pair whose rows are in
# raw.txt, so RESUME=<run_id>|auto continues from there instead of throwing
# away forty minutes of a forty-five-minute family.
declare -A done_pair=()
if [ "$resumed" = 1 ] && [ -f "$out/progress.tsv" ]; then
  while IFS=$'\t' read -r pi pslug; do
    [ -n "${pi:-}" ] && done_pair["$pi/$pslug"]=1
  done < "$out/progress.tsv"
  echo "== $family: ${#done_pair[@]} (count, kind) pair(s) already measured; skipping them"
else
  : > "$out/raw.txt"; : > "$out/progress.tsv"
fi
contended=0
failed=0
lost=0
refused=0
expected=0
taken=0
declare -A lost_by_round=()
for i in $(seq 1 "$count"); do
  for slug in $runs; do
    expected=$((expected+1))
    if [ -n "${done_pair["$i/$slug"]:-}" ]; then taken=$((taken+1)); continue; fi
    if [ "$slug" = "__all__" ]; then
      bench_filter="$(anchor_bench "$BENCH")"
    else
      bench_filter="$(anchor_bench "$BENCH/$slug")"
    fi
    got=0
    rc=0
    for try in 1 2 3; do
      # Each kind run is load-checked before and after; a contended run is
      # discarded and repeated (up to three times) so a foreign benchmark that
      # starts mid-run never leaks into the decision data.
      tmp="$(mktemp)"
      # Backgrounded and waited for so that a signal reaches the trap while
      # the measurement is still running, rather than after it.
      # shellcheck disable=SC2086
      IQMQ_BENCH_LOCK_HELD=1 IQMQ_BENCH_OWN_LOAD="$own_load" "$benchguard" run $smoke "${tflag[@]}" -r "$runnable" -w "${IQMQ_BENCH_MAXWAIT:-600}" -o "$out/guard-$i-$slug-$try.json" 9>&- -- \
        env GOWORK=off go test $gotest_dir -run='^$' -bench="$bench_filter" -benchtime="$benchtime" -count=1 -cpu="$cpus" -benchmem -timeout "${bench_timeout}s" ${EXTRA_ARGS:-} "$PKG" > "$tmp" 2>&1 &
      guard_pid=$!
      wait "$guard_pid" && rc=0 || rc=$?
      guard_pid=""
      # `sed -n …p` over the file, not `grep … | sed …`: the pipeline failed
      # whenever the guarded run printed no summary line, and under `set -e`
      # with `pipefail` that ended this script with no diagnostic at all.
      if grep -q '^benchguard:' "$tmp"; then
        sed -n 's/^benchguard:/   benchguard:/p' "$tmp"
      else
        echo "   $slug run $i try $try: the guard printed no summary line (exit $rc)" >&2
      fi
      if [ "$rc" -eq 5 ]; then
        # The guard refused to start: nothing ran, so there is no sample to
        # discard, and repeating it buys another wait for the same answer. The
        # host is not measurable; stop rather than measure half a family
        # before a quiet period and half after it.
        echo "!! $family: the host never went quiet; $slug run $i of $count was refused before it started" >&2
        refused=$((refused+1)); rm -f "$tmp"; tmp=""; break 3
      fi
      if [ "$rc" -eq 3 ]; then echo "   $slug run $i try $try contended; repeating"; contended=$((contended+1)); rm -f "$tmp"; tmp=""; continue; fi
      if [ "$rc" -ne 0 ]; then
        # Never discard the output of a failed run: this is the only place the
        # reason is written down. `tail | sed >&2`, not `sed >&2 | tail >&2`:
        # the redirection defeated the pipe, so tail read nothing, printed
        # nothing, and the whole log went to stderr with the truncation dead.
        echo "   $slug run $i try $try failed (exit $rc), last 40 lines:" >&2
        tail -n 40 "$tmp" | sed 's/^/     /' >&2
        cp "$tmp" "$out/failed-$i-$slug-$try.log"
        failed=$((failed+1)); rm -f "$tmp"; tmp=""; break
      fi
      grep '^Benchmark' "$tmp" >> "$out/raw.txt" || true
      # raw.txt first, then the progress line: a resume must never claim a
      # pair whose rows are not in raw.txt.
      printf '%s\t%s\n' "$i" "$slug" >> "$out/progress.tsv"
      taken=$((taken+1)); got=1
      rm -f "$tmp"; tmp=""; break
    done
    if [ "$got" -eq 0 ] && [ "$rc" -eq 3 ]; then
      # Three contended attempts, and this (count, kind) pair contributes
      # nothing. It used to `break` here in silence: the log said "contended;
      # repeating" and then nothing, `contended_runs` counted attempts rather
      # than losses, nothing recorded which kind lost, and the family exited
      # zero with data the decision treated as complete.
      lost=$((lost+1)); lost_by_round["$i"]=$(( ${lost_by_round["$i"]:-0} + 1 ))
      if [ "$slug" = "__all__" ]; then
        echo "!! $family: LOST SAMPLE -- round $i of $count (every kind): three contended attempts, no data" >&2
      else
        echo "!! $family: LOST SAMPLE -- kind '$slug', run $i of $count: three contended attempts, no data" >&2
      fi
    fi
  done
done
lost_rounds=""
if [ "${#lost_by_round[@]}" -gt 0 ]; then
  for k in $(printf '%s\n' "${!lost_by_round[@]}" | sort -n); do
    lost_rounds="$lost_rounds,$k"
  done
  lost_rounds="${lost_rounds#,}"
fi
{
  printf 'contended_runs=%s\n' "$contended"
  printf 'lost_samples=%s\n' "$lost"
  printf 'lost_rounds=%s\n' "${lost_rounds:-none}"
  printf 'refused_runs=%s\n' "$refused"
  printf 'failed_runs=%s\n' "$failed"
  printf 'samples_expected=%s\n' "$expected"
  printf 'samples_taken=%s\n' "$taken"
  printf 'matrix=%s\n' "$matrix"
  printf 'resumed=%s\n' "$resumed"
} > "$out/contention.txt"
# The specific diagnoses come before the generic one: a refusal, a failed run
# or a lost sample is WHY raw.txt may be short or empty, and reporting "no
# benchmark output" instead would hide the reason the reader needs.
if [ "$refused" -ne 0 ]; then
  { echo "!! $family: the guard refused to measure; $taken of $expected samples were taken and the rest were never attempted."
    echo "   Nothing is decided from a partly measured family. Re-run it (RESUME=$run_id continues from $taken)."; } >&2
  exit 4
fi
[ "$failed" -eq 0 ] || { echo "$failed run(s) failed; see $out/failed-*.log" >&2; exit 1; }
if [ "$lost" -gt 0 ]; then
  {
    echo "!! $family: $lost of $expected rounds lost to contention (rounds: $lost_rounds; each round measures every kind)"
    echo "   §9.4 step 2 measures every arm interleaved at the same count, so a kind with"
    echo "   fewer samples than its siblings is compared on different data -- and"
    echo "   \`bakeoff decide\` cannot tell: it sees a shorter list of rows, not a gap."
    echo "   So the family fails rather than deciding. The data and the guard reports are"
    echo "   in $out; re-run the family (RESUME=$run_id keeps what was measured), or set"
    echo "   IQMQ_BENCH_ALLOW_LOST=1 to decide anyway -- the loss is then recorded in the"
    echo "   scorecard's methodology, where whoever reads the ADR will see it."
  } >&2
  [ "${IQMQ_BENCH_ALLOW_LOST:-0}" = 1 ] || exit 6
  echo "   IQMQ_BENCH_ALLOW_LOST=1: deciding on $taken of $expected samples." >&2
fi
[ -s "$out/raw.txt" ] || { echo "$family: no benchmark output" >&2; exit 1; }

echo "== $family: deciding"
# AUTO_FILE=- is a family that binds nothing (Appendix A "all shipped"); it is
# passed through unchanged, because "-" is what disables the generated binding.
auto_file="../${AUTO_FILE:-$pkg_rel/auto.go}"
if [ "${AUTO_FILE:-}" = "-" ]; then auto_file="-"; fi
# The scorecard is written twice: into the run directory, which is named after
# the run, and under the family's name into benchmarks/reports/scorecards,
# which is where `bakeoff adr` reads it from. Rendering an ADR is then a pure
# function of the scorecards of the families that share its number, repeatable
# without measuring again -- which is what an ADR shared by nine families
# needs. -profile and -methodology record how these numbers were produced,
# because §9.4 step 5 requires the ADR to state it -- the dimension grid above
# all, since that is which of §10.4's rungs the row was read at.
methodology="matrix=$matrix count=$count cpu=$cpus max_cpus=$max_cpus benchtime=$benchtime per_benchmark=${per_bench_ms}ms(max ${per_bench_max_ms}ms) timeout=${bench_timeout}s${cpuset:+ cpus=$cpuset} profile=$profile; kinds interleaved, warm-up discarded by go test, each run started only with no foreign benchmark and fewer than $runnable foreign runnable threads (median of three samples a second apart; the 1-minute load average recorded beside every run is of the whole host, this measurement included, and is not what the run waited for), contended runs discarded and repeated (hack/benchguard.sh), $taken of $expected samples taken, $contended contended attempt(s)"
if [ "$lost" -gt 0 ]; then
  methodology="$methodology; INCOMPLETE: $lost round(s) lost to contention (rounds $lost_rounds), recorded under IQMQ_BENCH_ALLOW_LOST=1"
fi
(cd benchmarks && GOWORK=off go run ./cmd/bakeoff decide -family "$family" -results "$out/raw.txt" -pkg "../$pkg_rel" \
   -bench "$BENCH" -ref "$REF" -disqualified "$disq" -auto-key "$AUTO_KEY" -idents "$IDENTS" \
   ${KIND_DIM:+-kind-dim "$KIND_DIM"} -sysinfo "benchmarks/reports/results/$run_id/sysinfo.json" \
   -raw "$out" -out-report ../docs/benchmarks/variants \
   -scorecards "$root/benchmarks/reports/scorecards" \
   -profile "$profile" -methodology "$methodology" \
   -auto-file "$auto_file" ${AUTO_PREFIX:+-auto-prefix "$AUTO_PREFIX"} \
   ${ADR:+-adr "$ADR"} ${TITLE:+-title "$TITLE"}) 9>&-
gzip -kf "$out/raw.txt"
echo "== $family: done -> $out"
