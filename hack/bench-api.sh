#!/usr/bin/env bash
# Measure the frozen public API's own benchmarks and store the run.
#
#   hack/bench-api.sh [full|quick] [PACKAGE] [ID]
#
# The root package holds the benchmarks §10.2's budgets are written about --
# `BenchmarkHeaders`, the option call sites, the publish and receive paths --
# and it is not a bake-off family: it has no variant kinds, so it has no
# `hack/bakeoff/<id>.env` and `hack/bakeoff.sh` cannot measure it. Two things
# needed a stored run of it anyway: `make gate` measures the packages
# `bakeoff gate-pattern -packages` derives, which now includes the
# `gate_packages` list from budgets.yaml, and `benchgate -lint` checks every
# budget pattern against the names of stored runs -- so without this, a budget
# naming a root row would be reported as matching nothing, which is how three
# budgets and five §8.10 targets came to match nothing for four increments.
#
# It writes the same shape a family's run does, under the run id
# `<timestamp>-root`, so the lint and the gate need no second source.
set -euo pipefail
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# The host CPU ceiling. This script invokes `go` itself, so it is capped when
# run by hand and not only when a make recipe exports the cap into it.
# hack/cpucap.sh carries the arithmetic; the eval is a
# no-op beyond re-deriving the same GOMAXPROCS and -p when make already set
# them, and it never RAISES either.
eval "$("$root/hack/cpucap.sh" env)"
profile="${1:-full}"
pkg="${2:-.}"
id="${3:-root}"
case "$profile" in full|quick) ;; *) echo "usage: $0 [full|quick] [PACKAGE] [ID]" >&2; exit 2 ;; esac
case "$id" in *[!A-Za-z0-9_-]*|"") echo "$0: ID must be a plain identifier" >&2; exit 2 ;; esac

# The module PKG is in, and its path inside it, from the one implementation of
# the rule (`bakeoff modpkg`). This script assumed the root module, so a
# package in a nested module could not have a run stored at all -- and a budget
# pattern naming one of its rows would then be reported by `benchgate -lint` as
# matching nothing, which is the failure mode that check exists for.
modpkg="$(cd "$root/benchmarks" && GOWORK=off go run ./cmd/bakeoff modpkg -root .. "$pkg")" || {
  echo "$0: $pkg is in no Go module in this repository" >&2; exit 2; }
mod="${modpkg%%	*}"
pkg_in_mod="${modpkg#*	}"
moddir="$root/${mod#.}"

max_cpus="${IQMQ_BENCH_MAX_CPUS:-${IQMQ_MAX_CPUS:-16}}"
# Asked of the guard, not computed here: this expression used to be copied into
# four scripts, and `nproc` reports CPU AFFINITY rather than the size of the
# machine, so on a host whose mask had been narrowed to the measurement's own
# width every copy quietly evaluated to 1.
headroom="$("$root/hack/benchguard.sh" headroom)"
case "$profile" in
  full)  count=10; benchtime="${IQMQ_BENCH_TIME:-50ms}"; smoke="" ;;
  quick) count=3;  benchtime="${IQMQ_BENCH_TIME:-50ms}"; smoke="-s" ;;
esac
cpus="${IQMQ_BENCH_CPUS:-$max_cpus}"

run_id="$(date -u +%Y%m%dT%H%M%SZ)-$id"
out="$root/benchmarks/reports/results/$run_id"; mkdir -p "$out"
[ "$profile" = full ] || printf '*\n' > "$out/.gitignore"
cd "$moddir"

echo "== $id: correctness gate ($pkg, module $mod)"
GOWORK=off go test -race -count=1 -json "$pkg_in_mod" > "$out/gate.jsonl" || {
  echo "   $pkg's own tests fail; refusing to measure" >&2
  grep -o '"Action":"fail","Package":"[^"]*","Test":"[^"]*' "$out/gate.jsonl" | sed 's/.*"Test":"/     /' | sort -u >&2
  exit 1
}

(cd "$root/benchmarks" && GOWORK=off go run ./cmd/sysinfo -o "$out/sysinfo.json")

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

n="$(GOWORK=off go test -run='^$' -bench=. -benchtime=1x -count=1 -cpu=1 "$pkg_in_mod" 2>&1 | grep -c '^Benchmark' || true)"
[ "$n" -gt 0 ] || { echo "$pkg has no benchmarks" >&2; exit 1; }
per_bench_ms=$(( count * 1 * ${benchtime%ms} ))
per_bench_max_ms="${IQMQ_BENCH_PER_BENCH_MAX_MS:-3000}"
[ "$per_bench_ms" -le "$per_bench_max_ms" ] || {
  echo "$id: one benchmark would take ${per_bench_ms}ms, above the ${per_bench_max_ms}ms ceiling" >&2; exit 2; }
echo "== $id: $n benchmark(s), ${per_bench_ms}ms each, width $cpus${cpuset:+, pinned by cpu-policy to $cpuset}"

printf '{"profile":"%s","matrix":"n/a","count":%s,"gomaxprocs":"%s","benchtime":"%s","max_cpus":%s,"headroom":%s,"per_benchmark_ms":%s,"per_benchmark_max_ms":%s,"cpuset":"%s","loadmax":"%s","runnable_max":"%s","pinned":%s,"benchmarks_per_run":%s}\n' \
  "$profile" "$count" "$cpus" "$benchtime" "$max_cpus" "$headroom" "$per_bench_ms" "$per_bench_max_ms" \
  "$cpuset" "$headroom" "$headroom" "$([ -n "$cpuset" ] && echo true || echo false)" "$n" > "$out/method.json"

: > "$out/raw.txt"
for i in $(seq 1 "$count"); do
  tmp="$(mktemp)"
  # shellcheck disable=SC2086
  IQMQ_BENCH_OWN_LOAD=$(( cpus * 2 )) "$root/hack/benchguard.sh" run $smoke -t "$headroom" -r "$headroom" \
    -w "${IQMQ_BENCH_MAXWAIT:-600}" -o "$out/guard-$i.json" -- \
    env GOWORK=off go test -run='^$' -bench=. -benchtime="$benchtime" -count=1 -cpu="$cpus" -benchmem "$pkg_in_mod" > "$tmp" 2>&1 && rc=0 || rc=$?
  grep '^benchguard:' "$tmp" | sed 's/^/   /' || true
  if [ "$rc" -ne 0 ]; then
    { echo "   round $i failed (exit $rc):"; tail -20 "$tmp" | sed 's/^/     /'; } >&2
    cp "$tmp" "$out/failed-$i.log"; rm -f "$tmp"; exit 1
  fi
  grep '^Benchmark' "$tmp" >> "$out/raw.txt" || true
  rm -f "$tmp"
done
gzip -kf "$out/raw.txt"
echo "== $id: done -> $out ($(grep -c '^Benchmark' "$out/raw.txt") rows)"
