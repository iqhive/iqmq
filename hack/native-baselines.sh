#!/usr/bin/env bash
# Native-client baselines (the performance budgets layer 3, plan A21).
#
#   hack/native-baselines.sh [CLIENT...]
#
# Runs BenchmarkNative for each baseline in benchmarks/native-baselines
# against its own container (conformance/harness starts and stops it), one
# client at a time under the host benchmark lock, and writes benchstat files
# into benchmarks/reports/native/<stamp>/.
#
#   CLIENTS=""        clients to run (default: every registered baseline)
#   COUNT=5           repetitions per benchmark (benchstat needs >= 5 to be useful)
#   BENCHTIME=1s      go test -benchtime
#   GATE=0            skip the per-client correctness gate (TestNativeSession)
#   SMOKE=1           smoke mode: never blocked by load or foreign benchmarks, never a decision
#   LOADMAX=<n>       smoke-mode load threshold (default: hack/benchguard.sh headroom)
#   TIMEOUT=60m       per-client go test timeout
#   CPUS=16           GOMAXPROCS for the measured run (default IQMQ_BENCH_MAX_CPUS)
#
# Container benchmarks are never authoritative on a shared host: the guard
# verdict for every client is recorded next to its results, and a run marked
# contended must be repeated before anyone quotes it.
set -euo pipefail
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# The host CPU ceiling. This script invokes `go` itself, so it is capped when
# run by hand and not only when a make recipe exports the cap into it.
# hack/cpucap.sh carries the arithmetic; the eval is a
# no-op beyond re-deriving the same GOMAXPROCS and -p when make already set
# them, and it never RAISES either.
eval "$("$root/hack/cpucap.sh" env)"
mod="$root/benchmarks/native-baselines"

count="${COUNT:-5}"
benchtime="${BENCHTIME:-1s}"
timeout="${TIMEOUT:-60m}"
smoke="${SMOKE:-0}"
# Derived, not 32: one figure for the host, from the policy where there is one
# (is why a literal here goes stale without saying so).
smoke_loadmax="${LOADMAX:-$("$root/hack/benchguard.sh" headroom)}"
# -cpu, because this was one of the two benchmark paths with neither a taskset
# pin nor a -cpu flag: `go test -bench` with neither runs at GOMAXPROCS=nproc --
# every core the container is allowed, which on this host was 64 -- several
# times the ceiling
# hack/bakeoff.sh, hack/benchgate.sh and hack/bench/run-*.sh are all held to,
# in a script whose whole point is that a container benchmark shares the
# machine. The ceiling is the same one number as everywhere else.
max_cpus="${IQMQ_BENCH_MAX_CPUS:-${IQMQ_MAX_CPUS:-16}}"
cpus="${CPUS:-$max_cpus}"
# Derived, not a literal 4.0. A literal was defensible when the ceiling beside it
# was also one; it is not now that the guard asks the policy (24 here), because a
# strict ceiling six times tighter than the admission limit refuses runs the policy
# has already judged fine -- assumption C29.2 with the sign reversed.
loadmax="${IQMQ_BENCH_LOADMAX:-$("$root/hack/benchguard.sh" headroom)}"

clients=("$@")
if [ "${#clients[@]}" -eq 0 ] && [ -n "${CLIENTS:-}" ]; then
  read -r -a clients <<< "$CLIENTS"
fi
if [ "${#clients[@]}" -eq 0 ]; then
  # The registry is the source of truth for the client list.
  read -r -a clients <<< "$(cd "$mod" && GOWORK=off go test -run '^TestClientsList$' -v -count=1 ./nativebench/ \
    | sed -n 's/.*clients: *//p' | head -n1 | tr ',' '\n' | cut -d= -f1 | tr '\n' ' ')"
fi
[ "${#clients[@]}" -gt 0 ] || { echo "native-baselines: no clients" >&2; exit 2; }

stamp="$(date -u +%Y%m%dT%H%M%SZ)"
out="$root/benchmarks/reports/native/$stamp"
mkdir -p "$out"
echo "== native baselines: ${clients[*]}"
echo "== output: $out"
(cd "$root/benchmarks" && GOWORK=off go run ./cmd/sysinfo -o "$out/sysinfo.json") || true
(cd "$mod" && GOWORK=off go test -run '^TestClientsList$' -v -count=1 ./nativebench/ | sed -n 's/.*clients: */clients: /p') > "$out/clients.txt" || true
{
  echo "date=$stamp"
  echo "count=$count benchtime=$benchtime smoke=$smoke"
  echo "go=$(go version | awk '{print $3}') goarch=$(go env GOARCH)"
} > "$out/run.txt"

# Smoke runs record contention without waiting for a quiet host; decision
# runs hold the strict threshold and repeat a contended measurement.
guard=("$root/hack/benchguard.sh" run)
if [ "$smoke" = 1 ]; then
  guard+=(-s -t "$smoke_loadmax")
else
  guard+=(-t "$loadmax")
fi

status=0
: > "$out/all.txt"
for c in "${clients[@]}"; do
  echo "== $c"
  # Correctness gate first (the performance budgets: a disqualified implementation
  # is never measured). It round-trips single messages and full batches at
  # every payload size against the same container, and needs no benchmark
  # lock because it measures nothing.
  if [ "${GATE:-1}" = 1 ]; then
    if ! IQMQ_NATIVE_BASELINES=1 go test -C "$mod" -run '^TestNativeSession$' -count=1 \
         -timeout "$timeout" -v "./$c/" > "$out/$c-gate.log" 2>&1; then
      echo "   $c: correctness gate FAILED; not measured (see $out/$c-gate.log)" >&2
      grep -E "^(\s+)?--- FAIL|FAIL|.*\.go:[0-9]+:" "$out/$c-gate.log" | head -n 10 >&2
      status=1
      continue
    fi
    if grep -q "^--- SKIP\|^ok.*\[no tests to run\]" "$out/$c-gate.log"; then
      echo "   $c: correctness gate skipped (no Docker?)"
    fi
  fi
  for try in 1 2 3; do
    rc=0
    "${guard[@]}" -o "$out/guard-$c-$try.json" -- \
      env GOWORK=off go test -C "$mod" -run '^$' -bench "^BenchmarkNative/$c/" \
      -benchtime="$benchtime" -count="$count" -cpu="$cpus" -benchmem -timeout "$timeout" "./$c/" \
      > "$out/$c.log" 2>&1 || rc=$?
    if [ "$rc" -eq 3 ] && [ "$smoke" != 1 ]; then
      echo "   $c: contended (try $try); repeating"
      continue
    fi
    if [ "$rc" -eq 4 ]; then
      echo "native-baselines: another benchmark holds the host lock; try again later" >&2
      exit 4
    fi
    if [ "$rc" -ne 0 ] && [ "$rc" -ne 3 ]; then
      echo "   $c: FAILED (exit $rc); see $out/$c.log" >&2
      tail -n 20 "$out/$c.log" >&2
      status=1
    fi
    break
  done
  grep -E '^(goos|goarch|pkg|cpu|Benchmark)' "$out/$c.log" > "$out/$c.txt" || true
  grep '^Benchmark' "$out/$c.log" >> "$out/all.txt" || true
  tail -n 1 "$out/$c.log" | grep '^benchguard:' >> "$out/guards.txt" || true
  if [ -s "$out/$c.txt" ]; then
    "$root/hack/tool.sh" benchstat "$out/$c.txt" > "$out/benchstat-$c.txt" 2>/dev/null || true
  fi
done

if [ -s "$out/all.txt" ]; then
  "$root/hack/tool.sh" benchstat "$out/all.txt" > "$out/benchstat.txt" || status=1
  echo "== benchstat: $out/benchstat.txt"
else
  echo "native-baselines: no benchmark output" >&2
  status=1
fi
exit $status
