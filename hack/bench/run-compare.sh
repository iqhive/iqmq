#!/usr/bin/env bash
# Measure the performance budgets's layer-5 competitor comparison locally: every
# arm of one family -- iqmq, Watermill, gocloud and a raw client -- interleaved
# inside ONE guarded window, against ONE broker at a time, and store the run.
#
#   hack/bench/run-compare.sh [FAMILY...]       # default: memory nats
#
# Environment:
#   ROUNDS     samples per arm per benchmark (default 10)
#   BENCHTIME  -benchtime per invocation (default 50ms)
#   CPUS       GOMAXPROCS and the CPU pin width (default IQMQ_BENCH_MAX_CPUS)
#   TRIES      attempts per family when the guard calls a run contended (3)
#   SMOKE=1    guard in smoke mode and ROUNDS=1: a shape check, never a number
#
# ---------------------------------------------------------------------------
# Why it is shaped like this
#
# It is hack/bench/run-local.sh's shape, for hack/bench/run-local.sh's reasons,
# because a competitor comparison is the same KIND of measurement as §10.2's
# overhead ratio: a difference between arms, which is only a difference if the
# arms were measured under one set of conditions.
#
# INTERLEAVED, IN ONE WINDOW. Every arm of a family is measured against the
# same container in the same guarded window, with the arm order reversed on
# every other round, so a host that drifts during the measurement does not
# drift into one library's favour. Four arms measured an hour apart on this
# host differ by more than the effects being reported.
#
# ONE ARM PER INVOCATION, ALL ARMS IN ONE PACKAGE. The arms are one Go package
# (benchmarks/competitors), so interleaving is a `-bench` filter per arm rather
# than a package switch. That also means every arm is compiled by one `go
# test`, with one resolved module graph: the pinned versions the published page
# reports are the versions the run used, which
# `TestPinnedVersionsMatchGoMod` checks from the other side.
#
# THE CORRECTNESS GATE FIRST. `TestSessionsRoundTrip` round-trips one message
# and a full batch at every payload size through every arm before anything is
# timed -- and a broker that is not ready yet fails it, which is also how this
# script waits for readiness. §10.1 disqualifies an implementation that cannot
# do the work, and a competitor arm that silently dropped messages would
# produce the most flattering number in the table.
#
# EVERY MEASUREMENT THROUGH hack/benchguard.sh, strict, and a run the guard
# calls contended or that `benchguard.sh swamped` calls swamped is DISCARDED
# and repeated. It is never read (the contributor notes, assumptions B11.11,
# B11.13).
#
# CONTAINERS ARE REMOVED BY ID, NEVER BY FILTER. About fifty containers that
# belong to other people run on this host. `docker ps` is recorded before and
# after and the two are diffed into the stored evidence.
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
# The host CPU ceiling. This script invokes `go` itself, so it is capped when
# run by hand and not only when a make recipe exports the cap into it.
# hack/cpucap.sh carries the arithmetic; the eval never
# RAISES either half.
eval "$("$root/hack/cpucap.sh" env)"

# arms FAMILY prints one "-bench PATTERN" line per arm, in measurement order,
# from the file the outer phase wrote.
#
# It reads a FILE rather than running `go run ./cmd/compare-arms`, which is
# what the first version did: that compiles the competitors package and its
# dependency graph INSIDE the guarded window, so the first seconds of every
# round would be a build competing with the measurement. The patterns still
# come from the package's own declaration -- a new arm needs no edit here --
# they are just resolved before the guard takes the lock.
arms() {
  cat "$2/arms-$1.txt"
}

# The guarded half comes FIRST, because the guard re-enters this script and
# everything below it has side effects (run-local.sh created its output
# directory before this check and made an empty run directory per round).
if [ "${1:-}" = __measure ]; then
  shift
  name="$1" out="$2" rounds="$3" benchtime="$4" cpus="$5"
  mapfile -t arm_lines < <(arms "$name" "$out")
  started_at=$(date +%s)
  for r in $(seq 1 "$rounds"); do
    order=("${arm_lines[@]}")
    if [ $((r % 2)) -eq 0 ]; then
      rev=(); for ((i=${#arm_lines[@]}-1; i>=0; i--)); do rev+=("${arm_lines[i]}"); done
      order=("${rev[@]}")
    fi
    for pattern in "${order[@]}"; do
      # shellcheck disable=SC2086
      (cd "$root/benchmarks/competitors" && env GOWORK=off go test -run='^$' -bench "$pattern" \
          -benchtime="$benchtime" -count=1 -cpu="$cpus" -benchmem -timeout=20m .) \
        >> "$out/round-$name.txt" 2>&1
    done
    echo "== $name: round $r/$rounds done ($(( $(date +%s) - started_at ))s elapsed)"
  done
  exit 0
fi

lock_file="${IQMQ_BENCH_IMAGES_LOCK:-$root/conformance/harness/images.lock}"

rounds="${ROUNDS:-10}"
benchtime="${BENCHTIME:-50ms}"
max_cpus="${IQMQ_BENCH_MAX_CPUS:-${IQMQ_MAX_CPUS:-16}}"
cpus="${CPUS:-$max_cpus}"
tries="${TRIES:-3}"
smoke_flag=""
if [ "${SMOKE:-0}" = 1 ]; then
  rounds=1
  smoke_flag="-s"
fi

# The per-benchmark ceiling is a project constraint, not a knob (assumptions
# A24.90, A24.91). One invocation is one benchtime at count=1; samples come
# from repeated ROUNDS rather than from a longer benchmark.
per_bench_max_ms="${IQMQ_BENCH_PER_BENCH_MAX_MS:-3000}"
case "$benchtime" in
  *ms) bt_ms="${benchtime%ms}" ;;
  *s)  bt_ms=$(( ${benchtime%s} * 1000 )) ;;
  *)   echo "run-compare: BENCHTIME='$benchtime' must be in ms or s" >&2; exit 2 ;;
esac
if [ "$bt_ms" -gt "$per_bench_max_ms" ]; then
  echo "run-compare: one benchmark would run for ${bt_ms}ms, over the ${per_bench_max_ms}ms ceiling" >&2
  echo "  lower BENCHTIME; do not raise IQMQ_BENCH_PER_BENCH_MAX_MS for a measurement" >&2
  exit 2
fi
if [ "$cpus" -gt "$max_cpus" ]; then
  echo "run-compare: GOMAXPROCS $cpus exceeds the ${max_cpus}-CPU ceiling a measurement may take" >&2
  exit 2
fi

families=("$@")
[ "${#families[@]}" -gt 0 ] || families=(memory nats)

total_max_s="${IQMQ_BENCH_TOTAL_MAX_S:-2400}"
campaign_started=$(date +%s)

stamp="$(date -u +%Y%m%dT%H%M%SZ)"
out="$root/benchmarks/reports/results/$stamp-compare"
mkdir -p "$out"
[ "${SMOKE:-0}" = 1 ] && printf '*\n' > "$out/.gitignore"
echo "== run -> $out"

# ------------------------------------------------------------------ containers
image() {
  local name="$1" line
  line="$(awk -v n="$name" '$1 == n {print $2}' "$lock_file")"
  [ -n "$line" ] || { echo "run-compare: $lock_file pins no image for $name" >&2; return 1; }
  printf '%s\n' "$line"
}

# freeport asks the kernel for an unused port: this host already runs other
# people's brokers, a foreign nats among them, so a fixed port would either
# clash or quietly measure somebody else's server.
freeport() {
  python3 - <<'PY'
import socket
s = socket.socket()
s.bind(("127.0.0.1", 0))
print(s.getsockname()[1])
s.close()
PY
}

started=()
cleanup() {
  local id
  for id in ${started+"${started[@]}"}; do
    echo "== removing the container this run started: $id"
    docker rm -f "$id" >/dev/null 2>&1 || true
  done
  started=()
}
trap cleanup EXIT INT TERM

start_broker() {
  local name="$1" img port id
  case "$name" in
    memory) addr=""; addr_env=""; broker_image="in-process"; return 0 ;;
    nats)
      img="$(image nats)"; port="$(freeport)"
      id="$(docker run -d -p "127.0.0.1:$port:4222" "$img" -js)"
      addr="127.0.0.1:$port"; addr_env=IQMQ_NATS_ADDR ;;
    *) echo "run-compare: no broker recipe for $name" >&2; return 1 ;;
  esac
  started+=("$id")
  broker_image="$img"
  echo "== $name: container ${id:0:12} on $addr ($img)"
  local i
  for i in $(seq 1 120); do
    if (exec 3<>"/dev/tcp/127.0.0.1/$port") 2>/dev/null; then exec 3>&- 2>/dev/null || true; return 0; fi
    sleep 0.5
  done
  echo "!! $name: nothing accepted a connection on $addr within 60s" >&2
  return 1
}

# gate FAMILY runs the correctness gate for every arm of a family.
#
# It lives in hack/bench/compare-gate.sh rather than here so that it can be
# run, and therefore watched failing, without starting a campaign -- which is
# what the version that could not fail was never subjected to.
gate() {
  "$root/hack/bench/compare-gate.sh" "$1" "$out"
}

# ------------------------------------------------------------------ the driver
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

(cd "$root/benchmarks" && GOWORK=off go run ./cmd/sysinfo -o "$out/sysinfo.json")
# The arm list and the test binary are both resolved BEFORE the guard: a build
# inside the window is a build the measurement competes with.
for name in "${families[@]}"; do
  (cd "$root/benchmarks" && GOWORK=off go run ./cmd/compare-arms "$name") > "$out/arms-$name.txt"
done
(cd "$root/benchmarks/competitors" && GOWORK=off go test -c -o /dev/null .)
(cd "$root/benchmarks/competitors" && GOWORK=off go list -m all) > "$out/modules.txt" 2>&1 || true
docker ps --format '{{.ID}} {{.Image}} {{.Names}}' | sort > "$out/docker-before.txt"

# Asked of the guard, not computed here: this expression used to be copied into
# four scripts, and `nproc` reports CPU AFFINITY rather than the size of the
# machine, so on a host whose mask had been narrowed to the measurement's own
# width every copy quietly evaluated to 1.
headroom="$("$root/hack/benchguard.sh" headroom)"
: > "$out/raw.txt"
: > "$out/guards.txt"
measured=() unmeasured=()

for name in "${families[@]}"; do
  echo "== $name: starting"
  addr=""; addr_env=""; broker_image=""
  if ! start_broker "$name"; then
    unmeasured+=("$name=broker-did-not-start")
    cleanup
    continue
  fi
  [ -n "$addr_env" ] && export "$addr_env=$addr"
  export "IQMQ_BENCH_${name}_IMAGE=$broker_image"

  : > "$out/gate-$name.log"
  ok=0
  for attempt in 1 2 3 4 5 6; do
    if gate "$name"; then ok=1; break; fi
    echo "== $name: not ready yet (gate attempt $attempt)"
    sleep 5
  done
  if [ "$ok" -ne 1 ]; then
    echo "!! $name: the correctness gate never passed; refusing to measure it" >&2
    tail -20 "$out/gate-$name.log" | sed 's/^/     /' >&2
    unmeasured+=("$name=correctness-gate-failed")
    cleanup
    continue
  fi

  verdict=""
  for try in $(seq 1 "$tries"); do
    : > "$out/round-$name.txt"
    rc=0
    # shellcheck disable=SC2086
    IQMQ_BENCH_OWN_LOAD=$(( cpus * 2 )) "$root/hack/benchguard.sh" run $smoke_flag \
      -t "$headroom" -r "$headroom" -w "${IQMQ_BENCH_MAXWAIT:-600}" \
      -o "$out/guard-$name-$try.json" -- \
      "${BASH_SOURCE[0]}" __measure "$name" "$out" "$rounds" "$benchtime" "$cpus" \
      > "$out/measure-$name-$try.log" 2>&1 || rc=$?
    grep '^benchguard:' "$out/measure-$name-$try.log" | sed 's/^/   /' || true
    grep '^benchguard:' "$out/measure-$name-$try.log" | sed "s/^/$name try=$try /" >> "$out/guards.txt" || true
    case "$rc" in
      0) verdict=clean ;;
      3) echo "== $name: the run was CONTENDED (try $try of $tries); discarding it and repeating" >&2
         verdict=contended; continue ;;
      5) echo "!! $name: the guard REFUSED to start; nothing ran" >&2
         verdict=refused; break ;;
      *) echo "!! $name: the measurement failed with status $rc" >&2
         tail -20 "$out/measure-$name-$try.log" | sed 's/^/     /' >&2
         verdict="failed-$rc"; break ;;
    esac
    # A run may be `clean` and still be SWAMPED: the guard's verdict answers
    # "did a foreign benchmark run", and a measurement asks one more thing.
    # `benchguard.sh swamped` is the one implementation of that rule, applied
    # to the p95 of the window's samples.
    sw=0
    swout="$("$root/hack/benchguard.sh" swamped "$out/guard-$name-$try.json" "${IQMQ_BENCH_RUNNABLE_MAX:-}" 2>&1)" || sw=$?
    echo "$swout" | sed 's/^/   /'
    echo "$swout" | sed "s/^/$name try=$try /" >> "$out/guards.txt"
    if [ "$sw" -ne 0 ]; then
      echo "!! $name: the run was SWAMPED or undecidable -- $swout" >&2
      echo "   Discarding it (try $try of $tries)." >&2
      verdict=swamped
      continue
    fi
    rows="$(grep -c '^BenchmarkCompare' "$out/round-$name.txt" || true)"
    if [ "${rows:-0}" -eq 0 ]; then
      echo "!! $name: the run measured no benchmark -- the pattern selected nothing" >&2
      verdict=selected-nothing; break
    fi
    grep '^BenchmarkCompare' "$out/round-$name.txt" >> "$out/raw.txt"
    cp "$out/round-$name.txt" "$out/arm-$name.txt"
    echo "== $name: $rows benchmark row(s), verdict=clean"
    measured+=("$name")
    break
  done
  if [ "$verdict" != clean ]; then
    unmeasured+=("$name=$verdict")
  fi
  cleanup

  spent=$(( $(date +%s) - campaign_started ))
  if [ "$spent" -ge "$total_max_s" ] && [ "$name" != "${families[-1]}" ]; then
    echo "!! run-compare: the campaign has spent ${spent}s of its ${total_max_s}s ceiling; stopping here" >&2
    echo "   Raise IQMQ_BENCH_TOTAL_MAX_S deliberately, or run the remaining families separately." >&2
    for rest in "${families[@]}"; do
      case " ${measured[*]-} ${unmeasured[*]-} " in
        *" $rest"*) ;;
        *) unmeasured+=("$rest=campaign-ceiling-reached") ;;
      esac
    done
    break
  fi
done

docker ps --format '{{.ID}} {{.Image}} {{.Names}}' | sort > "$out/docker-after.txt"
if ! diff -u "$out/docker-before.txt" "$out/docker-after.txt" > "$out/docker-diff.txt"; then
  echo "!! run-compare: the container list changed across this run; see $out/docker-diff.txt" >&2
  cat "$out/docker-diff.txt" >&2
fi

printf '{"profile":"%s","rounds":%s,"benchtime":"%s","gomaxprocs":"%s","max_cpus":%s,"headroom":%s,"per_benchmark_ms":%s,"per_benchmark_max_ms":%s,"cpuset":"%s","pinned":%s,"families":"%s","measured":"%s","unmeasured":"%s","images_lock":"%s"}\n' \
  "$([ "${SMOKE:-0}" = 1 ] && echo smoke || echo full)" "$rounds" "$benchtime" "$cpus" "$max_cpus" "$headroom" \
  "$bt_ms" "$per_bench_max_ms" "$cpuset" "$([ -n "$cpuset" ] && echo true || echo false)" \
  "${families[*]}" "${measured[*]-}" "${unmeasured[*]-}" "$lock_file" > "$out/method.json"

gzip -kf "$out/raw.txt"
echo "== done -> $out ($(grep -c '^BenchmarkCompare' "$out/raw.txt" || true) rows)"
echo "== measured:   ${measured[*]-none}"
echo "== unmeasured: ${unmeasured[*]-none}"
[ "${#measured[@]}" -gt 0 ] || { echo "!! run-compare: nothing was measured" >&2; exit 1; }

# The table, from the run that was just taken. The measurement and the
# rendering are one step for the reason the overhead campaign's verdict is:
# this file is the only input the renderer can read -- every arm, one
# container, one guarded window -- and a campaign that stored it without
# rendering it is a campaign whose numbers get retyped.
echo "== rendering the comparison:"
(cd "$root/benchmarks" && GOWORK=off go run ./cmd/compare -results "$out/raw.txt" > "$out/table.md") \
  && echo "== table -> $out/table.md" \
  || echo "!! run-compare: the table could not be rendered" >&2
