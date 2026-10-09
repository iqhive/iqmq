#!/usr/bin/env bash
# Measure the performance budgets's overhead budget locally: the driver arm
# (`BenchmarkDriver/<driver>/…`) and the raw-client arm
# (`BenchmarkNative/<client>/…`) against ONE broker at a time, interleaved
# inside ONE guarded window, and store the run.
#
#   hack/bench/run-local.sh [BROKER...]        # default: memory redis nats
#
# Environment:
#   ROUNDS     samples per arm per benchmark (default 10)
#   BENCHTIME  -benchtime per invocation (default 50ms)
#   CPUS       GOMAXPROCS and the CPU pin width (default IQMQ_BENCH_MAX_CPUS)
#   TRIES      attempts per broker when the guard calls a run contended (3)
#   SMOKE=1    guard in smoke mode and ROUNDS=1: a shape check, never a number
#
# ---------------------------------------------------------------------------
# Why it is shaped like this
#
# INTERLEAVED, IN ONE WINDOW. §10.2's budget is "core overhead vs raw client",
# which is a difference. The loop is already shared (benchmarks/opbench,
# assumption B11.4); what this adds is that the two arms are measured against
# the same container, in the same guarded window, alternating which goes first
# per round -- so a host that drifts during the measurement does not drift into
# one arm's favour. Two arms measured an hour apart on this host differ by more
# than the budget they are being held to.
#
# ONE BROKER AT A TIME. Pulsar, RabbitMQ and Artemis all take tens of seconds
# to become useful and several brokers at once make every number contended
# rather than comparable. Each broker gets its container started, its arms
# measured, its container removed BY ID, and then the next one starts.
#
# EVERY MEASUREMENT THROUGH hack/benchguard.sh, strict. A run the guard calls
# contended is discarded and repeated up to TRIES times; it is never read. That
# is the project rule and it is the only thing that makes a
# 3 % budget mean anything on a machine with fifty other containers on it.
#
# THE CORRECTNESS GATE FIRST. opbench.CheckSession round-trips one message and
# one full batch at every payload size, on both arms, before anything is
# timed -- and a broker that is not ready yet fails it, which is also how this
# script waits for readiness. §10.1 disqualifies an implementation that cannot
# do the work; measuring one that cannot is worse than not measuring.
#
# CONTAINERS ARE REMOVED BY ID, NEVER BY FILTER. About fifty containers that
# belong to other people run on this host. `docker ps` is recorded before and
# after every run and the two are diffed, so a container this script leaked --
# or removed and should not have -- is visible in the stored evidence.
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
# The host CPU ceiling. This script invokes `go` itself, so it is capped when
# run by hand and not only when a make recipe exports the cap into it.
# hack/cpucap.sh carries the arithmetic; the eval never
# RAISES either half.
eval "$("$root/hack/cpucap.sh" env)"

# The guarded half comes FIRST, because the guard re-enters this script and
# everything below it has side effects: the first version created its output
# directory before the `__measure` check, so every guarded round made a second,
# empty run directory (two of them, two seconds apart, is how it was noticed).

# arms NAME prints one "MODULE<TAB>PACKAGE<TAB>-bench PATTERN" line per arm, in
# the order they are measured. The driver arm and the raw-client arm of one
# broker are two packages in two modules; what makes them one experiment is
# that they run the same loop and are measured in the same window.
arms() {
  case "$1" in
    memory)
      printf 'benchmarks\t./overhead\t^BenchmarkDriver/memory/\n' ;;
    redis)
      printf 'benchmarks\t./overhead\t^BenchmarkDriver/redis/\n'
      printf 'benchmarks/native-baselines\t./rueidis\t^BenchmarkNative/rueidis/\n'
      printf 'benchmarks/native-baselines\t./goredis\t^BenchmarkNative/goredis/\n' ;;
    nats)
      printf 'benchmarks\t./overhead\t^BenchmarkDriver/nats/\n'
      printf 'benchmarks\t./overhead\t^BenchmarkDriver/jetstream/\n'
      printf 'benchmarks/native-baselines\t./nats\t^BenchmarkNative/nats/\n'
      printf 'benchmarks/native-baselines\t./jetstream\t^BenchmarkNative/jetstream/\n' ;;
    *) return 1 ;;
  esac
}

# measure NAME runs every arm of one broker, ROUNDS times, alternating the
# order between rounds. It is the command hack/benchguard.sh guards, so it must
# do nothing but measure.
if [ "${1:-}" = __measure ]; then
  shift
  name="$1" out="$2" rounds="$3" benchtime="$4" cpus="$5"
  mapfile -t arm_lines < <(arms "$name")
  started_at=$(date +%s)
  for r in $(seq 1 "$rounds"); do
    order=("${arm_lines[@]}")
    if [ $((r % 2)) -eq 0 ]; then
      # Reverse the order on even rounds.
      rev=(); for ((i=${#arm_lines[@]}-1; i>=0; i--)); do rev+=("${arm_lines[i]}"); done
      order=("${rev[@]}")
    fi
    for line in "${order[@]}"; do
      IFS=$'\t' read -r mod pkg pattern <<< "$line"
      # shellcheck disable=SC2086
      (cd "$root/$mod" && env GOWORK=off go test -run='^$' -bench "$pattern" \
          -benchtime="$benchtime" -count=1 -cpu="$cpus" -benchmem -timeout=20m "$pkg") \
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

# The per-benchmark ceiling is a project constraint, not a knob.
# One invocation is one benchtime at count=1, and
# the samples come from repeated ROUNDS rather than from a longer benchmark.
per_bench_max_ms="${IQMQ_BENCH_PER_BENCH_MAX_MS:-3000}"
case "$benchtime" in
  *ms) bt_ms="${benchtime%ms}" ;;
  *s)  bt_ms=$(( ${benchtime%s} * 1000 )) ;;
  *)   echo "run-local: BENCHTIME='$benchtime' must be in ms or s" >&2; exit 2 ;;
esac
if [ "$bt_ms" -gt "$per_bench_max_ms" ]; then
  echo "run-local: one benchmark would run for ${bt_ms}ms, over the ${per_bench_max_ms}ms ceiling" >&2
  echo "  lower BENCHTIME; do not raise IQMQ_BENCH_PER_BENCH_MAX_MS for a measurement" >&2
  exit 2
fi
if [ "$cpus" -gt "$max_cpus" ]; then
  echo "run-local: GOMAXPROCS $cpus exceeds the ${max_cpus}-CPU ceiling a measurement may take" >&2
  exit 2
fi

brokers=("$@")
[ "${#brokers[@]}" -gt 0 ] || brokers=(memory redis nats)

# The campaign ceiling the contributor notes names, spent rather than estimated: a
# measurement is a thing the machine does instead of everything else, and
# "no campaign that runs for hours" is a project constraint and not a knob.
# Checked after every broker, so an over-long
# campaign stops with the brokers it has rather than being estimated away in
# advance -- which is how the per-benchmark ceiling is enforced too.
total_max_s="${IQMQ_BENCH_TOTAL_MAX_S:-2400}"
campaign_started=$(date +%s)

stamp="$(date -u +%Y%m%dT%H%M%SZ)"
out="$root/benchmarks/reports/results/$stamp-overhead"
mkdir -p "$out"
[ "${SMOKE:-0}" = 1 ] && printf '*\n' > "$out/.gitignore"
echo "== run -> $out"

# ------------------------------------------------------------------ containers
# image looks a name up in conformance/harness/images.lock, which pins every
# broker image to a DIGEST. A floating tag would make two runs of this script
# incomparable without saying so.
image() {
  local name="$1" line
  line="$(awk -v n="$name" '$1 == n {print $2}' "$lock_file")"
  [ -n "$line" ] || { echo "run-local: $lock_file pins no image for $name" >&2; return 1; }
  printf '%s\n' "$line"
}

# freeport asks the kernel for an unused port. The host already runs other
# people's brokers -- there is a foreign nats among them -- so a fixed port
# would either clash or, worse, quietly measure somebody else's server.
freeport() {
  python3 - <<'PY'
import socket
s = socket.socket()
s.bind(("127.0.0.1", 0))
print(s.getsockname()[1])
s.close()
PY
}

started=()   # container ids this script created, for removal by id
# Where the overhead gate's binary is built. It is built rather than `go run`
# so its exit status survives; see the overhead gate below.
gatebin="$(mktemp -d)"
cleanup() {
  local id
  for id in ${started+"${started[@]}"}; do
    echo "== removing the container this run started: $id"
    docker rm -f "$id" >/dev/null 2>&1 || true
  done
  started=()
  rm -rf "$gatebin"
}
trap cleanup EXIT INT TERM

# start_broker NAME -> sets $addr and $addr_env, appends to $started
start_broker() {
  local name="$1" img port id
  case "$name" in
    memory) addr=""; addr_env=""; broker_image="in-process"; return 0 ;;
    redis)
      img="$(image redis)"; port="$(freeport)"
      # `--appendonly yes` is not a convenience: WAITAOF is the Redis driver's
      # ConfirmDurable and its condition is an append-only file. The measured
      # boundary here is ConfirmAccepted (the XADD reply), which is what the
      # rueidis and goredis baselines document, but a server without an AOF
      # advertises a different capability table and would be a different
      # broker.
      id="$(docker run -d -p "127.0.0.1:$port:6379" "$img" redis-server --appendonly yes)"
      addr="127.0.0.1:$port"; addr_env=IQMQ_REDIS_ADDR ;;
    nats)
      img="$(image nats)"; port="$(freeport)"
      id="$(docker run -d -p "127.0.0.1:$port:4222" "$img" -js)"
      addr="127.0.0.1:$port"; addr_env=IQMQ_NATS_ADDR ;;
    *) echo "run-local: no broker recipe for $name" >&2; return 1 ;;
  esac
  started+=("$id")
  broker_image="$img"
  echo "== $name: container ${id:0:12} on $addr ($img)"
  # TCP readiness, then the correctness gate decides whether it is really
  # usable. A 60-second ceiling: a broker that is not listening by then is a
  # failure to report, not something to wait out.
  local i
  for i in $(seq 1 120); do
    if (exec 3<>"/dev/tcp/127.0.0.1/$port") 2>/dev/null; then exec 3>&- 2>/dev/null || true; return 0; fi
    sleep 0.5
  done
  echo "!! $name: nothing accepted a connection on $addr within 60s" >&2
  return 1
}

# ------------------------------------------------------------------ the arms
# gate NAME runs the correctness gate for every arm of a broker. A session that
# cannot round-trip is not measured (§10.1).
gate() {
  local name="$1" mod pkg pattern test rc=0
  while IFS=$'\t' read -r mod pkg pattern; do
    [ -n "$mod" ] || continue
    case "$mod" in
      benchmarks) test='^TestTheInProcessProfileRoundTrips$|^TestEveryProfileNamesABaselineThatExists$' ;;
      *)          test='^TestNativeSession$' ;;
    esac
    echo "== $name: correctness gate $mod $pkg"
    if ! (cd "$root/$mod" && GOWORK=off IQMQ_NATIVE_BASELINES=1 go test -count=1 -run "$test" "$pkg") \
        >> "$out/gate-$name.log" 2>&1; then
      rc=1
    fi
  done < <(arms "$name")
  return "$rc"
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
docker ps --format '{{.ID}} {{.Image}} {{.Names}}' | sort > "$out/docker-before.txt"

# Asked of the guard, not computed here: this expression used to be copied into
# four scripts, and `nproc` reports CPU AFFINITY rather than the size of the
# machine, so on a host whose mask had been narrowed to the measurement's own
# width every copy quietly evaluated to 1.
headroom="$("$root/hack/benchguard.sh" headroom)"
: > "$out/raw.txt"
: > "$out/guards.txt"
measured=() unmeasured=()

for name in "${brokers[@]}"; do
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
    sw=0
    swout="$("$root/hack/benchguard.sh" swamped "$out/guard-$name-$try.json" "${IQMQ_BENCH_RUNNABLE_MAX:-}" 2>&1)" || sw=$?
    echo "$swout" | sed 's/^/   /'
    echo "$swout" | sed "s/^/$name try=$try /" >> "$out/guards.txt"
    if [ "$sw" -ne 0 ]; then
      echo "!! $name: the run was SWAMPED or undecidable -- $swout" >&2
      echo "   The guard called it clean because no foreign BENCHMARK ran, which is its question;" >&2
      echo "   a measurement's question is stricter. Discarding it (try $try of $tries)." >&2
      verdict=swamped
      continue
    fi
    rows="$(grep -c '^Benchmark' "$out/round-$name.txt" || true)"
    if [ "${rows:-0}" -eq 0 ]; then
      echo "!! $name: the run measured no benchmark -- the pattern selected nothing" >&2
      verdict=selected-nothing; break
    fi
    grep '^Benchmark' "$out/round-$name.txt" >> "$out/raw.txt"
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
  if [ "$spent" -ge "$total_max_s" ] && [ "$name" != "${brokers[-1]}" ]; then
    echo "!! run-local: the campaign has spent ${spent}s of its ${total_max_s}s ceiling; stopping here" >&2
    echo "   Raise IQMQ_BENCH_TOTAL_MAX_S deliberately, or run the remaining brokers separately." >&2
    for rest in "${brokers[@]}"; do
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
  echo "!! run-local: the container list changed across this run; see $out/docker-diff.txt" >&2
  cat "$out/docker-diff.txt" >&2
fi

printf '{"profile":"%s","rounds":%s,"benchtime":"%s","gomaxprocs":"%s","max_cpus":%s,"headroom":%s,"per_benchmark_ms":%s,"per_benchmark_max_ms":%s,"cpuset":"%s","pinned":%s,"brokers":"%s","measured":"%s","unmeasured":"%s","images_lock":"%s"}\n' \
  "$([ "${SMOKE:-0}" = 1 ] && echo smoke || echo full)" "$rounds" "$benchtime" "$cpus" "$max_cpus" "$headroom" \
  "$bt_ms" "$per_bench_max_ms" "$cpuset" "$([ -n "$cpuset" ] && echo true || echo false)" \
  "${brokers[*]}" "${measured[*]-}" "${unmeasured[*]-}" "$lock_file" > "$out/method.json"

gzip -kf "$out/raw.txt"
echo "== done -> $out ($(grep -c '^Benchmark' "$out/raw.txt" || true) rows)"
echo "== measured:   ${measured[*]-none}"
echo "== unmeasured: ${unmeasured[*]-none}"
[ "${#measured[@]}" -gt 0 ] || { echo "!! run-local: nothing was measured" >&2; exit 1; }

# §10.2's verdict, from the run that was just taken. The measurement and the
# judgement are one step because they are the same thing said twice otherwise:
# this file is the ONLY input a `kind: ratio` budget can be evaluated over --
# both arms, one container, one guarded window -- and a campaign that stored it
# without judging it is how the first two budgets came to be measured,
# published and ungated. A breach is REPORTED here and does
# not fail the campaign: the numbers are the campaign's product, and §10.2
# being unmet is a finding about the implementation rather than a reason to
# discard a measurement.
echo "== overhead gate (§10.2's kind: ratio budgets over this run):"
rg=0
# BUILT and then run, so the `case` below can actually reach its 3. `go run`
# exits 1 for any non-zero status the program returns, printing the real one on
# stderr as "exit status N" -- so the 3 arm was dead code and every no-verdict
# run took the 1 arm, which announces that §10.2's budgets "are BREACHED by this
# run". That is the opposite of what a 3 means: 3 is the gate declining to
# decide. Same defect as hack/benchgate.sh's.
(cd "$root/benchmarks" && GOWORK=off go build -o "$gatebin/benchgate" ./cmd/benchgate)
(cd "$root/benchmarks" && "$gatebin/benchgate" -ratios \
   -gate "${IQMQ_OVERHEAD_GATE:-overhead}" -budgets budgets.yaml \
   -run "$out/raw.txt" -out "$out/ratio-verdict.md") || rg=$?
case "$rg" in
  0) echo "== overhead gate: PASS" ;;
  1) echo "!! overhead gate: §10.2's overhead budgets are BREACHED by this run; see $out/ratio-verdict.md" >&2 ;;
  3) echo "!! overhead gate: NO VERDICT -- either a declared ratio row has no numerator or no denominator in" >&2
     echo "   this run (assumption A25.9), or a row was measured and its ceiling lies inside the combined" >&2
     echo "   confidence interval of its two medians (assumption C01.2). Neither is a pass; the verdict" >&2
     echo "   table says which rows and why: see $out/ratio-verdict.md" >&2
     echo "   An UNDECIDED row needs a quieter host or more samples per arm; an INCONCLUSIVE one needs a" >&2
     echo "   run that measures both arms." >&2 ;;
  *) echo "!! overhead gate: could not be evaluated (exit $rg)" >&2 ;;
esac
