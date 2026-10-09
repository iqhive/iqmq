#!/usr/bin/env bash
# Fuzz helpers.
#   hack/fuzz.sh smoke PKG TIME      run every Fuzz target of PKG for TIME each
#   hack/fuzz.sh all TIME [JOBS]     run every Fuzz target of every module for TIME each, JOBS in parallel
#   hack/fuzz.sh seeds               run every Fuzz target's seed corpus as tests (-race)
#   hack/fuzz.sh shard N I TIME      run shard I of N for TIME each
#   hack/fuzz.sh targets [N] [I]     print the targets shard I of N would run
#                                    ("DIR PKG TARGET" per line; all of them
#                                    with no arguments)
#   hack/fuzz.sh nightly [TIME] [SHARDS] [SHARD]
#                                    the test suite's nightly campaign:
#                                    TIME per target (default 10m) over SHARDS
#                                    shards run side by side, then the corpus
#                                    export and the fuzz manifest
#   hack/fuzz.sh race TIME           every target under -race for TIME each
#   hack/fuzz.sh race-shard N I TIME shard I of N under -race for TIME each
#
# FUZZ_WORKERS is -parallel for one target (default 8; nightly sets 2, so that
# eight shards ask for sixteen fuzz workers rather than sixty-four -- this is a
# shared host and the contributor notes's host etiquette applies to a nightly too).
#
# FUZZ_RACE=1 adds -race. `race` used to be a loop of its own that called `go
# test` directly, so it recorded NO run result -- the weekly job's `make
# fuzz-manifest` therefore merged nothing and wrote a manifest with the target
# list, the corpus hashes and not one duration, which is the shape assumption
# A25.2 was about. It goes through run_one like every other mode now, so a race
# campaign is a manifest like any other.
#
# A -race run also sets GORACE=log_path, so the race REPORT survives the run:
# see run_one. FUZZ_RACE_LOG_BYTES caps how much of each report is echoed into
# the job log (default 20000); the whole file is kept under
# benchmarks/reports/fuzz/race-logs/ for the artefact.
set -euo pipefail
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
mode="${1:-}"; shift || true
results_dir="$root/benchmarks/reports/fuzz"
mkdir -p "$results_dir"
stamp="$(date -u +%Y%m%dT%H%M%SZ)"
# One results file per invocation. FUZZ_RESULTS_SUFFIX distinguishes shards
# that run on separate machines: eight runners starting inside the same second
# would otherwise all write `<stamp>.jsonl`, and merging their artefacts into
# one directory would keep one shard's results and drop seven -- which
# `fuzzmanifest -require-runs` would then report as seven dead shards.
results_file="$results_dir/$stamp${FUZZ_RESULTS_SUFFIX:-}.jsonl"

targets_of() { # dir pkg
  (cd "$1" && GOWORK=off go test -list '^Fuzz' "$2" 2>/dev/null | grep '^Fuzz' || true)
}

# every_target prints "DIR PKG TARGET" for every fuzz target of every module,
# in a stable order. It is the ONE enumeration the campaign uses: `shard`,
# `race-shard` and `targets` all read it, so `hack/fuzz.sh targets` really
# answers "what would the nightly run", and the eight shards of a run partition
# the same list rather than three similar ones.
every_target() {
  for m in $("$root/hack/modules.sh"); do
    for pkg in $(cd "$root/$m" && GOWORK=off go list ./... 2>/dev/null); do
      for t in $(targets_of "$root/$m" "$pkg"); do
        echo "$root/$m $pkg $t"
      done
    done
  done
}

# shard_list writes shard I of N of every_target to a file it prints. The
# partition is round robin over the stable order, so shard i's membership does
# not change when a target is added to another module.
shard_list() { # n i
  local n="$1" i="$2" list k=0
  list="$(mktemp)"
  while read -r dir pkg t; do
    if [ $((k % n)) -eq "$i" ]; then echo "$dir $pkg $t" >> "$list"; fi
    k=$((k+1))
  done < <(every_target)
  printf '%s\n' "$list"
}

run_list() { # listfile time
  local list="$1" time="$2" status=0
  while read -r dir pkg t; do run_one "$dir" "$pkg" "$t" "$time" || status=1; done < "$list"
  return $status
}

run_one() { # dir pkg target time
  local dir="$1" pkg="$2" target="$3" time="$4" start end result
  local race=() logdir="" f
  local envv=(GOWORK=off)
  if [ "${FUZZ_RACE:-0}" = 1 ]; then
    race=(-race)
    # KEEP THE RACE REPORT. `go test -fuzz` runs the fuzz function in WORKER
    # PROCESSES, and Go's fuzz coordinator does not relay a worker's stderr --
    # so a race a shard finds reached the CI log as nothing but
    #
    #   testing.go:1712: race detected during execution of test
    #
    # with no addresses, no goroutines and no stacks. That one line is the
    # whole reason four races in this repository sat undiagnosed: the shard
    # that found them threw the evidence away and the only way back was to
    # guess. `log_path` writes the full report to a file instead of to the
    # stderr nobody reads.
    #
    # log_path and NOT halt_on_error=1. Both still save a crasher (verified
    # 3 of 3 for halt_on_error on go1.27.1), but halt_on_error kills the worker
    # mid-report: the coordinator then reports "fuzzing process hung or
    # terminated unexpectedly: exit status 66" and the report text is lost
    # again -- which is the one thing this is for.
    logdir="$results_dir/race-logs/${pkg//\//_}.$target"
    rm -rf "$logdir"
    mkdir -p "$logdir"
    envv+=("GORACE=log_path=$logdir/race")
  fi
  start=$(date +%s)
  if (cd "$dir" && env "${envv[@]}" go test "${race[@]}" -run='^$' -fuzz="^${target}\$" -fuzztime="$time" -parallel "${FUZZ_WORKERS:-8}" "$pkg"); then
    result=ok
  else
    result=crash
  fi
  end=$(date +%s)
  # Into the shard's own log as well as the artefact. An artefact is a download
  # and a click; the log is what the person reading the red job already has
  # open, and a race report is the one thing they came for.
  if [ -n "$logdir" ]; then
    for f in "$logdir"/race.*; do
      [ -f "$f" ] || continue
      echo "fuzz: race report for $pkg $target, from GORACE=log_path ($f):"
      head -c "${FUZZ_RACE_LOG_BYTES:-20000}" "$f"
      echo
    done
    # An empty directory per target would bury the reports that matter in the
    # artefact: log_path writes nothing at all when no race was detected.
    rmdir "$logdir" 2>/dev/null || true
  fi
  printf '{"target":"%s","package":"%s","duration_s":%d,"result":"%s","arch":"%s","go":"%s","date":"%s"}\n' \
    "$target" "$pkg" "$((end-start))" "$result" "$(go env GOARCH)" "$(go version | awk '{print $3}')" "$stamp" \
    >> "$results_file"
  [ "$result" = ok ]
}

case "$mode" in
  smoke)
    pkg="${1:?PKG required}"; time="${2:-20s}"
    status=0
    for t in $(targets_of "$root" "$pkg"); do
      run_one "$root" "$pkg" "$t" "$time" || status=1
    done
    exit $status ;;
  all)
    time="${1:-30s}"; jobs="${2:-8}"
    list="$(mktemp)"
    for m in $("$root/hack/modules.sh"); do
      for pkg in $(cd "$root/$m" && GOWORK=off go list ./... 2>/dev/null); do
        for t in $(targets_of "$root/$m" "$pkg"); do
          echo "$root/$m $pkg $t" >> "$list"
        done
      done
    done
    export -f run_one
    export results_dir stamp results_file
    if [ ! -s "$list" ]; then echo "no fuzz targets"; exit 0; fi
    xargs -P "$jobs" -L 1 bash -c 'run_one "$0" "$1" "$2" "'"$time"'"' < "$list" ;;
  seeds)
    "$root/hack/for-each-module.sh" --go go test -race -count=1 -run '^Fuzz' ./... ;;
  shard)
    n="${1:?shard count}"; i="${2:?shard index}"; time="${3:-10m}"
    list="$(shard_list "$n" "$i")"
    run_list "$list" "$time" ;;
  targets)
    if [ $# -ge 2 ] && [ "$2" != all ]; then
      cat "$(shard_list "$1" "$2")"
    else
      every_target
    fi ;;
  nightly)
    # §11.3: "nightly 10 min/target sharded". 195 targets at ten minutes is
    # thirty-two hours in one line -- the number was 84 when this comment was
    # written -- so the shards run side by side and the wall clock is
    # ceil(targets/SHARDS) * TIME, which at eight shards is four hours.
    time="${1:-10m}"; shards="${2:-8}"; only="${3:-all}"
    export FUZZ_WORKERS="${FUZZ_WORKERS:-2}"
    status=0
    if [ "$only" = all ]; then
      # Nothing this fan-out starts may outlive it. Eight shards each run
      # `go test -fuzz` for TIME per target, and on a shared host the campaign
      # runs under cpu-policy's machine-wide heavy-job lock -- which is released
      # when the LAST PROCESS OF THE JOB exits, not when this script does. So an
      # interrupted nightly used to leave eight fuzzing trees running and the
      # lock held against every other repository on the machine for as long as
      # they took to finish by themselves, which is hours.
      #
      # Killed by DESCENT, never by pattern, for the reason assumption A24.55
      # gives: a pattern that matches a fuzz worker also matches the shell that
      # typed it. `hack/benchguard.sh` carries the same helper for the same
      # reason; if one changes, change both.
      kill_tree() {
        local p="$1" sig="${2:-TERM}" kid
        for kid in $(ps -o pid= --ppid "$p" 2>/dev/null); do kill_tree "$kid" "$sig"; done
        kill -"$sig" "$p" 2>/dev/null || true
      }
      pids=()
      cleanup_shards() {
        trap - EXIT INT TERM HUP
        local p
        if [ "${#pids[@]}" -gt 0 ]; then
          for p in "${pids[@]}"; do [ -n "$p" ] && kill_tree "$p" TERM; done
        fi
        pids=()
      }
      trap 'rc=$?; cleanup_shards; exit "$rc"' EXIT
      trap 'echo "fuzz: signalled; killing the shards so nothing is left holding the heavy-job lock" >&2; cleanup_shards; exit 130' INT TERM HUP
      for i in $(seq 0 $((shards-1))); do
        FUZZ_RESULTS_SUFFIX="-shard$i" "$0" shard "$shards" "$i" "$time" & pids+=("$!")
      done
      for pid in "${pids[@]}"; do wait "$pid" || status=1; done
      # Cleared on the normal path: the shards are reaped, and the rest of this
      # mode (corpus export, manifest) must not be killed by an EXIT trap.
      trap - EXIT INT TERM HUP
    else
      FUZZ_RESULTS_SUFFIX="-shard$only" "$0" shard "$shards" "$only" "$time" || status=1
    fi
    # Save changed fuzz inputs before exporting the exploration corpus.
    (cd "$root" && git ls-files --others --modified --exclude-standard -- \
        '**/testdata/fuzz/**') > "$results_dir/$stamp.crashers" || status=1
    # The corpus the campaign found, then the manifest §11.3 attaches to a
    # release. export removes nothing, so the operator commits what is worth
    # committing; -require-runs is only meaningful over the whole target set.
    (cd "$root" && GOWORK=off go run ./tools/fuzzcorpus export) || status=1
    require=""
    [ "$only" = all ] && require="-require-runs"
    (cd "$root" && GOWORK=off go run ./tools/fuzzmanifest -results benchmarks/reports/fuzz \
        $require -o reports/fuzz-manifest.json) || status=1
    echo "fuzz: nightly $time/target over $shards shard(s), results in $results_dir, manifest in reports/fuzz-manifest.json"
    exit $status ;;
  race)
    # Every target under -race in ONE job. At 195 targets this is TIME x 195
    # plus a race build per package, which is why the weekly job runs
    # `race-shard` instead: the single-job form was cancelled at its 340-minute
    # timeout every week and reported nothing (run 35488657610).
    time="${1:-2m}"
    export FUZZ_RACE=1 FUZZ_WORKERS="${FUZZ_WORKERS:-4}"
    list="$(mktemp)"; every_target > "$list"
    run_list "$list" "$time" ;;
  race-shard)
    n="${1:?shard count}"; i="${2:?shard index}"; time="${3:-2m}"
    export FUZZ_RACE=1 FUZZ_WORKERS="${FUZZ_WORKERS:-4}"
    list="$(shard_list "$n" "$i")"
    run_list "$list" "$time" ;;
  *)
    echo "usage: $0 (smoke PKG TIME | all TIME [JOBS] | shard N I TIME | targets [N I] | nightly [TIME] [SHARDS] [SHARD] | race TIME | race-shard N I TIME | seeds)" >&2; exit 2 ;;
esac
