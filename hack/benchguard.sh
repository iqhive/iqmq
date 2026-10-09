#!/usr/bin/env bash
# Load-guarded, host-serialised benchmark runner for a shared development
# server. Other workloads (and other agents) run on this machine, so a
# measurement starts only when nothing foreign is running, and records what
# else the host was doing before, during and after it: the load average, the
# number of runnable threads that are not ours, and any foreign benchmark
# process that started before or during the run.
#
#   hack/benchguard.sh run [-s] [-t LOADMAX] [-r RUNNABLE_MAX] [-w MAXWAIT_S] [-o REPORT] -- CMD [ARGS...]
#       Take the host-wide benchmark lock, wait (up to MAXWAIT_S, default 900)
#       until nothing FOREIGN is running -- no foreign benchmark process and
#       fewer than RUNNABLE_MAX foreign runnable threads (default the policy's
#       bench load limit, else nproc - IQMQ_BENCH_MAX_CPUS; per
#       IQMQ_BENCH_RUNNABLE_MAX) -- run CMD, sample load, foreign benchmark
#       processes and foreign runnable threads every 5 s, and print one summary
#       line:
#         benchguard: verdict=clean|contended reason=… load_before=… load_after=… load_max=… foreign=N foreign_runnable_before=N foreign_runnable_max=N foreign_runnable_p95=N swamped=no …
#       The wait does NOT key on the 1-minute load average: that average is a
#       decaying average of everything, so it cannot tell our own predecessor's
#       exit from a neighbour's build, and at the campaign's settings each run
#       spent 40-60 s waiting for its own decay with `clean foreign=0` every
#       time. LOADMAX still feeds the verdict's backstop
#       half  and is still what `check` compares.
#       Exit status, and the three meanings a caller must tell apart:
#         0  clean (or smoke mode): the numbers may be used
#         3  the command RAN and the run was contended -- discard and repeat
#         5  the guard REFUSED to start: the host never went quiet within
#            MAXWAIT_S, so the command never ran and there is nothing to
#            discard. Repeating it immediately buys another MAXWAIT_S wait for
#            the same answer; the caller should stop rather than retry.
#         CMD's own status when CMD failed (which wins over 3 and 5).
#       -s is smoke mode for runs that never feed a decision: foreign
#       benchmarks neither delay the start nor fail the run, but the verdict
#       is still recorded so the log shows what else was running.
#   hack/benchguard.sh ownership ME PGID < process-table
#       Test hook: print which rows of a `ps -eo pid,ppid,pgid,args` table the
#       guard would call foreign benchmarks. See hack/benchguard_test.sh.
#   hack/benchguard.sh runnable ME PGID TOTAL_RUNNABLE < process-table
#       Test hook: the same ownership rule applied to the other question --
#       how many of the host's TOTAL_RUNNABLE runnable threads are not ours,
#       given a `ps -eLo pid,ppid,pgid,state,args` table (one row per thread).
#   hack/benchguard.sh percentile P < series
#       Test hook: the nearest-rank P-th percentile of a whitespace-separated
#       series. It is the statistic the SWAMPED rule decides on.
#   hack/benchguard.sh swamped REPORT [CEILING]
#       The SWAMPED decision for a stored guard report -- the one
#       implementation of it, called by hack/benchgate.sh and
#       hack/bench/run-local.sh. Exit 0 not swamped, 3 swamped, 2 the report
#       cannot decide. A run may be `clean` (no foreign benchmark ran) and
#       still be swamped, which is the whole reason the check exists.
#   hack/benchguard.sh swamped-audit DIR [CEILING]
#       What the rule would have said for every stored guard report under DIR,
#       as TSV. The evidence assumption B11.13 was taken against.
#   hack/benchguard.sh headroom
#       How much of this machine is NOT a measurement's own share -- the ceiling
#       both the gate and the verdict are read against. The ONE implementation:
#       four bench scripts used to carry a copy of the same expression, so when
#       the figure under it changed they all went wrong at once and silently.
#   hack/benchguard.sh check [-t LOADMAX] [-r RUNNABLE_MAX]
#       Print the current load, the foreign runnable count and foreign
#       benchmark processes; exit 3 when the host is not quiet enough for a
#       measurement. `check` measures nothing, so its 3 answers the question
#       "is the host quiet?" with no -- it is not the `run` verdict. It keeps
#       its load half, which the run's wait no longer has, so a `check` that
#       reports busy on the load alone does not mean a strict run would refuse.
#
# A caller that already holds the lock (hack/bakeoff.sh) sets
# IQMQ_BENCH_LOCK_HELD=1 so nested runs do not deadlock on it.
#
# UNDER cpu-policy (CPU_POLICY_HELD=1) the pre-run gate and this script's own
# lock are both SKIPPED: `cpu-policy run --bench` has already waited for the
# machine, taken a lock shared with every repository on the host and pinned the
# run to quiet whole cores, all against the physical core count rather than this
# process's CPU mask. The verdict still runs, because whether a non-cooperating
# neighbour spoiled the measurement is the one thing the policy cannot say
# afterwards. Statuses 4 and 5 are therefore unreachable under the policy, and
# both remain reachable everywhere else.
#
# "Foreign" means outside this run, by descent (see own_awk below), and
# the guard asks two questions about it from one process-table snapshot: is a
# benchmark-like process (go test -bench, *.test -test.bench, benchstat,
# bakeoff, benchgate) running, and how many runnable threads are not ours. A
# result marked contended must never be used for a decision: rerun it.
#
# Nothing this script starts outlives it: a trap kills the sampler and the
# measured command's whole process tree, and neither of them inherits the lock
# file descriptor. A lock held on an inherited descriptor kept this host locked
# for sixteen minutes after its holder died.
set -euo pipefail

lock="${IQMQ_BENCH_LOCK:-/tmp/iqmq-benchguard.lock}"
# A measurement takes at most IQMQ_BENCH_MAX_CPUS of this machine, so the rest
# of it is free for everything else -- and the host counts as quiet whenever
# what is running fits in that rest. Both ceilings are therefore "everything
# except the measurement's own share", which is also far above anything the
# measurement itself can contribute, and that is what stops the guard waiting
# for its own decay. The SIZE of the machine it is a share of comes from
# headroom() below; do not re-derive it from `nproc` here, which is the mistake
# that expression used to make.
max_cpus="${IQMQ_BENCH_MAX_CPUS:-${IQMQ_MAX_CPUS:-16}}"
# How big the machine is, is the POLICY's to state and not `nproc`'s. `nproc`
# reports this process's CPU AFFINITY, so once the container's mask was narrowed
# to sixteen  `nproc - max_cpus` evaluated to max(1, 0) = 1 --
# a ceiling of ONE foreign runnable thread, which no shared host ever meets. The
# arithmetic never changed and never announced that it had stopped describing
# the machine; what changed was the mask underneath it. That is how a §10.5
# re-measurement came to be refused three times running on a host sitting at
# load 7 against a real limit of 32.
#
# cpu-policy derives its limits from the PHYSICAL CORE count, so a narrowed mask
# cannot shrink them. Take its figure when there is one, and otherwise keep the
# old expression, which is still the right answer on a host without the tool --
# including a 2-CPU GitHub runner, where it yields 1 because the runner really
# does have no room to spare.
# The policy's own figure, read from whichever shape of `status` this version
# speaks, and LOUD when it speaks none of them.
#
# The first version of this function read `limits.bench_load` and fell back to the
# old arithmetic in silence. cpu-policy 4.0.0 deleted that key -- it replaced the
# single exclusive lock with job slots, so there is no separate benchmark ceiling
# any more -- and the silent fallback put this ceiling straight back to the 1 that
# assumption C29.2 exists to have removed, with nothing saying so and the guard's
# own suite skipping the three cases that would have noticed. A value derived from
# somebody else's schema has to FAIL LOUDLY when that schema moves, or the fallback
# becomes the defect's way back in.
#
# v4: the capacity is what the host admits in total and a heavy job takes its own
# share of it, so what else may legitimately be running is the difference --
# 56 - 24 = 32 here. v2: `limits.bench_load` said it directly.
policy_bench_load() {
  if [ -n "${CPU_BENCH_LOAD_LIMIT:-}" ]; then printf '%s\n' "$CPU_BENCH_LOAD_LIMIT"; return 0; fi
  command -v cpu-policy >/dev/null 2>&1 || return 1
  local js; js="$(cpu-policy status 2>/dev/null)" || return 1
  [ -n "$js" ] || return 1
  local v
  # Three shapes, newest first. Each is READ from the tool rather than
  # re-derived from it: every version of this function that computed a ceiling
  # out of parts got it wrong when the parts moved.
  #
  # v5 removed `capacity` deliberately -- "capacity and quiet-machine settings
  # removed", cpu-policy 5.0.0 -- and replaced the model: a bench job now holds
  # 24 CPUs of its own that light jobs are MOVED OFF, and everything foreign
  # runs on the light CPUs. `light.count` is the tool's own statement of how
  # many CPUs that is right now, which is exactly the ceiling this guard wants:
  # the most foreign runnable threads that are normal rather than a sign the
  # host is too busy to measure on. It is read once, inside the job, so it
  # already accounts for this bench holding a heavy slot, and the summary line
  # records the figure that was used.
  v="$(printf '%s' "$js" | python3 -c '
import json, sys
try: d = json.load(sys.stdin)
except Exception: sys.exit(1)
l = d.get("light") or {}
for key in ("count", "max"):                           # v5
    n = l.get(key)
    if isinstance(n, (int, float)) and n > 0:
        print(int(n)); sys.exit(0)
c = d.get("capacity") or {}                            # v4
tot, heavy = c.get("cpus"), c.get("heavy_cpus")
if isinstance(tot, (int, float)) and isinstance(heavy, (int, float)) and tot > heavy:
    print(int(tot - heavy)); sys.exit(0)
b = (d.get("limits") or {}).get("bench_load")          # v2 and v3
if isinstance(b, (int, float)): print(int(b)); sys.exit(0)
sys.exit(1)' 2>/dev/null)" && [ -n "$v" ] && { printf '%s\n' "$v"; return 0; }
  # Installed, answering, and in a shape this script does not know. Say so on
  # stderr every time rather than quietly using a number that is probably wrong:
  # the fallback below is derived from `nproc`, which is this process's CPU
  # affinity and not the machine.
  echo "benchguard: cpu-policy $(printf '%s' "$js" | sed -n 's/.*"version"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p') reports no ceiling this script can read (no light.count/light.max, no capacity.cpus/heavy_cpus, no limits.bench_load); falling back to nproc arithmetic, which is this process's CPU AFFINITY and not the machine. Set CPU_BENCH_LOAD_LIMIT, or teach policy_bench_load the new shape." >&2
  return 1
}
# Resolved ONCE, at load, because three ceilings are derived from it and each call
# costs a `cpu-policy status` (a subprocess and a sampling window) and would repeat
# the warning above. Computed before the first use below; `headroom` just reports it.
_headroom_cache=""
headroom() {
  if [ -z "$_headroom_cache" ]; then
    local p
    if p="$(policy_bench_load)" && [ -n "$p" ]; then
      _headroom_cache="${p%.*}"
    else
      _headroom_cache="$(awk -v n="$(nproc)" -v m="$max_cpus" 'BEGIN{h=n-m; if (h<1) h=1; print h}')"
    fi
  fi
  printf '%s\n' "$_headroom_cache"
}
_headroom_cache="$(headroom)"
loadmax="${IQMQ_BENCH_LOADMAX:-$_headroom_cache}"
# Whether that ceiling is the guard's own INFERENCE or the caller's
# instruction, which decides whether the load half of the gate may be skipped
# when the inference does not hold (see load_gate_applies).
loadmax_derived=1; [ -n "${IQMQ_BENCH_LOADMAX:-}" ] && loadmax_derived=0
# The pre-run wait's ceiling on FOREIGN runnable threads, in threads, not in
# load average. 6 on this host: the median-of-three foreign count measured 0-3
# with a single 5 over 25 consecutive samples on an idle host (single samples
# reached 6), so 6 is one above the highest quiet-host median and an idle host
# never waits; it is also under a fifth of the 32 CPUs a full-profile run is
# pinned to, and a neighbour that matters -- a build, a test run, another
# agent's benchmark -- puts tens of threads on the run queue, an order of
# magnitude above it.
runmax="${IQMQ_BENCH_RUNNABLE_MAX:-$(headroom)}"
maxwait="${IQMQ_BENCH_MAXWAIT:-900}"
report=""
smoke=0

load1() { cut -d' ' -f1 /proc/loadavg; }
running() { cut -d' ' -f4 /proc/loadavg | cut -d/ -f1; }
# Kill a process and everything below it. Descendants are found through
# `ps --ppid`, never by a command-line pattern: a pattern that matches a
# benchmark also matches the shell that typed it, and that has already killed
# an agent's own shell twice. Children go first, so a
# parent cannot be reaped while they are still running and be reparented.
kill_tree() {
  local p="$1" sig="${2:-TERM}" kid
  for kid in $(ps -o pid= --ppid "$p" 2>/dev/null); do kill_tree "$kid" "$sig"; done
  kill -"$sig" "$p" 2>/dev/null || true
}
# The ownership rule: which rows of a process table belong to this run.
# Membership of our process group is not enough to decide it: a shell with job
# control puts each command it starts in a group of its own, so the chain of
# shells that invoked the guard sits outside the guard's group, and their
# command lines quite legitimately contain the words the benchmark half matches
# on -- `hack/bakeoff.sh A1 full`, `make bakeoff`, a `go test -bench` line
# being edited. Counting those as competitors marked every run contended and
# left the campaign with no usable numbers.
#
# So "ours" is the guard's process group, the chain of processes that invoked
# it, and everything the guard started. Expansion deliberately stops at the
# ancestors: another agent's benchmark hangs off the same session leader we do,
# and a sibling of an ancestor is exactly what this rule exists to find.
#
# The rule is written once and shared verbatim by the two questions the guard
# asks of a process table -- "is a foreign benchmark running?" and "how many
# runnable threads are not ours?" -- because two notions of "ours" that can
# disagree is worse than a slow campaign, and the disagreement is exactly the
# defect this replaced: the wait used the bare load ceiling while the verdict
# allowed for our own load.
#
# It is kept separate from the process table it reads so that
# hack/benchguard_test.sh can feed it tables this host will not produce on
# demand: a sibling of an ancestor, a great-grandchild, a stranger at pid 1.
# Input is `ps -eo pid,ppid,pgid,args` (ownership and the benchmark half) or
# `ps -eLo pid,ppid,pgid,state,args`, one row per thread, with -v sc=4 naming
# the state column (the runnable half). A table with no state column leaves sc
# unset, and no thread is then counted as runnable.
own_awk='
    NR>1 { parent[$1]=$2; line[$1]=$0; if ($3==pg) ours[$1]=1
           if (sc && $sc=="R") runnable[$1]=runnable[$1]+1 }
    function claim_ours() {
      ours[me]=1
      for (p=parent[me]; p>1 && !(p in anc); p=parent[p]) { anc[p]=1; ours[p]=1 }
      changed=1
      while (changed) {
        changed=0
        for (c in parent)
          if (!(c in ours) && (parent[c] in ours) && !(parent[c] in anc)) { ours[c]=1; changed=1 }
      }
    }'

# The benchmark half: every row that is not ours and looks like a benchmark.
#
# Matched on TOKENS, not on the line. The rule used to be a set of substring
# tests, and `benchgate` anywhere in a command line was one of them -- so
# another agent'"'"'s `make fmt-check`, which hands gofumpt every Go file in the
# repository and therefore names `benchmarks/cmd/benchgate/main.go`, was a
# foreign benchmark for as long as it ran. So was `make bench-lint`
# (`go run ./cmd/bakeoff lint`) and `make budgets-lint`
# (`go run ./cmd/benchgate -lint`), neither of which measures anything, and
# both of which are in `make ci`. A30'"'"'s pre-landing gate produced no verdict
# at all for that reason: twice contended, once refused after a 911 s wait.
#
# What actually identifies a running measurement is a benchmark SELECTOR flag
# -- `-bench`, `-bench=X`, `-test.bench=X` -- which only a process running
# benchmarks carries, plus the basenames of the harnesses that drive one. A
# source path handed to a formatter has a basename of `main.go`, so it cannot
# match; `./cmd/bakeoff` has a basename of `bakeoff`, which is deliberately not
# in the list, because that binary decides and lints and never measures.
foreign_awk="$own_awk"'
    function bench_token(t,   b, j) {
      if (t == "-bench" || t ~ /^-bench=/) return 1
      if (t == "-test.bench" || t ~ /^-test\.bench=/) return 1
      b=t; j=index(b, "/")
      while (j > 0) { b=substr(b, j+1); j=index(b, "/") }
      return (b == "bakeoff.sh" || b == "bakeoff-campaign.sh" || b == "bench-api.sh" ||
              b == "benchgate.sh" || b == "native-baselines.sh" || b == "benchstat")
    }
    function looks_like_bench(s,   n, i, tok) {
      n=split(s, tok, /[ \t]+/)
      for (i=1; i<=n; i++) if (bench_token(tok[i])) return 1
      return 0
    }
    END {
      claim_ours()
      for (p in line) {
        if (p in ours) continue
        if (looks_like_bench(line[p])) print p
      }
    }'

# The runnable half: how many of the host's `total` runnable threads are not
# ours. It is a subtraction rather than a count of foreign R rows because the
# process table is namespaced and the run queue is not -- `ps` here sees this
# container's 246 processes while /proc/loadavg counts the host's 9298 tasks,
# so counting only what `ps` can see would be blind to the neighbouring
# containers that are the whole reason the guard exists. The host-wide count is
# instantaneous (the kernel'\''s nr_running, not a decaying average), and the
# ownership rule is what makes it attributable: our own threads are subtracted,
# so between two runs of the campaign the answer is 0 even though the 1-minute
# average still reads 12.
foreign_runnable_awk="$own_awk"'
    END {
      claim_ours()
      n=0
      for (p in runnable) if (p in ours) n+=runnable[p]
      f=total-n
      print (f > 0) ? f : 0
    }'

foreign_from_table() { awk -v me="$1" -v pg="$2" "$foreign_awk" | sort -n | tr '\n' ' '; }
foreign_runnable_from_table() { awk -v me="$1" -v pg="$2" -v sc=4 -v total="$3" "$foreign_runnable_awk"; }

# One snapshot, both answers: sets `probe_foreign` (foreign benchmark pids) and
# `probe_frun` (foreign runnable threads).
#
# The order of the two reads matters and is deliberate. /proc/loadavg is read
# first, before `ps` is forked: at that instant this shell is itself runnable
# and is not yet in the table, and by the time the table is taken it is asleep
# in `wait` while `ps` is the runnable one. The two errors are of opposite sign
# -- one of ours missed, one of ours added -- so they cancel to within a thread
# or so, and the caller's median over several samples absorbs the rest.
probe_foreign=""; probe_frun=0
host_probe() {
  local pgid snap rr total
  pgid="$(ps -o pgid= -p $$ | tr -d ' ')"
  read -r _ _ _ rr _ < /proc/loadavg
  total="${rr%%/*}"
  snap="$(mktemp)"
  # `ps` into a file, and only then `awk` over the file. In a pipeline the awk
  # that runs the ownership rule is itself in the snapshot it is reading, and
  # the rule needed a `/awk/` exclusion to avoid reporting itself -- matched
  # against the whole process row, so any neighbour whose command line
  # happened to contain "awk" was silently exempt from foreign-benchmark
  # detection. Taking the snapshot first removes the need
  # for the exclusion instead of narrowing it. Still one pass of the table per
  # question, over the same file, so the ownership map and the matches cannot
  # disagree about a process that started or exited between two reads -- and
  # the benchmark half and the runnable half cannot disagree either.
  # -L is one row per thread: the run queue counts threads, and a build with
  # eight busy compiler threads is one process.
  # shellcheck disable=SC2009
  ps -eLo pid,ppid,pgid,state,args > "$snap"
  probe_foreign="$(foreign_from_table "$$" "$pgid" < "$snap")"
  probe_frun="$(foreign_runnable_from_table "$$" "$pgid" "$total" < "$snap")"
  rm -f "$snap"
}

# A window of `samples` probes a second apart, because one instant is noisy:
# on an idle host single samples reached 6 foreign runnable threads while the
# median of three stayed at 0-3. Sets `win_foreign` (the union: any sighting of
# a foreign benchmark refuses, that half being the one that does the work) and
# `win_frun` (the median: a spike must not cost a ten-second wait and a lull
# must not buy a measurement).
samples=3
probe_window() {
  local i vals="" f=""
  for i in $(seq 1 "$samples"); do
    [ "$i" -gt 1 ] && sleep 1
    host_probe
    f="$f $probe_foreign"
    vals="$vals$probe_frun
"
  done
  win_foreign="$(printf '%s' "$f" | tr ' ' '\n' | awk 'NF' | sort -nu | tr '\n' ' ')"
  win_frun="$(printf '%s' "$vals" | sort -n | awk '{a[NR]=$1} END{print a[int((NR+1)/2)]+0}')"
}

gt() { awk -v a="$1" -v b="$2" 'BEGIN{exit !(a > b)}'; }
maxf() { awk -v a="$1" -v b="$2" 'BEGIN{print (a > b) ? a : b}'; }

# The percentile the SWAMPED rule decides on, and the rule itself.
#
# `foreign_runnable_max` is a MAXIMUM over every five-second sample, so over a
# long window it measures the tail of the host's own jitter rather than the
# conditions the run mostly had -- and on this host that makes the gate's
# whole-run ceiling unsatisfiable. About thirty containers belonging to other
# people idle at 9-15 foreign runnable threads and spike above the 48-thread
# ceiling (`nproc - IQMQ_BENCH_MAX_CPUS`); a `pr` gate window is 365 s, which is
# about 72 samples, and the max of 72 samples of a tailed quantity reaches 48
# reliably. Measured on three consecutive gate attempts: 35, 62 and 50, the last
# with every other agent on the box stopped.
#
# The guard's own two halves already disagreed about this, and that is the
# lever: the pre-run WAIT decides on a MEDIAN of three samples, because single
# samples reached 6 on an idle host and keying on them made the campaign wait
# for its own noise , while the verdict's swamped check
# decided on the MAX over the whole run. Median to start, max to judge.
#
# So the rule is a high percentile of the samples, NEAREST-RANK: sort them and
# take the ceil(p/100 * n)-th. Nearest rank rather than an interpolated
# percentile for one property that matters more than the smoothness -- it
# returns one of the measurements, so the number in a refusal is a sample the
# guard actually took -- and because ceil(0.95n) == n exactly when n < 20, which
# makes the rule DEGENERATE TO THE MAX on a window too short for a percentile to
# mean anything. A `selftest` half is about 30 s, six samples, and is judged on
# its maximum exactly as it is today; a `pr` window is about 72 samples, and p95
# then discards the busiest three of them -- fifteen seconds of a six-minute
# measurement -- while a run that competed for more than five per cent of its
# length still fails.
swamp_pctile="${IQMQ_BENCH_SWAMP_PCTILE:-95}"

# Nearest-rank percentile of a whitespace-separated series on stdin. Prints
# nothing and exits 1 for an empty series: "no samples" is not a percentile,
# and a caller that reads it as 0 would call a run it knows nothing about
# quiet.
pctile_awk='
    { for (i = 1; i <= NF; i++) if ($i ~ /^[0-9]+$/) v[++n] = $i + 0 }
    END {
      if (n == 0) exit 1
      for (i = 2; i <= n; i++) { x = v[i]; for (j = i-1; j > 0 && v[j] > x; j--) v[j+1] = v[j]; v[j+1] = x }
      k = int(p / 100 * n); if (p / 100 * n > k) k++
      if (k < 1) k = 1; if (k > n) k = n
      printf "%d\n", v[k]
    }'
percentile() { awk -v p="$1" "$pctile_awk"; }

# The swamped rule, in ONE place. It used to be a copy in hack/benchgate.sh and
# another in hack/bench/run-local.sh, both keyed on `foreign_runnable_max`, and
# two copies of a rule that decides whether a measurement may become a number
# is the same defect shape as two notions of "ours".
#
# Sets `swamped` (yes/no), `s_p95`, `s_n` and `s_max` from a series and a
# ceiling. A series with no samples at all is `unknown`, never `no`.
decide_swamped() { # SERIES CEILING
  local series="$1" ceiling="$2"
  s_n="$(printf '%s\n' "$series" | tr ' \t' '\n\n' | awk 'NF' | grep -c . || true)"
  s_p95=""; s_max=""
  if [ "${s_n:-0}" -gt 0 ]; then
    s_p95="$(printf '%s\n' "$series" | percentile "$swamp_pctile")"
    s_max="$(printf '%s\n' "$series" | percentile 100)"
  fi
  if [ -z "$s_p95" ] || [ -z "$ceiling" ]; then swamped=unknown; return; fi
  if [ "$s_p95" -gt "$ceiling" ]; then swamped=yes; else swamped=no; fi
}

# Whether the 1-minute load average may be a gate at all.
#
# The average is a decaying average of EVERYTHING, so it cannot say whose load
# it is -- which is why the pre-run gate's other half counts foreign runnable
# threads instead. The average was allowed back as a gate on the argument that
# a derived ceiling of `nproc - max_cpus` is far above what the measurement
# itself can contribute: with 64 CPUs and 16 pinned, a run adds at most 16 to
# an average whose ceiling is 48, so it cannot be its own reason to wait.
#
# That argument holds only while `nproc - max_cpus` EXCEEDS `max_cpus`, and on
# any host with 32 CPUs or fewer it does not. On a 4-CPU CI runner the derived
# ceiling is 1 while a 16-thread generator lifts the average to 8, so the guard
# refused for 99 s over load that was its own and had already left the run
# queue -- `load1=1.64 loadmax=1 foreign_runnable=0`, nothing foreign runnable,
# waiting for nobody. That is the defect case 21 of hack/benchguard_test.sh
# exists to catch, and it could only ever catch it on an IDLE small host: on a
# busy one the case skips, which is how this shipped.
#
# So a DERIVED ceiling gates on the average only when it is above a
# measurement's own width. An explicit -t or IQMQ_BENCH_LOADMAX is the caller
# saying what busy means and is always honoured, however small.
load_gate_applies() {
  local lmax="$1" derived="$2" own_width="$3"
  [ "$derived" != 1 ] && return 0
  # At EQUALITY the gate still applies: a run of our own contributes at most
  # own_width, which cannot exceed a ceiling of own_width, so the average can
  # only pass it with somebody else's help. The gate is dropped only where our
  # own width alone could push the average past the ceiling.
  awk -v a="$lmax" -v b="$own_width" 'BEGIN{exit !(a >= b)}'
}

# The pre-run gate, from explicit inputs so that hack/benchguard_test.sh can
# hold it to a table on any host rather than only on one quiet enough to
# produce the conditions. Sets `gate_reason`; returns 0 to measure.
gate_ok() {
  local f="$1" l="$2" lmax="$3" derived="$4" own_width="$5" frun="$6" rmax="$7"
  if [ -n "${f// /}" ]; then gate_reason=foreign_benchmark; return 1; fi
  if load_gate_applies "$lmax" "$derived" "$own_width" && gt "$l" "$lmax"; then gate_reason=load; return 1; fi
  if [ "$frun" -ge "$rmax" ]; then gate_reason=foreign_runnable; return 1; fi
  gate_reason=ok; return 0
}

# The verdict rule, kept separate from the run that feeds it for the same
# reason as the ownership rule: hack/benchguard_test.sh can then hold it to a
# table instead of hoping this host produces the load it needs. Sets
# `verdict`, `reason` and `ceiling` from the peak load, the ceiling, our own
# allowance and the number of foreign benchmarks seen.
#
# The load half used to compare `after` -- the load once the run had finished
# -- against loadmax + own, which on this host is 4.0 + 32 = 36 against a
# measured peak of 12.4: it could not fire, and the peak the sampler collects
# was compared to nothing at all.
#
# It is skipped under the same condition as the pre-run gate, and for the same
# reason: where the DERIVED ceiling does not exceed a measurement's own width,
# the average cannot separate this run's load from a neighbour's, so the half
# fires on ourselves. A 4-CPU runner derives loadmax=1, and `loadmax + own` =
# 3 against the 8.43 our own sixteen threads put in the average -- every run
# on such a host reported `contended reason=load` with `foreign_runnable` zero
# throughout. The attributed half and the swamped rule still decide there.
decide_verdict() {
  local peak="$1" lmax="$2" own="$3" fn="$4" derived="${5:-0}" own_width="${6:-0}"
  ceiling="$(awk -v a="$lmax" -v o="$own" 'BEGIN{print a+o}')"
  if [ "$fn" -gt 0 ]; then verdict=contended; reason=foreign
  elif load_gate_applies "$ceiling" "$derived" "$own_width" && gt "$peak" "$ceiling"; then verdict=contended; reason=load
  else verdict=clean; reason=clean
  fi
}

mode="${1:-}"; shift || true
case "$mode" in
  check)
    while getopts 't:r:' opt; do case "$opt" in t) loadmax="$OPTARG"; loadmax_derived=0;; r) runmax="$OPTARG";; *) exit 2;; esac; done
    l="$(load1)"; probe_window
    # foreign_bench_pids stays last on the line: it is what hack/benchguard_test.sh
    # reads with a greedy `foreign_bench_pids=\[\(.*\)\]`.
    lg=applied; load_gate_applies "$loadmax" "$loadmax_derived" "$max_cpus" || lg="skipped(derived ceiling $loadmax is not above a measurement's own $max_cpus)"
    echo "benchguard: load1=$l running=$(running) loadmax=$loadmax load_gate=$lg foreign_runnable=$win_frun runnable_ceiling=$runmax foreign_bench_pids=[${win_foreign% }]"
    # The same three conditions `run` waits for, so that `check` cannot report
    # a host busy that `run` would measure on, or the reverse.
    if ! gate_ok "$win_foreign" "$l" "$loadmax" "$loadmax_derived" "$max_cpus" "$win_frun" "$runmax"; then
      echo "benchguard: host busy (reason=$gate_reason)" >&2; exit 3
    fi
    exit 0 ;;
  runnable)
    # Test hook (hack/benchguard_test.sh): apply the ownership rule to the
    # other question -- how many of TOTAL runnable threads are not ours --
    # over a `ps -eLo pid,ppid,pgid,state,args` table on stdin.
    foreign_runnable_from_table "${1:?me}" "${2:?pgid}" "${3:?total}"; exit 0 ;;
  ownership)
    # Test hook (hack/benchguard_test.sh): apply the ownership rule to a
    # process table on stdin, as though the guard were pid ME in group PGID.
    foreign_from_table "${1:?me}" "${2:?pgid}"; echo; exit 0 ;;
  gate)
    # Test hook (hack/benchguard_test.sh): apply the PRE-RUN gate to explicit
    # inputs -- `gate FOREIGN LOAD1 LOADMAX DERIVED OWN_WIDTH FRUN RUNMAX` --
    # so that the rule can be held to a table on any host. Case 21 is the
    # end-to-end version and can only see its subject on an idle small host.
    if gate_ok "${1?foreign}" "${2:?load1}" "${3:?loadmax}" "${4:?derived}" "${5:?own_width}" "${6:?frun}" "${7:?runmax}"; then
      echo "gate=measure reason=$gate_reason"; exit 0
    fi
    echo "gate=wait reason=$gate_reason"; exit 3 ;;
  verdict)
    # Test hook (hack/benchguard_test.sh): apply the verdict rule to a peak
    # load, a ceiling, an own-load allowance and a foreign count.
    # The 5th and 6th are optional -- whether the ceiling was DERIVED and a
    # measurement's own width -- so the table can drive both directions and a
    # four-argument call keeps its old meaning (an explicit ceiling).
    decide_verdict "${1:?peak}" "${2:?loadmax}" "${3:?own}" "${4:?foreign_n}" "${5:-0}" "${6:-0}"
    echo "verdict=$verdict reason=$reason ceiling=$ceiling"
    [ "$verdict" = clean ] && exit 0
    exit 3 ;;
  headroom)
    # The one implementation of "how much of this machine is not the
    # measurement's own share". Four scripts carried their own copy of the same
    # awk expression, so when `nproc` stopped describing the machine all four
    # went wrong together and none of them said so. Callers ask for it here.
    headroom
    exit 0
    ;;
  percentile)
    # Test hook (hack/benchguard_test.sh): the nearest-rank percentile of a
    # series on stdin. It is a hook because the statistic is the whole of
    # assumption B11.13 and a rule nobody has watched decide is assumed not to.
    percentile "${1:?p}"; exit 0 ;;
  swamped)
    # The SWAMPED decision for a stored guard report: `swamped REPORT
    # [CEILING]`. Called by hack/benchgate.sh and hack/bench/run-local.sh, which
    # each used to carry their own copy of it over `foreign_runnable_max`.
    #
    # A report written before assumption B11.13 has no series, only the max.
    # Such a report is decided on the max alone and SAYS so, because a p95 is
    # not recoverable from a maximum -- and the direction is safe: p95 <= max,
    # so a report whose max is inside the ceiling was not swamped under either
    # rule, and one whose max is over it is undecidable rather than clean.
    rep="${1:?report}"
    [ -r "$rep" ] || { echo "benchguard: swamped: cannot read $rep" >&2; exit 2; }
    sw_series="$(sed -n 's/.*"foreign_runnable_series":\[\([0-9, ]*\)\].*/\1/p' "$rep" | tr ',' ' ')"
    sw_ceiling="${2:-$(sed -n 's/.*"runnable_ceiling":\([0-9]*\).*/\1/p' "$rep")}"
    sw_max="$(sed -n 's/.*"foreign_runnable_max":\([0-9]*\).*/\1/p' "$rep")"
    sw_source=series
    if [ -z "${sw_series// /}" ]; then sw_series="$sw_max"; sw_source=max-only; fi
    decide_swamped "$sw_series" "$sw_ceiling"
    echo "benchguard: swamped=$swamped foreign_runnable_p95=${s_p95:-?} swamp_pctile=$swamp_pctile samples=${s_n:-0} max=${s_max:-?} runnable_ceiling=${sw_ceiling:-?} source=$sw_source"
    # A kept run whose busiest sample was over the ceiling says so. This is the
    # whole of what assumption B11.13 changes, so it is stated on the run it
    # changes rather than only in the ledger: a reader of the verdict can see
    # that the spike was there and that the rule looked past it.
    if [ "$swamped" = no ] && [ -n "${s_max:-}" ] && [ -n "${sw_ceiling:-}" ] && [ "$s_max" -gt "$sw_ceiling" ]; then
      echo "benchguard: the busiest sample was $s_max foreign runnable thread(s), over the ${sw_ceiling}-thread ceiling; the run is KEPT because the p${swamp_pctile} of its ${s_n} sample(s) is ${s_p95} (assumption B11.13)"
    fi
    case "$swamped" in
      no) exit 0 ;;
      yes) exit 3 ;;
      *) exit 2 ;;
    esac ;;
  swamped-audit)
    # What the p95 rule would have said for every stored guard report under
    # DIR, which is the evidence assumption B11.13 is taken against: a rule
    # that would have passed a run everybody agrees was contended is the wrong
    # rule. `swamped-audit DIR [CEILING]`.
    #
    # Reports written before B11.13 carry no series, so the audit states what
    # the record can and cannot decide rather than inventing a series:
    #
    #   max <= ceiling            pass under both rules, because p95 <= max
    #   max > ceiling, n < 20     STILL SWAMPED -- nearest rank makes p95 == max
    #   max > ceiling, n >= 20    not recoverable from a record holding the max
    #
    # n is estimated as 1 + duration_s/5 for a record with no series: one
    # pre-run sample (the median of three the start decision was made on) plus
    # one every five seconds.
    dir="${1:?dir}"; ceil_override="${2:-}"
    printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\n' run records over_ceiling worst dur_s n_est new_rule
    for d in "$dir"/*/; do
      [ -d "$d" ] || continue
      recs=0; over=0; worst=0; oworst=0; odur=0; verdicts=""
      for f in "$d"guard*.json; do
        [ -r "$f" ] || continue
        recs=$((recs+1))
        m="$(sed -n 's/.*"foreign_runnable_max":\([0-9]*\).*/\1/p' "$f")"
        c="${ceil_override:-$(sed -n 's/.*"runnable_ceiling":\([0-9]*\).*/\1/p' "$f")}"
        u="$(sed -n 's/.*"duration_s":\([0-9]*\).*/\1/p' "$f")"
        ser="$(sed -n 's/.*"foreign_runnable_series":\[\([0-9, ]*\)\].*/\1/p' "$f" | tr ',' ' ')"
        [ -n "${m:-}" ] && [ -n "${c:-}" ] || continue
        [ "$m" -gt "$worst" ] && worst="$m"
        if [ "$m" -le "$c" ]; then continue; fi
        over=$((over+1))
        [ "$m" -gt "$oworst" ] && { oworst="$m"; odur="${u:-0}"; }
        if [ -n "${ser// /}" ]; then
          decide_swamped "$ser" "$c"
          verdicts="$verdicts ${swamped}"
        else
          n_est=$(( 1 + ${u:-0} / 5 ))
          if [ "$n_est" -lt 20 ]; then verdicts="$verdicts yes"; else verdicts="$verdicts unknown"; fi
        fi
      done
      [ "$recs" -gt 0 ] || continue
      n_est=$(( 1 + odur / 5 ))
      rule="pass"
      case "$verdicts" in
        *unknown*) rule="unknown" ;;
        *yes*) rule="swamped" ;;
      esac
      [ "$over" -eq 0 ] && { rule="pass"; n_est=0; oworst=0; odur=0; }
      printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\n' "$(basename "$d")" "$recs" "$over" "$worst" "$odur" "$n_est" "$rule"
    done
    exit 0 ;;
  run) ;;
  *) echo "usage: $0 run [-s] [-t LOADMAX] [-r RUNNABLE_MAX] [-w MAXWAIT_S] [-o REPORT] -- CMD...; $0 check [-t LOADMAX] [-r RUNNABLE_MAX]; $0 swamped REPORT [CEILING]; $0 swamped-audit DIR [CEILING]; $0 headroom" >&2; exit 2 ;;
esac

while getopts 'st:r:w:o:' opt; do
  case "$opt" in
    s) smoke=1 ;;
    t) loadmax="$OPTARG"; loadmax_derived=0 ;;
    r) runmax="$OPTARG" ;;
    w) maxwait="$OPTARG" ;;
    o) report="$OPTARG" ;;
    *) exit 2 ;;
  esac
done
shift $((OPTIND - 1))
[ "${1:-}" = "--" ] && shift
[ $# -gt 0 ] || { echo "benchguard: no command" >&2; exit 2; }

spid=""; cmdpid=""; samp=""; serf=""
cleanup() {
  trap - EXIT INT TERM HUP
  if [ -n "$spid" ]; then kill_tree "$spid" TERM; spid=""; fi
  if [ -n "$cmdpid" ]; then kill_tree "$cmdpid" TERM; cmdpid=""; fi
  if [ -n "$samp" ]; then rm -f "$samp"; samp=""; fi
  if [ -n "$serf" ]; then rm -f "$serf"; serf=""; fi
  # Close the host-wide lock explicitly rather than relying on exit: it makes
  # the release the last thing that happens rather than something the kernel
  # does on the way out, and it is the half of the sixteen-minute lockout this
  # script can own.
  exec 9>&- 2>/dev/null || true
  :
}
trap 'rc=$?; cleanup; exit "$rc"' EXIT
trap 'echo "benchguard: signalled; killing the measured command and releasing the lock" >&2; cleanup; exit 130' INT TERM HUP

# IQMQ_BENCH_LOCK_HELD=1 is a claim by the caller that it already holds the
# host-wide lock, and it is inherited by everything under the guard -- so it
# was believed by runs that held nothing. Verify it: if the
# lock is actually free then nobody holds it, whoever set the variable is gone
# or never took it, and this run would measure unserialised against every other
# agent on the machine.
lock_held="${IQMQ_BENCH_LOCK_HELD:-0}"
if [ "$lock_held" = 1 ] && flock -n "$lock" -c true 2>/dev/null; then
  echo "benchguard: IQMQ_BENCH_LOCK_HELD=1 but $lock is not held by anyone; taking it" >&2
  lock_held=0
fi
# Under cpu-policy both halves of the admission have already happened, before
# this script started and at a scope this one cannot reach: CPU_POLICY_HELD=1
# says the job holds the MACHINE-WIDE heavy-job lock, shared with every other
# repository and session on the host, and `run --bench` admitted the run against
# the physical core count and pinned it to quiet whole cores. This lock is
# iqmq-only and serialises nothing that one has not already serialised, so
# taking it would only invite the two to disagree.
#
# The VERDICT below still runs, and deliberately: it judges machine-wide load
# AFTER the fact, which is the one question the policy cannot answer for us --
# whether a foreign workload that does not cooperate with the lock spoiled the
# measurement while it was running.
under_policy=0
if [ "${CPU_POLICY_HELD:-}" = 1 ]; then
  under_policy=1
  lock_held=1
fi
if [ "$lock_held" != 1 ]; then
  exec 9>"$lock"
  if ! flock -n 9; then
    echo "benchguard: waiting for the host benchmark lock ($lock)"
    flock -w "$maxwait" 9 || { echo "benchguard: lock not acquired within ${maxwait}s" >&2; exit 4; }
  fi
fi

# The pre-run wait, on an instantaneous and attributed measure of what ELSE is
# running: no foreign benchmark, and fewer than `runmax` foreign runnable
# threads. It used to compare the 1-minute load average against the bare
# `loadmax`, while the verdict below allowed for the run's own load -- the two
# halves disagreed about whose load it was, and the average cannot tell them
# apart because it is a decaying average of everything. At the campaign's
# settings a run at GOMAXPROCS=32 on 32 pinned CPUs left the average near 12.4,
# so the next run waited 40-60 s for its own predecessor's decay and every
# verdict in that window was `clean foreign=0`: about 40 % of the campaign's
# wall clock was the guard waiting for the campaign. Between two runs our own
# processes have exited and contribute nothing to the run queue, while a
# neighbour's build still does.
#
# Smoke mode takes one sample rather than three and gates on neither half: it
# neither delays nor fails a run that will never feed a decision, so there is
# nothing for the noise reduction to decide, and `make bench-quick` must not
# start by waiting two seconds.
[ "$smoke" = 1 ] && samples=1
wait_start="$(date +%s)"
waited=0
announced=0
while :; do
  probe_window
  gate_f="$win_foreign"; gate_frun="$win_frun"; gate_l="$(load1)"
  [ "$smoke" = 1 ] && { gate_f=""; gate_frun=0; gate_l=0; }
  # Three conditions, all about the machine outside this measurement's own
  # share: no foreign benchmark at all, a 1-minute load average below the
  # ceiling, and fewer foreign runnable threads than the same ceiling. The
  # load average is back as a gate now that the ceiling is `nproc - max_cpus`
  # rather than 4.0 -- a run pinned to sixteen CPUs contributes at most sixteen
  # to an average whose ceiling is forty-eight, so it can no longer be its own
  # reason to wait, which is what made the average unusable before.
  # The pre-run gate is cpu-policy's job where cpu-policy is driving. Probing
  # still happens, because `frun_before` is part of the record either way; what
  # is skipped is the REFUSAL, which here would be a second opinion formed from
  # a narrower view of the machine than the one that already admitted this run.
  if [ "$under_policy" = 1 ]; then
    [ "$announced" -eq 0 ] && {
      echo "benchguard: cpu-policy admitted this run and holds the machine-wide lock (CPU_POLICY_CPUS=${CPU_POLICY_CPUS:-unpinned}, budget=${CPU_POLICY_BUDGET:-?}); not gating again"
      announced=1
    }
    break
  fi
  if gate_ok "$gate_f" "$gate_l" "$loadmax" "$loadmax_derived" "$max_cpus" "$gate_frun" "$runmax"; then break; fi
  waited=$(( $(date +%s) - wait_start ))
  if [ "$waited" -ge "$maxwait" ]; then
    # Exit 5, not 3: nothing was measured, so there is no contended sample to
    # discard, and repeating this immediately costs another maxwait for the
    # same answer.
    lg=applied; load_gate_applies "$loadmax" "$loadmax_derived" "$max_cpus" || lg=skipped
    echo "benchguard: host still busy after ${waited}s (reason=$gate_reason foreign=[${win_foreign% }] load1=$(load1) loadmax=$loadmax load_gate=$lg foreign_runnable=$win_frun runnable_ceiling=$runmax); refused to measure" >&2
    exit 5
  fi
  [ "$announced" -eq 0 ] && { echo "benchguard: reason=$gate_reason foreign=[${win_foreign% }] load1=$(load1)/$loadmax foreign_runnable=$win_frun/$runmax; waiting"; announced=1; }
  sleep 10
done

before="$(load1)"; before_run="$(running)"; frun_before="$win_frun"
peak="$before"; foreign_seen=""; frun_peak="$frun_before"
# The sampler appends every in-run foreign-runnable sample to `$serf`, because
# the SWAMPED rule is a percentile of the samples and a percentile cannot be
# recovered from a maximum. The first entry is `frun_before`, the median of
# three taken immediately before the command started: it is the number the
# start decision was made on, and it means a run too short for any five-second
# tick still has one sample to be judged on rather than none (assumption
# B11.13).
sampler() {
  while :; do
    sleep 5
    l="$(load1)"; if gt "$l" "$peak"; then peak="$l"; fi
    host_probe
    [ -n "${probe_foreign// /}" ] && foreign_seen="$foreign_seen $probe_foreign"
    if [ "$probe_frun" -gt "$frun_peak" ]; then frun_peak="$probe_frun"; fi
    printf '%s\n' "$probe_frun" >> "$serf"
    printf '%s %s %s\n' "$peak" "$frun_peak" "$foreign_seen" > "$samp"
  done
}
samp="$(mktemp)"; printf '%s %s \n' "$peak" "$frun_peak" > "$samp"
serf="$(mktemp)"; printf '%s\n' "$frun_before" > "$serf"
# 9>&- on both: neither the sampler nor the measured command (nor anything
# either of them starts) may hold the host-wide lock, or killing this script
# leaves the host locked by an immortal sampler.
sampler 9>&- & spid=$!
start=$(date +%s)
status=0
# Started in the background and waited for, so a signal reaches the trap while
# the command is still running instead of after it.
"$@" 9>&- & cmdpid=$!
wait "$cmdpid" || status=$?
cmdpid=""
end=$(date +%s)
kill_tree "$spid" TERM; wait "$spid" 2>/dev/null || true; spid=""
read -r peak frun_peak foreign_seen < "$samp" || true; rm -f "$samp"; samp=""
frun_series="$(tr '\n' ' ' < "$serf")"; rm -f "$serf"; serf=""
after="$(load1)"
host_probe
foreign_after="$probe_foreign"
# The post-run probe is a SINGLE instantaneous sample taken after the command
# has exited, and it is folded into `foreign_runnable_max` -- which is how a
# one-second run came to record 78 foreign runnable threads (A17 guard-6). It
# stays in the max for continuity (assumption A24.81 describes that number as
# an upper bound on what else was there) and is recorded separately, and it is
# deliberately NOT in the series the swamped percentile is computed over: it
# describes the host after the measurement rather than the conditions the
# measurement had.
frun_after="$probe_frun"
if [ "$probe_frun" -gt "$frun_peak" ]; then frun_peak="$probe_frun"; fi
decide_swamped "$frun_series" "$runmax"
# awk 'NF' (not grep -v '^$'): with pipefail an empty list would make grep
# exit 1 and set -e would end the script before the summary on a clean host.
foreign_all="$(echo "$foreign_seen $foreign_after" | tr ' ' '\n' | awk 'NF' | sort -u | tr '\n' ' ')"
foreign_n="$(echo "$foreign_all" | wc -w | tr -d ' ')"
# The peak is the highest 1-minute average anywhere in the run, the sampler's
# samples included: a neighbour's build that started and finished inside the
# run is invisible to `after`, and the sampler is the only thing that can see
# it. It used to be recorded and compared to nothing.
peak="$(maxf "$peak" "$after")"
# Contended when a foreign benchmark ran, or when the load rose above what
# this command itself can explain. IQMQ_BENCH_OWN_LOAD is that allowance: the
# number of CPUs the command is pinned to, since a pinned run cannot make more
# than that many of its own threads runnable. Callers that know the width pass
# it; `nproc` is the fallback for a bare run whose width is unknown, and a
# negative value (hack/benchguard_test.sh uses one) makes the load half fire
# on any load at all.
own="${IQMQ_BENCH_OWN_LOAD:-$(nproc)}"
decide_verdict "$peak" "$loadmax" "$own" "$foreign_n" "$loadmax_derived" "$max_cpus"
# load_before, load_after and load_max are 1-minute averages of EVERYTHING on
# the host, this run and its predecessor included, and the wait no longer keys
# on any of them -- so a family whose load_before reads 11 was not measured on
# a busier host than one whose reads 3.5, it was measured sooner after its own
# predecessor. What the guard decided on is foreign_runnable_before, and what
# the measurement competed with is foreign_runnable_max; both are recorded
# here and in the JSON report so that the evidence says what was decided.
#
# foreign_runnable_before is a median of three samples and is the number to
# compare between two runs. foreign_runnable_max is a MAXIMUM over every
# five-second sample, so it reaches the tail of the host's own jitter rather
# than its middle: single instantaneous samples on an idle host reach 6, and
# one in fourteen taken during a 32-thread run of our own read 12 while our
# own 33 runnable threads were attributed correctly in every one of them. So
# it is an upper bound on what else was there, and it is not the quantity
# runnable_ceiling is applied to; a neighbour worth knowing about shows tens
# of threads in it.
summary="benchguard: verdict=$verdict reason=$reason load_before=$before load_after=$after load_max=$peak load_ceiling=$ceiling running_before=$before_run foreign_runnable_before=$frun_before foreign_runnable_max=$frun_peak foreign_runnable_p95=${s_p95:-?} swamp_pctile=$swamp_pctile foreign_runnable_samples=${s_n:-0} foreign_runnable_after=$frun_after swamped=$swamped runnable_ceiling=$runmax foreign=$foreign_n foreign_pids=[${foreign_all% }] loadmax=$loadmax own_load=$own cpu_policy_cpus=${CPU_POLICY_CPUS:-unpinned} duration_s=$((end-start)) status=$status"
echo "$summary"
if [ -n "$report" ]; then
  printf '{"verdict":"%s","reason":"%s","load_before":%s,"load_after":%s,"load_max":%s,"load_ceiling":%s,"foreign_runnable_before":%s,"foreign_runnable_max":%s,"foreign_runnable_after":%s,"foreign_runnable_pctile":%s,"foreign_runnable_p95":%s,"foreign_runnable_samples":%s,"foreign_runnable_series":[%s],"swamped":"%s","runnable_ceiling":%s,"foreign":%s,"foreign_pids":"%s","loadmax":%s,"own_load":%s,"cpu_policy_cpus":"%s","duration_s":%s,"status":%s}\n' \
    "$verdict" "$reason" "$before" "$after" "$peak" "$ceiling" "$frun_before" "$frun_peak" "$frun_after" "$swamp_pctile" "${s_p95:-0}" "${s_n:-0}" "$(printf '%s' "${frun_series% }" | tr -s ' ' ',')" "$swamped" "$runmax" "$foreign_n" "${foreign_all% }" "$loadmax" "$own" "${CPU_POLICY_CPUS:-unpinned}" "$((end-start))" "$status" > "$report"
fi
[ "$status" -ne 0 ] && exit "$status"
[ "$verdict" = contended ] && [ "$smoke" = 0 ] && exit 3
exit 0
