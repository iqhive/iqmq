#!/usr/bin/env bash
# Tests for hack/benchguard.sh, whose verdict decides whether a measurement is
# allowed to feed a decision (the benchmark configuration, the contributor notes). Every
# promise the guard makes is checked here, because nothing else stands behind
# it:
#
#   - a run of ours is never called foreign, and someone else's benchmark
#     always is (cases 1, 2, 3, 8, 9);
#   - the two ways a strict run can end are told apart: it REFUSED to start
#     because the host was busy (exit 5, case 4) or it RAN and was contended
#     (exit 3, cases 11, 11b) -- both used to be exit 3, and hack/bakeoff.sh
#     retried the one that cannot usefully be retried;
#   - the verdict rule itself, over a table rather than over whatever load
#     this host happens to have (case 10) -- its load half could not fire at
#     the campaign's own settings and nothing noticed;
#   - a foreign benchmark that exists only *while* the run does is caught by
#     the sampler, which no case exercised at all (cases 11, 11b);
#   - the pre-run wait waits for the HOST and not for the campaign's own
#     decaying load average (cases 8b, 14, 15) -- it used to key on the
#     1-minute average, which cannot tell our own predecessor's exit from a
#     neighbour's build, and roughly forty per cent of the campaign's wall
#     clock was the guard waiting for the campaign. Case
#     14 is the half that must no longer wait and case 15 the half that must
#     still hold off, because a guard that never waits is not a fix but the
#     removal of a check;
#   - nothing the guard starts outlives it, and the host-wide lock is never
#     left held (case 12).
#
#   hack/benchguard_test.sh            # all cases
#   GUARD=/tmp/other-benchguard.sh hack/benchguard_test.sh
#       Run the cases against another copy of the guard. This is how each case
#       is shown to be able to fail: point it at
#       `git show <old-rev>:hack/benchguard.sh` and the cases for that
#       revision's defects go red.
#
# The process table is shared with the rest of the host, so a case that would
# turn on "no foreign benchmark anywhere" asserts on the pid it is about
# instead. Where a stronger assertion is impossible the case says which half
# of the property it did not check, and the half it drops is always covered by
# a host-independent case (10 for the verdict rule, 8 and 9 for ownership).
set -uo pipefail
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# The host CPU ceiling. This script invokes `go` itself, so it is capped when
# run by hand and not only when a make recipe exports the cap into it.
# hack/cpucap.sh carries the arithmetic; the eval never
# RAISES either half.
eval "$("$root/hack/cpucap.sh" env)"
guard="${GUARD:-$root/hack/benchguard.sh}"
tmp="$(mktemp -d)"
trap 'p="${foreign_pid:-}"; [ -n "$p" ] && kill "$p" 2>/dev/null;
      g="${spin_pgid:-}"; [ -n "$g" ] && kill -- "-$g" 2>/dev/null; rm -rf "$tmp"' EXIT
export IQMQ_BENCH_LOCK="$tmp/lock"
# The lock is isolated above; the POLICY has to be isolated for the same reason.
# `make ci` runs under `cpu-policy`, which exports CPU_POLICY_HELD=1 into
# everything below it, and the guard reads that as "the gate and the lock have
# already been decided at a wider scope, do not form a second opinion" -- which is
# right for a measurement and wrong here. These cases drive the guard's own gate
# against SYNTHETIC conditions and measure nothing, so two of them (the ones that
# assert a refusal, exit 5) cannot pass while the gate is being skipped: under
# `make ci` they saw 3 and 0 instead.
#
# Unsetting it tests the guard rather than the policy. The DEFERRAL itself is
# covered separately, by the case below that sets it deliberately -- which is the
# only way round that tests both halves.
unset CPU_POLICY_HELD

pass=0; fail=0; skip=0
ok()   { printf 'ok   %s\n' "$1"; pass=$((pass+1)); }
bad()  { printf 'FAIL %s\n     %s\n' "$1" "$2"; fail=$((fail+1)); }
noted(){ printf 'skip %s\n     not checked: %s\n' "$1" "$2"; skip=$((skip+1)); }

# Processes still alive in a process group, read from the process table and
# never matched by a command-line pattern (the contributor notes: a pattern that
# matches a benchmark also matches the shell that typed it).
pgroup_members() { ps -eo pid,pgid | awk -v g="$1" 'NR>1 && $2==g {print $1}' | tr '\n' ' '; }

# A benchmark-like process that is genuinely not ours: setsid --fork detaches
# it into its own session and reparents it away from this script, which is what
# another agent's benchmark looks like from here.
# It reports its own pid through a file: matching the process table by pattern
# would also match this script's own caller, whose command line quotes the
# pattern, and the first version of this test killed the shell running it.
start_foreign() {
  rm -f "$tmp/foreign.pid"
  setsid --fork bash -c 'echo $$ > "$0"; exec -a "go test -bench=BenchmarkFake ./x" sleep 120' \
    "$tmp/foreign.pid" >/dev/null 2>&1
  for _ in 1 2 3 4 5 6 7 8 9 10; do
    if [ -s "$tmp/foreign.pid" ]; then
      foreign_pid="$(cat "$tmp/foreign.pid")"
      # Only ever a pid this function started, and only while it still exists.
      case "$foreign_pid" in ''|*[!0-9]*) return 1;; esac
      kill -0 "$foreign_pid" 2>/dev/null && return 0
    fi
    sleep 0.2
  done
  return 1
}
stop_foreign() {
  [ -n "${foreign_pid:-}" ] && kill "$foreign_pid" 2>/dev/null
  foreign_pid=""; rm -f "$tmp/foreign.pid"; sleep 0.3
}

# A CPU-bound workload of `$1` threads. `setsid --fork` puts it in a session
# and a process group of its own, so it is nobody's descendant and the
# ownership rule cannot claim it -- which is what another container's build
# looks like from here. Its argv deliberately mentions nothing the
# foreign-benchmark half matches on: case 15 is about the runnable half, and a
# stand-in that also tripped the benchmark half would pass the case without
# exercising it. Its pid is reported through a file (never matched by pattern),
# and it leads its own group so the whole thing can be killed by process group.
spin_pgid=""
start_spinners() {
  rm -f "$tmp/spin.pid"
  setsid --fork bash -c 'echo $$ > "$1"; for _ in $(seq 1 "$2"); do (exec -a "cc1plus -O2 neighbour.cc" bash -c "while :; do :; done") & done; wait' \
    _ "$tmp/spin.pid" "$1" >/dev/null 2>&1
  for _ in 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15; do
    if [ -s "$tmp/spin.pid" ]; then
      spin_pgid="$(cat "$tmp/spin.pid")"
      case "$spin_pgid" in ''|*[!0-9]*) spin_pgid=""; return 1;; esac
      # Wait until the threads are actually on the run queue.
      for _ in 1 2 3 4 5 6 7 8 9 10; do
        [ "$(pgroup_members "$spin_pgid" | wc -w)" -gt "$1" ] && return 0
        sleep 0.3
      done
      return 0
    fi
    sleep 0.2
  done
  return 1
}
stop_spinners() {
  [ -n "${spin_pgid:-}" ] || return 0
  kill -- "-$spin_pgid" 2>/dev/null
  for _ in $(seq 1 40); do
    [ -z "$(pgroup_members "$spin_pgid" | tr -d ' ')" ] && break
    sleep 0.25
  done
  spin_pgid=""; rm -f "$tmp/spin.pid"
}

# The guarded command for the two cases that need a foreign benchmark to exist
# only while the run does: start one detached, let it live past the sampler's
# five-second tick, then kill it before the run ends so that only the sampler
# can have seen it. $1 is the file it reports its pid through -- a quoted
# heredoc, so every $ below belongs to the shell that finally runs it.
transient_cmd="$(cat <<'INNER'
setsid --fork bash -c 'echo $$ > "$0"; exec -a "go test -bench=BenchmarkTransient ./z" sleep 30' "$1" >/dev/null 2>&1
for _ in 1 2 3 4 5 6 7 8 9 10; do [ -s "$1" ] && break; sleep 0.2; done
sleep 9
p="$(cat "$1" 2>/dev/null)"; case "$p" in ""|*[!0-9]*) ;; *) kill "$p" 2>/dev/null;; esac
sleep 1
INNER
)"

# What the host was already running, so a case can tell its own noise apart
# from everyone else's.
pre="$("$guard" check -t 99999 2>/dev/null | sed -n 's/.*foreign_bench_pids=\[\(.*\)\].*/\1/p')"
[ -n "${pre// /}" ] && printf 'note host already runs foreign benchmarks: [%s]\n' "$pre"
true

# 1. The shell that invokes the guard mentions the word "bakeoff" in its own
#    command line, exactly as `hack/bakeoff.sh A1 full` and `make bakeoff` do.
#    It is our own caller and must never be reported as a competing workload.
# `:` discards its arguments, so the command text is present in the shell's
# argv — which is the whole point — without running a bake-off.
out="$(bash -c "echo \$\$ > '$tmp/caller.pid'; cd '$root' && : ./hack/bakeoff.sh A1 full && '$guard' check -t 99999" 2>&1)"
rc=$?
caller="$(cat "$tmp/caller.pid" 2>/dev/null)"
seen="$(printf '%s\n' "$out" | sed -n 's/.*foreign_bench_pids=\[\(.*\)\].*/\1/p')"
# Assert on the pid this case is about. Another agent's benchmark may legitimately
# be running -- this host is shared, which is the whole reason the guard exists --
# so "the list is empty" is not the property under test and would make the case
# fail for a fact about the host rather than a fault in the rule.
if [ -z "$caller" ]; then
  bad "the invoking shell is not a foreign benchmark" "could not record the caller's pid"
elif printf ' %s ' "$seen" | grep -q " $caller "; then
  bad "the invoking shell is not a foreign benchmark" "check reported the caller (pid $caller) among foreign_bench_pids=[$seen]"
else
  ok "the invoking shell is not a foreign benchmark"
fi

# 1b. The other half of case 1, and the only half a busy host can take away:
#     with nothing foreign anywhere, `check` says so and exits 0. When the
#     host is running someone else's benchmark that cannot be asserted, and
#     the case names what it did not check rather than passing quietly.
# The runnable half is the second reason a genuinely busy host can take this
# half away: `check` exits 3 when foreign runnable threads reach the ceiling,
# and on a shared host that is the correct answer rather than a fault in the
# rule (case 8b holds that rule over a table, host-independently).
cfr="$(printf '%s\n' "$out" | sed -n 's/.*foreign_runnable=\([0-9]*\).*/\1/p')"
cfrmax="$(printf '%s\n' "$out" | sed -n 's/.*runnable_ceiling=\([0-9]*\).*/\1/p')"
if [ -n "${seen// /}" ]; then
  noted "a quiet host passes check" "check's exit-0 path -- the host was running other benchmarks [$seen], for which a non-zero exit is the correct answer"
elif [ -n "$cfr" ] && [ -n "$cfrmax" ] && [ "$cfr" -ge "$cfrmax" ]; then
  noted "a quiet host passes check" "check's exit-0 path -- $cfr foreign runnable thread(s) against a ceiling of $cfrmax, for which a non-zero exit is the correct answer"
elif [ "$rc" -ne 0 ]; then
  bad "a quiet host passes check" "nothing was foreign and the load was under 99999, so check should exit 0, got $rc: $out"
else
  ok "a quiet host passes check"
fi

# 2. A run's own benchmark process matches the pattern by construction; the
#    guard must not call its own child a foreign benchmark.
rep="$tmp/own.json"
out="$(bash -c "cd '$root' && '$guard' run -s -t 99999 -w 0 -o '$rep' -- bash -c 'echo \$\$ > \"$tmp/own.pid\"; exec -a \"go test -bench=BenchmarkOwn ./y\" sleep 6'" 2>&1)"
rc=$?
own="$(cat "$tmp/own.pid" 2>/dev/null)"
seen="$(sed -n 's/.*"foreign_pids":"\([^"]*\)".*/\1/p' "$rep" 2>/dev/null)"
if [ ! -s "$rep" ]; then
  bad "a run's own benchmark process is not foreign" "no JSON report written to $rep: $out"
elif [ -z "$own" ]; then
  bad "a run's own benchmark process is not foreign" "the guarded command did not record its pid"
elif printf ' %s ' "$seen" | grep -q " $own "; then
  bad "a run's own benchmark process is not foreign" "the guard reported its own child (pid $own) as foreign: $out"
else
  ok "a run's own benchmark process is not foreign"
fi

# 2b. And with nothing foreign the run is clean and exits 0 -- the half a busy
#     host takes away. Case 10 holds the same rule without needing a quiet
#     host, so what is lost here is only that a real run reaches it.
if [ -n "${seen// /}" ]; then
  noted "a clean run exits 0 with verdict=clean" "the clean end-to-end path -- another benchmark ran during the case [$seen], for which contended is the correct verdict (the rule itself is case 10)"
elif [ "$rc" -ne 0 ]; then
  bad "a clean run exits 0 with verdict=clean" "expected exit 0 with nothing foreign, got $rc: $out"
elif ! printf '%s' "$out" | grep -q 'verdict=clean reason=clean'; then
  bad "a clean run exits 0 with verdict=clean" "nothing was foreign, so the verdict should be clean: $out"
else
  ok "a clean run exits 0 with verdict=clean"
fi

# 3. Someone else's benchmark is still caught — the check that must be able to
#    fail, or the guard is decoration.
if ! start_foreign; then
  bad "a foreign benchmark is detected" "could not start the stand-in process"
else
  out="$("$guard" check -t 99999 2>&1)"; rc=$?
  if [ "$rc" -ne 3 ]; then
    bad "a foreign benchmark is detected" "expected exit 3, got $rc: $out"
  elif ! printf '%s' "$out" | grep -q "$foreign_pid"; then
    bad "a foreign benchmark is detected" "pid $foreign_pid missing from: $out"
  else
    ok "a foreign benchmark is detected"
  fi

  # 4. A foreign benchmark already running when a strict run starts makes the
  #    guard REFUSE: exit 5, the command never ran, nothing to discard. This
  #    used to be exit 3 -- the same status a contended *measurement* returns
  #    -- so hack/bakeoff.sh counted it as a contended sample and repeated it,
  #    buying another maxwait wait for the same answer.
  out="$("$guard" run -t 99999 -w 0 -- true 2>&1)"; rc=$?
  if [ "$rc" -ne 5 ]; then
    bad "a busy host refuses a strict run before it starts" "expected exit 5 (refused, nothing measured), got $rc: $out"
  elif ! printf '%s' "$out" | grep -q 'refused to measure'; then
    bad "a busy host refuses a strict run before it starts" "the refusal is not named in: $out"
  else
    ok "a busy host refuses a strict run before it starts"
  fi

  # 4b. The SAME host, under cpu-policy: the refusal must disappear and the
  #     verdict must not. This is the negative control for the deferral added
  #     with assumption C29.9, and it is the case that pins the division of
  #     labour: the policy decides whether a run may START -- it has already
  #     waited for the machine and holds a lock shared with every repository on
  #     the host, judged against the physical core count -- while the guard
  #     decides whether the numbers may be USED. So a foreign benchmark no
  #     longer refuses the run (not 5), and still condemns it (3), because a
  #     neighbour that does not take the lock can spoil a measurement the policy
  #     was right to admit.
  #
  #     Without this case the deferral is only observable by `make ci` going red
  #     on the two cases above, which is how it was found.
  out="$(CPU_POLICY_HELD=1 "$guard" run -t 99999 -w 0 -- true 2>&1)"; rc=$?
  if [ "$rc" -eq 5 ]; then
    bad "under cpu-policy the gate defers and the verdict does not" "still refused (exit 5) with CPU_POLICY_HELD=1: $out"
  elif [ "$rc" -ne 3 ]; then
    bad "under cpu-policy the gate defers and the verdict does not" "expected exit 3 (admitted, then condemned), got $rc: $out"
  elif ! printf '%s' "$out" | grep -q 'not gating again'; then
    bad "under cpu-policy the gate defers and the verdict does not" "the deferral is not announced in: $out"
  elif ! printf '%s' "$out" | grep -q 'verdict=contended'; then
    bad "under cpu-policy the gate defers and the verdict does not" "the verdict stopped condemning a foreign benchmark: $out"
  else
    ok "under cpu-policy the gate defers and the verdict does not"
  fi

  # 5. Smoke mode tolerates it but still records what was running, so a log
  #    never claims a clean host it did not have.
  rep="$tmp/smoke.json"
  out="$("$guard" run -s -t 99999 -w 0 -o "$rep" -- true 2>&1)"; rc=$?
  if [ "$rc" -ne 0 ]; then
    bad "smoke mode tolerates a foreign benchmark" "expected exit 0, got $rc: $out"
  elif ! printf '%s' "$out" | grep -q 'verdict=contended'; then
    bad "smoke mode tolerates a foreign benchmark" "verdict was not recorded as contended: $out"
  elif ! grep -q '"verdict":"contended"' "$rep"; then
    bad "smoke mode tolerates a foreign benchmark" "JSON report does not record it: $(cat "$rep")"
  else
    ok "smoke mode tolerates a foreign benchmark"
  fi
  stop_foreign
fi

# 6. The load half of `check` can fail too. The ceiling used to be 0, which
#    made this case pass for the wrong reason whenever any foreign benchmark
#    was running -- `check` exits 3 for either half and the case only read the
#    status -- and fail outright on a genuinely idle host, where
#    /proc/loadavg reads 0.00 and `0.00 > 0` is false. A negative ceiling is
#    below every load average a host can have, idle or not.
out="$("$guard" check -t -1 2>&1)"; rc=$?
cf="$(printf '%s\n' "$out" | sed -n 's/.*foreign_bench_pids=\[\(.*\)\].*/\1/p')"
cl="$(printf '%s\n' "$out" | sed -n 's/.*load1=\([^ ]*\).*/\1/p')"
if [ "$rc" -ne 3 ]; then
  bad "a load ceiling below the current load is refused" "expected exit 3, got $rc: $out"
elif [ -z "$cl" ]; then
  bad "a load ceiling below the current load is refused" "check did not report the load it compared: $out"
elif [ -n "${cf// /}" ]; then
  # Still a pass -- the ceiling is below any load a host can have, so the load
  # half fired -- but with the host busy the exit status alone cannot say that
  # the foreign half did not also fire. Case 10 separates the two halves.
  ok "a load ceiling below the current load is refused (load1=$cl; foreign [$cf] also present)"
else
  ok "a load ceiling below the current load is refused (load1=$cl, nothing foreign)"
fi

# 7. The command's own failure is what the caller sees, not the verdict.
out="$("$guard" run -s -t 99999 -w 0 -- bash -c 'exit 7' 2>&1)"; rc=$?
if [ "$rc" -ne 7 ]; then
  bad "the command's exit status reaches the caller" "expected exit 7, got $rc: $out"
else
  ok "the command's exit status reaches the caller"
fi

# 8. The ownership rule itself, over a process table this host will not
#    produce on demand. The guard is pid 400 in group 300, started by a shell
#    that was started by `hack/bakeoff.sh`, which another agent's `make ci` and
#    two strangers share a host with.
table="$(printf '%s\n' \
  '  PID  PPID  PGID COMMAND' \
  '    1     0     1 /sbin/init' \
  '  100     1   100 claude --session' \
  '  200   100   200 bash -c cd repo && ./hack/bakeoff.sh A1 full' \
  '  300   200   300 /bin/bash ./hack/bakeoff.sh A1 full' \
  '  400   300   300 /bin/bash ./hack/benchguard.sh run -- go test -bench=X' \
  '  500   400   300 go test -bench=BenchmarkMemory/Queue ./internal/queue' \
  '  510   500   300 /tmp/queue.test -test.bench=BenchmarkMemory/Queue' \
  '  600   100   600 bash -c make ci' \
  '  610   600   600 go test -bench=BenchmarkOther ./x' \
  '  700     1   700 /tmp/stranger.test -test.bench=.' \
  '  800   100   800 bash -c grep -n benchstat Makefile' \
  '  900     1   900 sh -c go test -bench=. ./z | awk NF')"
got="$(printf '%s\n' "$table" | "$guard" ownership 400 300 | sed 's/ *$//')"
# 610 is a sibling agent's benchmark and 700 a stranger: both foreign. 800 only
# mentions benchstat while grepping for it, and is reported too -- the rule errs
# towards refusing to measure, which costs a wait rather than a wrong number.
# 900 is a stranger's benchmark piped through `awk`, and it is reported: the
# rule used to skip every row whose text contained "awk", which it needed
# because the awk running the rule was in the process table it was reading, and
# which silently exempted any neighbour whose command line mentioned awk. The
# snapshot is now taken before the awk starts, so the exclusion is gone.
want='610 700 800 900'
if [ "$got" != "$want" ]; then
  bad "the ownership rule reads a process table correctly" "want [$want], got [$got]"
else
  ok "the ownership rule reads a process table correctly"
fi

# 8a. The benchmark half's precision, over a table of the things that are NOT
#     a measurement but used to be read as one. The rule was a set of substring
#     tests and `benchgate` was one of them, so another agent's `make fmt-check`
#     -- gofumpt, handed every Go file in the repository, including
#     `benchmarks/cmd/benchgate/main.go` -- was a foreign benchmark for as long
#     as it ran, and so were `make bench-lint` and `make budgets-lint`, both of
#     which are in `make ci` and neither of which measures anything. A30's
#     pre-landing gate produced no verdict at all for that reason. The rule now
#     matches tokens: a benchmark selector flag, or the basename of a harness
#     that drives one.
ftable="$(printf '%s\n' \
  '  PID  PPID  PGID COMMAND' \
  '    1     0     1 /sbin/init' \
  '  400   100   400 /bin/bash ./hack/benchguard.sh run -- true' \
  '  500     1   500 gofumpt -extra -l ./message.go ./benchmarks/cmd/benchgate/main.go ./benchmarks/cmd/bakeoff/decide.go' \
  '  600     1   600 go run ./cmd/bakeoff lint -root .. ./...' \
  '  700     1   700 go run ./cmd/benchgate -lint -budgets budgets.yaml' \
  '  800     1   800 go test -run=^$ -bench=BenchmarkX -benchtime=50ms ./internal/queue' \
  '  900     1   900 /tmp/go-build/queue.test -test.bench=. -test.benchtime=50ms' \
  ' 1000     1  1000 /bin/bash ./hack/bakeoff.sh A1 full')"
got="$(printf '%s\n' "$ftable" | "$guard" ownership 400 400 | sed 's/ *$//')"
# 800, 900 and 1000 measure. 500, 600 and 700 name a path or a subcommand that
# merely lives in the benchmark tree, and a guard that calls them benchmarks
# refuses to measure for as long as somebody is formatting the repository.
want='800 900 1000'
if [ "$got" != "$want" ]; then
  bad "a formatter and a linter are not benchmarks" "want [$want], got [$got]"
else
  ok "a formatter and a linter are not benchmarks"
fi

# 8b. The other question the same ownership rule answers, over a table for the
#     same reason: how many of the host's runnable threads are not ours. It is
#     a subtraction, because the process table is namespaced on this host
#     (`ps` sees this container; /proc/loadavg counts the host's 9 000 tasks),
#     so a count of foreign R rows would be blind to the neighbouring
#     containers the guard exists to detect. One row per thread: pid 500 below
#     is one of our processes with two runnable threads and one asleep, and
#     pid 700 a stranger's compiler with two.
ltable="$(printf '%s\n' \
  '  PID  PPID  PGID S COMMAND' \
  '    1     0     1 S /sbin/init' \
  '  100     1   100 S claude --session' \
  '  300   100   300 S /bin/bash ./hack/bakeoff.sh A1 full' \
  '  400   300   300 S /bin/bash ./hack/benchguard.sh run -- go test -bench=X' \
  '  500   400   300 R go test -bench=X ./internal/queue' \
  '  500   400   300 R go test -bench=X ./internal/queue' \
  '  500   400   300 S go test -bench=X ./internal/queue' \
  '  700     1   700 R /usr/bin/cc1plus -O2 neighbour.cc' \
  '  700     1   700 R /usr/bin/cc1plus -O2 neighbour.cc')"
rfail=""
while IFS='|' read -r rtot rwant; do
  [ -z "$rtot" ] && continue
  rgot="$(printf '%s\n' "$ltable" | "$guard" runnable 400 300 "$rtot" 2>&1)"
  [ "$rgot" = "$rwant" ] || rfail="$rfail
       total=$rtot: want [$rwant], got [$rgot]"
done <<'EOF'
34|32
12|10
4|2
2|0
0|0
EOF
# The last two rows are the clamp: our own two runnable threads are subtracted
# from a host total that cannot have been read at the same instant, so the
# answer must be 0 rather than a negative count that compares as quiet.
if [ -n "$rfail" ]; then
  bad "the runnable rule subtracts our own threads from the host's count" "$rfail"
else
  ok "the runnable rule subtracts our own threads from the host's count"
fi

# 9. Ours is decided by descent, not by the words in a command line: with the
#    same table, a guard inside the sibling `make ci` tree must see the
#    bake-off's processes as the foreign ones -- pid 200 included, which is an
#    ancestor from 400's point of view and a stranger driving a bake-off from
#    610's.
got="$(printf '%s\n' "$table" | "$guard" ownership 610 600 | sed 's/ *$//')"
want='200 300 400 500 510 700 800 900'
if [ "$got" != "$want" ]; then
  bad "ownership is symmetric between two agents" "want [$want], got [$got]"
else
  ok "ownership is symmetric between two agents"
fi

# 10. The verdict rule, over a table, so its load half is held to something
#     this host does not have to produce. The first two rows are the ones that
#     matter, and they are the campaign's settings before and after assumption
#     A24.80. Before: a ceiling of 4.0 + 32 = 36 against a measured peak of
#     12.4, because the strict wait held every run until the average had
#     decayed to 4.0 -- the load half could not fire at all, and the peak the
#     sampler collects was compared to nothing. After: a
#     run starts as soon as nothing FOREIGN is running, so it inherits its own
#     predecessor's average -- measured at 33.7 on the third of three
#     back-to-back 80 s runs of 32 pinned threads, 2.3 under that same ceiling
#     -- and `hack/bakeoff.sh` therefore passes twice the pinned width, which
#     is the load a run can explain when the load it starts from is its own
#     too. 33.7 against 68 is clean; 70 is not.
vfail=""
while IFS='|' read -r vpeak vmax vown vfn wv wr wrc vder vown_width; do
  [ -z "$vpeak" ] && continue
  case "$vpeak" in '#'*) continue;; esac
  gotv="$("$guard" verdict "$vpeak" "$vmax" "$vown" "$vfn" "${vder:-0}" "${vown_width:-0}" 2>&1)"; grc=$?
  wantv="verdict=$wv reason=$wr ceiling=$(awk -v a="$vmax" -v o="$vown" 'BEGIN{print a+o}')"
  if [ "$gotv" != "$wantv" ] || [ "$grc" -ne "$wrc" ]; then
    vfail="$vfail
       peak=$vpeak loadmax=$vmax own=$vown foreign=$vfn: want [$wantv] rc=$wrc, got [$gotv] rc=$grc"
  fi
done <<'EOF'
12.4|4.0|32|0|clean|clean|0
33.7|4.0|64|0|clean|clean|0
70.0|4.0|64|0|contended|load|3
12.4|4.0|32|2|contended|foreign|3
40.0|4.0|32|0|contended|load|3
5.0|4.0|0|0|contended|load|3
3.9|4.0|0|0|clean|clean|0
# The 4-CPU runner, where the DERIVED ceiling is 1 and `loadmax + own` is 3
# against the 8.43 our own sixteen threads put in the average. Nothing
# foreign ran; every such run reported `contended reason=load` and the load
# half was firing on the measurement itself. A neighbour
# there is still caught, by the attributed half.
8.43|1|2|0|clean|clean|0|1|16
8.43|1|2|1|contended|foreign|3|1|16
# The same numbers with the ceiling ASKED for rather than derived: an
# explicit -t is obeyed, so the load half fires.
8.43|1|2|0|contended|load|3|0|16
0.00|4.0|-100000|0|contended|load|3
EOF
if [ -n "$vfail" ]; then
  bad "the verdict rule reads a load table correctly" "$vfail"
else
  ok "the verdict rule reads a load table correctly"
fi

# 10b. The PRE-RUN gate, over a table, for the reason case 21 cannot be the
#      only check on it: case 21 needs an idle host SMALL enough for the
#      derived ceiling to sit under a measurement's own width, and on a busy
#      host it skips -- so the defect it exists to catch (the guard waiting for
#      load that is its own) rode into CI green on every machine except the one
#      that found it. Row 1 is the GitHub runner's own numbers, from the run
#      that failed: nproc=4 gives a derived ceiling of 1, a 16-thread
#      generator lifts the average to 1.64, nothing foreign is runnable, and
#      the guard waited 99 s for nobody. Row 2 is the same ceiling asked for
#      with -t, which is an instruction and must still be obeyed.
gfail=""
while IFS='|' read -r gf gl glmax gder gown gfrun grmax wg wr wrc; do
  [ -z "$gl" ] && continue
  [ "$gf" = "-" ] && gf=""
  gotg="$("$guard" gate "$gf" "$gl" "$glmax" "$gder" "$gown" "$gfrun" "$grmax" 2>&1)"; grc2=$?
  wantg="gate=$wg reason=$wr"
  if [ "$gotg" != "$wantg" ] || [ "$grc2" -ne "$wrc" ]; then
    gfail="$gfail
       foreign=[$gf] load1=$gl loadmax=$glmax derived=$gder own=$gown frun=$gfrun runmax=$grmax: want [$wantg] rc=$wrc, got [$gotg] rc=$grc2"
  fi
done <<'EOF'
-|1.64|1|1|16|0|1|measure|ok|0
-|1.64|1|0|16|0|1|wait|load|3
-|8.01|1|1|16|0|1|measure|ok|0
-|60|48|1|16|0|6|wait|load|3
-|33.7|48|1|16|0|6|measure|ok|0
-|0.1|48|1|16|12|6|wait|foreign_runnable|3
-|0.1|1|1|16|12|1|wait|foreign_runnable|3
1234|0.1|48|1|16|0|6|wait|foreign_benchmark|3
1234|1.64|1|1|16|0|1|wait|foreign_benchmark|3
-|0.1|16|1|16|0|6|measure|ok|0
-|20|16|1|16|0|6|wait|load|3
-|20|15|1|16|0|6|measure|ok|0
-|20|17|1|16|0|6|wait|load|3
EOF
if [ -n "$gfail" ]; then
  bad "the pre-run gate reads a table correctly" "$gfail"
else
  ok "the pre-run gate reads a table correctly"
fi

# 11b. The sampler is the only thing that can see a benchmark which starts and
#      finishes inside our run: it is gone before load_after and
#      foreign_after are read. No case exercised the sampler at all, so its
#      five-second loop could have been deleted with every case still green.
#      Smoke mode, so the pre-run wait cannot refuse for the host's own
#      traffic and the sampler's detection is what is under test.
rep="$tmp/sampler.json"
rm -f "$tmp/transient.pid"
out="$("$guard" run -s -t 99999 -w 0 -o "$rep" -- bash -c "$transient_cmd" _ "$tmp/transient.pid" 2>&1)"
rc=$?
tpid="$(cat "$tmp/transient.pid" 2>/dev/null)"
seen="$(sed -n 's/.*"foreign_pids":"\([^"]*\)".*/\1/p' "$rep" 2>/dev/null)"
if [ -z "$tpid" ]; then
  bad "the sampler catches a benchmark that only ran mid-run" "the transient process did not record its pid: $out"
elif kill -0 "$tpid" 2>/dev/null; then
  kill "$tpid" 2>/dev/null
  bad "the sampler catches a benchmark that only ran mid-run" "the transient process (pid $tpid) outlived the run, so foreign_after could have found it and the sampler is not what is being tested"
elif ! printf ' %s ' "$seen" | grep -q " $tpid "; then
  bad "the sampler catches a benchmark that only ran mid-run" "pid $tpid is not among foreign_pids=[$seen]: $out"
elif ! printf '%s' "$out" | grep -q 'verdict=contended reason=foreign'; then
  bad "the sampler catches a benchmark that only ran mid-run" "verdict/reason not recorded: $out"
elif [ "$rc" -ne 0 ]; then
  bad "the sampler catches a benchmark that only ran mid-run" "smoke mode must still exit 0, got $rc: $out"
else
  ok "the sampler catches a benchmark that only ran mid-run"
fi

# 11. And strictly: a run contended by something only the sampler saw exits 3
#     -- the contended-strict path, reached through a command that really ran.
#     Before, the only case that reached exit 3 was case 4, where the guard
#     refused to start and measured nothing at all.
#     The strict pre-run wait refuses (5) when the host itself is running a
#     benchmark as the case starts, which is not something the case can
#     arrange away; it says so rather than passing.
rep="$tmp/strict.json"
rm -f "$tmp/transient2.pid"
out="$("$guard" run -t 99999 -w 0 -o "$rep" -- bash -c "$transient_cmd" _ "$tmp/transient2.pid" 2>&1)"
rc=$?
tpid="$(cat "$tmp/transient2.pid" 2>/dev/null)"
[ -n "$tpid" ] && kill "$tpid" 2>/dev/null
seen="$(sed -n 's/.*"foreign_pids":"\([^"]*\)".*/\1/p' "$rep" 2>/dev/null)"
if [ "$rc" -eq 5 ]; then
  # Either half of the pre-run wait can produce this: a benchmark of the host's
  # own, or foreign runnable threads at the ceiling. Both are correct answers
  # for a busy host and neither is arrangeable away from inside the case.
  noted "a contended strict run exits 3" "the strict contended exit -- the host was busy when the case started (a benchmark of its own, or foreign runnable threads at the ceiling), so the guard refused before measuring, which is correct (case 11b covers the sampler and case 10 the rule): $out"
elif [ -z "$tpid" ]; then
  bad "a contended strict run exits 3" "the transient process did not record its pid: $out"
elif [ "$rc" -ne 3 ]; then
  bad "a contended strict run exits 3" "expected exit 3 (ran, and was contended), got $rc: $out"
elif ! printf ' %s ' "$seen" | grep -q " $tpid "; then
  # Insisting on this pid, not merely on a contended verdict: whatever else
  # the host is running would otherwise pass the case for us.
  bad "a contended strict run exits 3" "exit 3 but pid $tpid is not what made it contended: foreign_pids=[$seen] $out"
elif ! printf '%s' "$out" | grep -q 'verdict=contended reason=foreign'; then
  bad "a contended strict run exits 3" "verdict/reason not recorded: $out"
elif ! grep -q '"status":0' "$rep"; then
  bad "a contended strict run exits 3" "exit 3 must mean the command ran and succeeded: $(cat "$rep")"
else
  ok "a contended strict run exits 3"
fi

# 12. Nothing the guard starts outlives it, and the host-wide lock is never
#     left held. Signalling the guard used to orphan the load sampler and the
#     measured command, both of which had inherited the lock file descriptor:
#     an immortal sampler then held this host's benchmark lock for sixteen
#     minutes after its parent died, with nothing measuring (assumption
#     A24.55). The guard is put in a session of its own so that its survivors
#     can be found by process group instead of by a command-line pattern.
rm -f "$tmp/sig.pid" "$tmp/sig.lock"
setsid --fork bash -c 'echo $$ > "$1/sig.pid"; exec env IQMQ_BENCH_LOCK="$1/sig.lock" IQMQ_BENCH_LOCK_HELD=0 "$2" run -s -t 99999 -w 0 -- sleep 60' \
  _ "$tmp" "$guard" > "$tmp/sig.out" 2>&1
gpid=""
for _ in 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15; do
  if [ -s "$tmp/sig.pid" ]; then gpid="$(cat "$tmp/sig.pid")"; break; fi
  sleep 0.2
done
case "$gpid" in ''|*[!0-9]*) gpid="";; esac
if [ -z "$gpid" ]; then
  bad "a signalled guard leaves no sampler, no command and no held lock" "the guard did not record its pid"
else
  # Long enough to have taken the lock and started both children.
  sleep 3
  before_members="$(pgroup_members "$gpid")"
  kill -TERM "$gpid" 2>/dev/null
  for _ in 1 2 3 4 5 6 7 8 9 10; do
    kill -0 "$gpid" 2>/dev/null || break
    sleep 0.3
  done
  sleep 1
  survivors="$(pgroup_members "$gpid")"
  locked=no
  flock -n "$tmp/sig.lock" -c true 2>/dev/null || locked=yes
  if [ -n "${survivors// /}" ]; then
    for s in $survivors; do kill -9 "$s" 2>/dev/null; done
    bad "a signalled guard leaves no sampler, no command and no held lock" \
      "process group $gpid still holds [$survivors] (it held [$before_members] while running)"
  elif [ "$locked" = yes ]; then
    bad "a signalled guard leaves no sampler, no command and no held lock" \
      "the host benchmark lock $tmp/sig.lock is still held after the guard died"
  else
    ok "a signalled guard leaves no sampler, no command and no held lock"
  fi
fi

# 13. IQMQ_BENCH_LOCK_HELD is a claim by the caller, and it is inherited by
#     everything under the guard, so it used to be believed by runs that held
#     nothing at all: a measurement then ran unserialised against every other
#     agent on this machine while reporting a clean verdict. A false claim must
#     be caught and the lock taken; a true one must still be honoured, or the
#     guard deadlocks against its own caller.
rm -f "$tmp/false.lock"
out="$(IQMQ_BENCH_LOCK="$tmp/false.lock" IQMQ_BENCH_LOCK_HELD=1 "$guard" run -s -t 99999 -w 0 -- true 2>&1)"; rc=$?
rm -f "$tmp/held.lock"
out2="$(flock "$tmp/held.lock" -c "IQMQ_BENCH_LOCK='$tmp/held.lock' IQMQ_BENCH_LOCK_HELD=1 '$guard' run -s -t 99999 -w 0 -- true" 2>&1)"; rc2=$?
if [ "$rc" -ne 0 ]; then
  bad "a false claim to hold the host lock is caught" "expected exit 0, got $rc: $out"
elif ! printf '%s' "$out" | grep -q 'not held by anyone; taking it'; then
  bad "a false claim to hold the host lock is caught" "the guard believed a lock nobody held: $out"
elif [ "$rc2" -ne 0 ]; then
  bad "a false claim to hold the host lock is caught" "a caller that really holds the lock was not honoured (exit $rc2): $out2"
elif printf '%s' "$out2" | grep -q 'not held by anyone'; then
  bad "a false claim to hold the host lock is caught" "a lock genuinely held by the caller was reported free: $out2"
else
  ok "a false claim to hold the host lock is caught"
fi

# 14. The campaign must not wait for itself. A measurement takes at most
#     IQMQ_BENCH_MAX_CPUS CPUs and both ceilings are `nproc - max_cpus`, so a
#     run's own footprint cannot reach the ceiling its successor is held to --
#     which is the whole reason those two numbers are defined against each
#     other. The stand-in is therefore exactly
#     one measurement's width, not more: it drives the 1-minute average up, it
#     EXITS, and the guard must start at once on an average that is still
#     decaying, because an exited process contributes nothing to the run queue
#     and everything to the average for the next minute. Against the 4.0
#     ceiling this used to be measured under, this waits the full -w and
#     exits 5.
lt=""
own_width="${IQMQ_BENCH_MAX_CPUS:-${IQMQ_MAX_CPUS:-16}}"
if ! start_spinners "$own_width"; then
  bad "a strict run does not wait for its own decayed load" "could not start the load generator"
else
  t0=$(date +%s)
  while :; do
    lt="$(cut -d' ' -f1 /proc/loadavg)"
    awk -v a="$lt" -v w="$own_width" 'BEGIN{exit !(a >= w/2)}' && break
    [ $(( $(date +%s) - t0 )) -ge 60 ] && break
    sleep 2
  done
  stop_spinners
  # Whether the host has foreign work of its own is asked BEFORE the run and
  # with a separate reading, not read off the guard's own refusal: a guard that
  # counted our own dead generator as foreign would refuse and say
  # `foreign_runnable=48`, and a case that believed that message would report
  # the defect it exists to catch as a fact about the host.
  pre="$("$guard" check -t 99999 2>&1)"
  pfr="$(printf '%s\n' "$pre" | sed -n 's/.*foreign_runnable=\([0-9]*\).*/\1/p')"
  pceil="$(printf '%s\n' "$pre" | sed -n 's/.*runnable_ceiling=\([0-9]*\).*/\1/p')"
  lt="$(cut -d' ' -f1 /proc/loadavg)"
  t0=$(date +%s)
  out="$("$guard" run -w 90 -o "$tmp/selfwait.json" -- true 2>&1)"; rc=$?
  el=$(( $(date +%s) - t0 ))
  lb="$(printf '%s' "$out" | sed -n 's/.*load_before=\([^ ]*\).*/\1/p')"
  fb="$(printf '%s' "$out" | sed -n 's/.*foreign_runnable_before=\([0-9]*\).*/\1/p')"
  if [ -z "$pfr" ] || [ -z "$pceil" ]; then
    bad "a strict run does not wait for its own decayed load" "check did not report the foreign runnable count and its ceiling: $pre"
  elif [ "$pfr" -ge "$pceil" ]; then
    # Something foreign was on the run queue once our generator had gone, so
    # waiting is the correct answer and this case cannot see its own subject.
    # Case 15 holds the other direction, that the guard waits at all.
    noted "a strict run does not wait for its own decayed load" "the self-wait -- the host had $pfr foreign runnable thread(s) of its own against a ceiling of $pceil once the generator had exited, for which waiting is correct: $pre"
  elif awk -v a="$lt" -v w="$own_width" 'BEGIN{exit !(a < w/2)}'; then
    noted "a strict run does not wait for its own decayed load" "the self-wait -- $own_width threads did not lift the 1-minute average to half a measurement's own width within 60 s (load1=$lt), so there was no decaying load to start on"
  elif printf '%s' "$out" | grep -q 'foreign=\[[0-9]'; then
    # A foreign BENCHMARK appeared, so the guard waited on that half and this
    # case cannot see its own subject. The case already skips when the host
    # has foreign runnable threads; a foreign benchmark is the same kind of
    # fact about the machine and was the one branch it lacked.
    noted "a strict run does not wait for its own decayed load" "the self-wait -- a benchmark of the host's own ran during the case, for which waiting is correct: $(printf '%s' "$out" | sed -n 's/.*\(foreign=\[[^]]*\]\).*/\1/p' | head -1)"
  elif [ "$rc" -ne 0 ]; then
    bad "a strict run does not wait for its own decayed load" "the 1-minute average was $lt and only $pfr thread(s) of the host's own were runnable, so the guard should have measured at once; it exited $rc after ${el}s: $out"
  elif [ "$el" -gt 20 ]; then
    bad "a strict run does not wait for its own decayed load" "the guard waited ${el}s for its own decaying load (load1=$lt at the start, $pfr foreign runnable thread(s), load_before=$lb): $out"
  elif [ -z "$lb" ] || awk -v a="$lb" -v w="$own_width" 'BEGIN{exit !(a < w/2)}'; then
    noted "a strict run does not wait for its own decayed load" "the self-wait -- the average had already decayed to load_before=$lb, below half a measurement's own width, by the time the guard sampled it"
  elif [ -z "$fb" ]; then
    bad "a strict run does not wait for its own decayed load" "the run did not record the foreign runnable count it decided on: $out"
  elif ! grep -q '"foreign_runnable_before":' "$tmp/selfwait.json" 2>/dev/null; then
    bad "a strict run does not wait for its own decayed load" "the JSON report does not record the foreign component: $(cat "$tmp/selfwait.json" 2>/dev/null)"
  else
    ok "a strict run does not wait for its own decayed load (${el}s at load_before=$lb, foreign_runnable_before=$fb)"
  fi
fi

# 15. And the other direction, without which case 14 would be satisfied by a
#     guard that never waits at all: work that is genuinely not ours must still
#     hold a strict run off. The stand-in is detached with `setsid --fork`, so
#     ownership by descent cannot claim it, and its command line mentions
#     nothing the foreign-benchmark half matches on -- so the refusal has to
#     come from the runnable half. -t 99999 for the same reason: the load
#     ceiling must not be what fires, and -r 4 so that the case does not depend
#     on the host being small enough for twelve threads to clear a ceiling
#     derived from its CPU count.
if ! start_spinners 12; then
  bad "a genuine neighbour still holds a strict run off" "could not start the load generator"
else
  # -r 4 rather than the derived default: the ceiling is `nproc - max_cpus`,
  # which on a large host is far above twelve spinners, and this case is about
  # whether the gate fires at all -- not about how big the machine is. -t 99999
  # still keeps the load half from being what fires.
  out="$("$guard" run -t 99999 -r 4 -w 0 -o "$tmp/neighbour.json" -- true 2>&1)"; rc=$?
  wfr="$(printf '%s' "$out" | sed -n 's/.*foreign_runnable=\([0-9]*\).*/\1/p')"
  wceil="$(printf '%s' "$out" | sed -n 's/.*runnable_ceiling=\([0-9]*\).*/\1/p')"
  fseen="$(printf '%s' "$out" | sed -n 's/.*foreign=\[\([^]]*\)\].*/\1/p')"
  stop_spinners
  if [ "$rc" -ne 5 ]; then
    bad "a genuine neighbour still holds a strict run off" "12 foreign runnable threads must refuse a strict run before it starts (exit 5); got $rc: $out"
  elif ! printf '%s' "$out" | grep -q 'refused to measure'; then
    bad "a genuine neighbour still holds a strict run off" "the refusal is not named in: $out"
  elif [ -z "$wfr" ] || [ -z "$wceil" ] || [ "$wfr" -lt "$wceil" ]; then
    bad "a genuine neighbour still holds a strict run off" "the refusal must name the foreign runnable count that caused it, at or above the ceiling; got foreign_runnable=[$wfr] ceiling=[$wceil]: $out"
  elif [ -n "${fseen// /}" ]; then
    # Not a failure of the guard, but the case has not exercised what it is
    # for: a foreign benchmark was also present, and that half would have
    # refused on its own (case 4 is that half).
    noted "a genuine neighbour still holds a strict run off" "the runnable half in isolation -- a foreign benchmark [$fseen] was running too, and case 4 is that half"
  else
    ok "a genuine neighbour still holds a strict run off ($wfr foreign runnable thread(s), nothing foreign that looks like a benchmark)"
  fi
fi

# 16. The statistic the SWAMPED rule decides on, over a table, because it is a
#     rule about the tail of a distribution and this host cannot be asked for a
#     distribution on demand. The property that matters most is the degenerate
#     one: nearest rank makes p95 == max exactly while n < 20, so a window too
#     short for a percentile to mean anything is judged on its maximum, which
#     is what the rule did for every window before assumption B11.13. The rows
#     with n=72 are a `pr` gate window: one five-second spike must not discard
#     six minutes, and a run that competed for most of its length must still
#     fail.
pfail=""
pcase() { # LABEL SERIES P WANT
  local got
  got="$(printf '%s\n' "$2" | "$guard" percentile "$3" 2>&1)" || got="exit$?"
  [ "$got" = "$4" ] || pfail="$pfail
       $1: percentile $3 of [$2] = [$got], want [$4]"
}
pcase "one sample"            "7" 95 7
pcase "n=19 is its own max"   "$(seq 1 19 | tr '\n' ' ')" 95 19
pcase "n=20 drops the top"    "$(seq 1 20 | tr '\n' ' ')" 95 19
pcase "n=21 drops the top"    "$(seq 1 21 | tr '\n' ' ')" 95 20
pcase "n=72 drops the top 3"  "$(seq 1 72 | tr '\n' ' ')" 95 69
pcase "a pr window with one spike" "$(printf '10 %.0s' $(seq 1 71))200" 95 10
pcase "a pr window mostly busy"    "$(printf '60 %.0s' $(seq 1 60))$(printf '5 %.0s' $(seq 1 12))" 95 60
pcase "p100 is the max"       "1 2 3 400" 100 400
pcase "an empty series is not a percentile" "" 95 exit1
# The DERIVED CEILING itself, against a stubbed cpu-policy, because nothing else
# here can see it collapse.
#
# This case exists for a regression that happened: `policy_bench_load` read
# `limits.bench_load`, cpu-policy 4.0.0 deleted that key, and the ceiling fell
# silently back to `max(1, nproc - 16)` = 1 -- the figure assumption C29.2 exists to
# have removed. The suite stayed GREEN through it, because the three cases that
# would have noticed are host-dependent and `noted()` skipped them on a quiet
# machine. A ceiling nothing asserts is one that can return to 1 unobserved, so it
# is asserted here, deterministically, with no dependence on what the host is doing.
hcase() { # NAME STATUS_JSON WANT   (WANT=warn means: fall back AND say so)
  local name="$1" js="$2" want="$3" stub="$tmp/hstub" got rc warned
  mkdir -p "$stub"
  {
    printf '#!/bin/sh\n'
    printf 'if [ "$1" = status ]; then cat <<%s\n%s\n%s\nfi\n' "'JSONEOF'" "$js" "JSONEOF"
    printf 'exit 0\n'
  } > "$stub/cpu-policy"
  chmod +x "$stub/cpu-policy"
  got="$(PATH="$stub:$PATH" "$guard" headroom 2>"$tmp/hwarn")"; rc=$?
  warned=0; grep -q 'reports no ceiling' "$tmp/hwarn" 2>/dev/null && warned=1
  rm -rf "$stub"
  if [ "$rc" -ne 0 ]; then hfail="$hfail
     $name: headroom exited $rc"; return; fi
  case "$want" in
    warn)
      [ "$warned" = 1 ] || hfail="$hfail
     $name: an unreadable status must warn on stderr; it did not (got [$got])" ;;
    *)
      [ "$got" = "$want" ] || hfail="$hfail
     $name: headroom = [$got], want [$want]"
      [ "$warned" = 0 ] || hfail="$hfail
     $name: a readable status must not warn; it did" ;;
  esac
}
hfail=""
# v5 removed `capacity` on purpose and replaced the model: a bench job holds 24
# CPUs of its own and everything foreign runs on the light CPUs, so `light.count`
# IS the ceiling. These cases exist because cpu-policy reached 5.1.0 while this
# branch was open and the warning above is what caught it -- the third schema
# move under this one figure, and the first one a test was watching for.
hcase "v5 light.count"     '{"version":"5.1.0","light":{"count":72,"max":72,"min":36},"heavy":{"cpus_each":24,"running":0}}' 72
hcase "v5 beside a heavy job" '{"version":"5.1.0","light":{"count":60,"max":72,"min":36},"heavy":{"cpus_each":24,"running":1}}' 60
hcase "v5 at the floor"    '{"version":"5.1.0","light":{"count":36,"max":72,"min":36},"heavy":{"cpus_each":24,"running":2}}'  36
# max without count, in case a future status reports only the static figure.
hcase "v5 light.max only"  '{"version":"5.1.0","light":{"max":72,"min":36}}'                            72
# A count of zero is not a ceiling of zero: every light CPU being taken is a
# reason to WAIT, and reading it as "tolerate no foreign thread" would make the
# guard refuse for ever. It falls through and warns.
hcase "v5 light.count zero" '{"version":"5.1.0","light":{"count":0,"min":36},"heavy":{"cpus_each":24}}'  warn
# v4 and later: the capacity the host admits, less what a heavy job of ours takes.
hcase "v4 capacity"        '{"version":"4.0.0","capacity":{"cpus":56,"heavy_cpus":24,"light_cpus":16}}' 32
hcase "v4 a smaller host"  '{"version":"4.0.0","capacity":{"cpus":24,"heavy_cpus":12,"light_cpus":8}}'  12
# v2 and v3 said it directly, and must keep working: a collaborator may have either.
hcase "v2 bench_load"      '{"version":"2.0.0","limits":{"load":32.0,"bench_load":24.0,"psi":25.0}}'    24
# A shape this script cannot read must be LOUD, never a silent 1.
hcase "an unknown shape"   '{"version":"9.9.9","limits":{"load":48.0,"psi":25.0}}'                      warn
hcase "capacity without heavy_cpus" '{"version":"5.0.0","capacity":{"cpus":56}}'                        warn
hcase "status that is not json"     'not json at all'                                                   warn
if [ -n "$hfail" ]; then
  bad "the derived ceiling reads every known status shape and is loud about the rest" "$hfail"
else
  ok "the derived ceiling reads every known status shape and is loud about the rest"
fi

if [ -n "$pfail" ]; then
  bad "the nearest-rank percentile reads a table correctly" "$pfail"
else
  ok "the nearest-rank percentile reads a table correctly"
fi

# 17. And the decision the gate takes from it, over stored reports, because
#     that is the interface hack/benchgate.sh and hack/bench/run-local.sh use.
#     Three cases and all three are the point: a long window with one spike is
#     KEPT (the check that could not pass --), a long window
#     that competed for most of its length is REFUSED, and a report written
#     before the series existed is decided on its maximum and says so rather
#     than being read as quiet.
report() { # PATH SERIES MAX CEILING
  local ser
  ser="$(printf '%s\n' "$2" | tr -s ' ' ',' | sed 's/^,//; s/,$//')"
  printf '{"verdict":"clean","foreign_runnable_max":%s,"foreign_runnable_series":[%s],"runnable_ceiling":%s}\n' \
    "$3" "$ser" "$4" > "$1"
}
sfail=""
scase() { # LABEL WANT_RC WANT_SUBSTRING
  local got rc=0
  got="$("$guard" swamped "$tmp/sw.json" 2>&1)" || rc=$?
  if [ "$rc" -ne "$2" ] || ! printf '%s' "$got" | grep -q -- "$3"; then
    sfail="$sfail
       $1: want exit $2 and [$3], got exit $rc: $got"
  fi
}
report "$tmp/sw.json" "$(printf '10 %.0s' $(seq 1 71))200" 200 48
scase "one spike in 72 samples is kept" 0 "swamped=no"
report "$tmp/sw.json" "$(printf '60 %.0s' $(seq 1 60))$(printf '5 %.0s' $(seq 1 12))" 60 48
scase "a window that competed for most of its length is refused" 3 "swamped=yes"
report "$tmp/sw.json" "60 60 60" 60 48
scase "a short window is still judged on its maximum" 3 "swamped=yes"
printf '{"verdict":"clean","foreign_runnable_max":54,"runnable_ceiling":48}\n' > "$tmp/sw.json"
scase "a report with no series is decided on the max and says so" 3 "source=max-only"
printf '{"verdict":"clean","foreign_runnable_max":15,"runnable_ceiling":48}\n' > "$tmp/sw.json"
scase "a pre-series report inside the ceiling was not swamped either way" 0 "source=max-only"
if [ -n "$sfail" ]; then
  bad "the swamped rule decides a stored report correctly" "$sfail"
else
  ok "the swamped rule decides a stored report correctly"
fi

# 18. The audit that produced assumption B11.13's evidence table. It classifies
#     stored reports the rule cannot re-decide -- 985 of them carry a maximum
#     and no series -- and the classification is the argument for the rule, so
#     a wrong one would be a wrong argument: a record inside the ceiling passed
#     under both rules, a record over it from a window of fewer than twenty
#     samples is still swamped, and a record over it from a longer window is
#     NOT RECOVERABLE and must say so rather than be counted either way.
mkdir -p "$tmp/audit/inside" "$tmp/audit/short" "$tmp/audit/long"
printf '{"foreign_runnable_max":15,"runnable_ceiling":48,"duration_s":23}\n'    > "$tmp/audit/inside/guard-1.json"
printf '{"foreign_runnable_max":69,"runnable_ceiling":48,"duration_s":24}\n'    > "$tmp/audit/short/guard-1.json"
printf '{"foreign_runnable_max":54,"runnable_ceiling":48,"duration_s":1189}\n'  > "$tmp/audit/long/guard-1.json"
aout="$("$guard" swamped-audit "$tmp/audit" 2>&1)"; arc=$?
afail=""
[ "$arc" -eq 0 ] || afail="$afail
       the audit exited $arc"
for want in "inside	1	0	15	0	0	pass" "short	1	1	69	24	5	swamped" "long	1	1	54	1189	238	unknown"; do
  printf '%s\n' "$aout" | grep -qF "$want" || afail="$afail
       no row [$want] in:
$aout"
done
if [ -n "$afail" ]; then
  bad "the swamped audit classifies stored reports correctly" "$afail"
else
  ok "the swamped audit classifies stored reports correctly"
fi

printf '\nbenchguard_test: %d passed, %d failed, %d skipped\n' "$pass" "$fail" "$skip"
[ "$fail" -eq 0 ]
