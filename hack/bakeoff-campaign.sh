#
#   hack/bakeoff-campaign.sh [probe|quick|full] [FAMILY...]
#
# With no families it runs every hack/bakeoff/*.env. Each family is measured by
# hack/bakeoff.sh, which holds the host-wide benchmark lock and waits for the
# load to drop, so families run one at a time by construction -- and nothing
# else on this host may benchmark while the campaign is running.
#
# The campaign is resumable and idempotent: a family that has completed is
# skipped on a later run unless FORCE=1. A family that fails is recorded and
# the campaign continues, because one broken family must not cost the other
# thirty-six their measurement.
#
# It is meant to run for a day unattended, so:
#   - Ctrl-C (or SIGTERM/SIGHUP) stops it. It used to be swallowed: bash defers
#     a signal that arrives while a foreground child runs, and with the child
#     exiting normally the pending SIGINT was discarded -- the loop went on to
#     the next family and finished with exit 0. The family now runs in the
#     background under `wait`, so the trap fires while it is still running and
#     takes its whole process tree with it.
#   - every family is bounded by a timeout (IQMQ_BENCH_FAMILY_TIMEOUT, or the
#     per-profile default below), so a hung family costs hours rather than the
#     rest of the campaign.
#   - a retried family's log is kept, not overwritten. The first attempt's log
#     is the evidence of why it was retried.
#   - the manifest records every family's contention counts, failures included:
#     it used to parse the result directory out of the family's "done ->" line,
#     which only a successful family prints, so exactly the families whose
#     counts matter contributed none. hack/bakeoff.sh now announces the
#     directory before anything can fail.
#   - RESUME=1 continues an interrupted family from the (count, kind) pairs it
#     had already measured instead of starting it again.
#
# No profile renders an ADR. Nine families share ADR-014 and seven share
# ADR-017, so the ADRs are rendered once when the campaign is over, from the
# per-family scorecards it stores:
#
#   make bakeoff-adr        # (cd benchmarks && go run ./cmd/bakeoff adr)
#
set -uo pipefail
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
bakeoff="${BAKEOFF:-$root/hack/bakeoff.sh}"
profile="${1:-probe}"; shift || true
case "$profile" in probe|quick|full) ;; *) echo "usage: $0 [probe|quick|full] [FAMILY...]" >&2; exit 2 ;; esac

if [ $# -gt 0 ]; then
  families=("$@")
else
  # A-rows before B-rows, numerically within each, so a partial campaign is
  # always a prefix of the same order and the log reads the way the plan does.
  mapfile -t families < <(cd "$root/hack/bakeoff" && ls *.env | sed 's/\.env$//' \
    | sed 's/^\([AB]\)\([0-9]*\)$/\1 \2/' | sort -k1,1 -k2,2n | tr -d ' ')
fi

# A family's ceiling. The widest full-profile family is 144 benchmarks x 4
# GOMAXPROCS rungs x 1 s x 10 counts = 1.6 h of measured time, roughly 3.2 h
# with `go test`'s warm-up ramp and the per-benchmark fixtures, and the strict
# guard's pre-run wait adds something like 60 % on this host (assumption
# A24.32), so six hours bounds it with margin: a family that exceeds it is
# hung, not slow. Nothing bounded a family before, and a day-long campaign has
# to be able to get past one.
case "$profile" in
  probe) fam_timeout="${IQMQ_BENCH_FAMILY_TIMEOUT:-1800}" ;;
  quick) fam_timeout="${IQMQ_BENCH_FAMILY_TIMEOUT:-3600}" ;;
  # 1800, not 21600: the campaign's whole budget is an hour, so a six-hour
  # per-family wall clock bounded nothing it could reach. The slowest family
  # measured is A4 at 1130 s, where its own estimate was 85 s -- see the note
  # on IQMQ_BENCH_FAMILY_MAX_S in hack/bakeoff.sh for why the estimate is not
  # a prediction.
  full)  fam_timeout="${IQMQ_BENCH_FAMILY_TIMEOUT:-1800}" ;;
esac

state="$root/benchmarks/reports/campaign/$profile"
mkdir -p "$state/logs"
manifest="$state/manifest.tsv"
header=$'family\tstatus\tattempt\tseconds\tmatrix\tcontended\tlost\trefused\tfailed\trun_id'
if [ ! -f "$manifest" ]; then
  printf '%s\n' "$header" > "$manifest"
elif [ "$(head -n1 "$manifest")" != "$header" ]; then
  # Keep the old manifest rather than appending rows with different columns to
  # it: it is somebody's evidence of an earlier campaign.
  mv "$manifest" "$manifest.$(date -u +%Y%m%dT%H%M%SZ)"
  printf '%s\n' "$header" > "$manifest"
fi

# Nothing this campaign starts outlives it. kill_tree walks `ps --ppid` rather
# than matching a command line: a pattern that matches a benchmark also matches
# the shell that typed it, and that has killed an agent's own shell twice.
child=""; current=""
kill_tree() {
  local p="$1" sig="${2:-TERM}" kid
  for kid in $(ps -o pid= --ppid "$p" 2>/dev/null); do kill_tree "$kid" "$sig"; done
  kill -"$sig" "$p" 2>/dev/null || true
}
on_signal() {
  trap - INT TERM HUP
  echo >&2
  echo "campaign: signalled; stopping${current:+ during $current}" >&2
  if [ -n "$child" ]; then
    kill_tree "$child" TERM
    # Give hack/bakeoff.sh's own trap a moment to release the host-wide lock
    # and stop the measured `go test`, then insist.
    for _ in 1 2 3 4 5 6 7 8 9 10; do kill -0 "$child" 2>/dev/null || break; sleep 0.5; done
    kill -0 "$child" 2>/dev/null && kill_tree "$child" KILL
  fi
  if [ -n "$current" ]; then
    printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
      "$current" "interrupted" "${attempt:-1}" "$(( $(date +%s) - ${t0:-0} ))" "-" "-" "-" "-" "-" "-" >> "$manifest"
  fi
  exit 130
}
trap on_signal INT TERM HUP

started="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
echo "campaign: profile=$profile families=${#families[@]} started=$started"
echo "campaign: state=$state family_timeout=${fam_timeout}s${RESUME:+ resume=on}"
# A hard total budget, spent rather than estimated. `hack/bakeoff.sh` already
# refuses a family whose own cost exceeds IQMQ_BENCH_FAMILY_MAX_S, so the
# campaign cannot run away one family at a time; this is what stops it running
# away thirty-eight families at a time. It is a running total of measured
# seconds, so it cannot be wrong the way an estimate can, and the families it
# does not reach are recorded as unmeasured rather than silently dropped -- the
# campaign is resumable, so the next run continues where the budget ran out.
total_max="${IQMQ_BENCH_TOTAL_MAX_S:-3600}"
spent=0; budgeted=0
# A probe measures nothing, so it has nothing to spend. Said with its own flag
# rather than by setting the budget to zero: a budget of zero has to keep
# meaning "measure nothing", or a typo in IQMQ_BENCH_TOTAL_MAX_S would remove
# the guard instead of tightening it.
budgeted_profile=1
[ "$profile" = probe ] && budgeted_profile=0
if [ "$budgeted_profile" = 1 ]; then
  echo "campaign: budget=${total_max}s total"
else
  echo "campaign: no budget (a probe measures nothing)"
fi
ok=0; failed=0; skipped=0
# The grid a family declares, so that a completed family is only skipped if it
# was completed on the SAME grid. A <family>.ok left by a campaign that
# measured the smoke grid must not satisfy one that needs the full sweep, and
# the thirty-eight .ok files this repository already carries record no grid at
# all -- taken before anything selected one -- so they never skip.
declared_matrix() { sed -n 's/^[[:space:]]*MATRIX=//p' "$root/hack/bakeoff/$1.env" 2>/dev/null | tail -n1; }
for family in "${families[@]}"; do
  if [ "$budgeted_profile" = 1 ] && [ "$spent" -ge "$total_max" ]; then
    echo "campaign: $family not measured -- the ${total_max}s budget is spent (${spent}s)"
    printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' "$family" "budget" 0 0 "$(declared_matrix "$family")" 0 0 0 0 "-" >> "$manifest"
    budgeted=$((budgeted+1)); continue
  fi
  if [ -f "$state/$family.ok" ] && [ "${FORCE:-0}" != 1 ]; then
    prev_matrix="$(sed -n 's/^matrix=//p' "$state/$family.ok")"
    want_matrix="$(declared_matrix "$family")"; want_matrix="${want_matrix:-${MATRIX:-}}"
    if [ -n "$prev_matrix" ] && [ "$prev_matrix" = "$want_matrix" ]; then
      echo "campaign: $family already done on matrix=$prev_matrix (FORCE=1 to repeat)"; skipped=$((skipped+1)); continue
    fi
    echo "campaign: $family completed on matrix=${prev_matrix:-<unrecorded>} but now needs ${want_matrix:-<undeclared>}; re-running"
  fi
  # A retried family keeps the evidence of its earlier attempts: the log used
  # to be overwritten, so the reason for the first failure was gone by the
  # time anyone read the second.
  log="$state/logs/$family.log"
  attempt=1
  while [ -f "$state/logs/$family.$attempt.log" ]; do attempt=$((attempt+1)); done
  if [ -f "$log" ]; then mv "$log" "$state/logs/$family.$attempt.log"; attempt=$((attempt+1)); fi
  current="$family"
  echo "campaign: $family starting attempt $attempt $(date -u +%H:%M:%SZ)"
  t0=$(date +%s)
  # Backgrounded and waited for, so a signal reaches the trap above while the
  # family is still measuring rather than after it. `timeout` bounds a hung
  # family; -k gives hack/bakeoff.sh's trap time to release the host lock
  # before SIGKILL.
  if [ -n "${RESUME:-}" ]; then
    RESUME=auto timeout -k 60 "$fam_timeout" "$bakeoff" "$family" "$profile" > "$log" 2>&1 &
  else
    timeout -k 60 "$fam_timeout" "$bakeoff" "$family" "$profile" > "$log" 2>&1 &
  fi
  child=$!
  wait "$child"; rc=$?
  child=""; current=""
  t1=$(date +%s)
  spent=$(( spent + t1 - t0 ))
  # The run directory comes from the "run ->" line hack/bakeoff.sh prints
  # before it can fail, with the old "done ->" line as a fallback so a log
  # written by an older revision still reads.
  run_id="$(sed -n 's#^== [A-Za-z0-9]*: run -> .*/results/##p' "$log" | tail -n1)"
  [ -n "$run_id" ] || run_id="$(sed -n 's#.*done -> .*/results/##p' "$log" | tail -n1)"
  cont=""; lost=""; refused=""; fail=""; mtx=""
  ctx="$root/benchmarks/reports/results/$run_id/contention.txt"
  if [ -n "$run_id" ] && [ -f "$ctx" ]; then
    cont="$(sed -n 's/^contended_runs=//p' "$ctx")"
    lost="$(sed -n 's/^lost_samples=//p' "$ctx")"
    refused="$(sed -n 's/^refused_runs=//p' "$ctx")"
    fail="$(sed -n 's/^failed_runs=//p' "$ctx")"
    mtx="$(sed -n 's/^matrix=//p' "$ctx")"
  fi
  [ -n "$mtx" ] || mtx="$(sed -n 's/^== [A-Za-z0-9]*: matrix=\([a-z]*\) .*/\1/p' "$log" | tail -n1)"
  if [ "$rc" -eq 0 ]; then
    # The marker records what was proved, not just that something was: the
    # profile and the dimension grid it ran on.
    printf 'profile=%s\nmatrix=%s\nrun_id=%s\nfinished=%s\n' \
      "$profile" "${mtx:-unknown}" "${run_id:-–}" "$(date -u +%Y-%m-%dT%H:%M:%SZ)" > "$state/$family.ok"
    ok=$((ok+1)); status=ok
  else
    rm -f "$state/$family.ok"; failed=$((failed+1))
    case "$rc" in
      124|137) status="timeout(${fam_timeout}s)" ;;
      4)       status="refused" ;;
      6)       status="lostsamples" ;;
      130)     status="interrupted" ;;
      *)       status="fail($rc)" ;;
    esac
    echo "campaign: $family $status; last lines:"
    tail -n 12 "$log" | sed 's/^/    /'
  fi
  printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
    "$family" "$status" "$attempt" "$((t1-t0))" "${mtx:-–}" "${cont:-–}" "${lost:-–}" \
    "${refused:-–}" "${fail:-–}" "${run_id:-–}" >> "$manifest"
  echo "campaign: $family $status in $((t1-t0))s matrix=${mtx:-–} contended=${cont:-–} lost=${lost:-–} refused=${refused:-–} failed=${fail:-–}"
done

echo
echo "campaign: spent=${spent}s of ${total_max}s budget; $budgeted famil(y/ies) not reached"
echo "campaign: profile=$profile ok=$ok failed=$failed skipped=$skipped finished=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
awk -F'\t' '{printf "  %-6s %-14s %3s %8s %8s %6s %5s %8s %7s  %s\n", $1, $2, $3, $4, $5, $6, $7, $8, $9, $10}' "$manifest"
if [ "$profile" != probe ]; then
  echo
  echo "campaign: render the ADRs from the stored scorecards with:  make bakeoff-adr"
fi
[ "$failed" -eq 0 ]
