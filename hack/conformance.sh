#!/usr/bin/env bash
# Conformance, critical, matrix and in-process runs of one conformance suite,
# or of every suite that has one.
#
#   hack/conformance.sh integration [SUITE]  # testkit.RunConformance against the suite's broker
#   hack/conformance.sh critical [SUITE]     # §11.2's driver-specific critical tests
#   hack/conformance.sh matrix [SUITE]       # broker-version matrix from IQMQ_<BROKER>_IMAGES
#   hack/conformance.sh inprocess [SUITE]    # the kit against a driver that needs no broker
#   hack/conformance.sh harness              # start and ping every broker image
#   hack/conformance.sh list [MODE]          # which suites have that run
#   hack/conformance.sh modes                # every run mode, and what is missing
#
# SUITE is the `name` of an entry in conformance/suites.json, not a driver
# name: `sqs` and `sqs-fifo` are two suites of one driver, and `kafka` and
# `kafka-kafkago` are two clients of one. That file is the only place that says
# where a mode's tests live and which suites have them, and `tools/suites` is
# the only thing that reads it -- so a mode this repository does not implement
# is refused here, by name and with the reason, instead of failing as a
# `go test` on a package that is not there.
#
# That is exactly what three increments in a row could not add a suite entry
# because of: `make critical` resolved ./critical/... in the conformance
# module, conformance/critical has never existed, and the first entry added
# would have turned a silently skipped job into a red one at a step that
# cannot pass.
set -euo pipefail
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# The host CPU ceiling. This script invokes `go` itself, so it is capped when
# run by hand and not only when a make recipe exports the cap into it.
# hack/cpucap.sh carries the arithmetic; the eval is a
# no-op beyond re-deriving the same GOMAXPROCS and -p when make already set
# them, and it never RAISES either.
eval "$("$root/hack/cpucap.sh" env)"
mode="${1:?mode}"; suite="${2:-}"; extra="${3:-}"
export GOWORK=off IQMQ_CONFORMANCE_REPORT_DIR="${IQMQ_CONFORMANCE_REPORT_DIR:-$root/conformance/reports/out}"
mkdir -p "$IQMQ_CONFORMANCE_REPORT_DIR"
# Every harness reads IQMQ_COMMIT into the report's `commit` field and nothing
# ever set it: three of the eleven committed reports carried a value, from
# somebody's manual run, and the workflow that produces the rest sets nothing.
# A report whose provenance is empty cannot be read against the kit that
# produced it, which is how conformance/reports/latest came to hold a report
# that predates a probe by four increments with nothing able to notice.
export IQMQ_COMMIT="${IQMQ_COMMIT:-$(git -C "$root" rev-parse HEAD 2>/dev/null || echo unknown)}"

suites() { (cd "$root" && go run ./tools/suites "$@"); }

field() { printf '%s\n' "$1" | sed -n "s/^$2=//p"; }

run_one() { # mode suite
  local m="$1" name="$2" out dir pattern tags timeout
  out="$(suites -resolve "$name" -mode "$m")"
  dir="$(field "$out" dir)"; pattern="$(field "$out" pattern)"
  tags="$(field "$out" tags)"; timeout="$(field "$out" timeout)"
  local args=(-race -count=1 -run "$pattern" -timeout "$timeout" -v)
  [ -n "$tags" ] && args+=(-tags "$tags")
  echo "==> $m/$name: (cd $dir && go test ${args[*]} ./...)"
  # The grep drops the per-subtest noise of 156 scenarios; pipefail keeps
  # `go test`'s own status, which is the only thing that decides the run.
  #
  # And the log is kept, because `go test -run PAT` exits 0 and prints PASS
  # when PAT selects nothing: without the check below, a run that measured
  # NOTHING is indistinguishable from one that passed. `tools/suites -check`
  # holds every pattern to the repository statically, and this is the same
  # rule at run time, where a mode's tags or a driver's build constraints can
  # still empty a package the parser could see into (the contributor notes's
  # family A29 lesson).
  local log; log="$(mktemp)"
  set -o pipefail
  local status=0
  (cd "$root/$dir" && go test "${args[@]}" ./...) 2>&1 | grep -vE '^(=== RUN|=== PAUSE|=== CONT)' | tee "$log" || status=$?
  if [ "$status" = 0 ] && ! grep -qE '^ *--- (PASS|FAIL|SKIP)' "$log"; then
    echo "conformance: $m/$name selected no test at all ($pattern in $dir); go test reports that as a pass" >&2
    status=2
  fi
  rm -f "$log"
  return "$status"
}

run_mode() { # mode
  local m="$1" names status=0
  if [ -n "$suite" ]; then
    run_one "$m" "$suite"
    return
  fi
  names="$(suites -list -mode "$m")"
  if [ -z "$names" ]; then
    echo "conformance: no suite in conformance/suites.json declares the \"$m\" run" >&2
    exit 2
  fi
  for name in $names; do
    run_one "$m" "$name" || status=1
  done
  return "$status"
}

case "$mode" in
  harness) (cd "$root/conformance" && go test -count=1 -tags integration -run '^TestHarness' -timeout 40m -v ./harness/...) ;;
  list)    suites -list -mode "${suite:-}" ;;
  modes)   suites -modes ;;
  soak)    IQMQ_SOAK_DURATION="${extra:-1h}" run_mode soak ;;
  *)       run_mode "$mode" ;;
esac
