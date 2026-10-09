#!/usr/bin/env bash
# Produce one coverage profile spanning every module the test suite's
# three areas live in, then check it against hack/coverage-floors.yaml.
#
#   hack/cover.sh [OUT]        # default: coverage.out at the repository root
#
# An area is a set of PACKAGE PATTERNS, not a module: core is the root
# package plus eleven directories, memory is two, and drivers is eighteen
# separate modules. That last fact is the whole reason this script exists: the
# check was only ever handed `go test ./... -coverprofile` from the root
# module, where no driver package lives, so the drivers threshold matched
# nothing on every run for the life of the repository. A
# per-area threshold needs a profile that reaches the area -- and reaching it
# means running the tests wherever they are, not only where the code is.
#
# Every module is measured with `-coverpkg=github.com/iqhive/iqmq/...`, so the
# test run of a module that EXERCISES core code is credited with it. Without
# that, a package is only ever covered by tests in its own module -- and the
# core area is full of code whose callers live elsewhere by construction:
# `internal/streams/fakestreams` read 13.1 % (805 uncovered statements, 85 % of
# the whole core shortfall) while being driven all day by `drivers/redis`, and
# `testkit`, `driverkit` and `codec/codectest` are all exercised from the
# modules they exist to serve. Crediting ONE driver module moved the measured
# core figure from 85.82 % to 88.25 %.
#
# That makes the profiles OVERLAP, and overlapping profiles cannot be
# concatenated: the same block would be counted once per module that compiled
# it, moving numerator and denominator both. They are merged by UNION instead,
# taking the highest count per block -- which is the merge assumption C02.4
# said was needed and did not exist. hack/cover-threshold.sh still refuses a
# profile with duplicate blocks, and that check is what stands behind this one.
#
# Modules excluded, and why: everything under benchmarks/ is measurement
# scaffolding whose tests are shaped by the current campaign rather than by
# this repository's correctness, and hack/tools carries no Go sources.
#
# No `-race`: `make test-race` is the race gate and runs separately. Coverage
# instrumentation plus the race detector over forty-five modules is twice the
# work for the same statements.
set -euo pipefail
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# The host CPU ceiling. This script invokes `go` itself, so it is capped when
# run by hand and not only when a make recipe exports the cap into it.
# hack/cpucap.sh carries the arithmetic; the eval is a
# no-op beyond re-deriving the same GOMAXPROCS and -p when make already set
# them, and it never RAISES either.
eval "$("$root/hack/cpucap.sh" env)"
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
out="${1:-$root/coverage.out}"
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

mods=(".")
while IFS= read -r m; do
  d="$(dirname "${m#./}")"
  case "$d" in
    .|benchmarks|benchmarks/*|hack/tools) continue ;;
  esac
  mods+=("$d")
done < <(cd "$root" && find . -name go.mod -not -path './.git/*' | sort)

i=0
for m in "${mods[@]}"; do
  i=$((i + 1))
  (cd "$root/$m" && GOWORK=off go test -count=1 -covermode=atomic -coverpkg=github.com/iqhive/iqmq/... -coverprofile="$tmp/p$i.out" ./... > "$tmp/p$i.log" 2>&1) || {
    echo "cover: go test failed in $m:" >&2
    # The FAILING package is not near the end of the log. `go test ./...`
    # prints one line per package in import order, so a failure early in the
    # list leaves twenty successes after it -- and `tail -n 20` then printed
    # twenty "ok" lines and a bare "FAIL", saying nothing about which test
    # failed. The first GitHub Actions run of this repository failed here and
    # the log named no test: forty minutes went into finding what the
    # instrument already knew. Everything that is not a plain success comes
    # first, then the tail for context.
    grep -vE '^(ok[[:space:]]|\?[[:space:]]|go: downloading )' "$tmp/p$i.log" | head -n 80 >&2
    echo "cover: --- last 20 lines of $m's log ---" >&2
    tail -n 20 "$tmp/p$i.log" >&2
    exit 1
  }
done

# The union, in hack/cover-merge.awk so that it can be held to a table by
# hack/cover-threshold_test.sh: one row per statement block, carrying the
# HIGHEST count any module's run recorded for it.
{
  echo "mode: atomic"
  awk -f "$root/hack/cover-merge.awk" "$tmp"/p*.out
} > "$out"
echo "cover: $(($(wc -l < "$out") - 1)) statement block(s) from ${#mods[@]} module(s) -> $out" >&2
exec "$root/hack/cover-threshold.sh" "$out"
