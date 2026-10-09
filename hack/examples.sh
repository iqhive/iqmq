#!/usr/bin/env bash
# Build, vet and RUN every example module, and say which ones reached a broker.
#
# The test run is the half that matters. An example that compiles and deadlocks
# is the first documentation defect anybody meets, and `go build` cannot see
# it: every example module therefore carries a test -- an Example with checked
# output, or a test that runs the program and reads what it printed -- and this
# is where it runs. `make test-race` reaches them too, because hack/modules.sh
# finds every module; what this target adds is that the named gate for examples
# is the one that fails.
#
# # Why this script exists rather than a Makefile recipe
#
#   1. the in-process arm. An embedded nats-server, a kfake cluster and the
#      sqsfake server are real enough to hold each example to its printed
#      output with no Docker at all, so every example's behaviour is checked on
#      every run.
#   2. this script NAMES the skips. Each example appends one line per arm to
#      the file IQMQ_EXAMPLE_REPORT points at (see any example's
#      report_test.go), and the summary below prints them. A module whose
#      sources mention the variable and then report nothing is a FAILURE: that
#      is the case where a renamed test, a `-run` filter or a lost gate would
#      otherwise turn into a silent green.
#
# `go test` without -v discards a passing test's stdout, which is why the
# report is a file and not a printed marker.
set -euo pipefail
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# The host CPU ceiling. This script invokes `go` itself, so it is capped when
# run by hand and not only when a make recipe exports the cap into it.
# hack/cpucap.sh carries the arithmetic; the eval is a
# no-op beyond re-deriving the same GOMAXPROCS and -p when make already set
# them, and it never RAISES either.
eval "$("$root/hack/cpucap.sh" env)"
go="${GO:-go}"

report="$(mktemp "${TMPDIR:-/tmp}/iqmq-examples.XXXXXX")"
trap 'rm -f "$report"' EXIT
# Always this script's own file, never one inherited from the environment: a
# stale path would collect two runs' arms into one summary.
export IQMQ_EXAMPLE_REPORT="$report"

# Refuse tracked ELF binaries under examples. A build of one main
# package can leave an executable beside go.mod; ignore rules and
# this check keep build output out of the tracked source.
if [ -d "$root/.git" ] || [ -f "$root/.git" ]; then
  binaries=""
  while IFS= read -r f; do
    [ -f "$root/$f" ] || continue
    magic="$(head -c 4 "$root/$f" 2>/dev/null | od -An -tx1 | tr -d ' \n')"
    [ "$magic" = "7f454c46" ] && binaries="$binaries $f"
  done < <(git -C "$root" ls-files -- examples)
  if [ -n "$binaries" ]; then
    echo "!! a tracked ELF binary lives in an example module:" >&2
    for f in $binaries; do echo "      $f" >&2; done
    echo '   Delete it, and add its path to .gitignore beside the other' >&2
    echo '   example binaries: `go build ./...` in a module whose ./... is one' >&2
    echo '   main package writes an executable beside the go.mod.' >&2
    exit 1
  fi
fi

status=0
modules=0
declaring=""
for dir in "$root"/examples/*/; do
  [ -f "$dir/go.mod" ] || continue
  name="$(basename "$dir")"
  modules=$((modules + 1))
  echo "==> examples/$name"
  before="$(wc -l <"$report")"
  if ! (cd "$dir" && GOWORK=off "$go" build -o /dev/null ./... &&
    GOWORK=off "$go" vet ./... &&
    GOWORK=off "$go" test -count=1 ./...); then
    echo "!! examples/$name: build, vet or test failed" >&2
    status=1
  fi
  # A module that reads the report variable has promised to write to it.
  if grep -rq 'IQMQ_EXAMPLE_REPORT' "$dir" --include='*.go'; then
    declaring="$declaring $name"
    if [ "$(wc -l <"$report")" = "$before" ]; then
      echo "!! examples/$name: reads IQMQ_EXAMPLE_REPORT and reported no arm." >&2
      echo "   Its tests ran without saying whether they reached a broker, so a" >&2
      echo "   run in which nothing was exercised would look the same as one in" >&2
      echo "   which everything was. Check that its arms call reportArm." >&2
      status=1
    fi
  fi
done

echo "==> examples: $modules module(s)"
if [ -z "$declaring" ]; then
  echo "==> examples: no module declares a broker arm"
  exit "$status"
fi

ran="$(awk -F'\t' '$2=="broker" && $3=="ran"' "$report" | wc -l)"
arms="$(awk -F'\t' '$2=="broker"' "$report" | wc -l)"
echo "==> examples: broker arms: $ran of $arms ran (gate: IQMQ_<BROKER>_ADDR; IQMQ_REQUIRE_BROKERS=1 makes an unset one a failure)"
sort "$report" | while IFS=$'\t' read -r name arm st detail; do
  printf '      %-16s %-11s %-8s %s\n' "$name" "$arm" "$st" "$detail"
done
if [ "$ran" = 0 ]; then
  echo "      NOTE: no example reached a broker on this run. The in-process arms above"
  echo "      are what held these examples to their output; see any example's README.md"
  echo "      for the container command."
fi
exit "$status"
