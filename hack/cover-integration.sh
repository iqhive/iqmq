#!/usr/bin/env bash
# The coverage figure with the CONTAINER arm counted, which `make ci` cannot
# produce and which is the only honest answer for the drivers area.
#
#   hack/cover-integration.sh [OUT]   # default: coverage-integration.out
#
# Why this exists. hack/cover.sh measures every module's `go test`, and that
# is what `make cover-threshold` gates on. It cannot reach a driver's
# management API, its conformance harness or its Open family, because those
# run only against a broker -- so a driver's `admin.go` and `testing.go` read
# near zero in the gated profile while being exercised on every
# `make integration` run. The drivers area was 78.0 % under hack/cover.sh
# alone and 83.7 % with the suites merged in, and the difference is not
# missing tests: it is tests the gate cannot see.
#
# This script runs both arms over ONE tree and merges them by union. The tree
# matters: a source edit between the two arms shifts line numbers, and the old
# blocks then survive in the merge as statements nothing covers. That was
# measured, not feared -- an edit to drivers/pulsar/admin.go mid-run left 140
# phantom statements in the denominator and made the figure read 1.4 points
# low.
#
# It needs Docker and it takes roughly fifty minutes, which is why it is not
# in `make ci`. It is the instrument for the question "how much of the
# remainder is broker-only", and hack/coverage-floors.yaml's drivers entry
# asks exactly that.
set -euo pipefail
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# The host CPU ceiling. This script invokes `go` itself, so it is capped when
# run by hand and not only when a make recipe exports the cap into it.
# hack/cpucap.sh carries the arithmetic; the eval is a
# no-op beyond re-deriving the same GOMAXPROCS and -p when make already set
# them, and it never RAISES either.
eval "$("$root/hack/cpucap.sh" env)"
out="${1:-$root/coverage-integration.out}"
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

# A dirty tree is a refusal rather than a warning: the two arms take about an
# hour between them, and a figure measured across an edit is wrong in a way
# that looks fine.
if [ -n "$(cd "$root" && git status --porcelain)" ]; then
  echo "cover-integration: the working tree is dirty; the two arms are measured an hour apart and an edit" >&2
  echo "cover-integration: between them leaves the old line numbers in the merge as uncovered statements" >&2
  exit 2
fi
echo "cover-integration: measuring $(cd "$root" && git rev-parse --short HEAD)"

echo "cover-integration: the unit arm (hack/cover.sh)"
"$root/hack/cover.sh" "$tmp/unit.out"

# One suite at a time. They share a Docker host and several of them bind the
# broker's own port, so two at once is a port conflict rather than a speedup.
#
# The suite list comes from `go test -list` and NOT from the conformance suites manifest,
# because the two can drift and did: the JSON names a suite `kafka-kafkago` and
# the test function is `TestConformance_kafka_kafkago`, so a -run pattern built
# from the JSON matched NOTHING and every suite "passed" in zero seconds with
# an empty profile. Asking the test binary what it has cannot go stale.
#
# `conformance` is its own MODULE, so the run happens there; from the root it
# is "main module does not contain package .../conformance/suites".
n=0
while IFS= read -r test; do
  n=$((n + 1))
  echo "cover-integration: ${test}"
  if ! (cd "$root/conformance" && GOWORK=off go test ./suites/ -tags=integration -count=1 -timeout=30m \
        -run "^${test}\$" \
        -covermode=atomic -coverpkg=github.com/iqhive/iqmq/... \
        -coverprofile="$tmp/s$n.out" > "$tmp/s$n.log" 2>&1); then
    echo "cover-integration: $test failed:" >&2
    grep -vE '^(ok[[:space:]]|\?[[:space:]]|go: downloading )' "$tmp/s$n.log" | head -n 40 >&2
    exit 1
  fi
  # A suite that SKIPS writes a profile with a header and nothing else. It is
  # not an error -- an image this host cannot pull is a suite that did not
  # run -- but it must not be merged as though every block in it were
  # uncovered, which is what an empty profile would do.
  if [ "$(wc -l < "$tmp/s$n.out")" -le 1 ]; then
    echo "cover-integration: $test produced no profile (skipped); not merged"
    rm -f "$tmp/s$n.out"
  fi
done < <(cd "$root/conformance" && GOWORK=off go test ./suites/ -tags=integration -list 'TestConformance_.*' 2>/dev/null | grep '^TestConformance_' | sort)

if [ "$n" -eq 0 ]; then
  echo "cover-integration: no conformance suites were found; the -list pattern matched nothing" >&2
  exit 1
fi
echo "cover-integration: $n suite(s) run"

# The union, taking the highest count per block. The profiles OVERLAP by
# construction -- every arm is measured with -coverpkg over the whole
# repository -- so concatenating them would count a block once per arm and
# move numerator and denominator both. hack/cover-merge.awk is the same merge
# hack/cover.sh uses across modules.
echo "mode: atomic" > "$out"
awk -f "$root/hack/cover-merge.awk" "$tmp"/unit.out "$tmp"/s*.out >> "$out"
echo "cover-integration: merged into $out"

# The areas, against the same floors `make cover-threshold` uses. The figure
# it prints is NOT what the gate enforces and must not be used to raise a
# floor: hack/cover.sh's profile is, because that is what a pull request can
# reproduce. This one answers a different question and says so.
"$root/hack/cover-threshold.sh" "$out" || true
echo
echo "cover-integration: the figures above INCLUDE the container arm."
echo "cover-integration: hack/coverage-floors.yaml is a ratchet on hack/cover.sh's"
echo "cover-integration: profile alone; do not raise a floor from this one."
