#!/usr/bin/env bash
# The competitor comparison's correctness gate (§10.1): every arm of one
# family must round-trip one message and a full batch at every canonical
# payload size before anything is measured.
#
#   hack/bench/compare-gate.sh FAMILY OUTDIR
#
# OUTDIR holds `arms-FAMILY.txt` (written by hack/bench/run-compare.sh from
# the package's own arm declaration) and receives `gate-FAMILY.log`.
#
# ---------------------------------------------------------------------------
# Why this is a script of its own, and why it counts
#
# It was a function inside hack/bench/run-compare.sh, and it could not fail:
#
#     go test -run "^TestSessionsRoundTrip\$/^memory".
#
# The subtest path is `TestSessionsRoundTrip/<library>/<family>/<size>`, so
# depth 1 is the LIBRARY. `^memory` was therefore matched against `iqmq`,
# selected nothing, and `go test` exited 0 printing
#
#     ok  github.com/iqhive/iqmq/benchmarks/competitors  0.010s [no tests to run]
#
# which is what both families' gate logs hold for the campaign of
# 2026-09-14. A correctness gate that passes whatever the arms do is worse
# than no gate, because the campaign then reports one.
#
# Two changes fix it and one makes the fix checkable. The family is matched at
# the depth it occupies. The PASS lines are COUNTED against the arm list, so
# selecting nothing is a failure rather than a pass and an arm that cannot
# round-trip is one missing PASS. And the arm count is asserted non-zero in
# the same place, because "at least as many passes as there are arms" is
# trivially true of zero arms -- the same hole one level along.
#
# It is a separate script so that it can be RUN, and therefore watched
# failing, without starting a measurement campaign. A gate nobody has watched
# fail is assumed not to work, and six in this repository turned out not to.
set -euo pipefail
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# The host CPU ceiling. This script invokes `go` itself, so it is capped when
# run by hand and not only when a make recipe exports the cap into it.
# hack/cpucap.sh carries the arithmetic; the eval is a
# no-op beyond re-deriving the same GOMAXPROCS and -p when make already set
# them, and it never RAISES either.
eval "$("$root/hack/cpucap.sh" env)"

if [ "$#" -ne 2 ]; then
  echo "usage: $0 FAMILY OUTDIR" >&2
  exit 2
fi
name="$1"
out="$2"
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

arms="$out/arms-$name.txt"
if [ ! -f "$arms" ]; then
  echo "!! $name: $arms does not exist, so the gate has no arm list to check against" >&2
  exit 2
fi
want="$(grep -c . "$arms" || true)"
if [ "${want:-0}" -eq 0 ]; then
  echo "!! $name: the arm list is empty, so the gate would pass by having nothing to check" >&2
  exit 1
fi

echo "== $name: correctness gate ($want arm(s) expected)"
rc=0
(cd "$root/benchmarks/competitors" && GOWORK=off go test -count=1 -timeout=10m -v \
    -run "^TestSessionsRoundTrip\$/.*/^$name\$" .) >> "$out/gate-$name.log" 2>&1 || rc=$?

# Every arm must show a PASS at the PAYLOAD-SIZE depth, not merely at its own.
#
# Counting arm-depth PASS lines was the second version of this check and it
# also could not fail: `opbench.CheckSession` opens a session per size
# subtest, so an arm whose Open calls tb.Skip skips all three CHILDREN while
# its own parent line still reads PASS. Watched -- with gocloud/memory made to
# skip unconditionally, `go test` exited 0, printed
# `--- SKIP: TestSessionsRoundTrip/gocloud/memory/64B`, and the gate reported
# "passed 3 arm(s) of 3". A gate that cannot tell a round trip from a skip is
# the same hole one level down.
#
# So the check is per arm and at size depth: for each library in the arm list,
# at least one `        --- PASS: TestSessionsRoundTrip/<library>/<family>/…`
# line. The eight-space indent is the size level; four is the arm's own. The
# trailing context after the family name is load-bearing too: `go test -v`
# writes ` (0.03s)` after a name, so a `$` anchor matches nothing -- the first
# version had one and reported "passed 0 arm(s) of 3" for three sound arms.
missing=""
got=0
while IFS= read -r pattern; do
  [ -n "$pattern" ] || continue
  # ^BenchmarkCompare/<library>/<family>/  ->  <library>
  lib="${pattern#^BenchmarkCompare/}"
  lib="${lib%%/*}"
  if grep -qE "^        --- PASS: TestSessionsRoundTrip/$lib/$name/" "$out/gate-$name.log"; then
    got=$((got + 1))
  else
    missing="$missing $lib"
  fi
done < "$arms"

if [ "$rc" -ne 0 ]; then
  echo "!! $name: the gate's own run failed (exit $rc); $got of $want arm(s) round-tripped at every size" >&2
  [ -n "$missing" ] && echo "   no payload-size PASS for:$missing" >&2
  exit 1
fi
if [ "$got" -lt "$want" ]; then
  echo "!! $name: $got arm(s) of $want round-tripped; an arm that cannot round-trip is not measured" >&2
  echo "   no payload-size PASS for:$missing" >&2
  exit 1
fi
echo "== $name: correctness gate passed $got arm(s) of $want"
