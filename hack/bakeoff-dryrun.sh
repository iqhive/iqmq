#!/usr/bin/env bash
# Pre-campaign check: every family's declaration, at its own declared grid, all
# the way through `decide -dry-run`.
#
#   hack/bakeoff-dryrun.sh <output-directory>
#
# It measures nothing -- `-benchtime=1x`, one sample per row -- so the winners
# it prints are meaningless as decisions. What it asks is whether the names a
# family's benchmarks emit can be turned into a decision at all: whether the
# root selects, whether every kind has rows, whether the kinds share a
# comparable set, whether the decision rule can be applied and whether the
# scorecard renders. `make bench-lint` checks most of that statically; this
# runs the real scorer over the real names.
#
# Run it before starting a campaign. Two families would have failed *after*
# being measured without it: A22, whose four kinds had no benchmark name in
# common, and A5, whose two baselines sat at kind depth. A campaign costs a
# day; this costs five minutes.
set -uo pipefail
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# The host CPU ceiling. This script invokes `go` itself, so it is capped when
# run by hand and not only when a make recipe exports the cap into it.
# hack/cpucap.sh carries the arithmetic; the eval is a
# no-op beyond re-deriving the same GOMAXPROCS and -p when make already set
# them, and it never RAISES either.
eval "$("$root/hack/cpucap.sh" env)"
out="${1:?out dir}"; mkdir -p "$out"
ok=0; bad=0
for f in "$root"/hack/bakeoff/*.env; do
  fam=$(basename "$f" .env)
  ( unset PKG BENCH REF AUTO_KEY KIND_DIM KINDS_TEST EXTRA_ARGS AUTO_FILE AUTO_PREFIX ADR TITLE MATRIX IDENTS MODDIR
    # shellcheck disable=SC1090
    source "$f"
    export IQMQ_BENCH_MATRIX="${MATRIX:-}"
    cd "$root"
    # A family whose arms cannot live in the root module declares the module
    # PKG is relative to, exactly as hack/bakeoff.sh reads it; without this the
    # dry run would report the family as selecting no benchmark, which is the
    # answer it exists to distinguish from a real one.
    moddir="${MODDIR:-}"; case "$moddir" in .|./) moddir="" ;; esac
    pkg_rel="${PKG#./}"
    if [ -n "$moddir" ]; then moddir="${moddir%/}"; cd "$root/$moddir"; pkg_rel="$moddir/${PKG#./}"; fi
    # Same per-element anchoring the harness uses (hack/bakeoff.sh anchor_bench).
    filter="$(printf '%s\n' "$BENCH" | awk -F/ '{s="";for(i=1;i<=NF;i++){s=s (i>1?"/":"") "^" $i "$"}; print s}')"
    # shellcheck disable=SC2086
    GOWORK=off go test -run='^$' -bench="$filter" -benchtime=1x -count=1 -cpu=1 -benchmem \
      ${EXTRA_ARGS:-} "$PKG" 2>&1 | grep '^Benchmark' > "$out/$fam.raw"
    kinds=$(GOWORK=off go test -run "^${KINDS_TEST:-TestKindsList}\$" -v -count=1 ${EXTRA_ARGS:-} "$PKG" 2>/dev/null | sed -n 's/.*kinds: *//p' | head -n1)
    cd "$root/benchmarks"
    GOWORK=off go run ./cmd/bakeoff decide -dry-run -family "$fam" -results "$out/$fam.raw" \
      -pkg "../$pkg_rel" -bench "$BENCH" -ref "$REF" -auto-key "$AUTO_KEY" -idents "$kinds" \
      ${KIND_DIM:+-kind-dim "$KIND_DIM"} -profile full > "$out/$fam.json" 2> "$out/$fam.err"
  )
  rc=$?
  n=$(wc -l < "$out/$fam.raw" 2>/dev/null || echo 0)
  if [ "$rc" -eq 0 ]; then
    w=$(sed -n 's/.*"winner": *"\([^"]*\)".*/\1/p' "$out/$fam.json" | head -1)
    printf 'ok   %-5s rows=%-4s winner=%s\n' "$fam" "$n" "${w:-–}"; ok=$((ok+1))
  else
    printf 'FAIL %-5s rows=%-4s %s\n' "$fam" "$n" "$(head -c 240 "$out/$fam.err" | tr '\n' ' ')"; bad=$((bad+1))
  fi
done
printf '\ndry-run: %d ok, %d failed\n' "$ok" "$bad"
[ "$bad" -eq 0 ]
