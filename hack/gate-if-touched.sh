#!/usr/bin/env bash
# Run the §10.6 benchmark gate when, and only when, this branch changed a
# package that holds a gated benchmark.
#
#   hack/gate-if-touched.sh [BASE_REF] [GATE]      # make gate-if-touched BASE=…
#
# `make gate` cannot join `make ci`: it is two interleaved measurement runs on a
# quiet host, minutes rather than seconds, and `make ci` runs several times per
# increment. But "green `make ci`" has been reading as "gates passed" while the
# budget gate ran for nobody, which is how A23 landed a family that breached a
# ceiling by construction and how `make gate` came to be red on `main` with
# nothing in the increment gate able to notice.
#
# So the gate has two triggers, both written into the contributor notes's "Gate"
# section :
#
#   1. this one, before landing, for an increment that touched a gated package;
#   2. the nightly job, over the whole `nightly` gate set.
#
# An increment that touched nothing gated exits 0 here and says so, which is
# the common case and costs a `git diff`.
set -euo pipefail
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# The host CPU ceiling. This script invokes `go` itself, so it is capped when
# run by hand and not only when a make recipe exports the cap into it.
# hack/cpucap.sh carries the arithmetic; the eval is a
# no-op beyond re-deriving the same GOMAXPROCS and -p when make already set
# them, and it never RAISES either.
eval "$("$root/hack/cpucap.sh" env)"
base_ref="${1:-main}"; gate="${2:-pr}"

base_sha="$(git -C "$root" rev-parse --verify "$base_ref")"
merge_base="$(git -C "$root" merge-base "$base_sha" HEAD 2>/dev/null || echo "$base_sha")"

# The packages that hold a benchmark this gate set selects, from the family
# declarations rather than a list that would go stale.
mapfile -t pkgs < <(cd "$root/benchmarks" && GOWORK=off go run ./cmd/bakeoff gate-pattern -budgets budgets.yaml -packages "$gate")
[ "${#pkgs[@]}" -gt 0 ] || { echo "gate-if-touched: gate set $gate names no package" >&2; exit 2; }

# Changed files, including anything not yet committed: an agent runs this
# before it commits as often as after.
changed="$(git -C "$root" diff --name-only "$merge_base" HEAD; git -C "$root" status --porcelain | sed 's/^...//')"

touched=()
for pkg in "${pkgs[@]}"; do
  dir="${pkg#./}"
  # A package's own directory only: a change under internal/engine/sub is a
  # different package, and a change in a dependency is not detected here. That
  # is the honest limit of a diff-based trigger and the nightly run is what
  # covers it.
  #
  # The ROOT package is the case the obvious pattern cannot see: `git diff
  # --name-only` prints its files as `run.go`, not `./run.go`, so
  # `^./[^/]+\.go$` matched nothing and budgets.yaml's `gate_packages: ["."]`
  # -- the entry that exists precisely because the root holds §10.2's own
  # benchmarks -- could never trigger the gate.
  if [ "$dir" = "." ]; then pat='^[^/]+\.go$'; else pat="^$dir/[^/]+\.go$"; fi
  if echo "$changed" | grep -qE "$pat"; then touched+=("$pkg"); fi
done

# The gate's own declaration counts as a gated file. `the benchmark budgets`
# holds the gate sets, the absolute ceilings, the `gate_packages` lists and the
# waivers, so an increment that adds a pattern, adds a budget row, relaxes a
# ceiling or writes a waiver changes WHAT THE GATE CHECKS while touching no
# gated `.go` file -- and the trigger did not fire. Watched, on main, against a
# branch whose only change is `ns_op_max: 150` -> `400` on §10.2's wire encode
# row:
#
#     gate-if-touched: no gated package changed against 2703192 (2703192);
#     the gate is not needed for this increment
#
# That is the mirror image of the case recorded against B13, where the trigger
# fired and the coverage did not: here the coverage changes and the trigger
# does not.
if echo "$changed" | grep -qE '^benchmarks/budgets\.yaml$'; then
  touched+=("benchmarks/budgets.yaml")
fi

# And the gate's own IMPLEMENTATION, which is the same argument one level
# along: `budgets.yaml` declares what the gate checks and
# `benchmarks/cmd/benchgate`, `benchmarks/internal/budgets` and
# `hack/benchgate.sh` decide what it SAYS about it. An increment that changes a
# verdict rule changes every answer the gate can give while touching no gated
# `.go` file and no declaration.
#
# `make gate-selftest` is the stronger obligation for such a change and
# the contributor notes already asks for it by name; this makes the pre-landing
# trigger agree with it instead of contradicting it.
if echo "$changed" | grep -qE '^(benchmarks/cmd/benchgate/|benchmarks/internal/budgets/).*\.go$|^hack/benchgate\.sh$'; then
  touched+=("the gate's own implementation")
fi

if [ "${#touched[@]}" -eq 0 ]; then
  echo "gate-if-touched: no gated package changed against $base_ref ($(git -C "$root" rev-parse --short "$merge_base")); the gate is not needed for this increment"
  echo "gate-if-touched: gated packages are ${pkgs[*]}"
  exit 0
fi
echo "gate-if-touched: ${touched[*]} changed against $base_ref; running the $gate gate"
exec "$root/hack/benchgate.sh" "$base_ref" "$gate"
