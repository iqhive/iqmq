#!/usr/bin/env bash
# Check per-area coverage against the test suite and the floors in
# hack/coverage-floors.yaml.
#
#   hack/cover-threshold.sh [-update] PROFILE...
#
# The core pattern ENUMERATES the directories that are core rather than
# excluding the ones that are not, which is what keeps a prototype tree out of
# the core figure by construction (ADR-025, and ADR-023 before it). An x/
# package that is product rather than prototype is added here when it ships;
# x/future is the first.
#
#         drivers: no statements          <- what it was supposed to print
#         awk: cmd. line:1: fatal: division by zero attempted
#
#     An area that matches no statement is now a FAILURE naming its pattern.
#     That is the difference between "this area is at 0 %" and "nobody has
#     ever measured this area", and only one of them is worth a gate.
set -euo pipefail
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# IQMQ_COVER_FLOORS lets hack/cover-threshold_test.sh point the check at a
# throwaway floors file, which is how its five failure modes are watched.
floors="${IQMQ_COVER_FLOORS:-$root/hack/coverage-floors.yaml}"

update=0
if [ "${1:-}" = "-update" ]; then update=1; shift; fi
[ $# -ge 1 ] || { echo "usage: hack/cover-threshold.sh [-update] PROFILE..." >&2; exit 2; }

# One merged profile: the areas span modules, so the caller may pass several.
merged="$(mktemp)"
trap 'rm -f "$merged"' EXIT
echo "mode: atomic" > "$merged"
for p in "$@"; do
  [ -s "$p" ] || { echo "cover-threshold: $p is missing or empty" >&2; exit 2; }
  tail -n +2 "$p" >> "$merged"
done

# The merge is a concatenation, which is only arithmetic if the profiles are
# DISJOINT: `n+=$2` over the same statement block twice counts it twice, and
# both the numerator and the denominator move. Plain per-module `-cover`
# instruments only the module's own packages, so they are disjoint; a
# `-coverpkg` that spans the repository is not, and would need a merge that
# takes the maximum count per block instead. Until somebody writes that, an
# overlap is a refusal rather than a wrong percentage.
dups="$(awk 'NR>1 {print $1}' "$merged" | sort | uniq -d | head -n 3)"
if [ -n "$dups" ]; then
  echo "cover-threshold: the profiles overlap and concatenating them would count statements twice; first: $(printf '%s' "$dups" | tr '\n' ' ')" >&2
  exit 2
fi

# The check above compares WHOLE block keys, so it sees two profiles that name
# the same block -- and it is blind to the case that actually happened
# : TWO DECOMPOSITIONS of one file, from two different
# versions of its source, in one merged profile. Their keys differ, so `uniq -d`
# finds nothing, the union has no duplicate to collapse, and both sets of
# statements are summed. `run.go` was counted as 889 statements instead of 364
# that way -- the same 58 functions, twice -- and the core figure moved by six
# tenths of a point in the WRONG direction on a branch that only added tests.
#
# It happens whenever the tree is edited while a measurement is running, which
# is easy to do because hack/cover.sh takes twenty minutes: a mutation-testing
# pass over a source file during the run is enough.
#
# A basic block's start position is unique within a file, so one start position
# with two different end positions is two decompositions and nothing else. Over
# the 651 files and 45,000 rows of a clean profile there are none, so this is a
# refusal and not a warning: a profile this script cannot count correctly must
# not produce a percentage at all.
recut="$(awk 'NR>1 {i=index($1, ","); if (i>0) print substr($1, 1, i-1) "\t" substr($1, i+1)}' "$merged" \
  | sort -u | awk -F'\t' '$1 == prev { print $1 } { prev = $1 }' | sort -u | head -n 3)"
if [ -n "$recut" ]; then
  echo "cover-threshold: the profile holds more than one decomposition of the same file, so its statements would be counted once per decomposition; this is what a tree edited DURING a measurement produces. Re-measure on a quiescent tree. First block start with two different ends: $(printf '%s' "$recut" | tr '\n' ' ')" >&2
  exit 2
fi

# set_floor rewrites one area's floor in hack/coverage-floors.yaml: it takes
# the area, the new floor and the old one (for the message).
#
# It is a function because -update has to reach it from TWO places and used
# to reach it from one. The met-area branch `continue`d before the update
# block, so `-update` could raise the floor of an area that was still short
# and could NOT raise the floor of one that had reached its §11.4 target --
# the single case that ENDS a shortfall, and the case the flag exists for.
# `make cover-update` printed "raise it to the target" and changed nothing.
set_floor() {
  python3 - "$floors" "$1" "$2" <<'PY'
import re, sys
path, area, new = sys.argv[1], sys.argv[2], sys.argv[3]
lines = open(path).read().split("\n")
cur = None
for i, l in enumerate(lines):
    m = re.match(r'^\s*-\s+area:\s*(\S+)', l)
    if m: cur = m.group(1)
    if cur == area and re.match(r'^\s*floor:\s*[0-9.]+', l):
        lines[i] = re.sub(r'([0-9.]+)', new, l, count=1)
        break
open(path, "w").write("\n".join(lines))
PY
  echo "cover-threshold: $1 floor $3% -> $2%" >&2
}

pattern_for() {
  case "$1" in
    core)    printf '%s' 'github.com/iqhive/iqmq/[a-z_]*\.go:|github.com/iqhive/iqmq/(dsn|wire|codec|typed|runner|reqreply|middleware|testkit|driverkit|internal|x/future)/' ;;
    memory)  printf '%s' 'github.com/iqhive/iqmq/(memory|internal/engine)/' ;;
    drivers) printf '%s' 'github.com/iqhive/iqmq/drivers/' ;;
    *)       return 1 ;;
  esac
}

# The floors file is read field by field rather than through a YAML library,
# which is the same choice `hack/fuzz-ratchet.yaml` and tools/yamlmini make.
read_floors() {
  python3 - "$floors" <<'PY'
import re, sys
area = target = floor = owner = None
out = []
for line in open(sys.argv[1]):
    s = line.strip()
    m = re.match(r'^-\s+area:\s*(\S+)', s)
    if m:
        if area: out.append((area, target, floor, owner))
        area, target, floor, owner = m.group(1), None, None, ""
        continue
    m = re.match(r'^target:\s*([0-9.]+)', s)
    if m and area: target = m.group(1); continue
    m = re.match(r'^floor:\s*([0-9.]+)', s)
    if m and area: floor = m.group(1); continue
    m = re.match(r'^owner:\s*(.+)$', s)
    if m and area: owner = m.group(1).strip(); continue
if area: out.append((area, target, floor, owner))
for a, t, f, o in out:
    if t is None or f is None:
        print("PROBLEM %s: needs both a target and a floor" % a); continue
    print("AREA %s %s %s %s" % (a, t, f, o or "-"))
PY
}

status=0
areas=0
while read -r tag a b c d; do
  case "$tag" in
    PROBLEM) echo "cover-threshold: ${a} ${b} ${c} ${d}" >&2; status=1; continue ;;
    AREA) ;;
    *) continue ;;
  esac
  area="$a"; target="$b"; floor="$c"; owner="$d"
  areas=$((areas + 1))
  pat="$(pattern_for "$area")" || { echo "$area: no coverage pattern is defined for this area" >&2; status=1; continue; }
  # Total and covered statements in ONE pass with explicit initialisation, so
  # an empty match is an empty match and not an unset awk variable.
  read -r total covered < <(grep -E "$pat" "$merged" | awk 'BEGIN{n=0;c=0} {n+=$2; if ($3>0) c+=$2} END{print n, c}')
  if [ "${total:-0}" -eq 0 ]; then
    echo "$area: NO STATEMENTS matched $pat in the profile(s) given; this area is unmeasured, not covered" >&2
    status=1
    continue
  fi
  pct=$(awk -v c="$covered" -v n="$total" 'BEGIN{printf "%.1f", 100*c/n}')
  # A floor above its target would bank slack a met threshold does not earn.
  if awk -v f="$floor" -v t="$target" 'BEGIN{exit !(f > t)}'; then
    echo "$area: floor $floor% is above §11.4's target $target%; a floor is a ratchet toward the target, not past it" >&2
    status=1
    continue
  fi
  if awk -v p="$pct" -v f="$floor" 'BEGIN{exit !(p < f)}'; then
    printf '%s: %s%% is BELOW the floor %s%% (§11.4 target %s%%) over %s statements\n' "$area" "$pct" "$floor" "$target" "$total" >&2
    status=1
    continue
  fi
  if awk -v p="$pct" -v t="$target" 'BEGIN{exit !(p >= t)}'; then
    # Met: the floor has to be the target, or the area is carrying slack.
    if awk -v f="$floor" -v t="$target" 'BEGIN{exit !(f < t)}'; then
      if [ "$update" = 1 ]; then
        set_floor "$area" "$target" "$floor"
        printf '%s: %s%% (§11.4 target %s%%, met) over %s statements\n' "$area" "$pct" "$target" "$total"
        continue
      fi
      echo "$area: $pct% meets §11.4's $target% but the floor is still $floor%; raise it to the target" >&2
      status=1
      continue
    fi
    printf '%s: %s%% (§11.4 target %s%%, met) over %s statements\n' "$area" "$pct" "$target" "$total"
    continue
  fi
  [ "$owner" = "-" ] && { echo "$area: $pct% is short of §11.4's $target% and hack/coverage-floors.yaml names no owner for the gap" >&2; status=1; continue; }
  gap=$(awk -v p="$pct" -v t="$target" 'BEGIN{printf "%.1f", t-p}')
  printf '%s: %s%% >= floor %s%%, SHORTFALL %s points against §11.4 target %s%% (owner: %s) over %s statements\n' \
    "$area" "$pct" "$floor" "$gap" "$target" "$owner" "$total"
  if [ "$update" = 1 ]; then
    newfloor=$(awk -v p="$pct" -v f="$floor" -v t="$target" 'BEGIN{x=(p>f)?p:f; if (x>t) x=t; printf "%.1f", x}')
    [ "$newfloor" != "$floor" ] && set_floor "$area" "$newfloor" "$floor"
  fi
done < <(read_floors)

[ "$areas" -eq 3 ] || { echo "cover-threshold: $areas area(s) declared; §11.4 names three (core, memory, drivers)" >&2; status=1; }
exit $status
