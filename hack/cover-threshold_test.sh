#!/usr/bin/env bash
# hack/cover-threshold.sh's own failure modes, watched.
#
# The coverage gate spent the repository's whole history unable to fail three
# different ways at once , so every branch of the replacement
# is exercised here against synthetic profiles. It measures nothing and takes
# milliseconds.
set -uo pipefail
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT
pass=0; fail=0
ok()  { printf 'ok   %s\n' "$1"; pass=$((pass+1)); }
bad() { printf 'FAIL %s\n     %s\n' "$1" "$2"; fail=$((fail+1)); }

# A profile with `want` of `total` statements covered in each area.
profile() { # PATH CORE_COVERED MEM_COVERED DRV_COVERED
  local p="$1"
  { echo "mode: atomic"
    printf 'github.com/iqhive/iqmq/client.go:1.1,1.2 100 %s\n' "$2"
    printf 'github.com/iqhive/iqmq/memory/m.go:1.1,1.2 100 %s\n' "$3"
    printf 'github.com/iqhive/iqmq/drivers/x/d.go:1.1,1.2 100 %s\n' "$4"
  } > "$p"
}
floors() { # PATH CORE_FLOOR MEM_FLOOR DRV_FLOOR [OWNERS]
  cat > "$1" <<EOF
areas:
  - area: core
    target: 90.0
    floor: $2
    owner: ${5:-someone}
  - area: memory
    target: 85.0
    floor: $3
    owner: ${5:-someone}
  - area: drivers
    target: 80.0
    floor: $4
    owner: ${5:-someone}
EOF
}
run() { IQMQ_COVER_FLOORS="$1" "$root/hack/cover-threshold.sh" "$2" 2>&1; }

# 1. Every area over its floor and under its target: green, with the shortfall
#    named. This is today's state and the only green the gate has.
profile "$tmp/p1" 1 1 1   # 100/100 covered in each = 100%
floors "$tmp/f1" 90.0 85.0 80.0
out="$(run "$tmp/f1" "$tmp/p1")"; st=$?
if [ "$st" = 0 ] && printf '%s' "$out" | grep -q "met"; then
  ok "three areas at their targets pass and say so"
else
  bad "three areas at their targets pass and say so" "exit=$st out=$out"
fi

# 2. THE ORIGINAL DEFECT: a profile from the root module alone leaves the
#    drivers area matching nothing. It used to `continue` and then divide by
#    zero; it must fail and name the pattern.
{ echo "mode: atomic"; echo 'github.com/iqhive/iqmq/client.go:1.1,1.2 100 1'; \
  echo 'github.com/iqhive/iqmq/memory/m.go:1.1,1.2 100 1'; } > "$tmp/p2"
out="$(run "$tmp/f1" "$tmp/p2")"; st=$?
if [ "$st" != 0 ] && printf '%s' "$out" | grep -q "NO STATEMENTS"; then
  ok "an area the profile does not reach fails instead of being skipped"
else
  bad "an area the profile does not reach fails instead of being skipped" "exit=$st out=$out"
fi

# 3. A regression below a floor fails.
{ echo "mode: atomic"; echo 'github.com/iqhive/iqmq/client.go:1.1,1.2 100 0'; \
  echo 'github.com/iqhive/iqmq/memory/m.go:1.1,1.2 100 1'; \
  echo 'github.com/iqhive/iqmq/drivers/x/d.go:1.1,1.2 100 1'; } > "$tmp/p3"
out="$(run "$tmp/f1" "$tmp/p3")"; st=$?
if [ "$st" != 0 ] && printf '%s' "$out" | grep -q "core: 0.0% is BELOW the floor"; then
  ok "a drop below the floor fails"
else
  bad "a drop below the floor fails" "exit=$st out=$out"
fi

# 4. A met threshold may not keep a floor below it: that is banked slack, and
#    it is how a threshold silently stops being one.
floors "$tmp/f4" 90.0 40.0 80.0
out="$(run "$tmp/f4" "$tmp/p1")"; st=$?
if [ "$st" != 0 ] && printf '%s' "$out" | grep -q "raise it to the target"; then
  ok "a met area with a floor below its target fails"
else
  bad "a met area with a floor below its target fails" "exit=$st out=$out"
fi

# 5. A floor above §11.4's target is an error, not extra credit; and a
#    shortfall with no owner is not a recorded shortfall.
floors "$tmp/f5" 95.0 85.0 80.0
out="$(run "$tmp/f5" "$tmp/p1")"; st=$?
if [ "$st" != 0 ] && printf '%s' "$out" | grep -q "is a ratchet toward the target"; then
  ok "a floor above its §11.4 target fails"
else
  bad "a floor above its §11.4 target fails" "exit=$st out=$out"
fi
{ echo "mode: atomic"; echo 'github.com/iqhive/iqmq/client.go:1.1,1.2 150 0'; \
  echo 'github.com/iqhive/iqmq/client.go:2.1,2.2 850 1'; \
  echo 'github.com/iqhive/iqmq/memory/m.go:1.1,1.2 100 1'; \
  echo 'github.com/iqhive/iqmq/drivers/x/d.go:1.1,1.2 100 1'; } > "$tmp/p6"
floors "$tmp/f6" 80.0 85.0 80.0 "-"
out="$(run "$tmp/f6" "$tmp/p6")"; st=$?
if [ "$st" != 0 ] && printf '%s' "$out" | grep -q "names no owner for the gap"; then
  ok "a shortfall with no owner fails"
else
  bad "a shortfall with no owner fails" "exit=$st out=$out"
fi

# 6. An area §11.4 names that the floors file drops is a missing threshold.
cat > "$tmp/f7" <<'EOF'
areas:
  - area: core
    target: 90.0
    floor: 90.0
  - area: memory
    target: 85.0
    floor: 85.0
EOF
out="$(run "$tmp/f7" "$tmp/p1")"; st=$?
if [ "$st" != 0 ] && printf '%s' "$out" | grep -q "§11.4 names three"; then
  ok "dropping one of §11.4's three areas fails"
else
  bad "dropping one of §11.4's three areas fails" "exit=$st out=$out"
fi

# 7. Two profiles naming the same statement block would be counted twice by
#    the concatenating merge, so they are refused. This is what stops a
#    `-coverpkg` profile being merged as if it were disjoint.
{ echo "mode: atomic"; echo 'github.com/iqhive/iqmq/client.go:1.1,1.2 100 1'; \
  echo 'github.com/iqhive/iqmq/client.go:1.1,1.2 100 0'; \
  echo 'github.com/iqhive/iqmq/memory/m.go:1.1,1.2 100 1'; \
  echo 'github.com/iqhive/iqmq/drivers/x/d.go:1.1,1.2 100 1'; } > "$tmp/p8"
out="$(run "$tmp/f1" "$tmp/p8")"; st=$?
if [ "$st" != 0 ] && printf '%s' "$out" | grep -q "would count statements twice"; then
  ok "overlapping profiles are refused rather than double-counted"
else
  bad "overlapping profiles are refused rather than double-counted" "exit=$st out=$out"
fi

# 7b. TWO DECOMPOSITIONS of one file, which case 7 cannot see: the block keys
#     differ, so nothing is a duplicate, and the arithmetic sums both. It is what
#     a tree edited DURING a twenty-minute measurement produces, and it moved
#     this repository's core figure by six tenths of a point in the wrong
#     direction on a branch that only added tests.
#
#     Here client.go's first block is present as 1.1,1.2 and as 1.1,2.20 -- one
#     start position, two ends -- which no single compilation of one file emits.
{ echo "mode: atomic"
  echo 'github.com/iqhive/iqmq/client.go:1.1,1.2 100 1'
  echo 'github.com/iqhive/iqmq/client.go:1.1,2.20 150 1'
  echo 'github.com/iqhive/iqmq/memory/m.go:1.1,1.2 100 1'
  echo 'github.com/iqhive/iqmq/drivers/x/d.go:1.1,1.2 100 1'; } > "$tmp/p7b"
out="$(run "$tmp/f1" "$tmp/p7b")"; st=$?
if [ "$st" != 0 ] && printf '%s' "$out" | grep -q "more than one decomposition"; then
  ok "two decompositions of one file are refused rather than summed"
else
  bad "two decompositions of one file are refused rather than summed" "exit=$st out=$out"
fi

# 7c. The control for 7b: a profile whose blocks all have distinct start
#     positions passes it. Without this, 7b would be satisfied by a check that
#     refused every profile it was ever handed.
{ echo "mode: atomic"
  echo 'github.com/iqhive/iqmq/client.go:1.1,1.2 50 1'
  echo 'github.com/iqhive/iqmq/client.go:3.1,4.20 50 1'
  echo 'github.com/iqhive/iqmq/memory/m.go:1.1,1.2 100 1'
  echo 'github.com/iqhive/iqmq/drivers/x/d.go:1.1,1.2 100 1'; } > "$tmp/p7c"
out="$(run "$tmp/f1" "$tmp/p7c")"; st=$?
if printf '%s' "$out" | grep -q "more than one decomposition"; then
  bad "distinct block starts in one file are not a decomposition clash" "exit=$st out=$out"
else
  ok "distinct block starts in one file are not a decomposition clash"
fi

# 8. The MERGE, over a table. hack/cover.sh measures every module with
#    `-coverpkg=github.com/iqhive/iqmq/...`, so the profiles overlap by design
#    and hack/cover-merge.awk unions them -- and the first version of that
#    union reported the repository's core coverage as 100.00 %: an awk array
#    element that was never assigned prints as the empty string, so a block
#    NOBODY covered came out as a two-field row that everything downstream
#    skips, and the covered statements became the whole denominator. Case 7
#    above could not catch it, because it is about the profiles going IN.
merge="$root/hack/cover-merge.awk"
{ echo "mode: atomic"
  echo 'a.go:1.1,1.2 3 0'      # covered by nobody -- the row that vanished
  echo 'b.go:1.1,1.2 5 1'      # covered here, not in the second profile
  echo 'c.go:1.1,1.2 7 2'      # a lower count than the second profile's
  echo 'd.go:1.1,1.2 9 4'; } > "$tmp/m1"
{ echo "mode: atomic"
  echo 'a.go:1.1,1.2 3 0'
  echo 'b.go:1.1,1.2 5 0'
  echo 'c.go:1.1,1.2 7 6'
  echo 'e.go:1.1,1.2 2 1'; } > "$tmp/m2"
got="$(awk -f "$merge" "$tmp/m1" "$tmp/m2" 2>&1)"; mst=$?
want='a.go:1.1,1.2 3 0
b.go:1.1,1.2 5 1
c.go:1.1,1.2 7 6
d.go:1.1,1.2 9 4
e.go:1.1,1.2 2 1'
if [ "$mst" != 0 ]; then
  bad "the merge unions blocks and keeps the uncovered ones" "exit=$mst out=$got"
elif [ "$got" != "$want" ]; then
  bad "the merge unions blocks and keeps the uncovered ones" "want [$want], got [$got]"
elif [ "$(printf '%s\n' "$got" | awk 'NF!=3' | wc -l)" != 0 ]; then
  bad "the merge unions blocks and keeps the uncovered ones" "a row is not three fields: $got"
else
  ok "the merge unions blocks and keeps the uncovered ones"
fi

# 8b. A row the merge does not understand is refused, not dropped: a silently
#     ignored row is a statement that leaves the denominator, which is the
#     same failure as 8 by another route.
{ echo "mode: atomic"; echo 'a.go:1.1,1.2 3 0'; echo 'this is not a profile row at all'; } > "$tmp/m3"
got="$(awk -f "$merge" "$tmp/m3" 2>&1)"; mst=$?
if [ "$mst" = 0 ]; then
  bad "the merge refuses a row it cannot read" "it exited 0 over a malformed profile: $got"
elif ! printf '%s' "$got" | grep -q 'not a coverage row'; then
  bad "the merge refuses a row it cannot read" "the refusal does not name the row: $got"
else
  ok "the merge refuses a row it cannot read"
fi

# 9. -update has to be able to FINISH a shortfall, which is the one thing it
#    could not do: the met-area branch returned before the update block, so
#    `make cover-update` on an area that had reached its §11.4 target printed
#    "raise it to the target" and rewrote nothing, for ever. The case measures
#    an area at its target with a floor below it, runs -update, and asserts the
#    file was rewritten and the plain check then passes.
cat > "$tmp/f9" <<'YAML'
areas:
  - area: core
    target: 90.0
    floor: 85.5
    owner: somebody
    reason: >-
      a recorded shortfall
  - area: memory
    target: 85.0
    floor: 85.0
  - area: drivers
    target: 80.0
    floor: 80.0
YAML
# One covered statement in each area, so every area is measured at 100 %.
{ echo "mode: atomic"
  echo 'github.com/iqhive/iqmq/client.go:1.1,1.2 10 1'
  echo 'github.com/iqhive/iqmq/memory/m.go:1.1,1.2 10 1'
  echo 'github.com/iqhive/iqmq/drivers/x/d.go:1.1,1.2 10 1'; } > "$tmp/p9"
before="$(grep -A2 'area: core' "$tmp/f9" | grep floor)"
out="$(IQMQ_COVER_FLOORS="$tmp/f9" "$root/hack/cover-threshold.sh" -update "$tmp/p9" 2>&1)"; st=$?
after="$(grep -A2 'area: core' "$tmp/f9" | grep floor)"
out2="$(IQMQ_COVER_FLOORS="$tmp/f9" "$root/hack/cover-threshold.sh" "$tmp/p9" 2>&1)"; st2=$?
if printf '%s' "$after" | grep -q '85.5'; then
  bad "-update raises a floor whose area has reached its target" \
    "the floor was not rewritten (before [$before], after [$after]): $out"
elif ! printf '%s' "$after" | grep -q '90.0'; then
  bad "-update raises a floor whose area has reached its target" "the floor is not the target: [$after]"
elif [ "$st2" != 0 ]; then
  bad "-update raises a floor whose area has reached its target" \
    "the check still fails after the update (exit $st2): $out2"
else
  ok "-update raises a floor whose area has reached its target"
fi

# 10. Compare the rounded percentage against the floor.
# Synthetic counts exercise both sides of the rounding boundary at 80.0%:
# 24,143 / 30,197 rounds to 80.0 and passes; 24,142 rounds to 79.9 and fails.
boundary_profile() { # PATH DRV_COVERED DRV_TOTAL
  { echo "mode: atomic"
    echo 'github.com/iqhive/iqmq/client.go:1.1,1.2 100 1'
    echo 'github.com/iqhive/iqmq/memory/m.go:1.1,1.2 100 1'
    printf 'github.com/iqhive/iqmq/drivers/x/d.go:1.1,1.2 %s 1\n' "$2"
    printf 'github.com/iqhive/iqmq/drivers/x/d.go:2.1,2.2 %s 0\n' "$(( $3 - $2 ))"
  } > "$1"
}
boundary_profile "$tmp/p10a" 24143 30197
out="$(run "$tmp/f1" "$tmp/p10a")"; st=$?
if [ "$st" = 0 ] && printf '%s' "$out" | grep -q 'drivers: 80.0%'; then
  ok "the last count that PRINTS the floor passes (24143/30197 = 79.9517 -> 80.0)"
else
  bad "the last count that PRINTS the floor passes (24143/30197 = 79.9517 -> 80.0)" \
    "exit=$st out=$out -- if this fails, the gate now compares the RAW percentage and assumption C15.1's margins are each wrong by one"
fi

boundary_profile "$tmp/p10b" 24142 30197
out="$(run "$tmp/f1" "$tmp/p10b")"; st=$?
if [ "$st" != 0 ] && printf '%s' "$out" | grep -q 'drivers: 79.9% is BELOW the floor'; then
  ok "one statement further down prints 79.9 and fails"
else
  bad "one statement further down prints 79.9 and fails" "exit=$st out=$out"
fi

printf '\ncover-threshold_test: %d passed, %d failed\n' "$pass" "$fail"
[ "$fail" -eq 0 ]
