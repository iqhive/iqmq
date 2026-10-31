#!/usr/bin/env bash
# Self-test for hack/pinsweep.py.
#
# the contributor notes: "an instrument nobody has watched fail is assumed not to
# work."  Two earlier versions of this sweep did not work, and both reported
# success while doing so -- one wrote byte-identical files and called it "4
# flags flipped"; the other scored one verdict per module and would have
# reported PINNED for a rule nothing pinned.  So this file watches the
# instrument produce each of its verdicts on a case whose answer is known
# independently of the instrument.
#
# It is deliberately CHEAP: three cases in ONE module, not the 32-construction
# sweep, so it can be run on any change to the tool.  The full sweep is
# `make pinsweep-nightly` and is far too expensive for a per-change gate -- it
# runs nightly against the verdicts stored in `hack/pinsweep-baseline.tsv`, and
# the COMPARISON against that baseline is watched here too, because it takes two
# files and so every one of its answers costs milliseconds.
#
# Usage: hack/pinsweep_test.sh
set -uo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$root" || exit 2
export GOMAXPROCS=${GOMAXPROCS:-4} GOFLAGS=${GOFLAGS:--p=4}

pass=0
fail=0

# check <want> <name> <file> <pattern> <modules>
check() {
	local want="$1" name="$2" file="$3" pattern="$4" mods="$5"
	local out got
	out="$(python3 hack/pinsweep.py one "$file" "$pattern" "$mods" 2>&1)"
	got="${out%%$'\t'*}"
	if [ "$got" = "$want" ]; then
		printf '  ok        %-34s %s\n' "$name" "$got"
		pass=$((pass + 1))
	else
		printf '  FAILED    %-34s got %s, want %s\n' "$name" "${got:-<none>}" "$want"
		printf '            %s\n' "$out"
		fail=$((fail + 1))
	fi
	# The tool must leave the tree exactly as it found it, whatever it decided.
	if ! git diff --quiet -- "$file"; then
		printf '  FAILED    %-34s left %s modified\n' "$name" "$file"
		git checkout -- "$file"
		fail=$((fail + 1))
	fi
}

echo "pinsweep self-test: an inventory audit, then three known cases in drivers/amqp10"

# The inventory must still name every ambiguous construction in the tree, and
# every pattern must still select exactly one line. A hand-written list rots,
# and a sweep over a rotted list reports a clean bill of health for a class it
# has stopped covering -- which is the failure this instrument exists to stop.
# Costs no test run.
if out="$(python3 hack/pinsweep.py audit 2>&1)"; then
	printf '  ok        %-34s %s\n' "the inventory covers the tree" "${out##*$'\n'}"
	pass=$((pass + 1))
else
	printf '  FAILED    %-34s\n%s\n' "the inventory covers the tree" "$out"
	fail=$((fail + 1))
fi

# A construction whose flag IS pinned. drivers/amqp10's `received` outcome row
# asserts the driver's Retryable flag off the *OpError , so
# flipping the rule must be noticed. This is the case review round 1 found
# unpinned and this branch repaired: if the repair is ever undone, or if the
# instrument stops being able to say PINNED, this fails.
check PINNED "a pinned rule reports PINNED" \
	drivers/amqp10/errors.go \
	'{Match: matchOutcome(OutcomeReceived)' \
	drivers/amqp10

# A construction whose flag is NOT pinned: amqp10's second ambiguous rule,
# `settleLost` under ReceiverSettleSecond. Nothing asserts its flag, on this
# branch or on main, and it is recorded in the maintenance notes rather than
# repaired. An instrument that cannot say UNPINNED cannot find the class.
check UNPINNED "an unpinned rule reports UNPINNED" \
	drivers/amqp10/errors.go \
	'{Match: isTransportGone, Code: mq.CodeSettlementUnknown' \
	drivers/amqp10

# And it must refuse rather than guess when it cannot locate the construction.
# A verdict the tool cannot support is the failure mode that produced two
# rounds of wrong findings; costs no test run.
check NO-MUTATION "a stale pattern refuses to guess" \
	drivers/amqp10/errors.go \
	'{Match: thisPatternMatchesNothing' \
	drivers/amqp10

# ------------------------------------------------ a failure the flip did not cause
#
# PINNED means a test noticed the flip, so a failure the flip did not cause must
# not read as one. The sweep used to call any failing run PINNED unless it saw a
# build error: a test already failing on the unmutated tree, a TestMain that
# exits, or a module that does not build under GOWORK=off (the state of every
# driver whose go.mod requires an unpublished root) each pinned every
# construction they touched, and the nightly could never report one going
# UNPINNED. These run in a throwaway module with one ambiguous rule, so the
# answer is known without the instrument; a few seconds each.
fixture_case() { # want name test-file-source
	local want="$1" name="$2" src="$3" dir out got
	dir="$(mktemp -d)"
	mkdir -p "$dir/m"
	printf 'module example.com/m\n\ngo 1.27.0\n' > "$dir/m/go.mod"
	cat > "$dir/m/rules.go" <<-'EOF'
	// Package m holds one ambiguous rule for hack/pinsweep_test.sh to flip.
	package m

	// Rule is a stand-in for a driver's classification row.
	type Rule struct {
		Name      string
		Ambiguous bool
		Retryable bool
	}

	// Rules holds the one ambiguous construction.
	var Rules = []Rule{
		{Name: "lost", Ambiguous: true, Retryable: false},
	}
	EOF
	printf '%s\n' "$src" > "$dir/m/rules_test.go"
	out="$(python3 hack/pinsweep.py --tree "$dir" one m/rules.go '{Name: "lost"' m 2>&1)"
	got="${out%%$'\t'*}"
	rm -rf "$dir"
	if [ "$got" = "$want" ]; then
		printf '  ok        %-34s %s\n' "$name" "$got"
		pass=$((pass + 1))
	else
		printf '  FAILED    %-34s got %s, want %s\n' "$name" "${got:-<none>}" "$want"
		printf '            %s\n' "$out"
		fail=$((fail + 1))
	fi
}

echo "pinsweep self-test: failures the flip did not cause, in a throwaway module"

pin='func TestTheLostRuleIsNotRetryable(t *testing.T) {
	if Rules[0].Retryable {
		t.Fatal("the lost rule is retryable")
	}
}'
broken='func TestSomethingElseIsBroken(t *testing.T) { t.Fatal("broken before any flip") }'

fixture_case PINNED "a test that reads the flag pins it" \
	"$(printf 'package m\n\nimport "testing"\n\n%s' "$pin")"
fixture_case TEST-ERR "a test already failing pins nothing" \
	"$(printf 'package m\n\nimport "testing"\n\n%s' "$broken")"
fixture_case PINNED "a pin beside a test already failing" \
	"$(printf 'package m\n\nimport "testing"\n\n%s\n\n%s' "$broken" "$pin")"
fixture_case TEST-ERR "a run failing with no test pins nothing" \
	"$(printf 'package m\n\nimport (\n\t"os"\n\t"testing"\n)\n\nfunc TestMain(m *testing.M) { os.Exit(1) }\n\n%s' "$pin")"

# ------------------------------------------------- an ambiguous ASSIGNMENT
#
# The audit used to match `.Ambiguous = true` and nothing else on the left of
# an `=`, so a parallel assignment (`oe.Code, oe.Ambiguous, oe.Retryable =
# mq.CodeSettlementUnknown, true, false`, amqp10's unknownSettlement), a flag
# given a variable, and `oe.Code = mq.CodeAmbiguous` were constructions it
# could not see; and the sweep rewrote that parallel form into a statement that
# does not compile. Each form is held here in a throwaway module: the audit has
# to name it, and the sweep has to flip it and see the pin.
assign_case() { # name assignment-line
	local name="$1" line="$2" dir out got
	dir="$(mktemp -d)"
	mkdir -p "$dir/m"
	printf 'module example.com/m\n\ngo 1.27.0\n' > "$dir/m/go.mod"
	cat > "$dir/m/errors.go" <<-EOF
	// Package m holds one ambiguous assignment for hack/pinsweep_test.sh.
	package m

	// OpError is a stand-in for *mq.OpError.
	type OpError struct {
		Code      string
		Ambiguous bool
		Retryable bool
	}

	// CodeAmbiguous stands in for mq.CodeAmbiguous.
	const CodeAmbiguous = "ambiguous"

	// Lost marks e as an unknown outcome.
	func Lost(e *OpError, sent bool) {
		$line
	}
	EOF
	cat > "$dir/m/errors_test.go" <<-'EOF'
	package m

	import "testing"

	func TestALostErrorIsNotRetryable(t *testing.T) {
		e := &OpError{Ambiguous: true}
		Lost(e, true)
		if e.Retryable {
			t.Fatal("a lost error is retryable")
		}
	}
	EOF
	: > "$dir/inventory.tsv"
	out="$(python3 hack/pinsweep.py --tree "$dir" --inventory "$dir/inventory.tsv" audit 2>&1)" && got=0 || got=$?
	if [ "$got" != 1 ] || ! printf '%s' "$out" | grep -q "m/errors.go:16 is an ambiguous construction the inventory does not name"; then
		printf '  FAILED    %-34s the audit did not name it (exit %s)\n%s\n' "$name: audit" "$got" "$out"
		fail=$((fail + 1))
	else
		printf '  ok        %-34s %s\n' "$name: audit" "named"
		pass=$((pass + 1))
	fi
	out="$(python3 hack/pinsweep.py --tree "$dir" one m/errors.go "$line" m 2>&1)"
	got="${out%%$'\t'*}"
	rm -rf "$dir"
	if [ "$got" = PINNED ]; then
		printf '  ok        %-34s %s\n' "$name: sweep" "$got"
		pass=$((pass + 1))
	else
		printf '  FAILED    %-34s got %s, want PINNED\n            %s\n' "$name: sweep" "${got:-<none>}" "$out"
		fail=$((fail + 1))
	fi
}

echo "pinsweep self-test: ambiguous assignments, in a throwaway module"

assign_case "a parallel assignment" 'e.Code, e.Ambiguous, e.Retryable = CodeAmbiguous, true, false'
assign_case "a flag given a variable" 'e.Code, e.Retryable, e.Ambiguous = "lost", false, sent'
assign_case "an ambiguous code assigned" 'e.Code = CodeAmbiguous'

# ---------------------------------------------------------- the stored baseline
#
# The full sweep is minutes of module test runs, so it is a nightly job and the
# thing that makes a nightly job READ is a stored baseline: a construction going
# from PINNED to UNPINNED has to be an alert rather than a line somebody eyeballs
# in a diff.
#
# The comparison is watched here rather than in the nightly, and deliberately:
# it takes two FILES, so every one of its answers can be provoked in
# milliseconds. A comparison exercised only by the nightly would be an
# instrument nobody has watched fail -- and the one answer that matters is the
# one that happens least often.
cmp_case() { # want-status name baseline-lines results-lines want-text
	local want="$1" name="$2" base="$3" res="$4" text="$5" dir out got
	dir="$(mktemp -d)"
	# `%b` and not `%s`: the fixtures below spell their tabs as `\t`, and %s would
	# write the two characters. The comparison would then refuse every line as
	# malformed and every case would "fail" for a reason that is the test's.
	printf '%b\n' "$base" > "$dir/base.tsv"
	printf '%b\n' "$res" > "$dir/res.tsv"
	out="$(python3 hack/pinsweep.py compare "$dir/base.tsv" "$dir/res.tsv" 2>&1)" && got=0 || got=$?
	rm -rf "$dir"
	if [ "$got" != "$want" ]; then
		printf '  FAILED    %-34s exit %s, want %s\n%s\n' "$name" "$got" "$want" "$out"
		fail=$((fail + 1))
		return
	fi
	if [ -n "$text" ] && ! printf '%s' "$out" | grep -q -- "$text"; then
		printf '  FAILED    %-34s did not say %s\n%s\n' "$name" "$text" "$out"
		fail=$((fail + 1))
		return
	fi
	printf '  ok        %-34s exit=%s\n' "$name" "$got"
	pass=$((pass + 1))
}

echo "pinsweep self-test: the stored-baseline comparison, every answer"

P='PINNED	drivers/x/errors.go	{Match: a'
U='UNPINNED	drivers/x/errors.go	{Match: a'
B='BUILD-ERR	drivers/x/errors.go	{Match: a'
OTHER='PINNED	drivers/y/errors.go	{Match: b'

cmp_case 0 "a matching baseline passes" "$P" "$P" "construction(s) match"
cmp_case 1 "PINNED -> UNPINNED is a regression" "$P" "$U" "REGRESSION"
cmp_case 1 "a verdict that became a refusal" "$P" "$B" "REGRESSION"
cmp_case 3 "UNPINNED -> PINNED is drift" "$U" "$P" "out of date"
cmp_case 3 "a construction the baseline lacks" "$P" "$(printf '%s\n%s' "$P" "$OTHER")" "new"
cmp_case 3 "a construction the sweep dropped" "$(printf '%s\n%s' "$P" "$OTHER")" "$P" "gone"
# A regression AND drift in one run must report the REGRESSION, because a run
# that is both is a regression: an exit status of 3 would have a reader
# regenerate the baseline and commit the missing test away.
cmp_case 1 "a regression beside drift reports the regression" \
	"$(printf '%s\n%s' "$P" "$OTHER")" "$(printf '%s\n%s' "$U" "UNPINNED\tdrivers/z/errors.go\t{Match: c")" \
	"REGRESSION"

# No stored baseline at all is the most out-of-date baseline there is, so it is
# drift (3) and not a pass. It used to exit 0, and with the baseline file
# gitignored every run of the monthly sweep had none: the job went green
# whatever the sweep found, so it could never report a regression. The sweep
# still finishes and keeps its verdicts, which are the baseline to commit.
# `--filter` matching nothing makes the sweep itself free.
nobase_case() { # name command...
	local name="$1" dir out got
	shift
	dir="$(mktemp -d)"
	out="$("$@" "$dir/missing.tsv" "$dir/res.tsv" 2>&1)" && got=0 || got=$?
	rm -rf "$dir"
	if [ "$got" != 3 ] || ! printf '%s' "$out" | grep -q -- "no stored baseline"; then
		printf '  FAILED    %-34s exit %s, want 3 and "no stored baseline"\n%s\n' "$name" "$got" "$out"
		fail=$((fail + 1))
		return
	fi
	printf '  ok        %-34s exit=%s\n' "$name" "$got"
	pass=$((pass + 1))
}
# The verdicts must be written whatever the exit status: they are the baseline
# to commit. A missing file is its own status, so it cannot pass as 3.
sweep_nobase() {
	local st=0
	python3 hack/pinsweep.py all --filter '#matches-nothing#' --baseline "$1" --out "$2" || st=$?
	[ -f "$2" ] || { echo "no verdicts written to $2"; return 9; }
	return "$st"
}
nobase_case "compare with no stored baseline" python3 hack/pinsweep.py compare
nobase_case "a sweep with no stored baseline" sweep_nobase

echo "pinsweep self-test: $pass passed, $fail failed"
[ "$fail" -eq 0 ]
