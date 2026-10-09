#!/usr/bin/env bash
# The §10.6 gate's EXIT STATUS, watched where it was never watched.
#
# hack/benchgate.sh documents six statuses and hack/bench/run-local.sh branches
# on three of them, but both reached benchmarks/cmd/benchgate through `go run`,
# which does not propagate a program's exit status: it prints "exit status N" on
# stderr and exits 1 itself, whatever N was. So 2 (setup error) and 3
# (inconclusive) could not be observed by any caller, and 1 is the status that
# means "regression". #85's gate measured cleanly, called two rows inconclusive,
# wrote `benchgate: INCONCLUSIVE` into its own verdict, and failed the pull
# request as a regression with `Regressions: 0` in the table above the verdict.
#
# hack/gate-selftest.sh could not catch it: it asserts 0 on a clean tree and
# "not 0" on an injected allocation, so 2 and 3 fall between its two cases.
# benchmarks/cmd/benchgate's own Go tests could not catch it either -- they call
# `run()` in-process and see the right number. The defect lived only in the seam
# between the two, which is what this file tests.
#
# It measures nothing and takes a second or two: the only Go it compiles is an
# eight-line stub.
set -uo pipefail
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# The stub below is eight lines, but this script does invoke `go`, and the rule
# that every such path applies the ceiling is not worth an exception for a small
# compile: `make cpu-cap-check` flagged it as `uncapped-script` and taking the
# cap is cheaper than arguing for a waiver.
eval "$("$root/hack/cpucap.sh" env)"
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT
pass=0; fail=0
ok()  { printf 'ok   %s\n' "$1"; pass=$((pass+1)); }
bad() { printf 'FAIL %s\n     %s\n' "$1" "$2"; fail=$((fail+1)); }

# ---------------------------------------------------------------------------
# The mechanism. A stub that exits 3, reached both ways.
#
# These two cases are the REASON the rest of this file exists, so they assert
# the toolchain's behaviour rather than assuming it. If a future Go propagates
# the status through `go run`, case 1 fails, and the comments in
# hack/benchgate.sh and hack/bench/run-local.sh that justify a `go build` by
# this behaviour become stale rather than silently wrong.
# ---------------------------------------------------------------------------
mkdir -p "$tmp/stub"
cat > "$tmp/stub/main.go" <<'EOF'
package main

import "os"

func main() { os.Exit(3) }
EOF
cat > "$tmp/stub/go.mod" <<'EOF'
module gatestub

go 1.26
EOF

st=0
(cd "$tmp/stub" && GOWORK=off go run . >/dev/null 2>&1) || st=$?
if [ "$st" = 1 ]; then
  ok "1. \`go run\` of a program exiting 3 exits 1 -- the flattening this file guards against"
else
  bad "1. \`go run\` of a program exiting 3" \
      "exited $st, want 1. If this is now 3, go run propagates the status and the \`go build\` in hack/benchgate.sh no longer needs its comment."
fi

st=0
(cd "$tmp/stub" && GOWORK=off go build -o "$tmp/stub/bin" . && "$tmp/stub/bin") || st=$?
[ "$st" = 3 ] \
  && ok "2. the same program, BUILT and run, exits 3" \
  || bad "2. a built program exiting 3" "exited $st, want 3"

# ---------------------------------------------------------------------------
# The call sites. A tool whose exit status is a documented contract has to be
# built, not `go run`.
# ---------------------------------------------------------------------------
for f in hack/benchgate.sh hack/bench/run-local.sh; do
  if grep -qE '^[^#]*go run [^|]*\./cmd/benchgate' "$root/$f"; then
    bad "3. $f reaches ./cmd/benchgate through \`go run\`" \
        "$(grep -nE '^[^#]*go run [^|]*\./cmd/benchgate' "$root/$f" | head -n1)"
  else
    ok "3. $f builds ./cmd/benchgate rather than \`go run\`-ing it"
  fi
done

# ---------------------------------------------------------------------------
# The general shape, so the next tool with a meaningful status does not repeat
# it: a script that CAPTURES a `go run`'s status into a variable is reading a
# number that can only ever be 0 or 1.
#
# Capturing with `|| var=$?` is the shape that means "I am about to branch on
# this". `if ! ...` and `|| { ... }` are not matched: those ask only whether it
# failed, which `go run` does answer correctly.
# ---------------------------------------------------------------------------
offenders=""
while IFS= read -r f; do
  hit="$(awk '/go run/ {buf = NR}
              /\|\| *[A-Za-z_]+=\$\?/ && buf && NR - buf < 8 {print FILENAME ":" NR}' "$f")"
  [ -n "$hit" ] && offenders="$offenders$hit"$'\n'
done < <(find "$root/hack" -name '*.sh' -not -name '*_test.sh' | sort)

if [ -z "$offenders" ]; then
  ok "4. no hack/*.sh captures a \`go run\` exit status into a variable"
else
  bad "4. a \`go run\` exit status is captured and branched on" \
      "$(printf '%s' "$offenders" | tr '\n' ' ')-- build the command instead; go run collapses every non-zero status to 1"
fi

# ---------------------------------------------------------------------------
# The flag that stops a hosted runner judging an absolute TIME, and the fact
# that it is OPT-IN. A gate that passed it unconditionally would stop enforcing
# the ns/op ceilings everywhere, which is the failure this is meant to avoid
# rather than cause.
# ---------------------------------------------------------------------------
if grep -q 'IQMQ_GATE_PROVISIONAL_HOST' "$root/hack/benchgate.sh" \
   && grep -q -- '-provisional-host' "$root/hack/benchgate.sh"; then
  ok "6. hack/benchgate.sh passes -provisional-host, and only when IQMQ_GATE_PROVISIONAL_HOST is set"
else
  bad "6. hack/benchgate.sh does not gate -provisional-host on IQMQ_GATE_PROVISIONAL_HOST" \
      "a hosted runner cannot judge an absolute ns/op ceiling, and the flag must not be unconditional"
fi

# The command must accept it, so the wrapper is not passing a flag into a
# version of ./cmd/benchgate that does not have one.
if (cd "$root/benchmarks" && GOWORK=off go run ./cmd/benchgate -help 2>&1) \
   | grep -q -- '-provisional-host'; then
  ok "7. ./cmd/benchgate accepts -provisional-host and documents it in its usage"
else
  bad "7. ./cmd/benchgate does not accept -provisional-host" \
      "hack/benchgate.sh passes it; a flag the command rejects makes the gate a setup error"
fi

# And the workflow that measures on a hosted runner has to set it, or the
# breach that started this comes straight back.
if grep -q 'IQMQ_GATE_PROVISIONAL_HOST' "$root/.github/workflows/bench.yml"; then
  ok "8. the hosted-runner gate in bench.yml sets IQMQ_GATE_PROVISIONAL_HOST"
else
  bad "8. bench.yml's hosted-runner gate does not set IQMQ_GATE_PROVISIONAL_HOST" \
      "it measured 10.1 and 31.9 ns/op for one benchmark on two runner draws, against an 11 ns/op ceiling"
fi

# ---------------------------------------------------------------------------
# The contract itself: the statuses hack/benchgate.sh's header documents are the
# ones benchmarks/cmd/benchgate can return. The header is what a reader of a red
# gate consults, and #85 showed what it costs when the two drift.
# ---------------------------------------------------------------------------
declared="$(sed -n '/^# Exit status:/,/^#$/p' "$root/hack/benchgate.sh" \
            | grep -oE '\b[0-9]\b' | sort -u | tr '\n' ' ')"
implemented="$(grep -oE 'return [0-9]$' "$root/benchmarks/cmd/benchgate/main.go" \
               | grep -oE '[0-9]' | sort -u | tr '\n' ' ')"
# The script adds 4 and 5 itself (the guard's verdicts) on top of the Go tool's
# 0..3, so the tool's set must be a SUBSET of what the header declares.
mismatch=""
for c in $implemented; do
  case " $declared " in *" $c "*) ;; *) mismatch="$mismatch $c" ;; esac
done
if [ -z "$mismatch" ]; then
  ok "9. every status ./cmd/benchgate returns ($implemented) is documented in hack/benchgate.sh's header ($declared)"
else
  bad "9. ./cmd/benchgate returns a status its wrapper does not document" \
      "undocumented:$mismatch; header declares: $declared"
fi

printf '\n%d passed / %d failed\n' "$pass" "$fail"
[ "$fail" -eq 0 ]
