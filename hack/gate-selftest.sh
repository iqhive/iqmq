#!/usr/bin/env bash
# Prove the §10.6 benchmark gate can fail, by making it fail.
#
#   hack/gate-selftest.sh [GATE]        # make gate-selftest
#
#   1. the gate on a clean tree against the same commit, which must PASS;
#   2. the gate on a throwaway branch carrying a deliberately injected
#      allocation in a gated hot path, which must FAIL and must say so on the
#      row that gained the allocation.
#
# It exists because this repository has found six gates vacuous after trusting
# them -- a `|| true` that swallowed every allocation regression, a lint that
# ran from no target, a model checker that was a `strings.Contains`, a load
# guard that called its own caller a competitor. A gate nobody has watched fail
# is assumed not to work.
#
# The injection is one package-level variable and one assignment in
# internal/wire's fixed-layout Encode, which the performance budgets requires to
# make zero allocations with a caller buffer. Assigning to a package-level
# variable is what makes the allocation escape; a discarded `make` would be
# stack-allocated or removed outright and would prove nothing.
set -euo pipefail
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
gate="${1:-selftest}"

# Twenty samples a side at 100 ms, which is 200 ms per benchmark against the
# 3000 ms ceiling and about fifteen seconds a half over four rows: the clean
# half has to come back CONCLUSIVE, and ten samples of a 64 KiB encode did not
# (a ±9.6 % median interval against the ±5 % reject threshold).
export IQMQ_GATE_ROUNDS="${IQMQ_GATE_ROUNDS:-10}"
export IQMQ_GATE_BENCHTIME="${IQMQ_GATE_BENCHTIME:-100ms}"

head_sha="$(git -C "$root" rev-parse --verify HEAD)"
stamp="$(date -u +%Y%m%dT%H%M%SZ)"
branch="gate-selftest/$stamp"
wt="$(mktemp -d)"
cleanup() {
  git -C "$root" worktree remove --force "$wt" >/dev/null 2>&1 || true
  git -C "$root" branch -D "$branch" >/dev/null 2>&1 || true
}
trap cleanup EXIT

# A guard refusal is not a gate failure, and confusing the two would make this
# script cry wolf on a busy host: 4 is "the run was contended, discard it" and
# 5 is "the host never went quiet, nothing ran". 3 is benchgate's own
# "inconclusive": a self-test that cannot decide has not demonstrated anything,
# so it does not pass, but it is reported and exited AS 3 rather than as 1 --
# it is a statement about the samples, not about the gate or the tree.
guarded() { # LABEL CMD...
  local label="$1"; shift
  local st=0
  "$@" || st=$?
  case "$st" in
    4) echo "!! gate-selftest: $label was CONTENDED; the host is busy. Nothing is proved either way — retry." >&2; exit 4 ;;
    5) echo "!! gate-selftest: the load guard refused to start $label; the host never went quiet. Retry later." >&2; exit 5 ;;
  esac
  return "$st"
}

echo "############ 1/2 clean tree: the gate must PASS ############"
clean=0
guarded "the clean run" "$root/hack/benchgate.sh" "$head_sha" "$gate" || clean=$?
# 3 still fails this half, for the reason the comment above gives, but it is
# not the same finding as 1 and must not be reported as one. Until the `go run`
# in hack/benchgate.sh became a `go build`, 3 arrived here AS 1 and this branch
# printed "a real regression" over a gate that had declined to decide.
if [ "$clean" -eq 3 ]; then
  echo "!! gate-selftest: the gate could not DECIDE on a clean tree (exit 3): at least one gate row's" >&2
  echo "   median confidence interval was wider than its delta. Nothing is proved about the gate" >&2
  echo "   either way; this needs a quieter host or more samples, not a code change. Retry." >&2
  exit 3
fi
if [ "$clean" -ne 0 ]; then
  echo "!! gate-selftest: the gate FAILED on a clean tree (exit $clean). Either the tree has a real"
  echo "   regression against $head_sha, or the gate is broken. Read the verdict above." >&2
  exit 1
fi
echo "== gate-selftest: clean tree passed"

echo "############ 2/2 injected allocation: the gate must FAIL ############"
git -C "$root" worktree add -b "$branch" "$wt" "$head_sha" >/dev/null
target="$wt/internal/wire/fixed.go"
python3 - "$target" <<'PY'
import sys
path = sys.argv[1]
src = open(path).read()
anchor = "func (*fixedLayout) Encode(dst []byte, p *payload.Payload) ([]byte, error) {\n"
if anchor not in src:
    sys.exit("gate-selftest: the injection anchor is gone from " + path)
src = src.replace(anchor, anchor +
    "\t// gate self-test: a deliberately injected allocation (hack/gate-selftest.sh).\n"
    "\tgateSelfTestSink = make([]byte, 1)\n", 1)
src += "\n// gateSelfTestSink is what makes the injected allocation escape.\nvar gateSelfTestSink []byte\n"
open(path, "w").write(src)
PY
grep -q gateSelfTestSink "$target" || { echo "!! gate-selftest: the injection did not take" >&2; exit 2; }
git -C "$wt" add -A
git -C "$wt" -c user.name=gate-selftest -c user.email=gate-selftest@localhost \
  commit -qm "test(wire): inject one allocation into Encode (gate self-test)"

injected=0
"$wt/hack/benchgate.sh" "$head_sha" "$gate" > "$wt/verdict.txt" 2>&1 || injected=$?
cat "$wt/verdict.txt"
case "$injected" in
  4) echo "!! gate-selftest: the injected run was CONTENDED; the host is busy. Nothing is proved — retry." >&2; exit 4 ;;
  5) echo "!! gate-selftest: the load guard refused to start the injected run. Retry later." >&2; exit 5 ;;
esac
if [ "$injected" -eq 0 ]; then
  echo "!! gate-selftest: THE GATE PASSED a tree with an injected allocation in a 0-allocation"
  echo "   hot path. The gate does not work." >&2
  exit 1
fi
if ! grep -qE 'allocs/op 0 -> 1' "$wt/verdict.txt"; then
  echo "!! gate-selftest: the gate failed (exit $injected) but not on the allocation:" >&2
  echo "   no row reports 'allocs/op 0 -> 1'. It may have failed for another reason." >&2
  exit 1
fi
rows="$(grep -cE 'allocs/op 0 -> 1' "$wt/verdict.txt" || true)"
echo "== gate-selftest: the injected allocation failed the gate (exit $injected) on $rows row(s)"
echo "== gate-selftest: PASS — the gate is green on a clean tree and red on an injected allocation"
