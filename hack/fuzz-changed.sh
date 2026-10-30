#!/usr/bin/env bash
# Fuzz every target in packages changed since BASE for TIME each:
#   hack/fuzz-changed.sh BASE_REF [TIME]
#
# Exit status: 0 no crasher · 1 at least one target produced one.
#
# This script used to carry an exemption for go.dev/issue/75804, in which
# go1.26's fuzzing coordinator could report an expired `-fuzztime` deadline AS
# the fuzzing error -- a target failing with a bare `context deadline exceeded`,
# no named input and nothing to replay. It is gone because its first condition
# was "the running toolchain is older than go1.27", and the floor is now
# `go 1.27.0` (ADR-015), so it could never fire again. The
# fix is in go1.27.0 itself, read from the source rather than from the bug
# report: `src/internal/fuzz/fuzz.go:129` suppresses `ctx.Err()` as well as
# `fuzzCtx.Err()` on go1.27.0 and go1.27.1, and only `fuzzCtx.Err()` on
# go1.26.0. Self-limiting by construction is why removing it is a deletion and
# not a decision.
set -euo pipefail
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# Heavy, and CI invokes this script DIRECTLY rather than through make, so the
# Makefile's $(CPU_RUN) cannot reach it and the wrapper has to be here as well.
# On the shared hosts cpu-policy waits for the machine, takes the machine-wide
# heavy-job lock and caps GOMAXPROCS; elsewhere it is a passthrough, and on the
# GitHub runners it is not installed and this is a no-op.
# The CPU_POLICY_HELD test is what stops the re-exec looping, and it doubles as
# the nesting rule: run from a make target that already holds the lock, this
# passes straight through instead of waiting on its own parent.
if [ "${CPU_POLICY_HELD:-}" != 1 ] && command -v cpu-policy >/dev/null 2>&1; then
  exec cpu-policy run --kind fuzz -- "$0" "$@"
fi
# -parallel is the fuzz WORKER count, and a fuzz worker is a PROCESS with its
# own GOMAXPROCS, so it multiplies with the width exactly as -p does. It was a
# hardcoded 4; FUZZ_WORKERS comes from the ceiling.
eval "$("$root/hack/cpucap.sh" env)"
workers="${FUZZ_WORKERS:-$("$root/hack/cpucap.sh" p)}"
base="${1:?base ref}"; time="${2:-15s}"
cd "$root"
dirs="$(git diff --name-only "$base"...HEAD -- '*.go' | xargs -r -n1 dirname | sort -u)"
[ -n "$dirs" ] || { echo "no Go changes"; exit 0; }
status=0
# The output is STREAMED, never captured. A target that hangs is exactly the
# case where the log matters, and a 35-minute job timeout would discard a
# buffer -- so the first version of this, which captured into a variable, would
# have thrown away the evidence for the one failure mode that is hardest to
# diagnose. It went through `tee` into a file while `deadline_race` had to read
# the text back; with that exemption gone nothing reads it, so the file, the
# pipeline and the PIPESTATUS dance that the pipe required are all gone too.

for d in $dirs; do
  m="$d"; while [ ! -f "$m/go.mod" ] && [ "$m" != "." ]; do m="$(dirname "$m")"; done
  # The package's path inside its module. The root module is the case a prefix
  # strip gets wrong: `git diff` names internal/engine, not ./internal/engine,
  # so stripping "./" left it unchanged and the old test then read that as
  # "the module's own directory" and fuzzed the root package instead of every
  # changed package of the root module.
  if [ "$m" = "." ]; then rel="$d"; elif [ "$m" = "$d" ]; then rel="."; else rel="${d#"$m"/}"; fi
  for t in $(cd "$m" && GOWORK=off go test -list '^Fuzz' "./$rel" 2>/dev/null | grep '^Fuzz' || true); do
    echo "==> $d $t"
    # `|| st=$?` rather than a bare call: errexit would otherwise abort the
    # whole script on the first target that finds something, and the remaining
    # changed packages would never be fuzzed at all.
    st=0
    (cd "$m" && GOWORK=off go test -run='^$' -fuzz="^${t}\$" -fuzztime="$time" -parallel "$workers" "./$rel") || st=$?
    [ "$st" -eq 0 ] && continue
    status=1
  done
done

exit $status
