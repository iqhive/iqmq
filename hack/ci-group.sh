#!/usr/bin/env bash
# Run a command in every module of a CI group (hack/module-groups.json) that
# exists and contains Go sources: hack/ci-group.sh GROUP CMD [ARGS...]
set -euo pipefail
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# Heavy, and CI invokes this script DIRECTLY rather than through make, so the
# Makefile's $(CPU_RUN) cannot reach it and the wrapper has to be here as well.
# On the shared hosts cpu-policy waits for the machine, takes the machine-wide
# heavy-job lock and caps GOMAXPROCS; elsewhere it is a passthrough, and on the
# GitHub runners it is not installed and this is a no-op.
#
# The CPU_POLICY_HELD test is what stops the re-exec looping, and it doubles as
# the nesting rule: run from a make target that already holds the lock, this
# passes straight through instead of waiting on its own parent.
if [ "${CPU_POLICY_HELD:-}" != 1 ] && command -v cpu-policy >/dev/null 2>&1; then
  exec cpu-policy run --kind unit -- "$0" "$@"
fi
# The sharded twin of hack/for-each-module.sh, and capped the same way: the
# workflow calls this script directly rather than through make, so it does not
# inherit the Makefile's exports. On a two-CPU runner hack/cpucap.sh computes
# the 2 and 2 `go` already chose, so nothing changes there; the cap matters
# when this is run on a big machine.
eval "$("$root/hack/cpucap.sh" env)"
group="${1:?group}"; shift
mapfile -t mods < <(python3 -c 'import json,sys; print("\n".join(json.load(open(sys.argv[1]))[sys.argv[2]]))' "$root/hack/module-groups.json" "$group")
status=0; ran=0
for m in "${mods[@]}"; do
  [ -f "$root/$m/go.mod" ] || continue
  [ -n "$(find "$root/$m" -name '*.go' -not -path '*/testdata/*' -print -quit)" ] || continue
  echo "==> $m: $*"; ran=$((ran+1))
  if ! (cd "$root/$m" && GOWORK=off "$@"); then echo "!!! $m failed" >&2; status=1; fi
done
[ "$ran" -gt 0 ] || echo "group $group has no modules yet"
exit $status
