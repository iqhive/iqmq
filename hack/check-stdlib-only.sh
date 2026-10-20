#!/usr/bin/env bash
# Fail if the root module depends on anything outside the
# standard library (runtime and test dependencies alike).
set -euo pipefail
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# The host CPU ceiling. This script invokes `go` itself, so it is capped when
# run by hand and not only when a make recipe exports the cap into it.
# hack/cpucap.sh carries the arithmetic; the eval is a
# no-op beyond re-deriving the same GOMAXPROCS and -p when make already set
# them, and it never RAISES either.
eval "$("$root/hack/cpucap.sh" env)"
status=0
for m in .; do
  [ -f "$root/$m/go.mod" ] || continue
  for flags in "-deps" "-deps -test"; do
    # shellcheck disable=SC2086
    bad="$(cd "$root/$m" && GOWORK=off go list $flags ./... 2>/dev/null \
      | grep -v '^github.com/iqhive/iqmq' \
      | awk -F/ '$1 ~ /\./' || true)"
    if [ -n "$bad" ]; then
      echo "non-stdlib dependencies in $m ($flags):" >&2
      echo "$bad" >&2
      status=1
    fi
  done
done
[ $status -eq 0 ] && echo "stdlib-only: ok"
exit $status
