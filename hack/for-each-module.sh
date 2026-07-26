#!/usr/bin/env bash
# Run a command in every module directory with GOWORK=off.
#   hack/for-each-module.sh [--go] CMD [ARGS...]
# With --go, modules that contain no Go sources (tool-only modules) are skipped.
set -euo pipefail
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# Every test target of the Makefile runs through here, so this is where the
# host CPU ceiling is applied for the scripts as well as for make: the Makefile
# exports the same GOMAXPROCS and -p, and re-deriving them costs one subshell
# and makes `hack/for-each-module.sh --go go test ./...` typed by hand obey the
# ceiling too. The arithmetic and the reason are in hack/cpucap.sh.
# Modules are visited one at a time, so the ceiling is per
# module and per run at once.
eval "$("$root/hack/cpucap.sh" env)"
need_go=0
if [ "${1:-}" = "--go" ]; then need_go=1; shift; fi
status=0
for m in $("$root/hack/modules.sh"); do
  if [ "$need_go" = 1 ] && [ -z "$(find "$root/$m" -name '*.go' -not -path '*/testdata/*' -not -path '*/node_modules/*' -print -quit)" ]; then
    echo "==> $m: skipped (no Go sources)"
    continue
  fi
  echo "==> $m: $*"
  if ! (cd "$root/$m" && GOWORK=off "$@"); then
    echo "!!! $m: failed" >&2
    status=1
  fi
done
exit $status
