#!/usr/bin/env bash
# Report-only: upgrade every driver module's dependencies in a scratch copy and
# build/test them, to learn about upcoming client incompatibilities early.
set -euo pipefail
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# The host CPU ceiling. This script invokes `go` itself, so it is capped when
# run by hand and not only when a make recipe exports the cap into it.
# hack/cpucap.sh carries the arithmetic; the eval is a
# no-op beyond re-deriving the same GOMAXPROCS and -p when make already set
# them, and it never RAISES either.
eval "$("$root/hack/cpucap.sh" env)"
tmp="$(mktemp -d)"; trap 'rm -rf "$tmp"' EXIT
cp -r "$root/." "$tmp/"
status=0
for m in $("$tmp/hack/modules.sh"); do
  case "$m" in drivers/*|integrations/*|codec/*) ;; *) continue ;; esac
  [ -f "$tmp/$m/go.mod" ] || continue
  echo "==> $m"
  (cd "$tmp/$m" && GOWORK=off go get -u ./... && GOWORK=off go mod tidy && GOWORK=off go build ./... && GOWORK=off go test -short -count=1 ./...) || { echo "!!! $m fails with upgraded dependencies" >&2; status=1; }
done
exit $status
