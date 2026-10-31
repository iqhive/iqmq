#!/usr/bin/env bash
# Fail if the root module depends on anything outside the
# standard library (runtime and test dependencies alike).
#
# Two readings, because the first alone missed a dependency. The package graph
# is listed under the default build AND under every tag set `make test-tags`
# builds: a third-party import in a file behind `//go:build iqmq_debug` is in
# no default build, so `go list` without the tag never saw it. And go.mod may
# require nothing at all, which is the module graph every consumer of the root
# module inherits, whichever tags the importing file carries (`go mod tidy`
# adds a requirement for an import under any tag).
set -euo pipefail
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# The host CPU ceiling. This script invokes `go` itself, so it is capped when
# run by hand and not only when a make recipe exports the cap into it.
# hack/cpucap.sh carries the arithmetic; the eval is a
# no-op beyond re-deriving the same GOMAXPROCS and -p when make already set
# them, and it never RAISES either.
eval "$("$root/hack/cpucap.sh" env)"
status=0
# The tag sets `make test-tags` builds the root module's packages with, beside
# the default build (""). A tag set added there belongs here too; the go.mod
# reading below holds whatever this list misses.
tagsets=("" "iqmq_unsafe,iqmq_debug" "iqmq_queue_mpmc" "iqmq_queue_native")
for m in .; do
  [ -f "$root/$m/go.mod" ] || continue
  if ! modjson="$(cd "$root/$m" && GOWORK=off go mod edit -json)"; then
    echo "stdlib-only: go mod edit -json failed in $m, so its requirements are unknown" >&2
    status=1
  elif printf '%s\n' "$modjson" | grep -q '"Require"'; then
    echo "non-stdlib requirements in $m/go.mod, which every consumer of the module inherits:" >&2
    (cd "$root/$m" && GOWORK=off go mod edit -print) | awk '/^require \(/ { b = 1; next } b && /^\)/ { b = 0 } b || /^require [^(]/' >&2
    status=1
  fi
  for tags in "${tagsets[@]}"; do
    for flags in "-deps" "-deps -test"; do
      # go list's own status is taken before anything filters its output. It
      # used to run with stderr discarded inside a pipeline ending in
      # `|| true`, so a module that did not load (a stray file declaring
      # another package, a missing go.sum entry) listed nothing, and nothing
      # read as "stdlib-only: ok" with a third-party import in the tree.
      # shellcheck disable=SC2086
      if ! pkgs="$(cd "$root/$m" && GOWORK=off go list $flags -tags "$tags" ./...)"; then
        echo "stdlib-only: go list $flags -tags '$tags' failed in $m, so its dependencies are unknown" >&2
        status=1
        continue
      fi
      bad="$(printf '%s\n' "$pkgs" | grep -v '^github.com/iqhive/iqmq' | awk -F/ '$1 ~ /\./' || true)"
      if [ -n "$bad" ]; then
        echo "non-stdlib dependencies in $m ($flags -tags '$tags'):" >&2
        echo "$bad" >&2
        status=1
      fi
    done
  done
done
[ $status -eq 0 ] && echo "stdlib-only: ok, go.mod requires nothing and no package depends on a third-party one under the default build or the tags ${tagsets[*]:1}"
exit $status
