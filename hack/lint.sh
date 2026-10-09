#!/usr/bin/env bash
# golangci-lint one module: the one named by $1, or the current directory.
#
#   hack/lint.sh                 # this module
#   hack/lint.sh drivers/redis   # that module
#
# It used to ignore its argument and lint whatever directory it was run from,
# so `hack/lint.sh drivers/redis` from the repository root printed "0 issues"
# about a module it never looked at -- four real findings survived a run of
# exactly that shape. `make lint` was never wrong (hack/for-each-module.sh cds
# first), but the script is the obvious thing to reach for when one module's
# gate is red, and it answered a different question than the one asked.
set -euo pipefail
if [ "$#" -gt 1 ]; then
  echo "usage: $0 [MODULE_DIR]" >&2
  exit 2
fi
if [ "$#" -eq 1 ]; then
  [ -d "$1" ] || { echo "$0: $1 is not a directory" >&2; exit 2; }
  cd "$1"
fi
[ -f go.mod ] || { echo "$0: $(pwd) is not a module root (no go.mod)" >&2; exit 2; }
if [ -z "$(find . -name '*.go' -not -path '*/testdata/*' -print -quit)" ]; then
  echo "no Go sources; skipping lint"
  exit 0
fi
# `--allow-parallel-runners` instead of exiting: golangci-lint takes a global
# lock and, without the flag, a second runner exits 1 with "parallel
# golangci-lint is running" rather than waiting. On this machine two or three
# agents run `make ci` at once, so that turned every concurrent lint into a
# failed increment gate -- three modules of one run, with nothing wrong in any
# of them. The flag makes the runners queue on the lock.
exec golangci-lint run --allow-parallel-runners ./...
