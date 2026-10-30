#!/usr/bin/env bash
# Print every Go module directory in the repository, root (".") first.
#
# ./.claude is skipped: an agent's worktree under .claude/worktrees is a whole
# second checkout, so its 51 modules would be visited (tested, linted,
# measured) as though they were this tree's.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
echo "."
find . -mindepth 2 -name go.mod \
  -not -path './.git/*' -not -path './.claude/*' -not -path '*/testdata/*' -not -path '*/node_modules/*' -not -path '*/states/*' \
  | sed -e 's#/go.mod$##' -e 's#^\./##' | sort
