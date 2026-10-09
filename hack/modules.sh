#!/usr/bin/env bash
# Print every Go module directory in the repository, root (".") first.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
echo "."
find . -mindepth 2 -name go.mod \
  -not -path './.git/*' -not -path '*/testdata/*' -not -path '*/node_modules/*' -not -path '*/states/*' \
  | sed -e 's#/go.mod$##' -e 's#^\./##' | sort
