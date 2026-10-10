#!/bin/bash -eu
# Print "import-path FuzzName" pairs for every fuzz target of the root module.
cd "$(dirname "$0")/.."
for pkg in $(GOWORK=off go list ./...); do
  for t in $(GOWORK=off go test -list '^Fuzz' "$pkg" 2>/dev/null | grep '^Fuzz' || true); do
    echo "$pkg $t"
  done
done
