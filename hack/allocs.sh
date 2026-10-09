#!/usr/bin/env bash
# Run the current module's allocation-count tests.
#
# It is a script rather than a line in the Makefile because the output needs
# filtering -- `go test -run '^TestAllocs'` prints "testing: warning: no tests
# to run" for every package that has none, which is most of them -- and a
# pipeline in a for-each-module command would lose the exit status to the sed
# at the end of it. Here the status is the one `go test` gave.
set -euo pipefail
# Capped when run by hand as well as through hack/for-each-module.sh, which
# already exports the cap into it. BASH_SOURCE, not $root:
# this script is invoked with the module directory as its working directory.
eval "$("$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/cpucap.sh" env)"
out="$(GOWORK=off "${GO:-go}" test -count=1 -run '^TestAllocs' ./... 2>&1)" && status=0 || status=$?
echo "$out" | sed -e '/no tests to run/d' -e '/^ok .*\[no tests to run\]$/d'
exit "$status"
