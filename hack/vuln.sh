#!/usr/bin/env bash
# govulncheck every module, then judge what it found against the advisories
# this repository has decided it cannot fix yet (`make vuln`).
#
#   hack/vuln.sh
#
# It used to be `govulncheck ./...` in every module, text output, which exits
# 3 on any finding. Four modules carry Avro advisories with NO fixed version,
# so the command could not pass on any day, and a job that is red every day is
# silent on the day a new advisory arrives. The scan is
# unchanged -- symbol level, every module, the network and the vulnerability
# database -- and it now writes JSON, which tools/securitycheck -vuln reads:
# every finding must be named by a waiver with an owner, every waiver must
# match a finding, and a waiver for an advisory with no fixed version fails
# the day the database names one.
#
# It needs the network, which is why it is NOT a prerequisite of `make ci`;
# .github/workflows/vuln.yml runs it per pull request, on main and daily.
set -euo pipefail
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# The host CPU ceiling. This script invokes `go` itself, so it is capped when
# run by hand and not only when a make recipe exports the cap into it.
# hack/cpucap.sh carries the arithmetic; the eval is a
# no-op beyond re-deriving the same GOMAXPROCS and -p when make already set
# them, and it never RAISES either.
eval "$("$root/hack/cpucap.sh" env)"
out="$(mktemp -d "${TMPDIR:-/tmp}/iqmq-vuln.XXXXXX")"
trap 'rm -rf "$out"' EXIT
status=0
for m in $("$root/hack/modules.sh"); do
  # The same rule as for-each-module.sh --go: a tool-only module has nothing
  # to scan, and govulncheck refuses a module with no packages.
  if [ -z "$(find "$root/$m" -name '*.go' -not -path '*/testdata/*' -not -path '*/node_modules/*' -print -quit)" ]; then
    echo "==> $m: skipped (no Go sources)"
    continue
  fi
  echo "==> $m"
  mkdir -p "$out/$m"
  # -format json exits 0 whatever it finds; a non-zero status here is a scan
  # that did not complete, and the module is then MISSING from the judgement
  # rather than clean -- so it fails this script on its own.
  if ! (cd "$root/$m" && GOWORK=off "$root/hack/tool.sh" govulncheck -format json ./... > "$out/$m/govulncheck.json"); then
    echo "!!! $m: govulncheck did not complete" >&2
    rm -f "$out/$m/govulncheck.json"
    status=1
  fi
done
if ! (cd "$root" && GOWORK=off go run ./tools/securitycheck -vuln "$out"); then
  status=1
fi
exit "$status"
