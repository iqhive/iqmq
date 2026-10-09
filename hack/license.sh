#!/usr/bin/env bash
# License allow-list check for every module with third-party dependencies.
set -euo pipefail
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
allowed="Apache-2.0,MIT,BSD-2-Clause,BSD-3-Clause,ISC,MPL-2.0,Unlicense,0BSD"
status=0
for m in $("$root/hack/modules.sh"); do
  [ -n "$(find "$root/$m" -name '*.go' -not -path '*/testdata/*' -print -quit)" ] || continue
  if ! grep -q '^\s*[a-z].*\s\+v[0-9]' "$root/$m/go.mod" 2>/dev/null && ! grep -q '^require' "$root/$m/go.mod"; then
    echo "==> $m: no dependencies"; continue
  fi
  echo "==> $m"
  (cd "$root/$m" && GOWORK=off "$root/hack/tool.sh" go-licenses check --allowed_licenses="$allowed" --ignore github.com/iqhive/iqmq ./... ) || status=1
done
exit $status
