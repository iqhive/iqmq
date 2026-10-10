#!/usr/bin/env bash
# One parser for release/manifest.yaml, because three scripts used to read it
# with three different `sed` expressions and two of them read the wrong lines.
#
#   release/manifest.sh train                 -> the train version
#   release/manifest.sh paths [train|internal|example]
#   release/manifest.sh release <path>        -> that module's class
#
# Parse the manifest's module list and release classes. This accepts the
# repository's simple YAML layout rather than arbitrary YAML.
set -euo pipefail
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
file="${IQMQ_RELEASE_MANIFEST:-$root/release/manifest.yaml}"
[ -f "$file" ] || { echo "manifest.sh: $file is missing" >&2; exit 2; }

case "${1:-}" in
  train)
    v="$(sed -n 's/^train:[[:space:]]*//p' "$file" | head -1)"
    [ -n "$v" ] || { echo "manifest.sh: no train: line in $file" >&2; exit 2; }
    printf '%s\n' "$v" ;;
  paths)
    want="${2:-}"
    # Walk the modules: block only. A `- path:` line outside it -- in a comment
    # block, or in a `compatibility:` entry -- is not a module, and the sed
    # expression the old scripts used could not tell the difference.
    awk -v want="$want" '
      /^modules:[[:space:]]*$/ { inmods = 1; next }
      /^[a-z]/ && !/^modules:/  { inmods = 0 }
      !inmods { next }
      /^[[:space:]]+-[[:space:]]+path:[[:space:]]*/ {
        sub(/^[[:space:]]+-[[:space:]]+path:[[:space:]]*/, ""); path = $0; cls = ""; next
      }
      /^[[:space:]]+release:[[:space:]]*/ {
        sub(/^[[:space:]]+release:[[:space:]]*/, ""); cls = $0
        if (path != "" && (want == "" || want == cls)) print path
        path = ""
      }
    ' "$file" ;;
  release)
    p="${2:?path}"
    for c in train internal example; do
      if "$0" paths "$c" | grep -qxF "$p"; then printf '%s\n' "$c"; exit 0; fi
    done
    echo "manifest.sh: $p is in no modules: entry of $file" >&2; exit 1 ;;
  *)
    echo "usage: manifest.sh {train|paths [class]|release <path>}" >&2; exit 2 ;;
esac
