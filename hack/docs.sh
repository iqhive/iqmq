#!/usr/bin/env bash
# Build or serve the docs site with mkdocs-material, using the local venv
# (~/.cache/iqmq/venv), a PATH mkdocs, or creating the venv on demand.
#   hack/docs.sh build|serve
set -euo pipefail
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
venv="${IQMQ_DOCS_VENV:-$HOME/.cache/iqmq/venv}"
export PATH="$HOME/.local/bin:$PATH"
if [ -x "$venv/bin/mkdocs" ]; then
  mkdocs="$venv/bin/mkdocs"
elif command -v mkdocs >/dev/null; then
  mkdocs="$(command -v mkdocs)"
elif python3 -m venv "$venv" 2>/dev/null && "$venv/bin/pip" install --quiet -r "$root/docs/requirements.txt"; then
  mkdocs="$venv/bin/mkdocs"
else
  # No venv support on this host: install into the user site.
  pip install --user --break-system-packages --quiet -r "$root/docs/requirements.txt"
  mkdocs="$HOME/.local/bin/mkdocs"
fi
cd "$root"
case "${1:-build}" in
  build)  "$mkdocs" build --strict --site-dir "${2:-site}" ;;
  serve)  "$mkdocs" serve ;;
  *) echo "usage: $0 build|serve" >&2; exit 2 ;;
esac
