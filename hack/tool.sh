#!/usr/bin/env bash
# Run a tool declared in hack/tools/go.mod: hack/tool.sh NAME [args...]
set -euo pipefail
dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/tools" && pwd)"
name="$1"; shift
bin="$(cd "$dir" && GOWORK=off go tool -n "$name" | head -n1 | awk '{print $1}')"
exec "$bin" "$@"
