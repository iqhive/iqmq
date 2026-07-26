#!/usr/bin/env bash
set -euo pipefail

ceil="${IQMQ_MAX_CPUS:-16}"
width="${IQMQ_TEST_WIDTH:-4}"
if [ -n "${IQMQ_CPUCAP_AVAIL:-}" ]; then
  avail="$IQMQ_CPUCAP_AVAIL"
elif avail="$(nproc 2>/dev/null)" && [ -n "$avail" ]; then
  :
else
  avail=1
  echo "cpucap: WARNING: nproc is unavailable, so the available CPU count is assumed to be 1;" >&2
  echo "cpucap:          GOMAXPROCS will be 1, which defangs the sharded and Spin paths." >&2
  echo "cpucap:          Set IQMQ_CPUCAP_AVAIL to the real count." >&2
fi

for nv in ceil:"$ceil" width:"$width" avail:"$avail"; do
  name="${nv%%:*}"; val="${nv#*:}"
  case "$val" in
    ''|*[!0-9]*) echo "cpucap: $name must be a whole number (got '$val')" >&2; exit 2 ;;
    0|[1-9]*)    ;;
    *)           echo "cpucap: $name must not have a leading zero (got '$val'): bash would read it as octal" >&2; exit 2 ;;
  esac
  if [ "${#val}" -gt 7 ]; then
    echo "cpucap: $name is absurd (got '$val'); no host has that many processors" >&2; exit 2
  fi
done
if [ "$ceil" -lt 1 ]; then ceil=1; fi
if [ "$width" -lt 1 ]; then width=1; fi
if [ "$avail" -lt 1 ]; then avail=1; fi

g="$width"
if [ "$g" -gt "$ceil" ]; then g="$ceil"; fi
if [ "$g" -gt "$avail" ]; then g="$avail"; fi

p=$(( ceil / g ))
if [ "$p" -gt "$avail" ]; then p="$avail"; fi
if [ "$p" -lt 1 ]; then p=1; fi

case "${1:-pair}" in
  pair)       echo "$g $p" ;;
  gomaxprocs) echo "$g" ;;
  p)          echo "$p" ;;
  ceiling)    echo "$ceil" ;;
  env)
    keep=""
    for f in ${GOFLAGS:-}; do
      case "$f" in -p=*|-p) continue ;; esac
      keep="${keep:+$keep }$f"
    done
    printf 'GOMAXPROCS=%s; export GOMAXPROCS\n' "$g"
    printf 'GOFLAGS=%s; export GOFLAGS\n' "$(printf '%q' "-p=$p${keep:+ $keep}")"
    ;;
  report)
    echo "cpucap: available=$avail ceiling=$ceil width=$width -> GOMAXPROCS=$g -p=$p (product $(( g * p )) <= $ceil)"
    ;;
  fuzz)
    fg=2
    if [ "$fg" -gt "$ceil" ]; then fg="$ceil"; fi
    fw=1
    shards=$(( ceil / (fg * fw) ))
    if [ "$shards" -lt 1 ]; then shards=1; fi
    echo "$shards $fw $fg"
    ;;
  *)
    echo "usage: $0 [pair|gomaxprocs|p|ceiling|env|report|fuzz]" >&2
    exit 2 ;;
esac
