#!/usr/bin/env bash
# Run `iqmq doctor` against the brokers conformance/suites.json names.
#
#   hack/doctor-harness.sh list                 what can be reached, and what cannot
#   hack/doctor-harness.sh run [suite...]       start each broker, run doctor, remove it
#   hack/doctor-harness.sh run                  every suite that has a recipe
#
# The suite list is the single source of truth for which harnesses exist, so
# this script has no list of its own: a suite is reachable when its entry
# carries a `doctor` recipe, and a suite without one is reported as unreachable
# rather than guessed at. A recipe is added to suites.json
# after somebody has watched doctor succeed against it, never before.
#
# Host etiquette, because this machine runs other people's containers:
#
#   - ONE broker at a time, started from the digest conformance/harness/
#     images.lock pins -- never from a tag, which is not the same image.
#   - The host port is chosen from the ephemeral range, so nothing collides
#     with a neighbour holding 4222 or 6379.
#   - The container is removed by the id this script started, with
#     `docker rm -f <id>`. There is no filter, no prune and no name match
#     anywhere in this file, and a `docker ps -q` diff before and after says
#     whether anything else moved.
#   - Nothing is killed by a command pattern.
set -uo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# The host CPU ceiling. This script invokes `go` itself, so it is capped when
# run by hand and not only when a make recipe exports the cap into it.
# hack/cpucap.sh carries the arithmetic; the eval is a
# no-op beyond re-deriving the same GOMAXPROCS and -p when make already set
# them, and it never RAISES either.
eval "$("$root/hack/cpucap.sh" env)"
suites="$root/conformance/suites.json"
lock="$root/conformance/harness/images.lock"

# READY_TIMEOUT bounds how long a broker gets to answer. doctor's own ping IS
# the readiness check, so there is no second wait strategy here to disagree
# with the harness's.
READY_TIMEOUT="${IQMQ_DOCTOR_READY_TIMEOUT:-90}"

die() { echo "doctor-harness: $*" >&2; exit 2; }

[ -f "$suites" ] || die "$suites is missing"
[ -f "$lock" ] || die "$lock is missing"
command -v python3 >/dev/null || die "python3 is required to read the suite list"

# free_port picks a host port nothing is listening on.
#
# The port is chosen BEFORE the container starts, rather than published as `:0`
# and read back, because several brokers advertise their own address to the
# client and have to be told it at start-up -- Kafka's
# KAFKA_ADVERTISED_LISTENERS is the case that forced this. A recipe's dsn, env
# and command may all carry {addr}, {host} and {port}, so one placeholder set
# covers the DSN and the broker's own idea of where it is.
free_port() {
  python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1]); s.close()'
}

# suite_field prints one field of one suite, or the empty string. A list is
# printed with 0x1f separators, which no DSN or environment value carries.
suite_field() {
  python3 - "$suites" "$1" "$2" <<'PY'
import json, sys
path, name, field = sys.argv[1], sys.argv[2], sys.argv[3]
for s in json.load(open(path))["suites"]:
    if s["name"] != name:
        continue
    cur = s
    for part in field.split("."):
        if not isinstance(cur, dict) or part not in cur:
            sys.exit(0)
        cur = cur[part]
    if isinstance(cur, list):
        print("\x1f".join(str(x) for x in cur))
    else:
        print(cur)
    sys.exit(0)
PY
}

# reachable prints the name of every suite with a doctor recipe.
reachable() {
  python3 - "$suites" <<'PY'
import json, sys
for s in json.load(open(sys.argv[1]))["suites"]:
    if s.get("doctor"):
        print(s["name"])
PY
}

cmd_list() {
  printf '%-24s %-14s %-18s %-10s %s\n' SUITE DRIVER IMAGE EVIDENCE 'DOCTOR'
  python3 - "$suites" <<'PY'
import json, sys
for s in json.load(open(sys.argv[1]))["suites"]:
    d = s.get("doctor")
    if d:
        note = "yes  " + d["dsn"] + "  (verified " + d.get("verified", "?") + ")"
    elif s.get("evidence") == "inprocess":
        note = "n/a  in-process: no container, and memory:// needs no recipe"
    else:
        note = "no   no verified DSN recipe in suites.json yet"
    print('%-24s %-14s %-18s %-10s %s' % (
        s["name"], s.get("driver", ""), s.get("image", "-"), s.get("evidence", ""), note))
PY
}

# digest_for prints the image@digest images.lock pins for an image name.
digest_for() {
  awk -v want="$1" '$1 == want { print $2; found = 1 } END { exit !found }' "$lock"
}

# build_cli builds the CLI into a temporary directory, or honours IQMQ_BIN.
build_cli() {
  if [ -n "${IQMQ_BIN:-}" ]; then
    [ -x "$IQMQ_BIN" ] || die "IQMQ_BIN=$IQMQ_BIN is not executable"
    echo "$IQMQ_BIN"
    return
  fi
  local out
  out="$(mktemp -d)/iqmq"
  ( cd "$root/cmd/iqmq" && GOWORK=off go build -o "$out" . ) >&2 || die "building the CLI failed"
  echo "$out"
}

cmd_run_one() {
  local suite="$1" cli="$2"
  local image dsn ports verified
  image="$(suite_field "$suite" image)"
  dsn="$(suite_field "$suite" doctor.dsn)"
  ports="$(suite_field "$suite" doctor.ports)"
  verified="$(suite_field "$suite" doctor.verified)"
  if [ -z "$dsn" ]; then
    echo "SKIP  $suite: no doctor recipe in conformance/suites.json" >&2
    return 0
  fi
  [ -n "$image" ] || die "$suite has a doctor recipe and no image"
  local ref
  ref="$(digest_for "$image")" || die "$image is not pinned in $lock"

  # One host port per container port, each from the ephemeral range. The
  # first is what {addr} and {port} name; the second is {addr2}/{port2}.
  #
  # The list is read into an array FIRST and only then iterated. A
  # `while read` loop over a process substitution looks equivalent and is
  # not: free_port runs python3, which inherits the loop's stdin and eats
  # the rest of the list, so the loop ran exactly once. Pulsar's recipe --
  # the first with two ports and a command -- came out with one port and
  # the command "bin/pulsar", which printed the image's usage text.
  local -a cports=() hostports=() publish=()
  mapfile -t cports < <(printf '%s' "$ports" | tr '\037' '\n')
  local n=${#cports[@]}
  [ "$n" -gt 0 ] || die "$suite has a doctor recipe and no ports"
  local j
  for j in $(seq 0 $((n - 1))); do
    hostports+=("$(free_port)")
  done
  local i
  for i in $(seq 0 $((n - 1))); do
    publish+=(-p "127.0.0.1:${hostports[$i]}:${cports[$i]}")
  done
  local hostport addr
  hostport="${hostports[0]}"
  addr="127.0.0.1:$hostport"
  local addr2="" hostport2=""
  if [ "$n" -gt 1 ]; then
    hostport2="${hostports[1]}"
    addr2="127.0.0.1:$hostport2"
  fi

  local -a env_args=() cmd_args=() envs=() cmds=()
  local raw kv a
  raw="$(suite_field "$suite" doctor.env)"
  if [ -n "$raw" ]; then
    mapfile -t envs < <(printf '%s' "$raw" | tr '\037' '\n')
    for kv in "${envs[@]}"; do
      [ -n "$kv" ] || continue
      env_args+=(-e "$(expand "$kv" "$addr" "$hostport" "$addr2" "$hostport2")")
    done
  fi
  raw="$(suite_field "$suite" doctor.command)"
  if [ -n "$raw" ]; then
    mapfile -t cmds < <(printf '%s' "$raw" | tr '\037' '\n')
    for a in "${cmds[@]}"; do
      [ -n "$a" ] || continue
      cmd_args+=("$(expand "$a" "$addr" "$hostport" "$addr2" "$hostport2")")
    done
  fi

  echo "=== $suite  image $ref  (recipe verified $verified)"
  echo "    publishing ${publish[*]}"
  local before after id
  before="$(docker ps -q | sort)"
  id="$(docker run -d "${publish[@]}" "${env_args[@]}" "$ref" "${cmd_args[@]}")" \
    || die "$suite: docker run failed"
  echo "    container ${id:0:12}"

  local status=1 target deadline
  target="$(expand "$dsn" "$addr" "$hostport" "$addr2" "$hostport2")"
  deadline=$((SECONDS + READY_TIMEOUT))
  while :; do
    if "$cli" doctor --timeout 8s "$target"; then status=0; break; fi
    if [ "$SECONDS" -ge "$deadline" ]; then
      echo "    FAILED: doctor did not succeed within ${READY_TIMEOUT}s" >&2
      echo "    last 20 lines of the container's log:" >&2
      docker logs --tail 20 "$id" 2>&1 | sed 's/^/      /' >&2
      break
    fi
    sleep 3
  done

  # Removed by the id this script started. No filter, no name, no prune.
  docker rm -f "$id" >/dev/null 2>&1 || echo "    WARNING: could not remove ${id:0:12}" >&2
  after="$(docker ps -q | sort)"
  if [ "$before" != "$after" ]; then
    echo "    WARNING: the running-container set changed beyond this script's own container." >&2
    echo "    before: $(echo "$before" | tr '\n' ' ')" >&2
    echo "    after:  $(echo "$after" | tr '\n' ' ')" >&2
  fi
  if [ "$status" -eq 0 ]; then echo "    RESULT: doctor ok"; else echo "    RESULT: doctor FAILED"; fi
  return "$status"
}

# expand substitutes the placeholder set into one recipe string.
expand() {
  local v="$1" addr="$2" port="$3" addr2="${4:-}" port2="${5:-}"
  v="${v//\{addr2\}/$addr2}"
  v="${v//\{port2\}/$port2}"
  v="${v//\{addr\}/$addr}"
  v="${v//\{host\}/127.0.0.1}"
  v="${v//\{port\}/$port}"
  printf '%s' "$v"
}

cmd_run() {
  command -v docker >/dev/null || die "docker is required"
  local cli
  cli="$(build_cli)"
  local -a want=("$@")
  if [ "${#want[@]}" -eq 0 ]; then
    mapfile -t want < <(reachable)
  fi
  [ "${#want[@]}" -gt 0 ] || die "no suite has a doctor recipe yet; see 'list'"
  local failed=0 s
  for s in "${want[@]}"; do
    cmd_run_one "$s" "$cli" || failed=$((failed + 1))
  done
  echo
  echo "ran ${#want[@]} suite(s), $failed failed"
  [ "$failed" -eq 0 ]
}

case "${1:-}" in
  list) cmd_list ;;
  run) shift; cmd_run "$@" ;;
  *) echo "usage: hack/doctor-harness.sh <list|run> [suite...]" >&2; exit 2 ;;
esac
