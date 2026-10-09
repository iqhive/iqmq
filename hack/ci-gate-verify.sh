#!/usr/bin/env bash
# The `gate` job's verdict, as a script that can be watched failing.
#
#   NEEDS='{"fmt":{"result":"success"},...}' hack/ci-gate-verify.sh
#   hack/ci-gate-verify.sh --selftest
#
# `.github/workflows/ci.yml`'s `gate` job is what a branch-protection rule
# keys on, and it was one line:
#
#     echo '${{ toJSON(needs) }}' | jq -e 'to_entries | all(.value.result == "success")'
#
# That line is correct about the jobs it is given and says nothing about the
# jobs it is not: `gate` listed nine of the eleven jobs, so `generated` --
# which runs generate-check, suites-check, docs-check, security-check,
# security-check-selftest -- could go
# red with `gate` green, and a pull request would merge. `hack/ci-parity.sh`
# now holds the `needs` list; this holds the verdict, so both halves of "the
# gate gates" have something that can fail.
#
# SKIPPED is the part worth being careful about. A blanket "skipped counts as
# success" is how a job stops running without anybody noticing -- an `if:` that
# silently stops matching looks exactly like a job that was never meant to run
# here. So a skip is accepted only for jobs named in MAY_SKIP, one by one, with
# the reason in the caller. Today that is `test-arm64`, whose
# `if: !github.event.repository.private` is false on this private repository
# and true on a public fork.
set -euo pipefail

may_skip="${MAY_SKIP:-test-arm64}"

verify() { # needs-json may-skip-list -- prints a table, returns 0 or 1
  local needs="$1" allow=" ${2//,/ } " bad=0 n rows=0 job result
  if ! n="$(printf '%s' "$needs" | jq 'length' 2>/dev/null)"; then
    echo "gate: needs is not JSON: $needs" >&2
    return 1
  fi
  # A zero exit from jq does not make `n` a count. jq exits 0 while emitting
  # NOTHING (whitespace-only input) and while emitting one value PER DOCUMENT
  # (a stream of JSON documents), so `n` becomes "" or "1\n1". Both of the
  # arithmetic tests below then fail with "integer expected" and status 2 --
  # and because each is the condition of an `if`, `set -euo pipefail` is exempt
  # and status 2 reads as false, so BOTH vacuity checks are skipped and the
  # gate prints "ok, N job(s)" having examined nothing. That is the vacuity the
  # comment further down refuses, reached one level further in.
  case "$n" in
    ''|*[!0-9]*)
      echo "gate: needs length is not a count: [$n]; the needs context is not the shape this expects" >&2
      return 1 ;;
  esac
  if [ "$n" -eq 0 ]; then
    # An empty map satisfies `all(...)` vacuously, which is the oldest way for
    # an aggregate gate to pass having checked nothing.
    echo "gate: needs is empty; the gate would pass having checked nothing" >&2
    return 1
  fi
  while IFS='=' read -r job result; do
    rows=$((rows + 1))
    case "$result" in
      success)
        printf '  %-16s %s\n' "$job" "$result" ;;
      skipped)
        case "$allow" in
          *" $job "*) printf '  %-16s %s (declared in MAY_SKIP)\n' "$job" "$result" ;;
          *) printf '  %-16s %s -- not a job that may be skipped\n' "$job" "$result" >&2; bad=1 ;;
        esac ;;
      *)
        printf '  %-16s %s\n' "$job" "$result" >&2; bad=1 ;;
    esac
  done < <(printf '%s' "$needs" | jq -r 'to_entries[] | "\(.key)=\(.value.result)"')
  if [ "$bad" -ne 0 ]; then
    echo "gate: FAILED over $n job(s)" >&2
    return 1
  fi
  # The count came from one jq invocation and the rows from a second, and a
  # process substitution's exit status is invisible to `set -euo pipefail`.
  # jq streams `to_entries[]` in order, so an entry it cannot evaluate --
  # anything that is not an object with a `result` -- ends the stream after the
  # rows before it, the loop finishes early with `bad` still 0, and the gate
  # prints "ok, N job(s)" having looked at fewer than N. Every job after the
  # bad entry, a `failure` among them, goes unexamined. So the rows are counted
  # and compared, which is the same vacuity the empty-map check above refuses,
  # one level in.
  if [ "$rows" -ne "$n" ]; then
    echo "gate: read $rows of $n job(s); the needs context is not the shape this expects" >&2
    return 1
  fi
  echo "gate: ok, $n job(s)"
  return 0
}

if [ "${1:-}" = "--selftest" ]; then
  # One row per defect, each watched failing. The verdict is what a branch
  # protection rule reads, so every way it could wrongly say success is here.
  ok=0
  fails=0
  t() { # want-status description needs-json [may-skip]
    local want="$1" desc="$2" njson="$3" allow="${4:-test-arm64}" got
    if verify "$njson" "$allow" >/dev/null 2>&1; then got=0; else got=1; fi
    if [ "$got" = "$want" ]; then
      ok=$((ok + 1)); printf '  ok    %s\n' "$desc"
    else
      fails=$((fails + 1)); printf '  FAIL  %s: exit %s, want %s\n' "$desc" "$got" "$want" >&2
    fi
  }
  echo "ci-gate-verify: self-test"
  t 0 "every job success" '{"fmt":{"result":"success"},"checks":{"result":"success"}}'
  t 1 "one job failed" '{"fmt":{"result":"success"},"checks":{"result":"failure"}}'
  t 1 "one job cancelled" '{"fmt":{"result":"success"},"checks":{"result":"cancelled"}}'
  t 1 "the generated job failed" '{"fmt":{"result":"success"},"generated":{"result":"failure"}}'
  t 0 "a declared skip" '{"fmt":{"result":"success"},"test-arm64":{"result":"skipped"}}'
  t 1 "an undeclared skip" '{"fmt":{"result":"success"},"checks":{"result":"skipped"}}'
  t 1 "an empty needs map" '{}'
  t 1 "needs that is not JSON" 'not json at all'
  # jq exits 0 on both of these and is the reason the exit status alone is not
  # enough: whitespace produces no value at all, and a stream of documents
  # produces one length per document. `n` is then not a number, every
  # arithmetic test below is skipped with "integer expected", and the gate used
  # to print ok over a context it had not read.
  t 1 "needs that is only whitespace" ' '
  t 1 "needs that is a stream of documents" '{"a":{"result":"success"}} {"b":{"result":"success"}}'
  # jq dies part-way through this one, after emitting `a`: the loop used to
  # end early with nothing wrong recorded and print "ok, 3 job(s)" without
  # ever looking at `c`, which failed.
  t 1 "a needs entry that is not an object" '{"a":{"result":"success"},"b":"x","c":{"result":"failure"}}'
  t 1 "a needs entry with no result field" '{"a":{"result":"success"},"b":{"outputs":{}}}'
  # And the exemption is narrow: MAY_SKIP excuses the job it names and no
  # other, so a typo in it does not excuse everything.
  t 1 "a skip of a job MAY_SKIP does not name" '{"checks":{"result":"skipped"}}' "test-arm64"
  t 0 "a skip of a job MAY_SKIP does name" '{"checks":{"result":"skipped"}}' "checks"
  printf 'ci-gate-verify: %d passed, %d failed\n' "$ok" "$fails"
  [ "$fails" -eq 0 ]
  exit
fi

verify "${NEEDS:?NEEDS is required: the gate job passes it as toJSON(needs)}" "$may_skip"
