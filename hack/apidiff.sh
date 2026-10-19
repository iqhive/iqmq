#!/usr/bin/env bash
# Compare exported APIs against committed baselines in hack/api/<module>.export.
#   hack/apidiff.sh          check every module that has a baseline
#   hack/apidiff.sh -w       (re)write baselines for every module
#
# Every module is either checked against a committed baseline or named in
# NOBASELINE with the reason it has none, and a module that is neither FAILS:
# the list is closed, so a module that appears and is forgotten is a module
# nothing freezes, rather than a line in a summary nobody reads. The reason
# this is not merely a summary any more is that it was one for twenty-five
# increments -- "the public API is built by A26" was true when one module had a
# baseline and stopped being true once twenty-eight did (assumptions A15.15,
# C03.12).
#
# A module whose every package is a command has no exported surface at all, so
# it is neither checked nor waived: it is reported separately, because a
# baseline for one would be a check that cannot fail -- the comparison filters
# every command's report lines (see below).
#
# For a module that *does* have a baseline the check is strict: any difference
# at all fails, compatible or not, because a baseline that no longer matches is
# either an API change to be reviewed or a stale file to be rewritten.
#
# A difference inside a `package main` is NOT one of those. A command's
# identifiers are not importable by anybody, so they are not API -- and while
# they counted, every field added to a CI list or a report struct inside
# tools/ read as a public API change in the freeze record and had to be blessed
# with a baseline rewrite (`chore(hack/api): the root baseline records
# tools/docscheck`). Eight tools/* commands are in the root module's recorded
# surface. apidiff's module mode takes a module and not a package list, so the
# commands stay IN the stored baseline and their report lines are filtered out
# of the comparison; per-package baselines would drop them from the file too,
# at the cost of turning one root.export into ~50 files that every increment
# would have to regenerate.
#
# It also runs the documentation half of the freeze record, tools/exportdoc,
# because the two halves are one audit and because this script is what CI runs
# by another route -- hack/ci-parity.sh's waiver names it -- so a `make` target
# of its own would be run locally and not on a branch.
set -euo pipefail
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# The host CPU ceiling. This script invokes `go` itself, so it is capped when
# run by hand and not only when a make recipe exports the cap into it.
# hack/cpucap.sh carries the arithmetic; the eval is a
# no-op beyond re-deriving the same GOMAXPROCS and -p when make already set
# them, and it never RAISES either.
eval "$("$root/hack/cpucap.sh" env)"
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
mkdir -p "$root/hack/api"
write=0; [ "${1:-}" = "-w" ] && write=1
GO="${GO:-go}"

# Modules with no committed baseline, and why. A module missing from both this
# table and hack/api/ is a failure.
declare -A NOBASELINE=(
  [benchmarks]="measurement scaffolding: its surface is the shape of the current campaign and changes with it"
  [benchmarks/competitors]="measurement scaffolding: §10.5's competitor arms, added and removed per comparison"
  [benchmarks/native-baselines]="measurement scaffolding: one arm per native client library"
  [benchmarks/opbench]="measurement scaffolding: the timed-body harness B11 built"
  [conformance]="the harness is being extended by B22 (chaos, matrix, soak, differential) in this same round; C06 or B22 commits its baseline"
  [examples/custom-driver]="a worked example of the SPI: its exported surface exists to be read and copied, and freezing it would make an improvement to the teaching material an API change"
)
status=0
checked=0
skipped=""
mains=""

# mainFilter prints an extended regexp matching apidiff's report lines for the
# commands of the module in $1, or nothing when it has none.
#
# apidiff names a package TWO ways and both have to be matched, which is why
# this was watched failing before it worked: a package that already existed is
# reported by a path relative to the module ("- ./tools/adrcheck.ProbeC03:
# added") and one that is new by its full import path ("- package
# github.com/iqhive/iqmq/tools/exportdoc: added"). A filter that knew only the
# import path let the common case -- a field added to an existing command --
# straight through.
mainFilter() {
  local dir="$1" re="" p esc
  for p in $( (cd "$dir" && GOWORK=off $GO list -f '{{if eq .Name "main"}}{{.ImportPath}}{{if .Module}} .{{slice .ImportPath (len .Module.Path)}}{{end}}{{end}}' ./... 2>/dev/null) ); do
    esc="$(printf '%s' "$p" | sed 's/[][\.*^$(){}?+|/]/\\&/g')"
    re="${re:+$re|}(^- package $esc: |^- $esc\\.)"
  done
  printf '%s' "$re"
}
for m in $("$root/hack/modules.sh"); do
  [ -n "$(find "$root/$m" -name '*.go' -not -path '*/testdata/*' -not -name '*_test.go' -print -quit)" ] || continue
  # "." would name a hidden file, which is exactly the kind of baseline that
  # gets left out of a commit.
  name="$(echo "$m" | tr '/' '_')"; [ "$name" = "." ] && name=root
  base="$root/hack/api/$name.export"
  # A module made only of commands has no exported surface, so a baseline for
  # it would be a check that cannot fail: every report line would be filtered.
  # A module that does not load is not one of those. Its packages are unknown,
  # and reading the empty output as "commands only" skipped the module and
  # passed (a sibling version missing from go.sum did exactly that).
  # stderr is kept apart: `go: downloading` lines on a cold module cache would
  # otherwise read as packages and give a command-only module a baseline.
  listerr="$(mktemp)"
  if ! kinds="$(cd "$root/$m" && GOWORK=off $GO list -f '{{if ne .Name "main"}}x{{end}}' ./... 2>"$listerr")"; then
    sed 's/^/   /' "$listerr"; rm -f "$listerr"
    echo "!! $m: go list failed, so its exported API cannot be checked"
    status=1
    continue
  fi
  rm -f "$listerr"
  if [ -z "$(echo "$kinds" | tr -d '\n')" ]; then
    mains="$mains $m"
    continue
  fi
  # Write mode honours the SAME two exclusions as check mode, and running it
  # AFTER them is the whole of the fix. It used to run first, so
  # `make apidiff-baseline` wrote a baseline for every module with a
  # non-test .go file -- including the nineteen that check mode deliberately
  # refuses to freeze. The files arrived untracked, `git add -A` would have
  # taken them, and every NOBASELINE reason would have become a frozen
  # surface in silence: the next improvement to examples/custom-driver's
  # teaching material would then have read as an API change, which is the
  # reason that entry exists.
  if [ "$write" = 1 ]; then
    if [ -n "${NOBASELINE[$m]:-}" ]; then
      skipped="$skipped $m"
      continue
    fi
    (cd "$root/$m" && GOWORK=off "$root/hack/tool.sh" apidiff -m -w "$base" ./...) && echo "wrote $base"
    continue
  fi
  if [ ! -f "$base" ]; then
    if [ -n "${NOBASELINE[$m]:-}" ]; then
      skipped="$skipped $m"
      continue
    fi
    echo "!! $m: no baseline in hack/api/ and no reason in hack/apidiff.sh's NOBASELINE table"
    echo "   run 'make apidiff-baseline' and commit hack/api/$name.export, or add the module to NOBASELINE with the reason it never will have one"
    status=1
    continue
  fi
  echo "==> $m"
  checked=$((checked + 1))
  # apidiff narrates the internal packages it leaves out on every run; the
  # report itself is what is left once those lines are dropped.
  if ! out="$(cd "$root/$m" && GOWORK=off "$root/hack/tool.sh" apidiff -m "$base" ./... 2>&1 |
    grep -v '^Ignoring internal package ' || true)"; then
    echo "$out"
    echo "!! $m: apidiff failed to run against hack/api/$name.export"
    status=1
    continue
  fi
  # Drop the report lines that belong to a command, then judge on what is left:
  # every apidiff finding is a line starting with "- ", so a report whose only
  # remaining lines are section headers is a report of nothing.
  mre="$(mainFilter "$root/$m")"
  if [ -n "$mre" ]; then
    out="$(printf '%s\n' "$out" | grep -Ev "$mre" || true)"
  fi
  body="$(printf '%s\n' "$out" | grep '^- ' || true)"
  if [ -n "$body" ]; then
    echo "$out"
    echo "!! $m: the exported API no longer matches hack/api/$name.export"
    echo "   review the change, then 'make apidiff-baseline' and commit it if it is intended"
    status=1
  fi
done
if [ "$write" = 1 ]; then
  # Named out loud, because the whole defect above was a write mode that
  # reported only what it HAD done.
  if [ -n "$mains" ]; then
    echo "==> apidiff: wrote nothing for the command-only module(s):$mains"
  fi
  if [ -n "$skipped" ]; then
    echo "==> apidiff: wrote nothing for the module(s) NOBASELINE names, and why:"
    for m in $skipped; do printf '      %-30s %s\n' "$m" "${NOBASELINE[$m]}"; done
  fi
  exit 0
fi
echo "==> apidiff: $checked module(s) checked against a committed baseline"
if [ -n "$mains" ]; then
  echo "==> apidiff: NO EXPORTED SURFACE — every package is a command, so there is nothing to freeze:"
  for m in $mains; do echo "      $m"; done
fi
if [ -n "$skipped" ]; then
  echo "==> apidiff: NOT CHECKED, and why — this step freezes nothing for:"
  for m in $skipped; do printf '      %-30s %s\n' "$m" "${NOBASELINE[$m]}"; done
fi
for m in "${!NOBASELINE[@]}"; do
  case " $skipped $mains " in
    *" $m "*) ;;
    *) echo "!! NOBASELINE names $m, which either has a baseline now or is not a module: remove the entry"; status=1 ;;
  esac
done

# The documentation half of the same record.
echo "==> exportdoc: the documentation half of the freeze record"
if ! (cd "$root" && GOWORK=off $GO run ./tools/exportdoc); then
  echo "!! exportdoc: the documentation checks's exported-doc or example coverage is not met"
  status=1
fi
exit $status
