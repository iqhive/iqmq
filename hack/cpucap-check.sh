#!/usr/bin/env bash
# Fail when a path that runs Go code escapes the host CPU ceiling.
#
#   hack/cpucap-check.sh [-root DIR]   # check a tree (default: this repository)
#   hack/cpucap-check.sh --self-test   # doctor a copy of the tree and watch each half fail
#
# `make ci` runs both (`make cpu-cap-check`). An instrument nobody has watched
# fail is assumed not to work, so the self-test is not optional: it copies the
# Makefile and hack/ into a temp directory, breaks one thing at a time, and
# requires this script to notice each break and to pass on the untouched copy.
#
# What it checks, and why each one is a way out of the ceiling:
#
#  A  the arithmetic   hack/cpucap.sh over a table of (ceiling, width, available)
#                      -- -p x GOMAXPROCS must never exceed the ceiling, and
#                      neither factor may exceed what the host has, or the cap
#                      would RAISE parallelism on a 2-core GitHub runner.
#  B  the exports      the Makefile must export GOMAXPROCS and GOFLAGS from
#                      hack/cpucap.sh, and hack/for-each-module.sh must apply
#                      the same thing. That inheritance IS the enforcement:
#                      every recipe, and every hack/*.sh a recipe calls, gets
#                      the cap without knowing about it. Delete either and
#                      every path escapes at once.
#  C  no re-raising    no GOMAXPROCS=, GOFLAGS= or literal -p=N on a `go` line.
#  D  bench width      a `go test -bench` run with neither -cpu nor a pin uses
#                      GOMAXPROCS=nproc -- every core the container is allowed,
#                      which was 64. Two such paths existed (make bench-quick,
#                      hack/native-baselines.sh).
#  E  one ceiling      every IQMQ_BENCH_MAX_CPUS / IQMQ_GATE_CPUS default falls
#                      through to IQMQ_MAX_CPUS instead of repeating a literal
#                      16, so the benchmark ceiling and the test ceiling cannot
#                      drift apart.
#  F  no fixed pin     the cap is CAPACITY, not affinity: nothing may pin to a
#                      hardcoded core list, and the one harness that pins at
#                      all picks the least busy cores at run time.
#  G  fuzz workers     `go test -fuzz -parallel N` starts N extra PROCESSES, so
#                      for fuzzing -parallel multiplies with GOMAXPROCS exactly
#                      as -p does.
#
# A hit that is not a defect goes in WAIVERS with the reason, which is printed,
# so "is this path capped?" is always yes or a written explanation.
set -euo pipefail

self="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
root="$(cd "$self/.." && pwd)"
mode=check
while [ "$#" -gt 0 ]; do
  case "$1" in
    -root) root="$2"; shift 2 ;;
    --self-test) mode=selftest; shift ;;
    *) echo "usage: $0 [-root DIR] [--self-test]" >&2; exit 2 ;;
  esac
done

# file <TAB> kind <TAB> reason. A waiver is exact: an unlisted hit and a hit
# whose file is listed under a different kind both fail.
WAIVERS=$(cat <<'EOF'
hack/cpucap.sh	gomaxprocs-override	the implementation of the ceiling: it is the file that decides GOMAXPROCS
hack/cpucap.sh	goflags-override	the implementation of the ceiling: it is the file that decides GOFLAGS
hack/cpucap-check.sh	gomaxprocs-override	this checker, which has to contain the patterns it looks for
hack/cpucap-check.sh	goflags-override	this checker, which has to contain the patterns it looks for
hack/cpucap-check.sh	fuzz-workers	this checker, which has to contain the patterns it looks for
hack/cpucap-check.sh	literal-p	this checker, which has to contain the patterns it looks for
hack/cpucap-check.sh	unbounded-bench	this checker, which has to contain the patterns it looks for
hack/cpucap-check.sh	bare-bench-ceiling	this checker, which has to contain the patterns it looks for
hack/cpucap-check.sh	fixed-cpu-list	this checker, which has to contain the patterns it looks for
hack/cpucap-check.sh	shell-fanout	this checker, which has to contain the patterns it looks for
hack/benchguard_test.sh	unbounded-bench	fixtures, not runs: `exec -a "go test -bench=..." sleep` and /proc table rows give the guard something to recognise; nothing here measures
hack/fuzz.sh	uncapped-script	owned by fix/nightly-plumbing this wave. Its fan-out is bounded from the Makefile instead: JOBS, SHARDS and FUZZ_WORKERS all come from `hack/cpucap.sh fuzz`, so jobs x workers x GOMAXPROCS is 2 x 2 x 4 = 16. Run by hand with none of those set it is uncapped, which is the residue of the branch boundary
hack/fuzz.sh	fuzz-workers	owned by fix/nightly-plumbing this wave; its `race` mode carries a literal `-parallel 4`, which at GOMAXPROCS 4 is exactly the ceiling because that mode is a sequential nested loop with no shell fan-out
hack/ci-parity.sh	uncapped-script	owned by fix/nightly-plumbing this wave; it runs `go` only to read the module list and starts no test
EOF
)

waived() { # file kind
  printf '%s\n' "$WAIVERS" | awk -F'\t' -v f="$1" -v k="$2" '
    $1 == f && ($2 == k || $2 == "any") { print $3; found = 1 }
    END { exit(found ? 0 : 1) }'
}

problems=0
waivers_used=""
report() { # file:line kind message
  local where="$1" kind="$2" msg="$3" file="${1%%:*}" reason
  # `arithmetic` and `no-export` are NOT waivable. The self-test caught this:
  # hack/cpucap.sh was waived for "any" kind because it must contain the
  # patterns the scan looks for, and that waiver also swallowed the arithmetic
  # failure -- breaking the product bound inside cpucap.sh left the check
  # green. A waiver that covers a whole file covers the defect the file is
  # there to prevent.
  case "$kind" in arithmetic|no-export) reason="" ;;
  *)
  if reason="$(waived "$file" "$kind")"; then
    # One line per waived file+kind, not per hit: the reason is the thing worth
    # reading and hack/benchguard_test.sh alone has six fixture lines.
    case "$waivers_used" in
      *"|$file $kind|"*) return ;;
    esac
    waivers_used="$waivers_used|$file $kind|"
    printf '  waived %-20s %-26s %s\n' "$kind" "$file" "$reason"
    return
  fi ;;
  esac
  printf '  %-22s %s\n     %s\n' "$kind" "$where" "$msg" >&2
  problems=$((problems + 1))
}

# ---------------------------------------------------------------- A: arithmetic
cap="$root/hack/cpucap.sh"
[ -x "$cap" ] || { echo "cpucap-check: $cap is missing or not executable" >&2; exit 2; }
checked=0
for avail in 1 2 3 4 6 8 16 64 96; do
  for ceil in 1 2 3 4 8 16 64; do
    for width in 1 4 8; do
      read -r g p <<<"$(IQMQ_CPUCAP_AVAIL="$avail" IQMQ_MAX_CPUS="$ceil" IQMQ_TEST_WIDTH="$width" "$cap" pair)"
      checked=$((checked + 1))
      bad=""
      [ "$g" -ge 1 ] && [ "$p" -ge 1 ] || bad="a factor is below 1"
      [ $(( g * p )) -le "$ceil" ] || bad="-p x GOMAXPROCS = $(( g * p )) exceeds the ceiling $ceil"
      [ "$g" -le "$avail" ] || bad="GOMAXPROCS $g is above the $avail CPU(s) the host has, which RAISES it"
      [ "$p" -le "$avail" ] || bad="-p $p is above the $avail CPU(s) the host has, which RAISES it"
      # A FLOOR, not only a ceiling. Every other assertion here is an upper
      # bound, so a cap that answered GOMAXPROCS=1 everywhere would pass all of
      # them -- and at 1 the sharded queue collapses to one shard, internal/pool
      # to one stripe and internal/wait's Spin kind skips, which is the whole
      # argument for width 4. The cap may never drop the
      # width below what the host and the configured width both allow.
      want_g="$width"
      [ "$want_g" -le "$ceil" ]  || want_g="$ceil"
      [ "$want_g" -le "$avail" ] || want_g="$avail"
      [ "$g" -ge "$want_g" ] || bad="GOMAXPROCS $g is below the $want_g that ceiling $ceil, width $width and $avail CPU(s) all allow"
      if [ -n "$bad" ]; then
        report "hack/cpucap.sh:0" arithmetic "avail=$avail ceiling=$ceil width=$width gave GOMAXPROCS=$g -p=$p: $bad"
      fi
    done
  done
done

# --------------------------------------------------------------- B: the exports
mk="$root/Makefile"
grep -q '^export GOMAXPROCS GOFLAGS$' "$mk" \
  || report "Makefile:0" no-export "the Makefile does not 'export GOMAXPROCS GOFLAGS', so no recipe inherits the cap and every path escapes at once"
grep -q 'cpucap.sh pair' "$mk" \
  || report "Makefile:0" no-export "the Makefile does not derive its width from hack/cpucap.sh, so the ceiling has a second implementation"
grep -q 'cpucap.sh fuzz' "$mk" \
  || report "Makefile:0" no-export "the Makefile does not derive the fuzz fan-out (JOBS/SHARDS/FUZZ_WORKERS) from hack/cpucap.sh, so jobs x workers x GOMAXPROCS is unbounded"
# The two module runners are checked BY NAME as well as by the property below,
# because they run `"$@"` rather than naming `go` themselves -- the property
# rule cannot see them, and they are the two scripts every test target goes
# through.
for runner in hack/for-each-module.sh hack/ci-group.sh; do
  grep -q 'cpucap.sh" env' "$root/$runner" \
    || report "$runner:0" no-export "the capped runner does not apply hack/cpucap.sh, so every module it visits runs uncapped"
done

# ------------------------------------------------- the scan (logical lines)
files=("$mk")
# `.py` as well as `.sh`: rule C's "no re-raising" has to reach a Python caller
# that assigns GOMAXPROCS or GOFLAGS, which is where the literal ceiling
# `hack/pinsweep.py` carried actually lived.
while IFS= read -r f; do files+=("$f"); done < <(find "$root/hack" \( -name '*.sh' -o -name '*.py' \) | sort)

scan() {
  awk '
    { if (buf == "") { start = FNR; buf = $0 } else { buf = buf " " $0 }
      if (buf ~ /\\$/) { sub(/\\$/, "", buf); next }
      print FILENAME ":" start "\t" buf; buf = "" }
    END { if (buf != "") print FILENAME ":" start "\t" buf }
  ' "${files[@]}" | sed "s|^$root/||"
}
lines="$(scan)"

# Comment lines are dropped; a make RECIPE comment is "<tab>@# ...". The text is
# "everything after the FIRST tab", because a recipe line starts with a tab of
# its own and splitting on tabs put the empty string in $2.
code() { printf '%s\n' "$lines" | awk '{ i = index($0, "\t"); if (substr($0, i + 1) !~ /^[[:space:]]*@?#/) print }'; }

# B, as a PROPERTY rather than a list of two file names. Rule B used to name
# hack/for-each-module.sh and hack/ci-group.sh, so a brand-new hack script that
# ran `go test` without the cap was invisible, and 25 existing ones were
# uncapped while the Makefile and the PLAYBOOK both claimed otherwise.
# Now: every hack script that invokes `go` itself must
# apply hack/cpucap.sh, or carry a waiver saying who owns it.
# A `go test` inside a quoted string can be text rather than an invocation.
# Corpus headers and assertions against stub output are two examples.
#
# Quoting is NOT the discriminator on its own, though: `bash -c "go test ./..."`
# is quoted and does execute. So the quoted spans are stripped unless the line
# also names something that executes its argument.
SHELL_INVOKER='(bash|sh|zsh)[[:space:]]+-[a-z]*c|eval[[:space:]]|-exec[[:space:]]'
# strip_quoted and executable_text answer into $REPLY and spawn NOTHING, and
# that is the whole of why they look like this.
#
# They used to be `printf | sed` read through a `$(...)`, called once per LINE
# of every scanned file, and the per-line matcher below was `printf | grep -qE`.
# That is four forks a line, about twenty thousand lines, and it cost 33 of this
# checker's 34 seconds -- measured: the 189 arithmetic cases take 894 ms, the
# scan took the rest. The self-test then runs the whole checker once per case,
# so the forks were paid fifty times over and the job took 18 minutes on every
# pull request. Nothing about WHAT is matched changes here; the semantics are
# pinned by the fifty self-test cases and by the main pass's output being
# identical before and after.
#
# The pairing rule is sed's, kept exactly: spans are taken left to right and
# non-overlapping, and a span's body cannot contain its own quote, so quotes
# pair 1-2, 3-4 and so on. A span becomes the empty pair it was before ('' or
# "") rather than vanishing, because the matcher's leading class looks at the
# character before `go`.
strip_quoted() { # remove '...' then "..." spans; answers in $REPLY
  local s="$1" out="" rest
  while [[ $s == *"'"*"'"* ]]; do
    out+="${s%%\'*}''"
    rest="${s#*\'}"
    s="${rest#*\'}"
  done
  s="$out$s"
  out=""
  while [[ $s == *'"'*'"'* ]]; do
    out+="${s%%\"*}\"\""
    rest="${s#*\"}"
    s="${rest#*\"}"
  done
  REPLY="$out$s"
}
executable_text() { # the part of a line that could actually run a command
  case "$1" in
    *bash\ -c*|*sh\ -c*|*eval\ *|*-exec\ *) REPLY="$1" ;;
    *) strip_quoted "$1" ;;
  esac
}

# The character class used to exclude a double quote before `go`, which meant
# `bash -c "go test ./..."` -- a quoted string that really does execute -- never
# matched at all. That was a pre-existing hole, not one the quote-stripping
# introduced; the control case written to prove the stripping had not opened a
# hole is what found it. Quoted TEXT is removed by executable_text() before this
# regex runs, so allowing a quote here is safe.
GO_INVOKE='(^|[^-_[:alnum:]])(go|\$GO|\$\{GO[^}]*\}|"\$\{GO[^}]*\}"|"\$go"|\$\(GO\))[[:space:]]+(test|build|run|vet|list|mod)'
# GO_INVOKE is a SHELL matcher and it is run over `.py` files too, where its only
# true-positive shape is a shell script held in a multi-line string and handed to
# something that runs it -- every other way a Python file spells a command puts it
# inside quotes, which executable_text() strips (that is why the argv
# discriminator below exists at all). A docstring BODY line carries no quotes, so
# nothing was stripped and the matcher read PROSE: `hack/gate-parity.py`'s
# docstring says "a hung `go test` holds a runner for a working day" and the rule
# reported a file that imports json, re and sys and starts no process as an
# escape from the CPU ceiling. Waived on the wave-3 landing, narrowed here.
#
# The narrowing is a NECESSARY CONDITION of the property, not a guess about
# wording: a Python file that can start no process cannot invoke `go`, whatever
# its prose says. So GO_INVOKE is asked of a `.py` only when the FILE also names
# something that runs a command. Two deliberate limits, both the conservative way
# round for a ceiling check:
#
#   * it gates GO_INVOKE only. The argv discriminator stays unconditional, so a
#     `.py` that builds `["go", "test"]` and hands it to a runner spelled in no
#     way listed here is still reported. Gating both would have made the list's
#     completeness load-bearing for the rule's main path.
#   * a file that DOES execute and also has prose about `go test` still fires.
#     That residue is kept on purpose: a false positive costs a waiver line,
#     whereas a matcher that stops seeing a real `go test` costs the 64-core
#     stall the ceiling exists to prevent. Stripping docstrings instead would
#     have cost the one true-positive shape above -- the self-test's "python go
#     test in an executed string" is exactly that shape, and it is watched
#     firing.
PY_EXEC='subprocess|Popen|check_call|check_output|getoutput|getstatusoutput|os\.system|os\.popen|os\.exec|os\.spawn|pty\.spawn|runpy|asyncio\.create_subprocess|multiprocessing|shell=True'
while IFS= read -r f; do
  rel="${f#"$root"/}"
  # `if ... then continue; fi`, never `[ ... ] && continue`: an `&&` list whose
  # left side is false returns 1, and under `set -e` that aborts the loop -- the
  # checker exited 1 having reported nothing at all.
  if [ "$rel" = "hack/cpucap.sh" ]; then continue; fi   # it IS the cap
  shell_matcher=1
  if [ "${rel##*.}" = py ] && ! grep -qE "$PY_EXEC" "$f"; then shell_matcher=0; fi
  hit=0
  while IFS= read -r ln; do
    case "$ln" in [[:space:]]*\#*|\#*) continue ;; esac
    if [ "$shell_matcher" = 1 ]; then
      executable_text "$ln"
      if [[ $REPLY =~ $GO_INVOKE ]]; then hit=1; break; fi
    fi
    # A Python caller writes no command LINE, it writes an argv LIST:
    # `subprocess.run(["go", "test", ...])`. GO_INVOKE cannot see that, and not
    # by accident -- quoted spans are stripped before it runs (assumption
    # C10.17), and in Python the command IS a quoted span. The discriminator is
    # the argv ELEMENT, `"go",` exactly: a sentence about the go command never
    # looks like that, and stripping fewer quotes in a .py would report every
    # docstring that mentions `go test`.
    if [ "${rel##*.}" = py ] && [[ $ln =~ [\'\"]go[\'\"][[:space:]]*, ]]; then hit=1; break; fi
  done < "$f"
  if [ "$hit" = 0 ]; then continue; fi
  # How the cap is APPLIED depends on the language, so the test does too, and
  # the shell half is left exactly as strict as it was. A `.sh` has to carry the
  # `eval "$(hack/cpucap.sh env)"` line; loosening that to "mentions the script"
  # would accept every capped script's COMMENT about it, which is the weakening
  # the existing "a capped script unhooked" case is there to catch. A `.py`
  # cannot eval a shell assignment, so it spells the script as an argv element --
  # in QUOTES, which is what distinguishes calling it from writing about it.
  if [ "${rel##*.}" = py ]; then
    if grep -qE '"[^"]*cpucap\.sh"' "$f"; then continue; fi
  elif grep -q 'cpucap.sh" env' "$f"; then
    continue
  fi
  report "$rel:0" uncapped-script "invokes \`go\` itself but never applies hack/cpucap.sh, so running it by hand gets GOMAXPROCS=nproc and -p=nproc"
# `.py` as well as `.sh`, because the first version of this rule scanned shell
# only and `hack/pinsweep.py` -- 32 module test runs, now a weekly job -- carried
# `GOMAXPROCS="4", GOFLAGS="-p=4"` as LITERALS. That is a second implementation
# of the ceiling ('s class) that no rule here could see, found
# by reading rather than by this checker. The Python files
# that run no `go` cost one grep each.
done < <(find "$root/hack" \( -name '*.sh' -o -name '*.py' \) | sort)

# --------------------------------------------------------------------- C
# "no re-raising". The exemption used to be by SHAPE -- any line starting with
# an uppercase letter and an `=` -- which is the same shape as a command prefix,
# so `GOMAXPROCS=64 go test ./...` at column 0 of a shell script was skipped and
# a make recipe escaped by dropping one `@`. Six BLOCKING findings were that one
# character wide. It is now by POSITION and IDENTITY :
#
#   * a Makefile line that is NOT tab-indented and carries no `go` invocation is
#     a variable assignment, and only the derivation's own names may assign to
#     GOMAXPROCS/GOFLAGS/FUZZ_WORKERS;
#   * anywhere else -- a recipe body, any line of any .sh -- an assignment to
#     those names is a path around the cap and is always reported.
derivation_line() { # NORMALISED text -> 0 when it is one of this repo's derivation lines
  case "$1" in
    'GOMAXPROCS := $(word 1,$(IQMQ_CPUCAP))'*)      return 0 ;;
    'GOFLAGS := $(strip -p=$(GOTEST_P)'*)           return 0 ;;
    'FUZZ_WORKERS ?= $(word 2,$(IQMQ_FUZZCAP))'*)   return 0 ;;
    'FUZZ_GOMAXPROCS ?= $(word 3,$(IQMQ_FUZZCAP))'*) return 0 ;;
    'export GOMAXPROCS GOFLAGS'*)                   return 0 ;;
    'export FUZZ_WORKERS'*)                         return 0 ;;
    'export IQMQ_MAX_CPUS IQMQ_TEST_WIDTH'*)        return 0 ;;
  esac
  return 1
}
while IFS=$'\t' read -r where text; do
  [ -n "${where:-}" ] || continue
  file="${where%%:*}"
  # A .py is rule J's, not rule C's, and the reason is that the DEFECT and the
  # DERIVATION have the same shape in Python. A shell script exports the cap and
  # a re-raise is always an assignment to the name; a Python helper passes it as
  # an environment dict, so the line that applies the cap honestly reads
  # `GOMAXPROCS=pair[0]` and spells exactly what rule C rejects. Rule J decides
  # it BY VALUE instead -- the same widening rule D took for `-cpu`.
  case "$file" in *.py) continue ;; esac
  # A Makefile recipe body keeps its leading tab in the joined text.
  is_recipe=1; case "$text" in $'\t'*) is_recipe=0 ;; esac
  ntext="$(printf '%s' "$text" | tr -s ' \t' ' ')"
  ntext="${ntext# }"
  if [ "$file" = Makefile ] && [ "$is_recipe" = 1 ]; then
    if derivation_line "$ntext"; then continue; fi
    # a top-level assignment that runs no go command is a variable, and the
    # identity test above already rejected an assignment to a capped name
    case "$ntext" in
      *GOMAXPROCS*|*GOFLAGS*|*FUZZ_WORKERS*) : ;;
      *) continue ;;
    esac
  fi
  # The fuzz recipes run at the fuzz width, which is a different derivation from
  # the test width. Exactly this token is allowed, and only
  # in the Makefile: in a .sh `$(FUZZ_GOMAXPROCS)` is command substitution, not
  # a make variable, so it is never the derivation there. Both halves of this
  # narrowing were holes the mutation battery found minutes after the exemption
  # was added -- an exemption is a rule that has to be probed like any other.
  skip_gm=1
  if [ "$file" = Makefile ]; then
    case "$ntext" in *'GOMAXPROCS=$(FUZZ_GOMAXPROCS)'*) skip_gm=0 ;; esac
  fi
  case "$ntext" in
    *'GOMAXPROCS=${GOMAXPROCS:'*|*'GOFLAGS=${GOFLAGS:'*|*'FUZZ_WORKERS=${FUZZ_WORKERS:'*) continue ;;
  esac
  if [ "$skip_gm" = 1 ]; then
    case "$ntext" in
      *GOMAXPROCS=*|*GOMAXPROCS\ :=*|*GOMAXPROCS:=*|*'GOMAXPROCS ='*|*'GOMAXPROCS ?='*|*'GOMAXPROCS?='*)
        report "$where" gomaxprocs-override "assigns GOMAXPROCS, which overrides the exported ceiling; only hack/cpucap.sh may decide it" ;;
    esac
  fi
  case "$ntext" in
    *GOFLAGS=*|*GOFLAGS\ :=*|*GOFLAGS:=*|*'GOFLAGS ='*|*'GOFLAGS ?='*|*'GOFLAGS?='*)
      report "$where" goflags-override "assigns GOFLAGS, which drops the exported -p" ;;
  esac
  case "$ntext" in
    *FUZZ_WORKERS=*|*'FUZZ_WORKERS ?='*|*'FUZZ_WORKERS :='*|*'FUZZ_WORKERS?='*)
      report "$where" fuzz-workers "assigns FUZZ_WORKERS outside the derivation; it multiplies with GOMAXPROCS and with the shell fan-out" ;;
  esac
done < <(code | grep -E $'\t.*(GOMAXPROCS|GOFLAGS|FUZZ_WORKERS)[[:space:]]*[:?+]?=' || true)

# Any -p on a go command line, literal or through a variable. The old rule
# required `-p[= ][0-9]`, so `-p $(GOTEST_PWIDE)` sailed through. There is never
# a reason to pass -p: GOFLAGS carries it.
while IFS=$'\t' read -r where text; do
  [ -n "${where:-}" ] || continue
  report "$where" literal-p "passes -p to the go command; GOFLAGS already carries the capped value, and anything here overrides it"
done < <(code | grep -E $'\t.*(go|\\$\\(GO\\)|"\\$go"|\\$\\{GO[^}]*\\}) (test|build|run|vet).*[[:space:]]-p[= ]' || true)

# ----------------------------------------------- D: benchmark width, by VALUE
# The old rule tested only that `-cpu` was PRESENT, so `-cpu=64` passed.
while IFS=$'\t' read -r where text; do
  [ -n "${where:-}" ] || continue
  # `|| true`: grep exits 1 when it matches nothing, and with `pipefail` that
  # makes the assignment fail and `set -e` abort the whole checker -- which
  # exited 1 having reported nothing, the most dangerous way for a check to go
  # wrong.
  cpuval="$(printf '%s\n' "$text" | grep -oE -- '-cpu[= ][^ ]+' | head -1 | sed 's/^-cpu[= ]//' || true)"
  if [ -z "$cpuval" ]; then
    # There used to be an exemption here for a line carrying `$pin`, on the grounds
    # that a pinned run was bounded by its pin rather than by -cpu. Nothing in this
    # repository pins and no `pin` variable exists any more , so
    # the exemption could only excuse a line naming a variable nothing defines --
    # a loophole rather than a rule. Every `go test -bench` line names its own -cpu.
    report "$where" unbounded-bench "runs 'go test -bench' with no -cpu, so it takes GOMAXPROCS=nproc -- every core the host has"
    continue
  fi
  case "$cpuval" in
    *[!0-9]*) continue ;;   # a variable; the ceiling decides it
  esac
  if [ "$cpuval" -gt "${IQMQ_MAX_CPUS:-16}" ]; then
    report "$where" unbounded-bench "runs 'go test -bench -cpu=$cpuval', above the ${IQMQ_MAX_CPUS:-16}-CPU ceiling"
  fi
done < <(code | grep -E $'\t.*(go|\\$\\(GO\\)) test .*-bench' || true)

# E: one ceiling, not two.
while IFS=$'\t' read -r where text; do
  [ -n "${where:-}" ] || continue
  report "$where" bare-bench-ceiling "defaults a CPU ceiling to a bare number; it must fall through to \${IQMQ_MAX_CPUS:-16} so the benchmark ceiling and the test ceiling are one number"
done < <(code | grep -E $'\t.*(IQMQ_BENCH_MAX_CPUS|IQMQ_GATE_CPUS):-[0-9]' || true)

# F: capacity, not affinity. Broadened twice. First past `taskset -c <digit>`
# and `cut -d, -f1-`, because a fixed pin written with `head -n N` or a `seq`
# list is the same defect spelled differently. Now past the fixed list
# altogether: `taskset` in ANY form is a finding, because nothing in this
# repository pins any more. Choosing CPUs is the shared `cpu-policy` tool's job
# -- it picks quiet WHOLE cores rather than loose threads, so two of a job's
# threads cannot land on the two halves of one core, and it keeps off a reserved
# cache group. A second pin chosen here from a second sample could only narrow or
# contradict the one the tool already made.
#
# Comments are safe: `code` strips them, which is what assumption C10.17 is about
# -- three of this rule's five findings were once prose rather than code.
while IFS=$'\t' read -r where text; do
  [ -n "${where:-}" ] || continue
  report "$where" fixed-cpu-list "pins CPUs, and nothing here may: the ceiling is a capacity cap (assumption C10.4), and a measurement that genuinely needs a pin is run under \`cpu-policy run --bench\`, which chooses quiet whole cores and names them in CPU_POLICY_CPUS"
done < <(code | grep -E $'\t.*(taskset|cut -d, -f1-|cpuset=.*(head -n|seq )|(head -n|seq ).*\\|.*paste -sd, )' || true)

# G: a literal -parallel on a -fuzz run. Those workers are PROCESSES, so the
# count multiplies with GOMAXPROCS the way -p does.
while IFS=$'\t' read -r where text; do
  [ -n "${where:-}" ] || continue
  report "$where" fuzz-workers "passes a literal -parallel to a -fuzz run; those workers are PROCESSES, so the count multiplies with GOMAXPROCS and with the shell fan-out. Use FUZZ_WORKERS, which the Makefile derives from the ceiling"
done < <(code | grep -E $'\t.*-fuzz' | grep -E -- '-parallel[= ]+[0-9]' || true)

# ------------------------------ I: a -parallel on a -fuzz run through a VARIABLE
# Rule G reads the number off the line, so it sees nothing when the number is not
# on the line. `hack/linearize-campaign.sh` carried `-parallel "$parallel"` with
# `parallel="${LIN_PARALLEL:-4}"` forty lines away: four worker PROCESSES at
# GOMAXPROCS 4, which is exactly the ceiling by coincidence, and `LIN_PARALLEL=8`
# was 32 with nothing in the tree able to say so. It was found by reading and not
# by this checker, which is the gap this rule closes.
#
# WHAT IT CHECKS, and it is deliberately weaker than "the flag is bounded": that
# the FILE derives that variable from `hack/cpucap.sh` at least once. Proving
# every path to the flag is bounded needs dataflow a grep does not have -- a file
# that derived the number and then overwrote it would pass -- so this is the same
# strength as the FUZZ_WORKERS blessing rule G already rests on, and it is chosen
# because the alternative (flagging every variable `-parallel`) fires on clean
# code, which is worse than a rule with a stated bound.
#
# FUZZ_WORKERS is accepted by NAME, in the Makefile and in a script alike,
# because rule C already refuses an assignment to it outside the derivation and
# the self-test below watches that rule fail. The literal DEFAULT beside it in
# `hack/fuzz.sh` (`${FUZZ_WORKERS:-8}`, which is 8 when the script is run by hand
# rather than from the Makefile) is not this rule's subject and is already
# recorded in that file's `uncapped-script` waiver.
while IFS=$'\t' read -r where text; do
  [ -n "${where:-}" ] || continue
  pfile="${where%%:*}"
  pval="$(printf '%s\n' "$text" | grep -oE -- '-parallel[= ]+[^ ]+' | head -1 | sed 's/^-parallel[= ]*//' || true)"
  case "$pval" in
    *'$'*) : ;;
    *) continue ;;   # a literal, or nothing; rule G owns those
  esac
  # The variable's name, whichever of $v "$v" ${v} ${v:-N} $(v) it is spelled as.
  pname="$(printf '%s\n' "$pval" | grep -oE '[A-Za-z_][A-Za-z_0-9]*' | head -1 || true)"
  [ -n "$pname" ] || continue
  case "$pname" in FUZZ_WORKERS) continue ;; esac
  if grep -qE "(^|[^A-Za-z_0-9])$pname[[:space:]]*[?:]?=[^=].*cpucap\.sh" "$root/$pfile"; then continue; fi
  report "$where" fuzz-workers "passes -parallel through \$$pname on a -fuzz run and nothing in $pfile assigns $pname from hack/cpucap.sh; those workers are PROCESSES, so the count multiplies with GOMAXPROCS. Derive it (hack/linearize-campaign.sh and hack/fuzz-changed.sh both use \`cpucap.sh p\`) or use FUZZ_WORKERS"
done < <(code | grep -E $'\t.*-fuzz' | grep -E -- '-parallel[= ]+[^ ]*\$' || true)

# ------------------------- J: a hack/*.py that hardcodes the ceiling, BY VALUE
# `hack/pinsweep.py` runs a module test suite per ambiguous construction -- 32 of
# them, and a weekly job since assumption C13.7 -- and carried
# `GOMAXPROCS="4", GOFLAGS="-p=4"` as LITERALS. That is a second implementation
# of the ceiling, the class assumption C10.11 is about, and no rule here could
# see it: the scan was shell and the Makefile only. It was found by reading.
#
# Rule C cannot be reused as it stands, because in Python the honest derivation
# spells the same thing the defect does -- `GOMAXPROCS=pair[0]`, where `pair`
# came from `hack/cpucap.sh`. So this decides BY VALUE, which is the widening
# rule D took for `-cpu`: a LITERAL number is the offence, and a derivation
# never is one. The uncapped-script property above is the other half, and it
# requires such a file to name `hack/cpucap.sh` at all.
while IFS=$'\t' read -r where text; do
  [ -n "${where:-}" ] || continue
  report "$where" gomaxprocs-override "sets GOMAXPROCS to a literal in a Python helper, which is a second implementation of the ceiling; read it from hack/cpucap.sh"
done < <(code | grep -E $'^hack/[^\t]*\\.py:' | grep -E 'GOMAXPROCS[[:space:]]*=[[:space:]]*"?[0-9]' || true)
while IFS=$'\t' read -r where text; do
  [ -n "${where:-}" ] || continue
  report "$where" goflags-override "sets GOFLAGS to a literal -p in a Python helper, which drops the derived -p; read it from hack/cpucap.sh"
done < <(code | grep -E $'^hack/[^\t]*\\.py:' | grep -E 'GOFLAGS[[:space:]]*=[[:space:]]*f?"?-p=[0-9]' || true)

# --------------------------------------------- H: the SHELL fan-out multiplies
# `-parallel` was bounded one `go test` line at a time while the shell started
# several at once: `make fuzz` was `xargs -P 8` x `-parallel 4` x GOMAXPROCS 4 =
# 128 against a ceiling of 16. A fan-out primitive on a line
# that reaches a `go` invocation must take its width from the ceiling.
while IFS=$'\t' read -r where text; do
  [ -n "${where:-}" ] || continue
  width="$(printf '%s\n' "$text" | grep -oE -- '(-P|-j)[= ]*[0-9]+' | head -1 || true)"
  [ -n "$width" ] || continue
  report "$where" shell-fanout "fans out $width in the shell over a go invocation; jobs x workers x GOMAXPROCS must come from hack/cpucap.sh, not from a literal"
done < <(code | grep -E $'\t.*(xargs.*-P|make.*-j[0-9])' || true)

if [ "$problems" -gt 0 ]; then
  echo "cpu-cap-check: $problems path(s) escape the ${IQMQ_MAX_CPUS:-16}-CPU ceiling" >&2
  exit 1
fi
printf 'cpu-cap-check: %s arithmetic case(s) and %s file(s) clean; %s\n' \
  "$checked" "${#files[@]}" "$("$cap" report)"

# ------------------------------------------------------------------ self-test
[ "$mode" = selftest ] || exit 0

tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT
mkdir -p "$tmp/hack/bench"
cp "$root/Makefile" "$tmp/Makefile"
cp "$root"/hack/*.sh "$tmp/hack/"
cp "$root"/hack/*.py "$tmp/hack/"
cp "$root"/hack/bench/*.sh "$tmp/hack/bench/" 2>/dev/null || true
chmod +x "$tmp/hack/cpucap.sh"

echo "== cpu-cap-check self-test"
fails=0
expect() { # name expected-status expected-kind doctoring-command...
  local name="$1" want="$2" kind="$3"; shift 3
  ( cd "$tmp" && "$@" )
  local got=0
  "$self/cpucap-check.sh" -root "$tmp" >"$tmp/out.txt" 2>&1 || got=$?
  # The KIND is asserted, not only the exit status. A mutant caught by some
  # unrelated rule looks exactly like a kill if you only read rc, and a rule
  # that can no longer fire for its own shape is invisible behind a sibling
  # that happens to fire instead. That is the same error as proving a flake is
  # contention by varying load while holding the variable that mattered fixed.
  local fired
  fired="$(grep -vE '^  waived' "$tmp/out.txt" | grep -oE '^  [a-z-]+ ' | tr -d ' ' | sort -u | tr '\n' ',' || true)"
  local ok=1
  [ "$got" = "$want" ] || ok=0
  if [ "$want" != 0 ] && [ -n "$kind" ]; then
    printf '%s' "$fired" | grep -q "$kind" || ok=0
  fi
  if [ "$ok" = 1 ]; then
    printf '   ok    %-34s exit=%s  %s\n' "$name" "$got" "${fired%,}"
  else
    printf '   FAIL  %-34s exit=%s (wanted %s via %s), fired [%s]\n' \
      "$name" "$got" "$want" "${kind:-any}" "${fired%,}" >&2
    sed -n '1,6p' "$tmp/out.txt" | sed 's/^/          /' >&2
    fails=$((fails + 1))
  fi
  ( cd "$tmp" && git_restore )
}
git_restore() {
  # A COPY restores the tree, never `git checkout --`: that would discard
  # uncommitted work in the real worktree, and worse, it fails SILENTLY, so the
  # cases after the first revert would be measured against the wrong tree and
  # would report kills they never achieved.
  rm -f "$tmp/hack/newscript.sh" "$tmp/hack/runner.sh" "$tmp/hack/capper.py"
  cp "$root/Makefile" "$tmp/Makefile"
  cp "$root"/hack/*.sh "$tmp/hack/"
  cp "$root"/hack/*.py "$tmp/hack/"
  cp "$root"/hack/bench/*.sh "$tmp/hack/bench/" 2>/dev/null || true
  chmod +x "$tmp/hack/cpucap.sh"
}
export -f git_restore 2>/dev/null || true

# The clean copy must PASS, or every failure below proves nothing.
expect "untouched copy" 0 "" true

# Each of these is a real way a path has escaped, or could.
expect "a new uncapped benchmark" 1 "unbounded-bench" \
  bash -c 'printf "\nbench-new:\n\t@\$(GO) test -run=%s -bench=. ./internal/queue\n" "^\$\$" >> Makefile'
expect "a recipe that re-raises GOMAXPROCS" 1 "gomaxprocs-override" \
  bash -c 'printf "\ntest-wide:\n\t@GOMAXPROCS=64 \$(GO) test ./...\n" >> Makefile'
expect "a literal -p on a go line" 1 "literal-p" \
  bash -c 'printf "\ntest-p:\n\t@\$(GO) test -p 16 ./...\n" >> Makefile'
expect "the exports deleted" 1 "no-export" \
  bash -c 'sed -i "s/^export GOMAXPROCS GOFLAGS\$/# export removed/" Makefile'
expect "a second, literal ceiling" 1 "bare-bench-ceiling" \
  bash -c 'sed -i "s/IQMQ_BENCH_MAX_CPUS:-\${IQMQ_MAX_CPUS:-16}/IQMQ_BENCH_MAX_CPUS:-16/" hack/benchguard.sh'
expect "a hardcoded core pin" 1 "fixed-cpu-list" \
  bash -c 'printf "\npin=\"taskset -c 0-15\"\n" >> hack/benchgate.sh'
expect "arithmetic that breaks the product" 1 "arithmetic" \
  bash -c 'sed -i "s|^p=\$(( ceil / g ))\$|p=\$(( ceil ))|" hack/cpucap.sh'
expect "the capped runner unhooked" 1 "no-export" \
  bash -c 'sed -i "s|^eval \"\$(\"\$root/hack/cpucap.sh\" env)\"\$|: no cap|" hack/for-each-module.sh'
# Four more, added because an audit of "every rule has a mutation" found these
# four enforced by code that nothing had ever watched fail. A check that cannot
# fail is the defect this wave has found on four branches running.
expect "the CI shard runner unhooked" 1 "no-export" \
  bash -c 'sed -i "s|^eval \"\$(\"\$root/hack/cpucap.sh\" env)\"\$|: no cap|" hack/ci-group.sh'
expect "the width no longer derived" 1 "no-export" \
  bash -c 'sed -i "s|cpucap.sh pair|echo 64 64|" Makefile'
expect "a recipe that re-raises GOFLAGS" 1 "goflags-override" \
  bash -c 'printf "\ntest-goflags:\n\t@GOFLAGS=-p=32 \$(GO) test ./...\n" >> Makefile'
expect "a literal -parallel on a fuzz run" 1 "fuzz-workers" \
  bash -c 'printf "\nfuzz-wide:\n\t@\$(GO) test -fuzz=. -parallel 16 ./internal/queue\n" >> Makefile'
# The thirteen below are the mutants an adversarial review found SURVIVING, plus
# one per rule added since. Each was watched failing before it was added here.
# The three "harness control" cases come first on purpose: a battery that cannot
# tell the clean tree from a doctored one reports kills it never achieved, which
# is a worse failure than a missed defect because it points at confidence.
expect "CONTROL a harmless comment" 0 "" \
  bash -c 'printf "\n# a harmless comment\n" >> hack/benchgate.sh'
expect "shell prefix at column 0" 1 "gomaxprocs-override" \
  bash -c 'printf "\nGOMAXPROCS=64 go test ./...\n" >> hack/benchgate.sh'
expect "make recipe without @" 1 "gomaxprocs-override" \
  bash -c 'printf "\ntest-wide2:\n\tGOMAXPROCS=64 \$(GO) test ./...\n" >> Makefile'
expect "make variable re-raise" 1 "gomaxprocs-override" \
  bash -c 'printf "\nGOMAXPROCS := 64\n" >> Makefile'
expect "shell GOFLAGS at column 0" 1 "goflags-override" \
  bash -c 'printf "\nGOFLAGS=\"-p=32\"\ngo test ./...\n" >> hack/benchgate.sh'
expect "export re-raise in a script" 1 "gomaxprocs-override" \
  bash -c 'printf "\nexport GOMAXPROCS=64\n" >> hack/benchgate.sh'
expect "-cpu above the ceiling" 1 "unbounded-bench" \
  bash -c 'sed -i "s|-cpu=\$(IQMQ_MAX_CPUS)|-cpu=64|" Makefile'
expect "fixed pin spelled head -n" 1 "fixed-cpu-list" \
  bash -c 'printf "\ncpuset=\"\$(echo \"\$a\" | tr , \"\\\\n\" | head -n 16 | paste -sd, -)\"\n" >> hack/benchgate.sh'
expect "-p through a variable" 1 "literal-p" \
  bash -c 'printf "\nGOTEST_PWIDE ?= 16\ntp:\n\t@\$(GO) test -p \$(GOTEST_PWIDE) ./...\n" >> Makefile'
expect "shell fan-out xargs -P 8" 1 "shell-fanout" \
  bash -c 'printf "\nfz:\n\t@cat l | xargs -P 8 -L1 \$(GO) test -fuzz=. ./x\n" >> Makefile'
expect "a new uncapped hack script" 1 "uncapped-script" \
  bash -c 'printf "#!/usr/bin/env bash\ngo test ./...\n" > hack/newscript.sh'
expect "a capped script unhooked" 1 "uncapped-script" \
  bash -c 'sed -i "s|^eval \"\$(\"\$root/hack/cpucap.sh\" env)\"\$|: uncapped|" hack/cover.sh'
expect "the fuzz fan-out not derived" 1 "no-export" \
  bash -c 'sed -i "s|cpucap.sh fuzz|echo 8 8|" Makefile'
expect "a fixed pin inside cpucap.sh itself" 1 "fixed-cpu-list" \
  bash -c 'printf "\ncpuset=\"\$(echo \"\$allowed\" | cut -d, -f1-16)\"\n" >> hack/cpucap.sh'
expect "FUZZ_WORKERS no longer derived" 1 "fuzz-workers" \
  bash -c 'sed -i "s|FUZZ_WORKERS    ?= .*|FUZZ_WORKERS    ?= 8|" Makefile'
expect "FUZZ_GOMAXPROCS no longer derived" 1 "gomaxprocs-override" \
  bash -c 'sed -i "s|FUZZ_GOMAXPROCS ?= .*|FUZZ_GOMAXPROCS ?= 16|" Makefile'
expect "GOMAXPROCS no longer derived" 1 "gomaxprocs-override" \
  bash -c 'sed -i "s|^GOMAXPROCS  := .*|GOMAXPROCS  := 64|" Makefile'
expect "GOFLAGS derivation altered" 1 "goflags-override" \
  bash -c 'sed -i "s|^GOFLAGS     := .*|GOFLAGS     := -p=32|" Makefile'
expect "the fuzz token smuggled into a script" 1 "gomaxprocs-override" \
  bash -c 'printf "\nGOMAXPROCS=\$(FUZZ_GOMAXPROCS)\n" >> hack/benchgate.sh'
# Quoted text must NOT fire, and the shapes that DO must still fire. Widening a
# rule is exactly when a clean shape starts failing, so both directions are
# pinned: three idioms that are text, and the one quoted form that executes.
expect "CONTROL a fuzz corpus header" 0 "" \
  bash -c 'printf "\nprintf \x27go test fuzz v1\\n[]byte(\\\"s\\\")\\n\x27 > c\n" >> hack/benchgate.sh'
expect "CONTROL a grep assertion naming go test" 0 "" \
  bash -c 'printf "\nif grep -q \"cd x && GOWORK=off go test -run Y\" f; then :; fi\n" >> hack/benchgate.sh'
expect "CONTROL a rendered reproduction line" 0 "" \
  bash -c 'printf "\nprintf \x27cd %%s && GOWORK=off go test -run %%s\\n\x27 \"\$d\" \"\$t\"\n" >> hack/benchgate.sh'
expect "bash -c that really runs go test" 1 "uncapped-script" \
  bash -c 'printf "#!/usr/bin/env bash\nbash -c \"go test ./...\"\n" > hack/runner.sh'
expect "CONTROL the honouring default form" 0 "" \
  bash -c 'printf "\nexport GOMAXPROCS=\${GOMAXPROCS:-4} GOFLAGS=\${GOFLAGS:--p=4}\n" >> hack/benchgate.sh'
expect "a bare literal export still fires" 1 "gomaxprocs-override" \
  bash -c 'printf "\nexport GOMAXPROCS=64\n" >> hack/benchgate.sh'
expect "the arithmetic floor (g=1)" 1 "arithmetic" \
  bash -c 'sed -i "s|^g=\"\$width\"\$|g=1|" hack/cpucap.sh'
# Rule I, both directions. A `-parallel` through a variable was invisible to
# every rule here until the linearize campaign was read by hand, so the rule that
# closes it is watched firing on the underived form and NOT firing on the derived
# one -- widening a rule is exactly when clean code starts failing.
expect "-parallel through an underived variable" 1 "fuzz-workers" \
  bash -c 'printf "\nn=16\ngo test -run=X -fuzz=. -parallel \"\$n\" ./x\n" >> hack/benchgate.sh'
expect "CONTROL -parallel through a derived variable" 0 "" \
  bash -c 'printf "\nn=\"\$(\"\$root/hack/cpucap.sh\" p)\"\ngo test -run=X -fuzz=. -parallel \"\$n\" ./x\n" >> hack/benchgate.sh'
expect "the linearize -parallel no longer derived" 1 "fuzz-workers" \
  bash -c 'sed -i "s|^parallel=.*|parallel=4|" hack/linearize-campaign.sh'
# Rule J, both directions. The scan reached shell and the Makefile only, so
# hack/pinsweep.py's literal GOMAXPROCS lived through every rule above it; the
# rule that closes it decides BY VALUE, so the derivation form has to stay clean
# beside the literal that must fire.
expect "a python helper with a literal GOMAXPROCS" 1 "gomaxprocs-override" \
  bash -c 'printf "import subprocess\nE = dict(GOMAXPROCS=\"4\")\nsubprocess.run([\"go\", \"test\"], env=E)\n" > hack/capper.py'
expect "a python helper with a literal GOFLAGS -p" 1 "goflags-override" \
  bash -c 'printf "import subprocess\nE = dict(GOFLAGS=\"-p=8\")\nsubprocess.run([\"go\", \"test\"], env=E)\n" > hack/capper.py'
expect "CONTROL a python helper that derives it" 0 "" \
  bash -c 'printf "import subprocess\np = subprocess.run([\"hack/cpucap.sh\", \"pair\"]).stdout.split()\nE = dict(GOMAXPROCS=p[0], GOFLAGS=f\"-p={p[1]}\")\nsubprocess.run([\"go\", \"test\"], env=E)\n" > hack/capper.py'
expect "a python helper with no cap at all" 1 "uncapped-script" \
  bash -c 'printf "import subprocess\nsubprocess.run([\"go\", \"test\", \"./...\"])\n" > hack/capper.py'
# The other direction for the argv discriminator, because widening the scan to
# .py is exactly when a file that only TALKS about the go command starts failing.
# `hack/pinsweep.py`'s own docstring says "Every `go test` runs under the host's
# core cap", and stripping fewer quotes in a .py would have reported it.
expect "CONTROL python prose about go test" 0 "" \
  bash -c 'printf "\"\"\"This module explains how to run go test with a cap.\"\"\"\nprint(\"nothing runs\")\n" > hack/capper.py'
# The prose narrowing, both directions. The control above is
# SINGLE-line, and it passes for a reason that has nothing to do with prose:
# `strip_quoted` erases the whole sentence because both triple quotes sit on the
# line. A docstring BODY line carries no quotes, nothing is stripped, and that is
# the shape that reported `hack/gate-parity.py`. The firing case beside it is the
# one true-positive shape GO_INVOKE-over-`.py` has -- a shell script held in a
# multi-line string and handed to something that runs it -- and it is here
# because the argv discriminator CANNOT satisfy it: exempting `.py` from
# GO_INVOKE would have left "a python helper with no cap at all" green while
# deleting the path this narrowing is about, which is a check that passes by
# finding nothing. The third case spells the runner `os.popen` rather than
# `subprocess`, so PY_EXEC losing an entry is watched rather than assumed.
expect "CONTROL python prose in a multi-line docstring" 0 "" \
  bash -c 'printf "\"\"\"Why the ceiling exists.\n\nA hung go test holds a runner for a working day.\n\"\"\"\nimport json\nprint(json.dumps({}))\n" > hack/capper.py'
expect "python go test in an executed string" 1 "uncapped-script" \
  bash -c 'printf "import subprocess\nSCRIPT = \"\"\"\ngo test ./...\n\"\"\"\nsubprocess.run([\"bash\", \"-c\", SCRIPT])\n" > hack/capper.py'
expect "the same script run through os.popen" 1 "uncapped-script" \
  bash -c 'printf "import os\nSCRIPT = \"\"\"\ngo test ./...\n\"\"\"\nos.popen(SCRIPT).read()\n" > hack/capper.py'

if [ "$fails" -gt 0 ]; then
  echo "cpu-cap-check: $fails self-test case(s) did not behave as required" >&2
  exit 1
fi
echo "   every half of cpu-cap-check was watched failing, and the clean tree passes"
