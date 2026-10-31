#!/usr/bin/env bash
# Validate a release tag against release/manifest.yaml:
#   release/validate-tags.sh vX.Y.Z                 (release train: every `release: train` module must be tagged)
#   release/validate-tags.sh drivers/kafka/vX.Y.Z   (independent patch)
#
# The manifest lists the modules included in a release train, and each one's
# `version:`. Both arms read the manifest and go.mod files as the tag's own
# commit has them, not as the checkout has them, so the answer is about the
# release and not about whatever is checked out.
#
# A train is one commit. Every train module's tag must exist AND point at the
# commit the root tag points at, and every train module's `version:` must be
# the train's. The old arm checked only that each tag existed, so a module tag
# left on an earlier commit (a tag made before a last fix, and tags cannot be
# moved once pushed) passed, and the train shipped two different trees under
# one version. An independent patch's `version:` must be the patch's.
set -euo pipefail
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
manifest="$root/release/manifest.sh"
tag="${1:?tag}"
cd "$root"

# commit_of <tag>: the commit a tag points at, or nothing when it is missing.
commit_of() { git rev-parse -q --verify "refs/tags/$1^{commit}" 2>/dev/null || true; }

# intra_replaces: the replace directives of the go.mod on stdin whose LEFT side
# is a module of this repository, as `line:text`. The left side is the first
# field after `replace` (or the first field of a line inside a `replace ( )`
# block), whatever follows it: `X => Y`, `X v1.1.1 => Y` (what `go mod edit
# -replace=X@v1.1.1=Y` writes, and Go honours it) and `X => module v1.0.0`.
# The grep this replaced wanted `=>` straight after the module path, so the
# versioned form passed it.
intra_replaces() {
  awk '
    { line = $0; sub(/\/\/.*/, "", line); gsub(/[()]/, " & ", line); gsub(/"/, "", line); n = split(line, f, " ") }
    n == 0 { next }
    inblock && f[1] == ")" { inblock = 0; next }
    f[1] == "replace" && f[2] == "(" && n == 2 { inblock = 1; next }
    { lhs = "" }
    f[1] == "replace" { lhs = f[2] }
    inblock { lhs = f[1] }
    lhs ~ /^github\.com\/iqhive\/iqmq(\/|$)/ { printf "%d:%s\n", NR, $0 }
  '
}

case "$tag" in
  v* | */v*) ;;
  *) echo "unrecognised tag $tag" >&2; exit 1 ;;
esac
rc="$(commit_of "$tag")"
[ -n "$rc" ] || { echo "MISSING $tag" >&2; exit 1; }
# The manifest the release commit carries.
tmp="$(mktemp)"
trap 'rm -f "$tmp"' EXIT
git show "$rc:release/manifest.yaml" > "$tmp" || { echo "$tag's commit $rc has no release/manifest.yaml" >&2; exit 1; }
export IQMQ_RELEASE_MANIFEST="$tmp"

case "$tag" in
  v*)
    train="$("$manifest" train)"
    [ "$train" = "$tag" ] || { echo "manifest train is $train, tag is $tag" >&2; exit 1; }
    status=0 want=0
    while read -r path; do
      [ -n "$path" ] || continue
      want=$((want + 1))
      [ "$path" = "." ] && t="$tag" || t="$path/$tag"
      c="$(commit_of "$t")"
      v="$("$manifest" version "$path" 2>&1)" || true
      [ "$path" = "." ] && gm="go.mod" || gm="$path/go.mod"
      left="$(git show "$rc:$gm" 2>/dev/null | intra_replaces)"
      if [ -z "$c" ]; then
        echo "MISSING $t" >&2; status=1
      elif [ "$c" != "$rc" ]; then
        echo "MOVED   $t is on $c, not on $tag's release commit $rc" >&2; status=1
      elif [ "$v" != "$tag" ]; then
        echo "VERSION $t: release/manifest.yaml gives $path version $v, not $tag" >&2; status=1
      elif [ -n "$left" ]; then
        # The train arm read no go.mod at all and left this to the module
        # audit in `make ci`, which had the same blind spot for a versioned
        # replace. A consumer ignores a dependency's replace, so a train
        # module that keeps one is tested here against this tree and built
        # by everybody else against the published version.
        echo "REPLACE $t: $gm still replaces modules inside this repository:" >&2
        printf '%s\n' "$left" | sed 's/^/        /' >&2
        status=1
      else
        echo "ok   $t"
      fi
    done < <("$manifest" paths train)
    # Say what was deliberately NOT tagged. The old arm skipped nothing and
    # demanded a tag for `benchmarks` and `hack/tools` as well, so its
    # `internal: true` flag was read by nothing at all.
    printf 'train %s at %s: %d module(s) required a tag; %d internal and %d example module(s) are not tagged by design\n' \
      "$tag" "$rc" "$want" "$("$manifest" paths internal | wc -l)" "$("$manifest" paths example | wc -l)"
    exit $status ;;
  */v*)
    mod="${tag%/v*}"
    ver="${tag##*/}"
    gomod="$(git show "$rc:$mod/go.mod" 2>/dev/null)" || { echo "no module at $mod in $tag's commit $rc" >&2; exit 1; }
    case "$("$manifest" release "$mod")" in
      train) ;;
      *) echo "$mod is not a \`release: train\` module: it is never published" >&2; exit 1 ;;
    esac
    v="$("$manifest" version "$mod")"
    [ "$v" = "$ver" ] || { echo "release/manifest.yaml gives $mod version $v, not $ver: record the patch there" >&2; exit 1; }
    # An intra-repo replace makes a published module unbuildable from the
    # proxy. Report every one of them rather than the first: the old form
    # grepped for one prefix and said "still has a replace directive", which
    # for drivers/rabbitmq/amqp10 is true of two different directives and the
    # message named neither.
    left="$(printf '%s\n' "$gomod" | intra_replaces)"
    if [ -n "$left" ]; then
      echo "$mod still replaces modules inside this repository:" >&2
      printf '%s\n' "$left" >&2
      echo "remove intra-repository replace directives before publishing" >&2
      exit 1
    fi
    echo "ok   $tag (independent patch at $rc)" ;;
esac
