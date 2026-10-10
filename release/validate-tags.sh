#!/usr/bin/env bash
# Validate a release tag against release/manifest.yaml:
#   release/validate-tags.sh vX.Y.Z                 (release train: every `release: train` module must be tagged)
#   release/validate-tags.sh drivers/kafka/vX.Y.Z   (independent patch)
#
# The manifest lists the modules included in a release train.
set -euo pipefail
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
manifest="$root/release/manifest.sh"
tag="${1:?tag}"
cd "$root"
case "$tag" in
  v*)
    train="$("$manifest" train)"
    [ "$train" = "$tag" ] || { echo "manifest train is $train, tag is $tag" >&2; exit 1; }
    status=0 want=0
    while read -r path; do
      [ -n "$path" ] || continue
      want=$((want + 1))
      [ "$path" = "." ] && t="$tag" || t="$path/$tag"
      if git rev-parse -q --verify "refs/tags/$t" >/dev/null; then echo "ok   $t"; else echo "MISSING $t" >&2; status=1; fi
    done < <("$manifest" paths train)
    # Say what was deliberately NOT tagged. The old arm skipped nothing and
    # demanded a tag for `benchmarks` and `hack/tools` as well, so its
    # `internal: true` flag was read by nothing at all.
    printf 'train %s: %d module(s) required a tag; %d internal and %d example module(s) are not tagged by design\n' \
      "$tag" "$want" "$("$manifest" paths internal | wc -l)" "$("$manifest" paths example | wc -l)"
    exit $status ;;
  */v*)
    mod="${tag%/v*}"
    [ -f "$mod/go.mod" ] || { echo "no module at $mod" >&2; exit 1; }
    case "$("$manifest" release "$mod")" in
      train) ;;
      *) echo "$mod is not a \`release: train\` module: it is never published" >&2; exit 1 ;;
    esac
    # An intra-repo replace makes a published module unbuildable from the
    # proxy. Report every one of them rather than the first: the old form
    # grepped for one prefix and said "still has a replace directive", which
    # for drivers/rabbitmq/amqp10 is true of two different directives and the
    # message named neither.
    left="$(grep -nE '^[[:space:]]*(replace[[:space:]]+)?github\.com/iqhive/iqmq(/[^[:space:]]*)?[[:space:]]+=>' "$mod/go.mod" || true)"
    if [ -n "$left" ]; then
      echo "$mod still replaces modules inside this repository:" >&2
      printf '%s\n' "$left" >&2
      echo "remove intra-repository replace directives before publishing" >&2
      exit 1
    fi
    echo "ok   $tag (independent patch)" ;;
  *) echo "unrecognised tag $tag" >&2; exit 1 ;;
esac
