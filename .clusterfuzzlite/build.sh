#!/bin/bash -eu
# Build the root module's native Go fuzz targets for ClusterFuzzLite.
#
# oss-fuzz builds a Go fuzz target with go-118-fuzz-build_v2, which links it
# into a libFuzzer binary by building the target's package as ordinary code
# against a stand-in for package testing. The image's default builder
# (compile_native_go_fuzzer) built 11 of the 117 targets here: it compiles only
# the target's own file, so helpers in other _test.go files, testing.TB and
# external test packages were all out of reach. Three things make every
# target build:
#
#  1. go.mod's toolchain is fetched outside GOMODCACHE. The builder overlays
#     GOROOT/src/testing, and go refuses an overlay of a file beneath
#     GOMODCACHE, which is where GOTOOLCHAIN=auto puts a toolchain newer than
#     the image's.
#  2. The stand-in's testing.F gets the testing.TB methods it lacks
#     (ArtifactDir, Attr, Output), since most seed helpers here take a TB.
#  3. Each package is built from a scratch copy of the module in which the
#     package's _test.go files are ordinary files (as `go test` compiles them
#     into the package) and its external test package (package x_test) is
#     moved to ./ossfuzzxtest, which imports the package as `go test` would.
#     One package per copy: every package's test files at once can import
#     each other in a cycle `go test` never builds.
#
# Steps 1 and 2 rest on details of the image's builder, which is why the
# Dockerfile pins the image by digest.
#
# The stand-in also runs a Cleanup at once and panics in T.Failed, so a
# target whose code relies on either crashes on its first input under
# libFuzzer. skip.txt lists those, with the reason; fuzz.yml runs every
# target natively every week.
cd "$SRC/iqmq"
export GOWORK=off
goroot="$(GOMODCACHE=/root/toolchain go env GOROOT)"
export PATH="$goroot/bin:$PATH" GOTOOLCHAIN=local
go version

tmp=/tmp/ossfuzz
mkdir -p "$tmp"
cat > "$tmp/tb.go" <<'EOF'
package testing

import "io"

func (f *F) ArtifactDir() string    { return f.TempDir() }
func (f *F) Attr(key, value string) {}
func (f *F) Output() io.Writer      { return io.Discard }
EOF
printf '{"Replace":{"%s":"%s"}}\n' "$goroot/src/testing/ossfuzz_tb.go" "$tmp/tb.go" > "$tmp/overlay.json"

./.clusterfuzzlite/list.sh > "$tmp/all.txt"
grep -v -E '^[[:space:]]*(#|$)' .clusterfuzzlite/skip.txt | awk '{print $1, $2}' > "$tmp/skip.txt" || true
# A skip entry that names no target is stale, and would hide the next target
# given its name.
if stale="$(grep -v -x -F -f "$tmp/all.txt" "$tmp/skip.txt")"; then
  echo "build.sh: .clusterfuzzlite/skip.txt names targets that do not exist: $stale" >&2
  exit 1
fi
grep -v -x -F -f "$tmp/skip.txt" "$tmp/all.txt" > "$tmp/targets.txt" || true
if [ ! -s "$tmp/targets.txt" ]; then
  echo "build.sh: no fuzz target to build" >&2
  exit 1
fi

tags=gofuzz,gofuzz_libfuzzer,libfuzzer
work="$tmp/work"
for pkg in $(cut -d' ' -f1 "$tmp/targets.txt" | sort -u); do
  rm -rf "$work"
  mkdir -p "$work"
  (cd "$SRC/iqmq" && tar --exclude=./.git -cf - .) | (cd "$work" && tar -xf -)
  cd "$work"
  dir="$(go list -tags "$tags" -f '{{.Dir}}' "$pkg")"
  for f in $(go list -tags "$tags" -f '{{join .TestGoFiles " "}}' "$pkg"); do
    mv "$dir/$f" "$dir/${f%_test.go}_ossfuzz.go"
  done
  xtest="$(go list -tags "$tags" -f '{{join .XTestGoFiles " "}}' "$pkg")"
  if [ -n "$xtest" ]; then
    mkdir "$dir/ossfuzzxtest"
    for f in $xtest; do
      mv "$dir/$f" "$dir/ossfuzzxtest/${f%_test.go}_ossfuzz.go"
    done
  fi
  while read -r p target; do
    [ "$p" = "$pkg" ] || continue
    from="$dir"
    if [ -n "$xtest" ] && grep -q -E "^func $target\(" "$dir"/ossfuzzxtest/*.go; then
      from="$dir/ossfuzzxtest"
    fi
    fuzzer="$(echo "$pkg" | tr '/' '_')_$target"
    go-118-fuzz-build_v2 -tags gofuzz -overlay "$tmp/overlay.json" -o "$tmp/$fuzzer.a" -func "$target" "$from"
    $CXX $CXXFLAGS $LIB_FUZZING_ENGINE "$tmp/$fuzzer.a" -o "$OUT/$fuzzer"
    rm -f "$tmp/$fuzzer.a" "$tmp/$fuzzer.h"
  done < "$tmp/targets.txt"
  cd "$SRC/iqmq"
done
rm -rf "$work"
