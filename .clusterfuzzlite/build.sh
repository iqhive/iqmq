#!/bin/bash -eu
# Build every native Go fuzz target of the root module for ClusterFuzzLite.
cd "$SRC/iqmq"
export GOWORK=off
# compile_native_go_fuzzer rewrites each target onto
# github.com/AdamKorcz/go-118-fuzz-build/testing, which the stdlib-only root
# module does not require. It is added to this build container's copy of
# go.mod only: the committed module stays stdlib-only (make stdlib-check).
go get github.com/AdamKorcz/go-118-fuzz-build/testing
while read -r pkg target; do
  compile_native_go_fuzzer "$pkg" "$target" "$(echo "$pkg" | tr '/' '_')_$target"
done < <(./.clusterfuzzlite/list.sh)
