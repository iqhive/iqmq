#!/bin/bash -eu
# Build every native Go fuzz target of the root module for ClusterFuzzLite.
cd "$SRC/iqmq"
export GOWORK=off
while read -r pkg target; do
  compile_native_go_fuzzer "$pkg" "$target" "$(echo "$pkg" | tr '/' '_')_$target"
done < <(./.clusterfuzzlite/list.sh)
