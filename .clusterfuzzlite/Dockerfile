# Pinned by digest (the image of 2026-10-12): build.sh leans on this image's
# go-118-fuzz-build_v2 and its stand-in for package testing, so a new image is
# taken on purpose, with build.sh run in it, not whenever upstream pushes one.
FROM gcr.io/oss-fuzz-base/base-builder-go@sha256:1dbadefbb28c6bfd796c64d9c2115977deb8f19494533cc5a3b724c3441df23d
COPY . $SRC/iqmq
WORKDIR $SRC/iqmq
COPY .clusterfuzzlite/build.sh $SRC/
