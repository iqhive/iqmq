package harness

import (
	_ "embed"
	"fmt"
	"os"
	"strings"
	"testing"
)

// Images pins the container image for every broker the harness can start.
// hack/images-lock.sh parses this map (one `"name": "image"` entry per line)
// and writes images.lock with resolved digests; keep the formatting.
//
// The TAG here is the version a broker's report records and the reference the
// lock's digest was resolved from; it is NOT what the harness starts. [Image]
// starts the digest images.lock pins for the same name, and the script records
// the digest whose repository is the one named here ,
// so this map and the lock have to name the same repository for every entry --
// TestTheLockPinsEveryImage is what holds them together.
//
// MinIO is quay.io's image and not Docker Hub's: minio/minio no longer
// resolves on Docker Hub ("pull access denied ... repository does not exist"),
// and x/claimcheck/s3store's MinIO arm runs against this release.
//
// LocalStack is pinned to the last 4.x release: from 2026.x the image exits
// with code 55 unless LOCALSTACK_AUTH_TOKEN is set. Users with a token can
// select a newer image with IQMQ_LOCALSTACK_IMAGE; the harness forwards the
// token when present.
var Images = map[string]string{
	"redpanda":         "redpandadata/redpanda:v26.2.2",
	"kafka":            "apache/kafka:4.3.1",
	"nats":             "nats:2.14.6-alpine",
	"nsq":              "nsqio/nsq:v1.3.0",
	"pubsub":           "gcr.io/google.com/cloudsdktool/google-cloud-cli:emulators",
	"elasticmq":        "softwaremill/elasticmq-native:1.7.1",
	"localstack":       "localstack/localstack:4.14.0",
	"rabbitmq":         "rabbitmq:4.3.5-management-alpine",
	"artemis":          "apache/activemq-artemis:2.44.0-alpine",
	"activemq-classic": "apache/activemq-classic:6.2.0",
	"redis":            "redis:7.4-alpine",
	"valkey":           "valkey/valkey:8.1.10-alpine",
	"pulsar":           "apachepulsar/pulsar:4.2.4",
	"postgres":         "postgres:17-alpine",
	"toxiproxy":        "ghcr.io/shopify/toxiproxy:2.12.0",
	"minio":            "quay.io/minio/minio:RELEASE.2025-09-07T16-13-09Z",
}

// RabbitMQDelayedRepo and RabbitMQDelayedTag name the image built from
// hack/images/rabbitmq-delayed/Dockerfile (RabbitMQ plus the delayed-message
// exchange plugin). It is built on demand, never pulled.
const (
	RabbitMQDelayedRepo = "iqmq/rabbitmq-delayed"
	RabbitMQDelayedTag  = "4.2.9-plugin4.2.0"
)

// imagesLock is images.lock, which hack/images-lock.sh writes: one
// `name repository@sha256:...` line per entry of [Images], after a comment
// header. It is embedded because [Image] RESOLVES THROUGH IT -- until it did,
// the lock was a record nothing read at run time, so a suite started whatever
// the declared tag resolved to that day and the file pinned nothing.
//
//go:embed images.lock
var imagesLock string

// lockedImages maps an images.lock name to the `repository@sha256:...` it pins.
var lockedImages = parseLock(imagesLock)

// parseLock reads images.lock's `name repository@digest` lines and ignores its
// comment header. A line that is not two fields is SKIPPED rather than
// diagnosed here -- a package initialiser has nobody to report to -- which
// turns a malformed line into a missing name, and a missing name is what
// [pinnedRef] refuses loudly. TestTheLockPinsEveryImage is what holds the whole
// file to [Images].
func parseLock(s string) map[string]string {
	m := map[string]string{}
	for line := range strings.SplitSeq(s, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if f := strings.Fields(line); len(f) == 2 {
			m[f[0]] = f[1]
		}
	}
	return m
}

// repository is the repository part of an image reference: the digest and then
// the tag are dropped, where the tag's colon is the last one AFTER the last
// slash so a registry port is not read as a tag.
func repository(ref string) string {
	if i := strings.IndexByte(ref, '@'); i >= 0 {
		ref = ref[:i]
	}
	slash := strings.LastIndexByte(ref, '/')
	if colon := strings.LastIndexByte(ref, ':'); colon > slash {
		return ref[:colon]
	}
	return ref
}

// pinnedRef is the reference to start for name: the `repository@sha256:...`
// images.lock pins, held against the repository declared names.
//
// It returns an error rather than the declared tag when the lock has no line
// for name or pins another repository. Falling back to the tag is the defect
// itself -- it pins nothing and says nothing -- and the repositories are
// compared as whole strings because `minio/minio` is a SUFFIX of
// `quay.io/minio/minio`, so a substring test accepts either.
func pinnedRef(name, declared string, lock map[string]string) (string, error) {
	locked, ok := lock[name]
	if !ok {
		return "", fmt.Errorf("harness: %s is in Images but images.lock pins no such name; run `make images-lock`", name)
	}
	repo, digest, ok := strings.Cut(locked, "@")
	if !ok || !strings.HasPrefix(digest, "sha256:") {
		return "", fmt.Errorf("harness: images.lock pins %s as %q, which is not repository@sha256:...; run `make images-lock`", name, locked)
	}
	if want := repository(declared); repo != want {
		return "", fmt.Errorf("harness: images.go declares %s as %s and images.lock pins repository %s; run `make images-lock`", name, declared, repo)
	}
	return locked, nil
}

// Image returns the image reference for name, honouring IQMQ_<NAME>_IMAGE.
//
// Without an override it is the DIGEST images.lock pins, not the tag [Images]
// declares: a suite that starts the tag runs whatever the tag resolves to that
// day. A name [Images] does not know still returns "", but a
// name it does know whose lock line is missing or names another repository
// FAILS -- starting the floating tag instead is the thing this resolution
// exists to stop, and doing it silently is worse than not doing it at all.
func Image(name string) (string, error) {
	if v := os.Getenv(envKey(name, "IMAGE")); v != "" {
		return v, nil
	}
	declared := Images[name]
	if declared == "" {
		return "", nil
	}
	ref, err := pinnedRef(name, declared, lockedImages)
	if err != nil {
		return "", fmt.Errorf("harness: image %s: %w", name, err)
	}
	return ref, nil
}

// imageRef is [Image] for a broker starter, which fails tb rather than start
// an image it could not resolve.
func imageRef(tb testing.TB, name string) string {
	tb.Helper()
	ref, err := Image(name)
	if err != nil {
		tb.Fatal(err)
	}
	return ref
}

// brokerVersion is the version to report for a container started from image
// under the harness name.
//
// [Image] resolves through images.lock and a digest reference carries no tag at
// all, so [Version] would answer "latest" for every pinned broker; the tag is
// read from the DECLARED reference instead. A reference that has a tag of its
// own -- an IQMQ_<NAME>_IMAGE override, or the locally built
// rabbitmq-delayed image -- is the authority, and a digest-only override gets
// "latest" rather than the declared tag, because an override names its own
// image and the declared tag is not its version.
func brokerVersion(name, image string) string {
	if v := Version(image); v != "latest" {
		return v
	}
	if os.Getenv(envKey(name, "IMAGE")) != "" {
		return "latest"
	}
	return Version(Images[name])
}

// Version extracts the version from an image reference: everything after the
// last ":" that is not part of a registry port, minus any leading "v".
func Version(image string) string {
	if i := strings.LastIndexByte(image, '@'); i >= 0 {
		image = image[:i]
	}
	slash := strings.LastIndexByte(image, '/')
	colon := strings.LastIndexByte(image, ':')
	if colon < 0 || colon < slash {
		return "latest"
	}
	return strings.TrimPrefix(image[colon+1:], "v")
}

// envKey builds IQMQ_<NAME>_<SUFFIX> from a broker name.
func envKey(name, suffix string) string {
	return "IQMQ_" + strings.ToUpper(strings.ReplaceAll(name, "-", "_")) + "_" + suffix
}
