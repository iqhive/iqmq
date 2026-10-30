package harness

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestRepository(t *testing.T) {
	cases := map[string]string{
		"redis:7.4-alpine": "redis",
		"quay.io/minio/minio:RELEASE.2025-09-07T16-13-09Z": "quay.io/minio/minio",
		"minio/minio@sha256:14cea":                         "minio/minio",
		"quay.io/minio/minio@sha256:14cea":                 "quay.io/minio/minio",
		"localhost:5000/foo/bar":                           "localhost:5000/foo/bar",
		"localhost:5000/foo/bar:1.2":                       "localhost:5000/foo/bar",
		"nats":                                             "nats",
		"redis:7@sha256:abcdef":                            "redis",
	}
	for ref, want := range cases {
		if got := repository(ref); got != want {
			t.Errorf("repository(%q) = %q, want %q", ref, got, want)
		}
	}
}

// TestPinnedRefRefusesTheWrongRepository is the resolution half of the two-name
// defect. `{{index .RepoDigests 0}}` gave MinIO's DOCKER HUB name because
// Docker reports it first, so a lock line can name a repository images.go does
// not -- and the answer to that is a refusal, never the declared tag, because a
// harness that quietly starts the tag pins nothing and reports nothing.
func TestPinnedRefRefusesTheWrongRepository(t *testing.T) {
	const (
		declaredMinIO = "quay.io/minio/minio:RELEASE.2025-09-07T16-13-09Z"
		quay          = "quay.io/minio/minio@sha256:14cea493"
	)
	lock := map[string]string{
		"minio":    quay,
		"hub":      "minio/minio@sha256:14cea493",
		"notag":    "redis:7.4-alpine",
		"nosha":    "redis@md5:14cea493",
		"scheme":   "sha256:14cea493",
		"unknown":  "redis@sha256:ff02",
		"oldtag":   "nats@sha256:ad7a",
		"wrongreg": "nats@sha256:ad7a",
	}
	from := map[string]string{
		"minio":    declaredMinIO,
		"hub":      declaredMinIO,
		"oldtag":   "nats:2.14.6-alpine",
		"wrongreg": "docker.io/library/nats:2.14.6-alpine",
	}
	if got, err := pinnedRef("minio", declaredMinIO, lock, from); err != nil || got != quay {
		t.Fatalf("pinnedRef(minio) = %q, %v; want %q, nil", got, err, quay)
	}
	for _, c := range []struct{ what, name, declared, want string }{
		{"a name the lock does not pin", "absent", "redis:7.4-alpine", "pins no such name"},
		{"the other repository of a two-name image", "hub", declaredMinIO, "pins repository minio/minio"},
		{"a lock line with no digest at all", "notag", "redis:7.4-alpine", "not repository@sha256"},
		{"a lock line whose digest is not sha256", "nosha", "redis:7.4-alpine", "not repository@sha256"},
		{"a lock line that is only a digest", "scheme", "redis:7.4-alpine", "not repository@sha256"},
		{"a pin with no record of what it was resolved from", "unknown", "redis:7.4-alpine", "does not record the reference"},
		{"a pin resolved from an older tag", "oldtag", "nats:2.99.0-alpine", "was resolved from nats:2.14.6-alpine"},
		{"a pin resolved from another spelling of the reference", "wrongreg", "nats:2.14.6-alpine", "was resolved from docker.io/library/nats"},
	} {
		got, err := pinnedRef(c.name, c.declared, lock, from)
		if err == nil {
			t.Errorf("%s: pinnedRef accepted it and returned %q", c.what, got)
			continue
		}
		if !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: the refusal is %q, want it to mention %q", c.what, err, c.want)
		}
	}
}

// TestTheLockPinsEveryImage holds images.lock to [Images] in BOTH directions.
// It is what makes [Image]'s panic unreachable on a tree where this passes, and
// it is the detector for a lock that has gone stale against a repository rename
// or a tag bump: each pin's `# from` line records the reference its digest was
// resolved from, and [pinnedRef] holds that to the entry in [Images].
func TestTheLockPinsEveryImage(t *testing.T) {
	if len(lockedImages) == 0 {
		t.Fatal("images.lock parsed to nothing, so every check below would pass by finding nothing")
	}
	for name, declared := range Images {
		ref, err := pinnedRef(name, declared, lockedImages, lockedFrom)
		if err != nil {
			t.Errorf("%s: %v", name, err)
			continue
		}
		if want := repository(declared) + "@sha256:"; !strings.HasPrefix(ref, want) {
			t.Errorf("%s: the lock pins %q, want %s...", name, ref, want)
		}
	}
	for name := range lockedImages {
		if Images[name] == "" {
			t.Errorf("images.lock pins %q, which Images does not declare", name)
		}
	}
	for name := range lockedFrom {
		if lockedImages[name] == "" {
			t.Errorf("images.lock records what %q was resolved from and pins no digest for it", name)
		}
	}
}

// TestABumpedTagIsRefusedUntilTheLockIsRegenerated is the stale-tag half of the
// lock. A tag bumped in [Images] without `make images-lock` leaves the lock
// pinning the OLD tag's digest under the same repository, so a check of the
// repository alone starts the old broker while the report records the new
// version. The bump is made to the map itself, the way an edit to images.go
// makes it, and undone when the test ends.
func TestABumpedTagIsRefusedUntilTheLockIsRegenerated(t *testing.T) {
	declared := Images["nats"]
	bumped := repository(declared) + ":2.99.0-alpine"
	Images["nats"] = bumped
	t.Cleanup(func() { Images["nats"] = declared })

	ref, err := Image("nats")
	if err == nil {
		t.Fatalf("Image(nats) with images.go at %s and images.lock as it was = %q, nil: the old tag's digest would run as %s",
			bumped, ref, Version(bumped))
	}
	for _, want := range []string{bumped, declared, "make images-lock"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the refusal does not mention %q: %v", want, err)
		}
	}
}

// TestImageResolvesThroughTheLock is the run-time half: what a suite starts is
// the digest the lock pins, not the tag the map declares.
func TestImageResolvesThroughTheLock(t *testing.T) {
	for name, declared := range Images {
		got := imageRef(t, name)
		if got == declared {
			t.Errorf("%s: Image returned the declared tag %q", name, declared)
		}
		if got != lockedImages[name] {
			t.Errorf("%s: Image = %q, images.lock pins %q", name, got, lockedImages[name])
		}
	}
	// The dead Docker Hub name is `minio/minio`, which is a SUFFIX of the
	// quay.io one, so this asserts the repository up to the "@".
	if got := imageRef(t, "minio"); !strings.HasPrefix(got, "quay.io/minio/minio@sha256:") {
		t.Errorf("Image(minio) = %q, want quay.io/minio/minio@sha256:...", got)
	}
	if got := imageRef(t, "nosuchbroker"); got != "" {
		t.Errorf("Image of a name Images does not declare = %q, want the empty string", got)
	}
}

func TestBrokerVersion(t *testing.T) {
	if got := brokerVersion("redis", imageRef(t, "redis")); got != "7.4-alpine" {
		t.Errorf("brokerVersion of the pinned redis digest = %q, want 7.4-alpine", got)
	}
	built := RabbitMQDelayedRepo + ":" + RabbitMQDelayedTag
	if got := brokerVersion("rabbitmq-delayed", built); got != RabbitMQDelayedTag {
		t.Errorf("brokerVersion(%q) = %q, want %q", built, got, RabbitMQDelayedTag)
	}
	t.Run("an override names its own version", func(t *testing.T) {
		t.Setenv("IQMQ_REDIS_IMAGE", "redis:8")
		if got := brokerVersion("redis", imageRef(t, "redis")); got != "8" {
			t.Errorf("brokerVersion = %q, want 8", got)
		}
	})
	t.Run("a digest-only override has none", func(t *testing.T) {
		t.Setenv("IQMQ_REDIS_IMAGE", "redis@sha256:abcdef")
		if got := brokerVersion("redis", imageRef(t, "redis")); got != "latest" {
			t.Errorf("brokerVersion = %q, want latest: the declared tag is not an override's version", got)
		}
	})
}

// fakeDocker answers `docker image inspect` out of a table of
// `image repodigest...` lines named by $FAKE_DOCKER_TABLE, and refuses every
// other subcommand with exit 90 so a test that reaches for the daemon, a pull
// or a container fails instead of doing it.
//
// It answers `{{index .RepoDigests 0}}` with the first entry and a `range` over
// the list with all of them, so restoring the OLD selection into the script
// reproduces the defect exactly rather than approximately -- that mutation is
// the evidence this fixture exists to produce.
const fakeDocker = `#!/usr/bin/env bash
set -u
if [ "${1:-}" != "image" ] || [ "${2:-}" != "inspect" ]; then
  echo "fake docker: refusing: $*" >&2
  exit 90
fi
shift 2
format=""
if [ "${1:-}" = "--format" ]; then
  format="$2"
  shift 2
fi
image="${1:-}"
digests="$(awk -v want="$image" '$1 == want { $1 = ""; print; exit }' "$FAKE_DOCKER_TABLE")"
if [ -z "${digests// /}" ]; then
  echo "Error: No such image: $image" >&2
  exit 1
fi
case "$format" in
  "") exit 0 ;;
  *index*)
    set -- $digests
    printf '%s\n' "$1"
    ;;
  *json*)
    out=""
    for d in $digests; do out="$out,\"$d\""; done
    printf '[%s]\n' "${out#,}"
    ;;
  *)
    for d in $digests; do printf '%s\n' "$d"; done
    ;;
esac
`

// lockRun is what one run of hack/images-lock.sh over fixtures did.
type lockRun struct {
	out   string // stdout and stderr together
	err   error  // non-nil when the script exited non-zero
	lock  string // what it wrote, header line dropped
	wrote bool   // whether it wrote a lock at all
}

// runImagesLock runs the REAL hack/images-lock.sh over a throwaway tree: a copy
// of the script, a fixture images.go and a fake docker. The script derives its
// root from its own location, so the copy's root is the temporary tree and
// nothing in the repository is touched. No daemon, no pull, no container.
func runImagesLock(t *testing.T, imagesGo, table string) lockRun {
	t.Helper()
	body, err := os.ReadFile(filepath.Join("..", "..", "hack", "images-lock.sh"))
	if err != nil {
		t.Fatalf("the script under test cannot be read, so nothing below tests anything: %v", err)
	}
	root := t.TempDir()
	write := func(rel, content string, mode os.FileMode) string {
		p := filepath.Join(root, rel)
		if err := os.MkdirAll(filepath.Dir(p), 0o750); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(content), mode); err != nil {
			t.Fatal(err)
		}
		return p
	}
	script := write("hack/images-lock.sh", string(body), 0o700)
	write("conformance/harness/images.go", imagesGo, 0o600)
	write("bin/docker", fakeDocker, 0o700)
	tablePath := write("table", table, 0o600)

	cmd := exec.Command("bash", script)
	cmd.Env = append(os.Environ(),
		"PATH="+filepath.Join(root, "bin")+string(os.PathListSeparator)+os.Getenv("PATH"),
		"FAKE_DOCKER_TABLE="+tablePath,
		"PULL=0",
	)
	combined, runErr := cmd.CombinedOutput()
	r := lockRun{out: string(combined), err: runErr}
	written, readErr := os.ReadFile(filepath.Join(root, "conformance", "harness", "images.lock"))
	if readErr == nil {
		r.wrote = true
		if _, body, ok := strings.Cut(string(written), "\n"); ok {
			r.lock = body
		}
	}
	return r
}

// TestImagesLockSelectsTheDeclaredRepository is the generator half of the
// two-name defect, watched over fixture inspect output. MinIO's table row lists
// the Docker Hub name FIRST, which is the order this host reports and the whole
// reason `{{index .RepoDigests 0}}` wrote a reference nobody can pull.
//
// The last entry is a registry PORT, and it is two-named on the same registry
// on purpose. The port is why repository_of drops a tag only when the colon is
// after the last slash -- but that normalisation is symmetric, so a version of
// it that reads the port as a tag still matches for a one-named image and only
// goes wrong when it has to tell two repositories on one registry apart. This
// is the fixture that makes that branch fail when it is broken.
func TestImagesLockSelectsTheDeclaredRepository(t *testing.T) {
	r := runImagesLock(t, fixtureImagesGo(
		`"redis":            "redis:7.4-alpine"`,
		`"toxiproxy":        "ghcr.io/shopify/toxiproxy:2.12.0"`,
		`"minio":            "quay.io/minio/minio:RELEASE.2025-09-07T16-13-09Z"`,
		`"private":          "localhost:5000/team/broker:1.2"`,
	), strings.Join([]string{
		"redis:7.4-alpine redis@sha256:ff02",
		"ghcr.io/shopify/toxiproxy:2.12.0 ghcr.io/shopify/toxiproxy@sha256:9378",
		"quay.io/minio/minio:RELEASE.2025-09-07T16-13-09Z minio/minio@sha256:14cea quay.io/minio/minio@sha256:14cea",
		"localhost:5000/team/broker:1.2 localhost:5000/other/broker@sha256:dead localhost:5000/team/broker@sha256:dead",
		"",
	}, "\n"))
	if r.err != nil {
		t.Fatalf("the script failed: %v\n%s", r.err, r.out)
	}
	want := "# from redis redis:7.4-alpine\n" +
		"redis redis@sha256:ff02\n" +
		"# from toxiproxy ghcr.io/shopify/toxiproxy:2.12.0\n" +
		"toxiproxy ghcr.io/shopify/toxiproxy@sha256:9378\n" +
		"# from minio quay.io/minio/minio:RELEASE.2025-09-07T16-13-09Z\n" +
		"minio quay.io/minio/minio@sha256:14cea\n" +
		"# from private localhost:5000/team/broker:1.2\n" +
		"private localhost:5000/team/broker@sha256:dead\n"
	if r.lock != want {
		t.Errorf("the lock holds\n%s\nwant\n%s", r.lock, want)
	}
}

// TestImagesLockRefusesWhenNoRepoDigestMatches is the fail-loudly half. The
// image is present and HAS a repo digest; it is simply not one images.go names,
// which is the case the old `RepoDigests 0` read silently accepted.
func TestImagesLockRefusesWhenNoRepoDigestMatches(t *testing.T) {
	r := runImagesLock(t,
		fixtureImagesGo(`"minio":            "quay.io/minio/minio:RELEASE.2025-09-07T16-13-09Z"`),
		"quay.io/minio/minio:RELEASE.2025-09-07T16-13-09Z minio/minio@sha256:14cea\n")
	if r.err == nil {
		t.Fatalf("the script locked a repository images.go does not name and exited 0:\n%s%s", r.out, r.lock)
	}
	if r.wrote {
		t.Errorf("the script left a lock behind after refusing:\n%s", r.lock)
	}
	for _, want := range []string{"minio", "quay.io/minio/minio", "no repo digest"} {
		if !strings.Contains(r.out, want) {
			t.Errorf("the refusal does not mention %q:\n%s", want, r.out)
		}
	}
}

// TestImagesLockRefusesAnImagesGoItCannotParse is the other way this script can
// pass by finding nothing: the grep that reads the map finds no entry, and the
// old script wrote a lock of nothing but a header and exited 0.
func TestImagesLockRefusesAnImagesGoItCannotParse(t *testing.T) {
	r := runImagesLock(t,
		"package harness\n\nvar Images = map[string]string{\"redis\": \"redis:7.4-alpine\"}\n",
		"redis:7.4-alpine redis@sha256:ff02\n")
	if r.err == nil {
		t.Fatalf("the script wrote a lock out of an images.go it parsed nothing from:\n%s%s", r.out, r.lock)
	}
	if r.wrote {
		t.Errorf("the script left a lock behind after refusing:\n%s", r.lock)
	}
	if !strings.Contains(r.out, "images.go") {
		t.Errorf("the refusal does not name the file it could not parse:\n%s", r.out)
	}
}

// fixtureImagesGo builds an images.go the script's grep can read: the map's
// formatting -- one `"name": "image"` entry per indented line -- is the
// contract, so the fixture has to keep it too.
func fixtureImagesGo(entries ...string) string {
	var b strings.Builder
	b.WriteString("package harness\n\nvar Images = map[string]string{\n")
	for _, e := range entries {
		b.WriteString("\t" + e + ",\n")
	}
	b.WriteString("}\n")
	return b.String()
}
