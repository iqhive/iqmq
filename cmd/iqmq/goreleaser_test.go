package main

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// The goreleaser configuration is validated three ways, and this file is the
// third because the first two cannot run in CI.
//
// `goreleaser check` and a `--snapshot --clean` build were run locally, and
// their output is in the increment's hand-off. Neither is repeatable here:
// goreleaser is not in hack/tools, the base runner does not carry it, and
// adding it as a Go tool would put its whole dependency tree into a module
// `make license` scans. So `make ci` gets this instead -- the file held to the
// CONTRACT the workflow imposes on it, which is what actually breaks when
// somebody edits one of the two and not the other.
//
// It is a text check rather than a schema check on purpose. The question is
// not "is this valid YAML that goreleaser accepts" -- `goreleaser check`
// answers that, and it answered it -- but "does it still agree with
// release.yml", and the two disagree in ways a schema cannot see: a workdir,
// a path relative to it, an installed binary, a permission.

// configPath is the path release.yml's validate job tests for.
const configPath = ".goreleaser.yaml"

// configBody is the configuration with its comment lines removed.
//
// The comments explain the two workflow defects this file fixed, and they
// therefore MENTION the wrong forms -- "gomod.proxy: true would fetch the
// modules from a proxy that has never seen this repository". A check that
// grepped the whole file found its own explanation and failed. The settings
// are what the checks are about, so the settings are what they read.
func configBody(t *testing.T) string {
	t.Helper()
	var b strings.Builder
	for _, line := range strings.Split(readConfig(t), "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), "#") {
			continue
		}
		b.WriteString(line)
		b.WriteByte('\n')
	}
	return b.String()
}

// readConfig reads the configuration.
func readConfig(t *testing.T) string {
	t.Helper()
	data, err := os.ReadFile(configPath)
	if err != nil {
		t.Fatalf("reading %s: %v\nThe release workflow's CLI arm is gated on this exact path "+
			"existing, so losing it silently turns the CLI release off", configPath, err)
	}
	return string(data)
}

// workflowBody is release.yml with its comment lines removed, for the same
// reason configBody exists and found by the same mistake twice.
//
// The workflow's comments explain WHY the syft installer and the provenance
// attestation are there, so they contain the words "syft" and
// "attest-build-provenance". Deleting the installer step was watched leaving
// TestGoreleaserConfigMatchesTheWorkflow green, because the check found the
// comment that explains the step rather than the step. A check that is
// satisfied by its own justification is worth nothing.
func workflowBody(t *testing.T) string {
	t.Helper()
	var b strings.Builder
	for _, line := range strings.Split(readWorkflow(t), "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), "#") {
			continue
		}
		b.WriteString(line)
		b.WriteByte('\n')
	}
	return b.String()
}

// hasLine reports whether doc carries the exact line, trimmed. It is a line
// check and not a substring one because a top-level YAML key at the start of
// a file has no newline in front of it, which a "\nkey:\n" substring check
// gets wrong the moment the comment above it is stripped.
func hasLine(doc, want string) bool {
	for _, line := range strings.Split(doc, "\n") {
		if strings.TrimSpace(line) == want {
			return true
		}
	}
	return false
}

// readWorkflow reads .github/workflows/release.yml.
func readWorkflow(t *testing.T) string {
	t.Helper()
	path := filepath.Join(repoRoot(t), ".github", "workflows", "release.yml")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("reading %s: %v", path, err)
	}
	return string(data)
}

// TestGoreleaserConfigIsWhereTheWorkflowLooks: the gate is the literal path.
func TestGoreleaserConfigIsWhereTheWorkflowLooks(t *testing.T) {
	wf := readWorkflow(t)
	const gate = "cmd/iqmq/.goreleaser.yaml"
	if !strings.Contains(wf, gate) {
		t.Fatalf("release.yml no longer tests for %s, so the CLI arm's switch has moved and "+
			"this file may no longer turn it on", gate)
	}
	if _, err := os.Stat(configPath); err != nil {
		t.Fatalf("%s is missing: %v", configPath, err)
	}
}

// TestGoreleaserConfigMatchesTheWorkflow is the contract.
func TestGoreleaserConfigMatchesTheWorkflow(t *testing.T) {
	cfg := configBody(t)
	wf := workflowBody(t)

	t.Run("schema version", func(t *testing.T) {
		// The action pins `version: "~> v2"`, so the file has to be a v2 one.
		if !strings.Contains(wf, `version: "~> v2"`) {
			t.Error("release.yml no longer pins goreleaser v2")
		}
		if !hasLine(cfg, "version: 2") {
			t.Error("the configuration does not declare `version: 2`, which the pinned " +
				"goreleaser major requires")
		}
	})

	t.Run("workdir and the paths relative to it", func(t *testing.T) {
		if !strings.Contains(wf, "workdir: cmd/iqmq") {
			t.Fatal("release.yml no longer enters cmd/iqmq, so every ../../ path in the " +
				"configuration now points somewhere else")
		}
		// The release's extra files reach two levels up, which release.extra_files
		// allows.
		if !strings.Contains(cfg, "../../dist/reports/") {
			t.Error("the configuration does not attach ../../dist/reports/")
		}
		// The ARCHIVE's files cannot: an archive glob is resolved against the
		// working directory and one with ".." in it is rejected, which a
		// --snapshot build found. They are copied into .archive/ by a before
		// hook instead, so the config must carry both halves or the archives
		// come out with no LICENSE in them.
		if strings.Contains(cfg, "src: ../../") {
			t.Error("an archive file reaches outside the workdir with \"..\"; goreleaser " +
				"rejects that glob and the release fails at the archive step")
		}
		// The third-party half: the binary links every driver's client
		// library, whose licences ask a binary distribution to carry them,
		// and the archives held only this repository's LICENSE and NOTICE.
		for _, want := range []string{
			"mkdir -p .archive", "cp ../../LICENSE",
			"src: .archive/LICENSE", "src: .archive/NOTICE",
			"go-licenses save . ", "--save_path=.archive/third_party", "src: .archive/third_party/**/*",
		} {
			if !strings.Contains(cfg, want) {
				t.Errorf("the configuration is missing %q, so the archives would ship "+
					"without their licence files", want)
			}
		}
		ignore, err := os.ReadFile(filepath.Join(repoRoot(t), ".gitignore"))
		if err != nil {
			t.Fatalf("reading .gitignore: %v", err)
		}
		if !strings.Contains(string(ignore), "/cmd/iqmq/.archive/") {
			t.Error(".gitignore does not cover /cmd/iqmq/.archive/, which the before hook " +
				"writes into; goreleaser refuses to release from a dirty tree")
		}
	})

	t.Run("the release notes path is gitignored", func(t *testing.T) {
		// goreleaser refuses to release from a dirty tree, and the workflow
		// creates the notes itself. This is the defect that would have failed
		// the CLI arm on its first ever run.
		if !strings.Contains(wf, "dist/release-notes.md") {
			t.Error("release.yml no longer writes the notes under dist/; an untracked " +
				"release-notes.md at the repository root fails goreleaser's dirty-tree check")
		}
		if strings.Contains(wf, "> release-notes.md") {
			t.Error("release.yml writes release-notes.md to the repository root, which " +
				".gitignore does not cover")
		}
		ignore, err := os.ReadFile(filepath.Join(repoRoot(t), ".gitignore"))
		if err != nil {
			t.Fatalf("reading .gitignore: %v", err)
		}
		for _, want := range []string{"/dist/", "/cmd/iqmq/dist/"} {
			if !strings.Contains(string(ignore), want) {
				t.Errorf(".gitignore does not cover %s; goreleaser writes there and a "+
					"`git add -A` would take it", want)
			}
		}
	})

	t.Run("cosign keyless", func(t *testing.T) {
		if !strings.Contains(wf, "sigstore/cosign-installer@") {
			t.Fatal("release.yml no longer installs cosign, so the signs block cannot run")
		}
		if !strings.Contains(wf, "id-token: write") {
			t.Error("the release job has no id-token: write, which is what keyless signing's " +
				"OIDC identity comes from")
		}
		if !strings.Contains(cfg, "cmd: cosign") {
			t.Error("the configuration signs nothing with cosign")
		}
		for _, want := range []string{"sign-blob", "--yes", "artifacts: checksum"} {
			if !strings.Contains(cfg, want) {
				t.Errorf("the cosign invocation is missing %q", want)
			}
		}
		// A private key anywhere would mean it is not keyless.
		for _, forbidden := range []string{"--key", "COSIGN_PRIVATE_KEY", "cosign.key"} {
			if strings.Contains(cfg, forbidden) {
				t.Errorf("the configuration references %q, so the signing is not keyless", forbidden)
			}
		}
	})

	t.Run("SBOM has a generator installed", func(t *testing.T) {
		if !strings.Contains(cfg, "sboms:") {
			t.Fatal("the configuration declares no SBOM, and the specification asks for one per CLI binary")
		}
		// goreleaser's default SBOM command is syft, which the base runner
		// does not carry. A declared SBOM with no installed generator fails
		// the release.
		if !strings.Contains(wf, "syft") {
			t.Error("the configuration declares an SBOM and release.yml installs no syft, " +
				"so the release would fail at the SBOM step")
		}
	})

	t.Run("SLSA provenance is attested somewhere", func(t *testing.T) {
		// goreleaser's open-source distribution has no provenance feature, so
		// this half is the workflow's.
		if !strings.Contains(wf, "attest-build-provenance") {
			t.Error("nothing attests build provenance; the specification asks for SBOM AND SLSA provenance " +
				"for CLI binaries, and goreleaser produces only the first")
		}
		if !strings.Contains(wf, "attestations: write") {
			t.Error("the release job cannot write an attestation")
		}
		if !strings.Contains(wf, "cmd/iqmq/dist/") {
			t.Error("the attestation names no artefact under the workdir's dist tree")
		}
	})

	t.Run("the reports are still attached", func(t *testing.T) {
		// The `gh release create` fallback attached dist/reports/* and is
		// SKIPPED when cli=true, so the configuration has to re-attach them
		// or a v* release silently loses the artefacts the specification promises.
		if !strings.Contains(wf, "needs.validate.outputs.cli != 'true'") {
			t.Fatal("release.yml's fallback is no longer gated the way this check assumes")
		}
		if !strings.Contains(cfg, "extra_files:") {
			t.Error("the configuration attaches no extra files, so a v* tag's release carries " +
				"no fuzz manifest and no benchmark results -- the fallback step that used to " +
				"attach them is skipped when this file exists")
		}
	})

	t.Run("the build does not proxy the module", func(t *testing.T) {
		// cmd/iqmq reaches the root module and fifteen drivers through
		// replace directives. A proxied build would fetch modules a proxy has
		// never seen.
		if strings.Contains(cfg, "proxy: true") {
			t.Error("the configuration proxies the module, which cannot work: cmd/iqmq's " +
				"dependencies are replace directives into this repository")
		}
		if !strings.Contains(cfg, "GOWORK=off") {
			t.Error("the build does not set GOWORK=off, which every go command in this " +
				"repository runs with")
		}
	})

	t.Run("the version stamps match the variables", func(t *testing.T) {
		// A stamp naming a variable that does not exist is silently ignored
		// by the linker, and the binary then prints devel-<sha> on a release.
		for _, name := range []string{"buildVersion", "buildCommit", "buildDate"} {
			if !strings.Contains(cfg, "-X main."+name+"=") {
				t.Errorf("the configuration does not stamp main.%s", name)
			}
		}
		src, err := os.ReadFile("main.go")
		if err != nil {
			t.Fatalf("reading main.go: %v", err)
		}
		// gofumpt aligns a var block, so the declaration is `buildCommit  = ""`
		// with the spacing the longest name in the group decides. A check on
		// one space would break the next time a name in that block changes
		// length, which is a check that fails for the wrong reason.
		decl := regexp.MustCompile(`(?m)^\s*(buildVersion|buildCommit|buildDate)\s+= ""$`)
		declared := map[string]bool{}
		for _, m := range decl.FindAllStringSubmatch(string(src), -1) {
			declared[m[1]] = true
		}
		for _, name := range []string{"buildVersion", "buildCommit", "buildDate"} {
			if !declared[name] {
				t.Errorf("main.go declares no %s variable for the linker to stamp", name)
			}
		}
	})

	t.Run("every platform in the matrix was built", func(t *testing.T) {
		// The CLI links every shipped driver, so it inherits every driver's
		// platform support. A platform is added to the matrix after a
		// --snapshot build has produced it, and this is the note that says so.
		if !strings.Contains(cfg, "goos:") || !strings.Contains(cfg, "goarch:") {
			t.Error("the build declares no platform matrix at all")
		}
		// Every platform here was watched building. freebsd is absent
		// because github.com/AthenZ/athenz -- which the Pulsar client pulls
		// in -- does not compile for it, and a CLI that links every driver
		// cannot ship for a platform any one of them refuses.
		for _, want := range []string{"- linux", "- darwin", "- windows", "- amd64", "- arm64"} {
			if !strings.Contains(cfg, want) {
				t.Errorf("the build matrix is missing %q", want)
			}
		}
		if strings.Contains(cfg, "- freebsd") {
			t.Error("freebsd is in the matrix and the CLI does not compile for it: " +
				"github.com/AthenZ/athenz, a Pulsar client dependency, fails with " +
				"\"undefined: UpdateFile\"")
		}
		// A Windows user's shell opens a zip without a third-party tool.
		if !strings.Contains(cfg, "format_overrides:") {
			t.Error("windows is in the matrix and no format override gives it a zip")
		}
	})
}

// TestGoreleaserConfigIsTwoSpaceIndentedYAML is the repository's own file
// convention, which .editorconfig states and nothing else checks for a file
// outside .github.
func TestGoreleaserConfigIsTwoSpaceIndentedYAML(t *testing.T) {
	cfg := readConfig(t)
	if strings.Contains(cfg, "\t") {
		t.Error("the configuration contains a tab; YAML indentation is spaces")
	}
	if !strings.HasSuffix(cfg, "\n") {
		t.Error("the configuration does not end with a newline")
	}
	if strings.Contains(cfg, "\r\n") {
		t.Error("the configuration has CRLF line endings")
	}
}
