package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestSelfCheck runs the tool's own fixture table, so that `make test-race`
// and `go test ./...` exercise the instrument as well as `make ci`'s explicit
// target does.
//
// Every check is run twice by selfCheck: once over a tree that breaks it,
// where it must report, and once over a tree that satisfies it, where it must
// not. A check nobody has watched fail is assumed not to work -- this
// repository has nine instances of one that could not -- and a check that
// always fails is exactly as useless, because it gets switched off.
//
// The first run of this fixture table found a defect in the table itself: the
// provenance check reported NOTHING on the failing fixture and the self-test
// passed, because the fixture's finding has the same subject as this
// repository's real waiver and the waiver silenced it. The self-test runs with
// waivers off for that reason.
func TestSelfCheck(t *testing.T) {
	if err := selfCheck(); err != nil {
		t.Error(err)
	}
}

// TestProvenanceSubjects pins what counts as an attested path: the lines of
// subject-path's block (or its inline value), and nothing else in the file --
// not a comment, not another step, not the next key.
func TestProvenanceSubjects(t *testing.T) {
	cases := []struct {
		name, body string
		want       []string
	}{
		{
			name: "block",
			body: "      - uses: actions/attest-build-provenance@v4\n        with:\n          subject-path: |\n" +
				"            cmd/iqmq/dist/*.tar.gz\n\n            # cmd/iqmq/dist/*.zip\n            cmd/iqmq/dist/*.zip\n" +
				"          show-summary: true\n      - run: ls dist/*.deb\n",
			want: []string{"cmd/iqmq/dist/*.tar.gz", "cmd/iqmq/dist/*.zip"},
		},
		{
			name: "inline",
			body: "        with:\n          subject-path: 'cmd/iqmq/dist/*.zip'\n",
			want: []string{"cmd/iqmq/dist/*.zip"},
		},
		{
			name: "commented out",
			body: "        with:\n          # subject-path: cmd/iqmq/dist/*.zip\n          subject-digest: sha256:00\n",
			want: nil,
		},
		{
			name: "named elsewhere only",
			body: "# the archives are dist/*.zip\n      - run: ls cmd/iqmq/dist/*.zip\n",
			want: nil,
		},
	}
	for _, tc := range cases {
		got := provenanceSubjects(tc.body)
		if strings.Join(got, "\n") != strings.Join(tc.want, "\n") {
			t.Errorf("%s: provenanceSubjects = %q, want %q", tc.name, got, tc.want)
		}
	}
}

// TestAWorkflowRunsOnlyTheCommandsItsRunStepsRun pins what counts as running
// `make vuln` or `make build`: a run step whose whole script is the command,
// inline or in a block, and nothing else in the file. The real codeql.yml and
// vuln.yml both name their command in a comment, and that comment used to
// satisfy the check with the step itself deleted.
//
// The rule is one shape rather than a list of bad ones: the script is the
// one line `make vuln` (or `make vuln || exit 1`), under the default shell, in
// the default directory, in a job that needs no other job. Each list of bad
// shapes this check kept was beaten by a form nobody had listed: a dry run, a
// pipe, a heredoc, an `exit 0` above it.
func TestAWorkflowRunsOnlyTheCommandsItsRunStepsRun(t *testing.T) {
	cases := []struct {
		name, body string
		want       bool
	}{
		{name: "inline", body: "    steps:\n      - run: make vuln\n", want: true},
		{name: "quoted", body: "    steps:\n      - run: \"make vuln\"\n", want: true},
		{name: "in a block", body: "      - run: |\n          # every module\n          make vuln\n", want: true},
		{name: "a block of more than the command", body: "      - run: |\n          set -e\n          GOWORK=off make -k vuln\n", want: false},
		{name: "after a separator", body: "      - run: cd x && make vuln\n", want: false},
		{name: "a run key under the step", body: "      - name: scan\n        run: make vuln\n", want: true},
		{name: "a comment", body: "# the scan is `make vuln`\n      - run: govulncheck ./...\n", want: false},
		{name: "a shell comment", body: "      - run: |\n          # make vuln\n          govulncheck ./...\n", want: false},
		{name: "a step name", body: "      - name: make vuln\n        run: govulncheck ./...\n", want: false},
		{name: "an argument", body: "      - run: echo make vuln\n", want: false},
		{name: "another target", body: "      - run: make vuln-report\n", want: false},
		{
			name: "a key after the block",
			body: "      - run: |\n          govulncheck ./...\n        env:\n          SCAN: make vuln\n",
			want: false,
		},
		{
			name: "the next step after the block",
			body: "      - run: |\n          govulncheck ./...\n      - name: make vuln\n        uses: x/y@v1\n",
			want: false,
		},

		// A step that may not run, or whose failure cannot fail the
		// workflow, does not count: vuln.yml passed with `make vuln` gated on
		// an event it never receives, and with its failure swallowed.
		{name: "a step if:", body: "      - if: github.event_name == 'release'\n        run: make vuln\n", want: false},
		{name: "a step if: after the run", body: "      - run: make vuln\n        if: always() && false\n", want: false},
		{name: "a step continue-on-error", body: "      - continue-on-error: true\n        run: make vuln\n", want: false},
		{name: "continue-on-error from an expression", body: "      - run: make vuln\n        continue-on-error: ${{ matrix.x }}\n", want: false},
		{name: "continue-on-error: false", body: "      - run: make vuln\n        continue-on-error: false\n", want: true},
		{
			name: "a job if:",
			body: "jobs:\n  scan:\n    if: github.event_name == 'release'\n    steps:\n      - run: make vuln\n",
			want: false,
		},
		{
			name: "a job if: after the steps",
			body: "jobs:\n  scan:\n    steps:\n      - run: make vuln\n      - run: echo done\n    if: github.event_name == 'release'\n",
			want: false,
		},
		{
			name: "a job continue-on-error",
			body: "jobs:\n  scan:\n    continue-on-error: true\n    steps:\n      - run: make vuln\n",
			want: false,
		},
		{
			name: "a condition this repository always meets",
			body: "jobs:\n  scan:\n    if: ${{ !github.event.repository.private }}\n    steps:\n      - run: make vuln\n",
			want: true,
		},
		{
			name: "another job's if:",
			body: "jobs:\n  docs:\n    if: github.event_name == 'release'\n    steps:\n      - run: echo docs\n" +
				"  scan:\n    steps:\n      - run: make vuln\n",
			want: true,
		},
		{
			name: "lists before the steps",
			body: "on:\n  schedule:\n    - cron: \"0 5 * * *\"\njobs:\n  scan:\n    strategy:\n      matrix:\n" +
				"        go:\n          - stable\n    steps:\n      - uses: actions/checkout@v7\n      - run: make vuln\n",
			want: true,
		},
		{name: "|| true", body: "      - run: make vuln || true\n", want: false},
		{name: "|| :", body: "      - run: make vuln || :\n", want: false},
		{name: "|| a status kept for later", body: "      - run: make vuln || rc=$?\n", want: false},
		{name: "|| exit 1", body: "      - run: make vuln || exit 1\n", want: true},
		{name: "&& after it", body: "      - run: make vuln && echo scanned\n", want: false},
		{name: "after set +e", body: "      - run: |\n          set +e\n          make vuln\n          echo done\n", want: false},
		{name: "the last line after set +e", body: "      - run: |\n          set +e\n          make vuln\n", want: false},
		{name: "after set -e again", body: "      - run: |\n          set +e\n          rm -f x\n          set -e\n          make vuln\n          echo done\n", want: false},

		// Forms that never scan, or whose failure cannot fail the job, which
		// the list of bad shapes this check used to keep let through.
		{name: "a dry run", body: "      - run: make -n vuln\n", want: false},
		{name: "question mode", body: "      - run: make -q vuln\n", want: false},
		{name: "after a short-circuit", body: "      - run: true || make vuln\n", want: false},
		{name: "after a false &&", body: "      - run: false && make vuln\n", want: false},
		{name: "after exit 0", body: "      - run: |\n          exit 0\n          make vuln\n", want: false},
		{name: "inside a heredoc", body: "      - run: |\n          cat <<'END'\n          make vuln\n          END\n", want: false},
		{name: "into a pipe (no pipefail)", body: "      - run: make vuln | tee vuln.txt\n", want: false},
		{name: "in the background", body: "      - run: make vuln &\n", want: false},
		{name: "after an assignment", body: "      - run: MAKEFLAGS=n make vuln\n", want: false},
		{name: "under a shell without -e", body: "      - shell: bash {0}\n        run: |\n          make vuln\n          echo scanned\n", want: false},
		{name: "under a step shell", body: "      - run: make vuln\n        shell: bash\n", want: false},
		{name: "in another directory", body: "      - run: make vuln\n        working-directory: drivers/kafka\n", want: false},
		{
			name: "under a workflow defaults: shell",
			body: "defaults:\n  run:\n    shell: bash {0}\njobs:\n  scan:\n    steps:\n      - run: make vuln\n",
			want: false,
		},
		{
			name: "under a job defaults:",
			body: "jobs:\n  scan:\n    defaults:\n      run:\n        working-directory: x\n    steps:\n      - run: make vuln\n",
			want: false,
		},
		// make's own flags from the environment: MAKEFLAGS=n is make -n, at
		// any level and in any spelling of the key.
		{name: "a step env: MAKEFLAGS", body: "      - run: make vuln\n        env:\n          MAKEFLAGS: n\n", want: false},
		{
			name: "a job env: GNUMAKEFLAGS",
			body: "jobs:\n  scan:\n    env:\n      GNUMAKEFLAGS: --just-print\n    steps:\n      - run: make vuln\n",
			want: false,
		},
		{name: "a workflow env: MFLAGS", body: "env:\n  MFLAGS: -n\njobs:\n  scan:\n    steps:\n      - run: make vuln\n", want: false},
		{name: "a flow-mapping env:", body: "      - run: make vuln\n        env: {GOWORK: \"off\", \"MAKEFLAGS\": --dry-run}\n", want: false},
		{name: "another env: key", body: "      - run: make vuln\n        env:\n          GOWORK: \"off\"\n", want: true},
		{name: "MAKEFLAGS in a comment", body: "# MAKEFLAGS: n would make this a dry run\n      - run: make vuln\n", want: true},
		// A trailing comment is not part of the command: YAML strips it from
		// a plain value, and the shell ignores it in a block.
		{name: "a trailing comment", body: "      - run: make vuln # every module\n", want: true},
		{name: "a trailing comment in a block", body: "      - run: |\n          make vuln || exit 1  # every module\n", want: true},
		{name: "a comment that hides nothing", body: "      - run: make vuln #; true\n", want: true},
		{name: "a quoted hash is not a comment", body: "      - run: make vuln '#' x\n", want: false},
		{
			name: "a job that needs a job no event runs",
			body: "on:\n  pull_request:\njobs:\n  gatekeeper:\n    if: github.event_name == 'release'\n" +
				"    runs-on: ubuntu-latest\n    steps:\n      - run: echo ok\n" +
				"  govulncheck:\n    needs: gatekeeper\n    runs-on: ubuntu-latest\n    steps:\n      - run: make vuln\n",
			want: false,
		},
	}
	for _, tc := range cases {
		if got := runsCommand(runCommands(tc.body), makes("vuln")); got != tc.want {
			t.Errorf("%s: runs make vuln = %v, want %v (commands %q)", tc.name, got, tc.want, runCommands(tc.body))
		}
	}
}

// TestAnAgentWorktreeIsNotReadAsPartOfTheTree keeps a stale copy of the
// repository from answering for it. An agent's worktree under .claude/worktrees
// is a whole second checkout: its go.mod files read as more modules and its
// sources as more files, so a check could be satisfied by a declaration the
// tree itself has lost.
func TestAnAgentWorktreeIsNotReadAsPartOfTheTree(t *testing.T) {
	root := t.TempDir()
	write := func(rel, body string) {
		t.Helper()
		p := filepath.Join(root, rel)
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	write("go.mod", "module example.test\n\ngo 1.26\n")
	write("a.go", "package a\n")
	write(".claude/worktrees/stale-agent/go.mod", "module example.test\n\ngo 1.26\n")
	write(".claude/worktrees/stale-agent/a.go", "package a\n")

	c := &checker{root: root}
	if err := c.load(); err != nil {
		t.Fatal(err)
	}
	if strings.Join(c.modules, " ") != "." {
		t.Errorf("modules = %q, want only the root", c.modules)
	}
	for _, f := range c.files {
		if strings.HasPrefix(f.rel, ".claude/") {
			t.Errorf("read %s, which is a worktree's file and not the tree's", f.rel)
		}
	}
}

// TestARunScriptInterpolatesNoPRControlledContext pins what the injection
// check reads: a github or inputs context in a ${{ }} anywhere in a run:
// value, as GitHub's expression engine sees the value, with the line the
// expression opens on; and not the same expression in an env: value or a
// step's name, nor a context the check leaves out.
func TestARunScriptInterpolatesNoPRControlledContext(t *testing.T) {
	head := "on:\n  pull_request:\njobs:\n  gate:\n    steps:\n" // lines 1-5
	cases := []struct {
		name, steps string
		// head replaces the pull_request head above when it is set.
		head string
		want []injection
	}{
		{name: "inline", steps: "      - run: ./bench.sh \"origin/${{ github.base_ref }}\"\n", want: []injection{{6, "github.base_ref"}}},
		{
			name:  "in a block",
			steps: "      - run: |\n          ./bench.sh\n          gh pr comment ${{ github.event.pull_request.number }}\n",
			want:  []injection{{8, "github.event.pull_request.number"}},
		},
		{name: "no spaces", steps: "      - run: echo ${{github.event.comment.body}}\n", want: []injection{{6, "github.event.comment.body"}}},
		{name: "inside a function", steps: "      - run: echo ${{ format('{0}', inputs.family) }}\n", want: []injection{{6, "inputs.family"}}},
		{name: "the whole context", steps: "      - run: echo '${{ toJSON(github) }}'\n", want: []injection{{6, "github"}}},
		{
			// The engine substitutes into a shell comment like any other
			// line, and a value with a newline in it ends the comment.
			name:  "a shell comment line in a block",
			steps: "      - run: |\n          # ${{ github.event.issue.body }}\n          echo done\n",
			want:  []injection{{7, "github.event.issue.body"}},
		},
		{
			name:  "an expression across lines",
			steps: "      - run: |\n          echo ${{\n            github.event.pull_request.title }}\n",
			want:  []injection{{7, "github.event.pull_request.title"}},
		},
		{name: "the index form", steps: "      - run: echo ${{ github['head_ref'] }}\n", want: []injection{{6, "github['head_ref']"}}},
		{
			name:  "a plain value continued on the next line",
			steps: "      - run: echo one\n          ${{ github.head_ref }}\n      - run: echo two\n",
			want:  []injection{{7, "github.head_ref"}},
		},
		{name: "a quoted run key", steps: "      - \"run\": echo ${{ github.base_ref }}\n", want: []injection{{6, "github.base_ref"}}},
		{name: "a field no pull request chooses", steps: "      - run: git checkout ${{ github.sha }}\n", want: []injection{{6, "github.sha"}}},
		{
			name: "env, a step name, and contexts left out",
			steps: "      - name: ${{ github.head_ref }}\n        env:\n          BASE_REF: ${{ github.base_ref }}\n" +
				"        run: |\n          ./bench.sh \"origin/$BASE_REF\"\n" +
				"          echo ${{ matrix.group }} ${{ steps.gate.outputs.rc }} ${{ needs.a.outputs.b }} ${{ env.X }}\n" +
				"          echo ${{ contains(matrix.os, 'github') }}\n",
		},
		// GitHub looks a context's name up whatever its case, so GitHub.x
		// and GITHUB.X are github.x.
		{name: "a capitalised context", steps: "      - run: ./bench.sh \"origin/${{ GitHub.base_ref }}\"\n", want: []injection{{6, "GitHub.base_ref"}}},
		{name: "an upper-case context", steps: "      - run: echo ${{ GITHUB.HEAD_REF }}\n", want: []injection{{6, "GITHUB.HEAD_REF"}}},
		{name: "a capitalised inputs", steps: "      - run: echo ${{ Inputs.family }}\n", want: []injection{{6, "Inputs.family"}}},
		// GitHub resolves YAML anchors, so a run: value that is an alias runs
		// the anchored text, which this reader never sees as a script. Any
		// alias is refused, wherever it stands.
		{
			name: "a run: that is a YAML alias",
			steps: "      - env:\n          SCRIPT: &script |\n            ./bench.sh \"origin/${{ github.base_ref }}\"\n" +
				"        run: *script\n",
			want: []injection{{9, "*script"}},
		},
		{name: "a step that is a YAML alias", steps: "      - &step\n        run: echo one\n      - *step\n", want: []injection{{8, "*step"}}},
		{name: "a merge key", steps: "      - <<: *base\n        name: scan\n", want: []injection{{6, "*base"}}},
		{
			name:  "an asterisk inside a block is shell, not an alias",
			steps: "      - run: |\n          ls\n          *.go\n        env:\n          GLOB: '*.go'\n",
		},
		// Under pull_request_target, workflow_run and issue_comment no
		// expression may stand in a run: value at all: env.* and steps.*
		// can carry a pull request's text as well as github.* can.
		{
			name: "env indirection under pull_request_target",
			head: "on:\n  pull_request_target:\n    types: [opened]\njobs:\n  label:\n    steps:\n", // lines 1-6
			steps: "      - env:\n          TITLE: ${{ github.event.pull_request.title }}\n" +
				"        run: echo \"${{ env.TITLE }}\"\n",
			want: []injection{{9, "env.TITLE"}},
		},
		{
			name:  "a step output under a workflow_run trigger",
			head:  "on: [push, workflow_run]\njobs:\n  label:\n    steps:\n", // lines 1-4
			steps: "      - id: l\n        uses: actions/labeler@v6\n      - run: echo \"${{ steps.l.outputs.all-labels }}\"\n",
			want:  []injection{{7, "steps.l.outputs.all-labels"}},
		},
		{
			name:  "a matrix value under issue_comment",
			head:  "\"on\":\n  - issue_comment\njobs:\n  label:\n    steps:\n", // lines 1-5
			steps: "      - run: echo ${{ matrix.x }} ${{ github.head_ref }}\n",
			want:  []injection{{6, "matrix.x"}, {6, "github.head_ref"}},
		},
		{
			name:  "the same env indirection under pull_request",
			steps: "      - env:\n          TITLE: ${{ github.event.pull_request.title }}\n        run: echo \"${{ env.TITLE }}\"\n",
		},
	}
	for _, tc := range cases {
		h := head
		if tc.head != "" {
			h = tc.head
		}
		got := runInjections(h + tc.steps)
		if len(got) != len(tc.want) {
			t.Errorf("%s: found %v, want %v", tc.name, got, tc.want)
			continue
		}
		for i := range tc.want {
			if got[i] != tc.want[i] {
				t.Errorf("%s: finding %d is %v, want %v", tc.name, i, got[i], tc.want[i])
			}
		}
	}
}

// TestThePatchedToolchainIsTheLatestPatchRelease pins which setup-go inputs
// count as Go at its latest patch release for vuln.yml's scan: `stable`, or a
// minor version, with check-latest: true. A full version is one fixed release
// however check-latest is set, and `go-version: "1.27.3"` installed exactly the
// go.mod floor the check exists to keep the scan off.
func TestThePatchedToolchainIsTheLatestPatchRelease(t *testing.T) {
	release := "on:\n  push:\njobs:\n  release:\n    steps:\n" +
		"      - uses: actions/setup-go@v7\n        with:\n          go-version: stable\n          check-latest: true\n" +
		"      - uses: goreleaser/goreleaser-action@v7\n"
	cases := []struct {
		name, with string
		ok         bool
	}{
		{"stable", "          go-version: stable\n          check-latest: true\n", true},
		{"a quoted minor", "          go-version: \"1.27\"\n          check-latest: true\n", true},
		{"a minor's x", "          go-version: 1.27.x\n          check-latest: true\n", true},
		{"the floor by its full version", "          go-version: \"1.27.3\"\n          check-latest: true\n", false},
		{"a full version with a go prefix", "          go-version: go1.28.1\n          check-latest: true\n", false},
		{"a matrix value", "          go-version: ${{ matrix.go }}\n          check-latest: true\n", false},
		{"go-version-file", "          go-version-file: go.mod\n          check-latest: true\n", false},
		{"stable without check-latest", "          go-version: stable\n", false},
		{"check-latest only in a comment", "          go-version: stable\n          # check-latest: true\n", false},
	}
	for _, tc := range cases {
		root := t.TempDir()
		wf := filepath.Join(root, ".github", "workflows")
		if err := os.MkdirAll(wf, 0o755); err != nil {
			t.Fatal(err)
		}
		files := map[string]string{
			filepath.Join(root, "go.mod"): "module example.com/m\n\ngo 1.27.3\n",
			filepath.Join(wf, "vuln.yml"): "on:\n  push:\njobs:\n  govulncheck:\n    steps:\n" +
				"      - uses: actions/setup-go@v7\n        with:\n" + tc.with + "      - run: make vuln\n",
			filepath.Join(wf, "release.yml"): release,
		}
		for p, body := range files {
			if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
				t.Fatal(err)
			}
		}
		c := &checker{root: root, used: map[string]bool{}, noWaivers: true}
		c.checkPatchedToolchain()
		if got := len(c.problems) == 0; got != tc.ok {
			t.Errorf("%s: passed = %v, want %v; problems %q", tc.name, got, tc.ok, c.problems)
		}
	}
}
