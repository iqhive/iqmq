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
// `make vuln` or `make build`: a command in a run step, inline or in a block,
// and nothing else in the file. The real codeql.yml and vuln.yml both name
// their command in a comment, and that comment used to satisfy the check with
// the step itself deleted.
func TestAWorkflowRunsOnlyTheCommandsItsRunStepsRun(t *testing.T) {
	cases := []struct {
		name, body string
		want       bool
	}{
		{name: "inline", body: "    steps:\n      - run: make vuln\n", want: true},
		{name: "quoted", body: "    steps:\n      - run: \"make vuln\"\n", want: true},
		{name: "in a block", body: "      - run: |\n          set -e\n          GOWORK=off make -k vuln\n", want: true},
		{name: "after a separator", body: "      - run: cd x && make vuln\n", want: true},
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
		{name: "&& after it", body: "      - run: make vuln && echo scanned\n", want: true},
		{name: "after set +e", body: "      - run: |\n          set +e\n          make vuln\n          echo done\n", want: false},
		{name: "the last line after set +e", body: "      - run: |\n          set +e\n          make vuln\n", want: true},
		{name: "after set -e again", body: "      - run: |\n          set +e\n          rm -f x\n          set -e\n          make vuln\n          echo done\n", want: true},
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
		want        []injection
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
	}
	for _, tc := range cases {
		got := runInjections(head + tc.steps)
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
