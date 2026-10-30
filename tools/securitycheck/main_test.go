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
