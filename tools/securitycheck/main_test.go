package main

import (
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
