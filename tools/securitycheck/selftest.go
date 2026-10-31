package main

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// selfCheck runs every check twice: once over a tree that breaks it, where it
// must report, and once over a tree that satisfies it, where it must not.
//
// Both halves are the point. A check nobody has watched fail is assumed not to
// work -- this repository has nine instances of one that could not -- and a
// check that always fails is exactly as useless, because it is turned off
// within the week. `make security-check-selftest` is in `make ci`, so the
// instrument is tested on every run rather than on the day it was written.
func selfCheck() error {
	dir, err := os.MkdirTemp("", "securitycheck-selftest-")
	if err != nil {
		return err
	}
	defer func() { _ = os.RemoveAll(dir) }()

	bad := filepath.Join(dir, "bad")
	good := filepath.Join(dir, "good")
	if err := writeFixture(bad, false); err != nil {
		return err
	}
	if err := writeFixture(good, true); err != nil {
		return err
	}

	// Each check, and a substring the finding it must produce contains. The
	// substring is asserted rather than just the count, so that a check
	// failing for an unrelated reason does not read as a pass.
	cases := []struct{ check, want string }{
		{"configs", "with no String method"},
		{"configs", "with no LogValue method"},
		{"credentials", "drivers/bad.Config.Password is a string"},

		{"credentials", "drivers/bad.Auth.Password is a string"},
		// A type CALLED Secret that renders what it holds. The passing
		// fixture used to be exactly this, and passed.
		{"credentials", "drivers/bad.Config.Token is a local Secret with no String or LogValue method"},
		{"insecure", "does not say Insecure in its name"},
		{"supplychain", "does not build every module"},
		{"supplychain", "must run `make vuln`"},
		// The failing vuln.yml runs `make vuln` too, in a step whose job
		// runs on a release only, an event the workflow never receives.
		{"supplychain", "only in a step that may not run or whose failure cannot fail the workflow (the job's if: github.event_name == 'release')"},
		{"supplychain", "ship with no SLSA attestation"},
		{"supplychain", "vuln.yml's scan installs Go with go-version-file"},
		{"supplychain", "release.yml's goreleaser job installs Go with go-version-file"},
		{"injection", "pr.yml:7 writes ${{ github.head_ref }} into a run: script"},
		{"injection", "pr.yml:9 writes ${{ inputs.ref }} into a run: script"},
		// Each form the check first missed: a shell comment line in a
		// block, an expression across lines, the index form, and a plain
		// value continued on the next line.
		{"injection", "pr.yml:11 writes ${{ github.event.issue.body }} into a run: script"},
		{"injection", "pr.yml:14 writes ${{ github.event.pull_request.title }} into a run: script"},
		{"injection", "pr.yml:16 writes ${{ github['head_ref'] }} into a run: script"},
		{"injection", "pr.yml:18 writes ${{ github.event.comment.body }} into a run: script"},
	}
	byCheck := map[string][]string{}
	for _, tc := range cases {
		byCheck[tc.check] = append(byCheck[tc.check], tc.want)
	}
	checks := make([]string, 0, len(byCheck))
	for k := range byCheck {
		checks = append(checks, k)
	}
	sort.Strings(checks)

	var problems []string
	for _, check := range checks {
		c := &checker{root: bad, noWaivers: true}
		if err := c.load(); err != nil {
			return fmt.Errorf("loading the failing fixture: %w", err)
		}
		c.run(check)
		got := strings.Join(c.problems, "\n")
		for _, want := range byCheck[check] {
			if !strings.Contains(got, want) {
				problems = append(problems, fmt.Sprintf(
					"check %q did not report %q on a tree that breaks it; it reported:\n%s", check, want, indent(got),
				))
			}
		}

		c = &checker{root: good, noWaivers: true}
		if err := c.load(); err != nil {
			return fmt.Errorf("loading the passing fixture: %w", err)
		}
		c.run(check)
		if len(c.problems) != 0 {
			problems = append(problems, fmt.Sprintf(
				"check %q reported on a tree that satisfies it:\n%s", check, indent(strings.Join(c.problems, "\n")),
			))
		}
	}
	problems = append(problems, vulnSelfCheck()...)
	if len(problems) > 0 {
		return fmt.Errorf("%d self-test failure(s):\n%s", len(problems), indent(strings.Join(problems, "\n")))
	}
	fmt.Printf("securitycheck: ok, %d check(s) reported on a tree that breaks them and stayed quiet on one that does not\n",
		len(checks)+1)
	return nil
}

// govulnStream renders a minimal govulncheck JSON stream: a config message, an
// osv entry per finding, and the findings. A finding's trace is either symbol
// level (the first frame names a function, which is what counts) or module
// level (it does not, which must never count).
func govulnStream(config bool, findings ...string) string {
	var b strings.Builder
	if config {
		b.WriteString(`{"config":{"protocol_version":"v1.0.0","scanner_name":"govulncheck","go_version":"go1.27.1",` +
			`"db_last_modified":"2026-09-16T18:00:43Z","scan_level":"symbol"}}` + "\n")
	}
	for _, f := range findings {
		b.WriteString(f + "\n")
	}
	return b.String()
}

func symbolFinding(osv, fixed string) string {
	fix := ""
	if fixed != "" {
		fix = `"fixed_version":"` + fixed + `",`
	}
	return `{"osv":{"id":"` + osv + `","summary":"a fixture advisory"}}` + "\n" +
		`{"finding":{"osv":"` + osv + `",` + fix + `"trace":[` +
		`{"module":"example.com/dep","version":"v1.0.0","package":"example.com/dep","function":"Decode"},` +
		`{"module":"fixture","package":"fixture/p","function":"Run","position":{"filename":"p/p.go","line":7}}]}}`
}

func moduleFinding(osv string) string {
	return `{"finding":{"osv":"` + osv + `","fixed_version":"v9.9.9","trace":[{"module":"example.com/dep","version":"v1.0.0"}]}}`
}

// vulnSelfCheck is the self-test for `securitycheck -vuln`: the same two
// halves as the tree checks, over govulncheck streams instead of a tree, and
// with the fixture's own waiver table rather than this repository's.
func vulnSelfCheck() []string {
	var problems []string
	read := func(module, stream string) *vulnScan {
		s, err := readVulnScan(module, strings.NewReader(stream))
		if err != nil {
			problems = append(problems, "vuln: reading a well-formed fixture stream: "+err.Error())
			return &vulnScan{module: module, findings: map[string]*vulnFinding{}}
		}
		return s
	}
	waivers := []vulnWaiver{
		{osv: "GO-0000-0002", modules: []string{"m/nofix"}, noFix: true, owner: "fixture", reason: "no fix"},
		{osv: "GO-0000-0003", modules: []string{"m/dead", "m/deferred"}, owner: "fixture", reason: "deferred"},
	}

	// Breaks it three ways: an advisory nobody waived, a no-fix waiver whose
	// advisory now has a fix, and a waiver naming a module that no longer has
	// its finding (m/dead).
	bad := []*vulnScan{
		read("m/unwaived", govulnStream(true, symbolFinding("GO-0000-0001", "v1.2.3"))),
		read("m/nofix", govulnStream(true, symbolFinding("GO-0000-0002", "v2.0.0"))),
		read("m/deferred", govulnStream(true, symbolFinding("GO-0000-0003", "v3.0.0"))),
		read("m/dead", govulnStream(true)),
	}
	_, got := judgeVulns(bad, waivers)
	for _, want := range []string{
		"m/unwaived: GO-0000-0001 (a fixture advisory) in example.com/dep@v1.0.0, fixed in v1.2.3",
		"p/p.go:7 fixture/p.Run -> ... -> example.com/dep.Decode",
		"GO-0000-0002 is waived as having no fixed version, and the database now names v2.0.0",
		"waiver for GO-0000-0003 in m/dead (owner fixture) matched no finding",
	} {
		if !strings.Contains(strings.Join(got, "\n"), want) {
			problems = append(problems, fmt.Sprintf("check \"vuln\" did not report %q on scans that break it; it reported:\n%s",
				want, indent(strings.Join(got, "\n"))))
		}
	}
	if _, err := readVulnScan("m/died", strings.NewReader(govulnStream(false))); err == nil ||
		!strings.Contains(err.Error(), "the scan did not run") {
		problems = append(problems, fmt.Sprintf("check \"vuln\" read a stream with no config message as a clean scan (err = %v)", err))
	}

	// Satisfies it: every symbol-level finding waived, the no-fix one still
	// without a fix, and a module-level finding -- code the module requires
	// and never calls -- which must not count at all.
	good := []*vulnScan{
		read("m/nofix", govulnStream(true, symbolFinding("GO-0000-0002", ""), moduleFinding("GO-0000-0009"))),
		read("m/deferred", govulnStream(true, symbolFinding("GO-0000-0003", "v3.0.0"))),
		read("m/dead", govulnStream(true, symbolFinding("GO-0000-0003", "v3.0.0"))),
		read("m/clean", govulnStream(true, moduleFinding("GO-0000-0001"))),
	}
	if _, got := judgeVulns(good, waivers); len(got) != 0 {
		problems = append(problems, fmt.Sprintf("check \"vuln\" reported on scans that satisfy it:\n%s",
			indent(strings.Join(got, "\n"))))
	}
	return problems
}

func indent(s string) string {
	if s == "" {
		return "    (nothing)"
	}
	return "    " + strings.ReplaceAll(s, "\n", "\n    ")
}

// writeFixture builds a miniature repository. With ok=false every check has
// something to find; with ok=true every check is satisfied.
func writeFixture(root string, ok bool) error {
	cfgBad := `package bad

// Secret is named for what it should do and does not do it: no String, no
// LogValue, so %v and slog print the credential.
type Secret string

// Config is a connection configuration with none of §12's obligations met.
type Config struct {
	Addr       string
	Password   string
	Token      Secret
	SkipVerify bool
}

// Auth is a login with no address and no configuration name.
type Auth struct {
	User, Password string
}
`
	cfgGood := `package good

import "log/slog"

// Secret is a local redacting type, which is one of the spellings the
// credentials check accepts -- because it has both renderings.
type Secret string

// String renders the redaction.
func (Secret) String() string { return "redacted" }

// LogValue renders the redaction for slog.
func (s Secret) LogValue() slog.Value { return slog.StringValue(s.String()) }

// Config is a connection configuration that meets §12.
type Config struct {
	Addr               string
	Password           Secret
	Token              Secret
	InsecureSkipVerify bool
}

// Auth is a login whose credential redacts itself.
type Auth struct {
	User     string
	Password Secret
}

// String renders the configuration with the credential redacted.
func (c Config) String() string { return c.Addr }

// LogValue renders the configuration for slog.
func (c Config) LogValue() slog.Value { return slog.StringValue(c.String()) }
`
	// The failing codeql.yml and vuln.yml NAME the command they should run, in
	// the places the real files do: a comment explaining the step, a step's
	// name, and a shell comment inside a run block. The checks used to search
	// the whole file for the text, so the real codeql.yml's own comment about
	// this check satisfied it with its `run: make build` step deleted.
	codeqlBad := "name: codeql\njobs:\n  analyze:\n    steps:\n" +
		"      # make security-check fails when this file neither runs `make build` nor uses hack/for-each-module.sh.\n" +
		"      - name: make build\n        run: go build ./...\n"
	codeqlGood := "name: codeql\njobs:\n  analyze:\n    steps:\n      - run: make build\n"
	// setup-go's two spellings: the go.mod directive's floor, which the
	// failing workflows install, and the latest patch release. The failing
	// vuln.yml names check-latest in a comment, which must not count.
	floorGo := "      - uses: actions/setup-go@v7\n        with:\n          go-version-file: go.mod\n" +
		"          # check-latest: true\n"
	patchedGo := "      - uses: actions/setup-go@v7\n        with:\n          go-version: stable\n          check-latest: true\n"
	vulnBad := "name: vuln\n# govulncheck over every module (`make vuln` -> hack/vuln.sh)\njobs:\n  govulncheck:\n    steps:\n" +
		floorGo + "      - run: |\n          # make vuln scans every module\n          govulncheck ./...\n" +
		"  release-scan:\n    if: github.event_name == 'release'\n    steps:\n      - run: make vuln\n"
	vulnGood := "name: vuln\njobs:\n  govulncheck:\n    steps:\n" + patchedGo +
		"      - run: |\n          # every module, against the waivers\n          make vuln\n        env:\n          GOWORK: \"off\"\n"
	// The failing release.yml NAMES the zip archives -- in a comment, and in a
	// step that is not the attestation. The check used to search the whole
	// file for "*.zip", so either would have satisfied it (assumption
	// C06b.5); the subject list is the only place the name counts.
	//
	// Its goreleaser job installs the floor while ANOTHER job carries the
	// patched spelling: the rule is about the job that builds the binaries.
	releaseBad := "name: release\n# the windows archives are cmd/iqmq/dist/*.zip\njobs:\n" +
		"  validate:\n    steps:\n" + patchedGo +
		"  release:\n    steps:\n" + floorGo +
		"      - uses: goreleaser/goreleaser-action@v7\n" +
		"      - run: ls cmd/iqmq/dist/*.zip\n" +
		"      - uses: actions/attest-build-provenance@v4\n" +
		"        with:\n          subject-path: |\n            cmd/iqmq/dist/*.tar.gz\n" +
		"            # cmd/iqmq/dist/*.zip\n"
	releaseGood := "name: release\njobs:\n  validate:\n    steps:\n" + floorGo +
		"  release:\n    steps:\n" + patchedGo + "      - uses: goreleaser/goreleaser-action@v7\n" +
		"      - uses: actions/attest-build-provenance@v4\n" +
		"        with:\n          subject-path: |\n            cmd/iqmq/dist/*.tar.gz\n            cmd/iqmq/dist/*.zip\n" +
		"            cmd/iqmq/dist/checksums.txt\n"
	// The failing pull-request workflow writes a branch name, a dispatch
	// input and pull-request text into its scripts, in every shape the
	// expression engine substitutes into; the passing one hands them over
	// through env: and keeps the contexts the check leaves out.
	prBad := "name: pr\non:\n  pull_request:\njobs:\n  build:\n    steps:\n" + // lines 1-6
		"      - run: echo \"${{ github.head_ref }}\"\n" + // 7
		"      - run: |\n          ./build.sh ${{ inputs.ref }}\n" + // 8-9
		"      - run: |\n          # ${{ github.event.issue.body }}\n          echo done\n" + // 10-12
		"      - run: |\n          echo ${{\n            github.event.pull_request.title }}\n" + // 13-15
		"      - run: echo ${{ github['head_ref'] }}\n" + // 16
		"      - run: echo one\n          ${{ github.event.comment.body }}\n" // 17-18
	prGood := "name: pr\non:\n  pull_request:\njobs:\n  build:\n    steps:\n" +
		"      - env:\n          HEAD_REF: ${{ github.head_ref }}\n        run: echo \"$HEAD_REF\"\n" +
		"      - env:\n          BODY: ${{ github.event.issue.body }}\n        run: |\n" +
		"          # the issue body is $BODY, never the expression\n          printf '%s\\n' \"$BODY\"\n" +
		"      - run: |\n          ./build.sh ${{ matrix.shard }} ${{ steps.prep.outputs.dir }}\n"
	goreleaser := "archives:\n  - id: cli\n    formats:\n      - tar.gz\n    format_overrides:\n" +
		"      - goos: windows\n        formats:\n          - zip\n"

	pkgDir := "drivers/bad"
	src := cfgBad
	codeql, vuln, release, pr := codeqlBad, vulnBad, releaseBad, prBad
	if ok {
		pkgDir, src = "drivers/good", cfgGood
		codeql, vuln, release, pr = codeqlGood, vulnGood, releaseGood, prGood
	}
	files := map[string]string{
		"go.mod":                        "module fixture\n\ngo 1.26.0\n",
		pkgDir + "/go.mod":              "module fixture/" + pkgDir + "\n\ngo 1.26.0\n",
		pkgDir + "/config.go":           src,
		".github/workflows/codeql.yml":  codeql,
		".github/workflows/vuln.yml":    vuln,
		".github/workflows/release.yml": release,
		".github/workflows/pr.yml":      pr,
		"cmd/iqmq/.goreleaser.yaml":     goreleaser,
	}
	for rel, body := range files {
		p := filepath.Join(root, filepath.FromSlash(rel))
		if err := os.MkdirAll(filepath.Dir(p), 0o750); err != nil {
			return err
		}
		if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
			return err
		}
	}
	return nil
}
