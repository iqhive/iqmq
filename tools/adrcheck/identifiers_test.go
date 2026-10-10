package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// identFixture builds a throwaway repository with one module, one package and
// one ledger row, and returns the repo root and the ledger path.
//
// The module has no requirements, so `go test -list` inside it needs no
// network and no module cache beyond the standard library.
func identFixture(t *testing.T, where string, extra ...string) (repo, ledger string) {
	t.Helper()
	repo = t.TempDir()
	writeFile(t, filepath.Join(repo, "go.mod"), "module example.test\n\ngo 1.26\n")
	writeFile(t, filepath.Join(repo, "thing.go"), "package thing\n\n"+
		"type Sink struct{ Depth int }\n\n"+
		"func (s *Sink) Close() error { return nil }\n\n"+
		"const Answer = 42\n\n"+
		"var table = map[string]int{}\n\n"+
		"func use() { local := table; _ = local }\n")
	writeFile(t, filepath.Join(repo, "thing_test.go"), "package thing\n\nimport \"testing\"\n\n"+
		"func TestSinkCloses(t *testing.T) { t.Run(\"a named subtest\", func(t *testing.T) {}) }\n\n"+
		"func TestFamilyOne(t *testing.T) {}\n")
	writeFile(t, filepath.Join(repo, "Makefile"), "target:\n\t@true\n")
	writeFile(t, filepath.Join(repo, "run.sh"), "#!/bin/sh\nshell_function() { :; }\n")
	// The Assumption column is fixed prose that does NOT echo the evidence:
	// a fixture whose claim repeats its Where column cannot tell "the row
	// cites this" from "the row's own marker mentions it", and that is exactly
	// the check the marker needs.
	body := "# ADR-017\n\n" + testRegistry + tableHeader + "\n|---|---|---|---|\n" +
		"| A27.1 | a claim with no names in it | " + where + " | A27 |\n"
	for _, row := range extra {
		body += row + "\n"
	}
	body += "\n## Alternatives\n\n- **Assumption A27.1**: the alternative.\n"
	adrDir := filepath.Join(repo, "adr")
	ledger = filepath.Join(adrDir, ledgerFile)
	writeFile(t, ledger, body)
	return repo, ledger
}

// runIdent runs the -identifiers mode over a fixture and returns its problems.
func runIdent(t *testing.T, repo, ledger string) []string {
	t.Helper()
	rows, ps := loadLedger(ledger)
	if len(ps) != 0 {
		t.Fatalf("the fixture ledger is malformed: %v", ps)
	}
	_, problems := checkIdentifiers(repo, ledger, rows, repoModules(repo))
	return problems
}

// hasProblem reports whether any problem mentions want.
func hasProblem(ps []string, want string) bool {
	for _, p := range ps {
		if strings.Contains(p, want) {
			return true
		}
	}
	return false
}

func hasAll(t *testing.T, problems []string, want ...string) {
	t.Helper()
	for _, w := range want {
		if !hasProblem(problems, w) {
			t.Errorf("no problem mentions %q; problems = %v", w, problems)
		}
	}
}

// TestIdentifiersResolveWhatTheEvidenceNames is the mode's happy path: a row
// whose evidence names a test the toolchain selects, a subtest, a test family
// and four kinds of Go declaration reports nothing.
//
// It is one test rather than five because the point is that NOTHING is
// reported: a mode that passes everything passes each of these too, which is
// why every case below has a failing twin.
func TestIdentifiersResolveWhatTheEvidenceNames(t *testing.T) {
	repo, ledger := identFixture(t,
		"`thing.go`: `Sink`, `Sink.Close`, `Answer`, `table`, `local`; `thing_test.go`: `TestSinkCloses`, `TestFamily`, `a named subtest`")
	if ps := runIdent(t, repo, ledger); len(ps) != 0 {
		t.Fatalf("a row whose evidence all resolves reported %d problem(s): %v", len(ps), ps)
	}
}

// TestIdentifiersReportADanglingTestCitation is the sixteen-citation backlog
// this mode exists for: a Test name no package defines.
func TestIdentifiersReportADanglingTestCitation(t *testing.T) {
	repo, ledger := identFixture(t, "`thing_test.go`: `TestSinkClosesTwice`")
	hasAll(t, runIdent(t, repo, ledger), "TestSinkClosesTwice", "go test -list")
}

// TestIdentifiersReportADeletedIdentifier is the half a name-only checker
// cannot see, and the half fix/rabbit-publisher found two more defects with:
// a back-ticked identifier a commit deleted.
func TestIdentifiersReportADeletedIdentifier(t *testing.T) {
	repo, ledger := identFixture(t, "`thing.go`: `Sink.Freed`")
	hasAll(t, runIdent(t, repo, ledger), "Sink.Freed", "no package in the tree declares")
}

// TestIdentifiersLeaveProseAlone keeps the mode from reporting names that
// exist, which is how it would come to be turned off.
//
// The Where column's convention is a path, a colon and a comma-separated list.
// A back-tick reached across a WORD is prose about the code — "the
// `FeatureDeadLetter` exercise", "`cliEnv.bridge`'s `abort`", "the
// `incomplete` branch of `order`" — and reading those as citations produced
// nineteen findings about things that were all there.
func TestIdentifiersLeaveProseAlone(t *testing.T) {
	for name, where := range map[string]string{
		"a probe":             "`thing.go`: the `notAName` probe",
		"a possessive":        "`thing.go`: `Sink`'s `notAName`",
		"a second prose tick": "`thing.go`: the `notAName`/`alsoNotAName` block",
		"a branch of":         "`thing.go`: `Answer`, the `notAName` branch of `use`",
	} {
		t.Run(name, func(t *testing.T) {
			repo, ledger := identFixture(t, where)
			if ps := runIdent(t, repo, ledger); len(ps) != 0 {
				t.Errorf("prose was read as a citation: %v", ps)
			}
		})
	}
}

// TestIdentifiersLeaveANonGoPathsNamesAlone is the other false-positive class,
// and it was forty of the first ninety findings: a shell function, a Makefile
// target, a TLA+ operator and a YAML key are all real names of real things and
// none of them is a Go identifier.
func TestIdentifiersLeaveANonGoPathsNamesAlone(t *testing.T) {
	for name, where := range map[string]string{
		"a shell function":  "`run.sh`: `shell_function`",
		"a make target":     "`Makefile`: `target`",
		"a name after both": "`thing.go`: `Answer`; `run.sh`: `shell_function`",
	} {
		t.Run(name, func(t *testing.T) {
			repo, ledger := identFixture(t, where)
			if ps := runIdent(t, repo, ledger); len(ps) != 0 {
				t.Errorf("a non-Go path's names were resolved as Go: %v", ps)
			}
		})
	}
}

// TestAbsentMarkerExemptsAName is the escape marker's first direction: a row
// may name something that must not resolve, because a branch removed it or
// because the row is ABOUT a misspelling.
func TestAbsentMarkerExemptsAName(t *testing.T) {
	repo, ledger := identFixture(t,
		"`thing_test.go`: `TestSinkClosesTwice`; absent: `TestSinkClosesTwice` -- deliberately a name this package does not define")
	if ps := runIdent(t, repo, ledger); len(ps) != 0 {
		t.Fatalf("a marked name was still reported: %v", ps)
	}
}

// TestAbsentMarkerIsCheckedInBothDirections is the half that makes the marker
// a claim rather than an exemption.
//
// The day a marked name starts resolving is the day assumption C08.6's claim
// silently becomes false — its pattern would select a test, the campaign would
// start, and the row would be untrue — and the day a deleted package comes
// back under its old name. Neither is a thing to discover by reading.
func TestAbsentMarkerIsCheckedInBothDirections(t *testing.T) {
	t.Run("a marked test that resolves", func(t *testing.T) {
		repo, ledger := identFixture(t,
			"`thing_test.go`: `TestSinkCloses`; absent: `TestSinkCloses` -- this reason is long enough to be a reason")
		hasAll(t, runIdent(t, repo, ledger), "marks `TestSinkCloses` absent", "now selects it")
	})
	t.Run("a marked identifier that resolves", func(t *testing.T) {
		repo, ledger := identFixture(t,
			"`thing.go`: `Answer`; absent: `Answer` -- this reason is long enough to be a reason")
		hasAll(t, runIdent(t, repo, ledger), "marks `Answer` absent", "declares it now")
	})
}

// TestAbsentMarkerNeedsAReasonAndASubject stops the marker becoming the silent
// exemption this whole mode exists to remove.
func TestAbsentMarkerNeedsAReasonAndASubject(t *testing.T) {
	for name, tc := range map[string]struct{ where, want string }{
		"no reason at all": {
			"`thing.go`: `Answer`; absent: `TestGone`",
			"no reason",
		},
		"a reason too short": {
			"`thing.go`: `Answer`; absent: `TestGone` -- gone",
			"say why the name must not resolve",
		},
		"nothing in back-ticks": {
			"`thing.go`: `Answer`; absent: TestGone -- this reason is long enough to be a reason",
			"names nothing in back-ticks",
		},
		"a name the row does not cite": {
			"`thing.go`: `Answer`; absent: `TestNeverMentioned` -- this reason is long enough to be a reason",
			"does not cite it",
		},
	} {
		t.Run(name, func(t *testing.T) {
			repo, ledger := identFixture(t, tc.where)
			hasAll(t, runIdent(t, repo, ledger), tc.want)
		})
	}
}

// TestSubtestCitationIsMatchedExactly is the exemption that used to hide nine
// defects.
//
// An earlier checker asked whether any t.Run name was a substring of the
// citation or the citation of it, so a `t.Run("Rank")` anywhere in the
// repository excused `TestRankIsNotTheDeclarationOrder` — a test that existed
// nowhere, in a row whose evidence had gone with `x/apibench`. Nine citations
// passed that way. The exemption is kept, because a row may legitimately cite
// a subtest `go test -list` cannot see; it is kept EXACT.
func TestSubtestCitationIsMatchedExactly(t *testing.T) {
	repo, ledger := identFixture(t, "`thing_test.go`: `a named subtest`")
	if ps := runIdent(t, repo, ledger); len(ps) != 0 {
		t.Fatalf("an exact subtest name was reported: %v", ps)
	}
	// "named" is a substring of the subtest name, and a substring rule would
	// have accepted it. It is not a test and not a subtest.
	repo, ledger = identFixture(t, "`thing_test.go`: `TestNamed`")
	hasAll(t, runIdent(t, repo, ledger), "TestNamed", "go test -list")
}

// TestTagGatedTestsAreListed is the instrument's own failure mode: a check that
// reports success by not looking.
//
// Nineteen test files in the `conformance` module sit behind `//go:build
// integration`, so a plain `go test -list` selects none of them and six good
// citations read as absent — and the repair a reader would reach for is the
// escape marker, which would then record "this name does not exist" about a
// test that does.
func TestTagGatedTestsAreListed(t *testing.T) {
	repo, ledger := identFixture(t, "`gated_test.go`: `TestOnlyBehindATag`")
	writeFile(t, filepath.Join(repo, "gated_test.go"),
		"//go:build somegate\n\npackage thing\n\nimport \"testing\"\n\nfunc TestOnlyBehindATag(t *testing.T) {}\n")
	if ps := runIdent(t, repo, ledger); len(ps) != 0 {
		t.Fatalf("a test behind a build tag read as absent: %v", ps)
	}
	if tags := gatingTags(repo, repoModules(repo)); len(tags["."]) != 1 || tags["."][0] != "somegate" {
		t.Errorf("gatingTags = %v, want the one tag that gates a test file", tags)
	}
}

// TestGatingTagsIgnoreTheToolchainsOwn keeps the tag discovery from listing
// every module under `-tags race`, which is not a thing to build under.
func TestGatingTagsIgnoreTheToolchainsOwn(t *testing.T) {
	repo, _ := identFixture(t, "`thing.go`: `Answer`")
	writeFile(t, filepath.Join(repo, "plat_test.go"),
		"//go:build race && unix && go1.27\n\npackage thing\n\nimport \"testing\"\n\nfunc TestPlatform(t *testing.T) {}\n")
	if tags := gatingTags(repo, repoModules(repo)); len(tags["."]) != 0 {
		t.Errorf("gatingTags = %v, want none: race, unix and go1.27 are the toolchain's", tags)
	}
}

// TestEvidencePathsAreExpanded covers the three shapes the Where column writes
// a package in, because a path this check cannot resolve silently stops
// checking the names beside it.
func TestEvidencePathsAreExpanded(t *testing.T) {
	repo, _ := identFixture(t, "`thing.go`: `Answer`")
	sub := filepath.Join(repo, "inner")
	writeFile(t, filepath.Join(sub, "inner.go"), "package inner\n\nfunc Deep() {}\n")
	tr := loadTree(repo)
	for _, tc := range []struct{ path, want string }{
		{"inner/inner.go", sub},
		{"inner", sub},
		{"inner/*.go", sub},
		{"{inner}/inner.go", sub},
		{"inner.go", sub}, // a bare base name, resolved by where the file is
	} {
		dirs, ok := tr.dirsFor(tc.path)
		if !ok || len(dirs) != 1 || dirs[0] != tc.want {
			t.Errorf("dirsFor(%q) = %v, %v; want [%s]", tc.path, dirs, ok, tc.want)
		}
	}
}

// TestRowWithALiteralPipeNamesItself is the trap the contributor notes describes:
// there is no escaping in this file, so an operator written as `||` makes an
// eight-column row, the row defines no assumption, and every citation of it is
// then reported — in the CITING files, which is everywhere except the defect.
//
// The row's identifier is what a reader needs first and its first cell still
// holds it whatever the split did to the rest.
func TestRowWithALiteralPipeNamesItself(t *testing.T) {
	path := filepath.Join(t.TempDir(), ledgerFile)
	writeFile(t, path, testRegistry+tableHeader+"\n|---|---|---|---|\n"+
		"| A27.1 | a claim about success() || failure() | somewhere | A27 |\n"+
		"\n## Alternatives\n\n- **Assumption A27.1**: the alternative.\n")
	_, ps := loadLedger(path)
	if !hasProblem(ps, "A27.1") {
		t.Errorf("the problems do not name the broken ROW: %v", ps)
	}
	if !hasProblem(ps, "literal pipe") {
		t.Errorf("the problems do not say what to do instead: %v", ps)
	}
}

// TestPackageSymbolsSeeHowTheCodeIsWritten is the reason this check went from
// nineteen false findings to none: the ledger cites a driver's capability
// table as `CheckBroker`, which is a key inside a composite literal, and a rule
// set as `wrongProtocol`, which is a short variable declaration. Reading only
// top-level declarations reported both as deleted.
func TestPackageSymbolsSeeHowTheCodeIsWritten(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "x.go"), "package x\n\n"+
		"type T struct{ Field int }\n\n"+
		"func f() {\n"+
		"\tshortVar := T{Field: 1}\n"+
		"\t_ = shortVar\n"+
		"Label:\n"+
		"\tfor {\n\t\tbreak Label\n\t}\n"+
		"}\n")
	syms := packageSymbols(dir)
	for _, want := range []string{"T", "Field", "f", "shortVar", "Label"} {
		if !syms[want] {
			t.Errorf("packageSymbols did not see %q; got %v", want, keysOf(syms))
		}
	}
}

func keysOf(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

func TestIdentifiersDoNotCheckPaths(t *testing.T) {
	repo, ledger := identFixture(t, "`gone/away.go`: `Vanished`")
	rows, _ := loadLedger(ledger)
	v, ps := checkIdentifiers(repo, ledger, rows, repoModules(repo))
	if len(ps) != 0 {
		t.Fatalf("a path the tree does not hold was reported as a name: %v", ps)
	}
	if v.skipped == 0 {
		t.Error("the skipped-entry count is zero, so the gap this leaves is invisible")
	}
}

// TestIdentifiersAreOffByDefault is what keeps `make adr-check` answering in a
// second: this mode builds every module's test binaries.
func TestIdentifiersAreOffByDefault(t *testing.T) {
	data, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), `flag.Bool("identifiers", false,`) {
		t.Error("-identifiers is not declared with a false default")
	}
}

// TestATestNamedInTheProseIsACitation is where the backlog actually lived.
//
// Ten of the sixteen dangling citations were named in the Assumption column
// and not beside a file path: "which is what `TestOldBandPrefersTheUnmeasuredArm`
// was watched reporting", "`go test -run TestManifest` selected no test". A
// row's prose cites evidence exactly as much as its Where column does, so
// Test and Fuzz names are collected from the whole row while back-ticked Go
// identifiers are not — the prose legitimately names identifiers that used to
// exist, that another project has, or that a rejected alternative would have.
func TestATestNamedInTheProseIsACitation(t *testing.T) {
	repo := t.TempDir()
	writeFile(t, filepath.Join(repo, "go.mod"), "module example.test\n\ngo 1.26\n")
	writeFile(t, filepath.Join(repo, "thing.go"), "package thing\n\nconst Answer = 42\n")
	ledger := filepath.Join(repo, "adr", ledgerFile)
	writeFile(t, ledger, testRegistry+tableHeader+"\n|---|---|---|---|\n"+
		"| A27.1 | which is what `TestGoneFromTheProse` was watched reporting, and `alsoGoneButNotATest` | `thing.go`: `Answer` | A27 |\n"+
		"\n## Alternatives\n\n- **Assumption A27.1**: the alternative.\n")
	ps := runIdent(t, repo, ledger)
	hasAll(t, ps, "TestGoneFromTheProse")
	if hasProblem(ps, "alsoGoneButNotATest") {
		t.Errorf("an identifier in the PROSE was resolved; only the Where column's are: %v", ps)
	}
}
