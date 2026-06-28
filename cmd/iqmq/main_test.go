package main

import (
	"bytes"
	"strings"
	"testing"
)

// TestDispatch holds the top-level argument vector to the exit-code contract.
func TestDispatch(t *testing.T) {
	cases := []struct {
		name string
		args []string
		code int
		out  string
		err  string
	}{
		{"no arguments", nil, exitUsage, "", "usage:"},
		{"version", []string{"version"}, exitOK, "iqmq ", ""},
		{"version short", []string{"-v"}, exitOK, "iqmq ", ""},
		{"help", []string{"--help"}, exitOK, "usage:", ""},
		{"help of a command", []string{"help", "doctor"}, exitOK, "iqmq doctor", ""},
		{"help of no command", []string{"help", "frobnicate"}, exitUsage, "", "no command"},
		{"unknown command", []string{"bogus"}, exitUsage, "", "unknown command"},
		{"doctor of the build", []string{"doctor", "--drivers"}, exitOK, "build", ""},
		{"pub with no topic", []string{"pub", memDSN}, exitUsage, "", "a DSN and a topic"},
		{"sub with no subscription", []string{"sub", memDSN, "t"}, exitUsage, "", "a subscription are required"},
		{"tail with no topic", []string{"tail", memDSN}, exitUsage, "", "a DSN and a topic"},
		{"bench with no topic", []string{"bench", memDSN}, exitUsage, "", "a DSN and a topic"},
		{"topology with no subcommand", []string{"topology"}, exitUsage, "", "plan or apply"},
		{"topology plan with no DSN", []string{"topology", "plan"}, exitUsage, "", "a DSN is required"},
		{"bridge with too few arguments", []string{"bridge", memDSN, "t"}, exitUsage, "", "are required"},
		{"help of topology", []string{"help", "topology"}, exitOK, "iqmq topology", ""},
		{"help of bridge", []string{"help", "bridge"}, exitOK, "iqmq bridge", ""},
		{"unknown flag", []string{"doctor", "--nonsense"}, exitUsage, "", "flag provided but not defined"},
		{"bad output format", []string{"doctor", "-o", "yaml"}, exitUsage, "", "text or json"},
		{"unclaimed scheme", []string{"doctor", "nosuchbroker://host"}, exitUsage, "", "no driver"},
		{"not a DSN at all", []string{"doctor", "just-a-word"}, exitUsage, "", "connection string"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			h := newFullHarness(t)
			if got := h.run(c.args...); got != c.code {
				t.Errorf("run(%v) = %d, want %d\nstdout:\n%s\nstderr:\n%s",
					c.args, got, c.code, h.stdout(), h.stderr())
			}
			if c.out != "" && !strings.Contains(h.stdout(), c.out) {
				t.Errorf("run(%v) stdout does not contain %q:\n%s", c.args, c.out, h.stdout())
			}
			if c.err != "" && !strings.Contains(h.stderr(), c.err) {
				t.Errorf("run(%v) stderr does not contain %q:\n%s", c.args, c.err, h.stderr())
			}
		})
	}
}

// TestUnclaimedSchemeNamesWhatTheBinaryDoesClaim: "no driver for that scheme"
// is only half an answer, and the other half is the list.
func TestUnclaimedSchemeNamesWhatTheBinaryDoesClaim(t *testing.T) {
	h := newFullHarness(t)
	if got := h.run("pub", "nosuchbroker://host", "topic", "body"); got != exitUsage {
		t.Fatalf("exit = %d, want %d", got, exitUsage)
	}
	for _, want := range []string{"nosuchbroker", "this binary opens", "memory", "kafka"} {
		if !strings.Contains(h.stderr(), want) {
			t.Errorf("the refusal does not mention %q:\n%s", want, h.stderr())
		}
	}
}

// TestEveryNonZeroExitWritesAMessage is the property FuzzCLIArgs enforces over
// arbitrary vectors, held here over the curated ones so that a failure names
// the case.
func TestEveryNonZeroExitWritesAMessage(t *testing.T) {
	for _, args := range [][]string{
		nil,
		{"bogus"},
		{"pub"},
		{"sub"},
		{"tail"},
		{"bench"},
		{"doctor", "--nonsense"},
		{"doctor", "a", "b"},
		{"pub", "memory://", "t", "--confirm", "sideways"},
		{"sub", memDSN, "t", "s", "--settle", "maybe"},
		{"sub", memDSN, "t", "s", "--concurrency", "0"},
		{"bench", memDSN, "t", "--messages", "0"},
		{"tail", memDSN, "t", "x", "y"},
		{"topology"},
		{"topology", "destroy", memDSN},
		{"topology", "plan", memDSN},
		{"topology", "plan", memDSN, "--subscription", "no-equals-sign"},
		{"topology", "apply", "--no-connect", memDSN, "--topic", "t"},
		{"bridge"},
		{"bridge", memDSN, "t", "s", memDSN},
		{"bridge", memDSN, "t", "s", memDSN, "u", "--headers", "maybe"},
	} {
		h := newFullHarness(t)
		code := h.run(args...)
		if code == exitOK {
			t.Errorf("run(%v) succeeded; it is a malformed invocation", args)
			continue
		}
		if h.stderr() == "" {
			t.Errorf("run(%v) = %d and wrote nothing to stderr", args, code)
		}
	}
}

// FuzzCLIArgs drives the whole argument vector.
//
// The registry is EMPTY, so no vector the fuzzer invents can open a target and
// the target is never network I/O: a fuzz target that dialled whatever host a
// random DSN named would be a port scanner with a corpus. What it holds is the
// three properties that do not need a broker:
//
//  1. no vector panics;
//  2. the exit code is one of the four the contract defines;
//  3. a non-zero exit always says something, and exit 0 only ever happens for
//     an informational invocation -- which, with no driver registered, is the
//     only thing that can succeed.
func FuzzCLIArgs(f *testing.F) {
	for _, seed := range []string{
		"", "version", "help", "help doctor", "--help", "-v",
		"doctor", "doctor --drivers", "doctor --no-connect memory://",
		"pub", "pub memory:// t body", "pub -H k=v memory:// t body",
		"sub memory:// t s --max 1", "tail memory:// t", "bench memory:// t --messages 1",
		"doctor -o json memory://", "pub --confirm durable memory:// t b",
		"sub --settle none memory:// t s", "bench --variants memory:// t",
		"-", "--", "---", "pub -", "doctor ://", "doctor %%",
		"pub memory:// t --priority 999", "sub memory:// t s --max -1",
		"topology", "topology plan", "topology apply", "topology -h",
		"topology plan memory:// --topic t", "topology apply memory:// --topic t",
		"topology plan --no-connect memory:// --topic t",
		"topology plan memory:// --subscription t=s", "topology plan memory:// nosuchfile.json",
		"topology plan -o json memory:// --topic t", "topology frobnicate memory://",
		"topology help", "topology help memory://",
		"bridge", "bridge memory:// t s memory:// u", "bridge --dry-run memory:// t s memory:// u",
		"bridge --dedup-from id memory:// t s memory:// u", "bridge --max 1 a:// t s b:// u",
	} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, argv string) {
		h := newEmptyHarness(t)
		args := strings.Fields(argv)
		code := h.run(args...)
		switch code {
		case exitOK, exitFailure, exitUsage, exitUnsupported:
		default:
			t.Fatalf("run(%q) = %d, which is not one of the four documented exit codes", argv, code)
		}
		if code != exitOK && h.stderr() == "" {
			t.Fatalf("run(%q) = %d and wrote nothing to stderr", argv, code)
		}
		if code == exitOK && !informational(args) {
			t.Fatalf("run(%q) succeeded with no driver registered, and it is not an "+
				"informational invocation; stdout:\n%s", argv, h.stdout())
		}
	})
}

// informational reports whether an argument vector COULD succeed with no
// driver registered.
//
// The sharp half is the commands that are absent from it: pub, sub, tail and
// bench all have to open a target, so with an empty registry not one of them
// can exit 0 whatever flags it was given. That makes "exit 0 implies
// informational" a real property rather than a restatement of the code -- a
// refactor that let `pub` report success without publishing would fail the
// fuzz target.
//
// It deliberately does NOT try to work out whether `doctor` was given a
// target: that needs the flag set (`-o json` puts a bare word in the vector
// that is a flag's value and not a positional), and a second, cruder parser
// here would disagree with the real one. `doctor` with no target reports the
// build, which needs no driver, so the whole command is allowed to succeed.
func informational(args []string) bool {
	if len(args) == 0 {
		return false
	}
	switch args[0] {
	case "version", "-v", "--version", "-h", "--help", "-help", "doctor":
		return true
	}
	// topology and bridge are deliberately ABSENT, like pub, sub, tail and
	// bench: both have to resolve a scheme through the registry before they
	// can do anything, so with an empty registry neither can exit 0 however
	// it is invoked -- including `topology plan --no-connect`, which opens no
	// connection and still refuses a scheme no driver claims.
	// Any command asked for its own usage prints it and succeeds, whether it
	// was asked with a flag the flag package answers with ErrHelp or with one
	// of the bare words a dispatcher routes itself.
	if helpWord(args) {
		return true
	}
	for _, a := range args[1:] {
		if helpRequest(a) {
			return true
		}
	}
	return false
}

// helpWord reports whether this vector reaches a dispatcher that answers the
// BARE WORD "help" -- no dash, so helpRequest can never match it -- with a
// usage on stdout and exit 0.
//
// Two dispatchers route an argument themselves before any flag set sees it,
// and both take the word: dispatch (main.go:144) at the top level, and
// cmdTopology (topology.go:299) for topology's own subcommand. Neither looks
// at what follows, so `topology help` and `topology help <anything>` both
// print topologyUsage and succeed. No other command does this -- pub, sub,
// tail, bench, bridge and doctor hand their whole vector to a flag set, where
// a bare word is a positional or an error, never help; `topology` is the only
// args[0] switch below the top level in the binary (the sweep is held by
// TestOnlyTheKnownDispatchersAnswerABareHelp).
//
// Leaving this out is what the FuzzCLIArgs vector `topology help` found after
// helpRequest replaced the three literal spellings: exit 0, informational
// false, which is the same false violation issues #65 and #56 reported in
// their flag form.
func helpWord(args []string) bool {
	if len(args) == 0 {
		return false
	}
	if args[0] == "help" {
		return true
	}
	return args[0] == "topology" && len(args) > 1 && args[1] == "help"
}

// helpRequest reports whether the flag package will answer this argument with
// flag.ErrHelp, which parseFlags turns into the command's usage on stdout and
// exit 0 (main.go:316-324).
//
// It has to mirror (*flag.FlagSet).parseOne's special case rather than list the
// three spellings a human would type, because the package applies it to any
// UNDEFINED flag whose name is "h" or "help" after the dashes are stripped and
// an "=value" is split off: `--h`, `-h=`, `--help=anything` all request help
// just as `-h` does. No command in this binary defines a flag called h or help,
// so the special case always applies. Listing spellings instead is what issues
// #65 (`topology plan -h=`) and #56 (`bench --h`) found: the CLI printed its
// usage and exited 0, correctly, and this oracle called that a violation.
func helpRequest(a string) bool {
	if len(a) < 2 || a[0] != '-' {
		return false
	}
	dashes := 1
	if a[1] == '-' {
		dashes = 2
		if len(a) == 2 {
			return false // "--" terminates the vector
		}
	}
	name := a[dashes:]
	if name[0] == '-' || name[0] == '=' {
		return false // flag reports this as bad flag syntax
	}
	if i := strings.IndexByte(name, '='); i > 0 {
		name = name[:i]
	}
	return name == "h" || name == "help"
}

// TestEveryHelpSpellingPrintsTheUsage holds the half of informational that the
// two crashers above falsified, so widening helpRequest is a claim about the
// CLI that can fail rather than a blank cheque: each vector really does print
// the command's own usage and succeed, and a spelling the flag package does NOT
// treat as help really does not.
func TestEveryHelpSpellingPrintsTheUsage(t *testing.T) {
	for _, c := range []struct {
		args []string
		help bool
	}{
		{[]string{"bench", "-h"}, true},
		{[]string{"bench", "--h"}, true},   // issue #56
		{[]string{"bench", "-help"}, true}, //
		{[]string{"bench", "--help"}, true},
		{[]string{"bench", "-h="}, true},
		{[]string{"bench", "--help=anything"}, true},
		{[]string{"topology", "plan", "-h="}, true}, // issue #65
		{[]string{"bench", "---h"}, false},          // bad flag syntax, not help
		{[]string{"bench", "-hh"}, false},           // undefined flag
	} {
		t.Run(strings.Join(c.args, " "), func(t *testing.T) {
			if got := helpRequest(c.args[len(c.args)-1]); got != c.help {
				t.Errorf("helpRequest(%q) = %v, want %v", c.args[len(c.args)-1], got, c.help)
			}
			h := newEmptyHarness(t)
			code := h.run(c.args...)
			if c.help {
				if code != exitOK {
					t.Errorf("run(%v) = %d, want %d\nstderr:\n%s", c.args, code, exitOK, h.stderr())
				}
				if !strings.Contains(h.stdout(), "usage: iqmq "+c.args[0]) {
					t.Errorf("run(%v) printed no usage for %s:\n%s", c.args, c.args[0], h.stdout())
				}
				return
			}
			if code != exitUsage {
				t.Errorf("run(%v) = %d, want %d; it is not a help request\nstdout:\n%s",
					c.args, code, exitUsage, h.stdout())
			}
		})
	}
}

// TestOnlyTheKnownDispatchersAnswerABareHelp sweeps every command in dispatch
// with the bare word `help` and holds what each one does with it, so the
// non-flag half of informational cannot silently go stale.
//
// Today exactly two routes answer the word: the top-level `help` and
// `topology help` (topology.go:304). The rest give exit 2, because their whole
// vector goes to a flag set that reads `help` as a positional. If a
// dispatcher is added, or one of these grows a `case "help"` of its own, its
// row here fails -- and so does FuzzCLIArgs, for the same reason, which is the
// gap this test exists to keep shut.
func TestOnlyTheKnownDispatchersAnswerABareHelp(t *testing.T) {
	for _, c := range []struct {
		args  []string
		code  int
		usage string // expected on stdout when it succeeds
	}{
		{[]string{"help"}, exitOK, "usage: iqmq <command>"},
		{[]string{"help", "topology"}, exitOK, "usage: iqmq topology"},
		{[]string{"topology", "help"}, exitOK, "usage: iqmq topology"},
		{[]string{"topology", "help", memDSN}, exitOK, "usage: iqmq topology"},
		{[]string{"topology", "help", "doctor"}, exitOK, "usage: iqmq topology"},
		{[]string{"version", "help"}, exitOK, ""},
		{[]string{"pub", "help"}, exitUsage, ""},
		{[]string{"sub", "help"}, exitUsage, ""},
		{[]string{"tail", "help"}, exitUsage, ""},
		{[]string{"bench", "help"}, exitUsage, ""},
		{[]string{"bridge", "help"}, exitUsage, ""},
		{[]string{"doctor", "help"}, exitUsage, ""},
		{[]string{"frobnicate", "help"}, exitUsage, ""},
	} {
		t.Run(strings.Join(c.args, " "), func(t *testing.T) {
			h := newEmptyHarness(t)
			code := h.run(c.args...)
			if code != c.code {
				t.Errorf("run(%v) = %d, want %d\nstdout:\n%s\nstderr:\n%s",
					c.args, code, c.code, h.stdout(), h.stderr())
			}
			if c.usage != "" && !strings.Contains(h.stdout(), c.usage) {
				t.Errorf("run(%v) printed no %q:\n%s", c.args, c.usage, h.stdout())
			}
			// FuzzCLIArgs' third property, on this one vector.
			if code == exitOK && !informational(c.args) {
				t.Errorf("run(%v) exited 0 and informational says it is not an "+
					"informational invocation", c.args)
			}
		})
	}
}

// TestTheDriverlessCommandsCannotSucceed is informational's sharp half held
// directly, so that the fuzz property above cannot be satisfied by a
// characterisation that happens to be vacuous.
func TestTheDriverlessCommandsCannotSucceed(t *testing.T) {
	for _, args := range [][]string{
		{"pub", memDSN, "t", "body"},
		{"sub", memDSN, "t", "s", "--max", "1"},
		{"tail", memDSN, "t"},
		{"bench", memDSN, "t", "--messages", "1"},
		{"pub", "memory://", "t", "-f", "-"},
		{"bench", memDSN, "t", "--variants"},
		{"topology", "plan", memDSN, "--topic", "orders"},
		{"topology", "plan", "--no-connect", memDSN, "--topic", "orders"},
		{"topology", "apply", memDSN, "--topic", "orders"},
		{"bridge", "--max", "1", memDSN, "t", "s", memDSN, "u"},
	} {
		h := newEmptyHarness(t)
		if got := h.run(args...); got == exitOK {
			t.Errorf("run(%v) succeeded with NO driver registered:\nstdout:\n%s",
				args, h.stdout())
		}
		if informational(args) {
			t.Errorf("informational(%v) = true; it must be false, or the fuzz property is "+
				"vacuous for this command", args)
		}
	}
}

// TestVersionPrefersTheReleaseStamps covers the goreleaser path: a released
// binary has to print its tag, and debug.ReadBuildInfo cannot give it one.
func TestVersionPrefersTheReleaseStamps(t *testing.T) {
	oldV, oldC, oldD := buildVersion, buildCommit, buildDate
	t.Cleanup(func() { buildVersion, buildCommit, buildDate = oldV, oldC, oldD })

	buildVersion, buildCommit, buildDate = "1.2.3", "0123456789abcdef0123", "2026-09-15T00:00:00Z"
	got := version()
	for _, want := range []string{"iqmq 1.2.3", "0123456789ab", "2026-09-15T00:00:00Z"} {
		if !strings.Contains(got, want) {
			t.Errorf("version() = %q, want it to contain %q", got, want)
		}
	}
	if strings.Contains(got, "0123456789abcdef0123") {
		t.Errorf("version() = %q, and it printed the full commit rather than the short one", got)
	}

	buildVersion, buildCommit, buildDate = "", "", ""
	if got := version(); !strings.HasPrefix(got, "iqmq ") {
		t.Errorf("version() with no stamps = %q; the build-info fallback is broken", got)
	}
}

// TestRunBuildsARealEnvironment covers run() itself, which the harness
// bypasses: it is the function main calls and the one FuzzCLIArgs's signature
// belongs to, so a break in its registry construction would be invisible.
func TestRunBuildsARealEnvironment(t *testing.T) {
	var out, errb bytes.Buffer
	if got := run([]string{"version"}, &out, &errb); got != exitOK {
		t.Fatalf("run(version) = %d, stderr: %s", got, errb.String())
	}
	if !strings.HasPrefix(out.String(), "iqmq ") {
		t.Errorf("run(version) stdout = %q", out.String())
	}
	out.Reset()
	errb.Reset()
	if got := run([]string{"doctor", "--drivers"}, &out, &errb); got != exitOK {
		t.Fatalf("run(doctor --drivers) = %d, stderr: %s", got, errb.String())
	}
	if !strings.Contains(out.String(), "schemes this binary opens") {
		t.Errorf("run(doctor --drivers) did not report the schemes:\n%s", out.String())
	}
}
