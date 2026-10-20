// Command iqmq is the iqmq command-line tool: pub, sub, tail, bench and
// doctor.
//
// It is the first consumer of the DSN layer, the driver registry and the
// capability model from outside the library's own tests, and it is built to be
// held to the same rules as the library. Three of those rules shape everything
// below:
//
//   - the security checks: no credential reaches the terminal. The CLI renders
//     no DSN parameter and no configuration field itself; see redact.go for
//     the two layers and the four places a credential can appear.
//   - a capability the driver does not have is said out loud, with the
//     feature and the level that was read. `tail` refusing to tail is a fact
//     about the broker, not a limitation of this tool.
//   - the numbers `bench` prints are operational. The repository's
//     measured budgets come from the benchmarks module under a load guard on
//     a quiet host, and bench says so in its own output every time.
//
// Every command is driven through run(args, stdout, stderr), which never
// panics on any argument vector (FuzzCLIArgs) and never performs I/O the
// arguments did not ask for.
package main

import (
	"context"
	"errors"
	"flag"
	"io"
	"os"
	"os/signal"
	"runtime/debug"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/dsn"
)

// The process exit codes. They are a contract: a script branches on them, and
// the difference between "the broker said no" and "you typed it wrong" is the
// difference between retrying and editing.
const (
	// exitOK is success.
	exitOK = 0
	// exitFailure is an operation that ran and did not succeed: the broker
	// refused, a publication was not confirmed, a delivery could not be
	// settled, a deadline passed with the work unfinished.
	exitFailure = 1
	// exitUsage is an invocation this tool could not act on at all: an
	// unknown command, a malformed flag, a DSN that does not parse, a scheme
	// no driver claims.
	exitUsage = 2
	// exitUnsupported is a capability the driver genuinely lacks. It
	// is separate from exitFailure because nothing the caller retries will
	// change it, and separate from exitUsage because the invocation was
	// well formed: the answer is about the broker.
	exitUnsupported = 3
)

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

const usage = `usage: iqmq <command> [flags] [arguments]

commands:
  pub       publish messages to a topic
  sub       consume messages from a subscription and settle them
  tail      print messages as they arrive, settling nothing where the driver allows it
  bench     generate load against a DSN and report operational figures
  doctor    resolve a DSN and report capabilities, limits and variant bindings
  topology  plan and apply the topics and subscriptions a deployment needs
  bridge    move messages from one broker to another
  version   print the build version
  help      print this text, or "iqmq help <command>"

Every command that takes a target takes a DSN. "iqmq doctor" with no DSN lists
the schemes this binary can open.

exit codes:
  0  success
  1  the operation ran and did not succeed
  2  the invocation could not be acted on (bad flags, bad DSN, unknown scheme)
  3  the driver does not have a capability the command needs (the message contract)
`

// cliEnv is everything a command reaches outside itself.
//
// It exists so that the whole CLI is testable without a process: a test
// supplies its own registry (one driver, or none), its own streams, its own
// environment and its own clock. The flake policy asks for fake clocks
// instead of sleeps, and a CLI whose timeouts read time.Now cannot honour
// that -- so every wait in this package goes through cliEnv.clock.
type cliEnv struct {
	stdout io.Writer
	stderr io.Writer
	stdin  io.Reader
	// registry is the DSN registry this process opens targets through. The
	// binary owns it so that a test can replace it.
	registry *dsn.Registry
	// getenv reads the environment. A test gives its own, so no test has to
	// mutate the process environment to cover the third place a credential
	// can appear.
	getenv func(string) (string, bool)
	// clock is the time source every timeout, idle deadline and rate
	// measurement in this package reads.
	clock mq.Clock
	// ctx is cancelled when the process is interrupted.
	ctx context.Context
}

// run executes the CLI and returns the process exit code. It never panics on
// any argument vector, which is FuzzCLIArgs's whole subject, and it is the
// signature that fuzz target drives.
func run(args []string, stdout, stderr io.Writer) int {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	reg, err := newRegistry()
	if err != nil {
		// A scheme collision in drivers() is a build that cannot work; it is
		// reported rather than panicked so that `iqmq version` still answers
		// on a binary somebody has to diagnose.
		_, _ = io.WriteString(stderr, "iqmq: driver registration failed: "+err.Error()+"\n")
		return exitFailure
	}
	e := &cliEnv{
		stdout: stdout, stderr: stderr, stdin: os.Stdin,
		registry: reg, getenv: os.LookupEnv, clock: realClock{}, ctx: ctx,
	}
	return dispatch(e, args)
}

// dispatch routes one argument vector to a command.
func dispatch(e *cliEnv, args []string) int {
	if len(args) == 0 {
		_, _ = io.WriteString(e.stderr, usage)
		return exitUsage
	}
	switch args[0] {
	case "version", "-v", "--version":
		_, _ = io.WriteString(e.stdout, version()+"\n")
		return exitOK
	case "help", "-h", "--help", "-help":
		return help(e, args[1:])
	case "pub":
		return cmdPub(e, args[1:])
	case "sub":
		return cmdSub(e, args[1:])
	case "tail":
		return cmdTail(e, args[1:])
	case "bench":
		return cmdBench(e, args[1:])
	case "doctor":
		return cmdDoctor(e, args[1:])
	case "topology":
		return cmdTopology(e, args[1:])
	case "bridge":
		return cmdBridge(e, args[1:])
	default:
		_, _ = io.WriteString(e.stderr, "iqmq: unknown command "+quote(args[0])+"\n"+usage)
		return exitUsage
	}
}

// help prints the general usage, or one command's.
func help(e *cliEnv, args []string) int {
	if len(args) == 0 {
		_, _ = io.WriteString(e.stdout, usage)
		return exitOK
	}
	for _, c := range []struct {
		name  string
		usage string
	}{
		{"pub", pubUsage},
		{"sub", subUsage},
		{"tail", tailUsage},
		{"bench", benchUsage},
		{"doctor", doctorUsage},
		{"topology", topologyUsage},
		{"bridge", bridgeUsage},
	} {
		if c.name == args[0] {
			_, _ = io.WriteString(e.stdout, c.usage)
			return exitOK
		}
	}
	_, _ = io.WriteString(e.stderr, "iqmq help: no command "+quote(args[0])+"\n"+usage)
	return exitUsage
}

// The release stamps, set with -X by cmd/iqmq/.goreleaser.yaml.
//
// They are variables rather than a pure reading of the build info because a
// released binary has to print its TAG. debug.ReadBuildInfo reports
// Main.Version as "(devel)" for anything built from a directory rather than
// resolved by the module proxy -- which is every goreleaser build, since
// cmd/iqmq reaches the root module through a replace directive -- so a binary
// that relied on the build info alone would print "devel-<sha>" on a release
// artefact.
//
// A build that does not set them falls back to the build info, which is what a
// `go build` and a `go run` do.
var (
	buildVersion = ""
	buildCommit  = ""
	buildDate    = ""
)

// version renders the build version.
func version() string {
	if buildVersion != "" {
		out := "iqmq " + buildVersion
		if buildCommit != "" {
			out += " " + shortCommit(buildCommit)
		}
		if buildDate != "" {
			out += " " + buildDate
		}
		if info, ok := debug.ReadBuildInfo(); ok {
			out += " " + info.GoVersion
		}
		return out
	}
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return "iqmq (unknown)"
	}
	v := info.Main.Version
	if v == "" || v == "(devel)" {
		for _, s := range info.Settings {
			if s.Key == "vcs.revision" && len(s.Value) >= 12 {
				v = "devel-" + s.Value[:12]
			}
		}
	}
	return "iqmq " + v + " " + info.GoVersion
}

// shortCommit abbreviates a commit to twelve characters, which is what the
// rest of this repository's tooling prints.
func shortCommit(c string) string {
	if len(c) > 12 {
		return c[:12]
	}
	return c
}

// newFlagSet returns a flag set that writes to the command's own error stream
// and never prints the flag package's own usage, which would bypass the usage
// text each command documents itself with.
func newFlagSet(name string, e *cliEnv) *flag.FlagSet {
	fs := flag.NewFlagSet("iqmq "+name, flag.ContinueOnError)
	fs.SetOutput(e.stderr)
	fs.Usage = func() {}
	return fs
}

// permute moves the flags of an argument vector in front of its positional
// arguments, and stops at "--".
//
// Go's flag package stops parsing at the FIRST non-flag argument, so
// `iqmq pub memory:// orders --header k=v body` would take `--header`, `k=v`
// and `body` as three positionals and report "a DSN and a topic are
// required" — which is true of nothing the caller typed. Every CLI a user has
// met accepts a flag after a positional, and a tool whose flags only work in
// one order is a tool people get wrong every time.
//
// The cost is that a positional beginning with "-" has to be written after a
// "--" terminator, which matters for exactly one case: a `pub` body that
// starts with a dash. Every command's usage says so.
//
// It asks the flag set itself whether a flag takes a value, so a boolean flag
// does not swallow the argument after it; a flag the set does not know is
// passed through for the flag package to report, because guessing would turn
// a typo into a positional.
func permute(fs *flag.FlagSet, args []string) []string {
	flags := make([]string, 0, len(args))
	positional := make([]string, 0, len(args))
	for i := 0; i < len(args); i++ {
		a := args[i]
		switch {
		case a == "--":
			positional = append(positional, args[i+1:]...)
			return append(flags, positional...)
		case a == "-" || !strings.HasPrefix(a, "-"):
			positional = append(positional, a)
		case strings.Contains(a, "="):
			// -name=value carries its own value.
			flags = append(flags, a)
		default:
			flags = append(flags, a)
			f := fs.Lookup(strings.TrimLeft(a, "-"))
			if f == nil {
				continue // the flag package reports it
			}
			if b, ok := f.Value.(interface{ IsBoolFlag() bool }); ok && b.IsBoolFlag() {
				continue // -v takes no value
			}
			if i+1 < len(args) {
				i++
				flags = append(flags, args[i])
			}
		}
	}
	return append(flags, positional...)
}

// parseFlags parses a command's flags and reports the exit code to return, or
// -1 to carry on. flag.ErrHelp is the -h form and is a success.
func parseFlags(fs *flag.FlagSet, e *cliEnv, args []string, usageText string) int {
	if err := fs.Parse(permute(fs, args)); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			_, _ = io.WriteString(e.stdout, usageText)
			return exitOK
		}
		// flag has already written the failure to e.stderr.
		_, _ = io.WriteString(e.stderr, usageText)
		return exitUsage
	}
	return -1
}

// codeFor maps an error to the exit code it deserves (the codes).
//
// The mapping is the point of having three failure codes: a capability the
// driver lacks is CodeUnsupported and nothing a caller retries will change it;
// a configuration the caller wrote is CodeInvalidConfig or
// CodeInvalidArgument and the caller edits it; everything else is an
// operation that ran and failed.
func codeFor(err error) int {
	switch mq.CodeOf(err) {
	case mq.CodeUnsupported, mq.CodeNotSettleable:
		return exitUnsupported
	case mq.CodeInvalidConfig, mq.CodeInvalidArgument, mq.CodeDriverNotRegistered:
		return exitUsage
	default:
		return exitFailure
	}
}

// fail writes one error line and returns the code it deserves. The message
// carries the redacted target and never the DSN it was given.
func fail(e *cliEnv, cmd string, err error) int {
	_, _ = io.WriteString(e.stderr, "iqmq "+cmd+": "+err.Error()+"\n")
	return codeFor(err)
}

// open resolves a DSN through the registry.
//
// It separates the three failures a caller has to tell apart: a DSN that is
// not a connection string, a scheme nobody claimed (naming the schemes this
// binary does claim), and a driver that could not open it.
func (e *cliEnv) open(ctx context.Context, cmd, target string) (*mq.Client, int) {
	scheme, err := dsn.Scheme(target)
	if err != nil {
		_, _ = io.WriteString(e.stderr, "iqmq "+cmd+": "+err.Error()+"\n")
		return nil, exitUsage
	}
	if _, ok := e.registry.Lookup(scheme); !ok {
		_, _ = io.WriteString(e.stderr, "iqmq "+cmd+": no driver in this binary claims the scheme "+
			quote(scheme)+"\n  this binary opens: "+strings.Join(e.registry.Schemes(), " ")+"\n")
		return nil, exitUsage
	}
	client, err := e.registry.Open(ctx, target)
	if err != nil {
		_, _ = io.WriteString(e.stderr, "iqmq "+cmd+": opening "+redactTarget(target)+": "+err.Error()+"\n")
		return nil, codeFor(err)
	}
	return client, -1
}

// deadline returns a context bounded by d on the environment's clock, and the
// function that releases it. A zero or negative d is no bound of its own.
//
// It is the clock-injected form of context.WithTimeout: the specification asks for a fake
// clock instead of a sleep, and a CLI test for "--timeout expired" has to be
// able to move time rather than wait for it.
func (e *cliEnv) deadline(parent context.Context, d time.Duration) (context.Context, func()) {
	ctx, cancel, _ := e.expiring(parent, d)
	return ctx, cancel
}

// expiring is deadline with the third answer a caller sometimes needs: whether
// it was the timer that cancelled the context, rather than the caller or an
// interrupt.
//
// That distinction is load-bearing for `sub --idle`: "no message arrived for
// five seconds" and "you pressed Ctrl-C" both surface as context.Canceled from
// Receive, and a run that reported the wrong one would be lying about why it
// stopped. It cannot be recovered from the error, because a cancelled context
// carries no cause through the driver.
func (e *cliEnv) expiring(parent context.Context, d time.Duration) (context.Context, func(), func() bool) {
	if d <= 0 {
		ctx, cancel := context.WithCancel(parent)
		return ctx, cancel, func() bool { return false }
	}
	ctx, cancel := context.WithCancel(parent)
	t := e.clock.NewTimer(d)
	var fired atomic.Bool
	done := make(chan struct{})
	go func() {
		defer close(done)
		select {
		case <-t.C():
			fired.Store(true)
			cancel()
		case <-ctx.Done():
			t.Stop()
		}
	}()
	return ctx, func() {
		cancel()
		<-done
	}, fired.Load
}

// realClock is the wall clock, which follows the bubble clock inside
// testing/synctest.
type realClock struct{}

// Now returns the current time.
func (realClock) Now() time.Time { return time.Now() }

// NewTimer returns a single-shot timer.
func (realClock) NewTimer(d time.Duration) mq.Timer { return realTimer{time.NewTimer(d)} }

// realTimer adapts *time.Timer to mq.Timer.
type realTimer struct{ t *time.Timer }

// C returns the expiry channel.
func (r realTimer) C() <-chan time.Time { return r.t.C }

// Stop prevents the timer from firing.
func (r realTimer) Stop() bool { return r.t.Stop() }

// Reset re-arms the timer.
func (r realTimer) Reset(d time.Duration) bool { return r.t.Reset(d) }

// quote renders a token for a message. It is strconv.Quote, named so that a
// reader of an error line can see that the value is the caller's.
func quote(s string) string { return strconv.Quote(s) }

// itoa renders an integer.
func itoa(n int) string { return strconv.Itoa(n) }

// i64toa renders a 64-bit integer.
func i64toa(n int64) string { return strconv.FormatInt(n, 10) }
