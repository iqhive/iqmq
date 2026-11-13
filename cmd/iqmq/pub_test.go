package main

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/dsn"
	"github.com/iqhive/iqmq/memory"
)

// memoryCaps opens a memory client and returns its advertised capabilities,
// so that a test asserting a capability refusal asserts the right one.
func memoryCaps(t *testing.T) mq.Capabilities {
	t.Helper()
	client, err := memory.OpenDSN(context.Background(), memDSN)
	if err != nil {
		t.Fatalf("opening the memory driver: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return client.Capabilities()
}

// TestRoundTripInOneInvocation covers the only way a memory target can carry
// a message from a publish to a consume.
//
// memory.Open builds a NEW broker per call, so `pub memory://` and
// `sub memory://` are two brokers and see nothing of each other.
// `bench --subscription` publishes and consumes through
// ONE client, so it is the round trip a memory DSN can actually do; the
// two-command round trip is TestSubSettlesWhatItPrinted, over a shared broker.
func TestRoundTripInOneInvocation(t *testing.T) {
	h := newHarness(t)
	code := h.run("bench", memDSN, "orders", "--messages", "3", "--size", "8",
		"--subscription", "billing")
	if code != exitOK {
		t.Fatalf("bench round trip = %d\nstdout:\n%s\nstderr:\n%s", code, h.stdout(), h.stderr())
	}
	out := h.stdout()
	if !strings.Contains(out, "consumed") {
		t.Errorf("the round trip consumed nothing:\n%s", out)
	}
}

// TestPubReportsConfirmationExplicitly is the specification read off the output: a caller
// has to be able to see which boundary the publication waited for.
func TestPubReportsConfirmationExplicitly(t *testing.T) {
	for _, c := range []struct{ flag, want string }{
		{"default", "accepted"},
		{"accepted", "accepted"},
	} {
		h := newHarness(t)
		code := h.run("pub", memDSN, "orders", "--confirm", c.flag, "hello")
		if code != exitOK {
			t.Fatalf("pub --confirm %s = %d\nstderr:\n%s", c.flag, code, h.stderr())
		}
		out := h.stdout()
		if !strings.Contains(out, "confirmation "+c.want) {
			t.Errorf("pub --confirm %s did not report the resolved boundary %q:\n%s",
				c.flag, c.want, out)
		}
		if !strings.Contains(out, "confirmed 1 of 1") {
			t.Errorf("pub did not report what was confirmed:\n%s", out)
		}
	}
}

// TestPubDurableConfirmationIsACapabilityQuestion: --confirm durable fails at
// construction where the profile cannot define a durable boundary, and the
// exit code says it is not a retryable failure.
func TestPubDurableConfirmationIsACapabilityQuestion(t *testing.T) {
	caps := memoryCaps(t)
	durable := caps.Support(mq.FeatureDurableConfirm)

	h := newHarness(t)
	code := h.run("pub", memDSN, "orders", "--confirm", "durable", "hello")
	switch durable.Level {
	case mq.Unsupported:
		if code != exitUnsupported {
			t.Errorf("the memory driver advertises DurableConfirm=%s and pub --confirm durable "+
				"exited %d, want %d\nstderr:\n%s", durable.Level, code, exitUnsupported, h.stderr())
		}
	default:
		if code != exitOK {
			t.Errorf("the memory driver advertises DurableConfirm=%s and pub --confirm durable "+
				"exited %d\nstderr:\n%s", durable.Level, code, h.stderr())
		}
	}
	t.Logf("memory advertises %s; pub --confirm durable exited %d", supportLine(caps, mq.FeatureDurableConfirm), code)
}

// TestPubReceiptNeedsTheFeature is the fail-fast through a flag: the
// refusal names the feature and is exit 3, because no retry changes it.
func TestPubReceiptNeedsTheFeature(t *testing.T) {
	caps := memoryCaps(t)
	h := newHarness(t)
	code := h.run("pub", memDSN, "orders", "--receipt", "hello")
	if caps.Support(mq.FeatureReceipts).Level == mq.Unsupported {
		if code != exitUnsupported {
			t.Errorf("the memory driver advertises no Receipts and pub --receipt exited %d, "+
				"want %d\nstderr:\n%s", code, exitUnsupported, h.stderr())
		}
		if !strings.Contains(h.stderr(), "Receipts") && !strings.Contains(h.stderr(), "receipt") {
			t.Errorf("the refusal does not name the feature:\n%s", h.stderr())
		}
		return
	}
	if code != exitOK {
		t.Errorf("the memory driver advertises %s and pub --receipt exited %d\nstderr:\n%s",
			supportLine(caps, mq.FeatureReceipts), code, h.stderr())
	}
	if !strings.Contains(h.stdout(), "receipt 0") {
		t.Errorf("pub --receipt printed no receipt:\n%s", h.stdout())
	}
}

// TestPubDelayNeedsTheFeature is the same shape for the delay.
func TestPubDelayNeedsTheFeature(t *testing.T) {
	caps := memoryCaps(t)
	h := newHarness(t)
	code := h.run("pub", memDSN, "orders", "--delay", "1h", "hello")
	if caps.Support(mq.FeatureScheduledPublish).Level == mq.Unsupported {
		if code != exitUnsupported {
			t.Errorf("no ScheduledPublish and pub --delay exited %d, want %d\nstderr:\n%s",
				code, exitUnsupported, h.stderr())
		}
		return
	}
	if code != exitOK {
		t.Errorf("%s and pub --delay exited %d\nstderr:\n%s",
			supportLine(caps, mq.FeatureScheduledPublish), code, h.stderr())
	}
}

// TestPubBodiesFromEveryInput covers the three ways a body arrives.
func TestPubBodiesFromEveryInput(t *testing.T) {
	t.Run("positional", func(t *testing.T) {
		h := newHarness(t)
		if got := h.run("pub", memDSN, "orders", "one", "two", "three"); got != exitOK {
			t.Fatalf("exit %d: %s", got, h.stderr())
		}
		if !strings.Contains(h.stdout(), "messages 3") {
			t.Errorf("three positional bodies did not become three messages:\n%s", h.stdout())
		}
	})
	t.Run("stdin lines", func(t *testing.T) {
		h := newHarness(t).withStdin("a\nb\nc\n")
		if got := h.run("pub", memDSN, "orders", "--file", "-"); got != exitOK {
			t.Fatalf("exit %d: %s", got, h.stderr())
		}
		if !strings.Contains(h.stdout(), "messages 3") {
			t.Errorf("a trailing newline produced the wrong count:\n%s", h.stdout())
		}
	})
	t.Run("stdin whole", func(t *testing.T) {
		h := newHarness(t).withStdin("a\nb\nc\n")
		if got := h.run("pub", memDSN, "orders", "--file", "-", "--no-lines"); got != exitOK {
			t.Fatalf("exit %d: %s", got, h.stderr())
		}
		if !strings.Contains(h.stdout(), "messages 1") {
			t.Errorf("--no-lines did not produce one message:\n%s", h.stdout())
		}
	})
	t.Run("file", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "bodies.ndjson")
		if err := os.WriteFile(path, []byte("{\"a\":1}\n{\"a\":2}\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		h := newHarness(t)
		if got := h.run("pub", memDSN, "orders", "--file", path); got != exitOK {
			t.Fatalf("exit %d: %s", got, h.stderr())
		}
		if !strings.Contains(h.stdout(), "messages 2") {
			t.Errorf("the file did not produce two messages:\n%s", h.stdout())
		}
	})
	t.Run("no body at all", func(t *testing.T) {
		h := newHarness(t)
		if got := h.run("pub", memDSN, "orders"); got != exitUsage {
			t.Errorf("pub with no body = %d, want %d", got, exitUsage)
		}
		if !strings.Contains(h.stderr(), "no body") {
			t.Errorf("the refusal does not say what is missing:\n%s", h.stderr())
		}
	})
}

// TestPubBatchIsOneCall covers the batch path and, more importantly, that its
// message count is right: a batch counted as one message would understate
// every batched run.
func TestPubBatchIsOneCall(t *testing.T) {
	h := newHarness(t).withStdin("a\nb\nc\nd\n")
	if got := h.run("pub", memDSN, "orders", "--file", "-", "--batch"); got != exitOK {
		t.Fatalf("exit %d: %s", got, h.stderr())
	}
	if !strings.Contains(h.stdout(), "confirmed 4 of 4") {
		t.Errorf("--batch did not confirm four messages:\n%s", h.stdout())
	}
}

// TestPubHeadersAndKey covers the flags that reach and.
func TestPubHeadersAndKey(t *testing.T) {
	h := newHarness(t)
	code := h.run("pub", memDSN, "orders", "-H", "trace=abc", "-H", "trace=def",
		"-k", "customer-7", "hello")
	if code != exitOK {
		t.Fatalf("exit %d: %s", code, h.stderr())
	}
	// A malformed header is a usage failure naming the form.
	h2 := newHarness(t)
	if got := h2.run("pub", memDSN, "orders", "-H", "nonsense", "hello"); got != exitUsage {
		t.Errorf("a header with no = exited %d, want %d", got, exitUsage)
	}
	if !strings.Contains(h2.stderr(), "key=value") {
		t.Errorf("the refusal does not say the form:\n%s", h2.stderr())
	}
}

// TestHeaderFlagStringPrintsKeysOnly: the flag package prints a flag's value
// in some diagnostics, and a header value can be a bearer token.
func TestHeaderFlagStringPrintsKeysOnly(t *testing.T) {
	var f headerFlag
	if err := f.Set("authorization=Bearer " + credToken); err != nil {
		t.Fatal(err)
	}
	if err := f.Set("x-trace=abc"); err != nil {
		t.Fatal(err)
	}
	got := f.String()
	requireAbsent(t, "header value in headerFlag.String", credToken, got)
	for _, want := range []string{"authorization", "x-trace"} {
		if !strings.Contains(got, want) {
			t.Errorf("headerFlag.String() = %q, want it to name the key %q", got, want)
		}
	}
}

// TestPubDoesNotEchoAMalformedHeader: `-H "Authorization: Bearer TOKEN"` is
// the curl spelling, and it has no "=". The flag package reports a value Set
// refuses by quoting it, so the refusal printed the bearer token to stderr
// before the scrub existed to stop it.
//
// A Basic credential ends in "=" padding, so its curl spelling does have one,
// and it was published as a header whose KEY was "Authorization: Basic
// dXNlcjpwYXNzd29yZA": the credential went to the broker in a header name. A
// key with a colon or white space in it is refused the same way.
func TestPubDoesNotEchoAMalformedHeader(t *testing.T) {
	const basic = "dXNlcjpwYXNzd29yZA"
	for _, c := range []struct{ header, cred string }{
		{"Authorization: Bearer " + credToken, credToken},
		{"=" + credToken, credToken},
		{"Authorization: Basic " + basic + "==", basic},
		{"Authorization:" + basic + "==", basic},
		{"x-api-key\t" + credToken + "=", credToken},
	} {
		h := newHarness(t)
		if got := h.run("pub", memDSN, "orders", "-H", c.header, "hello"); got != exitUsage {
			t.Errorf("pub -H %q = %d, want %d", c.header, got, exitUsage)
		}
		requireAbsent(t, "malformed header value on stderr", c.cred, h.stderr())
		requireAbsent(t, "malformed header value on stdout", c.cred, h.stdout())
		if !strings.Contains(h.stderr(), "key=value") {
			t.Errorf("the refusal does not say the form:\n%s", h.stderr())
		}
	}
}

// TestPubStopsReadingStdinAfterAnInterrupt: pub --file - read stdin to its end
// whatever happened to the run, so Ctrl-C at a terminal changed nothing until
// Ctrl-D. It checks the run after every line now, so Ctrl-C and then Enter
// ends it with nothing published. A second Ctrl-C ends the process from any
// read; run says how.
//
// The bubble makes "still reading" observable without a sleep: synctest.Wait
// returns once every goroutine in it is blocked, and a pub still reading is
// blocked on the pipe.
func TestPubStopsReadingStdinAfterAnInterrupt(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		stdin, typed := io.Pipe()
		h := newHarness(t)
		h.env.stdin = stdin
		ctx, interrupt := context.WithCancel(context.Background())
		h.env.ctx = ctx
		done := make(chan int, 1)
		go func() { done <- h.run("pub", memDSN, "orders", "--file", "-") }()
		interrupt()
		go func() { _, _ = typed.Write([]byte("typed after the interrupt\n")) }()
		synctest.Wait()
		select {
		case code := <-done:
			if code == exitOK {
				t.Errorf("pub = %d after an interrupt, want a failure", code)
			}
			if !strings.Contains(h.stderr(), "interrupted") || strings.Contains(h.stdout(), "confirmed") {
				t.Errorf("pub did not stop at the interrupt\nstdout:\n%s\nstderr:\n%s", h.stdout(), h.stderr())
			}
		default:
			t.Errorf("pub --file - went on reading stdin after an interrupt and a newline")
			_ = typed.Close()
			<-done
		}
	})
}

// TestPubTimeoutBoundsReadingTheInput: the usage says --timeout bounds the
// whole run, but its deadline was set only once the input had been read, so a
// slow producer piped into `pub --file - --timeout 1s` kept the run going for
// as long as it took and then published everything under a fresh budget. The
// deadline now covers the read, which ends at the first line after it.
//
// The bubble's clock is the one the CLI's timers read, so the second passes
// without a sleep, and synctest.Wait shows whether pub is still reading.
func TestPubTimeoutBoundsReadingTheInput(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		stdin, typed := io.Pipe()
		h := newHarness(t)
		h.env.stdin = stdin
		done := make(chan int, 1)
		go func() { done <- h.run("pub", memDSN, "orders", "--file", "-", "--timeout", "1s") }()
		if _, err := typed.Write([]byte("typed in time\n")); err != nil {
			t.Fatal(err)
		}
		time.Sleep(2 * time.Second)
		go func() { _, _ = typed.Write([]byte("typed after the timeout\n")) }()
		synctest.Wait()
		select {
		case code := <-done:
			if code != exitFailure {
				t.Errorf("pub = %d after its --timeout ran out while reading, want %d", code, exitFailure)
			}
			if !strings.Contains(h.stderr(), "--timeout") || strings.Contains(h.stdout(), "confirmed") {
				t.Errorf("pub did not stop at its --timeout\nstdout:\n%s\nstderr:\n%s", h.stdout(), h.stderr())
			}
		default:
			t.Errorf("pub --file - --timeout 1s went on reading 2s later")
			_ = typed.Close()
			<-done
		}
	})
}

// TestPubNamesAStopAfterTheLastLine: readBodies asked the run after each line
// it scanned and not after the last one, so a --timeout that ran out, or a
// Ctrl-C typed, after the last line and before the end of the input was never
// seen by the read. pub went on to open the target on the ended context and
// printed only "opening <target>: context canceled", which names neither the
// stop nor the read.
func TestPubNamesAStopAfterTheLastLine(t *testing.T) {
	t.Run("--timeout", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			stdin, typed := io.Pipe()
			h := newHarness(t)
			h.env.stdin = stdin
			done := make(chan int, 1)
			go func() { done <- h.run("pub", memDSN, "orders", "--file", "-", "--timeout", "1s") }()
			if _, err := typed.Write([]byte("one\ntwo\n")); err != nil {
				t.Fatal(err)
			}
			time.Sleep(2 * time.Second) // the producer is slow to close its end
			_ = typed.Close()
			if code := <-done; code != exitFailure {
				t.Errorf("pub = %d, want %d", code, exitFailure)
			}
			if !strings.Contains(h.stderr(), "the --timeout of 1s ran out while reading the input") {
				t.Errorf("a --timeout that ran out while pub waited for the end of its input is not "+
					"named\nstdout:\n%s\nstderr:\n%s", h.stdout(), h.stderr())
			}
		})
	})
	t.Run("interrupt", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			stdin, typed := io.Pipe()
			h := newHarness(t)
			h.env.stdin = stdin
			ctx, interrupt := context.WithCancel(context.Background())
			h.env.ctx = ctx
			done := make(chan int, 1)
			go func() { done <- h.run("pub", memDSN, "orders", "--file", "-") }()
			if _, err := typed.Write([]byte("one\n")); err != nil {
				t.Fatal(err)
			}
			synctest.Wait()
			interrupt()
			synctest.Wait()
			_ = typed.Close() // Ctrl-D
			if code := <-done; code != exitFailure {
				t.Errorf("pub = %d after an interrupt, want %d", code, exitFailure)
			}
			if !strings.Contains(h.stderr(), "interrupted while reading the input") {
				t.Errorf("an interrupt before the end of the input is not reported as one\nstdout:\n%s\nstderr:\n%s",
					h.stdout(), h.stderr())
			}
		})
	})
}

// TestPubMissingFileIsAUsageError: exit 2 is "the invocation could not be
// acted on", and topology answers a --file that does not exist with it. pub
// routed the same mistake through codeFor, which has no code for an open
// error, and answered exit 1, the code for an operation that ran and failed.
func TestPubMissingFileIsAUsageError(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "no-such-file")
	h := newHarness(t)
	if code := h.run("pub", memDSN, "orders", "--file", missing); code != exitUsage {
		t.Errorf("pub --file <missing> = %d, want %d\nstderr:\n%s", code, exitUsage, h.stderr())
	}
	topo := newHarness(t)
	if code := topo.run("topology", "plan", "--no-connect", memDSN, "--file", missing); code != exitUsage {
		t.Errorf("topology plan --file <missing> = %d, want %d\nstderr:\n%s", code, exitUsage, topo.stderr())
	}
}

// TestPubInterruptedKeepsTheCountOfWhatTheBrokerTook: after a Ctrl-C (or a
// --timeout) that arrived while pub worked through its bodies, pub flushed on
// the run's own context, which had ended. A sink that answers an ended
// context with its error (the memory sink's Flush does, though it holds
// nothing) failed the Flush, and pub relabelled the publication the broker
// had taken "NOT CONFIRMED (acceptance unknown)", which invites a republish
// and so a duplicate. The publications after the interrupt were reported as
// "refused", which no broker did. The final Flush now runs on a context
// detached from the stop, with a bound of its own, and what the stop cut off
// is named as such.
//
// The interrupt is the broker's own publish hook cancelling the run once
// the first publication is done.
func TestPubInterruptedKeepsTheCountOfWhatTheBrokerTook(t *testing.T) {
	ctx, interrupt := context.WithCancel(context.Background())
	defer interrupt()
	var once sync.Once
	broker, err := memory.NewBroker(memory.Config{
		AutoCreate: true,
		Hooks: mq.Hooks{OnPublishDone: func(context.Context, mq.PublishDoneEvent) {
			once.Do(interrupt)
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = broker.Close() })
	reg := dsn.NewRegistry()
	if err := reg.Register("pubseam", func(ctx context.Context, _ string) (*mq.Client, error) {
		return memory.OpenWithClient(ctx, broker, memory.ClientBorrowed)
	}); err != nil {
		t.Fatal(err)
	}
	ensure(t, broker, "orders", "billing")

	h := harnessWith(t, reg, nil)
	h.env.ctx = ctx
	code := h.run("pub", "pubseam://", "orders", "m1", "m2", "m3", "m4", "m5")
	if ctx.Err() == nil {
		t.Fatal("the interrupt never happened, so this run proves nothing")
	}
	if got := depth(t, broker, "orders", "billing"); got != 1 {
		t.Fatalf("the broker holds %d publications, want the 1 made before the interrupt", got)
	}
	if code != exitFailure {
		t.Errorf("pub = %d after an interrupt that cut four publications off, want %d", code, exitFailure)
	}
	if !strings.Contains(h.stdout(), "confirmed 1 of 5") || strings.Contains(h.stderr(), notConfirmed) {
		t.Errorf("the broker holds the publication made before the interrupt and pub does not count "+
			"it confirmed\nstdout:\n%s\nstderr:\n%s", h.stdout(), h.stderr())
	}
	if strings.Contains(h.stderr(), "refused") {
		t.Errorf("publications the interrupt cut off are reported as refused by the broker\nstderr:\n%s",
			h.stderr())
	}
	if !strings.Contains(h.stderr(), "interrupted before message 1, so 4 of the 5 messages were not published") {
		t.Errorf("pub does not say what the interrupt cut off\nstderr:\n%s", h.stderr())
	}
}

// TestPubOptionsAreValidatedBeforeAConnection: a contradictory invocation must
// fail before a broker is opened, not at the first message.
//
// The registry is EMPTY, which is what makes the ordering observable: if the
// options were folded after the connection, every case here would report the
// unclaimed scheme instead of the option the caller got wrong.
func TestPubOptionsAreValidatedBeforeAConnection(t *testing.T) {
	for _, c := range []struct {
		args []string
		want string
	}{
		{[]string{"--priority", "999"}, "0 to 255"},
		{[]string{"--partition", "-3"}, "non-negative"},
		{[]string{"--confirm", "sideways"}, "default, accepted or durable"},
		{[]string{"--lines", "--no-lines"}, "contradict"},
	} {
		h := newEmptyHarness(t)
		args := append([]string{"pub", memDSN, "orders"}, c.args...)
		args = append(args, "body")
		if got := h.run(args...); got != exitUsage {
			t.Errorf("pub %v = %d, want %d", c.args, got, exitUsage)
		}
		if !strings.Contains(h.stderr(), c.want) {
			t.Errorf("pub %v: the refusal does not say %q. If it named the scheme instead, the "+
				"options are being folded after the connection is opened:\n%s",
				c.args, c.want, h.stderr())
		}
	}
}

// TestPubQuietPrintsOnlyFailures covers the flag a script uses.
func TestPubQuietPrintsOnlyFailures(t *testing.T) {
	h := newHarness(t)
	if got := h.run("pub", memDSN, "orders", "-q", "hello"); got != exitOK {
		t.Fatalf("exit %d: %s", got, h.stderr())
	}
	if h.stdout() != "" {
		t.Errorf("pub -q printed to stdout on success:\n%s", h.stdout())
	}
}

// TestReadBodiesTrailingNewline is the parsing rule on its own, because the
// off-by-one is the kind that ships: a file written by an editor ends in a
// newline and must not publish a spurious empty message.
func TestReadBodiesTrailingNewline(t *testing.T) {
	for _, c := range []struct {
		in   string
		want []string
	}{
		{"a\nb\n", []string{"a", "b"}},
		{"a\nb", []string{"a", "b"}},
		{"a\n\nb\n", []string{"a", "", "b"}},
		{"", nil},
		{"\n", []string{""}},
	} {
		got, err := readBodies(context.Background(), strings.NewReader(c.in), true)
		if err != nil {
			t.Fatalf("readBodies(%q): %v", c.in, err)
		}
		if len(got) != len(c.want) {
			t.Errorf("readBodies(%q) = %q, want %q", c.in, got, c.want)
			continue
		}
		for i := range got {
			if got[i] != c.want[i] {
				t.Errorf("readBodies(%q)[%d] = %q, want %q", c.in, i, got[i], c.want[i])
			}
		}
	}
}
