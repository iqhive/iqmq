package main

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	mq "bitbucket.org/iqhive/iqmq"
	"bitbucket.org/iqhive/iqmq/memory"
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
		got, err := readBodies(strings.NewReader(c.in), true)
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
