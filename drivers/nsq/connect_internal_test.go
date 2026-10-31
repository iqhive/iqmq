package nsq

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"slices"
	"strings"
	"testing"
	"time"

	gonsq "github.com/nsqio/go-nsq"

	mq "github.com/iqhive/iqmq"
)

// closeDuringConnectEnv names the scenario a child copy of the test binary
// runs for TestClosingWhileGoNSQConnectsDoesNotCrashTheProcess.
const closeDuringConnectEnv = "IQMQ_NSQ_CLOSE_DURING_CONNECT"

// TestClosingWhileGoNSQConnectsDoesNotCrashTheProcess closes a source while
// go-nsq is still making one of its connections, and the process has to
// survive it.
//
// go-nsq's Stop counts only the connections that have finished their
// handshake. With none finished it closes the channel its handler pool reads
// from at once, and a connection still waiting for nsqd's answer to IDENTIFY
// does not look at the stop again when the answer comes: it subscribes, sends
// RDY, and the first message nsqd sends is put on the closed channel. That is
// a panic on a go-nsq goroutine, which no caller can recover, so each scenario
// runs in a child copy of this binary and the parent reads how it ended.
//
// There are two ways in. OpenSource connects outside the backend's lock, so a
// backend Close can reach the source while its connect is in progress. And
// with no API race at all, go-nsq redials a dropped connection on its own, so
// closing a source during an outage can land on a redial.
func TestClosingWhileGoNSQConnectsDoesNotCrashTheProcess(t *testing.T) {
	if os.Getenv(closeDuringConnectEnv) != "" {
		t.Skip("this process is the child; TestClosingWhileGoNSQConnects is the subject")
	}
	for _, scenario := range []string{"open", "redial"} {
		t.Run(scenario, func(t *testing.T) {
			cmd := exec.CommandContext(t.Context(), os.Args[0],
				"-test.run=^TestClosingWhileGoNSQConnects$", "-test.v", "-test.count=1")
			cmd.Env = append(os.Environ(), closeDuringConnectEnv+"="+scenario)
			raw, err := cmd.CombinedOutput()
			out := string(raw)
			if err != nil || !strings.Contains(out, "--- PASS: TestClosingWhileGoNSQConnects") {
				t.Fatalf("the child process running %q did not pass (%v):\n%s", scenario, err, out)
			}
		})
	}
}

// TestClosingWhileGoNSQConnects is the child half of
// TestClosingWhileGoNSQConnectsDoesNotCrashTheProcess, and skips unless that
// test started it.
func TestClosingWhileGoNSQConnects(t *testing.T) {
	scenario := os.Getenv(closeDuringConnectEnv)
	if scenario == "" {
		t.Skip("run in a child process by TestClosingWhileGoNSQConnectsDoesNotCrashTheProcess")
	}
	w := newWireNSQD(t)
	b, err := OpenBackend(context.Background(), Config{
		Addrs:               []string{w.ln.Addr().String()},
		AckDeadline:         30 * time.Second,
		LeaseGuard:          time.Second,
		LookupdPollInterval: 20 * time.Millisecond,
		APITimeout:          2 * time.Second,
	})
	if err != nil {
		t.Fatalf("OpenBackend: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })
	cfg := mq.SourceConfig{
		Topic: "orders", Subscription: "workers", Prefetch: 1,
		Metadata: mq.MetadataNativeStrict, AckDeadline: 30 * time.Second,
	}
	release := make(chan struct{})

	switch scenario {
	case "open":
		// The backend closes while OpenSource waits for nsqd's answer to
		// IDENTIFY, and the answer comes after the Close.
		w.mu.Lock()
		w.hold[0] = release
		w.mu.Unlock()
		type opened struct {
			src mq.Source
			err error
		}
		done := make(chan opened, 1)
		go func() {
			src, err := b.OpenSource(context.Background(), cfg)
			done <- opened{src, err}
		}()
		<-w.identified
		if err := b.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
		close(release)
		got := <-done
		if got.src != nil {
			_ = got.src.Close()
			t.Fatalf("OpenSource returned a source for a backend that closed during its connect (err %v)", got.err)
		}
		if !errors.Is(got.err, mq.ErrClosed) {
			t.Fatalf("OpenSource on a backend that closed during its connect returned %v, want ErrClosed", got.err)
		}
		// And the connection go-nsq finished after the Close is closed
		// rather than left open: the stop came after it, so it was told.
		if cmds := w.commandsOf(0); !slices.Contains(cmds, "CLS") {
			t.Fatalf("the connection that finished after the backend closed was never closed: %q", cmds)
		}

	case "redial":
		// The first connection carries no message, so dropping it lets go-nsq
		// close it at once and redial. The source closes while the redial
		// waits for nsqd's answer to IDENTIFY.
		w.mu.Lock()
		w.quiet[0] = true
		w.hold[1] = release
		w.mu.Unlock()
		src, err := b.OpenSource(context.Background(), cfg)
		if err != nil {
			t.Fatalf("OpenSource: %v", err)
		}
		<-w.identified
		waitUntil(t, "the first connection to be ready", func() bool { return hasRDY(w.commandsOf(0)) })
		w.drop()
		select {
		case <-w.identified:
		case <-time.After(5 * time.Second):
			t.Fatal("go-nsq never redialled")
		}
		if err := src.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
		close(release)
		waitUntil(t, "the redial to subscribe", func() bool {
			for _, c := range w.commandsOf(1) {
				if strings.HasPrefix(c, "SUB ") {
					return true
				}
			}
			return false
		})
		// A RDY follows the SUB at once when go-nsq sends one, and the
		// message it releases is the panic.
		time.Sleep(200 * time.Millisecond)
		if hasRDY(w.commandsOf(1)) {
			t.Fatalf("the redial that finished after Close asked nsqd for messages: %q", w.commandsOf(1))
		}
		// The documented residue (docs/drivers/nsq.md, Reconnect): nothing
		// closes the late connection, so it stays open with go-nsq's read and
		// write goroutines for it. It is asserted rather than left to goleak,
		// which would not see it: the server's cleanup closes the socket
		// before TestMain looks, and that is what ends those goroutines. A
		// failure here means the residue is gone; say so in the docs.
		if slices.Contains(w.commandsOf(1), "CLS") || w.hasEnded(1) {
			t.Fatalf("the redial that finished after Close was closed (commands %q); the residue the driver "+
				"page documents is gone, so update it", w.commandsOf(1))
		}

	default:
		t.Fatalf("unknown scenario %q", scenario)
	}
}

// hasRDY reports whether a connection asked nsqd for at least one message.
func hasRDY(commands []string) bool {
	for _, c := range commands {
		if strings.HasPrefix(c, "RDY ") && c != "RDY 0" {
			return true
		}
	}
	return false
}

// TestAHandshakeTheLinkCutIsUnavailableNotAConfigurationError holds the
// classification of a failed IDENTIFY to what failed.
//
// go-nsq wraps everything that goes wrong inside the handshake in one error
// type: nsqd's own refusal of the parameters, but also an EOF or a timeout
// reading its answer. Mapping the type to CodeInvalidConfig told a caller whose
// nsqd was restarting that the configuration was wrong, the core did not retry
// a publication that had never been sent, and health stayed ready. Only an nsqd
// error frame, or a TLS certificate the client refuses or is refused for, is
// about the configuration.
func TestAHandshakeTheLinkCutIsUnavailableNotAConfigurationError(t *testing.T) {
	hungUp := func(t *testing.T) *Backend {
		t.Helper()
		w := newWireNSQD(t)
		w.mu.Lock()
		w.hangUp[0] = true
		w.mu.Unlock()
		b, err := OpenBackend(context.Background(), Config{Addrs: []string{w.ln.Addr().String()}, APITimeout: 2 * time.Second})
		if err != nil {
			t.Fatalf("OpenBackend: %v", err)
		}
		t.Cleanup(func() { _ = b.Close() })
		return b
	}
	t.Run("a publication", func(t *testing.T) {
		b := hungUp(t)
		snk, err := b.OpenSink(context.Background(), mq.SinkConfig{Topic: "orders"})
		if err != nil {
			t.Fatalf("OpenSink: %v", err)
		}
		err = snk.Publish(context.Background(), &mq.Message{Body: []byte("x")}, nil)
		if err == nil {
			t.Fatal("Publish succeeded against an nsqd that hung up during the handshake")
		}
		if code := mq.CodeOf(err); code != mq.CodeUnavailable || !mq.IsRetryable(err) || mq.IsAmbiguous(err) {
			t.Fatalf("a link cut during IDENTIFY mapped to %v (retryable %v, ambiguous %v): %v; want a retryable, "+
				"unambiguous Unavailable, because no PUB was written", code, mq.IsRetryable(err), mq.IsAmbiguous(err), err)
		}
		if st := b.Health().State; st != mq.HealthDegraded {
			t.Fatalf("health is %v after the connection failed, want degraded", st)
		}
	})
	t.Run("a subscription", func(t *testing.T) {
		b := hungUp(t)
		src, err := b.OpenSource(context.Background(), mq.SourceConfig{Topic: "orders", Subscription: "workers", Prefetch: 1})
		if src != nil {
			_ = src.Close()
		}
		if code := mq.CodeOf(err); code != mq.CodeUnavailable || !mq.IsRetryable(err) {
			t.Fatalf("OpenSource against an nsqd that hung up during the handshake returned %v (code %v, retryable %v), "+
				"want a retryable Unavailable", err, code, mq.IsRetryable(err))
		}
	})
	t.Run("the reasons", func(t *testing.T) {
		m := newErrorMapper()
		for reason, want := range map[string]mq.ErrorCode{
			"E_BAD_BODY IDENTIFY msg timeout (100) is invalid":                                 mq.CodeInvalidConfig,
			"E_AUTH_FAILED AUTH failed":                                                        mq.CodeUnauthorized,
			"tls: failed to verify certificate: x509: certificate signed by unknown authority": mq.CodeInvalidConfig,
			"remote error: tls: bad certificate":                                               mq.CodeInvalidConfig,
			"EOF":                                                                              mq.CodeUnavailable,
			"read tcp 127.0.0.1:1->127.0.0.1:2: i/o timeout":                                   mq.CodeUnavailable,
			"write tcp 127.0.0.1:1->127.0.0.1:2: connection reset by peer":                     mq.CodeUnavailable,
			"invalid response from Snappy upgrade":                                             mq.CodeUnavailable,
		} {
			mapped := m.Map(mq.OpPublish, "orders", "", gonsq.ErrIdentify{Reason: reason})
			if got := mq.CodeOf(mapped); got != want {
				t.Errorf("IDENTIFY failing with %q mapped to %v, want %v", reason, got, want)
			}
			if retry := mq.IsRetryable(mapped); retry != (want == mq.CodeUnavailable) {
				t.Errorf("IDENTIFY failing with %q is retryable %v", reason, retry)
			}
		}
	})
}
