package v1compat_test

import (
	"context"
	"fmt"
	"runtime/debug"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"cloud.google.com/go/pubsub/v2/pstest"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	mq "github.com/iqhive/iqmq"
	gps "github.com/iqhive/iqmq/drivers/gcppubsub"
	"github.com/iqhive/iqmq/drivers/gcppubsub/v1compat"
)

// drivers/gcppubsub's publication fixes of hunt 8 (cs8-2, cs8-3, cs8-4) on
// this arm. The sink is drivers/gcppubsub's, shared by both arms, and what
// these tests add is the v1 library under it, whose Topic keeps the body,
// pauses a key and races Flush against Stop the way the v2 Publisher does.

// gcppubsubBefore reports the version of drivers/gcppubsub this test binary
// linked, and whether it is a release older than want.
//
// CI also builds this module with GOWORK=off against the drivers/gcppubsub
// release its go.mod requires, so a test of behaviour the shared driver gained
// after that release skips there until the release train moves the
// requirement on. A workspace build links the module in the tree, which the
// build information reports as (devel), and that is older than nothing.
func gcppubsubBefore(t *testing.T, want string) (string, bool) {
	t.Helper()
	info, ok := debug.ReadBuildInfo()
	if !ok {
		t.Fatal("this test binary carries no build information")
	}
	for _, dep := range info.Deps {
		if dep.Path != "github.com/iqhive/iqmq/drivers/gcppubsub" {
			continue
		}
		if dep.Replace != nil {
			dep = dep.Replace
		}
		have, okHave := releaseNumbers(dep.Version)
		limit, okLimit := releaseNumbers(want)
		return dep.Version, okHave && okLimit && slices.Compare(have, limit) < 0
	}
	t.Fatal("this test binary's build information does not list drivers/gcppubsub")
	return "", false
}

// releaseNumbers is the major, minor and patch numbers of a module version
// such as v1.1.2 or v1.1.3-0.20261107010203-0123456789ab, and false for
// anything else, (devel) and the empty version of a directory among them.
func releaseNumbers(version string) ([]int, bool) {
	digits, ok := strings.CutPrefix(version, "v")
	if !ok {
		return nil, false
	}
	if i := strings.IndexAny(digits, "-+"); i >= 0 {
		digits = digits[:i]
	}
	parts := strings.Split(digits, ".")
	if len(parts) != 3 {
		return nil, false
	}
	out := make([]int, 3)
	for i, p := range parts {
		n, err := strconv.Atoi(p)
		if err != nil {
			return nil, false
		}
		out[i] = n
	}
	return out, true
}

// needsHunt8Sink skips a test of the shared sink's hunt 8 behaviour on a
// drivers/gcppubsub release that does not have it.
func needsHunt8Sink(t *testing.T) {
	t.Helper()
	if have, before := gcppubsubBefore(t, "v1.1.3"); before {
		t.Skipf("drivers/gcppubsub %s is older than v1.1.3, whose sink this test is of", have)
	}
}

// openV1 opens this arm over a fresh pstest server with the given reactors.
func openV1(t *testing.T, tweak func(*gps.Config), reactors ...pstest.ServerReactorOption) *mq.Client {
	t.Helper()
	srv := pstest.NewServer(reactors...)
	t.Cleanup(func() { _ = srv.Close() })
	cfg := gps.Config{
		ProjectID: "iqmq-test", Endpoint: srv.Addr, InsecureTransport: true, AutoCreate: true,
		PublishCountThreshold: 1, PublishDelayThreshold: time.Millisecond,
	}
	if tweak != nil {
		tweak(&cfg)
	}
	c, err := v1compat.Open(context.Background(), cfg)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

// slowPublish holds every Publish RPC at the pstest server.
type slowPublish struct{ d time.Duration }

// React delays the RPC and lets pstest answer it.
func (s slowPublish) React(any) (bool, any, error) {
	time.Sleep(s.d)
	return false, nil, nil
}

// TestAShutdownWhoseBoundEndsInTheFlushLeavesTheV1LibraryAlone: the v1
// library's Topic.Flush reads Topic.stopped without the lock Topic.Stop writes
// it under. A Flush that returned on its context left its goroutine there, and
// the shutdown's Close stopped the topic beside it, a data race. It fails
// under -race only.
func TestAShutdownWhoseBoundEndsInTheFlushLeavesTheV1LibraryAlone(t *testing.T) {
	needsHunt8Sink(t)
	c := openV1(t, func(cfg *gps.Config) {
		cfg.PublishCountThreshold = 1000
		cfg.PublishDelayThreshold = time.Hour
	}, pstest.ServerReactorOption{FuncName: "Publish", Reactor: slowPublish{d: 100 * time.Millisecond}})
	topic := newName("v1flush")
	provision(t, c, topic, "s")
	p, err := c.Publisher(context.Background(), mq.PublisherConfig{Topic: topic})
	if err != nil {
		t.Fatalf("Publisher: %v", err)
	}
	if err := p.PublishAsync(context.Background(), &mq.Message{Body: []byte("held")}, nil); err != nil {
		t.Fatalf("PublishAsync: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if err := c.Shutdown(ctx); err == nil {
		t.Log("Shutdown finished inside its bound; the race needs the flush step to outlive it")
	}
}

// TestAPublicationWhoseWaitEndedKeepsNoneOfTheCallersStorageOnTheV1Library:
// the v1 library's bundler keeps the caller's body, not a copy, past a
// Publish whose deadline ended during the wait, and PublishOwned released the
// storage on return while the library still held it.
func TestAPublicationWhoseWaitEndedKeepsNoneOfTheCallersStorageOnTheV1Library(t *testing.T) {
	needsHunt8Sink(t)
	const original, reused = "original-body", "REUSED-BUFFER"
	for _, path := range []string{"Publish", "PublishOwned"} {
		t.Run(path, func(t *testing.T) {
			c := openV1(t, func(cfg *gps.Config) {
				cfg.PublishCountThreshold = 1000
				cfg.PublishDelayThreshold = 300 * time.Millisecond
			})
			topic := newName("v1retain")
			provision(t, c, topic, "s")
			p, err := c.Publisher(context.Background(), mq.PublisherConfig{Topic: topic})
			if err != nil {
				t.Fatalf("Publisher: %v", err)
			}
			defer func() { _ = p.Close() }()
			cons, err := c.Consumer(context.Background(), mq.ConsumerConfig{Topic: topic, Subscription: "s"})
			if err != nil {
				t.Fatalf("Consumer: %v", err)
			}
			defer func() { _ = cons.Close() }()
			buf := []byte(original)
			var released atomic.Int32
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
			defer cancel()
			if path == "Publish" {
				err = p.Publish(ctx, &mq.Message{Body: buf})
			} else {
				err = p.PublishOwned(ctx, mq.OwnedMessage{Message: &mq.Message{Body: buf}, Release: func(m *mq.Message) {
					copy(m.Body, reused)
					released.Add(1)
				}})
			}
			if !mq.IsAmbiguous(err) {
				t.Fatalf("%s = %v (code %v), want the ambiguous answer of a wait the bundler outlasted",
					path, err, mq.CodeOf(err))
			}
			if path == "Publish" {
				copy(buf, reused)
			}
			d := one(t, cons, 5*time.Second)
			if string(d.Body) != original {
				t.Fatalf("%s: the publication whose wait ended carried %q, want %q", path, d.Body, original)
			}
			_ = d.Ack()
			if path == "PublishOwned" {
				deadline := time.Now().Add(5 * time.Second)
				for released.Load() == 0 && time.Now().Before(deadline) {
					time.Sleep(time.Millisecond)
				}
				if got := released.Load(); got != 1 {
					t.Fatalf("PublishOwned released the message %d times, want exactly once", got)
				}
			}
		})
	}
}

// refuseFirstPublish refuses the first publish RPC once handed is closed, so
// that the publications behind it are with the library before the refusal
// can be read (a fixed delay could be outlasted by a loaded runner between
// two PublishAsync calls).
type refuseFirstPublish struct {
	handed chan struct{}
	calls  atomic.Int32
}

// React refuses the first call and leaves the rest to pstest. The wait is
// bounded so that a test that never closes handed fails rather than hangs.
func (r *refuseFirstPublish) React(any) (bool, any, error) {
	if r.calls.Add(1) != 1 {
		return false, nil, nil
	}
	select {
	case <-r.handed:
	case <-time.After(10 * time.Second):
	}
	return true, nil, status.Error(codes.PermissionDenied, "injected: m1 is refused")
}

// TestAFailedOrderedPublicationLetsNoLaterMessageOfItsKeyThroughTheV1Library:
// the v1 library also refuses the publications of a paused key only as each
// comes up to be sent, and the driver resumed the key on reading the failure,
// which could let m2 or m3 through ahead of m1's retry.
func TestAFailedOrderedPublicationLetsNoLaterMessageOfItsKeyThroughTheV1Library(t *testing.T) {
	needsHunt8Sink(t)
	const runs = 20
	for i := range runs {
		refuse := &refuseFirstPublish{handed: make(chan struct{})}
		c := openV1(t, func(cfg *gps.Config) { cfg.Ordering = true },
			pstest.ServerReactorOption{FuncName: "Publish", Reactor: refuse})
		topic := newName("v1keys")
		provision(t, c, topic, "s")
		p, err := c.Publisher(context.Background(), mq.PublisherConfig{Topic: topic})
		if err != nil {
			t.Fatalf("Publisher: %v", err)
		}
		var mu sync.Mutex
		out := make([]error, 3)
		var wg sync.WaitGroup
		for j := range out {
			wg.Add(1)
			m := &mq.Message{Key: []byte("k"), Body: []byte(fmt.Sprintf("m%d", j+1))}
			if err := p.PublishAsync(context.Background(), m, func(r mq.PublishResult) {
				mu.Lock()
				out[j] = r.Err
				mu.Unlock()
				wg.Done()
			}); err != nil {
				t.Fatalf("PublishAsync(%s): %v", m.Body, err)
			}
		}
		close(refuse.handed)
		wg.Wait()
		if out[0] == nil {
			t.Fatalf("run %d: m1's injected refusal was not reported", i)
		}
		if out[1] == nil || out[2] == nil {
			t.Fatalf("run %d published a later message of the key after m1 failed (m1 = %v; m2 = %v; m3 = %v)",
				i, out[0], out[1], out[2])
		}
		if !mq.IsRetryable(out[1]) {
			t.Fatalf("run %d: m2 = %v, not retryable, so the caller cannot retry the key in order", i, out[1])
		}
		if err := p.Publish(context.Background(), &mq.Message{Key: []byte("k"), Body: []byte("m1 again")}); err != nil {
			t.Fatalf("run %d: the retry of m1: %v", i, err)
		}
		_ = p.Close()
		_ = c.Close()
	}
}
