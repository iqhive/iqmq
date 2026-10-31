package nsq

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	gonsq "github.com/nsqio/go-nsq"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit"
)

// commandLog is a producer that accepts every publication and records the
// command each one was sent as, with the size of an MPUB's body as nsqd
// measures it: a 4-byte count, then a 4-byte length before each message.
type commandLog struct {
	mu       sync.Mutex
	commands []string
}

func (c *commandLog) record(s string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.commands = append(c.commands, s)
}

func (c *commandLog) Publish(_ context.Context, _ string, body []byte) error {
	c.record(fmt.Sprintf("PUB %d", len(body)))
	return nil
}

func (c *commandLog) MultiPublish(_ context.Context, _ string, bodies [][]byte) error {
	size := 4
	for _, b := range bodies {
		size += 4 + len(b)
	}
	c.record(fmt.Sprintf("MPUB %d", size))
	return nil
}

func (c *commandLog) DeferredPublish(_ context.Context, _ string, _ time.Duration, body []byte) error {
	c.record(fmt.Sprintf("DPUB %d", len(body)))
	return nil
}

func (c *commandLog) Ping(ctx context.Context) error { return ctx.Err() }

func (c *commandLog) Close() {}

func (c *commandLog) Addr() string { return "commandlog" }

// TestEveryMPUBFitsTheBodyLimit holds batch publication to nsqd's
// --max-body-size, which bounds an MPUB's whole body, framing included.
//
// A message at the advertised MaxMessageBytes is publishable on its own, and
// with MaxBodyBytes equal to it, as nsqd runs with --max-body-size equal to
// --max-msg-size, a batch of that one message went out alone in an MPUB eight
// bytes over the limit. nsqd refuses that with E_BAD_BODY, a fatal error that
// also closes the connection under every other publication in flight on it, so
// a message Publish took was a message PublishBatch could not send.
func TestEveryMPUBFitsTheBodyLimit(t *testing.T) {
	const limit = 1 << 10
	for name, sizes := range map[string][]int{
		"one message at the limit":                {limit},
		"two that fit alone and not together":     {limit / 2, limit / 2},
		"a message at the limit among small ones": {10, limit, 10, 10},
		"small ones that fit together":            {100, 100, 100},
	} {
		t.Run(name, func(t *testing.T) {
			log := &commandLog{}
			b, err := BackendWithClient(context.Background(), log, driverkit.ClientOwned, Config{
				Addrs:           []string{"commandlog:4150"},
				MaxMessageBytes: limit,
				MaxBodyBytes:    limit,
			})
			if err != nil {
				t.Fatalf("BackendWithClient: %v", err)
			}
			t.Cleanup(func() { _ = b.Close() })
			snk, err := b.OpenSink(context.Background(), mq.SinkConfig{Topic: "orders"})
			if err != nil {
				t.Fatalf("OpenSink: %v", err)
			}
			ms := make([]*mq.Message, len(sizes))
			for i, n := range sizes {
				ms[i] = &mq.Message{Body: bytes.Repeat([]byte("x"), n)}
			}
			if err := snk.(mq.BatchSink).PublishBatch(context.Background(), ms, nil); err != nil {
				t.Fatalf("PublishBatch: %v", err)
			}
			log.mu.Lock()
			defer log.mu.Unlock()
			for _, c := range log.commands {
				var verb string
				var size int
				if _, err := fmt.Sscanf(c, "%s %d", &verb, &size); err != nil {
					t.Fatalf("reading %q: %v", c, err)
				}
				if verb == "MPUB" && size > limit {
					t.Errorf("an MPUB of %d bytes was sent where nsqd's --max-body-size is %d; commands %q",
						size, limit, log.commands)
				}
			}
		})
	}
}

// TestAnMPUBNSQDFailedPartWayIsAmbiguous is nsqd's E_MPUB_FAILED, which is not
// the refusal of a whole command. nsqd puts an MPUB's messages into the topic
// one at a time and keeps those it put before the one that failed, so on a
// disk that fills part way through it holds some of the batch and answers
// E_MPUB_FAILED. Reported as a definite failure of every message, the batch
// was republished and the stored part duplicated, with the duplicate looking
// like a loss. "exiting" is included: nsqd before v1.3.0 stops a topic's disk
// queue under open connections, part way through an MPUB, with that text.
func TestAnMPUBNSQDFailedPartWayIsAmbiguous(t *testing.T) {
	for _, reason := range []string{
		"E_MPUB_FAILED MPUB failed write /data/orders.diskqueue.000000.dat: no space left on device",
		"E_MPUB_FAILED MPUB failed exiting",
	} {
		t.Run(reason, func(t *testing.T) {
			w := newWireNSQD(t)
			w.mu.Lock()
			w.publish = wireAnswer{frameType: gonsq.FrameTypeError, data: reason}
			w.mu.Unlock()
			b := openWireBackend(t, w, Config{})
			snk, err := b.OpenSink(context.Background(), mq.SinkConfig{Topic: "orders"})
			if err != nil {
				t.Fatalf("OpenSink: %v", err)
			}
			ms := []*mq.Message{{Body: []byte("a")}, {Body: []byte("b")}, {Body: []byte("c")}}
			err = snk.(mq.BatchSink).PublishBatch(context.Background(), ms, nil)
			if !mq.IsAmbiguous(err) || mq.CodeOf(err) != mq.CodeAmbiguous {
				t.Fatalf("PublishBatch answered %q returned %v (code %v, ambiguous %v), want ambiguous: nsqd may "+
					"hold some of the batch", reason, err, mq.CodeOf(err), mq.IsAmbiguous(err))
			}
			// The flag the predicate hides once the error is ambiguous; this
			// driver documents that an ambiguous outcome is never retryable.
			var oe *mq.OpError
			if !errors.As(err, &oe) || oe.Retryable {
				t.Fatalf("the ambiguous E_MPUB_FAILED is %#v, want an *mq.OpError with Retryable false", err)
			}
			if got := w.commandsOf(0); len(got) != 1 || !strings.HasPrefix(got[0], "MPUB ") {
				t.Fatalf("the batch was sent as %q, want one MPUB", got)
			}
		})
	}
}
