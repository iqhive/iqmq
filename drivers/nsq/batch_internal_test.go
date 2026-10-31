package nsq

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

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
