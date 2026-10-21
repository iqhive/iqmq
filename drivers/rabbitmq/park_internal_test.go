package rabbitmq

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	amqp "github.com/rabbitmq/amqp091-go"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/drivers/rabbitmq/internal/client"
	"github.com/iqhive/iqmq/internal/rabbit/confirm"
	"github.com/iqhive/iqmq/internal/rabbit/delay"
)

// publishCounter counts the publications written through it. Every other
// method is the embedded nil interface, which a test that reaches it fails on
// loudly.
type publishCounter struct {
	client.Channel
	n atomic.Int32
}

func (c *publishCounter) PublishWithContext(context.Context, string, string, bool, bool, amqp.Publishing) error {
	c.n.Add(1)
	return nil
}

// TestAParkOnAReplacedChannelIsRefusedBeforeItIsNumbered holds the channel
// identity a park depends on: the tracker numbers the CURRENT channel's
// publications, so a park for a delivery whose channel has since been replaced
// must be refused before it takes a tag, or every confirm on the new channel
// resolves the wrong publication from then on.
func TestAParkOnAReplacedChannelIsRefusedBeforeItIsNumbered(t *testing.T) {
	old, cur := &publishCounter{}, &publishCounter{}
	src := &source{ch: cur, tr: confirm.NewTracker(4), parks: map[uint64]*parkWait{}}
	err := (*settler)(src).park(t.Context(), old, &amqp.Delivery{}, &delay.Plan{}, 1)
	if !errors.Is(err, mq.ErrLeaseLost) {
		t.Fatalf("a park on a replaced channel returned %v, want ErrLeaseLost", err)
	}
	if n := old.n.Load() + cur.n.Load(); n != 0 {
		t.Fatalf("a refused park wrote %d publications, want 0", n)
	}
	if n := src.tr.Outstanding(); n != 0 {
		t.Fatalf("a refused park left %d tags outstanding on the current tracker, want 0", n)
	}
}
