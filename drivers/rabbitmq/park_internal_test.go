package rabbitmq

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

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
	src := &source{b: &Backend{}, ch: cur, tr: confirm.NewTracker(4), parks: map[uint64]*parkWait{}}
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

// TestAParkedCopyCarriesItsTTLPastTheDelay holds [parkExpiration] to both
// arms. A copy parked in a delay queue goes without `expiration`, which would
// cut the delay short, and carries the TTL and its due moment in the headers
// instead; every other park keeps `expiration`, restored from the header for a
// copy whose `expiration` an earlier delay queue's hop removed. A dead letter
// has no TTL, so the headers a copy kept when it was dead-lettered carry
// nothing into its next park.
func TestAParkedCopyCarriesItsTTLPastTheDelay(t *testing.T) {
	due := time.Date(2026, 1, 1, 0, 0, 3, 200_000_000, time.UTC)
	queue := &delay.Plan{Declare: &delay.QueueDecl{Name: "q.delay.3200", TTLMillis: 3200}, Granted: 3200 * time.Millisecond}
	plugin := &delay.Plan{Header: delay.DelayHeader, HeaderMillis: 3000}
	carried := func(reason, from string) amqp.Table {
		return amqp.Table{
			ttlHeader:   int64(1000),
			dueHeader:   int64(1),
			deathHeader: []any{amqp.Table{"reason": reason, "queue": from}},
		}
	}
	backFromAQueue := amqp.Delivery{RoutingKey: "q", Headers: carried("expired", "q.delay.3200")}
	deadLetter := amqp.Delivery{RoutingKey: "q", Headers: carried("rejected", "q")}
	for _, tc := range []struct {
		name           string
		d              amqp.Delivery
		plan           *delay.Plan
		wantExpiration string
		wantTTL        any
		wantDue        any
	}{
		{"a delay queue, a message with a TTL", amqp.Delivery{Expiration: "1000"}, queue, "", int64(1000), due.UnixMilli()},
		{"a delay queue, a copy back from one", backFromAQueue, queue, "", int64(1000), due.UnixMilli()},
		{"a delay queue, a dead letter that was a copy back from one", deadLetter, queue, "", nil, nil},
		{"a delay queue, a message without a TTL", amqp.Delivery{}, queue, "", nil, nil},
		{"the plugin, a message with a TTL", amqp.Delivery{Expiration: "1000"}, plugin, "1000", nil, nil},
		{"the plugin, a copy back from a delay queue", backFromAQueue, plugin, "1000", nil, nil},
		{"the plugin, a dead letter that was a copy back from a delay queue", deadLetter, plugin, "", nil, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pub := amqp.Publishing{Headers: parkHeaders(tc.d.Headers, tc.plan, 1), Expiration: tc.d.Expiration}
			parkExpiration(&pub, &tc.d, "q", tc.plan, due)
			if pub.Expiration != tc.wantExpiration {
				t.Errorf("expiration = %q, want %q", pub.Expiration, tc.wantExpiration)
			}
			if got := pub.Headers[ttlHeader]; got != tc.wantTTL {
				t.Errorf("%s = %v, want %v", ttlHeader, got, tc.wantTTL)
			}
			if got := pub.Headers[dueHeader]; got != tc.wantDue {
				t.Errorf("%s = %v, want %v", dueHeader, got, tc.wantDue)
			}
		})
	}
}

// TestOnlyTheLatestDeadLetteringDecidesWhetherACopyIsBackFromADelay holds
// [expiredFromDelay] to the `x-death` entry it reads: the first, which is the
// latest, and only an expiry from one of the queue's own delay queues.
func TestOnlyTheLatestDeadLetteringDecidesWhetherACopyIsBackFromADelay(t *testing.T) {
	death := func(reason, from string) amqp.Table { return amqp.Table{"reason": reason, "queue": from} }
	for _, tc := range []struct {
		name   string
		deaths any
		want   bool
	}{
		{"an expiry from the queue's delay queue", []any{death("expired", "q.delay.3200")}, true},
		{"rejected since it came back", []any{death("rejected", "q"), death("expired", "q.delay.3200")}, false},
		{"back again after an earlier rejection", []any{death("expired", "q.delay.3200"), death("rejected", "q")}, true},
		{"an expiry from the queue itself", []any{death("expired", "q")}, false},
		{"an expiry from another queue's delay queue", []any{death("expired", "other.delay.3200")}, false},
		{"an expiry from a queue that only starts with the name", []any{death("expired", "q.delay.x")}, false},
		{"rejected from the delay queue", []any{death("rejected", "q.delay.3200")}, false},
		{"no dead-lettering", nil, false},
		{"an empty record", []any{}, false},
		{"a record of the wrong kind", "expired", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := amqp.Table{}
			if tc.deaths != nil {
				h[deathHeader] = tc.deaths
			}
			if got := expiredFromDelay(h, "q"); got != tc.want {
				t.Errorf("expiredFromDelay = %v, want %v", got, tc.want)
			}
		})
	}
}
