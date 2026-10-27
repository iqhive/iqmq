package activemq_test

import (
	"context"
	"testing"
	"time"

	"github.com/Azure/go-amqp"

	mq "github.com/iqhive/iqmq"
	amq "github.com/iqhive/iqmq/drivers/activemq"
)

// TestScheduledPublicationHoldsADeliverAtOnBothProducts is a DeliverAt through
// both products' delay rules, which are both RELATIVE: each reads a delay in
// milliseconds and neither reads an instant.
//
// The core used to write a DeliverAt to a relative rule as a delay of zero, so
// a publication meant for an hour from now was delivered at once and reported
// success. What must go out is the delay left until the instant, measured on
// [amq.Config.Clock]; the clock here is stopped years away from the wall's, so
// a delay read off the wall clock fails as well. A DeliverAt that has already
// passed is due now and carries no delay at all, because an explicit zero is a
// scheduled publication on both products and not an ordinary one.
func TestScheduledPublicationHoldsADeliverAtOnBothProducts(t *testing.T) {
	t.Parallel()
	now := time.Date(2031, 3, 4, 5, 6, 7, 0, time.UTC)
	classic := artemisConfig()
	classic.Broker = amq.Classic
	classic.Scheduler = true
	for _, tc := range []struct {
		name  string
		cfg   amq.Config
		props map[string]any
		key   string
		held  func(m *amqp.Message, key string) (any, bool)
	}{
		{
			"artemis", artemisConfig(), artemisProps(), amq.AnnotationDeliveryDelay,
			func(m *amqp.Message, key string) (any, bool) { v, ok := m.Annotations[key]; return v, ok },
		},
		// Classic sends no product and no version in its open performative.
		{
			"classic", classic, nil, amq.PropertyScheduledDelay,
			func(m *amqp.Message, key string) (any, bool) { v, ok := m.ApplicationProperties[key]; return v, ok },
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cfg := tc.cfg
			cfg.Clock = stoppedClock{now: now}
			b, p := openPeer(t, cfg, tc.props)
			sink, _ := addresses(t, cfg, "orders", "")
			snk := openSink(t, b, "orders", mq.ConfirmAccepted)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			// The sink reuses its encode scratch, so each transfer is read
			// before the next publication can overwrite it.
			if err := snk.Publish(ctx, &mq.Message{Body: []byte("later")},
				&mq.PublishOptions{DeliverAt: now.Add(90 * time.Second)}); err != nil {
				t.Fatalf("Publish: %v", err)
			}
			sent := p.Sent(sink)
			if len(sent) != 1 {
				t.Fatalf("the peer received %d transfer(s), want 1", len(sent))
			}
			if got, _ := tc.held(sent[0], tc.key); got != int64(90_000) {
				t.Errorf("a DeliverAt 90s ahead went out as %s=%v (%T), want 90000 milliseconds as an int64",
					tc.key, got, got)
			}

			if err := snk.Publish(ctx, &mq.Message{Body: []byte("overdue")},
				&mq.PublishOptions{DeliverAt: now.Add(-time.Minute)}); err != nil {
				t.Fatalf("Publish: %v", err)
			}
			sent = p.Sent(sink)
			if len(sent) != 2 {
				t.Fatalf("the peer received %d transfer(s), want 2", len(sent))
			}
			if got, ok := tc.held(sent[1], tc.key); ok {
				t.Errorf("a DeliverAt already past went out as %s=%v (%T), want no delay written", tc.key, got, got)
			}
		})
	}
}

// stoppedClock reads one instant for ever and hands out real timers, so that a
// test can say exactly what a delay measured on it is while the backend's own
// waits still end.
type stoppedClock struct{ now time.Time }

func (c stoppedClock) Now() time.Time { return c.now }

func (stoppedClock) NewTimer(d time.Duration) mq.Timer { return stoppedClockTimer{t: time.NewTimer(d)} }

// stoppedClockTimer adapts *time.Timer to mq.Timer.
type stoppedClockTimer struct{ t *time.Timer }

func (s stoppedClockTimer) C() <-chan time.Time        { return s.t.C }
func (s stoppedClockTimer) Stop() bool                 { return s.t.Stop() }
func (s stoppedClockTimer) Reset(d time.Duration) bool { return s.t.Reset(d) }
