package v1compat_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strconv"
	"testing"
	"time"

	mq "bitbucket.org/iqhive/iqmq"
	gps "bitbucket.org/iqhive/iqmq/drivers/gcppubsub"
)

// TestModel is the model test: the SAME scripted sequence through
// each arm, checked against a model of what the sequence must produce.
//
// It is what "correctness" means in evidence column. The two
// arms share every line of the driver above
// [bitbucket.org/iqhive/iqmq/drivers/gcppubsub/internal/psapi], so what this can
// disagree about is the client library: its bundler, its streaming pull, its
// flow control, its ordering-key handling and its settlement. A difference
// here is a difference between the libraries, which is the only kind of
// difference the family is about.
//
// The model is deliberately small and entirely about what
// promise:
//
//   - every publication is delivered at least once;
//   - an acknowledged message is not delivered again;
//   - a negatively settled message IS delivered again;
//   - within one ordering key, deliveries arrive in publication order;
//   - a second settlement of one delivery is refused, and the refusal names
//     which of the answers applies.
func TestModel(t *testing.T) {
	for _, arm := range a34Arms() {
		t.Run(arm.slug, func(t *testing.T) {
			conn := service(t)
			cfg := armConfig(1)
			cfg.Ordering = true
			c := arm.open(t, conn, cfg)
			topic, sub := newName("model"), "model"
			provision(t, c, topic, sub)
			ctx := context.Background()
			cons, err := c.Consumer(ctx, mq.ConsumerConfig{
				Topic: topic, Subscription: sub, Prefetch: 32,
			})
			if err != nil {
				t.Fatalf("Consumer: %v", err)
			}
			defer func() { _ = cons.Close() }()

			modelAtLeastOnce(t, c, cons, topic)
			modelAckIsTerminal(t, c, cons, topic)
			modelNackRedelivers(t, c, cons, topic)
			modelOrderWithinAKey(t, c, cons, topic)
			modelSecondSettlementIsRefused(t, c, cons, topic)
		})
	}
}

// modelAtLeastOnce: every publication is delivered.
func modelAtLeastOnce(t *testing.T, c *mq.Client, cons *mq.ConsumerHandle, topic string) {
	t.Helper()
	const n = 16
	want := map[string]bool{}
	ctx := context.Background()
	for i := range n {
		body := "m" + strconv.Itoa(i)
		want[body] = true
		if err := c.Publish(ctx, topic, &mq.Message{Body: []byte(body)}); err != nil {
			t.Fatalf("Publish: %v", err)
		}
	}
	for len(want) > 0 {
		d := one(t, cons, 10*time.Second)
		delete(want, string(d.Body))
		if err := d.Ack(); err != nil {
			t.Fatalf("Ack: %v", err)
		}
	}
}

// modelAckIsTerminal: an acknowledged message is not delivered again.
func modelAckIsTerminal(t *testing.T, c *mq.Client, cons *mq.ConsumerHandle, topic string) {
	t.Helper()
	ctx := context.Background()
	if err := c.Publish(ctx, topic, &mq.Message{Body: []byte("acked")}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	d := one(t, cons, 10*time.Second)
	if err := d.Ack(); err != nil {
		t.Fatalf("Ack: %v", err)
	}
	none(t, cons, 500*time.Millisecond)
}

// modelNackRedelivers: a negatively settled message is delivered again.
func modelNackRedelivers(t *testing.T, c *mq.Client, cons *mq.ConsumerHandle, topic string) {
	t.Helper()
	ctx := context.Background()
	if err := c.Publish(ctx, topic, &mq.Message{Body: []byte("nacked")}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	first := one(t, cons, 10*time.Second)
	if err := first.Nack(); err != nil {
		t.Fatalf("Nack: %v", err)
	}
	again := one(t, cons, 10*time.Second)
	if string(again.Body) != "nacked" {
		t.Fatalf("the redelivery carries %q", again.Body)
	}
	if err := again.Ack(); err != nil {
		t.Fatalf("Ack: %v", err)
	}
}

// modelOrderWithinAKey: the ordering, which is the claim Config.Ordering
// makes conditional.
func modelOrderWithinAKey(t *testing.T, c *mq.Client, cons *mq.ConsumerHandle, topic string) {
	t.Helper()
	ctx := context.Background()
	const n = 12
	for i := range n {
		m := &mq.Message{Key: []byte("ordered"), Body: []byte(strconv.Itoa(i))}
		if err := c.Publish(ctx, topic, m); err != nil {
			t.Fatalf("Publish(%d): %v", i, err)
		}
	}
	for i := range n {
		d := one(t, cons, 10*time.Second)
		if got := string(d.Body); got != strconv.Itoa(i) {
			t.Fatalf("position %d carries %q; an ordering key must be delivered in publication order", i, got)
		}
		if !bytes.Equal(d.Key, []byte("ordered")) {
			t.Fatalf("the key came back %q", d.Key)
		}
		if err := d.Ack(); err != nil {
			t.Fatalf("Ack: %v", err)
		}
	}
}

// modelSecondSettlementIsRefused: the answer to a second settlement.
func modelSecondSettlementIsRefused(t *testing.T, c *mq.Client, cons *mq.ConsumerHandle, topic string) {
	t.Helper()
	ctx := context.Background()
	if err := c.Publish(ctx, topic, &mq.Message{Body: []byte("twice")}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	d := one(t, cons, 10*time.Second)
	if err := d.Ack(); err != nil {
		t.Fatalf("Ack: %v", err)
	}
	err := d.Ack()
	if err == nil {
		t.Fatal("a second acknowledgement succeeded; the specification refuses one")
	}
	if !errors.Is(err, mq.ErrAlreadySettled) && !errors.Is(err, mq.ErrLeaseLost) {
		t.Fatalf("a second acknowledgement reported %v (code %v), want ErrAlreadySettled", err, mq.CodeOf(err))
	}
	if err := d.Nack(); err == nil {
		t.Fatal("a negative settlement after an acknowledgement succeeded")
	}
}

// TestBothArmsAnswerOneCapabilityTable is the other half of the correctness
// column, and the property that makes the family a comparison of LIBRARIES.
//
// The capability table, the metadata mapping and the settlement rules live in
// drivers/gcppubsub and are shared, so the two arms must advertise exactly the
// same thing. Anything else means one of them has grown a behaviour of its
// own, and a caller choosing an arm would be choosing a feature set.
func TestBothArmsAnswerOneCapabilityTable(t *testing.T) {
	conn := service(t)
	arms := a34Arms()
	tables := make([]mq.Capabilities, len(arms))
	for i, arm := range arms {
		tables[i] = arm.open(t, conn, armConfig(1)).Capabilities()
	}
	for f := range mq.FeatureCount {
		feature := mq.Feature(f)
		a, b := tables[0].Support(feature), tables[1].Support(feature)
		if a.Level != b.Level || a.Scope != b.Scope || a.Note != b.Note {
			t.Errorf("%v is %v/%v in %s and %v/%v in %s: the arms share one table and must advertise one answer",
				feature, a.Level, a.Scope, arms[0].slug, b.Level, b.Scope, arms[1].slug)
		}
	}
	for k := range mq.LimitKeyCount {
		key := mq.LimitKey(k)
		if tables[0].Limit(key) != tables[1].Limit(key) {
			t.Errorf("%v is %v in %s and %v in %s", key,
				tables[0].Limit(key), arms[0].slug, tables[1].Limit(key), arms[1].slug)
		}
	}
}

// TestTheArmsAreWireCompatible is what a migration needs to be true: a message
// published through one client generation is readable through the other, with
// every field intact.
//
// It is not a formality. The arms share the mapping, so the assertion is
// really that neither client library reshapes a message on its way to or from
// the service — an attribute renamed, an ordering key dropped, a body
// re-encoded — and that is exactly what a deployment moving between them is
// betting on.
func TestTheArmsAreWireCompatible(t *testing.T) {
	conn := service(t)
	arms := a34Arms()
	cfg := armConfig(1)
	cfg.Ordering = true
	clients := make([]*mq.Client, len(arms))
	for i, arm := range arms {
		clients[i] = arm.open(t, conn, cfg)
	}
	ctx := context.Background()
	for from := range arms {
		for to := range arms {
			name := arms[from].slug + "-to-" + arms[to].slug
			t.Run(name, func(t *testing.T) {
				topic, sub := newName("cross"), "cross"
				provision(t, clients[from], topic, sub)
				cons, err := clients[to].Consumer(ctx, mq.ConsumerConfig{
					Topic: topic, Subscription: sub, Prefetch: 8,
				})
				if err != nil {
					t.Fatalf("Consumer: %v", err)
				}
				defer func() { _ = cons.Close() }()
				m := &mq.Message{
					Key:  []byte("customer-7"),
					Body: []byte("payload"),
					ID:   "app-1",
					Properties: mq.Properties{
						ContentType:   "application/json",
						CorrelationID: "corr-1",
					},
				}
				m.Headers.Add("tenant", []byte("acme"))
				if err := clients[from].Publish(ctx, topic, m); err != nil {
					t.Fatalf("Publish: %v", err)
				}
				d := one(t, cons, 10*time.Second)
				if !bytes.Equal(d.Body, m.Body) || !bytes.Equal(d.Key, m.Key) || d.ID != m.ID {
					t.Fatalf("the message came back %+v", d.Message)
				}
				if got, _ := d.Headers.Get("tenant"); string(got) != "acme" {
					t.Fatalf("the header came back %q", got)
				}
				if d.Properties.ContentType != "application/json" || d.Properties.CorrelationID != "corr-1" {
					t.Fatalf("the properties came back %+v", d.Properties)
				}
				if err := d.Ack(); err != nil {
					t.Fatalf("Ack: %v", err)
				}
			})
		}
	}
	_ = fmt.Sprint(gps.DriverName)
}

// one fetches exactly one delivery, failing the test if none arrives.
func one(tb testing.TB, c *mq.ConsumerHandle, within time.Duration) mq.Delivery {
	tb.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), within)
	defer cancel()
	dst := make([]mq.Delivery, 1)
	n, err := c.Receive(ctx, dst)
	if n != 1 {
		tb.Fatalf("Receive returned %d, %v", n, err)
	}
	return dst[0]
}

// none asserts that nothing arrives within d.
func none(tb testing.TB, c *mq.ConsumerHandle, d time.Duration) {
	tb.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), d)
	defer cancel()
	dst := make([]mq.Delivery, 1)
	n, err := c.Receive(ctx, dst)
	if n != 0 {
		tb.Fatalf("a delivery arrived when none should have: body %q", dst[0].Body)
	}
	if err != nil && !errors.Is(err, context.DeadlineExceeded) {
		tb.Fatalf("Receive: %v", err)
	}
}
