package main

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/memory"
)

// fixture executes the public API. No timing, message IDs, or random metadata
// enters the frames. A timeout makes a broken fixture fail rather than hang.
func fixture() (body, key string, attempts [2]int, err error) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	client, err := memory.Open(ctx, memory.Config{AutoCreate: true})
	if err != nil {
		return "", "", attempts, fmt.Errorf("hero fixture: %w", err)
	}
	defer func() {
		if cerr := client.Close(); cerr != nil && err == nil {
			err = fmt.Errorf("hero fixture: %w", cerr)
		}
	}()
	body, key, attempts, err = deliverTwice(ctx, client)
	if err != nil {
		return "", "", attempts, fmt.Errorf("hero fixture: %w", err)
	}
	return body, key, attempts, nil
}

// deliverTwice publishes one message, nacks its first delivery and acks the
// redelivery, reporting what both deliveries carried.
func deliverTwice(ctx context.Context, client *mq.Client) (body, key string, attempts [2]int, err error) {
	c, err := client.Consumer(ctx, mq.ConsumerConfig{Topic: "orders", Subscription: "billing"})
	if err != nil {
		return "", "", attempts, err
	}
	if err := client.Publish(ctx, "orders", &mq.Message{ID: "order-1", Key: []byte("cust-42"), Body: []byte(`{"id":1}`)}); err != nil {
		return "", "", attempts, err
	}
	var dst [1]mq.Delivery
	n, err := c.Receive(ctx, dst[:])
	if err != nil {
		return "", "", attempts, err
	}
	if n != 1 {
		return "", "", attempts, errors.New("expected one delivery")
	}
	body, key, attempts[0] = string(dst[0].Body), string(dst[0].Key), dst[0].Attempt
	if err := dst[0].Nack(); err != nil {
		return "", "", attempts, err
	}
	n, err = c.Receive(ctx, dst[:])
	if err != nil {
		return "", "", attempts, err
	}
	if n != 1 || string(dst[0].Body) != body || string(dst[0].Key) != key {
		return "", "", attempts, errors.New("redelivery changed the message")
	}
	attempts[1] = dst[0].Attempt
	if err := dst[0].Ack(); err != nil {
		return "", "", attempts, err
	}
	return body, key, attempts, nil
}

func (m *film) typed(y int, s string, ms int) {
	for i := 0; i <= len(s); i++ {
		m.cur.Clear(y, y+1)
		m.cur.Text(3, y, s[:i], plain)
		m.cur.Text(3+i, y, "▏", caret)
		m.cut(ms)
	}
	m.cur.Clear(y, y+1)
	m.cur.Text(3, y, s, plain)
}

func (m *film) queue() error {
	body, key, _, err := fixture()
	if err != nil {
		return err
	}
	m.cur = frame{}
	m.cur.Text(3, 1, "01 / PUBLISH → RECEIVE", bright)
	m.cur.Text(3, 2, "Real memory driver · orders / billing · no broker required", muted)
	m.cur.Text(3, 4, `msg := &mq.Message{Key: []byte("cust-42"), Body: []byte(`+"`"+body+"`"+`)} `, lit)
	m.typed(6, `client.Publish(ctx, "orders", msg)`, 35)
	m.cur.Text(3, 8, "producer", muted)
	m.cur.Text(34, 8, "orders", muted)
	m.cur.Text(59, 8, "billing", muted)
	for step := range 8 {
		m.cur.Clear(10, 12)
		m.cur.Text(3, 10, "───────", dim)
		m.cur.Text(17, 10, "────────────────────────────────────────────────────", dim)
		m.cur.Text(10+step*7, 10, "████", on)
		m.cut(140)
	}
	m.cur.Text(3, 13, `consumer.Receive(ctx, deliveries[:])`, plain)
	m.cur.Text(3, 15, fmt.Sprintf("1 delivery  key=%s  body=%s", key, body), bright)
	m.cur.Text(3, 17, "Bytes arrive unchanged. Settlement is explicit.", muted)
	m.cut(1900)
	return nil
}

func (m *film) redelivery() error {
	body, key, attempts, err := fixture()
	if err != nil {
		return err
	}
	m.cur = frame{}
	m.cur.Text(3, 1, "02 / NACK → REDELIVER → ACK", bright)
	m.cur.Text(3, 2, "Real memory driver · one message, two delivery attempts", muted)
	m.cur.Text(3, 4, fmt.Sprintf("key=%s  body=%s", key, body), lit)
	m.cur.Text(3, 7, fmt.Sprintf("attempt %d", attempts[0]), bright)
	m.typed(9, "delivery.Nack()", 55)
	for step := range 9 {
		m.cur.Clear(11, 13)
		m.cur.Text(3, 11, "requeue", muted)
		m.cur.Text(16, 11, strings.Repeat("─", 48), dim)
		m.cur.Text(60-step*5, 11, "████", hot)
		m.cut(125)
	}
	m.cur.Text(3, 14, fmt.Sprintf("attempt %d  same key · same body", attempts[1]), bright)
	m.typed(16, "delivery.Ack()", 55)
	m.cur.Text(3, 18, "Settled. The subscription has finished this message.", muted)
	m.cut(1800)
	return nil
}
