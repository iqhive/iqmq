package iqmq_test

import (
	"context"
	"errors"
	mq "bitbucket.org/iqhive/iqmq/legacy"
	"bitbucket.org/iqhive/iqmq/legacy/memory"
	"testing"
)

func TestPublishReceiveAndSettlement(t *testing.T) {
	ctx := context.Background()
	c := mq.NewClient(memory.New(2))
	defer c.Close()
	body := []byte("order-42")
	if err := c.Publish(ctx, mq.Message{Body: body}); err != nil {
		t.Fatal(err)
	}
	body[0] = 'X'
	d, err := c.Receive(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if string(d.Message.Body) != "order-42" {
		t.Fatal("publisher retained caller storage")
	}
	if err = d.Nack(ctx); err != nil {
		t.Fatal(err)
	}
	if err = d.Ack(ctx); !errors.Is(err, mq.ErrSettled) {
		t.Fatal(err)
	}
	d, err = c.Receive(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if string(d.Message.Body) != "order-42" {
		t.Fatal(string(d.Message.Body))
	}
	if err = d.Ack(ctx); err != nil {
		t.Fatal(err)
	}
}
func TestCancellationAndClose(t *testing.T) {
	c := mq.NewClient(memory.New(1))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := c.Receive(ctx); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if err := c.Publish(ctx, mq.Message{}); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Receive(context.Background()); !errors.Is(err, mq.ErrClosed) {
		t.Fatal(err)
	}
}
func TestSettlementFailureCanRetry(t *testing.T) {
	calls := 0
	fail := errors.New("temporary")
	d := mq.NewDelivery(mq.Message{}, func(context.Context) error {
		calls++
		if calls == 1 {
			return fail
		}
		return nil
	}, nil)
	if err := d.Ack(context.Background()); !errors.Is(err, fail) {
		t.Fatal(err)
	}
	if err := d.Ack(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := d.Nack(context.Background()); !errors.Is(err, mq.ErrSettled) {
		t.Fatal(err)
	}
}
