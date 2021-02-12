package iqmq_test

import (
	"context"
	"errors"
	mq "bitbucket.org/iqhive/iqmq"
	"bitbucket.org/iqhive/iqmq/memory"
	"testing"
)

func TestRunRequeuesFailedMessage(t *testing.T) {
	ctx := context.Background()
	c := mq.NewClient(memory.New(2))
	defer c.Close()
	c.Publish(ctx, mq.Message{Body: []byte("42")})
	failure := errors.New("handler")
	if err := c.Run(ctx, func(context.Context, mq.Message) error { return failure }); !errors.Is(err, failure) {
		t.Fatal(err)
	}
	d, err := c.Receive(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if string(d.Message.Body) != "42" {
		t.Fatal(string(d.Message.Body))
	}
	d.Ack(ctx)
}
