package iqmq_test

import (
	"context"
	"errors"
	mq "bitbucket.org/iqhive/iqmq"
	"bitbucket.org/iqhive/iqmq/memory"
	"testing"
)

func TestAsyncDrainAndClose(t *testing.T) {
	c := mq.NewClient(memory.New(3))
	defer c.Close()
	p := mq.NewAsyncPublisher(c, 2)
	body := []byte("42")
	receipt, err := p.Publish(context.Background(), mq.Message{Body: body})
	if err != nil {
		t.Fatal(err)
	}
	body[0] = 'X'
	if err = p.Close(); err != nil {
		t.Fatal(err)
	}
	if err = <-receipt; err != nil {
		t.Fatal(err)
	}
	d, err := c.Receive(context.Background())
	if err != nil || string(d.Message.Body) != "42" {
		t.Fatal(d, err)
	}
	if _, err = p.Publish(context.Background(), mq.Message{}); !errors.Is(err, mq.ErrClosed) {
		t.Fatal(err)
	}
}
