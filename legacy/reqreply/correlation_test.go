package reqreply

import (
	"context"
	"errors"
	mq "github.com/iqhive/iqmq/legacy"
	"testing"
)

func TestReplyAndCancellation(t *testing.T) {
	c := New(1)
	var id string
	m, err := c.Request(context.Background(), mq.Message{}, func(_ context.Context, key string, _ mq.Message) error {
		id = key
		if !c.Reply(key, mq.Message{Body: []byte("answer")}) {
			t.Fatal("reply refused")
		}
		return nil
	})
	if err != nil || string(m.Body) != "answer" {
		t.Fatal(m, err)
	}
	if c.Reply(id, mq.Message{}) {
		t.Fatal("late reply accepted")
	}
	ctx, cancel := context.WithCancel(context.Background())
	_, err = c.Request(ctx, mq.Message{}, func(context.Context, string, mq.Message) error { cancel(); return nil })
	if !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	m, err = c.Request(context.Background(), mq.Message{}, func(_ context.Context, key string, _ mq.Message) error {
		c.Reply(key, mq.Message{Body: []byte("again")})
		return nil
	})
	if err != nil || string(m.Body) != "again" {
		t.Fatal(m, err)
	}
}
