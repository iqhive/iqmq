package iqmq_test

import (
	"context"
	"errors"
	mq "bitbucket.org/iqhive/iqmq/legacy"
	"testing"
)

func TestBatchPrefixAndBound(t *testing.T) {
	calls := 0
	failure := errors.New("broker")
	c := mq.NewClient(&mq.FuncBackend{PublishFunc: func(context.Context, mq.Message) error {
		calls++
		if calls == 2 {
			return failure
		}
		return nil
	}})
	n, err := c.PublishBatch(context.Background(), make([]mq.Message, 3))
	if n != 1 || !errors.Is(err, failure) || calls != 2 {
		t.Fatal(n, err, calls)
	}
	n, err = c.PublishBatch(context.Background(), make([]mq.Message, 1025))
	if n != 0 || !errors.Is(err, mq.ErrBatchTooLarge) || calls != 2 {
		t.Fatal(n, err, calls)
	}
}
