package iqmq_test

import (
	"context"
	mq "bitbucket.org/iqhive/iqmq/legacy"
	"testing"
)

func TestMiddlewareOrder(t *testing.T) {
	var got string
	wrap := func(s string) mq.Middleware {
		return func(next mq.Handler) mq.Handler {
			return func(c context.Context, m mq.Message) error { got += s; err := next(c, m); got += s; return err }
		}
	}
	h := mq.Chain(func(context.Context, mq.Message) error { got += "H"; return nil }, wrap("A"), wrap("B"))
	if err := h(context.Background(), mq.Message{}); err != nil {
		t.Fatal(err)
	}
	if got != "ABHBA" {
		t.Fatal(got)
	}
}
