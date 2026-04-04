package iqmq_test

import (
	"context"
	"errors"
	mq "bitbucket.org/iqhive/iqmq/legacy"
	"testing"
)

func TestProcessingErrorRetainsBothFailures(t *testing.T) {
	handler := errors.New("handler")
	settlement := errors.New("settlement")
	c := mq.NewClient(&mq.FuncBackend{ReceiveFunc: func(context.Context) (*mq.Delivery, error) {
		return mq.NewDelivery(mq.Message{}, nil, func(context.Context) error { return settlement }), nil
	}})
	err := c.Run(context.Background(), func(context.Context, mq.Message) error { return handler })
	var combined *mq.ProcessingError
	if !errors.As(err, &combined) || !errors.Is(err, handler) || !errors.Is(combined.Settlement, settlement) {
		t.Fatal(err)
	}
}
