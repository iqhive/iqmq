package claimcheck_test

import (
	"context"

	mq "github.com/iqhive/iqmq"
)

// handlerFunc adapts a body-only callback to mq.Handler, so that a fuzz
// target's body does not repeat the Delivery plumbing three times.
func handlerFunc(fn func([]byte)) mq.Handler {
	return mq.HandlerFunc(func(_ context.Context, d *mq.Delivery) error {
		fn(d.Body)
		return nil
	})
}

// delivery wraps a body as a *mq.Delivery.
func delivery(body []byte) *mq.Delivery {
	return &mq.Delivery{Message: mq.Message{Body: body}}
}

// publishRaw runs one body through a publish middleware and returns what
// would have gone on the wire. It is publish() without a testing.TB, for the
// fuzz targets.
func publishRaw(pm mq.PublishMiddleware, body []byte) ([]byte, error) {
	var out []byte
	err := pm(func(_ context.Context, _ string, ms []*mq.Message) error {
		out = ms[0].Body
		return nil
	})(context.Background(), "t", []*mq.Message{{Body: body}})
	return out, err
}
