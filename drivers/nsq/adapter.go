// Package nsq maps messages to its native broker client.
package nsq

import (
	"context"

	mq "bitbucket.org/iqhive/iqmq"
	native "github.com/nsqio/go-nsq"
)

// New registers the handler before the caller connects the consumer to nsqd.
func New(producer *native.Producer, consumer *native.Consumer, topic string) mq.Backend {
	queue := make(chan *mq.Delivery, 64)
	consumer.AddHandler(native.HandlerFunc(func(m *native.Message) error {
		m.DisableAutoResponse()
		d := mq.NewDelivery(mq.Message{Body: append([]byte(nil), m.Body...)}, func(context.Context) error { m.Finish(); return nil }, func(context.Context) error { m.Requeue(-1); return nil })
		select {
		case queue <- d:
		case <-consumer.StopChan:
			m.Requeue(-1)
		}
		return nil
	}))
	return &mq.FuncBackend{
		PublishFunc: func(c context.Context, m mq.Message) error {
			if err := c.Err(); err != nil {
				return err
			}
			return producer.Publish(topic, encode(m))
		},
		ReceiveFunc: func(c context.Context) (*mq.Delivery, error) {
			select {
			case <-c.Done():
				return nil, c.Err()
			case d := <-queue:
				return d, nil
			case <-consumer.StopChan:
				return nil, mq.ErrClosed
			}
		},
		CloseFunc: func() error { producer.Stop(); consumer.Stop(); <-consumer.StopChan; return nil },
	}
}
