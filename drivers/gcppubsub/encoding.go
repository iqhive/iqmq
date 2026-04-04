// Package gcppubsub maps messages to its native broker client.
package gcppubsub

import (
	"cloud.google.com/go/pubsub/v2"
	mq "bitbucket.org/iqhive/iqmq/legacy"
)

func encode(m mq.Message) *pubsub.Message {
	return &pubsub.Message{Data: append([]byte(nil), m.Body...)}
}
