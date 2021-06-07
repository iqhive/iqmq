// Package jetstream maps messages to its native broker client.
package jetstream

import (
	mq "bitbucket.org/iqhive/iqmq"
	"github.com/nats-io/nats.go"
)

func encode(subject string, m mq.Message) *nats.Msg {
	return &nats.Msg{Subject: subject, Data: append([]byte(nil), m.Body...)}
}
