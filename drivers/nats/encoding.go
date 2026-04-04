// Package nats maps messages to its native broker client.
package nats

import (
	mq "bitbucket.org/iqhive/iqmq/legacy"
	"github.com/nats-io/nats.go"
)

func encode(subject string, m mq.Message) *nats.Msg {
	return &nats.Msg{Subject: subject, Data: append([]byte(nil), m.Body...)}
}
