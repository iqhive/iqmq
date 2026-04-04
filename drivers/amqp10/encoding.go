// Package amqp10 maps messages to its native broker client.
package amqp10

import (
	mq "bitbucket.org/iqhive/iqmq/legacy"
	"pack.ag/amqp"
)

func encode(m mq.Message) *amqp.Message { return amqp.NewMessage(append([]byte(nil), m.Body...)) }
