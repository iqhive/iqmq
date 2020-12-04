// Package activemq selects the portable AMQP 1.0 backend for ActiveMQ brokers.
package activemq

import (
	mq "bitbucket.org/iqhive/iqmq"
	"bitbucket.org/iqhive/iqmq/drivers/amqp10"
	"pack.ag/amqp"
)

// New requires links attached to caller-provisioned Artemis or Classic destinations.
func New(sender *amqp.Sender, receiver *amqp.Receiver) mq.Backend {
	return amqp10.New(sender, receiver)
}
