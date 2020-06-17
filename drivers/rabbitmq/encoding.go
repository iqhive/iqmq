// Package rabbitmq maps messages to its native broker client.
package rabbitmq

import (
	mq "bitbucket.org/iqhive/iqmq"
	"github.com/streadway/amqp"
)

func encode(m mq.Message) amqp.Publishing {
	return amqp.Publishing{Body: append([]byte(nil), m.Body...), DeliveryMode: amqp.Persistent}
}
