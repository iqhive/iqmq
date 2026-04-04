// Package rabbitmq maps messages to its native broker client.
package rabbitmq

import (
	mq "bitbucket.org/iqhive/iqmq/legacy"
	"github.com/rabbitmq/amqp091-go"
)

func encode(m mq.Message) amqp091.Publishing {
	return amqp091.Publishing{Body: append([]byte(nil), m.Body...), DeliveryMode: amqp091.Persistent}
}
