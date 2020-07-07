// Package kafkago maps messages to its native broker client.
package kafkago

import (
	mq "bitbucket.org/iqhive/iqmq"
	"github.com/segmentio/kafka-go"
)

func encode(m mq.Message) kafka.Message { return kafka.Message{Value: append([]byte(nil), m.Body...)} }
