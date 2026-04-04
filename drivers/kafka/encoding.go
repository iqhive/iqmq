// Package kafka maps messages to its native broker client.
package kafka

import (
	mq "bitbucket.org/iqhive/iqmq/legacy"
	"github.com/twmb/kafka-go/pkg/kgo"
)

func encode(topic string, m mq.Message) *kgo.Record {
	return &kgo.Record{Topic: topic, Value: append([]byte(nil), m.Body...)}
}
