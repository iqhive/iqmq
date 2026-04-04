// Package pulsar maps messages to its native broker client.
package pulsar

import (
	"github.com/apache/pulsar-client-go/pulsar"
	mq "bitbucket.org/iqhive/iqmq/legacy"
)

func encode(m mq.Message) *pulsar.ProducerMessage {
	return &pulsar.ProducerMessage{Payload: append([]byte(nil), m.Body...)}
}
