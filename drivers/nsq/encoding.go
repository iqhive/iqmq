// Package nsq maps messages to its native broker client.
package nsq

import (
	mq "bitbucket.org/iqhive/iqmq/legacy"
)

func encode(m mq.Message) []byte { return append([]byte(nil), m.Body...) }
