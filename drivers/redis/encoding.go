// Package redis maps messages to its native broker client.
package redis

import (
	mq "bitbucket.org/iqhive/iqmq/legacy"
)

func encode(m mq.Message) string { return string(m.Body) }
