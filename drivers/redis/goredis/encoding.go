// Package goredis maps messages to its native broker client.
package goredis

import (
	mq "bitbucket.org/iqhive/iqmq"
)

func encode(m mq.Message) map[string]interface{} {
	return map[string]interface{}{"body": string(m.Body)}
}
