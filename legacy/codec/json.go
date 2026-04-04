// Package codec encodes structured values into message bodies.
package codec

import (
	"encoding/json"
	mq "bitbucket.org/iqhive/iqmq/legacy"
)

func Encode(value interface{}) (mq.Message, error) {
	body, err := json.Marshal(value)
	return mq.Message{Body: body}, err
}
func Decode(message mq.Message, value interface{}) error { return json.Unmarshal(message.Body, value) }
