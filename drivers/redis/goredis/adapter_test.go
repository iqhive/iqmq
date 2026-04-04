package goredis

import (
	mq "bitbucket.org/iqhive/iqmq/legacy"
	"testing"
)

func TestEncodingOwnsPayload(t *testing.T) {
	body := []byte("order-42")
	m := encode(mq.Message{Body: body})
	got := []byte(m["body"].(string))
	body[0] = 'X'
	if string(got) != "order-42" {
		t.Fatalf("payload = %q", got)
	}
}
