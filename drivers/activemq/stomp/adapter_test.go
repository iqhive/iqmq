package stomp

import (
	mq "bitbucket.org/iqhive/iqmq"
	"testing"
)

func TestEncodingOwnsPayload(t *testing.T) {
	body := []byte("order-42")
	got := encode(mq.Message{Body: body})
	body[0] = 'X'
	if string(got) != "order-42" {
		t.Fatalf("payload = %q", got)
	}
}
