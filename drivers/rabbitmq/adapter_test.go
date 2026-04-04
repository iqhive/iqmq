package rabbitmq

import (
	mq "bitbucket.org/iqhive/iqmq/legacy"
	"testing"
)

func TestEncodingOwnsPayload(t *testing.T) {
	body := []byte("order-42")
	m := encode(mq.Message{Body: body})
	if m.DeliveryMode != 2 {
		t.Fatal(m.DeliveryMode)
	}
	got := m.Body
	body[0] = 'X'
	if string(got) != "order-42" {
		t.Fatalf("payload = %q", got)
	}
}
