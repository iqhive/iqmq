package kafka

import (
	mq "bitbucket.org/iqhive/iqmq"
	"testing"
)

func TestEncodingOwnsPayload(t *testing.T) {
	body := []byte("order-42")
	m := encode("orders", mq.Message{Body: body})
	if m.Topic != "orders" {
		t.Fatal(m.Topic)
	}
	got := m.Value
	body[0] = 'X'
	if string(got) != "order-42" {
		t.Fatalf("payload = %q", got)
	}
}
