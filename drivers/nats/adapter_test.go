package nats

import (
	mq "bitbucket.org/iqhive/iqmq/legacy"
	"testing"
)

func TestEncodingOwnsPayload(t *testing.T) {
	body := []byte("order-42")
	m := encode("orders", mq.Message{Body: body})
	if m.Subject != "orders" {
		t.Fatal(m.Subject)
	}
	got := m.Data
	body[0] = 'X'
	if string(got) != "order-42" {
		t.Fatalf("payload = %q", got)
	}
}
