package snssqs

import (
	mq "bitbucket.org/iqhive/iqmq"
	"testing"
)

func TestEncodingOwnsPayload(t *testing.T) {
	body := []byte("order-42")
	m := encode("topic", mq.Message{Body: body})
	if *m.TopicArn != "topic" {
		t.Fatal(*m.TopicArn)
	}
	got := []byte(*m.Message)
	body[0] = 'X'
	if string(got) != "order-42" {
		t.Fatalf("payload = %q", got)
	}
}
