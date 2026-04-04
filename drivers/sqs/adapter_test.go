package sqs

import (
	mq "bitbucket.org/iqhive/iqmq/legacy"
	"testing"
)

func TestEncodingOwnsPayload(t *testing.T) {
	body := []byte("order-42")
	m := encode("queue", mq.Message{Body: body})
	if *m.QueueUrl != "queue" {
		t.Fatal(*m.QueueUrl)
	}
	got := []byte(*m.MessageBody)
	body[0] = 'X'
	if string(got) != "order-42" {
		t.Fatalf("payload = %q", got)
	}
}
