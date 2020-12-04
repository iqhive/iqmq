package activemq

import (
	mq "bitbucket.org/iqhive/iqmq"
	"pack.ag/amqp"
	"testing"
)

func TestAdapterShape(t *testing.T) {
	var constructor func(*amqp.Sender, *amqp.Receiver) mq.Backend = New
	if constructor == nil {
		t.Fatal("missing adapter")
	}
}
