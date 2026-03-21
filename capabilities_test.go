package iqmq_test

import (
	mq "bitbucket.org/iqhive/iqmq"
	"bitbucket.org/iqhive/iqmq/memory"
	"testing"
)

func TestCapabilitiesDoNotGuess(t *testing.T) {
	c := mq.NewClient(memory.New(1))
	defer c.Close()
	caps := c.Capabilities()
	if caps.Durable || !caps.ExplicitAck || !caps.Nack {
		t.Fatal(caps)
	}
	if got := mq.NewClient(&mq.FuncBackend{}).Capabilities(); got != (mq.Capabilities{}) {
		t.Fatal(got)
	}
}
