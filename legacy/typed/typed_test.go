package typed

import (
	"context"
	mq "bitbucket.org/iqhive/iqmq/legacy"
	"bitbucket.org/iqhive/iqmq/legacy/memory"
	"testing"
)

func TestTypedPublish(t *testing.T) {
	c := mq.NewClient(memory.New(1))
	defer c.Close()
	p := NewPublisher[struct{ ID int }](c)
	if err := p.Publish(context.Background(), struct{ ID int }{42}); err != nil {
		t.Fatal(err)
	}
	d, err := c.Receive(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if string(d.Message.Body) != `{"ID":42}` {
		t.Fatal(string(d.Message.Body))
	}
}
