package codec

import (
	mq "bitbucket.org/iqhive/iqmq"
	"testing"
)

func TestJSON(t *testing.T) {
	m, err := Encode(struct{ ID int }{42})
	if err != nil {
		t.Fatal(err)
	}
	if string(m.Body) != `{"ID":42}` {
		t.Fatal(string(m.Body))
	}
	var v struct{ ID int }
	if err = Decode(m, &v); err != nil || v.ID != 42 {
		t.Fatal(v, err)
	}
	if err = Decode(mq.Message{Body: []byte("{")}, &v); err == nil {
		t.Fatal("accepted invalid JSON")
	}
}
