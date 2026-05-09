package outbox_test

import (
	"testing"

	mq "bitbucket.org/iqhive/iqmq"
	"bitbucket.org/iqhive/iqmq/x/outbox"
)

// TestAllocsRowMessage: the relay builds one mq.Message per row, and that is
// the only allocation the conversion may cost.
//
// The row is built BEFORE the measured closure and the result is read after
// it, which is the discipline the 840-allocs/op finding established.
func TestAllocsRowMessage(t *testing.T) {
	row := outbox.Row{Topic: "orders", Key: []byte("k"), Body: []byte("body"), MessageID: "id-1"}
	var m *mq.Message
	var err error
	got := testing.AllocsPerRun(200, func() {
		m, err = row.Message()
	})
	if err != nil {
		t.Fatal(err)
	}
	if m.Topic != "orders" || string(m.Body) != "body" {
		t.Fatalf("Message = %+v", m)
	}
	if got > 1 {
		t.Errorf("Row.Message allocs/op = %v, want at most 1 (the message): the row's slices must be shared, not copied", got)
	}
	t.Logf("Row.Message allocs/op = %v", got)
}

// TestAllocsDecodeHeadersOnNoHeaders: the overwhelmingly common row has no
// headers, and that case must cost nothing at all -- it is a NULL column.
func TestAllocsDecodeHeadersOnNoHeaders(t *testing.T) {
	for name, blob := range map[string][]byte{"nil": nil, "empty": {}} {
		t.Run(name, func(t *testing.T) {
			var h mq.Headers
			var err error
			if got := testing.AllocsPerRun(200, func() {
				h, err = outbox.DecodeHeaders(blob)
			}); got != 0 {
				t.Errorf("DecodeHeaders(%s) allocs/op = %v, want 0", name, got)
			}
			if err != nil || h.Len() != 0 {
				t.Fatalf("DecodeHeaders = (%d, %v)", h.Len(), err)
			}
		})
	}
}

// TestAllocsEncodeHeadersOnNoHeaders: the same case on the append side.
func TestAllocsEncodeHeadersOnNoHeaders(t *testing.T) {
	var empty mq.Headers
	var out []byte
	if got := testing.AllocsPerRun(200, func() {
		out = outbox.EncodeHeaders(empty)
	}); got != 0 {
		t.Errorf("EncodeHeaders of no headers allocs/op = %v, want 0", got)
	}
	if out != nil {
		t.Errorf("EncodeHeaders of no headers = %x, want nil", out)
	}
}
