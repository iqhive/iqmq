package main

import (
	"strings"
	"testing"

	mq "github.com/iqhive/iqmq"
)

// TestTextOutputPrintsALargeSequenceUnsigned: a broker sequence is a uint64,
// and the text forms of a delivery's metadata and of a publication receipt
// converted it to int64 to print it, so a sequence above 2^63 printed as a
// negative number. The JSON form was already right.
func TestTextOutputPrintsALargeSequenceUnsigned(t *testing.T) {
	const seq = 1<<63 + 5
	const want = "sequence=9223372036854775813"

	c := captured{
		msg:  &mq.Message{Topic: "orders", Body: []byte("x")},
		info: mq.DeliveryInfo{Topic: "orders", Sequence: seq, HasSequence: true},
	}
	var b strings.Builder
	renderCaptured(&b, 1, &c, "acked", &renderOptions{meta: true})
	if !strings.Contains(b.String(), want) {
		t.Errorf("the delivery line does not carry %q:\n%s", want, b.String())
	}

	if line := receiptLine(&mq.PublishReceipt{Sequence: seq}); !strings.Contains(line, want) {
		t.Errorf("the receipt line does not carry %q: %s", want, line)
	}
}
