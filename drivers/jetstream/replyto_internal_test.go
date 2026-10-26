package jetstream

import (
	"testing"
)

// TestDeliveryReplyToIsNotTheAckSubject holds that a delivery never exposes the
// server's $JS.ACK subject as Properties.ReplyTo. A JetStream message's native
// reply subject is its acknowledgement address, not an application reply-to: a
// responder that published to it would never reach the requester, and would ack
// the message outside the settler.
func TestDeliveryReplyToIsNotTheAckSubject(t *testing.T) {
	src := newScriptedSource(t, 4)
	m := &scriptedMsg{}
	meta, _ := m.Metadata()
	msg, err := src.decode(m)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	d, err := src.delivery(m, meta, msg)
	if err != nil {
		t.Fatalf("delivery: %v", err)
	}
	if got := d.Message.Properties.ReplyTo; got != "" {
		t.Fatalf("Properties.ReplyTo = %q, want empty (the ack subject is %q)", got, m.Reply())
	}
}
