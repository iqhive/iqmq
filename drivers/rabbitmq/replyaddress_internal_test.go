package rabbitmq

import (
	"testing"

	mq "github.com/iqhive/iqmq"
)

// FuzzReplyTopic holds the escaping of a direct reply-to address to the two
// things the reply path rests on: the escaped form of every address the
// broker can write is a valid topic name the core accepts, and it turns back
// into exactly the address the broker wrote, which is the routing key the
// reply needs.
//
// The seeds are what RabbitMQ 4.3.5 wrote, what the fake writes, and the
// corners: an address with nothing to escape, one with the escape byte in it,
// and addresses that are not reply addresses at all.
func FuzzReplyTopic(f *testing.F) {
	for _, seed := range []string{
		"amq.rabbitmq.reply-to.g1h2AA5yZXBseUA0NDgxNDkxNwAAB+MAAAAAauT+Jg==.HZc5a7BxOjB/Wivxbuep4Q==",
		"amq.rabbitmq.reply-to.+/+/cmFiYml0ZmFrZUAwLjI=.+/+/AAAAAAAAAAAAAAAAAQ==",
		"amq.rabbitmq.reply-to.g1AAAAB",
		"amq.rabbitmq.reply-to._2B",
		"amq.rabbitmq.reply-to.a_b/c",
		ReplyAddress,
		"orders",
		"",
	} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, addr string) {
		topic := replyTopic(addr)
		if !IsReplyAddress(addr) {
			if topic != addr {
				t.Fatalf("replyTopic(%q) = %q, want a name that is not a reply address left alone", addr, topic)
			}
			return
		}
		if got := replyKey(topic); got != addr {
			t.Fatalf("replyKey(replyTopic(%q)) = %q, want the address back", addr, got)
		}
		if !IsReplyAddress(topic) {
			t.Fatalf("replyTopic(%q) = %q, which IsReplyAddress does not recognise", addr, topic)
		}
		if len(topic) <= 255 && addr != ReplyAddress {
			if err := mq.ValidateTopic(topic); err != nil {
				t.Fatalf("replyTopic(%q) = %q, which the core refuses: %v", addr, topic, err)
			}
		}
	})
}

// TestAnEscapedReplyAddressNamesTheBrokersOwn pins the escaped spelling of the
// address RabbitMQ 4.3.5 wrote, so that a change to the escaping is a
// deliberate one: a responder of one version publishing to a name another
// version escaped differently would route its reply nowhere.
func TestAnEscapedReplyAddressNamesTheBrokersOwn(t *testing.T) {
	const (
		addr = "amq.rabbitmq.reply-to.g1h2AA5yZXBseUA0NDgxNDkxNwAAB+MAAAAAauT+Jg==.HZc5a7BxOjB/Wivxbuep4Q=="
		want = "amq.rabbitmq.reply-to._g1h2AA5yZXBseUA0NDgxNDkxNwAAB_2BMAAAAAauT_2BJg_3D_3D.HZc5a7BxOjB_2FWivxbuep4Q_3D_3D"
	)
	if got := replyTopic(addr); got != want {
		t.Fatalf("replyTopic(%q) = %q, want %q", addr, got, want)
	}
	if got := replyKey(want); got != addr {
		t.Fatalf("replyKey(%q) = %q, want %q", want, got, addr)
	}
	if got := replyTopic("amq.rabbitmq.reply-to.g1AAAAB"); got != "amq.rabbitmq.reply-to.g1AAAAB" {
		t.Fatalf("an address with nothing to escape became %q", got)
	}
}
