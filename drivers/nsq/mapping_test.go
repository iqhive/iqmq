package nsq_test

import (
	"strings"
	"testing"

	gonsq "github.com/nsqio/go-nsq"

	mq "github.com/iqhive/iqmq"
	nq "github.com/iqhive/iqmq/drivers/nsq"
)

// TestTopicGrammarAgreesWithTheClient is the oracle FuzzNSQTopicName runs at
// scale, written out for the cases that matter.
//
// This driver validates a name by hand so that checking one costs no
// regular-expression match; go-nsq's own validator IS the regular expression.
// Two spellings of one grammar drift the day somebody edits either, and a test
// that runs both over the same input is the only thing that notices.
func TestTopicGrammarAgreesWithTheClient(t *testing.T) {
	for _, name := range []string{
		"", "a", "orders", "a.b_c-d", "ORDERS", "0",
		"a#ephemeral", "#ephemeral", "a#ephemerals", "a#ephemeral#ephemeral",
		strings.Repeat("x", 64), strings.Repeat("x", 65),
		"a/b", "a b", "a\x00b", "é", ".", "..", "a*", "a>", "a?",
	} {
		ours := nq.CheckTopic(name) == nil
		theirs := gonsq.IsValidTopicName(name)
		if ours != theirs {
			t.Errorf("CheckTopic(%q) says %v and go-nsq says %v", name, ours, theirs)
		}
		if (nq.CheckChannel(name) == nil) != gonsq.IsValidChannelName(name) {
			t.Errorf("CheckChannel(%q) disagrees with go-nsq", name)
		}
	}
}

// TestAPatternIsRefusedAsUnsupportedAndNotAsAMistake is what makes the specification's
// negative direction provable for FeatureWildcardSubscribe.
//
// A pattern metacharacter is outside NSQ's alphabet, so the grammar would
// refuse it as an invalid name — and the conformance kit's negative probe reads
// ErrUnsupported and nothing else. It is also the more useful answer for a
// caller who wrote a pattern on purpose: the feature is missing, not the name
// mistyped. A legitimate "#ephemeral" suffix is not a pattern.
func TestAPatternIsRefusedAsUnsupportedAndNotAsAMistake(t *testing.T) {
	for _, topic := range []string{"orders.*", "orders.>", "orders.?", "orders.#", "*"} {
		if _, err := nq.TopicOf("", topic); mq.CodeOf(err) != mq.CodeUnsupported {
			t.Errorf("TopicOf(%q) answered %v (%v), want CodeUnsupported", topic, mq.CodeOf(err), err)
		}
	}
	if _, err := nq.TopicOf("", "orders#ephemeral"); err != nil {
		t.Errorf("an ephemeral topic was refused as a pattern: %v", err)
	}
}

// TestTheTopicMapperIsAPurePrependAndNeverAnEscape is.
//
// A mapper that replaced the illegal bytes would map "a/b" and "a-b" onto one
// NSQ topic, and a caller would then find one subscription receiving another's
// messages. Refusing is the only answer that keeps the mapping injective.
func TestTheTopicMapperIsAPurePrependAndNeverAnEscape(t *testing.T) {
	got, err := nq.TopicOf("acme.", "orders")
	if err != nil || got != "acme.orders" {
		t.Fatalf("TopicOf(\"acme.\", \"orders\") = %q, %v", got, err)
	}
	if _, err := nq.TopicOf("acme.", "a/b"); err == nil {
		t.Fatal("a topic NSQ cannot name was mapped rather than refused; two portable topics would " +
			"then share one NSQ topic")
	}
	// A prefix that makes a legal topic illegal is refused too: the answer
	// is about the mapped name, not the caller's.
	if _, err := nq.TopicOf(strings.Repeat("p", 60), "orders"); err == nil {
		t.Fatal("a mapped name over sixty-four bytes was accepted")
	}
}

// TestASubscriptionIsAChannelAndCannotBeEmpty pins the mapping the specification gives this
// driver: a subscription is an nsqd channel, and a channel with no name is not
// a thing nsqd has.
func TestASubscriptionIsAChannelAndCannotBeEmpty(t *testing.T) {
	if _, err := nq.ChannelOf(""); mq.CodeOf(err) != mq.CodeInvalidArgument {
		t.Fatalf("ChannelOf(\"\") answered %v", err)
	}
	got, err := nq.ChannelOf("workers")
	if err != nil || got != "workers" {
		t.Fatalf("ChannelOf(\"workers\") = %q, %v", got, err)
	}
}

// TestRepresentableNamesEveryFieldNSQHasNoPlaceFor is what
// MetadataNativeStrict refuses on, and the list is short because the broker is.
func TestRepresentableNamesEveryFieldNSQHasNoPlaceFor(t *testing.T) {
	withHeader := &mq.Message{Body: []byte("b")}
	withHeader.Headers.Add("k", []byte("v"))
	for name, tc := range map[string]struct {
		m    *mq.Message
		want bool
	}{
		"a bare body":   {&mq.Message{Body: []byte("b")}, true},
		"a header":      {withHeader, false},
		"an identifier": {&mq.Message{Body: []byte("b"), ID: "id"}, false},
		"a property":    {&mq.Message{Body: []byte("b"), Properties: mq.Properties{ReplyTo: "r"}}, false},
		"a key":         {&mq.Message{Body: []byte("b"), Key: []byte("k")}, false},
		"an EMPTY key":  {&mq.Message{Body: []byte("b"), Key: []byte{}}, false},
		"a nil key":     {&mq.Message{Body: []byte("b"), Key: nil}, true},
	} {
		t.Run(name, func(t *testing.T) {
			err := nq.Representable(tc.m)
			if (err == nil) != tc.want {
				t.Fatalf("Representable answered %v, want representable=%v", err, tc.want)
			}
			if err != nil && mq.CodeOf(err) != mq.CodeUnsupported {
				t.Fatalf("the refusal carries %v, want CodeUnsupported: %v", mq.CodeOf(err), err)
			}
		})
	}
	if nq.Representable(nil) == nil {
		t.Error("a nil message was called representable")
	}
}
