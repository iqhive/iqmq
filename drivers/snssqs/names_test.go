package snssqs_test

import (
	"errors"
	"strings"
	"testing"

	mq "github.com/iqhive/iqmq"
	sns "github.com/iqhive/iqmq/drivers/snssqs"
	sq "github.com/iqhive/iqmq/drivers/sqs"
)

// TestTopicNameMapping is the topic mapping, one level up from the queue
// driver's: a portable topic is a slash-separated path and an SNS topic name
// is not.
func TestTopicNameMapping(t *testing.T) {
	for _, tc := range []struct {
		topic  string
		prefix string
		want   string
	}{
		{"orders", "", "orders"},
		{"orders/eu", "", "orders-eu"},
		{"orders/eu/billing", "acme", "acmeorders-eu-billing"},
		{"a_b-c", "", "a_b-c"},
	} {
		got, err := sns.TopicName(tc.topic, tc.prefix)
		if err != nil {
			t.Fatalf("TopicName(%q, %q): %v", tc.topic, tc.prefix, err)
		}
		if got != tc.want {
			t.Errorf("TopicName(%q, %q) = %q, want %q", tc.topic, tc.prefix, got, tc.want)
		}
	}
	// The mapping is NOT injective, which is stated rather than hidden: two
	// portable topics become one SNS topic.
	a, _ := sns.TopicName("orders/eu", "")
	b, _ := sns.TopicName("orders-eu", "")
	if a != b {
		t.Errorf("the mapping is injective after all (%q vs %q); the driver page documents that it is not", a, b)
	}
}

// TestCheckTopicNameRefusals holds the SNS name grammar and the FIFO refusal.
func TestCheckTopicNameRefusals(t *testing.T) {
	for _, tc := range []struct {
		name string
		says string
		code mq.ErrorCode
	}{
		{"", "empty", mq.CodeInvalidArgument},
		{strings.Repeat("a", sns.MaxTopicNameLen+1), "256", mq.CodeInvalidArgument},
		{"orders.fifo", "FIFO", mq.CodeUnsupported},
		{"orders.eu", "alphanumerics", mq.CodeInvalidArgument},
		{"orders eu", "alphanumerics", mq.CodeInvalidArgument},
	} {
		err := sns.CheckTopicName(tc.name)
		if err == nil {
			t.Fatalf("CheckTopicName(%q) was accepted", tc.name)
		}
		if got := mq.CodeOf(err); got != tc.code {
			t.Errorf("CheckTopicName(%q) reported code %v (%v), want %v", tc.name, got, err, tc.code)
		}
		if !strings.Contains(err.Error(), tc.says) {
			t.Errorf("CheckTopicName(%q) does not mention %q: %v", tc.name, tc.says, err)
		}
	}
	if err := sns.CheckTopicName(strings.Repeat("a", sns.MaxTopicNameLen)); err != nil {
		t.Errorf("a name exactly at the limit was refused: %v", err)
	}
}

// TestQueueNameComposesTopicAndSubscription is where the fan-out lives: one
// topic, one queue per subscription.
func TestQueueNameComposesTopicAndSubscription(t *testing.T) {
	for _, tc := range []struct {
		topic, sub, tprefix, qprefix, want string
	}{
		{"orders", "billing", "", "", "orders-billing"},
		{"orders/eu", "billing", "", "", "orders-eu-billing"},
		{"orders", "billing", "acme", "q", "qacmeorders-billing"},
		{"orders", "a/b", "", "", "orders-a-b"},
	} {
		got, err := sns.QueueName(tc.topic, tc.sub, tc.tprefix, tc.qprefix)
		if err != nil {
			t.Fatalf("QueueName(%q, %q): %v", tc.topic, tc.sub, err)
		}
		if got != tc.want {
			t.Errorf("QueueName(%q, %q, %q, %q) = %q, want %q", tc.topic, tc.sub, tc.tprefix, tc.qprefix, got, tc.want)
		}
		if err := sq.CheckQueueName(got, false); err != nil {
			t.Errorf("the composed name %q is not one the queue service accepts: %v", got, err)
		}
	}
	// Two subscriptions of one topic are two queues, which is the whole
	// mechanism.
	a, _ := sns.QueueName("orders", "billing", "", "")
	b, _ := sns.QueueName("orders", "shipping", "", "")
	if a == b {
		t.Fatalf("two subscriptions of one topic map to one queue %q: the fan-out would deliver once and two consumers would compete", a)
	}
}

// TestQueueNameHashesWhatWillNotFit is.
//
// An SNS topic name may be 256 characters and a queue name may be 80, so two
// entirely legal inputs compose past the queue side's budget. The composed
// name then keeps a readable head and ends in a hash of the WHOLE identity —
// which is the part that matters: a mapping that only truncated would give two
// different (topic, subscription) pairs the same queue, and one
// subscription's consumers would settle the other's messages.
func TestQueueNameHashesWhatWillNotFit(t *testing.T) {
	long := strings.Repeat("topic", 20) // 100 characters, a legal SNS topic name
	if err := sns.CheckTopicName(long); err != nil {
		t.Fatalf("the test's own topic name is illegal: %v", err)
	}
	first, err := sns.QueueName(long, "alpha", "", "")
	if err != nil {
		t.Fatalf("QueueName with a long topic: %v", err)
	}
	second, err := sns.QueueName(long, "beta", "", "")
	if err != nil {
		t.Fatalf("QueueName with a long topic: %v", err)
	}
	for _, name := range []string{first, second} {
		if len(name) > sns.MaxQueueNameLen {
			t.Errorf("the composed name is %d characters (%q), and a queue name may be %d", len(name), name, sns.MaxQueueNameLen)
		}
		if err := sq.CheckQueueName(name, false); err != nil {
			t.Errorf("the composed name %q is not one the queue service accepts: %v", name, err)
		}
	}
	if first == second {
		t.Fatalf("two subscriptions of one long topic both map to %q: a mapping that truncated without hashing gives one subscription's consumers another subscription's messages", first)
	}
	if !strings.HasPrefix(first, "topic") {
		t.Errorf("the composed name keeps no readable head: %q", first)
	}
	// The hash is over the identity, so the same pair maps to the same queue
	// every time — a resolver that was not a function would lose a queue on
	// every restart.
	again, _ := sns.QueueName(long, "alpha", "", "")
	if again != first {
		t.Fatalf("the mapping is not a function: %q then %q", first, again)
	}
}

// TestQueueNameRefusals holds the parts a hash must not rescue.
//
// The hash makes every LONG name legal, so a subscription whose characters a
// queue name cannot hold has to be refused on its own: without that check,
// which of two answers a deployment got — a refusal, or a legal hashed name —
// would depend on how long its topic was.
func TestQueueNameRefusals(t *testing.T) {
	for _, tc := range []struct {
		name       string
		topic, sub string
		says       string
	}{
		{"empty subscription", "orders", "", "has to be named"},
		{"subscription with a space", "orders", "a b", "alphanumerics"},
		{"subscription with a dot", "orders", "a.b", "alphanumerics"},
		{"wildcard topic", "orders-*", "s", "pattern subscription"},
		{"fifo topic", "orders.fifo", "s", "FIFO"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := sns.QueueName(tc.topic, tc.sub, "", ""); err == nil {
				t.Fatalf("QueueName(%q, %q) was accepted", tc.topic, tc.sub)
			} else if !strings.Contains(err.Error(), tc.says) {
				t.Fatalf("the refusal does not mention %q: %v", tc.says, err)
			}
		})
	}
	// And the same subscription with a long topic is refused too, rather
	// than hashed into legality.
	long := strings.Repeat("topic", 20)
	if _, err := sns.QueueName(long, "a b", "", ""); err == nil {
		t.Fatalf("a subscription a queue name cannot hold was accepted because the composed name was long enough to be hashed")
	}
}

// TestARNHelpers holds the ARN shapes this driver builds and recognises.
func TestARNHelpers(t *testing.T) {
	arn := sns.TopicARN("", "eu-west-1", "000000000000", "orders")
	if want := "arn:aws:sns:eu-west-1:000000000000:orders"; arn != want {
		t.Fatalf("TopicARN = %q, want %q", arn, want)
	}
	if got := sns.TopicARN("aws-cn", "cn-north-1", "1", "orders"); !strings.HasPrefix(got, "arn:aws-cn:") {
		t.Errorf("a partition was ignored: %q", got)
	}
	if !sns.IsTopicARN(arn) {
		t.Errorf("IsTopicARN(%q) = false", arn)
	}
	// A QUEUE ARN also starts with "arn:", and publishing to one would be
	// publishing to nothing.
	if sns.IsTopicARN("arn:aws:sqs:eu-west-1:000000000000:orders-billing") {
		t.Errorf("IsTopicARN accepted an SQS queue ARN")
	}
	for _, s := range []string{"orders", "orders/eu", ""} {
		if sns.IsTopicARN(s) {
			t.Errorf("IsTopicARN(%q) = true", s)
		}
	}
	if got := sns.TopicNameOfARN(arn); got != "orders" {
		t.Errorf("TopicNameOfARN(%q) = %q", arn, got)
	}
	// A SUBSCRIPTION ARN is a topic ARN with one more element, and the topic
	// is still the sixth.
	if got := sns.TopicNameOfARN(arn + ":0f3a"); got != "orders" {
		t.Errorf("TopicNameOfARN of a subscription ARN = %q, want the topic", got)
	}
	if got := sns.TopicNameOfARN("orders"); got != "orders" {
		t.Errorf("TopicNameOfARN of a bare name = %q", got)
	}
}

// TestCheckFilterPolicy holds what a pure Validate can say about a policy.
func TestCheckFilterPolicy(t *testing.T) {
	if err := sns.CheckFilterPolicy("eu", `{"region":["eu","uk"]}`); err != nil {
		t.Fatalf("a well-formed policy was refused: %v", err)
	}
	if err := sns.CheckFilterPolicy("eu", `{"region":[{"prefix":"eu-"}]}`); err != nil {
		t.Fatalf("a policy with a matcher object was refused: %v", err)
	}
	for _, tc := range []struct{ sub, policy, says string }{
		{"", `{"a":["b"]}`, "subscription name"},
		{"eu", "", "matches everything"},
		{"eu", "{}", "matches everything"},
		{"eu", "not json", "JSON object"},
		{"eu", `["region"]`, "JSON object"},
		{"eu", strings.Repeat("x", sns.MaxFilterPolicyBytes+1), "256 KiB"},
	} {
		if err := sns.CheckFilterPolicy(tc.sub, tc.policy); err == nil {
			t.Errorf("CheckFilterPolicy(%q, %.20q) was accepted", tc.sub, tc.policy)
		} else if !strings.Contains(err.Error(), tc.says) {
			t.Errorf("CheckFilterPolicy(%q, %.20q) does not mention %q: %v", tc.sub, tc.policy, tc.says, err)
		}
	}
}

// TestFilterScopeStrings holds the enumeration to its slugs and its service
// attribute values, which is what a DSN and a Subscribe respectively carry.
func TestFilterScopeStrings(t *testing.T) {
	for _, tc := range []struct {
		scope     sns.FilterScope
		slug      string
		attribute string
	}{
		{sns.FilterScopeAttributes, "attributes", "MessageAttributes"},
		{sns.FilterScopeBody, "body", "MessageBody"},
	} {
		if got := tc.scope.String(); got != tc.slug {
			t.Errorf("%v.String() = %q, want %q", tc.scope, got, tc.slug)
		}
		if got := tc.scope.Attribute(); got != tc.attribute {
			t.Errorf("%v.Attribute() = %q, want the service's own %q", tc.scope, got, tc.attribute)
		}
		if !tc.scope.Valid() {
			t.Errorf("%v is not valid", tc.scope)
		}
	}
	if bad := sns.FilterScope(9); bad.Valid() || bad.Attribute() != "" {
		t.Errorf("a scope outside the enumeration is valid or has an attribute value")
	}
	if !strings.Contains(sns.FilterScope(9).String(), "?") {
		t.Errorf("a scope outside the enumeration renders as %q", sns.FilterScope(9).String())
	}
}

// TestNoWildcardRefusalIsUnsupportedNotInvalid is the detail that decides
// whether the negative direction for FeatureWildcardSubscribe can be proved
// at all.
//
// mq.ValidateTopic refuses a metacharacter as an invalid topic name, and the
// conformance kit's negative probe reads ErrUnsupported and nothing else — so
// the wildcard check has to come FIRST.
func TestNoWildcardRefusalIsUnsupportedNotInvalid(t *testing.T) {
	_, err := sns.TopicName("orders-*", "")
	if !errors.Is(err, mq.ErrUnsupported) {
		t.Fatalf("a pattern topic reported %v (code %v), want ErrUnsupported: the kit's negative probe reads that and nothing else", err, mq.CodeOf(err))
	}
}
