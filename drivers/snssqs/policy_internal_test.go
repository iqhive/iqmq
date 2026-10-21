package snssqs

import (
	"encoding/json"
	"testing"
)

// TestPolicyAllowsReadsAnExistingGrant is the check allowTopic makes on a
// queue that already carries a policy. It used to make none: a queue whose
// policy did not let the topic deliver was subscribed anyway, and the
// subscription received nothing.
func TestPolicyAllowsReadsAnExistingGrant(t *testing.T) {
	t.Parallel()
	const (
		queue = "arn:aws:sqs:ap-southeast-2:123456789012:orders-workers"
		topic = "arn:aws:sns:ap-southeast-2:123456789012:orders"
	)
	own, err := json.Marshal(queuePolicy{
		Version: "2012-10-17",
		Statement: []queueStatement{{
			Sid: "iqmq-snssqs", Effect: "Allow",
			Principal: queuePrincipal{Service: "sns.amazonaws.com"},
			Action:    "sqs:SendMessage", Resource: queue,
			Condition: queueCondition{ArnEquals: map[string]string{"aws:SourceArn": topic}},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name string
		doc  string
		want bool
	}{
		{"the policy this driver writes", string(own), true},
		{"a single statement, a wildcard action and a wildcard topic", `{"Statement":{"Effect":"Allow",
			"Principal":{"Service":["sns.amazonaws.com"]},"Action":["sqs:*"],"Resource":"*",
			"Condition":{"ArnLike":{"aws:SourceArn":"arn:aws:sns:*:123456789012:*"}}}}`, true},
		{"everyone, unconditionally", `{"Statement":[{"Effect":"Allow","Principal":"*","Action":"SQS:SendMessage"}]}`, true},
		{"another topic", `{"Statement":[{"Effect":"Allow","Principal":{"Service":"sns.amazonaws.com"},
			"Action":"sqs:SendMessage","Resource":"` + queue + `",
			"Condition":{"ArnEquals":{"aws:SourceArn":"arn:aws:sns:ap-southeast-2:123456789012:payments"}}}]}`, false},
		{"an account, not SNS", `{"Statement":[{"Effect":"Allow","Principal":{"AWS":"arn:aws:iam::123456789012:root"},
			"Action":"sqs:*","Resource":"` + queue + `"}]}`, false},
		{"another queue", `{"Statement":[{"Effect":"Allow","Principal":"*","Action":"sqs:SendMessage",
			"Resource":"arn:aws:sqs:ap-southeast-2:123456789012:other"}]}`, false},
		{"a deny", `{"Statement":[{"Effect":"Deny","Principal":"*","Action":"sqs:SendMessage"}]}`, false},
		{"a receive grant only", `{"Statement":[{"Effect":"Allow","Principal":"*","Action":"sqs:ReceiveMessage"}]}`, false},
		{"not a policy", `not json`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := policyAllows(tc.doc, queue, topic); got != tc.want {
				t.Errorf("policyAllows = %v, want %v", got, tc.want)
			}
		})
	}
}
