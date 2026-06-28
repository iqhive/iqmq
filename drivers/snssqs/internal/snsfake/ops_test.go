package snsfake

import (
	"context"
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/aws/aws-sdk-go-v2/service/sns/types"
)

// The refusals every operation shares, for the same reason
// drivers/sqs/sqsfake/ops_test.go has them: a driver that is working never
// addresses a topic that is not there and never has a fault installed, so the
// paths this fake exists to produce on demand were produced by nothing.

// topicRig is a server holding one topic, with its ARN.
func topicRig(t *testing.T) (*Server, string) {
	t.Helper()
	s := New(Options{})
	out, err := s.CreateTopic(t.Context(), &sns.CreateTopicInput{Name: aws.String("orders")})
	if err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	return s, aws.ToString(out.TopicArn)
}

// operations is every operation, against a topic ARN that may or may not
// name a topic.
func operations(s *Server, arn string) []struct {
	name string
	call func(context.Context) error
} {
	return []struct {
		name string
		call func(context.Context) error
	}{
		{"CreateTopic", func(ctx context.Context) error {
			_, err := s.CreateTopic(ctx, &sns.CreateTopicInput{Name: aws.String("fresh")})
			return err
		}},
		{"DeleteTopic", func(ctx context.Context) error {
			_, err := s.DeleteTopic(ctx, &sns.DeleteTopicInput{TopicArn: aws.String(arn)})
			return err
		}},
		{"ListTopics", func(ctx context.Context) error {
			_, err := s.ListTopics(ctx, &sns.ListTopicsInput{})
			return err
		}},
		{"GetTopicAttributes", func(ctx context.Context) error {
			_, err := s.GetTopicAttributes(ctx, &sns.GetTopicAttributesInput{TopicArn: aws.String(arn)})
			return err
		}},
		{"Subscribe", func(ctx context.Context) error {
			_, err := s.Subscribe(ctx, &sns.SubscribeInput{
				TopicArn: aws.String(arn), Protocol: aws.String("sqs"),
				Endpoint: aws.String("arn:aws:sqs:us-east-1:000000000000:q"),
			})
			return err
		}},
		{"ListSubscriptionsByTopic", func(ctx context.Context) error {
			_, err := s.ListSubscriptionsByTopic(ctx, &sns.ListSubscriptionsByTopicInput{
				TopicArn: aws.String(arn),
			})
			return err
		}},
		{"Publish", func(ctx context.Context) error {
			_, err := s.Publish(ctx, &sns.PublishInput{
				TopicArn: aws.String(arn), Message: aws.String("x"),
			})
			return err
		}},
		{"PublishBatch", func(ctx context.Context) error {
			_, err := s.PublishBatch(ctx, &sns.PublishBatchInput{
				TopicArn: aws.String(arn),
				PublishBatchRequestEntries: []types.PublishBatchRequestEntry{
					{Id: aws.String("1"), Message: aws.String("x")},
				},
			})
			return err
		}},
	}
}

// TestAnInstalledFaultReachesEveryOperation.
func TestAnInstalledFaultReachesEveryOperation(t *testing.T) {
	boom := errors.New("the endpoint is unavailable")
	for _, op := range operations(New(Options{}), "") {
		s, arn := topicRig(t)
		s.Fault(Fault{Err: boom})
		for _, tc := range operations(s, arn) {
			if tc.name != op.name {
				continue
			}
			if err := tc.call(t.Context()); err == nil {
				t.Errorf("%s succeeded with a fault installed for every operation", tc.name)
			}
		}
	}
}

// TestEveryOperationRespectsACancelledContext.
func TestEveryOperationRespectsACancelledContext(t *testing.T) {
	s, arn := topicRig(t)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	for _, tc := range operations(s, arn) {
		if err := tc.call(ctx); !errors.Is(err, context.Canceled) {
			t.Errorf("%s with a cancelled context is %v, want context.Canceled", tc.name, err)
		}
	}
}

// TestAnOperationOnATopicThatIsNotThereIsNotFound.
//
// It is what the driver maps onto CodeNotFound, and it is the case a
// deployment meets: a topic deleted underneath it.
func TestAnOperationOnATopicThatIsNotThereIsNotFound(t *testing.T) {
	s, _ := topicRig(t)
	gone := s.topicARN("never-created")

	for _, tc := range operations(s, gone) {
		if tc.name == "CreateTopic" || tc.name == "ListTopics" {
			continue // neither addresses an existing topic
		}
		err := tc.call(t.Context())
		if err == nil {
			t.Errorf("%s against a topic that does not exist returned nil", tc.name)
			continue
		}
		var missing *types.NotFoundException
		if !errors.As(err, &missing) {
			t.Errorf("%s against a topic that does not exist is %v (%T), want NotFoundException",
				tc.name, err, err)
		}
	}
}

// TestCreateTopicIsIdempotentAndCarriesItsAttributes.
//
// The service returns the EXISTING topic's ARN for a name it already has,
// which is what makes the driver's EnsureTopic one call rather than a
// lookup and a create with a window between them.
func TestCreateTopicIsIdempotentAndCarriesItsAttributes(t *testing.T) {
	s := New(Options{})
	first, err := s.CreateTopic(t.Context(), &sns.CreateTopicInput{
		Name:       aws.String("orders"),
		Attributes: map[string]string{"FifoTopic": "false"},
	})
	if err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	again, err := s.CreateTopic(t.Context(), &sns.CreateTopicInput{Name: aws.String("orders")})
	if err != nil {
		t.Fatalf("CreateTopic a second time: %v", err)
	}
	if aws.ToString(again.TopicArn) != aws.ToString(first.TopicArn) {
		t.Errorf("re-creating gave the ARN %q, want the existing %q",
			aws.ToString(again.TopicArn), aws.ToString(first.TopicArn))
	}

	attrs, err := s.GetTopicAttributes(t.Context(), &sns.GetTopicAttributesInput{TopicArn: first.TopicArn})
	if err != nil {
		t.Fatalf("GetTopicAttributes: %v", err)
	}
	if attrs.Attributes["FifoTopic"] != "false" {
		t.Errorf("the topic's attributes are %v; the ones the creation carried are missing", attrs.Attributes)
	}
	if attrs.Attributes[attrTopicARN] == "" {
		t.Error("the topic's attributes do not carry its own ARN, which the service always reports")
	}
}

// TestATopicWithNoNameIsRefused, and so is a subscription with no endpoint.
func TestATopicWithNoNameIsRefused(t *testing.T) {
	s, arn := topicRig(t)

	_, err := s.CreateTopic(t.Context(), &sns.CreateTopicInput{})
	var bad *types.InvalidParameterException
	if !errors.As(err, &bad) {
		t.Errorf("creating a topic with no name is %v (%T), want InvalidParameterException", err, err)
	}

	_, err = s.Subscribe(t.Context(), &sns.SubscribeInput{TopicArn: aws.String(arn)})
	if !errors.As(err, &bad) {
		t.Errorf("subscribing with no protocol or endpoint is %v (%T), want InvalidParameterException",
			err, err)
	}
}

// TestDeletingATopicTakesItsSubscriptionsWithIt.
//
// A subscription belongs to its topic at the service, so a driver that
// deleted a topic and then tried to clean up its subscriptions would be
// addressing things that are already gone.
func TestDeletingATopicTakesItsSubscriptionsWithIt(t *testing.T) {
	s, arn := topicRig(t)
	sub, err := s.Subscribe(t.Context(), &sns.SubscribeInput{
		TopicArn: aws.String(arn), Protocol: aws.String("sqs"),
		Endpoint: aws.String("arn:aws:sqs:us-east-1:000000000000:q"),
	})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	if _, err := s.GetSubscriptionAttributes(t.Context(), &sns.GetSubscriptionAttributesInput{
		SubscriptionArn: sub.SubscriptionArn,
	}); err != nil {
		t.Fatalf("the subscription is not there before the topic was deleted: %v", err)
	}

	if _, err := s.DeleteTopic(t.Context(), &sns.DeleteTopicInput{TopicArn: aws.String(arn)}); err != nil {
		t.Fatalf("DeleteTopic: %v", err)
	}
	if _, err := s.GetSubscriptionAttributes(t.Context(), &sns.GetSubscriptionAttributesInput{
		SubscriptionArn: sub.SubscriptionArn,
	}); err == nil {
		t.Error("the subscription outlived its topic; at the service a subscription belongs to its topic")
	}
}

// TestASubscriptionsAttributesAreReadableAndWritable.
//
// `RawMessageDelivery` is the one that matters: without it the SQS payload is
// the SNS envelope, and every message the driver reads is JSON wrapping the
// real body.
func TestASubscriptionsAttributesAreReadableAndWritable(t *testing.T) {
	s, arn := topicRig(t)
	sub, err := s.Subscribe(t.Context(), &sns.SubscribeInput{
		TopicArn: aws.String(arn), Protocol: aws.String("sqs"),
		Endpoint:   aws.String("arn:aws:sqs:us-east-1:000000000000:q"),
		Attributes: map[string]string{"RawMessageDelivery": "true"},
	})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	got, err := s.GetSubscriptionAttributes(t.Context(), &sns.GetSubscriptionAttributesInput{
		SubscriptionArn: sub.SubscriptionArn,
	})
	if err != nil {
		t.Fatalf("GetSubscriptionAttributes: %v", err)
	}
	if got.Attributes["RawMessageDelivery"] != "true" {
		t.Errorf("the subscription's attributes are %v; the ones Subscribe carried are missing", got.Attributes)
	}

	if _, err := s.SetSubscriptionAttributes(t.Context(), &sns.SetSubscriptionAttributesInput{
		SubscriptionArn: sub.SubscriptionArn,
		AttributeName:   aws.String("RawMessageDelivery"),
		AttributeValue:  aws.String("false"),
	}); err != nil {
		t.Fatalf("SetSubscriptionAttributes: %v", err)
	}
	got, err = s.GetSubscriptionAttributes(t.Context(), &sns.GetSubscriptionAttributesInput{
		SubscriptionArn: sub.SubscriptionArn,
	})
	if err != nil {
		t.Fatalf("GetSubscriptionAttributes after the set: %v", err)
	}
	if got.Attributes["RawMessageDelivery"] != "false" {
		t.Errorf("RawMessageDelivery is %q after being set to false", got.Attributes["RawMessageDelivery"])
	}

	// A subscription that is not there is not an empty one.
	if _, err := s.GetSubscriptionAttributes(t.Context(), &sns.GetSubscriptionAttributesInput{
		SubscriptionArn: aws.String("arn:aws:sns:us-east-1:000000000000:orders:no-such-sub"),
	}); err == nil {
		t.Error("GetSubscriptionAttributes answered for a subscription that does not exist")
	}
	if _, err := s.Unsubscribe(t.Context(), &sns.UnsubscribeInput{
		SubscriptionArn: aws.String("arn:aws:sns:us-east-1:000000000000:orders:no-such-sub"),
	}); err == nil {
		t.Error("Unsubscribe succeeded for a subscription that does not exist")
	}
}

// TestUnsubscribeRemovesItFromTheTopicsListing.
func TestUnsubscribeRemovesItFromTheTopicsListing(t *testing.T) {
	s, arn := topicRig(t)
	sub, err := s.Subscribe(t.Context(), &sns.SubscribeInput{
		TopicArn: aws.String(arn), Protocol: aws.String("sqs"),
		Endpoint: aws.String("arn:aws:sqs:us-east-1:000000000000:q"),
	})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	list, err := s.ListSubscriptionsByTopic(t.Context(), &sns.ListSubscriptionsByTopicInput{
		TopicArn: aws.String(arn),
	})
	if err != nil {
		t.Fatalf("ListSubscriptionsByTopic: %v", err)
	}
	if len(list.Subscriptions) != 1 {
		t.Fatalf("the topic reports %d subscriptions, want 1", len(list.Subscriptions))
	}

	if _, err := s.Unsubscribe(t.Context(), &sns.UnsubscribeInput{
		SubscriptionArn: sub.SubscriptionArn,
	}); err != nil {
		t.Fatalf("Unsubscribe: %v", err)
	}
	list, err = s.ListSubscriptionsByTopic(t.Context(), &sns.ListSubscriptionsByTopicInput{
		TopicArn: aws.String(arn),
	})
	if err != nil {
		t.Fatalf("ListSubscriptionsByTopic after the unsubscribe: %v", err)
	}
	if len(list.Subscriptions) != 0 {
		t.Errorf("the topic still reports %d subscriptions after Unsubscribe", len(list.Subscriptions))
	}
}
