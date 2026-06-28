// Package snsapi is the narrow slice of the SNS API this driver uses
// (the specification's `internal/client/{iface,real,fake}.go`).
//
// It exists for the same reason `drivers/sqs/internal/sqsapi` does: every
// operation the driver performs has to be drivable by a scripted in-process
// server as well as by the real client, because the states this driver must
// get right are the SERVICE's to decide. A subscription that exists with raw
// delivery turned OFF is the one that matters most — a consumer would then
// receive SNS's JSON envelope around the publisher's body and every message
// would silently change — and no amount of correct code can make a real
// endpoint produce it on request, whereas
// [bitbucket.org/iqhive/iqmq/drivers/snssqs/internal/snsfake] produces it in
// microseconds. The same test then runs against LocalStack to prove the fake
// did not lie.
//
// The interface is written against the pinned SDK's own input and output types
// rather than an abstraction over them, so *sns.Client satisfies it as it is
// and the compiler keeps it honest.
package snsapi

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
)

// SNS is every SNS operation this driver performs.
//
// *sns.Client implements it. So does
// [bitbucket.org/iqhive/iqmq/drivers/snssqs/internal/snsfake.Server].
type SNS interface {
	// CreateTopic creates a topic, or returns the existing one when the
	// attributes match. It is idempotent at the service.
	CreateTopic(ctx context.Context, in *sns.CreateTopicInput, opts ...func(*sns.Options)) (*sns.CreateTopicOutput, error)
	// DeleteTopic removes a topic and every subscription on it.
	DeleteTopic(ctx context.Context, in *sns.DeleteTopicInput, opts ...func(*sns.Options)) (*sns.DeleteTopicOutput, error)
	// ListTopics lists topic ARNs.
	ListTopics(ctx context.Context, in *sns.ListTopicsInput, opts ...func(*sns.Options)) (*sns.ListTopicsOutput, error)
	// GetTopicAttributes reads a topic's attributes, which is how a topic
	// ARN is verified to name a topic that exists.
	GetTopicAttributes(ctx context.Context, in *sns.GetTopicAttributesInput, opts ...func(*sns.Options)) (*sns.GetTopicAttributesOutput, error)
	// Publish publishes one message to a topic.
	Publish(ctx context.Context, in *sns.PublishInput, opts ...func(*sns.Options)) (*sns.PublishOutput, error)
	// PublishBatch publishes at most ten messages, each entry with its own
	// outcome.
	PublishBatch(ctx context.Context, in *sns.PublishBatchInput, opts ...func(*sns.Options)) (*sns.PublishBatchOutput, error)
	// Subscribe subscribes an endpoint to a topic.
	Subscribe(ctx context.Context, in *sns.SubscribeInput, opts ...func(*sns.Options)) (*sns.SubscribeOutput, error)
	// Unsubscribe removes a subscription.
	Unsubscribe(ctx context.Context, in *sns.UnsubscribeInput, opts ...func(*sns.Options)) (*sns.UnsubscribeOutput, error)
	// ListSubscriptionsByTopic lists a topic's subscriptions, which is how
	// the driver finds the one that delivers into a given queue.
	ListSubscriptionsByTopic(ctx context.Context, in *sns.ListSubscriptionsByTopicInput, opts ...func(*sns.Options)) (*sns.ListSubscriptionsByTopicOutput, error)
	// GetSubscriptionAttributes reads a subscription's attributes. It is the
	// one call that answers whether raw message delivery is on.
	GetSubscriptionAttributes(ctx context.Context, in *sns.GetSubscriptionAttributesInput, opts ...func(*sns.Options)) (*sns.GetSubscriptionAttributesOutput, error)
	// SetSubscriptionAttributes writes one subscription attribute.
	SetSubscriptionAttributes(ctx context.Context, in *sns.SetSubscriptionAttributesInput, opts ...func(*sns.Options)) (*sns.SetSubscriptionAttributesOutput, error)
}

// Queues is the slice of the SQS API this driver performs ITSELF, over and
// above everything it delegates to `drivers/sqs`.
//
// It is two calls and no more, and the boundary is deliberate: the consume
// path, the settlement path, the queue resolution and the provisioning are all
// the SQS driver's, and duplicating any of them here would be a second
// implementation of behaviour the specification already got right. What is left is what only
// a fan-out needs — a queue's ARN, which is the Subscribe endpoint, and the
// queue's access policy, which is what lets SNS deliver into it at all.
type Queues interface {
	// GetQueueAttributes reads a queue's attributes, for its QueueArn.
	GetQueueAttributes(ctx context.Context, in *sqs.GetQueueAttributesInput, opts ...func(*sqs.Options)) (*sqs.GetQueueAttributesOutput, error)
	// SetQueueAttributes writes a queue's attributes, for its Policy.
	SetQueueAttributes(ctx context.Context, in *sqs.SetQueueAttributesInput, opts ...func(*sqs.Options)) (*sqs.SetQueueAttributesOutput, error)
}

// compile-time proof that the real clients are these.
var (
	_ SNS    = (*sns.Client)(nil)
	_ Queues = (*sqs.Client)(nil)
)
