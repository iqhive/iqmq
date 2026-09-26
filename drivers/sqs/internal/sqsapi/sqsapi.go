// Package sqsapi is the narrow slice of the SQS API this driver uses
// (the specification's `internal/client/{iface,real,fake}.go`).
//
// The interface is written against the pinned SDK's own input and output types
// rather than an abstraction over them. That is deliberate: an abstraction
// would have to be kept honest by hand, and *sqs.Client satisfies this as it
// is, so the compiler keeps it honest instead.
package sqsapi

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/service/sqs"
)

// API is every SQS operation this driver performs.
//
// *sqs.Client implements it. So does
// [github.com/iqhive/iqmq/drivers/sqs/sqsfake.Server].
type API interface {
	// CreateQueue creates a queue, or returns the existing one when the
	// attributes match.
	CreateQueue(ctx context.Context, in *sqs.CreateQueueInput, opts ...func(*sqs.Options)) (*sqs.CreateQueueOutput, error)
	// DeleteQueue removes a queue and everything in it.
	DeleteQueue(ctx context.Context, in *sqs.DeleteQueueInput, opts ...func(*sqs.Options)) (*sqs.DeleteQueueOutput, error)
	// GetQueueUrl resolves a queue name to its URL. The spelling is the
	// SDK's: this interface is satisfied by *sqs.Client, so the method name
	// is not a choice.
	GetQueueUrl(ctx context.Context, in *sqs.GetQueueUrlInput, opts ...func(*sqs.Options)) (*sqs.GetQueueUrlOutput, error)
	// ListQueues lists queue URLs, optionally by name prefix.
	ListQueues(ctx context.Context, in *sqs.ListQueuesInput, opts ...func(*sqs.Options)) (*sqs.ListQueuesOutput, error)
	// GetQueueAttributes reads a queue's attributes.
	GetQueueAttributes(ctx context.Context, in *sqs.GetQueueAttributesInput, opts ...func(*sqs.Options)) (*sqs.GetQueueAttributesOutput, error)
	// SetQueueAttributes writes a queue's attributes.
	SetQueueAttributes(ctx context.Context, in *sqs.SetQueueAttributesInput, opts ...func(*sqs.Options)) (*sqs.SetQueueAttributesOutput, error)
	// PurgeQueue discards everything a queue holds.
	PurgeQueue(ctx context.Context, in *sqs.PurgeQueueInput, opts ...func(*sqs.Options)) (*sqs.PurgeQueueOutput, error)
	// SendMessage publishes one message.
	SendMessage(ctx context.Context, in *sqs.SendMessageInput, opts ...func(*sqs.Options)) (*sqs.SendMessageOutput, error)
	// SendMessageBatch publishes at most ten messages, each entry with its
	// own outcome.
	SendMessageBatch(ctx context.Context, in *sqs.SendMessageBatchInput, opts ...func(*sqs.Options)) (*sqs.SendMessageBatchOutput, error)
	// ReceiveMessage long-polls for at most ten messages.
	ReceiveMessage(ctx context.Context, in *sqs.ReceiveMessageInput, opts ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error)
	// DeleteMessage settles one receipt handle.
	DeleteMessage(ctx context.Context, in *sqs.DeleteMessageInput, opts ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error)
	// DeleteMessageBatch settles at most ten receipt handles, each entry
	// with its own outcome.
	DeleteMessageBatch(ctx context.Context, in *sqs.DeleteMessageBatchInput, opts ...func(*sqs.Options)) (*sqs.DeleteMessageBatchOutput, error)
	// ChangeMessageVisibility resets one message's visibility window.
	ChangeMessageVisibility(ctx context.Context, in *sqs.ChangeMessageVisibilityInput, opts ...func(*sqs.Options)) (*sqs.ChangeMessageVisibilityOutput, error)
	// ChangeMessageVisibilityBatch resets at most ten visibility windows.
	ChangeMessageVisibilityBatch(ctx context.Context, in *sqs.ChangeMessageVisibilityBatchInput, opts ...func(*sqs.Options)) (*sqs.ChangeMessageVisibilityBatchOutput, error)
	// StartMessageMoveTask redrives a dead-letter queue back to its source.
	StartMessageMoveTask(ctx context.Context, in *sqs.StartMessageMoveTaskInput, opts ...func(*sqs.Options)) (*sqs.StartMessageMoveTaskOutput, error)
}

// compile-time proof that the real client is one.
var _ API = (*sqs.Client)(nil)
