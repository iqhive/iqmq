package sqs

import (
	"context"
	"iter"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/internal/errcode"
)

// Admin returns the topology surface.
func (b *Backend) Admin() (mq.Admin, error) {
	if b.Closed() {
		return nil, mq.ErrClosed
	}
	return &admin{b: b}, nil
}

// TypedAdmin returns this driver's own administration, which is where the
// redrive lives (the specification's "DLQ native via redrive (typed Admin)").
//
// The specification keeps the portable [github.com/iqhive/iqmq.Admin] to the portable
// intersection — a topic's name and nothing else — so everything that is SQS's
// own is here: the queue's attributes, the redrive policy that IS the native
// dead-letter route, and the message-move task that drains a dead-letter queue
// back to its source.
func (b *Backend) TypedAdmin() (*TypedAdmin, error) {
	if b.Closed() {
		return nil, mq.ErrClosed
	}
	return &TypedAdmin{b: b}, nil
}

// admin is the portable topology surface.
type admin struct{ b *Backend }

var _ mq.Admin = (*admin)(nil)

// EnsureTopic creates the queue if it does not exist.
//
// It is idempotent: CreateQueue with the same attributes returns the existing
// queue, and CreateQueue with DIFFERENT ones is the service's own refusal
// rather than a silent reconfiguration.
func (a *admin) EnsureTopic(ctx context.Context, spec mq.TopicSpec) error {
	name, err := a.b.res.Name(spec.Name)
	if err != nil {
		return err
	}
	callCtx, cancel := a.b.call(ctx)
	defer cancel()
	attrs, err := a.b.res.attributes(callCtx, name)
	if err != nil {
		return a.b.errs.Map(mq.OpAdmin, spec.Name, "", err)
	}
	out, err := a.b.api.CreateQueue(callCtx, &sqs.CreateQueueInput{
		QueueName: aws.String(name), Attributes: attrs,
	})
	if err != nil {
		return a.b.errs.Map(mq.OpAdmin, spec.Name, "", err)
	}
	a.b.res.remember(spec.Name, aws.ToString(out.QueueUrl))
	return nil
}

// DeleteTopic removes the queue and everything in it.
func (a *admin) DeleteTopic(ctx context.Context, topic string) error {
	callCtx, cancel := a.b.call(ctx)
	defer cancel()
	url, err := a.b.res.Lookup(callCtx, topic)
	if err != nil {
		return a.b.errs.Map(mq.OpAdmin, topic, "", err)
	}
	if _, err := a.b.api.DeleteQueue(callCtx, &sqs.DeleteQueueInput{QueueUrl: aws.String(url)}); err != nil {
		return a.b.errs.Map(mq.OpAdmin, topic, "", err)
	}
	// The cached URL names a queue that no longer exists, and handing it out
	// again would produce a NonExistentQueue on every subsequent call.
	a.b.res.Forget(topic)
	return nil
}

// ListTopics iterates the queues the service reports, as topics.
//
// The names come back as queue names and are reported as such: the inverse of
// the topic mapping is not a function — "orders/eu" and "orders-eu" map to one
// queue — so inventing a topic from a name would be inventing the one a
// caller happened to use.
func (a *admin) ListTopics(ctx context.Context) iter.Seq2[string, error] {
	return func(yield func(string, error) bool) {
		var token *string
		for {
			callCtx, cancel := a.b.call(ctx)
			out, err := a.b.api.ListQueues(callCtx, &sqs.ListQueuesInput{
				QueueNamePrefix: prefixOrNil(a.b.cfg.QueuePrefix),
				NextToken:       token,
			})
			cancel()
			if err != nil {
				yield("", a.b.errs.Map(mq.OpAdmin, "", "", err))
				return
			}
			for _, url := range out.QueueUrls {
				// The PORTABLE topic, not the queue name: every other method
				// on this surface prepends the configured prefix to what it
				// is given, so yielding the raw name made the listing's
				// output unusable with the surface it came from.
				topic, ok := TopicOfQueueName(QueueNameOfURL(url), a.b.cfg.QueuePrefix)
				if !ok {
					continue
				}
				if !yield(topic, nil) {
					return
				}
			}
			if out.NextToken == nil || aws.ToString(out.NextToken) == "" {
				return
			}
			token = out.NextToken
		}
	}
}

// prefixOrNil is the list filter, and nil for a configuration with no prefix.
func prefixOrNil(prefix string) *string {
	if prefix == "" {
		return nil
	}
	return aws.String(prefix)
}

// EnsureSubscription is a no-op that succeeds for the subscription of a queue
// that exists.
//
// A queue IS its own subscription: there is nothing to create, and there is
// nothing to create it FROM — SourceConfig.Subscription is ignored by this
// driver because a queue has exactly one set of messages. Reporting
// ErrUnsupported would be wrong: the subscription a caller asked for does
// exist, and the operation has nothing left to do. What it does do is make
// sure the queue is there, so that a caller cannot be told a subscription
// exists on a queue that does not.
func (a *admin) EnsureSubscription(ctx context.Context, spec mq.SubscriptionSpec) error {
	return a.EnsureTopic(ctx, mq.TopicSpec{Name: spec.Topic})
}

// DeleteSubscription is refused: a subscription is the queue, and deleting it
// would delete the topic.
//
// Answering "done" would be worse than refusing. A caller who deleted a
// subscription and found every message still being consumed would have been
// told something false; ErrUnsupported says what is true, and DeleteTopic is
// the operation that exists.
func (a *admin) DeleteSubscription(context.Context, string, string) error {
	return errNoSubscriptionDelete
}

// Purge discards everything the queue holds.
//
// The service allows one purge per queue per sixty seconds and reports
// PurgeQueueInProgress for a second one, which maps to CodeConflict rather
// than to a retryable failure: a caller who retried immediately would be
// refused for the rest of the minute.
func (a *admin) Purge(ctx context.Context, topic, sub string) error {
	callCtx, cancel := a.b.call(ctx)
	defer cancel()
	url, err := a.b.res.Lookup(callCtx, topic)
	if err != nil {
		return a.b.errs.Map(mq.OpAdmin, topic, sub, err)
	}
	if _, err := a.b.api.PurgeQueue(callCtx, &sqs.PurgeQueueInput{QueueUrl: aws.String(url)}); err != nil {
		return a.b.errs.Map(mq.OpAdmin, topic, sub, err)
	}
	return nil
}

// Stats reports the queue's depth, in-flight count and lag.
//
// Every number the service gives is APPROXIMATE and is reported as such: the
// counts are eventually consistent, which is what the service's own attribute
// names say. Lag is the depth plus the delayed messages — everything that has
// not been delivered yet — because a queue has no committed position for a lag
// to be measured from.
func (a *admin) Stats(ctx context.Context, topic, sub string) (mq.QueueStats, error) {
	callCtx, cancel := a.b.call(ctx)
	defer cancel()
	url, err := a.b.res.Lookup(callCtx, topic)
	if err != nil {
		return mq.QueueStats{}, a.b.errs.Map(mq.OpAdmin, topic, sub, err)
	}
	out, err := a.b.api.GetQueueAttributes(callCtx, &sqs.GetQueueAttributesInput{
		QueueUrl: aws.String(url),
		AttributeNames: []types.QueueAttributeName{
			types.QueueAttributeNameApproximateNumberOfMessages,
			types.QueueAttributeNameApproximateNumberOfMessagesNotVisible,
			types.QueueAttributeNameApproximateNumberOfMessagesDelayed,
		},
	})
	if err != nil {
		return mq.QueueStats{}, a.b.errs.Map(mq.OpAdmin, topic, sub, err)
	}
	depth, okDepth := int64Attr(out.Attributes, types.QueueAttributeNameApproximateNumberOfMessages)
	hidden, okHidden := int64Attr(out.Attributes, types.QueueAttributeNameApproximateNumberOfMessagesNotVisible)
	delayed, okDelayed := int64Attr(out.Attributes, types.QueueAttributeNameApproximateNumberOfMessagesDelayed)
	stats := mq.QueueStats{}
	if okDepth {
		stats.Depth = mq.Limit[int64]{Value: depth, Known: true}
	}
	if okHidden {
		stats.InFlight = mq.Limit[int64]{Value: hidden, Known: true}
	}
	if okDepth && okDelayed {
		stats.Lag = mq.Limit[int64]{Value: depth + delayed, Known: true}
	}
	return stats, nil
}

// int64Attr reads one queue attribute as a count, reporting whether the
// service sent it. The specification forbids reading an unadvertised number as zero, and an
// attribute the service omitted is exactly that.
func int64Attr(attrs map[string]string, name types.QueueAttributeName) (int64, bool) {
	v, ok := attrs[string(name)]
	if !ok || v == "" {
		return 0, false
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		return 0, false
	}
	return n, true
}

// TypedAdmin is this driver's own administration: the queue attributes, the
// redrive policy and the message-move task.
type TypedAdmin struct{ b *Backend }

// RedrivePolicy is a queue's native dead-letter route.
type RedrivePolicy struct {
	// DeadLetterQueue is the name of the queue a message is moved to.
	DeadLetterQueue string
	// MaxReceiveCount is how many receives a message survives first.
	MaxReceiveCount int
}

// Attributes reads a queue's attributes, which is the escape hatch for
// everything the specification keeps out of the portable surface.
func (a *TypedAdmin) Attributes(ctx context.Context, topic string) (map[string]string, error) {
	callCtx, cancel := a.b.call(ctx)
	defer cancel()
	url, err := a.b.res.Lookup(callCtx, topic)
	if err != nil {
		return nil, a.b.errs.Map(mq.OpAdmin, topic, "", err)
	}
	out, err := a.b.api.GetQueueAttributes(callCtx, &sqs.GetQueueAttributesInput{
		QueueUrl:       aws.String(url),
		AttributeNames: []types.QueueAttributeName{types.QueueAttributeNameAll},
	})
	if err != nil {
		return nil, a.b.errs.Map(mq.OpAdmin, topic, "", err)
	}
	return out.Attributes, nil
}

// SetRedrivePolicy points a queue's native dead-letter route at a queue.
//
// It is the NATIVE half of FeatureDeadLetter: the service moves a message it
// has delivered MaxReceiveCount times, with no settlement from the consumer at
// all, which is what makes a message nobody ever settles safe. The other half
// is an explicit Reject, which the settler routes.
//
// A zero MaxReceiveCount removes the policy, which is how a queue's redrive is
// turned off.
func (a *TypedAdmin) SetRedrivePolicy(ctx context.Context, topic string, policy RedrivePolicy) error {
	callCtx, cancel := a.b.call(ctx)
	defer cancel()
	url, err := a.b.res.Lookup(callCtx, topic)
	if err != nil {
		return a.b.errs.Map(mq.OpAdmin, topic, "", err)
	}
	value := ""
	if policy.MaxReceiveCount > 0 {
		if policy.DeadLetterQueue == "" {
			return errRedriveWithoutQueue
		}
		dlq, err := a.b.res.URLOfName(callCtx, policy.DeadLetterQueue)
		if err != nil {
			return a.b.errs.Map(mq.OpAdmin, topic, "", err)
		}
		arn, err := a.b.res.ARN(callCtx, dlq)
		if err != nil {
			return a.b.errs.Map(mq.OpAdmin, topic, "", err)
		}
		value = `{"deadLetterTargetArn":"` + arn + `","maxReceiveCount":"` +
			strconv.Itoa(policy.MaxReceiveCount) + `"}`
	}
	_, err = a.b.api.SetQueueAttributes(callCtx, &sqs.SetQueueAttributesInput{
		QueueUrl:   aws.String(url),
		Attributes: map[string]string{string(types.QueueAttributeNameRedrivePolicy): value},
	})
	if err != nil {
		return a.b.errs.Map(mq.OpAdmin, topic, "", err)
	}
	return nil
}

// RedriveRequest asks for a dead-letter queue to be drained.
type RedriveRequest struct {
	// Source is the queue to move messages OUT of, by topic or by queue
	// name. It is normally the dead-letter queue.
	Source string
	// Destination is the queue to move them INTO, by topic or by queue
	// name. Empty asks the service to return each message to the queue it
	// was dead-lettered from, which only works for a genuine dead-letter
	// queue.
	Destination string
	// Rate bounds the move, in messages per second. Zero lets the service
	// choose.
	Rate int
}

// Redrive moves messages off a dead-letter queue, which is the specification's "DLQ native
// via redrive (typed Admin)".
//
// It is asynchronous at the service: the call starts a task and returns its
// handle, and the messages move afterwards. So a caller that wants to know the
// move finished watches the destination queue rather than this call, which is
// why the handle is returned rather than swallowed.
//
// The task needs ARNs rather than URLs, which is two more calls; both are
// cached for the life of the backend.
func (a *TypedAdmin) Redrive(ctx context.Context, req RedriveRequest) (string, error) {
	if req.Source == "" {
		return "", errNoRedriveSource
	}
	callCtx, cancel := a.b.call(ctx)
	defer cancel()
	srcARN, err := a.arnOf(callCtx, req.Source)
	if err != nil {
		return "", a.b.errs.Map(mq.OpAdmin, req.Source, "", err)
	}
	in := &sqs.StartMessageMoveTaskInput{SourceArn: aws.String(srcARN)}
	if req.Destination != "" {
		dstARN, err := a.arnOf(callCtx, req.Destination)
		if err != nil {
			return "", a.b.errs.Map(mq.OpAdmin, req.Destination, "", err)
		}
		in.DestinationArn = aws.String(dstARN)
	}
	if req.Rate > 0 {
		in.MaxNumberOfMessagesPerSecond = aws.Int32(int32(req.Rate))
	}
	out, err := a.b.api.StartMessageMoveTask(callCtx, in)
	if err != nil {
		return "", a.b.errs.Map(mq.OpAdmin, req.Source, "", err)
	}
	return aws.ToString(out.TaskHandle), nil
}

// arnOf resolves a topic or a queue name to an ARN.
func (a *TypedAdmin) arnOf(ctx context.Context, nameOrTopic string) (string, error) {
	url, err := a.b.res.Lookup(ctx, nameOrTopic)
	if err != nil {
		url, err = a.b.res.URLOfName(ctx, nameOrTopic)
		if err != nil {
			return "", err
		}
	}
	return a.b.res.ARN(ctx, url)
}

// SetVisibilityTimeout changes a queue's default visibility timeout, which is
// the lease every delivery of it gets when a source names none.
func (a *TypedAdmin) SetVisibilityTimeout(ctx context.Context, topic string, d time.Duration) error {
	if d <= 0 || d > MaxVisibility {
		return errAckDeadline
	}
	callCtx, cancel := a.b.call(ctx)
	defer cancel()
	url, err := a.b.res.Lookup(callCtx, topic)
	if err != nil {
		return a.b.errs.Map(mq.OpAdmin, topic, "", err)
	}
	_, err = a.b.api.SetQueueAttributes(callCtx, &sqs.SetQueueAttributesInput{
		QueueUrl: aws.String(url),
		Attributes: map[string]string{
			string(types.QueueAttributeNameVisibilityTimeout): strconv.Itoa(int(d / time.Second)),
		},
	})
	if err != nil {
		return a.b.errs.Map(mq.OpAdmin, topic, "", err)
	}
	return nil
}

// The admin refusals.
var (
	errNoSubscriptionDelete = errcode.New(errcode.Unsupported,
		"iqmq/sqs: a subscription IS the queue, so deleting one would delete the topic; DeleteTopic is the operation that exists")
	errNoRedriveSource = errcode.New(errcode.InvalidArgument,
		"iqmq/sqs: a redrive needs a source queue")
)
