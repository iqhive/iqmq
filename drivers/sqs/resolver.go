package sqs

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	mq "bitbucket.org/iqhive/iqmq"
	"bitbucket.org/iqhive/iqmq/drivers/sqs/internal/sqsapi"
	"bitbucket.org/iqhive/iqmq/internal/errcode"
)

// resolver turns a portable topic into the queue URL every API call needs
// (the specification's "topic→queue name/URL (resolver option)").
//
// The indirection is the service's, not iqmq's: every SQS operation takes a
// queue URL, and a URL is not something an application knows — it carries the
// region and the account. So a topic maps to a NAME by a pure function, and
// the name maps to a URL by one GetQueueUrl call whose answer is cached for
// the life of the backend. That cache is what keeps a publication from costing
// two round trips.
//
// Three things can resolve a topic, in this order, and each of them exists
// because a deployment asked for it:
//
//   - [Config.QueueURLs] maps a topic straight to a URL, for queues that are
//     not named after their topics at all. It costs no call.
//   - a topic that IS a URL is used verbatim, which is what a program that
//     already has the URL from its infrastructure does.
//   - otherwise [Config.Resolver] (or [QueueName]) produces the name, and
//     GetQueueUrl produces the URL.
//
// Only a SUCCESSFUL resolution is cached. A queue that does not exist yet must
// be re-resolved, or a consumer opened before its queue was created would
// never find it.
type resolver struct {
	api sqsapi.API
	cfg Config

	mu   sync.RWMutex
	urls map[string]string
	arns map[string]string
}

// newResolver builds the resolver for one backend.
func newResolver(api sqsapi.API, cfg Config) *resolver {
	return &resolver{
		api:  api,
		cfg:  cfg,
		urls: make(map[string]string, 8),
		arns: make(map[string]string, 4),
	}
}

// Name is the queue name a topic maps to, without any I/O.
//
// It is exported through [Backend.QueueName] so that a deployment can see what
// its topics become before it creates anything, and it is pure so that a topic
// the mapping cannot express is a refusal rather than a request.
func (r *resolver) Name(topic string) (string, error) {
	if url, ok := r.cfg.QueueURLs[topic]; ok {
		return QueueNameOfURL(url), nil
	}
	if isQueueURL(topic) {
		return QueueNameOfURL(topic), nil
	}
	if r.cfg.Resolver != nil {
		name, err := r.cfg.Resolver(topic)
		if err != nil {
			return "", err
		}
		if err := CheckQueueName(name, r.cfg.FIFO); err != nil {
			return "", err
		}
		return name, nil
	}
	return QueueName(topic, r.cfg.QueuePrefix, r.cfg.FIFO)
}

// URL is the queue URL a topic maps to, resolving and caching it.
func (r *resolver) URL(ctx context.Context, topic string) (string, error) {
	if url, ok := r.cfg.QueueURLs[topic]; ok {
		return url, nil
	}
	if isQueueURL(topic) {
		return topic, nil
	}
	r.mu.RLock()
	url, ok := r.urls[topic]
	r.mu.RUnlock()
	if ok {
		return url, nil
	}
	name, err := r.Name(topic)
	if err != nil {
		return "", err
	}
	out, err := r.api.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{QueueName: aws.String(name)})
	switch {
	case err == nil:
		return r.remember(topic, aws.ToString(out.QueueUrl)), nil
	case !isMissingQueue(err):
		return "", err
	case !r.cfg.AutoCreate:
		// The specification: the queue does not exist and this driver may not create it.
		// Naming the setting is what turns a mystery into a configuration
		// error.
		return "", errNoAutoCreate
	}
	created, err := r.create(ctx, name)
	if err != nil {
		return "", err
	}
	return r.remember(topic, created), nil
}

// Lookup is URL without the creation, for a caller that must not provision.
func (r *resolver) Lookup(ctx context.Context, topic string) (string, error) {
	if url, ok := r.cfg.QueueURLs[topic]; ok {
		return url, nil
	}
	if isQueueURL(topic) {
		return topic, nil
	}
	r.mu.RLock()
	url, ok := r.urls[topic]
	r.mu.RUnlock()
	if ok {
		return url, nil
	}
	name, err := r.Name(topic)
	if err != nil {
		return "", err
	}
	out, err := r.api.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{QueueName: aws.String(name)})
	if err != nil {
		return "", err
	}
	return r.remember(topic, aws.ToString(out.QueueUrl)), nil
}

// URLOfName resolves a queue NAME, which is what the dead-letter destination
// and the redrive source are: a queue shared by several topics is named
// directly rather than derived from one of them.
func (r *resolver) URLOfName(ctx context.Context, name string) (string, error) {
	if isQueueURL(name) {
		return name, nil
	}
	key := "\x00name:" + name
	r.mu.RLock()
	url, ok := r.urls[key]
	r.mu.RUnlock()
	if ok {
		return url, nil
	}
	if err := CheckQueueName(name, r.cfg.FIFO); err != nil {
		return "", err
	}
	out, err := r.api.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{QueueName: aws.String(name)})
	switch {
	case err == nil:
		return r.remember(key, aws.ToString(out.QueueUrl)), nil
	case !isMissingQueue(err) || !r.cfg.AutoCreate:
		return "", err
	}
	created, err := r.create(ctx, name)
	if err != nil {
		return "", err
	}
	return r.remember(key, created), nil
}

// create provisions a queue, with the attributes this configuration implies.
func (r *resolver) create(ctx context.Context, name string) (string, error) {
	attrs, err := r.attributes(ctx, name)
	if err != nil {
		return "", err
	}
	out, err := r.api.CreateQueue(ctx, &sqs.CreateQueueInput{
		QueueName: aws.String(name), Attributes: attrs,
	})
	if err != nil {
		return "", err
	}
	return aws.ToString(out.QueueUrl), nil
}

// attributes are the queue attributes a created queue is given.
//
// The redrive policy is the interesting one: it is the NATIVE dead-letter
// route, and it needs the destination's ARN rather than its name or its URL,
// which is one more call. A dead-letter queue that does not exist is created
// first, so that the redrive policy cannot name a queue the service will
// refuse.
func (r *resolver) attributes(ctx context.Context, name string) (map[string]string, error) {
	cfg := r.cfg.resolve()
	attrs := map[string]string{
		// Whole seconds, rounded UP. A sub-second deadline truncated to
		// zero would create the queue with a visibility timeout of ZERO,
		// which means every message is visible to every consumer at once
		// and nothing has a lease. ElasticMQ lets the per-receive timeout
		// override it and LocalStack does not, which is how the difference
		// was found.
		string(types.QueueAttributeNameVisibilityTimeout):      strconv.Itoa(int(visibilitySeconds(cfg.AckDeadline))),
		string(types.QueueAttributeNameMessageRetentionPeriod): strconv.Itoa(int(cfg.Retention / time.Second)),
		string(types.QueueAttributeNameMaximumMessageSize):     strconv.FormatInt(cfg.MaxMessageBytes, 10),
	}
	if cfg.FIFO {
		attrs[string(types.QueueAttributeNameFifoQueue)] = "true"
		if cfg.ContentBasedDeduplication {
			attrs[string(types.QueueAttributeNameContentBasedDeduplication)] = "true"
		}
	}
	if cfg.DeadLetterQueue == "" || cfg.MaxReceiveCount <= 0 || cfg.DeadLetterQueue == name {
		return attrs, nil
	}
	arn, err := r.DeadLetterARN(ctx)
	if err != nil {
		return nil, err
	}
	attrs[string(types.QueueAttributeNameRedrivePolicy)] =
		`{"deadLetterTargetArn":"` + arn + `","maxReceiveCount":"` + strconv.Itoa(cfg.MaxReceiveCount) + `"}`
	return attrs, nil
}

// DeadLetterARN is the ARN of the configured dead-letter queue, creating it
// when the driver may.
func (r *resolver) DeadLetterARN(ctx context.Context) (string, error) {
	if r.cfg.DeadLetterQueue == "" {
		return "", mq.ErrUnsupported
	}
	url, err := r.URLOfName(ctx, r.cfg.DeadLetterQueue)
	if err != nil {
		return "", err
	}
	return r.ARN(ctx, url)
}

// ARN is a queue's ARN, which the redrive policy and the move task need and
// which nothing else does. It is read from the queue's own attributes rather
// than assembled from the account and the region, because a configuration
// that named no account would otherwise assemble a wrong one silently.
func (r *resolver) ARN(ctx context.Context, url string) (string, error) {
	r.mu.RLock()
	arn, ok := r.arns[url]
	r.mu.RUnlock()
	if ok {
		return arn, nil
	}
	out, err := r.api.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
		QueueUrl:       aws.String(url),
		AttributeNames: []types.QueueAttributeName{types.QueueAttributeNameQueueArn},
	})
	if err != nil {
		return "", err
	}
	arn = out.Attributes[string(types.QueueAttributeNameQueueArn)]
	if arn == "" {
		return "", errNoARN
	}
	r.mu.Lock()
	r.arns[url] = arn
	r.mu.Unlock()
	return arn, nil
}

// remember caches a resolution and returns it.
func (r *resolver) remember(topic, url string) string {
	r.mu.Lock()
	r.urls[topic] = url
	r.mu.Unlock()
	return url
}

// Forget drops a cached resolution, which is what a caller does after
// deleting a queue: the next resolution has to ask the service again rather
// than hand out the URL of a queue that no longer exists.
func (r *resolver) Forget(topic string) {
	r.mu.Lock()
	delete(r.urls, topic)
	delete(r.urls, "\x00name:"+topic)
	r.mu.Unlock()
}

// isQueueURL reports whether s is already a queue URL rather than a topic.
func isQueueURL(s string) bool {
	return strings.HasPrefix(s, "https://") || strings.HasPrefix(s, "http://")
}

// isMissingQueue reports whether the service said the queue does not exist.
func isMissingQueue(err error) bool {
	var missing *types.QueueDoesNotExist
	if errors.As(err, &missing) {
		return true
	}
	var notFound *types.ResourceNotFoundException
	return errors.As(err, &notFound)
}

// The resolver's refusals.
var (
	errNoAutoCreate = errcode.New(errcode.NotFound,
		"iqmq/sqs: the queue does not exist and Config.AutoCreate is false, so this driver will not create it (the API)")
	errNoARN = errcode.New(errcode.Unknown,
		"iqmq/sqs: the service did not report the queue's ARN, which the redrive policy needs")
)
