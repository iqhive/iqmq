package snsfake

import (
	"context"
	"strconv"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	snstypes "github.com/aws/aws-sdk-go-v2/service/sns/types"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"

	"bitbucket.org/iqhive/iqmq/drivers/snssqs/internal/snsapi"
)

// CreateTopic creates a topic, or returns the existing one.
func (s *Server) CreateTopic(ctx context.Context, in *sns.CreateTopicInput, _ ...func(*sns.Options)) (*sns.CreateTopicOutput, error) {
	name := aws.ToString(in.Name)
	s.mu.Lock()
	defer s.mu.Unlock()
	if f := s.enter("CreateTopic", name); f != nil && f.Err != nil {
		return nil, f.Err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if name == "" {
		return nil, invalid("no topic name")
	}
	if _, ok := s.topics[name]; ok {
		// CreateTopic is idempotent at the service and returns the
		// existing topic's ARN, which is what makes EnsureTopic one call.
		return &sns.CreateTopicOutput{TopicArn: aws.String(s.topicARN(name))}, nil
	}
	t := &topic{name: name, attrs: map[string]string{}}
	for k, v := range in.Attributes {
		t.attrs[k] = v
	}
	s.topics[name] = t
	s.names = append(s.names, name)
	return &sns.CreateTopicOutput{TopicArn: aws.String(s.topicARN(name))}, nil
}

// DeleteTopic removes a topic and every subscription on it, which is the
// service's behaviour: a subscription belongs to its topic.
func (s *Server) DeleteTopic(ctx context.Context, in *sns.DeleteTopicInput, _ ...func(*sns.Options)) (*sns.DeleteTopicOutput, error) {
	arn := aws.ToString(in.TopicArn)
	name := topicNameOfARN(arn)
	s.mu.Lock()
	defer s.mu.Unlock()
	if f := s.enter("DeleteTopic", name); f != nil && f.Err != nil {
		return nil, f.Err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if _, ok := s.topics[name]; !ok {
		return nil, notFound(arn)
	}
	delete(s.topics, name)
	for i, n := range s.names {
		if n == name {
			s.names = append(s.names[:i], s.names[i+1:]...)
			break
		}
	}
	for subARN, sub := range s.subs {
		if sub.topicARN == arn {
			delete(s.subs, subARN)
		}
	}
	return &sns.DeleteTopicOutput{}, nil
}

// ListTopics lists topic ARNs.
func (s *Server) ListTopics(ctx context.Context, _ *sns.ListTopicsInput, _ ...func(*sns.Options)) (*sns.ListTopicsOutput, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if f := s.enter("ListTopics", ""); f != nil && f.Err != nil {
		return nil, f.Err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	out := &sns.ListTopicsOutput{}
	for _, n := range s.names {
		out.Topics = append(out.Topics, snstypes.Topic{TopicArn: aws.String(s.topicARN(n))})
	}
	return out, nil
}

// GetTopicAttributes reads a topic's attributes, which is how an ARN is
// verified to name a topic that exists.
func (s *Server) GetTopicAttributes(ctx context.Context, in *sns.GetTopicAttributesInput, _ ...func(*sns.Options)) (*sns.GetTopicAttributesOutput, error) {
	arn := aws.ToString(in.TopicArn)
	name := topicNameOfARN(arn)
	s.mu.Lock()
	defer s.mu.Unlock()
	if f := s.enter("GetTopicAttributes", name); f != nil && f.Err != nil {
		return nil, f.Err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	t, ok := s.topics[name]
	if !ok || arn != s.topicARN(name) {
		return nil, notFound(arn)
	}
	attrs := map[string]string{attrTopicARN: arn}
	for k, v := range t.attrs {
		attrs[k] = v
	}
	return &sns.GetTopicAttributesOutput{Attributes: attrs}, nil
}

// Subscribe subscribes an endpoint to a topic.
//
// It refuses a second subscription with the same protocol and endpoint, which
// the real service also does: a subscription's identity IS what it delivers
// to, so two would be one.
func (s *Server) Subscribe(ctx context.Context, in *sns.SubscribeInput, _ ...func(*sns.Options)) (*sns.SubscribeOutput, error) {
	arn := aws.ToString(in.TopicArn)
	name := topicNameOfARN(arn)
	s.mu.Lock()
	defer s.mu.Unlock()
	if f := s.enter("Subscribe", name); f != nil && f.Err != nil {
		return nil, f.Err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if _, ok := s.topics[name]; !ok || arn != s.topicARN(name) {
		return nil, notFound(arn)
	}
	protocol, endpoint := aws.ToString(in.Protocol), aws.ToString(in.Endpoint)
	if protocol == "" || endpoint == "" {
		return nil, invalid("a subscription needs a protocol and an endpoint")
	}
	for _, sub := range s.subs {
		if sub.topicARN == arn && sub.protocol == protocol && sub.endpoint == endpoint {
			return &sns.SubscribeOutput{SubscriptionArn: aws.String(sub.arn)}, nil
		}
	}
	s.seq++
	sub := &subscription{
		arn:      arn + ":" + strconv.FormatUint(s.seq, 10),
		topicARN: arn,
		protocol: protocol,
		endpoint: endpoint,
		attrs:    map[string]string{},
	}
	for k, v := range in.Attributes {
		sub.attrs[k] = v
	}
	s.subs[sub.arn] = sub
	return &sns.SubscribeOutput{SubscriptionArn: aws.String(sub.arn)}, nil
}

// Unsubscribe removes a subscription.
func (s *Server) Unsubscribe(ctx context.Context, in *sns.UnsubscribeInput, _ ...func(*sns.Options)) (*sns.UnsubscribeOutput, error) {
	arn := aws.ToString(in.SubscriptionArn)
	s.mu.Lock()
	defer s.mu.Unlock()
	if f := s.enter("Unsubscribe", topicNameOfARN(arn)); f != nil && f.Err != nil {
		return nil, f.Err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if _, ok := s.subs[arn]; !ok {
		return nil, notFound(arn)
	}
	delete(s.subs, arn)
	return &sns.UnsubscribeOutput{}, nil
}

// ListSubscriptionsByTopic lists a topic's subscriptions.
func (s *Server) ListSubscriptionsByTopic(ctx context.Context, in *sns.ListSubscriptionsByTopicInput, _ ...func(*sns.Options)) (*sns.ListSubscriptionsByTopicOutput, error) {
	arn := aws.ToString(in.TopicArn)
	name := topicNameOfARN(arn)
	s.mu.Lock()
	defer s.mu.Unlock()
	if f := s.enter("ListSubscriptionsByTopic", name); f != nil && f.Err != nil {
		return nil, f.Err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if _, ok := s.topics[name]; !ok || arn != s.topicARN(name) {
		return nil, notFound(arn)
	}
	out := &sns.ListSubscriptionsByTopicOutput{}
	for _, sub := range s.subs {
		if sub.topicARN != arn {
			continue
		}
		out.Subscriptions = append(out.Subscriptions, snstypes.Subscription{
			SubscriptionArn: aws.String(sub.arn),
			TopicArn:        aws.String(sub.topicARN),
			Protocol:        aws.String(sub.protocol),
			Endpoint:        aws.String(sub.endpoint),
		})
	}
	return out, nil
}

// GetSubscriptionAttributes reads a subscription's attributes. It is the one
// call that answers whether raw message delivery is on.
func (s *Server) GetSubscriptionAttributes(ctx context.Context, in *sns.GetSubscriptionAttributesInput, _ ...func(*sns.Options)) (*sns.GetSubscriptionAttributesOutput, error) {
	arn := aws.ToString(in.SubscriptionArn)
	s.mu.Lock()
	defer s.mu.Unlock()
	if f := s.enter("GetSubscriptionAttributes", topicNameOfARN(arn)); f != nil && f.Err != nil {
		return nil, f.Err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	sub, ok := s.subs[arn]
	if !ok {
		return nil, notFound(arn)
	}
	attrs := map[string]string{
		attrSubARN:   sub.arn,
		attrTopicARN: sub.topicARN,
		attrProtocol: sub.protocol,
		attrEndpoint: sub.endpoint,
	}
	for k, v := range sub.attrs {
		attrs[k] = v
	}
	return &sns.GetSubscriptionAttributesOutput{Attributes: attrs}, nil
}

// SetSubscriptionAttributes writes one subscription attribute.
func (s *Server) SetSubscriptionAttributes(ctx context.Context, in *sns.SetSubscriptionAttributesInput, _ ...func(*sns.Options)) (*sns.SetSubscriptionAttributesOutput, error) {
	arn := aws.ToString(in.SubscriptionArn)
	s.mu.Lock()
	defer s.mu.Unlock()
	if f := s.enter("SetSubscriptionAttributes", topicNameOfARN(arn)); f != nil && f.Err != nil {
		return nil, f.Err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	sub, ok := s.subs[arn]
	if !ok {
		return nil, notFound(arn)
	}
	name, value := aws.ToString(in.AttributeName), aws.ToString(in.AttributeValue)
	if name == "" {
		return nil, invalid("no attribute name")
	}
	if value == "" {
		delete(sub.attrs, name)
	} else {
		sub.attrs[name] = value
	}
	return &sns.SetSubscriptionAttributesOutput{}, nil
}

// Publish publishes one message and fans it out.
func (s *Server) Publish(ctx context.Context, in *sns.PublishInput, _ ...func(*sns.Options)) (*sns.PublishOutput, error) {
	arn := aws.ToString(in.TopicArn)
	name := topicNameOfARN(arn)
	s.mu.Lock()
	if f := s.enter("Publish", name); f != nil && f.Err != nil {
		s.mu.Unlock()
		return nil, f.Err
	}
	if err := ctx.Err(); err != nil {
		s.mu.Unlock()
		return nil, err
	}
	if _, ok := s.topics[name]; !ok || arn != s.topicARN(name) {
		s.mu.Unlock()
		return nil, notFound(arn)
	}
	body := aws.ToString(in.Message)
	if body == "" {
		// The service requires a non-empty Message, which is the reason
		// drivers/sqs carries a body placeholder for a nil or empty
		// payload. Refusing it here is what makes the placeholder's
		// absence a test failure rather than a surprise against AWS.
		s.mu.Unlock()
		return nil, invalid("Message is empty")
	}
	s.seq++
	id := "sns-" + strconv.FormatUint(s.seq, 10)
	pending := s.deliver(arn, body, in.MessageAttributes, id)
	s.mu.Unlock()
	s.flush(ctx, pending)
	return &sns.PublishOutput{MessageId: aws.String(id)}, nil
}

// PublishBatch publishes at most ten messages, each entry with its own
// outcome.
//
// The per-entry faults are the point: a real endpoint will not fail three
// entries of ten on request, and an index-aligned result is exactly what a
// batch publish has to get right.
func (s *Server) PublishBatch(ctx context.Context, in *sns.PublishBatchInput, _ ...func(*sns.Options)) (*sns.PublishBatchOutput, error) {
	arn := aws.ToString(in.TopicArn)
	name := topicNameOfARN(arn)
	s.mu.Lock()
	fault := s.enter("PublishBatch", name)
	if fault != nil && fault.Err != nil {
		s.mu.Unlock()
		return nil, fault.Err
	}
	if err := ctx.Err(); err != nil {
		s.mu.Unlock()
		return nil, err
	}
	if _, ok := s.topics[name]; !ok || arn != s.topicARN(name) {
		s.mu.Unlock()
		return nil, notFound(arn)
	}
	entries := in.PublishBatchRequestEntries
	switch {
	case len(entries) == 0:
		s.mu.Unlock()
		return nil, &snstypes.EmptyBatchRequestException{Message: aws.String("no entries")}
	case len(entries) > 10:
		s.mu.Unlock()
		return nil, &snstypes.TooManyEntriesInBatchRequestException{Message: aws.String("more than ten entries")}
	}
	seen := make(map[string]struct{}, len(entries))
	for i := range entries {
		id := aws.ToString(entries[i].Id)
		if id == "" {
			s.mu.Unlock()
			return nil, &snstypes.InvalidBatchEntryIdException{Message: aws.String("empty entry id")}
		}
		if _, dup := seen[id]; dup {
			s.mu.Unlock()
			return nil, &snstypes.BatchEntryIdsNotDistinctException{Message: aws.String(id)}
		}
		seen[id] = struct{}{}
	}
	out := &sns.PublishBatchOutput{}
	var pending []sqsInput
	for i := range entries {
		e := &entries[i]
		id := aws.ToString(e.Id)
		if code, bad := failedEntry(fault, id, i); bad {
			out.Failed = append(out.Failed, snstypes.BatchResultErrorEntry{
				Id:          aws.String(id),
				Code:        aws.String(code),
				Message:     aws.String("injected by snsfake"),
				SenderFault: fault.SenderFault,
			})
			continue
		}
		body := aws.ToString(e.Message)
		if body == "" {
			out.Failed = append(out.Failed, snstypes.BatchResultErrorEntry{
				Id:          aws.String(id),
				Code:        aws.String("InvalidParameter"),
				Message:     aws.String("Message is empty"),
				SenderFault: true,
			})
			continue
		}
		s.seq++
		mid := "sns-" + strconv.FormatUint(s.seq, 10)
		pending = append(pending, s.deliver(arn, body, e.MessageAttributes, mid)...)
		out.Successful = append(out.Successful, snstypes.PublishBatchResultEntry{
			Id: aws.String(id), MessageId: aws.String(mid),
		})
	}
	s.mu.Unlock()
	s.flush(ctx, pending)
	return out, nil
}

// failedEntry reports whether an installed fault fails this entry, and with
// which code.
func failedEntry(f *Fault, id string, index int) (string, bool) {
	if f == nil {
		return "", false
	}
	if code, ok := f.Entries[id]; ok {
		return orInternal(code), true
	}
	if code, ok := f.EntryIndexes[index]; ok {
		return orInternal(code), true
	}
	return "", false
}

// orInternal defaults an empty code.
func orInternal(code string) string {
	if code == "" {
		return "InternalError"
	}
	return code
}

// flush performs the deliveries this publication produced, with this server's
// lock released.
//
// A delivery that the SQS server refuses is COUNTED rather than reported: the
// real service's Publish has already answered by the time it delivers, and a
// fake that failed the publication for a delivery failure would model an
// ordering the service does not have.
func (s *Server) flush(ctx context.Context, pending []sqsInput) {
	for i := range pending {
		in := &pending[i]
		_, err := s.opts.Queues.SendMessage(ctx, &awssqs.SendMessageInput{
			QueueUrl:          aws.String(in.QueueURL),
			MessageBody:       aws.String(in.Body),
			MessageAttributes: in.Attrs,
		})
		if err != nil {
			s.mu.Lock()
			s.undeliverable++
			s.mu.Unlock()
		}
	}
}

// compile-time proof that the fake really is the API slice the driver is
// written against. Without it a method whose signature drifted would be found
// by whichever test happened to call it, rather than by the build.
var _ snsapi.SNS = (*Server)(nil)
