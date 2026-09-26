// Package psv2 implements [psapi.Client] over cloud.google.com/go/pubsub/v2,
// the current Pub/Sub client generation and the reference arm.
package psv2

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"cloud.google.com/go/pubsub/v2"
	pubsubpb "cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"google.golang.org/api/iterator"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/iqhive/iqmq/drivers/gcppubsub/internal/psapi"
)

// Module and Version name the client library this package is built over. They
// are reported through DriverInfo and are what every service limit the driver
// advertises was read out of, so they move together.
const (
	Module  = "cloud.google.com/go/pubsub/v2"
	Version = "v2.6.0"
	// Generation is the slug for this arm.
	Generation = "v2"
)

// Client is the v2 implementation of [psapi.Client].
type Client struct {
	cl      *pubsub.Client
	project string
}

var _ psapi.Client = (*Client)(nil)

// Wrap adapts a *pubsub.Client the caller already holds.
func Wrap(cl *pubsub.Client) *Client {
	return &Client{cl: cl, project: cl.Project()}
}

// Project is the client's Google Cloud project.
func (c *Client) Project() string { return c.project }

// Generation is the arm slug.
func (c *Client) Generation() string { return Generation }

// ClientModule is the native client's module path.
func (c *Client) ClientModule() string { return Module }

// ClientVersion is the pinned version of that client.
func (c *Client) ClientVersion() string { return Version }

// Native is the library's own client.
func (c *Client) Native() any { return c.cl }

// Close releases the client.
func (c *Client) Close() error { return c.cl.Close() }

// Publisher builds a publisher for one topic.
//
// Nothing here is I/O: the library's publisher is a handle whose first RPC is
// its first publication.
func (c *Client) Publisher(cfg psapi.PublisherConfig) (psapi.Publisher, error) {
	if cfg.Topic == "" {
		return nil, errNoTopic
	}
	p := c.cl.Publisher(qualifyTopic(c.project, cfg.Topic))
	p.EnableMessageOrdering = cfg.Ordering
	if cfg.CountThreshold > 0 {
		p.PublishSettings.CountThreshold = cfg.CountThreshold
	}
	if cfg.ByteThreshold > 0 {
		p.PublishSettings.ByteThreshold = cfg.ByteThreshold
	}
	if cfg.DelayThreshold > 0 {
		p.PublishSettings.DelayThreshold = cfg.DelayThreshold
	}
	if cfg.Timeout > 0 {
		p.PublishSettings.Timeout = cfg.Timeout
	}
	if cfg.NumGoroutines > 0 {
		p.PublishSettings.NumGoroutines = cfg.NumGoroutines
	}
	if cfg.FlowMaxOutstandingMessages > 0 || cfg.FlowMaxOutstandingBytes > 0 {
		p.PublishSettings.FlowControlSettings = pubsub.FlowControlSettings{
			MaxOutstandingMessages: cfg.FlowMaxOutstandingMessages,
			MaxOutstandingBytes:    cfg.FlowMaxOutstandingBytes,
			// Block rather than SignalError: the specification asks an operation to be
			// bounded and a publication that waits for room is bounded by the
			// caller's context, while one that fails on a full buffer turns a
			// publisher's own batching into an error a caller cannot act on.
			LimitExceededBehavior: pubsub.FlowControlBlock,
		}
	}
	p.PublishSettings.EnableCompression = cfg.Compression
	if cfg.CompressionThreshold > 0 {
		p.PublishSettings.CompressionBytesThreshold = cfg.CompressionThreshold
	}
	return &publisher{p: p}, nil
}

// Subscription builds a subscription handle.
func (c *Client) Subscription(cfg psapi.SubscriptionConfig) (psapi.Subscription, error) {
	if cfg.Subscription == "" {
		return nil, errNoSubscription
	}
	s := c.cl.Subscriber(qualifySub(c.project, cfg.Subscription))
	if cfg.MaxOutstandingMessages != 0 {
		s.ReceiveSettings.MaxOutstandingMessages = cfg.MaxOutstandingMessages
	}
	if cfg.MaxOutstandingBytes != 0 {
		s.ReceiveSettings.MaxOutstandingBytes = cfg.MaxOutstandingBytes
	}
	if cfg.NumGoroutines > 0 {
		s.ReceiveSettings.NumGoroutines = cfg.NumGoroutines
	}
	if cfg.MaxExtension != 0 {
		s.ReceiveSettings.MaxExtension = cfg.MaxExtension
	}
	if cfg.MaxDurationPerAckExtension != 0 {
		s.ReceiveSettings.MaxDurationPerAckExtension = cfg.MaxDurationPerAckExtension
	}
	if cfg.MinDurationPerAckExtension != 0 {
		s.ReceiveSettings.MinDurationPerAckExtension = cfg.MinDurationPerAckExtension
	}
	return &subscription{s: s}, nil
}

// Admin is the administration surface.
func (c *Client) Admin() psapi.Admin { return &admin{cl: c.cl, project: c.project} }

// publisher is one topic's publication handle.
type publisher struct{ p *pubsub.Publisher }

// Publish hands the message to the library's bundler.
func (p *publisher) Publish(ctx context.Context, m *psapi.Outgoing) psapi.Result {
	return &result{r: p.p.Publish(ctx, &pubsub.Message{
		Data:        m.Data,
		Attributes:  m.Attributes,
		OrderingKey: m.OrderingKey,
	})}
}

// ResumePublish re-enables an ordering key the library has paused.
func (p *publisher) ResumePublish(key string) { p.p.ResumePublish(key) }

// Flush waits for every buffered publication.
func (p *publisher) Flush() { p.p.Flush() }

// Stop flushes and releases the publisher's goroutines.
func (p *publisher) Stop() { p.p.Stop() }

// result is one pending publication.
type result struct{ r *pubsub.PublishResult }

// Get waits for the service's answer, translating the library's paused-key
// error into [psapi.ErrOrderingPaused].
func (r *result) Get(ctx context.Context) (string, error) {
	id, err := r.r.Get(ctx)
	return id, translatePublish(err)
}

// translatePublish turns the library's own publication failures into this
// package's sentinels, preserving the native error for errors.As.
func translatePublish(err error) error {
	if err == nil {
		return nil
	}
	var paused pubsub.ErrPublishingPaused
	if errors.As(err, &paused) {
		return fmt.Errorf("%w: %w", psapi.ErrOrderingPaused, err)
	}
	return err
}

// subscription is one subscription's consumption handle.
type subscription struct{ s *pubsub.Subscriber }

// Receive delivers by callback until ctx is done.
func (s *subscription) Receive(ctx context.Context, fn func(context.Context, *psapi.Incoming)) error {
	return s.s.Receive(ctx, func(cctx context.Context, m *pubsub.Message) {
		in := psapi.Incoming{
			ID:          m.ID,
			Data:        m.Data,
			Attributes:  m.Attributes,
			PublishTime: m.PublishTime,
			OrderingKey: m.OrderingKey,
			Native:      m,
			Settle:      settlement{m: m},
		}
		if m.DeliveryAttempt != nil {
			in.Attempt, in.HasAttempt = *m.DeliveryAttempt, true
		}
		fn(cctx, &in)
	})
}

// settlement settles one message.
type settlement struct{ m *pubsub.Message }

// Ack acknowledges without waiting for an outcome.
func (s settlement) Ack() { s.m.Ack() }

// Nack declines without waiting for an outcome.
func (s settlement) Nack() { s.m.Nack() }

// AckWithResult acknowledges and waits for the service's answer, which is only
// a real answer on an exactly-once subscription.
func (s settlement) AckWithResult(ctx context.Context) error {
	return ackStatus(s.m.AckWithResult().Get(ctx))
}

// NackWithResult declines and waits for the service's answer.
func (s settlement) NackWithResult(ctx context.Context) error {
	return ackStatus(s.m.NackWithResult().Get(ctx))
}

// ackStatus turns an AcknowledgeStatus into an error the driver can map.
func ackStatus(st pubsub.AcknowledgeStatus, err error) error {
	switch st {
	case pubsub.AcknowledgeStatusSuccess:
		return err
	case pubsub.AcknowledgeStatusInvalidAckID:
		return fmt.Errorf("%w: %w", psapi.ErrAckIDNotReachable, errOr(err, errInvalidAckID))
	case pubsub.AcknowledgeStatusPermissionDenied:
		return errOr(err, errAckPermission)
	case pubsub.AcknowledgeStatusFailedPrecondition:
		return errOr(err, errAckPrecondition)
	case pubsub.AcknowledgeStatusOther:
		return errOr(err, errAckOther)
	default:
		return errOr(err, errAckOther)
	}
}

// errOr returns err when it says something, and fallback otherwise: the
// library reports a status with no error for some outcomes, and a settlement
// that failed must never render as a success.
func errOr(err, fallback error) error {
	if err != nil {
		return err
	}
	return fallback
}

// admin is the topic and subscription administration surface.
type admin struct {
	cl      *pubsub.Client
	project string
}

// CreateTopic creates one topic.
func (a *admin) CreateTopic(ctx context.Context, topic string) error {
	_, err := a.cl.TopicAdminClient.CreateTopic(ctx, &pubsubpb.Topic{Name: qualifyTopic(a.project, topic)})
	return translateStatus(err)
}

// DeleteTopic removes one topic.
func (a *admin) DeleteTopic(ctx context.Context, topic string) error {
	return translateStatus(a.cl.TopicAdminClient.DeleteTopic(ctx,
		&pubsubpb.DeleteTopicRequest{Topic: qualifyTopic(a.project, topic)}))
}

// TopicExists reports whether the topic is there.
func (a *admin) TopicExists(ctx context.Context, topic string) (bool, error) {
	_, err := a.cl.TopicAdminClient.GetTopic(ctx,
		&pubsubpb.GetTopicRequest{Topic: qualifyTopic(a.project, topic)})
	switch {
	case err == nil:
		return true, nil
	case errors.Is(translateStatus(err), psapi.ErrNotFound):
		return false, nil
	default:
		return false, translateStatus(err)
	}
}

// ListTopics iterates the project's topics.
func (a *admin) ListTopics(ctx context.Context, fn func(topic string) bool) error {
	it := a.cl.TopicAdminClient.ListTopics(ctx,
		&pubsubpb.ListTopicsRequest{Project: "projects/" + a.project})
	for {
		t, err := it.Next()
		if errors.Is(err, iterator.Done) {
			return nil
		}
		if err != nil {
			return translateStatus(err)
		}
		if !fn(lastSegment(t.GetName())) {
			return nil
		}
	}
}

// CreateSubscription creates one subscription.
func (a *admin) CreateSubscription(ctx context.Context, spec psapi.SubscriptionSpec) error {
	sub := &pubsubpb.Subscription{
		Name:                      qualifySub(a.project, spec.Subscription),
		Topic:                     qualifyTopic(a.project, spec.Topic),
		Filter:                    spec.Filter,
		EnableExactlyOnceDelivery: spec.ExactlyOnce,
		EnableMessageOrdering:     spec.Ordering,
		RetainAckedMessages:       spec.RetainAcked,
	}
	if spec.AckDeadline > 0 {
		sub.AckDeadlineSeconds = wholeSeconds(spec.AckDeadline)
	}
	if spec.Retention > 0 {
		sub.MessageRetentionDuration = durationpb.New(spec.Retention)
	}
	if spec.DeadLetterTopic != "" {
		sub.DeadLetterPolicy = &pubsubpb.DeadLetterPolicy{
			DeadLetterTopic:     qualifyTopic(a.project, spec.DeadLetterTopic),
			MaxDeliveryAttempts: int32(spec.MaxDeliveryAttempts),
		}
	}
	_, err := a.cl.SubscriptionAdminClient.CreateSubscription(ctx, sub)
	return translateStatus(err)
}

// DeleteSubscription removes one subscription.
func (a *admin) DeleteSubscription(ctx context.Context, sub string) error {
	return translateStatus(a.cl.SubscriptionAdminClient.DeleteSubscription(ctx,
		&pubsubpb.DeleteSubscriptionRequest{Subscription: qualifySub(a.project, sub)}))
}

// GetSubscription reads a subscription's configuration back.
func (a *admin) GetSubscription(ctx context.Context, sub string) (psapi.SubscriptionInfo, error) {
	got, err := a.cl.SubscriptionAdminClient.GetSubscription(ctx,
		&pubsubpb.GetSubscriptionRequest{Subscription: qualifySub(a.project, sub)})
	if err != nil {
		mapped := translateStatus(err)
		if errors.Is(mapped, psapi.ErrNotFound) {
			return psapi.SubscriptionInfo{}, nil
		}
		return psapi.SubscriptionInfo{}, mapped
	}
	info := psapi.SubscriptionInfo{
		Exists:      true,
		Topic:       lastSegment(got.GetTopic()),
		AckDeadline: time.Duration(got.GetAckDeadlineSeconds()) * time.Second,
		Filter:      got.GetFilter(),
		ExactlyOnce: got.GetEnableExactlyOnceDelivery(),
		Ordering:    got.GetEnableMessageOrdering(),
		RetainAcked: got.GetRetainAckedMessages(),
	}
	if d := got.GetMessageRetentionDuration(); d != nil {
		info.Retention = d.AsDuration()
	}
	if p := got.GetDeadLetterPolicy(); p != nil {
		info.DeadLetterTopic = lastSegment(p.GetDeadLetterTopic())
		info.MaxDeliveryAttempts = int(p.GetMaxDeliveryAttempts())
	}
	return info, nil
}

// Seek moves a subscription's acknowledgement state to a timestamp.
func (a *admin) Seek(ctx context.Context, sub string, to time.Time) error {
	_, err := a.cl.SubscriptionAdminClient.Seek(ctx, &pubsubpb.SeekRequest{
		Subscription: qualifySub(a.project, sub),
		Target:       &pubsubpb.SeekRequest_Time{Time: timestamppb.New(to)},
	})
	return translateStatus(err)
}

// Purge discards what a subscription has not acknowledged, which on this
// broker is a seek to now.
func (a *admin) Purge(ctx context.Context, sub string) error {
	return a.Seek(ctx, sub, time.Now())
}

// translateStatus turns a gRPC status into this package's sentinels while
// preserving the native error for errors.As.
func translateStatus(err error) error {
	if err == nil {
		return nil
	}
	switch status.Code(err) {
	case codes.NotFound:
		return fmt.Errorf("%w: %w", psapi.ErrNotFound, err)
	case codes.AlreadyExists:
		return fmt.Errorf("%w: %w", psapi.ErrAlreadyExists, err)
	default:
		return err
	}
}

// qualifyTopic builds a fully qualified topic name, passing one through.
func qualifyTopic(project, topic string) string {
	if strings.HasPrefix(topic, "projects/") {
		return topic
	}
	return "projects/" + project + "/topics/" + topic
}

// qualifySub builds a fully qualified subscription name, passing one through.
func qualifySub(project, sub string) string {
	if strings.HasPrefix(sub, "projects/") {
		return sub
	}
	return "projects/" + project + "/subscriptions/" + sub
}

// lastSegment is the identifier at the end of a qualified resource name.
func lastSegment(name string) string {
	if i := strings.LastIndexByte(name, '/'); i >= 0 {
		return name[i+1:]
	}
	return name
}

// wholeSeconds rounds a duration UP to whole seconds, which is the unit the
// service takes. Rounding up is the safe direction for a lease: a sub-second
// deadline truncated to zero is a subscription whose messages are immediately
// redeliverable, which is no lease at all.
func wholeSeconds(d time.Duration) int32 {
	if d <= 0 {
		return 0
	}
	return int32((d + time.Second - 1) / time.Second)
}

// The implementation's own refusals.
var (
	errNoTopic         = errors.New("psv2: a publisher needs a topic")
	errNoSubscription  = errors.New("psv2: a subscription handle needs a subscription")
	errInvalidAckID    = errors.New("psv2: the service rejected the acknowledgement identifier")
	errAckPermission   = errors.New("psv2: the service refused the settlement: permission denied")
	errAckPrecondition = errors.New("psv2: the service refused the settlement: failed precondition")
	errAckOther        = errors.New("psv2: the service refused the settlement")
)
