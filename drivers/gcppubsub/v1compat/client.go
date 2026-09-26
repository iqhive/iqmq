package v1compat

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	//nolint:staticcheck // the deprecated v1 client library IS this package: the specification measures it against pubsub/v2, and the deprecation is its own "while buildable" clause
	"cloud.google.com/go/pubsub"
	"google.golang.org/api/iterator"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/iqhive/iqmq/drivers/gcppubsub/internal/psapi"
)

// Module and Version name the client library this package is built over, and
// Generation is the slug for this arm. They are reported through
// DriverInfo, so a report made through this package names the library that
// answered.
const (
	Module  = "cloud.google.com/go/pubsub"
	Version = "v1.50.2"
	// Generation is the slug for this arm.
	Generation = "v1compat"
)

// Client is the v1 implementation of [psapi.Client].
//
// The topics and subscriptions it hands out are the library's own handles,
// which the v1 API makes both a resource reference AND a publisher: a *Topic
// carries the publish bundler, so one has to be kept per topic rather than
// built per publication, exactly as v2's *Publisher does.
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

// Generation is the arm slug, and DriverInfo.Profile.
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
// The v1 API's *Topic is the publisher, and its EnableMessageOrdering and
// PublishSettings must both be set before the first publication — which is
// the same construction-time constraint v2 has and the reason
// FeatureOrderedByKey is conditional on Config.Ordering in both arms.
func (c *Client) Publisher(cfg psapi.PublisherConfig) (psapi.Publisher, error) {
	if cfg.Topic == "" {
		return nil, errNoTopic
	}
	t := c.cl.Topic(lastSegment(cfg.Topic))
	t.EnableMessageOrdering = cfg.Ordering
	if cfg.CountThreshold > 0 {
		t.PublishSettings.CountThreshold = cfg.CountThreshold
	}
	if cfg.ByteThreshold > 0 {
		t.PublishSettings.ByteThreshold = cfg.ByteThreshold
	}
	if cfg.DelayThreshold > 0 {
		t.PublishSettings.DelayThreshold = cfg.DelayThreshold
	}
	if cfg.Timeout > 0 {
		t.PublishSettings.Timeout = cfg.Timeout
	}
	if cfg.NumGoroutines > 0 {
		t.PublishSettings.NumGoroutines = cfg.NumGoroutines
	}
	if cfg.FlowMaxOutstandingMessages > 0 || cfg.FlowMaxOutstandingBytes > 0 {
		t.PublishSettings.FlowControlSettings = pubsub.FlowControlSettings{
			MaxOutstandingMessages: cfg.FlowMaxOutstandingMessages,
			MaxOutstandingBytes:    cfg.FlowMaxOutstandingBytes,
			LimitExceededBehavior:  pubsub.FlowControlBlock,
		}
	}
	t.PublishSettings.EnableCompression = cfg.Compression
	if cfg.CompressionThreshold > 0 {
		t.PublishSettings.CompressionBytesThreshold = cfg.CompressionThreshold
	}
	return &publisher{t: t}, nil
}

// Subscription builds a subscription handle.
func (c *Client) Subscription(cfg psapi.SubscriptionConfig) (psapi.Subscription, error) {
	if cfg.Subscription == "" {
		return nil, errNoSubscription
	}
	s := c.cl.Subscription(lastSegment(cfg.Subscription))
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
	// The v1 API spells these MaxExtensionPeriod and MinExtensionPeriod; v2
	// renamed them to MaxDurationPerAckExtension and
	// MinDurationPerAckExtension. Same setting, same units, different name —
	// which is the kind of difference this arm exists to absorb.
	if cfg.MaxDurationPerAckExtension != 0 {
		s.ReceiveSettings.MaxExtensionPeriod = cfg.MaxDurationPerAckExtension
	}
	if cfg.MinDurationPerAckExtension != 0 {
		s.ReceiveSettings.MinExtensionPeriod = cfg.MinDurationPerAckExtension
	}
	return &subscription{s: s}, nil
}

// Admin is the administration surface.
func (c *Client) Admin() psapi.Admin { return &admin{cl: c.cl} }

// publisher is one topic's publication handle.
type publisher struct{ t *pubsub.Topic }

// Publish hands the message to the library's bundler.
func (p *publisher) Publish(ctx context.Context, m *psapi.Outgoing) psapi.Result {
	return &result{r: p.t.Publish(ctx, &pubsub.Message{
		Data:        m.Data,
		Attributes:  m.Attributes,
		OrderingKey: m.OrderingKey,
	})}
}

// ResumePublish re-enables an ordering key the library has paused.
func (p *publisher) ResumePublish(key string) { p.t.ResumePublish(key) }

// Flush waits for every buffered publication.
func (p *publisher) Flush() { p.t.Flush() }

// Stop flushes and releases the publisher's goroutines.
func (p *publisher) Stop() { p.t.Stop() }

// result is one pending publication.
type result struct{ r *pubsub.PublishResult }

// Get waits for the service's answer, translating the library's paused-key
// error into [psapi.ErrOrderingPaused].
func (r *result) Get(ctx context.Context) (string, error) {
	id, err := r.r.Get(ctx)
	if err == nil {
		return id, nil
	}
	var paused pubsub.ErrPublishingPaused
	if errors.As(err, &paused) {
		return id, fmt.Errorf("%w: %w", psapi.ErrOrderingPaused, err)
	}
	return id, err
}

// subscription is one subscription's consumption handle.
type subscription struct{ s *pubsub.Subscription }

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
//
// The v1 API puts administration on the resource handles rather than on a
// generated client, so every method here is a Topic or a Subscription method
// where the v2 arm calls TopicAdminClient or SubscriptionAdminClient. What the
// driver sees is identical.
type admin struct{ cl *pubsub.Client }

// CreateTopic creates one topic.
func (a *admin) CreateTopic(ctx context.Context, topic string) error {
	_, err := a.cl.CreateTopic(ctx, lastSegment(topic))
	return translate(err)
}

// DeleteTopic removes one topic.
func (a *admin) DeleteTopic(ctx context.Context, topic string) error {
	return translate(a.cl.Topic(lastSegment(topic)).Delete(ctx))
}

// TopicExists reports whether the topic is there.
func (a *admin) TopicExists(ctx context.Context, topic string) (bool, error) {
	ok, err := a.cl.Topic(lastSegment(topic)).Exists(ctx)
	return ok, translate(err)
}

// ListTopics iterates the project's topics.
func (a *admin) ListTopics(ctx context.Context, fn func(topic string) bool) error {
	it := a.cl.Topics(ctx)
	for {
		t, err := it.Next()
		if errors.Is(err, iterator.Done) {
			return nil
		}
		if err != nil {
			return translate(err)
		}
		if !fn(t.ID()) {
			return nil
		}
	}
}

// CreateSubscription creates one subscription.
func (a *admin) CreateSubscription(ctx context.Context, spec psapi.SubscriptionSpec) error {
	cfg := pubsub.SubscriptionConfig{
		Topic:                     a.cl.Topic(lastSegment(spec.Topic)),
		Filter:                    spec.Filter,
		EnableExactlyOnceDelivery: spec.ExactlyOnce,
		EnableMessageOrdering:     spec.Ordering,
		RetainAckedMessages:       spec.RetainAcked,
	}
	if spec.AckDeadline > 0 {
		// The service takes whole seconds, so the deadline is rounded UP to
		// what it will actually grant: rounding down is what makes a
		// sub-second deadline into no lease at all.
		cfg.AckDeadline = time.Duration(wholeSeconds(spec.AckDeadline)) * time.Second
	}
	if spec.Retention > 0 {
		cfg.RetentionDuration = spec.Retention
	}
	if spec.DeadLetterTopic != "" {
		cfg.DeadLetterPolicy = &pubsub.DeadLetterPolicy{
			DeadLetterTopic:     qualifyTopic(a.cl.Project(), spec.DeadLetterTopic),
			MaxDeliveryAttempts: spec.MaxDeliveryAttempts,
		}
	}
	_, err := a.cl.CreateSubscription(ctx, lastSegment(spec.Subscription), cfg)
	return translate(err)
}

// DeleteSubscription removes one subscription.
func (a *admin) DeleteSubscription(ctx context.Context, sub string) error {
	return translate(a.cl.Subscription(lastSegment(sub)).Delete(ctx))
}

// GetSubscription reads a subscription's configuration back.
func (a *admin) GetSubscription(ctx context.Context, sub string) (psapi.SubscriptionInfo, error) {
	cfg, err := a.cl.Subscription(lastSegment(sub)).Config(ctx)
	if err != nil {
		mapped := translate(err)
		if errors.Is(mapped, psapi.ErrNotFound) {
			return psapi.SubscriptionInfo{}, nil
		}
		return psapi.SubscriptionInfo{}, mapped
	}
	info := psapi.SubscriptionInfo{
		Exists:      true,
		AckDeadline: cfg.AckDeadline,
		Filter:      cfg.Filter,
		ExactlyOnce: cfg.EnableExactlyOnceDelivery,
		Ordering:    cfg.EnableMessageOrdering,
		RetainAcked: cfg.RetainAckedMessages,
		Retention:   cfg.RetentionDuration,
	}
	if cfg.Topic != nil {
		info.Topic = cfg.Topic.ID()
	}
	if p := cfg.DeadLetterPolicy; p != nil {
		info.DeadLetterTopic = lastSegment(p.DeadLetterTopic)
		info.MaxDeliveryAttempts = p.MaxDeliveryAttempts
	}
	return info, nil
}

// Seek moves a subscription's acknowledgement state to a timestamp.
func (a *admin) Seek(ctx context.Context, sub string, to time.Time) error {
	return translate(a.cl.Subscription(lastSegment(sub)).SeekToTime(ctx, to))
}

// Purge discards what a subscription has not acknowledged, which on this
// broker is a seek to now.
func (a *admin) Purge(ctx context.Context, sub string) error {
	return a.Seek(ctx, sub, time.Now())
}

// translate turns a gRPC status into this driver's sentinels while preserving
// the native error for errors.As.
func translate(err error) error {
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

// lastSegment is the identifier at the end of a qualified resource name. The
// v1 API takes bare identifiers where v2 takes qualified names, so this is
// where the two meet.
func lastSegment(name string) string {
	if i := strings.LastIndexByte(name, '/'); i >= 0 {
		return name[i+1:]
	}
	return name
}

// qualifyTopic builds a fully qualified topic name, which the dead-letter
// policy needs even in the v1 API.
func qualifyTopic(project, topic string) string {
	if strings.HasPrefix(topic, "projects/") {
		return topic
	}
	return "projects/" + project + "/topics/" + lastSegment(topic)
}

// wholeSeconds rounds a duration UP to whole seconds.
func wholeSeconds(d time.Duration) int64 {
	if d <= 0 {
		return 0
	}
	return int64((d + time.Second - 1) / time.Second)
}

// The implementation's own refusals.
var (
	errNoTopic         = errors.New("v1compat: a publisher needs a topic")
	errNoSubscription  = errors.New("v1compat: a subscription handle needs a subscription")
	errInvalidAckID    = errors.New("v1compat: the service rejected the acknowledgement identifier")
	errAckPermission   = errors.New("v1compat: the service refused the settlement: permission denied")
	errAckPrecondition = errors.New("v1compat: the service refused the settlement: failed precondition")
	errAckOther        = errors.New("v1compat: the service refused the settlement")
)
