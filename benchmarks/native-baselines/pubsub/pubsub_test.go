// Package pubsub measures cloud.google.com/go/pubsub/v2 against the Google
// Cloud Pub/Sub emulator container.
//
// Emulator numbers are not evidence for managed Pub/Sub (layer 4: real
// services are measured separately and emulators never stand in for them).
// The baseline exists to compare the iqmq driver against the raw client on
// the same emulator, holding everything else equal.
//
// Confirmation boundary: PublishResult.Get, i.e. the server-assigned message
// ID. Publisher batching is left at the client defaults for the batching
// leaves and disabled (CountThreshold 1, no delay) for the per-message
// leaves, because the default DelayThreshold would otherwise be measured
// instead of the server.
// Settlement: Message.Ack. AckWithResult is the exactly-once path and is not
// used here: the emulator does not support exactly-once delivery, and a
// result-returning ack is a different, slower boundary.
package pubsub

import (
	"context"
	"errors"
	"testing"
	"time"

	"cloud.google.com/go/pubsub/v2"
	pubsubpb "cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/iqhive/iqmq/conformance/harness"

	"github.com/iqhive/iqmq/benchmarks/native-baselines/nativebench"
)

func BenchmarkNative(b *testing.B) {
	nativebench.Run(b, "pubsub", opener(harness.PubSub(b)))
}

// TestNativeSession is the baseline's correctness gate: it round-trips
// single messages and full batches at every payload size before any
// measurement is taken.
func TestNativeSession(t *testing.T) {
	nativebench.SkipUnlessEnabled(t)
	nativebench.CheckSession(t, opener(harness.PubSub(t)))
}

// opener binds the running emulator to the session constructor.
func opener(broker *harness.Broker) nativebench.Open {
	project := broker.Endpoint("project")
	if project == "" {
		project = harness.PubSubProject
	}
	return func(tb testing.TB, p nativebench.Params) nativebench.Session {
		return open(tb, broker.Addr, project, p)
	}
}

type session struct {
	client     *pubsub.Client
	publisher  *pubsub.Publisher
	subscriber *pubsub.Subscriber
	topic      string
	sub        string
	msgs       chan *pubsub.Message
	cancel     context.CancelFunc
	done       chan error
}

func open(tb testing.TB, addr, project string, p nativebench.Params) *session {
	tb.Helper()
	ctx := nativebench.Context(tb)
	client, err := pubsub.NewClient(ctx, project,
		option.WithEndpoint(addr),
		option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())),
		option.WithoutAuthentication(),
		option.WithTelemetryDisabled(),
	)
	if err != nil {
		tb.Fatalf("pubsub: client for %s: %v", addr, err)
	}
	name := nativebench.Name("iqmqbench")
	topic := "projects/" + project + "/topics/" + name
	sub := "projects/" + project + "/subscriptions/" + name
	if _, err := client.TopicAdminClient.CreateTopic(ctx, &pubsubpb.Topic{Name: topic}); err != nil {
		_ = client.Close()
		tb.Fatalf("pubsub: create topic %s: %v", topic, err)
	}
	if _, err := client.SubscriptionAdminClient.CreateSubscription(ctx, &pubsubpb.Subscription{
		Name:               sub,
		Topic:              topic,
		AckDeadlineSeconds: 600,
	}); err != nil {
		_ = client.Close()
		tb.Fatalf("pubsub: create subscription %s: %v", sub, err)
	}
	publisher := client.Publisher(topic)
	if !p.Batched() {
		publisher.PublishSettings.CountThreshold = 1
		publisher.PublishSettings.DelayThreshold = time.Millisecond
	}
	subscriber := client.Subscriber(sub)
	subscriber.ReceiveSettings.MaxOutstandingMessages = 4096
	subscriber.ReceiveSettings.NumGoroutines = 1

	recvCtx, cancel := context.WithCancel(context.Background())
	s := &session{
		client: client, publisher: publisher, subscriber: subscriber,
		topic: topic, sub: sub,
		msgs: make(chan *pubsub.Message, 4096), cancel: cancel, done: make(chan error, 1),
	}
	go func() {
		s.done <- s.subscriber.Receive(recvCtx, func(ctx context.Context, m *pubsub.Message) {
			// Ack in the callback, as the library requires, and hand the
			// message to Receive so the benchmark counts settled messages.
			m.Ack()
			select {
			case s.msgs <- m:
			case <-ctx.Done():
			}
		})
	}()
	return s
}

func (s *session) Publish(ctx context.Context, payload []byte) error {
	_, err := s.publisher.Publish(ctx, &pubsub.Message{Data: payload}).Get(ctx)
	return err
}

func (s *session) PublishBatch(ctx context.Context, payloads [][]byte) error {
	results := make([]*pubsub.PublishResult, len(payloads))
	for i, p := range payloads {
		results[i] = s.publisher.Publish(ctx, &pubsub.Message{Data: p})
	}
	for _, r := range results {
		if _, err := r.Get(ctx); err != nil {
			return err
		}
	}
	return nil
}

func (s *session) Receive(ctx context.Context, n int) error {
	for range n {
		select {
		case <-s.msgs:
		case err := <-s.done:
			return errors.Join(errors.New("pubsub: receiver stopped"), err)
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

func (s *session) Close() error {
	s.publisher.Stop()
	s.cancel()
	err := <-s.done
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	serr := s.client.SubscriptionAdminClient.DeleteSubscription(ctx, &pubsubpb.DeleteSubscriptionRequest{Subscription: s.sub})
	terr := s.client.TopicAdminClient.DeleteTopic(ctx, &pubsubpb.DeleteTopicRequest{Topic: s.topic})
	return errors.Join(err, serr, terr, s.client.Close())
}
