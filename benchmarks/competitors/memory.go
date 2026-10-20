package competitors

import (
	"context"
	"errors"
	"testing"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/pubsub/gochannel"
	"gocloud.dev/pubsub"
	"gocloud.dev/pubsub/mempubsub"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/memory"

	"github.com/iqhive/iqmq/benchmarks/opbench"
)

// The in-process family. Every arm holds one publisher and one consumer over
// one in-process broker, acknowledges every delivery, and is bounded where its
// library lets it be bounded.
//
// The buffer every bounded arm is given is opbench's own outstanding bound, so
// that no arm's overflow policy is reached inside a measurement and the
// difference between them (iqmq and Watermill block; gocloud's queue is
// unbounded) cannot show up as a timing difference. It is stated in the
// family's Proof rather than hidden.
const memoryBuffer = 8192

// errNoProgress is what a Receive that returned nothing and reported nothing
// becomes: a contract violation, reported rather than spun on.
var errNoProgress = errors.New("competitors: a receive returned no delivery and no error")

// ---------------------------------------------------------------- iqmq/memory

type iqmqSession struct {
	client *mq.Client
	pub    *mq.PublisherHandle
	con    *mq.ConsumerHandle
	one    *mq.Message
	batch  []*mq.Message
	dst    []mq.Delivery
	settle bool
}

func openIqmqMemory(tb testing.TB, _ string, p opbench.Params) opbench.Session {
	tb.Helper()
	ctx := context.Background()
	client, err := memory.Open(ctx, memory.Config{})
	if err != nil {
		tb.Fatalf("competitors: memory.Open: %v", err)
	}
	// The broker is this call's own, so the name needs no uniquifier.
	topic := "compare." + p.Op
	adm, err := client.Admin()
	if err != nil {
		_ = client.Close()
		tb.Fatalf("competitors: Admin: %v", err)
	}
	if err := adm.EnsureTopic(ctx, mq.TopicSpec{Name: topic}); err != nil {
		_ = client.Close()
		tb.Fatalf("competitors: EnsureTopic: %v", err)
	}
	if err := adm.EnsureSubscription(ctx, mq.SubscriptionSpec{Topic: topic, Name: "compare"}); err != nil {
		_ = client.Close()
		tb.Fatalf("competitors: EnsureSubscription: %v", err)
	}
	return openIqmq(tb, client, topic, mq.ConfirmDefault)
}

// openIqmq builds the iqmq session over an already-provisioned topic.
func openIqmq(tb testing.TB, client *mq.Client, topic string, confirm mq.ConfirmationMode) opbench.Session {
	tb.Helper()
	ctx := context.Background()
	pub, err := client.Publisher(ctx, mq.PublisherConfig{Topic: topic, Confirmation: confirm})
	if err != nil {
		_ = client.Close()
		tb.Fatalf("competitors: Publisher: %v", err)
	}
	con, err := client.Consumer(ctx, mq.ConsumerConfig{Topic: topic, Subscription: "compare"})
	if err != nil {
		_ = pub.Close()
		_ = client.Close()
		tb.Fatalf("competitors: Consumer: %v", err)
	}
	return &iqmqSession{
		client: client,
		pub:    pub,
		con:    con,
		one:    &mq.Message{},
		dst:    make([]mq.Delivery, 64),
		settle: client.Capabilities().Has(mq.FeatureExplicitAck),
	}
}

func (s *iqmqSession) Publish(ctx context.Context, payload []byte) error {
	s.one.Body = payload
	return s.pub.Publish(ctx, s.one)
}

func (s *iqmqSession) PublishBatch(ctx context.Context, payloads [][]byte) error {
	for len(s.batch) < len(payloads) {
		s.batch = append(s.batch, &mq.Message{})
	}
	ms := s.batch[:len(payloads)]
	for i, p := range payloads {
		ms[i].Body = p
	}
	return s.pub.PublishBatch(ctx, ms)
}

func (s *iqmqSession) Receive(ctx context.Context, n int) error {
	for n > 0 {
		dst := s.dst
		if n < len(dst) {
			dst = dst[:n]
		}
		got, rerr := s.con.Receive(ctx, dst)
		if s.settle {
			for i := range got {
				if err := dst[i].Ack(); err != nil {
					return err
				}
			}
		}
		if rerr != nil {
			return rerr
		}
		if got == 0 {
			return errNoProgress
		}
		n -= got
	}
	return nil
}

func (s *iqmqSession) Close() error {
	err := s.con.Close()
	if perr := s.pub.Close(); err == nil {
		err = perr
	}
	if cerr := s.client.Close(); err == nil {
		err = cerr
	}
	return err
}

// ----------------------------------------------------------- watermill/memory

func openWatermillMemory(tb testing.TB, _ string, p opbench.Params) opbench.Session {
	tb.Helper()
	ch := gochannel.NewGoChannel(gochannel.Config{
		// Bounded, like iqmq's lane; large enough that opbench's drain
		// happens before the bound is reached, so the arms' overflow
		// policies cannot show up in the timings.
		OutputChannelBuffer: memoryBuffer,
		// Off: with it on, Publish waits for the subscriber's Ack, which is
		// a round trip no other arm in this family performs.
		BlockPublishUntilSubscriberAck: false,
	}, watermill.NopLogger{})
	// The channel is this call's own, so the name needs no uniquifier.
	topic := "compare-" + p.Op
	sub, err := ch.Subscribe(context.Background(), topic)
	if err != nil {
		tb.Fatalf("competitors: gochannel Subscribe: %v", err)
	}
	return &watermillSession{ch: ch, topic: topic, sub: sub}
}

type watermillSession struct {
	ch    *gochannel.GoChannel
	topic string
	sub   <-chan *message.Message
	one   *message.Message
	batch []*message.Message
}

func (s *watermillSession) Publish(_ context.Context, payload []byte) error {
	if s.one == nil {
		s.one = message.NewMessage(watermill.NewUUID(), nil)
	}
	// A Watermill message carries a UUID, and a real publisher mints a fresh
	// one per message; so does this, because that cost is part of what a
	// Watermill user pays.
	s.one.UUID = watermill.NewUUID()
	s.one.Payload = payload
	return s.ch.Publish(s.topic, s.one)
}

func (s *watermillSession) PublishBatch(_ context.Context, payloads [][]byte) error {
	for len(s.batch) < len(payloads) {
		s.batch = append(s.batch, message.NewMessage("", nil))
	}
	ms := s.batch[:len(payloads)]
	for i, p := range payloads {
		ms[i].UUID = watermill.NewUUID()
		ms[i].Payload = p
	}
	// Publish is variadic: this IS Watermill's client-level batch API.
	return s.ch.Publish(s.topic, ms...)
}

func (s *watermillSession) Receive(ctx context.Context, n int) error {
	for range n {
		select {
		case m, ok := <-s.sub:
			if !ok {
				return errNoProgress
			}
			m.Ack()
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

func (s *watermillSession) Close() error { return s.ch.Close() }

// ------------------------------------------------------------- gocloud/memory

type gocloudSession struct {
	topic *pubsub.Topic
	sub   *pubsub.Subscription
	msg   *pubsub.Message
	// closeTopic and closeSub are the shutdown calls; gocloud's Shutdown
	// takes a context, which the session does not otherwise carry.
	shutdown func(context.Context) error
}

func openGocloudMemory(tb testing.TB, _ string, _ opbench.Params) opbench.Session {
	tb.Helper()
	// mempubsub requires the subscription to exist before a publication, like
	// every other arm in this family.
	topic := mempubsub.NewTopic()
	sub := mempubsub.NewSubscription(topic, ackDeadline)
	return &gocloudSession{
		topic: topic,
		sub:   sub,
		msg:   &pubsub.Message{},
		shutdown: func(ctx context.Context) error {
			serr := sub.Shutdown(ctx)
			terr := topic.Shutdown(ctx)
			if serr != nil {
				return serr
			}
			return terr
		},
	}
}

func (s *gocloudSession) Publish(ctx context.Context, payload []byte) error {
	s.msg.Body = payload
	return s.topic.Send(ctx, s.msg)
}

// PublishBatch is a LOOP of Send, because gocloud.dev/pubsub has no batch API.
// It is what a gocloud user writes and it is not a client-level batch, so the
// row it produces is measured and excluded from the comparison (Exclusions()).
func (s *gocloudSession) PublishBatch(ctx context.Context, payloads [][]byte) error {
	for _, p := range payloads {
		s.msg.Body = p
		if err := s.topic.Send(ctx, s.msg); err != nil {
			return err
		}
	}
	return nil
}

func (s *gocloudSession) Receive(ctx context.Context, n int) error {
	for range n {
		m, err := s.sub.Receive(ctx)
		if err != nil {
			return err
		}
		m.Ack()
	}
	return nil
}

func (s *gocloudSession) Close() error {
	return s.shutdown(context.Background())
}
