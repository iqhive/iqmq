package competitors

import (
	"context"
	"testing"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	wmnats "github.com/ThreeDotsLabs/watermill-nats/v2/pkg/nats"
	"github.com/ThreeDotsLabs/watermill/message"
	natsgo "github.com/nats-io/nats.go"
	"gocloud.dev/pubsub"
	"gocloud.dev/pubsub/natspubsub"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit"
	iqnats "github.com/iqhive/iqmq/drivers/nats"

	"github.com/iqhive/iqmq/benchmarks/opbench"
)

// ackDeadline is mempubsub's redelivery window. It is long enough that no
// message in a benchmark is ever redelivered, which would otherwise make the
// gocloud/memory arm measure a different number of messages from its
// neighbours.
const ackDeadline = 10 * time.Minute

// natsTimeout bounds a synchronous receive, so an arm whose subscriber lost a
// message reports it instead of blocking until the test's own timeout. NATS
// Core is at-most-once and a slow consumer really can drop, so this is a
// condition the harness has to be able to state rather than hang on.
const natsTimeout = 30 * time.Second

// dial builds the ONE connection every arm of the nats family is handed.
//
// This is the equivalence the family's Proof claims: Watermill
// (NewPublisherWithNatsConn), gocloud (OpenTopicV2) and iqmq
// (nats.OpenWithClient) all accept an existing *nats.Conn, so the transport,
// the dial options and the write buffer are one object rather than four
// libraries' defaults.
func dial(tb testing.TB, addr string) *natsgo.Conn {
	tb.Helper()
	nc, err := natsgo.Connect("nats://"+addr,
		natsgo.Name("iqmq-compare"),
		natsgo.MaxReconnects(-1),
	)
	if err != nil {
		tb.Fatalf("competitors: dialling nats at %s: %v", addr, err)
	}
	return nc
}

// ------------------------------------------------------------------ iqmq/nats

func openIqmqNats(tb testing.TB, addr string, p opbench.Params) opbench.Session {
	tb.Helper()
	nc := dial(tb, addr)
	topic := topicName()
	client, err := iqnats.OpenWithClient(context.Background(), nc, driverkit.ClientBorrowed, iqnats.Config{
		Addrs: []string{"nats://" + addr},
	})
	if err != nil {
		nc.Close()
		tb.Fatalf("competitors: nats.OpenWithClient: %v", err)
	}
	_ = p
	// ConfirmNone, not the ConfirmDefault this driver resolves to
	// ConfirmAccepted: ConfirmAccepted costs a PING/PONG round trip per
	// publication and neither gocloud's natspubsub nor watermill-nats can
	// offer that boundary at all, so comparing against it would be comparing
	// a confirmed publication with unconfirmed ones -- which the specification forbids.
	s := openIqmq(tb, client, topic, mq.ConfirmNone)
	return &closeAfter{Session: s, after: func() { nc.Close() }}
}

// closeAfter runs a function after the session closes, for an arm that owns a
// connection the library borrowed.
type closeAfter struct {
	opbench.Session
	after func()
}

func (c *closeAfter) Close() error {
	err := c.Session.Close()
	c.after()
	return err
}

// ------------------------------------------------------------- watermill/nats

func openWatermillNats(tb testing.TB, addr string, _ opbench.Params) opbench.Session {
	tb.Helper()
	nc := dial(tb, addr)
	topic := topicName()
	marshaler := &wmnats.NATSMarshaler{}
	pub, err := wmnats.NewPublisherWithNatsConn(nc, wmnats.PublisherPublishConfig{
		// The native-header marshaler, not the default GobMarshaler: the
		// default gob-encodes the watermill message INTO the NATS body, so
		// the bytes on the wire would not be the payload and the arm would be
		// measuring a gob encode the others never do.
		Marshaler: marshaler,
		// JetStream off: this family is NATS Core.
		JetStream: wmnats.JetStreamConfig{Disabled: true},
	}, watermill.NopLogger{})
	if err != nil {
		nc.Close()
		tb.Fatalf("competitors: watermill NewPublisherWithNatsConn: %v", err)
	}
	sub, err := wmnats.NewSubscriberWithNatsConn(nc, wmnats.SubscriberSubscriptionConfig{
		Unmarshaler:      marshaler,
		SubscribersCount: 1,
		AckWaitTimeout:   natsTimeout,
		CloseTimeout:     natsTimeout,
		SubscribeTimeout: natsTimeout,
		JetStream:        wmnats.JetStreamConfig{Disabled: true},
	}, watermill.NopLogger{})
	if err != nil {
		_ = pub.Close()
		nc.Close()
		tb.Fatalf("competitors: watermill NewSubscriberWithNatsConn: %v", err)
	}
	ch, err := sub.Subscribe(context.Background(), topic)
	if err != nil {
		_ = sub.Close()
		_ = pub.Close()
		nc.Close()
		tb.Fatalf("competitors: watermill Subscribe: %v", err)
	}
	return &watermillNatsSession{
		pub:   pub,
		sub:   sub,
		nc:    nc,
		topic: topic,
		ch:    ch,
	}
}

type watermillNatsSession struct {
	pub   *wmnats.Publisher
	sub   *wmnats.Subscriber
	nc    *natsgo.Conn
	topic string
	ch    <-chan *message.Message
	one   *message.Message
	batch []*message.Message
}

func (s *watermillNatsSession) Publish(_ context.Context, payload []byte) error {
	if s.one == nil {
		s.one = message.NewMessage("", nil)
	}
	s.one.UUID = watermill.NewUUID()
	s.one.Payload = payload
	return s.pub.Publish(s.topic, s.one)
}

func (s *watermillNatsSession) PublishBatch(_ context.Context, payloads [][]byte) error {
	for len(s.batch) < len(payloads) {
		s.batch = append(s.batch, message.NewMessage("", nil))
	}
	ms := s.batch[:len(payloads)]
	for i, p := range payloads {
		ms[i].UUID = watermill.NewUUID()
		ms[i].Payload = p
	}
	return s.pub.Publish(s.topic, ms...)
}

func (s *watermillNatsSession) Receive(ctx context.Context, n int) error {
	for range n {
		select {
		case m, ok := <-s.ch:
			if !ok {
				return errNoProgress
			}
			// NATS Core has no acknowledgement to send; Watermill's Ack is a
			// client-side signal that releases the subscriber's slot, and
			// every arm in this family needs its equivalent (iqmq's consumer
			// advertises no FeatureExplicitAck here and settles nothing).
			m.Ack()
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(natsTimeout):
			return errNoProgress
		}
	}
	return nil
}

func (s *watermillNatsSession) Close() error {
	err := s.sub.Close()
	if perr := s.pub.Close(); err == nil {
		err = perr
	}
	s.nc.Close()
	return err
}

// --------------------------------------------------------------- gocloud/nats

func openGocloudNats(tb testing.TB, addr string, _ opbench.Params) opbench.Session {
	tb.Helper()
	nc := dial(tb, addr)
	subject := topicName()
	topic, err := natspubsub.OpenTopicV2(nc, subject, nil)
	if err != nil {
		nc.Close()
		tb.Fatalf("competitors: natspubsub.OpenTopicV2: %v", err)
	}
	sub, err := natspubsub.OpenSubscriptionV2(nc, subject, nil)
	if err != nil {
		_ = topic.Shutdown(context.Background())
		nc.Close()
		tb.Fatalf("competitors: natspubsub.OpenSubscriptionV2: %v", err)
	}
	return &gocloudSession{
		topic: topic,
		sub:   sub,
		msg:   &pubsub.Message{},
		shutdown: func(ctx context.Context) error {
			serr := sub.Shutdown(ctx)
			terr := topic.Shutdown(ctx)
			nc.Close()
			if serr != nil {
				return serr
			}
			return terr
		},
	}
}

// ------------------------------------------------------------------- raw/nats

type rawNatsSession struct {
	nc      *natsgo.Conn
	sub     *natsgo.Subscription
	subject string
	msg     *natsgo.Msg
}

func openRawNats(tb testing.TB, addr string, _ opbench.Params) opbench.Session {
	tb.Helper()
	nc := dial(tb, addr)
	subject := topicName()
	sub, err := nc.SubscribeSync(subject)
	if err != nil {
		nc.Close()
		tb.Fatalf("competitors: SubscribeSync: %v", err)
	}
	// Unlimited pending, so that a drain of opbench's outstanding bound
	// cannot be lost to NATS Core's slow-consumer policy. Every other arm in
	// this family layers its own buffering on top of the same subscription
	// machinery, so this is the one place the limit is reachable.
	if err := sub.SetPendingLimits(-1, -1); err != nil {
		_ = sub.Unsubscribe()
		nc.Close()
		tb.Fatalf("competitors: SetPendingLimits: %v", err)
	}
	return &rawNatsSession{nc: nc, sub: sub, subject: subject, msg: &natsgo.Msg{Subject: subject}}
}

func (s *rawNatsSession) Publish(_ context.Context, payload []byte) error {
	s.msg.Data = payload
	// PublishMsg and no Flush: the family's stated boundary, and the one
	// gocloud's natspubsub and watermill-nats can offer.
	return s.nc.PublishMsg(s.msg)
}

func (s *rawNatsSession) PublishBatch(_ context.Context, payloads [][]byte) error {
	for _, p := range payloads {
		s.msg.Data = p
		if err := s.nc.PublishMsg(s.msg); err != nil {
			return err
		}
	}
	return nil
}

func (s *rawNatsSession) Receive(ctx context.Context, n int) error {
	for range n {
		if _, err := s.sub.NextMsgWithContext(ctx); err != nil {
			return err
		}
	}
	return nil
}

func (s *rawNatsSession) Close() error {
	err := s.sub.Unsubscribe()
	s.nc.Close()
	return err
}
