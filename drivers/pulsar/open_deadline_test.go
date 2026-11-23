package pulsar_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit"
	pd "github.com/iqhive/iqmq/drivers/pulsar"
	"github.com/iqhive/iqmq/drivers/pulsar/internal/pulsarapi"
	"github.com/iqhive/iqmq/drivers/pulsar/pulsarfake"
)

// stalledClient is the in-process model behind a broker that accepted the
// connection and answers nothing. The step stall names (CreateProducer,
// Subscribe, or the consumer's SeekByTime, none of which takes a context)
// waits until release is closed and then does what it was asked. It counts
// the producers and consumers it created and the ones closed since, and the
// seeks its consumers made.
type stalledClient struct {
	pulsarapi.Client
	stall   string
	release chan struct{}
	created atomic.Int32
	closed  atomic.Int32
	seeks   atomic.Int32
}

func (c *stalledClient) CreateProducer(o pulsar.ProducerOptions) (pulsar.Producer, error) {
	if c.stall == "CreateProducer" {
		<-c.release
	}
	p, err := c.Client.CreateProducer(o)
	if err != nil {
		return nil, err
	}
	c.created.Add(1)
	return &stalledProducer{Producer: p, c: c}, nil
}

func (c *stalledClient) Subscribe(o pulsar.ConsumerOptions) (pulsar.Consumer, error) {
	if c.stall == "Subscribe" {
		<-c.release
	}
	cons, err := c.Client.Subscribe(o)
	if err != nil {
		return nil, err
	}
	c.created.Add(1)
	return &stalledConsumer{Consumer: cons, c: c}, nil
}

type stalledProducer struct {
	pulsar.Producer
	c *stalledClient
}

func (p *stalledProducer) Close() {
	p.c.closed.Add(1)
	p.Producer.Close()
}

type stalledConsumer struct {
	pulsar.Consumer
	c *stalledClient
}

func (s *stalledConsumer) SeekByTime(at time.Time) error {
	s.c.seeks.Add(1)
	if s.c.stall == "SeekByTime" {
		<-s.c.release
	}
	return s.Consumer.SeekByTime(at)
}

func (s *stalledConsumer) Close() {
	s.c.closed.Add(1)
	s.Consumer.Close()
}

// TestAnOpenOnABrokerThatAnswersNothingEndsAtItsDeadline: pulsar-client-go's
// CreateProducer, Subscribe and SeekByTime take no context, and a topic's
// first producer and every consumer were created on the caller's goroutine,
// so an open on a broker that accepted the connection and answers nothing
// outlived its deadline by several OperationTimeouts. The open returns the
// context's error at the deadline, definite because nothing was published or
// acknowledged, and the backend closes the handle the late creation makes
// once it arrives, before its own Close returns. A late subscription whose
// caller has given up does not seek: a seek moves the durable subscription's
// cursor for every consumer on it.
func TestAnOpenOnABrokerThatAnswersNothingEndsAtItsDeadline(t *testing.T) {
	fromATime := func(ctx context.Context, b *pd.Backend) (func() error, error) {
		s, err := b.OpenSource(ctx, mq.SourceConfig{
			Topic: "stalled", Subscription: "workers", Prefetch: 4, Metadata: mq.MetadataNativeStrict,
			Start: mq.StartAt(time.Unix(0, 0)),
		})
		if err != nil {
			return nil, err
		}
		return s.Close, nil
	}
	for _, tc := range []struct {
		name  string
		stall string
		open  func(context.Context, *pd.Backend) (func() error, error)
		seeks int32
	}{
		{"OpenSink", "CreateProducer", func(ctx context.Context, b *pd.Backend) (func() error, error) {
			s, err := b.OpenSink(ctx, mq.SinkConfig{Topic: "stalled", Metadata: mq.MetadataNativeStrict})
			if err != nil {
				return nil, err
			}
			return s.Close, nil
		}, 0},
		{"OpenSource", "Subscribe", func(ctx context.Context, b *pd.Backend) (func() error, error) {
			s, err := b.OpenSource(ctx, mq.SourceConfig{
				Topic: "stalled", Subscription: "workers", Prefetch: 4, Metadata: mq.MetadataNativeStrict,
			})
			if err != nil {
				return nil, err
			}
			return s.Close, nil
		}, 0},
		{"OpenSource from a time, the seek stalled", "SeekByTime", fromATime, 1},
		{"OpenSource from a time, the subscription stalled", "Subscribe", fromATime, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := pulsarfake.New()
			cl := &stalledClient{Client: server.Client(), stall: tc.stall, release: make(chan struct{})}
			b, err := pd.BackendWithClient(context.Background(), cl, driverkit.ClientOwned, rigConfig(server))
			if err != nil {
				t.Fatalf("BackendWithClient: %v", err)
			}
			// The broker answers 3 s later, as pulsar-client-go's
			// OperationTimeout would end the creation.
			release := time.AfterFunc(3*time.Second, func() { close(cl.release) })
			defer release.Stop()
			ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
			defer cancel()
			began := time.Now()
			closeFn, err := tc.open(ctx, b)
			took := time.Since(began)
			if closeFn != nil {
				_ = closeFn()
			}
			if took > time.Second {
				t.Errorf("%s under a 100 ms deadline on a broker that answers nothing returned after %v = %v, want at its deadline",
					tc.name, took.Round(time.Millisecond), err)
			}
			if !errors.Is(err, context.DeadlineExceeded) || mq.CodeOf(err) != mq.CodeDeadlineExceeded || mq.IsAmbiguous(err) {
				t.Errorf("%s whose deadline passed during %s = %v (code %v, ambiguous %v), want a definite DeadlineExceeded",
					tc.name, tc.stall, err, mq.CodeOf(err), mq.IsAmbiguous(err))
			}
			if err := b.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}
			if created, closed := cl.created.Load(), cl.closed.Load(); created != 1 || closed != created {
				t.Fatalf("after Close, %d handle(s) created and %d closed: the late creation was not closed", created, closed)
			}
			if seeks := cl.seeks.Load(); seeks != tc.seeks {
				t.Fatalf("the late subscription made %d seek(s), want %d: its caller had given up", seeks, tc.seeks)
			}
		})
	}
}
