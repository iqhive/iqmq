package kafka_test

import (
	"context"
	"errors"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit"
	kq "github.com/iqhive/iqmq/drivers/kafka"
)

// The ways a broker that took a produce request can stop being reachable
// afterwards. lookup is false for a broker that refuses every dial, as a
// restarted one does, and true for one whose name no longer resolves, as a
// broker pod gone from its headless service does.
var unreachable = []struct {
	name   string
	lookup bool
}{
	{"every dial refused", false},
	{"the broker's name no longer resolves", true},
}

// openUnreachable opens a backend over a client whose dials can be made to
// fail their name lookup, and returns cut, which takes the cluster away: the
// listener and the connections close, so every redial is refused, or with
// lookup set fails to resolve. The client's options are the driver's own
// that matter here: a delivery timeout short enough to keep the test short,
// and AllowIdempotentProduceCancellation, which lets franz-go fail a record
// it may have sent.
func openUnreachable(t *testing.T, cl *kfake.Cluster, lookup bool, mutate ...func(*kq.Config)) (c *mq.Client, b *kq.Backend, cut func()) {
	t.Helper()
	var unresolved atomic.Bool
	var d net.Dialer
	raw, err := kgo.NewClient(
		kgo.SeedBrokers(cl.ListenAddrs()...),
		kgo.RecordPartitioner(kq.Partitioner()),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.ProducerLinger(0),
		kgo.RecordDeliveryTimeout(2*time.Second),
		kgo.AllowIdempotentProduceCancellation(),
		kgo.Dialer(func(ctx context.Context, network, host string) (net.Conn, error) {
			if unresolved.Load() {
				return nil, &net.OpError{Op: "dial", Net: network, Err: &net.DNSError{Err: "no such host", Name: "broker-0.kafka.invalid", IsNotFound: true}}
			}
			return d.DialContext(ctx, network, host)
		}),
	)
	if err != nil {
		t.Fatalf("kgo.NewClient: %v", err)
	}
	b, err = kq.BackendWithClient(t.Context(), raw, driverkit.ClientOwned, config(cl, mutate...))
	if err != nil {
		raw.Close()
		t.Fatalf("BackendWithClient: %v", err)
	}
	c = mq.NewClient(b)
	t.Cleanup(func() { _ = c.Close() })
	return c, b, func() {
		unresolved.Store(lookup)
		cl.Close()
	}
}

// awaitUnreachable waits for the failure of a write whose broker went away,
// and skips a run in which franz-go failed the record with something other
// than the connection failure the case is about: its delivery timeout can
// also end the record first.
func awaitUnreachable(t *testing.T, errc <-chan error, lookup bool) error {
	t.Helper()
	var err error
	select {
	case err = <-errc:
	case <-time.After(40 * time.Second):
		t.Fatal("the write never resolved after the broker went")
	}
	_, unresolved := errors.AsType[*net.DNSError](err)
	oe, dialled := errors.AsType[*net.OpError](err)
	if !dialled || oe.Op != "dial" || unresolved != lookup {
		t.Skipf("franz-go failed the record with %v, not the connection failure this case is about", err)
	}
	return err
}

// TestAmbiguousPublishWhenNoBrokerCanBeReached: a record whose produce
// request reached the broker, after which no broker can be reached. franz-go
// fails the record, once its delivery timeout has passed, with the error of
// the metadata request that could not connect, which says nothing about the
// record's own request: that one may have been appended.
//
// driverkit.ClassifyNet calls a name lookup that failed definite, because the
// attempt that made it wrote nothing, so a publication whose broker's name
// stopped resolving came out as a definite, retryable Unavailable for a
// record that may be in the log, and a caller who repeated it put it there
// twice. A refused dial is the same case, so it is held here as well.
func TestAmbiguousPublishWhenNoBrokerCanBeReached(t *testing.T) {
	for _, tc := range unreachable {
		t.Run(tc.name, func(t *testing.T) {
			topic := newTopic("ambig-unreachable")
			cl := cluster(t, 1, []string{topic})
			c, _, cut := openUnreachable(t, cl, tc.lookup)
			publish(t, c, topic, &mq.Message{Body: []byte("warm")})
			held, release := holdFirstProduce(t, cl)
			errc := make(chan error, 1)
			go func() { errc <- c.Publish(t.Context(), topic, &mq.Message{Body: []byte("in flight")}) }()
			<-held // the produce request is at the broker
			cut()
			err := awaitUnreachable(t, errc, tc.lookup)
			release()
			assertUnknown(t, err)
		})
	}
}

// TestAmbiguousMoveWhenNoBrokerCanBeReached is the same answer for a nack's
// move to a dead-letter topic: the copy may be in the dead-letter topic, so
// the nack must not report a definite refusal a caller would retry into a
// second copy.
func TestAmbiguousMoveWhenNoBrokerCanBeReached(t *testing.T) {
	for _, tc := range unreachable {
		t.Run(tc.name, func(t *testing.T) {
			topic, dlq := newTopic("ambig-unreachable-move"), newTopic("ambig-unreachable-move-dead")
			cl := cluster(t, 1, []string{topic, dlq})
			// The move's wait is bounded by RequestTimeout, which has to
			// outlast the delivery timeout for franz-go's answer to arrive.
			c, b, cut := openUnreachable(t, cl, tc.lookup, func(cfg *kq.Config) {
				cfg.NackConfig.DeadLetterTopic = dlq
				cfg.RequestTimeout = 30 * time.Second
			})
			publish(t, c, topic, &mq.Message{Body: []byte("poison")})
			src, err := b.OpenSource(t.Context(), mq.SourceConfig{
				Topic: topic, Subscription: "g-" + topic, Prefetch: 16, Start: mq.StartEarliest(),
			})
			if err != nil {
				t.Fatalf("OpenSource: %v", err)
			}
			t.Cleanup(func() { _ = src.Close() })
			d := fetchN(t, t.Context(), src, 1)[0]
			held, release := holdFirstProduce(t, cl)
			errc := make(chan error, 1)
			go func() { errc <- src.Settler().Nack(t.Context(), &d, mq.NackOptions{Action: mq.NackActionDeadLetter}) }()
			<-held // the copy's produce request is at the broker
			cut()
			err = awaitUnreachable(t, errc, tc.lookup)
			release()
			assertUnknown(t, err)
		})
	}
}
