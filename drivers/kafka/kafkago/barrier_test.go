package kafkago_test

import (
	"context"
	"errors"
	"testing"
	"time"

	segkafka "github.com/segmentio/kafka-go"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kmsg"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit"
	kq "github.com/iqhive/iqmq/drivers/kafka/kafkago"
)

// synchronousPaths are the two publications that go through the synchronous
// writer and wait for the broker.
var synchronousPaths = []struct {
	name    string
	publish func(ctx context.Context, s mq.Sink, m *mq.Message) error
}{
	{"Publish", func(ctx context.Context, s mq.Sink, m *mq.Message) error { return s.Publish(ctx, m, nil) }},
	{"PublishBatch", func(ctx context.Context, s mq.Sink, m *mq.Message) error {
		return s.(mq.BatchSink).PublishBatch(ctx, []*mq.Message{m}, nil)
	}},
}

// TestAKeyPublishedAsynchronouslyThenSynchronouslyKeepsItsOrder is kf-18.
//
// PublishAsync, PublishOwned and a ConfirmNone Publish go through the
// asynchronous writer, and Publish and PublishBatch through the synchronous
// one, and nothing ordered one writer's batches against the other's. So a key
// published asynchronously and then synchronously landed the other way round
// when the first record's produce was refused and retried: the later
// publication overtook it on the other writer. Since kf-12 that held on a
// borrowed synchronous writer too. A synchronous publication now waits for the
// asynchronous ones its publisher admitted before it.
func TestAKeyPublishedAsynchronouslyThenSynchronouslyKeepsItsOrder(t *testing.T) {
	for _, writer := range []string{"Open", "a borrowed writer"} {
		for _, sync := range synchronousPaths {
			t.Run(writer+"/"+sync.name, func(t *testing.T) {
				topic := newTopic("barrier-order")
				cl := cluster(t, 1, []string{topic})
				var b *kq.Backend
				if writer == "Open" {
					b = backendOn(t, cl, func(cfg *kq.Config) {
						cfg.MaxAttempts = 5
						cfg.WriteBackoffMin = 300 * time.Millisecond
						cfg.WriteBackoffMax = 300 * time.Millisecond
					})
				} else {
					w := borrowedWriter(t, cl, func(w *segkafka.Writer) {
						w.MaxAttempts = 5
						w.WriteBackoffMin = 300 * time.Millisecond
						w.WriteBackoffMax = 300 * time.Millisecond
					})
					var err error
					if b, err = kq.BackendWithWriter(t.Context(), w, driverkit.ClientOwned, config(cl)); err != nil {
						t.Fatalf("BackendWithWriter: %v", err)
					}
					t.Cleanup(func() { _ = b.Close() })
				}
				snk, err := b.OpenSink(t.Context(), mq.SinkConfig{Topic: topic})
				if err != nil {
					t.Fatalf("OpenSink: %v", err)
				}
				key := []byte("k")
				if err := snk.Publish(t.Context(), &mq.Message{Key: key, Body: []byte("warm")}, nil); err != nil {
					t.Fatalf("warm: %v", err)
				}
				// The first record's first attempt is refused with a retriable
				// answer, so the asynchronous writer holds it through a backoff.
				fault := cl.Fault(kfake.Fault{Keys: []kmsg.Key{kmsg.Produce}, Topic: topic, Err: kerr.NotEnoughReplicas, Count: 1})
				defer fault.Remove()
				first := make(chan error, 1)
				if err := snk.(mq.AsyncSink).PublishAsync(t.Context(), &mq.Message{Key: key, Body: []byte("1")}, nil,
					func(err error) { first <- err }); err != nil {
					t.Fatalf("PublishAsync 1: %v", err)
				}
				if err := fault.Wait(t.Context(), 1); err != nil {
					t.Fatalf("the first record's produce never reached the broker: %v", err)
				}
				if err := sync.publish(t.Context(), snk, &mq.Message{Key: key, Body: []byte("2")}); err != nil {
					t.Fatalf("%s 2: %v", sync.name, err)
				}
				select {
				case err := <-first:
					if err != nil {
						t.Fatalf("publication 1: %v", err)
					}
				case <-time.After(10 * time.Second):
					t.Fatal("publication 1 never completed")
				}
				got := readLog(t, cl, topic, 3)
				if len(got) != 3 || got[1] != "1" || got[2] != "2" {
					t.Errorf("key k's records are %q in the log, want [warm 1 2]", got)
				}
			})
		}
	}
}

// TestASynchronousPublicationThatEndsBehindTheBarrierWasNotSent: a
// synchronous publication waiting for its publisher's earlier asynchronous
// ones has sent nothing, so one whose context ends there fails definitely,
// and one whose publisher closes there fails with ErrClosed.
func TestASynchronousPublicationThatEndsBehindTheBarrierWasNotSent(t *testing.T) {
	for _, end := range []string{"its context", "its publisher"} {
		t.Run(end, func(t *testing.T) {
			topic := newTopic("barrier-end")
			cl := cluster(t, 1, []string{topic})
			b := backendOn(t, cl, func(cfg *kq.Config) { cfg.WriteTimeout = 30 * time.Second })
			snk, err := b.OpenSink(t.Context(), mq.SinkConfig{Topic: topic})
			if err != nil {
				t.Fatalf("OpenSink: %v", err)
			}
			if err := snk.Publish(t.Context(), &mq.Message{Body: []byte("warm")}, nil); err != nil {
				t.Fatalf("warm: %v", err)
			}
			reached, release := holdProduce(t, cl, topic, 0)
			defer release()
			first := make(chan error, 1)
			if err := snk.(mq.AsyncSink).PublishAsync(t.Context(), &mq.Message{Body: []byte("1")}, nil,
				func(err error) { first <- err }); err != nil {
				t.Fatalf("PublishAsync 1: %v", err)
			}
			<-reached
			ctx, cancel := context.WithTimeout(t.Context(), 300*time.Millisecond)
			defer cancel()
			if end == "its publisher" {
				ctx = t.Context()
				time.AfterFunc(300*time.Millisecond, func() { _ = snk.Close() })
			}
			err = snk.Publish(ctx, &mq.Message{Body: []byte("2")}, nil)
			switch {
			case err == nil:
				t.Fatal("Publish 2 returned nil while its publisher's earlier PublishAsync was unanswered: it overtook it")
			case mq.IsAmbiguous(err):
				t.Errorf("Publish 2 = %v, ambiguous, though it ended before anything was sent", err)
			case end == "its context" && !errors.Is(err, context.DeadlineExceeded):
				t.Errorf("Publish 2 = %v, want its context's deadline", err)
			case end == "its publisher" && !errors.Is(err, mq.ErrClosed):
				t.Errorf("Publish 2 = %v, want ErrClosed", err)
			}
			release()
			if err := <-first; err != nil {
				t.Fatalf("publication 1: %v", err)
			}
			waitFor(t, 5*time.Second, "publication 1 appended", func() bool { return cl.PartitionInfo(topic, 0).HighWatermark >= 2 })
			time.Sleep(200 * time.Millisecond)
			if hw := cl.PartitionInfo(topic, 0).HighWatermark; hw != 2 {
				t.Errorf("the log holds %d records, want warm and 1: the publication that ended behind the barrier was sent", hw)
			}
		})
	}
}

// TestTheBarrierWaitsOnlyForItsOwnPublisher: the barrier is the publisher's,
// as Flush is, so another publisher's unanswered record does not hold a
// synchronous publication.
func TestTheBarrierWaitsOnlyForItsOwnPublisher(t *testing.T) {
	ta, tb := newTopic("barrier-a"), newTopic("barrier-b")
	cl := cluster(t, 1, []string{ta, tb})
	b := backendOn(t, cl, func(cfg *kq.Config) {
		cfg.MaxAttempts = 100
		cfg.WriteBackoffMin = 100 * time.Millisecond
		cfg.WriteBackoffMax = 200 * time.Millisecond
		cfg.WriteTimeout = 30 * time.Second
	})
	sa, err := b.OpenSink(t.Context(), mq.SinkConfig{Topic: ta})
	if err != nil {
		t.Fatalf("OpenSink A: %v", err)
	}
	sb, err := b.OpenSink(t.Context(), mq.SinkConfig{Topic: tb})
	if err != nil {
		t.Fatalf("OpenSink B: %v", err)
	}
	fault := cl.Fault(kfake.Fault{Keys: []kmsg.Key{kmsg.Produce}, Topic: tb, Err: kerr.NotEnoughReplicas, Count: -1})
	defer fault.Remove()
	bdone := make(chan error, 1)
	if err := sb.(mq.AsyncSink).PublishAsync(t.Context(), &mq.Message{Body: []byte("b")}, nil, func(err error) { bdone <- err }); err != nil {
		t.Fatalf("PublishAsync B: %v", err)
	}
	if err := fault.Wait(t.Context(), 1); err != nil {
		t.Fatalf("B's produce never reached the broker: %v", err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	if err := sa.Publish(ctx, &mq.Message{Body: []byte("a")}, nil); err != nil {
		t.Errorf("A's Publish = %v while B's record was retried: it waited for another publisher's record", err)
	}
	fault.Remove()
	select {
	case err := <-bdone:
		if err != nil {
			t.Errorf("B's publication, once the fault was gone: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("B's publication never completed")
	}
}
