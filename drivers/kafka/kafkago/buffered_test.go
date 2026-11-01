package kafkago_test

import (
	"context"
	"errors"
	"strconv"
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
	kq "github.com/iqhive/iqmq/drivers/kafka/kafkago"
)

// unconfirmedPaths are the three publications that hand the record to the
// asynchronous writer and return before the broker answers. done runs once
// the publication has ended, as each path reports it.
var unconfirmedPaths = []struct {
	name    string
	confirm mq.ConfirmationMode
	publish func(ctx context.Context, s mq.Sink, m *mq.Message, done func(error)) error
}{
	{"ConfirmNone Publish", mq.ConfirmNone, func(ctx context.Context, s mq.Sink, m *mq.Message, done func(error)) error {
		return s.Publish(ctx, m, nil)
	}},
	{"PublishOwned", mq.ConfirmAccepted, func(ctx context.Context, s mq.Sink, m *mq.Message, done func(error)) error {
		return s.(mq.OwnedSink).PublishOwned(ctx, mq.OwnedMessage{Message: m, Release: func(*mq.Message) { done(nil) }}, nil)
	}},
	{"PublishAsync", mq.ConfirmAccepted, func(ctx context.Context, s mq.Sink, m *mq.Message, done func(error)) error {
		return s.(mq.AsyncSink).PublishAsync(ctx, m, nil, done)
	}},
}

// TestTheUnconfirmedPathsWaitForRoomInTheBuffer is kf-15.
//
// A ConfirmNone Publish, a PublishOwned and a PublishAsync hand the record to
// kafka-go's asynchronous writer and return, and that writer queues without
// bound: while the broker answered nothing, 50,000 ConfirmNone publications
// were admitted in 1.5 s and the heap grew by 102 MiB. Once
// Config.MaxBufferedRecords records (10,000 by default) are unanswered, a
// publication waits for room, and one whose context ends first fails
// definitely, because nothing reached the writer.
func TestTheUnconfirmedPathsWaitForRoomInTheBuffer(t *testing.T) {
	for _, path := range unconfirmedPaths {
		t.Run(path.name, func(t *testing.T) {
			topic := newTopic("buffered")
			cl := cluster(t, 1, []string{topic})
			b := backendOn(t, cl, func(cfg *kq.Config) { cfg.WriteTimeout = 30 * time.Second })
			snk, err := b.OpenSink(t.Context(), mq.SinkConfig{Topic: topic, Confirmation: path.confirm})
			if err != nil {
				t.Fatalf("OpenSink: %v", err)
			}
			if err := snk.Publish(t.Context(), &mq.Message{Body: []byte("warm")}, nil); err != nil {
				t.Fatalf("warm: %v", err)
			}
			if err := snk.(mq.FlushSink).Flush(t.Context()); err != nil {
				t.Fatalf("warm Flush: %v", err)
			}
			reached, release := holdProduce(t, cl, topic, 0)
			ignore := func(error) {}
			for i := range kq.DefaultMaxBufferedRecords {
				if err := path.publish(t.Context(), snk, &mq.Message{Body: []byte(strconv.Itoa(i))}, ignore); err != nil {
					t.Fatalf("publication %d of %d: %v", i, kq.DefaultMaxBufferedRecords, err)
				}
				if i == 0 {
					<-reached
				}
			}
			ctx, cancel := context.WithTimeout(t.Context(), 300*time.Millisecond)
			defer cancel()
			start := time.Now()
			err = path.publish(ctx, snk, &mq.Message{Body: []byte("one too many")}, ignore)
			if err == nil {
				t.Fatalf("publication %d was admitted while %d records waited for the broker's answer: the buffer has no bound",
					kq.DefaultMaxBufferedRecords+1, kq.DefaultMaxBufferedRecords)
			}
			if !errors.Is(err, context.DeadlineExceeded) || mq.IsAmbiguous(err) {
				t.Errorf("a publication whose context ended while it waited for room = %v (ambiguous %t), want a definite deadline",
					err, mq.IsAmbiguous(err))
			}
			if waited := time.Since(start); waited < 250*time.Millisecond {
				t.Errorf("the refused publication returned after %v, before its context ended", waited)
			}
			release()
			if err := snk.(mq.FlushSink).Flush(t.Context()); err != nil {
				t.Fatalf("Flush once the broker answered: %v", err)
			}
			// The answers gave the room back.
			last := make(chan error, 1)
			if err := path.publish(t.Context(), snk, &mq.Message{Body: []byte("after")}, func(err error) { last <- err }); err != nil {
				t.Fatalf("a publication once the buffer drained: %v", err)
			}
			if err := snk.(mq.FlushSink).Flush(t.Context()); err != nil {
				t.Fatalf("Flush: %v", err)
			}
			if hw := cl.PartitionInfo(topic, 0).HighWatermark; hw != int64(kq.DefaultMaxBufferedRecords)+2 {
				t.Errorf("the log holds %d records, want the warm one, %d buffered and the last one: the refused one was not sent",
					hw, kq.DefaultMaxBufferedRecords)
			}
		})
	}
}

// TestAPublicationWaitingForRoomEndsWhenItsPublisherCloses: the publisher's
// Close is how the core releases a publication it is waiting for, and a
// publication waiting for room in the buffer is one. It ends with ErrClosed,
// and nothing was sent.
func TestAPublicationWaitingForRoomEndsWhenItsPublisherCloses(t *testing.T) {
	for _, path := range unconfirmedPaths {
		t.Run(path.name, func(t *testing.T) {
			topic := newTopic("buffered-close")
			cl := cluster(t, 1, []string{topic})
			b := backendOn(t, cl, func(cfg *kq.Config) {
				cfg.WriteTimeout = 30 * time.Second
				cfg.MaxBufferedRecords = 1
			})
			snk, err := b.OpenSink(t.Context(), mq.SinkConfig{Topic: topic, Confirmation: path.confirm})
			if err != nil {
				t.Fatalf("OpenSink: %v", err)
			}
			if err := snk.Publish(t.Context(), &mq.Message{Body: []byte("warm")}, nil); err != nil {
				t.Fatalf("warm: %v", err)
			}
			if err := snk.(mq.FlushSink).Flush(t.Context()); err != nil {
				t.Fatalf("warm Flush: %v", err)
			}
			reached, release := holdProduce(t, cl, topic, 0)
			defer release()
			if err := path.publish(t.Context(), snk, &mq.Message{Body: []byte("held")}, func(error) {}); err != nil {
				t.Fatalf("the held publication: %v", err)
			}
			<-reached
			waiting := make(chan error, 1)
			go func() {
				waiting <- path.publish(context.Background(), snk, &mq.Message{Body: []byte("waits")}, func(error) {})
			}()
			select {
			case err := <-waiting:
				t.Fatalf("a publication beyond MaxBufferedRecords 1 returned %v at once", err)
			case <-time.After(200 * time.Millisecond):
			}
			if err := snk.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}
			select {
			case err := <-waiting:
				if !errors.Is(err, mq.ErrClosed) {
					t.Errorf("the waiting publication = %v after its publisher closed, want ErrClosed", err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("closing the publisher did not end a publication waiting for room")
			}
		})
	}
}
