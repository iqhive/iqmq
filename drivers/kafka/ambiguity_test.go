package kafka_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit"
	kq "github.com/iqhive/iqmq/drivers/kafka"
)

// The Ambiguous publication, which is the one outcome a Kafka driver is most
// likely to get wrong and the most expensive to get wrong.
//
// Once a record has entered franz-go's buffer the client may have written it.
// Everything that goes wrong after that moment — the context being cancelled,
// the deadline passing, the broker answering REQUEST_TIMED_OUT or
// NOT_ENOUGH_REPLICAS_AFTER_APPEND — may be a failure to HEAR the answer
// rather than a failure to write the record. Reporting any of them as a
// definite failure is how a caller retries and publishes twice while believing
// it published once, and ErrSettlementUnknown exists for exactly this.
//
// Every case below asserts three things: that the error says the outcome is
// unknown ([github.com/iqhive/iqmq.IsAmbiguous]), that it matches
// ErrSettlementUnknown so a caller who reaches for the sentinel finds it, and
// that the native error is still underneath for a caller who needs the
// broker's own answer.

// TestAmbiguousPublishAfterRetries: a RETRYABLE broker error is retried by
// franz-go until the delivery timeout, and the record then fails with
// kgo.ErrRecordTimeout — which is the honest answer and an ambiguous one.
//
// The caller never sees the broker's own code for these, and that is not a
// mapping gap: by the time the record is failed there have been several
// attempts, and the client cannot know whether one of them landed before its
// reply was lost. REQUEST_TIMED_OUT and NOT_ENOUGH_REPLICAS_AFTER_APPEND are
// the two the broker answers AFTER writing, and NOT_ENOUGH_REPLICAS is one it
// answers before — retried, all three end in the same place, and reporting any
// of them as a definite failure is what makes a caller publish twice.
func TestAmbiguousPublishAfterRetries(t *testing.T) {
	for _, ke := range []*kerr.Error{
		kerr.RequestTimedOut,
		kerr.NotEnoughReplicasAfterAppend,
		kerr.NotEnoughReplicas,
	} {
		t.Run(ke.Message, func(t *testing.T) {
			topic := newTopic("ambig")
			cl := cluster(t, 1, []string{topic})
			c := openOn(t, cl)
			fault := cl.Fault(kfake.Fault{Keys: []kmsg.Key{kmsg.Produce}, Topic: topic, Err: ke, Count: -1})
			defer fault.Remove()
			err := c.Publish(t.Context(), topic, &mq.Message{Body: []byte("x")})
			if err == nil {
				t.Fatal("the publication succeeded through the fault")
			}
			assertUnknown(t, err)
			// The native cause is still reachable, so a caller who wants
			// to know WHY the outcome is unknown can find out.
			if !errors.Is(err, kgo.ErrRecordTimeout) {
				t.Fatalf("the native cause is not reachable: %v", err)
			}
		})
	}
}

// TestAmbiguousPublishThatEndedOnAnUnknownTopic: franz-go retries a produce
// the broker answers UNKNOWN_TOPIC_OR_PARTITION, and fails the record with
// that bare answer once it has had it too many times. An attempt before those
// may have been appended: here the first is appended and answered
// REQUEST_TIMED_OUT. The rule read the bare answer as a definite refusal, so
// a caller's retry wrote a second copy.
func TestAmbiguousPublishThatEndedOnAnUnknownTopic(t *testing.T) {
	topic := newTopic("ambig-unknown")
	cl := cluster(t, 1, []string{topic})
	c, b := openRetryingIntoUnknownTopics(t, cl, topic)
	err := c.Publish(t.Context(), topic, &mq.Message{Body: []byte("x")})
	if hw := cl.PartitionInfo(topic, 0).HighWatermark; hw != 1 {
		t.Fatalf("the log holds %d records, want 1: the timed-out attempt was not appended", hw)
	}
	if err == nil {
		t.Fatal("the publication succeeded through the fault")
	}
	assertUnknown(t, err)
	if !errors.Is(err, kerr.UnknownTopicOrPartition) {
		t.Fatalf("the last answer is not reachable: %v", err)
	}
	if n := b.AmbiguousPublications(); n != 1 {
		t.Fatalf("%d ambiguous publications counted, want 1", n)
	}
}

// TestAmbiguousMoveThatEndedOnAnUnknownTopic is the same answer for a nack's
// move to a dead-letter topic. The move may be in the dead-letter topic, so
// the nack must neither acknowledge the original nor report a definite
// refusal a caller would retry into a second copy.
func TestAmbiguousMoveThatEndedOnAnUnknownTopic(t *testing.T) {
	topic, dlq := newTopic("ambig-unknown-move"), newTopic("ambig-unknown-move-dead")
	cl := cluster(t, 1, []string{topic, dlq})
	c, b := openRetryingIntoUnknownTopics(t, cl, dlq, func(cfg *kq.Config) { cfg.NackConfig.DeadLetterTopic = dlq })
	publish(t, c, topic, &mq.Message{Body: []byte("poison")})
	src, err := b.OpenSource(t.Context(), mq.SourceConfig{
		Topic: topic, Subscription: "g-ambig-unknown-move", Prefetch: 16, Start: mq.StartEarliest(),
	})
	if err != nil {
		t.Fatalf("OpenSource: %v", err)
	}
	t.Cleanup(func() { _ = src.Close() })
	d := fetchN(t, t.Context(), src, 1)[0]
	err = src.Settler().Nack(t.Context(), &d, mq.NackOptions{Action: mq.NackActionDeadLetter})
	if hw := cl.PartitionInfo(dlq, 0).HighWatermark; hw != 1 {
		t.Fatalf("the dead-letter topic holds %d records, want 1: the timed-out attempt was not appended", hw)
	}
	if err == nil {
		t.Fatal("the move succeeded through the fault")
	}
	assertUnknown(t, err)
	if n := b.AmbiguousPublications(); n != 1 {
		t.Fatalf("%d ambiguous publications counted, want 1", n)
	}
}

// openRetryingIntoUnknownTopics opens a backend whose produces to topic are
// appended and answered REQUEST_TIMED_OUT once, and answered
// UNKNOWN_TOPIC_OR_PARTITION from then on. Its client gives up after two
// unknown answers and refreshes its metadata quickly, which keeps the tests
// short; the classification is the driver's whatever the client's limits.
func openRetryingIntoUnknownTopics(t *testing.T, cl *kfake.Cluster, topic string, mutate ...func(*kq.Config)) (*mq.Client, *kq.Backend) {
	t.Helper()
	raw, err := kgo.NewClient(
		kgo.SeedBrokers(cl.ListenAddrs()...),
		kgo.RecordPartitioner(kq.Partitioner()),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.ProducerLinger(0),
		kgo.UnknownTopicRetries(1),
		kgo.MetadataMinAge(10*time.Millisecond),
		kgo.RecordDeliveryTimeout(30*time.Second),
	)
	if err != nil {
		t.Fatalf("kgo.NewClient: %v", err)
	}
	b, err := kq.BackendWithClient(t.Context(), raw, driverkit.ClientOwned, config(cl, mutate...))
	if err != nil {
		raw.Close()
		t.Fatalf("BackendWithClient: %v", err)
	}
	c := mq.NewClient(b)
	t.Cleanup(func() { _ = c.Close() })
	// Faults answer in the order they were added, and kfake appends a
	// produce it answers with its own REQUEST_TIMED_OUT.
	fault := cl.Fault(
		kfake.Fault{Keys: []kmsg.Key{kmsg.Produce}, Topic: topic, Err: kerr.RequestTimedOut, Count: 1},
		kfake.Fault{Keys: []kmsg.Key{kmsg.Produce}, Topic: topic, Err: kerr.UnknownTopicOrPartition, Count: -1},
	)
	t.Cleanup(fault.Remove)
	return c, b
}

// TestAmbiguousPublishOnBrokenIdempotence: an idempotent producer whose
// sequence went wrong can no longer tell what the broker has.
//
// DUPLICATE_SEQUENCE_NUMBER is deliberately NOT here: franz-go treats it as a
// SUCCESS, because it means the broker already has this exact record — which
// is the idempotent producer working rather than failing, and the publication
// is not ambiguous at all. The driver's rule for it only matters for a
// caller who reaches the error through a path franz-go does not resolve.
func TestAmbiguousPublishOnBrokenIdempotence(t *testing.T) {
	for _, ke := range []*kerr.Error{
		kerr.OutOfOrderSequenceNumber,
		kerr.UnknownProducerID,
	} {
		t.Run(ke.Message, func(t *testing.T) {
			topic := newTopic("ambig-idem")
			cl := cluster(t, 1, []string{topic})
			c := openOn(t, cl)
			fault := cl.Fault(kfake.Fault{Keys: []kmsg.Key{kmsg.Produce}, Topic: topic, Err: ke, Count: -1})
			defer fault.Remove()
			err := c.Publish(t.Context(), topic, &mq.Message{Body: []byte("x")})
			if err == nil {
				t.Fatal("the publication succeeded through the fault")
			}
			assertUnknown(t, err)
		})
	}
}

// TestDefinitePublishFailures are the other half of the distinction, and they
// matter as much: an error the broker gives once and does not retry is a
// definite refusal, and reporting it as ambiguous would make every caller
// reconcile a publication that certainly did not happen.
func TestDefinitePublishFailures(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  *kerr.Error
		code mq.ErrorCode
	}{
		{"message too large", kerr.MessageTooLarge, mq.CodeMessageTooLarge},
		{"topic authorization failed", kerr.TopicAuthorizationFailed, mq.CodeForbidden},
		{"invalid required acks", kerr.InvalidRequiredAcks, mq.CodeInvalidArgument},
		{"invalid record", kerr.InvalidRecord, mq.CodeCorruptMessage},
	} {
		t.Run(tc.name, func(t *testing.T) {
			topic := newTopic("definite")
			cl := cluster(t, 1, []string{topic})
			c := openOn(t, cl)
			fault := cl.Fault(kfake.Fault{Keys: []kmsg.Key{kmsg.Produce}, Topic: topic, Err: tc.err, Count: -1})
			defer fault.Remove()
			err := c.Publish(t.Context(), topic, &mq.Message{Body: []byte("x")})
			if err == nil {
				t.Fatal("the publication succeeded through the fault")
			}
			if mq.IsAmbiguous(err) {
				t.Fatalf("%v was reported as ambiguous; the broker refused it outright, so the caller may retry without duplicating", tc.err)
			}
			if got := mq.CodeOf(err); got != tc.code {
				t.Fatalf("%v mapped to %v, want %v", tc.err, got, tc.code)
			}
			var kerror *kerr.Error
			if !errors.As(err, &kerror) || kerror.Code != tc.err.Code {
				t.Fatalf("the native error is not reachable through errors.As: %v", err)
			}
		})
	}
}

// TestAmbiguousPublishOnCancellationAfterBuffering is the case the specification names and the hardest to see: the record is in the producer's buffer and the
// CALLER's context is cancelled.
//
// franz-go resolves the record's promise with the context's error, and the
// record may still be written — the client has it and the connection is open.
// So the answer is not "cancelled", it is "unknown", and a driver that
// returned context.Canceled as a plain failure would have a caller retry a
// record that is already in the log.
func TestAmbiguousPublishOnCancellationAfterBuffering(t *testing.T) {
	topic := newTopic("ambig-cancel")
	cl := cluster(t, 1, []string{topic})
	c := openOn(t, cl)

	// Hold the produce request inside the broker until the test says so,
	// which is the window in which the record is buffered and its outcome
	// is not yet known.
	released := make(chan struct{})
	var once bool
	// (nil, nil, false) is kfake's own "I did not handle this request; let
	// the cluster answer it", which is why the two returns are both nil.
	cl.ControlKey(int16(kmsg.Produce), func(kmsg.Request) (kmsg.Response, error, bool) {
		if once {
			return nil, nil, false //nolint:nilnil // kfake's "not handled" is (nil, nil, false)
		}
		once = true
		cl.KeepControl()
		cl.SleepControl(func() { <-released })
		return nil, nil, false //nolint:nilnil // kfake's "not handled" is (nil, nil, false)
	})

	ctx, cancel := context.WithCancel(t.Context())
	errc := make(chan error, 1)
	go func() { errc <- c.Publish(ctx, topic, &mq.Message{Body: []byte("in-flight")}) }()
	// Give the record time to reach the buffer and the produce time to
	// reach the broker, then cancel.
	time.Sleep(300 * time.Millisecond)
	cancel()

	var err error
	select {
	case err = <-errc:
	case <-time.After(20 * time.Second):
		close(released)
		t.Fatal("the publication never resolved after its context was cancelled")
	}
	close(released)

	if err == nil {
		t.Skip("the record was written before the cancellation was observed, so there was no ambiguity to report")
	}
	assertUnknown(t, err)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("the native cause is no longer reachable: errors.Is(%v, context.Canceled) is false, so a caller cannot see WHY the outcome is unknown", err)
	}
}

// TestDefinitePublishCancelledWhileWaitingForBufferRoom is the other side of
// the case above: a record whose context ends while franz-go is still waiting
// for room in a full buffer ([kq.Config.MaxBufferedRecords]) is failed before
// it is buffered, so it was never sent and its outcome is known. Every promise
// error was mapped as a post-buffer one, and a caller told "unknown"
// reconciles a publication that certainly did not happen. The log is read at
// the end to show that it did not.
func TestDefinitePublishCancelledWhileWaitingForBufferRoom(t *testing.T) {
	for _, tc := range []struct {
		name    string
		publish func(context.Context, *mq.PublisherHandle) error
	}{
		{"Publish", func(ctx context.Context, p *mq.PublisherHandle) error {
			return p.Publish(ctx, &mq.Message{Body: []byte("waiting")})
		}},
		{"PublishBatch", func(ctx context.Context, p *mq.PublisherHandle) error {
			return p.PublishBatch(ctx, []*mq.Message{{Body: []byte("waiting")}, {Body: []byte("waiting too")}})
		}},
		{"PublishAsync", func(ctx context.Context, p *mq.PublisherHandle) error {
			done := make(chan error, 1)
			if err := p.PublishAsync(ctx, &mq.Message{Body: []byte("waiting")}, func(res mq.PublishResult) { done <- res.Err }); err != nil {
				return err
			}
			return <-done
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			topic := newTopic("room")
			cl := cluster(t, 1, []string{topic})
			c, b := openPair(t, cl, func(cfg *kq.Config) { cfg.MaxBufferedRecords = 1 })
			p, err := c.Publisher(t.Context(), mq.PublisherConfig{Topic: topic})
			if err != nil {
				t.Fatalf("opening a publisher: %v", err)
			}
			defer func() { _ = p.Close() }()

			// The first record fills the buffer, and its produce is held at
			// the broker so the buffer stays full.
			held, released := make(chan struct{}), make(chan struct{})
			release := sync.OnceFunc(func() { close(released) })
			t.Cleanup(release)
			var once bool
			cl.ControlKey(int16(kmsg.Produce), func(kmsg.Request) (kmsg.Response, error, bool) {
				if once {
					return nil, nil, false //nolint:nilnil // kfake's "not handled" is (nil, nil, false)
				}
				once = true
				close(held)
				cl.KeepControl()
				cl.SleepControl(func() { <-released })
				return nil, nil, false //nolint:nilnil // kfake's "not handled" is (nil, nil, false)
			})
			first := make(chan error, 1)
			go func() { first <- p.Publish(context.Background(), &mq.Message{Body: []byte("first")}) }()
			<-held

			ctx, cancel := context.WithCancel(t.Context())
			errc := make(chan error, 1)
			go func() { errc <- tc.publish(ctx, p) }()
			waitFor(t, 10*time.Second, "a publication waiting for buffer room", func() bool {
				return b.Client().BufferedProduceRecords() > 1
			})
			cancel()
			err = <-errc
			release()
			if ferr := <-first; ferr != nil {
				t.Fatalf("the first publication: %v", ferr)
			}
			if err == nil {
				t.Fatal("the publication cancelled while it waited for buffer room succeeded")
			}
			if mq.IsAmbiguous(err) || errors.Is(err, mq.ErrSettlementUnknown) {
				t.Fatalf("%v is reported as an unknown outcome; the record was never buffered, so nothing was sent", err)
			}
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("the native cause is not reachable: errors.Is(%v, context.Canceled) is false", err)
			}
			cons := consumerFrom(t, c, topic, "g-room", mq.StartEarliest())
			if got := receiveOne(t, cons, 10*time.Second); string(got.Body) != "first" {
				t.Fatalf("the log starts with %q, want the first publication", got.Body)
			}
			expectNothing(t, cons, time.Second, "a publication cancelled before it was buffered")
		})
	}
}

// TestDefiniteMoveCancelledWhileWaitingForBufferRoom is the same case for a
// nack's move to a dead-letter topic, which the settler produces itself. The
// move's context ended while franz-go waited for room in a full buffer, so
// the copy was never buffered, and the settler still reported every failure
// of a move as one that may have been sent. The dead-letter topic is read at
// the end to show that nothing reached it.
func TestDefiniteMoveCancelledWhileWaitingForBufferRoom(t *testing.T) {
	topic, dlq := newTopic("move-room"), newTopic("move-room-dead")
	cl := cluster(t, 1, []string{topic, dlq})
	c, b := openPair(t, cl, func(cfg *kq.Config) {
		cfg.MaxBufferedRecords = 1
		cfg.NackConfig.DeadLetterTopic = dlq
	})
	publish(t, c, topic, &mq.Message{Body: []byte("poison")})
	src, err := b.OpenSource(t.Context(), mq.SourceConfig{
		Topic: topic, Subscription: "g-move-room", Prefetch: 16, Start: mq.StartEarliest(),
	})
	if err != nil {
		t.Fatalf("OpenSource: %v", err)
	}
	t.Cleanup(func() { _ = src.Close() })
	d := fetchN(t, t.Context(), src, 1)[0]

	// A publication fills the buffer, and its produce is held at the
	// broker so the buffer stays full.
	held, released := make(chan struct{}), make(chan struct{})
	release := sync.OnceFunc(func() { close(released) })
	t.Cleanup(release)
	var once bool
	cl.ControlKey(int16(kmsg.Produce), func(kmsg.Request) (kmsg.Response, error, bool) {
		if once {
			return nil, nil, false //nolint:nilnil // kfake's "not handled" is (nil, nil, false)
		}
		once = true
		close(held)
		cl.KeepControl()
		cl.SleepControl(func() { <-released })
		return nil, nil, false //nolint:nilnil // kfake's "not handled" is (nil, nil, false)
	})
	first := make(chan error, 1)
	go func() { first <- c.Publish(context.Background(), topic, &mq.Message{Body: []byte("filler")}) }()
	<-held

	ctx, cancel := context.WithCancel(t.Context())
	errc := make(chan error, 1)
	go func() {
		errc <- src.Settler().Nack(ctx, &d, mq.NackOptions{Action: mq.NackActionDeadLetter})
	}()
	waitFor(t, 10*time.Second, "a move waiting for buffer room", func() bool {
		return b.Client().BufferedProduceRecords() > 1
	})
	cancel()
	err = <-errc
	release()
	if ferr := <-first; ferr != nil {
		t.Fatalf("the filling publication: %v", ferr)
	}
	if err == nil {
		t.Fatal("the nack whose move was cancelled while it waited for buffer room succeeded")
	}
	if mq.IsAmbiguous(err) || errors.Is(err, mq.ErrSettlementUnknown) {
		t.Fatalf("%v is reported as an unknown outcome; the dead-letter copy was never buffered, so nothing was sent", err)
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("the native cause is not reachable: errors.Is(%v, context.Canceled) is false", err)
	}
	if n := b.AmbiguousPublications(); n != 0 {
		t.Fatalf("%d ambiguous publications counted for a move that was never sent", n)
	}
	expectNothing(t, consumerFrom(t, c, dlq, "g-move-room-dead", mq.StartEarliest()), time.Second, "a move cancelled before it was buffered")
}

// TestDefiniteMoveWhoseTopicNeverResolved is a nack's move whose copy franz-go
// failed before placing it, because the dead-letter topic's partitions never
// loaded. The failure is definite, and it names the subscription the nack
// was for, as every other failure of a move does: the definite answer was
// built afresh and dropped it.
func TestDefiniteMoveWhoseTopicNeverResolved(t *testing.T) {
	topic, dlq := newTopic("move-unresolved"), newTopic("move-unresolved-dead")
	cl := cluster(t, 1, []string{topic, dlq})
	c, b := openPair(t, cl, func(cfg *kq.Config) { cfg.NackConfig.DeadLetterTopic = dlq })
	publish(t, c, topic, &mq.Message{Body: []byte("poison")})
	src, err := b.OpenSource(t.Context(), mq.SourceConfig{
		Topic: topic, Subscription: "g-move-unresolved", Prefetch: 16, Start: mq.StartEarliest(),
	})
	if err != nil {
		t.Fatalf("OpenSource: %v", err)
	}
	t.Cleanup(func() { _ = src.Close() })
	d := fetchN(t, t.Context(), src, 1)[0]
	// The dead-letter topic's partitions never load.
	fault := cl.Fault(kfake.Fault{Keys: []kmsg.Key{kmsg.Metadata}, Topic: dlq, Err: kerr.LeaderNotAvailable, Count: -1})
	defer fault.Remove()
	err = src.Settler().Nack(t.Context(), &d, mq.NackOptions{Action: mq.NackActionDeadLetter})
	if err == nil {
		t.Fatal("the move to a topic whose partitions never loaded succeeded")
	}
	if mq.IsAmbiguous(err) || errors.Is(err, mq.ErrSettlementUnknown) {
		t.Fatalf("%v is reported as an unknown outcome; the dead-letter copy was never placed, so nothing was sent", err)
	}
	if mq.CodeOf(err) != mq.CodeUnavailable || !mq.IsRetryable(err) {
		t.Fatalf("%v (code %v, retryable %v), want CodeUnavailable and retryable", err, mq.CodeOf(err), mq.IsRetryable(err))
	}
	var oe *mq.OpError
	if !errors.As(err, &oe) {
		t.Fatalf("%v is not an *mq.OpError", err)
	}
	if oe.Subscription != "g-move-unresolved" || oe.Operation != mq.OpNack {
		t.Fatalf("%v names operation %q and subscription %q, want %q and %q", err, oe.Operation, oe.Subscription, mq.OpNack, "g-move-unresolved")
	}
	if n := b.AmbiguousPublications(); n != 0 {
		t.Fatalf("%d ambiguous publications counted for a move that was never sent", n)
	}
}

// TestDefinitePublishOfARecordFranzGoNeverPlaced covers the other ways
// franz-go fails a record before it places it. While a topic's partitions are
// unknown the record waits outside every partition's buffer, and franz-go
// fails it with kgo.ErrRecordTimeout when the topic does not exist or the
// cluster is down, with kgo.ErrAborting when the producer aborts its buffered
// records, and with the broker's own answer when a transactional producer is
// fenced. The publish rule for those errors is ambiguous, because a placed
// record may have been written by an attempt whose answer was lost, and that
// rule won over the placement mark. A record with no partition was in no
// request, so each outcome is definite and the ambiguity counter counts only
// the records that were placed. A topic the cluster does not know is
// CodeNotFound, which the timeout rule hid, since franz-go reports it as the
// timeout's last error. A fenced producer is a conflict with the producer that
// fenced it, and only a new client can publish again.
func TestDefinitePublishOfARecordFranzGoNeverPlaced(t *testing.T) {
	for _, tc := range []struct {
		name      string
		code      mq.ErrorCode
		retryable bool
		native    error
		// counted is how many ambiguous publications the setup itself
		// makes.
		counted uint64
		// publish publishes one record whose topic's partitions never
		// load, and returns its outcome.
		publish func(t *testing.T) (*kq.Backend, error)
	}{
		{"a topic that does not exist", mq.CodeNotFound, true, kerr.UnknownTopicOrPartition, 0, func(t *testing.T) (*kq.Backend, error) {
			cl := cluster(t, 1, nil)
			c, b := openPair(t, cl)
			return b, c.Publish(t.Context(), newTopic("missing"), &mq.Message{Body: []byte("never sent")})
		}},
		{"a cluster that is down", mq.CodeUnavailable, true, kgo.ErrRecordTimeout, 0, func(t *testing.T) (*kq.Backend, error) {
			topic := newTopic("down")
			cl := cluster(t, 1, []string{topic})
			c, b := openPair(t, cl)
			p, err := c.Publisher(t.Context(), mq.PublisherConfig{Topic: topic})
			if err != nil {
				t.Fatalf("opening a publisher: %v", err)
			}
			defer func() { _ = p.Close() }()
			cl.Close()
			return b, p.Publish(t.Context(), &mq.Message{Body: []byte("never sent")})
		}},
		{"an abort of the buffered records", mq.CodeCanceled, false, kgo.ErrAborting, 0, func(t *testing.T) (*kq.Backend, error) {
			topic := newTopic("abort")
			cl := cluster(t, 1, []string{topic})
			// The delivery timeout is far off, so the abort is what ends
			// the wait.
			c, b := openPair(t, cl, func(cfg *kq.Config) { cfg.DeliveryTimeout = time.Minute })
			// The topic's partitions never load.
			fault := cl.Fault(kfake.Fault{Keys: []kmsg.Key{kmsg.Metadata}, Topic: topic, Err: kerr.LeaderNotAvailable, Count: -1})
			defer fault.Remove()
			errc := make(chan error, 1)
			go func() { errc <- c.Publish(t.Context(), topic, &mq.Message{Body: []byte("never sent")}) }()
			waitFor(t, 10*time.Second, "a record waiting for its topic's partitions", func() bool {
				return b.Client().BufferedProduceRecords() == 1
			})
			if err := b.Client().AbortBufferedRecords(t.Context()); err != nil {
				t.Fatalf("aborting the buffered records: %v", err)
			}
			return b, <-errc
		}},
		{"a fenced transactional producer", mq.CodeConflict, false, kerr.InvalidProducerEpoch, 2, func(t *testing.T) (*kq.Backend, error) {
			placed, waiting := newTopic("fenced-placed"), newTopic("fenced-waiting")
			cl := cluster(t, 1, []string{placed, waiting})
			c, b := openPair(t, cl, func(cfg *kq.Config) {
				cfg.TransactionalID = "fenced"
				cfg.DeliveryTimeout = time.Minute
			})
			if err := b.Client().BeginTransaction(); err != nil {
				t.Fatalf("BeginTransaction: %v", err)
			}
			// One record waits for partitions that never load.
			fault := cl.Fault(kfake.Fault{Keys: []kmsg.Key{kmsg.Metadata}, Topic: waiting, Err: kerr.LeaderNotAvailable, Count: -1})
			defer fault.Remove()
			errc := make(chan error, 1)
			go func() { errc <- c.Publish(t.Context(), waiting, &mq.Message{Body: []byte("never sent")}) }()
			waitFor(t, 10*time.Second, "a record waiting for its topic's partitions", func() bool {
				return b.Client().BufferedProduceRecords() == 1
			})
			// The broker answers another record as it answers a fenced
			// producer, which fails the producer id, and franz-go fails
			// every buffered record with that answer when the next record
			// needs the id. The placed records' outcomes are unknown.
			fenced := cl.Fault(kfake.Fault{Keys: []kmsg.Key{kmsg.Produce}, Topic: placed, Err: kerr.InvalidProducerEpoch, Count: -1})
			defer fenced.Remove()
			for range 2 {
				if err := c.Publish(t.Context(), placed, &mq.Message{Body: []byte("placed")}); !mq.IsAmbiguous(err) {
					t.Fatalf("a placed record = %v, want an ambiguous outcome", err)
				}
			}
			select {
			case err := <-errc:
				return b, err
			case <-time.After(15 * time.Second):
				t.Fatal("the waiting record was not failed with the fenced producer")
			}
			return b, nil
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b, err := tc.publish(t)
			if err == nil {
				t.Fatal("the publication succeeded")
			}
			if mq.IsAmbiguous(err) || errors.Is(err, mq.ErrSettlementUnknown) {
				t.Fatalf("%v is reported as an unknown outcome; franz-go never placed the record, so nothing was sent", err)
			}
			if got := mq.CodeOf(err); got != tc.code || mq.IsRetryable(err) != tc.retryable {
				t.Fatalf("%v mapped to %v, retryable %v, want %v, retryable %v", err, got, mq.IsRetryable(err), tc.code, tc.retryable)
			}
			if !errors.Is(err, tc.native) {
				t.Fatalf("the native cause is not reachable: errors.Is(%v, %v) is false", err, tc.native)
			}
			if n := b.AmbiguousPublications(); n != tc.counted {
				t.Fatalf("%d ambiguous publications counted, want %d: the record that was never sent counted", n, tc.counted)
			}
		})
	}
}

// TestAmbiguousPublishCountsUp: a deployment that cannot resolve an ambiguous
// publication on its own at least has to be able to see that one happened.
func TestAmbiguousPublishCountsUp(t *testing.T) {
	topic := newTopic("ambig-count")
	cl := cluster(t, 1, []string{topic})
	c, b := openPair(t, cl)
	if b.AmbiguousPublications() != 0 {
		t.Fatalf("the counter starts at %d", b.AmbiguousPublications())
	}
	fault := cl.Fault(kfake.Fault{Keys: []kmsg.Key{kmsg.Produce}, Topic: topic, Err: kerr.RequestTimedOut, Count: -1})
	defer fault.Remove()
	if err := c.Publish(t.Context(), topic, &mq.Message{Body: []byte("x")}); err == nil {
		t.Fatal("the publication succeeded through the fault")
	}
	if b.AmbiguousPublications() == 0 {
		t.Fatal("an ambiguous publication was not counted, so a deployment cannot tell it happened")
	}
}

// TestAmbiguousBatchIsIndexAligned: an ambiguous batch reports its outcome per
// message, because a caller reconciling a batch has to know WHICH messages it
// cannot account for.
func TestAmbiguousBatchIsIndexAligned(t *testing.T) {
	topic := newTopic("ambig-batch")
	cl := cluster(t, 1, []string{topic})
	c := openOn(t, cl)
	p, err := c.Publisher(t.Context(), mq.PublisherConfig{Topic: topic})
	if err != nil {
		t.Fatalf("opening a publisher: %v", err)
	}
	defer func() { _ = p.Close() }()
	fault := cl.Fault(kfake.Fault{Keys: []kmsg.Key{kmsg.Produce}, Topic: topic, Err: kerr.RequestTimedOut, Count: -1})
	defer fault.Remove()
	ms := []*mq.Message{{Body: []byte("a")}, {Body: []byte("b")}, {Body: []byte("c")}}
	err = p.PublishBatch(t.Context(), ms)
	if err == nil {
		t.Fatal("the batch succeeded through the fault")
	}
	var batch *mq.BatchError
	if errors.As(err, &batch) {
		if len(batch.Errs) != len(ms) {
			t.Fatalf("the batch error carries %d outcomes for %d messages", len(batch.Errs), len(ms))
		}
		// EVERY message was refused by the fault, so every outcome has to
		// carry an error. A nil here is the misalignment this test exists
		// for: a driver that collected outcomes in completion order would
		// leave the later indices nil and a caller would believe those
		// messages were accepted.
		for i, e := range batch.Errs {
			if e == nil {
				t.Fatalf("message %d has no outcome, and the fault refused every produce: a caller would believe it was accepted", i)
			}
			assertUnknown(t, e)
		}
		return
	}
	// Every message failed identically, which the SPI reports as one error.
	assertUnknown(t, err)
}

// TestAmbiguousRepublicationNeverAcks is with the ambiguous half rather
// than the failed half: a dead-letter publication whose outcome is UNKNOWN
// must not acknowledge the original either, because the record may be in two
// places or in none and only one of those is safe to forget.
func TestAmbiguousRepublicationNeverAcks(t *testing.T) {
	topic, dlq := newTopic("ambig-dl"), newTopic("ambig-dl-dead")
	cl := cluster(t, 1, []string{topic, dlq})
	c := openOn(t, cl, func(cfg *kq.Config) { cfg.NackConfig.DeadLetterTopic = dlq })
	cons := consumerOn(t, c, topic, "g-ambig-dl")
	publish(t, c, topic, &mq.Message{Body: []byte("poison")})
	d := receiveOne(t, cons, 15*time.Second)

	fault := cl.Fault(kfake.Fault{Keys: []kmsg.Key{kmsg.Produce}, Topic: dlq, Err: kerr.RequestTimedOut, Count: -1})
	err := d.Reject()
	fault.Remove()
	if err == nil {
		t.Fatal("Reject succeeded while the dead-letter publication's outcome was unknown: the specification forbids acknowledging the original")
	}
	if !mq.IsAmbiguous(err) {
		t.Fatalf("the refused Reject = %v; a REQUEST_TIMED_OUT on the move is ambiguous, and a caller that retried it needs to know the record may already be there", err)
	}
	if err := cons.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	again := consumerOn(t, c, topic, "g-ambig-dl")
	back := receiveOne(t, again, 15*time.Second)
	if string(back.Body) != "poison" {
		t.Fatalf("the reopened subscription holds %q, want the record whose move was ambiguous", back.Body)
	}
	if err := back.Ack(); err != nil {
		t.Fatalf("Ack: %v", err)
	}
}

// assertUnknown holds one error to the answer for an outcome nobody knows.
func assertUnknown(t *testing.T, err error) {
	t.Helper()
	if !mq.IsAmbiguous(err) {
		t.Fatalf("%v is not reported as ambiguous: a caller that retries it may publish twice while believing it published once ", err)
	}
	if !errors.Is(err, mq.ErrSettlementUnknown) {
		t.Fatalf("%v does not match ErrSettlementUnknown (code %v): the specification names that sentinel for an outcome nobody knows, and a caller who reaches for it has to find it ",
			err, mq.CodeOf(err))
	}
	// The driver's judgement about a transient-but-unknown outcome, logged
	// rather than asserted because it is a judgement and not a defect. It is
	// read off the *OpError: since every error reaching
	// here is ambiguous -- the check above insists on it -- so mq.IsRetryable
	// is false for all of them and `if !mq.IsRetryable(err)` logged
	// unconditionally, and logged something that was false whenever the
	// driver HAD marked the rule retryable (kerr.RequestTimedOut does).
	var oe *mq.OpError
	if errors.As(err, &oe) && !oe.Retryable {
		t.Logf("%v is not marked retryable by the driver, which is a judgement rather than a defect", err)
	}
}
