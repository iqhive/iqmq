package kafkatx_test

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit"
	kq "github.com/iqhive/iqmq/drivers/kafka"
	"github.com/iqhive/iqmq/x/kafkatx"
)

// The transaction helper's tests run against kfake, which speaks the real
// transaction protocol: InitProducerId, AddPartitionsToTxn, the
// WriteTxnMarkers, and the read-committed isolation a consumer needs to see
// the result.

// cluster starts a kfake cluster with the given topics.
func cluster(tb testing.TB, topics ...string) *kfake.Cluster {
	tb.Helper()
	c, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(1, topics...))
	if err != nil {
		tb.Fatalf("kfake cluster: %v", err)
	}
	tb.Cleanup(c.Close)
	return c
}

// open opens a transactional client and its non-transactional reader.
func open(tb testing.TB, c *kfake.Cluster, topic, txnID string) (*mq.Client, *mq.Client) {
	tb.Helper()
	base := kq.Config{
		Brokers:        c.ListenAddrs(),
		Linger:         -1,
		CommitInterval: 50 * time.Millisecond,
		RequestTimeout: 10 * time.Second,
	}
	txCfg := base
	txCfg.TransactionalID = txnID
	producer, err := kq.Open(tb.Context(), txCfg)
	if err != nil {
		tb.Fatalf("opening the transactional client: %v", err)
	}
	tb.Cleanup(func() { _ = producer.Close() })
	readCfg := base
	readCfg.ReadCommitted = true
	reader, err := kq.Open(tb.Context(), readCfg)
	if err != nil {
		tb.Fatalf("opening the reader: %v", err)
	}
	tb.Cleanup(func() { _ = reader.Close() })
	_ = topic
	return producer, reader
}

// TestCommitMakesRecordsVisible: a committed transaction's records reach a
// read-committed consumer, and they reach it only after the commit.
func TestCommitMakesRecordsVisible(t *testing.T) {
	topic := "tx-commit"
	c := cluster(t, topic)
	producer, reader := open(t, c, topic, "tx-1")
	cons, err := reader.Consumer(t.Context(), mq.ConsumerConfig{
		Topic: topic, Subscription: "g-commit", Start: mq.StartEarliest(),
	})
	if err != nil {
		t.Fatalf("opening a consumer: %v", err)
	}
	defer func() { _ = cons.Close() }()

	tx, err := kafkatx.New(producer)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := tx.Begin(); err != nil {
		t.Fatalf("Begin: %v", err)
	}
	if err := producer.Publish(t.Context(), topic, &mq.Message{Body: []byte("inside")}); err != nil {
		t.Fatalf("Publish inside the transaction: %v", err)
	}
	// Before the commit, a read-committed consumer sees nothing.
	if got := receive(t, cons, 700*time.Millisecond); got != "" {
		t.Fatalf("a read-committed consumer saw %q before the commit", got)
	}
	if err := tx.Commit(t.Context()); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if !tx.Ended() {
		t.Fatal("the transaction does not report itself ended")
	}
	if got := receive(t, cons, 15*time.Second); got != "inside" {
		t.Fatalf("after the commit the consumer saw %q", got)
	}
}

// TestAbortDiscardsRecords: an aborted transaction's records never reach a
// read-committed consumer, which is the whole point of the isolation level.
func TestAbortDiscardsRecords(t *testing.T) {
	topic := "tx-abort"
	c := cluster(t, topic)
	producer, reader := open(t, c, topic, "tx-2")
	cons, err := reader.Consumer(t.Context(), mq.ConsumerConfig{
		Topic: topic, Subscription: "g-abort", Start: mq.StartEarliest(),
	})
	if err != nil {
		t.Fatalf("opening a consumer: %v", err)
	}
	defer func() { _ = cons.Close() }()

	tx, err := kafkatx.New(producer)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := tx.Begin(); err != nil {
		t.Fatalf("Begin: %v", err)
	}
	if err := producer.Publish(t.Context(), topic, &mq.Message{Body: []byte("discarded")}); err != nil {
		t.Fatalf("Publish inside the transaction: %v", err)
	}
	if err := tx.Abort(t.Context()); err != nil {
		t.Fatalf("Abort: %v", err)
	}
	if got := receive(t, cons, 1500*time.Millisecond); got != "" {
		t.Fatalf("a read-committed consumer saw %q from an aborted transaction", got)
	}
}

// TestDoEndsExactlyOnce is the form a caller should reach for, and this is
// what it buys: the early return, the error and the panic all end the
// transaction, so the next Begin is not refused by a producer that still has
// one open.
func TestDoEndsExactlyOnce(t *testing.T) {
	topic := "tx-do"
	c := cluster(t, topic)
	producer, _ := open(t, c, topic, "tx-3")
	tx, err := kafkatx.New(producer)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	boom := errors.New("boom")
	if err := tx.Do(t.Context(), func(context.Context) error { return boom }); !errors.Is(err, boom) {
		t.Fatalf("Do = %v, want the function's own error", err)
	}
	if !tx.Ended() {
		t.Fatal("Do left the transaction open after its function failed")
	}
	// The same handle is spent: a transaction is begun once and ended once.
	if err := tx.Do(t.Context(), func(context.Context) error { return nil }); !errors.Is(err, kafkatx.ErrAlreadyEnded) {
		t.Fatalf("a second Do on a spent handle = %v, want ErrAlreadyEnded", err)
	}

	// A panic aborts and is re-raised, so it reaches the caller's own
	// recovery rather than being turned into an error.
	fresh, err := kafkatx.New(producer)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	func() {
		defer func() {
			if r := recover(); r == nil {
				t.Error("the panic was swallowed")
			}
		}()
		_ = fresh.Do(t.Context(), func(context.Context) error { panic("handler") })
	}()
	if !fresh.Ended() {
		t.Fatal("a panicking function left the transaction open, so the producer can never begin another")
	}
}

// TestDoAbortsACommitWhoseFlushRanOut: a commit whose flush runs out of ctx
// never sends the end, so the transaction is still open; Do aborts it, on a
// context the expired one does not reach, and the producer can begin again.
func TestDoAbortsACommitWhoseFlushRanOut(t *testing.T) {
	topic := "tx-flushout"
	c := cluster(t, topic)
	producer, _ := open(t, c, topic, "tx-8")
	cl, _ := producer.Native().(*kgo.Client)
	// A retryable refusal keeps the record buffered for as long as it lasts.
	fault := c.Fault(kfake.Fault{
		Keys:  []kmsg.Key{kmsg.Produce},
		Topic: topic,
		Err:   kerr.NotEnoughReplicas,
		Count: -1,
	})
	tx, err := kafkatx.New(producer)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 500*time.Millisecond)
	defer cancel()
	err = tx.Do(ctx, func(context.Context) error {
		cl.Produce(context.Background(), &kgo.Record{Topic: topic, Value: []byte("stuck")}, nil)
		return nil
	})
	if err == nil {
		t.Fatal("Do committed a transaction whose record never left the buffer")
	}
	fault.Remove()
	if !tx.Ended() {
		t.Fatalf("Do returned %v and left the transaction open", err)
	}
	next, err := kafkatx.New(producer)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := next.Do(t.Context(), func(context.Context) error { return nil }); err != nil {
		t.Fatalf("the next transaction = %v; the producer is still in the last one", err)
	}
}

// TestStateTransitions: begun once, ended once, and every other order
// refused rather than silently accepted.
func TestStateTransitions(t *testing.T) {
	topic := "tx-state"
	c := cluster(t, topic)
	producer, _ := open(t, c, topic, "tx-4")
	tx, err := kafkatx.New(producer)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := tx.Commit(t.Context()); !errors.Is(err, kafkatx.ErrNotBegun) {
		t.Fatalf("Commit before Begin = %v, want ErrNotBegun", err)
	}
	if err := tx.Abort(t.Context()); !errors.Is(err, kafkatx.ErrNotBegun) {
		t.Fatalf("Abort before Begin = %v, want ErrNotBegun", err)
	}
	if err := tx.Begin(); err != nil {
		t.Fatalf("Begin: %v", err)
	}
	if err := tx.Begin(); !errors.Is(err, kafkatx.ErrAlreadyBegun) {
		t.Fatalf("a second Begin = %v, want ErrAlreadyBegun", err)
	}
	if err := tx.Abort(t.Context()); err != nil {
		t.Fatalf("Abort: %v", err)
	}
	if err := tx.Commit(t.Context()); !errors.Is(err, kafkatx.ErrAlreadyEnded) {
		t.Fatalf("Commit after Abort = %v, want ErrAlreadyEnded", err)
	}
	if err := tx.Abort(t.Context()); !errors.Is(err, kafkatx.ErrAlreadyEnded) {
		t.Fatalf("a second Abort = %v, want ErrAlreadyEnded", err)
	}
}

// TestCommitFlushesBufferedRecords: a record produced asynchronously and
// still buffered when Commit is called is part of the commit, because
// EndTransaction does not flush.
func TestCommitFlushesBufferedRecords(t *testing.T) {
	topic := "tx-buffered"
	c := cluster(t, topic)
	producer, reader := open(t, c, topic, "tx-6")
	cons, err := reader.Consumer(t.Context(), mq.ConsumerConfig{
		Topic: topic, Subscription: "g-buffered", Start: mq.StartEarliest(),
	})
	if err != nil {
		t.Fatalf("opening a consumer: %v", err)
	}
	defer func() { _ = cons.Close() }()

	tx, err := kafkatx.New(producer)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := tx.Begin(); err != nil {
		t.Fatalf("Begin: %v", err)
	}
	cl, _ := producer.Native().(*kgo.Client)
	cl.Produce(t.Context(), &kgo.Record{Topic: topic, Value: []byte("buffered")}, nil)
	if err := tx.Commit(t.Context()); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if got := receive(t, cons, 15*time.Second); got != "buffered" {
		t.Fatalf("after the commit the consumer saw %q, want the buffered record", got)
	}
}

// TestAbortAfterAnUnconfirmedCommit: a commit whose outcome is unknown leaves
// the transaction open, so the Abort franz-go prescribes for it is accepted
// rather than refused as a second ending.
func TestAbortAfterAnUnconfirmedCommit(t *testing.T) {
	topic := "tx-unconfirmed"
	c := cluster(t, topic)
	producer, _ := open(t, c, topic, "tx-7")
	c.ControlKey(kmsg.EndTxn.Int16(), func(req kmsg.Request) (kmsg.Response, error, bool) {
		resp, _ := req.ResponseKind().(*kmsg.EndTxnResponse)
		resp.ErrorCode = kerr.UnknownServerError.Code
		return resp, nil, true
	})
	tx, err := kafkatx.New(producer)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := tx.Begin(); err != nil {
		t.Fatalf("Begin: %v", err)
	}
	if err := producer.Publish(t.Context(), topic, &mq.Message{Body: []byte("inside")}); err != nil {
		t.Fatalf("Publish inside the transaction: %v", err)
	}
	if err := tx.Commit(t.Context()); mq.CodeOf(err) != mq.CodeSettlementUnknown {
		t.Fatalf("Commit = %v, want CodeSettlementUnknown", err)
	}
	if tx.Ended() {
		t.Fatal("a commit nobody confirmed marked the transaction ended")
	}
	if err := tx.Abort(t.Context()); err != nil {
		t.Fatalf("Abort after an unconfirmed commit = %v, want nil", err)
	}
	if !tx.Ended() {
		t.Fatal("the abort did not end the transaction")
	}
}

// TestNewRefusesTheWrongClient: a client that is not transactional fails
// EVERY publication once a transaction is begun, and the failure names the
// producer rather than the configuration — so the refusal happens here.
func TestNewRefusesTheWrongClient(t *testing.T) {
	if _, err := kafkatx.New(nil); !errors.Is(err, kafkatx.ErrNotKafka) {
		t.Fatalf("New(nil) = %v, want ErrNotKafka", err)
	}
	if _, err := kafkatx.NewWithClient(nil); !errors.Is(err, kafkatx.ErrNotKafka) {
		t.Fatalf("NewWithClient(nil) = %v, want ErrNotKafka", err)
	}
	topic := "tx-plain"
	c := cluster(t, topic)
	plain, err := kq.Open(t.Context(), kq.Config{Brokers: c.ListenAddrs(), Linger: -1})
	if err != nil {
		t.Fatalf("opening a plain client: %v", err)
	}
	defer func() { _ = plain.Close() }()
	if _, err := kafkatx.New(plain); !errors.Is(err, kafkatx.ErrNotTransactional) {
		t.Fatalf("New on a client with no transactional id = %v, want ErrNotTransactional", err)
	}
	// And a client that is not this driver's at all.
	if _, err := kafkatx.New(&mq.Client{}); !errors.Is(err, kafkatx.ErrNotKafka) {
		t.Fatalf("New on a foreign client = %v, want ErrNotKafka", err)
	}
}

// TestOffsetsOf is the plus-one every Kafka consumer has to get right: a
// committed offset is the offset of the NEXT record to consume, and
// committing the record's own offset reprocesses it on every restart, for
// ever.
func TestOffsetsOf(t *testing.T) {
	if got := kafkatx.OffsetsOf(); got != nil {
		t.Fatalf("OffsetsOf() = %v, want nil", got)
	}
	got := kafkatx.OffsetsOf(
		mq.DeliveryInfo{Topic: "orders", Partition: 0, HasPartition: true, Offset: 7, HasOffset: true},
		mq.DeliveryInfo{Topic: "orders", Partition: 0, HasPartition: true, Offset: 3, HasOffset: true},
		mq.DeliveryInfo{Topic: "orders", Partition: 1, HasPartition: true, Offset: 0, HasOffset: true},
		mq.DeliveryInfo{Topic: "other", Source: "acme.other", Partition: 2, HasPartition: true, Offset: 9, HasOffset: true},
		// Nothing to commit: the specification makes an unset offset "the broker did
		// not say", and committing zero for it would rewind the
		// partition to the start.
		mq.DeliveryInfo{Topic: "orders", Partition: 5, HasPartition: true},
		mq.DeliveryInfo{Topic: "", Partition: 0, HasPartition: true, Offset: 1, HasOffset: true},
	)
	if got["orders"][0] != 8 {
		t.Fatalf("the highest offset of orders/0 committed as %d, want 8 (7 plus one)", got["orders"][0])
	}
	if got["orders"][1] != 1 {
		t.Fatalf("orders/1 committed as %d, want 1", got["orders"][1])
	}
	if _, ok := got["orders"][5]; ok {
		t.Fatal("a delivery with no offset produced a committable offset")
	}
	// The resolved broker-side topic wins, because that is the topic the
	// group's offsets are keyed by (Source).
	if got["acme.other"][2] != 10 {
		t.Fatalf("the Source topic was not used: %v", got)
	}
	if _, ok := got[""]; ok {
		t.Fatal("a delivery with no topic produced a committable offset")
	}
}

// TestSessionWrapsTheReadProcessWriteLoop: the session's committed flag is
// the information a read-process-write loop needs and the error is not, so
// the helper returns both.
func TestSessionWrapsTheReadProcessWriteLoop(t *testing.T) {
	in, out := "tx-in", "tx-out"
	c := cluster(t, in, out)
	sess, err := kgo.NewGroupTransactSession(
		kgo.SeedBrokers(c.ListenAddrs()...),
		kgo.TransactionalID("tx-session"),
		kgo.ConsumerGroup("g-session"),
		kgo.ConsumeTopics(in),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.FetchIsolationLevel(kgo.ReadCommitted()),
	)
	if err != nil {
		t.Fatalf("NewGroupTransactSession: %v", err)
	}
	defer sess.Close()

	if _, err := kafkatx.WrapSession(nil); !errors.Is(err, kafkatx.ErrNotKafka) {
		t.Fatalf("WrapSession(nil) = %v, want ErrNotKafka", err)
	}
	s, err := kafkatx.WrapSession(sess)
	if err != nil {
		t.Fatalf("WrapSession: %v", err)
	}
	if s.Client() == nil {
		t.Fatal("Client returned nil")
	}

	// Put one record in, with a plain producer.
	producer, err := kq.Open(t.Context(), kq.Config{Brokers: c.ListenAddrs(), Linger: -1})
	if err != nil {
		t.Fatalf("opening a producer: %v", err)
	}
	defer func() { _ = producer.Close() }()
	if err := producer.Publish(t.Context(), in, &mq.Message{Body: []byte("source")}); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	committed, err := s.Do(ctx, func(ctx context.Context) error {
		fetches := sess.PollFetches(ctx)
		if ferr := fetches.Err0(); ferr != nil {
			return ferr
		}
		n := 0
		fetches.EachRecord(func(r *kgo.Record) {
			n++
			sess.Produce(ctx, &kgo.Record{Topic: out, Value: r.Value}, nil)
		})
		if n == 0 {
			return errors.New("nothing was fetched")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	if !committed {
		t.Fatal("the session did not commit; a caller that ignored this flag would skip input it never produced from")
	}

	// An aborting function commits nothing.
	committed, err = s.Do(ctx, func(context.Context) error { return errors.New("no") })
	if err == nil || committed {
		t.Fatalf("an aborting function reported committed=%v err=%v", committed, err)
	}
	if !strings.Contains(err.Error(), "no") {
		t.Fatalf("the abort hid the reason: %v", err)
	}
}

// TestSessionDoAbortsWhenItsContextEnds: franz-go's End returns a done ctx
// before it sends anything, so a Session.Do whose ctx ended left the
// transaction open, and every later Do failed at Begin for the life of the
// process. Do aborts on a context the cancellation does not reach, and a
// commit it never sent is a definite failure rather than an unknown one.
func TestSessionDoAbortsWhenItsContextEnds(t *testing.T) {
	for _, tc := range []struct {
		name string
		// ends is where ctx ends: inside the function, which then fails, or
		// during the commit's flush, which a retryable refusal holds open.
		ends string
	}{
		{name: "the function fails after ctx ended", ends: "fn"},
		{name: "ctx ends during the commit's flush", ends: "commit"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			in, out := "tx-cancel-in-"+tc.ends, "tx-cancel-out-"+tc.ends
			c := cluster(t, in, out)
			sess := session(t, c, "tx-cancel-"+tc.ends, "g-cancel-"+tc.ends, in)
			s, err := kafkatx.WrapSession(sess)
			if err != nil {
				t.Fatalf("WrapSession: %v", err)
			}
			fault := c.Fault(kfake.Fault{
				Keys:  []kmsg.Key{kmsg.Produce},
				Topic: out,
				Err:   kerr.NotEnoughReplicas,
				Count: -1,
			})
			ctx, cancel := context.WithTimeout(t.Context(), 500*time.Millisecond)
			defer cancel()
			committed, err := s.Do(ctx, func(ctx context.Context) error {
				sess.Produce(context.Background(), &kgo.Record{Topic: out, Value: []byte("stuck")}, nil)
				if tc.ends == "commit" {
					return nil
				}
				<-ctx.Done()
				return ctx.Err()
			})
			if err == nil || committed {
				t.Fatalf("Do = committed %v, %v; want a failure", committed, err)
			}
			if tc.ends == "commit" && mq.IsAmbiguous(err) {
				t.Fatalf("a commit that was never sent = %v, want a definite failure", err)
			}
			fault.Remove()

			next, cancelNext := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancelNext()
			if committed, err := s.Do(next, func(context.Context) error { return nil }); err != nil {
				t.Fatalf("the next Do = committed %v, %v; the last one left the transaction open", committed, err)
			}
		})
	}
}

// TestACommitNotAttemptedIsDefinite: franz-go refuses a commit whose producer
// id has failed with OPERATION_NOT_ATTEMPTED before it sends anything, so the
// transaction is still open and certainly not committed. Reporting it as
// unknown told the caller never to retry work that was not done.
func TestACommitNotAttemptedIsDefinite(t *testing.T) {
	topic := "tx-notattempted"
	c := cluster(t, topic)
	// No ledger on this client, so the commit reaches franz-go's own check.
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(c.ListenAddrs()...),
		kgo.TransactionalID("tx-9"),
	)
	if err != nil {
		t.Fatalf("kgo.NewClient: %v", err)
	}
	defer cl.Close()
	tx, err := kafkatx.NewWithClient(cl)
	if err != nil {
		t.Fatalf("NewWithClient: %v", err)
	}
	if err := tx.Begin(); err != nil {
		t.Fatalf("Begin: %v", err)
	}
	if err := cl.ProduceSync(t.Context(), &kgo.Record{Topic: topic, Value: []byte("kept")}).FirstErr(); err != nil {
		t.Fatalf("producing inside the transaction: %v", err)
	}
	// A fenced epoch fails the producer id, which is what makes franz-go
	// refuse the commit rather than send it.
	c.Fault(kfake.Fault{Keys: []kmsg.Key{kmsg.Produce}, Topic: topic, Err: kerr.InvalidProducerEpoch, Count: 1})
	cl.Produce(t.Context(), &kgo.Record{Topic: topic, Value: []byte("fenced")}, nil)

	err = tx.Commit(t.Context())
	if !errors.Is(err, kerr.OperationNotAttempted) {
		t.Fatalf("Commit = %v, want franz-go's OPERATION_NOT_ATTEMPTED", err)
	}
	if mq.IsAmbiguous(err) {
		t.Fatalf("a commit franz-go never attempted = %v, want a definite failure", err)
	}
	if tx.Ended() {
		t.Fatal("a commit that was never sent marked the transaction ended")
	}
	if err := tx.Abort(t.Context()); err != nil {
		t.Fatalf("Abort after a commit that was never sent = %v, want nil", err)
	}
}

// TestACommitWhoseContextEndedIsDefinite: a ctx that has ended by the time
// the records are flushed is not handed to EndTransaction, because whether
// franz-go wrote the request before it noticed would be unknowable. Not
// sending it keeps the outcome definite and the transaction open for the
// abort.
func TestACommitWhoseContextEndedIsDefinite(t *testing.T) {
	topic := "tx-ctxdone"
	c := cluster(t, topic)
	producer, _ := open(t, c, topic, "tx-10")
	tx, err := kafkatx.New(producer)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := tx.Begin(); err != nil {
		t.Fatalf("Begin: %v", err)
	}
	if err := producer.Publish(t.Context(), topic, &mq.Message{Body: []byte("inside")}); err != nil {
		t.Fatalf("Publish inside the transaction: %v", err)
	}
	done, cancel := context.WithCancel(t.Context())
	cancel()
	if err := tx.Commit(done); err == nil || mq.IsAmbiguous(err) {
		t.Fatalf("Commit on an ended ctx = %v (ambiguous %v), want a definite failure", err, mq.IsAmbiguous(err))
	}
	if tx.Ended() {
		t.Fatal("a commit that was never sent marked the transaction ended")
	}
	if err := tx.Abort(t.Context()); err != nil {
		t.Fatalf("Abort after a commit that was never sent = %v, want nil", err)
	}
}

// TestDoAbortsATransactionWithARefusedRecord: franz-go keeps the producer id
// valid after a broker refuses a record outright, and commits the records it
// accepted. An asynchronous publication's refusal arrives during the commit's
// own flush, too late for the function to see it, so the helper counts the
// transaction's records and aborts instead of committing a part of them.
func TestDoAbortsATransactionWithARefusedRecord(t *testing.T) {
	good, bad := "tx-good", "tx-bad"
	c := cluster(t, good, bad)
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(c.ListenAddrs()...),
		kgo.TransactionalID("tx-11"),
		kafkatx.TrackRecords(),
	)
	if err != nil {
		t.Fatalf("kgo.NewClient: %v", err)
	}
	producer, err := kq.OpenWithClient(t.Context(), cl, driverkit.ClientOwned, kq.Config{Brokers: c.ListenAddrs()})
	if err != nil {
		t.Fatalf("OpenWithClient: %v", err)
	}
	t.Cleanup(func() { _ = producer.Close() })
	refusedRecord(t, c, producer, good, bad)
}

// TestTheDriversProducerTracksItsRecords is the same guarantee for the client
// most callers have: the Kafka driver puts the record ledger on a
// transactional producer of its own.
func TestTheDriversProducerTracksItsRecords(t *testing.T) {
	good, bad := "tx-driver-good", "tx-driver-bad"
	c := cluster(t, good, bad)
	producer, _ := open(t, c, good, "tx-12")
	if cl, _ := producer.Native().(*kgo.Client); !kafkatx.TracksRecordsForTest(cl) {
		// CI tests this module against the published driver, and v1.1.1
		// predates the ledger; the version bump of the next release turns
		// the test on. The driver's own tests pin that it is there.
		t.Skip("this build's Kafka driver puts no record ledger on a transactional producer")
	}
	refusedRecord(t, c, producer, good, bad)
}

// refusedRecord runs a Do that publishes one record the broker accepts and
// one it refuses, and checks that neither became visible.
func refusedRecord(t *testing.T, c *kfake.Cluster, producer *mq.Client, good, bad string) {
	t.Helper()
	readCfg := kq.Config{Brokers: c.ListenAddrs(), Linger: -1, ReadCommitted: true}
	reader, err := kq.Open(t.Context(), readCfg)
	if err != nil {
		t.Fatalf("opening the reader: %v", err)
	}
	t.Cleanup(func() { _ = reader.Close() })
	cons, err := reader.Consumer(t.Context(), mq.ConsumerConfig{
		Topic: good, Subscription: "g-" + good, Start: mq.StartEarliest(),
	})
	if err != nil {
		t.Fatalf("opening a consumer: %v", err)
	}
	defer func() { _ = cons.Close() }()
	c.Fault(kfake.Fault{Keys: []kmsg.Key{kmsg.Produce}, Topic: bad, Err: kerr.InvalidRecord, Count: -1})
	pubGood, err := producer.Publisher(t.Context(), mq.PublisherConfig{Topic: good})
	if err != nil {
		t.Fatalf("Publisher: %v", err)
	}
	pubBad, err := producer.Publisher(t.Context(), mq.PublisherConfig{Topic: bad})
	if err != nil {
		t.Fatalf("Publisher: %v", err)
	}
	tx, err := kafkatx.New(producer)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	err = tx.Do(t.Context(), func(ctx context.Context) error {
		for _, p := range []*mq.PublisherHandle{pubGood, pubBad} {
			if err := p.PublishAsync(ctx, &mq.Message{Body: []byte(p.Topic())}, func(mq.PublishResult) {}); err != nil {
				return err
			}
		}
		return nil
	})
	if !errors.Is(err, kerr.InvalidRecord) {
		t.Fatalf("Do = %v, want the refusal of the record to %q", err, bad)
	}
	if mq.IsAmbiguous(err) {
		t.Fatalf("Do = %v; an aborted transaction is a definite outcome", err)
	}
	// The code the publication itself reported, so the caller does not
	// retry into the same refusal.
	if mq.CodeOf(err) != mq.CodeCorruptMessage || mq.IsRetryable(err) {
		t.Fatalf("Do = %v (code %v, retryable %v), want CorruptMessage and not retryable", err, mq.CodeOf(err), mq.IsRetryable(err))
	}
	if !tx.Ended() {
		t.Fatal("Do left the transaction open")
	}
	if got := receive(t, cons, 1500*time.Millisecond); got != "" {
		t.Fatalf("a read-committed consumer saw %q from a transaction whose record to %q was refused", got, bad)
	}
	next, err := kafkatx.New(producer)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := next.Do(t.Context(), func(ctx context.Context) error {
		return pubGood.Publish(ctx, &mq.Message{Body: []byte("next")})
	}); err != nil {
		t.Fatalf("the next transaction = %v", err)
	}
	if got := receive(t, cons, 15*time.Second); got != "next" {
		t.Fatalf("after the next commit the consumer saw %q", got)
	}
}

// TestSessionAbortsATransactionWithARefusedRecord is the refused record on
// the read-process-write path: the session's End would commit the records
// the broker accepted, so Do checks the ledger before it ends the
// transaction.
func TestSessionAbortsATransactionWithARefusedRecord(t *testing.T) {
	in, good, bad := "tx-refused-in", "tx-refused-good", "tx-refused-bad"
	c := cluster(t, in, good, bad)
	sess := session(t, c, "tx-refused", "g-refused", in, kafkatx.TrackRecords())
	s, err := kafkatx.WrapSession(sess)
	if err != nil {
		t.Fatalf("WrapSession: %v", err)
	}
	c.Fault(kfake.Fault{Keys: []kmsg.Key{kmsg.Produce}, Topic: bad, Err: kerr.InvalidRecord, Count: -1})
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	committed, err := s.Do(ctx, func(ctx context.Context) error {
		sess.Produce(ctx, &kgo.Record{Topic: good, Value: []byte("good")}, nil)
		sess.Produce(ctx, &kgo.Record{Topic: bad, Value: []byte("bad")}, nil)
		return nil
	})
	if committed || !errors.Is(err, kerr.InvalidRecord) {
		t.Fatalf("Do = committed %v, %v; want the refusal of the record to %q", committed, err, bad)
	}
	if mq.IsAmbiguous(err) || mq.CodeOf(err) != mq.CodeCorruptMessage {
		t.Fatalf("Do = %v (ambiguous %v, code %v); an aborted transaction is a definite outcome with the record's own code",
			err, mq.IsAmbiguous(err), mq.CodeOf(err))
	}
	reader, err := kq.Open(t.Context(), kq.Config{Brokers: c.ListenAddrs(), Linger: -1, ReadCommitted: true})
	if err != nil {
		t.Fatalf("opening the reader: %v", err)
	}
	defer func() { _ = reader.Close() }()
	cons, err := reader.Consumer(t.Context(), mq.ConsumerConfig{
		Topic: good, Subscription: "g-refused-reader", Start: mq.StartEarliest(),
	})
	if err != nil {
		t.Fatalf("opening a consumer: %v", err)
	}
	defer func() { _ = cons.Close() }()
	if got := receive(t, cons, 1500*time.Millisecond); got != "" {
		t.Fatalf("a read-committed consumer saw %q from a transaction whose record to %q was refused", got, bad)
	}
}

// session builds a group transact session that consumes in.
func session(tb testing.TB, c *kfake.Cluster, txnID, group, in string, opts ...kgo.Opt) *kgo.GroupTransactSession {
	tb.Helper()
	sess, err := kgo.NewGroupTransactSession(append([]kgo.Opt{
		kgo.SeedBrokers(c.ListenAddrs()...),
		kgo.TransactionalID(txnID),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(in),
		kgo.FetchIsolationLevel(kgo.ReadCommitted()),
	}, opts...)...)
	if err != nil {
		tb.Fatalf("NewGroupTransactSession: %v", err)
	}
	tb.Cleanup(sess.Close)
	return sess
}

// TestAnEarlierRecordsFailureDoesNotRefuseTheNextCommit: franz-go fails a
// record it refused before buffering on its promise goroutine, after Produce
// has returned, and neither an abort nor the gap between transactions waits
// for that. A transaction that took its mark before the failure was counted
// blamed it on its own records and refused a good commit, with a code a
// runner does not retry. The promise goroutine is held up here, as a slow
// callback holds it, so the failure lands after the next transaction began
// unless beginning waits for it.
func TestAnEarlierRecordsFailureDoesNotRefuseTheNextCommit(t *testing.T) {
	out, in := "tx-stale", "tx-stale-in"
	c := cluster(t, out, in)
	good := func(ctx context.Context, cl *kgo.Client) error {
		return cl.ProduceSync(ctx, &kgo.Record{Topic: out, Value: []byte("good")}).FirstErr()
	}

	t.Run("a record of the aborted transaction before it", func(t *testing.T) {
		cl := trackedClient(t, c, "tx-stale-aborted")
		first, err := kafkatx.NewWithClient(cl)
		if err != nil {
			t.Fatalf("NewWithClient: %v", err)
		}
		workFailed := errors.New("work failed")
		if err := first.Do(t.Context(), func(ctx context.Context) error {
			holdPromises(cl)
			cl.Produce(ctx, &kgo.Record{Value: []byte("no topic")}, nil)
			return workFailed
		}); !errors.Is(err, workFailed) {
			t.Fatalf("the first transaction = %v, want its own failure", err)
		}
		next, err := kafkatx.NewWithClient(cl)
		if err != nil {
			t.Fatalf("NewWithClient: %v", err)
		}
		if err := next.Do(t.Context(), func(ctx context.Context) error { return good(ctx, cl) }); err != nil {
			t.Fatalf("the next transaction = %v; a record of the one before refused its commit", err)
		}
	})

	t.Run("a record produced between transactions", func(t *testing.T) {
		cl := trackedClient(t, c, "tx-stale-between")
		holdPromises(cl)
		cl.Produce(t.Context(), &kgo.Record{Topic: out, Value: []byte("outside")}, nil)
		tx, err := kafkatx.NewWithClient(cl)
		if err != nil {
			t.Fatalf("NewWithClient: %v", err)
		}
		if err := tx.Begin(); err != nil {
			t.Fatalf("Begin: %v", err)
		}
		if err := good(t.Context(), cl); err != nil {
			t.Fatalf("producing inside the transaction: %v", err)
		}
		if err := tx.Commit(t.Context()); err != nil {
			t.Fatalf("Commit = %v; a record produced outside the transaction refused it", err)
		}
	})

	t.Run("a record produced before a session's transaction", func(t *testing.T) {
		sess := session(t, c, "tx-stale-session", "g-stale", in, kafkatx.TrackRecords())
		s, err := kafkatx.WrapSession(sess)
		if err != nil {
			t.Fatalf("WrapSession: %v", err)
		}
		holdPromises(sess.Client())
		sess.Produce(t.Context(), &kgo.Record{Topic: out, Value: []byte("outside")}, nil)
		ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
		defer cancel()
		committed, err := s.Do(ctx, func(ctx context.Context) error { return good(ctx, sess.Client()) })
		if err != nil || !committed {
			t.Fatalf("Do = committed %v, %v; a record produced outside the transaction refused it", committed, err)
		}
	})
}

// TestACommitRetriedAfterAnUnknownOutcomeStaysUnknown: franz-go refuses to
// retry a commit whose outcome is unknown, and a retry refused before it was
// sent (ctx ended, or OPERATION_NOT_ATTEMPTED) read as a definite failure.
// That told the caller the transaction had certainly not committed, when the
// first attempt may have.
func TestACommitRetriedAfterAnUnknownOutcomeStaysUnknown(t *testing.T) {
	topic := "tx-retried"
	c := cluster(t, topic)
	producer, _ := open(t, c, topic, "tx-13")
	c.ControlKey(kmsg.EndTxn.Int16(), func(req kmsg.Request) (kmsg.Response, error, bool) {
		resp, _ := req.ResponseKind().(*kmsg.EndTxnResponse)
		resp.ErrorCode = kerr.UnknownServerError.Code
		return resp, nil, true
	})
	tx, err := kafkatx.New(producer)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := tx.Begin(); err != nil {
		t.Fatalf("Begin: %v", err)
	}
	if err := producer.Publish(t.Context(), topic, &mq.Message{Body: []byte("inside")}); err != nil {
		t.Fatalf("Publish inside the transaction: %v", err)
	}
	if err := tx.Commit(t.Context()); !mq.IsAmbiguous(err) {
		t.Fatalf("Commit = %v, want an unknown outcome", err)
	}
	done, cancel := context.WithCancel(t.Context())
	cancel()
	if err := tx.Commit(done); !mq.IsAmbiguous(err) || mq.CodeOf(err) != mq.CodeSettlementUnknown {
		t.Fatalf("a commit retried after an unknown outcome = %v (ambiguous %v), want CodeSettlementUnknown",
			err, mq.IsAmbiguous(err))
	}
	if tx.Ended() {
		t.Fatal("a refused retry marked the transaction ended")
	}
	if err := tx.Abort(t.Context()); err != nil {
		t.Fatalf("Abort after a retried commit = %v, want nil", err)
	}
}

// TestAnAbortFranzGoCouldNotConfirmIsRetried: franz-go keeps the producer in
// the transaction after an abort whose EndTxn failed, so that an abort
// retried at once can finish it. Nothing retried it, so the producer stayed in
// the transaction, and every later Begin on the client was refused, as
// unavailable and retryable, for the life of the process.
func TestAnAbortFranzGoCouldNotConfirmIsRetried(t *testing.T) {
	workFailed := errors.New("work failed")

	t.Run("Tx.Do", func(t *testing.T) {
		out := "tx-abortretry"
		c := cluster(t, out)
		on, refused := failEndTxn(c)
		cl, err := kgo.NewClient(
			kgo.SeedBrokers(c.ListenAddrs()...),
			kgo.TransactionalID("tx-abortretry"),
			kgo.RetryTimeout(time.Second),
		)
		if err != nil {
			t.Fatalf("kgo.NewClient: %v", err)
		}
		defer cl.Close()
		reader, err := kq.Open(t.Context(), kq.Config{Brokers: c.ListenAddrs(), Linger: -1, ReadCommitted: true})
		if err != nil {
			t.Fatalf("opening the reader: %v", err)
		}
		defer func() { _ = reader.Close() }()
		cons, err := reader.Consumer(t.Context(), mq.ConsumerConfig{
			Topic: out, Subscription: "g-abortretry", Start: mq.StartEarliest(),
		})
		if err != nil {
			t.Fatalf("opening a consumer: %v", err)
		}
		defer func() { _ = cons.Close() }()

		tx, err := kafkatx.NewWithClient(cl)
		if err != nil {
			t.Fatalf("NewWithClient: %v", err)
		}
		on.Store(true)
		err = tx.Do(t.Context(), func(ctx context.Context) error {
			if err := cl.ProduceSync(ctx, &kgo.Record{Topic: out, Value: []byte("aborted")}).FirstErr(); err != nil {
				return err
			}
			return workFailed
		})
		on.Store(false)
		if !errors.Is(err, workFailed) {
			t.Fatalf("Do = %v, want the function's own error", err)
		}
		if refused.Load() == 0 {
			t.Fatal("no EndTxn failed, so the abort had nothing to retry")
		}
		if !tx.Ended() {
			t.Fatal("the retried abort did not end the transaction")
		}
		next, err := kafkatx.NewWithClient(cl)
		if err != nil {
			t.Fatalf("NewWithClient: %v", err)
		}
		if err := next.Do(t.Context(), func(ctx context.Context) error {
			return cl.ProduceSync(ctx, &kgo.Record{Topic: out, Value: []byte("next")}).FirstErr()
		}); err != nil {
			t.Fatalf("the next transaction = %v; the failed abort left the producer in the last one", err)
		}
		// The next commit is visible and the aborted record is not, so the
		// retry ended the transaction on the broker as an abort.
		if got := receive(t, cons, 15*time.Second); got != "next" {
			t.Fatalf("after the next commit the consumer saw %q, want only the next transaction's record", got)
		}
	})

	t.Run("Session.Do", func(t *testing.T) {
		in, out := "tx-abortretry-in", "tx-abortretry-out"
		c := cluster(t, in, out)
		on, refused := failEndTxn(c)
		sess := session(t, c, "tx-abortretry-session", "g-abortretry-session", in, kgo.RetryTimeout(time.Second))
		s, err := kafkatx.WrapSession(sess)
		if err != nil {
			t.Fatalf("WrapSession: %v", err)
		}
		on.Store(true)
		committed, err := s.Do(t.Context(), func(ctx context.Context) error {
			if err := sess.ProduceSync(ctx, &kgo.Record{Topic: out, Value: []byte("aborted")}).FirstErr(); err != nil {
				return err
			}
			return workFailed
		})
		on.Store(false)
		if committed || !errors.Is(err, workFailed) {
			t.Fatalf("Do = committed %v, %v; want the function's own error", committed, err)
		}
		if refused.Load() == 0 {
			t.Fatal("no EndTxn failed, so the abort had nothing to retry")
		}
		ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
		defer cancel()
		if committed, err := s.Do(ctx, func(context.Context) error { return nil }); err != nil || !committed {
			t.Fatalf("the next Do = committed %v, %v; the failed abort left the producer in the last transaction", committed, err)
		}
	})
}

// failEndTxn fails every EndTxn the cluster is sent while on is set, at the
// transport, and counts the ones it failed.
func failEndTxn(c *kfake.Cluster) (on *atomic.Bool, refused *atomic.Int32) {
	on, refused = new(atomic.Bool), new(atomic.Int32)
	c.ControlKey(kmsg.EndTxn.Int16(), func(kmsg.Request) (kmsg.Response, error, bool) {
		c.KeepControl()
		if !on.Load() {
			return nil, nil, false //nolint:nilnil // kfake's "not handled" is (nil, nil, false), then the cluster answers
		}
		refused.Add(1)
		return nil, errors.New("the connection dropped"), true
	})
	return on, refused
}

// TestABeginOnAProducerInATransactionIsAConflict: a producer has one
// transaction at a time, and a Begin while another is open cannot succeed
// however often it is retried; only ending that transaction lets it. It was
// reported as unavailable and retryable, so a caller that trusted the flag
// retried it for ever when an abort had failed and left the producer there.
func TestABeginOnAProducerInATransactionIsAConflict(t *testing.T) {
	conflict := func(t *testing.T, what string, err error) {
		t.Helper()
		if mq.CodeOf(err) != mq.CodeConflict || mq.IsRetryable(err) {
			t.Fatalf("%s = %v (code %v, retryable %v), want CodeConflict and not retryable",
				what, err, mq.CodeOf(err), mq.IsRetryable(err))
		}
	}

	t.Run("a second handle", func(t *testing.T) {
		topic := "tx-conflict"
		c := cluster(t, topic)
		producer, _ := open(t, c, topic, "tx-conflict")
		first, err := kafkatx.New(producer)
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		if err := first.Begin(); err != nil {
			t.Fatalf("Begin: %v", err)
		}
		second, err := kafkatx.New(producer)
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		conflict(t, "a Begin while another handle's transaction is open", second.Begin())
		if err := first.Abort(t.Context()); err != nil {
			t.Fatalf("Abort: %v", err)
		}
		if err := second.Do(t.Context(), func(context.Context) error { return nil }); err != nil {
			t.Fatalf("Do once the other transaction ended = %v", err)
		}
	})

	t.Run("a session", func(t *testing.T) {
		in := "tx-conflict-in"
		c := cluster(t, in)
		sess := session(t, c, "tx-conflict-session", "g-conflict", in)
		s, err := kafkatx.WrapSession(sess)
		if err != nil {
			t.Fatalf("WrapSession: %v", err)
		}
		if err := sess.Begin(); err != nil {
			t.Fatalf("the session's own Begin: %v", err)
		}
		committed, err := s.Do(t.Context(), func(context.Context) error { return nil })
		if committed {
			t.Fatal("a Do that could not begin reported a commit")
		}
		conflict(t, "a Do while the session's own transaction is open", err)
		if _, err := sess.End(t.Context(), kgo.TryAbort); err != nil {
			t.Fatalf("the session's own End: %v", err)
		}
		ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
		defer cancel()
		if committed, err := s.Do(ctx, func(context.Context) error { return nil }); err != nil || !committed {
			t.Fatalf("Do once the other transaction ended = committed %v, %v", committed, err)
		}
	})
}

// TestASessionWhoseOffsetsWereRefusedIsDefinite: franz-go's End aborts when
// the broker refuses the transaction's offsets, and returns the refusal once
// that abort has succeeded. Do read the refusal as an end that may have
// reached the broker, so a GROUP_AUTHORIZATION_FAILED reported an unknown
// outcome rather than CodeForbidden.
func TestASessionWhoseOffsetsWereRefusedIsDefinite(t *testing.T) {
	in, out := "tx-offsets-in", "tx-offsets-out"
	c := cluster(t, in, out)
	seed, err := kgo.NewClient(kgo.SeedBrokers(c.ListenAddrs()...))
	if err != nil {
		t.Fatalf("kgo.NewClient: %v", err)
	}
	defer seed.Close()
	if err := seed.ProduceSync(t.Context(), &kgo.Record{Topic: in, Value: []byte("input")}).FirstErr(); err != nil {
		t.Fatalf("producing the input: %v", err)
	}
	c.ControlKey(kmsg.TxnOffsetCommit.Int16(), func(req kmsg.Request) (kmsg.Response, error, bool) {
		r, _ := req.(*kmsg.TxnOffsetCommitRequest)
		resp, _ := r.ResponseKind().(*kmsg.TxnOffsetCommitResponse)
		for _, rt := range r.Topics {
			st := kmsg.NewTxnOffsetCommitResponseTopic()
			st.Topic = rt.Topic
			for _, rp := range rt.Partitions {
				sp := kmsg.NewTxnOffsetCommitResponseTopicPartition()
				sp.Partition = rp.Partition
				sp.ErrorCode = kerr.GroupAuthorizationFailed.Code
				st.Partitions = append(st.Partitions, sp)
			}
			resp.Topics = append(resp.Topics, st)
		}
		return resp, nil, true
	})
	sess := session(t, c, "tx-offsets", "g-offsets", in)
	s, err := kafkatx.WrapSession(sess)
	if err != nil {
		t.Fatalf("WrapSession: %v", err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	committed, err := s.Do(ctx, func(ctx context.Context) error {
		for sess.PollFetches(ctx).NumRecords() == 0 {
			if ctx.Err() != nil {
				return ctx.Err()
			}
		}
		return sess.ProduceSync(ctx, &kgo.Record{Topic: out, Value: []byte("derived")}).FirstErr()
	})
	if committed || !errors.Is(err, kerr.GroupAuthorizationFailed) {
		t.Fatalf("Do = committed %v, %v; want the refusal of the offsets", committed, err)
	}
	if mq.IsAmbiguous(err) || mq.CodeOf(err) != mq.CodeForbidden || mq.IsRetryable(err) {
		t.Fatalf("Do = %v (ambiguous %v, code %v, retryable %v); End's own abort is a definite outcome with the code of the offsets' refusal",
			err, mq.IsAmbiguous(err), mq.CodeOf(err), mq.IsRetryable(err))
	}
	if _, err := s.Do(ctx, func(context.Context) error { return nil }); err != nil {
		t.Fatalf("the next Do = %v; End's abort left the producer in the transaction", err)
	}
}

// TestOneTrackRecordsOnTwoOpenClientsIsRefused: the option holds one ledger,
// so every client built from one option slice shared it, and the ledger
// cannot tell their records apart. Another client's failure refused this
// client's good commit, and a record the other client held back made Begin
// wait 5 seconds and the commit wait until its ctx ended. A transaction on a
// client whose ledger another open client has is refused at once instead, and
// once the other client closes the ledger is this client's alone.
func TestOneTrackRecordsOnTwoOpenClientsIsRefused(t *testing.T) {
	invalid := func(t *testing.T, what string, err error) {
		t.Helper()
		if mq.CodeOf(err) != mq.CodeInvalidConfig {
			t.Fatalf("%s = %v (code %v), want CodeInvalidConfig", what, err, mq.CodeOf(err))
		}
	}
	// other builds a client from the same options that holds one record back
	// and fails another.
	other := func(t *testing.T, opts ...kgo.Opt) *kgo.Client {
		t.Helper()
		cl, err := kgo.NewClient(append(opts, kgo.ManualFlushing())...)
		if err != nil {
			t.Fatalf("kgo.NewClient: %v", err)
		}
		t.Cleanup(cl.Close)
		cl.Produce(context.Background(), &kgo.Record{Topic: "tx-shared", Value: []byte("held back")}, nil)
		cl.Produce(context.Background(), &kgo.Record{Value: []byte("no topic")}, nil)
		return cl
	}

	t.Run("a transaction", func(t *testing.T) {
		topic := "tx-shared"
		c := cluster(t, topic)
		common := []kgo.Opt{kgo.SeedBrokers(c.ListenAddrs()...), kafkatx.TrackRecords()}
		cl, err := kgo.NewClient(append(common, kgo.TransactionalID("tx-shared"))...)
		if err != nil {
			t.Fatalf("kgo.NewClient: %v", err)
		}
		t.Cleanup(cl.Close)
		good := func(ctx context.Context) error {
			return cl.ProduceSync(ctx, &kgo.Record{Topic: topic, Value: []byte("good")}).FirstErr()
		}

		// Begun while the ledger was this client's alone.
		tx, err := kafkatx.NewWithClient(cl)
		if err != nil {
			t.Fatalf("NewWithClient: %v", err)
		}
		if err := tx.Begin(); err != nil {
			t.Fatalf("Begin: %v", err)
		}
		if err := good(t.Context()); err != nil {
			t.Fatalf("producing inside the transaction: %v", err)
		}
		shared := other(t, common...)
		ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
		defer cancel()
		invalid(t, "a commit after another client was given the ledger", tx.Commit(ctx))
		if tx.Ended() {
			t.Fatal("a refused commit marked the transaction ended")
		}
		if err := tx.Abort(t.Context()); err != nil {
			t.Fatalf("Abort after a refused commit = %v, want nil", err)
		}

		next, err := kafkatx.NewWithClient(cl)
		if err != nil {
			t.Fatalf("NewWithClient: %v", err)
		}
		start := time.Now()
		invalid(t, "a Begin while another client has the ledger", next.Begin())
		if waited := time.Since(start); waited > time.Second {
			t.Fatalf("Begin waited %v for the other client's record before it refused", waited)
		}

		shared.Close()
		after, err := kafkatx.NewWithClient(cl)
		if err != nil {
			t.Fatalf("NewWithClient: %v", err)
		}
		if err := after.Do(t.Context(), good); err != nil {
			t.Fatalf("the transaction after the other client closed = %v", err)
		}
	})

	t.Run("a session", func(t *testing.T) {
		in := "tx-shared-in"
		c := cluster(t, in, "tx-shared")
		track := kafkatx.TrackRecords()
		sess := session(t, c, "tx-shared-session", "g-shared", in, track)
		s, err := kafkatx.WrapSession(sess)
		if err != nil {
			t.Fatalf("WrapSession: %v", err)
		}
		shared := other(t, kgo.SeedBrokers(c.ListenAddrs()...), track)
		// Bounded, because the other client's held-back record held the
		// commit until its ctx ended.
		ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
		defer cancel()
		committed, err := s.Do(ctx, func(context.Context) error { return nil })
		if committed {
			t.Fatal("a Do that could not begin reported a commit")
		}
		invalid(t, "a Do while another client has the ledger", err)

		shared.Close()
		if committed, err := s.Do(ctx, func(context.Context) error { return nil }); err != nil || !committed {
			t.Fatalf("the Do after the other client closed = committed %v, %v", committed, err)
		}
	})
}

// trackedClient is a transactional client of the caller's own, with the
// record ledger it needs.
func trackedClient(tb testing.TB, c *kfake.Cluster, txnID string) *kgo.Client {
	tb.Helper()
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(c.ListenAddrs()...),
		kgo.TransactionalID(txnID),
		kafkatx.TrackRecords(),
	)
	if err != nil {
		tb.Fatalf("kgo.NewClient: %v", err)
	}
	tb.Cleanup(cl.Close)
	return cl
}

// holdPromises holds up the client's promise goroutine for a moment, as a
// caller's slow callback does, so the failures of the records produced next
// land late. The record that holds it has no topic, so it fails before
// buffering and no flush or abort waits for it.
func holdPromises(cl *kgo.Client) {
	release := make(chan struct{})
	time.AfterFunc(300*time.Millisecond, func() { close(release) })
	cl.Produce(context.Background(), &kgo.Record{Value: []byte("held")}, func(*kgo.Record, error) { <-release })
}

// receive waits for one delivery and returns its body, or "" when none came.
func receive(tb testing.TB, c *mq.ConsumerHandle, budget time.Duration) string {
	tb.Helper()
	ctx, cancel := context.WithTimeout(tb.Context(), budget)
	defer cancel()
	dst := make([]mq.Delivery, 1)
	for {
		n, err := c.Receive(ctx, dst)
		if n == 1 {
			body := string(dst[0].Body)
			_ = dst[0].Ack()
			return body
		}
		if err != nil || ctx.Err() != nil {
			return ""
		}
	}
}
