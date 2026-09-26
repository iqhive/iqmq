package kafkatx_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"

	mq "github.com/iqhive/iqmq"
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
