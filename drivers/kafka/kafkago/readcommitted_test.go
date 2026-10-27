package kafkago_test

import (
	"strconv"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	mq "github.com/iqhive/iqmq"
	kq "github.com/iqhive/iqmq/drivers/kafka/kafkago"
)

// produceTxn writes bodies to topic in one transaction and commits or aborts
// it. This driver cannot produce transactionally, so a franz-go producer
// stands in for the other application that did.
func produceTxn(tb testing.TB, cl *kgo.Client, topic string, commit bool, bodies ...string) {
	tb.Helper()
	ctx := tb.Context()
	if err := cl.BeginTransaction(); err != nil {
		tb.Fatalf("beginning a transaction: %v", err)
	}
	for _, b := range bodies {
		if err := cl.ProduceSync(ctx, &kgo.Record{Topic: topic, Value: []byte(b)}).FirstErr(); err != nil {
			tb.Fatalf("producing %q: %v", b, err)
		}
	}
	if err := cl.EndTransaction(ctx, kgo.TransactionEndTry(commit)); err != nil {
		tb.Fatalf("ending a transaction: %v", err)
	}
}

// transactionalProducer opens a franz-go transactional producer on c.
func transactionalProducer(tb testing.TB, addrs []string) *kgo.Client {
	tb.Helper()
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(addrs...),
		kgo.TransactionalID("readcommitted-"+newTopic("txn")),
		kgo.TransactionTimeout(10*time.Second),
	)
	if err != nil {
		tb.Fatalf("opening a transactional producer: %v", err)
	}
	tb.Cleanup(cl.Close)
	return cl
}

// A ReadCommitted consumer must not see a record of an aborted transaction.
// kafka-go's Client.Fetch drops the response's aborted-transaction list, so a
// reader built on it handed every aborted record out as committed.
func TestReadCommittedSkipsAbortedTransactions(t *testing.T) {
	topic := newTopic("rc")
	c := cluster(t, 1, []string{topic})
	p := transactionalProducer(t, c.ListenAddrs())
	produceTxn(t, p, topic, false, "aborted-1", "aborted-2")
	produceTxn(t, p, topic, true, "committed-1")
	produceTxn(t, p, topic, false, "aborted-3")
	produceTxn(t, p, topic, true, "committed-2", "committed-3")

	cl := openOn(t, c, func(cfg *kq.Config) { cfg.ReadCommitted = true })
	cons := consumerFrom(t, cl, topic, "readcommitted", mq.StartEarliest())
	var got []string
	receiveN(t, cons, 3, 10*time.Second, func(d *mq.Delivery) error {
		got = append(got, string(d.Body))
		return d.Ack()
	})
	want := []string{"committed-1", "committed-2", "committed-3"}
	if len(got) != len(want) {
		t.Fatalf("received %q, want %q", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("received %q, want %q", got, want)
		}
	}
	expectNothing(t, cons, 500*time.Millisecond, "after the committed records")
}

// A ReadCommitted consumer that starts between an aborted transaction's
// records and its ABORT marker must still receive the producer's next
// committed transaction. The fetch lists the aborted transaction, because its
// marker is in range, and the first batch of that producer it reads is the
// marker: the filter left the abort pending there, and the committed
// transaction's batch then activated it and was dropped whole.
func TestReadCommittedFromAnAbortMarker(t *testing.T) {
	topic := newTopic("rc")
	c := cluster(t, 1, []string{topic})
	p := transactionalProducer(t, c.ListenAddrs())
	// aborted-1 and aborted-2 are offsets 0 and 1 and their ABORT marker is
	// 2; committed-1 is 3 and its COMMIT marker 4.
	produceTxn(t, p, topic, false, "aborted-1", "aborted-2")
	produceTxn(t, p, topic, true, "committed-1")
	if hw := c.PartitionInfo(topic, 0).HighWatermark; hw != 5 {
		t.Fatalf("the log ends at %d, want 5: the start offsets below assume another layout", hw)
	}

	cl := openOn(t, c, func(cfg *kq.Config) { cfg.ReadCommitted = true })
	for start := range uint64(4) {
		t.Run("from-"+strconv.FormatUint(start, 10), func(t *testing.T) {
			cons := consumerFrom(t, cl, topic, "readcommitted-from-"+strconv.FormatUint(start, 10), mq.StartAtSequence(start))
			d := receiveOne(t, cons, 10*time.Second)
			if got := string(d.Body); got != "committed-1" {
				t.Fatalf("received %q, want %q", got, "committed-1")
			}
			if err := d.Ack(); err != nil {
				t.Fatalf("Ack: %v", err)
			}
			expectNothing(t, cons, 500*time.Millisecond, "after the committed record")
		})
	}
}
