package kafka_test

import (
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	mq "github.com/iqhive/iqmq"
	kq "github.com/iqhive/iqmq/drivers/kafka"
)

// TestADeadLetterMoveIsNotPartOfTheProducersTransaction is the transactional
// producer's half of the dead-letter route. A franz-go client with a
// transactional id fails every record produced outside a transaction, and one
// produced inside the application's transaction joins it, so a move that went
// through that client either never succeeded or was discarded by the
// application's abort after the original's offset had been completed.
func TestADeadLetterMoveIsNotPartOfTheProducersTransaction(t *testing.T) {
	for _, tc := range []struct {
		name string
		// open runs a transaction across the Reject and aborts it after,
		// which is a read-process-write pipeline whose processing failed.
		open bool
	}{
		{name: "no transaction open"},
		{name: "the application's transaction is open and aborts", open: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			topic, dlq := newTopic("txmove"), newTopic("txmove-dead")
			cl := cluster(t, 1, []string{topic, dlq})
			plain := openOn(t, cl)
			c, b := openPair(t, cl, func(cfg *kq.Config) {
				cfg.TransactionalID = "txmove-" + topic
				cfg.NackConfig.DeadLetterTopic = dlq
			})
			cons := consumerOn(t, c, topic, "g-txmove")
			publish(t, plain, topic, &mq.Message{Body: []byte("poison")})
			d := receiveOne(t, cons, 15*time.Second)

			txn := b.Client()
			if tc.open {
				if err := txn.BeginTransaction(); err != nil {
					t.Fatalf("BeginTransaction: %v", err)
				}
			}
			if err := d.Reject(); err != nil {
				t.Fatalf("Reject = %v, want the copy on the dead-letter topic", err)
			}
			if tc.open {
				if err := txn.AbortBufferedRecords(t.Context()); err != nil {
					t.Fatalf("AbortBufferedRecords: %v", err)
				}
				if err := txn.EndTransaction(t.Context(), kgo.TryAbort); err != nil {
					t.Fatalf("EndTransaction(abort): %v", err)
				}
			}

			reader := openOn(t, cl, func(cfg *kq.Config) { cfg.ReadCommitted = true })
			got := receiveOne(t, consumerFrom(t, reader, dlq, "g-txmove-reader", mq.StartEarliest()), 15*time.Second)
			if string(got.Body) != "poison" {
				t.Fatalf("the dead-letter topic holds %q", got.Body)
			}
			if err := got.Ack(); err != nil {
				t.Fatalf("Ack: %v", err)
			}
		})
	}
}
