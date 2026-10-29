package kafka_test

import (
	"errors"
	"reflect"
	"testing"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	kq "github.com/iqhive/iqmq/drivers/kafka"
)

// ledger is the method x/kafkatx finds the record ledger by. kafkatx cannot
// name a type of this module, so this method is the whole contract between
// the two.
type ledger interface {
	RecordLedger() (taken, finished, failed uint64, last error)
}

// TestATransactionalProducerCountsItsRecords: franz-go commits a transaction
// one of whose records the broker refused, and the failure reaches only the
// record's promise. x/kafkatx refuses that commit through a hook on the
// producer that counts the records and their failures, so the driver puts one
// on every transactional producer it opens, and on no other.
func TestATransactionalProducerCountsItsRecords(t *testing.T) {
	topic := newTopic("ledger")
	cl := cluster(t, 1, []string{topic})
	_, b := openPair(t, cl, func(cfg *kq.Config) { cfg.TransactionalID = "ledger-" + topic })
	l := ledgerOn(b.Client())
	if l == nil {
		t.Fatal("the transactional producer carries no record ledger")
	}

	txn := b.Client()
	if err := txn.BeginTransaction(); err != nil {
		t.Fatalf("BeginTransaction: %v", err)
	}
	if err := txn.ProduceSync(t.Context(), &kgo.Record{Topic: topic, Value: []byte("kept")}).FirstErr(); err != nil {
		t.Fatalf("producing the accepted record: %v", err)
	}
	cl.Fault(kfake.Fault{Keys: []kmsg.Key{kmsg.Produce}, Topic: topic, Err: kerr.InvalidRecord, Count: 1})
	if err := txn.ProduceSync(t.Context(), &kgo.Record{Topic: topic, Value: []byte("refused")}).FirstErr(); !errors.Is(err, kerr.InvalidRecord) {
		t.Fatalf("producing the refused record = %v, want INVALID_RECORD", err)
	}
	taken, finished, failed, last := l.RecordLedger()
	if taken != 2 || finished != 2 || failed != 1 || !errors.Is(last, kerr.InvalidRecord) {
		t.Fatalf("the ledger reads taken %d, finished %d, failed %d, last %v; want 2, 2, 1 and the refusal",
			taken, finished, failed, last)
	}
	if err := txn.EndTransaction(t.Context(), kgo.TryAbort); err != nil {
		t.Fatalf("EndTransaction(abort): %v", err)
	}

	if ledgerOn(backendOn(t, cl).Client()) != nil {
		t.Fatal("a producer with no transactional id carries a record ledger")
	}
}

// ledgerOn finds the record ledger among the client's hooks the way
// x/kafkatx does.
func ledgerOn(cl *kgo.Client) ledger {
	for _, v := range cl.OptValues(kgo.WithHooks) {
		hooks := reflect.ValueOf(v)
		if hooks.Kind() != reflect.Slice {
			continue
		}
		for i := range hooks.Len() {
			if l, ok := hooks.Index(i).Interface().(ledger); ok {
				return l
			}
		}
	}
	return nil
}
