package kafka_test

import (
	"errors"
	"reflect"
	"sync/atomic"
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

// endCount is the method x/kafkatx finds the ledger's EndTxn count by, as it
// finds the records by [ledger]'s.
type endCount interface {
	EndTxnLedger() (written, answered uint64, lookedUp bool)
}

// TestATransactionalProducerCountsItsEndTxnRequests: x/kafkatx tells a failed
// end that never reached the broker, and the broker's refusal of the only end
// written, from one whose outcome is unknown by the EndTxn requests the
// producer wrote and the answers to them it read in full. The ledger counted
// records only, so on a producer the driver opened every such end read as
// unknown. It counts both now: an EndTxn whose connection dropped is written
// and not answered, and the one franz-go sends again is both. A coordinator
// lookup franz-go makes before it writes an EndTxn is not one after it, which
// is the lookup whose refusal x/kafkatx must not read as the end's.
func TestATransactionalProducerCountsItsEndTxnRequests(t *testing.T) {
	topic := newTopic("ledger-ends")
	cl := cluster(t, 1, []string{topic})
	_, b := openPair(t, cl, func(cfg *kq.Config) { cfg.TransactionalID = "ledger-" + topic })
	l, ok := ledgerOn(b.Client()).(endCount)
	if !ok {
		t.Fatal("the transactional producer's ledger counts no EndTxn requests x/kafkatx can find")
	}

	txn := b.Client()
	commit := func(what string, written, answered uint64) {
		t.Helper()
		if err := txn.BeginTransaction(); err != nil {
			t.Fatalf("BeginTransaction: %v", err)
		}
		if err := txn.ProduceSync(t.Context(), &kgo.Record{Topic: topic, Value: []byte(what)}).FirstErr(); err != nil {
			t.Fatalf("producing inside the transaction: %v", err)
		}
		if err := txn.EndTransaction(t.Context(), kgo.TryCommit); err != nil {
			t.Fatalf("EndTransaction(commit) for %s: %v", what, err)
		}
		if w, a, lookedUp := l.EndTxnLedger(); w != written || a != answered || lookedUp {
			t.Fatalf("after %s the ledger counts %d EndTxn written and %d answered, and a lookup after the latest %v; want %d and %d, and none", what, w, a, lookedUp, written, answered)
		}
	}
	commit("a commit", 1, 1)
	var dropped atomic.Bool
	cl.ControlKey(kmsg.EndTxn.Int16(), func(kmsg.Request) (kmsg.Response, error, bool) {
		dropped.Store(true)
		return nil, errors.New("the connection dropped"), true
	})
	commit("a commit whose first EndTxn lost its connection", 3, 2)
	if !dropped.Load() {
		t.Fatal("the cluster dropped no EndTxn, so the test proves nothing")
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
