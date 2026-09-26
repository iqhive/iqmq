package pulsartx_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/x/pulsartx"
)

// TestNewRefusesAClientWithoutTransactions is why the check is at construction
// rather than at the first Commit: a client without them refuses every
// transaction and the refusal names the coordinator rather than the
// configuration.
func TestNewRefusesAClientWithoutTransactions(t *testing.T) {
	if _, err := pulsartx.New(nil, 0); !errors.Is(err, mq.ErrInvalidTopic) && mq.CodeOf(err) != mq.CodeInvalidArgument {
		t.Fatalf("New(nil) reported %v (code %v)", err, mq.CodeOf(err))
	}
	if _, err := pulsartx.NewWithClient(nil, 0); mq.CodeOf(err) != mq.CodeInvalidArgument {
		t.Fatalf("NewWithClient(nil) reported %v (code %v)", err, mq.CodeOf(err))
	}
	cl := &fakeClient{err: pulsar.ErrClientTransactionsNotEnabled}
	if _, err := pulsartx.NewWithClient(cl, 0); !errors.Is(err, mq.ErrUnsupported) {
		t.Fatalf("a client with transactions disabled reported %v (code %v), want ErrUnsupported",
			err, mq.CodeOf(err))
	}
}

// TestEndsExactlyOnce holds the one contract a transaction handle has to have.
func TestEndsExactlyOnce(t *testing.T) {
	cl := &fakeClient{}
	tx, err := pulsartx.NewWithClient(cl, time.Minute)
	if err != nil {
		t.Fatalf("NewWithClient: %v", err)
	}
	if cl.timeout != time.Minute {
		t.Fatalf("the transaction was begun with a timeout of %v", cl.timeout)
	}
	if tx.Ended() {
		t.Fatal("a fresh transaction reports itself ended; Pulsar has no separate Begin, so it is open " +
			"from the moment it exists")
	}
	if err := tx.Commit(context.Background()); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if !tx.Ended() {
		t.Fatal("a committed transaction does not report itself ended")
	}
	if err := tx.Commit(context.Background()); !errors.Is(err, mq.ErrAlreadySettled) {
		t.Fatalf("a second Commit reported %v (code %v), want ErrAlreadySettled", err, mq.CodeOf(err))
	}
	if err := tx.Abort(context.Background()); !errors.Is(err, mq.ErrAlreadySettled) {
		t.Fatalf("an Abort after a Commit reported %v, want ErrAlreadySettled", err)
	}
	if cl.txn.commits != 1 || cl.txn.aborts != 0 {
		t.Fatalf("the coordinator saw %d commit(s) and %d abort(s) for one transaction",
			cl.txn.commits, cl.txn.aborts)
	}
}

// TestAnUnknownCommitIsAmbiguous is the rule and the reason it matters more
// here than anywhere: retrying an unknown commit replays a whole
// read-process-write batch.
func TestAnUnknownCommitIsAmbiguous(t *testing.T) {
	cl := &fakeClient{txnErr: errors.New("the reply never arrived")}
	tx, err := pulsartx.NewWithClient(cl, 0)
	if err != nil {
		t.Fatalf("NewWithClient: %v", err)
	}
	err = tx.Commit(context.Background())
	if err == nil {
		t.Fatal("a commit whose reply was lost was reported as a success")
	}
	if !mq.IsAmbiguous(err) {
		t.Fatalf("a commit whose reply was lost reported %v (ambiguous %v), and the specification makes an unknown "+
			"outcome ambiguous rather than a failure: a caller that retried it would replay the "+
			"whole batch", err, mq.IsAmbiguous(err))
	}
	if mq.CodeOf(err) != mq.CodeSettlementUnknown {
		t.Fatalf("the error carries code %v, want SettlementUnknown", mq.CodeOf(err))
	}
	// mq.IsRetryable is false for anything ambiguous since, so reading it here could no longer fail: this package builds
	// the error itself (`Retryable: !ambiguous && ...`) and the flag is what
	// this check is about. It is read where it is set, so it can fail again.
	var e *mq.OpError
	if !errors.As(err, &e) || e.Retryable {
		t.Fatalf("an ambiguous commit was marked retryable by this package: %+v", e)
	}
	if mq.IsRetryable(err) {
		t.Fatal("an ambiguous commit was reported safe to repeat")
	}
}

// TestDoEndsOnEveryPath is the path nobody tests: an early return, a panic and
// a cancelled context all leave a transaction open at the coordinator until its
// timeout, holding its publications invisible and its acknowledgements
// unapplied.
func TestDoEndsOnEveryPath(t *testing.T) {
	t.Run("success commits", func(t *testing.T) {
		cl := &fakeClient{}
		tx, _ := pulsartx.NewWithClient(cl, 0)
		if err := tx.Do(context.Background(), func(context.Context, pulsar.Transaction) error {
			return nil
		}); err != nil {
			t.Fatalf("Do: %v", err)
		}
		if cl.txn.commits != 1 || cl.txn.aborts != 0 {
			t.Fatalf("a successful Do produced %d commit(s) and %d abort(s)", cl.txn.commits, cl.txn.aborts)
		}
	})
	t.Run("an error aborts", func(t *testing.T) {
		cl := &fakeClient{}
		tx, _ := pulsartx.NewWithClient(cl, 0)
		want := errors.New("the work failed")
		if err := tx.Do(context.Background(), func(context.Context, pulsar.Transaction) error {
			return want
		}); !errors.Is(err, want) {
			t.Fatalf("Do reported %v, want the function's own error", err)
		}
		if cl.txn.commits != 0 || cl.txn.aborts != 1 {
			t.Fatalf("a failed Do produced %d commit(s) and %d abort(s)", cl.txn.commits, cl.txn.aborts)
		}
	})
	t.Run("a panic aborts and is re-raised", func(t *testing.T) {
		cl := &fakeClient{}
		tx, _ := pulsartx.NewWithClient(cl, 0)
		func() {
			defer func() {
				if recover() == nil {
					t.Error("the panic did not reach the caller's own recovery")
				}
			}()
			_ = tx.Do(context.Background(), func(context.Context, pulsar.Transaction) error {
				panic("boom")
			})
		}()
		if cl.txn.aborts != 1 {
			t.Fatalf("a panicking Do produced %d abort(s), want 1: a transaction left open holds its "+
				"coordinator's state until it times out", cl.txn.aborts)
		}
	})
	t.Run("no function", func(t *testing.T) {
		cl := &fakeClient{}
		tx, _ := pulsartx.NewWithClient(cl, 0)
		if err := tx.Do(context.Background(), nil); mq.CodeOf(err) != mq.CodeInvalidArgument {
			t.Fatalf("Do(nil) reported %v (code %v)", err, mq.CodeOf(err))
		}
		_ = tx.Abort(context.Background())
	})
}

// TestPublishAndAckEnrolTheMessage is the mistake the two helpers exist to
// prevent: a Pulsar transaction is a VALUE attached per message, so a caller
// who forgets it publishes outside the transaction with no error anywhere.
func TestPublishAndAckEnrolTheMessage(t *testing.T) {
	cl := &fakeClient{}
	tx, _ := pulsartx.NewWithClient(cl, 0)
	prod := &fakeProducer{}
	m := &pulsar.ProducerMessage{Payload: []byte("x")}
	if _, err := tx.Publish(context.Background(), prod, m); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if prod.sent == nil || prod.sent.Transaction != tx.Native() {
		t.Fatal("the publication was not enrolled in the transaction; it would become visible at once " +
			"and the commit that was supposed to reveal it would have nothing to do")
	}
	cons := &fakeConsumer{}
	if err := tx.Ack(cons, &fakeMessage{}); err != nil {
		t.Fatalf("Ack: %v", err)
	}
	if cons.txn != tx.Native() {
		t.Fatal("the acknowledgement was not enrolled in the transaction; it would be applied at once " +
			"and a commit that never happened would leave the message consumed anyway")
	}
	// And neither is offered to a transaction that has ended.
	if err := tx.Commit(context.Background()); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if _, err := tx.Publish(context.Background(), prod, m); !errors.Is(err, mq.ErrAlreadySettled) {
		t.Fatalf("publishing into an ended transaction reported %v", err)
	}
	if err := tx.Ack(cons, &fakeMessage{}); !errors.Is(err, mq.ErrAlreadySettled) {
		t.Fatalf("acknowledging into an ended transaction reported %v", err)
	}
	if _, err := tx.Publish(context.Background(), nil, m); mq.CodeOf(err) != mq.CodeInvalidArgument {
		t.Fatalf("Publish with no producer reported %v", err)
	}
	if err := tx.Ack(nil, &fakeMessage{}); mq.CodeOf(err) != mq.CodeInvalidArgument {
		t.Fatalf("Ack with no consumer reported %v", err)
	}
}

// TestConcurrentEndingsSerialise is what the guard is for: two goroutines that
// both end one transaction, with the loser refused rather than the
// coordinator's view corrupted.
func TestConcurrentEndingsSerialise(t *testing.T) {
	for range 200 {
		cl := &fakeClient{}
		tx, _ := pulsartx.NewWithClient(cl, 0)
		var wg sync.WaitGroup
		errs := make([]error, 2)
		start := make(chan struct{})
		for i := range 2 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				if i == 0 {
					errs[i] = tx.Commit(context.Background())
					return
				}
				errs[i] = tx.Abort(context.Background())
			}()
		}
		close(start)
		wg.Wait()
		winners := 0
		for _, err := range errs {
			if err == nil {
				winners++
			}
		}
		if winners != 1 {
			t.Fatalf("%d of two concurrent endings succeeded, want exactly one", winners)
		}
		if cl.txn.commits+cl.txn.aborts != 1 {
			t.Fatalf("the coordinator saw %d ending(s) for one transaction", cl.txn.commits+cl.txn.aborts)
		}
	}
}

// fakeClient is a transaction factory, which is the only part of pulsar.Client
// this helper touches.
type fakeClient struct {
	err     error
	txnErr  error
	timeout time.Duration
	txn     *fakeTxn
}

func (c *fakeClient) NewTransaction(timeout time.Duration) (pulsar.Transaction, error) {
	c.timeout = timeout
	if c.err != nil {
		return nil, c.err
	}
	c.txn = &fakeTxn{err: c.txnErr}
	return c.txn, nil
}

// fakeTxn counts what the coordinator was asked to do.
type fakeTxn struct {
	mu      sync.Mutex
	err     error
	commits int
	aborts  int
}

func (t *fakeTxn) Commit(context.Context) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.commits++
	return t.err
}

func (t *fakeTxn) Abort(context.Context) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.aborts++
	return t.err
}

func (t *fakeTxn) GetState() pulsar.TxnState { return pulsar.TxnOpen }
func (t *fakeTxn) GetTxnID() pulsar.TxnID    { return pulsar.TxnID{MostSigBits: 1, LeastSigBits: 2} }

// fakeProducer records the message it was given, so that the enrolment can be
// asserted.
type fakeProducer struct {
	pulsar.Producer
	sent *pulsar.ProducerMessage
}

func (p *fakeProducer) Send(_ context.Context, m *pulsar.ProducerMessage) (pulsar.MessageID, error) {
	cp := *m
	p.sent = &cp
	return pulsar.EarliestMessageID(), nil
}

// fakeConsumer records the transaction an acknowledgement was enrolled in.
type fakeConsumer struct {
	pulsar.Consumer
	txn pulsar.Transaction
}

func (c *fakeConsumer) AckWithTxn(_ pulsar.Message, txn pulsar.Transaction) error {
	c.txn = txn
	return nil
}

// fakeMessage is the narrowest pulsar.Message this test needs.
type fakeMessage struct{ pulsar.Message }
