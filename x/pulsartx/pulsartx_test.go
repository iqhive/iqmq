package pulsartx_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit"
	pd "github.com/iqhive/iqmq/drivers/pulsar"
	"github.com/iqhive/iqmq/drivers/pulsar/pulsarfake"
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

// TestACancelledCommitIsDefinite: the client returns a done ctx before it
// sends the commit and leaves the transaction open, so the failure is not
// ambiguous, the transaction is not ended, and the abort that follows works.
func TestACancelledCommitIsDefinite(t *testing.T) {
	cl := &fakeClient{}
	tx, _ := pulsartx.NewWithClient(cl, 0)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := tx.Commit(ctx)
	if err == nil || mq.IsAmbiguous(err) {
		t.Fatalf("a commit refused before it was sent = %v (ambiguous %v), want a definite failure", err, mq.IsAmbiguous(err))
	}
	if tx.Ended() {
		t.Fatal("a commit that was never sent marked the transaction ended")
	}
	if err := tx.Abort(context.Background()); err != nil {
		t.Fatalf("Abort after a cancelled commit = %v, want nil", err)
	}
	if !tx.Ended() || cl.txn.aborts != 1 {
		t.Fatalf("the abort did not end the transaction (ended %v, %d abort(s))", tx.Ended(), cl.txn.aborts)
	}
}

// TestDoAbortsACancelledCommit: Do's commit runs out of ctx before it is sent,
// and Do aborts on a context the cancelled one does not reach, rather than
// leaving the transaction open until the coordinator times it out.
func TestDoAbortsACancelledCommit(t *testing.T) {
	cl := &fakeClient{}
	tx, _ := pulsartx.NewWithClient(cl, 0)
	ctx, cancel := context.WithCancel(context.Background())
	err := tx.Do(ctx, func(context.Context, pulsar.Transaction) error {
		cancel()
		return nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Do = %v, want the commit's own cancellation", err)
	}
	if cl.txn.commits != 0 || cl.txn.aborts != 1 || !tx.Ended() {
		t.Fatalf("Do left %d commit(s), %d abort(s), ended %v; want the transaction aborted",
			cl.txn.commits, cl.txn.aborts, tx.Ended())
	}
}

// TestAnEndingTheClientRefusesIsDefiniteAndFinal is a commit of a transaction
// the client has already made unable to end: an enrolled send or
// acknowledgement failed (TxnError), its timeout passed (TxnTimeout), or an
// operation was still pending at the client's operation timeout, which times
// it out on the way. Nothing reaches the coordinator, and neither a commit nor
// an abort can follow.
//
// It was reported as a retryable CodeUnavailable, and the retry it invited
// answered ErrAlreadyEnded, "already been committed or aborted", which a
// caller or a retry middleware reads as done: a read-process-write batch that
// never committed looked committed.
func TestAnEndingTheClientRefusesIsDefiniteAndFinal(t *testing.T) {
	for _, tc := range []struct {
		name  string
		setup func(*fakeTxn)
	}{
		{"an enrolled operation failed", func(txn *fakeTxn) { txn.set(pulsar.TxnError) }},
		{"the transaction timed out", func(txn *fakeTxn) { txn.set(pulsar.TxnTimeout) }},
		{"an operation outlived the operation timeout", func(txn *fakeTxn) { txn.pending = true }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cl := &fakeClient{}
			tx, err := pulsartx.NewWithClient(cl, 0)
			if err != nil {
				t.Fatalf("NewWithClient: %v", err)
			}
			tc.setup(cl.txn)
			err = tx.Commit(context.Background())
			var oe *mq.OpError
			if !errors.As(err, &oe) {
				t.Fatalf("Commit = %v, want an *mq.OpError", err)
			}
			if oe.Code != mq.CodeConflict || oe.Retryable || oe.Ambiguous {
				t.Fatalf("a commit the client refused before sending anything reported code %v, retryable %v, "+
					"ambiguous %v; want a definite CodeConflict nobody retries: the transaction can never "+
					"commit", oe.Code, oe.Retryable, oe.Ambiguous)
			}
			for name, again := range map[string]error{
				"a second Commit": tx.Commit(context.Background()),
				"an Abort":        tx.Abort(context.Background()),
			} {
				if errors.Is(again, mq.ErrAlreadySettled) || mq.CodeOf(again) != mq.CodeConflict {
					t.Errorf("%s after the refusal answered %v (code %v), want the same refusal: nothing "+
						"was committed or aborted", name, again, mq.CodeOf(again))
				}
			}
			if cl.txn.commits != 0 || cl.txn.aborts != 0 {
				t.Fatalf("the coordinator saw %d commit(s) and %d abort(s)", cl.txn.commits, cl.txn.aborts)
			}
		})
	}
	t.Run("through Do", func(t *testing.T) {
		cl := &fakeClient{}
		tx, _ := pulsartx.NewWithClient(cl, 0)
		err := tx.Do(context.Background(), func(context.Context, pulsar.Transaction) error {
			cl.txn.set(pulsar.TxnError)
			return nil
		})
		if mq.CodeOf(err) != mq.CodeConflict || mq.IsRetryable(err) {
			t.Fatalf("Do whose commit the client refused = %v (code %v, retryable %v), want a definite "+
				"CodeConflict", err, mq.CodeOf(err), mq.IsRetryable(err))
		}
	})
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

// TestAPublicationTheClientRefusedIsDefinite is a transactional Send that
// pulsar-client-go v0.21 refuses before it queues anything (internalSendAsync:
// validateMsg, prepareTransaction, its done-context check, updateChunkInfo,
// reserveResources), so the message never left this process. Publish reported
// every Send failure as an ambiguous CodeSettlementUnknown, so a full queue
// read as a lost reply, and a caller could not tell backpressure from an
// outcome nobody knows. Each now has the definite code the Pulsar driver gives
// the same refusal, and only what may have been written stays ambiguous.
func TestAPublicationTheClientRefusedIsDefinite(t *testing.T) {
	refusedTxn := errors.Join(pulsar.ErrTransaction, errors.New("failed to send message by a non-open transaction"))
	for _, tc := range []struct {
		name string
		err  error
		// closed is a transaction the client no longer sends in.
		closed    bool
		code      mq.ErrorCode
		retryable bool
	}{
		{"a full queue", pulsar.ErrSendQueueIsFull, false, mq.CodeBackpressure, false},
		{"a full memory buffer", pulsar.ErrMemoryBufferIsFull, false, mq.CodeBackpressure, false},
		{"a message too large", pulsar.ErrMessageTooLarge, false, mq.CodeMessageTooLarge, false},
		{"metadata too large", pulsar.ErrMetaTooLarge, false, mq.CodeInvalidArgument, false},
		{"an invalid message", pulsar.ErrInvalidMessage, false, mq.CodeInvalidArgument, false},
		{"a transaction that is not open", refusedTxn, true, mq.CodeConflict, false},
		{
			"a topic the coordinator did not enrol", errors.Join(pulsar.ErrTransaction, errors.New("the coordinator did not answer")),
			false, mq.CodeUnavailable, true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cl := &fakeClient{}
			tx, err := pulsartx.NewWithClient(cl, 0)
			if err != nil {
				t.Fatalf("NewWithClient: %v", err)
			}
			if tc.closed {
				cl.txn.set(pulsar.TxnError)
			}
			_, err = tx.Publish(context.Background(), &refusingProducer{err: tc.err}, &pulsar.ProducerMessage{Payload: []byte("x")})
			var oe *mq.OpError
			if !errors.As(err, &oe) {
				t.Fatalf("Publish = %v, want an *mq.OpError", err)
			}
			if oe.Ambiguous || oe.Code != tc.code || oe.Retryable != tc.retryable {
				t.Errorf("a Send the client refused before queuing it (%v) reported code %v, ambiguous %v, "+
					"retryable %v; want the definite %v, retryable %v: the message never left this process",
					tc.err, oe.Code, oe.Ambiguous, oe.Retryable, tc.code, tc.retryable)
			}
			if !errors.Is(err, tc.err) {
				t.Errorf("Publish = %v, which does not wrap the client's own refusal", err)
			}
		})
	}
	t.Run("a done context", func(t *testing.T) {
		cl := &fakeClient{}
		tx, _ := pulsartx.NewWithClient(cl, 0)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		prod := &refusingProducer{}
		_, err := tx.Publish(ctx, prod, &pulsar.ProducerMessage{Payload: []byte("x")})
		if !errors.Is(err, context.Canceled) || mq.IsAmbiguous(err) || prod.sends != 0 {
			t.Errorf("Publish on a done context = %v (ambiguous %v) after %d Send(s), want a definite "+
				"context.Canceled and no Send", err, mq.IsAmbiguous(err), prod.sends)
		}
	})
}

// TestAPublicationThatMayHaveBeenWrittenStaysAmbiguous is the other half: a
// Send failure the client also gives a message it has written stays an
// ambiguous CodeSettlementUnknown nobody retries. The asymmetry is the Pulsar
// driver's (PublishAdmitted): calling a definite refusal ambiguous costs a
// de-duplication, and calling an ambiguous outcome definite costs a lost
// message or a duplicate.
func TestAPublicationThatMayHaveBeenWrittenStaysAmbiguous(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	for _, tc := range []struct {
		name string
		err  error
		// gaveUp ends ctx inside the Send, as a caller's deadline passing
		// while the message waited for its receipt does.
		gaveUp bool
	}{
		{"the send timeout", pulsar.ErrSendTimeout, false},
		{"a producer the client closed", pulsar.ErrProducerClosed, false},
		{"a topic terminated under it", errors.New("server error: TopicTerminatedError: the topic was terminated"), false},
		{"a caller that gave up waiting for the receipt", context.Canceled, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cl := &fakeClient{}
			tx, _ := pulsartx.NewWithClient(cl, 0)
			prod := &refusingProducer{err: tc.err}
			if tc.gaveUp {
				prod.during = cancel
			}
			_, err := tx.Publish(ctx, prod, &pulsar.ProducerMessage{Payload: []byte("x")})
			var oe *mq.OpError
			if !errors.As(err, &oe) || !oe.Ambiguous || oe.Code != mq.CodeSettlementUnknown || oe.Retryable {
				t.Errorf("a Send failure the client also gives a written message (%v) reported %v, want an "+
					"ambiguous CodeSettlementUnknown nobody retries", tc.err, err)
			}
		})
	}
}

// TestAnAcknowledgementTheClientRefusedIsDefinite is AckWithTxn refused before
// the acknowledgement was sent: v0.21 refuses a consumer that has closed
// (ackIDCommon, internalAckWithTxn) and a transaction that is not open
// (internalAckWithTxn; a broker that refuses it answers InvalidTxnStatus).
// Ack reported every failure as an ambiguous CodeSettlementUnknown; what may
// have reached the broker stays so.
func TestAnAcknowledgementTheClientRefusedIsDefinite(t *testing.T) {
	for _, tc := range []struct {
		name      string
		err       error
		ambiguous bool
		code      mq.ErrorCode
	}{
		{"a closed consumer", errors.New("consumer state is closed"), false, mq.CodeClosed},
		{
			"a consumer closing as the acknowledgement went in",
			resultError{pulsar.ConsumerClosed, "Failed to ack by closing or closed consumer"},
			false, mq.CodeClosed,
		},
		{
			"a transaction that is not open",
			resultError{pulsar.InvalidStatus, "Failed to ack by a non-open transaction."},
			false, mq.CodeConflict,
		},
		{
			"a broker that refused the transaction", errors.New("server error: InvalidTxnStatus: the transaction is not open"),
			false, mq.CodeConflict,
		},
		{"a request that timed out", resultError{pulsar.TimeoutError, "request timed out"}, true, mq.CodeSettlementUnknown},
		{"a connection lost after the write", errors.New("connection closed"), true, mq.CodeSettlementUnknown},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cl := &fakeClient{}
			tx, _ := pulsartx.NewWithClient(cl, 0)
			err := tx.Ack(&fakeConsumer{err: tc.err}, &fakeMessage{})
			var oe *mq.OpError
			if !errors.As(err, &oe) {
				t.Fatalf("Ack = %v, want an *mq.OpError", err)
			}
			if oe.Ambiguous != tc.ambiguous || oe.Code != tc.code || oe.Retryable {
				t.Errorf("an acknowledgement that failed with %v reported code %v, ambiguous %v, retryable %v; "+
					"want %v, ambiguous %v, not retryable", tc.err, oe.Code, oe.Ambiguous, oe.Retryable,
					tc.code, tc.ambiguous)
			}
		})
	}
}

// TestARefusalHasThePulsarDriversCode ties the codes Publish gives a refusal
// to the Pulsar driver's own. Every result code the driver reads as a refusal
// made before anything was sent (pd.PublishAdmitted) is published once
// through the driver, over its in-process model with a producer that answers
// that code, and once through Tx.Publish, and the two must report the same
// definite code and the same retryability. The codes are a copy, since the
// driver's error rules are not exported, so a code the driver's rule lists
// later, or a rule it changes, fails here until the copy follows.
func TestARefusalHasThePulsarDriversCode(t *testing.T) {
	server := pulsarfake.New()
	prod := &answeringProducer{}
	cl := &answeringClient{nativeClient: server.Client(), prod: prod}
	b, err := pd.BackendWithClient(context.Background(), cl, driverkit.ClientOwned, pd.Config{
		ServiceURL: "pulsar://pulsarfake:6650", Clock: server,
	})
	if err != nil {
		t.Fatalf("BackendWithClient: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })
	snk, err := b.OpenSink(context.Background(), mq.SinkConfig{Topic: "refused"})
	if err != nil {
		t.Fatalf("OpenSink: %v", err)
	}
	t.Cleanup(func() { _ = snk.Close() })
	checked := 0
	// pulsar.Result has no bound of its own; a value it does not name is
	// one the driver reads as sent, and is skipped like any other.
	for r := range pulsar.Result(256) {
		refusal := resultError{r, "refused"}
		if pd.PublishAdmitted(refusal) {
			continue
		}
		checked++
		prod.err = refusal
		viaDriver := snk.Publish(context.Background(), &mq.Message{Body: []byte("x")}, nil)
		tx, err := pulsartx.NewWithClient(&fakeClient{}, 0)
		if err != nil {
			t.Fatalf("NewWithClient: %v", err)
		}
		_, viaTx := tx.Publish(context.Background(), &refusingProducer{err: refusal}, &pulsar.ProducerMessage{Payload: []byte("x")})
		_ = tx.Abort(context.Background())
		var d, x *mq.OpError
		if !errors.As(viaDriver, &d) || !errors.As(viaTx, &x) {
			t.Fatalf("result %v: the driver reported %v and Publish %v, want an *mq.OpError from both", r, viaDriver, viaTx)
		}
		if x.Code != d.Code || x.Retryable != d.Retryable || x.Ambiguous || d.Ambiguous {
			t.Errorf("result %v: Publish reported code %v, retryable %v, ambiguous %v; the Pulsar driver reports "+
				"code %v, retryable %v, ambiguous %v for the same refusal", r, x.Code, x.Retryable, x.Ambiguous,
				d.Code, d.Retryable, d.Ambiguous)
		}
	}
	if checked < 12 {
		t.Fatalf("only %d result codes read as refusals made before anything was sent, so this test tests little", checked)
	}
}

// nativeClient is the client the Pulsar driver is built over, which its
// in-process model provides.
type nativeClient interface {
	CreateProducer(pulsar.ProducerOptions) (pulsar.Producer, error)
	Subscribe(pulsar.ConsumerOptions) (pulsar.Consumer, error)
	TopicPartitions(topic string) ([]string, error)
	NewTransaction(time.Duration) (pulsar.Transaction, error)
	Close()
}

// answeringClient is the driver's in-process model, every producer of which
// is prod over the model's own.
type answeringClient struct {
	nativeClient
	prod *answeringProducer
}

func (c *answeringClient) CreateProducer(o pulsar.ProducerOptions) (pulsar.Producer, error) {
	p, err := c.nativeClient.CreateProducer(o)
	if err != nil {
		return nil, err
	}
	c.prod.Producer = p
	return c.prod, nil
}

// answeringProducer answers every Send with err.
type answeringProducer struct {
	pulsar.Producer
	err error
}

func (p *answeringProducer) Send(context.Context, *pulsar.ProducerMessage) (pulsar.MessageID, error) {
	return nil, p.err
}

// refusingProducer answers every Send with err, after running during when it
// is set, and counts its Sends.
type refusingProducer struct {
	pulsar.Producer
	err    error
	during func()
	sends  int
}

func (p *refusingProducer) Send(context.Context, *pulsar.ProducerMessage) (pulsar.MessageID, error) {
	p.sends++
	if p.during != nil {
		p.during()
	}
	return nil, p.err
}

// resultError is an error carrying a Pulsar result code, which
// pulsar-client-go's own *pulsar.Error does but cannot be built outside it.
type resultError struct {
	code pulsar.Result
	text string
}

func (e resultError) Error() string         { return e.text }
func (e resultError) Result() pulsar.Result { return e.code }

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
	c.txn = &fakeTxn{err: c.txnErr, state: pulsar.TxnOpen}
	return c.txn, nil
}

// fakeTxn counts what the coordinator was asked to do, and keeps the state the
// way pulsar-client-go v0.21 does (transaction_impl.go): an ending of a
// transaction that is not open is refused before anything is sent and leaves
// the state alone; a done ctx is returned before anything is sent and leaves
// the transaction open; operations still pending at the client's operation
// timeout time it out, also before anything is sent; and any other failure is
// one the coordinator was sent and leaves it unable to end again.
type fakeTxn struct {
	mu      sync.Mutex
	err     error
	state   pulsar.TxnState
	commits int
	aborts  int
	// pending is an enrolled send or acknowledgement that has not
	// completed by the client's operation timeout.
	pending bool
}

func (t *fakeTxn) Commit(ctx context.Context) error {
	return t.end(ctx, &t.commits, pulsar.TxnCommitting, pulsar.TxnCommitted)
}

func (t *fakeTxn) Abort(ctx context.Context) error {
	return t.end(ctx, &t.aborts, pulsar.TxnAborting, pulsar.TxnAborted)
}

func (t *fakeTxn) end(ctx context.Context, n *int, ending, ended pulsar.TxnState) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.state != pulsar.TxnOpen {
		return errors.New("Expected transaction state: TxnOpen, actual: " + t.state.String())
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if t.pending {
		t.state = pulsar.TxnTimeout
		return errors.New("There are some operations that are not completed after the timeout.")
	}
	*n++
	if t.err != nil {
		t.state = ending
		return t.err
	}
	t.state = ended
	return nil
}

func (t *fakeTxn) GetState() pulsar.TxnState {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.state
}

// set moves the transaction to a state the client puts it in on its own: a
// failed enrolled send or acknowledgement (TxnError), or its timeout passing
// (TxnTimeout).
func (t *fakeTxn) set(state pulsar.TxnState) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.state = state
}

func (t *fakeTxn) GetTxnID() pulsar.TxnID { return pulsar.TxnID{MostSigBits: 1, LeastSigBits: 2} }

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

// fakeConsumer records the transaction an acknowledgement was enrolled in, and
// answers err.
type fakeConsumer struct {
	pulsar.Consumer
	txn pulsar.Transaction
	err error
}

func (c *fakeConsumer) AckWithTxn(_ pulsar.Message, txn pulsar.Transaction) error {
	c.txn = txn
	return c.err
}

// fakeMessage is the narrowest pulsar.Message this test needs.
type fakeMessage struct{ pulsar.Message }
