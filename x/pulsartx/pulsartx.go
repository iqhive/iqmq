// Package pulsartx adds Pulsar transactions alongside the portable API.
//
// # Why it is not in the driver
//
// The portable API has no transaction verb, and it is not going to get one.
// the confirmation modes describe how strong an acknowledgement ONE
// publication waits for; a transaction is a different thing — a set of
// publications and a set of acknowledgements that become visible together or
// not at all — and only three of iqmq's brokers have anything like it. A
// portable Begin/Commit would therefore be a method that worked on three
// drivers and returned ErrUnsupported on the rest, in a package whose whole
// premise is that a caller can tell what a driver does from its capability
// table.
//
// So a transaction is reached the way every broker-specific thing is reached:
// through a small helper module that takes the driver's own client, is imported
// deliberately, and is not in the import graph of a program that does not use
// it (the specification's "no conditional compilation, no build tags: an optional
// integration is a module").
//
// # What a Pulsar transaction is, and how it differs from Kafka's
//
// Two halves, as in Kafka: messages produced inside the transaction become
// visible to a reader only when it commits, and the acknowledgements taken
// inside it are applied with the same commit. What differs is WHERE the
// transaction lives. A Kafka producer has one transaction at a time and every
// publication through it belongs to that transaction; a Pulsar transaction is a
// VALUE, attached per message (`ProducerMessage.Transaction`) and per
// acknowledgement (`Consumer.AckWithTxn`). Nothing is implicitly enrolled.
//
// That is the whole reason this package exposes [Tx.Native] and does not try to
// hide it. Publication and acknowledgement inside a transaction are the native
// client's, because the portable [github.com/iqhive/iqmq.PublishOptions] has no
// field a transaction could travel in and
// [github.com/iqhive/iqmq.SinkConfig] has none either — the gap
// the maintenance notes records as "mq.SinkConfig and mq.SourceConfig carry nothing
// driver-defined". What this package adds is the contract that is easy to get
// wrong.
//
// # What it guarantees
//
//   - a transaction is ended exactly once ([ErrAlreadyEnded]);
//   - [Tx.Do] ends it on every path, including a panic and an early return,
//     which is the form a caller should reach for: a transaction left open
//     holds its coordinator's state until it times out;
//   - an END whose outcome is unknown reports ErrSettlementUnknown with
//     Ambiguous set rather than a failure, because a commit whose reply was
//     lost may have committed — and retrying an unknown commit is how a
//     read-process-write pipeline duplicates a whole batch.
//
// # Requirements
//
// The client must have been opened with
// [github.com/iqhive/iqmq/drivers/pulsar.Config.EnableTransactions], and the
// BROKER must have `transactionCoordinatorEnabled` on. Without the first,
// [New] refuses; without the second, the client's own NewTransaction does.
package pulsartx

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"

	mq "github.com/iqhive/iqmq"
)

// DriverName is the driver this helper is for, carried by every error it
// builds.
const DriverName = "pulsar"

// DefaultTimeout is how long a transaction may stay open when the caller names
// no bound. The coordinator aborts it afterwards, so a bound is not optional —
// a transaction with none would be a coordinator leak.
const DefaultTimeout = time.Minute

// Client is the part of pulsar.Client this helper needs: the transaction
// factory.
//
// It is an interface rather than the library's concrete client so that the
// helper's own contract can be tested without a broker — a transaction
// coordinator is not something a unit test can start, and the three things this
// package promises are all about what happens around Commit and Abort.
type Client interface {
	// NewTransaction begins a transaction that the coordinator will abort
	// after the timeout.
	NewTransaction(timeout time.Duration) (pulsar.Transaction, error)
}

// Tx is one Pulsar transaction in progress.
//
// It is begun by [New]: Pulsar has no separate Begin, because
// `Client.NewTransaction` both registers the transaction with the coordinator
// and returns it. A Tx is therefore open from the moment it exists, which is
// why [Tx.Do] is the form to reach for.
//
// It is not safe for two goroutines that both end it: the state transitions are
// guarded, so the loser is refused rather than corrupting the coordinator's
// view, but a transaction is a unit of one goroutine's work and two goroutines
// sharing one is a design mistake this type reports rather than accommodates.
type Tx struct {
	txn pulsar.Transaction

	mu     sync.Mutex
	ending bool
	ended  bool
	// refused is the answer to every ending once the client has refused
	// one without sending it and left the transaction unable to end: see
	// [Tx.end].
	refused error
}

// New begins a transaction on a client the Pulsar driver opened.
//
// c must be a client of github.com/iqhive/iqmq/drivers/pulsar whose
// configuration enabled transactions. That is checked here rather than at the
// first Commit, because a client without them refuses every transaction and the
// refusal names the coordinator rather than the configuration.
//
// A timeout of zero means [DefaultTimeout].
func New(c *mq.Client, timeout time.Duration) (*Tx, error) {
	if c == nil {
		return nil, ErrNotPulsar
	}
	cl, ok := c.Native().(Client)
	if !ok {
		return nil, ErrNotPulsar
	}
	return NewWithClient(cl, timeout)
}

// NewWithClient is [New] for a caller that already holds the native client,
// which is what a program composing pulsar-client-go and iqmq has.
func NewWithClient(cl Client, timeout time.Duration) (*Tx, error) {
	if cl == nil {
		return nil, ErrNotPulsar
	}
	if timeout <= 0 {
		timeout = DefaultTimeout
	}
	txn, err := cl.NewTransaction(timeout)
	if err != nil {
		if errors.Is(err, pulsar.ErrClientTransactionsNotEnabled) {
			return nil, ErrNotTransactional
		}
		return nil, wrap(err, false)
	}
	if txn == nil {
		return nil, ErrNotTransactional
	}
	return &Tx{txn: txn}, nil
}

// Native is the transaction itself, to attach to a publication
// (`ProducerMessage.Transaction`) or an acknowledgement
// (`Consumer.AckWithTxn`).
//
// It is exposed rather than wrapped because that is where a Pulsar transaction
// lives: it is a value carried per message, not a mode a producer is in. A
// helper that hid it would have to re-export the whole producer and consumer
// surface to be usable.
func (t *Tx) Native() pulsar.Transaction { return t.txn }

// ID is the transaction's identifier, for a log line that has to be correlated
// with the broker's.
func (t *Tx) ID() pulsar.TxnID { return t.txn.GetTxnID() }

// State is the coordinator's view of the transaction.
func (t *Tx) State() pulsar.TxnState { return t.txn.GetState() }

// Ended reports whether this transaction has been committed or aborted. One
// that is being ended counts, so no work is enrolled in it meanwhile.
func (t *Tx) Ended() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.ended || t.ending
}

// Commit ends the transaction and makes its publications and acknowledgements
// visible together.
//
// An outcome nobody knows is reported as ErrSettlementUnknown with
// Ambiguous set, not as a failure. A commit whose reply was lost may have
// committed, and a caller that retried it would replay the whole
// read-process-write batch — which is the duplicate the transaction existed to
// prevent.
//
// A commit the client refuses without sending it, because the transaction can
// no longer end (an enrolled operation failed, or it timed out), is a definite
// CodeConflict that is not retryable, and every later Commit or Abort answers
// the same.
func (t *Tx) Commit(ctx context.Context) error { return t.end(ctx, true) }

// Abort ends the transaction and discards its publications and
// acknowledgements.
//
// It is what a handler does when the work failed: the messages produced inside
// the transaction never become visible, the acknowledgements are not applied,
// and the input is consumed again.
func (t *Tx) Abort(ctx context.Context) error { return t.end(ctx, false) }

// end performs the one ending this transaction gets.
//
// The transaction is only marked ended once the client has left it unable to
// end again. pulsar-client-go returns a done ctx before it sends anything and
// puts the transaction back to open, so a commit cancelled that way is
// definite and the abort that follows it must not be refused.
//
// The client also refuses an ending without sending it, and leaves the
// transaction unable to end, when an enrolled send or acknowledgement failed
// (TxnError), when its timeout passed (TxnTimeout), and when an enrolled
// operation was still pending at its operation timeout, which times it out
// on the way. That is a definite, final [mq.CodeConflict], and every later
// ending answers the same: it used to be a retryable CodeUnavailable whose
// retry answered ErrAlreadyEnded, which reads as committed.
func (t *Tx) end(ctx context.Context, commit bool) error {
	t.mu.Lock()
	if t.refused != nil {
		err := t.refused
		t.mu.Unlock()
		return err
	}
	if t.ended || t.ending {
		t.mu.Unlock()
		return ErrAlreadyEnded
	}
	t.ending = true
	t.mu.Unlock()
	before := t.txn.GetState()
	var err error
	if commit {
		err = t.txn.Commit(ctx)
	} else {
		err = t.txn.Abort(ctx)
	}
	after := t.txn.GetState()
	sent := endSent(before, after)
	var out error
	switch {
	case err == nil:
	case sent:
		out = wrap(err, true)
	case after != pulsar.TxnOpen:
		out = unendable(after, err)
	default:
		out = wrap(err, false)
	}
	t.mu.Lock()
	t.ending = false
	t.ended = err == nil || after != pulsar.TxnOpen
	if err != nil && !sent && after != pulsar.TxnOpen {
		t.refused = out
	}
	t.mu.Unlock()
	return out
}

// unendable is the refusal of an ending the client did not send because the
// transaction was in state, from which the client ends it no more. Nothing
// reached the coordinator, which aborts a transaction nobody ends once its
// timeout passes, and no retry changes it.
func unendable(state pulsar.TxnState, err error) error {
	return &mq.OpError{
		Driver:    DriverName,
		Operation: mq.OpPublish,
		Code:      mq.CodeConflict,
		Err: fmt.Errorf("iqmq/pulsartx: pulsar-client-go refused to end the transaction, in state %v, without "+
			"sending anything to the coordinator, and it cannot be ended through this client again: %w", state, err),
	}
}

// endSent reports whether a failed ending reached the coordinator, which is
// what makes its outcome unknown. The client refuses an ending of a
// transaction that was not open, puts a cancelled one back to open, and marks
// one whose operations never completed timed out, all before sending.
func endSent(before, after pulsar.TxnState) bool {
	return before == pulsar.TxnOpen && after != pulsar.TxnOpen && after != pulsar.TxnTimeout
}

// Do runs fn inside the transaction and ends it exactly once.
//
// It is the form a caller should reach for, and the reason is the path nobody
// tests: fn returning early, fn panicking, ctx being cancelled midway. Each of
// them leaves a transaction open at the coordinator until its timeout, holding
// the messages it produced invisible and the acknowledgements it took
// unapplied — and on a topic with a transaction buffer, holding the
// SUBSCRIPTION's backlog with them. Do aborts on every one of those and commits
// on none of them.
//
// A panic is re-raised after the abort, so it reaches the caller's own recovery
// rather than being turned into an error.
//
// A commit that fails before it reaches the coordinator leaves the transaction
// open, so it is aborted as well; the commit's own error is the one returned.
// One the client refused because the transaction can no longer end returns
// that CodeConflict, and there is nothing left to abort.
func (t *Tx) Do(ctx context.Context, fn func(context.Context, pulsar.Transaction) error) (err error) {
	if fn == nil {
		return ErrNoFunc
	}
	committed := false
	defer func() {
		if committed || t.Ended() {
			return
		}
		// An abort failure must not hide the reason for the abort, so it
		// is only reported when there is nothing else to report.
		if aerr := t.abortDetached(ctx); aerr != nil && err == nil {
			err = aerr
		}
	}()
	// Assigned rather than declared: err is the NAMED return, and the
	// deferred abort reads it to decide whether to report its own failure on
	// top of the reason for the abort.
	err = fn(ctx, t.txn)
	if err != nil {
		return err
	}
	err = t.Commit(ctx)
	committed = err == nil
	return err
}

// abortTimeout bounds the abort [Tx.Do] runs on its way out.
const abortTimeout = 30 * time.Second

// abortDetached aborts under a context the caller's cancellation does not
// reach. The usual reason Do is aborting is that ctx ended, and the client
// returns a done ctx before it sends the abort, which would leave the
// transaction open until the coordinator times it out.
func (t *Tx) abortDetached(ctx context.Context) error {
	actx, cancel := context.WithTimeout(context.WithoutCancel(ctx), abortTimeout)
	defer cancel()
	return t.Abort(actx)
}

// Publish publishes one message as part of this transaction.
//
// It is three lines of the caller's own code, and it is here for one reason: a
// caller who forgets `ProducerMessage.Transaction` publishes OUTSIDE the
// transaction, with no error anywhere and no way to tell from the return value.
// The message becomes visible at once and the commit that was supposed to
// reveal it has nothing to do.
func (t *Tx) Publish(ctx context.Context, p pulsar.Producer, m *pulsar.ProducerMessage) (pulsar.MessageID, error) {
	switch {
	case p == nil:
		return nil, ErrNoProducer
	case m == nil:
		return nil, ErrNoMessage
	}
	if t.Ended() {
		return nil, ErrAlreadyEnded
	}
	m.Transaction = t.txn
	id, err := p.Send(ctx, m)
	if err != nil {
		return nil, wrap(err, true)
	}
	return id, nil
}

// Ack acknowledges one message as part of this transaction.
//
// Like [Tx.Publish] it exists because the mistake it prevents is silent: a
// plain Ack inside a transaction is applied at once, so a commit that never
// happens leaves the message consumed anyway and the input is not reprocessed.
func (t *Tx) Ack(c pulsar.Consumer, m pulsar.Message) error {
	switch {
	case c == nil:
		return ErrNoConsumer
	case m == nil:
		return ErrNoMessage
	}
	if t.Ended() {
		return ErrAlreadyEnded
	}
	if err := c.AckWithTxn(m, t.txn); err != nil {
		return wrap(err, true)
	}
	return nil
}

// wrap maps a pulsar-client-go transaction failure onto the model.
//
// Every failure it sees is about the records and acknowledgements the
// transaction carries, which is why there is no operation to pass: the specification's
// Operation is what a caller reads to know which of its own calls failed, and
// "publish" is the honest answer for all of them.
//
// sentAlready is what decides ambiguity, and it is true for every ending: the
// end request has been written, so a failure to hear the answer is not a
// failure to end the transaction.
func wrap(err error, sentAlready bool) error {
	if err == nil {
		return nil
	}
	code := mq.CodeOf(err)
	ambiguous := false
	switch {
	case errors.Is(err, pulsar.ErrClientTransactionsNotEnabled):
		code = mq.CodeUnsupported
	case sentAlready:
		code, ambiguous = mq.CodeSettlementUnknown, true
	case code == mq.CodeUnknown:
		code = mq.CodeUnavailable
	}
	return &mq.OpError{
		Driver:    DriverName,
		Operation: mq.OpPublish,
		Code:      code,
		Ambiguous: ambiguous,
		Retryable: !ambiguous && code == mq.CodeUnavailable,
		Err:       err,
	}
}

// The helper's own refusals. The specification names no sentinel for them and errors.Is
// matches by code, so a caller compares mq.CodeOf(err).
var (
	// ErrNotPulsar reports a client that is not this repository's Pulsar
	// driver.
	ErrNotPulsar = &mq.OpError{
		Driver: DriverName, Operation: mq.OpOpen, Code: mq.CodeInvalidArgument,
		Err: errors.New("iqmq/pulsartx: the client is not a github.com/iqhive/iqmq/drivers/pulsar client"),
	}
	// ErrNotTransactional reports a client opened without transactions, or
	// a broker with no transaction coordinator.
	ErrNotTransactional = &mq.OpError{
		Driver: DriverName, Operation: mq.OpOpen, Code: mq.CodeUnsupported,
		Err: errors.New("iqmq/pulsartx: the client was opened without Config.EnableTransactions, or the broker has no transaction coordinator"),
	}
	// ErrAlreadyEnded reports a second ending of one transaction, or work
	// offered to one that has ended.
	ErrAlreadyEnded = &mq.OpError{
		Driver: DriverName, Operation: mq.OpPublish, Code: mq.CodeAlreadySettled,
		Err: errors.New("iqmq/pulsartx: this transaction has already been committed or aborted"),
	}
	// ErrNoFunc reports a Do with nothing to do.
	ErrNoFunc = &mq.OpError{
		Driver: DriverName, Operation: mq.OpPublish, Code: mq.CodeInvalidArgument,
		Err: errors.New("iqmq/pulsartx: Do needs a function; a transaction with no work in it would commit nothing"),
	}
	// ErrNoProducer reports a transactional publication with no producer.
	ErrNoProducer = &mq.OpError{
		Driver: DriverName, Operation: mq.OpPublish, Code: mq.CodeInvalidArgument,
		Err: errors.New("iqmq/pulsartx: the producer is nil"),
	}
	// ErrNoConsumer reports a transactional acknowledgement with no
	// consumer.
	ErrNoConsumer = &mq.OpError{
		Driver: DriverName, Operation: mq.OpAck, Code: mq.CodeInvalidArgument,
		Err: errors.New("iqmq/pulsartx: the consumer is nil"),
	}
	// ErrNoMessage reports a transactional publication or acknowledgement
	// with no message.
	ErrNoMessage = &mq.OpError{
		Driver: DriverName, Operation: mq.OpPublish, Code: mq.CodeInvalidArgument,
		Err: errors.New("iqmq/pulsartx: the message is nil"),
	}
)
