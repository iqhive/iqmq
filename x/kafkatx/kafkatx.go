// Package kafkatx adds Kafka transactions alongside the portable API.
//
// # Why it is not in the driver
//
// The portable API has no transaction verb, and it is not going to get one.
// the confirmation modes describe how strong an acknowledgement ONE
// publication waits for; a transaction is a different thing — a set of
// publications and a set of consumed offsets that become visible together or
// not at all — and only three of iqmq's brokers have anything like it. A
// portable `Begin`/`Commit` would therefore be a method that worked on three
// drivers and returned ErrUnsupported on the rest, in a package whose whole
// premise is that a caller can tell what a driver does from its capability
// table.
//
// So a transaction is reached the way every broker-specific thing is reached:
// through a small helper module that takes the driver's own client, is
// imported deliberately, and is not in the import graph of a program that does
// not use it (the specification's "no conditional compilation, no build tags: an optional
// integration is a module").
//
// # What a Kafka transaction is
//
// Two things at once, and the second is the one people forget:
//
//   - records produced inside the transaction become visible to a
//     read-committed consumer only when the transaction commits;
//   - the OFFSETS a consumer group consumed inside the transaction are
//     committed as part of it.
//
// Together they are Kafka's read-process-write: a consumer reads records,
// produces derived records, and commits its own position atomically with them.
// Without the second half the derived records and the consumer's position can
// disagree after a crash, which is the duplicate a transaction was supposed to
// prevent.
//
// # What this helper does and does not do
//
// It drives the transaction and nothing else. Producing goes through the iqmq
// publisher, so the mapping, the partitioner, the headers and the error model
// are the driver's; [Tx.Commit] and [Tx.Abort] are the two verbs franz-go has
// and this package's job is to make their CONTRACT hard to get wrong:
//
//   - a transaction is begun once and ended once ([ErrNotBegun],
//     [ErrAlreadyBegun]);
//   - [Tx.Do] ends it exactly once on every path, including a panic, which is
//     the form a caller should reach for;
//   - an END whose outcome is unknown reports ErrSettlementUnknown
//     rather than a failure, because a commit whose reply was lost may have
//     committed. That is the same rule the driver applies to a publication and
//     it matters more here: retrying an unknown commit is how a
//     read-process-write pipeline duplicates a whole batch. A commit that was
//     never sent is a definite failure and leaves the transaction open for
//     the abort;
//   - a transaction one of whose records failed does not commit. franz-go
//     would commit it: a broker that refuses a record outright
//     (INVALID_RECORD, MESSAGE_TOO_LARGE) leaves the producer id valid, and
//     the records it accepted become visible without the refused one. The
//     failure reaches only the record's promise, and an asynchronous
//     publication's promise runs during the commit's own flush, after the
//     code that published it has returned. So the commit counts the
//     transaction's records through a hook on the client, and refuses when
//     one of them failed.
//
// # Requirements
//
// The client must have been opened with [github.com/iqhive/iqmq/drivers/kafka.Config.TransactionalID]
// set, and its consumers with ReadCommitted when they read a transactional
// topic. A transactional producer cannot use the driver's bounded-delivery
// default — franz-go refuses the cancellation option together with a
// transactional id — so a publication inside a transaction is unbounded, and
// the caller's own context is what ends it.
//
// The driver puts the record-counting hook on a transactional producer it
// opens. A client the caller built needs a [TrackRecords] of its own among
// its options, whether it reaches this package through [NewWithClient],
// through the driver's OpenWithClient, or as a [kgo.GroupTransactSession].
// Without it a commit cannot see a record of its transaction fail.
package kafkatx

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	mq "github.com/iqhive/iqmq"
	kq "github.com/iqhive/iqmq/drivers/kafka"
	"github.com/iqhive/iqmq/internal/errcode"
)

// Tx is one Kafka transaction in progress.
//
// It is not safe for concurrent use by two goroutines that both end it: the
// state transitions are guarded, so the loser is refused rather than
// corrupting the producer, but a transaction is a unit of one goroutine's work
// and two goroutines sharing one is a design mistake this type reports rather
// than accommodates.
type Tx struct {
	cl *kgo.Client
	// records is the client's record ledger, nil when it carries none.
	records ledger

	mu     sync.Mutex
	begun  bool
	ending bool
	ended  bool
	// unconfirmed is set once a commit's outcome is unknown. franz-go
	// refuses to retry that commit, and some of its refusals, such as
	// OPERATION_NOT_ATTEMPTED, would read as definite although the first
	// attempt may have committed.
	unconfirmed bool
	// mark is the ledger's failure count when the transaction began, so a
	// failure counted after it is one of the transaction's records.
	mark uint64
}

// New returns a transaction handle for a client the Kafka driver opened.
//
// c must be a client of github.com/iqhive/iqmq/drivers/kafka whose
// configuration named a transactional id. It is checked here rather than at
// the first Commit, because a producer that is not transactional fails EVERY
// publication once a transaction is begun and the failure names the producer
// rather than the configuration.
func New(c *mq.Client) (*Tx, error) {
	if c == nil {
		return nil, ErrNotKafka
	}
	cl, ok := c.Native().(*kgo.Client)
	if !ok {
		return nil, ErrNotKafka
	}
	if !transactional(cl) {
		return nil, ErrNotTransactional
	}
	return &Tx{cl: cl, records: ledgerOf(cl)}, nil
}

// NewWithClient is [New] for a caller that already holds the native client,
// which is what a program composing franz-go and iqmq has. That client needs
// [TrackRecords] among its options for a commit to see a record fail.
func NewWithClient(cl *kgo.Client) (*Tx, error) {
	if cl == nil {
		return nil, ErrNotKafka
	}
	if !transactional(cl) {
		return nil, ErrNotTransactional
	}
	return &Tx{cl: cl, records: ledgerOf(cl)}, nil
}

// transactional reports whether the client was built with a transactional id.
//
// kgo.Client.OptValue is the only route: the option is not readable as a
// field, and asking the broker would be a round trip to learn something the
// configuration already knows.
func transactional(cl *kgo.Client) bool {
	id, ok := cl.OptValue(kgo.TransactionalID).(string)
	return ok && id != ""
}

// Begin starts the transaction.
//
// Every publication this client makes afterwards belongs to it, which is
// franz-go's model and Kafka's: a transactional producer has one transaction
// at a time and it is the producer's, not a handle's. That is why Begin is
// refused when one is already in progress — the second caller would be adding
// records to the first caller's transaction.
// A handle is spent once its transaction has ended, and Begin says so with
// ErrAlreadyEnded rather than ErrAlreadyBegun: the two are different
// conditions and a caller retrying on the second would wait for a transaction
// that finished.
//
// Begin first waits, for at most 5 seconds, for the records the client took
// before it to finish, so that a commit is refused only for a record of its
// own transaction.
//
// A producer still in another transaction, begun by another handle or left
// open by an abort that failed, refuses Begin with CodeConflict, which is not
// retryable: only ending that transaction lets a new one begin. A producer
// that another one with the same transactional id has fenced refuses it with
// CodeConflict too, and only a new client can begin. A client whose
// [TrackRecords] option is on another open client too refuses it with
// CodeInvalidConfig.
func (t *Tx) Begin() error { return t.begin(context.Background()) }

// begin is [Tx.Begin], with ctx ending its wait for the earlier records.
func (t *Tx) begin(ctx context.Context) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	switch {
	case t.ended:
		return ErrAlreadyEnded
	case t.begun:
		return ErrAlreadyBegun
	case shared(t.records, t.cl):
		return errSharedLedger
	}
	mark := markBefore(ctx, t.cl, t.records)
	if err := t.cl.BeginTransaction(); err != nil {
		return beginRefused(err)
	}
	t.begun = true
	t.mark = mark
	return nil
}

// Commit ends the transaction and makes its records and offsets visible
// together.
//
// An outcome nobody knows is reported as ErrSettlementUnknown with
// Ambiguous set, not as a failure. A commit whose reply was lost may have
// committed, and a caller that retried it would replay the whole
// read-process-write batch — which is the duplicate the transaction existed to
// prevent.
//
// A commit that is refused before it is sent is a definite failure and leaves
// the transaction open, so [Tx.Ended] reports false and the caller aborts it,
// which [Tx.Do] does. Commit refuses when a record of the transaction failed,
// when ctx ends before the end is sent, and when franz-go itself declines to
// attempt it.
//
// A commit retried after one whose outcome was unknown reports
// ErrSettlementUnknown again without trying. franz-go refuses that retry,
// because nothing it can do learns the first attempt's outcome; the recovery
// is the abort.
func (t *Tx) Commit(ctx context.Context) error { return t.end(ctx, kgo.TryCommit) }

// Abort ends the transaction and discards its records.
//
// It is what a handler does when the work failed: the records produced inside
// the transaction are never visible to a read-committed consumer, and the
// offsets are not committed, so the input is consumed again.
//
// An abort whose end franz-go could not confirm is retried at once, which is
// franz-go's recovery for it. An abort that still fails leaves the
// transaction open: Ended reports false, every Begin on the client is
// refused, and Abort on this handle is the way out.
func (t *Tx) Abort(ctx context.Context) error { return t.end(ctx, kgo.TryAbort) }

// end performs the one ending this transaction gets.
//
// The transaction is only marked ended once franz-go has confirmed it. A
// commit whose outcome is unconfirmed is to be retried with Abort, which is
// franz-go's documented recovery, so that Abort must not be refused.
func (t *Tx) end(ctx context.Context, commit kgo.TransactionEndTry) error {
	t.mu.Lock()
	switch {
	case !t.begun:
		t.mu.Unlock()
		return ErrNotBegun
	case t.ended, t.ending:
		t.mu.Unlock()
		return ErrAlreadyEnded
	case commit == kgo.TryCommit && t.unconfirmed:
		t.mu.Unlock()
		return wrap(errCommitUnconfirmed, true)
	}
	t.ending = true
	t.mu.Unlock()
	err := t.finish(ctx, commit)
	t.mu.Lock()
	t.ending = false
	t.ended = err == nil
	if commit == kgo.TryCommit && mq.IsAmbiguous(err) {
		t.unconfirmed = true
	}
	t.mu.Unlock()
	return err
}

// errCommitUnconfirmed is a commit retried after one whose outcome is unknown.
var errCommitUnconfirmed = errors.New("iqmq/kafkatx: an earlier commit of this transaction has an unknown outcome, which a retried commit cannot learn; abort the transaction")

// finish settles the buffered records and ends the transaction.
//
// EndTransaction does neither of the first two itself: an unflushed record
// is missing from the commit, and an unaborted one joins the NEXT
// transaction.
func (t *Tx) finish(ctx context.Context, commit kgo.TransactionEndTry) error {
	switch commit {
	case kgo.TryCommit:
		if err := settle(ctx, t.cl, t.records, t.mark); err != nil {
			return err
		}
	case kgo.TryAbort:
		if err := t.cl.AbortBufferedRecords(ctx); err != nil {
			// Nothing has been sent to end the transaction, so it is
			// still open and not ambiguous.
			return wrap(err, false)
		}
	}
	if err := t.cl.EndTransaction(ctx, commit); err != nil {
		if commit == kgo.TryAbort && abortRetried(ctx, t.cl) {
			return nil
		}
		return wrap(err, endSent(err))
	}
	return nil
}

// abortRetried retries an abort whose EndTransaction failed, and reports
// whether the retry ended the transaction.
//
// franz-go keeps the producer in the transaction after an end it could not
// confirm, so that a TryAbort retried at once finishes it: the retry reloads
// the producer id, and the epoch bump fences the transaction on the broker,
// with no second EndTxn to fail. Without the retry the producer stayed in the
// transaction, and every later Begin on the client was refused.
func abortRetried(ctx context.Context, cl *kgo.Client) bool {
	return cl.EndTransaction(ctx, kgo.TryAbort) == nil
}

// settle readies a transaction for its commit: it flushes the records and
// refuses the commit when one of them failed or ctx has ended. Nothing has
// been sent to end the transaction when it refuses, so the refusal is
// definite and the transaction is still open for the abort.
func settle(ctx context.Context, cl *kgo.Client, records ledger, mark uint64) error {
	if err := cl.Flush(ctx); err != nil {
		return wrap(err, false)
	}
	if err := recordFailure(ctx, cl, records, mark); err != nil {
		return err
	}
	// EndTransaction given a done ctx may have written the request before
	// it noticed, and that would make a commit that never happened
	// unknowable.
	if err := ctx.Err(); err != nil {
		return wrap(err, false)
	}
	return nil
}

// Ended reports whether this transaction has been committed or aborted.
func (t *Tx) Ended() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.ended
}

// Do runs fn inside a transaction and ends it exactly once.
//
// It is the form a caller should reach for, and the reason is the path nobody
// tests: fn returning early, fn panicking, ctx being cancelled midway. Each of
// them leaves a transactional producer with an open transaction, and the next
// Begin then fails — so a pipeline that panicked once stops producing
// altogether until the process restarts. Do aborts on every one of those and
// commits on none of them.
//
// A panic is re-raised after the abort, so it reaches the caller's own
// recovery rather than being turned into an error.
//
// A commit that fails is aborted too, which is franz-go's recovery for every
// failed commit: one that never sent the end (its flush ran out of ctx, or a
// record of the transaction failed) left the transaction open, and one whose
// end went unconfirmed is fenced by the abort. The commit's own error is the
// one returned.
//
// An abort that fails even after [Tx.Abort]'s retry leaves the producer in
// the transaction. Do still returns the reason for the abort, Ended reports
// false, and Abort on this handle is the way out.
func (t *Tx) Do(ctx context.Context, fn func(context.Context) error) (err error) {
	if err := t.begin(ctx); err != nil {
		return err
	}
	committed := false
	defer func() {
		if committed {
			return
		}
		// An abort failure must not hide the reason for the abort, so it
		// is only reported when there is nothing else to report.
		if aerr := t.abortDetached(ctx); aerr != nil && err == nil {
			err = aerr
		}
	}()
	// Assigned rather than declared: err is the NAMED return, and the
	// deferred abort reads it to decide whether to report its own failure
	// on top of the reason for the abort.
	err = fn(ctx)
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
// reach. The usual reason Do is aborting is that ctx ended, and an abort run
// on that ctx fails its flush before it sends anything, which leaves the
// producer in the transaction Do exists to close.
func (t *Tx) abortDetached(ctx context.Context) error {
	actx, cancel := context.WithTimeout(context.WithoutCancel(ctx), abortTimeout)
	defer cancel()
	return t.Abort(actx)
}

// Session is Kafka's read-process-write, which is the half of a transaction
// [Tx] cannot reach.
//
// A transaction commits produced records AND the consumer offsets that were
// consumed to make them, together — and franz-go can only do that from ONE
// client that is both the group consumer and the transactional producer
// (kgo.GroupTransactSession). This driver's clients are deliberately split:
// the backend owns the producer and every source owns a group client of its
// own, because a franz-go client consumes at most one group and a backend that
// shared one could offer only one subscription. So there is no iqmq client
// whose native handle is a transact session, and `Tx.CommitOffsets` does not
// exist — a method that always returned ErrUnsupported would be worse than its
// absence (recorded in the maintenance notes).
//
// What this type is, therefore, is the escape hatch made safe: the caller
// builds the session with franz-go — which is the one thing only franz-go can
// build — and Session adds the contract that is easy to get wrong. Publication
// and consumption inside it are the native client's, not iqmq's, which the
// package documentation says plainly rather than pretending otherwise.
type Session struct {
	s *kgo.GroupTransactSession
	// records is the session client's record ledger, nil when it carries
	// none.
	records ledger

	mu    sync.Mutex
	begun bool
}

// WrapSession returns the helper for a session the caller built with
// [github.com/twmb/franz-go/pkg/kgo.NewGroupTransactSession]. The session
// needs [TrackRecords] among its options for a commit to see a record fail.
func WrapSession(s *kgo.GroupTransactSession) (*Session, error) {
	if s == nil {
		return nil, ErrNotKafka
	}
	return &Session{s: s, records: ledgerOf(s.Client())}, nil
}

// Client returns the session's native client, for the fetching and producing
// the caller does inside a transaction.
func (s *Session) Client() *kgo.Client { return s.s.Client() }

// Do runs fn inside one transaction and ends it exactly once, reporting
// whether it committed.
//
// The committed flag is not a formality. franz-go's End aborts rather than
// commits when the group rebalanced while the transaction was open, and it
// reports that by returning committed=false with a NIL error — because
// nothing failed: the records were discarded and the offsets were not
// committed, which is the correct outcome and means the input will be
// processed again. A caller that only checked the error would treat a
// deliberate abort as a successful commit and skip the input it never
// produced from, which is silent data loss.
//
// fn returning an error aborts. A panic aborts and is re-raised.
//
// Every abort runs on a context the caller's cancellation does not reach, as
// [Tx.Do]'s does. franz-go's End returns a done ctx before it sends anything,
// so an abort on the ctx that ended left the transaction open, and every
// later Do failed at Begin for the life of the process.
//
// A commit that fails is aborted too, which is franz-go's recovery for a
// failed End. One refused before it was sent (ctx ended during the flush, or
// a record of the transaction failed) is a definite failure; one whose end
// may have reached the broker reports ErrSettlementUnknown. End itself aborts
// when it cannot commit the transaction's offsets, and once that abort has
// succeeded the failure is definite too, with the code the driver gives the
// offsets' own failure (CodeForbidden for GROUP_AUTHORIZATION_FAILED). As
// [Tx.Begin] does, Do first waits up to 5 seconds for the records the client
// took before the transaction to finish, so only a record of its own refuses
// its commit, and Do refuses to begin with CodeConflict or CodeInvalidConfig
// where Begin does.
//
// A commit whose end franz-go could not confirm (the broker answered the
// EndTxn UNKNOWN_SERVER_ERROR) may have committed, and End aborts in its place
// and reports committed=false with a nil error, as it does a deliberate
// abort. Do reports ErrSettlementUnknown for it instead, as [Tx.Commit] does,
// which needs [TrackRecords] on the session's client: the hook is how Do sees
// that End healed an end. The producer is out of the transaction either way,
// so the next Do can begin.
//
// An abort whose end franz-go could not confirm is retried at once, as
// [Tx.Abort] does. Without the retry the producer stayed in the transaction,
// and every later Do failed at Begin.
func (s *Session) Do(ctx context.Context, fn func(context.Context) error) (committed bool, err error) {
	s.mu.Lock()
	if s.begun {
		s.mu.Unlock()
		return false, ErrAlreadyBegun
	}
	s.begun = true
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		s.begun = false
		s.mu.Unlock()
	}()
	if shared(s.records, s.s.Client()) {
		return false, errSharedLedger
	}
	mark := markBefore(ctx, s.s.Client(), s.records)
	if berr := s.s.Begin(); berr != nil {
		return false, beginRefused(berr)
	}
	ended := false
	defer func() {
		if ended {
			return
		}
		// A panic, an early return or a failed commit: abort, and let
		// the panic through. An abort failure must not hide the reason
		// for the abort.
		if aerr := s.abortDetached(ctx); aerr != nil && err == nil {
			err = aerr
		}
	}()
	err = fn(ctx)
	if err != nil {
		return false, err
	}
	// End flushes as well, but it reports a flush that ran out of ctx the
	// same way as an end that did, and it commits a transaction whose
	// record failed.
	if serr := settle(ctx, s.s.Client(), s.records, mark); serr != nil {
		return false, serr
	}
	since := s.endMark()
	ok, eerr := s.s.End(ctx, kgo.TryCommit)
	if eerr != nil {
		if offsetsNotCommitted(eerr) {
			return false, abortedByEnd(eerr)
		}
		return false, wrap(eerr, endSent(eerr))
	}
	ended = true
	if !ok && s.endReloaded(since) {
		return false, wrap(errEndUnconfirmed, true)
	}
	return ok, nil
}

// errEndUnconfirmed is a session commit franz-go's End could not confirm and
// aborted in its place.
var errEndUnconfirmed = errors.New("iqmq/kafkatx: franz-go could not confirm the session's commit and aborted in its place, so the transaction may have committed")

// endMark is the point [Session.endReloaded] looks for an end after, or zero
// when the session client carries no [TrackRecords].
func (s *Session) endMark() uint64 {
	if l, ok := s.records.(*recordLedger); ok {
		return l.writes.Load()
	}
	return 0
}

// endReloaded reports whether, since mark, the session client wrote an EndTxn
// and then reloaded its producer id.
//
// That is how franz-go's End heals an end it could not confirm: an EndTxn
// commit answered UNKNOWN_SERVER_ERROR may have committed, and End retries it
// as an abort, which reloads the producer id to fence whatever the broker
// still has open. End then reports committed=false with a nil error, the
// answer it gives a deliberate abort, and only the order of the requests tells
// the two apart: the answer to an EndTxn is not visible to a hook. A
// TRANSACTION_ABORTABLE answer, or an abort answered UNKNOWN_SERVER_ERROR, is
// healed the same way and reads as unknown too, which is the safe side. A
// session client without the hook cannot tell.
func (s *Session) endReloaded(mark uint64) bool {
	l, ok := s.records.(*recordLedger)
	if !ok {
		return false
	}
	end := l.endAt.Load()
	return end > mark && l.initAt.Load() > end
}

// abortDetached is [Tx.abortDetached] for the session, with the retry
// [abortRetried] gives the abort of a [Tx].
func (s *Session) abortDetached(ctx context.Context) error {
	actx, cancel := context.WithTimeout(context.WithoutCancel(ctx), abortTimeout)
	defer cancel()
	if _, err := s.s.End(actx, kgo.TryAbort); err != nil {
		if _, rerr := s.s.End(actx, kgo.TryAbort); rerr == nil {
			return nil
		}
		return wrap(err, endSent(err))
	}
	return nil
}

// offsetsNotCommitted reports the failure franz-go's End returns when it
// could not commit the transaction's offsets.
//
// End then aborts rather than commits, and it returns this failure only once
// that abort has succeeded: an abort that failed is reported in its place. So
// the transaction certainly did not commit. franz-go has no sentinel for it,
// and an answer to an EndTxn can carry the same codes, so it is matched by
// its wording. A wording franz-go changes reads as an unknown outcome again,
// which is the safe side.
func offsetsNotCommitted(err error) bool {
	return strings.HasPrefix(err.Error(), "unable to commit transaction offsets")
}

// abortedByEnd is a session commit that End turned into a confirmed abort. It
// is definite, and its code and retryability are the ones the driver gives
// the offsets' failure.
func abortedByEnd(cause error) error {
	code, retryable := definiteCode(cause)
	return &mq.OpError{
		Driver:    "kafka",
		Operation: mq.OpPublish,
		Code:      code,
		Retryable: retryable,
		Err:       fmt.Errorf("iqmq/kafkatx: the session aborted the transaction: %w", cause),
	}
}

// OffsetsOf is a committable offset map built from the deliveries a handler
// processed, for a caller driving offsets itself — inside a [Session], or
// through the native client.
//
// It records the offset AFTER each delivery, which is what Kafka means by a
// committed offset ("the offset of the next record to consume"), and it keeps
// the highest per partition — so a caller may hand it a whole batch in any
// order. Getting that plus-one wrong is the classic Kafka off-by-one: the
// record is reprocessed on every restart, for ever.
func OffsetsOf(ds ...mq.DeliveryInfo) map[string]map[int32]int64 {
	if len(ds) == 0 {
		return nil
	}
	out := make(map[string]map[int32]int64, 1)
	// Indexed rather than ranged by value: mq.DeliveryInfo is 144 bytes and
	// a caller hands this a whole batch.
	for i := range ds {
		d := &ds[i]
		if !d.HasOffset || !d.HasPartition {
			continue
		}
		topic := d.Source
		if topic == "" {
			topic = d.Topic
		}
		if topic == "" {
			continue
		}
		parts := out[topic]
		if parts == nil {
			parts = make(map[int32]int64, 1)
			out[topic] = parts
		}
		if next := d.Offset + 1; next > parts[d.Partition] {
			parts[d.Partition] = next
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// TrackRecords is the franz-go option that lets a commit see a record of its
// transaction fail. It puts a hook on the client that counts the records the
// client produces and the ones that failed, and [Tx.Commit] and [Session.Do]
// refuse the commit when one of the transaction's records failed. The same
// hook notes the order of the client's EndTxn and InitProducerID requests,
// which is how [Session.Do] sees a commit franz-go could not confirm.
//
// The Kafka driver puts the same hook on a transactional producer it opens,
// so a client from [github.com/iqhive/iqmq/drivers/kafka.Open] does not need
// it. A client the caller built does: one given to [NewWithClient] or to the
// driver's OpenWithClient, and the client of a [kgo.GroupTransactSession].
//
// Each client needs a TrackRecords of its own. The option holds one hook, so
// every client built from one option slice shares it, and the hook cannot
// tell their records apart: another client's failure would refuse this
// client's commit, and its unfinished records would hold the commit up. A
// transaction on a client whose option is on another open client is refused
// at Begin and at its commit with CodeInvalidConfig. A client built from the
// same options after the other one closed has the hook to itself.
func TrackRecords() kgo.Opt { return kgo.WithHooks(&recordLedger{}) }

// errSharedLedger refuses a transaction on a client whose [TrackRecords]
// option is on another open client too.
var errSharedLedger = errcode.New(errcode.InvalidConfig,
	"iqmq/kafkatx: one TrackRecords option is on more than one open client, so a commit cannot tell their records apart; give each client a TrackRecords() of its own")

// shared reports a [TrackRecords] ledger that is on an open client other than
// cl, and so counts that client's records with cl's. The driver's own ledger
// is never shared: it puts a new one on each producer it opens.
func shared(records ledger, cl *kgo.Client) bool {
	l, ok := records.(*recordLedger)
	return ok && l.sharedWith(cl)
}

// ledger is a hook on the client that counts its records and their failures.
//
// [TrackRecords] installs one, and the Kafka driver puts its own on a
// transactional producer. They are matched by this method rather than by
// type, because this module is built against published releases of the
// driver, and one older than the ledger has no type to name.
type ledger interface {
	// RecordLedger reports how many records the client has taken, how many
	// of them have finished, how many of those failed, and the latest
	// failure. finished is read before taken, so finished >= taken means
	// every record taken by the time of the call has finished and its
	// failure is counted.
	RecordLedger() (taken, finished, failed uint64, last error)
}

// ledgerOf returns the record ledger among the client's hooks, or nil when it
// carries none.
func ledgerOf(cl *kgo.Client) ledger {
	for _, v := range cl.OptValues(kgo.WithHooks) {
		// franz-go keeps its hooks in a slice of an unexported type, so
		// only reflection can walk it.
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

// beginWait bounds how long a transaction waits, as it begins, for the
// records the client took before it.
const beginWait = 5 * time.Second

// markBefore is the ledger's failure count for a transaction about to begin,
// taken once the records the client took before it have finished.
//
// franz-go fails a record it refuses before buffering (one with no topic, one
// produced outside a transaction, one whose ctx ended while Produce waited for
// room) on its promise goroutine, after Produce has returned, and an abort
// does not wait for that: AbortBufferedRecords returns at once when nothing is
// buffered, and EndTransaction sends nothing for a transaction that added no
// partition. A mark taken before such a failure was counted blamed it on the
// next transaction, whose good commit was refused.
//
// The mark is taken before BeginTransaction. A record that fails in between
// was produced outside the transaction and counts against it, which can only
// refuse a good commit; a mark taken after would miss a record of the
// transaction that failed in between, and commit without it.
//
// Records still buffered are not waited for: they belong to a transaction in
// progress, which BeginTransaction refuses, and they finish only when the
// broker answers. The wait is bounded too, because [Tx.Begin] has no ctx and a
// caller's promise that blocks holds up every promise behind it. Giving up
// risks only a refused commit.
func markBefore(ctx context.Context, cl *kgo.Client, records ledger) uint64 {
	if records == nil {
		return 0
	}
	ctx, cancel := context.WithTimeout(ctx, beginWait)
	defer cancel()
	for {
		taken, finished, failed, _ := records.RecordLedger()
		if finished >= taken || cl.BufferedProduceRecords() > 0 {
			return failed
		}
		select {
		case <-ctx.Done():
			_, _, failed, _ = records.RecordLedger()
			return failed
		case <-time.After(time.Millisecond):
		}
	}
}

// recordFailure reports a record that failed since mark, once every record
// the client has taken has finished.
//
// Flush waits for the records franz-go buffered. One it refused before
// buffering, such as a record whose context ended while Produce waited for
// room, has its promise run on a goroutine of its own afterwards, so its
// failure can land after Flush has returned.
//
// A ledger that another open client has too refuses the commit, checked on
// every pass: a record that client holds back would otherwise hold the wait
// until ctx ended.
func recordFailure(ctx context.Context, cl *kgo.Client, records ledger, mark uint64) error {
	if records == nil {
		return nil
	}
	for {
		if shared(records, cl) {
			return errSharedLedger
		}
		taken, finished, failed, last := records.RecordLedger()
		if finished >= taken {
			if failed == mark {
				return nil
			}
			return refused(last)
		}
		select {
		case <-ctx.Done():
			return wrap(ctx.Err(), false)
		case <-time.After(time.Millisecond):
		}
	}
}

// refused is a commit turned down because a record of the transaction failed.
//
// It is definite even when the record's own outcome was not: the transaction
// has not ended, and the abort that follows discards the record whether or
// not the broker appended it. Its code and retryability are the ones the
// driver gives the record's failure, so the caller reads the same answer the
// publication itself reported. A failure the driver has no code for is
// unavailable and retryable, as [wrap] makes one.
func refused(cause error) error {
	code, retryable := definiteCode(cause)
	return &mq.OpError{
		Driver:    "kafka",
		Operation: mq.OpPublish,
		Code:      code,
		Retryable: retryable,
		Err:       fmt.Errorf("iqmq/kafkatx: a record of the transaction failed, so the commit was refused: %w", cause),
	}
}

// definiteCode is the code and retryability the driver gives err, for a
// failure whose outcome is definite. A code [unmapped] names is unavailable
// and retryable instead, as [wrap] makes a failure the driver cannot name.
func definiteCode(err error) (mq.ErrorCode, bool) {
	var oe *mq.OpError
	if errors.As(kq.BatchOutcome([]error{err})[0], &oe) && !unmapped(oe.Code) {
		return oe.Code, oe.Retryable
	}
	return mq.CodeUnavailable, true
}

// unmapped reports a driver code a definite failure does not take: one that
// says a publication's outcome is unknown, which a refused or aborted
// commit's outcome is not, and CodeUnknown, which says the driver had no code
// for the failure.
func unmapped(code mq.ErrorCode) bool {
	return code == mq.CodeSettlementUnknown || code == mq.CodeAmbiguous || code == mq.CodeUnknown
}

// recordLedger is the hook [TrackRecords] installs. The Kafka driver's own is
// the same, and the two answer RecordLedger alike.
type recordLedger struct {
	taken    atomic.Uint64
	finished atomic.Uint64
	failed   atomic.Uint64

	mu   sync.Mutex
	last error
	// clients is every open client the ledger is on. It is more than one
	// when one TrackRecords option went to several clients.
	clients []*kgo.Client

	// writes counts the EndTxn and InitProducerID requests the client
	// wrote, and endAt and initAt are the count at the latest of each:
	// the order [Session.endReloaded] reads.
	writes atomic.Uint64
	endAt  atomic.Uint64
	initAt atomic.Uint64
}

// compile-time proof of which of franz-go's hooks the ledger takes.
var (
	_ kgo.HookNewClient               = (*recordLedger)(nil)
	_ kgo.HookClientClosed            = (*recordLedger)(nil)
	_ kgo.HookProduceRecordBuffered   = (*recordLedger)(nil)
	_ kgo.HookProduceRecordUnbuffered = (*recordLedger)(nil)
	_ kgo.HookBrokerWrite             = (*recordLedger)(nil)
	_ ledger                          = (*recordLedger)(nil)
)

// OnBrokerWrite numbers the EndTxn and InitProducerID requests the client
// writes. franz-go calls it once a request is written, before it reads the
// answer, so a request written for an End is numbered before End returns.
func (l *recordLedger) OnBrokerWrite(_ kgo.BrokerMetadata, key int16, _ int, _, _ time.Duration, _ error) {
	switch key {
	case kmsg.EndTxn.Int16():
		l.endAt.Store(l.writes.Add(1))
	case kmsg.InitProducerID.Int16():
		l.initAt.Store(l.writes.Add(1))
	}
}

// OnNewClient records a client the ledger is on. franz-go calls it for every
// client built with the option, before the client can produce.
func (l *recordLedger) OnNewClient(cl *kgo.Client) {
	l.mu.Lock()
	l.clients = append(l.clients, cl)
	l.mu.Unlock()
}

// OnClientClosed forgets a client that can take no more records, so a client
// built again from the same options has the ledger to itself.
func (l *recordLedger) OnClientClosed(cl *kgo.Client) {
	l.mu.Lock()
	l.clients = slices.DeleteFunc(l.clients, func(c *kgo.Client) bool { return c == cl })
	l.mu.Unlock()
}

// sharedWith reports whether the ledger is on an open client other than cl.
func (l *recordLedger) sharedWith(cl *kgo.Client) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return slices.ContainsFunc(l.clients, func(c *kgo.Client) bool { return c != cl })
}

// OnProduceRecordBuffered counts a record the client has taken. franz-go
// calls it for every record, including one it then refuses before buffering.
func (l *recordLedger) OnProduceRecordBuffered(*kgo.Record) { l.taken.Add(1) }

// OnProduceRecordUnbuffered counts a record whose promise is about to run.
// The failure is recorded before the record is counted finished, so a reader
// that sees it finished also sees its failure.
func (l *recordLedger) OnProduceRecordUnbuffered(_ *kgo.Record, err error) {
	if err != nil {
		l.mu.Lock()
		l.last = err
		l.mu.Unlock()
		l.failed.Add(1)
	}
	l.finished.Add(1)
}

// RecordLedger implements the ledger.
func (l *recordLedger) RecordLedger() (taken, finished, failed uint64, last error) {
	finished = l.finished.Load()
	taken = l.taken.Load()
	failed = l.failed.Load()
	l.mu.Lock()
	last = l.last
	l.mu.Unlock()
	return taken, finished, failed, last
}

// endSent reports whether a failed EndTransaction may have reached the
// broker, which is what makes its outcome unknown.
//
// franz-go refuses two commits before it sends anything, and both leave the
// transaction open: one whose producer id has failed, which it reports as
// OPERATION_NOT_ATTEMPTED, and one that produced to a topic deleted and
// recreated since, which it reports as TRANSACTION_ABORTABLE and
// UNKNOWN_TOPIC_ID together. No broker answer carries both of those.
func endSent(err error) bool {
	if errors.Is(err, kerr.OperationNotAttempted) {
		return false
	}
	return !errors.Is(err, kerr.TransactionAbortable) || !errors.Is(err, kerr.UnknownTopicID)
}

// wrap maps a franz-go transaction failure onto the model.
//
// Every failure it sees is a PUBLICATION's, which is why there is no operation
// to pass: a transaction's Begin, Commit and Abort are all about the records
// it carries, and the offsets a Session commits are committed as part of the
// same produce. Operation is what a caller reads to know which of its
// own calls failed, and "publish" is the honest answer for all of them.
//
// sentAlready is what decides ambiguity, and it is true for an ending that
// may have reached the broker: the records have been produced and the end
// request may have been written, so a failure to hear the answer is not a
// failure to end the transaction. [endSent] decides it for an ending.
func wrap(err error, sentAlready bool) error {
	if err == nil {
		return nil
	}
	code := mq.CodeOf(err)
	ambiguous := false
	switch {
	case errors.Is(err, kgo.ErrClientClosed):
		code = mq.CodeClosed
	case sentAlready && (errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)):
		code, ambiguous = mq.CodeSettlementUnknown, true
	case sentAlready:
		code, ambiguous = mq.CodeSettlementUnknown, true
	case code == mq.CodeUnknown:
		code = mq.CodeUnavailable
	}
	return &mq.OpError{
		Driver:    "kafka",
		Operation: mq.OpPublish,
		Code:      code,
		Ambiguous: ambiguous,
		Retryable: !ambiguous && code == mq.CodeUnavailable,
		Err:       err,
	}
}

// beginRefused maps a refused BeginTransaction onto the model.
//
// A producer still in another transaction is a conflict, and not a retryable
// one: retrying Begin does not end that transaction, only an abort does. One
// left open by an abort that failed was reported as unavailable and
// retryable, so a caller retried the Begin for ever.
//
// A producer whose id failed for good is not retryable either, and it was
// reported as unavailable and retryable too. franz-go refuses every Begin on
// it, so only a new client can begin a transaction. A producer that another
// one with the same transactional id fenced is a conflict with that producer;
// a failure the driver has a definite code for, such as an authorization
// failure, keeps that code.
func beginRefused(err error) error {
	switch {
	case inTransaction(err):
		return &mq.OpError{
			Driver:    "kafka",
			Operation: mq.OpPublish,
			Code:      mq.CodeConflict,
			Err:       fmt.Errorf("iqmq/kafkatx: the producer is still in another transaction, which has to end before this one begins: %w", err),
		}
	case producerIDFailed(err):
		code := mq.CodeConflict
		var oe *mq.OpError
		if errors.As(kq.BatchOutcome([]error{err})[0], &oe) && !unmapped(oe.Code) {
			code = oe.Code
		}
		return &mq.OpError{
			Driver:    "kafka",
			Operation: mq.OpPublish,
			Code:      code,
			Err:       fmt.Errorf("iqmq/kafkatx: the producer's id failed for good, as it does once another producer with the same transactional id fences it, so no transaction can begin on this client: %w", err),
		}
	}
	return wrap(err, false)
}

// inTransaction reports franz-go's refusal of a Begin on a producer that is
// already in a transaction. franz-go has no sentinel for it, so it is matched
// by its wording; a wording franz-go changes reads as unavailable and
// retryable again.
func inTransaction(err error) bool {
	return strings.Contains(err.Error(), "while already in a transaction")
}

// producerIDFailed reports franz-go's refusal of a Begin on a producer whose
// id has a fatal error it cannot recover from. It is matched by its wording,
// as [inTransaction] is.
func producerIDFailed(err error) bool {
	return strings.Contains(err.Error(), "producer ID has a fatal, unrecoverable error")
}

// The helper's own refusals. The specification names no sentinel for them and errors.Is
// matches by code, so a caller compares mq.CodeOf(err).
var (
	// ErrNotKafka reports a client that is not this repository's Kafka
	// driver.
	ErrNotKafka = errcode.New(errcode.InvalidArgument,
		"iqmq/kafkatx: the client is not a github.com/iqhive/iqmq/drivers/kafka client")
	// ErrNotTransactional reports a client opened without a transactional
	// id, which cannot produce transactionally at all.
	ErrNotTransactional = errcode.New(errcode.InvalidConfig,
		"iqmq/kafkatx: the client was opened without Config.TransactionalID, so its producer is not transactional")
	// ErrAlreadyBegun reports a second Begin on a producer that already has
	// a transaction in progress.
	ErrAlreadyBegun = errcode.New(errcode.Conflict,
		"iqmq/kafkatx: a transaction is already in progress; a Kafka producer has one at a time")
	// ErrNotBegun reports a Commit, Abort or CommitOffsets before Begin.
	ErrNotBegun = errcode.New(errcode.InvalidArgument,
		"iqmq/kafkatx: no transaction has been begun")
	// ErrAlreadyEnded reports a second ending of one transaction.
	ErrAlreadyEnded = errcode.New(errcode.AlreadySettled,
		"iqmq/kafkatx: this transaction has already been committed or aborted")
)
