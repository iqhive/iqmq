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
//     read-process-write pipeline duplicates a whole batch.
//
// # Requirements
//
// The client must have been opened with [github.com/iqhive/iqmq/drivers/kafka.Config.TransactionalID]
// set, and its consumers with ReadCommitted when they read a transactional
// topic. A transactional producer cannot use the driver's bounded-delivery
// default — franz-go refuses the cancellation option together with a
// transactional id — so a publication inside a transaction is unbounded, and
// the caller's own context is what ends it.
package kafkatx

import (
	"context"
	"errors"
	"sync"

	"github.com/twmb/franz-go/pkg/kgo"

	mq "github.com/iqhive/iqmq"
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

	mu    sync.Mutex
	begun bool
	ended bool
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
	return &Tx{cl: cl}, nil
}

// NewWithClient is [New] for a caller that already holds the native client,
// which is what a program composing franz-go and iqmq has.
func NewWithClient(cl *kgo.Client) (*Tx, error) {
	if cl == nil {
		return nil, ErrNotKafka
	}
	if !transactional(cl) {
		return nil, ErrNotTransactional
	}
	return &Tx{cl: cl}, nil
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
func (t *Tx) Begin() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	switch {
	case t.ended:
		return ErrAlreadyEnded
	case t.begun:
		return ErrAlreadyBegun
	}
	if err := t.cl.BeginTransaction(); err != nil {
		return wrap(err, false)
	}
	t.begun = true
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
func (t *Tx) Commit(ctx context.Context) error { return t.end(ctx, kgo.TryCommit) }

// Abort ends the transaction and discards its records.
//
// It is what a handler does when the work failed: the records produced inside
// the transaction are never visible to a read-committed consumer, and the
// offsets are not committed, so the input is consumed again.
func (t *Tx) Abort(ctx context.Context) error { return t.end(ctx, kgo.TryAbort) }

// end performs the one ending this transaction gets.
func (t *Tx) end(ctx context.Context, commit kgo.TransactionEndTry) error {
	t.mu.Lock()
	switch {
	case !t.begun:
		t.mu.Unlock()
		return ErrNotBegun
	case t.ended:
		t.mu.Unlock()
		return ErrAlreadyEnded
	}
	t.ended = true
	t.mu.Unlock()
	if err := t.cl.EndTransaction(ctx, commit); err != nil {
		// The records have been produced and the end has been sent, so
		// every failure here is post-send: the transaction may have
		// ended the way it was asked to.
		return wrap(err, true)
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
func (t *Tx) Do(ctx context.Context, fn func(context.Context) error) (err error) {
	if err := t.Begin(); err != nil {
		return err
	}
	committed := false
	defer func() {
		if committed {
			return
		}
		// An abort failure must not hide the reason for the abort, so it
		// is only reported when there is nothing else to report.
		if aerr := t.Abort(ctx); aerr != nil && err == nil {
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
	committed = true
	return t.Commit(ctx)
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

	mu    sync.Mutex
	begun bool
}

// WrapSession returns the helper for a session the caller built with
// [github.com/twmb/franz-go/pkg/kgo.NewGroupTransactSession].
func WrapSession(s *kgo.GroupTransactSession) (*Session, error) {
	if s == nil {
		return nil, ErrNotKafka
	}
	return &Session{s: s}, nil
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
	if berr := s.s.Begin(); berr != nil {
		return false, wrap(berr, false)
	}
	try := kgo.TryCommit
	ended := false
	defer func() {
		if ended {
			return
		}
		// A panic or an early return: abort, and let the panic through.
		if _, aerr := s.s.End(ctx, kgo.TryAbort); aerr != nil && err == nil {
			err = wrap(aerr, true)
		}
	}()
	err = fn(ctx)
	if err != nil {
		try = kgo.TryAbort
	}
	ended = true
	ok, eerr := s.s.End(ctx, try)
	if eerr != nil {
		if err == nil {
			err = wrap(eerr, true)
		}
		return false, err
	}
	return ok && try == kgo.TryCommit, err
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

// wrap maps a franz-go transaction failure onto the model.
//
// Every failure it sees is a PUBLICATION's, which is why there is no operation
// to pass: a transaction's Begin, Commit and Abort are all about the records
// it carries, and the offsets a Session commits are committed as part of the
// same produce. Operation is what a caller reads to know which of its
// own calls failed, and "publish" is the honest answer for all of them.
//
// sentAlready is what decides ambiguity, and it is true for every ending: the
// records have been produced and the end request has been written, so a
// failure to hear the answer is not a failure to end the transaction.
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
