package iqmq

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/iqhive/iqmq/internal/clock"
	"github.com/iqhive/iqmq/internal/completion"
	"github.com/iqhive/iqmq/internal/errcode"
	"github.com/iqhive/iqmq/internal/lifecycle"
)

// PublisherConfig is what a publisher is opened with.
//
// Like ConsumerConfig it is a struct, which is the winner (ADR-006,
// Struct). Topic is required: the recommended publisher is bound to its
// destination, which is the winner (ADR-021, Bound), so a publisher
// resolves its destination once instead of per call.
type PublisherConfig struct {
	// Topic is the destination every publication through this handle goes
	// to. Message.Topic is ignored: a bound publisher does not re-read its
	// destination per message (rule 4).
	Topic string
	// Confirmation is how strong an acknowledgement a publication waits
	// for. The zero value is ConfirmDefault, which resolves to
	// ConfirmAccepted.
	Confirmation ConfirmationMode
	// Metadata is the metadata mode. The zero value is
	// MetadataNativeStrict.
	Metadata MetadataMode
	// Backpressure is what the publisher does when its own bounded buffer
	// is full. The zero value is BackpressureBlock.
	Backpressure BackpressureMode
	// MaxPendingMessages bounds how many publications the publisher's own
	// buffer holds: the asynchronous publications outstanding at the driver
	// and the messages waiting in the batching buffer. Zero selects
	// DefaultMaxPendingMessages — the specification requires the buffer to be bounded by
	// default, so zero cannot mean unbounded.
	MaxPendingMessages int
	// MaxPendingBytes bounds the same buffer by accumulated body size. Zero
	// means the size does not bound it.
	MaxPendingBytes int64
	// Batching is the publisher's own batching of asynchronous publications
	// The zero value disables it.
	Batching BatchingConfig
	// Callbacks is where an asynchronous publication's callback runs.
	// The zero value is CallbackInline.
	Callbacks CallbackMode
	// Timeout bounds the waits the core itself performs for one
	// publication: the wait for room in a full buffer under
	// BackpressureBlock. It is measured on the client's clock and never
	// wraps the caller's context, because a context per message is what
	// The specification rule 4 forbids; a bound on the broker round trip is the caller's
	// context's job. Zero means the core's waits are bounded only by the
	// caller's context.
	Timeout time.Duration
	// AutoID stamps an identifier on every message that carries none
	// Nil means the publisher generates none, which is the default:
	// the specification's "Publish never generates IDs or timestamps on the hot path
	// unless configured". It writes into the caller's message.
	AutoID IDGenerator
	// Timestamp fills Properties.CreatedAt on every message whose CreatedAt
	// is the zero time, from the client's clock. Like AutoID it writes into
	// the caller's message, and like AutoID it is the only thing that makes
	// the publish path read the clock (rule 4).
	Timestamp bool
	// Receipts asks the driver to report what the broker said about each
	// accepted publication, in place, into the PublishReceipt the message
	// points at. It needs FeatureReceipts and fails at construction
	// with ErrUnsupported without it. Nothing is allocated per publication
	// for it: the receipt's storage is the caller's.
	Receipts bool
	// Requirements are the capabilities the publisher needs, checked before
	// the sink is opened.
	Requirements []Requirement
	// Defaults are the publication controls every publish through this
	// handle starts from; a per-call option overrides one of them.
	Defaults PublishOptions
}

// resolve validates the configuration and fills in every default.
func (cfg PublisherConfig) resolve() (PublisherConfig, error) {
	switch {
	case cfg.Topic == "":
		return cfg, errPublisherNoTopic
	case !cfg.Confirmation.Valid():
		return cfg, errPublisherConfirmation
	case !cfg.Metadata.Valid():
		return cfg, errPublisherMetadata
	case !cfg.Backpressure.Valid():
		return cfg, errPublisherBackpressure
	case !cfg.Callbacks.Valid():
		return cfg, errPublisherCallbacks
	case cfg.MaxPendingMessages < 0 || cfg.MaxPendingBytes < 0:
		return cfg, errPublisherPending
	case cfg.Batching.MaxMessages < 0 || cfg.Batching.MaxBytes < 0 || cfg.Batching.Linger < 0:
		return cfg, errPublisherBatching
	case cfg.Timeout < 0:
		return cfg, errPublisherTimeout
	}
	if err := ValidateTopic(cfg.Topic); err != nil {
		return cfg, err
	}
	cfg.Confirmation = cfg.Confirmation.Resolve()
	return cfg, nil
}

// Publisher is the narrow publication interface, for library authors
// who take a publisher rather than a client. *PublisherHandle implements it
// (ADR-003: concrete handles behind narrow interfaces).
type Publisher interface {
	// Publish admits one message.
	Publish(ctx context.Context, m *Message, opts ...PublishOption) error
	// PublishBatch admits a batch.
	PublishBatch(ctx context.Context, ms []*Message, opts ...PublishOption) error
	// Flush completes the publications already admitted.
	Flush(ctx context.Context) error
	// Close closes the publisher promptly.
	Close() error
}

var _ Publisher = (*PublisherHandle)(nil)

// PublisherHandle is one open publisher, bound to one destination.
//
// Every method is safe for concurrent use. Close and Shutdown are idempotent.
type PublisherHandle struct {
	client *Client
	cfg    PublisherConfig
	// pubSeq numbers publications for the hooks, so a start event and a
	// done event of the same publication can be paired. It is read only
	// when a hook is set.
	pubSeq atomic.Uint64
	sink   Sink
	// batch is the driver's native batch method, or nil when it has none.
	// The assertion happens once, at open — which is what the specification's
	// "type-asserted once at open" means, and why no message's path pays
	// for it.
	batch BatchSink
	// flush is the driver's flush method, or nil when it buffers nothing.
	flush FlushSink
	// async is the driver's native asynchronous publication method, or nil
	// when it has none, in which case PublishAsync is adapted over the
	// mandatory sink. Asserted once, at open.
	async AsyncSink
	// owned is the driver's native ownership-transfer method, or nil when it
	// has none, in which case PublishOwned publishes through the mandatory
	// sink and releases when it returns.
	owned OwnedSink
	// maxBatch is the largest batch the broker accepts, or 0 when it
	// advertises no limit. A larger batch is split and the pieces' results
	// re-aligned with the caller's messages.
	maxBatch int
	// maxBytes and maxHeaders are the limits the specification requires to be enforced
	// before any I/O, compiled into fields at construction. Zero means the
	// backend advertised none.
	maxBytes   int64
	maxHeaders int
	// sup is which per-message controls the backend supports, compiled into
	// booleans at construction (rule 4).
	sup optionSupport

	// receipts is PublisherConfig.Receipts, checked against FeatureReceipts
	// at open so that no publication performs a capability lookup.
	receipts bool
	// maxPending is the resolved MaxPendingMessages, which is both the
	// buffer's bound and the completion table's capacity.
	maxPending int
	// bound is the publisher's bounded buffer.
	bound *pendingBound
	// abandoned is the outcome of a publication still outstanding when the
	// publisher closed: accepted or not, and iqmq cannot tell. It is
	// preallocated per handle, which is the rule for a hot-path
	// *OpError, and it is what makes both IsAmbiguous and
	// CodeOf(...) == CodeSettlementUnknown true of an abandoned publication.
	abandoned *OpError
	// flusher is the batching goroutine, or nil when the publisher does not
	// batch.
	flusher *publishFlusher
	// comp is the completion table of the asynchronous publications, built
	// on the first one. compMu serialises the building and nothing else.
	comp   atomic.Pointer[asyncCompleter]
	compMu sync.Mutex

	life     *lifecycle.Lifecycle
	clock    clock.Clock
	hooks    Hooks
	counters publisherCounters

	// optsPool holds the resolved option sets of publications that named an
	// option. The driver is handed a *PublishOptions, so a resolved set
	// escapes into the interface call and cannot live on the stack; pooling
	// it keeps a call that carries options free of per-message allocation.
	// A publication that names none hands the driver the publisher's own
	// defaults and never comes here.
	optsPool sync.Pool
	// asyncPool holds the per-publication states of PublishAsync, with their
	// closures already bound.
	asyncPool sync.Pool
}

// optionSupport is the capability answer for each per-message control, read
// once so that the specification's "fail before any I/O" costs a branch and not a
// capability lookup.
type optionSupport struct {
	delay    bool
	ttl      bool
	priority bool
	dedup    bool
	maxDelay Limit[int64]
}

// getOptions takes a resolved option set out of the publisher's pool, or
// builds one when the pool is empty. The pool has no New so that a handle
// built without the constructor still works.
func (p *PublisherHandle) getOptions() *PublishOptions {
	o, _ := p.optsPool.Get().(*PublishOptions)
	if o == nil {
		return new(PublishOptions)
	}
	return o
}

// Publish admits one message to the publisher's destination.
//
// The caller keeps ownership: Publish does not retain m, its Key, its Body or
// its header storage after it returns, so all of them may be reused
// immediately. A driver that needs to keep the message copies it.
//
// A control the backend cannot honour — a delay, a TTL, a priority or a dedup
// key it has no native support for — fails with ErrUnsupported before any
// I/O, and a message past an advertised limit fails with ErrMessageTooLarge,
// also before any I/O.
func (p *PublisherHandle) Publish(ctx context.Context, m *Message, opts ...PublishOption) error {
	if m == nil {
		return errNilMessage
	}
	o := &p.cfg.Defaults
	var pooled *PublishOptions
	if len(opts) > 0 {
		pooled = p.getOptions()
		*pooled = p.cfg.Defaults
		if err := pooled.Apply(opts...); err != nil {
			p.optsPool.Put(pooled)
			return err
		}
		o = pooled
	}
	err := p.publishOne(ctx, m, o)
	if pooled != nil {
		p.optsPool.Put(pooled)
	}
	return err
}

// publishOne is Publish with its options already resolved.
func (p *PublisherHandle) publishOne(ctx context.Context, m *Message, o *PublishOptions) error {
	// The gate comes before the validation so that the specification's "after close all
	// ops return ErrClosed" holds without exception: a closed publisher
	// answers for being closed and not for an option it will never send.
	// The validation is still before any I/O, which is what the specification asks —
	// taking a ticket is not I/O.
	t := p.life.Enter(lifecycle.OpPublish)
	if !t.OK() {
		return ErrClosed
	}
	defer p.life.Leave(t)
	p.stamp(m)
	if err := p.check(m, o); err != nil {
		return err
	}
	p.resetReceipt(m)
	bytes := int64(len(m.Body))
	start, seq := p.begin(ctx, 1, bytes)
	err := p.sink.Publish(ctx, m, o)
	if err != nil {
		p.counters.failed.Add(1)
	} else {
		p.counters.published.Add(1)
	}
	p.finish(ctx, start, seq, 1, bytes, err)
	return err
}

// PublishBatch admits a batch of messages.
//
// It returns nil when every message was accepted, a *BatchError whose Errs
// are index-aligned with ms when some were — so errors.Is(err, ErrPartial) is
// true and the caller can see which — and a single error when none were and
// every message has the same outcome: all definitely refused, or all
// ambiguous. A batch none of which was accepted but whose messages' outcomes
// differ — some refused, some that may already be in the broker — is a
// *BatchError too, because no one error can report both: IsAmbiguous is true
// of it, and Errs says which message is which. Nothing is published at all
// when a message or an option fails validation: the specification puts that check before
// any I/O.
//
// A driver with native batching gets the whole batch, split into
// broker-sized pieces and the pieces' results re-aligned with ms; a driver
// without it gets one call per message. Both report the same thing, which is
// what FuzzBatchResults checks.
func (p *PublisherHandle) PublishBatch(ctx context.Context, ms []*Message, opts ...PublishOption) error {
	if len(ms) == 0 {
		return nil
	}
	o := &p.cfg.Defaults
	var pooled *PublishOptions
	if len(opts) > 0 {
		pooled = p.getOptions()
		*pooled = p.cfg.Defaults
		if err := pooled.Apply(opts...); err != nil {
			p.optsPool.Put(pooled)
			return err
		}
		o = pooled
	}
	err := p.publishBatch(ctx, ms, o)
	if pooled != nil {
		p.optsPool.Put(pooled)
	}
	return err
}

// publishBatch is PublishBatch with its options already resolved.
func (p *PublisherHandle) publishBatch(ctx context.Context, ms []*Message, o *PublishOptions) error {
	t := p.life.Enter(lifecycle.OpPublish)
	if !t.OK() {
		return ErrClosed
	}
	defer p.life.Leave(t)
	// Nothing is published at all when any message fails validation: the specification
	// puts the check before any I/O, and a batch that was half refused
	// before it started would have no index alignment to report.
	var bytes int64
	for _, m := range ms {
		if m == nil {
			return errNilMessage
		}
		p.stamp(m)
		if err := p.check(m, o); err != nil {
			return err
		}
		p.resetReceipt(m)
		bytes += int64(len(m.Body))
	}
	start, seq := p.begin(ctx, len(ms), bytes)
	var err error
	if p.batch != nil {
		err = p.nativeBatch(ctx, ms, o)
	} else {
		err = p.loopBatch(ctx, ms, o)
	}
	p.finish(ctx, start, seq, len(ms), bytes, err)
	return err
}

// nativeBatch publishes through the driver's own batch method, splitting the
// batch into broker-sized pieces.
//
// The split is where a batch loses a message: a piece's results are the
// piece's, and they have to be written back at the piece's offset in the
// caller's batch. FuzzBatchResults publishes batches larger than the
// broker's maximum for exactly this reason.
func (p *PublisherHandle) nativeBatch(ctx context.Context, ms []*Message, o *PublishOptions) error {
	limit := p.maxBatch
	if limit <= 0 || limit > len(ms) {
		limit = len(ms)
	}
	var errs []error
	var first error
	for lo := 0; lo < len(ms); lo += limit {
		hi := min(lo+limit, len(ms))
		err := p.batch.PublishBatch(ctx, ms[lo:hi], o)
		if err == nil {
			continue
		}
		if first == nil {
			first = err
		}
		if errs == nil {
			errs = make([]error, len(ms))
		}
		p.alignPiece(err, errs[lo:hi])
		if fatal := firstFatal(errs[lo:hi]); fatal != nil {
			// The sink cannot continue. Everything the batch did not reach
			// is refused, so the results stay aligned with the messages:
			// leaving them nil would report success for a message nothing
			// was ever told about.
			fillNotAttempted(errs[hi:], p.notAttempted(fatal))
			break
		}
	}
	return p.batchOutcome(ms, errs, first)
}

// alignPiece writes the driver's answer for one native piece into dst, the
// piece's slice of the batch's results.
//
// A *BatchError aligned with the piece that blames at least one of its
// messages is the driver's per-item answer and is copied entry by entry. If an
// error around it says the outcome is unknown -- an ambiguous *OpError the
// driver wrapped it in -- an entry that reads "accepted" is not accepted: it
// is ambiguous, and so is what the core writes there (the driver's definite
// refusals stay as they are). Any other answer is a whole-piece one and every
// message gets it, with one exception: a *BatchError the core cannot use per
// item -- one that blames no message, or is not aligned with the piece, or is
// reachable only through another error -- is never handed on. A caller that
// read it per item would be told that messages were accepted that nobody
// knows were, so every message of the piece gets the core's own AMBIGUOUS
// answer instead.
func (p *PublisherHandle) alignPiece(err error, dst []error) {
	be, aroundAmbiguous := batchErrorIn(err)
	if be != nil && len(be.Errs) == len(dst) && be.Failed() > 0 {
		copy(dst, be.Errs)
		if aroundAmbiguous {
			var unknown error
			for i := range dst {
				if dst[i] == nil {
					if unknown == nil {
						unknown = p.unattributed(err)
					}
					dst[i] = unknown
				}
			}
		}
		return
	}
	fill := err
	var any *BatchError
	if errors.As(err, &any) {
		fill = p.unattributed(err)
	}
	for i := range dst {
		dst[i] = fill
	}
}

// batchErrorIn finds a *BatchError along err's unwrap chain -- through
// single-error wrappers only -- and reports whether an error around it says
// the outcome is unknown: the outermost *OpError decides, as it does for
// IsAmbiguous, and a code-carrying error with one of the two ambiguous codes
// counts when no *OpError has decided.
func batchErrorIn(err error) (be *BatchError, aroundAmbiguous bool) {
	decided := false
	for err != nil {
		switch e := err.(type) { //nolint:errorlint // the chain is walked by hand: the wrappers above the batch are the question
		case *BatchError:
			return e, aroundAmbiguous
		case *OpError:
			if !decided {
				decided = true
				aroundAmbiguous = e.Ambiguous || ambiguousCode(e.Code)
			}
		case errcode.Coder:
			if !decided && ambiguousCode(ErrorCode(e.Code())) {
				aroundAmbiguous = true
			}
		}
		u, ok := err.(interface{ Unwrap() error })
		if !ok {
			return nil, false
		}
		err = u.Unwrap()
	}
	return nil, false
}

// unattributed is the core's answer for the messages of a piece whose driver
// answer cannot be attributed to them: a CodeAmbiguous *OpError whose cause
// renders the driver's error but does not unwrap to it, so that errors.As
// cannot reach a *BatchError that would misreport them.
func (p *PublisherHandle) unattributed(err error) error {
	return &OpError{
		Driver: p.abandoned.Driver, Operation: OpPublish, Topic: p.abandoned.Topic,
		Code: CodeAmbiguous, Ambiguous: true, Err: unattributedCause{err},
	}
}

// unattributedCause renders a driver error it deliberately does not unwrap
// to. See unattributed.
type unattributedCause struct{ cause error }

func (u unattributedCause) Error() string {
	return "iqmq: the driver's answer for this batch cannot be attributed to its messages, so which it took is unknown: " +
		u.cause.Error()
}

// Is keeps what the driver's error carries discoverable through errors.Is --
// the context.Canceled and context.DeadlineExceeded above all -- without an
// Unwrap errors.As could follow to the unusable *BatchError. It answers no to
// ErrPartial: the answer is not a per-item one, and a caller told "partial"
// would look for a *BatchError that is not there.
func (u unattributedCause) Is(target error) bool {
	if CodeOf(target) == CodePartial {
		return false
	}
	return errors.Is(u.cause, target)
}

// notAttempted is the answer for the messages a fatal error kept from the
// driver: DEFINITE, because they never left the process, and RETRYABLE, because
// a message that never left is safe to send again. Its code is the one
// errcode.NotAttempted chooses -- the fatal error's own, so errors.Is against
// ErrClosed, context.Canceled or context.DeadlineExceeded still holds, and
// never one whose meaning is an unknown outcome. They used to get the fatal
// error itself, and a post-send deadline is ambiguous, so a message that was
// never sent read "may be in the broker" and a caller that republishes only
// the definite refusals never published it.
// drivers/activemq/stomp's stopped loop answers through the same rule.
func (p *PublisherHandle) notAttempted(err error) error {
	return &OpError{
		Driver: p.abandoned.Driver, Operation: OpPublish, Topic: p.abandoned.Topic,
		Code:      ErrorCode(errcode.NotAttempted(errcode.Code(CodeOf(err)))),
		Retryable: true,
		Err:       errcode.ErrNotAttempted,
	}
}

// fillNotAttempted gives every entry of dst the not-attempted answer.
func fillNotAttempted(dst []error, na error) {
	for i := range dst {
		dst[i] = na
	}
}

// loopBatch is the adapter the specification promises a driver that cannot batch: one native
// call per message, reporting exactly what a native batch would have
// (the winner, ADR-021).
func (p *PublisherHandle) loopBatch(ctx context.Context, ms []*Message, o *PublishOptions) error {
	var errs []error
	var first error
	for i, m := range ms {
		err := p.sink.Publish(ctx, m, o)
		if err == nil {
			continue
		}
		if first == nil {
			first = err
		}
		if errs == nil {
			errs = make([]error, len(ms))
		}
		errs[i] = err
		if isFatalBatch(err) {
			fillNotAttempted(errs[i+1:], p.notAttempted(err))
			break
		}
	}
	return p.batchOutcome(ms, errs, first)
}

// batchOutcome turns the per-message results into the batch's one error and
// counts the outcome.
//
// The three answers are the whole contract: nil means every message was
// accepted, a *BatchError means some were -- or none were and their outcomes
// differ -- and names which, and a single error means none were and that one
// error is every message's outcome (noneAccepted). The specification forbids
// constructing a BatchError on the success path, which is why errs stays nil
// until something fails.
func (p *PublisherHandle) batchOutcome(ms []*Message, errs []error, first error) error {
	if errs == nil {
		p.counters.published.Add(int64(len(ms)))
		return nil
	}
	failed := 0
	for _, err := range errs {
		if err != nil {
			failed++
		}
	}
	switch {
	case failed == 0:
		// The driver reported a failure and blamed no message. alignPiece
		// keeps both paths from reaching here -- it gives every message of
		// such a piece an answer -- and the arm stays so that the answer
		// would still be right: never success, and never a *BatchError whose
		// entries read "accepted".
		p.counters.failed.Add(int64(len(ms)))
		return p.unattributed(first)
	case failed == len(ms):
		p.counters.failed.Add(int64(failed))
		return noneAccepted(errs)
	default:
		p.counters.published.Add(int64(len(ms) - failed))
		p.counters.failed.Add(int64(failed))
		return &BatchError{Errs: errs}
	}
}

// noneAccepted is the result of a batch none of whose messages was accepted.
// errs is index-aligned with the batch and has no nil entry.
//
// "A single error when none were accepted" is only honest when that one error
// is every message's outcome, and the specification gives a message one of three:
// accepted, definitely rejected, ambiguous. When every message was refused,
// or every message is ambiguous, the first error says so for all of them and
// is returned unwrapped, so a caller whose whole call was refused keeps the
// handling it has for a single publication. When some were refused and some
// may already be in the broker, no one error can say both: the first used to
// be returned regardless, so a batch whose first message was refused and
// whose last was ambiguous reported IsAmbiguous = false, and a caller who
// republished it published the last message twice (the specification names that a
// disqualifying defect). The answer is then the index-aligned *BatchError,
// which IsAmbiguous reports as ambiguous and whose Errs say which is which.
//
// driverkit.CollapseBatch is the same rule for a driver, which cannot import
// this one.
func noneAccepted(errs []error) error {
	ambiguous := IsAmbiguous(errs[0])
	for _, err := range errs[1:] {
		if IsAmbiguous(err) != ambiguous {
			return &BatchError{Errs: errs}
		}
	}
	return errs[0]
}

// firstFatal is the first answer of a native piece that means the sink cannot
// continue, or nil. It reads the piece's per-item answers rather than the
// driver's one error: a piece whose messages failed with mixed outcomes comes
// back as a *BatchError, CodePartial, which isFatalBatch cannot see into, so a
// close or a deadline INSIDE it did not stop the batch and the next piece was
// offered to a sink that could not take it.
func firstFatal(piece []error) error {
	for _, err := range piece {
		if err != nil && isFatalBatch(err) {
			return err
		}
	}
	return nil
}

// isFatalBatch reports whether an error means the sink cannot continue with
// the rest of a batch: a closed sink or a finished context stops it, while a
// refused message is one message's outcome.
func isFatalBatch(err error) bool {
	switch CodeOf(err) {
	case CodeClosed, CodeCanceled, CodeDeadlineExceeded:
		return true
	default:
		return false
	}
}

// check enforces the and the specification's "before any I/O": a control the backend
// cannot honour, and a message past an advertised limit, fail without the
// broker being told anything.
//
// Every answer it needs was compiled into a field at construction, so it
// performs no capability lookup and no allocation.
func (p *PublisherHandle) check(m *Message, o *PublishOptions) error {
	if err := p.checkMessage(m); err != nil {
		return err
	}
	return p.checkOptions(o)
}

// checkMessage is check's half that depends on the message: the advertised
// limits, enforced before any I/O.
func (p *PublisherHandle) checkMessage(m *Message) error {
	if p.maxBytes > 0 && int64(len(m.Body)) > p.maxBytes {
		return ErrMessageTooLarge
	}
	if p.maxHeaders > 0 && m.Headers.Len() > p.maxHeaders {
		return ErrMessageTooLarge
	}
	return nil
}

// checkOptions is check's half that depends only on the option set, which is
// what CompileOptions performs once for a whole loop.
func (p *PublisherHandle) checkOptions(o *PublishOptions) error {
	if o.Delay != 0 || !o.DeliverAt.IsZero() {
		if !p.sup.delay {
			return ErrUnsupported
		}
		if p.sup.maxDelay.Known && p.pastMaxDelay(o) {
			return ErrUnsupported
		}
	}
	if o.TTL != 0 && !p.sup.ttl {
		return ErrUnsupported
	}
	if o.PrioritySet && !p.sup.priority {
		return ErrUnsupported
	}
	if o.DedupKey != "" && !p.sup.dedup {
		return ErrUnsupported
	}
	return nil
}

// pastMaxDelay reports whether o holds a publication back further than the
// backend's advertised ceiling. A DeliverAt is the same hold stated as an
// instant, so it is measured from the client's clock; the clock is read only
// for an option set that has one.
func (p *PublisherHandle) pastMaxDelay(o *PublishOptions) bool {
	if int64(o.Delay) > p.sup.maxDelay.Value {
		return true
	}
	if o.DeliverAt.IsZero() {
		return false
	}
	return int64(o.DeliverAt.Sub(p.clock.Now())) > p.sup.maxDelay.Value
}

// resetReceipt empties the receipt slot a message points at, so that a
// publication cannot read the previous one's answer and a failed publication
// leaves no receipt at all. It costs one branch for a publisher that was not
// configured with Receipts, and one more for a message that carries no slot.
func (p *PublisherHandle) resetReceipt(m *Message) {
	if p.receipts && m.Receipt != nil {
		m.Receipt.reset()
	}
}

// begin counts a publication as pending and reports it to the start hook. It
// returns the time the publication started, which is the zero time when no
// hook is watching: rule 4 both forbid reading the clock for
// an observer that is not there.
func (p *PublisherHandle) begin(ctx context.Context, messages int, bytes int64) (time.Time, uint64) {
	p.counters.pending.Add(int64(messages))
	watched := p.hooks.OnPublishStart != nil || p.hooks.OnPublishDone != nil
	var seq uint64
	if watched {
		// Numbered only for an observer that is there: the counter is one
		// atomic add, and the disabled path pays neither it nor the
		// clock below.
		seq = p.pubSeq.Add(1)
	}
	if p.hooks.OnPublishStart != nil {
		p.hooks.OnPublishStart(ctx, PublishStartEvent{
			Seq:      seq,
			Topic:    p.cfg.Topic,
			Messages: messages,
			Bytes:    bytes,
		})
	}
	if p.hooks.OnPublishDone == nil {
		return time.Time{}, seq
	}
	return p.clock.Now(), seq
}

// finish is begin's other half: it takes the publication out of the pending
// count and reports the outcome. The count is added in one place and taken
// away in one place, so a publication cannot leave it standing.
func (p *PublisherHandle) finish(ctx context.Context, start time.Time, seq uint64, messages int, bytes int64, err error) {
	p.counters.pending.Add(-int64(messages))
	if p.hooks.OnPublishDone == nil {
		return
	}
	p.hooks.OnPublishDone(ctx, PublishDoneEvent{
		Seq:      seq,
		Topic:    p.cfg.Topic,
		Messages: messages,
		Bytes:    bytes,
		Duration: p.clock.Now().Sub(start),
		Err:      err,
	})
}

// Flush completes the publications this publisher has already admitted
// the batching buffer goes out, the driver's own buffer is flushed,
// and the asynchronous publications outstanding at the driver are waited for.
// A publisher that batches nothing over a driver that buffers nothing has
// nothing to do and reports nil.
//
// It waits, so ctx is how a caller bounds it — and it must not be called from
// inside a completion callback: the wait includes that callback's own
// publication, which cannot complete until the callback returns, so such a
// call waits out ctx and reports its error. It is the same reason the specification asks
// that a callback not block.
func (p *PublisherHandle) Flush(ctx context.Context) error {
	if p.life.Closing() {
		return ErrClosed
	}
	return p.flushAdmitted(ctx)
}

// flushAdmitted completes everything the publisher has admitted, in the only
// order that can work: the batching buffer goes out first, because a message
// still in it has not reached the driver at all; then the driver's own buffer
// is flushed; and only then are the asynchronous completions waited for,
// because a driver that buffers does not complete a publication until its
// buffer has gone out.
//
// It is Flush's body and the Flushing phase's step, so a graceful shutdown
// and an explicit Flush do the same thing.
func (p *PublisherHandle) flushAdmitted(ctx context.Context) error {
	if p.flusher != nil {
		p.flusher.flush()
	}
	var err error
	if p.flush != nil {
		err = p.flush.Flush(ctx)
	}
	if t := p.comp.Load(); t != nil {
		if derr := t.c.Drain(ctx); derr != nil && err == nil {
			err = derr
		}
	}
	return err
}

// Stats returns what this publisher has counted.
func (p *PublisherHandle) Stats() PublisherStats { return p.counters.snapshot() }

// Topic is the destination this publisher is bound to.
func (p *PublisherHandle) Topic() string { return p.cfg.Topic }

// Shutdown closes the publisher gracefully, bounded by ctx:
// it stops accepting publications, flushes what it has admitted, and then
// closes the sink. It is idempotent.
func (p *PublisherHandle) Shutdown(ctx context.Context) error {
	err := p.life.Shutdown(ctx)
	p.client.forget(p)
	return err
}

// Close closes the publisher promptly: it stops accepting
// publications and closes the sink without flushing. It is idempotent and
// safe for concurrent use.
func (p *PublisherHandle) Close() error {
	err := p.life.Close()
	p.client.forget(p)
	return err
}

// closed reports whether the handle is closing or closed.
func (p *PublisherHandle) closed() bool { return p.life.Closing() }

// flushStep is the Flushing phase's step: it completes what the publisher
// admitted before the gate was barred.
//
// The gate is what makes it sound. Flushing phase runs after
// OpPublish's barrier has been drained, so no publication can be admitted
// into the batching buffer while this is emptying it — which is the defect
// class that shipped three times in the engine: a record placed into a
// buffer that a drain has already walked is never released and never
// reported.
func (p *PublisherHandle) flushStep(ctx context.Context) error {
	return p.flushAdmitted(ctx)
}

// closeStep is the Closed phase's step: it stops the flusher, settles
// everything the publisher still holds and closes the sink.
//
// The order is the one the promises need. The flusher stops first and settles
// whatever never left the batching buffer with ErrClosed, which is the truth
// for a message the driver was never told about. The completion table closes
// second and completes every publication still outstanding at the driver, so
// the specification's "the callback runs exactly once" survives a close. The sink closes
// last, because a driver may complete a publication from inside its own
// Close.
//
// Every callback has therefore run by the time this returns, and Close waits
// for the phase run, so no callback can arrive after Close returns.
func (p *PublisherHandle) closeStep() error {
	if p.flusher != nil {
		p.flusher.stop()
	}
	if t := p.comp.Load(); t != nil {
		_ = t.c.Close()
	}
	return p.sink.Close()
}

// newPublisher opens a publisher over the backend's sink. It is
// Client.Publisher's body.
func newPublisher(ctx context.Context, cl *Client, cfg PublisherConfig) (*PublisherHandle, error) {
	resolved, err := cfg.resolve()
	if err != nil {
		return nil, err
	}
	if err := cl.caps.Require(resolved.Requirements...); err != nil {
		return nil, err
	}
	caps := cl.caps
	if resolved.Receipts && !caps.Has(FeatureReceipts) {
		// the rule, checked once and before anything is opened: a
		// publisher asking for receipts from a driver that reports none
		// would publish happily and fill nothing in.
		return nil, ErrUnsupported
	}
	counters, err := newPublisherCounters()
	if err != nil {
		return nil, err
	}
	sink, err := cl.backend.OpenSink(ctx, SinkConfig{
		Topic:        resolved.Topic,
		Confirmation: resolved.Confirmation,
		Metadata:     resolved.Metadata,
		Receipts:     resolved.Receipts,
	})
	if err != nil {
		return nil, err
	}
	if sink == nil {
		return nil, errDriverNilSink
	}
	limits := caps.Limits()
	p := &PublisherHandle{
		client: cl,
		cfg:    resolved,
		sink:   sink,
		sup: optionSupport{
			delay:    caps.Has(FeatureScheduledPublish),
			ttl:      caps.Has(FeatureMessageTTL),
			priority: caps.Has(FeaturePriority),
			dedup:    caps.Has(FeatureDeduplication),
			maxDelay: Limit[int64]{Value: int64(limits.MaxDelay.Value), Known: limits.MaxDelay.Known},
		},
		clock:    cl.clock,
		hooks:    cl.hooks,
		counters: counters,
	}
	if limits.MaxMessageBytes.Known {
		p.maxBytes = limits.MaxMessageBytes.Value
	}
	if limits.MaxHeaderCount.Known {
		p.maxHeaders = limits.MaxHeaderCount.Value
	}
	if limits.MaxBatchMessages.Known {
		p.maxBatch = limits.MaxBatchMessages.Value
	}
	// The optional interfaces, asserted once — here — and remembered
	// as fields, so that no message's path performs a type assertion
	// (the winner, ADR-021).
	if bs, ok := sink.(BatchSink); ok {
		p.batch = bs
	}
	if fs, ok := sink.(FlushSink); ok {
		p.flush = fs
	}
	if as, ok := sink.(AsyncSink); ok {
		p.async = as
	}
	if os, ok := sink.(OwnedSink); ok {
		p.owned = os
	}
	p.receipts = resolved.Receipts
	p.abandoned = &OpError{
		Driver:    cl.info.Name,
		Operation: OpPublish,
		Topic:     resolved.Topic,
		Code:      CodeSettlementUnknown,
		Ambiguous: true,
		Err:       completion.ErrAbandoned,
	}
	p.bound = newPendingBound(resolved, cl.clock)
	p.maxPending = p.bound.maxMsgs
	if resolved.Batching.enabled() {
		if p.flusher, err = newPublishFlusher(p, resolved.Batching, cl.clock); err != nil {
			return nil, errors.Join(err, sink.Close())
		}
	}
	p.life = lifecycle.New(lifecycle.Config{
		Clock:        cl.clock,
		DrainTimeout: cl.drainTimeout,
		Steps: lifecycle.Steps{
			StopAccept:  p.bound.close,
			Flush:       p.flushStep,
			CloseNative: p.closeStep,
		},
	})
	return p, nil
}

// The publisher's construction failures. The specification names no sentinel for an
// invalid configuration; a caller compares CodeOf(err) against
// CodeInvalidConfig.
var (
	errPublisherNoTopic      = errcode.New(errcode.InvalidConfig, "iqmq: PublisherConfig.Topic is required")
	errPublisherConfirmation = errcode.New(errcode.InvalidConfig, "iqmq: PublisherConfig.Confirmation is not a confirmation mode")
	errPublisherMetadata     = errcode.New(errcode.InvalidConfig, "iqmq: PublisherConfig.Metadata is not a metadata mode")
	errNilMessage            = errcode.New(errcode.InvalidArgument, "iqmq: message is nil")
	errPublisherBackpressure = errcode.New(errcode.InvalidConfig, "iqmq: PublisherConfig.Backpressure is not a backpressure mode")
	errPublisherCallbacks    = errcode.New(errcode.InvalidConfig, "iqmq: PublisherConfig.Callbacks is not a callback mode")
	errPublisherPending      = errcode.New(errcode.InvalidConfig, "iqmq: PublisherConfig.MaxPendingMessages and MaxPendingBytes must not be negative")
	errPublisherBatching     = errcode.New(errcode.InvalidConfig, "iqmq: PublisherConfig.Batching fields must not be negative")
	errPublisherTimeout      = errcode.New(errcode.InvalidConfig, "iqmq: PublisherConfig.Timeout must not be negative")
)
