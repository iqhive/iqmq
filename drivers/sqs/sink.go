package sqs

import (
	"context"
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/internal/errcode"
)

// The sink implements the mandatory publication surface and the optional
// native-batch one.
//
// It implements neither AsyncSink nor OwnedSink, deliberately. Both would be a
// goroutine pool and a copy in front of the same synchronous HTTP request the
// SDK makes, and the answer (ADR-021, Optional) is that the CORE
// supplies those loops for a driver that does not: a driver adding its own
// would be the second owner of a policy the specification gives to one.
var (
	_ mq.Sink      = (*sink)(nil)
	_ mq.BatchSink = (*sink)(nil)
)

// sink is one bound publisher: the resolved queue URL and the encoder for the
// publisher's metadata mode.
type sink struct {
	b     *Backend
	cfg   mq.SinkConfig
	enc   *encoder
	url   string
	nonce uint64
	seq   atomic.Uint64
	once  sync.Once
}

// Publish admits one message.
//
// It retains nothing: the encoding copies the body, the key and every header
// into the request the SDK serialises, so a caller may reuse all of them the
// moment this returns.
func (s *sink) Publish(ctx context.Context, m *mq.Message, o *mq.PublishOptions) error {
	if s.b.Closed() {
		return mq.ErrClosed
	}
	enc, err := s.enc.encode(m, o)
	if err != nil {
		return s.fail(err, false)
	}
	in := &sqs.SendMessageInput{
		QueueUrl:          aws.String(s.url),
		MessageBody:       aws.String(enc.Body),
		MessageAttributes: enc.Attrs,
		DelaySeconds:      enc.Delay,
	}
	group, dedup, fifo := s.fifo(enc)
	if fifo {
		in.MessageGroupId = aws.String(group)
	}
	if dedup != "" {
		in.MessageDeduplicationId = aws.String(dedup)
	}
	callCtx, cancel := s.b.call(ctx)
	defer cancel()
	if err := callCtx.Err(); err != nil {
		// Done before the request was made, so nothing was sent: a
		// definite failure, which the same error from the SDK is not.
		return s.fail(err, false)
	}
	out, err := s.b.api.SendMessage(callCtx, in)
	if err != nil {
		// The request has left the socket, so a transport failure here is
		// AMBIGUOUS: the service may have stored the message and lost the
		// reply. The specification makes that distinction the driver, and reporting
		// it as a definite failure would make a duplicate look like a loss.
		return s.fail(err, true)
	}
	s.receipt(m, aws.ToString(out.MessageId), aws.ToString(out.SequenceNumber))
	return nil
}

// PublishBatch admits a batch.
//
// Two limits shape it, both the service's: ten entries per call, and one
// mebibyte for the sum of a call's payloads. So a caller's batch of any size
// becomes a sequence of SendMessageBatch calls, and the whole difficulty is
// what comes back.
//
// **Each entry's success is its own durable confirmation.** The service
// answers with a Successful list and a Failed list, and an entry in neither is
// impossible; an entry in Failed was not stored. So the result is a
// *BatchError whose Errs are index-aligned with ms — the CALLER's slice, not
// the chunk, not the failures. That alignment is the whole contract: a result
// aligned with anything else reports one message's outcome for another, which
// is how a batch publish loses a message silently. The entry identifier sent
// to the service is the message's index in ms, which is what makes the
// correlation exact rather than positional.
func (s *sink) PublishBatch(ctx context.Context, ms []*mq.Message, o *mq.PublishOptions) error {
	if s.b.Closed() {
		return mq.ErrClosed
	}
	if len(ms) == 0 {
		return nil
	}
	errs := make([]error, len(ms))
	encoded := make([]encoded, len(ms))
	for i, m := range ms {
		e, err := s.enc.encode(m, o)
		if err != nil {
			// A message that cannot be encoded never reaches the service,
			// and its error belongs at ITS index. Dropping it from the
			// batch and aligning the result with what was sent is the
			// mistake this whole method is written to avoid.
			errs[i] = s.fail(err, false)
			continue
		}
		encoded[i] = e
	}
	entries := make([]types.SendMessageBatchRequestEntry, 0, MaxBatchEntries)
	at := make([]int, 0, MaxBatchEntries)
	bytes := 0
	// stopped is, on a FIFO queue, the failure of each message group that has
	// had an entry fail. A group is an ordered stream, and a caller retries
	// the entries that failed: a later entry of the same group sent by a
	// later chunk would already be on the queue, ahead of the retried one.
	// So nothing more of a stopped group is sent.
	var stopped map[string]error
	flush := func() {
		if len(entries) == 0 {
			return
		}
		s.send(ctx, entries, at, ms, errs)
		for _, i := range at {
			if errs[i] == nil || !s.b.cfg.FIFO {
				continue
			}
			if stopped == nil {
				stopped = make(map[string]error)
			}
			stopped[encoded[i].GroupID] = errs[i]
		}
		entries, at, bytes = entries[:0], at[:0], 0
	}
	for i := range ms {
		if errs[i] != nil {
			continue
		}
		if len(entries) == MaxBatchEntries || (len(entries) > 0 && bytes+encoded[i].Bytes > int(s.b.cfg.resolve().MaxMessageBytes)) {
			flush()
		}
		if err, ok := stopped[encoded[i].GroupID]; ok {
			errs[i] = s.notSent(err)
			continue
		}
		entries = append(entries, s.entry(i, &encoded[i]))
		at = append(at, i)
		bytes += encoded[i].Bytes
	}
	flush()
	return collapse(errs)
}

// notSent is the answer for an entry held back because an earlier entry of
// its FIFO message group failed with err: DEFINITE, because it never left the
// process, and retryable, with errcode.NotAttempted's code, as the core's
// own not-attempted answer is.
func (s *sink) notSent(err error) error {
	return &mq.OpError{
		Driver:    DriverName,
		Operation: mq.OpPublish,
		Topic:     s.cfg.Topic,
		Code:      mq.ErrorCode(errcode.NotAttempted(errcode.Code(mq.CodeOf(err)))),
		Retryable: true,
		Err:       errcode.ErrNotAttempted,
	}
}

// entry builds one batch entry, identified by the message's index in the
// caller's slice.
func (s *sink) entry(i int, enc *encoded) types.SendMessageBatchRequestEntry {
	e := types.SendMessageBatchRequestEntry{
		Id:                aws.String(strconv.Itoa(i)),
		MessageBody:       aws.String(enc.Body),
		MessageAttributes: enc.Attrs,
		DelaySeconds:      enc.Delay,
	}
	group, dedup, fifo := s.fifo(*enc)
	if fifo {
		e.MessageGroupId = aws.String(group)
	}
	if dedup != "" {
		e.MessageDeduplicationId = aws.String(dedup)
	}
	return e
}

// send performs one SendMessageBatch and writes each entry's outcome to its
// own index in errs.
func (s *sink) send(ctx context.Context, entries []types.SendMessageBatchRequestEntry, at []int, ms []*mq.Message, errs []error) {
	callCtx, cancel := s.b.call(ctx)
	defer cancel()
	if err := callCtx.Err(); err != nil {
		// Done before the request was made: every entry of this chunk
		// failed definitely.
		mapped := s.fail(err, false)
		for _, i := range at {
			errs[i] = mapped
		}
		return
	}
	out, err := s.b.api.SendMessageBatch(callCtx, &sqs.SendMessageBatchInput{
		QueueUrl: aws.String(s.url),
		Entries:  entries,
	})
	if err != nil {
		// The whole call failed, so every entry of THIS chunk is ambiguous
		// and no entry of another chunk is affected.
		mapped := s.fail(err, true)
		for _, i := range at {
			errs[i] = mapped
		}
		return
	}
	// An entry in neither list is the service having answered about
	// something nobody sent. It cannot happen, and if it did, leaving the
	// slot nil would report a success that never happened — so the slots are
	// seeded with a refusal and overwritten by whichever list names them.
	for _, i := range at {
		errs[i] = errNoEntryResult
	}
	for i := range out.Successful {
		e := &out.Successful[i]
		idx, ok := entryIndex(aws.ToString(e.Id), len(ms))
		if !ok {
			continue
		}
		errs[idx] = nil
		s.receipt(ms[idx], aws.ToString(e.MessageId), aws.ToString(e.SequenceNumber))
	}
	for i := range out.Failed {
		e := &out.Failed[i]
		idx, ok := entryIndex(aws.ToString(e.Id), len(ms))
		if !ok {
			continue
		}
		errs[idx] = batchEntryError(mq.OpPublish, s.cfg.Topic,
			aws.ToString(e.Code), aws.ToString(e.Message), e.SenderFault)
	}
}

// entryIndex recovers the caller's index from a batch entry identifier.
//
// An identifier the driver did not send is ignored rather than trusted: the
// alternative is writing an outcome into a slot chosen by a malformed
// response, which is exactly the cross-message swap this whole path exists to
// prevent.
func entryIndex(id string, n int) (int, bool) {
	i, err := strconv.Atoi(id)
	if err != nil || i < 0 || i >= n {
		return 0, false
	}
	return i, true
}

// fifo returns the FIFO parameters, with "" for no deduplication identifier,
// and false on a standard queue.
//
// The deduplication identifier is the part worth reading. A FIFO queue
// requires either ContentBasedDeduplication on the queue or an explicit
// MessageDeduplicationId on every message, so a publication with no DedupKey
// has to be given one — and the choice of what matters:
//
//   - the caller's DedupKey when there is one, which is what makes
//     FeatureDeduplication and FeatureIdempotentPublish conditional claims;
//   - nothing, when the queue was created with ContentBasedDeduplication, so
//     that the service hashes the body as the operator asked;
//   - otherwise a UNIQUE identifier, so the publication is never deduplicated
//     by accident.
//
// The third case is the default and it is the conservative one. Turning
// content-based deduplication on by default would silently discard a second
// publication of an identical body inside a five-minute window, which is a
// message loss disqualifies — and inventing nothing at all would make
// every keyless publication fail.
func (s *sink) fifo(enc encoded) (group, dedup string, fifo bool) {
	if !s.b.cfg.FIFO {
		return "", "", false
	}
	switch {
	case enc.DedupID != "":
		return enc.GroupID, enc.DedupID, true
	case s.b.cfg.ContentBasedDeduplication:
		return enc.GroupID, "", true
	default:
		return enc.GroupID, s.uniqueDedupID(), true
	}
}

// uniqueDedupID is an identifier no other publication of this process will
// use: the sink's nonce and a counter. It reads no clock and allocates one
// string.
func (s *sink) uniqueDedupID() string {
	return "u-" + strconv.FormatUint(s.nonce, 36) + "-" + strconv.FormatUint(s.seq.Add(1), 36)
}

// receipt fills the caller's receipt slot in place, when it asked for one
//
// The storage is the caller's and the core has already zeroed it, so filling
// it in place is what keeps "no result struct allocation on success" true.
func (s *sink) receipt(m *mq.Message, id, sequence string) {
	if !s.cfg.Receipts || m == nil || m.Receipt == nil {
		return
	}
	r := m.Receipt
	r.MessageID = id
	// The service stores the message redundantly before it answers, so the
	// boundary a publication reached is the durable one whatever the
	// publisher asked for. Reporting the RESOLVED mode rather than the
	// stronger one is what the specification asks: the receipt says what this
	// publication waited for.
	r.Confirmation = s.cfg.Confirmation.Resolve()
	r.ConfirmedAt = s.b.now()
	if n, err := strconv.ParseUint(sequence, 10, 64); err == nil {
		r.Sequence = n
	}
}

// fail maps a native publication failure to the model.
//
// sentAlready is the distinction the specification asks for and that no native error type
// can make: a failure before the request left the socket is a clean refusal,
// and the same failure after it may have been accepted by the service. Every
// call site here knows which it is, which is why the flag is a parameter and
// not a guess. The operation is always OpPublish — a sink performs no other —
// so it is not one.
func (s *sink) fail(err error, sentAlready bool) error {
	const op = mq.OpPublish
	if err == nil {
		return nil
	}
	if !sentAlready && mq.CodeOf(err) != mq.CodeUnknown {
		// Already one of this driver's own refusals, built before any I/O.
		// Wrapping it in an *OpError whose code says something else is how a
		// caller comes to retry a publication that can never succeed.
		//
		// Only before the send: the SDK's timeout after it wraps
		// context.DeadlineExceeded, which has a code of its own, and
		// returning it here skipped the ambiguity MapNet adds.
		return err
	}
	return s.b.errs.MapNet(op, s.cfg.Topic, "", err, sentAlready)
}

// Close releases the sink. It is idempotent, and a Publish afterwards
// reports ErrClosed.
func (s *sink) Close() error {
	s.once.Do(func() { s.b.forget(nil, s) })
	return nil
}
