package snssqs

import (
	"context"
	"strconv"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	snstypes "github.com/aws/aws-sdk-go-v2/service/sns/types"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/drivers/sqs"
	"github.com/iqhive/iqmq/internal/errcode"
)

// The sink implements the mandatory publication surface and the optional
// native-batch one.
//
// It implements neither AsyncSink nor OwnedSink, deliberately. Both would be a
// goroutine pool and a copy in front of the same synchronous HTTP request the
// SDK makes, and the answer (ADR-021, Optional) is that the CORE
// supplies those loops for a driver that does not.
var (
	_ mq.Sink      = (*sink)(nil)
	_ mq.BatchSink = (*sink)(nil)
)

// sink is one bound publisher: the resolved SNS topic ARN and the encoder for
// the publisher's metadata mode.
type sink struct {
	b    *Backend
	cfg  mq.SinkConfig
	enc  *sqs.Encoder
	arn  string
	once sync.Once
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
	pub, err := s.encode(m, o)
	if err != nil {
		return s.fail(err, false)
	}
	callCtx, cancel := s.b.call(ctx)
	defer cancel()
	if err := callCtx.Err(); err != nil {
		// Done before the request was made, so nothing was sent: a
		// definite failure, which the same error from the SDK is not.
		return s.fail(err, false)
	}
	out, err := s.b.sns.Publish(callCtx, &sns.PublishInput{
		TopicArn:          aws.String(s.arn),
		Message:           aws.String(pub.Body),
		MessageAttributes: snsAttrs(pub.Attrs),
	})
	if err != nil {
		// The request has left the socket, so a transport failure here is
		// AMBIGUOUS: the service may have accepted the message and lost the
		// reply — and on a fan-out an accepted message is already on its
		// way to every subscribed queue. The specification makes that distinction the
		// driver's, and reporting it as a definite failure would make N
		// duplicates look like a loss.
		return s.fail(err, true)
	}
	s.receipt(m, aws.ToString(out.MessageId), aws.ToString(out.SequenceNumber))
	return nil
}

// encode is the mapping plus the two publication options SNS has no
// answer for.
//
// The order is the part that matters. The delay is refused BEFORE the shared
// encoder runs, because that encoder is the SQS driver's and an SQS queue DOES
// have a per-message timer: it would map mq.Delay to DelaySeconds, hand back a
// publication with the delay in a field SNS's Publish has no parameter for,
// and the delay would be silently dropped. The specification makes silently dropping an
// unsupported option a disqualifier, and this is the one place in the driver
// where reusing the queue driver's mapping could have caused it.
func (s *sink) encode(m *mq.Message, o *mq.PublishOptions) (sqs.Publication, error) {
	if err := checkPublishOptions(o); err != nil {
		return sqs.Publication{}, err
	}
	pub, err := s.enc.Encode(m, o)
	if err != nil {
		return sqs.Publication{}, err
	}
	if pub.Delay != 0 {
		// Unreachable through checkPublishOptions; asserting it here is
		// what keeps a future option that resolves into a delay from
		// reaching a service with nothing to carry it.
		return sqs.Publication{}, errNoDelay
	}
	return pub, nil
}

// checkPublishOptions refuses the publication controls SNS does not have.
//
// SNS has no per-message timer of any kind: neither a delay nor an absolute
// delivery time, and neither the topic nor the fan-out has anywhere to hold
// one. The capability table says so through FeatureScheduledPublish, and the specification
// requires the refusal to be immediate and to name what was refused.
func checkPublishOptions(o *mq.PublishOptions) error {
	if o == nil {
		return nil
	}
	if o.Delay != 0 || !o.DeliverAt.IsZero() {
		return errNoDelay
	}
	return nil
}

// PublishBatch admits a batch.
//
// Two limits shape it, both the service's: ten entries per call, and 256 KiB
// for the sum of a call's payloads — a QUARTER of what an SQS queue holds. So
// a caller's batch of any size becomes a sequence of PublishBatch calls, and
// the whole difficulty is what comes back.
//
// The service answers with a Successful list and a Failed list, and an entry
// in neither is impossible; an entry in Failed was not accepted and therefore
// reached no queue. So the result is a *BatchError whose Errs are
// index-aligned with ms — the CALLER's slice, not the chunk, not the failures.
// That alignment is the whole contract: a result aligned with anything else
// reports one message's outcome for another, which is how a batch publish
// loses a message silently. The entry identifier sent to the service is the
// message's index in ms, which is what makes the correlation exact rather than
// positional.
func (s *sink) PublishBatch(ctx context.Context, ms []*mq.Message, o *mq.PublishOptions) error {
	if s.b.Closed() {
		return mq.ErrClosed
	}
	if len(ms) == 0 {
		return nil
	}
	errs := make([]error, len(ms))
	pubs := make([]sqs.Publication, len(ms))
	for i, m := range ms {
		pub, err := s.encode(m, o)
		if err != nil {
			// A message that cannot be encoded never reaches the service,
			// and its error belongs at ITS index. Dropping it from the
			// batch and aligning the result with what was sent is the
			// mistake this whole method is written to avoid.
			errs[i] = s.fail(err, false)
			continue
		}
		pubs[i] = pub
	}
	entries := make([]snstypes.PublishBatchRequestEntry, 0, MaxBatchEntries)
	at := make([]int, 0, MaxBatchEntries)
	bytes := 0
	flush := func() {
		if len(entries) == 0 {
			return
		}
		s.send(ctx, entries, at, ms, errs)
		entries, at, bytes = entries[:0], at[:0], 0
	}
	limit := int(s.b.cfg.MaxMessageBytes)
	for i := range ms {
		if errs[i] != nil {
			continue
		}
		if len(entries) == MaxBatchEntries || (len(entries) > 0 && bytes+pubs[i].Bytes > limit) {
			flush()
		}
		entries = append(entries, snstypes.PublishBatchRequestEntry{
			Id:                aws.String(strconv.Itoa(i)),
			Message:           aws.String(pubs[i].Body),
			MessageAttributes: snsAttrs(pubs[i].Attrs),
		})
		at = append(at, i)
		bytes += pubs[i].Bytes
	}
	flush()
	return collapse(errs)
}

// send performs one PublishBatch and writes each entry's outcome to its own
// index in errs.
func (s *sink) send(ctx context.Context, entries []snstypes.PublishBatchRequestEntry, at []int, ms []*mq.Message, errs []error) {
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
	out, err := s.b.sns.PublishBatch(callCtx, &sns.PublishBatchInput{
		TopicArn:                   aws.String(s.arn),
		PublishBatchRequestEntries: entries,
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

// receipt fills the caller's receipt slot in place, when it asked for one
//
// The identifier is SNS's MessageId, and it is worth knowing what that is on
// this driver: it identifies the PUBLICATION, not any of the copies. Each
// subscribing queue mints its own SQS MessageId for its own copy, so the
// receipt a publisher holds and the identifier a consumer sees are different
// numbers — which is the honest answer for a fan-out and is why
// docs/drivers/snssqs.md says so rather than leaving a caller to correlate on
// it.
func (s *sink) receipt(m *mq.Message, id, sequence string) {
	if !s.cfg.Receipts || m == nil || m.Receipt == nil {
		return
	}
	r := m.Receipt
	r.MessageID = id
	// The service stores the message redundantly before it answers, so the
	// boundary a publication reached is the durable one whatever the
	// publisher asked for. Reporting the RESOLVED mode rather than the
	// stronger one is what the specification asks: the receipt says what this publication
	// waited for.
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
// not a guess.
func (s *sink) fail(err error, sentAlready bool) error {
	const op = mq.OpPublish
	if err == nil {
		return nil
	}
	if !sentAlready && mq.CodeOf(err) != mq.CodeUnknown {
		// Already one of this driver's own refusals, or one of the SQS
		// encoder's, built before any I/O. Wrapping it in an *OpError whose
		// code says something else is how a caller comes to retry a
		// publication that can never succeed.
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

// The sink's refusals.
var (
	errNoDelay = errcode.New(errcode.Unsupported,
		"iqmq/snssqs: SNS has no per-message timer, so a publication cannot be delayed or scheduled; drivers/sqs publishes straight to a queue, which has DelaySeconds, and a redelivery delay on this driver is a Nack with one")
	errNoEntryResult = errcode.New(errcode.Unknown,
		"iqmq/snssqs: the service named this batch entry in neither its successful nor its failed list")
)
