package sqsfake

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

// CreateQueue creates a queue, or returns the existing one.
func (s *Server) CreateQueue(ctx context.Context, in *sqs.CreateQueueInput, _ ...func(*sqs.Options)) (*sqs.CreateQueueOutput, error) {
	name := aws.ToString(in.QueueName)
	s.mu.Lock()
	defer s.mu.Unlock()
	if f := s.enter("CreateQueue", name); f != nil && f.Err != nil {
		return nil, f.Err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if name == "" {
		return nil, &types.InvalidAddress{Message: aws.String("no queue name")}
	}
	if q, ok := s.queues[name]; ok {
		for k, v := range in.Attributes {
			if q.attrs[k] != v && (k == string(types.QueueAttributeNameFifoQueue)) {
				return nil, &types.QueueNameExists{Message: aws.String(name)}
			}
		}
		return &sqs.CreateQueueOutput{QueueUrl: aws.String(s.url(name))}, nil
	}
	q := &queue{name: name, attrs: map[string]string{}}
	for k, v := range in.Attributes {
		q.attrs[k] = v
	}
	if strings.HasSuffix(name, ".fifo") {
		q.attrs[string(types.QueueAttributeNameFifoQueue)] = "true"
	}
	s.queues[name] = q
	s.names = append(s.names, name)
	return &sqs.CreateQueueOutput{QueueUrl: aws.String(s.url(name))}, nil
}

// DeleteQueue removes a queue.
func (s *Server) DeleteQueue(ctx context.Context, in *sqs.DeleteQueueInput, _ ...func(*sqs.Options)) (*sqs.DeleteQueueOutput, error) {
	name := nameOf(aws.ToString(in.QueueUrl))
	s.mu.Lock()
	defer s.mu.Unlock()
	if f := s.enter("DeleteQueue", name); f != nil && f.Err != nil {
		return nil, f.Err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if _, ok := s.queues[name]; !ok {
		return nil, &types.QueueDoesNotExist{Message: aws.String(name)}
	}
	delete(s.queues, name)
	for i, n := range s.names {
		if n == name {
			s.names = append(s.names[:i], s.names[i+1:]...)
			break
		}
	}
	return &sqs.DeleteQueueOutput{}, nil
}

// GetQueueUrl resolves a queue name.
func (s *Server) GetQueueUrl(ctx context.Context, in *sqs.GetQueueUrlInput, _ ...func(*sqs.Options)) (*sqs.GetQueueUrlOutput, error) { //nolint:revive // the SDK spells it Url
	name := aws.ToString(in.QueueName)
	s.mu.Lock()
	defer s.mu.Unlock()
	if f := s.enter("GetQueueUrl", name); f != nil && f.Err != nil {
		return nil, f.Err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if _, ok := s.queues[name]; !ok {
		return nil, &types.QueueDoesNotExist{Message: aws.String(name)}
	}
	return &sqs.GetQueueUrlOutput{QueueUrl: aws.String(s.url(name))}, nil
}

// ListQueues lists queue URLs by prefix.
func (s *Server) ListQueues(ctx context.Context, in *sqs.ListQueuesInput, _ ...func(*sqs.Options)) (*sqs.ListQueuesOutput, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if f := s.enter("ListQueues", ""); f != nil && f.Err != nil {
		return nil, f.Err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	prefix := aws.ToString(in.QueueNamePrefix)
	out := &sqs.ListQueuesOutput{}
	for _, n := range s.names {
		if strings.HasPrefix(n, prefix) {
			out.QueueUrls = append(out.QueueUrls, s.url(n))
		}
	}
	return out, nil
}

// GetQueueAttributes reads a queue's attributes, including the approximate
// counts this driver's Stats reports.
func (s *Server) GetQueueAttributes(ctx context.Context, in *sqs.GetQueueAttributesInput, _ ...func(*sqs.Options)) (*sqs.GetQueueAttributesOutput, error) {
	name := nameOf(aws.ToString(in.QueueUrl))
	s.mu.Lock()
	defer s.mu.Unlock()
	if f := s.enter("GetQueueAttributes", name); f != nil && f.Err != nil {
		return nil, f.Err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	q, ok := s.queues[name]
	if !ok {
		return nil, &types.QueueDoesNotExist{Message: aws.String(name)}
	}
	attrs := map[string]string{}
	for k, v := range q.attrs {
		attrs[k] = v
	}
	visible, hidden, delayed := 0, 0, 0
	for _, m := range q.msgs {
		switch {
		case !m.visibleAt.After(s.now):
			visible++
		case m.received > 0:
			hidden++
		default:
			delayed++
		}
	}
	attrs[string(types.QueueAttributeNameApproximateNumberOfMessages)] = strconv.Itoa(visible)
	attrs[string(types.QueueAttributeNameApproximateNumberOfMessagesNotVisible)] = strconv.Itoa(hidden)
	attrs[string(types.QueueAttributeNameApproximateNumberOfMessagesDelayed)] = strconv.Itoa(delayed)
	attrs[string(types.QueueAttributeNameQueueArn)] = s.arn(name)
	return &sqs.GetQueueAttributesOutput{Attributes: attrs}, nil
}

// SetQueueAttributes writes a queue's attributes.
func (s *Server) SetQueueAttributes(ctx context.Context, in *sqs.SetQueueAttributesInput, _ ...func(*sqs.Options)) (*sqs.SetQueueAttributesOutput, error) {
	name := nameOf(aws.ToString(in.QueueUrl))
	s.mu.Lock()
	defer s.mu.Unlock()
	if f := s.enter("SetQueueAttributes", name); f != nil && f.Err != nil {
		return nil, f.Err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	q, ok := s.queues[name]
	if !ok {
		return nil, &types.QueueDoesNotExist{Message: aws.String(name)}
	}
	for k, v := range in.Attributes {
		q.attrs[k] = v
	}
	return &sqs.SetQueueAttributesOutput{}, nil
}

// PurgeQueue discards everything a queue holds.
func (s *Server) PurgeQueue(ctx context.Context, in *sqs.PurgeQueueInput, _ ...func(*sqs.Options)) (*sqs.PurgeQueueOutput, error) {
	name := nameOf(aws.ToString(in.QueueUrl))
	s.mu.Lock()
	defer s.mu.Unlock()
	if f := s.enter("PurgeQueue", name); f != nil && f.Err != nil {
		return nil, f.Err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	q, ok := s.queues[name]
	if !ok {
		return nil, &types.QueueDoesNotExist{Message: aws.String(name)}
	}
	q.msgs = nil
	return &sqs.PurgeQueueOutput{}, nil
}

// SendMessage publishes one message.
func (s *Server) SendMessage(ctx context.Context, in *sqs.SendMessageInput, _ ...func(*sqs.Options)) (*sqs.SendMessageOutput, error) {
	name := nameOf(aws.ToString(in.QueueUrl))
	s.mu.Lock()
	defer s.mu.Unlock()
	if f := s.enter("SendMessage", name); f != nil && f.Err != nil {
		return nil, f.Err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	m, err := s.put(name, sendEntry{
		body:  aws.ToString(in.MessageBody),
		attrs: in.MessageAttributes,
		delay: in.DelaySeconds,
		group: aws.ToString(in.MessageGroupId),
		dedup: aws.ToString(in.MessageDeduplicationId),
	})
	if err != nil {
		return nil, err
	}
	s.wake()
	return &sqs.SendMessageOutput{
		MessageId:      aws.String(m.id),
		SequenceNumber: sequenceOf(m),
	}, nil
}

// SendMessageBatch publishes at most ten messages, each entry with its own
// outcome.
func (s *Server) SendMessageBatch(ctx context.Context, in *sqs.SendMessageBatchInput, _ ...func(*sqs.Options)) (*sqs.SendMessageBatchOutput, error) {
	name := nameOf(aws.ToString(in.QueueUrl))
	s.mu.Lock()
	defer s.mu.Unlock()
	f := s.enter("SendMessageBatch", name)
	if f != nil && f.Err != nil {
		return nil, f.Err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := checkBatch(len(in.Entries)); err != nil {
		return nil, err
	}
	out := &sqs.SendMessageBatchOutput{}
	for i := range in.Entries {
		e := &in.Entries[i]
		id := aws.ToString(e.Id)
		if code, bad := entryFault(f, id, i, aws.ToString(e.MessageBody)); bad {
			out.Failed = append(out.Failed, failure(id, code))
			continue
		}
		m, err := s.put(name, sendEntry{
			body:  aws.ToString(e.MessageBody),
			attrs: e.MessageAttributes,
			delay: e.DelaySeconds,
			group: aws.ToString(e.MessageGroupId),
			dedup: aws.ToString(e.MessageDeduplicationId),
		})
		if err != nil {
			out.Failed = append(out.Failed, failure(id, codeOf(err)))
			continue
		}
		out.Successful = append(out.Successful, types.SendMessageBatchResultEntry{
			Id: aws.String(id), MessageId: aws.String(m.id), SequenceNumber: sequenceOf(m),
		})
	}
	s.wake()
	return out, nil
}

// sendEntry is the part of a publication this server keeps.
type sendEntry struct {
	body  string
	attrs map[string]types.MessageAttributeValue
	delay int32
	group string
	dedup string
}

// put appends one message to a queue, honouring the FIFO rules.
func (s *Server) put(name string, e sendEntry) (*message, error) {
	q, ok := s.queues[name]
	if !ok {
		return nil, &types.QueueDoesNotExist{Message: aws.String(name)}
	}
	if e.body == "" {
		return nil, &types.InvalidMessageContents{Message: aws.String("the message body is empty")}
	}
	if err := checkMessageAttributes(e.attrs); err != nil {
		return nil, err
	}
	fifo := q.attrs[string(types.QueueAttributeNameFifoQueue)] == "true"
	switch {
	case fifo && e.group == "":
		return nil, &types.InvalidAttributeValue{Message: aws.String("a FIFO queue needs MessageGroupId")}
	case fifo && e.delay > 0:
		return nil, &types.InvalidAttributeValue{Message: aws.String("a FIFO queue takes no per-message delay")}
	case !fifo && e.group != "":
		return nil, &types.InvalidAttributeValue{Message: aws.String("MessageGroupId is FIFO-only")}
	}
	if fifo && e.dedup != "" {
		for _, m := range q.msgs {
			if m.dedup == e.dedup {
				// Inside the deduplication window the second send is
				// accepted and stored nowhere, which is what the service
				// does.
				return m, nil
			}
		}
	}
	s.seq++
	m := &message{
		id:        "msg-" + strconv.FormatUint(s.seq, 10),
		body:      e.body,
		attrs:     e.attrs,
		group:     e.group,
		dedup:     e.dedup,
		seqNum:    s.seq,
		sentAt:    s.now,
		visibleAt: s.now.Add(time.Duration(e.delay) * time.Second),
		stale:     map[string]struct{}{},
	}
	q.msgs = append(q.msgs, m)
	return m, nil
}

// checkMessageAttributes refuses what the service refuses in a message
// attribute: a data type that is not String, Number or Binary (optionally
// followed by "." and a custom label), and an empty value of any type. The
// pinned SDK's MessageAttributeValue documentation says the name, type and
// value must not be empty, and a fake that took an empty value would let a
// driver that sends one pass here and fail against AWS.
func checkMessageAttributes(attrs map[string]types.MessageAttributeValue) error {
	for k, v := range attrs {
		base, _, _ := strings.Cut(aws.ToString(v.DataType), ".")
		empty := false
		switch base {
		case "String", "Number":
			empty = aws.ToString(v.StringValue) == ""
		case "Binary":
			empty = len(v.BinaryValue) == 0
		default:
			return &types.InvalidAttributeValue{Message: aws.String("message attribute " + k + " has an unsupported data type")}
		}
		if empty {
			return &types.InvalidAttributeValue{Message: aws.String("message attribute " + k + " has an empty value")}
		}
	}
	return nil
}

// ReceiveMessage long-polls for at most ten messages.
//
// The wait is real but bounded by Options.MaxWait, whatever WaitTimeSeconds
// asks for: a poller that spun would burn a core in a unit test, and one that
// really parked for twenty seconds would make every test a timeout.
func (s *Server) ReceiveMessage(ctx context.Context, in *sqs.ReceiveMessageInput, _ ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
	name := nameOf(aws.ToString(in.QueueUrl))
	wait := time.Duration(in.WaitTimeSeconds) * time.Second
	if wait > s.opts.MaxWait {
		wait = s.opts.MaxWait
	}
	deadline := time.Now().Add(wait)
	for {
		s.mu.Lock()
		f := s.enter("ReceiveMessage", name)
		if f != nil && f.Err != nil {
			s.mu.Unlock()
			return nil, f.Err
		}
		if err := ctx.Err(); err != nil {
			s.mu.Unlock()
			return nil, err
		}
		out, err := s.take(in, name)
		if err != nil || len(out.Messages) > 0 {
			s.mu.Unlock()
			return out, err
		}
		ch := s.listen()
		s.mu.Unlock()
		left := time.Until(deadline)
		if left <= 0 {
			return out, nil
		}
		timer := time.NewTimer(left)
		select {
		case <-ch:
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		}
		timer.Stop()
	}
}

// take is one non-blocking receive. It is called with the lock held.
func (s *Server) take(in *sqs.ReceiveMessageInput, name string) (*sqs.ReceiveMessageOutput, error) {
	q, ok := s.queues[name]
	if !ok {
		return nil, &types.QueueDoesNotExist{Message: aws.String(name)}
	}
	want := int(in.MaxNumberOfMessages)
	if want <= 0 {
		want = 1
	}
	vis := time.Duration(in.VisibilityTimeout) * time.Second
	if in.VisibilityTimeout == 0 {
		vis = queueVisibility(q)
	}
	fifo := q.attrs[string(types.QueueAttributeNameFifoQueue)] == "true"
	maxRecv, dlqArn := redrive(q)
	out := &sqs.ReceiveMessageOutput{}
	taken := map[string]bool{}
	for i := 0; i < len(q.msgs) && len(out.Messages) < want; i++ {
		m := q.msgs[i]
		if !s.deliverable(q, m, fifo, taken) {
			continue
		}
		if maxRecv > 0 && int(m.received) >= maxRecv && dlqArn != "" {
			s.moveToDLQ(q, i, dlqArn)
			i--
			continue
		}
		m.received++
		if m.received == 1 {
			m.firstRecv = s.now
		}
		if m.handle != "" {
			m.stale[m.handle] = struct{}{}
		}
		s.seq++
		m.handle = "rh-" + m.id + "-" + strconv.FormatUint(s.seq, 10)
		m.visibleAt = s.now.Add(vis)
		if fifo {
			// One message per group per RECEIVE as well as one in flight.
			// groupHeld would already say so for the message just taken —
			// it is in flight now — but remembering it here is one map
			// write against a scan of the queue per candidate, and the two
			// halves of the rule read better side by side.
			taken[m.group] = true
		}
		out.Messages = append(out.Messages, s.render(m, in))
	}
	return out, nil
}

// render builds the wire message for one delivery.
func (s *Server) render(m *message, in *sqs.ReceiveMessageInput) types.Message {
	msg := types.Message{
		MessageId:     aws.String(m.id),
		ReceiptHandle: aws.String(m.handle),
		Body:          aws.String(m.body),
		Attributes: map[string]string{
			string(types.MessageSystemAttributeNameApproximateReceiveCount):          strconv.Itoa(int(m.received)),
			string(types.MessageSystemAttributeNameSentTimestamp):                    strconv.FormatInt(m.sentAt.UnixMilli(), 10),
			string(types.MessageSystemAttributeNameApproximateFirstReceiveTimestamp): strconv.FormatInt(m.firstRecv.UnixMilli(), 10),
		},
	}
	if m.group != "" {
		msg.Attributes[string(types.MessageSystemAttributeNameMessageGroupId)] = m.group
		msg.Attributes[string(types.MessageSystemAttributeNameSequenceNumber)] = strconv.FormatUint(m.seqNum, 10)
	}
	if m.dedup != "" {
		msg.Attributes[string(types.MessageSystemAttributeNameMessageDeduplicationId)] = m.dedup
	}
	if len(m.attrs) > 0 && wantsAttributes(in) {
		msg.MessageAttributes = make(map[string]types.MessageAttributeValue, len(m.attrs))
		for k, v := range m.attrs {
			msg.MessageAttributes[k] = v
		}
	}
	return msg
}

// wantsAttributes reports whether the request asked for message attributes.
func wantsAttributes(in *sqs.ReceiveMessageInput) bool {
	for _, n := range in.MessageAttributeNames {
		if n == "All" || n == ".*" {
			return true
		}
	}
	return len(in.MessageAttributeNames) > 0
}

// moveToDLQ moves the message at index i to its dead-letter queue.
func (s *Server) moveToDLQ(q *queue, i int, dlqArn string) {
	m := q.msgs[i]
	q.msgs = append(q.msgs[:i], q.msgs[i+1:]...)
	dlq, ok := s.queues[nameOf(dlqArn)]
	if !ok {
		return
	}
	m.received = 0
	m.visibleAt = s.now
	m.handle = ""
	m.stale = map[string]struct{}{}
	dlq.msgs = append(dlq.msgs, m)
}

// DeleteMessage settles one receipt handle.
func (s *Server) DeleteMessage(ctx context.Context, in *sqs.DeleteMessageInput, _ ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error) {
	name := nameOf(aws.ToString(in.QueueUrl))
	s.mu.Lock()
	defer s.mu.Unlock()
	if f := s.enter("DeleteMessage", name); f != nil && f.Err != nil {
		return nil, f.Err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := s.remove(name, aws.ToString(in.ReceiptHandle)); err != nil {
		return nil, err
	}
	s.wake()
	return &sqs.DeleteMessageOutput{}, nil
}

// DeleteMessageBatch settles at most ten receipt handles, each entry with its
// own outcome.
func (s *Server) DeleteMessageBatch(ctx context.Context, in *sqs.DeleteMessageBatchInput, _ ...func(*sqs.Options)) (*sqs.DeleteMessageBatchOutput, error) {
	name := nameOf(aws.ToString(in.QueueUrl))
	s.mu.Lock()
	defer s.mu.Unlock()
	f := s.enter("DeleteMessageBatch", name)
	if f != nil && f.Err != nil {
		return nil, f.Err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := checkBatch(len(in.Entries)); err != nil {
		return nil, err
	}
	out := &sqs.DeleteMessageBatchOutput{}
	for i := range in.Entries {
		e := &in.Entries[i]
		id := aws.ToString(e.Id)
		if code, bad := entryFault(f, id, i, s.bodyOf(name, aws.ToString(e.ReceiptHandle))); bad {
			out.Failed = append(out.Failed, failure(id, code))
			continue
		}
		if err := s.remove(name, aws.ToString(e.ReceiptHandle)); err != nil {
			out.Failed = append(out.Failed, failure(id, codeOf(err)))
			continue
		}
		out.Successful = append(out.Successful, types.DeleteMessageBatchResultEntry{Id: aws.String(id)})
	}
	s.wake()
	return out, nil
}

// remove deletes the message a receipt handle names.
//
// A superseded handle is the interesting case, and Options.StaleHandleDeletes
// is what decides it: the service's documented behaviour is that only the most
// recent handle is guaranteed to work, and the pessimistic reading — the one
// this driver's local lease check exists for — is that an old handle deletes
// the message anyway, even though another consumer now holds it.
func (s *Server) remove(name, handle string) error {
	q, ok := s.queues[name]
	if !ok {
		return &types.QueueDoesNotExist{Message: aws.String(name)}
	}
	for i, m := range q.msgs {
		switch {
		case m.handle == handle && handle != "":
		case handle == "":
			continue
		case hasStale(m, handle):
			if !s.opts.StaleHandleDeletes {
				return &types.ReceiptHandleIsInvalid{Message: aws.String("the receipt handle has been superseded")}
			}
		default:
			continue
		}
		q.msgs = append(q.msgs[:i], q.msgs[i+1:]...)
		return nil
	}
	return &types.ReceiptHandleIsInvalid{Message: aws.String("no message holds that receipt handle")}
}

// bodyOf is the body of the message a receipt handle names, and "" when no
// message holds it. It is called with the lock held.
func (s *Server) bodyOf(name, handle string) string {
	q, ok := s.queues[name]
	if !ok || handle == "" {
		return ""
	}
	for _, m := range q.msgs {
		if m.handle == handle || hasStale(m, handle) {
			return m.body
		}
	}
	return ""
}

// hasStale reports whether handle is one of the message's superseded handles.
func hasStale(m *message, handle string) bool {
	_, ok := m.stale[handle]
	return ok
}

// deliverable reports whether a receive may produce m right now, and it is the
// ONE place that decides: [Server.Deliverable] asks the same question without
// taking anything, so the count a test reads cannot drift from what a receive
// would do.
//
// taken is the groups the caller has already produced a message for; both
// callers keep one, because "one per group" binds a single receive as well as
// the messages in flight.
//
// It is called with the lock held.
func (s *Server) deliverable(q *queue, m *message, fifo bool, taken map[string]bool) bool {
	if m.visibleAt.After(s.now) {
		return false
	}
	if !fifo {
		return true
	}
	return !taken[m.group] && !s.groupHeld(q, m.group)
}

// groupHeld reports whether a FIFO message group has a message IN FLIGHT:
// received, and not yet visible again.
//
// It is derived from the messages rather than counted, and that is the whole
// The hold used to be a per-group counter incremented by
// a receive and decremented by a delete, by a visibility set to zero and by
// ExpireVisibility — three of the four ways a hold can end. The fourth is the
// CLOCK, and nothing decremented for it: [Server.Advance] made a message
// visible again and left its group held, so a FIFO message whose window lapsed
// by the clock alone was visible and undeliverable for ever — the queue
// reported depth 1, invisible 0 and every receive empty.
//
// A counter cannot be fixed by adding a fifth decrement, because the state it
// has to agree with is the set of deadlines and the clock: Advance would have
// to know which messages it had already released, and a double decrement
// releases somebody else's hold. Derived, there is nothing to disagree with —
// the service's own rule is "a group is blocked by an in-flight message and
// released when that message becomes visible again or is deleted", and this is
// that sentence.
//
// It is called with the lock held.
func (s *Server) groupHeld(q *queue, group string) bool {
	for _, m := range q.msgs {
		if m.group == group && m.received > 0 && m.visibleAt.After(s.now) {
			return true
		}
	}
	return false
}

// ChangeMessageVisibility resets one message's visibility window.
func (s *Server) ChangeMessageVisibility(ctx context.Context, in *sqs.ChangeMessageVisibilityInput, _ ...func(*sqs.Options)) (*sqs.ChangeMessageVisibilityOutput, error) {
	name := nameOf(aws.ToString(in.QueueUrl))
	s.mu.Lock()
	defer s.mu.Unlock()
	if f := s.enter("ChangeMessageVisibility", name); f != nil && f.Err != nil {
		return nil, f.Err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := s.setVisibility(name, aws.ToString(in.ReceiptHandle), in.VisibilityTimeout); err != nil {
		return nil, err
	}
	s.wake()
	return &sqs.ChangeMessageVisibilityOutput{}, nil
}

// ChangeMessageVisibilityBatch resets at most ten visibility windows.
func (s *Server) ChangeMessageVisibilityBatch(ctx context.Context, in *sqs.ChangeMessageVisibilityBatchInput, _ ...func(*sqs.Options)) (*sqs.ChangeMessageVisibilityBatchOutput, error) {
	name := nameOf(aws.ToString(in.QueueUrl))
	s.mu.Lock()
	defer s.mu.Unlock()
	f := s.enter("ChangeMessageVisibilityBatch", name)
	if f != nil && f.Err != nil {
		return nil, f.Err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := checkBatch(len(in.Entries)); err != nil {
		return nil, err
	}
	out := &sqs.ChangeMessageVisibilityBatchOutput{}
	for i := range in.Entries {
		e := &in.Entries[i]
		id := aws.ToString(e.Id)
		if code, bad := entryFault(f, id, i, s.bodyOf(name, aws.ToString(e.ReceiptHandle))); bad {
			out.Failed = append(out.Failed, failure(id, code))
			continue
		}
		if err := s.setVisibility(name, aws.ToString(e.ReceiptHandle), e.VisibilityTimeout); err != nil {
			out.Failed = append(out.Failed, failure(id, codeOf(err)))
			continue
		}
		out.Successful = append(out.Successful, types.ChangeMessageVisibilityBatchResultEntry{Id: aws.String(id)})
	}
	s.wake()
	return out, nil
}

// setVisibility moves one message's visibility deadline.
func (s *Server) setVisibility(name, handle string, seconds int32) error {
	q, ok := s.queues[name]
	if !ok {
		return &types.QueueDoesNotExist{Message: aws.String(name)}
	}
	for _, m := range q.msgs {
		if handle == "" || (m.handle != handle && !hasStale(m, handle)) {
			continue
		}
		if m.handle != handle && !s.opts.StaleHandleDeletes {
			return &types.ReceiptHandleIsInvalid{Message: aws.String("the receipt handle has been superseded")}
		}
		// A timeout of zero makes the message visible now, which releases
		// its FIFO group because the group's hold is derived from what is in
		// flight rather than counted.
		m.visibleAt = s.now.Add(time.Duration(seconds) * time.Second)
		return nil
	}
	return &types.MessageNotInflight{Message: aws.String("no in-flight message holds that receipt handle")}
}

// StartMessageMoveTask redrives a dead-letter queue back to its source, which
// is the typed Admin's redrive.
func (s *Server) StartMessageMoveTask(ctx context.Context, in *sqs.StartMessageMoveTaskInput, _ ...func(*sqs.Options)) (*sqs.StartMessageMoveTaskOutput, error) {
	src := nameOf(aws.ToString(in.SourceArn))
	s.mu.Lock()
	defer s.mu.Unlock()
	if f := s.enter("StartMessageMoveTask", src); f != nil && f.Err != nil {
		return nil, f.Err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	from, ok := s.queues[src]
	if !ok {
		return nil, &types.ResourceNotFoundException{Message: aws.String(src)}
	}
	dstName := nameOf(aws.ToString(in.DestinationArn))
	if dstName == "" {
		// With no destination the service moves messages back to the
		// queues they were dead-lettered from; this server only knows one
		// source, so a task with no destination is refused rather than
		// guessed at.
		return nil, &types.UnsupportedOperation{Message: aws.String("this server needs an explicit DestinationArn")}
	}
	to, ok := s.queues[dstName]
	if !ok {
		return nil, &types.ResourceNotFoundException{Message: aws.String(dstName)}
	}
	for _, m := range from.msgs {
		m.received, m.handle, m.visibleAt = 0, "", s.now
		m.stale = map[string]struct{}{}
		to.msgs = append(to.msgs, m)
	}
	from.msgs = nil
	s.seq++
	s.wake()
	return &sqs.StartMessageMoveTaskOutput{
		TaskHandle: aws.String("task-" + strconv.FormatUint(s.seq, 10)),
	}, nil
}

// queueVisibility is a queue's configured visibility timeout, with the
// service's own 30-second default.
func queueVisibility(q *queue) time.Duration {
	if v, err := strconv.Atoi(q.attrs[string(types.QueueAttributeNameVisibilityTimeout)]); err == nil && v > 0 {
		return time.Duration(v) * time.Second
	}
	return 30 * time.Second
}

// redrive is a queue's maxReceiveCount and dead-letter target ARN.
func redrive(q *queue) (int, string) {
	policy := q.attrs[string(types.QueueAttributeNameRedrivePolicy)]
	if policy == "" {
		return 0, ""
	}
	// The policy is JSON, and this server parses it by hand rather than
	// importing a codec: the two fields it needs are both flat.
	return jsonInt(policy, "maxReceiveCount"), jsonString(policy, "deadLetterTargetArn")
}

// jsonString reads a flat string field out of a JSON object.
func jsonString(src, key string) string {
	i := strings.Index(src, `"`+key+`"`)
	if i < 0 {
		return ""
	}
	rest := src[i+len(key)+2:]
	j := strings.IndexByte(rest, '"')
	if j < 0 {
		return ""
	}
	rest = rest[j+1:]
	k := strings.IndexByte(rest, '"')
	if k < 0 {
		return ""
	}
	return rest[:k]
}

// jsonInt reads a flat integer field out of a JSON object, accepting the
// string form the service uses.
func jsonInt(src, key string) int {
	i := strings.Index(src, `"`+key+`"`)
	if i < 0 {
		return 0
	}
	rest := src[i+len(key)+2:]
	j := strings.IndexByte(rest, ':')
	if j < 0 {
		return 0
	}
	rest = strings.TrimLeft(rest[j+1:], ` "`)
	end := 0
	for end < len(rest) && rest[end] >= '0' && rest[end] <= '9' {
		end++
	}
	v, err := strconv.Atoi(rest[:end])
	if err != nil {
		return 0
	}
	return v
}

// sequenceOf is the FIFO sequence number, and nil for a standard queue.
func sequenceOf(m *message) *string {
	if m.group == "" {
		return nil
	}
	return aws.String(strconv.FormatUint(m.seqNum, 10))
}

// checkBatch enforces the service's own entry-count limits, which is what
// makes a driver that sends eleven entries fail here rather than in production.
func checkBatch(n int) error {
	switch {
	case n == 0:
		return &types.EmptyBatchRequest{Message: aws.String("the batch has no entries")}
	case n > 10:
		return &types.TooManyEntriesInBatchRequest{Message: aws.String("a batch takes at most 10 entries")}
	default:
		return nil
	}
}

// entryFault reports whether an installed fault fails this batch entry.
//
// body is the body of the message the entry settles, and "" for an entry that
// settles nothing (a send, whose body is the entry's own).
func entryFault(f *Fault, id string, index int, body string) (string, bool) {
	if f == nil {
		return "", false
	}
	if code, ok := f.Entries[id]; ok {
		return orDefault(code), true
	}
	if code, ok := f.EntryIndexes[index]; ok {
		return orDefault(code), true
	}
	if body != "" {
		if code, ok := f.Bodies[body]; ok {
			return orDefault(code), true
		}
	}
	return "", false
}

// orDefault fills in the error code a fault left empty.
func orDefault(code string) string {
	if code == "" {
		return "InternalError"
	}
	return code
}

// failure is one failed batch entry.
func failure(id, code string) types.BatchResultErrorEntry {
	return types.BatchResultErrorEntry{
		Id:          aws.String(id),
		Code:        aws.String(code),
		Message:     aws.String("sqsfake: " + code),
		SenderFault: code != "InternalError",
	}
}

// codeOf is the batch error code for one of this server's own errors.
func codeOf(err error) string {
	var (
		handle  *types.ReceiptHandleIsInvalid
		missing *types.QueueDoesNotExist
		flight  *types.MessageNotInflight
		badAttr *types.InvalidAttributeValue
		badBody *types.InvalidMessageContents
	)
	switch {
	case errors.As(err, &handle):
		return "ReceiptHandleIsInvalid"
	case errors.As(err, &missing):
		return "AWS.SimpleQueueService.NonExistentQueue"
	case errors.As(err, &flight):
		return "AWS.SimpleQueueService.MessageNotInflight"
	case errors.As(err, &badAttr):
		return "InvalidAttributeValue"
	case errors.As(err, &badBody):
		return "InvalidMessageContents"
	default:
		return "InternalError"
	}
}
