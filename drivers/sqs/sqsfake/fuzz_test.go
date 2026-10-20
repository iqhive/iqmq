package sqsfake

import (
	"context"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"github.com/iqhive/iqmq/internal/fuzzutil"
)

// FuzzFakeQueue holds this server to the rules the driver's tests believe it
// follows.
//
// It matters more than a fake's own fuzz target usually does: four critical
// scenarios and three of the driver's fuzz targets rest on this server, so a
// server that lies is a test suite that lies — and the lie would look like a
// driver that works. The invariants below are the ones those tests depend on,
// checked after every step of an arbitrary interleaving of sends, receives,
// deletes, visibility changes and clock advances:
//
//   - CONSERVATION: every message sent is either in the queue or was deleted,
//     exactly once. A message that vanished would make a settlement test pass
//     for the wrong reason.
//   - EXCLUSIVITY: a message is invisible while a consumer holds it, so no two
//     receives can produce the same message at once. It is the visibility
//     timeout, and it is what the at-least-once window is measured against.
//   - HANDLES: every receive mints a NEW receipt handle and remembers the old
//     one, which is the state the stale-receipt scenario needs to exist.
//   - FIFO: at most one message per group is in flight, and a group's
//     messages are received in publication order.
//   - LIVENESS: a queue holding messages with NOTHING in flight can deliver
//     something. It is the other half of EXCLUSIVITY and it is the half that
//     was missing: a message that is visible and whose group is held is a
//     message conservation cannot see, and the server could reach that state
//     by moving the clock. A queue that reports depth and
//     no in-flight message and still delivers nothing is deadlocked against
//     the fake rather than against SQS.
func FuzzFakeQueue(f *testing.F) {
	for _, seed := range queueSeeds(f) {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		r := fuzzutil.NewReader(data)
		fifo := r.Bool()
		name := "q"
		if fifo {
			name += ".fifo"
		}
		s := New(Options{StaleHandleDeletes: r.Bool()})
		ctx := context.Background()
		if _, err := s.CreateQueue(ctx, &sqs.CreateQueueInput{
			QueueName: aws.String(name),
			Attributes: map[string]string{
				string(types.QueueAttributeNameVisibilityTimeout): "5",
			},
		}); err != nil {
			t.Fatalf("CreateQueue: %v", err)
		}
		st := &queueRun{s: s, name: name, fifo: fifo, ctx: ctx, handles: map[string]string{}}
		queueScript.Run(t, data[len(data)-r.Remaining():], st)
	})
}

// queueRun is the state one script drives, and the model it is checked
// against.
type queueRun struct {
	s    *Server
	ctx  context.Context
	name string
	fifo bool

	sent    int
	deleted int
	// handles maps a receipt handle to the body it settles, so a delete can
	// be attributed without asking the server.
	handles map[string]string
	// held is the receipt handles a consumer currently believes it holds.
	held []string
	// order is each group's publication order, so the FIFO rule can be
	// checked.
	order map[string][]string
	// seen is each group's receive order.
	seen map[string][]string
}

// send publishes one message.
func (q *queueRun) send(t testing.TB, r *fuzzutil.Reader) {
	body := "b" + itoa(q.sent)
	in := &sqs.SendMessageInput{
		QueueUrl:    aws.String(q.s.QueueURL(q.name)),
		MessageBody: aws.String(body),
	}
	group := ""
	if q.fifo {
		group = []string{"g0", "g1", "g2"}[r.Choice(3)]
		in.MessageGroupId = aws.String(group)
		in.MessageDeduplicationId = aws.String(body)
	}
	if _, err := q.s.SendMessage(q.ctx, in); err != nil {
		t.Fatalf("SendMessage: %v", err)
	}
	q.sent++
	if q.order == nil {
		q.order = map[string][]string{}
	}
	q.order[group] = append(q.order[group], body)
}

// receive takes up to three messages and records the handles.
func (q *queueRun) receive(t testing.TB, r *fuzzutil.Reader) {
	out, err := q.s.ReceiveMessage(q.ctx, &sqs.ReceiveMessageInput{
		QueueUrl:            aws.String(q.s.QueueURL(q.name)),
		MaxNumberOfMessages: int32(r.Int(1, 3)),
	})
	if err != nil {
		t.Fatalf("ReceiveMessage: %v", err)
	}
	inBatch := map[string]bool{}
	for i := range out.Messages {
		m := &out.Messages[i]
		body, handle := aws.ToString(m.Body), aws.ToString(m.ReceiptHandle)
		switch {
		case handle == "":
			t.Fatal("a receive produced no receipt handle")
		case inBatch[body]:
			t.Fatalf("one receive produced %q twice; a message is invisible while a consumer holds it", body)
		}
		inBatch[body] = true
		if prev, ok := q.handles[handle]; ok && prev != body {
			t.Fatalf("receipt handle %q was minted twice, for %q and %q; every receive mints a new one", handle, prev, body)
		}
		q.handles[handle] = body
		q.held = append(q.held, handle)
		if q.fifo {
			group := m.Attributes[string(types.MessageSystemAttributeNameMessageGroupId)]
			if q.seen == nil {
				q.seen = map[string][]string{}
			}
			q.seen[group] = append(q.seen[group], body)
		}
	}
}

// remove deletes one held handle.
func (q *queueRun) remove(t testing.TB, r *fuzzutil.Reader) {
	if len(q.held) == 0 {
		return
	}
	i := r.Choice(len(q.held))
	handle := q.held[i]
	q.held = append(q.held[:i], q.held[i+1:]...)
	_, err := q.s.DeleteMessage(q.ctx, &sqs.DeleteMessageInput{
		QueueUrl:      aws.String(q.s.QueueURL(q.name)),
		ReceiptHandle: aws.String(handle),
	})
	if err != nil {
		// A refusal is allowed — the handle may be superseded and the
		// server configured to refuse one — and it must not have deleted
		// anything.
		return
	}
	q.deleted++
	_ = t
}

// hide changes one held handle's visibility.
func (q *queueRun) hide(r *fuzzutil.Reader) {
	if len(q.held) == 0 {
		return
	}
	handle := q.held[r.Choice(len(q.held))]
	_, _ = q.s.ChangeMessageVisibility(q.ctx, &sqs.ChangeMessageVisibilityInput{
		QueueUrl:          aws.String(q.s.QueueURL(q.name)),
		ReceiptHandle:     aws.String(handle),
		VisibilityTimeout: int32(r.Int(0, 3)),
	})
}

// advance moves the clock.
func (q *queueRun) advance(r *fuzzutil.Reader) {
	q.s.Advance(time.Duration(r.Int(1, 8)) * time.Second)
}

// check is the invariant set.
func (q *queueRun) check(t testing.TB) {
	depth := q.s.Depth(q.name)
	if depth < 0 {
		t.Fatal("the queue disappeared")
	}
	if depth+q.deleted != q.sent {
		t.Fatalf("%d sent, %d in the queue and %d deleted: a message was lost or counted twice", q.sent, depth, q.deleted)
	}
	inv := q.s.Invisible(q.name)
	if inv > depth {
		t.Fatalf("%d of %d messages are invisible", inv, depth)
	}
	// LIVENESS. Nothing is in flight, so no group is held and no window is
	// running: every message the queue holds is one a receive can produce.
	// The queue this target creates has no redrive policy, so there is no
	// message a receive would move to a dead-letter queue instead of
	// producing.
	if deliverable := q.s.Deliverable(q.name); depth > 0 && inv == 0 && deliverable == 0 {
		t.Fatalf("the queue holds %d message(s), none of them in flight, and a receive would produce NONE of them: a group is held by a message that is visible, so this queue can never be drained ", depth)
	}
	if !q.fifo {
		return
	}
	// A FIFO group is received in publication order, with the caveat that a
	// message made visible again is received again: the RECEIVE order is
	// therefore the publication order with repeats, so the first receive of
	// each body has to follow the publication order.
	for group, seen := range q.seen {
		want := q.order[group]
		at, first := 0, map[string]bool{}
		for _, body := range seen {
			if first[body] {
				continue
			}
			first[body] = true
			if at >= len(want) || want[at] != body {
				t.Fatalf("group %q was first received as %v and published as %v", group, seen, want)
			}
			at++
		}
	}
}

// queueScript is the op script.
var queueScript = fuzzutil.Script[*queueRun]{
	MaxSteps: 128,
	Steps: []fuzzutil.Step[*queueRun]{
		{Name: "send", Weight: 4, Run: func(t testing.TB, r *fuzzutil.Reader, q *queueRun) { q.send(t, r) }},
		{Name: "receive", Weight: 3, Run: func(t testing.TB, r *fuzzutil.Reader, q *queueRun) { q.receive(t, r) }},
		{Name: "delete", Weight: 3, Run: func(t testing.TB, r *fuzzutil.Reader, q *queueRun) { q.remove(t, r) }},
		{Name: "visibility", Weight: 1, Run: func(_ testing.TB, r *fuzzutil.Reader, q *queueRun) { q.hide(r) }},
		{Name: "advance", Weight: 1, Run: func(_ testing.TB, r *fuzzutil.Reader, q *queueRun) { q.advance(r) }},
	},
	Check: func(t testing.TB, q *queueRun) { q.check(t) },
}

// queueSeeds are the committed inputs: one standard and one FIFO script that
// reaches a redelivery and a delete.
func queueSeeds(tb testing.TB) [][]byte {
	out := make([][]byte, 0, 2)
	for _, fifo := range []bool{false, true} {
		w := fuzzutil.NewWriter()
		w.Bool(fifo)
		w.Bool(true)
		for range 3 {
			queueScript.WriteStep(tb, w, "send")
			if fifo {
				w.Choice(3, 0)
			}
		}
		queueScript.WriteStep(tb, w, "receive")
		w.Int(1, 3, 3)
		queueScript.WriteStep(tb, w, "advance")
		w.Int(1, 8, 6)
		queueScript.WriteStep(tb, w, "receive")
		w.Int(1, 3, 3)
		queueScript.WriteStep(tb, w, "delete")
		w.Choice(1, 0)
		out = append(out, w.Data())
	}
	return out
}

// itoa is strconv.Itoa for a small non-negative number, spelled here so the
// target's imports stay to the two packages it is about.
func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var buf [8]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[i:])
}
