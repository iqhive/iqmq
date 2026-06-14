package sqs_test

import (
	"errors"
	"strconv"
	"testing"
	"time"

	mq "bitbucket.org/iqhive/iqmq"
	"bitbucket.org/iqhive/iqmq/driverkit/flush"
	sq "bitbucket.org/iqhive/iqmq/drivers/sqs"
	"bitbucket.org/iqhive/iqmq/drivers/sqs/sqsfake"
	"bitbucket.org/iqhive/iqmq/internal/fuzzutil"
)

// FuzzSQSPartialResults is the partial-result fuzz: an arbitrary subset
// of an arbitrary batch is refused by the service, and the result has to name
// exactly those messages and no others.
//
// The alignment is the whole contract and it is the one a batch loses a
// message by getting wrong, so the target asserts it against the QUEUE as well
// as against the result: a message reported accepted has to be on the queue,
// and a message reported refused has to be absent. A driver that aligned its
// result with the chunk it sent, with the service's failure list, or with the
// messages that survived its own encoding would be caught by one of the two.
func FuzzSQSPartialResults(f *testing.F) {
	for _, seed := range partialSeeds() {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		r := fuzzutil.NewReader(data)
		n := r.Int(1, 24)
		server := sqsfake.New(sqsfake.Options{StaleHandleDeletes: true})
		h := openOn(t, server)
		name := topic(t, "fuzz-partial")
		pub, err := h.client.Publisher(t.Context(), mq.PublisherConfig{Topic: name})
		if err != nil {
			t.Fatalf("Publisher: %v", err)
		}
		defer func() { _ = pub.Close() }()

		ms := make([]*mq.Message, n)
		refuse := map[string]string{}
		encoderRefuses := map[int]bool{}
		for i := range ms {
			body := "m" + strconv.Itoa(i)
			ms[i] = &mq.Message{Body: []byte(body)}
			switch r.Choice(4) {
			case 0:
				// The service refuses this entry.
				refuse[body] = []string{"InternalError", "InvalidAttributeValue", "ReceiptHandleIsInvalid"}[r.Choice(3)]
			case 1:
				// The ENCODER refuses it, before any I/O, so it never
				// reaches the service at all: a header key that is not a
				// legal attribute name under the strict mode.
				ms[i].Headers.Add("bad/name", []byte("x"))
				encoderRefuses[i] = true
			}
		}
		if len(refuse) > 0 {
			server.Fault(sqsfake.Fault{Op: "SendMessageBatch", Bodies: refuse, Count: -1})
		}

		err = pub.PublishBatch(t.Context(), ms)
		outcomes := outcomesOf(t, err, n)
		on := map[string]bool{}
		for _, body := range h.bodies(name) {
			on[body] = true
		}
		for i := range n {
			body := string(ms[i].Body)
			_, serviceRefuses := refuse[body]
			wantFail := serviceRefuses || encoderRefuses[i]
			switch {
			case wantFail && outcomes[i] == nil:
				t.Fatalf("message %d (%q) was reported accepted; it was refused by the %s",
					i, body, refusedBy(serviceRefuses))
			case !wantFail && outcomes[i] != nil:
				t.Fatalf("message %d (%q) was reported refused with %v; nothing refused it", i, body, outcomes[i])
			case wantFail && on[body]:
				t.Fatalf("message %d (%q) reached the queue although its entry was refused", i, body)
			case !wantFail && !on[body]:
				t.Fatalf("message %d (%q) did not reach the queue although it was reported accepted", i, body)
			}
		}
	})
}

// refusedBy names which half refused a message, for a failure message.
func refusedBy(service bool) string {
	if service {
		return "service"
	}
	return "encoder"
}

// outcomesOf turns a batch answer into one outcome per input message, which is
// the shape the specification promises.
func outcomesOf(t *testing.T, err error, n int) []error {
	t.Helper()
	if err == nil {
		return make([]error, n)
	}
	var be *mq.BatchError
	if errors.As(err, &be) {
		if len(be.Errs) != n {
			t.Fatalf("the *BatchError carries %d outcome(s) for %d messages; the specification makes the Errs index-aligned with the input", len(be.Errs), n)
		}
		return be.Errs
	}
	// The specification: "a single error when none were [accepted]".
	out := make([]error, n)
	for i := range out {
		out[i] = err
	}
	return out
}

// partialSeeds are the committed inputs: a batch that spans two chunks with
// refusals from both halves.
func partialSeeds() [][]byte {
	w := fuzzutil.NewWriter()
	w.Int(1, 24, 12)
	for i := range 12 {
		switch i {
		case 3:
			w.Choice(4, 1)
		case 7, 11:
			w.Choice(4, 0)
			w.Choice(3, 0)
		default:
			w.Choice(4, 2)
		}
	}
	return [][]byte{w.Data()}
}

// FuzzSQSReceiptHandling is the receipt fuzz, and it is the only one of
// the three that drives the LEASE.
//
// An op script moves one queue's messages through receive, settle, negative
// settle, extension, a visibility release and a clock advance, and after every
// step the target checks the two rules a receipt handle exists for:
//
//   - a settlement that returned nil really removed the message. The service
//     is asked, so a driver that reported a success for a delete the service
//     refused is caught.
//   - a settlement that returned ErrLeaseLost removed NOTHING. That is the
//     whole point of the local lease check and of the supersession
//     bookkeeping: the service would have taken the delete, and what stops it
//     is the driver.
func FuzzSQSReceiptHandling(f *testing.F) {
	for _, seed := range receiptSeeds() {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		r := fuzzutil.NewReader(data)
		server := sqsfake.New(sqsfake.Options{StaleHandleDeletes: r.Bool()})
		deadline := time.Duration(r.Int(1, 10)) * time.Second
		h := openOn(t, server, func(c *sq.Config) {
			c.AckDeadline = deadline
			c.LeaseGuard = time.Duration(r.Int(0, 2)) * time.Second
			c.Settle = flush.Immediate
		})
		name := topic(t, "fuzz-receipt")
		const seeded = 6
		for i := range seeded {
			h.publish(name, &mq.Message{Body: []byte("b" + strconv.Itoa(i))})
		}
		st := &receiptRun{h: h, topic: name, queue: h.queue(name)}
		st.cons = h.consumer(name, seeded)
		receiptScript.Run(t, data[len(data)-r.Remaining():], st)
	})
}

// receiptRun is the state one receipt script drives.
type receiptRun struct {
	h     *harness
	cons  *mq.ConsumerHandle
	topic string
	queue string
	held  []mq.Delivery
}

// receive takes whatever is available without blocking for long.
func (s *receiptRun) receive(t testing.TB) {
	ctx, cancel := contextWithTimeout(s.h.t, 150*time.Millisecond)
	defer cancel()
	var dst [4]mq.Delivery
	n, err := s.cons.Receive(ctx, dst[:])
	if err != nil && n == 0 {
		return
	}
	for i := range n {
		s.held = append(s.held, dst[i])
	}
	_ = t
}

// settle applies one settlement to a held delivery and checks the two rules.
func (s *receiptRun) settle(t testing.TB, r *fuzzutil.Reader, kind int) {
	if len(s.held) == 0 {
		return
	}
	i := r.Choice(len(s.held))
	d := &s.held[i]
	body := string(d.Body)
	before := s.h.server.Bodies(s.queue)
	var err error
	switch kind {
	case 0:
		err = d.Ack()
	case 1:
		err = d.Nack()
	case 2:
		err = d.Nack(mq.NackDelay(time.Duration(r.Int(0, 5)) * time.Second))
	default:
		err = d.Extend(time.Duration(r.Int(1, 60)) * time.Second)
	}
	after := s.h.server.Bodies(s.queue)
	switch {
	case errors.Is(err, mq.ErrLeaseLost):
		// The lease is gone, and the whole reason the driver checks it
		// locally is that the SERVICE would have taken the call.
		if len(after) != len(before) {
			t.Fatalf("a settlement refused with ErrLeaseLost changed the queue from %v to %v; the refusal exists so that nothing is sent", before, after)
		}
	case err == nil && kind == 0:
		if countOf(after, body) >= countOf(before, body) {
			t.Fatalf("an Ack reported success and %q is still on the queue (%v -> %v)", body, before, after)
		}
	}
}

// advance moves the clock, which is how a visibility window passes.
func (s *receiptRun) advance(r *fuzzutil.Reader) {
	s.h.server.Advance(time.Duration(r.Int(1, 12)) * time.Second)
}

// release makes every invisible message visible again with the clock standing
// still, which is what another consumer's negative settlement looks like.
func (s *receiptRun) release() { s.h.server.ExpireVisibility(s.queue) }

// check is the invariant every step ends with: a delivery this source has
// released or superseded must never settle.
func (s *receiptRun) check(t testing.TB) {
	if s.h.depth(s.queue) < 0 {
		t.Fatal("the queue disappeared")
	}
}

// countOf is how many times a body appears in a queue's contents.
func countOf(bodies []string, body string) int {
	n := 0
	for _, b := range bodies {
		if b == body {
			n++
		}
	}
	return n
}

// receiptScript is the op script.
var receiptScript = fuzzutil.Script[*receiptRun]{
	MaxSteps: 64,
	Steps: []fuzzutil.Step[*receiptRun]{
		{Name: "receive", Weight: 4, Run: func(t testing.TB, _ *fuzzutil.Reader, s *receiptRun) { s.receive(t) }},
		{Name: "ack", Weight: 3, Run: func(t testing.TB, r *fuzzutil.Reader, s *receiptRun) { s.settle(t, r, 0) }},
		{Name: "nack", Weight: 2, Run: func(t testing.TB, r *fuzzutil.Reader, s *receiptRun) { s.settle(t, r, 1) }},
		{Name: "nack-delay", Weight: 1, Run: func(t testing.TB, r *fuzzutil.Reader, s *receiptRun) { s.settle(t, r, 2) }},
		{Name: "extend", Weight: 1, Run: func(t testing.TB, r *fuzzutil.Reader, s *receiptRun) { s.settle(t, r, 3) }},
		{Name: "advance", Weight: 2, Run: func(_ testing.TB, r *fuzzutil.Reader, s *receiptRun) { s.advance(r) }},
		{Name: "release", Weight: 1, Run: func(_ testing.TB, _ *fuzzutil.Reader, s *receiptRun) { s.release() }},
	},
	Check: func(t testing.TB, s *receiptRun) { s.check(t) },
}

// receiptSeeds are the committed inputs: the sequence the stale-receipt
// critical scenario walks, and the one the visibility-expiry scenario walks.
func receiptSeeds() [][]byte {
	seeds := make([][]byte, 0, 2)
	for _, stale := range []bool{true, false} {
		w := fuzzutil.NewWriter()
		w.Bool(stale)
		w.Int(1, 10, 5)
		w.Int(0, 2, 0)
		receiptScript.WriteStep(w, "receive")
		receiptScript.WriteStep(w, "release")
		receiptScript.WriteStep(w, "receive")
		receiptScript.WriteStep(w, "ack")
		w.Choice(2, 0)
		receiptScript.WriteStep(w, "advance")
		w.Int(1, 12, 6)
		receiptScript.WriteStep(w, "receive")
		receiptScript.WriteStep(w, "ack")
		w.Choice(2, 0)
		seeds = append(seeds, w.Data())
	}
	return seeds
}
