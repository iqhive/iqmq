package stomp_test

import (
	"context"
	"regexp"
	"testing"
	"time"

	"github.com/go-stomp/stomp/v3/frame"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/drivers/activemq/stomp/internal/stompsrv"
)

// The reply queue Config.VerifyQueueAddress makes, when its question is cut
// short.
//
// The pinned Artemis removes neither half of a reply queue left behind: its
// address is never reaped and survives a restart, and a queue that never had a
// consumer is never reaped either (measured by the review of this flag:
// 60 OpenSource calls with deadlines of 0.5 to 15 ms left 11 reply addresses
// and 6 reply queues, still there 7 minutes later). A question that ends
// normally drops its own. One cut short after its createQueue went out leaves
// the backend remembering the reply queue, and the next question to the same
// node drops it, or Close does.

// replyName matches a reply queue's name in a management request's body.
var replyName = regexp.MustCompile(`activemq\.management\.iqmq\.[A-Z0-9]+`)

// managementFrames are the management requests the server received for one
// operation, in order.
func managementFrames(srv *stompsrv.Server, operation string) []stompsrv.Frame {
	var out []stompsrv.Frame
	for _, f := range srv.FramesOf(frame.SEND) {
		if f.Get(frame.Destination) == managementAddress && f.Get("_AMQ_OperationName") == operation {
			out = append(out, f)
		}
	}
	return out
}

// dropsOf are the destroyQueue and deleteAddress requests that name reply.
func dropsOf(srv *stompsrv.Server, reply string) (destroys, deletes []stompsrv.Frame) {
	for _, f := range managementFrames(srv, "destroyQueue") {
		if replyName.FindString(string(f.Body)) == reply {
			destroys = append(destroys, f)
		}
	}
	for _, f := range managementFrames(srv, "deleteAddress") {
		if replyName.FindString(string(f.Body)) == reply {
			deletes = append(deletes, f)
		}
	}
	return destroys, deletes
}

// aheadOfACreate reports whether a createQueue follows f on f's connection,
// whose receipt then says the broker processed f.
func aheadOfACreate(srv *stompsrv.Server, f stompsrv.Frame) bool {
	for _, c := range managementFrames(srv, "createQueue") {
		if c.Conn == f.Conn && c.Seq > f.Seq {
			return true
		}
	}
	return false
}

// cutShort opens a source whose question the server holds until ctx ends, and
// returns the name of the reply queue the question made.
func cutShort(t *testing.T, srv *stompsrv.Server, open func(context.Context) error) string {
	t.Helper()
	creates := len(managementFrames(srv, "createQueue"))
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	err := open(ctx)
	if code := mq.CodeOf(err); code != mq.CodeDeadlineExceeded {
		t.Fatalf("the question cut short by the caller's deadline returned %v (code %v), want DeadlineExceeded",
			err, code)
	}
	made := managementFrames(srv, "createQueue")
	if len(made) != creates+1 {
		t.Fatalf("the question sent %d createQueue requests, want 1\nframes:%s", len(made)-creates, frameLog(srv))
	}
	return replyName.FindString(string(made[len(made)-1].Body))
}

// TestAReplyQueueACutQuestionLeftIsDroppedByTheNextQuestion is a question cut
// short at both points that strand a reply queue: while its createQueue's
// receipt is outstanding, which the broker never reaps, and while it waits for
// the answer. The next question to the node drops both, queue and address,
// before it makes its own reply queue, drops its own once answered, and a
// question after that drops nothing again.
func TestAReplyQueueACutQuestionLeftIsDroppedByTheNextQuestion(t *testing.T) {
	t.Parallel()
	srv := artemisServer(t)
	b := openBackend(t, verifying(srv))
	open := func(ctx context.Context) error {
		src, err := b.OpenSource(ctx, mq.SourceConfig{Topic: "orders", Subscription: "billing", Prefetch: 4})
		if src != nil {
			closeSource(t, src)
		}
		return err
	}

	srv.HoldReceiptsOn(managementAddress, 1)
	beforeSubscribe := cutShort(t, srv, open)
	beforeAnswer := cutShort(t, srv, open)
	if err := srv.WaitSubscribed(beforeAnswer, 1, budget); err != nil {
		t.Fatalf("the second question never subscribed to its reply queue: %v", err)
	}

	got, q := openAnswered(t, b, srv, "orders", "billing", 2, answer(true, `["orders"]`))
	if got.err != nil {
		t.Fatalf("OpenSource after two cut questions: %v\nframes:%s", got.err, frameLog(srv))
	}
	// The question cut while it waited for its answer had its createQueue
	// answered, which confirmed the drops it sent ahead of it, so the first
	// reply queue is dropped by the second question and the second by the
	// third.
	for _, reply := range []string{beforeSubscribe, beforeAnswer} {
		destroys, deletes := dropsOf(srv, reply)
		if len(destroys) != 1 || len(deletes) != 1 {
			t.Errorf("the reply queue %s a cut question left was dropped by %d destroyQueue and %d deleteAddress "+
				"requests, want one of each\nframes:%s", reply, len(destroys), len(deletes), frameLog(srv))
			continue
		}
		if !aheadOfACreate(srv, deletes[0]) || destroys[0].Seq > deletes[0].Seq {
			t.Errorf("the drops of %s (frames #%d and #%d) are not ahead of a later question's createQueue on "+
				"their connection\nframes:%s", reply, destroys[0].Seq, deletes[0].Seq, frameLog(srv))
		}
	}
	assertReplyQueueGone(t, srv, q.reply)
	if _, deletes := dropsOf(srv, q.reply); len(deletes) != 1 {
		t.Errorf("the answered question's own reply address was deleted %d times, want once", len(deletes))
	}

	got, _ = openAnswered(t, b, srv, "orders", "billing", 3, answer(true, `["orders"]`))
	if got.err != nil {
		t.Fatalf("the fourth OpenSource: %v", got.err)
	}
	for _, reply := range []string{beforeSubscribe, beforeAnswer, q.reply} {
		if destroys, _ := dropsOf(srv, reply); len(destroys) != 1 {
			t.Errorf("the reply queue %s was dropped %d times, want once: a drop the broker confirmed is "+
				"forgotten", reply, len(destroys))
		}
	}
}

// TestCloseDropsAReplyQueueACutQuestionLeft is a cut question with no question
// after it. Close drops its reply queue over the backend's own connection,
// which reaches the same node, ahead of that connection's DISCONNECT, whose
// receipt says the broker processed the drop.
func TestCloseDropsAReplyQueueACutQuestionLeft(t *testing.T) {
	t.Parallel()
	srv := artemisServer(t)
	b := openBackend(t, verifying(srv))
	reply := cutShort(t, srv, func(ctx context.Context) error {
		src, err := b.OpenSource(ctx, mq.SourceConfig{Topic: "orders", Subscription: "billing", Prefetch: 4})
		if src != nil {
			closeSource(t, src)
		}
		return err
	})
	if destroys, _ := dropsOf(srv, reply); len(destroys) != 0 {
		t.Fatalf("the reply queue %s was dropped before Close", reply)
	}
	if err := b.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	backendConn := srv.FramesOf(frame.CONNECT)[0].Conn
	destroys, deletes := dropsOf(srv, reply)
	if len(destroys) != 1 || len(deletes) != 1 {
		t.Fatalf("Close dropped the stranded reply queue %s with %d destroyQueue and %d deleteAddress requests, "+
			"want one of each\nframes:%s", reply, len(destroys), len(deletes), frameLog(srv))
	}
	var disconnect stompsrv.Frame
	for _, f := range srv.FramesOf(frame.DISCONNECT) {
		if f.Conn == backendConn {
			disconnect = f
		}
	}
	if destroys[0].Conn != backendConn || deletes[0].Seq > disconnect.Seq {
		t.Errorf("the drops went out on connection %d (frames #%d, #%d), want the backend's connection %d ahead "+
			"of its DISCONNECT (#%d)", destroys[0].Conn, destroys[0].Seq, deletes[0].Seq, backendConn, disconnect.Seq)
	}
}

// TestARefusedQuestionStrandsNothing is a createQueue the broker refused: it
// made no queue, so the next question drops nothing.
func TestARefusedQuestionStrandsNothing(t *testing.T) {
	t.Parallel()
	srv := artemisServer(t)
	b := openBackend(t, verifying(srv))
	srv.FailSend(managementAddress, "AMQ339011 Error sending message CoreMessage[address=activemq.management]")
	ctx, cancel := context.WithTimeout(context.Background(), budget)
	defer cancel()
	src, err := b.OpenSource(ctx, mq.SourceConfig{Topic: "orders", Subscription: "billing", Prefetch: 4})
	if src != nil {
		closeSource(t, src)
	}
	if mq.CodeOf(err) != mq.CodeForbidden {
		t.Fatalf("OpenSource with a refused createQueue returned %v, want Forbidden", err)
	}
	refused := replyName.FindString(string(managementFrames(srv, "createQueue")[0].Body))
	got, _ := openAnswered(t, b, srv, "orders", "billing", 1, answer(true, `["orders"]`))
	if got.err != nil {
		t.Fatalf("OpenSource after the refusal: %v", got.err)
	}
	if destroys, deletes := dropsOf(srv, refused); len(destroys)+len(deletes) != 0 {
		t.Errorf("the next question dropped %s, which the broker refused to make\nframes:%s", refused, frameLog(srv))
	}
}

// TestAQuestionWhoseDropIsNotConfirmedStrandsItsReply is a question answered
// and then cut short while it drops its own reply queue: the broker never says
// it processed the drop. The question used to take go-stomp's Disconnect
// returning nil as that confirmation, and Disconnect returns nil as well when
// the connection closes under it, which the question's own bound does. The
// review found that case leaking on the pinned Artemis. The drop's last frame
// now carries a receipt, and a receipt that does not come strands the reply,
// which the next question drops.
func TestAQuestionWhoseDropIsNotConfirmedStrandsItsReply(t *testing.T) {
	t.Parallel()
	srv := artemisServer(t)
	b := openBackend(t, verifying(srv))

	done := make(chan opened, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), budget)
		defer cancel()
		src, err := b.OpenSource(ctx, mq.SourceConfig{Topic: "orders", Subscription: "billing", Prefetch: 4})
		done <- opened{src: src, err: err}
	}()
	q, err := awaitQuestion(srv, 1)
	if err == nil {
		// The drop's two frames: the destroyQueue, and the deleteAddress whose
		// receipt the server keeps.
		srv.HoldReceiptsOn(managementAddress, 2)
		_, err = srv.Deliver(q.reply, answer(true, `["orders"]`))
	}
	got := <-done
	if got.src != nil {
		t.Cleanup(func() { closeSource(t, got.src) })
	}
	if err != nil {
		t.Fatalf("answering the question: %v (OpenSource returned %v)\nframes:%s", err, got.err, frameLog(srv))
	}
	if got.err != nil {
		t.Fatalf("OpenSource: %v: the answer was in, so the source opens whatever its drop does", got.err)
	}
	if destroys, deletes := dropsOf(srv, q.reply); len(destroys) != 1 || len(deletes) != 1 ||
		!deletes[0].Has(frame.Receipt) {
		t.Fatalf("the question dropped its reply queue with %d destroyQueue and %d deleteAddress requests (the last "+
			"with a receipt: %v), want one of each, the deleteAddress asking for a receipt\nframes:%s",
			len(destroys), len(deletes), len(deletes) == 1 && deletes[0].Has(frame.Receipt), frameLog(srv))
	}

	next, _ := openAnswered(t, b, srv, "orders", "billing", 2, answer(true, `["orders"]`))
	if next.err != nil {
		t.Fatalf("the next OpenSource: %v", next.err)
	}
	destroys, deletes := dropsOf(srv, q.reply)
	if len(destroys) != 2 || len(deletes) != 2 || !aheadOfACreate(srv, deletes[1]) {
		t.Errorf("the next question dropped the unconfirmed reply queue %s %d times, want once, ahead of its "+
			"createQueue\nframes:%s", q.reply, len(destroys)-1, frameLog(srv))
	}
}
