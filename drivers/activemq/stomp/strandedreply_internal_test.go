package stomp

import (
	"errors"
	"strconv"
	"testing"
	"time"

	gostomp "github.com/go-stomp/stomp/v3"
	"github.com/go-stomp/stomp/v3/frame"
)

// TestCloseWithStrandedRepliesOnADeafBrokerTakesOneReceiptTimeout is Close
// with the most reply queues a backend remembers, on a connection whose broker
// has stopped reading and whose write queue is full.
//
// Close drops the reply queues stranded on its node ahead of its DISCONNECT.
// The drops used to be sent past the gate, and go-stomp waits up to
// Config.ReceiptTimeout for a slot for each frame, so Close took
// (2n+1) x ReceiptTimeout: measured by the review, 16.5 s for 16 at 500 ms,
// and about 16.5 minutes at the 30 s default. A drop now goes only while the
// queue has room, so Close costs its DISCONNECT and nothing more, the rule
// TestBackendCloseDoesNotWaitBehindAPublication holds it to.
func TestCloseWithStrandedRepliesOnADeafBrokerTakesOneReceiptTimeout(t *testing.T) {
	t.Parallel()
	const receipt = 500 * time.Millisecond
	srv, b := deafBackend(t, receipt)
	b.mu.Lock()
	at := b.cfg.Addrs[b.connAddr]
	b.mu.Unlock()
	for i := range maxStranded {
		b.strand(at, replyQueuePrefix+"DEAF"+strconv.Itoa(i))
	}
	if err := srv.StopReading(); err != nil {
		t.Fatal(err)
	}
	defer srv.ResumeReading()
	fillQueue(t, backendConn(b))
	start := time.Now()
	_ = b.Close()
	if took, limit := time.Since(start), receipt+receipt/2; took > limit {
		t.Errorf("Backend.Close took %v with %d reply queues stranded and ReceiptTimeout %v, want at most %v: "+
			"its own DISCONNECT is one ReceiptTimeout, and a drop that waits for room in a full write queue is "+
			"another for each of its frames", took, maxStranded, receipt, limit)
	}
}

// TestMayHaveMadeReadsACreateQueuesFailure is which failed createQueue may
// still have made its reply queue, and so is stranded for a later drop.
// Neither the broker's refusal nor a frame go-stomp never queued made one;
// stranding them only took one of the backend's slots.
func TestMayHaveMadeReadsACreateQueuesFailure(t *testing.T) {
	t.Parallel()
	refusal := gostomp.Error{
		Message: "AMQ339011 Error sending message",
		Frame:   frame.New(frame.ERROR, frame.Message, "AMQ339011 Error sending message", frame.ReceiptId, "1"),
	}
	closedUnder := gostomp.Error{
		Message: "connection closed",
		Frame:   frame.New(frame.ERROR, frame.Message, "connection closed"),
	}
	for _, tc := range []struct {
		name string
		err  error
		want bool
	}{
		{name: "the broker's ERROR frame answering it", err: refusal, want: false},
		{name: "a connection already closed", err: gostomp.ErrAlreadyClosed, want: false},
		{name: "no slot within MsgSendTimeout", err: gostomp.ErrMsgSendTimeout, want: false},
		{name: "the connection closed while the receipt was awaited", err: gostomp.ErrClosedUnexpectedly, want: true},
		{name: "go-stomp's own ERROR frame for a closed connection", err: closedUnder, want: true},
		{name: "a receipt that timed out", err: gostomp.ErrMsgReceiptTimeout, want: true},
		{name: "an error wrapped on the way", err: errors.Join(errors.New("ctx"), gostomp.ErrAlreadyClosed), want: false},
	} {
		if got := mayHaveMade(tc.err); got != tc.want {
			t.Errorf("%s: mayHaveMade(%v) = %v, want %v", tc.name, tc.err, got, tc.want)
		}
	}
}
