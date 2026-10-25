package sqs_test

import (
	"errors"
	"strconv"
	"strings"
	"testing"

	mq "github.com/iqhive/iqmq"
	sq "github.com/iqhive/iqmq/drivers/sqs"
	"github.com/iqhive/iqmq/drivers/sqs/sqsfake"
	"github.com/iqhive/iqmq/internal/errcode"
)

// TestAFIFOBatchStopsAGroupAtItsFirstFailure is a FIFO batch that the
// driver sends in several SendMessageBatch calls (its messages are too big for
// one), whose first call has an entry of group "b" refused. The driver used to
// send the later calls anyway, so group b's later messages were on the queue
// ahead of the refused one, and the caller's retry of it put the group out of
// order. Now the later group b entries are not sent and are reported not
// attempted, and group a, which had no failure, is sent as before.
func TestAFIFOBatchStopsAGroupAtItsFirstFailure(t *testing.T) {
	server := sqsfake.New(sqsfake.Options{StaleHandleDeletes: true})
	h := openOn(t, server, func(c *sq.Config) {
		c.FIFO = true
		c.MaxMessageBytes = 1024
	})
	name := topic(t, "fifo-batch")
	pub, err := h.client.Publisher(t.Context(), mq.PublisherConfig{Topic: name})
	if err != nil {
		t.Fatalf("Publisher: %v", err)
	}
	defer func() { _ = pub.Close() }()

	// Three 300-byte messages fit under the 1024-byte limit, so the ten go
	// in four calls: 0-2, 3-5, 6-8 and 9.
	const n = 10
	ms := make([]*mq.Message, n)
	group := func(i int) string { return map[bool]string{true: "a", false: "b"}[i%2 == 0] }
	pad := strings.Repeat("x", 300)
	for i := range ms {
		ms[i] = &mq.Message{Key: []byte(group(i)), Body: []byte("m" + strconv.Itoa(i) + pad)}
	}
	refused := string(ms[1].Body)
	server.Fault(sqsfake.Fault{
		Op:     "SendMessageBatch",
		Bodies: map[string]string{refused: "InternalError"},
		Count:  -1,
	})

	before := server.Calls("SendMessageBatch")
	err = pub.PublishBatch(t.Context(), ms)
	if calls := server.Calls("SendMessageBatch") - before; calls < 2 {
		t.Fatalf("the batch took %d SendMessageBatch call(s); the test needs it split", calls)
	}
	var be *mq.BatchError
	if !errors.As(err, &be) || len(be.Errs) != n {
		t.Fatalf("PublishBatch = %v, want a *BatchError with %d outcomes", err, n)
	}
	on := map[string]bool{}
	for _, body := range h.bodies(name) {
		on[body] = true
	}
	if be.Errs[1] == nil || on[refused] {
		t.Fatalf("m1 was refused by the service, and reported %v", be.Errs[1])
	}
	for i := 3; i < n; i++ {
		sent := on[string(ms[i].Body)]
		if group(i) == "a" {
			if be.Errs[i] != nil || !sent {
				t.Errorf("m%d of group a, which had no failure, reported %v (on the queue: %v)", i, be.Errs[i], sent)
			}
			continue
		}
		if sent {
			t.Errorf("m%d of group b was sent after m1 of its group was refused; the caller's retry of m1 lands behind it", i)
		}
		if !errors.Is(be.Errs[i], errcode.ErrNotAttempted) || !mq.IsRetryable(be.Errs[i]) {
			t.Errorf("m%d of group b reported %v, want a retryable not-attempted answer", i, be.Errs[i])
		}
	}
}

// TestAStandardBatchSendsEveryChunkWhateverFails holds the other half: a
// standard queue has no group order to keep, so a refused entry in the first
// chunk does not stop the second.
func TestAStandardBatchSendsEveryChunkWhateverFails(t *testing.T) {
	server := sqsfake.New(sqsfake.Options{StaleHandleDeletes: true})
	h := openOn(t, server, func(c *sq.Config) { c.MaxMessageBytes = 1024 })
	name := topic(t, "std-batch")
	pub, err := h.client.Publisher(t.Context(), mq.PublisherConfig{Topic: name})
	if err != nil {
		t.Fatalf("Publisher: %v", err)
	}
	defer func() { _ = pub.Close() }()

	const n = 10
	ms := make([]*mq.Message, n)
	pad := strings.Repeat("x", 300)
	for i := range ms {
		ms[i] = &mq.Message{Key: []byte("a"), Body: []byte("m" + strconv.Itoa(i) + pad)}
	}
	server.Fault(sqsfake.Fault{
		Op:     "SendMessageBatch",
		Bodies: map[string]string{string(ms[2].Body): "InternalError"},
		Count:  -1,
	})
	err = pub.PublishBatch(t.Context(), ms)
	var be *mq.BatchError
	if !errors.As(err, &be) || len(be.Errs) != n {
		t.Fatalf("PublishBatch = %v, want a *BatchError with %d outcomes", err, n)
	}
	for i := range n {
		if (i == 2) != (be.Errs[i] != nil) {
			t.Errorf("m%d reported %v", i, be.Errs[i])
		}
	}
	if got := h.depth(name); got != n-1 {
		t.Fatalf("the queue holds %d messages, want %d", got, n-1)
	}
}
