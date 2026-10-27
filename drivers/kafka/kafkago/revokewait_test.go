package kafkago_test

import (
	"errors"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kmsg"

	mq "github.com/iqhive/iqmq"
	kq "github.com/iqhive/iqmq/drivers/kafka/kafkago"
)

// TestASettlementAfterTheRevokeWaitIsRefused is step (4) of the end of a
// generation, held open at the broker. The wait for the in-flight deliveries
// ends at RevokeWait with one still out, the boundary is read without it and
// committed through the ending generation, and a settlement arriving while
// that commit was on the wire used to be accepted: the handler was told its
// record was done, and the commit that would have said so had already been
// read without it, so the record went to the partition's next owner as well.
//
// The commit is a real kafka-go Generation.CommitOffsets, held in kfake until
// the settlement has been answered.
func TestASettlementAfterTheRevokeWaitIsRefused(t *testing.T) {
	topic := newTopic("revoke-wait")
	const group = "g-revoke-wait"
	cl := cluster(t, 1, []string{topic})
	c, b := openPair(t, cl, func(cfg *kq.Config) {
		// Only the end of the generation commits, so the commit held below
		// is that one and not the ticker's.
		cfg.CommitInterval = -1
		cfg.RevokeWait = 200 * time.Millisecond
	})
	first := consumerOn(t, c, topic, group)
	for i := range 2 {
		publish(t, c, topic, &mq.Message{Body: []byte("r" + strconv.Itoa(i))})
	}
	dst := make([]mq.Delivery, 2)
	got := 0
	for got < 2 {
		n, err := first.Receive(t.Context(), dst[got:])
		got += n
		if err != nil {
			t.Fatalf("Receive: %v", err)
		}
	}
	// The first is settled, so the generation has a boundary to commit; the
	// second is held past the wait.
	if err := dst[0].Ack(); err != nil {
		t.Fatalf("Ack of the first record: %v", err)
	}

	reached, release := make(chan struct{}), make(chan struct{})
	var (
		holding atomic.Bool
		freed   sync.Once
	)
	free := func() { freed.Do(func() { close(release) }) }
	t.Cleanup(free)
	cl.ControlKey(int16(kmsg.OffsetCommit), func(req kmsg.Request) (kmsg.Response, error, bool) {
		// A member's commit carries its generation. The start position the
		// second member's Consumer resolves is committed without one.
		if r, ok := req.(*kmsg.OffsetCommitRequest); !ok || r.Group != group || r.Generation < 1 {
			return nil, nil, false //nolint:nilnil // kfake's "not handled" is (nil, nil, false), then the cluster answers
		}
		if holding.CompareAndSwap(false, true) {
			close(reached)
			cl.SleepControl(func() { <-release })
		}
		return nil, nil, false //nolint:nilnil // as above
	})

	before := b.Rebalances()
	// The second member's join is what ends the first member's generation.
	consumerOn(t, c, topic, group)
	select {
	case <-reached:
	case <-time.After(30 * time.Second):
		t.Fatalf("the ending generation's commit never reached the broker (rebalances %d, were %d)", b.Rebalances(), before)
	}
	err := dst[1].Ack()
	free()
	if !errors.Is(err, mq.ErrLeaseLost) {
		t.Fatalf("Ack after the generation's wait ran out = %v, want ErrLeaseLost: the commit on the wire was read without it, so the record goes to the partition's next owner", err)
	}
}
