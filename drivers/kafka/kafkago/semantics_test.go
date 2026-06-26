package kafkago_test

import (
	"context"
	"errors"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	mq "bitbucket.org/iqhive/iqmq"
	"bitbucket.org/iqhive/iqmq/driverkit/nack"
	kq "bitbucket.org/iqhive/iqmq/drivers/kafka/kafkago"
)

// TestFanOutIsolation is two subscriptions of one topic each receive
// everything, and one's acknowledgement does not settle the other's copy.
func TestFanOutIsolation(t *testing.T) {
	c, topic := openClient(t)
	a := consumerOn(t, c, topic, "fan-a")
	b := consumerOn(t, c, topic, "fan-b")
	publish(t, c, topic, &mq.Message{Body: []byte("both")})
	for name, cons := range map[string]*mq.ConsumerHandle{"fan-a": a, "fan-b": b} {
		d := receiveOne(t, cons, 15*time.Second)
		if string(d.Body) != "both" {
			t.Fatalf("%s received %q", name, d.Body)
		}
		if err := d.Ack(); err != nil {
			t.Fatalf("%s Ack: %v", name, err)
		}
	}
}

// TestCompetingConsumersShareThePartitions is the other half: two
// consumers of ONE subscription divide the topic between them rather than each
// receiving everything.
//
// The publications are a LOOP rather than a batch, and that is not padding. A
// group forms one member at a time: the first member is assigned all four
// partitions and starts reading, and the second joins afterwards. A test that
// published everything up front routinely had the first member consume all of
// it before the second had joined — which is a valid outcome of a
// competing-consumer group and tells you nothing about the assignor. So the
// test keeps publishing until both members have done work, which is the
// property it is actually about, and then checks that nothing was lost.
func TestCompetingConsumersShareThePartitions(t *testing.T) {
	topic := newTopic("compete")
	cl := cluster(t, 4, []string{topic})
	c := openOn(t, cl)
	var (
		mu        sync.Mutex
		seen      = map[string]int{}
		byCon     = map[int]int{}
		published int
		wg        sync.WaitGroup
	)
	ctx, cancel := context.WithTimeout(t.Context(), 40*time.Second)
	defer cancel()
	for id := range 2 {
		h, err := c.Consumer(t.Context(), mq.ConsumerConfig{
			Topic: topic, Subscription: "compete", Start: mq.StartEarliest(),
		})
		if err != nil {
			t.Fatalf("consumer %d: %v", id, err)
		}
		t.Cleanup(func() { _ = h.Close() })
		wg.Add(1)
		go func() {
			defer wg.Done()
			dst := make([]mq.Delivery, 8)
			for ctx.Err() == nil {
				m, _ := h.Receive(ctx, dst)
				for i := range m {
					mu.Lock()
					seen[string(dst[i].Body)]++
					byCon[id]++
					mu.Unlock()
					_ = dst[i].Ack()
				}
			}
		}()
	}
	// Keep publishing until both members have consumed something, or the
	// budget runs out.
	for ctx.Err() == nil {
		for range 8 {
			mu.Lock()
			body := strconv.Itoa(published)
			published++
			mu.Unlock()
			publish(t, c, topic, &mq.Message{Key: []byte("k" + body), Body: []byte(body)})
		}
		mu.Lock()
		both := byCon[0] > 0 && byCon[1] > 0
		count := published
		mu.Unlock()
		if both {
			break
		}
		if count > 400 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	// Let the group finish what is in flight, then stop the readers.
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		done := len(seen) >= published
		mu.Unlock()
		if done {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	cancel()
	wg.Wait()
	mu.Lock()
	defer mu.Unlock()
	// Both members did work: a group whose partitions all went to one member
	// is not a competing-consumer group, and it is what a broken assignor
	// looks like.
	if byCon[0] == 0 || byCon[1] == 0 {
		t.Errorf("one member consumed everything (%v) over %d records: four partitions over two members must give each of them some", byCon, published)
	}
	// And nothing was lost across the rebalance the second member's join
	// caused. At-least-once allows a duplicate and forbids a loss.
	missing := 0
	for i := range published {
		if seen[strconv.Itoa(i)] == 0 {
			missing++
		}
	}
	if missing > 0 {
		t.Errorf("%d of %d records never reached the group", missing, published)
	}
	t.Logf("%d records, %d distinct, per member %v", published, len(seen), byCon)
}

// TestBatchReceiveFillsMoreThanOneSlot is what kafka-go's Reader makes
// awkward and the specification requires anyway.
//
// [kafka.Reader] serves ONE record per FetchMessage — there is no "give me
// everything you have buffered" call — so a driver that returned after its
// first record would fill one slot of a caller's batch whatever the backlog.
// The per-partition readers buffer into a channel the fetch drains without
// blocking, which is what makes a batch a batch.
func TestBatchReceiveFillsMoreThanOneSlot(t *testing.T) {
	c, topic := openClient(t)
	cons := consumerOn(t, c, topic, "batch-recv")
	const n = 32
	ms := make([]*mq.Message, n)
	for i := range ms {
		ms[i] = &mq.Message{Body: []byte(strconv.Itoa(i))}
	}
	pub, err := c.Publisher(t.Context(), mq.PublisherConfig{Topic: topic})
	if err != nil {
		t.Fatalf("Publisher: %v", err)
	}
	defer func() { _ = pub.Close() }()
	if err := pub.PublishBatch(t.Context(), ms); err != nil {
		t.Fatalf("PublishBatch: %v", err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	dst := make([]mq.Delivery, n)
	best, total := 0, 0
	for total < n {
		m, err := cons.Receive(ctx, dst)
		if m > best {
			best = m
		}
		for i := range m {
			if err := dst[i].Ack(); err != nil {
				t.Fatalf("Ack: %v", err)
			}
		}
		total += m
		if err != nil {
			break
		}
	}
	if total != n {
		t.Fatalf("%d of %d records were received", total, n)
	}
	if best < 2 {
		t.Fatalf("the widest Receive filled %d slot(s) of %d: the driver is not draining what its readers have buffered, so a caller cannot batch at all", best, n)
	}
	t.Logf("the widest Receive filled %d of %d slots", best, n)
}

// TestStartPositions is, and on this client it is also the only place
// where a start position by TIME or by SEQUENCE can be honoured at all:
// kafka-go's group start offset is FirstOffset or LastOffset and nothing else,
// so the driver commits the resolved position for the group before it joins.
func TestStartPositions(t *testing.T) {
	topic := newTopic("start")
	cl := cluster(t, 1, []string{topic})
	c := openOn(t, cl)
	const n = 6
	for i := range n {
		publish(t, c, topic, &mq.Message{Body: []byte(strconv.Itoa(i))})
	}
	// A gap on BOTH sides of the instant the test asks for. A Kafka record
	// timestamp is a millisecond, and a ListOffsets by timestamp answers with
	// the first record at or AFTER it — so records published in the same
	// millisecond as `middle` are at or after it, and a `middle` taken
	// immediately after the first batch resolves into the middle of that
	// batch. The first run of this test asserted otherwise and failed on
	// exactly that.
	time.Sleep(20 * time.Millisecond)
	middle := time.Now()
	time.Sleep(20 * time.Millisecond)
	for i := n; i < 2*n; i++ {
		publish(t, c, topic, &mq.Message{Body: []byte(strconv.Itoa(i))})
	}

	t.Run("earliest", func(t *testing.T) {
		cons := consumerFrom(t, c, topic, "start-earliest", mq.StartEarliest())
		d := receiveOne(t, cons, 15*time.Second)
		if string(d.Body) != "0" {
			t.Fatalf("earliest gave %q, want the first record", d.Body)
		}
		_ = d.Ack()
	})
	t.Run("sequence", func(t *testing.T) {
		cons := consumerFrom(t, c, topic, "start-seq", mq.StartAtSequence(uint64(n)))
		d := receiveOne(t, cons, 15*time.Second)
		if string(d.Body) != strconv.Itoa(n) {
			t.Fatalf("StartAtSequence(%d) gave %q", n, d.Body)
		}
		_ = d.Ack()
	})
	t.Run("time", func(t *testing.T) {
		cons := consumerFrom(t, c, topic, "start-time", mq.StartAt(middle))
		d := receiveOne(t, cons, 15*time.Second)
		got, err := strconv.Atoi(string(d.Body))
		if err != nil {
			t.Fatalf("StartAt gave %q", d.Body)
		}
		if got < n {
			t.Fatalf("StartAt(middle) gave record %d, want one published after the gap (>= %d)", got, n)
		}
		_ = d.Ack()
	})
	t.Run("sequence-beyond-the-end-is-clamped", func(t *testing.T) {
		// the answer for an out-of-range position: the end. An unclamped
		// offset is OFFSET_OUT_OF_RANGE on every fetch, which is a
		// subscription that never delivers anything.
		cons := consumerFrom(t, c, topic, "start-far", mq.StartAtSequence(1<<40))
		expectNothing(t, cons, 2*time.Second, "a subscription starting beyond the end of the log")
		publish(t, c, topic, &mq.Message{Body: []byte("after")})
		d := receiveOne(t, cons, 15*time.Second)
		if string(d.Body) != "after" {
			t.Fatalf("the clamped subscription gave %q", d.Body)
		}
		_ = d.Ack()
	})
	t.Run("latest", func(t *testing.T) {
		cons := consumerFrom(t, c, topic, "start-latest", mq.StartLatest())
		expectNothing(t, cons, 2*time.Second, "a subscription starting at the end of an existing log")
		publish(t, c, topic, &mq.Message{Body: []byte("new"), Key: []byte("k")})
		d := receiveOne(t, cons, 15*time.Second)
		if string(d.Body) != "new" {
			t.Fatalf("latest gave %q", d.Body)
		}
		_ = d.Ack()
	})
}

// TestSubscriptionIsDurableFromTheMomentItExists is what
// FeatureDurableSubscriptions promises and what the group-position commit is
// for: a subscription opened, closed and published to while it was gone
// resumes where it was rather than at the end.
func TestSubscriptionIsDurableFromTheMomentItExists(t *testing.T) {
	topic := newTopic("durable")
	cl := cluster(t, 1, []string{topic})
	c := openOn(t, cl)
	cons := consumerOn(t, c, topic, "durable-g")
	// Nothing is consumed, but the group now has a position.
	if err := cons.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	publish(t, c, topic, &mq.Message{Body: []byte("while-gone")})
	again := consumerOn(t, c, topic, "durable-g")
	d := receiveOne(t, again, 20*time.Second)
	if string(d.Body) != "while-gone" {
		t.Fatalf("the reopened subscription gave %q; a durable subscription does not skip what was published while it was closed", d.Body)
	}
	if err := d.Ack(); err != nil {
		t.Fatalf("Ack: %v", err)
	}
}

// TestPerCallControlsAreRefusedBeforeIO is a control Kafka cannot honour
// is refused with ErrUnsupported before a byte is written, because a driver
// that ignored it would publish a message the caller asked to be delayed and
// the caller would have no way to find out.
func TestPerCallControlsAreRefusedBeforeIO(t *testing.T) {
	c, topic := openClient(t)
	pub, err := c.Publisher(t.Context(), mq.PublisherConfig{Topic: topic})
	if err != nil {
		t.Fatalf("Publisher: %v", err)
	}
	defer func() { _ = pub.Close() }()
	for name, opt := range map[string]mq.PublishOption{
		"delay":    mq.Delay(time.Minute),
		"ttl":      mq.TTL(time.Minute),
		"priority": mq.Priority(5),
		"dedupkey": mq.DedupKey("k"),
	} {
		t.Run(name, func(t *testing.T) {
			err := pub.Publish(t.Context(), &mq.Message{Body: []byte("x")}, opt)
			if !errors.Is(err, mq.ErrUnsupported) {
				t.Fatalf("Publish with %s = %v, want ErrUnsupported", name, err)
			}
		})
	}
	// And the one control Kafka DOES honour goes through.
	if err := pub.Publish(t.Context(), &mq.Message{Body: []byte("x")}, mq.Partition(0)); err != nil {
		t.Fatalf("Publish with an explicit partition: %v", err)
	}
}

// TestOversizedPayloadIsRefused is the limit enforced rather than
// advertised: the driver read message.max.bytes at open and refuses a body
// above it before the broker has to.
func TestOversizedPayloadIsRefused(t *testing.T) {
	c, topic := openClient(t)
	lim := c.Capabilities().Limit(mq.LimitMaxMessageBytes)
	if !lim.Known {
		t.Skip("the cluster did not report message.max.bytes")
	}
	body := make([]byte, lim.Value+1)
	err := c.Publish(t.Context(), topic, &mq.Message{Body: body})
	if !errors.Is(err, mq.ErrMessageTooLarge) {
		t.Fatalf("publishing %d bytes against a %d-byte limit = %v, want ErrMessageTooLarge", len(body), lim.Value, err)
	}
}

// TestCloseUnblocksReceive is a Close reaches a caller parked in
// Receive, and it reaches one who closed the CLIENT rather than the consumer.
func TestCloseUnblocksReceive(t *testing.T) {
	t.Run("consumer", func(t *testing.T) {
		c, topic := openClient(t)
		cons := consumerOn(t, c, topic, "unblock-1")
		done := make(chan error, 1)
		go func() {
			dst := make([]mq.Delivery, 1)
			_, err := cons.Receive(context.Background(), dst)
			done <- err
		}()
		time.Sleep(200 * time.Millisecond)
		if err := cons.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
		select {
		case err := <-done:
			if !errors.Is(err, mq.ErrClosed) {
				t.Fatalf("the parked Receive returned %v, want ErrClosed", err)
			}
		case <-time.After(15 * time.Second):
			t.Fatal("the parked Receive did not return after Close")
		}
	})
	t.Run("client", func(t *testing.T) {
		topic := newTopic("unblock")
		cl := cluster(t, 1, []string{topic})
		c, err := kq.Open(t.Context(), config(cl))
		if err != nil {
			t.Fatalf("Open: %v", err)
		}
		cons, err := c.Consumer(t.Context(), mq.ConsumerConfig{Topic: topic, Subscription: "unblock-2"})
		if err != nil {
			t.Fatalf("Consumer: %v", err)
		}
		done := make(chan error, 1)
		go func() {
			dst := make([]mq.Delivery, 1)
			_, rerr := cons.Receive(context.Background(), dst)
			done <- rerr
		}()
		time.Sleep(200 * time.Millisecond)
		if err := c.Close(); err != nil {
			t.Fatalf("client Close: %v", err)
		}
		select {
		case err := <-done:
			if !errors.Is(err, mq.ErrClosed) {
				t.Fatalf("the parked Receive returned %v, want ErrClosed", err)
			}
		case <-time.After(15 * time.Second):
			t.Fatal("the parked Receive did not return after the client closed")
		}
	})
}

// TestPublishAfterCloseIsRefused is the other half.
func TestPublishAfterCloseIsRefused(t *testing.T) {
	topic := newTopic("after-close")
	cl := cluster(t, 1, []string{topic})
	c, err := kq.Open(t.Context(), config(cl))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	pub, err := c.Publisher(t.Context(), mq.PublisherConfig{Topic: topic})
	if err != nil {
		t.Fatalf("Publisher: %v", err)
	}
	if err := c.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := pub.Publish(t.Context(), &mq.Message{Body: []byte("x")}); !errors.Is(err, mq.ErrClosed) {
		t.Fatalf("Publish after Close = %v, want ErrClosed", err)
	}
	if err := c.Close(); err != nil {
		t.Fatalf("the second Close: %v", err)
	}
}

// TestReceiptsCarryTheCoordinates is, and on this client the receipt can
// only come from the writer's completion callback: kafka-go's WriteMessages
// returns an error and nothing else.
func TestReceiptsCarryTheCoordinates(t *testing.T) {
	topic := newTopic("receipts")
	cl := cluster(t, 4, []string{topic})
	c := openOn(t, cl)
	pub, err := c.Publisher(t.Context(), mq.PublisherConfig{Topic: topic, Receipts: true})
	if err != nil {
		t.Fatalf("Publisher: %v", err)
	}
	defer func() { _ = pub.Close() }()
	var r mq.PublishReceipt
	m := &mq.Message{Key: []byte("k"), Body: []byte("b"), Receipt: &r}
	if err := pub.Publish(t.Context(), m); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if r.Offset < 0 {
		t.Errorf("receipt offset %d", r.Offset)
	}
	if want := int32(kq.PartitionFor([]byte("k"), 4)); r.Partition != want {
		t.Errorf("receipt partition %d, want %d", r.Partition, want)
	}
	if r.Confirmation != mq.ConfirmDurable && r.Confirmation != mq.ConfirmAccepted {
		t.Errorf("receipt confirmation %v; acks=all is both boundaries at once", r.Confirmation)
	}
	// Kafka assigns no message identifier: the specification makes an empty MessageID "the
	// broker assigns none" rather than a claim.
	if r.MessageID != "" {
		t.Errorf("receipt MessageID %q; Kafka assigns none", r.MessageID)
	}
	// A second publication fills the same slot again rather than leaving the
	// first answer in it.
	prev := r
	m2 := &mq.Message{Key: []byte("k"), Body: []byte("b2"), Receipt: &r}
	if err := pub.Publish(t.Context(), m2); err != nil {
		t.Fatalf("the second Publish: %v", err)
	}
	if r.Offset == prev.Offset {
		t.Errorf("the second publication reported the first's offset %d", r.Offset)
	}
}

// TestAsyncPublishReportsExactlyOnce is a nil return means the callback
// runs exactly once, and the callback costs no goroutine per message.
func TestAsyncPublishReportsExactlyOnce(t *testing.T) {
	c, topic := openClient(t)
	pub, err := c.Publisher(t.Context(), mq.PublisherConfig{Topic: topic})
	if err != nil {
		t.Fatalf("Publisher: %v", err)
	}
	defer func() { _ = pub.Close() }()
	const n = 64
	var (
		calls atomic.Int64
		wg    sync.WaitGroup
	)
	wg.Add(n)
	for i := range n {
		m := &mq.Message{Body: []byte(strconv.Itoa(i))}
		if err := pub.PublishAsync(t.Context(), m, func(res mq.PublishResult) {
			if res.Err != nil {
				t.Errorf("asynchronous publication failed: %v", res.Err)
			}
			calls.Add(1)
			wg.Done()
		}); err != nil {
			t.Fatalf("PublishAsync: %v", err)
		}
	}
	wg.Wait()
	// Flush is the bound: the callbacks have run, and nothing else may run
	// afterwards.
	if err := pub.Flush(t.Context()); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if got := calls.Load(); got != n {
		t.Fatalf("%d callbacks for %d publications", got, n)
	}
	cons := consumerFrom(t, c, topic, "async-g", mq.StartEarliest())
	if got := receiveN(t, cons, n, 25*time.Second, func(d *mq.Delivery) error { return d.Ack() }); got != n {
		t.Fatalf("%d of %d asynchronous publications arrived", got, n)
	}
}

// TestOwnedPublishReleasesExactlyOnce is PublishOwned takes ownership
// at invocation and releases it exactly once, on every path.
func TestOwnedPublishReleasesExactlyOnce(t *testing.T) {
	c, topic := openClient(t)
	pub, err := c.Publisher(t.Context(), mq.PublisherConfig{Topic: topic})
	if err != nil {
		t.Fatalf("Publisher: %v", err)
	}
	defer func() { _ = pub.Close() }()
	var released atomic.Int64
	const n = 32
	for i := range n {
		m := &mq.Message{Body: []byte(strconv.Itoa(i))}
		if err := pub.PublishOwned(t.Context(), mq.OwnedMessage{
			Message: m,
			Release: func(*mq.Message) { released.Add(1) },
		}); err != nil {
			t.Fatalf("PublishOwned: %v", err)
		}
	}
	if err := pub.Flush(t.Context()); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if got := released.Load(); got != n {
		t.Fatalf("%d releases for %d owned publications", got, n)
	}
	// A refusal releases too, which is the path a leak hides on.
	var refused atomic.Int64
	err = pub.PublishOwned(t.Context(), mq.OwnedMessage{
		Message: &mq.Message{Body: []byte("x")},
		Release: func(*mq.Message) { refused.Add(1) },
	}, mq.TTL(time.Minute))
	if !errors.Is(err, mq.ErrUnsupported) {
		t.Fatalf("PublishOwned with a TTL = %v, want ErrUnsupported", err)
	}
	if got := refused.Load(); got != 1 {
		t.Fatalf("a refused owned publication released %d times, want 1", got)
	}
}

// TestConfirmNoneReturnsBeforeTheBroker is the weakest boundary, and the
// copy it forces: the specification lets a caller reuse its buffers the instant Publish
// returns, and kafka-go is still reading them.
func TestConfirmNoneReturnsBeforeTheBroker(t *testing.T) {
	topic := newTopic("confirm-none")
	cl := cluster(t, 1, []string{topic})
	c := openOn(t, cl)
	pub, err := c.Publisher(t.Context(), mq.PublisherConfig{Topic: topic, Confirmation: mq.ConfirmNone})
	if err != nil {
		t.Fatalf("Publisher: %v", err)
	}
	defer func() { _ = pub.Close() }()
	const n = 16
	body := make([]byte, 8)
	for i := range n {
		// The caller's buffer is REUSED and mutated immediately after every
		// call, which is exactly what the specification permits. A driver that retained
		// it would publish the last value n times.
		copy(body, strconv.Itoa(i)+"       ")
		if err := pub.Publish(t.Context(), &mq.Message{Body: body}); err != nil {
			t.Fatalf("Publish: %v", err)
		}
	}
	if err := pub.Flush(t.Context()); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	cons := consumerFrom(t, c, topic, "confirm-none-g", mq.StartEarliest())
	got := map[string]bool{}
	receiveN(t, cons, n, 25*time.Second, func(d *mq.Delivery) error {
		got[string(d.Body)] = true
		return d.Ack()
	})
	for i := range n {
		want := strconv.Itoa(i) + "       "
		want = want[:8]
		if !got[want] {
			t.Errorf("the record for %d did not arrive intact; the driver retained the caller's buffer. Arrived: %v", i, keysOf(got))
		}
	}
}

func keysOf(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	slices.Sort(out)
	return out
}

// TestBatchPublishResultsAreIndexAligned is the contract and the one that
// loses a message silently when it is wrong: a result aligned with anything
// but the caller's slice reports one message's outcome for another, and on
// THIS client a wrongly retried message is a duplicate the broker does not
// de-duplicate.
func TestBatchPublishResultsAreIndexAligned(t *testing.T) {
	c, topic := openClient(t)
	pub, err := c.Publisher(t.Context(), mq.PublisherConfig{Topic: topic})
	if err != nil {
		t.Fatalf("Publisher: %v", err)
	}
	defer func() { _ = pub.Close() }()
	lim := c.Capabilities().Limit(mq.LimitMaxMessageBytes)
	if !lim.Known {
		t.Skip("the cluster did not report message.max.bytes")
	}
	ms := []*mq.Message{
		{Body: []byte("ok-0")},
		{Body: make([]byte, lim.Value+1)}, // refused by the driver itself
		{Body: []byte("ok-2")},
	}
	err = pub.PublishBatch(t.Context(), ms)
	if err == nil {
		t.Fatal("PublishBatch accepted a batch with an oversized message")
	}
	// the controls are checked for EVERY message before anything is
	// written, so an oversized message refuses the whole batch rather than
	// two of three: the driver must not write part of a batch it is going to
	// refuse.
	if !errors.Is(err, mq.ErrMessageTooLarge) {
		t.Fatalf("PublishBatch = %v, want ErrMessageTooLarge", err)
	}
	cons := consumerOn(t, c, topic, "batch-align")
	expectNothing(t, cons, time.Second, "any message of a batch the driver refused")

	// And a batch that is entirely acceptable arrives entirely.
	good := []*mq.Message{{Body: []byte("a")}, {Body: []byte("b")}, {Body: []byte("c")}}
	if err := pub.PublishBatch(t.Context(), good); err != nil {
		t.Fatalf("PublishBatch: %v", err)
	}
	seen := map[string]bool{}
	receiveN(t, cons, len(good), 20*time.Second, func(d *mq.Delivery) error {
		seen[string(d.Body)] = true
		return d.Ack()
	})
	for _, m := range good {
		if !seen[string(m.Body)] {
			t.Errorf("the batch message %q did not arrive", m.Body)
		}
	}
}

// TestBatchSummaryAndOutcome are the two pure functions the batch contract
// rests on, and they are exported because a wrong answer here is invisible in
// an integration test.
func TestBatchSummaryAndOutcome(t *testing.T) {
	if err := kq.BatchSummary(nil); err != nil {
		t.Errorf("BatchSummary(nil) = %v", err)
	}
	if err := kq.BatchSummary([]error{nil, nil}); err != nil {
		t.Errorf("BatchSummary of an accepted batch = %v", err)
	}
	one := errors.New("boom")
	if err := kq.BatchSummary([]error{one, one}); !errors.Is(err, one) {
		t.Errorf("BatchSummary of a wholly failed batch = %v, want the one error rather than a BatchError", err)
	}
	var be *mq.BatchError
	if err := kq.BatchSummary([]error{nil, one, nil}); !errors.As(err, &be) {
		t.Errorf("BatchSummary of a partial failure = %v, want a *BatchError naming which messages failed", err)
	} else if len(be.Errs) != 3 || be.Errs[0] != nil || be.Errs[1] == nil || be.Errs[2] != nil {
		t.Errorf("the BatchError's outcomes are %v, want [nil, err, nil]", be.Errs)
	}
	// BatchOutcome maps each native answer where it stands.
	out := kq.BatchOutcome([]error{nil, kafkaGoError(19), nil, kafkaGoError(20)})
	if len(out) != 4 || out[0] != nil || out[2] != nil {
		t.Fatalf("BatchOutcome moved an accepted message's nil: %v", out)
	}
	if mq.CodeOf(out[1]) != mq.CodeUnavailable {
		t.Errorf("NOT_ENOUGH_REPLICAS at index 1 mapped to %v", mq.CodeOf(out[1]))
	}
	if !mq.IsAmbiguous(out[3]) {
		t.Errorf("NOT_ENOUGH_REPLICAS_AFTER_APPEND at index 3 is not ambiguous, and it is an append that happened")
	}
	if kq.BatchOutcome(nil) != nil {
		t.Error("BatchOutcome(nil) is not nil")
	}
}

// TestMetadataEnvelopeRoundTrip is the portable mode: the whole message
// goes into the binary envelope, so a consumer needs no knowledge of the
// header mapping.
func TestMetadataEnvelopeRoundTrip(t *testing.T) {
	topic := newTopic("envelope")
	cl := cluster(t, 1, []string{topic})
	c := openOn(t, cl)
	pub, err := c.Publisher(t.Context(), mq.PublisherConfig{Topic: topic, Metadata: mq.MetadataPortableEnvelope})
	if err != nil {
		t.Fatalf("Publisher: %v", err)
	}
	defer func() { _ = pub.Close() }()
	cons, err := c.Consumer(t.Context(), mq.ConsumerConfig{Topic: topic, Subscription: "env-g", Metadata: mq.MetadataPortableEnvelope})
	if err != nil {
		t.Fatalf("Consumer: %v", err)
	}
	defer func() { _ = cons.Close() }()
	m := &mq.Message{ID: "e-1", Key: []byte("k"), Body: []byte("b")}
	m.Headers.Add("h", []byte("v"))
	m.Properties.ContentType = "text/plain"
	if err := pub.Publish(t.Context(), m); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	d := receiveOne(t, cons, 20*time.Second)
	if d.ID != "e-1" || string(d.Body) != "b" || string(d.Key) != "k" {
		t.Fatalf("the enveloped delivery is %+v", d)
	}
	if v, ok := d.Headers.Get("h"); !ok || string(v) != "v" {
		t.Errorf("the enveloped header is %q (present %v)", v, ok)
	}
	if d.Properties.ContentType != "text/plain" {
		t.Errorf("the enveloped content type is %q", d.Properties.ContentType)
	}
	if err := d.Ack(); err != nil {
		t.Fatalf("Ack: %v", err)
	}
}

// TestMetadataStrictRefusesOnlyWhatItMust records something worth stating:
// three of the four modes behave identically on Kafka, because Kafka's
// headers are ordered, binary and duplicate-preserving and its key and value
// are nullable byte strings. Only the reserved prefix is refused.
func TestMetadataStrictRefusesOnlyWhatItMust(t *testing.T) {
	c, topic := openClient(t)
	for _, mode := range []mq.MetadataMode{mq.MetadataNativeStrict, mq.MetadataLenientDrop, mq.MetadataAdaptive} {
		pub, err := c.Publisher(t.Context(), mq.PublisherConfig{Topic: topic, Metadata: mode})
		if err != nil {
			t.Fatalf("Publisher in %v: %v", mode, err)
		}
		m := &mq.Message{Body: []byte("b")}
		m.Headers.Add("a", []byte("1"))
		m.Headers.Add("a", []byte("2"))
		m.Headers.Add("bin", []byte{0, 1, 2, 0xff})
		if err := pub.Publish(t.Context(), m); err != nil {
			t.Errorf("Publish in %v refused a message Kafka can carry exactly: %v", mode, err)
		}
		_ = pub.Close()
	}
	strict, err := c.Publisher(t.Context(), mq.PublisherConfig{Topic: topic, Metadata: mq.MetadataNativeStrict})
	if err != nil {
		t.Fatalf("Publisher: %v", err)
	}
	defer func() { _ = strict.Close() }()
	bad := &mq.Message{Body: []byte("b")}
	bad.Headers.Add(mq.ReservedHeaderPrefix+"mine", []byte("x"))
	if err := strict.Publish(t.Context(), bad); err == nil {
		t.Error("strict mode accepted a caller's header under the reserved prefix")
	}
}

// TestNackBlockPartitionRedelivers is the default: the record is held
// in process, its offset is not committed, and it comes back after the delay
// — the only strategy that changes neither ordering nor durability.
func TestNackBlockPartitionRedelivers(t *testing.T) {
	topic := newTopic("nack-block")
	cl := cluster(t, 1, []string{topic})
	c := openOn(t, cl, func(cfg *kq.Config) {
		cfg.Nack = nack.BlockPartition
		cfg.NackConfig.Delay = 150 * time.Millisecond
	})
	cons := consumerOn(t, c, topic, "nack-block-g")
	publish(t, c, topic, &mq.Message{Body: []byte("retry-me")})
	d := receiveOne(t, cons, 15*time.Second)
	start := time.Now()
	if err := d.Nack(); err != nil {
		t.Fatalf("Nack: %v", err)
	}
	again := receiveOne(t, cons, 15*time.Second)
	if string(again.Body) != "retry-me" {
		t.Fatalf("the redelivery carries %q", again.Body)
	}
	if elapsed := time.Since(start); elapsed < 100*time.Millisecond {
		t.Errorf("the redelivery arrived after %v; the strategy's delay is 150ms and a redelivery that ignored it would spin", elapsed)
	}
	if got := again.Info().Attempt; got < 2 {
		t.Errorf("the redelivery reports attempt %d, want at least 2", got)
	}
	if !again.Info().Redelivered {
		t.Error("the redelivery does not report Redelivered")
	}
	if err := again.Ack(); err != nil {
		t.Fatalf("Ack: %v", err)
	}
	expectNothing(t, cons, 500*time.Millisecond, "a record that was nacked and then acknowledged")
}

// TestNackStopHaltsTheConsumer is ADR-011: the stop strategy leaves the record
// unacknowledged and refuses every later Fetch, so nothing after the refused
// record is consumed until the subscription is reopened.
func TestNackStopHaltsTheConsumer(t *testing.T) {
	topic := newTopic("nack-stop")
	cl := cluster(t, 1, []string{topic})
	c := openOn(t, cl, func(cfg *kq.Config) { cfg.Nack = nack.Stop })
	cons := consumerOn(t, c, topic, "nack-stop-g")
	publish(t, c, topic, &mq.Message{Body: []byte("poison")})
	publish(t, c, topic, &mq.Message{Body: []byte("after")})
	d := receiveOne(t, cons, 15*time.Second)
	if err := d.Nack(); err != nil {
		t.Fatalf("Nack: %v", err)
	}
	dst := make([]mq.Delivery, 1)
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	n, err := cons.Receive(ctx, dst)
	if n > 0 {
		t.Fatalf("a delivery (%q) arrived after the consumer was stopped", dst[0].Body)
	}
	if err == nil {
		t.Fatal("Receive after a stop-the-consumer nack returned no error")
	}
	if mq.CodeOf(err) != mq.CodePoisonMessage {
		t.Errorf("Receive after a stop = %v (code %v), want CodePoisonMessage", err, mq.CodeOf(err))
	}
	// The record is unacknowledged, so a reopened subscription gets it again.
	if err := cons.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	again := consumerOn(t, c, topic, "nack-stop-g")
	back := receiveOne(t, again, 15*time.Second)
	if string(back.Body) != "poison" {
		t.Fatalf("the reopened subscription gave %q, want the record the stop refused", back.Body)
	}
	_ = back.Ack()
}

// TestRejectWithoutADeadLetterTopicIsRefused is the rule: a driver that
// claims the dead-letter feature and has nowhere to route to must refuse
// rather than destroy the message.
func TestRejectWithoutADeadLetterTopicIsRefused(t *testing.T) {
	c, topic := openClient(t)
	cons := consumerOn(t, c, topic, "no-dlq")
	publish(t, c, topic, &mq.Message{Body: []byte("keep-me")})
	d := receiveOne(t, cons, 15*time.Second)
	if err := d.Reject(); !errors.Is(err, mq.ErrUnsupported) {
		t.Fatalf("Reject with no dead-letter topic = %v, want ErrUnsupported", err)
	}
	// The record is still settleable, so nothing was lost.
	if err := d.Ack(); err != nil {
		t.Fatalf("Ack after a refused Reject: %v", err)
	}
}

// TestDeadLetterCarriesProvenance is a moved record says where it came
// from, so an operator reading the dead-letter topic can find the original.
func TestDeadLetterCarriesProvenance(t *testing.T) {
	topic, dlq := newTopic("dl"), newTopic("dl-dead")
	cl := cluster(t, 1, []string{topic, dlq})
	c := openOn(t, cl, func(cfg *kq.Config) { cfg.NackConfig.DeadLetterTopic = dlq })
	cons := consumerOn(t, c, topic, "dl-g")
	publish(t, c, topic, &mq.Message{Body: []byte("dead")})
	d := receiveOne(t, cons, 15*time.Second)
	info := d.Info()
	if err := d.Reject(); err != nil {
		t.Fatalf("Reject: %v", err)
	}
	reader := consumerFrom(t, c, dlq, "dl-reader", mq.StartEarliest())
	got := receiveOne(t, reader, 15*time.Second)
	if string(got.Body) != "dead" {
		t.Fatalf("the dead-letter topic holds %q", got.Body)
	}
	for key, want := range map[string]string{
		kq.HeaderOriginTopic:        info.Topic,
		kq.HeaderOriginSubscription: "dl-g",
		kq.HeaderOriginPartition:    strconv.Itoa(int(info.Partition)),
		kq.HeaderOriginOffset:       strconv.FormatInt(info.Offset, 10),
	} {
		v, ok := got.Headers.Get(key)
		if !ok {
			t.Errorf("the dead-lettered record carries no %s", key)
			continue
		}
		if string(v) != want {
			t.Errorf("%s = %q, want %q", key, v, want)
		}
	}
	if _, ok := got.Headers.Get(kq.HeaderFirstDeliveredAt); !ok {
		t.Errorf("the dead-lettered record carries no %s", kq.HeaderFirstDeliveredAt)
	}
	_ = got.Ack()
	// And the original is settled, because the move was confirmed first.
	expectNothing(t, cons, time.Second, "a record whose dead-lettering was confirmed")
}

// TestAdminSurface is end to end.
func TestAdminSurface(t *testing.T) {
	topic := newTopic("adm")
	cl := cluster(t, 1, nil)
	c := openOn(t, cl, func(cfg *kq.Config) { cfg.Partitions = 2; cfg.ReplicationFactor = 1 })
	admin, err := c.Admin()
	if err != nil {
		t.Fatalf("Admin: %v", err)
	}
	if err := admin.EnsureTopic(t.Context(), mq.TopicSpec{Name: topic}); err != nil {
		t.Fatalf("EnsureTopic: %v", err)
	}
	// Idempotent, which is what "ensure" means.
	if err := admin.EnsureTopic(t.Context(), mq.TopicSpec{Name: topic}); err != nil {
		t.Fatalf("the second EnsureTopic: %v", err)
	}
	found := false
	for name, err := range admin.ListTopics(t.Context()) {
		if err != nil {
			t.Fatalf("ListTopics: %v", err)
		}
		if name == topic {
			found = true
		}
		if name == "__consumer_offsets" {
			t.Error("ListTopics reported an internal topic")
		}
	}
	if !found {
		t.Fatalf("ListTopics did not report %q", topic)
	}
	if err := admin.EnsureSubscription(t.Context(), mq.SubscriptionSpec{Topic: topic, Name: "adm-g"}); err != nil {
		t.Fatalf("EnsureSubscription: %v", err)
	}
	// Stats: the lag is the depth on Kafka and the in-flight count does not
	// exist, which the specification requires be reported as unknown rather than zero.
	const n = 5
	for i := range n {
		publish(t, c, topic, &mq.Message{Key: []byte("k" + strconv.Itoa(i)), Body: []byte(strconv.Itoa(i))})
	}
	stats, err := admin.Stats(t.Context(), topic, "adm-g")
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	if !stats.Lag.Known || stats.Lag.Value != n {
		t.Errorf("Stats lag = %+v, want %d", stats.Lag, n)
	}
	if !stats.Depth.Known || stats.Depth.Value != stats.Lag.Value {
		t.Errorf("Stats depth = %+v; on Kafka the depth IS the lag", stats.Depth)
	}
	if stats.InFlight.Known {
		t.Errorf("Stats in-flight = %+v; the broker does not know how many delivered records a consumer holds", stats.InFlight)
	}
	// Purge moves the group's position to the end rather than destroying
	// records, which the driver page says because a caller from a queue
	// broker expects otherwise.
	if err := admin.Purge(t.Context(), topic, "adm-g"); err != nil {
		t.Fatalf("Purge: %v", err)
	}
	after, err := admin.Stats(t.Context(), topic, "adm-g")
	if err != nil {
		t.Fatalf("Stats after Purge: %v", err)
	}
	if after.Lag.Value != 0 {
		t.Errorf("the lag after Purge is %d, want 0", after.Lag.Value)
	}
	cons := consumerOn(t, c, topic, "adm-g")
	expectNothing(t, cons, 2*time.Second, "the backlog a Purge skipped")
	if err := cons.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := admin.DeleteSubscription(t.Context(), topic, "adm-g"); err != nil {
		t.Fatalf("DeleteSubscription: %v", err)
	}
	// Idempotent.
	if err := admin.DeleteSubscription(t.Context(), topic, "adm-g"); err != nil {
		t.Fatalf("the second DeleteSubscription: %v", err)
	}
	if err := admin.DeleteTopic(t.Context(), topic); err != nil {
		t.Fatalf("DeleteTopic: %v", err)
	}
	if err := admin.DeleteTopic(t.Context(), topic); err != nil {
		t.Fatalf("the second DeleteTopic: %v", err)
	}
	// And every administrative call refuses a name Kafka does not accept,
	// before any round trip.
	if err := admin.EnsureTopic(t.Context(), mq.TopicSpec{Name: "a/b"}); err == nil {
		t.Error("EnsureTopic accepted a name Kafka does not")
	}
	if _, err := admin.Stats(t.Context(), topic, "bad group"); err == nil {
		t.Error("Stats accepted a group name Kafka does not")
	}
}

// TestPingAndNative are the two liveness and escape-hatch surfaces.
func TestPingAndNative(t *testing.T) {
	topic := newTopic("ping")
	cl := cluster(t, 1, []string{topic})
	c, b := openPair(t, cl)
	if err := c.Ping(t.Context()); err != nil {
		t.Fatalf("Ping: %v", err)
	}
	if b.Writer() == nil || b.AsyncWriter() == nil || b.Client() == nil || b.Transport() == nil {
		t.Error("one of the typed escape hatches is nil")
	}
	if b.Writer() == b.AsyncWriter() {
		t.Error("the synchronous and asynchronous writers are the same object; Async is a field of a kafka-go Writer, so one writer cannot offer both")
	}
	if c.Native() != any(b.Writer()) {
		t.Errorf("Client.Native() is not the driver's writer")
	}
	if err := b.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := c.Ping(t.Context()); !errors.Is(err, mq.ErrClosed) {
		t.Errorf("Ping after Close = %v, want ErrClosed", err)
	}
	if _, err := b.Admin(); !errors.Is(err, mq.ErrClosed) {
		t.Errorf("Admin after Close = %v, want ErrClosed", err)
	}
}

// TestTopicPrefixIsolatesTwoDeployments is what a shared cluster needs: two
// clients with different prefixes do not see each other's topics, and each
// one's deliveries name the unprefixed topic.
func TestTopicPrefixIsolatesTwoDeployments(t *testing.T) {
	cl := cluster(t, 1, nil)
	a := openOn(t, cl, func(cfg *kq.Config) { cfg.TopicPrefix = "a."; cfg.AutoCreateTopics = true })
	b := openOn(t, cl, func(cfg *kq.Config) { cfg.TopicPrefix = "b."; cfg.AutoCreateTopics = true })
	for _, c := range []*mq.Client{a, b} {
		admin, err := c.Admin()
		if err != nil {
			t.Fatalf("Admin: %v", err)
		}
		if err := admin.EnsureTopic(t.Context(), mq.TopicSpec{Name: "orders"}); err != nil {
			t.Fatalf("EnsureTopic: %v", err)
		}
	}
	consA := consumerOn(t, a, "orders", "g")
	consB := consumerOn(t, b, "orders", "g")
	publish(t, a, "orders", &mq.Message{Body: []byte("for-a")})
	d := receiveOne(t, consA, 20*time.Second)
	if string(d.Body) != "for-a" {
		t.Fatalf("the prefixed consumer received %q", d.Body)
	}
	if d.Topic != "orders" {
		t.Errorf("the delivery names %q; the resolved topic is the one the caller published to", d.Topic)
	}
	_ = d.Ack()
	expectNothing(t, consB, 2*time.Second, "a record published under another deployment's prefix")
}

// TestCountersReportWhatHappened is the sampled half: the numbers a
// deployment reads when health carries only the latest reason.
func TestCountersReportWhatHappened(t *testing.T) {
	topic := newTopic("counters")
	cl := cluster(t, 1, []string{topic})
	c, b := openPair(t, cl)
	cons := consumerOn(t, c, topic, "counters-g")
	publish(t, c, topic, &mq.Message{Body: []byte("x")})
	d := receiveOne(t, cons, 15*time.Second)
	if err := d.Ack(); err != nil {
		t.Fatalf("Ack: %v", err)
	}
	waitFor(t, 10*time.Second, "a commit", func() bool { return b.Commits() > 0 })
	if b.Reconnects() != 0 {
		t.Errorf("Reconnects = %d before anything reconnected", b.Reconnects())
	}
	if b.AmbiguousPublications() != 0 || b.DuplicablePublications() != 0 {
		t.Errorf("ambiguous=%d duplicable=%d after a clean publication", b.AmbiguousPublications(), b.DuplicablePublications())
	}
	// Lenient mode drops nothing on Kafka, and the counter exists to say so.
	if b.DroppedMetadata() != 0 {
		t.Errorf("DroppedMetadata = %d; Kafka can represent every field the specification names", b.DroppedMetadata())
	}
	if b.Rebalances() != 0 {
		t.Logf("Rebalances = %d", b.Rebalances())
	}
	if b.DegradedTransitions() != 0 {
		t.Logf("DegradedTransitions = %d", b.DegradedTransitions())
	}
}
