package pulsar_test

import (
	"bytes"
	"context"
	"errors"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"

	mq "bitbucket.org/iqhive/iqmq"
	pd "bitbucket.org/iqhive/iqmq/drivers/pulsar"
	"bitbucket.org/iqhive/iqmq/drivers/pulsar/pulsarfake"
)

// TestProducerIsCachedPerTopic is the specification's "producer per topic cached", and the
// refcount is what makes it safe: one sink closing must not close a producer
// its sibling is publishing through.
func TestProducerIsCachedPerTopic(t *testing.T) {
	b, _ := newRig(t, nil)
	first, err := b.OpenSink(context.Background(), mq.SinkConfig{Topic: "shared"})
	if err != nil {
		t.Fatalf("OpenSink: %v", err)
	}
	second, err := b.OpenSink(context.Background(), mq.SinkConfig{Topic: "shared"})
	if err != nil {
		t.Fatalf("a second sink on one topic: %v", err)
	}
	if err := first.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	// The second sink still works, which is what the reference count buys.
	if err := second.Publish(context.Background(), &mq.Message{Body: []byte("x")}, nil); err != nil {
		t.Fatalf("publishing through the surviving sink: %v", err)
	}
	if err := second.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := second.Publish(context.Background(), &mq.Message{Body: []byte("y")}, nil); !errors.Is(err, mq.ErrClosed) {
		t.Fatalf("publishing through a closed sink reported %v, want ErrClosed", err)
	}
}

// TestQueueFullIsBackpressureNotAFailure is the plan's own requirement and a
// real distinction: the message was NEVER admitted, so it is not ambiguous, and
// it is not a broker that is down either.
func TestQueueFullIsBackpressureNotAFailure(t *testing.T) {
	server := pulsarfake.New()
	cfg := rigConfig(server)
	b, err := pd.BackendWithClient(context.Background(), server.Client(), 1, cfg)
	if err != nil {
		t.Fatalf("BackendWithClient: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })
	snk := sinkOf(t, b, "full", mq.MetadataNativeStrict)
	prod, ok := b.Native().(interface {
		CreateProducer(pulsar.ProducerOptions) (pulsar.Producer, error)
	})
	if !ok {
		t.Fatal("the backend's native handle is not the client")
	}
	_ = prod
	// The model's producer is asked to answer the next publication with
	// ProducerQueueIsFull, which is what a producer whose send queue has
	// filled does when DisableBlockIfQueueFull is set — and this driver sets
	// it, because the specification gives the core a backpressure mode and a driver that
	// blocked inside Publish would hide the condition entirely.
	server.SetQueueFull("persistent://public/default/full", true)
	err = snk.Publish(context.Background(), &mq.Message{Body: []byte("x")}, nil)
	if mq.CodeOf(err) != mq.CodeBackpressure {
		t.Fatalf("a full producer queue reported %v (code %v), want Backpressure: mapping it to "+
			"Unavailable would make a caller's retry policy hammer a broker that is keeping up",
			err, mq.CodeOf(err))
	}
	if mq.IsAmbiguous(err) {
		t.Fatal("a full producer queue was reported as ambiguous; the message was never admitted, so " +
			"the outcome is certain ")
	}
	if mq.IsRetryable(err) {
		t.Fatal("a full producer queue was reported as retryable at the driver; the specification gives the " +
			"decision to the core's backpressure mode")
	}
	server.SetQueueFull("persistent://public/default/full", false)
	if err := snk.Publish(context.Background(), &mq.Message{Body: []byte("x")}, nil); err != nil {
		t.Fatalf("publishing once the queue drained: %v", err)
	}
}

// TestPublishAdmittedDecidesAmbiguity is the rule and the disqualifier:
// calling an ambiguous publication definite costs a lost message or a silent
// duplicate.
func TestPublishAdmittedDecidesAmbiguity(t *testing.T) {
	for _, tc := range []struct {
		err  error
		want bool
	}{
		{pulsar.ErrSendQueueIsFull, false},
		{pulsar.ErrMessageTooLarge, false},
		{pulsar.ErrProducerClosed, false},
		{pulsar.ErrProducerFenced, false},
		{pulsar.ErrTopicTerminated, false},
		// A timeout is the case that matters: the publication was
		// admitted and the receipt did not arrive, so the broker may
		// well have it.
		{pulsar.ErrSendTimeout, true},
		{pulsar.ErrContextExpired, true},
	} {
		if got := pd.PublishAdmitted(tc.err); got != tc.want {
			t.Errorf("PublishAdmitted(%v) is %v, want %v", tc.err, got, tc.want)
		}
	}
	if pd.PublishAdmitted(nil) {
		t.Error("PublishAdmitted(nil) is true")
	}
	if pd.PublishAdmitted(errors.New("not the library's")) {
		t.Error("PublishAdmitted reported an error that is not the library's as admitted; a validation " +
			"refusal this driver made itself never reached the client")
	}
}

// TestAsyncPublishUsesTheNativeCallback is the specification's "SendAsync native callback":
// exactly one callback per publication, on the client's own goroutine, with no
// completion pool of this driver's.
func TestAsyncPublishUsesTheNativeCallback(t *testing.T) {
	b, _ := newRig(t, nil)
	snk := sinkOf(t, b, "async", mq.MetadataNativeStrict)
	as, ok := snk.(mq.AsyncSink)
	if !ok {
		t.Fatal("the sink is not an AsyncSink, and SendAsync is the native asynchronous path")
	}
	const n = 32
	var mu sync.Mutex
	calls := map[int]int{}
	done := make(chan struct{}, n)
	for i := range n {
		m := &mq.Message{Body: []byte(strconv.Itoa(i))}
		if err := as.PublishAsync(context.Background(), m, nil, func(err error) {
			mu.Lock()
			calls[i]++
			mu.Unlock()
			if err != nil {
				t.Errorf("publication %d: %v", i, err)
			}
			done <- struct{}{}
		}); err != nil {
			t.Fatalf("PublishAsync %d: %v", i, err)
		}
	}
	for range n {
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("an asynchronous callback never ran; a nil return from PublishAsync promises it runs exactly once")
		}
	}
	if err := snk.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	time.Sleep(20 * time.Millisecond)
	mu.Lock()
	defer mu.Unlock()
	for i := range n {
		if calls[i] != 1 {
			t.Fatalf("the callback for publication %d ran %d times, want exactly once: a callback that "+
				"runs twice releases a pooled message twice", i, calls[i])
		}
	}
}

// TestPublishAsyncRefusesWithoutRunningTheCallback is the other clause: a
// non-nil return means done does NOT run.
func TestPublishAsyncRefusesWithoutRunningTheCallback(t *testing.T) {
	b, _ := newRig(t, nil)
	snk := sinkOf(t, b, "asyncrefuse", mq.MetadataNativeStrict)
	as := snk.(mq.AsyncSink)
	ran := false
	m := &mq.Message{Body: []byte("b")}
	m.Headers.Set("trace", []byte{0x00, 0xff}) // unrepresentable in strict mode
	err := as.PublishAsync(context.Background(), m, nil, func(error) { ran = true })
	if err == nil {
		t.Fatal("a strict asynchronous publication of a binary header was accepted")
	}
	if ran {
		t.Fatal("the callback ran after a non-nil return from PublishAsync; the specification says it does not")
	}
}

// TestOwnedPublishReleasesExactlyOnce is ownership transfers at
// invocation, so the release is the sink's obligation on every path — including
// when it refuses the publication.
func TestOwnedPublishReleasesExactlyOnce(t *testing.T) {
	b, _ := newRig(t, nil)
	snk := sinkOf(t, b, "owned", mq.MetadataNativeStrict)
	os, ok := snk.(mq.OwnedSink)
	if !ok {
		t.Fatal("the sink is not an OwnedSink")
	}
	t.Run("on success", func(t *testing.T) {
		const n = 16
		var mu sync.Mutex
		released := map[*mq.Message]int{}
		done := make(chan struct{}, n)
		for i := range n {
			m := &mq.Message{Body: []byte(strconv.Itoa(i))}
			err := os.PublishOwned(context.Background(), mq.OwnedMessage{Message: m, Release: func(got *mq.Message) {
				mu.Lock()
				released[m]++
				mu.Unlock()
				if got != m {
					t.Errorf("Release was handed a message other than the one transferred")
				}
				done <- struct{}{}
			}}, nil)
			if err != nil {
				t.Fatalf("PublishOwned %d: %v", i, err)
			}
		}
		for range n {
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Fatal("an owned publication never released its storage; a caller's pool leaks one message per publication")
			}
		}
		mu.Lock()
		defer mu.Unlock()
		for m, c := range released {
			if c != 1 {
				t.Fatalf("the storage of %q was released %d times; a second release corrupts the caller's free list", m.Body, c)
			}
		}
	})
	t.Run("on refusal", func(t *testing.T) {
		m := &mq.Message{Body: []byte("b")}
		m.Headers.Set("trace", []byte{0x00, 0xff})
		count := 0
		err := os.PublishOwned(context.Background(), mq.OwnedMessage{Message: m, Release: func(*mq.Message) {
			count++
		}}, nil)
		if err == nil {
			t.Fatal("an unrepresentable owned publication was accepted in strict mode")
		}
		if count != 1 {
			t.Fatalf("a refused owned publication released its storage %d times, want once: ownership "+
				"transfers at invocation, so the release is the sink's obligation even when it "+
				"refuses", count)
		}
	})
}

// TestBatchPublishAlignsItsResult is the whole batch contract: a result
// aligned with anything but the input reports one message's outcome for
// another.
func TestBatchPublishAlignsItsResult(t *testing.T) {
	b, _ := newRig(t, nil)
	src := sourceOf(t, b, "batchpub", "workers", mq.MetadataNativeStrict, 16)
	snk := sinkOf(t, b, "batchpub", mq.MetadataNativeStrict)
	bs, ok := snk.(mq.BatchSink)
	if !ok {
		t.Fatal("the sink is not a BatchSink")
	}
	bad := &mq.Message{Body: []byte("bad")}
	bad.Headers.Set("trace", []byte{0x00, 0xff})
	ms := []*mq.Message{
		{Body: []byte("a")},
		bad,
		{Body: []byte("c")},
	}
	err := bs.PublishBatch(context.Background(), ms, nil)
	var berr *mq.BatchError
	if !errors.As(err, &berr) {
		t.Fatalf("a partly refused batch reported %v (code %v), want a *BatchError", err, mq.CodeOf(err))
	}
	if len(berr.Errs) != len(ms) {
		t.Fatalf("the batch error carries %d outcomes for %d messages", len(berr.Errs), len(ms))
	}
	if berr.Errs[0] != nil || berr.Errs[2] != nil {
		t.Fatalf("the batch error blames a message that was accepted: %v / %v", berr.Errs[0], berr.Errs[2])
	}
	if berr.Errs[1] == nil {
		t.Fatal("the batch error does not blame the message that was refused")
	}
	// And an all-accepted batch returns nil, never an empty *BatchError.
	if err := bs.PublishBatch(context.Background(), []*mq.Message{{Body: []byte("d")}}, nil); err != nil {
		t.Fatalf("an accepted batch reported %v", err)
	}
	got := fetchN(t, src, 3)
	for i := range got {
		if err := src.Settler().Ack(context.Background(), &got[i]); err != nil {
			t.Fatalf("Ack: %v", err)
		}
	}
}

// TestReceiptsCarryWhatTheBrokerSaid is the receipt, filled in place into
// the caller's own storage.
func TestReceiptsCarryWhatTheBrokerSaid(t *testing.T) {
	b, _ := newRig(t, nil)
	snk, err := b.OpenSink(context.Background(), mq.SinkConfig{
		Topic: "receipted", Confirmation: mq.ConfirmAccepted, Receipts: true,
	})
	if err != nil {
		t.Fatalf("OpenSink: %v", err)
	}
	t.Cleanup(func() { _ = snk.Close() })
	var receipt mq.PublishReceipt
	m := &mq.Message{Body: []byte("x"), Receipt: &receipt}
	if err := snk.Publish(context.Background(), m, nil); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if receipt.MessageID == "" {
		t.Fatal("the receipt carries no message identifier, and Producer.Send returns one")
	}
	if receipt.Confirmation != mq.ConfirmAccepted {
		t.Fatalf("the receipt reports confirmation %v", receipt.Confirmation)
	}
	if receipt.ConfirmedAt.IsZero() {
		t.Fatal("the receipt carries no confirmation time")
	}
	if receipt.Native == nil {
		t.Fatal("the receipt carries no native handle")
	}
}

// TestDurableConfirmFollowsTheDomain is a publisher asking for a durable
// boundary on a topic that has none fails at construction rather than being
// given a weaker one silently.
func TestDurableConfirmFollowsTheDomain(t *testing.T) {
	persistent, _ := newRig(t, nil)
	if _, err := persistent.OpenSink(context.Background(), mq.SinkConfig{
		Topic: "durable", Confirmation: mq.ConfirmDurable,
	}); err != nil {
		t.Fatalf("a durable publisher on a persistent topic: %v", err)
	}
	volatile, _ := newRig(t, func(c *pd.Config) { c.Domain = pd.DomainNonPersistent })
	_, err := volatile.OpenSink(context.Background(), mq.SinkConfig{
		Topic: "durable", Confirmation: mq.ConfirmDurable,
	})
	if !errors.Is(err, mq.ErrUnsupported) {
		t.Fatalf("a durable publisher on a non-persistent topic reported %v (code %v), want "+
			"ErrUnsupported", err, mq.CodeOf(err))
	}
	// And a fully qualified non-persistent topic on a persistent client is
	// the same refusal, because the domain travels with the name.
	if _, err := persistent.OpenSink(context.Background(), mq.SinkConfig{
		Topic: "non-persistent://public/default/volatile", Confirmation: mq.ConfirmDurable,
	}); !errors.Is(err, mq.ErrUnsupported) {
		t.Fatalf("a durable publisher on a fully qualified non-persistent topic reported %v", err)
	}
}

// TestScheduledPublishIsHeldByTheBroker is the delayed publication's happy
// path, and the proof that it is the BROKER holding it rather than this
// process.
func TestScheduledPublishIsHeldByTheBroker(t *testing.T) {
	const topic = "persistent://public/default/scheduled"
	b, server := newRig(t, nil)
	src := sourceOf(t, b, "scheduled", "workers", mq.MetadataNativeStrict, 4)
	snk := sinkOf(t, b, "scheduled", mq.MetadataNativeStrict)
	if err := snk.Publish(context.Background(), &mq.Message{Body: []byte("later")},
		&mq.PublishOptions{Delay: 5 * time.Second}); err != nil {
		t.Fatalf("a delayed publication: %v", err)
	}
	if st := server.Stats(topic, "workers"); st.Deferred != 1 {
		t.Fatalf("the broker holds %d deferred message(s), want 1: the delay must be Pulsar's and not "+
			"a timer in this process", st.Deferred)
	}
	if b.HeldDelayedNacks() != 0 {
		t.Fatal("the driver held the delayed publication itself")
	}
	fetchNothing(t, src, "a scheduled publication was delivered early")
	server.Advance(6 * time.Second)
	d := fetchOne(t, src)
	if string(d.Message.Body) != "later" {
		t.Fatalf("the scheduled delivery carries %q", d.Message.Body)
	}
	if err := src.Settler().Ack(context.Background(), &d); err != nil {
		t.Fatalf("Ack: %v", err)
	}
	// A DeliverAt already past is "deliver now", not an error.
	if err := snk.Publish(context.Background(), &mq.Message{Body: []byte("now")},
		&mq.PublishOptions{DeliverAt: server.Now().Add(-time.Hour)}); err != nil {
		t.Fatalf("a DeliverAt in the past: %v", err)
	}
	now := fetchOne(t, src)
	if err := src.Settler().Ack(context.Background(), &now); err != nil {
		t.Fatalf("Ack: %v", err)
	}
	// And a delay past the driver's own bound is refused before any I/O.
	if err := snk.Publish(context.Background(), &mq.Message{Body: []byte("far")},
		&mq.PublishOptions{Delay: 2 * pd.MaxDelay}); mq.CodeOf(err) != mq.CodeInvalidArgument {
		t.Fatalf("a delay past pulsar.MaxDelay reported %v (code %v)", err, mq.CodeOf(err))
	}
}

// TestUnpublishableOptionsAreRefused is the rule about a feature a driver
// does not advertise: it is refused, never quietly done differently.
func TestUnpublishableOptionsAreRefused(t *testing.T) {
	b, _ := newRig(t, nil)
	snk := sinkOf(t, b, "options", mq.MetadataNativeStrict)
	for name, o := range map[string]*mq.PublishOptions{
		"a time to live":   {TTL: time.Minute},
		"a priority":       {Priority: 5, PrioritySet: true},
		"a dedup key":      {DedupKey: "k"},
		"a partition":      {Partition: 2, PartitionSet: true},
		"a negative delay": {Delay: -time.Second},
	} {
		t.Run(name, func(t *testing.T) {
			err := snk.Publish(context.Background(), &mq.Message{Body: []byte("x")}, o)
			if err == nil {
				t.Fatalf("%s was accepted", name)
			}
			if mq.CodeOf(err) != mq.CodeUnsupported && mq.CodeOf(err) != mq.CodeInvalidArgument {
				t.Fatalf("%s reported %v (code %v)", name, err, mq.CodeOf(err))
			}
		})
	}
}

// TestOversizedPublicationIsRefusedLocally is the refusal happens before
// the round trip, and it is measured on what will actually be written.
func TestOversizedPublicationIsRefusedLocally(t *testing.T) {
	b, server := newRig(t, func(c *pd.Config) { c.MaxMessageBytes = 1024 })
	snk := sinkOf(t, b, "big", mq.MetadataNativeStrict)
	err := snk.Publish(context.Background(), &mq.Message{Body: make([]byte, 1025)}, nil)
	if !errors.Is(err, mq.ErrMessageTooLarge) {
		t.Fatalf("an oversized publication reported %v (code %v), want ErrMessageTooLarge", err, mq.CodeOf(err))
	}
	if n := server.Messages("persistent://public/default/big"); n != 0 {
		t.Fatalf("the broker holds %d message(s) after a refused publication", n)
	}
	if err := snk.Publish(context.Background(), &mq.Message{Body: make([]byte, 1024)}, nil); err != nil {
		t.Fatalf("a publication exactly at the limit: %v", err)
	}
}

// TestCloseUnblocksAFetch is reaching a driver: Close unblocks a parked
// Fetch, and pulsar-client-go's own Close does not close the message channel
// the caller supplied — so the channel alone cannot wake it.
func TestCloseUnblocksAFetch(t *testing.T) {
	b, _ := newRig(t, nil)
	src := sourceOf(t, b, "blocked", "workers", mq.MetadataNativeStrict, 4)
	parked := make(chan error, 1)
	go func() {
		dst := make([]mq.DriverDelivery, 1)
		_, err := src.Fetch(context.Background(), dst)
		parked <- err
	}()
	time.Sleep(20 * time.Millisecond)
	if err := src.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	select {
	case err := <-parked:
		if !errors.Is(err, mq.ErrClosed) {
			t.Fatalf("the parked Fetch reported %v, want ErrClosed", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Close did not unblock a parked Fetch; pulsar-client-go's Close does not close the " +
			"message channel the caller supplied, so the channel alone cannot wake it")
	}
}

// TestCloseGivesBackWhatWasHandedOut is the specification's "requeues or abandons everything
// it has handed out", which on Pulsar is the broker's: closing a consumer makes
// every unacknowledged message of its subscription available again at once.
func TestCloseGivesBackWhatWasHandedOut(t *testing.T) {
	const topic = "persistent://public/default/closed"
	b, _ := newRig(t, nil)
	first := sourceOf(t, b, "closed", "workers", mq.MetadataNativeStrict, 8)
	snk := sinkOf(t, b, "closed", mq.MetadataNativeStrict)
	const messages = 4
	for i := range messages {
		publish(t, snk, &mq.Message{Body: []byte{byte('a' + i)}})
	}
	held := fetchN(t, first, messages)
	if err := first.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	// A settlement afterwards is refused rather than applied to whatever the
	// message becomes next.
	for i := range held {
		if err := first.Settler().Ack(context.Background(), &held[i]); !errors.Is(err, mq.ErrLeaseLost) {
			t.Fatalf("settling delivery %d after Close reported %v, want ErrLeaseLost", i, err)
		}
	}
	second := sourceOf(t, b, "closed", "workers", mq.MetadataNativeStrict, 8)
	again := fetchN(t, second, messages)
	seen := map[string]bool{}
	for i := range again {
		seen[string(again[i].Message.Body)] = true
		if err := second.Settler().Ack(context.Background(), &again[i]); err != nil {
			t.Fatalf("Ack: %v", err)
		}
	}
	if len(seen) != messages {
		t.Fatalf("a new consumer received %d of %d messages the closed one was holding", len(seen), messages)
	}
	_ = topic
}

// TestUndecodableDeliveryIsGivenBackAndCounted is there is no settlement
// that means "this is not a message I can read", so it goes back negatively and
// the failure surfaces as a count and a health transition.
func TestUndecodableDeliveryIsGivenBackAndCounted(t *testing.T) {
	const topic = "persistent://public/default/undecodable"
	b, server := newRig(t, nil)
	// A consumer in portable-envelope mode, and a publisher that is not:
	// the body is then not an envelope and the consumer cannot read it.
	src, err := b.OpenSource(context.Background(), mq.SourceConfig{
		Topic: "undecodable", Subscription: "workers", Prefetch: 4,
		Metadata: mq.MetadataPortableEnvelope, Start: mq.StartEarliest(),
	})
	if err != nil {
		t.Fatalf("OpenSource: %v", err)
	}
	t.Cleanup(func() { _ = src.Close() })
	snk := sinkOf(t, b, "undecodable", mq.MetadataNativeStrict)
	publish(t, snk, &mq.Message{Body: []byte("not an envelope")})
	// The count happens in Fetch, which is where the decode happens: the
	// driver does not read a delivery it was not asked for.
	fetchNothing(t, src, "an undecodable delivery was handed to a handler")
	waitFor(t, "the driver to count an undecodable delivery", func() bool { return b.Undecodable() > 0 })
	if st := server.Stats(topic, "workers"); st.Nacked == 0 {
		t.Fatal("an undecodable delivery was not given back; acknowledging it would discard somebody's message")
	}
	if h := b.Health(); h.State != mq.HealthDegraded {
		t.Fatalf("the backend reports health %v after an undecodable delivery; the specification makes a driver's "+
			"background failure surface through health and a metric", h.State)
	}
}

// TestFetchDrainsAWholeBatch is FeatureBatchReceive: one blocking wait and then
// a non-blocking drain, which is what makes a batch receive a batch.
func TestFetchDrainsAWholeBatch(t *testing.T) {
	b, _ := newRig(t, nil)
	src := sourceOf(t, b, "drain", "workers", mq.MetadataNativeStrict, 32)
	snk := sinkOf(t, b, "drain", mq.MetadataNativeStrict)
	const messages = 8
	for i := range messages {
		publish(t, snk, &mq.Message{Body: []byte{byte('a' + i)}})
	}
	dst := make([]mq.DriverDelivery, messages)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	n, err := src.Fetch(ctx, dst)
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if n < 2 {
		t.Fatalf("Fetch filled %d of %d slots; a driver advertising batch receive drains what is there", n, messages)
	}
	for i := range n {
		if err := src.Settler().Ack(context.Background(), &dst[i]); err != nil {
			t.Fatalf("Ack: %v", err)
		}
	}
}

// TestStartPositions holds the four positions: the two Pulsar has, the one
// it reaches by seeking, and the one no encoding can express.
func TestStartPositions(t *testing.T) {
	b, _ := newRig(t, nil)
	snk := sinkOf(t, b, "positions", mq.MetadataNativeStrict)
	publish(t, snk, &mq.Message{Body: []byte("before")})

	latest, err := b.OpenSource(context.Background(), mq.SourceConfig{
		Topic: "positions", Subscription: "latest", Prefetch: 4, Start: mq.StartLatest(),
	})
	if err != nil {
		t.Fatalf("a latest source: %v", err)
	}
	t.Cleanup(func() { _ = latest.Close() })
	fetchNothing(t, latest, "a StartLatest subscription replayed a message published before it existed")
	publish(t, snk, &mq.Message{Body: []byte("after")})
	d := fetchOne(t, latest)
	if string(d.Message.Body) != "after" {
		t.Fatalf("a StartLatest subscription received %q", d.Message.Body)
	}
	if err := latest.Settler().Ack(context.Background(), &d); err != nil {
		t.Fatalf("Ack: %v", err)
	}

	earliest := sourceOf(t, b, "positions", "earliest", mq.MetadataNativeStrict, 4)
	got := fetchN(t, earliest, 2)
	if string(got[0].Message.Body) != "before" {
		t.Fatalf("a StartEarliest subscription began at %q", got[0].Message.Body)
	}
	for i := range got {
		if err := earliest.Settler().Ack(context.Background(), &got[i]); err != nil {
			t.Fatalf("Ack: %v", err)
		}
	}

	// A sequence is a number and a Pulsar position is a triple.
	if _, err := b.OpenSource(context.Background(), mq.SourceConfig{
		Topic: "positions", Subscription: "seq", Prefetch: 4, Start: mq.StartAtSequence(1),
	}); !errors.Is(err, mq.ErrUnsupported) {
		t.Fatalf("StartAtSequence reported %v, want ErrUnsupported", err)
	}
	// And a time is a seek, which a multi-topic consumer cannot do.
	retry, _ := newRig(t, func(c *pd.Config) {
		c.RetryEnable, c.DeadLetterTopic = true, "positions-dlq"
	})
	if _, err := retry.OpenSource(context.Background(), mq.SourceConfig{
		Topic: "positions", Subscription: "t", Prefetch: 4, Start: mq.StartAt(time.Now()),
	}); !errors.Is(err, mq.ErrUnsupported) {
		t.Fatalf("StartAt on a retry-topic consumer reported %v, want ErrUnsupported: pulsar-client-go "+
			"refuses a seek on a multi-topic consumer and the retry topic makes one", err)
	}
}

// TestSeekByTime is the positive direction of FeatureSeekByTime.
func TestSeekByTime(t *testing.T) {
	b, server := newRig(t, nil)
	snk := sinkOf(t, b, "sought", mq.MetadataNativeStrict)
	publish(t, snk, &mq.Message{Body: []byte("old")})
	server.Advance(time.Hour)
	at := server.Now()
	publish(t, snk, &mq.Message{Body: []byte("new")})
	src, err := b.OpenSource(context.Background(), mq.SourceConfig{
		Topic: "sought", Subscription: "sought", Prefetch: 4, Start: mq.StartAt(at),
	})
	if err != nil {
		t.Fatalf("a source starting at a time: %v", err)
	}
	t.Cleanup(func() { _ = src.Close() })
	d := fetchOne(t, src)
	if string(d.Message.Body) != "new" {
		t.Fatalf("a subscription starting at a time received %q, want the message published at or after it", d.Message.Body)
	}
	if err := src.Settler().Ack(context.Background(), &d); err != nil {
		t.Fatalf("Ack: %v", err)
	}
}

// TestFanOutIsolation is two subscriptions on one topic each receive
// every message, and neither sees the other's settlements.
func TestFanOutIsolation(t *testing.T) {
	b, _ := newRig(t, nil)
	a := sourceOf(t, b, "fan", "a", mq.MetadataNativeStrict, 4)
	c := sourceOf(t, b, "fan", "b", mq.MetadataNativeStrict, 4)
	snk := sinkOf(t, b, "fan", mq.MetadataNativeStrict)
	publish(t, snk, &mq.Message{Body: []byte("both")})
	for i, src := range []mq.Source{a, c} {
		d := fetchOne(t, src)
		if !bytes.Equal(d.Message.Body, []byte("both")) {
			t.Fatalf("subscription %d received %q", i, d.Message.Body)
		}
		if err := src.Settler().Ack(context.Background(), &d); err != nil {
			t.Fatalf("Ack: %v", err)
		}
	}
}

// TestBackendSurfaces is the compile-time promise made observable: the core
// asserts these once, at open, and a surface that stopped being offered would
// silently remove a feature for ever.
func TestBackendSurfaces(t *testing.T) {
	b, _ := newRig(t, nil)
	if b.Info().Name != "pulsar" {
		t.Fatalf("Info reports driver %q", b.Info().Name)
	}
	if b.Info().Profile != "shared" {
		t.Fatalf("Info reports profile %q; the subscription type IS the profile, because it decides "+
			"every ordering claim", b.Info().Profile)
	}
	if b.Native() == nil {
		t.Fatal("Native is nil, and the escape hatch is the client")
	}
	if _, err := b.Admin(); !errors.Is(err, mq.ErrUnsupported) {
		t.Fatalf("Admin with no AdminURL reported %v, want ErrUnsupported: Pulsar's topology API is "+
			"HTTP on a different port and this driver does not guess it", err)
	}
	snk := sinkOf(t, b, "surfaces", mq.MetadataNativeStrict)
	for name, ok := range map[string]bool{
		"BatchSink": func() bool { _, ok := snk.(mq.BatchSink); return ok }(),
		"AsyncSink": func() bool { _, ok := snk.(mq.AsyncSink); return ok }(),
		"OwnedSink": func() bool { _, ok := snk.(mq.OwnedSink); return ok }(),
		"FlushSink": func() bool { _, ok := snk.(mq.FlushSink); return ok }(),
	} {
		if !ok {
			t.Errorf("the sink is not a %s", name)
		}
	}
	if err := snk.(mq.FlushSink).Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if err := b.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	// Info, Capabilities and Health answer after Close, from any goroutine.
	if b.Info().Name != "pulsar" {
		t.Fatal("a closed backend stopped naming its driver")
	}
	if h := b.Health(); h.State != mq.HealthClosed {
		t.Fatalf("a closed backend reports health %v", h.State)
	}
	if _, err := b.OpenSink(context.Background(), mq.SinkConfig{Topic: "x"}); !errors.Is(err, mq.ErrClosed) {
		t.Fatalf("opening a sink on a closed backend reported %v, want ErrClosed", err)
	}
}
