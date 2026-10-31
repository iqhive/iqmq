package sqs_test

import (
	"context"
	"runtime"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit"
	sq "github.com/iqhive/iqmq/drivers/sqs"
	"github.com/iqhive/iqmq/drivers/sqs/sqsfake"
)

// observedAPI is the fake with what a closing source does to it made visible:
// every receipt handle a ReceiveMessage minted, every release (a visibility
// change to zero, single or batched) per handle, and -- once hang is set -- a
// service that has stopped answering visibility changes and single sends,
// where every such call blocks until its context ends.
type observedAPI struct {
	*sqsfake.Server

	hang atomic.Bool
	// waits counts the visibility calls that found the service hanging with
	// time left on their context, so each one cost the caller a wait.
	waits atomic.Int64

	mu       sync.Mutex
	received map[string]bool
	released map[string]int
}

func newObservedAPI() *observedAPI {
	return &observedAPI{
		Server:   sqsfake.New(sqsfake.Options{StaleHandleDeletes: true}),
		received: map[string]bool{},
		released: map[string]int{},
	}
}

func (o *observedAPI) ReceiveMessage(ctx context.Context, in *sqs.ReceiveMessageInput, opts ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
	out, err := o.Server.ReceiveMessage(ctx, in, opts...)
	if err == nil {
		o.mu.Lock()
		for i := range out.Messages {
			o.received[aws.ToString(out.Messages[i].ReceiptHandle)] = true
		}
		o.mu.Unlock()
	}
	return out, err
}

func (o *observedAPI) ChangeMessageVisibility(ctx context.Context, in *sqs.ChangeMessageVisibilityInput, opts ...func(*sqs.Options)) (*sqs.ChangeMessageVisibilityOutput, error) {
	if in.VisibilityTimeout == 0 {
		o.count(aws.ToString(in.ReceiptHandle))
	}
	if o.hanging(ctx) {
		return nil, ctx.Err()
	}
	return o.Server.ChangeMessageVisibility(ctx, in, opts...)
}

func (o *observedAPI) ChangeMessageVisibilityBatch(ctx context.Context, in *sqs.ChangeMessageVisibilityBatchInput, opts ...func(*sqs.Options)) (*sqs.ChangeMessageVisibilityBatchOutput, error) {
	for i := range in.Entries {
		if in.Entries[i].VisibilityTimeout == 0 {
			o.count(aws.ToString(in.Entries[i].ReceiptHandle))
		}
	}
	if o.hanging(ctx) {
		return nil, ctx.Err()
	}
	return o.Server.ChangeMessageVisibilityBatch(ctx, in, opts...)
}

func (o *observedAPI) SendMessage(ctx context.Context, in *sqs.SendMessageInput, opts ...func(*sqs.Options)) (*sqs.SendMessageOutput, error) {
	if o.hanging(ctx) {
		return nil, ctx.Err()
	}
	return o.Server.SendMessage(ctx, in, opts...)
}

func (o *observedAPI) count(handle string) {
	o.mu.Lock()
	o.released[handle]++
	o.mu.Unlock()
}

// hanging blocks until ctx ends when the service has been told to hang, and
// reports whether it did.
func (o *observedAPI) hanging(ctx context.Context) bool {
	if !o.hang.Load() {
		return false
	}
	if ctx.Err() == nil {
		o.waits.Add(1)
	}
	<-ctx.Done()
	return true
}

// openObserved is openOn over the observed fake.
func openObserved(t *testing.T, api *observedAPI, opts ...func(*sq.Config)) *harness {
	t.Helper()
	cfg := sq.Config{
		Region:          "fake-region-1",
		AccessKeyID:     "AKIAFAKE",
		SecretAccessKey: "not-a-real-secret",
		AutoCreate:      true,
		WaitTime:        time.Second,
		AckDeadline:     time.Hour,
		Clock:           clock{s: api.Server},
	}
	for _, fn := range opts {
		fn(&cfg)
	}
	back, err := sq.BackendWithAPI(t.Context(), api, api.Server, driverkit.ClientBorrowed, cfg)
	if err != nil {
		t.Fatalf("BackendWithAPI: %v", err)
	}
	h := &harness{t: t, server: api.Server, back: back, cfg: cfg}
	h.client = mq.NewClient(back, mq.WithClock(cfg.Clock))
	t.Cleanup(func() {
		api.hang.Store(false)
		if err := h.client.Close(); err != nil {
			t.Errorf("closing the client: %v", err)
		}
	})
	return h
}

// parkMidAnswer opens a consumer whose one poller is parked part-way through
// its second ReceiveMessage answer, and returns it.
//
// One poller, ten messages an answer and a prefetch of fifteen make the ring
// fifteen deep. Twenty-five messages are waiting before the consumer opens, so
// the first answer is ten, all admitted, and the second is ten more of which
// five fit: the poller parks pushing the sixth, holding it and four more. Five
// is the point -- an answer's messages BEFORE the one the push failed on are
// in the ring, and Close's drain owns them.
func parkMidAnswer(t *testing.T, h *harness, name string) *mq.ConsumerHandle {
	t.Helper()
	for i := range 25 {
		h.publish(name, &mq.Message{Body: []byte(strconv.Itoa(i))})
	}
	return awaitParked(t, h, name, 15, 20)
}

// awaitParked opens a consumer with the given prefetch and waits until its
// one poller is parked in the ring's Push with invisible messages received.
func awaitParked(t *testing.T, h *harness, name string, prefetch, invisible int) *mq.ConsumerHandle {
	t.Helper()
	cons := h.consumer(name, prefetch)
	srcs := sq.SourcesForTest(h.back)
	if len(srcs) != 1 {
		t.Fatalf("the backend has %d sources open, want the consumer's one", len(srcs))
	}
	bound := time.Now().Add(10 * time.Second)
	for {
		parked, ok := sq.ParkedPushesForTest(srcs[0])
		if !ok {
			t.Fatalf("SourcesForTest returned a %T, not a source", srcs[0])
		}
		if parked == 1 {
			break
		}
		if time.Now().After(bound) {
			t.Fatalf("the poller never parked in its second answer (%d parked, %d invisible)", parked, h.invisible(name))
		}
		runtime.Gosched()
	}
	if inv := h.invisible(name); inv != invisible {
		t.Fatalf("%d messages invisible with the poller parked, want %d", inv, invisible)
	}
	return cons
}

// assertReleasedExactlyOnce fails unless every receipt handle the fake minted
// was released once, and nothing else was.
func assertReleasedExactlyOnce(t *testing.T, api *observedAPI, received int) {
	t.Helper()
	api.mu.Lock()
	defer api.mu.Unlock()
	if len(api.received) != received {
		t.Fatalf("the fake minted %d receipt handles, want %d", len(api.received), received)
	}
	var twice, never, stranger []string
	for handle := range api.received {
		switch n := api.released[handle]; {
		case n == 0:
			never = append(never, handle)
		case n > 1:
			twice = append(twice, handle+"x"+strconv.Itoa(n))
		}
	}
	for handle := range api.released {
		if !api.received[handle] {
			stranger = append(stranger, handle)
		}
	}
	sort.Strings(twice)
	sort.Strings(never)
	if len(never) > 0 || len(twice) > 0 || len(stranger) > 0 {
		t.Fatalf("Close's releases do not match what the source received: never released %v; released more than "+
			"once %v; released but never received %v", never, twice, stranger)
	}
}

// TestCloseReleasesEveryReceivedMessageExactlyOnce is the accounting the
// fix for TestCloseReleasesUnfetchedDeliveries has to satisfy from BOTH sides.
// Every message a closing source received and did not
// hand out is released -- the lower bound the flaky test was about -- and
// none is released twice: the messages of the interrupted answer that were
// already admitted are in the ring, Close's drain releases those, and a
// poller that released from the start of its answer rather than from the
// message its push failed on would release them again.
func TestCloseReleasesEveryReceivedMessageExactlyOnce(t *testing.T) {
	api := newObservedAPI()
	h := openObserved(t, api, func(c *sq.Config) {
		c.Pollers = 1
		c.MaxMessages = 10
	})
	name := topic(t, "close-exactly-once")
	cons := parkMidAnswer(t, h, name)

	if err := cons.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if inv := h.invisible(name); inv != 0 {
		t.Fatalf("%d messages still invisible after Close returned", inv)
	}
	assertReleasedExactlyOnce(t, api, 20)
}

// TestCloseReleasesAnInterruptedAnswerWithItsUndecodableMessages: the
// undecodable messages of the answer a poller was admitting are released with
// the rest of that answer when Close interrupts it, once each. They are held back to be released with the answer rather than one
// call each, so an interrupted answer is the one place they could be lost.
//
// The second answer is three undecodable messages and seven good ones. The
// undecodable ones take no ring slot, so five good ones fill the ring and the
// poller parks on the sixth -- the ninth message of the answer.
func TestCloseReleasesAnInterruptedAnswerWithItsUndecodableMessages(t *testing.T) {
	api := newObservedAPI()
	h := openObserved(t, api, func(c *sq.Config) {
		c.Pollers = 1
		c.MaxMessages = 10
	})
	name := topic(t, "close-undecodable")
	for i := range 10 {
		h.publish(name, &mq.Message{Body: []byte(strconv.Itoa(i))})
	}
	sendUndecodable(t, h, api, name, 3)
	for i := range 7 {
		h.publish(name, &mq.Message{Body: []byte(strconv.Itoa(10 + i))})
	}
	cons := awaitParked(t, h, name, 15, 20)

	if err := cons.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if inv := h.invisible(name); inv != 0 {
		t.Fatalf("%d messages still invisible after Close returned", inv)
	}
	assertReleasedExactlyOnce(t, api, 20)
}

// TestCloseLeavesAReceiptWhoseWindowClosedAlone: Close makes visible again
// what the ring holds and what a parked poller was admitting, by receipt
// handle, and it used to do so without the lease check every settlement makes.
// Once those windows have closed and another consumer has received the
// messages, the handles are stale, and on the reading of the service this fake
// models (StaleHandleDeletes) the release made the OTHER consumer's copies
// visible, so a third consumer received them while the second still worked on
// them. A message whose window has closed is visible already and needs no
// release. Here the ring's delivery (m0) and the parked poller's message (m1)
// are both stale.
func TestCloseLeavesAReceiptWhoseWindowClosedAlone(t *testing.T) {
	h := open(t, func(c *sq.Config) {
		c.Pollers = 1
		c.MaxMessages = 1
		c.AckDeadline = 30 * time.Second
	})
	name := topic(t, "close-stale")
	h.publish(name, &mq.Message{Body: []byte("m0")})
	h.publish(name, &mq.Message{Body: []byte("m1")})
	// A prefetch of one and one message an answer: m0 fills the ring, and the
	// poller parks pushing m1.
	cons := awaitParked(t, h, name, 1, 2)

	h.server.Advance(31 * time.Second)
	out, err := h.server.ReceiveMessage(t.Context(), &sqs.ReceiveMessageInput{
		QueueUrl: aws.String(h.server.QueueURL(h.queue(name))), MaxNumberOfMessages: 10, VisibilityTimeout: 30,
	})
	if err != nil || len(out.Messages) != 2 {
		t.Fatalf("the other consumer's ReceiveMessage: %d message(s), %v; want both", len(out.Messages), err)
	}
	if err := cons.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if inv := h.invisible(name); inv != 2 {
		t.Fatalf("after Close, %d of the 2 messages the other consumer holds are still invisible: Close released "+
			"receipt handles whose windows had closed, and made the other consumer's copies visible", inv)
	}
}

// TestCloseIsBoundedWhenTheServiceStopsAnswering: releasing is Close's work,
// so a service that has stopped answering visibility changes costs Close's
// releases their bound and no more. The bound is two
// waits of Config.APITimeout -- one for the answer the poller was admitting,
// one for the ring's drain -- whatever the number of messages, because each
// is one batched release under one deadline. It is the RELEASES' bound: a
// coalescing Settle policy's held settlements are flushed after them, each
// run with an APITimeout of its own, and this test uses the default policy,
// which holds none.
//
// It used to be one wait per MESSAGE: a release per handle, each with a fresh
// APITimeout because context.WithoutCancel strips the caller's deadline along
// with its cancellation. Here that is twenty.
func TestCloseIsBoundedWhenTheServiceStopsAnswering(t *testing.T) {
	const apiTimeout = 200 * time.Millisecond
	api := newObservedAPI()
	h := openObserved(t, api, func(c *sq.Config) {
		c.Pollers = 1
		c.MaxMessages = 10
		c.APITimeout = apiTimeout
	})
	name := topic(t, "close-hung-service")
	cons := parkMidAnswer(t, h, name)

	api.hang.Store(true)
	start := time.Now()
	if err := cons.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	took := time.Since(start)
	// The count is the assertion; the duration is its consequence, with an
	// allowance for scheduling on a loaded two-core runner that is still a
	// fraction of the old cost.
	if waits := api.waits.Load(); waits > 2 {
		t.Errorf("Close waited out %d visibility calls against a service that stopped answering, want at most 2 "+
			"(the poller's answer and the ring's drain, one batch each under one deadline)", waits)
	}
	if limit := 2*apiTimeout + 600*time.Millisecond; took > limit {
		t.Errorf("Close took %v against a service that stopped answering, want under %v (two APITimeouts of %v)",
			took, limit, apiTimeout)
	}
}

// sendUndecodable puts n messages on the topic's queue that this driver
// cannot decode: an encoding attribute naming no encoding it knows.
func sendUndecodable(t *testing.T, h *harness, api *observedAPI, name string, n int) {
	t.Helper()
	out, err := api.GetQueueUrl(t.Context(), &sqs.GetQueueUrlInput{QueueName: aws.String(h.queue(name))})
	if err != nil {
		t.Fatalf("GetQueueUrl: %v", err)
	}
	entries := make([]types.SendMessageBatchRequestEntry, 0, n)
	for i := range n {
		entries = append(entries, types.SendMessageBatchRequestEntry{
			Id:          aws.String(strconv.Itoa(i)),
			MessageBody: aws.String("undecodable"),
			MessageAttributes: map[string]types.MessageAttributeValue{
				"iqmq-enc": {DataType: aws.String("String"), StringValue: aws.String("bogus")},
			},
		})
	}
	if _, err := api.SendMessageBatch(t.Context(), &sqs.SendMessageBatchInput{QueueUrl: out.QueueUrl, Entries: entries}); err != nil {
		t.Fatalf("SendMessageBatch: %v", err)
	}
}

// TestCloseIsBoundedWhenAnAnswerCannotBeDecoded: the messages of an answer
// this driver cannot decode are dead-lettered one send at a time, under the
// poller's round, which Close cancels, so a service that has stopped answering
// costs Close one APITimeout for them however many there are.
// They were released one call at a time, each with a fresh APITimeout: ten
// undecodable messages cost Close ten, and a send per message is that shape
// again.
func TestCloseIsBoundedWhenAnAnswerCannotBeDecoded(t *testing.T) {
	const apiTimeout = 200 * time.Millisecond
	api := newObservedAPI()
	h := openObserved(t, api, func(c *sq.Config) {
		c.Pollers = 1
		c.MaxMessages = 10
		c.APITimeout = apiTimeout
		c.DeadLetterQueue = "dead"
	})
	name := topic(t, "undecodable-close")
	h.publish(name, &mq.Message{Body: []byte("good")})
	cons := h.consumer(name, 15)
	d := h.receiveOne(cons, 5*time.Second)
	if err := d.Ack(); err != nil {
		t.Fatalf("Ack: %v", err)
	}
	api.hang.Store(true)
	sendUndecodable(t, h, api, name, 10)
	bound := time.Now().Add(10 * time.Second)
	for api.waits.Load() < 1 {
		if time.Now().After(bound) {
			t.Fatal("the poller never started dead-lettering the undecodable answer")
		}
		runtime.Gosched()
	}
	start := time.Now()
	if err := cons.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	took := time.Since(start)
	if waits := api.waits.Load(); waits > 1 {
		t.Errorf("dead-lettering an answer of ten undecodable messages waited out %d sends against a "+
			"service that stopped answering, want 1: Close cancels the move it interrupts and the rest", waits)
	}
	if limit := apiTimeout + 600*time.Millisecond; took > limit {
		t.Errorf("Close took %v with the poller dead-lettering an undecodable answer, want under %v", took, limit)
	}
}
