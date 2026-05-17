package iqmq_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	mq "bitbucket.org/iqhive/iqmq"
	"bitbucket.org/iqhive/iqmq/memory"
)

// openForExample is the two lines every example below would otherwise repeat:
// an in-process broker with its topics created on demand.
//
// It panics rather than returning an error because an example's job is to show
// the workflow and not the error handling of its own scaffolding; a real
// program checks both errors.
func openForExample() (context.Context, *mq.Client) {
	ctx := context.Background()
	client, err := memory.Open(ctx, memory.Config{AutoCreate: true})
	if err != nil {
		panic(err)
	}
	return ctx, client
}

// ExamplePublisherHandle_Publish is the synchronous publication: a bound
// publisher, one message, and the error the call returns once the driver's
// confirmation boundary has been reached.
//
// The boundary is the driver, not the library: the specification makes "published"
// mean whatever the driver's ConfirmationMode says it means, so a nil error
// here is a broker acknowledgement on a driver that confirms and a successful
// hand-off on one that does not. The memory driver confirms.
func ExamplePublisherHandle_Publish() {
	ctx, client := openForExample()
	defer func() { _ = client.Shutdown(ctx) }()

	pub, err := client.Publisher(ctx, mq.PublisherConfig{Topic: "orders"})
	if err != nil {
		panic(err)
	}
	if err := pub.Publish(ctx, &mq.Message{Key: []byte("cust-42"), Body: []byte(`{"id":1}`)}); err != nil {
		panic(err)
	}
	fmt.Println("published to", pub.Topic())
	// Output: published to orders
}

// ExamplePublisherHandle_PublishAsync is the asynchronous publication: the
// call returns before the publication is settled and the result arrives on the
// callback.
//
// Two things about the callback are contract rather than detail. It runs once
// per publication, whatever the outcome, so a program that counts publications
// may count callbacks. And with the default CallbackInline it runs on the
// publisher's own completion goroutine, which is why the work here is a
// channel send and not the order processing: a slow callback is publisher
// backpressure (CallbackMode).
func ExamplePublisherHandle_PublishAsync() {
	ctx, client := openForExample()
	defer func() { _ = client.Shutdown(ctx) }()

	pub, err := client.Publisher(ctx, mq.PublisherConfig{Topic: "orders"})
	if err != nil {
		panic(err)
	}
	done := make(chan error, 1)
	if err := pub.PublishAsync(ctx, &mq.Message{Body: []byte("async")}, func(r mq.PublishResult) {
		done <- r.Err
	}); err != nil {
		panic(err)
	}
	if err := pub.Flush(ctx); err != nil {
		panic(err)
	}
	fmt.Println("callback err:", <-done)
	// Output: callback err: <nil>
}

// ExamplePublisherHandle_PublishBatch is the batch publication. The whole
// slice is one call; what a driver does with it is its own capability —
// FeatureAtomicBatchPublish says whether the batch is all-or-nothing, and a
// driver that lacks it publishes the messages one at a time behind this call
// rather than refusing it.
//
// A batch that partly fails returns a *BatchError whose Errs is index-aligned
// with the slice, so the caller learns WHICH message failed rather than only
// that one did.
func ExamplePublisherHandle_PublishBatch() {
	ctx, client := openForExample()
	defer func() { _ = client.Shutdown(ctx) }()

	pub, err := client.Publisher(ctx, mq.PublisherConfig{Topic: "orders"})
	if err != nil {
		panic(err)
	}
	ms := []*mq.Message{
		{Key: []byte("a"), Body: []byte("1")},
		{Key: []byte("b"), Body: []byte("2")},
		{Key: []byte("c"), Body: []byte("3")},
	}
	err = pub.PublishBatch(ctx, ms)
	var be *mq.BatchError
	switch {
	case errors.As(err, &be):
		fmt.Println("partial batch:", len(be.Errs))
	case err != nil:
		panic(err)
	default:
		fmt.Println("batch of", len(ms), "published")
	}
	// Output: batch of 3 published
}

// ExamplePublisherHandle_PublishOwned is the specification's owned publication, which is the other direction: the caller hands its buffer to iqmq instead of
// having it copied, and iqmq hands it back through Release when it is done
// with it.
//
// Release runs exactly once per message and never under an internal lock, so a
// pool Put is safe in it. The rule the caller must keep is the one this
// example demonstrates by waiting: the buffer may not be touched between the
// call and Release, because the driver may still be reading it.
func ExamplePublisherHandle_PublishOwned() {
	ctx, client := openForExample()
	defer func() { _ = client.Shutdown(ctx) }()

	pub, err := client.Publisher(ctx, mq.PublisherConfig{Topic: "orders"})
	if err != nil {
		panic(err)
	}
	var wg sync.WaitGroup
	wg.Add(1)
	om := mq.OwnedMessage{
		Message: &mq.Message{Body: []byte("owned")},
		Release: func(*mq.Message) { wg.Done() }, // a real program returns it to a pool here
	}
	if err := pub.PublishOwned(ctx, om); err != nil {
		panic(err)
	}
	wg.Wait()
	fmt.Println("storage returned")
	// Output: storage returned
}

// ExampleConsumerHandle_Receive is the manual receive: the caller owns the
// loop, the slice and the settlement.
//
// Receive fills a slice the CALLER allocated (ADR-006) and returns how
// many entries it filled, so a receive loop allocates nothing per message. The
// deliveries are valid until they are settled; under BufferReuse they are
// valid only until the next Receive, and CloneMessage is the escape.
func ExampleConsumerHandle_Receive() {
	ctx, client := openForExample()
	defer func() { _ = client.Shutdown(ctx) }()

	consumer, err := client.Consumer(ctx, mq.ConsumerConfig{Topic: "orders", Subscription: "billing"})
	if err != nil {
		panic(err)
	}
	if err := client.Publish(ctx, "orders", &mq.Message{Body: []byte("manual")}); err != nil {
		panic(err)
	}

	dst := make([]mq.Delivery, 8)
	n, err := consumer.Receive(ctx, dst)
	if err != nil {
		panic(err)
	}
	for i := range dst[:n] {
		fmt.Printf("got %s\n", dst[i].Body)
		if err := dst[i].Ack(); err != nil {
			panic(err)
		}
	}
	// Output: got manual
}

// ExampleHeaders_All is the iterator workflow: Headers.All is a
// range-over-func sequence, so a header set is walked with `for k, v := range`
// and no intermediate map.
//
// The specification keeps duplicates and insertion order, which is why this is a Seq2 over
// pairs and not a map: a map would silently lose the second value of a
// repeated key, and on Kafka a repeated header is normal.
func ExampleHeaders_All() {
	var h mq.Headers
	h.Add("x-trace", []byte("abc"))
	h.Add("x-tag", []byte("eu"))
	h.Add("x-tag", []byte("beta"))

	for k, v := range h.All() {
		fmt.Printf("%s=%s\n", k, v)
	}
	// Output:
	// x-trace=abc
	// x-tag=eu
	// x-tag=beta
}

// ExampleHeaders is Headers workflow: the ordered, duplicate-preserving
// header set (ADR-006).
//
// Get answers with the FIRST value and Values with every one of them, which is
// the distinction a map cannot make. Set replaces every value of a key; Add
// appends one and keeps the order.
func ExampleHeaders() {
	var h mq.Headers
	h.Add("x-tag", []byte("eu"))
	h.Add("x-tag", []byte("beta"))
	h.Set("content-type", []byte("application/json"))

	first, _ := h.Get("x-tag")
	fmt.Printf("first=%s all=%d len=%d\n", first, len(h.Values("x-tag")), h.Len())

	h.Delete("x-tag")
	_, ok := h.Get("x-tag")
	fmt.Println("after delete:", ok)
	// Output:
	// first=eu all=2 len=3
	// after delete: false
}

// ExampleDelivery_Nack is the settle workflow: the four terminal answers a
// delivery can be given, and what the specification guarantees about giving one twice.
//
// Exactly one settlement wins. A second returns ErrAlreadySettled rather than
// panicking or silently succeeding, because a handler that settles twice is a
// bug the caller has to be able to see — and because the alternative makes a
// double ack indistinguishable from a redelivery.
func ExampleDelivery_Nack() {
	ctx, client := openForExample()
	defer func() { _ = client.Shutdown(ctx) }()

	consumer, err := client.Consumer(ctx, mq.ConsumerConfig{Topic: "orders", Subscription: "billing"})
	if err != nil {
		panic(err)
	}
	if err := client.Publish(ctx, "orders", &mq.Message{Body: []byte("retry me")}); err != nil {
		panic(err)
	}
	dst := make([]mq.Delivery, 1)
	if _, err := consumer.Receive(ctx, dst); err != nil {
		panic(err)
	}

	// Nack asks for a redelivery; Reject refuses the message for good.
	if err := dst[0].Nack(); err != nil {
		panic(err)
	}
	fmt.Println("settled:", dst[0].Settled())
	fmt.Println("second settlement:", errors.Is(dst[0].Ack(), mq.ErrAlreadySettled))
	// Output:
	// settled: true
	// second settlement: true
}

// exampleBatchHandler is the adapter the specification does not ship.
//
// Handler has HandlerFunc; BatchHandler has no func form, so every batch
// handler is a named type with a method that its author declares — including
// this example's. Adding mq.BatchHandlerFunc would be one additive type, but
// The specification does not name it and the surface is frozen, so records the cost
// here rather than inventing the name.
type exampleBatchHandler func(ctx context.Context, ds []*mq.Delivery) error

// HandleBatch calls f.
func (f exampleBatchHandler) HandleBatch(ctx context.Context, ds []*mq.Delivery) error {
	return f(ctx, ds)
}

// ExampleConsumerHandle_RunBatch is the batch-handler workflow: the runner
// hands the handler the group of deliveries one fetch produced for one lane,
// and the handler's return value settles all of them.
//
// The three answers are nil (ack every delivery), a *BatchError (settle each
// delivery by its own index-aligned entry), and any other error (take the specification's
// failure path for every delivery in the batch). The middle one is why
// BatchError.Errs is index-aligned: a batch in which one message is poison
// must not cost the other nine a redelivery.
//
// There is no batch-size option. A batch never crosses a lane, so its size is
// what the fetch and the routing allow: under the default OrderingByKey a
// batch holds one key hash, and a runner whose every message has a distinct
// key is handed batches of one. OrderingUnordered below is what makes a whole
// fetch one batch.
func ExampleConsumerHandle_RunBatch() {
	ctx, client := openForExample()
	defer func() { _ = client.Shutdown(ctx) }()

	consumer, err := client.Consumer(ctx, mq.ConsumerConfig{Topic: "orders", Subscription: "billing"})
	if err != nil {
		panic(err)
	}
	for _, body := range []string{"1", "2", "3"} {
		if err := client.Publish(ctx, "orders", &mq.Message{Body: []byte(body)}); err != nil {
			panic(err)
		}
	}

	runCtx, stop := context.WithCancel(ctx)
	defer stop()
	seen := make(chan int, 8)
	go func() {
		_ = consumer.RunBatch(runCtx, exampleBatchHandler(func(_ context.Context, ds []*mq.Delivery) error {
			seen <- len(ds)
			return nil // ack every delivery in the batch
		}), mq.WithOrdering(mq.OrderingUnordered))
	}()

	total, batches := 0, 0
	for total < 3 {
		total += <-seen
		batches++
	}
	fmt.Println("handled", total, "in at most 3 batches:", batches <= 3)
	// Output: handled 3 in at most 3 batches: true
}

// ExampleWithOrdering is the ordering workflow, and the reason iqmq gives
// the scope and the retry interaction two separate options.
//
// WithOrdering names the ROUTING scope: which deliveries share a lane.
// WithOrderingByKey names what happens to a lane when a delivery in it FAILS,
// and OrderingStrict — its zero value — holds the lane until that delivery is
// settled or terminally routed. So a caller who asks for strict order is
// asking for a stall on failure, which is why it is opt-in and why raising
// WithConcurrency does not buy its way out.
func ExampleWithOrdering() {
	ctx, client := openForExample()
	defer func() { _ = client.Shutdown(ctx) }()

	consumer, err := client.Consumer(ctx, mq.ConsumerConfig{Topic: "orders", Subscription: "billing"})
	if err != nil {
		panic(err)
	}
	for _, body := range []string{"1", "2", "3"} {
		if err := client.Publish(ctx, "orders", &mq.Message{Key: []byte("cust-42"), Body: []byte(body)}); err != nil {
			panic(err)
		}
	}

	runCtx, stop := context.WithCancel(ctx)
	defer stop()
	order := make(chan string, 3)
	go func() {
		_ = consumer.Run(runCtx, mq.HandlerFunc(func(_ context.Context, d *mq.Delivery) error {
			order <- string(d.Body)
			return nil
		}),
			mq.WithConcurrency(8),                   // eight workers ...
			mq.WithOrdering(mq.OrderingByKey),       // ... but one lane per key
			mq.WithOrderingByKey(mq.OrderingStrict)) // ... and a failure holds that lane
	}()

	for range 3 {
		fmt.Print(<-order)
	}
	fmt.Println()
	// Output: 123
}

// ExampleClient_Shutdown is the graceful-shutdown workflow, and the
// difference between the two verbs the specification gives a caller.
//
// Shutdown is bounded and graceful: it stops accepting, lets in-flight
// deliveries finish inside the context's deadline, flushes what the publisher
// is holding and then closes. Close is prompt: it releases everything at once
// and does not wait. Both are idempotent, both are safe to call concurrently,
// and calling Close after Shutdown is the normal shape — which is why the
// deferred Close below is not a mistake.
func ExampleClient_Shutdown() {
	ctx, client := openForExample()
	defer func() { _ = client.Close() }()

	pub, err := client.Publisher(ctx, mq.PublisherConfig{Topic: "orders"})
	if err != nil {
		panic(err)
	}
	if err := pub.Publish(ctx, &mq.Message{Body: []byte("last")}); err != nil {
		panic(err)
	}

	stopCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := client.Shutdown(stopCtx); err != nil {
		panic(err)
	}
	fmt.Println("shutdown:", client.Shutdown(stopCtx) == nil, "then close:", client.Close() == nil)
	// Output: shutdown: true then close: true
}

// ExampleClient_Native is the native escape hatch: the
// driver's own client, reachable when the portable surface does not carry
// what a program needs.
//
// It is deliberately a type assertion the caller performs, because the type it
// returns is the driver's and the root package must not import a driver to
// name it. Two rules come with it: a program that uses it is bound to that
// driver, and anything it does to the native client behind iqmq's back is
// outside every guarantee — which is why the example only READS.
func ExampleClient_Native() {
	ctx, client := openForExample()
	defer func() { _ = client.Shutdown(ctx) }()

	broker, ok := client.Native().(*memory.Broker)
	fmt.Println("reached the memory broker:", ok && broker != nil)

	// And the same hatch on a delivery, with the assertion built in.
	var d mq.Delivery
	_, ok = mq.NativeAs[*memory.Broker](&d)
	fmt.Println("a zero delivery has no native record:", !ok)
	// Output:
	// reached the memory broker: true
	// a zero delivery has no native record: true
}

// ExampleClient_Info is the driver-identity half that the native
// escape hatch is the last resort of: what a program can learn about its
// driver WITHOUT binding itself to one.
//
// Info names the driver and its versions; Capabilities answers what the driver
// can do, at what level, so a program adapts rather than asserts.
//
// The third field printed is the rule in one value: the memory driver
// advertises no maximum message size, so Limit.Known is FALSE and Limit.Value
// is a zero that means nothing. A program that read the value without the flag
// would conclude that no message may be larger than zero bytes, which is why
// "unknown is never an overloaded zero" is a rule and not a convention.
func ExampleClient_Info() {
	ctx, client := openForExample()
	defer func() { _ = client.Shutdown(ctx) }()

	info := client.Info()
	caps := client.Capabilities()
	limit := caps.Limit(mq.LimitMaxMessageBytes)
	fmt.Printf("driver=%s ordered=%v max-bytes-known=%v\n",
		info.Name, caps.Has(mq.FeatureOrderedByKey), limit.Known)
	// Output: driver=memory ordered=true max-bytes-known=false
}
