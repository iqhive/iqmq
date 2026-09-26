package reqreply_test

import (
	"context"
	"fmt"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/memory"
	"github.com/iqhive/iqmq/reqreply"
)

// Example is the reqreply workflow: the request/reply over the iqmq
// core — a correlation id, a reply topic, and ONE reply consumer per client
// that serves every outstanding request.
//
// The single consumer is the whole design. A reply consumer per request means
// a subscription per request: topology created and destroyed on the message
// path, a broker round trip before the request is even sent, and — on brokers
// that do not garbage-collect subscriptions — a slow leak of server-side state
// that outlives the process.
//
// Config.Subscription must be unique to the client. Two clients sharing one
// subscription on a queue-shaped broker take each other's replies, and a reply
// delivered to the wrong client is a request that times out while its answer
// is acked somewhere else.
func Example() {
	ctx := context.Background()
	client, err := memory.Open(ctx, memory.Config{AutoCreate: true})
	if err != nil {
		panic(err)
	}
	defer func() { _ = client.Shutdown(ctx) }()

	// The emulated path is OPT-IN, and the panic without it is the point:
	// emulation creates a reply topic and a subscription on it, and on a
	// managed broker that is a resource with a cost, a quota and a lifetime.
	// the specification's "Open never creates production topology" is not something a
	// library gets to do quietly because it needed somewhere to put replies.
	rr, err := reqreply.New(ctx, client, reqreply.Config{
		ReplyTopic:   "replies",
		Subscription: "replies-instance-1",
	}, reqreply.WithEmulatedRequestReply())
	if err != nil {
		panic(err)
	}
	defer func() { _ = rr.Close() }()

	// The responder: an ordinary consumer whose handler is wrapped by Serve,
	// which publishes the answer to the reply topic the request carried.
	server, err := client.Consumer(ctx, mq.ConsumerConfig{Topic: "echo", Subscription: "workers"})
	if err != nil {
		panic(err)
	}
	// reqreply.Publisher takes the topic per call, which *mq.Client satisfies
	// and a bound *mq.PublisherHandle deliberately does not: the reply topic
	// is the REQUEST's, carried on the message, so a responder cannot be bound
	// to one destination.
	runCtx, stop := context.WithCancel(ctx)
	defer stop()
	go func() {
		_ = server.Run(runCtx, reqreply.Serve(client, func(_ context.Context, d *mq.Delivery) (*mq.Message, error) {
			return &mq.Message{Body: append([]byte("echo: "), d.Body...)}, nil
		}))
	}()

	reply, err := rr.Request(ctx, "echo", &mq.Message{Body: []byte("hello")})
	if err != nil {
		panic(err)
	}
	fmt.Printf("%s (native path: %v)\n", reply.Body, rr.Native())
	// Output: echo: hello (native path: false)
}
