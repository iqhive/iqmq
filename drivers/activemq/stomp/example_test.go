package stomp_test

import (
	"context"
	"fmt"
	"time"

	"github.com/go-stomp/stomp/v3/frame"

	mq "bitbucket.org/iqhive/iqmq"
	stomp "bitbucket.org/iqhive/iqmq/drivers/activemq/stomp"
	"bitbucket.org/iqhive/iqmq/drivers/activemq/stomp/internal/stompsrv"
	"bitbucket.org/iqhive/iqmq/internal/activemq"
)

// Example publishes one message and consumes it, against the in-process STOMP
// server this module tests with — so it needs no broker and no container, and
// `make ci` runs it.
//
// Two things in it are the driver's own and not boilerplate. `Broker` is
// required and has no useful default: the two products' destination forms name
// nothing in common, and on Classic a destination's own shape is the only
// thing that says whether a name is a queue or a topic. And the delivery is
// settled with `Ack`, never with `Nack(NackDrop())` — a STOMP NACK frame
// returns no message on either broker, so `drop` really does destroy it.
func Example() {
	// The in-process server. It is a real TCP listener speaking real STOMP
	// 1.2; what it is not is an ActiveMQ, so it does not route a publication
	// and the Forward call below is where a broker's own fan-out would be.
	srv, err := stompsrv.New(stompsrv.Options{ServerHeader: activemq.ServerArtemis + "/2.44.0"})
	if err != nil {
		fmt.Println("starting the server:", err)
		return
	}
	defer func() { _ = srv.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	cfg := stomp.Config{
		Broker:      activemq.Artemis,
		Addrs:       []string{srv.Addr()},
		NoReconnect: true,
	}
	b, err := stomp.OpenBackend(ctx, cfg)
	if err != nil {
		fmt.Println("opening the backend:", err)
		return
	}
	defer func() { _ = b.Close() }()

	// The source first: a destination only copies a publication into the
	// subscriptions that exist when it is published, so a consumer opened
	// afterwards reads nothing.
	src, err := b.OpenSource(ctx, mq.SourceConfig{Topic: "orders", Subscription: "workers", Prefetch: 4})
	if err != nil {
		fmt.Println("opening the source:", err)
		return
	}
	defer func() { _ = src.Close() }()
	publish, consume, err := stomp.DestinationsForTest(cfg, "orders", "workers")
	if err != nil {
		fmt.Println("deriving the destinations:", err)
		return
	}
	if err := srv.WaitSubscribed(consume, 1, 10*time.Second); err != nil {
		fmt.Println("waiting for the subscription:", err)
		return
	}

	snk, err := b.OpenSink(ctx, mq.SinkConfig{Topic: "orders", Confirmation: mq.ConfirmAccepted})
	if err != nil {
		fmt.Println("opening the sink:", err)
		return
	}
	defer func() { _ = snk.Close() }()

	// ConfirmAccepted is a `receipt` header on the SEND frame and the
	// broker's RECEIPT frame coming back, which is the only answer a STOMP
	// publication has.
	m := &mq.Message{Body: []byte("order-1"), Key: []byte("customer-7")}
	if err := snk.Publish(ctx, m, nil); err != nil {
		fmt.Println("publishing:", err)
		return
	}
	fmt.Println("published to", publish)

	// A broker would now copy the publication into the subscription. This
	// server does not route, so the delivery is explicit.
	if _, err := srv.Forward(publish, consume); err != nil {
		fmt.Println("delivering:", err)
		return
	}

	dst := make([]mq.DriverDelivery, 4)
	n, err := src.Fetch(ctx, dst)
	if err != nil {
		fmt.Println("fetching:", err)
		return
	}
	fmt.Printf("fetched %d, body %q, attempt %d, redelivered %v\n",
		n, dst[0].Message.Body, dst[0].Info.Attempt, dst[0].Info.Redelivered)

	if err := src.Settler().Ack(ctx, &dst[0]); err != nil {
		fmt.Println("acknowledging:", err)
		return
	}
	// An ACK frame carries no receipt -- go-stomp's Conn.Ack takes no
	// options, so there is no way to ask for one -- and the call therefore
	// returns as soon as the frame is queued. Waiting for the frame is what
	// makes the count below deterministic.
	if err := srv.WaitFrames(frame.ACK, 1, 10*time.Second); err != nil {
		fmt.Println("waiting for the acknowledgement:", err)
		return
	}
	fmt.Printf("frames the broker saw: %d ACK, %d NACK\n", srv.Count(frame.ACK), srv.Count(frame.NACK))

	// Output:
	// published to orders
	// fetched 1, body "order-1", attempt 0, redelivered false
	// frames the broker saw: 1 ACK, 0 NACK
}

// ExampleConfig_DSN shows the connection string this driver answers to, and
// the one parameter that is required.
//
// The scheme is `activemq+stomp` and not `activemq`, which `drivers/activemq`
// claims over AMQP 1.0. The rule is that a protocol is never silently
// substituted: a registry in which one scheme could open either would make a
// connection string ambiguous about which capability table the caller gets,
// and the two tables really differ — there is no native nack over STOMP and no
// message selector over AMQP 1.0.
func ExampleConfig_DSN() {
	cfg := stomp.Config{
		Broker:   activemq.Classic,
		Addrs:    []string{"broker.example:61613"},
		Login:    "app",
		Passcode: "hunter2",
		// A selector is the one thing this arm has that the AMQP 1.0 arm
		// does not, and it works on Classic: measured, a `colour = 'red'`
		// selector kept a blue message out of the subscription.
		Selector:           "colour = 'red'",
		InsecurePlainLogin: true,
	}
	fmt.Println(cfg.DSN())
	// Output:
	// activemq+stomp://app:redacted@broker.example:61613?broker=classic&selector=colour%20%3D%20%27red%27&prefetch=32&consumerwindow=1048576&receipttimeout=30s&dialtimeout=15s&insecureplainlogin=true
}
