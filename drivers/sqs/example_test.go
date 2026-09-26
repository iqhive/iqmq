package sqs_test

import (
	"context"
	"fmt"
	"log"
	"time"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit/flush"
	sq "github.com/iqhive/iqmq/drivers/sqs"
	"github.com/iqhive/iqmq/dsn"
)

// Example shows the shape of an SQS client: a typed configuration, a
// publisher and a consumer.
//
// Nothing here runs against a real endpoint, so the example is about the
// surface rather than the round trip.
func Example() {
	cfg := sq.Config{
		Region:          "eu-west-1",
		AccessKeyID:     "AKIAEXAMPLE",
		SecretAccessKey: "not-a-real-secret",
		// The queue a topic maps to, before anything is created.
		QueuePrefix: "prod-",
	}
	name, err := sq.QueueName("orders/eu", cfg.QueuePrefix, cfg.FIFO)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("queue:", name)
	fmt.Println("profile:", cfg.Profile())
	fmt.Println("config:", cfg.DSN())
	// The rendered host is "aws", which is how a DSN says "the SDK's own
	// regional endpoint": the shared grammar refuses an authority that
	// carries a credential and no host, so the hostless form this used to
	// render did not parse back.
	//
	// Output:
	// queue: prod-orders-eu
	// profile: standard
	// config: sqs://AKIAEXAMPLE:redacted@aws/prod-?region=eu-west-1&pollers=1&maxmessages=10&waittime=20s&ackdeadline=30s&leaseguard=0s&settle=auto&settlemaxitems=10&body=auto&maxmessagebytes=1048576&retention=96h0m0s&apitimeout=30s
}

// Example_fIFO shows the FIFO profile, where the key becomes the message group
// and the capability table changes with it.
func Example_fIFO() {
	cfg := sq.Config{Region: "eu-west-1", FIFO: true}
	name, err := sq.QueueName("orders", cfg.QueuePrefix, cfg.FIFO)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("queue:", name)
	fmt.Println("group for key \"eu-west\":", sq.GroupID([]byte("eu-west")))
	fmt.Println("group for no key:", sq.GroupID(nil))
	// Output:
	// queue: orders.fifo
	// group for key "eu-west": eu-west
	// group for no key: iqmq-default
}

// Example_dSN shows the DSN forms, including the one that means "the SDK's own
// regional endpoint".
func Example_dSN() {
	for _, s := range []string{
		"sqs://AKIAEXAMPLE:secret@aws?region=eu-west-1&settle=hybrid",
		"sqs://x:x@127.0.0.1:9324/dev-?region=elasticmq&insecuretransport=true",
	} {
		cfg, err := sq.ParseDSN(s)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("endpoint=%q region=%q prefix=%q settle=%v\n",
			cfg.Endpoint, cfg.Region, cfg.QueuePrefix, cfg.Settle)
	}
	// Output:
	// endpoint="" region="eu-west-1" prefix="" settle=hybrid
	// endpoint="http://127.0.0.1:9324" region="elasticmq" prefix="dev-" settle=auto
}

// Example_register shows how a driver reaches a DSN registry the caller owns.
func Example_register() {
	r := &dsn.Registry{}
	if err := sq.Register(r); err != nil {
		log.Fatal(err)
	}
	fmt.Println("registered:", sq.Scheme)
	// Output:
	// registered: sqs
}

// Example_settlementPolicy shows the flush policies, which is what
// decides how many DeleteMessageBatch calls a consumer makes.
func Example_settlementPolicy() {
	for _, k := range flush.Kinds() {
		p, err := flush.New(k, flush.Config{MaxItems: 10, Interval: 50 * time.Millisecond})
		if err != nil {
			log.Fatal(err)
		}
		period, ticks := p.Ticker()
		fmt.Printf("%-9s maxheld=%3d ticker=%v(%v)\n", k, p.MaxHeld(), ticks, period)
	}
	// Output:
	// immediate maxheld=  0 ticker=false(0s)
	// count     maxheld=  9 ticker=false(0s)
	// time      maxheld= -1 ticker=true(50ms)
	// hybrid    maxheld=  9 ticker=true(50ms)
}

// Example_openWithClient shows the escape hatch: a caller who needs the SDK's
// default credential chain, a custom HTTP client or a middleware stack builds
// the *sqs.Client itself.
func Example_openWithClient() {
	// The configuration still applies: the profile, the resolver, the
	// settlement policy and the metadata mode are iqmq's, and the client is
	// the caller's.
	cfg := sq.Config{Region: "eu-west-1", Settle: flush.Hybrid}
	fmt.Println("ownership decides who closes the client:", cfg.Profile())
	_ = context.Background
	_ = mq.ConfirmDurable
	// Output:
	// ownership decides who closes the client: standard
}
