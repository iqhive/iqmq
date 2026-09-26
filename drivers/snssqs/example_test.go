package snssqs_test

import (
	"context"
	"fmt"
	"log"
	"time"

	mq "github.com/iqhive/iqmq"
	sns "github.com/iqhive/iqmq/drivers/snssqs"
	"github.com/iqhive/iqmq/dsn"
)

// Example shows the shape of a fan-out client: a typed configuration, the SNS
// topic a portable topic becomes, and the queue each subscription gets.
//
// Nothing here runs against a real endpoint, so the example is about the
// surface rather than the round trip.
func Example() {
	cfg := sns.Config{
		Region:          "eu-west-1",
		AccessKeyID:     "AKIAEXAMPLE",
		SecretAccessKey: "not-a-real-secret",
		AccountID:       "000000000000",
		TopicPrefix:     "prod-",
		QueuePrefix:     "prod-",
	}
	topic, err := sns.TopicName("orders/eu", cfg.TopicPrefix)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("topic:", topic)
	for _, sub := range []string{"billing", "shipping"} {
		queue, err := sns.QueueName("orders/eu", sub, cfg.TopicPrefix, cfg.QueuePrefix)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("subscription %q -> queue %s\n", sub, queue)
	}
	fmt.Println("topic ARN:", sns.TopicARN(cfg.Partition, cfg.Region, cfg.AccountID, topic))
	// Output:
	// topic: prod-orders-eu
	// subscription "billing" -> queue prod-prod-orders-eu-billing
	// subscription "shipping" -> queue prod-prod-orders-eu-shipping
	// topic ARN: arn:aws:sns:eu-west-1:000000000000:prod-orders-eu
}

// Example_fanOut is the shape of the thing itself: one publication, two
// subscriptions, and two settlements that have nothing to do with each other.
//
// The commentary is the point rather than the code. There is no operation
// anywhere in this driver that settles "the publication": a subscriber that
// acknowledges has acknowledged ITS copy, and a message dead-lettered for one
// subscription is still being processed by the others.
func Example_fanOut() {
	fmt.Println("one Publish to the SNS topic reaches every subscribed queue")
	for _, sub := range []string{"billing", "shipping"} {
		queue, err := sns.QueueName("orders", sub, "", "")
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("  %s consumes %s and settles its own copy\n", sub, queue)
	}
	// Output:
	// one Publish to the SNS topic reaches every subscribed queue
	//   billing consumes orders-billing and settles its own copy
	//   shipping consumes orders-shipping and settles its own copy
}

// Example_filterPolicy shows the conditional capability: a subscription that
// receives a subset of the topic.
//
// The condition the specification makes the claim state is in the capability note, and the
// part worth knowing is which half of the message a policy can see: an
// attributes-scope policy filters on the message's HEADERS, and an enveloped
// message has no headers on the wire at all.
func Example_filterPolicy() {
	cfg := sns.Config{
		Region:            "eu-west-1",
		AccountID:         "000000000000",
		FilterPolicies:    map[string]string{"eu-billing": `{"region":["eu","uk"]}`},
		FilterPolicyScope: sns.FilterScopeAttributes,
	}
	if err := cfg.Validate(); err != nil {
		log.Fatal(err)
	}
	fmt.Println("scope:", cfg.FilterPolicyScope, "->", cfg.FilterPolicyScope.Attribute())
	fmt.Println("policy for eu-billing:", cfg.FilterPolicies["eu-billing"])
	if err := sns.CheckFilterPolicy("eu-billing", "{}"); err != nil {
		fmt.Println("an empty policy:", "refused")
	}
	// Output:
	// scope: attributes -> MessageAttributes
	// policy for eu-billing: {"region":["eu","uk"]}
	// an empty policy: refused
}

// Example_dSN shows the DSN forms, including the one that means "the services'
// own regional endpoints".
func Example_dSN() {
	for _, s := range []string{
		"snssqs://AKIAEXAMPLE:secret@aws?region=eu-west-1&accountid=000000000000",
		"snssqs://test:test@127.0.0.1:4566?region=localstack&insecuretransport=true&autocreate=true",
	} {
		cfg, err := sns.ParseDSN(s)
		if err != nil {
			log.Fatal(err)
		}
		endpoint := cfg.Endpoint
		if endpoint == "" {
			endpoint = "(the services' regional endpoints)"
		}
		fmt.Println(cfg.Region, endpoint)
	}
	// Output:
	// eu-west-1 (the services' regional endpoints)
	// localstack http://127.0.0.1:4566
}

// Example_register shows ADR-012's registration: a driver registers with a
// registry the caller owns and never reaches for a global one.
func Example_register() {
	var reg dsn.Registry
	if err := sns.Register(&reg); err != nil {
		log.Fatal(err)
	}
	// Opening performs no I/O at all: both services are request/response
	// over HTTP, so there is nothing to dial.
	client, err := openRegistered(&reg)
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = client.Close() }()
	info := client.Info()
	fmt.Println(info.Name, info.Profile)
	fmt.Println("fan-out:", client.Capabilities().Has(mq.FeatureFanOut))
	fmt.Println("scheduled publication:", client.Capabilities().Has(mq.FeatureScheduledPublish))
	// Output:
	// snssqs standard
	// fan-out: true
	// scheduled publication: false
}

// openRegistered opens the client in a function of its own, so that the
// context's cancel runs before the example's log.Fatal can exit the process.
func openRegistered(reg *dsn.Registry) (*mq.Client, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	return reg.Open(ctx, "snssqs://test:test@127.0.0.1:4566?region=localstack&insecuretransport=true")
}
