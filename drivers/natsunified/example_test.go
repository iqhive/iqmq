package natsunified_test

import (
	"context"
	"errors"
	"fmt"
	"strings"

	mq "bitbucket.org/iqhive/iqmq"
	"bitbucket.org/iqhive/iqmq/drivers/natsunified"
	"bitbucket.org/iqhive/iqmq/dsn"
)

// One entry point, two brokers: the scheme decides which.
func ExampleParseDSN() {
	for _, s := range []string{
		"nats://broker.internal:4222/acme",
		"natsjs://broker.internal:4222/acme?stream=ORDERS",
	} {
		cfg, err := natsunified.ParseDSN(s)
		if err != nil {
			fmt.Println("error:", err)
			continue
		}
		fmt.Printf("%-12s profile=%-9s driver=%s\n", cfg.Profile.Scheme()+"://", cfg.Profile, cfg.Profile.Driver())
	}
	// Output:
	// nats://      profile=core      driver=nats
	// natsjs://    profile=jetstream driver=jetstream
}

// A parameter that belongs to the other profile is refused by name, because the
// scheme is the only spelling of the profile.
func ExampleParseDSN_wrongProfilesParameter() {
	_, err := natsunified.ParseDSN("nats://broker:4222?stream=ORDERS")
	fmt.Println(strings.Contains(err.Error(), "stream"))
	// Output: true
}

// The profile without the rest of the DSN, for a program that has to decide
// which half of its own configuration to read.
func ExampleProfileForDSN() {
	p, err := natsunified.ProfileForDSN("natsjs://broker:4222?stream=ORDERS")
	fmt.Println(p, err)

	_, err = natsunified.ProfileForDSN("kafka://broker:9092")
	fmt.Println(err)
	// Output:
	// jetstream <nil>
	// iqmq/natsunified: the DSN's scheme is neither nats:// (NATS Core) nor natsjs:// (JetStream)
}

// What a caller who takes its profile from configuration may rely on before it
// has a client. A feature in this list may still mean different things on the
// two profiles — the level and the note are the client's own answer.
func ExampleCommon() {
	for _, f := range natsunified.Common() {
		fmt.Println(f)
	}
	fmt.Println("ExplicitAck:", natsunified.CommonHas(mq.FeatureExplicitAck))
	// Output:
	// ConsumerGroups
	// FanOut
	// OrderedByKey
	// OrderedByPartition
	// OrderedGlobally
	// NativeHeaders
	// DuplicateHeaders
	// BatchPublish
	// BatchReceive
	// WildcardSubscribe
	// PublisherConfirm
	// ExplicitAck: false
}

// The shape for a program whose profile comes from a connection string it did
// not write. A DSN cannot carry a capability requirement, so it is two steps:
// parse, state what the application needs, open. The Open is refused on a
// profile that cannot meet the requirement, rather than the first consumer
// being refused after the program is already running — and the client the
// facade refuses is closed before the error comes back.
func ExampleConfig_Require() {
	cfg := natsunified.Config{
		Profile: natsunified.ProfileCore,
		Core:    exampleCoreConfig(),
		Require: []mq.Requirement{
			mq.RequireAtLeast(mq.FeatureExplicitAck, mq.Native),
			mq.RequireAtLeast(mq.FeatureAtLeastOnce, mq.Native),
		},
	}
	client, err := natsunified.Open(context.Background(), cfg)
	if err == nil {
		_ = client.Close()
		fmt.Println("opened, which NATS Core should not be able to do")
		return
	}
	fmt.Printf("%v refused at Open: unsupported=%v\n", cfg.Profile, errors.Is(err, mq.ErrUnsupported))

	// The same requirement on the other profile is met, so the refusal is
	// about the broker and not about the facade.
	js := exampleJetStreamConfig()
	js.Require = cfg.Require
	client, err = natsunified.Open(context.Background(), js)
	if err != nil {
		fmt.Println("jetstream:", err)
		return
	}
	fmt.Printf("%v opened: driver=%s\n", js.Profile, client.Info().Name)
	_ = client.Close()
	// Output:
	// core refused at Open: unsupported=true
	// jetstream opened: driver=jetstream
}

// Registering both schemes with a registry the program owns. RegisterDriver is
// the atomic route: a collision on either scheme registers neither.
func ExampleRegister() {
	var r dsn.Registry
	if err := natsunified.Register(&r); err != nil {
		fmt.Println(err)
		return
	}
	for _, scheme := range natsunified.Schemes() {
		name, _ := r.DriverName(scheme)
		fmt.Printf("%s:// -> %s\n", scheme, name)
	}
	// Output:
	// nats:// -> natsunified
	// natsjs:// -> natsunified
}

// A round trip through the facade on the Core profile: at most once, no
// settlement, no topology to create.
func ExampleOpen_core() {
	ctx := context.Background()
	client, err := natsunified.Open(ctx, natsunified.Config{
		Profile: natsunified.ProfileCore,
		Core:    exampleCoreConfig(),
	})
	if err != nil {
		fmt.Println("open:", err)
		return
	}
	defer func() { _ = client.Close() }()

	con, err := client.Consumer(ctx, mq.ConsumerConfig{Topic: "example.core", Subscription: "readme"})
	if err != nil {
		fmt.Println("consumer:", err)
		return
	}
	defer func() { _ = con.Close() }()

	if err := client.Publish(ctx, "example.core", &mq.Message{Body: []byte("hello")}); err != nil {
		fmt.Println("publish:", err)
		return
	}
	dst := make([]mq.Delivery, 1)
	if _, err := con.Receive(ctx, dst); err != nil {
		fmt.Println("receive:", err)
		return
	}
	fmt.Printf("driver=%s body=%s\n", client.Info().Name, dst[0].Body)
	// Output: driver=nats body=hello
}

// The same code on the JetStream profile, which settles. The only difference in
// the program is the profile and the stream the deployment names.
func ExampleOpen_jetStream() {
	ctx := context.Background()
	client, err := natsunified.Open(ctx, exampleJetStreamConfig())
	if err != nil {
		fmt.Println("open:", err)
		return
	}
	defer func() { _ = client.Close() }()

	con, err := client.Consumer(ctx, mq.ConsumerConfig{
		Topic: "orders", Subscription: "readme", Start: mq.StartEarliest(),
	})
	if err != nil {
		fmt.Println("consumer:", err)
		return
	}
	defer func() { _ = con.Close() }()

	if err := client.Publish(ctx, "orders", &mq.Message{Body: []byte("hello")}); err != nil {
		fmt.Println("publish:", err)
		return
	}
	dst := make([]mq.Delivery, 1)
	if _, err := con.Receive(ctx, dst); err != nil {
		fmt.Println("receive:", err)
		return
	}
	fmt.Printf("driver=%s body=%s ack=%v\n", client.Info().Name, dst[0].Body, dst[0].Ack())
	// Output: driver=jetstream body=hello ack=<nil>
}
