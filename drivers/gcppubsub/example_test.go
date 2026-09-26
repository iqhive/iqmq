package gcppubsub_test

import (
	"context"
	"fmt"
	"time"

	mq "github.com/iqhive/iqmq"
	gps "github.com/iqhive/iqmq/drivers/gcppubsub"
	"github.com/iqhive/iqmq/dsn"
)

// Example shows the shape of an ordered, dead-lettered consumer: the two
// configuration fields that turn a conditional capability on, and the two
// settlements that mean something different on this broker from anywhere else.
func Example() {
	cfg := gps.Config{
		ProjectID: "my-project",
		// Ordering is a CONSTRUCTION flag because the client library's
		// EnableMessageOrdering is one: a publisher built without it refuses
		// every publication that carries an ordering key.
		Ordering: true,
		// The dead-letter policy is what makes DeliveryAttempt present at
		// all, and five is the service's own minimum for the attempt count.
		DeadLetterTopic:     "orders-dlq",
		MaxDeliveryAttempts: 5,
		// How long a handler may hold a delivery. It is NOT the
		// acknowledgement deadline: the client library keeps extending that
		// automatically, so this is the real bound.
		MaxExtension: time.Minute,
		AckDeadline:  10 * time.Second,
	}
	if err := cfg.Validate(); err != nil {
		fmt.Println("configuration:", err)
		return
	}
	// A configuration never renders a credential, so it is safe in a log line.
	cfg.CredentialsFile = "/etc/keys/sa.json"
	fmt.Println(cfg.DSN())

	// What the table says about the two claims this broker's own vocabulary
	// would get wrong.
	caps := mustCapabilities(cfg)
	fmt.Println("exactly-once delivery:", caps.Support(mq.FeatureExactlyOnceDelivery).Level)
	fmt.Println("delayed redelivery:", caps.Support(mq.FeatureDelayedRedelivery).Level)
	fmt.Println("ordering by key:", caps.Support(mq.FeatureOrderedByKey).Level)

	// Output:
	// gcppubsub://my-project?ordering=true&ackdeadline=10s&maxextension=1m0s&deadlettertopic=orders-dlq&maxdeliveryattempts=5&credentialsfile=/etc/keys/sa.json
	// exactly-once delivery: unsupported
	// delayed redelivery: emulatedvolatile
	// ordering by key: conditional
}

// ExampleConfig_DSN shows the two authority shapes: the project in the host
// when there is no endpoint, and the endpoint in the host with the project in
// the path when there is one.
func ExampleConfig_DSN() {
	for _, in := range []string{
		"gcppubsub://my-project?ordering=true",
		"gcppubsub://127.0.0.1:8085/my-project?insecuretransport=true&autocreate=true",
	} {
		cfg, err := gps.ParseDSN(in)
		if err != nil {
			fmt.Println("parse:", err)
			continue
		}
		fmt.Printf("project=%s endpoint=%q insecure=%v\n", cfg.ProjectID, cfg.Endpoint, cfg.InsecureTransport)
	}
	// A credential set in Go is redacted rather than round-tripped.
	cfg, _ := gps.ParseDSN("gcppubsub://my-project")
	cfg.CredentialsJSON = dsn.Secret(`{"private_key":"..."}`)
	fmt.Println(cfg.DSN())

	// Output:
	// project=my-project endpoint="" insecure=false
	// project=my-project endpoint="127.0.0.1:8085" insecure=true
	// gcppubsub://my-project?credentialsjson=redacted
}

// mustCapabilities builds a client's capability table, for the examples.
func mustCapabilities(cfg gps.Config) mq.Capabilities {
	srv := newExampleServer(cfg)
	b, err := gps.BackendWithAPI(context.Background(), srv, ownedForExample(), cfg)
	if err != nil {
		panic(err)
	}
	defer func() { _ = b.Close() }()
	return b.Capabilities()
}
