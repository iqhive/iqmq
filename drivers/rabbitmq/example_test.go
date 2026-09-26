package rabbitmq_test

import (
	"fmt"

	"github.com/iqhive/iqmq/drivers/rabbitmq"
	"github.com/iqhive/iqmq/dsn"
)

// Example is this driver's Open, in the typed form the specification prefers: an endpoint
// list, credentials, a virtual host and the topology facts two capability
// claims depend on.
//
// Declare is what separates this driver from `drivers/rabbitmq/amqp10`: the
// AMQP 0-9-1 endpoint CAN declare an exchange, a queue and a binding, so this
// driver is the one a test or a local development stack asks to create its
// own topology. It is off by default all the same — the specification's "Open never
// creates production topology" — and turning it on in production is a
// deliberate choice about who owns the topology.
//
// Open is not called here because it dials: the example shows the
// configuration a deployment holds, and `make integration DRIVER=rabbitmq`
// is what runs it against a broker.
func Example() {
	cfg := rabbitmq.Config{
		Addrs:    []string{"amqp://rabbit-1:5672", "amqp://rabbit-2:5672"},
		Username: "iqmq",
		Password: dsn.Secret("s3cret"),
		Vhost:    "acme",
		Prefix:   "acme.",
		// The two facts the capability table moves with.
		QueueType:    rabbitmq.QueueQuorum,
		DeadLetter:   true,
		Declare:      true, // DeadLetter needs it: the route has to be created
		ExchangeType: "topic",
	}
	fmt.Println(cfg)
	// Output:
	// rabbitmq{addrs=[amqp://iqmq:redacted@rabbit-1:5672/acme amqp://iqmq:redacted@rabbit-2:5672/acme] vhost=acme prefix=acme. exchange=topic queue=quorum declare=true deadletter=true mandatory=false confirm=auto delay=auto tls=false}
}

// ExampleConfig_DSN is the other form: the string a deployment can hand a
// process, and the round trip that says the two forms agree.
//
// The password is redacted by String and by DSN, which is the whole reason
// dsn.Secret exists: a Config printed into a log must not carry a credential,
// and the redaction has to be in the type rather than in every call site that
// prints one.
func ExampleConfig_DSN() {
	cfg, err := rabbitmq.ParseDSN("rabbitmq://iqmq:s3cret@rabbit-1:5672/acme?queuetype=quorum&deadletter=true&declare=true")
	if err != nil {
		panic(err)
	}
	fmt.Println("vhost:", cfg.Vhost, "queue:", cfg.QueueType)
	// queuetype is absent from the rendering because QueueQuorum is the zero
	// value: a DSN carries what differs from the defaults, so the round trip
	// is by VALUE and not by text.
	fmt.Println("dsn:", cfg.DSN())

	_, err = rabbitmq.ParseDSN("kafka://nope")
	fmt.Println("a foreign scheme is refused:", err != nil)
	// Output:
	// vhost: acme queue: quorum
	// dsn: rabbitmq://iqmq:redacted@rabbit-1:5672/acme?declare=true&deadletter=true
	// a foreign scheme is refused: true
}
