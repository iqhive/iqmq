package activemq_test

import (
	"fmt"

	amq "bitbucket.org/iqhive/iqmq/drivers/activemq"
	"bitbucket.org/iqhive/iqmq/internal/activemq"
)

// Example shows what a caller has to say to open this driver, and the one
// thing about it that is not obvious: the BROKER is required.
//
// It is a compile-and-render example rather than one that publishes, because
// publishing needs a broker and `make ci` runs examples on every commit. What
// it demonstrates is the destinations the two products derive from the same
// topic and subscription, which is the whole of what this driver decides.
func Example() {
	for _, broker := range activemq.Brokers() {
		cfg := amq.Config{
			// Required. There is no default, because nothing on the wire
			// says which ActiveMQ a caller meant and getting it wrong is
			// not an error on either: Artemis would create the other
			// product's destination and publish into it for ever.
			Broker: broker,
			Addrs:  []string{"localhost:5672"},
			Prefix: "iqmq.",
		}
		sink, source, err := amq.AddressForTest(cfg, "orders", "workers")
		if err != nil {
			fmt.Println("error:", err)
			return
		}
		fmt.Printf("%-8s publish to %-34s consume from %s\n", broker, sink, source)
	}
	// Output:
	// artemis  publish to iqmq.orders                        consume from iqmq.orders::iqmq.orders.workers
	// classic  publish to topic://VirtualTopic.iqmq.orders   consume from queue://Consumer.workers.VirtualTopic.iqmq.orders
}

// ExampleConfig_DSN shows the connection string, with the credential redacted
// require.
func ExampleConfig_DSN() {
	cfg := amq.Config{
		Broker:            activemq.Classic,
		Addrs:             []string{"localhost:5672"},
		Username:          "iqmq",
		Password:          "never-printed",
		Scheduler:         true,
		InsecurePlainSASL: true,
	}
	fmt.Println(cfg.DSN())
	// Output:
	// activemq://iqmq:redacted@localhost:5672?broker=classic&sasl=auto&receiversettle=default&sendersettle=default&backoff=fulljitter&scheduler=true&insecureplainsasl=true
}
