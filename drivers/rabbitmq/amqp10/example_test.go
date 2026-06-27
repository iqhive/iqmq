package amqp10_test

import (
	"context"
	"errors"
	"fmt"

	mq "bitbucket.org/iqhive/iqmq"
	core "bitbucket.org/iqhive/iqmq/drivers/amqp10"
	rmq10 "bitbucket.org/iqhive/iqmq/drivers/rabbitmq/amqp10"
	"bitbucket.org/iqhive/iqmq/dsn"
	"bitbucket.org/iqhive/iqmq/testkit"
)

// Example shows the shape of a deployment: an endpoint, a virtual host and the
// two assertions about the topology that two capability claims depend on.
//
// Nothing here declares anything. RabbitMQ 4's AMQP 1.0 endpoint cannot, so the
// exchange `acme.orders`, the quorum queue `acme.orders.workers` and its
// dead-letter route are somebody else's to create — `drivers/rabbitmq`, the
// management API or rabbitmqctl.
func Example() {
	cfg := rmq10.Config{
		Addrs:    []string{"amqp://rabbit-1:5672", "amqp://rabbit-2:5672"},
		Username: "iqmq",
		Password: dsn.Secret("s3cret"),
		Vhost:    "acme",
		Prefix:   "acme.",
		// The two facts the capability table moves with: the queues are
		// quorum queues, and they were declared with a dead-letter exchange.
		QueueType:  rmq10.QueueQuorum,
		DeadLetter: true,
		// Credit is what the AMQP 0-9-1 driver's prefetch AND its confirm
		// strategy became: every transfer gets its own disposition, so the
		// only window there is is the link's.
		Credit: 256,
	}
	// The password is rendered as the word "redacted" and not omitted: an
	// operator reading this line can tell a configuration with a credential
	// from one without, which is the first question an authentication
	// failure asks.
	fmt.Println(cfg)
	// Output:
	// rabbitmq+amqp10{addrs=[amqp://rabbit-1:5672 amqp://rabbit-2:5672] vhost=acme user=iqmq password=redacted prefix=acme. exchange=topic queue=quorum deadletter=true credit=256 receiversettle=default tls=false}
}

// ExampleConfig_DSN shows the connection string form and its redaction.
//
// The scheme is `rabbitmq+amqp10` and not `rabbitmq`, which belongs to the AMQP
// 0-9-1 driver: the specification says this profile is never silently substituted, and a
// registry in which one scheme could open either would make a connection string
// ambiguous about which capability table the caller gets.
func ExampleConfig_DSN() {
	cfg := rmq10.Config{
		Addrs:    []string{"rabbit-1:5672"},
		Username: "iqmq",
		Password: dsn.Secret("s3cret"),
		Vhost:    "acme",
	}
	fmt.Println(cfg.DSN())
	// Output:
	// rabbitmq+amqp10://iqmq:redacted@rabbit-1:5672?vhost=acme&exchangetype=topic&queuetype=quorum&credit=100&sasl=auto&receiversettle=default&sendersettle=default&backoff=default
}

// ExampleConfig_Profile shows what the module really produces: the profile the
// shared AMQP 1.0 core is opened with.
//
// A reader who wants to know what this driver decided reads these fields. The
// three absences are the specification's "feature subset" criterion — no
// delay, no de-duplication, no topology API — and the empty annotation prefix
// is what keeps a header in the section RabbitMQ converts between its two
// protocols through.
func ExampleConfig_Profile() {
	profile, err := rmq10.Config{Addrs: []string{"amqp://rabbit-1:5672"}}.Profile()
	if err != nil {
		panic(err)
	}
	sink, err := profile.SinkAddress("orders")
	if err != nil {
		panic(err)
	}
	source, err := profile.SourceAddress("orders", "workers")
	if err != nil {
		panic(err)
	}
	fmt.Println("driver:      ", profile.Driver+"/"+profile.Name)
	fmt.Println("sink:        ", sink)
	fmt.Println("source:      ", source)
	fmt.Println("auto-creates:", profile.AutoCreates)
	fmt.Println("delay:       ", profile.Delay.Supported())
	fmt.Println("dedup:       ", profile.Dedup.Supported())
	fmt.Println("admin:       ", profile.Admin != nil)
	// Output:
	// driver:       rabbitmq/amqp10
	// sink:         /exchanges/orders/orders
	// source:       /queues/orders.workers
	// auto-creates: false
	// delay:        false
	// dedup:        false
	// admin:        false
}

// ExampleOpen_versionGate is this module's headline behaviour as a caller sees
// it.
//
// A broker below RabbitMQ 4.0 is refused with ErrUnsupported naming both
// versions, and nothing falls back to AMQP 0-9-1. A caller who wants 0-9-1
// imports `drivers/rabbitmq` and says so, which is what the specification's "never silently
// substituted" asks of both modules.
func ExampleOpen_versionGate() {
	// A real deployment would name its own endpoint; this one is refused
	// before any dial because the configuration is invalid, which is the specification's
	// order: validate, then dial.
	_, err := rmq10.Open(context.Background(), rmq10.Config{})
	fmt.Println("no endpoint:", mq.CodeOf(err))

	// And the gate itself, without a broker.
	err = rmq10.CheckBrokerForTest("RabbitMQ", "3.13.7")
	fmt.Println("3.13.7:     ", errors.Is(err, mq.ErrUnsupported))
	fmt.Println("            ", err)
	fmt.Println("4.1.0:      ", rmq10.CheckBrokerForTest("RabbitMQ", "4.1.0") == nil)
	// Output:
	// no endpoint: InvalidConfig
	// 3.13.7:      true
	//              iqmq/rabbitmq: the AMQP 1.0 profile for RabbitMQ needs RabbitMQ 4.0.0 or newer; the broker is 3.13.7
	// 4.1.0:       true
}

// ExampleConfig_CoreConfig shows the one translation that is easiest to get
// wrong by hand, which is why this module exists as a typed configuration
// rather than as a profile beside a `core.Config`.
//
// AMQP 1.0 has no virtual-host field. RabbitMQ 4 selects one from the OPEN
// frame's `hostname`, as `vhost:<name>`, so a configuration whose virtual host
// went anywhere else lands in the default virtual host and publishes into the
// wrong topology with nothing said.
func ExampleConfig_CoreConfig() {
	cc, err := rmq10.Config{
		Addrs: []string{"rabbit-1"},
		Vhost: "acme",
	}.CoreConfig()
	if err != nil {
		panic(err)
	}
	fmt.Println("hostname:", cc.HostName)
	fmt.Println("endpoint:", cc.Addrs[0])
	fmt.Println("credit:  ", cc.MaxCredit)
	// Output:
	// hostname: vhost:acme
	// endpoint: amqp://rabbit-1:5672
	// credit:   100
}

// ExampleRegister wires the profile into a DSN registry a library owns.
//
// Registering this module and `drivers/rabbitmq` with one registry is the
// supported arrangement — the two schemes do not collide — and it is how a
// deployment moves one application from one profile to the other by editing a
// connection string.
func ExampleRegister() {
	r := dsn.NewRegistry()
	if err := rmq10.Register(r); err != nil {
		panic(err)
	}
	fmt.Println(r.Schemes())
	// Output:
	// [rabbitmq+amqp10]
}

// ExampleConformanceHarness shows what a conformance suite in another module
// hands the kit, and the one thing it has to do that no other driver's
// suite does.
func ExampleConformanceHarness() {
	cfg := rmq10.Config{Addrs: []string{"amqp://127.0.0.1:5672"}, DeadLetter: true}
	h := rmq10.ConformanceHarness(cfg, testkit.EvidenceContainer, testkit.Broker{Image: "rabbitmq:4.1-management"})
	fmt.Println(h.Driver + "/" + h.Profile)
	fmt.Println("client:", core.ClientModule, core.ClientVersion)
	fmt.Println("fidelity:", h.Fidelity)
	// Output:
	// rabbitmq/amqp10
	// client: github.com/Azure/go-amqp v1.6.0
	// fidelity: 5000
}
