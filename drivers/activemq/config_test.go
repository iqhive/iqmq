package activemq_test

import (
	"errors"
	"strings"
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
	amq "github.com/iqhive/iqmq/drivers/activemq"
	core "github.com/iqhive/iqmq/drivers/amqp10"
	"github.com/iqhive/iqmq/dsn"
	"github.com/iqhive/iqmq/internal/activemq"
)

// TestConfigValidateRefusals holds every refusal in config.go to a reason, in
// both directions: the configuration that is refused and the nearest one that
// is not.
//
// The specification requires a configuration to be validated before any background work
// starts, and the reason it matters more here than in most drivers is that a
// STOMP or AMQP broker answers a frame it refuses by detaching the link or
// closing the connection -- so a caller's mistake that reached the wire would
// cost every other handle on that connection.
func TestConfigValidateRefusals(t *testing.T) {
	t.Parallel()
	base := func() amq.Config {
		return amq.Config{
			Broker:            activemq.Artemis,
			Addrs:             []string{"localhost:5672"},
			InsecurePlainSASL: true,
		}
	}
	for name, tc := range map[string]struct {
		mutate func(*amq.Config)
		want   error
		code   mq.ErrorCode
	}{
		"no broker": {
			// There is no default and there must not be: nothing on the
			// wire says which product a caller meant, and Artemis would
			// create the other product's destination rather than refuse.
			mutate: func(c *amq.Config) { c.Broker = activemq.Broker(9) },
			code:   mq.CodeInvalidConfig,
		},
		"artemis with receiver-settle-second": {
			// Measured: Artemis answers an attach requesting `second` with
			// `first` and go-amqp refuses the negotiation, so every source
			// would fail at attach with a message about settlement modes
			// rather than about configuration.
			mutate: func(c *amq.Config) { c.ReceiverSettle = core.ReceiverSettleSecond },
			code:   mq.CodeInvalidConfig,
		},
		"dead letter both ways": {
			mutate: func(c *amq.Config) { c.DeadLetter, c.NoDeadLetter = true, true },
			code:   mq.CodeInvalidConfig,
		},
		"a prefix a destination cannot carry": {
			mutate: func(c *amq.Config) { c.Prefix = "a::b" },
			code:   mq.CodeInvalidArgument,
		},
		"a dead-letter topic with a wildcard": {
			mutate: func(c *amq.Config) { c.DeadLetterTopic = "orders.#" },
			code:   mq.CodeUnsupported,
		},
		"a negative credit": {
			mutate: func(c *amq.Config) { c.Credit = -1 },
			code:   mq.CodeInvalidConfig,
		},
		"a negative timeout": {
			mutate: func(c *amq.Config) { c.OperationTimeout = -time.Second },
			code:   mq.CodeInvalidConfig,
		},
		"a password in the clear": {
			// Insecure* convention: the default refuses, and the
			// flag is how a caller says so deliberately.
			mutate: func(c *amq.Config) { c.Password, c.InsecurePlainSASL = dsn.Secret("p"), false },
			code:   mq.CodeInvalidConfig,
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			cfg := base()
			tc.mutate(&cfg)
			err := cfg.Validate()
			if err == nil {
				t.Fatalf("Validate accepted %s", name)
			}
			if got := mq.CodeOf(err); got != tc.code {
				t.Errorf("the refusal of %s is %v (code %v), want code %v", name, err, got, tc.code)
			}
		})
	}
	// And the nearest configuration that is NOT refused, which is what makes
	// the table above a set of refusals rather than a validator that refuses
	// everything.
	t.Run("the base configuration is accepted", func(t *testing.T) {
		t.Parallel()
		if err := base().Validate(); err != nil {
			t.Fatalf("Validate refused the base configuration: %v", err)
		}
	})
	t.Run("classic with receiver-settle-second is accepted", func(t *testing.T) {
		t.Parallel()
		// The refusal above is ARTEMIS's, measured, and not the protocol's:
		// a driver that refused it everywhere would be taking a setting away
		// from a broker that has it.
		cfg := base()
		cfg.Broker, cfg.ReceiverSettle = activemq.Classic, core.ReceiverSettleSecond
		if err := cfg.Validate(); err != nil {
			t.Fatalf("Validate refused receiver-settle-second on Classic: %v", err)
		}
	})
}

// TestNamesDeriveBothProducts pins the two mappings against each other, which
// is the whole reason `internal/activemq` exists: the two products share no
// name and a driver that derived them separately would publish to one and
// consume from another.
func TestNamesDeriveBothProducts(t *testing.T) {
	t.Parallel()
	for name, tc := range map[string]struct {
		broker            activemq.Broker
		prefix            string
		wantPublish       string
		wantConsume       string
		wantQueueContains string
	}{
		"artemis": {
			broker:      activemq.Artemis,
			wantPublish: "orders",
			// The FQQN's address half must be the publish destination or a
			// consumer reads an address nothing publishes to, and the
			// queue name carries the TOPIC because an Artemis queue name is
			// unique across the whole broker.
			wantConsume:       "orders::orders.workers",
			wantQueueContains: "orders.workers",
		},
		"artemis with a prefix": {
			broker:            activemq.Artemis,
			prefix:            "iqmq.",
			wantPublish:       "iqmq.orders",
			wantConsume:       "iqmq.orders::iqmq.orders.workers",
			wantQueueContains: "iqmq.orders.workers",
		},
		"classic": {
			broker: activemq.Classic,
			// Every Classic destination carries its scheme, because the
			// broker decides whether a name is a queue or a topic from the
			// destination's own shape -- and an undecorated name is a QUEUE
			// that every consumer queue is subscribed to and none of them
			// receives anything from.
			wantPublish:       "topic://VirtualTopic.orders",
			wantConsume:       "queue://Consumer.workers.VirtualTopic.orders",
			wantQueueContains: "Consumer.workers.VirtualTopic.orders",
		},
		"classic with a prefix": {
			broker: activemq.Classic,
			prefix: "iqmq.",
			// The prefix goes INSIDE `VirtualTopic.`, and the ordering was
			// settled by the broker rather than by reasoning: Classic's
			// default interceptor matches `VirtualTopic.>`, so
			// `iqmq.VirtualTopic.orders` is an ordinary topic that accepts
			// every publication and writes to no consumer queue. Measured.
			wantPublish:       "topic://VirtualTopic.iqmq.orders",
			wantConsume:       "queue://Consumer.workers.VirtualTopic.iqmq.orders",
			wantQueueContains: "Consumer.workers.VirtualTopic.iqmq.orders",
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			cfg := amq.Config{Broker: tc.broker, Prefix: tc.prefix, Addrs: []string{"localhost:5672"}}
			n, err := cfg.Names("orders", "workers")
			if err != nil {
				t.Fatalf("Names: %v", err)
			}
			if n.Publish != tc.wantPublish {
				t.Errorf("publish destination is %q, want %q", n.Publish, tc.wantPublish)
			}
			if n.Consume != tc.wantConsume {
				t.Errorf("consume destination is %q, want %q", n.Consume, tc.wantConsume)
			}
			if !strings.Contains(n.Queue, tc.wantQueueContains) {
				t.Errorf("the queue name is %q, want it to contain %q", n.Queue, tc.wantQueueContains)
			}
			// The derivation is its own inverse, which is what Admin's
			// ListTopics rests on.
			topic, sub, ok := activemq.ParseQueue(tc.broker, tc.prefix, n.Queue)
			if !ok || topic != "orders" || sub != "workers" {
				t.Errorf("the queue %q parses back as (%q, %q, %v), want (\"orders\", \"workers\", true)",
					n.Queue, topic, sub, ok)
			}
		})
	}
}

// TestArtemisQueueNamesAreGloballyUnique is the measured fact that shaped the
// naming, pinned so that a future simplification cannot undo it.
//
// Measured against apache/activemq-artemis:2.44.0-alpine: `createQueue` of a
// queue called `subA` on one address is refused with "AMQ229019: Queue subA
// already exists on address iqmq.f1" when a queue of that name exists on
// ANOTHER address. So an Artemis queue name is unique across the whole broker,
// and two topics with a subscription called "workers" collide -- the second
// create is refused and, worse, a consumer that attached by FQQN would be
// refused with amqp:not-found while the queue it names exists elsewhere.
//
// The naming answers it by carrying the topic in the queue name, which this
// test is what holds it to.
func TestArtemisQueueNamesAreGloballyUnique(t *testing.T) {
	t.Parallel()
	cfg := amq.Config{Broker: activemq.Artemis, Addrs: []string{"localhost:5672"}}
	a, err := cfg.Names("orders", "workers")
	if err != nil {
		t.Fatalf("Names(orders): %v", err)
	}
	b, err := cfg.Names("invoices", "workers")
	if err != nil {
		t.Fatalf("Names(invoices): %v", err)
	}
	if a.Queue == b.Queue {
		t.Fatalf("two topics with a subscription called \"workers\" derive the same queue name %q; an Artemis queue "+
			"name is unique across the whole broker, so the second createQueue is refused with AMQ229019 and a "+
			"consumer attaching by FQQN is refused with amqp:not-found while the queue it names exists elsewhere",
			a.Queue)
	}
}

// TestConfigRedactsItsPassword is nothing a driver renders may
// carry a credential.
func TestConfigRedactsItsPassword(t *testing.T) {
	t.Parallel()
	const secret = "sup3r-s3cret"
	cfg := amq.Config{
		Broker:            activemq.Artemis,
		Addrs:             []string{"broker:5672"},
		Username:          "u",
		Password:          dsn.Secret(secret),
		InsecurePlainSASL: true,
	}
	for name, got := range map[string]string{
		"String":   cfg.String(),
		"DSN":      cfg.DSN(),
		"LogValue": cfg.LogValue().String(),
	} {
		if strings.Contains(got, secret) {
			t.Errorf("%s rendered the password: %s", name, got)
		}
		if !strings.Contains(got, "u") {
			t.Errorf("%s dropped the username, so the rendering cannot be parsed back: %s", name, got)
		}
	}
}

// TestCheckBrokerRefusesTheOtherProduct is the gate the version check cannot
// be, in both directions.
//
// The hazard is concrete and is why the product is checked FIRST: the two
// products' topic mappings name nothing in common, and Artemis does not fail
// an attach to a name it does not have -- it creates it. A client configured
// for Classic and pointed at Artemis would create an address literally called
// `topic://VirtualTopic.orders`, publish into it for ever, and deliver
// nothing.
func TestCheckBrokerRefusesTheOtherProduct(t *testing.T) {
	t.Parallel()
	for name, tc := range map[string]struct {
		broker  activemq.Broker
		product string
		version string
		wantErr bool
	}{
		"artemis configured, artemis found":     {activemq.Artemis, activemq.ProductArtemis, "2.44.0", false},
		"artemis configured, nothing reported":  {activemq.Artemis, "", "", false},
		"artemis configured, rabbitmq found":    {activemq.Artemis, "rabbitmq", "4.3.5", true},
		"classic configured, artemis found":     {activemq.Classic, activemq.ProductArtemis, "2.44.0", true},
		"classic configured, nothing reported":  {activemq.Classic, "", "", false},
		"artemis configured, artemis 1.x found": {activemq.Artemis, activemq.ProductArtemis, "1.5.6", true},
		"classic configured, classic 5.9 found": {activemq.Classic, "ActiveMQ", "5.9.0", false},
		"classic configured, classic 5.8 found": {activemq.Classic, "ActiveMQ", "5.8.0", true},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			cfg := amq.Config{Broker: tc.broker, Addrs: []string{"localhost:5672"}, InsecurePlainSASL: true}
			err := amq.CheckBrokerForTest(cfg, tc.product, tc.version)
			switch {
			case tc.wantErr && err == nil:
				t.Fatalf("the gate accepted %s", name)
			case !tc.wantErr && err != nil:
				t.Fatalf("the gate refused %s: %v", name, err)
			case tc.wantErr && !errors.Is(err, mq.ErrUnsupported):
				t.Errorf("the refusal of %s is %v, which does not reach ErrUnsupported: a broker that is real and "+
					"reachable and does not have the feature is Unsupported rather than a configuration error",
					name, err)
			}
		})
	}
}

// TestSilenceIsNotTooOld is the measured asymmetry against the RabbitMQ pair's
// rule, pinned because it looks like an oversight.
//
// Measured: ActiveMQ Classic sends NO product and NO version in its AMQP 1.0
// open performative at all. So a driver that refused silence -- which is what
// `drivers/rabbitmq/amqp10` does, correctly, for RabbitMQ -- would refuse one
// of the four combinations the specification names.
func TestSilenceIsNotTooOld(t *testing.T) {
	t.Parallel()
	cfg := amq.Config{Broker: activemq.Classic, Addrs: []string{"localhost:5672"}, InsecurePlainSASL: true}
	if err := amq.CheckBrokerForTest(cfg, "", ""); err != nil {
		t.Fatalf("a Classic broker that reported neither a product nor a version was refused: %v. Measured: Classic "+
			"sends neither over AMQP 1.0, so refusing silence refuses the broker itself", err)
	}
}
