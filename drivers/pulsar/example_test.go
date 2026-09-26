package pulsar_test

import (
	"context"
	"fmt"
	"log"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit"
	pd "github.com/iqhive/iqmq/drivers/pulsar"
	"github.com/iqhive/iqmq/drivers/pulsar/pulsarfake"
	"github.com/iqhive/iqmq/dsn"
)

// Example shows the shape of a Pulsar client: the namespace comes from the
// configuration and a topic is a local name inside it.
func Example() {
	cfg := pd.Config{
		ServiceURL: "pulsar://broker:6650",
		AdminURL:   "http://broker:8080",
		Tenant:     "acme",
		Namespace:  "billing",
		// Shared is the default and claims no ordering at all. Key_Shared
		// is what a keyed pipeline wants, and it claims per-key ordering
		// conditionally: see the capability table.
		SubscriptionType: pd.SubscriptionKeyShared,
		Token:            dsn.Secret("a-jwt-token"),
	}
	fmt.Println(cfg.String())
	// Output:
	// pulsar.Config{ServiceURL:pulsar://broker:6650 AdminURL:http://broker:8080 Topics:persistent://acme/billing SubscriptionType:keyshared Metadata:nativestrict Token:redacted}
}

// Example_topicNaming shows the two forms a topic may take, and the one that is
// refused.
func Example_topicNaming() {
	cfg := pd.Config{Tenant: "acme", Namespace: "billing", TopicPrefix: "iqmq-"}
	for _, topic := range []string{"orders", "persistent://other/ns/orders", "other/ns/orders"} {
		got, err := pd.TopicOf(cfg, topic)
		if err != nil {
			fmt.Printf("%-32s refused\n", topic)
			continue
		}
		fmt.Printf("%-32s %s\n", topic, got)
	}
	// Output:
	// orders                           persistent://acme/billing/iqmq-orders
	// persistent://other/ns/orders     persistent://other/ns/orders
	// other/ns/orders                  refused
}

// Example_dsn shows the connection string, whose path is the namespace.
func Example_dsn() {
	cfg, err := pd.ParseDSN("pulsar://broker:6650/acme/billing?subscriptiontype=keyshared&acktimeout=30s")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(cfg.Tenant, cfg.Namespace, cfg.SubscriptionType, cfg.AckTimeout)
	// Output:
	// acme billing keyshared 30s
}

// Example_orderingClaims prints what each of the four subscription types lets
// this driver claim about the ordering, which is the one thing a caller
// planning a keyed pipeline has to know.
func Example_orderingClaims() {
	for _, kind := range pd.SubscriptionKinds() {
		b, err := pd.BackendWithClient(context.Background(), pulsarfake.New().Client(),
			driverkit.ClientOwned, pd.Config{SubscriptionType: kind})
		if err != nil {
			log.Fatal(err)
		}
		caps := b.Capabilities()
		fmt.Printf("%-10s key=%-11s partition=%-11s global=%s\n", kind,
			caps.Support(mq.FeatureOrderedByKey).Level,
			caps.Support(mq.FeatureOrderedByPartition).Level,
			caps.Support(mq.FeatureOrderedGlobally).Level)
		_ = b.Close()
	}
	// Output:
	// shared     key=unsupported partition=unsupported global=unsupported
	// keyshared  key=conditional partition=unsupported global=unsupported
	// exclusive  key=conditional partition=native      global=conditional
	// failover   key=conditional partition=native      global=conditional
}
