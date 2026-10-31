package pulsar_test

import (
	"context"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit"
	pd "github.com/iqhive/iqmq/drivers/pulsar"
	"github.com/iqhive/iqmq/drivers/pulsar/pulsarfake"
)

// The administrative surface against a real broker's web service, named by
// IQMQ_PULSAR_ADMIN_URL (http://127.0.0.1:8080 of the pinned
// apachepulsar/pulsar standalone). The binary protocol is the in-process
// model: these are about what the HTTP API answers, which the fake in
// admin_test.go copies and a real broker is the authority on.

// brokerAdmin opens a backend whose administrative surface is the real
// broker's, with mutate applied to its configuration.
func brokerAdmin(t *testing.T, mutate func(*pd.Config)) (*pd.Backend, mq.Admin) {
	t.Helper()
	url := os.Getenv("IQMQ_PULSAR_ADMIN_URL")
	if url == "" {
		t.Skip("IQMQ_PULSAR_ADMIN_URL is not set; these tests ask a real broker's administrative API")
	}
	server := pulsarfake.New()
	cfg := rigConfig(server)
	cfg.AdminURL = url
	if mutate != nil {
		mutate(&cfg)
	}
	b, err := pd.BackendWithClient(context.Background(), server.Client(), driverkit.ClientOwned, cfg)
	if err != nil {
		t.Fatalf("BackendWithClient: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })
	adm, err := b.Admin()
	if err != nil {
		t.Fatalf("Admin: %v", err)
	}
	return b, adm
}

// uniqueName is a topic name no earlier run left behind.
func uniqueName(prefix string) string {
	return prefix + strconv.FormatInt(time.Now().UnixNano(), 36)
}

// TestBrokerStatsOfAPartitionedTopic is Stats of the partitioned topic
// EnsureTopic creates with Config.Partitions set, and of the subscription
// EnsureSubscription creates on it.
func TestBrokerStatsOfAPartitionedTopic(t *testing.T) {
	_, adm := brokerAdmin(t, func(c *pd.Config) { c.Partitions = 2 })
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	topic := uniqueName("stats")
	if err := adm.EnsureTopic(ctx, mq.TopicSpec{Name: topic}); err != nil {
		t.Fatalf("EnsureTopic: %v", err)
	}
	t.Cleanup(func() { _ = adm.DeleteTopic(context.Background(), topic) })
	if err := adm.EnsureSubscription(ctx, mq.SubscriptionSpec{Topic: topic, Name: "workers"}); err != nil {
		t.Fatalf("EnsureSubscription: %v", err)
	}
	st, err := adm.Stats(ctx, topic, "workers")
	if err != nil {
		t.Fatalf("Stats of the partitioned topic EnsureTopic created = %v (code %v)", err, mq.CodeOf(err))
	}
	if !st.Depth.Known || st.Depth.Value != 0 {
		t.Fatalf("Stats of a new subscription = %+v, want a known depth of zero", st)
	}
}

// TestBrokerListTopicsNamesAPartitionedTopicOnce is ListTopics after
// EnsureTopic created one partitioned topic.
func TestBrokerListTopicsNamesAPartitionedTopicOnce(t *testing.T) {
	prefix := uniqueName("list") + "-"
	_, adm := brokerAdmin(t, func(c *pd.Config) {
		c.Partitions = 2
		c.TopicPrefix = prefix
	})
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := adm.EnsureTopic(ctx, mq.TopicSpec{Name: "orders"}); err != nil {
		t.Fatalf("EnsureTopic: %v", err)
	}
	t.Cleanup(func() { _ = adm.DeleteTopic(context.Background(), "orders") })
	var mine []string
	for name, err := range adm.ListTopics(ctx) {
		if err != nil {
			t.Fatalf("ListTopics: %v", err)
		}
		if name == "orders" || strings.Contains(name, prefix) {
			mine = append(mine, name)
		}
	}
	if len(mine) != 1 || mine[0] != "orders" {
		t.Fatalf("ListTopics yielded %q for the one partitioned topic \"orders\"", mine)
	}
}

// TestBrokerVersionFromTheBroker is DriverInfo.BrokerVersion with an
// administrative endpoint.
func TestBrokerVersionFromTheBroker(t *testing.T) {
	b, _ := brokerAdmin(t, nil)
	v := b.Info().BrokerVersion
	if v == "" || strings.ContainsAny(v, "\"\n ") {
		t.Fatalf("DriverInfo.BrokerVersion is %q although the administrative API was asked", v)
	}
	if want := os.Getenv("IQMQ_PULSAR_VERSION"); want != "" && v != want {
		t.Fatalf("DriverInfo.BrokerVersion is %q, want %q", v, want)
	}
}
