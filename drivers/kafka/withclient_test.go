package kafka_test

import (
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/plain"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit"
	kq "github.com/iqhive/iqmq/drivers/kafka"
)

// TestAConsumerReachesTheClusterTheCallersClientReaches: a consumer of a
// client opened over OpenWithClient builds a group client of its own, since a
// franz-go client consumes at most one group, and it built it from Config
// alone. With no Config.Brokers it dialled DefaultBroker, 127.0.0.1:9092, not
// the caller's cluster; and a cluster that needs what only the caller's client
// carries, its SASL mechanism or its dialler, never let it in. Either way the
// consumer opened without an error and received nothing.
func TestAConsumerReachesTheClusterTheCallersClientReaches(t *testing.T) {
	for _, tc := range []struct {
		name string
		// sasl starts the cluster with SASL/PLAIN on.
		sasl bool
		// caller builds the caller's client for the cluster.
		caller func(addrs []string) []kgo.Opt
		// brokers is what Config.Brokers names.
		brokers func(addrs []string) []string
	}{
		{
			name:    "Config names no broker",
			caller:  func(addrs []string) []kgo.Opt { return []kgo.Opt{kgo.SeedBrokers(addrs...)} },
			brokers: func([]string) []string { return nil },
		},
		{
			name: "only the caller's client has the SASL mechanism",
			sasl: true,
			caller: func(addrs []string) []kgo.Opt {
				return []kgo.Opt{kgo.SeedBrokers(addrs...), kgo.SASL(plain.Auth{User: "admin", Pass: "adminpass"}.AsMechanism())}
			},
			brokers: func(addrs []string) []string { return addrs },
		},
		{
			name: "only the caller's dialler reaches the seed broker",
			caller: func(addrs []string) []kgo.Opt {
				var d net.Dialer
				return []kgo.Opt{
					kgo.SeedBrokers("kafka.invalid:9092"),
					kgo.Dialer(func(ctx context.Context, network, host string) (net.Conn, error) {
						if host == "kafka.invalid:9092" {
							host = addrs[0]
						}
						return d.DialContext(ctx, network, host)
					}),
				}
			},
			brokers: func([]string) []string { return []string{"kafka.invalid:9092"} },
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			topic := newTopic("withclient")
			var opts []kfake.Opt
			if tc.sasl {
				opts = append(opts, kfake.EnableSASL(), kfake.Superuser("PLAIN", "admin", "adminpass"))
			}
			cl := cluster(t, 1, []string{topic}, opts...)
			kcl, err := kgo.NewClient(tc.caller(cl.ListenAddrs())...)
			if err != nil {
				t.Fatalf("kgo.NewClient: %v", err)
			}
			c, err := kq.OpenWithClient(t.Context(), kcl, driverkit.ClientOwned,
				config(cl, func(cfg *kq.Config) { cfg.Brokers = tc.brokers(cl.ListenAddrs()) }))
			if err != nil {
				kcl.Close()
				t.Fatalf("OpenWithClient: %v", err)
			}
			t.Cleanup(func() { _ = c.Close() })
			h := consumerFrom(t, c, topic, "g-withclient", mq.StartEarliest())
			publish(t, c, topic, &mq.Message{Body: []byte("hello")})
			d, err := tryReceiveOne(t, h, 10*time.Second)
			if err != nil {
				hs := c.Health()
				t.Fatalf("the consumer received nothing (%v), health %v (%v): its group client did not reach "+
					"the cluster the caller's client reaches", err, hs.State, hs.Err)
			}
			if err := d.Ack(); err != nil {
				t.Fatalf("Ack: %v", err)
			}
		})
	}
}

// newClients counts the clients franz-go builds with it among their hooks.
type newClients struct{ n atomic.Int32 }

func (h *newClients) OnNewClient(*kgo.Client) { h.n.Add(1) }

// TestAConsumerTakesNoHookOfTheCallersClient: a hook is bound to the client
// it was built with. x/kafkatx's TrackRecords ledger refuses a transaction
// while it is on a second open client, and a metrics hook registers its
// metrics once, so a consumer's group client takes none of the caller's
// client's hooks, though it takes the rest of its client options.
func TestAConsumerTakesNoHookOfTheCallersClient(t *testing.T) {
	topic := newTopic("withclient-hook")
	cl := cluster(t, 1, []string{topic})
	hook := &newClients{}
	kcl, err := kgo.NewClient(kgo.SeedBrokers(cl.ListenAddrs()...), kgo.WithHooks(hook))
	if err != nil {
		t.Fatalf("kgo.NewClient: %v", err)
	}
	c, err := kq.OpenWithClient(t.Context(), kcl, driverkit.ClientOwned, config(cl, func(cfg *kq.Config) { cfg.Brokers = nil }))
	if err != nil {
		kcl.Close()
		t.Fatalf("OpenWithClient: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	h := consumerFrom(t, c, topic, "g-withclient-hook", mq.StartEarliest())
	publish(t, c, topic, &mq.Message{Body: []byte("hello")})
	if err := receiveOne(t, h, 10*time.Second).Ack(); err != nil {
		t.Fatalf("Ack: %v", err)
	}
	if n := hook.n.Load(); n != 1 {
		t.Fatalf("the caller's hook is on %d clients, want 1: the consumer's group client took it", n)
	}
}

// TestANackMovesThroughTheClusterTheCallersClientReaches: a record a consumer
// moves to a dead-letter topic goes through a producer of the driver's own
// when the producer is transactional, and that producer was built from Config
// alone as the group client was, so on a transactional client the caller
// built, with no Config.Brokers, every move dialled DefaultBroker and failed.
func TestANackMovesThroughTheClusterTheCallersClientReaches(t *testing.T) {
	topic, dlq := newTopic("withclient-move"), newTopic("withclient-move-dead")
	cl := cluster(t, 1, []string{topic, dlq})
	plain := openOn(t, cl)
	kcl, err := kgo.NewClient(kgo.SeedBrokers(cl.ListenAddrs()...), kgo.TransactionalID("withclient-move-"+topic))
	if err != nil {
		t.Fatalf("kgo.NewClient: %v", err)
	}
	c, err := kq.OpenWithClient(t.Context(), kcl, driverkit.ClientOwned, config(cl, func(cfg *kq.Config) {
		cfg.Brokers = nil
		cfg.NackConfig.DeadLetterTopic = dlq
	}))
	if err != nil {
		kcl.Close()
		t.Fatalf("OpenWithClient: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	cons := consumerFrom(t, c, topic, "g-withclient-move", mq.StartEarliest())
	publish(t, plain, topic, &mq.Message{Body: []byte("poison")})
	d, err := tryReceiveOne(t, cons, 10*time.Second)
	if err != nil {
		t.Fatalf("the consumer received nothing (%v): its group client did not reach the caller's cluster", err)
	}
	if err := d.Reject(); err != nil {
		t.Fatalf("Reject = %v (code %v), want the copy on the dead-letter topic: the move did not reach the caller's cluster",
			err, mq.CodeOf(err))
	}
	got := receiveOne(t, consumerFrom(t, plain, dlq, "g-withclient-move-reader", mq.StartEarliest()), 10*time.Second)
	if string(got.Body) != "poison" {
		t.Fatalf("the dead-letter topic holds %q, want poison", got.Body)
	}
	if err := got.Ack(); err != nil {
		t.Fatalf("Ack: %v", err)
	}
}
