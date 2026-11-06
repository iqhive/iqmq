package kafka_test

import (
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit"
	kq "github.com/iqhive/iqmq/drivers/kafka"
)

// TestATransactionalProducerRefusesABatchingPublisher: a publisher with
// PublisherConfig.Batching holds a PublishAsync in the core's buffer, and the
// batch reaches the producer after the call has returned. On a transactional
// producer that batch missed the commit of the transaction it was published
// in, and franz-go refused it as produced outside a transaction; or it joined
// the next transaction, and was lost with it when that one aborted, while its
// callback had reported it published. The driver refuses such a publisher at
// open with CodeInvalidConfig, on a producer it opened and on one the caller
// built alike. A publisher without Batching on the same producer still opens,
// and so does a Batching publisher on a producer that is not transactional.
//
// The root module tells a sink that its publisher batches from v1.1.2
// (mq.SinkConfig.Batching), so the test skips against an older root.
func TestATransactionalProducerRefusesABatchingPublisher(t *testing.T) {
	if v, old := rootModuleBefore(t, "v1.1.2"); old {
		t.Skipf("built against root module %s, which does not tell a sink that its publisher batches; the version bump of the next release runs this test", v)
	}
	topic := newTopic("txbatch")
	c := cluster(t, 1, []string{topic})
	batching := mq.PublisherConfig{Topic: topic, Batching: mq.BatchingConfig{MaxMessages: 100, Linger: 50 * time.Millisecond}}
	refused := func(t *testing.T, cl *mq.Client) {
		t.Helper()
		p, err := cl.Publisher(t.Context(), batching)
		if err == nil {
			_ = p.Close()
			t.Fatal("a publisher with Batching opened on a transactional producer, so its batch can leave after the transaction it was published in has ended")
		}
		if mq.CodeOf(err) != mq.CodeInvalidConfig || !strings.Contains(err.Error(), "transactional producer") {
			t.Fatalf("Publisher with Batching on a transactional producer = %v (code %v), want the driver's CodeInvalidConfig refusal", err, mq.CodeOf(err))
		}
		plain, err := cl.Publisher(t.Context(), mq.PublisherConfig{Topic: topic})
		if err != nil {
			t.Fatalf("a publisher without Batching on the same producer = %v, want it opened", err)
		}
		_ = plain.Close()
	}

	t.Run("a producer the driver opened", func(t *testing.T) {
		refused(t, openOn(t, c, func(cfg *kq.Config) { cfg.TransactionalID = "txbatch-" + topic }))
	})
	t.Run("a client the caller built", func(t *testing.T) {
		kcl, err := kgo.NewClient(kgo.SeedBrokers(c.ListenAddrs()...), kgo.TransactionalID("txbatch-own-"+topic))
		if err != nil {
			t.Fatalf("kgo.NewClient: %v", err)
		}
		cl, err := kq.OpenWithClient(t.Context(), kcl, driverkit.ClientOwned, config(c))
		if err != nil {
			kcl.Close()
			t.Fatalf("OpenWithClient: %v", err)
		}
		t.Cleanup(func() { _ = cl.Close() })
		refused(t, cl)
	})
	t.Run("a producer that is not transactional", func(t *testing.T) {
		p, err := openOn(t, c).Publisher(t.Context(), batching)
		if err != nil {
			t.Fatalf("a publisher with Batching on a producer that is not transactional = %v, want it opened", err)
		}
		_ = p.Close()
	})
}
